//! Process-wide bell syscall counters and receiver-local inbox observations.

#[cfg(feature = "probe")]
use std::{
    io,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(feature = "probe")]
struct Calls {
    attempts: AtomicU64,
    nanos: AtomicU64,
    would_block: AtomicU64,
    errors: AtomicU64,
}

#[cfg(feature = "probe")]
impl Calls {
    const fn new() -> Self {
        Self {
            attempts: AtomicU64::new(0),
            nanos: AtomicU64::new(0),
            would_block: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    fn record<T>(
        &self,
        call: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        let start = std::time::Instant::now();
        let result = call();
        self.nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if let Err(error) = &result {
            let count = if error.kind() == io::ErrorKind::WouldBlock {
                &self.would_block
            } else {
                &self.errors
            };
            count.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

#[cfg(feature = "probe")]
static SEND: Calls = Calls::new();

#[cfg(feature = "probe")]
static RECV: Calls = Calls::new();

/// Process-wide doorbell socket calls and their elapsed times.
#[cfg(feature = "probe")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BellCounts {
    pub rings: u64,
    pub ring_nanos: u64,
    pub ring_would_block: u64,
    pub ring_errors: u64,
    pub reads: u64,
    pub read_nanos: u64,
    pub empty_reads: u64,
    pub read_errors: u64,
}

/// Process totals since start. A ring attempt includes failed sends.
#[cfg(feature = "probe")]
pub fn bell_counts() -> BellCounts {
    let load = |count: &AtomicU64| count.load(Ordering::Relaxed);
    BellCounts {
        rings: load(&SEND.attempts),
        ring_nanos: load(&SEND.nanos),
        ring_would_block: load(&SEND.would_block),
        ring_errors: load(&SEND.errors),
        reads: load(&RECV.attempts),
        read_nanos: load(&RECV.nanos),
        empty_reads: load(&RECV.would_block),
        read_errors: load(&RECV.errors),
    }
}

#[inline(always)]
pub(crate) fn ring<T>(
    send: impl FnOnce() -> std::io::Result<T>
) -> std::io::Result<T> {
    #[cfg(feature = "probe")]
    let result = SEND.record(send);
    #[cfg(not(feature = "probe"))]
    let result = send();
    result
}

#[cfg(feature = "tokio")]
#[inline(always)]
pub(crate) fn read<T>(
    recv: impl FnOnce() -> std::io::Result<T>
) -> std::io::Result<T> {
    #[cfg(feature = "probe")]
    let result = RECV.record(recv);
    #[cfg(not(feature = "probe"))]
    let result = recv();
    result
}

#[cfg(all(feature = "probe", feature = "tokio"))]
pub(crate) use lane::LaneObservation;
#[cfg(all(feature = "probe", feature = "tokio"))]
pub use lane::{LaneCounts, LaneProbe};

#[cfg(all(feature = "probe", feature = "tokio"))]
mod lane {
    use std::sync::{Arc, Mutex};

    /// Completed receive batches for one receiver incarnation.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct LaneCounts {
        /// Successful readiness returns after at least one Pending poll.
        pub wakes: u64,
        /// Successful readiness returns on the first poll.
        pub immediate: u64,
        /// Data frames following a wake, until the next readiness wait.
        pub woken_frames: u64,
        /// Data frames following first-poll readiness.
        pub immediate_frames: u64,
        /// Data frames delivered without a preceding readiness return.
        pub direct_frames: u64,
        /// Directory, capacity and event notifications.
        pub notifications: u64,
        /// Completed wake batches with 0, 1, 2 or 3+ data frames.
        pub frames_per_wake: [u64; 4],
    }

    /// Snapshot handle for a single inbox receiver.
    #[derive(Clone, Default)]
    pub struct LaneProbe(Arc<Mutex<LaneCounts>>);

    impl LaneProbe {
        /// Counters include batches closed by the next wait or receiver drop.
        pub fn snapshot(&self) -> LaneCounts {
            *self.0.lock().expect("lane probe")
        }
    }

    #[derive(Default)]
    pub(crate) struct LaneObservation {
        pub(crate) probe: LaneProbe,
        // None before any readiness return, Some(false) if already readable,
        // Some(true) after a wait that actually returned Pending.
        readiness: Option<bool>,
        frames: u64,
        notifications: u64,
    }

    impl LaneObservation {
        pub(crate) fn frame(&mut self) {
            self.frames += 1;
        }

        pub(crate) fn notification(&mut self) {
            self.notifications += 1;
        }

        pub(crate) fn begin(
            &mut self,
            was_pending: bool,
        ) {
            self.readiness = Some(was_pending);
        }

        pub(crate) fn finish(&mut self) {
            let frames = std::mem::take(&mut self.frames);
            let notifications = std::mem::take(&mut self.notifications);
            let readiness = self.readiness.take();
            if readiness.is_none() && frames == 0 && notifications == 0 {
                return;
            }
            let mut counts = self.probe.0.lock().expect("lane probe");
            counts.notifications += notifications;
            match readiness {
                Some(true) => {
                    counts.wakes += 1;
                    counts.woken_frames += frames;
                    counts.frames_per_wake[frames.min(3) as usize] += 1;
                }
                Some(false) => {
                    counts.immediate += 1;
                    counts.immediate_frames += frames;
                }
                None => counts.direct_frames += frames,
            }
        }
    }

    impl Drop for LaneObservation {
        fn drop(&mut self) {
            self.finish();
        }
    }
}
