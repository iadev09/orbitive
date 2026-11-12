//! Counts of the kernel calls an inbox bell makes, for this crate's benchmarks.
//!
//! Compiled only with the `probe` feature. Without it the counting calls are
//! empty and nothing here exists. The counts are per process and are not part
//! of the crate's API: a benchmark reads them before and after a run.

#[cfg(feature = "probe")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "probe")]
static RINGS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "probe")]
static RING_NANOS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "probe")]
static READS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "probe")]
static EMPTY_READS: AtomicU64 = AtomicU64::new(0);

/// What this process's bells have cost so far.
#[cfg(feature = "probe")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BellCounts {
    /// Datagrams sent to ring a bell.
    pub rings: u64,
    /// Nanoseconds spent inside those sends.
    pub ring_nanos: u64,
    /// Reads of a bell socket, whatever they found.
    pub reads: u64,
    /// Reads of a bell socket that found nothing.
    pub empty_reads: u64,
}

/// This process's bell counts since it started.
#[cfg(feature = "probe")]
pub fn bell_counts() -> BellCounts {
    BellCounts {
        rings: RINGS.load(Ordering::Relaxed),
        ring_nanos: RING_NANOS.load(Ordering::Relaxed),
        reads: READS.load(Ordering::Relaxed),
        empty_reads: EMPTY_READS.load(Ordering::Relaxed),
    }
}

/// Send one bell datagram, counting it and the time the send took.
#[inline(always)]
pub(crate) fn ring<T>(send: impl FnOnce() -> T) -> T {
    #[cfg(feature = "probe")]
    {
        let began = std::time::Instant::now();
        let sent = send();
        RING_NANOS
            .fetch_add(began.elapsed().as_nanos() as u64, Ordering::Relaxed);
        RINGS.fetch_add(1, Ordering::Relaxed);
        sent
    }
    #[cfg(not(feature = "probe"))]
    send()
}

/// A bell socket was read; `found` says whether a datagram was there.
#[inline(always)]
#[cfg_attr(not(feature = "probe"), allow(unused_variables))]
#[cfg_attr(not(feature = "tokio"), allow(dead_code))]
pub(crate) fn read(found: bool) {
    #[cfg(feature = "probe")]
    {
        READS.fetch_add(1, Ordering::Relaxed);
        if !found {
            EMPTY_READS.fetch_add(1, Ordering::Relaxed);
        }
    }
}
