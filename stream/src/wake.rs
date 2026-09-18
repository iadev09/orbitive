//! Process-local readiness: the wakers a poll left behind, and the one
//! thread per process that turns a node's doorbell into those wakes.
//!
//! Nothing here crosses a process boundary. The shared side is a doorbell
//! generation and two bitmaps in the segment (`layout`); the driver parks
//! on the generation with the platform's address wait, swaps the pending
//! bitmap out, and wakes whichever local tasks registered on those slots.
//! A wake is a hint: every task re-checks the direction it is waiting on.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::thread::JoinHandle;

use crate::lock_unpoisoned;

/// Which task on a slot a waker belongs to: reader or writer of one direction.
#[derive(Clone, Copy)]
pub(crate) struct Interest {
    pub(crate) direction: usize,
    pub(crate) writer: bool,
}

impl Interest {
    fn index(self) -> usize {
        self.direction * 2 + usize::from(self.writer)
    }
}

#[derive(Default)]
struct SlotWakers {
    wakers: [Option<Waker>; 4],
}

/// One entry per slot in the table, sized once at open, plus the one task
/// waiting for a stream to be offered to this node.
pub(crate) struct Registry {
    slots: Box<[Mutex<SlotWakers>]>,
    offer: Mutex<Option<Waker>>,
}

impl Registry {
    pub(crate) fn new(total_slots: usize) -> Self {
        Self {
            slots: (0..total_slots)
                .map(|_| Mutex::new(SlotWakers::default()))
                .collect(),
            offer: Mutex::new(None),
        }
    }

    /// Remember `waker` for one task; a second registration by the same
    /// task replaces the first, which is the `AsyncRead` contract.
    pub(crate) fn register(&self, slot: usize, interest: Interest, waker: &Waker) {
        let mut entry = lock_unpoisoned(&self.slots[slot]);
        let place = &mut entry.wakers[interest.index()];
        match place {
            Some(existing) if existing.will_wake(waker) => {}
            _ => *place = Some(waker.clone()),
        }
    }

    pub(crate) fn register_offer(&self, waker: &Waker) {
        let mut place = lock_unpoisoned(&self.offer);
        match &*place {
            Some(existing) if existing.will_wake(waker) => {}
            _ => *place = Some(waker.clone()),
        }
    }

    /// Wake every task parked on the slot. They re-check; a wake nobody
    /// needed costs one poll.
    pub(crate) fn wake(&self, slot: usize) {
        let taken = {
            let mut entry = lock_unpoisoned(&self.slots[slot]);
            std::mem::take(&mut entry.wakers)
        };
        for waker in taken.into_iter().flatten() {
            waker.wake();
        }
    }

    pub(crate) fn wake_offer(&self) {
        if let Some(waker) = lock_unpoisoned(&self.offer).take() {
            waker.wake();
        }
    }

    pub(crate) fn clear(&self) {
        for slot in &self.slots {
            lock_unpoisoned(slot).wakers = Default::default();
        }
        lock_unpoisoned(&self.offer).take();
    }
}

/// What the driver thread needs: everything it reads lives in the table,
/// which outlives the thread because the table joins it on drop.
pub(crate) trait Doorstep: Send + Sync + 'static {
    /// The generation word of this process's node.
    fn generation(&self) -> &AtomicU32;
    /// Called once when the driver starts and once when it stops.
    fn listening(&self, delta: i32);
    /// Swap out every pending bit for this node and wake the local tasks;
    /// wake the offer waiter if an offer is pending.
    fn drain(&self);
}

pub(crate) struct Driver {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    /// The process that spawned the thread; a forked child inherits this
    /// struct and no thread, and must not try to join one.
    pid: u32,
}

impl Driver {
    /// Start the thread. `target` is a raw pointer only because the table
    /// that owns the driver also owns everything the pointer reaches, and
    /// stops the thread before any of it is freed.
    pub(crate) fn start<T: Doorstep>(target: *const T, name: String) -> std::io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let target = SendPtr(target);
        let thread = std::thread::Builder::new().name(name).spawn(move || {
            let target = target;
            // SAFETY: `Table::drop` sets `stop`, rings its own doorbell and
            // joins this thread before the mapping goes away.
            let table = unsafe { &*target.0 };
            run(table, &thread_stop);
        })?;
        Ok(Self {
            stop,
            thread: Some(thread),
            pid: std::process::id(),
        })
    }

    pub(crate) fn stop(&mut self, generation: &AtomicU32) {
        self.stop.store(true, Ordering::Release);
        // Change the word before waking, as the ring bridge does: a driver
        // between its stop check and its park then fails the compare and
        // never parks after our wake.
        generation.fetch_add(1, Ordering::SeqCst);
        crate::wake_on(generation);
        if let Some(thread) = self.thread.take()
            && self.pid == std::process::id()
        {
            let _ = thread.join();
        }
    }
}

struct SendPtr<T>(*const T);

// SAFETY: the pointee is `Sync` and outlives the thread (see `Driver::start`).
unsafe impl<T: Sync> Send for SendPtr<T> {}

fn run<T: Doorstep>(table: &T, stop: &AtomicBool) {
    let generation = table.generation();
    table.listening(1);
    let mut seen = generation.load(Ordering::SeqCst);
    while !stop.load(Ordering::Acquire) {
        table.drain();
        // A bit set after the drain comes with a bump after this load, so
        // either we see it here or the park returns at once: never neither.
        let now = generation.load(Ordering::SeqCst);
        if now != seen {
            seen = now;
            continue;
        }
        // The table refused to open where the platform cannot wait, so a
        // failure here is the table going away, not a system to poll around.
        if crate::wait_on(generation, seen).is_err() {
            break;
        }
    }
    table.listening(-1);
}
