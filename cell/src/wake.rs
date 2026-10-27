//! A parking generation distinct from the cell's value/seqlock version.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::{Error, Result};

#[repr(C)]
pub(crate) struct Wake {
    sequence: AtomicU32,
    pub(crate) waiters: AtomicU32,
}

impl Wake {
    pub(crate) fn new() -> Self {
        Self { sequence: AtomicU32::new(0), waiters: AtomicU32::new(0) }
    }

    /// Only called during installation, after every old handle has gone.
    pub(crate) fn reset(&self) {
        self.sequence.store(0, Ordering::Relaxed);
        self.waiters.store(0, Ordering::Relaxed);
    }

    /// An unwatched value write still pays only its existing waiter load.
    #[inline]
    pub(crate) fn changed(&self) {
        if self.waiters.load(Ordering::SeqCst) != 0 {
            self.notify();
        }
    }

    /// Retain the wake before issuing it, including when nobody is parked.
    pub(crate) fn notify(&self) {
        // Never wrap back to an outstanding native wait's expected value.
        // Once exhausted, later wait attempts fail instead of parking.
        let _ = self.sequence.try_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |sequence| sequence.checked_add(1),
        );
        #[cfg(test)]
        crate::text::tests::at(crate::text::tests::Phase::Sequence);
        crate::wake_on(&self.sequence);
    }

    /// Register, snapshot the native comparison word, then recheck the value
    /// and interrupt. A later notify changes the kernel's expected word; an
    /// earlier notify orders the caller's interrupt store before its recheck.
    pub(crate) fn wait_if(
        &self,
        should_wait: impl FnOnce() -> bool,
    ) -> Result<()> {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        struct Registration<'a>(&'a AtomicU32);
        impl Drop for Registration<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _registration = Registration(&self.waiters);
        let observed = self.sequence.load(Ordering::SeqCst);
        if should_wait() {
            if observed == u32::MAX {
                return Err(Error::WakeExhausted);
            }
            #[cfg(test)]
            before_park();
            crate::wait_on(&self.sequence, observed)?;
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    pub(crate) static BEFORE_PARK: std::cell::RefCell<Option<Box<dyn FnMut()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn before_park() {
    BEFORE_PARK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, mpsc};

    use crate::{Cells, Orbital, Text};
    use orbit_core::Fleet;

    #[test]
    fn exhausted_wake_sequence_never_wraps_or_parks() {
        let wake = Wake::new();
        wake.sequence.store(u32::MAX - 1, Ordering::SeqCst);
        wake.notify();
        wake.notify();
        assert_eq!(wake.sequence.load(Ordering::SeqCst), u32::MAX);
        assert!(matches!(wake.wait_if(|| true), Err(Error::WakeExhausted)));
        assert_eq!(wake.waiters.load(Ordering::SeqCst), 0);
        assert!(
            wake.wait_if(|| false).is_ok(),
            "an interrupt can still end the wait"
        );
        wake.reset();
        assert_eq!(wake.sequence.load(Ordering::SeqCst), 0);
    }

    #[derive(Clone)]
    enum Cell {
        Scalar(Orbital<u64>),
        Text(Text),
    }

    impl Cell {
        fn new(text: bool) -> Self {
            let cells =
                Cells::new(Arc::new(Fleet::join("example", 1).unwrap()))
                    .unwrap();
            if text {
                Self::Text(cells.allocate_text("old").unwrap())
            } else {
                Self::Scalar(cells.allocate(7_u64).unwrap())
            }
        }
        fn version(&self) -> u64 {
            match self {
                Self::Scalar(c) => u64::from(c.version().unwrap()),
                Self::Text(c) => c.version().unwrap(),
            }
        }
        fn wake(&self) {
            match self {
                Self::Scalar(c) => c.wake_waiters().unwrap(),
                Self::Text(c) => c.wake_waiters().unwrap(),
            }
        }
        fn write(&self) {
            match self {
                Self::Scalar(c) => c.store(42).unwrap(),
                Self::Text(c) => c.store("new").unwrap(),
            }
        }
        fn wait(
            &self,
            since: u64,
        ) -> u64 {
            match self {
                Self::Scalar(c) => u64::from(
                    c.wait_changed(since.try_into().unwrap()).unwrap(),
                ),
                Self::Text(c) => c.wait_changed(since).unwrap(),
            }
        }
        fn wait_until(
            &self,
            since: u64,
            stop: &AtomicBool,
        ) -> Option<u64> {
            match self {
                Self::Scalar(c) => c
                    .wait_changed_until(since.try_into().unwrap(), stop)
                    .unwrap()
                    .map(u64::from),
                Self::Text(c) => c.wait_changed_until(since, stop).unwrap(),
            }
        }
    }

    fn interrupt_in_gap(text: bool) {
        let cell = Cell::new(text);
        let since = cell.version();
        assert_eq!(cell.wait_until(since, &AtomicBool::new(true)), None);
        let stop = Arc::new(AtomicBool::new(false));
        let interrupt = stop.clone();
        let waiter = cell.clone();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            BEFORE_PARK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }))
            });
            waiter.wait_until(since, &interrupt)
        });
        ready_rx.recv().unwrap();
        stop.store(true, Ordering::SeqCst);
        cell.wake();
        assert_eq!(cell.version(), since, "cancellation is not a value write");
        resume_tx.send(()).unwrap();
        assert_eq!(
            thread.join().unwrap(),
            None,
            "one interrupt wake must suffice"
        );
        assert_eq!(cell.version(), since);
    }

    fn wake_is_not_a_write(text: bool) {
        let cell = Cell::new(text);
        let since = cell.version();
        let waiter = cell.clone();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            BEFORE_PARK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }))
            });
            waiter.wait(since)
        });
        ready_rx.recv().unwrap();
        cell.wake();
        resume_tx.send(()).unwrap();
        // A wake without a write must reach a second park, not return a
        // counterfeit value version. This signal replaces any timing guess.
        ready_rx.recv().unwrap();
        assert_eq!(cell.version(), since);
        cell.write();
        let written = cell.version();
        resume_tx.send(()).unwrap();
        assert_eq!(thread.join().unwrap(), written);
        assert_ne!(written, since);
    }

    #[test]
    fn scalar_interrupt_between_check_and_park_is_retained() {
        interrupt_in_gap(false);
    }
    #[test]
    fn text_interrupt_between_check_and_park_is_retained() {
        interrupt_in_gap(true);
    }
    #[test]
    fn scalar_wake_is_not_a_value_write() {
        wake_is_not_a_write(false);
    }
    #[test]
    fn text_wake_is_not_a_value_write() {
        wake_is_not_a_write(true);
    }
}
