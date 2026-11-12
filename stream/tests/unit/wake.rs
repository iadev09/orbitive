use std::time::Duration;

use super::*;

// Same lost-wake bound as the Link receiver tests.
const LOST_WAKE: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Probe {
    generation: AtomicU32,
    waiters: AtomicU32,
    blocking_waiters: u32,
    pending: AtomicBool,
    drained: AtomicBool,
    stop: AtomicBool,
}

impl Probe {
    fn notify(&self) {
        self.pending.store(true, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) > 0 {
            crate::wake_on(&self.generation);
        }
    }
}

struct Door {
    probe: Probe,
    on_drain: fn(&Probe),
}

impl Doorstep for Door {
    fn generation(&self) -> &AtomicU32 {
        &self.probe.generation
    }

    fn driver_waiters(&self) -> &AtomicU32 {
        &self.probe.waiters
    }

    fn drain(&self) {
        let probe = &self.probe;
        assert_eq!(
            probe.waiters.load(Ordering::SeqCst),
            probe.blocking_waiters
        );
        if probe.pending.swap(false, Ordering::SeqCst) {
            probe.drained.store(true, Ordering::SeqCst);
        }
        (self.on_drain)(probe);
    }
}

#[test]
fn a_change_during_drain_skips_park_and_leaves_blocking_waiters_counted() {
    let door = Door {
        probe: Probe {
            waiters: AtomicU32::new(1),
            blocking_waiters: 1,
            ..Probe::default()
        },
        on_drain: |probe| {
            if !probe.drained.load(Ordering::SeqCst) {
                probe.notify();
            }
        },
    };
    run(&door, &door.probe.stop, |word, _| {
        assert!(door.probe.drained.load(Ordering::SeqCst));
        assert_eq!(door.probe.waiters.load(Ordering::SeqCst), 2);
        signal_stop(&door.probe.stop, word);
        Ok(())
    });
    assert_eq!(door.probe.waiters.load(Ordering::SeqCst), 1);
}

#[test]
fn a_change_after_the_final_check_cannot_park_the_driver() {
    if !orbit_core::sync::supported() {
        return;
    }
    let door = Door { probe: Probe::default(), on_drain: |_| {} };
    run(&door, &door.probe.stop, |word, seen| {
        assert_eq!(door.probe.waiters.load(Ordering::SeqCst), 1);
        if door.probe.drained.load(Ordering::SeqCst) {
            signal_stop(&door.probe.stop, word);
        } else {
            door.probe.notify();
        }
        assert!(crate::wait_on_timeout(word, seen, LOST_WAKE)?);
        Ok(())
    });
    assert!(door.probe.drained.load(Ordering::SeqCst));
    assert_eq!(door.probe.waiters.load(Ordering::SeqCst), 0);
}

#[test]
fn stop_during_drain_skips_park() {
    let door = Door {
        probe: Probe::default(),
        on_drain: |probe| signal_stop(&probe.stop, &probe.generation),
    };
    run(&door, &door.probe.stop, |_, _| panic!("parked after shutdown"));
    assert_eq!(door.probe.waiters.load(Ordering::SeqCst), 0);
}
