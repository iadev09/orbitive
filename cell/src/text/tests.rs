use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};

use super::*;
use crate::Cells;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Claim,
    Stage,
    Prepared,
    Commit,
    Sequence,
    Woken,
    RecoveredWake,
    Read,
}

type Hook = Box<dyn FnMut(Phase)>;
thread_local! {
    static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
}

pub(crate) fn at(phase: Phase) {
    HOOK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(phase);
        }
    });
}

fn memory() -> TextBackend {
    TextBackend::new(&Arc::new(Fleet::join("example", 1).unwrap())).unwrap()
}

#[test]
fn a_retired_text_writer_cannot_cross_into_a_new_generation() {
    let table = memory();
    let old = table.allocate("old").unwrap();
    let id = old.id();
    let _held: Vec<_> = (1..CELL_TEXT_CAPACITY)
        .map(|_| table.allocate("held").unwrap())
        .collect();
    let slot = old.slot().unwrap();
    let write = slot.writer(table.writer_token()).unwrap();
    slot.wake.waiters.fetch_add(1, Ordering::SeqCst);
    table.release(id).unwrap();
    assert!(matches!(table.allocate("new"), Err(Error::Full { .. })));
    write.write(b"late", false).unwrap();
    assert_eq!(slot.wake.waiters.fetch_sub(1, Ordering::SeqCst), 1);
    drop(write);
    drop(old);
    let fresh = table.allocate("new").unwrap();
    assert_eq!(fresh.id().index(), id.index());
    assert_ne!(fresh.id().generation(), id.generation());
    assert_eq!(fresh.slot().unwrap().wake.waiters.load(Ordering::Acquire), 0);
    assert_eq!(fresh.load().unwrap(), "new");
}

#[test]
fn exhausted_text_generations_are_never_reissued() {
    let table = memory();
    table
        .with_structure(|slots, _, _| {
            for slot in slots {
                slot.generation.store(u32::MAX, Ordering::Relaxed);
            }
            Ok(())
        })
        .unwrap();
    assert!(matches!(table.allocate("new"), Err(Error::Full { .. })));
}

#[test]
fn delayed_text_copy_refuses_a_reused_bank_without_retrying() {
    let table = memory();
    let text = table.allocate("old").unwrap();
    let reader = text.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |phase| {
                if phase == Phase::Read {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }
            }))
        });
        reader.load()
    });
    ready_rx.recv().unwrap();
    text.store("first").unwrap();
    text.store("second").unwrap();
    resume_tx.send(()).unwrap();
    assert!(matches!(thread.join().unwrap(), Err(Error::Contended)));
    assert_eq!(text.load().unwrap(), "second");
}

#[test]
fn text_publication_exhaustion_never_changes_the_committed_bank() {
    let text = memory().allocate("old").unwrap();
    let slot = text.slot().unwrap();
    let last = u64::MAX - 1; // maximum revision, bank zero
    slot.committed.store(last, Ordering::Release);
    assert!(matches!(text.store("new"), Err(Error::RevisionExhausted)));
    assert_eq!(text.version().unwrap(), last);
    assert_eq!(text.load().unwrap(), "old");
    assert_eq!(slot.writer.load(Ordering::Acquire), 0);
}

#[test]
fn a_live_text_writer_returns_contention_and_raii_completes_its_wake() {
    let text = memory().allocate("old").unwrap();
    let before = text.version().unwrap();
    let writer = text.slot().unwrap().writer(1).unwrap();
    assert!(matches!(text.store("other"), Err(Error::Contended)));
    assert_eq!(text.load().unwrap(), "old");
    writer.write(b"new", false).unwrap();
    drop(writer);
    assert_ne!(text.version().unwrap(), before);
    assert_eq!(text.load().unwrap(), "new");
    assert_eq!(text.table.recover_writers().unwrap(), 0);
}

#[test]
fn text_writer_unwind_releases_ownership_and_finishes_committed_notification() {
    for target in [Phase::Claim, Phase::Stage, Phase::Prepared, Phase::Commit] {
        let text = memory().allocate("old").unwrap();
        let before = text.version().unwrap();
        let mut fired = false;
        HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |phase| {
                if !fired && phase == target {
                    fired = true;
                    panic!("injected text writer unwind");
                }
            }))
        });
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                text.store("new")
            }));
        HOOK.with(|hook| *hook.borrow_mut() = None);
        assert!(result.is_err());
        let committed = target == Phase::Commit;
        assert_eq!(text.load().unwrap(), if committed { "new" } else { "old" });
        assert_eq!(text.version().unwrap() != before, committed);
        text.append("!").unwrap();
    }
}

#[test]
fn text_retirement_wakes_a_late_park_without_fabricating_a_commit() {
    let table = memory();
    let text = table.allocate("old").unwrap();
    let version = text.version().unwrap();
    let id = text.id();
    let reader = text.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        crate::wake::BEFORE_PARK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                ready_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
            }))
        });
        reader.wait_changed(version)
    });
    ready_rx.recv().unwrap();
    table.release(id).unwrap();
    assert_eq!(
        table.slots()[id.index() as usize].committed.load(Ordering::Acquire),
        version
    );
    resume_tx.send(()).unwrap();
    assert!(
        matches!(thread.join().unwrap(), Err(Error::StaleText(stale)) if stale == id)
    );
}

#[cfg(unix)]
mod process {
    use super::*;
    use orbit_core::NodeId;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::process::{Child, Command, Stdio};

    const FLEET: &str = "ORBIT_TEXT_RECOVERY_TEST_FLEET";

    fn cells(
        name: &str,
        node: u16,
    ) -> Cells {
        Cells::new(Arc::new(
            Fleet::join_shm_as(name, 3, NodeId::new(node)).unwrap(),
        ))
        .unwrap()
    }

    struct Fixture {
        cells: Cells,
        name: String,
    }
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU32 = AtomicU32::new(0);
            const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
            let sequence =
                u16::try_from(NEXT.fetch_add(1, Ordering::Relaxed)).unwrap();
            let mut bits =
                (u64::from(std::process::id()) << 16) | u64::from(sequence);
            let name = (0..8)
                .map(|_| {
                    let c = ALPHABET[(bits & 63) as usize] as char;
                    bits >>= 6;
                    c
                })
                .collect::<String>();
            Self { cells: cells(&name, 0), name }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = self.cells.unlink();
        }
    }

    struct Peer(Child, BufReader<std::process::ChildStdout>);
    impl Peer {
        fn start(
            fixture: &Fixture,
            id: TextId,
            phase: Phase,
        ) -> Self {
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "text::tests::process::text_recovery_child",
                    "--nocapture",
                ])
                .env(FLEET, &fixture.name)
                .env("ORBIT_TEXT_RECOVERY_TEST_ID", id.to_string())
                .env("ORBIT_TEXT_RECOVERY_TEST_PHASE", format!("{phase:?}"))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut output = BufReader::new(child.stdout.take().unwrap());
            let mut line = String::new();
            loop {
                assert_ne!(
                    output.read_line(&mut line).unwrap(),
                    0,
                    "helper exited before fault point: {line}"
                );
                if line.contains("TEXT_FAULT_READY") {
                    break;
                }
                line.clear();
            }
            Self(child, output)
        }
        fn kill(&mut self) {
            self.0.kill().unwrap();
            assert!(!self.0.wait().unwrap().success());
        }
        fn finish(&mut self) {
            self.0.stdin.as_mut().unwrap().write_all(&[1]).unwrap();
            let mut output = String::new();
            self.1.read_to_string(&mut output).unwrap();
            assert!(self.0.wait().unwrap().success(), "{output}");
        }
    }
    impl Drop for Peer {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn text_recovery_child() {
        let Ok(name) = std::env::var(FLEET) else { return };
        let target = std::env::var("ORBIT_TEXT_RECOVERY_TEST_PHASE").unwrap();
        let mut fired = false;
        HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |phase| {
                if !fired && format!("{phase:?}") == target {
                    fired = true;
                    println!("TEXT_FAULT_READY");
                    std::io::stdout().flush().unwrap();
                    std::io::stdin().read_exact(&mut [0]).unwrap();
                }
            }))
        });
        let cells = cells(&name, 1);
        let text = cells
            .open_text(
                std::env::var("ORBIT_TEXT_RECOVERY_TEST_ID")
                    .unwrap()
                    .parse()
                    .unwrap(),
            )
            .unwrap();
        text.store("new committed text").unwrap();
    }

    #[test]
    fn dead_text_writers_preserve_the_last_committed_value_at_every_boundary() {
        for phase in [
            Phase::Claim,
            Phase::Stage,
            Phase::Prepared,
            Phase::Commit,
            Phase::Sequence,
            Phase::Woken,
        ] {
            let fixture = Fixture::new();
            let text = fixture.cells.allocate_text("last valid text").unwrap();
            // Exercise the sequence-before-native-wake gap even without a
            // parked reader. Real waiter behavior is checked separately.
            text.slot().unwrap().wake.waiters.store(1, Ordering::SeqCst);
            let mut child = Peer::start(&fixture, text.id(), phase);
            assert!(matches!(text.store("not allowed"), Err(Error::Contended)));
            assert_eq!(
                fixture.cells.recover_text_writers().unwrap(),
                0,
                "live owner must not be stolen"
            );
            child.kill();
            let expected = if matches!(
                phase,
                Phase::Claim | Phase::Stage | Phase::Prepared
            ) {
                "last valid text"
            } else {
                "new committed text"
            };
            assert_eq!(text.load().unwrap(), expected, "{phase:?}");
            let version = text.version().unwrap();
            assert_eq!(fixture.cells.recover_text_writers().unwrap(), 1);
            assert_eq!(
                text.version().unwrap(),
                version,
                "recovery is not a value write"
            );
            assert_eq!(fixture.cells.recover_text_writers().unwrap(), 0);
            text.slot().unwrap().wake.waiters.store(0, Ordering::SeqCst);
            text.append("!").unwrap();
            assert_eq!(text.load().unwrap(), format!("{expected}!"));
        }
    }

    #[test]
    fn owner_exit_recovery_completes_a_committed_text_waiter() {
        for phase in [Phase::Commit, Phase::Sequence] {
            for late_park in [false, true] {
                let fixture = Fixture::new();
                let text = fixture.cells.allocate_text("old").unwrap();
                let version = text.version().unwrap();
                let reader = text.clone();
                let (ready_tx, ready_rx) = mpsc::channel();
                let (resume_tx, resume_rx) = mpsc::channel();
                let thread = std::thread::spawn(move || {
                    crate::wake::BEFORE_PARK.with(|hook| {
                        *hook.borrow_mut() = Some(Box::new(move || {
                            ready_tx.send(()).unwrap();
                            if late_park {
                                resume_rx.recv().unwrap();
                            }
                        }))
                    });
                    reader.wait_changed(version)
                });
                ready_rx.recv().unwrap();
                let mut child = Peer::start(&fixture, text.id(), phase);
                child.kill();
                assert_eq!(fixture.cells.recover_text_writers().unwrap(), 1);
                if late_park {
                    resume_tx.send(()).unwrap();
                }
                assert_eq!(
                    thread.join().unwrap().unwrap(),
                    text.version().unwrap()
                );
                assert_eq!(text.load().unwrap(), "new committed text");
            }
        }
    }

    #[test]
    fn a_helper_crash_and_owner_row_reuse_cannot_orphan_text_ownership() {
        for recovery_phase in [Phase::Sequence, Phase::RecoveredWake] {
            let fixture = Fixture::new();
            let text = fixture.cells.allocate_text("old").unwrap();
            let unrelated = fixture.cells.allocate_text("unrelated").unwrap();
            let mut writer = Peer::start(&fixture, text.id(), Phase::Commit);
            writer.kill();
            // Reopening takes the old row hold and recovers before changing its
            // incarnation. Kill recovery before native wake or before clearing.
            let mut helper = Peer::start(&fixture, text.id(), recovery_phase);
            helper.kill();
            assert_eq!(fixture.cells.recover_text_writers().unwrap(), 1);
            let mut next = Peer::start(&fixture, text.id(), Phase::Claim);
            assert_eq!(fixture.cells.recover_text_writers().unwrap(), 0);
            assert!(matches!(text.store("late"), Err(Error::Contended)));
            next.finish();
            assert_eq!(text.load().unwrap(), "new committed text");
            text.store("after reuse").unwrap();
            assert_eq!(unrelated.load().unwrap(), "unrelated");
        }
    }

    #[test]
    fn stale_owner_tokens_and_exhausted_incarnations_cannot_clear_a_new_writer()
    {
        let fixture = Fixture::new();
        let text = fixture.cells.allocate_text("old").unwrap();
        let mut old = Peer::start(&fixture, text.id(), Phase::Claim);
        let stale = text.slot().unwrap().writer.load(Ordering::Acquire);
        old.kill();
        fixture.cells.recover_text_writers().unwrap();
        let mut fresh = Peer::start(&fixture, text.id(), Phase::Claim);
        let token = text.slot().unwrap().writer.load(Ordering::Acquire);
        assert_ne!(token, stale);
        // Even a delayed caller with a stale snapshot cannot clear this token.
        assert!(!text.slot().unwrap().recover(stale));
        assert_eq!(text.slot().unwrap().writer.load(Ordering::Acquire), token);
        fresh.finish();
        let TextBackend::Shm(table) = &text.table else { unreachable!() };
        let _lock = table.shared.region.lock_exclusive().unwrap();
        let owner = 2;
        let name = ring_segment_name(&fixture.name, CELL_TEXT_KIND);
        let _hold =
            orbit_core::shm::try_hold_lane(&name, owner).unwrap().unwrap();
        // This test owns the complete isolated namespace; no process uses row 2.
        owners::exhaust(&table.shared.region, owner);
        assert!(matches!(
            owners::claim(&table.shared.region, owner),
            Err(Error::OwnerExhausted)
        ));
    }
}
