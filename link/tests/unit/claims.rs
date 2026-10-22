use std::os::unix::process::ExitStatusExt;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{InboxGeometry, LinkSegment, LinkSpec};
use orbit_stream::StreamSpec;

const SPEC: LinkSpec = LinkSpec::new(
    2,
    194,
    InboxGeometry::new(4, 64),
    StreamSpec::new(195, 4, 256),
);

fn name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "lc{:x}{:x}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

struct Scratch(LinkSegment);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = self.0.unlink();
    }
}

#[test]
fn producer_crash_child() {
    let Ok(name) = std::env::var("ORBIT_LINK_CRASH_FLEET") else {
        return;
    };
    // Intentionally no consumer lane and no directory watcher registration:
    // a publisher-only mapping must have a recoverable producer identity too.
    let link = LinkSegment::open(&name, SPEC).unwrap();
    link.inbox().write(0, b"committed before wake").unwrap();
    panic!("the requested crash checkpoint was not reached");
}

#[test]
fn recovery_crash_child() {
    let Ok(name) = std::env::var("ORBIT_LINK_CRASH_FLEET") else {
        return;
    };
    let _link = LinkSegment::open(&name, SPEC).unwrap();
    panic!("recovery checkpoint was not reached");
}

#[test]
fn receiver_crash_child() {
    let Ok(name) = std::env::var("ORBIT_LINK_CRASH_FLEET") else {
        return;
    };
    let link = LinkSegment::open(&name, SPEC).unwrap();
    let _ = link.inbox().read(0, &mut Vec::new()).unwrap();
    panic!("reader checkpoint was not reached");
}

fn kill_at(
    name: &str,
    test: &str,
    phase: &str,
) {
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env("ORBIT_LINK_CRASH_FLEET", name)
        .env("ORBIT_LINK_CRASH_PHASE", phase)
        .output()
        .unwrap();
    assert_eq!(
        child.status.signal(),
        Some(libc::SIGKILL),
        "{phase}: {}{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
}

#[test]
fn recovery_actor_can_die_without_losing_cancellation_or_wake() {
    for phase in ["recovery-cancelled", "recovery-admitted", "recovery-woken"] {
        let name = name();
        let scratch = Scratch(LinkSegment::open(&name, SPEC).unwrap());
        let inbox = scratch.0.inbox();
        inbox.claim_lane(0, "example", "worker", 1).unwrap();
        kill_at(&name, "inbox::claims::tests::producer_crash_child", "claimed");
        kill_at(&name, "inbox::claims::tests::recovery_crash_child", phase);
        scratch.0.recover_producers(None).unwrap();
        assert!(!inbox.read(0, &mut Vec::new()).unwrap());
        assert_eq!(inbox.depth(0).unwrap(), 0);
        inbox.write(0, b"after recovery crash").unwrap();
        assert!(inbox.read(0, &mut Vec::new()).unwrap());
    }
}

#[test]
fn interrupted_consumer_reuse_is_finished_under_handoff() {
    for phase in ["reader-turn", "reader-free", "reader-head"] {
        let name = name();
        let scratch = Scratch(LinkSegment::open(&name, SPEC).unwrap());
        let inbox = scratch.0.inbox();
        inbox.claim_lane(0, "example", "worker", 1).unwrap();
        inbox.write(0, b"old reader").unwrap();
        kill_at(&name, "inbox::claims::tests::receiver_crash_child", phase);
        // This test has waited for exact reader exit. In normal SHM use the
        // replacement additionally holds the consumer lane's kernel lock.
        inbox.claim_lane(0, "example", "worker", 2).unwrap();
        for _ in 0..12 {
            inbox.write(0, b"replacement").unwrap();
            assert!(inbox.read(0, &mut Vec::new()).unwrap());
        }
    }
}

#[test]
fn killed_producer_intent_claim_and_commit_are_recovered() {
    for phase in [
        "intent-odd",
        "intent-published",
        "before-claim",
        "claimed",
        "admitted",
        "payload",
        "committed",
    ] {
        let name = name();
        let scratch = Scratch(LinkSegment::open(&name, SPEC).unwrap());
        let inbox = scratch.0.inbox();
        inbox.claim_lane(0, "example", "worker", 1).unwrap();
        let before = inbox.wait_word(0).unwrap().1;
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "inbox::claims::tests::producer_crash_child",
                "--nocapture",
            ])
            .env("ORBIT_LINK_CRASH_FLEET", &name)
            .env("ORBIT_LINK_CRASH_PHASE", phase)
            .output()
            .unwrap();
        assert_eq!(
            child.status.signal(),
            Some(libc::SIGKILL),
            "{phase}: {}",
            String::from_utf8_lossy(&child.stdout)
        );
        scratch.0.recover_producers(None).unwrap();
        scratch.0.recover_producers(None).unwrap(); // Repeat recovery is harmless.
        let mut out = Vec::new();
        let delivered = inbox.read(0, &mut out).unwrap();
        assert_eq!(delivered, phase == "committed", "{phase}");
        if delivered {
            assert_eq!(out, b"committed before wake");
        }
        if matches!(phase, "claimed" | "admitted" | "payload" | "committed") {
            assert_ne!(
                inbox.wait_word(0).unwrap().1,
                before,
                "recovery must repeat the missed wake"
            );
        }
        assert_eq!(inbox.depth(0).unwrap(), 0, "{phase}");
        // Reuse the dead registration, including an interrupted odd version.
        let peer = LinkSegment::open(&name, SPEC).unwrap();
        for _ in 0..12 {
            peer.inbox().write(0, b"next turn").unwrap();
            assert!(inbox.read(0, &mut out).unwrap());
            assert_eq!(out, b"next turn");
        }
    }
}

#[test]
fn live_producer_cannot_be_cancelled_and_consumer_restart_keeps_positions() {
    let scratch = Scratch(LinkSegment::open(&name(), SPEC).unwrap());
    let inbox = scratch.0.inbox();
    inbox.claim_lane(0, "example", "worker", 1).unwrap();
    let reservation = inbox.reserve_frame(0).unwrap();
    scratch.0.recover_producers(None).unwrap();
    let slot = inbox.slot_header(0, reservation.position);
    assert_eq!(
        slot.claim.load(Ordering::Acquire),
        reservation.claim(super::RESERVED)
    );
    // Simulate receiver handoff while a live producer still owns old work.
    inbox.claim_lane(0, "example", "worker", 1).unwrap();
    drop(reservation);
    let mut out = Vec::new();
    assert!(!inbox.read(0, &mut out).unwrap());
    let next = inbox.write(0, b"new generation").unwrap();
    assert_eq!(next, 1);
    assert!(inbox.read(0, &mut out).unwrap());
    inbox.claim_lane(0, "example", "worker", 1).unwrap();
    assert_eq!(inbox.write(0, b"another generation").unwrap(), 2);
}

#[test]
fn old_cancelled_claim_with_new_turn_cannot_admit_again() {
    let scratch = Scratch(LinkSegment::open(&name(), SPEC).unwrap());
    let inbox = scratch.0.inbox();
    inbox.claim_lane(0, "example", "worker", 1).unwrap();
    let reservation = inbox.reserve_frame(0).unwrap();
    let position = reservation.position;
    drop(reservation);
    let slot = inbox.slot_header(0, position);
    // Reader pause between next-turn publication and FREE publication.
    slot.seq.store(position + SPEC.inbox.capacity as u64, Ordering::Release);
    let before = inbox.lane_header(0).reserve.load(Ordering::Acquire);
    inbox.help_claim(0, position + SPEC.inbox.capacity as u64).unwrap();
    assert_eq!(inbox.lane_header(0).reserve.load(Ordering::Acquire), before);
}

#[test]
fn delayed_helper_snapshot_cannot_admit_a_reused_record_or_slot() {
    let scratch = Scratch(LinkSegment::open(&name(), SPEC).unwrap());
    let inbox = scratch.0.inbox();
    inbox.claim_lane(0, "example", "worker", 1).unwrap();
    let old = inbox.reserve_frame(0).unwrap();
    let old_claim = old.claim(super::RESERVED);
    let old_intent = inbox.intent_for_claim(old_claim).unwrap();
    drop(old);
    assert!(!inbox.read(0, &mut Vec::new()).unwrap());
    for _ in 1..SPEC.inbox.capacity {
        inbox.write(0, b"turn").unwrap();
        assert!(inbox.read(0, &mut Vec::new()).unwrap());
    }
    let next = inbox.reserve_frame(0).unwrap();
    assert_eq!(next.position, SPEC.inbox.capacity as u64);
    assert_eq!(
        next.claim(super::RESERVED),
        old_claim,
        "same record index, new turn"
    );
    let cursor = inbox.lane_header(0).reserve.load(Ordering::Acquire);
    inbox.help_snapshot(0, 0, old_claim, old_intent).unwrap();
    inbox.help_snapshot(0, next.position, old_claim, old_intent).unwrap();
    assert_eq!(inbox.lane_header(0).reserve.load(Ordering::Acquire), cursor);
    assert_eq!(
        inbox.slot_header(0, next.position).claim.load(Ordering::Acquire),
        old_claim
    );
}

#[test]
fn a_contending_writer_helps_then_uses_the_next_free_turn() {
    let scratch = Scratch(LinkSegment::open(&name(), SPEC).unwrap());
    let inbox = scratch.0.inbox();
    inbox.claim_lane(0, "example", "worker", 1).unwrap();
    let first = inbox.reserve_frame(0).unwrap();
    // The valid intermediate state of a producer paused after claiming but
    // before admitting its turn. No other participant has observed this fixture.
    inbox.lane_header(0).reserve.store(first.position, Ordering::Release);

    let second = inbox.reserve_frame(0).unwrap();
    assert_eq!(second.position, first.position + 1);
    assert_eq!(inbox.depth(0).unwrap(), 2);
    assert_eq!(
        inbox.slot_header(0, first.position).claim.load(Ordering::Acquire),
        first.claim(super::RESERVED),
        "helping never publishes or cancels a live producer's bytes"
    );
    drop((first, second));
    assert!(!inbox.read(0, &mut Vec::new()).unwrap());
    assert_eq!(inbox.depth(0).unwrap(), 0);
}

#[test]
fn a_live_claim_without_an_admissible_snapshot_refuses_without_waiting() {
    let scratch = Scratch(LinkSegment::open(&name(), SPEC).unwrap());
    let inbox = scratch.0.inbox();
    inbox.claim_lane(0, "example", "worker", 1).unwrap();
    let first = inbox.reserve_frame(0).unwrap();
    inbox.lane_header(0).reserve.store(first.position, Ordering::Release);
    let index = inbox.record_index(first.claim(super::RESERVED)).unwrap();
    let record = inbox.record(index);
    let even = record.seq.load(Ordering::Acquire);
    record.seq.store(even | 1, Ordering::Release);
    assert!(matches!(inbox.reserve_frame(0), Err(crate::Error::InboxFull)));
    assert_eq!(inbox.lane_header(0).reserve.load(Ordering::Acquire), 0);
    record.seq.store(even, Ordering::Release);
    inbox.help_claim(0, 0).unwrap();
    drop(first);
    assert!(!inbox.read(0, &mut Vec::new()).unwrap());
}
