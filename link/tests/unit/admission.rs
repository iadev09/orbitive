use super::*;

#[repr(align(64))]
#[derive(Clone)]
struct Block([u8; 64]);

struct Memory {
    inbox: Inbox,
    _bytes: Vec<Block>,
}

fn memory(limit: Option<usize>) -> Memory {
    let geometry = crate::InboxGeometry::new(4, 64);
    let mut bytes =
        vec![Block([0; 64]); geometry.segment_bytes(4).div_ceil(64)];
    // SAFETY: storage is zeroed, aligned and retained past the Inbox borrow.
    let inbox =
        unsafe { Inbox::initialize(bytes[0].0.as_mut_ptr(), 4, geometry) }
            .unwrap();
    inbox.claim_lane(0, "caller-a", "caller", 1).unwrap();
    inbox
        .claim_lane_with_capacity(1, "application", "worker", 1, limit)
        .unwrap();
    inbox.claim_lane(2, "caller-b", "caller", 1).unwrap();
    inbox.claim_lane(3, "caller-c", "caller", 1).unwrap();
    Memory { inbox, _bytes: bytes }
}

#[test]
fn many_origins_share_one_receiver_bound() {
    use std::sync::{Arc, Barrier, atomic::AtomicUsize};
    let memory = memory(Some(2));
    let held = AtomicUsize::new(0);
    let barrier = Arc::new(Barrier::new(3));
    std::thread::scope(|scope| {
        for origin in [0, 2, 3] {
            let barrier = Arc::clone(&barrier);
            let inbox = &memory.inbox;
            let held = &held;
            scope.spawn(move || {
                let mut tickets = Vec::new();
                for _ in 0..4 {
                    match inbox.reserve_work(1, origin) {
                        Ok(Some(ticket)) => {
                            assert!(held.fetch_add(1, Ordering::SeqCst) < 2);
                            tickets.push(ticket);
                        }
                        Err(Error::CapacityFull) => break,
                        other => panic!("unexpected reservation: {other:?}"),
                    }
                }
                barrier.wait();
                assert_eq!(held.load(Ordering::SeqCst), 2);
                // Hold all reservations until every thread has observed the bound.
                barrier.wait();
                for ticket in tickets {
                    inbox.cancel_work(ticket);
                }
            });
        }
    });
    assert!(memory.inbox.reserve_work(1, 0).is_ok());
}

#[test]
fn acceptance_transfers_completion_to_the_receiver() {
    let memory = memory(Some(1));
    let inbox = &memory.inbox;
    let ticket = inbox.reserve_work(1, 0).unwrap().unwrap();
    inbox.accept_work(1, ticket).unwrap();
    assert!(matches!(inbox.accept_work(1, ticket), Err(Error::StaleWork)));
    inbox.cancel_work(ticket);
    assert!(matches!(inbox.reserve_work(1, 2), Err(Error::CapacityFull)));
    inbox.recover_work(0); // confirmed origin death cannot end receiver execution
    assert!(matches!(inbox.reserve_work(1, 2), Err(Error::CapacityFull)));
    inbox.complete_work(ticket);
    let next = inbox.reserve_work(1, 2).unwrap().unwrap();
    inbox.complete_work(ticket); // delayed completion cannot release a successor
    assert!(matches!(inbox.reserve_work(1, 3), Err(Error::CapacityFull)));
    inbox.cancel_work(next);
}

#[test]
fn raw_frames_remain_readable_while_work_capacity_is_full() {
    let memory = memory(Some(1));
    let inbox = &memory.inbox;
    let ticket = inbox.reserve_work(1, 0).unwrap().unwrap();
    assert!(matches!(inbox.reserve_work(1, 2), Err(Error::CapacityFull)));
    inbox.write(1, b"control").unwrap();
    let mut frame = Vec::new();
    assert!(inbox.read(1, &mut frame).unwrap());
    assert_eq!(frame, b"control");
    inbox.cancel_work(ticket);
}

#[test]
fn publication_preserves_exact_work_identity_without_changing_payload() {
    let memory = memory(Some(1));
    let inbox = &memory.inbox;
    let ticket = inbox.reserve_work(1, 0).unwrap().unwrap();
    inbox.write_work(ticket, &[b"request", b" body"]).unwrap();
    let mut frame = Vec::new();
    assert_eq!(inbox.read_frame(1, &mut frame).unwrap(), Some(Some(ticket)));
    assert_eq!(frame, b"request body");
    inbox.accept_work(1, ticket).unwrap();
    assert!(matches!(inbox.reserve_work(1, 2), Err(Error::CapacityFull)));
    inbox.complete_work(ticket);
    assert!(inbox.reserve_work(1, 2).is_ok());
}

#[test]
fn old_readers_fail_closed_instead_of_losing_admission() {
    let memory = memory(Some(1));
    let inbox = &memory.inbox;
    let ticket = inbox.reserve_work(1, 0).unwrap().unwrap();
    inbox.write_work(ticket, &[b"request"]).unwrap();
    assert!(inbox.read(1, &mut Vec::new()).is_err());
    assert!(matches!(inbox.accept_work(1, ticket), Err(Error::StaleWork)));
    assert!(inbox.reserve_work(1, 2).is_ok());
}

#[test]
fn capacity_changes_wake_only_registered_members_once() {
    let memory = memory(Some(2));
    let inbox = &memory.inbox;
    let first = inbox.reserve_work(1, 0).unwrap().unwrap();
    let second = inbox.reserve_work(1, 2).unwrap().unwrap();
    inbox.watch_capacity(1, 0).unwrap();
    inbox.watch_capacity(1, 0).unwrap(); // two waits share the same interest
    inbox.watch_capacity(1, 3).unwrap();
    let before = inbox.capacity_generation(0).unwrap();
    let other = inbox.capacity_generation(3).unwrap();
    inbox.cancel_work(first);
    assert_eq!(inbox.capacity_generation(0).unwrap(), before + 1);
    assert_eq!(inbox.capacity_generation(3).unwrap(), other + 1);
    assert_eq!(inbox.capacity_generation(2).unwrap(), 0);
    inbox.cancel_work(second);
    assert_eq!(inbox.capacity_generation(0).unwrap(), before + 1);
}

#[test]
fn registration_recheck_covers_capacity_return_before_and_after_registration() {
    let memory = memory(Some(1));
    let inbox = &memory.inbox;
    let first = inbox.reserve_work(1, 0).unwrap().unwrap();
    assert!(matches!(inbox.reserve_work(1, 2), Err(Error::CapacityFull)));
    inbox.cancel_work(first); // return before registration: the recheck sees room
    inbox.watch_capacity(1, 2).unwrap();
    let second = inbox.reserve_work(1, 2).unwrap().unwrap();
    let seen = inbox.capacity_generation(3).unwrap();
    inbox.watch_capacity(1, 3).unwrap();
    assert!(matches!(inbox.reserve_work(1, 3), Err(Error::CapacityFull)));
    inbox.cancel_work(second); // return after registration: the waiter is signalled
    assert_ne!(inbox.capacity_generation(3).unwrap(), seen);
    assert!(inbox.reserve_work(1, 3).is_ok());
}

#[test]
fn receiver_replacement_fences_old_acceptance_and_completion() {
    let memory = memory(Some(1));
    let inbox = &memory.inbox;
    let old = inbox.reserve_work(1, 0).unwrap().unwrap();
    inbox.accept_work(1, old).unwrap();
    inbox.release_lane(1).unwrap();
    inbox
        .claim_lane_with_capacity(1, "application", "worker", 2, Some(1))
        .unwrap();
    let new = inbox.reserve_work(1, 2).unwrap().unwrap();
    assert_ne!(old.claim, new.claim);
    assert_ne!(old.epoch, new.epoch);
    assert!(matches!(inbox.accept_work(1, old), Err(Error::StaleWork)));
    inbox.complete_work(old);
    inbox.cancel_work(old);
    assert!(matches!(inbox.reserve_work(1, 3), Err(Error::CapacityFull)));
    inbox.cancel_work(new);
}

#[test]
fn origin_death_returns_only_unaccepted_reservations() {
    let memory = memory(Some(2));
    let inbox = &memory.inbox;
    let pending = inbox.reserve_work(1, 0).unwrap().unwrap();
    let active = inbox.reserve_work(1, 0).unwrap().unwrap();
    inbox.accept_work(1, active).unwrap();
    inbox.recover_work(0);
    assert!(matches!(inbox.accept_work(1, pending), Err(Error::StaleWork)));
    let free = inbox.reserve_work(1, 2).unwrap().unwrap();
    assert!(matches!(inbox.reserve_work(1, 3), Err(Error::CapacityFull)));
    inbox.complete_work(active);
    inbox.cancel_work(free);
}

#[test]
fn withdrawal_notifies_waiters_without_ending_accepted_work() {
    let memory = memory(Some(1));
    let inbox = &memory.inbox;
    let ticket = inbox.reserve_work(1, 0).unwrap().unwrap();
    inbox.accept_work(1, ticket).unwrap();
    let seen = inbox.capacity_generation(2).unwrap();
    inbox.watch_capacity(1, 2).unwrap();
    inbox.release_lane(1).unwrap();
    assert_ne!(inbox.capacity_generation(2).unwrap(), seen);
    assert!(matches!(inbox.reserve_work(1, 2), Err(Error::NoConsumer)));
    inbox.complete_work(ticket); // release is still owned while the receiver drains
}

#[test]
fn invalid_bounds_fail_before_a_lane_is_advertised() {
    let memory = memory(None);
    let inbox = &memory.inbox;
    inbox.release_lane(1).unwrap();
    for limit in [0, 5] {
        assert!(
            inbox
                .claim_lane_with_capacity(
                    1,
                    "application",
                    "worker",
                    2,
                    Some(limit)
                )
                .is_err()
        );
        assert_eq!(inbox.consumers(1).unwrap(), 0);
    }
    inbox
        .claim_lane_with_capacity(1, "application", "worker", 2, None)
        .unwrap();
    assert_eq!(inbox.reserve_work(1, 0).unwrap(), None);
}

#[test]
fn live_growth_notifies_registered_origins_once() {
    let memory = memory(Some(1));
    let inbox = &memory.inbox;
    let active = inbox.reserve_work(1, 0).unwrap().unwrap();
    inbox.accept_work(1, active).unwrap();
    inbox.watch_capacity(1, 2).unwrap();
    let seen = inbox.capacity_generation(2).unwrap();
    inbox.update_work_capacity(1, 3).unwrap();
    assert_eq!(inbox.capacity_generation(2).unwrap(), seen + 1);
    let second = inbox.reserve_work(1, 2).unwrap().unwrap();
    let third = inbox.reserve_work(1, 3).unwrap().unwrap();
    assert!(matches!(inbox.reserve_work(1, 0), Err(Error::CapacityFull)));
    inbox.watch_capacity(1, 2).unwrap();
    inbox.update_work_capacity(1, 3).unwrap();
    assert_eq!(inbox.capacity_generation(2).unwrap(), seen + 1);
    inbox.complete_work(active);
    inbox.cancel_work(second);
    inbox.cancel_work(third);
}

#[test]
fn shrink_drains_retiring_claims_without_cancelling_any_ticket() {
    let memory = memory(Some(4));
    let inbox = &memory.inbox;
    let tickets: Vec<_> =
        (0..4).map(|_| inbox.reserve_work(1, 0).unwrap().unwrap()).collect();
    for ticket in &tickets {
        inbox.accept_work(1, *ticket).unwrap();
    }
    inbox.update_work_capacity(1, 2).unwrap();
    assert_eq!(inbox.work_capacity(1).unwrap(), Some(2));
    // Free holes below the new boundary. Old work above it still belongs to
    // the receiver, so those holes cannot admit a replacement prematurely.
    for ticket in tickets.iter().filter(|ticket| ticket.slot < 2) {
        inbox.complete_work(*ticket);
    }
    assert!(matches!(inbox.reserve_work(1, 2), Err(Error::CapacityFull)));
    for ticket in tickets.iter().filter(|ticket| ticket.slot >= 2) {
        inbox.watch_capacity(1, 2).unwrap();
        let seen = inbox.capacity_generation(2).unwrap();
        inbox.complete_work(*ticket);
        assert_ne!(inbox.capacity_generation(2).unwrap(), seen);
    }
    let first = inbox.reserve_work(1, 2).unwrap().unwrap();
    let second = inbox.reserve_work(1, 3).unwrap().unwrap();
    assert!(first.slot < 2 && second.slot < 2);
    assert!(matches!(inbox.reserve_work(1, 0), Err(Error::CapacityFull)));
    // Stale completions cannot release the new claims.
    for ticket in tickets {
        inbox.complete_work(ticket);
    }
    assert!(matches!(inbox.reserve_work(1, 0), Err(Error::CapacityFull)));
    inbox.cancel_work(first);
    inbox.cancel_work(second);
}

#[test]
fn resize_validation_preserves_current_policy_and_untracked_work() {
    let bounded = memory(Some(2));
    for limit in [0, 5] {
        assert!(bounded.inbox.update_work_capacity(1, limit).is_err());
        assert_eq!(bounded.inbox.work_capacity(1).unwrap(), Some(2));
    }
    let unbounded = memory(None);
    assert!(unbounded.inbox.update_work_capacity(1, 1).is_err());
    assert_eq!(unbounded.inbox.reserve_work(1, 0).unwrap(), None);
}

#[test]
fn producer_using_an_old_policy_cannot_publish_after_shrink() {
    let memory = memory(Some(4));
    let inbox = &memory.inbox;
    let policy = inbox.admission_policy(1).unwrap();
    let epoch = inbox.lane_epoch(1).unwrap();
    let origin_epoch = inbox.lane_epoch(0).unwrap();
    inbox.update_work_capacity(1, 1).unwrap();
    assert!(matches!(
        inbox.reserve_with_policy(1, 0, epoch, origin_epoch, policy),
        Err(Error::CapacityFull)
    ));
    let current = inbox.reserve_work(1, 2).unwrap().unwrap();
    assert_eq!(current.slot, 0);
    assert!(matches!(inbox.reserve_work(1, 3), Err(Error::CapacityFull)));
    inbox.cancel_work(current);
}

#[test]
fn the_held_count_follows_every_slot_and_ignores_stale_releases() {
    let memory = memory(Some(2));
    let inbox = &memory.inbox;
    let held = || inbox.capacity_row(1).occupied.load(Ordering::SeqCst);

    let first = inbox.reserve_work(1, 0).unwrap().unwrap();
    let second = inbox.reserve_work(1, 2).unwrap().unwrap();
    assert_eq!(held(), 2);
    // Full: refused by the count alone.
    assert!(matches!(inbox.reserve_work(1, 3), Err(Error::CapacityFull)));

    inbox.cancel_work(first);
    inbox.cancel_work(first);
    assert_eq!(held(), 1, "a stale release must not lower it twice");

    inbox.accept_work(1, second).unwrap();
    inbox.complete_work(second);
    assert_eq!(held(), 0);
    inbox.complete_work(second);
    assert_eq!(held(), 0, "a stale completion must not raise it back");

    // An origin's death returns its reservations and their count.
    inbox.reserve_work(1, 3).unwrap().unwrap();
    inbox.reserve_work(1, 3).unwrap().unwrap();
    assert_eq!(held(), 2);
    inbox.recover_work(3);
    assert_eq!(held(), 0);
    inbox.reserve_work(1, 0).unwrap().unwrap();
    assert_eq!(held(), 1);
}

#[test]
fn a_window_runs_its_execution_and_queues_the_rest_until_withdrawn() {
    // Window 4: the receiver runs 2 and queues 2 of its own.
    let memory = memory(Some(4));
    let inbox = &memory.inbox;
    assert_eq!(inbox.work_execution(1).unwrap(), Some(4));
    inbox.set_work_execution(1, 2).unwrap();
    assert_eq!(inbox.work_execution(1).unwrap(), Some(2));
    assert!(inbox.set_work_execution(1, 5).is_err(), "beyond the window");
    assert!(inbox.set_work_execution(0, 1).is_err(), "unbounded lane");

    let tickets: Vec<_> =
        (0..4).map(|_| inbox.reserve_work(1, 0).unwrap().unwrap()).collect();
    assert_eq!(inbox.work_load(1).unwrap(), 4);
    assert!(matches!(inbox.reserve_work(1, 2), Err(Error::CapacityFull)));

    // Started work cannot be withdrawn; queued work can, once.
    inbox.accept_work(1, tickets[0]).unwrap();
    assert!(!inbox.withdraw_work(tickets[0]));
    assert!(inbox.check_work(tickets[3]).is_ok());
    assert!(inbox.withdraw_work(tickets[3]));
    assert!(inbox.check_work(tickets[3]).is_err());
    assert!(!inbox.withdraw_work(tickets[3]));
    assert_eq!(inbox.work_load(1).unwrap(), 3);
    // The receiver reaching a withdrawn entry finds it stale.
    assert!(matches!(inbox.accept_work(1, tickets[3]), Err(Error::StaleWork)));

    // A rejoin starts from a clean row.
    inbox.configure_capacity(1, Some(4)).unwrap();
    assert_eq!(inbox.work_load(1).unwrap(), 0);
    assert_eq!(inbox.work_execution(1).unwrap(), Some(4));
}
