use super::*;
use crate::{Incarnation, StreamSpec, Streams};
use orbit_core::Fleet;
use std::sync::atomic::AtomicUsize;
use std::task::Wake;

#[derive(Default)]
struct Count(AtomicUsize);
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn streams() -> Streams {
    Streams::with_spec(
        Arc::new(Fleet::join("creator-capacity", 2).unwrap()),
        Incarnation::new(1),
        StreamSpec::new(crate::STREAM_KIND, 1, 64),
    )
    .unwrap()
}

#[test]
fn released_peer_wakes_every_observer_after_its_last_half_not_on_bytes_or_fin()
{
    let streams = streams();
    let (mut origin, ticket) = streams.create().unwrap();
    let remote = streams.open(ticket).unwrap();
    let (read, mut write) = remote.split();
    let count = Arc::new(Count::default());
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut a = Box::pin(streams.capacity_changed());
    let mut b = Box::pin(streams.capacity_changed());
    assert!(a.as_mut().poll(&mut cx).is_pending());
    assert!(b.as_mut().poll(&mut cx).is_pending());
    let mut cancelled = Box::pin(streams.capacity_changed());
    assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    drop(cancelled);
    origin.try_write(b"request").unwrap();
    origin.finish().unwrap();
    write.try_write(b"reply").unwrap();
    write.finish().unwrap();
    drop(read);
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    drop(write);
    assert_eq!(count.0.load(Ordering::SeqCst), 2);
    assert!(matches!(a.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
    assert!(matches!(b.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
    origin.rearm(&streams).unwrap();
}

#[test]
fn release_before_first_poll_is_observed_and_cancel_disarms_the_lane() {
    let streams = streams();
    let (origin, ticket) = streams.create().unwrap();
    let remote = streams.open(ticket).unwrap();
    let mut changed = Box::pin(streams.capacity_changed());
    let mut cx = Context::from_waker(Waker::noop());
    drop(remote);
    assert!(matches!(changed.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(
        streams.table.own_doorbell().capacity_waiting.load(Ordering::SeqCst),
        0
    );
    drop(changed);
    assert_eq!(
        streams.table.own_doorbell().capacity_waiting.load(Ordering::SeqCst),
        0
    );
    let before =
        streams.table.own_doorbell().capacity_changes.load(Ordering::SeqCst);
    let cancelled = streams.capacity_changed();
    drop(cancelled);
    drop(origin);
    assert_eq!(
        streams.table.own_doorbell().capacity_changes.load(Ordering::SeqCst),
        before
    );
    streams.create().unwrap();
}

#[test]
fn creator_ending_and_confirmed_peer_death_signal_capacity() {
    let streams = streams();
    let (origin, _) = streams.create().unwrap();
    let mut changed = Box::pin(streams.capacity_changed());
    drop(origin);
    assert!(matches!(
        changed.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    let (origin, ticket) = streams.create().unwrap();
    let remote = streams.open(ticket).unwrap();
    let mut changed = Box::pin(streams.capacity_changed());
    streams.node_dead(orbit_core::NodeId::ZERO, Incarnation::new(1));
    assert!(matches!(
        changed.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    drop((origin, remote));
}

#[test]
fn capacity_words_preserve_existing_doorbell_layout() {
    assert_eq!(std::mem::size_of::<Doorbell>(), 64);
    assert_eq!(std::mem::offset_of!(Doorbell, incarnation), 8);
    assert_eq!(std::mem::offset_of!(Doorbell, capacity_waiting), 16);
    assert_eq!(std::mem::offset_of!(Doorbell, capacity_changes), 20);
}

#[test]
fn readiness_probes_do_not_claim_or_rearm_a_slot() {
    let streams = streams();
    assert!(streams.can_create());
    let (mut origin, ticket) = streams.create().unwrap();
    assert!(!streams.can_create());
    assert!(matches!(
        streams.can_rearm(ticket.id),
        Err(crate::Error::Stale(_))
    ));
    let remote = streams.open(ticket).unwrap();
    assert!(!streams.can_rearm(ticket.id).unwrap());
    assert!(matches!(streams.create(), Err(crate::Error::Full { .. })));
    drop(remote);
    assert!(streams.can_rearm(ticket.id).unwrap());
    assert!(
        streams.can_rearm(ticket.id).unwrap(),
        "probing leaves the released claim alone"
    );
    origin.rearm(&streams).unwrap();
    assert!(matches!(
        streams.can_rearm(ticket.id),
        Err(crate::Error::Stale(_))
    ));
    let remote = streams.open(ticket).unwrap();
    assert!(!streams.can_rearm(ticket.id).unwrap());
    drop((origin, remote));
    assert!(streams.can_create());
    assert!(matches!(
        streams.can_rearm(ticket.id),
        Err(crate::Error::Stale(_))
    ));
}

#[test]
fn readiness_hint_is_revalidated_after_acquisition_or_peer_death() {
    let streams = streams();
    assert!(streams.can_create());
    let (mut origin, ticket) = streams.create().unwrap();
    assert!(matches!(streams.create(), Err(crate::Error::Full { .. })));
    let remote = streams.open(ticket).unwrap();
    drop(remote);
    assert!(streams.can_rearm(ticket.id).unwrap());
    streams.node_dead(orbit_core::NodeId::ZERO, Incarnation::new(1));
    assert!(matches!(origin.rearm(&streams), Err(crate::Error::Stale(_))));
    assert!(matches!(
        streams.can_rearm(ticket.id),
        Err(crate::Error::Stale(_))
    ));
}

#[test]
fn epoch_reset_invalidates_a_probe_and_wakes_its_observer() {
    let streams = streams();
    let (mut origin, ticket) = streams.create().unwrap();
    let remote = streams.open(ticket).unwrap();
    drop(remote);
    assert!(streams.can_rearm(ticket.id).unwrap());
    let mut changed = Box::pin(streams.capacity_changed());
    streams.reset_all();
    assert!(streams.can_create());
    assert!(matches!(
        streams.can_rearm(ticket.id),
        Err(crate::Error::Stale(_))
    ));
    assert!(matches!(origin.rearm(&streams), Err(crate::Error::Stale(_))));
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
}
