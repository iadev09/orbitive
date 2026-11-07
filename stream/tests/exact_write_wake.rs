//! Deterministic capacity/waker checks, plus the shared-memory reactor path.
#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use orbit_core::Fleet;
use orbit_stream::{
    Error, Incarnation, ReadHalf, StreamSpec, Streams, WriteHalf,
};

#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn pair(occupied: usize) -> (WriteHalf, ReadHalf, Arc<Count>, Waker) {
    const CAPACITY: usize = 64;
    let streams = Streams::with_spec(
        Arc::new(Fleet::join("exact-write-wake", 2).unwrap()),
        Incarnation::new(1),
        StreamSpec::new(StreamSpec::DEFAULT.kind, 1, CAPACITY),
    )
    .unwrap();
    let (origin, ticket) = streams.create().unwrap();
    let remote = streams.open(ticket).unwrap();
    let (_incoming, writer) = origin.split();
    let (reader, _outgoing) = remote.split();
    assert_eq!(writer.try_write(&[7; CAPACITY][..occupied]).unwrap(), occupied);

    let count = Arc::new(Count::default());
    let waker = Waker::from(Arc::clone(&count));
    (writer, reader, count, waker)
}

fn check_exact_writer_wake(
    occupied: usize,
    consumed: usize,
    exact_read: bool,
) {
    const CAPACITY: usize = 64;
    const NEEDED: usize = 32;
    let (writer, reader, count, waker) = pair(occupied);
    let mut cx = Context::from_waker(&waker);
    assert!(writer.poll_writable_exact(NEEDED, &mut cx).is_pending());
    assert_eq!(count.0.load(Ordering::SeqCst), 0);

    let mut bytes = [0; CAPACITY];
    if exact_read {
        reader.try_read_exact(&mut bytes[..consumed]).unwrap();
    } else {
        assert_eq!(reader.try_read(&mut bytes[..consumed]).unwrap(), consumed);
    }
    assert_eq!(&bytes[..consumed], &[7; CAPACITY][..consumed]);

    // A manual poll proves that capacity is now available. It must also have
    // been announced: a real executor will not poll again without a wake.
    assert!(matches!(
        writer.poll_writable_exact(NEEDED, &mut cx),
        Poll::Ready(Ok(()))
    ));
    assert!(
        count.0.load(Ordering::SeqCst) > 0,
        "exact writer became ready without a wake: capacity={CAPACITY}, occupied={occupied}, consumed={consumed}, needed={NEEDED}, exact_read={exact_read}"
    );
}

#[test]
fn exact_writer_wakes_when_partial_ring_read_supplies_required_capacity() {
    check_exact_writer_wake(48, 16, false);
}

#[test]
fn exact_writer_wakes_when_partial_ring_exact_read_supplies_required_capacity()
{
    check_exact_writer_wake(48, 16, true);
}

#[test]
fn exact_writer_wakes_when_full_ring_read_supplies_required_capacity() {
    check_exact_writer_wake(64, 32, false);
}

#[test]
fn exact_writer_wakes_only_at_its_threshold_for_partial_and_full_rings() {
    for occupied in [48, 64] {
        let (writer, reader, count, waker) = pair(occupied);
        let mut cx = Context::from_waker(&waker);
        assert!(writer.poll_writable_exact(32, &mut cx).is_pending());
        // Neither a partial consume nor leaving a full ring supplies 32 bytes.
        assert_eq!(reader.try_read(&mut [0; 8]).unwrap(), 8);
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        let rest = occupied - 40;
        assert_eq!(reader.try_read(&mut [0; 24][..rest]).unwrap(), rest);
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        // More capacity without another wait is not another notification.
        assert_eq!(reader.try_read(&mut [0; 1]).unwrap(), 1);
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            writer.poll_writable_exact(32, &mut cx),
            Poll::Ready(Ok(()))
        ));
    }
}

#[test]
fn a_changed_exact_threshold_replaces_the_previous_wait() {
    let (writer, reader, count, waker) = pair(48);
    let mut cx = Context::from_waker(&waker);
    assert!(writer.poll_writable_exact(32, &mut cx).is_pending());
    assert!(writer.poll_writable_exact(48, &mut cx).is_pending());
    reader.try_read_exact(&mut [0; 16]).unwrap();
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    reader.try_read_exact(&mut [0; 16]).unwrap();
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
}

#[test]
fn normal_write_readiness_cancels_the_exact_threshold_without_extra_wakes() {
    let (writer, reader, count, waker) = pair(48);
    let mut cx = Context::from_waker(&waker);
    assert!(writer.poll_writable_exact(32, &mut cx).is_pending());
    assert!(matches!(writer.poll_writable(&mut cx), Poll::Ready(Ok(()))));
    reader.try_read_exact(&mut [0; 16]).unwrap();
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
}

#[test]
fn ordinary_write_attempt_cancels_a_previous_exact_wait() {
    let (writer, reader, count, waker) = pair(48);
    let mut cx = Context::from_waker(&waker);
    assert!(writer.poll_writable_exact(32, &mut cx).is_pending());
    assert_eq!(writer.try_write(&[9]).unwrap(), 1);
    reader.try_read_exact(&mut [0; 17]).unwrap();
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
}

#[test]
fn invalid_exact_request_clears_the_previous_threshold() {
    let (writer, reader, count, waker) = pair(48);
    let mut cx = Context::from_waker(&waker);
    assert!(writer.poll_writable_exact(32, &mut cx).is_pending());
    assert!(matches!(
        writer.poll_writable_exact(65, &mut cx),
        Poll::Ready(Err(Error::Malformed(_)))
    ));
    reader.try_read_exact(&mut [0; 16]).unwrap();
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
}

#[test]
fn ordinary_writer_keeps_its_full_ring_wake_rule() {
    for occupied in [48, 64] {
        let (writer, reader, count, waker) = pair(occupied);
        let mut cx = Context::from_waker(&waker);
        let pending = writer.poll_writable(&mut cx).is_pending();
        assert_eq!(pending, occupied == 64);
        reader.try_read_exact(&mut [0; 16]).unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), usize::from(pending));
    }
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn exact_writer_resumes_through_the_shared_memory_peer_bell() {
    use orbit_core::NodeId;
    use std::future::poll_fn;

    let name = format!("exactwake{:x}", std::process::id());
    let spec = StreamSpec::new(StreamSpec::DEFAULT.kind, 1, 64);
    let table = |node| {
        Streams::with_spec(
            Arc::new(Fleet::join_shm_as(&name, 2, NodeId::new(node)).unwrap()),
            Incarnation::new(1),
            spec,
        )
        .unwrap()
    };
    let owner = table(0);
    let peer = table(1);
    let (origin, ticket) = owner.create().unwrap();
    let remote = peer.open(ticket).unwrap();
    let (_incoming, writer) = origin.split();
    let (reader, _outgoing) = remote.split();
    assert_eq!(writer.try_write(&[7; 48]).unwrap(), 48);
    let (parked, ready) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(async move {
        let mut parked = Some(parked);
        poll_fn(|cx| {
            let result = writer.poll_writable_exact(32, cx);
            if result.is_pending()
                && let Some(parked) = parked.take()
            {
                parked.send(()).unwrap();
            }
            result
        })
        .await
    });
    ready.await.unwrap();
    reader.try_read_exact(&mut [0; 16]).unwrap();
    // Only an actual wake can resume the task: no manual repoll or timer.
    waiter.await.unwrap().unwrap();
    owner.unlink().unwrap();
}
