use super::*;

use std::future::Future;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use crate::{InboxGeometry, LinkSpec};
use orbit_stream::StreamSpec;

/// How long a test waits for a wake before it calls the wake lost. A wake
/// that works arrives in microseconds; this bound only turns a hang into a
/// failure, and it is the limit set for any test on 2026-11-13.
const LOST_WAKE: std::time::Duration = std::time::Duration::from_secs(10);

struct Scratch(Arc<LinkSegment>);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!(
            "wake{:x}{:x}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let spec = LinkSpec::new(
            2,
            198,
            InboxGeometry::new(8, 64),
            StreamSpec::new(199, 2, 1024),
        );
        let segment = Arc::new(LinkSegment::open(&name, spec).unwrap());
        segment.inbox().claim_lane(0, "example", "worker", 1).unwrap();
        Self(segment)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        self.0.unlink().unwrap();
    }
}

fn peek(socket: &tokio::net::UnixDatagram) -> io::Result<usize> {
    let mut byte = 0u8;
    // SAFETY: socket owns the fd; byte is writable and the call cannot block.
    let count = unsafe {
        libc::recv(
            socket.as_raw_fd(),
            std::ptr::addr_of_mut!(byte).cast(),
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    if count < 0 { Err(io::Error::last_os_error()) } else { Ok(count as usize) }
}

#[tokio::test(flavor = "current_thread")]
async fn returned_capacity_resumes_the_existing_receiver_without_a_frame() {
    let scratch = Scratch::new();
    scratch
        .0
        .inbox()
        .claim_lane_with_capacity(1, "target", "worker", 1, Some(1))
        .unwrap();
    let mut receiver = scratch.0.receiver(0).unwrap();
    let ticket = scratch.0.inbox().reserve_work(1, 0).unwrap().unwrap();
    scratch.0.inbox().watch_capacity(1, 0).unwrap();
    let (armed, waiting) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut frame = Vec::new();
        let mut next = Box::pin(receiver.next(&mut frame));
        let mut armed = Some(armed);
        std::future::poll_fn(|cx| {
            let result = next.as_mut().poll(cx);
            if result.is_pending()
                && let Some(armed) = armed.take()
            {
                armed.send(()).unwrap();
            }
            result
        })
        .await
    });
    waiting.await.unwrap();
    scratch.0.inbox().cancel_work(ticket);
    assert!(matches!(
        task.await.unwrap().unwrap(),
        crate::Received::Capacity(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn plain_recv_cannot_discard_a_work_ticket() {
    let scratch = Scratch::new();
    scratch
        .0
        .inbox()
        .claim_lane_with_capacity(1, "target", "worker", 1, Some(1))
        .unwrap();
    let mut receiver = scratch.0.receiver(1).unwrap();
    let ticket = scratch.0.inbox().reserve_work(1, 0).unwrap().unwrap();
    scratch.0.inbox().write_work(ticket, &[b"work"]).unwrap();
    assert!(receiver.recv(&mut Vec::new()).await.is_err());
    assert!(scratch.0.inbox().accept_work(1, ticket).is_err());
    assert!(scratch.0.inbox().reserve_work(1, 0).is_ok());
}

use std::io;

/// Drive arm/cancel/commit/read without yielding to the IO reactor. The idle
/// transition must drain even when Tokio has not cached kernel readiness.
#[tokio::test(flavor = "current_thread")]
async fn cancelled_receives_do_not_accumulate_obsolete_wakes() {
    let scratch = Scratch::new();
    let mut receiver = scratch.0.receiver(0).unwrap();
    let mut frame = Vec::new();
    let mut context = Context::from_waker(Waker::noop());

    for turn in 0..128u8 {
        let mut waiting = Box::pin(receiver.recv(&mut frame));
        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        drop(waiting);
        assert_eq!(
            peek(&receiver.socket).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );

        for item in 0..4u8 {
            scratch.0.inbox().write(0, &[b'x', turn, item]).unwrap();
        }
        assert_eq!(peek(&receiver.socket).unwrap(), 1);
        for item in 0..4u8 {
            receiver.recv(&mut frame).await.unwrap();
            assert_eq!(frame, [b'x', turn, item]);
            // Ready SHM frames do not consume or inspect the socket.
            assert_eq!(peek(&receiver.socket).unwrap(), 1);
        }
    }
}

/// A commit between the idle drain and arm is covered by the generation
/// recheck, even though no descriptor notification was required.
#[tokio::test(flavor = "current_thread")]
async fn a_commit_while_disarmed_remains_visible_before_parking() {
    let scratch = Scratch::new();
    let mut receiver = scratch.0.receiver(0).unwrap();
    let inbox = scratch.0.inbox();
    let seen = inbox.receiver_generation(0).unwrap();
    inbox.disarm_receiver(0);
    crate::bell::drain(&receiver.socket).unwrap();
    inbox.write(0, b"during drain").unwrap();
    inbox.arm_receiver(0).unwrap();
    assert_ne!(inbox.receiver_generation(0).unwrap(), seen);
    inbox.disarm_receiver(0);
    let mut frame = Vec::new();
    receiver.recv(&mut frame).await.unwrap();
    assert_eq!(frame, b"during drain");
    assert_eq!(
        peek(&receiver.socket).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[tokio::test(flavor = "current_thread")]
async fn live_growth_resumes_a_parked_receiver_without_a_frame() {
    let scratch = Scratch::new();
    scratch
        .0
        .inbox()
        .claim_lane_with_capacity(1, "target", "worker", 1, Some(1))
        .unwrap();
    let mut receiver = scratch.0.receiver(0).unwrap();
    let ticket = scratch.0.inbox().reserve_work(1, 0).unwrap().unwrap();
    scratch.0.inbox().watch_capacity(1, 0).unwrap();
    let (armed, waiting) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut frame = Vec::new();
        let mut next = Box::pin(receiver.next(&mut frame));
        let mut armed = Some(armed);
        std::future::poll_fn(|cx| {
            let result = next.as_mut().poll(cx);
            if result.is_pending()
                && let Some(armed) = armed.take()
            {
                armed.send(()).unwrap();
            }
            result
        })
        .await
    });
    waiting.await.unwrap();
    scratch.0.inbox().update_work_capacity(1, 2).unwrap();
    assert!(matches!(
        task.await.unwrap().unwrap(),
        crate::Received::Capacity(_)
    ));
    scratch.0.inbox().cancel_work(ticket);
}

/// A woken receiver reads the one bell sent for its arm and drops Tokio's
/// note of it without asking the socket again. The next wait must still be
/// woken by the next bell: a lost wake hangs here.
#[tokio::test(flavor = "current_thread")]
async fn a_receiver_that_read_its_bell_exactly_is_woken_again() {
    let scratch = Scratch::new();
    let mut receiver = scratch.0.receiver(0).unwrap();
    let (parked, mut waiting) = tokio::sync::mpsc::unbounded_channel();
    let reader = tokio::spawn(async move {
        let mut frame = Vec::new();
        for turn in 0..64u8 {
            let mut next = Box::pin(receiver.recv(&mut frame));
            let mut told = false;
            std::future::poll_fn(|cx| {
                let result = next.as_mut().poll(cx);
                if result.is_pending() && !told {
                    told = true;
                    parked.send(()).unwrap();
                }
                result
            })
            .await
            .unwrap();
            drop(next);
            assert_eq!(frame, [b'y', turn]);
        }
        receiver
    });
    for turn in 0..64u8 {
        waiting.recv().await.unwrap();
        scratch.0.inbox().write(0, &[b'y', turn]).unwrap();
    }
    let receiver = reader.await.unwrap();
    // The last wake's bell is the only one still unread.
    assert_eq!(peek(&receiver.socket).unwrap(), 1);
}

/// A writer keeps a socket connected to the lane's bell. When the lane's
/// receiver is replaced, that connection leads to a bell nobody reads: the
/// ring must find the new one, or the new receiver sleeps through its frame.
#[tokio::test(flavor = "current_thread")]
async fn a_replaced_receiver_is_still_rung() {
    let scratch = Scratch::new();
    for turn in 0..3u8 {
        let mut receiver = scratch.0.receiver(0).unwrap();
        let (parked, waiting) = tokio::sync::oneshot::channel();
        let reader = tokio::spawn(async move {
            let mut frame = Vec::new();
            let mut next = Box::pin(receiver.recv(&mut frame));
            let mut parked = Some(parked);
            std::future::poll_fn(|cx| {
                let result = next.as_mut().poll(cx);
                if result.is_pending()
                    && let Some(parked) = parked.take()
                {
                    parked.send(()).unwrap();
                }
                result
            })
            .await
            .unwrap();
            drop(next);
            frame
        });
        waiting.await.unwrap();
        scratch.0.inbox().write(0, &[b'z', turn]).unwrap();
        // A lost wake fails here rather than holding the run.
        let frame =
            tokio::time::timeout(LOST_WAKE, reader).await.unwrap_or_else(
                |_| panic!("turn {turn}: the replaced receiver was not rung"),
            );
        assert_eq!(frame.unwrap(), [b'z', turn]);
    }
}
