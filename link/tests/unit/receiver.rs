use super::*;

use std::future::Future;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use crate::{InboxGeometry, LinkSpec};
use orbit_stream::StreamSpec;

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
