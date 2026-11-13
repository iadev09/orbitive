use super::*;
use crate::{InboxGeometry, LinkSpec, Received};
use orbit_stream::StreamSpec;
use std::future::Future;
use std::io::{self, Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::task::{Context, Waker};
use std::time::Duration;

const SPEC: LinkSpec = LinkSpec::new(
    4,
    198,
    InboxGeometry::new(4, 64),
    StreamSpec::new(199, 2, 256),
);
const KIND: u8 = 200;
const LOST_WAKE: Duration = Duration::from_secs(10);

struct Scratch(Arc<ExchangeControls>);

impl Scratch {
    fn new() -> (String, Self) {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "xc{:x}{:x}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let segment = Arc::new(LinkSegment::open(&name, SPEC).unwrap());
        let controls = ExchangeControls::open(segment, KIND, 1).unwrap();
        (name, Self(Arc::new(controls)))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        self.0.unlink().unwrap();
        self.0.segment.unlink().unwrap();
    }
}

#[tokio::test]
async fn exchange_control_signals_cancel_and_failure_with_a_full_reply_lane() {
    let (name, scratch) = Scratch::new();
    let controls = &scratch.0;
    let caller = controls.segment.join("edge", "edge", 1).unwrap();
    let runner = controls.segment.join("runner", "worker", 1).unwrap();
    let remote = ExchangeControls::open(
        Arc::new(LinkSegment::open(&name, SPEC).unwrap()),
        KIND,
        1,
    )
    .unwrap();
    let inbox = controls.segment.inbox();
    for _ in 0..SPEC.inbox.capacity {
        remote.segment.inbox().write(caller.lane, b"unrelated").unwrap();
    }

    let mut edge_rx = controls.segment.receiver(caller.lane).unwrap();
    let mut runner_rx = remote.segment.receiver(runner.lane).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let lease = controls
        .reserve(&caller, runner.lane, runner.epoch())
        .unwrap()
        .unwrap();
    let old = lease.ticket();
    assert!(
        controls
            .reserve(&caller, runner.lane, runner.epoch())
            .unwrap()
            .is_none()
    );

    {
        let mut wait = std::pin::pin!(runner_rx.next_notification());
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        assert!(lease.cancel().unwrap());
        assert!(matches!(
            tokio::time::timeout(LOST_WAKE, wait).await.unwrap().unwrap(),
            Received::Capacity(_)
        ));
    }
    assert_eq!(remote.state(old).unwrap(), ExchangeControlState::Cancelled);
    assert!(!remote.fail(&runner, old).unwrap());
    drop(lease);

    let lease = controls
        .reserve(&caller, runner.lane, runner.epoch())
        .unwrap()
        .unwrap();
    assert!(matches!(remote.fail(&runner, old), Err(Error::StaleWork)));
    let ticket = lease.ticket();
    {
        let mut wait = std::pin::pin!(edge_rx.next_notification());
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        let oversized = [0; SPEC.inbox.payload_capacity + 1];
        assert!(matches!(
            remote.segment.inbox().write_reply_at_epoch(
                caller.lane,
                ticket.origin_epoch,
                &[&oversized]
            ),
            Err(Error::FrameTooLarge)
        ));
        assert!(remote.fail(&runner, ticket).unwrap());
        assert!(matches!(
            tokio::time::timeout(LOST_WAKE, wait).await.unwrap().unwrap(),
            Received::Capacity(_)
        ));
    }
    assert_eq!(controls.state(ticket).unwrap(), ExchangeControlState::Failed);
    assert!(!lease.cancel().unwrap());
    assert_eq!(inbox.depth(caller.lane).unwrap(), SPEC.inbox.capacity as u64);
    drop(lease);

    let lease = controls
        .reserve(&caller, runner.lane, runner.epoch())
        .unwrap()
        .unwrap();
    let ticket = lease.ticket();
    let mut wait = std::pin::pin!(runner_rx.next_notification());
    assert!(wait.as_mut().poll(&mut cx).is_pending());
    drop(lease);
    assert!(matches!(
        tokio::time::timeout(LOST_WAKE, wait).await.unwrap().unwrap(),
        Received::Capacity(_)
    ));
    assert!(matches!(remote.state(ticket), Err(Error::StaleWork)));
}

#[tokio::test]
async fn exchange_control_preserves_a_reply_committed_before_runner_death() {
    const CHILD: &str = "ORBIT_CONTROL_CRASH_FLEET";
    if let Ok(name) = std::env::var(CHILD) {
        let segment = LinkSegment::open(&name, SPEC).unwrap();
        let runner = segment.join("runner", "worker", 1).unwrap();
        assert_eq!(runner.lane, 1);
        io::stdout().lock().write_all(b"control-runner-ready\n").unwrap();
        io::stdin().lock().read_exact(&mut [0]).unwrap();
        let epoch = segment.inbox().lane_epoch(0).unwrap();
        segment.inbox().write_reply_at_epoch(0, epoch, &[b"reply"]).unwrap();
        panic!("did not reach the commit checkpoint");
    }

    let (name, scratch) = Scratch::new();
    let controls = &scratch.0;
    let caller = controls.segment.join("edge", "edge", 1).unwrap();
    assert_eq!(caller.lane, 0);
    // The child joins the next fresh lane. Stop after it commits and before
    // the ordinary publication wake; producer recovery supplies the wake.
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "control::tests::exchange_control_preserves_a_reply_committed_before_runner_death", "--nocapture"])
        .env(CHILD, &name)
        .env("ORBIT_LINK_CRASH_PHASE", "committed")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sent, ready) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        use std::io::BufRead;
        let mut output = io::BufReader::new(output);
        let mut line = String::new();
        while output.read_line(&mut line).unwrap_or(0) != 0 {
            if line == "control-runner-ready\n" {
                sent.send(()).unwrap();
                break;
            }
            line.clear();
        }
    });
    let ready = ready.recv_timeout(LOST_WAKE);
    if ready.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    reader.join().unwrap();
    ready.expect("runner did not join");

    let inbox = controls.segment.inbox();
    let runner_epoch = inbox.lane_epoch(1).unwrap();
    let lease = controls.reserve(&caller, 1, runner_epoch).unwrap().unwrap();
    let ticket = lease.ticket();
    assert_eq!(controls.state(ticket).unwrap(), ExchangeControlState::Pending);

    // A different, still-live publisher owns the first turn. An empty read
    // cannot tell the Edge that it has seen all replies from the dead Runner.
    let earlier = inbox.reserve_frame(caller.lane).unwrap();
    let mut reader = controls.segment.receiver(caller.lane).unwrap();

    let mut start = child.stdin.take().unwrap();
    start.write_all(&[1]).unwrap();
    drop(start);
    let (finished, status) = mpsc::sync_channel(1);
    let pid = child.id();
    let waiter =
        std::thread::spawn(move || finished.send(child.wait()).unwrap());
    let status = status.recv_timeout(LOST_WAKE);
    if status.is_err() {
        // SAFETY: the PID is the isolated test child being waited on.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    }
    waiter.join().unwrap();
    assert_eq!(
        status.expect("runner did not exit").unwrap().signal(),
        Some(libc::SIGKILL)
    );
    assert_eq!(controls.state(ticket).unwrap(), ExchangeControlState::Pending);
    assert_eq!(
        controls.segment.reclaim_process(pid, 1, |_| {}).unwrap().len(),
        1
    );
    assert!(matches!(controls.state(ticket), Err(Error::StaleWork)));
    let boundary = inbox.reserved_through(caller.lane).unwrap();
    let mut frame = Vec::new();
    assert!(!inbox.read(caller.lane, &mut frame).unwrap());
    assert!(inbox.read_position(caller.lane).unwrap() < boundary);
    inbox.write(caller.lane, b"later frame").unwrap();
    {
        let mut drain = std::pin::pin!(async {
            loop {
                match reader
                    .next_until_read(&mut frame, boundary)
                    .await
                    .unwrap()
                {
                    Some(Received::Frame) => break,
                    Some(Received::Capacity(_) | Received::Directory(_)) => {}
                    received => panic!(
                        "boundary reached before the reply: {received:?}"
                    ),
                }
            }
        });
        assert!(
            drain
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(earlier);
        tokio::time::timeout(LOST_WAKE, &mut drain).await.unwrap();
    }
    assert_eq!(frame, b"reply");
    assert!(
        reader.next_until_read(&mut frame, boundary).await.unwrap().is_none()
    );
    assert_eq!(inbox.read_position(caller.lane).unwrap(), boundary);
    assert_eq!(inbox.depth(caller.lane).unwrap(), 1);
    lease.complete();
}

#[test]
fn exchange_control_reuses_a_caller_lane_without_reusing_its_ticket() {
    let (_, scratch) = Scratch::new();
    let controls = &scratch.0;
    let segment = &controls.segment;
    let caller = segment.join("edge", "edge", 1).unwrap();
    let runner = segment.join("runner", "worker", 1).unwrap();
    let abandoned = controls
        .reserve(&caller, runner.lane, runner.epoch())
        .unwrap()
        .unwrap();
    let ticket = abandoned.ticket();
    // Model a caller that exits without releasing its control record.
    std::mem::forget(abandoned);
    segment.inbox().release_lane(caller.lane).unwrap();
    drop(caller);
    let replacement = segment.join("edge", "edge", 1).unwrap();
    assert_eq!(usize::from(ticket.origin), replacement.lane);
    let lease = controls
        .reserve(&replacement, runner.lane, runner.epoch())
        .unwrap()
        .unwrap();
    assert_ne!(ticket.generation, lease.ticket().generation);
    assert!(matches!(controls.fail(&runner, ticket), Err(Error::StaleWork)));
    assert_eq!(
        controls.state(lease.ticket()).unwrap(),
        ExchangeControlState::Pending
    );
    lease.complete();
}
