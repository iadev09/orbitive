//! Where a request and its reply spend their time between two processes on one
//! machine, in nanoseconds, with no HTTP in the way.
//!
//! The same exchange over five paths, each process on a current-thread Tokio
//! runtime:
//!
//! - `uds`: a connected Unix socket. The address is the connection.
//! - `stream`: one Orbit stream slot, held like that connection. Transport only.
//! - `link`: what a named call with a body costs: a frame announcing the
//!   exchange, a slot reused for its bytes, and a reply head back through the
//!   caller's inbox.
//! - `inline`: a named call whose request and reply each fit a frame, sent as
//!   such a call is today: a slot is still taken for it and ended at once.
//! - `frame`: the same two frames and no slot.
//!
//! `stream` against `uds` compares the transports. `link` against `stream` is
//! what naming and dispatch add. `inline` against `frame` is what a slot costs
//! an exchange that puts nothing in it.
//!
//! Beside the time, a row says what the exchange cost in wakes: bells rung
//! and what one send took, bell reads that found nothing, times a runtime
//! thread went to wait, and voluntary context switches.
//!
//! The processes read one clock, so a stamp taken on one side can be compared
//! with one taken on the other. See `docs/benches/README.md`.
//!
//! Run:
//! `cargo bench -p orbit-link --bench transport --features tokio,probe`
//! `cargo bench -p orbit-link --bench transport --features tokio,probe -- --paths uds,inline,frame --sizes 64 --conns 1,8 --seconds 5`

#![cfg(unix)]

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orbit_link::{InboxGeometry, LinkBodies, LinkSegment, LinkSpec, Received};
use orbit_stream::{Endpoint, StreamSpec, Ticket};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;

const ROLE: &str = "ORBIT_TRANSPORT_ROLE";
const FLEET: &str = "ORBIT_TRANSPORT_FLEET";
const SOCKET: &str = "ORBIT_TRANSPORT_SOCKET";
const SIZE: &str = "ORBIT_TRANSPORT_SIZE";

const CLIENT_NAME: &str = "bench-client";
const SERVER_NAME: &str = "bench-server";
const CLIENT_INCARNATION: u64 = 1;
const SERVER_INCARNATION: u64 = 2;

const SPEC: LinkSpec = LinkSpec::new(
    2,
    196,
    InboxGeometry::new(128, 4 * 1024),
    StreamSpec::new(197, 128, 64 * 1024),
);

/// How long each run warms up before anything is counted. Part of the method,
/// stated in the document, and only ever a bench's setting.
const WARM_UP: Duration = Duration::from_secs(1);

/// Shared by every process on the machine, in nanoseconds. macOS's
/// `CLOCK_MONOTONIC` only moves in microseconds.
#[cfg(target_os = "macos")]
const CLOCK: libc::clockid_t = libc::CLOCK_UPTIME_RAW;
#[cfg(not(target_os = "macos"))]
const CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC;

fn now() -> u64 {
    let mut time = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `time` is a valid timespec for the duration of the call.
    unsafe { libc::clock_gettime(CLOCK, &mut time) };
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

/// The largest request the two frame-only paths carry: a frame holds the
/// bytes and the head that names the exchange.
const WHOLE_LIMIT: usize = 3 * 1024;

/// Times this process's runtime thread ran out of work and went to wait.
static PARKS: AtomicU64 = AtomicU64::new(0);

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .on_thread_park(|| {
            PARKS.fetch_add(1, Ordering::Relaxed);
        })
        .build()
        .expect("runtime")
}

/// What a process has spent so far: read before and after the counted time.
#[derive(Clone, Copy, Default)]
struct Spent {
    /// User and system time, in nanoseconds.
    cpu: u64,
    /// Bell datagrams sent.
    rings: u64,
    /// Nanoseconds inside those sends.
    ring_nanos: u64,
    /// Bell reads that found nothing.
    empty_reads: u64,
    /// Times the runtime thread went to wait for an event.
    parks: u64,
    /// Voluntary context switches. macOS does not count them.
    switches: u64,
}

impl Spent {
    fn now() -> Self {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        // SAFETY: `usage` is a valid rusage for the duration of the call.
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
        let of = |time: libc::timeval| {
            time.tv_sec as u64 * 1_000_000_000 + time.tv_usec as u64 * 1_000
        };
        let bells = orbit_link::bell_counts();
        Self {
            cpu: of(usage.ru_utime) + of(usage.ru_stime),
            rings: bells.rings,
            ring_nanos: bells.ring_nanos,
            empty_reads: bells.empty_reads,
            parks: PARKS.load(Ordering::Relaxed),
            switches: usage.ru_nvcsw as u64,
        }
    }

    /// Both processes' spending between two readings of each.
    fn between(
        near: (Self, Self),
        far: (Self, Self),
    ) -> Self {
        let of = |field: fn(&Self) -> u64| {
            (field(&near.1) - field(&near.0)) + (field(&far.1) - field(&far.0))
        };
        Self {
            cpu: of(|s| s.cpu),
            rings: of(|s| s.rings),
            ring_nanos: of(|s| s.ring_nanos),
            empty_reads: of(|s| s.empty_reads),
            parks: of(|s| s.parks),
            switches: of(|s| s.switches),
        }
    }

    fn line(self) -> String {
        format!(
            "{} {} {} {} {} {}",
            self.cpu,
            self.rings,
            self.ring_nanos,
            self.empty_reads,
            self.parks,
            self.switches
        )
    }

    fn parse(line: &str) -> Self {
        let mut fields =
            line.split_whitespace().map(|n| n.parse().expect("a number"));
        let mut next = || fields.next().expect("a count");
        Self {
            cpu: next(),
            rings: next(),
            ring_nanos: next(),
            empty_reads: next(),
            parks: next(),
            switches: next(),
        }
    }
}

/// One exchange: the caller's stamps and the three the other side reported.
#[derive(Clone, Copy)]
struct Sample {
    /// The caller began.
    start: u64,
    /// The caller finished sending the request.
    sent: u64,
    /// The other side saw the first of it.
    arrived: u64,
    /// The other side had all of it.
    read: u64,
    /// The other side began answering.
    answering: u64,
    /// The caller saw the first of the answer.
    answered: u64,
    /// The caller had all of it.
    done: u64,
}

/// Read exactly `buffer.len()` bytes, saying when the first of them came.
async fn read_stamped<S: AsyncRead + Unpin>(
    stream: &mut S,
    buffer: &mut [u8],
) -> std::io::Result<u64> {
    let mut first = stream.read(buffer).await?;
    let stamp = now();
    if first == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    while first < buffer.len() {
        let more = stream.read(&mut buffer[first..]).await?;
        if more == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        first += more;
    }
    Ok(stamp)
}

const STAMPS: usize = 24;
/// A reply head's first byte. A frame's first byte says what it is, and one
/// that began with a correlation would sooner or later read as an event.
const REPLY: u8 = b'R';

fn encode_stamps(
    arrived: u64,
    read: u64,
    answering: u64,
) -> [u8; STAMPS] {
    let mut out = [0; STAMPS];
    out[..8].copy_from_slice(&arrived.to_le_bytes());
    out[8..16].copy_from_slice(&read.to_le_bytes());
    out[16..24].copy_from_slice(&answering.to_le_bytes());
    out
}

fn decode_stamps(bytes: &[u8]) -> (u64, u64, u64) {
    let word = |at: usize| {
        u64::from_le_bytes(bytes[at..at + 8].try_into().expect("eight bytes"))
    };
    (word(0), word(8), word(16))
}

/// The far side of `uds` and `stream`: read a request, answer with the stamps
/// and the same number of bytes, on one held connection.
async fn echo<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    size: usize,
) {
    let mut request = vec![0; size];
    let mut reply = vec![0; size + STAMPS];
    loop {
        let Ok(arrived) = read_stamped(&mut stream, &mut request).await else {
            return;
        };
        let read = now();
        let answering = now();
        reply[..STAMPS]
            .copy_from_slice(&encode_stamps(arrived, read, answering));
        if stream.write_all(&reply).await.is_err() {
            return;
        }
    }
}

/// The near side of `uds` and `stream`, until `end`.
async fn call<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    size: usize,
    from: u64,
    end: u64,
) -> Vec<Sample> {
    let request = vec![7; size];
    let mut reply = vec![0; size + STAMPS];
    let mut samples = Vec::new();
    loop {
        let start = now();
        if start >= end {
            return samples;
        }
        stream.write_all(&request).await.expect("request");
        let sent = now();
        let answered =
            read_stamped(&mut stream, &mut reply).await.expect("reply");
        let done = now();
        if start >= from {
            let (arrived, read, answering) = decode_stamps(&reply);
            samples.push(Sample {
                start,
                sent,
                arrived,
                read,
                answering,
                answered,
                done,
            });
        }
    }
}

/// The far side of `link`: every frame is one exchange on its own task.
async fn serve_link(
    segment: Arc<LinkSegment>,
    bodies: Arc<LinkBodies>,
    lane: usize,
    size: usize,
) {
    let mut receiver = segment.receiver(lane).expect("receiver");
    let mut frame = Vec::new();
    loop {
        match receiver.next(&mut frame).await.expect("next frame") {
            Received::Frame => {}
            _ => continue,
        }
        let arrived = now();
        let (correlation, origin, ticket) = decode_call(&frame);
        let endpoint = bodies.accept(ticket).expect("accept");
        let segment = Arc::clone(&segment);
        tokio::spawn(async move {
            answer_link(endpoint, &segment, correlation, origin, size, arrived)
                .await;
        });
    }
}

async fn answer_link(
    mut endpoint: Endpoint,
    segment: &LinkSegment,
    correlation: u64,
    origin: usize,
    size: usize,
    arrived: u64,
) {
    let mut request = vec![0; size];
    if endpoint.read_exact(&mut request).await.is_err() {
        return;
    }
    let read = now();
    let answering = now();
    let mut head = Vec::with_capacity(1 + 8 + STAMPS);
    head.push(REPLY);
    head.extend_from_slice(&correlation.to_le_bytes());
    head.extend_from_slice(&encode_stamps(arrived, read, answering));
    segment.inbox().write(origin, &head).expect("reply head");
    let _ = endpoint.write_all(&request).await;
    let _ = endpoint.shutdown().await;
}

/// The far side of `inline` and `frame`: the request came in the frame and
/// the answer goes back in one, each exchange on its own task.
async fn serve_whole(
    segment: Arc<LinkSegment>,
    bodies: Arc<LinkBodies>,
    lane: usize,
) {
    let mut receiver = segment.receiver(lane).expect("receiver");
    let mut frame = Vec::new();
    loop {
        match receiver.next(&mut frame).await.expect("next frame") {
            Received::Frame => {}
            _ => continue,
        }
        let arrived = now();
        let (correlation, origin, ticket, request) = decode_whole(&frame);
        // A slot announced with the call is opened as a body's would be.
        let endpoint =
            ticket.map(|ticket| bodies.accept(ticket).expect("accept"));
        let mut reply = Vec::with_capacity(1 + 8 + STAMPS + request.len());
        reply.push(REPLY);
        reply.extend_from_slice(&correlation.to_le_bytes());
        reply.extend_from_slice(&[0; STAMPS]);
        reply.extend_from_slice(request);
        let read = now();
        let segment = Arc::clone(&segment);
        tokio::spawn(async move {
            // Let go first, so the caller finds the slot free when the
            // answer reaches it.
            drop(endpoint);
            let answering = now();
            reply[9..9 + STAMPS]
                .copy_from_slice(&encode_stamps(arrived, read, answering));
            segment.inbox().write(origin, &reply).expect("reply");
        });
    }
}

/// A call that carries its request: the head, a line end, the bytes. `-`
/// stands where a slot's ticket would be when none was taken.
fn encode_whole(
    correlation: u64,
    origin: usize,
    ticket: Option<Ticket>,
    request: &[u8],
) -> Vec<u8> {
    let mut frame = match ticket {
        Some(ticket) => format!("{correlation} {origin} {ticket}\n"),
        None => format!("{correlation} {origin} -\n"),
    }
    .into_bytes();
    frame.extend_from_slice(request);
    frame
}

fn decode_whole(frame: &[u8]) -> (u64, usize, Option<Ticket>, &[u8]) {
    let end = frame.iter().position(|&byte| byte == b'\n').expect("a head");
    let head = std::str::from_utf8(&frame[..end]).expect("call head");
    let mut parts = head.splitn(3, ' ');
    let mut next = || parts.next().expect("call field");
    let correlation = next().parse().expect("correlation");
    let origin = next().parse().expect("origin");
    let ticket = match next() {
        "-" => None,
        text => Some(text.parse().expect("ticket")),
    };
    (correlation, origin, ticket, &frame[end + 1..])
}

fn encode_call(
    correlation: u64,
    origin: usize,
    ticket: Ticket,
) -> Vec<u8> {
    format!("{correlation} {origin} {ticket}").into_bytes()
}

fn decode_call(frame: &[u8]) -> (u64, usize, Ticket) {
    let text = std::str::from_utf8(frame).expect("call frame");
    let mut parts = text.splitn(3, ' ');
    let mut next = || parts.next().expect("call field");
    let correlation = next().parse().expect("correlation");
    let origin = next().parse().expect("origin");
    let ticket = next().parse().expect("ticket");
    (correlation, origin, ticket)
}

type Waiting = Arc<Mutex<HashMap<u64, oneshot::Sender<(u64, u64, u64, u64)>>>>;

/// What is taken for one exchange: a slot kept from the last one when its far
/// side has let go, otherwise a new one. The same choice a caller makes.
#[derive(Default)]
struct Hot(VecDeque<(Endpoint, Ticket)>);

impl Hot {
    fn take(
        &mut self,
        bodies: &LinkBodies,
    ) -> (Endpoint, Ticket) {
        if let Some((mut endpoint, ticket)) = self.0.pop_front() {
            if bodies.rearm_endpoint(&mut endpoint).is_ok() {
                return (endpoint, ticket);
            }
            self.0.push_back((endpoint, ticket));
        }
        bodies.create().expect("a free slot")
    }
}

struct Caller {
    segment: Arc<LinkSegment>,
    bodies: Arc<LinkBodies>,
    lane: usize,
    target: usize,
    waiting: Waiting,
    hot: Mutex<Hot>,
    correlation: std::sync::atomic::AtomicU64,
}

impl Caller {
    /// Replies come back to this lane and go to whoever waits on them.
    async fn read_replies(
        segment: Arc<LinkSegment>,
        lane: usize,
        waiting: Waiting,
    ) {
        let mut receiver = segment.receiver(lane).expect("receiver");
        let mut frame = Vec::new();
        loop {
            match receiver.next(&mut frame).await.expect("next reply") {
                Received::Frame => {}
                _ => continue,
            }
            let answered = now();
            let correlation = u64::from_le_bytes(
                frame[1..9].try_into().expect("correlation"),
            );
            let (arrived, read, answering) = decode_stamps(&frame[9..]);
            let sender = waiting.lock().expect("waiting").remove(&correlation);
            if let Some(sender) = sender {
                let _ = sender.send((arrived, read, answering, answered));
            }
        }
    }

    async fn call(
        &self,
        request: &[u8],
        reply: &mut [u8],
        from: u64,
    ) -> Option<Sample> {
        let start = now();
        let (mut endpoint, ticket) =
            self.hot.lock().expect("hot slots").take(&self.bodies);
        let correlation = self.correlation.fetch_add(1, Ordering::Relaxed);
        let (sender, head) = oneshot::channel();
        self.waiting.lock().expect("waiting").insert(correlation, sender);
        self.segment
            .inbox()
            .write(self.target, &encode_call(correlation, self.lane, ticket))
            .expect("call frame");
        endpoint.write_all(request).await.expect("request");
        let sent = now();
        let (arrived, read, answering, answered) =
            head.await.expect("reply head");
        endpoint.read_exact(reply).await.expect("reply");
        let done = now();
        self.hot.lock().expect("hot slots").0.push_back((endpoint, ticket));
        (start >= from).then_some(Sample {
            start,
            sent,
            arrived,
            read,
            answering,
            answered,
            done,
        })
    }

    /// One exchange whose request and reply each fit a frame. With `slot`, a
    /// stream is taken for it and ended at once, as today; without, the two
    /// frames are all there is.
    async fn call_whole(
        &self,
        request: &[u8],
        slot: bool,
        from: u64,
    ) -> Option<Sample> {
        let start = now();
        let held = slot.then(|| {
            let (mut endpoint, ticket) =
                self.hot.lock().expect("hot slots").take(&self.bodies);
            let _ = endpoint.finish();
            (endpoint, ticket)
        });
        let correlation = self.correlation.fetch_add(1, Ordering::Relaxed);
        let (sender, head) = oneshot::channel();
        self.waiting.lock().expect("waiting").insert(correlation, sender);
        let ticket = held.as_ref().map(|(_, ticket)| *ticket);
        self.segment
            .inbox()
            .write(
                self.target,
                &encode_whole(correlation, self.lane, ticket, request),
            )
            .expect("call frame");
        let sent = now();
        let (arrived, read, answering, answered) = head.await.expect("reply");
        let done = now();
        if let Some(held) = held {
            self.hot.lock().expect("hot slots").0.push_back(held);
        }
        (start >= from).then_some(Sample {
            start,
            sent,
            arrived,
            read,
            answering,
            answered,
            done,
        })
    }
}

/// Answer `mark` on standard input with what this process has spent, until
/// the parent closes it.
async fn control() {
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(_)) = lines.next_line().await {
        println!("{}", Spent::now().line());
        let _ = std::io::stdout().flush();
    }
}

/// The other process.
fn server(role: &str) {
    let size: usize =
        std::env::var(SIZE).expect("size").parse().expect("a size");
    let runtime = runtime();
    runtime.block_on(async move {
        let ready = || {
            println!("ready");
            let _ = std::io::stdout().flush();
        };
        if role == "uds" {
            let path = std::env::var(SOCKET).expect("socket");
            let listener = UnixListener::bind(&path).expect("bind");
            ready();
            tokio::select! {
                () = control() => {}
                () = async {
                    loop {
                        let (stream, _) =
                            listener.accept().await.expect("accept");
                        tokio::spawn(echo(stream, size));
                    }
                } => {}
            }
            return;
        }
        let fleet = std::env::var(FLEET).expect("fleet");
        let segment = Arc::new(LinkSegment::open(&fleet, SPEC).expect("link"));
        let joined = segment
            .join(SERVER_NAME, "worker", SERVER_INCARNATION)
            .expect("join");
        let bodies = Arc::new(
            segment.bodies(joined.lane, SERVER_INCARNATION).expect("bodies"),
        );
        ready();
        if role == "link" {
            tokio::select! {
                () = control() => {}
                () = serve_link(
                    Arc::clone(&segment),
                    Arc::clone(&bodies),
                    joined.lane,
                    size,
                ) => {}
            }
            return;
        }
        if role == "inline" || role == "frame" {
            tokio::select! {
                () = control() => {}
                () = serve_whole(
                    Arc::clone(&segment),
                    Arc::clone(&bodies),
                    joined.lane,
                ) => {}
            }
            return;
        }
        // `stream`: a frame only hands over a slot held from then on.
        let mut receiver = segment.receiver(joined.lane).expect("receiver");
        let accept = async {
            let mut frame = Vec::new();
            loop {
                match receiver.next(&mut frame).await.expect("next frame") {
                    Received::Frame => {}
                    _ => continue,
                }
                let (_, _, ticket) = decode_call(&frame);
                let endpoint = bodies.accept(ticket).expect("accept");
                tokio::spawn(echo(endpoint, size));
            }
        };
        tokio::select! {
            () = control() => {}
            () = accept => {}
        }
    });
}

struct Peer {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl Peer {
    fn start(
        role: &str,
        fleet: &str,
        socket: &Path,
        size: usize,
    ) -> Self {
        let mut child =
            Command::new(std::env::current_exe().expect("this program"))
                .env(ROLE, role)
                .env(FLEET, fleet)
                .env(SOCKET, socket)
                .env(SIZE, size.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("the other process");
        let input = child.stdin.take().expect("its input");
        let mut output =
            BufReader::new(child.stdout.take().expect("its output"));
        let mut line = String::new();
        output.read_line(&mut line).expect("ready");
        assert_eq!(line.trim(), "ready");
        Self { child, input, output }
    }

    fn spent(&mut self) -> Spent {
        writeln!(self.input, "mark").expect("mark");
        self.input.flush().expect("mark");
        let mut line = String::new();
        self.output.read_line(&mut line).expect("its spending");
        Spent::parse(&line)
    }

    fn stop(mut self) {
        drop(self.input);
        let _ = self.child.wait();
    }
}

fn median(values: &mut [i64]) -> i64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}

/// One row: rate, round trip, CPU, where the time went and what it cost in
/// wakes.
fn report(
    path: &str,
    size: usize,
    conns: usize,
    seconds: u64,
    samples: &[Sample],
    spent: Spent,
) {
    let of = |span: fn(&Sample) -> i64| -> i64 {
        let mut values: Vec<i64> = samples.iter().map(span).collect();
        median(&mut values)
    };
    let mut totals: Vec<i64> =
        samples.iter().map(|s| (s.done - s.start) as i64).collect();
    let p50 = median(&mut totals);
    let p99 = totals.get(totals.len() * 99 / 100).copied().unwrap_or(0);
    let count = samples.len().max(1) as u64;
    let each = |total: u64| total as f64 / count as f64;
    println!(
        "| {path} | {size} | {conns} | {} | {p50} | {p99} | {} | {} | {} | {} | {} | {} | {:.2} | {} | {:.2} | {:.2} | {:.2} |",
        samples.len() as u64 / seconds,
        spent.cpu / count,
        of(|s| s.sent as i64 - s.start as i64),
        of(|s| s.arrived as i64 - s.sent as i64),
        of(|s| s.read as i64 - s.arrived as i64),
        of(|s| s.answered as i64 - s.answering as i64),
        of(|s| s.done as i64 - s.answered as i64),
        each(spent.rings),
        spent.ring_nanos.checked_div(spent.rings).unwrap_or(0),
        each(spent.empty_reads),
        each(spent.parks),
        each(spent.switches),
    );
}

fn run(
    path: &str,
    size: usize,
    conns: usize,
    seconds: u64,
) {
    let fleet = format!("tb{:x}", std::process::id());
    let socket: PathBuf =
        std::env::temp_dir().join(format!("{fleet}-{path}.sock"));
    let _ = std::fs::remove_file(&socket);
    let setup = (path != "uds")
        .then(|| Arc::new(LinkSegment::open(&fleet, SPEC).expect("link")));
    let joined = setup.as_ref().map(|segment| {
        segment.join(CLIENT_NAME, "edge", CLIENT_INCARNATION).expect("join")
    });
    let bodies =
        setup.as_ref().zip(joined.as_ref()).map(|(segment, joined)| {
            let bodies = segment
                .bodies(joined.lane, CLIENT_INCARNATION)
                .expect("bodies");
            bodies.streams().reset_all();
            Arc::new(bodies)
        });
    let mut peer = Peer::start(path, &fleet, &socket, size);

    let named = matches!(path, "link" | "inline" | "frame");
    let runtime = runtime();
    let (samples, spent) = runtime.block_on(async {
        let measure = seconds * 1_000_000_000;
        let from = now() + WARM_UP.as_nanos() as u64;
        let end = from + measure;
        let mut tasks = Vec::new();
        let replies = match (&setup, &joined, &bodies) {
            (Some(segment), Some(joined), Some(_)) if named => {
                let waiting = Waiting::default();
                Some((
                    waiting.clone(),
                    tokio::spawn(Caller::read_replies(
                        Arc::clone(segment),
                        joined.lane,
                        waiting,
                    )),
                ))
            }
            _ => None,
        };
        let target = setup
            .as_ref()
            .map(|segment| segment.inbox().targets(SERVER_NAME)[0].0);
        if named {
            let caller = Arc::new(Caller {
                segment: Arc::clone(setup.as_ref().expect("link")),
                bodies: Arc::clone(bodies.as_ref().expect("bodies")),
                lane: joined.as_ref().expect("lane").lane,
                target: target.expect("target"),
                waiting: replies.as_ref().expect("replies").0.clone(),
                hot: Mutex::default(),
                correlation: AtomicU64::new(0),
            });
            let whole = path != "link";
            let slot = path == "inline";
            for _ in 0..conns {
                let caller = Arc::clone(&caller);
                tasks.push(tokio::spawn(async move {
                    let request = vec![7; size];
                    let mut reply = vec![0; size];
                    let mut samples = Vec::new();
                    while now() < end {
                        let sample = if whole {
                            caller.call_whole(&request, slot, from).await
                        } else {
                            caller.call(&request, &mut reply, from).await
                        };
                        if let Some(sample) = sample {
                            samples.push(sample);
                        }
                    }
                    samples
                }));
            }
        } else if path == "stream" {
            let segment = setup.as_ref().expect("link");
            let bodies = bodies.as_ref().expect("bodies");
            for correlation in 0..conns {
                let (endpoint, ticket) = bodies.create().expect("a slot");
                segment
                    .inbox()
                    .write(
                        target.expect("target"),
                        &encode_call(correlation as u64, 0, ticket),
                    )
                    .expect("hand over");
                tasks.push(tokio::spawn(call(endpoint, size, from, end)));
            }
        } else {
            for _ in 0..conns {
                let stream =
                    UnixStream::connect(&socket).await.expect("connect");
                tasks.push(tokio::spawn(call(stream, size, from, end)));
            }
        }

        // Warm up, then count both processes' spending across the counted
        // time.
        tokio::time::sleep(WARM_UP).await;
        let far = peer.spent();
        let near = Spent::now();
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        let spent = Spent::between((near, Spent::now()), (far, peer.spent()));
        let mut samples = Vec::new();
        for task in tasks {
            samples.extend(task.await.expect("a caller"));
        }
        if let Some((_, reader)) = replies {
            reader.abort();
        }
        (samples, spent)
    });
    drop(runtime);
    peer.stop();
    report(path, size, conns, seconds, &samples, spent);

    let _ = std::fs::remove_file(&socket);
    drop(joined);
    if let Some(bodies) = bodies {
        let _ = bodies.unlink();
    }
    if let Some(segment) = setup {
        let _ = segment.unlink();
    }
}

fn list(
    arguments: &[String],
    name: &str,
    default: &[usize],
) -> Vec<usize> {
    arguments
        .iter()
        .position(|argument| argument == name)
        .and_then(|at| arguments.get(at + 1))
        .map(|text| {
            text.split(',').map(|n| n.parse().expect("a number")).collect()
        })
        .unwrap_or_else(|| default.to_vec())
}

fn main() {
    if let Ok(role) = std::env::var(ROLE) {
        server(&role);
        return;
    }
    let arguments: Vec<String> = std::env::args().collect();
    let sizes = list(&arguments, "--sizes", &[64, 1024, 4096, 32768, 131072]);
    let conns = list(&arguments, "--conns", &[1, 8, 64]);
    let seconds = list(&arguments, "--seconds", &[5])[0] as u64;
    let paths: Vec<&str> = arguments
        .iter()
        .position(|argument| argument == "--paths")
        .and_then(|at| arguments.get(at + 1))
        .map_or(vec!["uds", "stream", "link", "inline", "frame"], |text| {
            text.split(',').collect()
        });
    println!(
        "| path | bytes | conns | ops/s | p50 ns | p99 ns | CPU ns/op | send ns | wake ns | read ns | reply wake ns | reply read ns | bells/op | ns/bell | empty bell reads/op | parks/op | switches/op |"
    );
    println!(
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for &size in &sizes {
        for &count in &conns {
            for path in &paths {
                // A frame-only call holds only what a frame holds.
                if matches!(*path, "inline" | "frame") && size > WHOLE_LIMIT {
                    continue;
                }
                run(path, size, count, seconds);
            }
        }
    }
}
