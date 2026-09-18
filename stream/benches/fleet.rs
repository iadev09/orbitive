//! The fleet talking to itself across real processes.
//!
//! Every other stream number in this crate has both ends in one process.
//! This one starts `--nodes` processes, each with its own fleet node, its
//! own Tokio runtime and its own driver thread, and has them exchange
//! request/response streams at the same time. It is the shape the pool's
//! remote reuse needs: a worker hands a small request to another worker's
//! process and streams the answer back. No HTTP anywhere; the question
//! here is only whether the async halves carry that traffic.
//!
//! Four dimensions, every one of them a real question:
//!
//! - shape: `mesh`, where every node is client and server at once and
//!   each request goes to the next peer in turn — do N processes hold
//!   their own lanes without standing on each other? — and `fanin`,
//!   where node 0 only serves and everyone else only asks: one process's
//!   driver thread and offer bitmap against the whole fleet.
//! - transport: `orbit-stream`, or a Unix socket per node. Same
//!   processes, same tasks, same protocol, so the difference is the
//!   transport and nothing else.
//! - link: `per` opens a stream (or connects a socket) for every
//!   request; `reuse` keeps one per peer per task and sends request
//!   after request down it, which is what a pooled upstream connection
//!   does. The gap between the two is what setup costs.
//! - in-flight: how many requests a node has outstanding at once, which
//!   is where a single driver thread per process would show up.
//!
//! The protocol is an 8-byte request (body size, tag) and a response of
//! that many bytes whose first four echo the tag. A response that
//! arrived on the wrong stream is caught by its tag, so a run is also a
//! cross-talk check; a run ends by counting every answer the fleet gave
//! against every request it made, so a lost one is caught too.
//!
//! Columns are per phase and fleet-wide: `ops/s` and `MiB/s` are the sum
//! over the nodes that asked, `cpu/op` and `vcsw/op` the sum over every
//! node's own `getrusage` between the same two instants, so work paid
//! for in the server's process is counted. Latencies are the merged
//! client samples of the whole fleet. (`ru_nvcsw` is not maintained on
//! macOS; the `vcsw` column is a Linux and FreeBSD number.)
//!
//! Run:
//!
//! ```sh
//! cargo bench -p orbit-stream --features tokio --bench fleet -- \
//!     [--nodes 4] [--threads 2] [--requests 20000] \
//!     [--shape mesh|fanin|both] [--transport stream|uds|both] \
//!     [--link per|reuse|both] [--inflight 1,8,64] \
//!     [--body 1024,65536,1048576] [--verbose]
//! ```

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::fmt::Write as _;
use std::future::poll_fn;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use orbit_core::{Fleet, NodeId};
use orbit_stream::{
    Error, Incarnation, STREAM_BUFFER_BYTES, STREAM_LANE_CAPACITY, Streams, segment_size
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Runtime;

/// The parent puts everything a node needs in its environment; its own
/// node id is what tells a process that it is a child at all.
const ENV_NODE: &str = "ORBIT_FLEET_BENCH_NODE";
const ENV_FLEET: &str = "ORBIT_FLEET_BENCH_FLEET";
const ENV_NODES: &str = "ORBIT_FLEET_BENCH_NODES";
const ENV_THREADS: &str = "ORBIT_FLEET_BENCH_THREADS";
const ENV_DIR: &str = "ORBIT_FLEET_BENCH_DIR";

/// Request header: body size, then the tag the response must echo.
const HEADER: usize = 8;
/// The largest body a phase may ask for, and the filler each node holds.
const MAX_BODY: usize = 1 << 20;
/// A round trip over this is reported on its own, with its parts.
const SLOW: Duration = Duration::from_secs(1);
/// A request still unanswered after this long is not slow, it is lost.
const STUCK: Duration = Duration::from_secs(10);
/// Long enough for the answers still in flight when a run ends to land.
const SETTLE: Duration = Duration::from_millis(200);
/// A node that says nothing for this long has hung or died.
const PATIENCE: Duration = Duration::from_secs(300);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Shape {
    Mesh,
    FanIn
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Transport {
    Stream,
    Uds
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Link {
    PerRequest,
    Reuse
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Self::Mesh => "mesh",
            Self::FanIn => "fanin"
        }
    }

    fn parse(text: &str) -> Self {
        match text {
            "mesh" => Self::Mesh,
            "fanin" => Self::FanIn,
            other => panic!("unknown shape {other}")
        }
    }
}

impl Transport {
    fn name(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::Uds => "uds"
        }
    }

    fn parse(text: &str) -> Self {
        match text {
            "stream" => Self::Stream,
            "uds" => Self::Uds,
            other => panic!("unknown transport {other}")
        }
    }
}

impl Link {
    fn name(self) -> &'static str {
        match self {
            Self::PerRequest => "per",
            Self::Reuse => "reuse"
        }
    }

    fn parse(text: &str) -> Self {
        match text {
            "per" => Self::PerRequest,
            "reuse" => Self::Reuse,
            other => panic!("unknown link mode {other}")
        }
    }
}

/// One measured run. The same line is sent to every node.
#[derive(Clone, Copy, Debug)]
struct Phase {
    shape: Shape,
    transport: Transport,
    link: Link,
    ops: usize,
    inflight: usize,
    body: usize
}

impl Phase {
    fn line(&self) -> String {
        format!(
            "arm {} {} {} {} {} {}",
            self.shape.name(),
            self.transport.name(),
            self.link.name(),
            self.ops,
            self.inflight,
            self.body
        )
    }

    fn parse(text: &str) -> Self {
        let mut parts = text.split_whitespace();
        let mut next = || parts.next().expect("a phase field");
        Self {
            shape: Shape::parse(next()),
            transport: Transport::parse(next()),
            link: Link::parse(next()),
            ops: next().parse().unwrap(),
            inflight: next().parse().unwrap(),
            body: next().parse().unwrap()
        }
    }
}

struct Usage {
    cpu: Duration,
    voluntary: i64,
    involuntary: i64
}

fn usage() -> Usage {
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) };
    let seconds = |time: libc::timeval| {
        Duration::from_secs(time.tv_sec as u64) + Duration::from_micros(time.tv_usec as u64)
    };
    Usage {
        cpu: seconds(rusage.ru_utime) + seconds(rusage.ru_stime),
        voluntary: rusage.ru_nvcsw as i64,
        involuntary: rusage.ru_nivcsw as i64
    }
}

/// Opened on `go`, closed on `stop`: every node measures itself over the
/// same two instants, whether it spent the phase asking or answering.
struct Window {
    usage: Usage,
    served: u64,
    start: Instant
}

/// What one node did during one phase. A node that only served reports
/// no operations of its own and the CPU it spent on everyone else's.
struct Report {
    node: u16,
    ops: usize,
    served: u64,
    wall: Duration,
    cpu: Duration,
    voluntary: i64,
    involuntary: i64,
    samples: Vec<Duration>
}

impl Report {
    fn line(&self) -> String {
        let mut text = format!(
            "done {} {} {} {} {} {} {} ",
            self.node,
            self.ops,
            self.served,
            self.wall.as_nanos(),
            self.cpu.as_nanos(),
            self.voluntary,
            self.involuntary
        );
        for sample in &self.samples {
            let _ = write!(text, "{:x},", sample.as_nanos() as u64);
        }
        text
    }

    fn parse(text: &str) -> Self {
        let mut parts = text.split_whitespace();
        let mut next = || parts.next().expect("a report field").to_owned();
        let node = next().parse().unwrap();
        let ops = next().parse().unwrap();
        let served = next().parse().unwrap();
        let wall = Duration::from_nanos(next().parse().unwrap());
        let cpu = Duration::from_nanos(next().parse().unwrap());
        let voluntary = next().parse().unwrap();
        let involuntary = next().parse().unwrap();
        let samples = parts
            .next()
            .unwrap_or("")
            .split_terminator(',')
            .map(|value| Duration::from_nanos(u64::from_str_radix(value, 16).unwrap()))
            .collect();
        Self { node, ops, served, wall, cpu, voluntary, involuntary, samples }
    }
}

/// The client's end of one link, whichever transport it is made of.
enum ClientLink {
    Stream(orbit_stream::ReadHalf, orbit_stream::WriteHalf),
    Uds(tokio::net::unix::OwnedReadHalf, tokio::net::unix::OwnedWriteHalf)
}

impl ClientLink {
    async fn open(
        transport: Transport,
        streams: &Streams,
        dir: &Path,
        target: u16
    ) -> Self {
        match transport {
            Transport::Stream => {
                let (endpoint, ticket) = loop {
                    match streams.create() {
                        Ok(pair) => break pair,
                        // The lane is the ceiling on the streams one node
                        // holds open at once; a consumer queues here.
                        Err(Error::Full { .. }) => tokio::task::yield_now().await,
                        Err(error) => panic!("create: {error}")
                    }
                };
                streams.offer(ticket, NodeId::new(target)).unwrap();
                let (read, write) = endpoint.split();
                Self::Stream(read, write)
            }
            Transport::Uds => {
                let path = socket_path(dir, target);
                let socket = loop {
                    match UnixStream::connect(&path).await {
                        Ok(socket) => break socket,
                        // A listener's backlog is a fixed queue and a
                        // fleet of clients overruns it; a real client
                        // retries. The stream has no queue to overrun:
                        // an offer is a bit in a bitmap, one per slot,
                        // and is never refused.
                        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                            tokio::time::sleep(Duration::from_micros(200)).await;
                        }
                        Err(error) => panic!("connect {}: {error}", path.display())
                    }
                };
                let (read, write) = socket.into_split();
                Self::Uds(read, write)
            }
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::Stream(read, _) => format!("stream {}", read.id()),
            Self::Uds(..) => "socket".to_owned()
        }
    }

    async fn round_trip(
        &mut self,
        body: usize,
        tag: u32,
        sink: &mut [u8]
    ) {
        match self {
            Self::Stream(read, write) => round_trip(read, write, body, tag, sink).await,
            Self::Uds(read, write) => round_trip(read, write, body, tag, sink).await
        }
    }
}

/// The client half of the protocol: the header out, the body back, and
/// the tag says the body belongs to this request and no other.
async fn round_trip<R, W>(
    read: &mut R,
    write: &mut W,
    body: usize,
    tag: u32,
    sink: &mut [u8]
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin
{
    let mut header = [0_u8; HEADER];
    header[..4].copy_from_slice(&(body as u32).to_le_bytes());
    header[4..].copy_from_slice(&tag.to_le_bytes());
    write.write_all(&header).await.unwrap();
    read.read_exact(&mut sink[..body]).await.unwrap();
    let echoed = u32::from_le_bytes(sink[..4].try_into().unwrap());
    assert_eq!(echoed, tag, "a response came back on the wrong link");
}

/// The server half: answer until the other end goes away. One request or
/// a thousand down the same link is the same loop.
async fn serve_link<R, W>(
    read: &mut R,
    write: &mut W,
    filler: &[u8],
    served: &AtomicU64
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin
{
    let mut header = [0_u8; HEADER];
    loop {
        if read.read_exact(&mut header).await.is_err() {
            return;
        }
        let body = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let tag = u32::from_le_bytes(header[4..].try_into().unwrap());
        if write.write_all(&tag.to_le_bytes()).await.is_err()
            || write.write_all(&filler[..body - 4]).await.is_err()
        {
            return;
        }
        served.fetch_add(1, Ordering::Relaxed);
    }
}

fn socket_path(
    dir: &Path,
    node: u16
) -> PathBuf {
    dir.join(format!("n{node}.sock"))
}

/// One fleet member. The parent runs this too, as node 0.
struct Node {
    id: u16,
    nodes: u16,
    streams: Streams,
    listener: Option<UnixListener>,
    dir: PathBuf,
    filler: Arc<Vec<u8>>,
    served: Arc<AtomicU64>
}

impl Node {
    fn new(
        id: u16,
        nodes: u16,
        fleet: &'static str,
        dir: PathBuf,
        runtime: &Runtime
    ) -> Self {
        let fleet = Arc::new(Fleet::join_shm_as(fleet, nodes, NodeId::new(id)).unwrap());
        let streams = Streams::new(fleet, Incarnation::new(10 + u64::from(id))).unwrap();
        if id == 0 {
            // Node 0 opens the segment before anyone else joins and
            // clears whatever a crashed earlier run left in it.
            streams.reset_all();
        }
        let path = socket_path(&dir, id);
        let _ = std::fs::remove_file(&path);
        let listener = runtime.block_on(async { UnixListener::bind(&path).unwrap() });
        Self {
            id,
            nodes,
            streams,
            listener: Some(listener),
            dir,
            filler: Arc::new((0..MAX_BODY).map(|index| index as u8).collect()),
            served: Arc::new(AtomicU64::new(0))
        }
    }

    /// Both servers run for the whole session; a phase only decides which
    /// one is asked anything. An idle listener costs nothing, and the
    /// tasks live on the runtime's own threads, so a node keeps answering
    /// while its main thread waits for the parent's next line.
    fn serve(&mut self) {
        let streams = self.streams.clone();
        let filler = Arc::clone(&self.filler);
        let served = Arc::clone(&self.served);
        tokio::spawn(async move {
            loop {
                let Ok(ticket) = poll_fn(|cx| streams.poll_take_offer(cx)).await else {
                    return;
                };
                // Nobody here gives up on a stream it offered, so a
                // refused open is a defect and not a lost race.
                let endpoint = streams
                    .open(ticket)
                    .unwrap_or_else(|error| panic!("open {ticket} offered to this node: {error}"));
                let filler = Arc::clone(&filler);
                let served = Arc::clone(&served);
                tokio::spawn(async move {
                    let (mut read, mut write) = endpoint.split();
                    serve_link(&mut read, &mut write, &filler, &served).await;
                });
            }
        });

        let listener = self.listener.take().expect("listener");
        let filler = Arc::clone(&self.filler);
        let served = Arc::clone(&self.served);
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let filler = Arc::clone(&filler);
                let served = Arc::clone(&served);
                tokio::spawn(async move {
                    let (mut read, mut write) = socket.into_split();
                    serve_link(&mut read, &mut write, &filler, &served).await;
                });
            }
        });
    }

    /// Fan-in gives node 0 nothing to ask; it spends the phase serving.
    fn asks(
        &self,
        shape: Shape
    ) -> bool {
        shape != Shape::FanIn || self.id != 0
    }

    /// Which peer a task asks next. Mesh spreads over every other node so
    /// no pair carries the fleet; fan-in always asks node 0. The rotation
    /// is the same in both link modes, so only the link's lifetime
    /// differs between them.
    fn target(
        &self,
        shape: Shape,
        task: usize,
        round: usize
    ) -> u16 {
        match shape {
            Shape::FanIn => 0,
            Shape::Mesh => {
                let peers = usize::from(self.nodes) - 1;
                let step = (task + round) % peers;
                ((usize::from(self.id) + 1 + step) % usize::from(self.nodes)) as u16
            }
        }
    }

    fn begin(&self) -> Window {
        Window {
            usage: usage(),
            served: self.served.load(Ordering::Relaxed),
            start: Instant::now()
        }
    }

    fn finish(
        &self,
        window: Window,
        samples: Vec<Duration>
    ) -> Report {
        let after = usage();
        Report {
            node: self.id,
            ops: samples.len(),
            served: self.served.load(Ordering::Relaxed) - window.served,
            wall: window.start.elapsed(),
            cpu: after.cpu - window.usage.cpu,
            voluntary: after.voluntary - window.usage.voluntary,
            involuntary: after.involuntary - window.usage.involuntary,
            samples
        }
    }

    /// The client side of a phase: `inflight` tasks, each with its own
    /// links and its own sink, each timing every round trip it makes.
    async fn ask(
        self: &Arc<Self>,
        phase: Phase
    ) -> Vec<Duration> {
        if !self.asks(phase.shape) {
            return Vec::new();
        }
        let inflight = phase.inflight.max(1);
        let per_task = (phase.ops / inflight).max(1);
        let mut tasks = Vec::with_capacity(inflight);
        for task in 0..inflight {
            let node = Arc::clone(self);
            tasks.push(tokio::spawn(async move {
                let mut sink = vec![0_u8; phase.body];
                let mut samples = Vec::with_capacity(per_task);
                let mut kept: Vec<Option<ClientLink>> = (0..node.nodes).map(|_| None).collect();
                for round in 0..per_task {
                    let target = node.target(phase.shape, task, round);
                    let tag = tag_of(node.id, task, round);
                    let started = Instant::now();
                    let mut fresh;
                    let link = match phase.link {
                        Link::PerRequest => {
                            fresh =
                                ClientLink::open(phase.transport, &node.streams, &node.dir, target)
                                    .await;
                            &mut fresh
                        }
                        Link::Reuse => {
                            let place = &mut kept[usize::from(target)];
                            if place.is_none() {
                                *place = Some(
                                    ClientLink::open(
                                        phase.transport,
                                        &node.streams,
                                        &node.dir,
                                        target
                                    )
                                    .await
                                );
                            }
                            place.as_mut().expect("a kept link")
                        }
                    };
                    let opened = started.elapsed();
                    // A request that never comes back would otherwise hang
                    // the whole run in silence. Note what this cannot
                    // tell apart: `timeout` polls the round trip again
                    // when the timer fires, so a wake that was lost
                    // rather than late is rescued by that poll and shows
                    // up as a trip of about `STUCK`, in the slow line
                    // below, instead of as a failure here.
                    let trip = link.round_trip(phase.body, tag, &mut sink);
                    if tokio::time::timeout(STUCK, trip).await.is_err() {
                        panic!(
                            "node {} task {task} round {round}: {} to node {target} \
                             answered nothing in {STUCK:?}",
                            node.id,
                            link.describe(),
                        );
                    }
                    let elapsed = started.elapsed();
                    // A round trip an order of magnitude over the rest is
                    // worth a line of its own: the medians hide it, and a
                    // trip near `STUCK` is the signature of a wake that
                    // only a timer found.
                    if elapsed > SLOW {
                        eprintln!(
                            "slow: node {} task {task} round {round} to node {target}: \
                             {} open={opened:?} trip={:?}",
                            node.id,
                            link.describe(),
                            elapsed - opened,
                        );
                    }
                    samples.push(elapsed);
                }
                samples
            }));
        }
        let mut samples = Vec::with_capacity(phase.ops);
        for task in tasks {
            samples.extend(task.await.unwrap());
        }
        samples
    }
}

/// Node, task and round in one word, so a response that belongs to
/// another request cannot pass for this one.
fn tag_of(
    node: u16,
    task: usize,
    round: usize
) -> u32 {
    (u32::from(node) << 26) | (((task as u32) & 0x3F) << 20) | ((round as u32) & 0xF_FFFF)
}

fn percentiles(samples: &mut [Duration]) -> (Duration, Duration, Duration) {
    if samples.is_empty() {
        return (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    }
    samples.sort_unstable();
    let at = |quantile: f64| samples[((samples.len() - 1) as f64 * quantile) as usize];
    (at(0.5), at(0.99), *samples.last().unwrap())
}

fn runtime(threads: usize) -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap()
}

fn report(line: &str) {
    println!("ORBIT:{line}");
    std::io::stdout().flush().unwrap();
}

// ---------------------------------------------------------------- child

/// A node that is not the parent: join, serve, and do what the parent's
/// lines say until it is told to quit. `go` opens the measurement
/// window, `stop` closes it, so every node in the fleet measures itself
/// over the same span.
fn child(id: u16) {
    let fleet: &'static str = Box::leak(std::env::var(ENV_FLEET).unwrap().into_boxed_str());
    let nodes: u16 = std::env::var(ENV_NODES).unwrap().parse().unwrap();
    let threads: usize = std::env::var(ENV_THREADS).unwrap().parse().unwrap();
    let dir = PathBuf::from(std::env::var(ENV_DIR).unwrap());
    let runtime = runtime(threads);
    let mut node = Node::new(id, nodes, fleet, dir, &runtime);
    runtime.block_on(async { node.serve() });
    let node = Arc::new(node);
    report("ready");

    let mut armed: Option<Phase> = None;
    let mut ran: Option<(Window, Vec<Duration>)> = None;
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap();
        if let Some(rest) = line.strip_prefix("arm ") {
            armed = Some(Phase::parse(rest));
            report("armed");
        } else if line == "go" {
            let phase = armed.take().expect("go without arm");
            let window = node.begin();
            let samples = runtime.block_on(node.ask(phase));
            report("finished");
            ran = Some((window, samples));
        } else if line == "stop" {
            let (window, samples) = ran.take().expect("stop without go");
            report(&node.finish(window, samples).line());
        } else if line == "total" {
            // Whatever was still in flight when the last phase ended has
            // landed by now; this is the run's integrity check.
            std::thread::sleep(SETTLE);
            report(&format!("total {}", node.served.load(Ordering::Relaxed)));
        } else if line == "quit" {
            break;
        }
    }
    report("gone");
}

// --------------------------------------------------------------- parent

struct Worker {
    child: Child,
    lines: Receiver<String>
}

impl Worker {
    fn spawn(
        id: u16,
        options: &Options,
        fleet: &str,
        dir: &Path
    ) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .env(ENV_NODE, id.to_string())
            .env(ENV_FLEET, fleet)
            .env(ENV_NODES, options.nodes.to_string())
            .env(ENV_THREADS, options.threads.to_string())
            .env(ENV_DIR, dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if sender.send(line).is_err() {
                    return;
                }
            }
        });
        let worker = Self { child, lines };
        worker.expect("ready");
        worker
    }

    fn expect(
        &self,
        what: &str
    ) -> String {
        let deadline = Instant::now() + PATIENCE;
        loop {
            let line = self
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("a node died or hung");
            if let Some(rest) = line.strip_prefix("ORBIT:")
                && let Some(rest) = rest.strip_prefix(what)
            {
                return rest.trim_start().to_owned();
            }
        }
    }

    fn send(
        &mut self,
        line: &str
    ) {
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{line}").unwrap();
        stdin.flush().unwrap();
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Only this run's children, and never one left behind on a panic.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Options {
    nodes: u16,
    threads: usize,
    requests: usize,
    shapes: Vec<Shape>,
    transports: Vec<Transport>,
    links: Vec<Link>,
    inflight: Vec<usize>,
    bodies: Vec<usize>,
    verbose: bool
}

fn parse_options() -> Options {
    let mut options = Options {
        nodes: 4,
        threads: 2,
        requests: 20_000,
        shapes: vec![Shape::Mesh, Shape::FanIn],
        transports: vec![Transport::Stream, Transport::Uds],
        links: vec![Link::PerRequest, Link::Reuse],
        inflight: vec![1, 8, 64],
        bodies: vec![1024, 64 * 1024, 1024 * 1024],
        verbose: false
    };
    let list = |text: String| -> Vec<usize> {
        text.split(',').map(|part| part.trim().parse().unwrap()).collect()
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().expect("a value after the flag");
        match arg.as_str() {
            "--nodes" => options.nodes = value().parse().unwrap(),
            "--threads" => options.threads = value().parse().unwrap(),
            "--requests" => options.requests = value().parse().unwrap(),
            "--shape" => {
                options.shapes = match value().as_str() {
                    "both" => vec![Shape::Mesh, Shape::FanIn],
                    one => vec![Shape::parse(one)]
                }
            }
            "--transport" => {
                options.transports = match value().as_str() {
                    "both" => vec![Transport::Stream, Transport::Uds],
                    one => vec![Transport::parse(one)]
                }
            }
            "--link" => {
                options.links = match value().as_str() {
                    "both" => vec![Link::PerRequest, Link::Reuse],
                    one => vec![Link::parse(one)]
                }
            }
            "--inflight" => options.inflight = list(value()),
            "--body" => options.bodies = list(value()),
            "--verbose" => options.verbose = true,
            // `cargo bench` passes flags of its own to the binary.
            _ => {}
        }
    }
    assert!(options.nodes >= 2, "a fleet needs at least two nodes");
    assert!(
        options.bodies.iter().all(|body| (4..=MAX_BODY).contains(body)),
        "a body is between 4 bytes and 1 MiB"
    );
    options
}

/// A shorter run for a bigger body: equal wall time per phase is worth
/// more here than an equal operation count.
fn ops_for(
    requests: usize,
    body: usize
) -> usize {
    (requests / (1 + body / (64 * 1024))).max(200)
}

fn fleet_name() -> &'static str {
    let pid = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos() & 0xFF_FFFF;
    // macOS allows 31 characters for a POSIX SHM name, prefix included.
    Box::leak(format!("f{pid:04x}{nonce:06x}").into_boxed_str())
}

fn main() {
    if let Ok(id) = std::env::var(ENV_NODE) {
        child(id.parse().unwrap());
        return;
    }

    let options = parse_options();
    let fleet = fleet_name();
    let dir = std::env::temp_dir().join(fleet);
    std::fs::create_dir_all(&dir).unwrap();
    let runtime = runtime(options.threads);
    // Node 0 first: it clears the segment before anyone else joins.
    let mut node = Node::new(0, options.nodes, fleet, dir.clone(), &runtime);
    runtime.block_on(async { node.serve() });
    let node = Arc::new(node);
    let mut workers: Vec<Worker> =
        (1..options.nodes).map(|id| Worker::spawn(id, &options, fleet, &dir)).collect();

    println!(
        "orbit-stream fleet: nodes={} threads/node={} lane={} ring={} KiB segment={} MiB dir={}",
        options.nodes,
        options.threads,
        STREAM_LANE_CAPACITY,
        STREAM_BUFFER_BYTES / 1024,
        segment_size(options.nodes) / (1024 * 1024),
        dir.display(),
    );
    println!(
        "{:<6} {:<7} {:<6} {:>8} {:>5} {:>8} {:>10} {:>10} {:>9} {:>10} {:>10} {:>10} {:>9} {:>8}",
        "shape",
        "via",
        "link",
        "body",
        "infl",
        "ops",
        "wall",
        "ops/s",
        "MiB/s",
        "p50",
        "p99",
        "max",
        "cpu/op",
        "vcsw/op",
    );

    let mut asked = 0_u64;
    for &shape in &options.shapes {
        for &body in &options.bodies {
            for &inflight in &options.inflight {
                for &link in &options.links {
                    for &transport in &options.transports {
                        let phase = Phase {
                            shape,
                            transport,
                            link,
                            ops: ops_for(options.requests, body),
                            inflight,
                            body
                        };
                        let reports = run_phase(&runtime, &node, &mut workers, phase);
                        asked += reports.iter().map(|report| report.ops as u64).sum::<u64>();
                        print_phase(phase, &reports, options.verbose);
                    }
                }
            }
        }
    }

    // Every request the fleet made was answered by somebody: the tags
    // proved each answer belonged to its own link, this proves none went
    // missing.
    let mut answered = 0_u64;
    for worker in &mut workers {
        worker.send("total");
    }
    for worker in &workers {
        answered += worker.expect("total").parse::<u64>().unwrap();
    }
    std::thread::sleep(SETTLE);
    answered += node.served.load(Ordering::Relaxed);
    println!("answered {answered} of {asked} requests");
    assert_eq!(answered, asked, "the fleet lost a request");

    for worker in &mut workers {
        worker.send("quit");
        worker.expect("gone");
    }
    drop(workers);
    let _ = node.streams.unlink();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Arm every node, let them all go at once, and stop them together. The
/// parent is node 0, so it runs its own side while the children run
/// theirs; the phase is over when the last node has finished asking.
fn run_phase(
    runtime: &Runtime,
    node: &Arc<Node>,
    workers: &mut [Worker],
    phase: Phase
) -> Vec<Report> {
    let line = phase.line();
    for worker in workers.iter_mut() {
        worker.send(&line);
    }
    for worker in workers.iter() {
        worker.expect("armed");
    }

    let start = Instant::now();
    let window = node.begin();
    for worker in workers.iter_mut() {
        worker.send("go");
    }
    let samples = runtime.block_on(node.ask(phase));
    for worker in workers.iter() {
        worker.expect("finished");
    }
    let wall = start.elapsed();

    for worker in workers.iter_mut() {
        worker.send("stop");
    }
    let mut reports = vec![node.finish(window, samples)];
    for worker in workers.iter() {
        reports.push(Report::parse(&worker.expect("done")));
    }
    // The fleet's wall clock, not each node's: the phase is over when the
    // last of them is.
    reports[0].wall = wall;
    reports
}

fn print_phase(
    phase: Phase,
    reports: &[Report],
    verbose: bool
) {
    let wall = reports[0].wall;
    let ops: usize = reports.iter().map(|report| report.ops).sum();
    let cpu: Duration = reports.iter().map(|report| report.cpu).sum();
    let voluntary: i64 = reports.iter().map(|report| report.voluntary).sum();
    let mut samples: Vec<Duration> =
        reports.iter().flat_map(|report| report.samples.iter().copied()).collect();
    let (p50, p99, max) = percentiles(&mut samples);
    let seconds = wall.as_secs_f64();
    let bytes = (ops * phase.body) as f64;
    println!(
        "{:<6} {:<7} {:<6} {:>8} {:>5} {:>8} {:>10.3?} {:>10.0} {:>9.1} {:>10.2?} {:>10.2?} {:>10.2?} {:>9.2?} {:>8.2}",
        phase.shape.name(),
        phase.transport.name(),
        phase.link.name(),
        human(phase.body),
        phase.inflight,
        ops,
        wall,
        ops as f64 / seconds,
        bytes / seconds / (1024.0 * 1024.0),
        p50,
        p99,
        max,
        cpu / ops.max(1) as u32,
        voluntary as f64 / ops.max(1) as f64,
    );
    if verbose {
        for report in reports {
            let mut samples = report.samples.clone();
            let (p50, p99, max) = percentiles(&mut samples);
            println!(
                "    node {:<3} asked={:<8} served={:<8} cpu={:>9.3?} vcsw={:<7} ivcsw={:<7} p50={:>9.2?} p99={:>9.2?} max={:>9.2?}",
                report.node,
                report.ops,
                report.served,
                report.cpu,
                report.voluntary,
                report.involuntary,
                p50,
                p99,
                max,
            );
        }
    }
}

fn human(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{} MiB", bytes / (1024 * 1024))
    } else if bytes >= 1024 {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{bytes} B")
    }
}
