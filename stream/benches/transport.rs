//! The stream against what it would replace: Tokio's in-process duplex and
//! a Unix socket pair, on the same runtime with the same task shape. Two
//! things are measured: bytes one way for several body sizes, and a
//! 128-byte round trip. The shared-memory case runs both nodes in one
//! process; that is the same doorbell path as two processes, minus the
//! scheduler noise of a second process.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use orbit_core::{Fleet, NodeId};
use orbit_stream::{Incarnation, STREAM_BUFFER_BYTES, Streams};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::runtime::Runtime;

const SIZES: [usize; 4] = [4 * 1024, 64 * 1024, 1024 * 1024, 16 * 1024 * 1024];
const PING: usize = 128;

fn fresh_fleet_name() -> &'static str {
    let pid = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    Box::leak(format!("b{pid:04x}{nonce:08x}").into_boxed_str())
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

/// Both ends of one connection, whatever it is made of. Halves are lent to
/// tasks and put back, so they live in options.
struct Link<R, W> {
    a_read: Option<R>,
    a_write: Option<W>,
    b_read: Option<R>,
    b_write: Option<W>,
}

impl<R, W> Link<R, W> {
    fn new(a: (R, W), b: (R, W)) -> Self {
        Self {
            a_read: Some(a.0),
            a_write: Some(a.1),
            b_read: Some(b.0),
            b_write: Some(b.1),
        }
    }
}

type StreamLink = Link<orbit_stream::ReadHalf, orbit_stream::WriteHalf>;

fn stream_link(owner: &Streams, peer: &Streams) -> StreamLink {
    let (a, ticket) = owner.create().expect("create");
    let b = peer.open(ticket).expect("open");
    Link::new(a.split(), b.split())
}

fn duplex_link()
-> Link<tokio::io::ReadHalf<tokio::io::DuplexStream>, tokio::io::WriteHalf<tokio::io::DuplexStream>>
{
    let (a, b) = tokio::io::duplex(STREAM_BUFFER_BYTES);
    Link::new(tokio::io::split(a), tokio::io::split(b))
}

fn uds_link() -> Link<tokio::net::unix::OwnedReadHalf, tokio::net::unix::OwnedWriteHalf> {
    let (a, b) = tokio::net::UnixStream::pair().expect("socket pair");
    Link::new(a.into_split(), b.into_split())
}

/// `body` A -> B: the writer is its own task, the reader this one.
async fn one_way<R, W>(link: &mut Link<R, W>, body: &Arc<Vec<u8>>, sink: &mut [u8])
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut writer = link.a_write.take().expect("writer");
    let body = Arc::clone(body);
    let writing = tokio::spawn(async move {
        writer.write_all(&body).await.expect("write");
        writer
    });
    link.b_read
        .as_mut()
        .expect("reader")
        .read_exact(sink)
        .await
        .expect("read");
    link.a_write = Some(writing.await.expect("writer task"));
}

/// 128 bytes A -> B and 128 bytes back, B answering from its own task.
async fn ping_pong<R, W>(link: &mut Link<R, W>, ping: &[u8; PING])
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut b_read = link.b_read.take().expect("b read");
    let mut b_write = link.b_write.take().expect("b write");
    let responder = tokio::spawn(async move {
        let mut buf = [0_u8; PING];
        b_read.read_exact(&mut buf).await.expect("read ping");
        b_write.write_all(&buf).await.expect("write pong");
        (b_read, b_write)
    });
    link.a_write
        .as_mut()
        .expect("a write")
        .write_all(ping)
        .await
        .expect("write ping");
    let mut buf = [0_u8; PING];
    link.a_read
        .as_mut()
        .expect("a read")
        .read_exact(&mut buf)
        .await
        .expect("read pong");
    let (b_read, b_write) = responder.await.expect("responder");
    link.b_read = Some(b_read);
    link.b_write = Some(b_write);
}

struct Fixtures {
    memory: Streams,
    shm_owner: Streams,
    shm_peer: Streams,
}

impl Fixtures {
    fn new() -> Self {
        let memory = Streams::new(
            Arc::new(Fleet::join("bench-memory", 2).expect("memory fleet")),
            Incarnation::new(1),
        )
        .expect("memory streams");
        let name = fresh_fleet_name();
        let shm_owner = Streams::new(
            Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet")),
            Incarnation::new(10),
        )
        .expect("owner streams");
        shm_owner.reset_all();
        let shm_peer = Streams::new(
            Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet")),
            Incarnation::new(11),
        )
        .expect("peer streams");
        Self {
            memory,
            shm_owner,
            shm_peer,
        }
    }
}

impl Drop for Fixtures {
    fn drop(&mut self) {
        let _ = self.shm_owner.unlink();
    }
}

fn one_way_benches(criterion: &mut Criterion) {
    let runtime = runtime();
    let fixtures = Fixtures::new();
    let mut group = criterion.benchmark_group("one_way");
    for size in SIZES {
        let body = Arc::new((0..size).map(|i| i as u8).collect::<Vec<_>>());
        let mut sink = vec![0_u8; size];
        group.throughput(Throughput::Bytes(size as u64));

        let mut link = stream_link(&fixtures.memory, &fixtures.memory);
        group.bench_with_input(BenchmarkId::new("stream-memory", size), &size, |b, _| {
            b.iter(|| runtime.block_on(one_way(&mut link, &body, &mut sink)));
        });
        let mut link = stream_link(&fixtures.shm_owner, &fixtures.shm_peer);
        group.bench_with_input(BenchmarkId::new("stream-shm", size), &size, |b, _| {
            b.iter(|| runtime.block_on(one_way(&mut link, &body, &mut sink)));
        });
        let mut link = duplex_link();
        group.bench_with_input(BenchmarkId::new("tokio-duplex", size), &size, |b, _| {
            b.iter(|| runtime.block_on(one_way(&mut link, &body, &mut sink)));
        });
        let mut link = runtime.block_on(async { uds_link() });
        group.bench_with_input(BenchmarkId::new("unix-socket", size), &size, |b, _| {
            b.iter(|| runtime.block_on(one_way(&mut link, &body, &mut sink)));
        });
    }
    group.finish();
}

fn ping_pong_benches(criterion: &mut Criterion) {
    let runtime = runtime();
    let fixtures = Fixtures::new();
    let ping = [7_u8; PING];
    let mut group = criterion.benchmark_group("ping_pong_128");

    let mut link = stream_link(&fixtures.memory, &fixtures.memory);
    group.bench_function("stream-memory", |b| {
        b.iter(|| runtime.block_on(ping_pong(&mut link, &ping)));
    });
    let mut link = stream_link(&fixtures.shm_owner, &fixtures.shm_peer);
    group.bench_function("stream-shm", |b| {
        b.iter(|| runtime.block_on(ping_pong(&mut link, &ping)));
    });
    let mut link = duplex_link();
    group.bench_function("tokio-duplex", |b| {
        b.iter(|| runtime.block_on(ping_pong(&mut link, &ping)));
    });
    let mut link = runtime.block_on(async { uds_link() });
    group.bench_function("unix-socket", |b| {
        b.iter(|| runtime.block_on(ping_pong(&mut link, &ping)));
    });
    group.finish();
}

fn open_close_benches(criterion: &mut Criterion) {
    let fixtures = Fixtures::new();
    let mut group = criterion.benchmark_group("open_close");
    group.bench_function("stream-memory", |b| {
        b.iter(|| {
            let (a, ticket) = fixtures.memory.create().expect("create");
            let b = fixtures.memory.open(ticket).expect("open");
            drop((a, b));
        });
    });
    group.bench_function("stream-shm", |b| {
        b.iter(|| {
            let (a, ticket) = fixtures.shm_owner.create().expect("create");
            let b = fixtures.shm_peer.open(ticket).expect("open");
            drop((a, b));
        });
    });
    group.bench_function("unix-socket-pair", |b| {
        b.iter(|| drop(std::os::unix::net::UnixStream::pair().expect("pair")));
    });
    group.finish();
}

criterion_group!(
    benches,
    open_close_benches,
    ping_pong_benches,
    one_way_benches
);
criterion_main!(benches);
