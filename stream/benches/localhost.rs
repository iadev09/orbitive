//! Paired exchanges against Unix sockets and localhost TCP with a
//! production-shaped size matrix. This is deliberately separate from the
//! stable `phase2` benchmark.
//!
//! The listener and SHM tables live for the whole benchmark. Each iteration
//! opens a fresh logical exchange and Unix socket pair. Localhost TCP is fresh
//! per iteration on Linux/FreeBSD; macOS reuses a pool because its ephemeral
//! port range is smaller than Criterion's warm-up connection count. Socket
//! payload boundaries are known by the scenario, so the socket baselines do
//! not pay for an application framing protocol.
//!
//! Run: `cargo bench -p orbit-stream --features tokio --bench localhost`

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::future::poll_fn;
use std::sync::Arc;
#[cfg(target_os = "macos")]
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use orbit_core::{Fleet, NodeId};
use orbit_stream::exchange::{
    ExchangeSpec, Exchanges, FlowEvent, PayloadArenaSpec, RequestConsumer, RequestProducer,
    ResponseConsumer, ResponseProducer
};
use orbit_stream::{Error, Incarnation, Result, StreamSpec};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::runtime::Runtime;

const FLEET_CAPACITY: u16 = 2;
const RUNTIME_THREADS: usize = 8;
const CONTROL: StreamSpec = StreamSpec::new(238, 32, 4 * 1024);
const REQUEST_PAYLOAD: PayloadArenaSpec = PayloadArenaSpec::new(239, 4_096, 4 * 1024);
const RESPONSE_PAYLOAD: PayloadArenaSpec = PayloadArenaSpec::new(240, 4_096, 4 * 1024);
const CONCURRENCY: [usize; 2] = [1, 16];

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    body_size: usize,
    chunk_size: usize
}

const SMALL: Scenario = Scenario { name: "small", body_size: 16 * 1024, chunk_size: 16 * 1024 };
const MEDIUM: Scenario = Scenario { name: "medium", body_size: 256 * 1024, chunk_size: 64 * 1024 };
const STREAM: Scenario =
    Scenario { name: "stream", body_size: 8 * 1024 * 1024, chunk_size: 256 * 1024 };
const LARGE: Scenario =
    Scenario { name: "large", body_size: 64 * 1024 * 1024, chunk_size: 256 * 1024 };
const ROUND_TRIP_SCENARIOS: [Scenario; 4] = [SMALL, MEDIUM, STREAM, LARGE];
const DUPLEX_SCENARIOS: [Scenario; 2] = [MEDIUM, STREAM];

fn fresh_name() -> &'static str {
    let pid = std::process::id() & 0xffff;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").subsec_nanos();
    Box::leak(format!("l{pid:04x}{nonce:08x}").into_boxed_str())
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(RUNTIME_THREADS)
        .enable_all()
        .build()
        .expect("runtime")
}

struct ExchangeFixtures {
    owner: Exchanges,
    peer: Exchanges
}

impl ExchangeFixtures {
    fn new() -> Self {
        let name = fresh_name();
        let spec = ExchangeSpec::new(CONTROL, REQUEST_PAYLOAD, RESPONSE_PAYLOAD);
        let owner = Exchanges::open(
            Arc::new(Fleet::join_shm_as(name, FLEET_CAPACITY, NodeId::ZERO).expect("owner fleet")),
            Incarnation::new(1),
            spec
        )
        .expect("owner exchanges");
        owner.reset_all();
        let peer = Exchanges::open(
            Arc::new(Fleet::join_shm_as(name, FLEET_CAPACITY, NodeId::new(1)).expect("peer fleet")),
            Incarnation::new(2),
            spec
        )
        .expect("peer exchanges");
        Self { owner, peer }
    }
}

impl Drop for ExchangeFixtures {
    fn drop(&mut self) {
        let _ = self.owner.unlink();
    }
}

struct TcpFixtures {
    listener: Arc<TcpListener>,
    address: std::net::SocketAddr
}

impl TcpFixtures {
    async fn new() -> Self {
        let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.expect("TCP listener"));
        let address = listener.local_addr().expect("TCP listener address");
        Self { listener, address }
    }

    async fn pair(&self) -> (TcpStream, TcpStream) {
        // A listening socket can complete the local connect before userspace
        // accepts it. Keeping these operations sequential also avoids losing
        // an accept-readiness transition across repeated runtime entries on
        // macOS/kqueue.
        let client = TcpStream::connect(self.address).await.expect("localhost TCP connect");
        let (server, _) = self.listener.accept().await.expect("localhost TCP accept");
        client.set_nodelay(true).expect("client TCP_NODELAY");
        server.set_nodelay(true).expect("server TCP_NODELAY");
        (client, server)
    }
}

#[cfg(target_os = "macos")]
struct TcpPool {
    pairs: Vec<Arc<Mutex<Option<(TcpStream, TcpStream)>>>>
}

#[cfg(target_os = "macos")]
impl TcpPool {
    async fn new(
        fixtures: &TcpFixtures,
        capacity: usize
    ) -> Self {
        let mut pairs = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            pairs.push(Arc::new(Mutex::new(Some(fixtures.pair().await))));
        }
        Self { pairs }
    }
}

trait Producer {
    fn poll_ready(
        &self,
        payload_len: usize,
        cx: &mut Context<'_>
    ) -> Poll<Result<()>>;
    fn start(&mut self) -> Result<()>;
    fn data(
        &mut self,
        payload: &[u8]
    ) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

macro_rules! producer {
    ($type:ty) => {
        impl Producer for $type {
            fn poll_ready(
                &self,
                payload_len: usize,
                cx: &mut Context<'_>
            ) -> Poll<Result<()>> {
                self.poll_ready(payload_len, cx)
            }

            fn start(&mut self) -> Result<()> {
                self.start(None)
            }

            fn data(
                &mut self,
                payload: &[u8]
            ) -> Result<()> {
                self.data(payload).map(|_| ())
            }

            fn finish(&mut self) -> Result<()> {
                self.finish()
            }
        }
    };
}

producer!(RequestProducer);
producer!(ResponseProducer);

trait Consumer {
    fn poll_readable(
        &self,
        cx: &mut Context<'_>
    ) -> Poll<Result<()>>;
    fn try_next(&mut self) -> Result<FlowEvent>;
}

macro_rules! consumer {
    ($type:ty) => {
        impl Consumer for $type {
            fn poll_readable(
                &self,
                cx: &mut Context<'_>
            ) -> Poll<Result<()>> {
                self.poll_readable(cx)
            }

            fn try_next(&mut self) -> Result<FlowEvent> {
                self.try_next()
            }
        }
    };
}

consumer!(RequestConsumer);
consumer!(ResponseConsumer);

async fn ready<P: Producer>(
    producer: &P,
    payload_len: usize
) {
    poll_fn(|cx| producer.poll_ready(payload_len, cx)).await.expect("producer readiness");
}

async fn send_body<P: Producer>(
    producer: &mut P,
    body: &[u8],
    chunk_size: usize
) {
    loop {
        ready(producer, 0).await;
        match producer.start() {
            Ok(()) => break,
            Err(Error::WouldBlock) => continue,
            Err(error) => panic!("start: {error}")
        }
    }
    for chunk in body.chunks(chunk_size) {
        loop {
            ready(producer, chunk.len()).await;
            match producer.data(chunk) {
                Ok(()) => break,
                Err(Error::WouldBlock | Error::PayloadFull { .. }) => continue,
                Err(error) => panic!("data: {error}")
            }
        }
    }
    loop {
        ready(producer, 0).await;
        match producer.finish() {
            Ok(()) => break,
            Err(Error::WouldBlock) => continue,
            Err(error) => panic!("finish: {error}")
        }
    }
}

async fn next<C: Consumer>(consumer: &mut C) -> FlowEvent {
    loop {
        poll_fn(|cx| consumer.poll_readable(cx)).await.expect("consumer readiness");
        match consumer.try_next() {
            Ok(event) => return event,
            Err(Error::WouldBlock) => continue,
            Err(error) => panic!("next event: {error}")
        }
    }
}

async fn receive_after_start<C: Consumer>(
    consumer: &mut C,
    expected: usize
) {
    let mut received = 0;
    loop {
        match next(consumer).await {
            FlowEvent::Data(chunk) => received += chunk.len(),
            FlowEvent::Fin => break,
            FlowEvent::Reset(code) => panic!("reset {}", code.get()),
            FlowEvent::Start { .. } => panic!("duplicate start")
        }
    }
    assert_eq!(received, expected);
}

async fn receive_body<C: Consumer>(
    consumer: &mut C,
    expected: usize
) {
    assert!(matches!(next(consumer).await, FlowEvent::Start { .. }));
    receive_after_start(consumer, expected).await;
}

async fn exchange_round_trip(
    owner: Exchanges,
    peer: Exchanges,
    body: Arc<Vec<u8>>,
    chunk_size: usize,
    duplex: bool
) {
    let (server, ticket) = owner.create().expect("server exchange");
    let client = peer.open_client(ticket).expect("client exchange");
    let (mut request_out, mut response_in) = server.split();
    let (mut request_in, mut response_out) = client.split();
    if duplex {
        let w1 = async {
            tokio::join!(
                send_body(&mut request_out, &body, chunk_size),
                receive_body(&mut response_in, body.len()),
            );
        };
        let w2 = async {
            assert!(matches!(next(&mut request_in).await, FlowEvent::Start { .. }));
            tokio::join!(
                receive_after_start(&mut request_in, body.len()),
                send_body(&mut response_out, &body, chunk_size),
            );
        };
        tokio::join!(w1, w2);
    } else {
        let w1 = async {
            send_body(&mut request_out, &body, chunk_size).await;
            receive_body(&mut response_in, body.len()).await;
        };
        let w2 = async {
            receive_body(&mut request_in, body.len()).await;
            send_body(&mut response_out, &body, chunk_size).await;
        };
        tokio::join!(w1, w2);
    }
}

async fn write_chunks<W: AsyncWrite + Unpin>(
    writer: &mut W,
    body: &[u8],
    chunk_size: usize
) {
    for chunk in body.chunks(chunk_size) {
        writer.write_all(chunk).await.expect("socket write");
    }
}

async fn socket_round_trip<S>(
    a: &mut S,
    b: &mut S,
    body: Arc<Vec<u8>>,
    chunk_size: usize,
    duplex: bool
) where
    S: AsyncRead + AsyncWrite + Unpin
{
    let (mut a_read, mut a_write) = tokio::io::split(a);
    let (mut b_read, mut b_write) = tokio::io::split(b);
    let mut at_a = vec![0_u8; body.len()];
    let mut at_b = vec![0_u8; body.len()];
    if duplex {
        let (_, _, a_read_result, b_read_result) = tokio::join!(
            write_chunks(&mut a_write, &body, chunk_size),
            write_chunks(&mut b_write, &body, chunk_size),
            a_read.read_exact(&mut at_a),
            b_read.read_exact(&mut at_b),
        );
        a_read_result.expect("socket response");
        b_read_result.expect("socket request");
    } else {
        let (_, read) = tokio::join!(
            write_chunks(&mut a_write, &body, chunk_size),
            b_read.read_exact(&mut at_b),
        );
        read.expect("socket request");
        let (_, read) = tokio::join!(
            write_chunks(&mut b_write, &body, chunk_size),
            a_read.read_exact(&mut at_a),
        );
        read.expect("socket response");
    }
}

async fn concurrent_exchange(
    fixtures: &ExchangeFixtures,
    body: Arc<Vec<u8>>,
    chunk_size: usize,
    concurrency: usize,
    duplex: bool
) {
    let mut tasks = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let owner = fixtures.owner.clone();
        let peer = fixtures.peer.clone();
        let body = Arc::clone(&body);
        tasks.push(tokio::spawn(async move {
            exchange_round_trip(owner, peer, body, chunk_size, duplex).await;
        }));
    }
    for task in tasks {
        task.await.expect("exchange task");
    }
}

async fn concurrent_unix(
    body: Arc<Vec<u8>>,
    chunk_size: usize,
    concurrency: usize,
    duplex: bool
) {
    let mut tasks = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let body = Arc::clone(&body);
        tasks.push(tokio::spawn(async move {
            let (mut a, mut b) = UnixStream::pair().expect("Unix socket pair");
            socket_round_trip(&mut a, &mut b, body, chunk_size, duplex).await;
        }));
    }
    for task in tasks {
        task.await.expect("Unix socket task");
    }
}

#[cfg(not(target_os = "macos"))]
async fn concurrent_tcp(
    fixtures: &TcpFixtures,
    body: Arc<Vec<u8>>,
    chunk_size: usize,
    concurrency: usize,
    duplex: bool
) {
    let mut tasks = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let body = Arc::clone(&body);
        let listener = Arc::clone(&fixtures.listener);
        let address = fixtures.address;
        tasks.push(tokio::spawn(async move {
            let local = TcpFixtures { listener, address };
            let (mut a, mut b) = local.pair().await;
            socket_round_trip(&mut a, &mut b, body, chunk_size, duplex).await;
        }));
    }
    for task in tasks {
        task.await.expect("localhost TCP task");
    }
}

#[cfg(target_os = "macos")]
async fn concurrent_tcp_persistent(
    pool: &TcpPool,
    body: Arc<Vec<u8>>,
    chunk_size: usize,
    duplex: bool
) {
    let mut tasks = Vec::with_capacity(pool.pairs.len());
    for cell in &pool.pairs {
        let body = Arc::clone(&body);
        let cell = Arc::clone(cell);
        tasks.push(tokio::spawn(async move {
            let (mut a, mut b) = cell.lock().expect("TCP pair lock").take().expect("TCP pair");
            socket_round_trip(&mut a, &mut b, body, chunk_size, duplex).await;
            cell.lock().expect("TCP pair lock").replace((a, b));
        }));
    }
    for task in tasks {
        task.await.expect("persistent localhost TCP task");
    }
}

fn bench_matrix(
    criterion: &mut Criterion,
    group_name: &str,
    scenarios: &[Scenario],
    duplex: bool
) {
    let runtime = runtime();
    let exchanges = ExchangeFixtures::new();
    let tcp = runtime.block_on(TcpFixtures::new());
    let mut group = criterion.benchmark_group(group_name);
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));
    for scenario in scenarios {
        let concurrencies: &[usize] =
            if scenario.body_size == LARGE.body_size { &[1] } else { &CONCURRENCY };
        for &concurrency in concurrencies {
            let body = Arc::new(
                (0..scenario.body_size)
                    .map(|index| index.wrapping_mul(31) as u8)
                    .collect::<Vec<_>>()
            );
            group.throughput(Throughput::Bytes((scenario.body_size * concurrency * 2) as u64));
            let geometry = format!(
                "{}-body{}-chunk{}-c{}",
                scenario.name, scenario.body_size, scenario.chunk_size, concurrency
            );
            group.bench_with_input(
                BenchmarkId::new(format!("exchange-slot4096-t{RUNTIME_THREADS}"), &geometry),
                &scenario.body_size,
                |bencher, _| {
                    bencher.iter(|| {
                        runtime.block_on(concurrent_exchange(
                            &exchanges,
                            Arc::clone(&body),
                            scenario.chunk_size,
                            concurrency,
                            duplex
                        ))
                    });
                }
            );
            group.bench_with_input(
                BenchmarkId::new(format!("unix-socket-t{RUNTIME_THREADS}"), &geometry),
                &scenario.body_size,
                |bencher, _| {
                    bencher.iter(|| {
                        runtime.block_on(concurrent_unix(
                            Arc::clone(&body),
                            scenario.chunk_size,
                            concurrency,
                            duplex
                        ))
                    });
                }
            );
            #[cfg(not(target_os = "macos"))]
            group.bench_with_input(
                BenchmarkId::new(format!("localhost-tcp-t{RUNTIME_THREADS}"), &geometry),
                &scenario.body_size,
                |bencher, _| {
                    bencher.iter(|| {
                        runtime.block_on(concurrent_tcp(
                            &tcp,
                            Arc::clone(&body),
                            scenario.chunk_size,
                            concurrency,
                            duplex
                        ))
                    });
                }
            );
            #[cfg(target_os = "macos")]
            {
                let tcp_pool = runtime.block_on(TcpPool::new(&tcp, concurrency));
                group.bench_with_input(
                    BenchmarkId::new(
                        format!("localhost-tcp-persistent-t{RUNTIME_THREADS}"),
                        &geometry
                    ),
                    &scenario.body_size,
                    |bencher, _| {
                        bencher.iter(|| {
                            runtime.block_on(concurrent_tcp_persistent(
                                &tcp_pool,
                                Arc::clone(&body),
                                scenario.chunk_size,
                                duplex
                            ))
                        });
                    }
                );
            }
        }
    }
    group.finish();
}

fn round_trip_benches(criterion: &mut Criterion) {
    bench_matrix(criterion, "localhost_transport_round_trip", &ROUND_TRIP_SCENARIOS, false);
}

fn duplex_benches(criterion: &mut Criterion) {
    bench_matrix(criterion, "localhost_transport_duplex", &DUPLEX_SCENARIOS, true);
}

criterion_group!(benches, round_trip_benches, duplex_benches);
criterion_main!(benches);
