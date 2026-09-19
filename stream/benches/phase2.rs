//! The paired exchange model against a Unix socket using the same application
//! chunk decisions. Payload slots are fixed physical units; the benchmarked
//! chunk sizes are publication decisions spanning many slots.
//!
//! Run: `cargo bench -p orbit-stream --features tokio --bench phase2`

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::future::poll_fn;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use orbit_core::{Fleet, NodeId};
use orbit_stream::exchange::{
    ExchangeSpec, Exchanges, FlowEvent, PayloadArenaSpec, Receiver, Sender
};
use orbit_stream::{Error, Incarnation, Result, StreamSpec};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::runtime::Runtime;

const FLEET_CAPACITY: u16 = 2;
const CONTROL: StreamSpec = StreamSpec::new(235, 32, 4 * 1024);
const REQUEST_PAYLOAD: PayloadArenaSpec = PayloadArenaSpec::new(236, 65_536, 256);
const CONCURRENCY: [usize; 2] = [1, 8];
const BODY_SIZES: [usize; 2] = [64 * 1024, 1024 * 1024];
const CHUNK_SIZES: [usize; 2] = [4 * 1024, 64 * 1024];

fn fresh_name() -> &'static str {
    let pid = std::process::id() & 0xffff;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").subsec_nanos();
    Box::leak(format!("e{pid:04x}{nonce:08x}").into_boxed_str())
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
}

struct Fixtures {
    owner: Exchanges,
    peer: Exchanges
}

impl Fixtures {
    fn new() -> Self {
        let name = fresh_name();
        let spec = ExchangeSpec::new(CONTROL, REQUEST_PAYLOAD);
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

impl Drop for Fixtures {
    fn drop(&mut self) {
        let _ = self.owner.unlink();
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

producer!(Sender);

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

consumer!(Receiver);

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
    chunk_size: usize
) {
    let (server, ticket) = owner.create().expect("server");
    let client = peer.open_peer(ticket).expect("client");
    let (mut request_out, mut response_in) = server.split();
    let (mut response_out, mut request_in) = client.split();
    let request = async {
        send_body(&mut request_out, &body, chunk_size).await;
        receive_body(&mut response_in, body.len()).await;
    };
    let response = async {
        receive_body(&mut request_in, body.len()).await;
        send_body(&mut response_out, &body, chunk_size).await;
    };
    tokio::join!(request, response);
}

async fn exchange_early_duplex(
    owner: Exchanges,
    peer: Exchanges,
    body: Arc<Vec<u8>>,
    chunk_size: usize
) {
    let (server, ticket) = owner.create().expect("server");
    let client = peer.open_peer(ticket).expect("client");
    let (mut request_out, mut response_in) = server.split();
    let (mut response_out, mut request_in) = client.split();
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
}

async fn socket_write_chunks(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    body: &[u8],
    chunk_size: usize
) {
    for chunk in body.chunks(chunk_size) {
        writer.write_all(chunk).await.expect("socket write");
    }
}

async fn socket_round_trip(
    body: Arc<Vec<u8>>,
    chunk_size: usize,
    early: bool
) {
    let (a, b) = UnixStream::pair().expect("socket pair");
    let (mut a_read, mut a_write) = a.into_split();
    let (mut b_read, mut b_write) = b.into_split();
    let mut at_a = vec![0_u8; body.len()];
    let mut at_b = vec![0_u8; body.len()];
    if early {
        let (_, _, a_read_result, b_read_result) = tokio::join!(
            socket_write_chunks(&mut a_write, &body, chunk_size),
            socket_write_chunks(&mut b_write, &body, chunk_size),
            a_read.read_exact(&mut at_a),
            b_read.read_exact(&mut at_b),
        );
        a_read_result.expect("socket response");
        b_read_result.expect("socket request");
    } else {
        let (_, read) = tokio::join!(
            socket_write_chunks(&mut a_write, &body, chunk_size),
            b_read.read_exact(&mut at_b),
        );
        read.expect("socket request");
        let (_, read) = tokio::join!(
            socket_write_chunks(&mut b_write, &body, chunk_size),
            a_read.read_exact(&mut at_a),
        );
        read.expect("socket response");
    }
}

async fn concurrent_exchange(
    fixtures: &Fixtures,
    body: Arc<Vec<u8>>,
    chunk_size: usize,
    concurrency: usize,
    early: bool
) {
    let mut tasks = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let owner = fixtures.owner.clone();
        let peer = fixtures.peer.clone();
        let body = Arc::clone(&body);
        tasks.push(tokio::spawn(async move {
            if early {
                exchange_early_duplex(owner, peer, body, chunk_size).await;
            } else {
                exchange_round_trip(owner, peer, body, chunk_size).await;
            }
        }));
    }
    for task in tasks {
        task.await.expect("exchange task");
    }
}

async fn concurrent_sockets(
    body: Arc<Vec<u8>>,
    chunk_size: usize,
    concurrency: usize,
    early: bool
) {
    let mut tasks = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let body = Arc::clone(&body);
        tasks.push(tokio::spawn(async move {
            socket_round_trip(body, chunk_size, early).await;
        }));
    }
    for task in tasks {
        task.await.expect("socket task");
    }
}

fn round_trip_benches(criterion: &mut Criterion) {
    let runtime = runtime();
    let fixtures = Fixtures::new();
    let mut group = criterion.benchmark_group("phase2_exchange_round_trip");
    group.sample_size(20);
    for concurrency in CONCURRENCY {
        for body_size in BODY_SIZES {
            for chunk_size in CHUNK_SIZES {
                let body = Arc::new((0..body_size).map(|index| index as u8).collect::<Vec<_>>());
                group.throughput(Throughput::Bytes((body_size * concurrency * 2) as u64));
                let label = format!("c{concurrency}-chunk{chunk_size}");
                group.bench_with_input(
                    BenchmarkId::new(format!("exchange-slot256-{label}"), body_size),
                    &body_size,
                    |bencher, _| {
                        bencher.iter(|| {
                            runtime.block_on(concurrent_exchange(
                                &fixtures,
                                Arc::clone(&body),
                                chunk_size,
                                concurrency,
                                false
                            ))
                        })
                    }
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("unix-socket-{label}"), body_size),
                    &body_size,
                    |bencher, _| {
                        bencher.iter(|| {
                            runtime.block_on(concurrent_sockets(
                                Arc::clone(&body),
                                chunk_size,
                                concurrency,
                                false
                            ))
                        })
                    }
                );
            }
        }
    }
    group.finish();
}

fn early_duplex_benches(criterion: &mut Criterion) {
    let runtime = runtime();
    let fixtures = Fixtures::new();
    let body_size = 1024 * 1024;
    let chunk_size = 64 * 1024;
    let body = Arc::new((0..body_size).map(|index| index as u8).collect::<Vec<_>>());
    let mut group = criterion.benchmark_group("phase2_early_duplex");
    group.sample_size(20);
    for concurrency in CONCURRENCY {
        group.throughput(Throughput::Bytes((body_size * concurrency * 2) as u64));
        group.bench_function(format!("exchange-slot256-chunk65536-c{concurrency}"), |bencher| {
            bencher.iter(|| {
                runtime.block_on(concurrent_exchange(
                    &fixtures,
                    Arc::clone(&body),
                    chunk_size,
                    concurrency,
                    true
                ))
            })
        });
        group.bench_function(format!("unix-socket-chunk65536-c{concurrency}"), |bencher| {
            bencher.iter(|| {
                runtime.block_on(concurrent_sockets(
                    Arc::clone(&body),
                    chunk_size,
                    concurrency,
                    true
                ))
            })
        });
    }
    group.finish();
}

criterion_group!(benches, round_trip_benches, early_duplex_benches);
criterion_main!(benches);
