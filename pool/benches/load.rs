//! The pool under contention, with the costs that criterion does not
//! show: CPU time, voluntary context switches (parks and wakes) and how
//! evenly contending tasks are served. Three shapes:
//!
//! - `local one-shot`: `claim_create` / drop per operation, the FastCGI
//!   admission shape; nothing but a CAS on the key and a wake.
//! - `local reuse`: `reserve` / `accept` / `complete` on a resource this
//!   process owns; no stream.
//! - `remote reuse`: `reserve` on the other node's resource, then the
//!   pool's own rendezvous (`open_session` / `accept_session`) carrying a
//!   small payload, the owner's task answers and completes. Both nodes in
//!   this process; a real second process adds scheduler noise this does
//!   not show.
//!
//! Run: `cargo bench -p orbit-pool --bench load -- [ops] [tasks]`.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use orbit_core::{Fleet, NodeId};
use orbit_pool::{Error, Incarnation, Key, Lease, Pool, ResourceId};
use orbit_stream::Streams;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KEY: Key = Key::new(0x10AD);

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

struct Report {
    name: &'static str,
    ops: usize,
    wall: Duration,
    cpu: Duration,
    voluntary: i64,
    involuntary: i64,
    p50: Duration,
    p99: Duration,
    max: Duration,
    shares: Vec<usize>
}

impl Report {
    fn print(&self) {
        let rate = self.ops as f64 / self.wall.as_secs_f64();
        let min_share = self.shares.iter().min().copied().unwrap_or(0);
        let max_share = self.shares.iter().max().copied().unwrap_or(0);
        println!(
            "{:<14} ops={:<8} wall={:>8.3?} ops/s={:>10.0} cpu={:>8.3?} cpu/op={:>7.2?} vcsw={:<7} vcsw/op={:<5.2} ivcsw={:<6} p50={:>8.2?} p99={:>8.2?} max={:>8.2?} share min/max={}/{}",
            self.name,
            self.ops,
            self.wall,
            rate,
            self.cpu,
            self.cpu / self.ops as u32,
            self.voluntary,
            self.voluntary as f64 / self.ops as f64,
            self.involuntary,
            self.p50,
            self.p99,
            self.max,
            min_share,
            max_share,
        );
    }
}

fn percentiles(mut samples: Vec<Duration>) -> (Duration, Duration, Duration) {
    samples.sort();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    (at(0.5), at(0.99), *samples.last().unwrap())
}

async fn run<F, Fut>(
    name: &'static str,
    ops: usize,
    tasks: usize,
    op: F
) -> Report
where
    F: Fn(usize) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Duration> + Send
{
    let before = usage();
    let start = Instant::now();
    let per_task = ops / tasks;
    let handles = (0..tasks)
        .map(|task| {
            let op = op.clone();
            tokio::spawn(async move {
                let mut samples = Vec::with_capacity(per_task);
                for _ in 0..per_task {
                    samples.push(op(task).await);
                }
                samples
            })
        })
        .collect::<Vec<_>>();
    let mut all = Vec::with_capacity(ops);
    let mut shares = Vec::with_capacity(tasks);
    for handle in handles {
        let samples = handle.await.unwrap();
        shares.push(samples.len());
        all.extend(samples);
    }
    let wall = start.elapsed();
    let after = usage();
    let (p50, p99, max) = percentiles(all);
    Report {
        name,
        ops: per_task * tasks,
        wall,
        cpu: after.cpu - before.cpu,
        voluntary: after.voluntary - before.voluntary,
        involuntary: after.involuntary - before.involuntary,
        p50,
        p99,
        max,
        shares
    }
}

/// Reserve with a bounded wait on the key: what a consumer's acquire loop
/// does when the resource is busy.
async fn reserve_waiting(
    pool: &Pool,
    id: ResourceId
) -> Lease {
    loop {
        // The version comes before the attempt, so a completion between
        // the two is seen by the wait instead of being missed.
        let since = pool.version(KEY).unwrap();
        match pool.reserve(id) {
            Ok(lease) => return lease,
            Err(Error::Busy(_)) => {
                let _ = poll_fn(|cx| pool.poll_capacity(KEY, since, cx)).await;
            }
            Err(error) => panic!("{error}")
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1).filter(|arg| !arg.starts_with("--"));
    let ops: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(20_000);
    let tasks: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(8);
    // Third argument: how many bytes one exchange carries. The ring is 64 KiB
    // a direction by default, so a payload past that makes the writer wait for
    // the reader to drain -- which is the transition worth sweeping, because a
    // relayed HTTP body is on the far side of it and a 128-byte probe is not.
    let payload: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(128);
    let capacity: u32 = 4;

    let name: &'static str = Box::leak(format!("ld{:x}", std::process::id()).into_boxed_str());
    let owner_fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).unwrap());
    let caller_fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).unwrap());
    let owner = Pool::new(Arc::clone(&owner_fleet), Incarnation::new(10)).unwrap();
    owner.reset_all();
    let caller = Pool::new(Arc::clone(&caller_fleet), Incarnation::new(11)).unwrap();
    let owner_streams = Streams::new(owner_fleet, orbit_stream::Incarnation::new(10)).unwrap();
    owner_streams.reset_all();
    let caller_streams = Streams::new(caller_fleet, orbit_stream::Incarnation::new(11)).unwrap();

    let runtime =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();

    println!(
        "orbit-pool load: ops={ops} tasks={tasks} payload={payload}B capacity={capacity} (two nodes in one process)"
    );

    runtime.block_on(async {
        // 1. Local one-shot: a fleet-wide semaphore of `capacity` claims.
        let pool = caller.clone();
        run("local one-shot", ops, tasks, move |_| {
            let pool = pool.clone();
            async move {
                let start = Instant::now();
                loop {
                    let since = pool.version(KEY).unwrap();
                    match pool.claim_create(KEY, capacity) {
                        Ok(permit) => {
                            tokio::task::yield_now().await;
                            permit.finish();
                            break;
                        }
                        Err(Error::CreationBudget { .. }) => {
                            let _ = poll_fn(|cx| pool.poll_capacity(KEY, since, cx)).await;
                        }
                        Err(error) => panic!("{error}")
                    }
                }
                start.elapsed()
            }
        })
        .await
        .print();

        // 2. Local reuse: the caller owns the resource.
        let id = caller.register(KEY, capacity).unwrap();
        let pool = caller.clone();
        run("local reuse", ops, tasks, move |_| {
            let pool = pool.clone();
            async move {
                let start = Instant::now();
                let lease = reserve_waiting(&pool, id).await;
                let execution = pool.accept(lease).unwrap();
                tokio::task::yield_now().await;
                execution.complete();
                start.elapsed()
            }
        })
        .await
        .print();
        caller.unregister(id).unwrap();

        // 3. Remote reuse: the owner node serves over streams.
        let id = owner.register(KEY, capacity).unwrap();
        let served = Arc::new(AtomicU64::new(0));
        let serving = {
            let owner = owner.clone();
            let owner_streams = owner_streams.clone();
            let served = Arc::clone(&served);
            let bytes = payload;
            tokio::spawn(async move {
                loop {
                    let ticket = poll_fn(|cx| owner_streams.poll_take_offer(cx)).await.unwrap();
                    // The lease is read and accepted exactly once here,
                    // before any of this bench's own bytes.
                    let Ok((execution, mut read, mut write)) =
                        owner.accept_session(&owner_streams, ticket)
                    else {
                        continue;
                    };
                    let served = Arc::clone(&served);
                    tokio::spawn(async move {
                        let mut payload = vec![0_u8; bytes];
                        // Every exchange on this session, not only the first:
                        // one serving task then covers both the borrow-per-
                        // request shape and the held-session one, and the
                        // difference between them is the rendezvous.
                        while read.read_exact(&mut payload).await.is_ok() {
                            if write.write_all(&payload).await.is_err() {
                                break;
                            }
                            served.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = write.shutdown().await;
                        execution.complete();
                    });
                }
            })
        };
        let pool = caller.clone();
        let streams = caller_streams.clone();
        let bytes = payload;
        run("remote reuse", ops, tasks, move |_| {
            let pool = pool.clone();
            let streams = streams.clone();
            async move {
                let start = Instant::now();
                let lease = reserve_waiting(&pool, id).await;
                let (mut read, mut write) = pool.open_session(lease, &streams).unwrap();
                write.write_all(&vec![7_u8; bytes]).await.unwrap();
                write.shutdown().await.unwrap();
                let mut reply = vec![0_u8; bytes];
                read.read_exact(&mut reply).await.unwrap();
                start.elapsed()
            }
        })
        .await
        .print();

        // 4. The same borrow, used more than once. `remote reuse` pays a
        // reserve and a rendezvous for every request, because an exclusive
        // per-request lease is the shape upstream settled on. Holding the
        // session over EXCHANGES round trips and dividing by them leaves the
        // transport with a share of the setup instead of all of it, so the
        // gap between the two lines is what entering and leaving a borrow
        // costs -- the number the break-even arithmetic needs separated.
        const EXCHANGES: usize = 8;
        let pool = caller.clone();
        let streams = caller_streams.clone();
        let bytes = payload;
        run("remote reuse x8", ops / EXCHANGES, tasks, move |_| {
            let pool = pool.clone();
            let streams = streams.clone();
            async move {
                let start = Instant::now();
                let lease = reserve_waiting(&pool, id).await;
                let (mut read, mut write) = pool.open_session(lease, &streams).unwrap();
                let mut reply = vec![0_u8; bytes];
                let out = vec![7_u8; bytes];
                for _ in 0..EXCHANGES {
                    write.write_all(&out).await.unwrap();
                    read.read_exact(&mut reply).await.unwrap();
                }
                write.shutdown().await.unwrap();
                start.elapsed() / EXCHANGES as u32
            }
        })
        .await
        .print();
        serving.abort();
        println!("remote served={}", served.load(Ordering::Relaxed));
        owner.unregister(id).unwrap();
    });

    let _ = owner_streams.unlink();
    let _ = owner.unlink();
}
