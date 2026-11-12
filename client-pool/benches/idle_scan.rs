//! What one local acquire and release costs as the idle set grows.
//!
//! The pool holds `idle` warm clients. Each operation acquires one and drops
//! it, so the idle set stays the same size throughout and every acquire runs
//! with that many idle entries under the pool's mutex. `idle_timeout` is
//! either unset, or set to a bench-declared value far beyond the run so no
//! entry expires and the idle set is not changed by the measurement.
//!
//! Run: `cargo bench -p orbit-client-pool --bench idle_scan -- [ops] [samples] [idle,...]`.
use std::convert::Infallible;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use orbit_client_pool::{
    ActiveRequestPolicy, ClientManager, ClientPool, PoolOptions,
};

/// Bench setting: long enough that no idle entry expires during a run.
const IDLE_TIMEOUT: Duration = Duration::from_secs(24 * 3600);

#[derive(Clone)]
struct Manager(Arc<AtomicUsize>);

#[async_trait]
impl ClientManager for Manager {
    type Client = usize;
    type Error = Infallible;

    async fn create(&self) -> Result<Self::Client, Self::Error> {
        Ok(self.0.fetch_add(1, Ordering::Relaxed))
    }

    async fn recycle(
        &self,
        _client: &mut Self::Client,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

async fn warm(
    pool: &ClientPool<Manager>,
    idle: usize,
) {
    let mut held = Vec::with_capacity(idle);
    for _ in 0..idle {
        held.push(
            pool.acquire(ActiveRequestPolicy::Preserve).await.expect("warm"),
        );
    }
    drop(held);
}

async fn measure(
    pool: &ClientPool<Manager>,
    operations: usize,
) -> Duration {
    let started = Instant::now();
    for _ in 0..operations {
        let client =
            pool.acquire(ActiveRequestPolicy::Preserve).await.expect("acquire");
        black_box(*client);
        drop(client);
    }
    started.elapsed()
}

fn main() {
    let mut args = std::env::args().skip(1).filter(|a| a != "--bench");
    let operations: usize =
        args.next().and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
    let samples: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(7);
    let sizes: Vec<usize> = args
        .next()
        .map(|v| v.split(',').map(|s| s.parse().unwrap()).collect())
        .unwrap_or_else(|| vec![1, 16, 256, 4096]);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    println!("| idle | idle_timeout | median ns/op | min | max | creates |");
    println!("| ---: | --- | ---: | ---: | ---: | ---: |");
    runtime.block_on(async {
        for &idle in &sizes {
            for timeout in [None, Some(IDLE_TIMEOUT)] {
                let creates = Arc::new(AtomicUsize::new(0));
                let options = PoolOptions::new(0, idle, idle, idle, 0)
                    .idle_timeout(timeout);
                let pool =
                    ClientPool::new(Manager(Arc::clone(&creates)), options)
                        .expect("pool");
                warm(&pool, idle).await;
                let _ = measure(&pool, operations / 10).await;
                let mut ns = Vec::with_capacity(samples);
                for _ in 0..samples {
                    let elapsed = measure(&pool, operations).await;
                    ns.push(elapsed.as_nanos() as f64 / operations as f64);
                }
                ns.sort_by(f64::total_cmp);
                println!(
                    "| {idle} | {} | {:.1} | {:.1} | {:.1} | {} |",
                    if timeout.is_some() { "set" } else { "unset" },
                    ns[ns.len() / 2],
                    ns[0],
                    ns[ns.len() - 1],
                    creates.load(Ordering::Relaxed),
                );
            }
        }
    });
}
