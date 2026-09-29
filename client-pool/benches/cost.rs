//! Cost of making an already-warm local client fleet-aware.
//!
//! Both paths reuse one process-local no-op client. The fleet case additionally
//! performs the Orbit policy decision, reservation, execution and availability
//! publication, but it never opens a remote exchange. This isolates the price
//! paid even when fleet ownership finds no capacity elsewhere.
//!
//! Run: `cargo bench -p orbit-client-pool --features fleet --bench cost -- [ops] [samples]`.

use std::convert::Infallible;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use orbit_client_pool::{
    ActiveRequestPolicy, ClientManager, ClientPool, FleetClient, FleetClientPool, FleetPoolOptions,
    PoolOptions
};
use orbit_core::Fleet;
use orbit_pool::{Incarnation as PoolIncarnation, Key, LocalFirst, Pool, PoolSpec};
use orbit_stream::exchange::{ExchangeSpec, Exchanges, PayloadArenaSpec};
use orbit_stream::{Incarnation as StreamIncarnation, StreamSpec};

const POOL_KIND: u8 = 11;
const CONTROL_KIND: u8 = 12;
const PAYLOAD_KIND: u8 = 13;
const KEY: Key = Key::new(1);

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
        _client: &mut Self::Client
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct Measurements {
    samples: Vec<Duration>,
    operations: usize
}

impl Measurements {
    fn new(operations: usize) -> Self {
        Self { samples: Vec::new(), operations }
    }

    fn push(
        &mut self,
        elapsed: Duration
    ) {
        self.samples.push(elapsed);
    }

    fn median(&self) -> Duration {
        let mut samples = self.samples.clone();
        samples.sort_unstable();
        samples[samples.len() / 2]
    }

    fn report(
        &self,
        name: &str
    ) {
        let elapsed = self.median();
        let min = self.samples.iter().min().expect("at least one sample");
        let max = self.samples.iter().max().expect("at least one sample");
        println!(
            "{name:<20} median={elapsed:>9.3?} min={min:>9.3?} max={max:>9.3?} ns/op={:>9.1} ops/s={:>12.0}",
            elapsed.as_secs_f64() * 1_000_000_000.0 / self.operations as f64,
            self.operations as f64 / elapsed.as_secs_f64()
        );
    }
}

fn local_options() -> PoolOptions {
    PoolOptions::new(0, 1, 1, 1, 0)
}

fn fleet_options() -> FleetPoolOptions {
    FleetPoolOptions::new(0, 1, 1, 1, 0, 1)
}

fn fleet_pool(manager: Manager) -> (FleetClientPool<Manager>, Arc<Pool>, Arc<Exchanges>) {
    let fleet_name = format!("cc{:x}", std::process::id());
    let fleet = Arc::new(Fleet::join_shm(&fleet_name, 1).expect("fleet"));
    let pool = Arc::new(
        Pool::with_spec(
            Arc::clone(&fleet),
            PoolIncarnation::new(1),
            PoolSpec::new(POOL_KIND, 4, 4).with_fleet_availability().with_fleet_concurrency()
        )
        .expect("pool")
    );
    let exchanges = Arc::new(
        Exchanges::open(
            fleet,
            StreamIncarnation::new(1),
            ExchangeSpec::new(
                StreamSpec::new(CONTROL_KIND, 4, 4_096),
                PayloadArenaSpec::new(PAYLOAD_KIND, 4, 4_096)
            )
        )
        .expect("exchanges")
    );
    let clients = FleetClientPool::new(
        manager,
        Arc::clone(&pool),
        Arc::clone(&exchanges),
        KEY,
        Arc::new(LocalFirst),
        fleet_options()
    )
    .expect("fleet client pool");
    (clients, pool, exchanges)
}

async fn measure_local(
    pool: &ClientPool<Manager>,
    operations: usize
) -> Duration {
    let started = Instant::now();
    for _ in 0..operations {
        let client = pool.acquire(ActiveRequestPolicy::Preserve).await.expect("local acquire");
        black_box(*client);
        drop(client);
    }
    started.elapsed()
}

async fn measure_fleet(
    pool: &FleetClientPool<Manager>,
    operations: usize
) -> Duration {
    let started = Instant::now();
    for _ in 0..operations {
        let FleetClient::Local(client) =
            pool.acquire(&[], ActiveRequestPolicy::Preserve).await.expect("fleet acquire")
        else {
            panic!("one-node benchmark unexpectedly selected a remote client");
        };
        black_box(*client);
        drop(client);
    }
    started.elapsed()
}

fn main() {
    let operations =
        std::env::args().nth(1).and_then(|value| value.parse().ok()).unwrap_or(1_000_000);
    let samples = std::env::args().nth(2).and_then(|value| value.parse().ok()).unwrap_or(7);
    assert!(operations > 0, "operation count must be greater than zero");
    assert!(samples > 0, "sample count must be greater than zero");

    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let local_creates = Arc::new(AtomicUsize::new(0));
        let local = ClientPool::new(Manager(Arc::clone(&local_creates)), local_options())
            .expect("local client pool");
        drop(local.acquire(ActiveRequestPolicy::Preserve).await.expect("warm local client"));

        let fleet_creates = Arc::new(AtomicUsize::new(0));
        let (fleet, orbit_pool, exchanges) = fleet_pool(Manager(Arc::clone(&fleet_creates)));
        drop(
            fleet
                .acquire(&[], ActiveRequestPolicy::Preserve)
                .await
                .expect("warm fleet client")
        );

        // Touch both paths once more after setup so lazy runtime work is not
        // charged to whichever measurement happens to run first.
        drop(local.acquire(ActiveRequestPolicy::Preserve).await.expect("prepared local client"));
        drop(
            fleet
                .acquire(&[], ActiveRequestPolicy::Preserve)
                .await
                .expect("prepared fleet client")
        );

        let mut local_result = Measurements::new(operations);
        let mut fleet_result = Measurements::new(operations);
        for sample in 0..samples {
            if sample % 2 == 0 {
                local_result.push(measure_local(&local, operations).await);
                fleet_result.push(measure_fleet(&fleet, operations).await);
            } else {
                fleet_result.push(measure_fleet(&fleet, operations).await);
                local_result.push(measure_local(&local, operations).await);
            }
        }

        assert_eq!(local_creates.load(Ordering::Relaxed), 1);
        assert_eq!(fleet_creates.load(Ordering::Relaxed), 1);
        println!(
            "orbit-client-pool warm local acquisition: operations/sample={operations} samples={samples}"
        );
        local_result.report("local");
        fleet_result.report("fleet-local");
        println!(
            "fleet/local cost ratio={:.3}x",
            fleet_result.median().as_secs_f64() / local_result.median().as_secs_f64()
        );

        drop(fleet);
        exchanges.unlink().expect("unlink exchange benchmark segments");
        orbit_pool.unlink().expect("unlink pool benchmark segment");
    });
}
