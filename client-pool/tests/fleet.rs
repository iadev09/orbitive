use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use orbit_core::Fleet;
use orbit_pool::{Incarnation as PoolIncarnation, Key, LocalFirst, Pool, PoolSpec};
use orbit_stream::exchange::{ExchangeSpec, Exchanges, PayloadArenaSpec};
use orbit_stream::{Incarnation as StreamIncarnation, StreamSpec};

use super::*;

const POOL_KIND: u8 = 11;
const CONTROL_KIND: u8 = 12;
const PAYLOAD_KIND: u8 = 13;

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

fn options() -> FleetPoolOptions {
    FleetPoolOptions::new(0, 1, 1, 1, 0, 1)
}

fn pool_with_options(
    manager: Manager,
    options: FleetPoolOptions
) -> FleetClientPool<Manager> {
    let fleet = Arc::new(Fleet::join("client-pool-test", 1).expect("fleet"));
    let shared = Arc::new(
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
    FleetClientPool::new(manager, shared, exchanges, Key::new(1), Arc::new(LocalFirst), options)
        .expect("client pool")
}

fn pool(manager: Manager) -> FleetClientPool<Manager> {
    pool_with_options(manager, options())
}

#[tokio::test]
async fn returned_local_client_is_published_and_reused() {
    let creates = Arc::new(AtomicUsize::new(0));
    let pool = pool(Manager(Arc::clone(&creates)));

    let FleetClient::Local(first) =
        pool.acquire(b"", ActiveRequestPolicy::Preserve).await.expect("first client")
    else {
        panic!("single-node fleet must select its local client");
    };
    assert_eq!(*first, 0);
    drop(first);

    let FleetClient::Local(second) =
        pool.acquire(b"", ActiveRequestPolicy::Preserve).await.expect("reused client")
    else {
        panic!("single-node fleet must reuse its local client");
    };
    assert_eq!(*second, 0);
    assert_eq!(creates.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn fleet_concurrency_limits_requests_independently_of_live_clients() {
    let pool = pool_with_options(
        Manager(Arc::new(AtomicUsize::new(0))),
        FleetPoolOptions::new(0, 2, 2, 1, 0, 1)
    );
    let first = pool.acquire(b"", ActiveRequestPolicy::Preserve).await.expect("first request");

    assert!(matches!(
        pool.acquire(b"", ActiveRequestPolicy::Preserve).await,
        Err(FleetAcquireError::QueueFull { max_waiting: 0 })
    ));

    drop(first);
    assert!(pool.acquire(b"", ActiveRequestPolicy::Preserve).await.is_ok());
}

#[tokio::test]
async fn warm_floor_uses_the_fleet_claim_once() {
    let creates = Arc::new(AtomicUsize::new(0));
    let client_pool =
        pool_with_options(Manager(Arc::clone(&creates)), FleetPoolOptions::new(1, 1, 1, 1, 0, 1));

    let cancellation = tokio_util::sync::CancellationToken::new();
    assert!(client_pool.warm_one(&cancellation).await.expect("first warm claim"));
    assert!(!client_pool.warm_one(&cancellation).await.expect("satisfied warm floor"));
    assert_eq!(creates.load(Ordering::Relaxed), 1);

    let FleetClient::Local(client) =
        client_pool.acquire(b"", ActiveRequestPolicy::Preserve).await.expect("warm client")
    else {
        panic!("single-node fleet must acquire its warm local client");
    };
    assert_eq!(*client, 0);
}

#[tokio::test]
async fn fleet_acquisition_observes_caller_cancellation_while_paused() {
    let pool = pool_with_options(
        Manager(Arc::new(AtomicUsize::new(0))),
        FleetPoolOptions::new(0, 1, 1, 1, 1, 1)
    );
    pool.apply_local(PoolDirective::pause("profile unavailable"));
    let cancellation = tokio_util::sync::CancellationToken::new();
    cancellation.cancel();

    assert!(matches!(
        pool.acquire_cancellable(b"", ActiveRequestPolicy::Preserve, &cancellation).await,
        Err(FleetAcquireError::Cancelled)
    ));
}
