use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use super::*;

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

fn options() -> PoolOptions {
    PoolOptions::new(0, 1, 1, 1, 1)
}

#[tokio::test]
async fn construction_is_cold_and_acquire_is_lazy() {
    let creates = Arc::new(AtomicUsize::new(0));
    let pool = ClientPool::new(Manager(creates.clone()), options()).expect("pool");
    assert_eq!(creates.load(Ordering::Relaxed), 0);

    let first = pool.acquire(ActiveRequestPolicy::Preserve).await.expect("first client");
    assert_eq!(*first, 0);
    assert_eq!(creates.load(Ordering::Relaxed), 1);
    drop(first);

    let reused = pool.acquire(ActiveRequestPolicy::Preserve).await.expect("reused client");
    assert_eq!(*reused, 0);
    assert_eq!(creates.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn zero_queue_rejects_a_saturated_acquire() {
    let pool =
        ClientPool::new(Manager(Arc::new(AtomicUsize::new(0))), PoolOptions::new(0, 1, 1, 1, 0))
            .expect("pool");
    let lease = pool.acquire(ActiveRequestPolicy::Preserve).await.expect("held client");

    assert!(matches!(
        pool.acquire(ActiveRequestPolicy::Preserve).await,
        Err(AcquireError::QueueFull { max_waiting: 0 })
    ));
    drop(lease);
}

#[tokio::test]
async fn max_concurrency_limits_requests_not_physical_clients() {
    let pool =
        ClientPool::new(Manager(Arc::new(AtomicUsize::new(0))), PoolOptions::new(0, 2, 2, 1, 0))
            .expect("pool");
    let first = pool.acquire(ActiveRequestPolicy::Preserve).await.expect("first request");
    assert_eq!(pool.stats().in_flight, 1);
    assert!(matches!(
        pool.acquire(ActiveRequestPolicy::Preserve).await,
        Err(AcquireError::QueueFull { max_waiting: 0 })
    ));
    assert_eq!(pool.stats().live, 1, "the second physical slot is not admission");

    drop(first);
    assert_eq!(pool.stats().in_flight, 0);
    assert!(pool.acquire(ActiveRequestPolicy::Preserve).await.is_ok());
}

#[tokio::test]
async fn paused_admission_resumes_without_polling() {
    let pool =
        ClientPool::new(Manager(Arc::new(AtomicUsize::new(0))), PoolOptions::new(0, 1, 1, 1, 1))
            .expect("pool");
    pool.pause("credentials are being repaired");

    let waiting_pool = pool.clone();
    let waiting =
        tokio::spawn(async move { waiting_pool.acquire(ActiveRequestPolicy::Preserve).await });
    tokio::task::yield_now().await;
    assert_eq!(pool.stats().waiting, 1);
    assert!(matches!(
        pool.stats().state,
        PoolState::Paused { ref reason } if reason.as_ref() == "credentials are being repaired"
    ));

    assert!(pool.resume());
    let lease = waiting.await.expect("acquire task").expect("resumed acquisition");
    assert_eq!(*lease, 0);
}

#[tokio::test]
async fn caller_can_cancel_a_paused_acquisition() {
    let pool =
        ClientPool::new(Manager(Arc::new(AtomicUsize::new(0))), PoolOptions::new(0, 1, 1, 1, 1))
            .expect("pool");
    pool.pause("profile unavailable");
    let cancellation = CancellationToken::new();

    let waiting_pool = pool.clone();
    let waiting_cancellation = cancellation.clone();
    let waiting = tokio::spawn(async move {
        waiting_pool.acquire_cancellable(ActiveRequestPolicy::Preserve, &waiting_cancellation).await
    });
    tokio::task::yield_now().await;
    cancellation.cancel();

    assert!(matches!(waiting.await.expect("acquire task"), Err(AcquireError::Cancelled)));
    assert_eq!(pool.stats().waiting, 0);
}

#[tokio::test]
async fn caller_cancellation_releases_a_pending_creation_reservation() {
    struct BlockingManager(Mutex<Option<tokio::sync::oneshot::Sender<()>>>);

    #[async_trait]
    impl ClientManager for BlockingManager {
        type Client = ();
        type Error = Infallible;

        async fn create(&self) -> Result<Self::Client, Self::Error> {
            if let Some(started) = self.0.lock().unwrap_or_else(|poison| poison.into_inner()).take()
            {
                let _ = started.send(());
            }
            std::future::pending().await
        }

        async fn recycle(
            &self,
            _client: &mut Self::Client
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let pool = ClientPool::new(
        BlockingManager(Mutex::new(Some(started_tx))),
        PoolOptions::new(0, 1, 1, 1, 0)
    )
    .expect("pool");
    let cancellation = CancellationToken::new();
    let acquiring_pool = pool.clone();
    let acquiring_cancellation = cancellation.clone();
    let acquiring = tokio::spawn(async move {
        acquiring_pool
            .acquire_cancellable(ActiveRequestPolicy::Preserve, &acquiring_cancellation)
            .await
    });
    started_rx.await.expect("creation started");

    cancellation.cancel();
    assert!(matches!(acquiring.await.expect("acquire task"), Err(AcquireError::Cancelled)));
    let stats = pool.stats();
    assert_eq!(stats.creating, 0);
    assert_eq!(stats.live, 0);
}

#[tokio::test]
async fn drain_applies_each_active_request_policy() {
    let pool =
        ClientPool::new(Manager(Arc::new(AtomicUsize::new(0))), PoolOptions::new(0, 3, 3, 3, 0))
            .expect("pool");
    let preserved = pool.acquire(ActiveRequestPolicy::Preserve).await.expect("preserved lease");
    let cancellable =
        pool.acquire(ActiveRequestPolicy::CancelOnDrain(None)).await.expect("cancellable lease");
    let grace_bounded = pool
        .acquire(ActiveRequestPolicy::CancelOnDrain(Some(std::time::Duration::from_secs(60))))
        .await
        .expect("grace-bounded lease");
    let preserved_cancellation = preserved.cancellation_token();
    let active_cancellation = cancellable.cancellation_token();
    let bounded_cancellation = grace_bounded.cancellation_token();

    let draining_pool = pool.clone();
    let draining = tokio::spawn(async move { draining_pool.drain().await });
    tokio::task::yield_now().await;
    assert_eq!(pool.stats().state, PoolState::Draining);
    assert!(!preserved_cancellation.is_cancelled());
    assert!(active_cancellation.is_cancelled());
    assert!(!bounded_cancellation.is_cancelled());

    assert!(pool.cancel_active());
    assert!(preserved_cancellation.is_cancelled());
    assert!(bounded_cancellation.is_cancelled());

    drop(preserved);
    drop(cancellable);
    drop(grace_bounded);
    draining.await.expect("drain task");
    assert_eq!(pool.stats().state, PoolState::Closed);
}

#[tokio::test]
async fn drain_rejects_capacity_waiters_before_active_work_finishes() {
    let pool =
        ClientPool::new(Manager(Arc::new(AtomicUsize::new(0))), PoolOptions::new(0, 1, 1, 1, 1))
            .expect("pool");
    let active = pool.acquire(ActiveRequestPolicy::Preserve).await.expect("active lease");
    let waiting_pool = pool.clone();
    let waiting =
        tokio::spawn(async move { waiting_pool.acquire(ActiveRequestPolicy::Preserve).await });
    tokio::task::yield_now().await;
    assert_eq!(pool.stats().waiting, 1);

    let draining_pool = pool.clone();
    let draining = tokio::spawn(async move { draining_pool.drain().await });
    assert!(matches!(waiting.await.expect("waiting acquire"), Err(AcquireError::Closed)));
    assert_eq!(pool.stats().state, PoolState::Draining);

    drop(active);
    draining.await.expect("drain task");
}

#[tokio::test]
async fn application_policy_only_controls_admission() {
    struct Policy;

    impl ClientPolicy<u16> for Policy {
        fn classify(
            &self,
            status: &u16
        ) -> PoolDirective {
            if *status == 419 {
                PoolDirective::pause("credentials expired")
            } else {
                PoolDirective::Continue
            }
        }
    }

    let pool =
        ClientPool::new(Manager(Arc::new(AtomicUsize::new(0))), PoolOptions::new(0, 1, 1, 1, 0))
            .expect("pool");
    let lease = pool.acquire(ActiveRequestPolicy::Preserve).await.expect("lease");
    lease.observe(&Policy, &419);

    assert!(matches!(pool.stats().state, PoolState::Paused { .. }));
    assert_eq!(*lease, 0, "the triggering request still owns its client");
}
