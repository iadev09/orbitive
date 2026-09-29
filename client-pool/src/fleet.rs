use std::future::poll_fn;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use orbit_pool::{
    AdmissionPermit, ExchangeSessionPlan, ExchangeSessionStart, Execution, Key, Limits, Policy,
    Pool, Reason, ResourceId
};
use orbit_stream::exchange::{ExchangeEndpoint, Exchanges};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    AcquireError, ActiveRequestPolicy, ClientLease, ClientManager, ClientPool, FleetPoolOptions,
    OptionsError, PoolDirective
};

/// Failure to select, create, or reach a fleet-owned client.
#[derive(Debug, thiserror::Error)]
pub enum FleetAcquireError<E: std::error::Error + 'static> {
    #[error("client pool is closed")]
    Closed,
    #[error("local client pool is at capacity while the fleet granted creation")]
    LocalCapacity,
    #[error("client manager failed: {0}")]
    Manager(#[source] E),
    #[error("Orbit client pool failed: {0}")]
    Orbit(#[from] orbit_pool::Error),
    #[error("client pool wait queue is full (maximum {max_waiting})")]
    QueueFull { max_waiting: usize },
    #[error("timed out acquiring a fleet client after {0:?}")]
    Timeout(std::time::Duration),
    #[error("fleet client acquisition was cancelled")]
    Cancelled,
    #[error("fleet selected local client {0}, but its owner does not hold it idle")]
    MissingLocal(ResourceId),
    #[error("client acquisition was rejected: {0:?}")]
    Rejected(Reason)
}

#[derive(Debug, thiserror::Error)]
enum RegisteredError<E: std::error::Error + 'static> {
    #[error(transparent)]
    Manager(E),
    #[error(transparent)]
    Orbit(orbit_pool::Error)
}

struct RegisteredClient<C> {
    client: C,
    pool: Arc<Pool>,
    resource: ResourceId,
    initial_execution: Option<Execution>
}

impl<C> Drop for RegisteredClient<C> {
    fn drop(&mut self) {
        let _ = self.pool.unregister(self.resource);
    }
}

struct RegisteredManager<M: ClientManager> {
    manager: M,
    pool: Arc<Pool>,
    key: Key
}

#[async_trait]
impl<M: ClientManager> ClientManager for RegisteredManager<M> {
    type Client = RegisteredClient<M::Client>;
    type Error = RegisteredError<M::Error>;

    async fn create(&self) -> Result<Self::Client, Self::Error> {
        let client = self.manager.create().await.map_err(RegisteredError::Manager)?;
        let (resource, execution) =
            self.pool.register_active(self.key, 1).map_err(RegisteredError::Orbit)?;
        Ok(RegisteredClient {
            client,
            pool: Arc::clone(&self.pool),
            resource,
            initial_execution: Some(execution)
        })
    }

    async fn recycle(
        &self,
        client: &mut Self::Client
    ) -> Result<(), Self::Error> {
        self.manager.recycle(&mut client.client).await.map_err(RegisteredError::Manager)
    }
}

/// One fleet acquisition: direct local use or a session to its remote owner.
pub enum FleetClient<M: ClientManager> {
    Local(FleetClientLease<M>),
    Remote(FleetRemoteClient)
}

/// A remote exchange that retains its fleet-wide request admission until the
/// caller finishes with the endpoint.
pub struct FleetRemoteClient {
    endpoint: ExchangeEndpoint,
    _admission: AdmissionPermit
}

impl Deref for FleetRemoteClient {
    type Target = ExchangeEndpoint;

    fn deref(&self) -> &Self::Target {
        &self.endpoint
    }
}

impl DerefMut for FleetRemoteClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.endpoint
    }
}

/// A direct local client paired with the Orbit execution that reserved it.
pub struct FleetClientLease<M: ClientManager> {
    local: Option<ClientLease<RegisteredManager<M>>>,
    execution: Option<Execution>,
    local_pool: ClientPool<RegisteredManager<M>>,
    resource: ResourceId,
    max_idle: u32,
    _admission: Option<AdmissionPermit>
}

impl<M: ClientManager> Deref for FleetClientLease<M> {
    type Target = M::Client;

    fn deref(&self) -> &Self::Target {
        &self.local.as_ref().expect("fleet client lease is live").client
    }
}

impl<M: ClientManager> DerefMut for FleetClientLease<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.local.as_mut().expect("fleet client lease is live").client
    }
}

impl<M: ClientManager> FleetClientLease<M> {
    /// Optional cancellation for active work holding this local fleet lease.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.local.as_ref().expect("fleet client lease is live").cancellation_token()
    }
}

impl<M: ClientManager> Drop for FleetClientLease<M> {
    fn drop(&mut self) {
        // Make the object locally reachable before publishing its free unit.
        if let Some(local) = self.local.take() {
            drop(local);
        }
        let retained = self
            .execution
            .take()
            .and_then(|execution| execution.complete_idle(self.max_idle).ok())
            .unwrap_or(false);
        if !retained {
            self.local_pool.discard_idle_where(|client| client.resource == self.resource);
        }
    }
}

/// A remotely initiated exchange paired with the exact local client it leased.
pub struct IncomingClientSession<M: ClientManager> {
    pub endpoint: ExchangeEndpoint,
    pub start: ExchangeSessionStart,
    client: FleetClientLease<M>
}

impl<M: ClientManager> Deref for IncomingClientSession<M> {
    type Target = M::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<M: ClientManager> DerefMut for IncomingClientSession<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

/// Fleet selection and exchange composition over a process-local client pool.
pub struct FleetClientPool<M: ClientManager> {
    local: ClientPool<RegisteredManager<M>>,
    pool: Arc<Pool>,
    exchanges: Arc<Exchanges>,
    key: Key,
    policy: Arc<dyn Policy>,
    options: FleetPoolOptions,
    waiting: AtomicUsize
}

impl<M: ClientManager> FleetClientPool<M> {
    pub fn new(
        manager: M,
        pool: Arc<Pool>,
        exchanges: Arc<Exchanges>,
        key: Key,
        policy: Arc<dyn Policy>,
        options: FleetPoolOptions
    ) -> Result<Self, OptionsError> {
        options.validate()?;
        if !pool.tracks_concurrency() {
            return Err(OptionsError::FleetConcurrencyDisabled);
        }
        let local = ClientPool::new(
            RegisteredManager { manager, pool: Arc::clone(&pool), key },
            options.local_options()
        )?;
        Ok(Self { local, pool, exchanges, key, policy, options, waiting: AtomicUsize::new(0) })
    }

    pub async fn acquire(
        &self,
        metadata: &[u8],
        active_policy: ActiveRequestPolicy
    ) -> Result<FleetClient<M>, FleetAcquireError<M::Error>> {
        self.acquire_inner(metadata, active_policy, None).await
    }

    /// Acquire one fleet client while also observing caller-owned cancellation.
    pub async fn acquire_cancellable(
        &self,
        metadata: &[u8],
        active_policy: ActiveRequestPolicy,
        cancellation: &CancellationToken
    ) -> Result<FleetClient<M>, FleetAcquireError<M::Error>> {
        self.acquire_inner(metadata, active_policy, Some(cancellation)).await
    }

    async fn acquire_inner(
        &self,
        metadata: &[u8],
        active_policy: ActiveRequestPolicy,
        cancellation: Option<&CancellationToken>
    ) -> Result<FleetClient<M>, FleetAcquireError<M::Error>> {
        let deadline = self.options.acquire_timeout.map(|duration| Instant::now() + duration);
        self.local.wait_until_open(deadline, cancellation).await.map_err(map_local_error)?;
        let admission = self.acquire_admission(deadline, cancellation).await?;
        loop {
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                return Err(FleetAcquireError::Cancelled);
            }
            self.local.wait_until_open(deadline, cancellation).await.map_err(map_local_error)?;
            let plan = self.pool.acquire_exchange_session(
                self.key,
                &Limits { max_live: self.options.max_live, attempts: self.options.attempts },
                self.policy.as_ref(),
                &self.exchanges,
                metadata
            )?;
            match plan {
                ExchangeSessionPlan::Local(execution) => {
                    let resource = execution.lease().id;
                    let local = match cancellation {
                        Some(cancellation) => {
                            self.local
                                .acquire_idle_where_cancellable(
                                    |client| client.resource == resource,
                                    active_policy,
                                    cancellation
                                )
                                .await
                        }
                        None => {
                            self.local
                                .acquire_idle_where(
                                    |client| client.resource == resource,
                                    active_policy
                                )
                                .await
                        }
                    }
                    .map_err(map_local_error)?;
                    let Some(local) = local else {
                        drop(execution);
                        return Err(FleetAcquireError::MissingLocal(resource));
                    };
                    return Ok(FleetClient::Local(self.lease(
                        local,
                        execution,
                        resource,
                        Some(admission)
                    )));
                }
                ExchangeSessionPlan::Remote(endpoint) => {
                    return Ok(FleetClient::Remote(FleetRemoteClient {
                        endpoint,
                        _admission: admission
                    }));
                }
                ExchangeSessionPlan::Create(permit) => {
                    let (local, execution, resource) =
                        self.create_registered(active_policy, cancellation).await?;
                    permit.finish();
                    return Ok(FleetClient::Local(self.lease(
                        local,
                        execution,
                        resource,
                        Some(admission)
                    )));
                }
                ExchangeSessionPlan::Wait(version) => {
                    let _waiter = FleetWaiter::enter(&self.waiting, self.options.max_waiting)?;
                    let wait = poll_fn(|cx| self.pool.poll_capacity(self.key, version, cx));
                    self.wait_for_capacity(wait, deadline, cancellation).await?;
                }
                ExchangeSessionPlan::Reject(reason) => {
                    return Err(FleetAcquireError::Rejected(reason));
                }
            }
        }
    }

    /// Wait for and accept one remote exchange addressed to a local client.
    pub async fn accept(
        &self,
        active_policy: ActiveRequestPolicy
    ) -> Result<IncomingClientSession<M>, FleetAcquireError<M::Error>> {
        self.accept_inner(active_policy, None).await
    }

    /// Accept a remote session while observing runtime cancellation.
    pub async fn accept_cancellable(
        &self,
        active_policy: ActiveRequestPolicy,
        cancellation: &CancellationToken
    ) -> Result<IncomingClientSession<M>, FleetAcquireError<M::Error>> {
        self.accept_inner(active_policy, Some(cancellation)).await
    }

    async fn accept_inner(
        &self,
        active_policy: ActiveRequestPolicy,
        cancellation: Option<&CancellationToken>
    ) -> Result<IncomingClientSession<M>, FleetAcquireError<M::Error>> {
        let accept = poll_fn(|cx| self.pool.poll_accept_exchange_session(&self.exchanges, cx));
        let (execution, endpoint, start) = match cancellation {
            Some(cancellation) => tokio::select! {
                result = accept => result?,
                _ = cancellation.cancelled() => return Err(FleetAcquireError::Cancelled)
            },
            None => accept.await?
        };
        let resource = execution.lease().id;
        let local = match cancellation {
            Some(cancellation) => {
                self.local
                    .acquire_idle_where_cancellable(
                        |client| client.resource == resource,
                        active_policy,
                        cancellation
                    )
                    .await
            }
            None => {
                self.local
                    .acquire_idle_where(|client| client.resource == resource, active_policy)
                    .await
            }
        }
        .map_err(map_local_error)?;
        let Some(local) = local else {
            drop(execution);
            return Err(FleetAcquireError::MissingLocal(resource));
        };
        Ok(IncomingClientSession {
            endpoint,
            start,
            client: self.lease(local, execution, resource, None)
        })
    }

    /// Maintain fleet-wide warm capacity and local retirement until cancelled.
    ///
    /// Protocol code runs [`Self::accept`] separately; this loop never consumes
    /// an exchange whose request bytes it cannot interpret.
    pub async fn run(
        &self,
        cancellation: CancellationToken
    ) -> Result<(), FleetAcquireError<M::Error>> {
        let local_cancellation = cancellation.child_token();
        tokio::select! {
            result = self.local.run(local_cancellation.clone()) => {
                local_cancellation.cancel();
                result.map_err(map_registered_error)?;
            }
            result = self.maintain_warm(cancellation.clone()) => {
                local_cancellation.cancel();
                result?;
                self.local.drain().await;
            }
        }
        Ok(())
    }

    async fn maintain_warm(
        &self,
        cancellation: CancellationToken
    ) -> Result<(), FleetAcquireError<M::Error>> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if !self.local.wait_until_open_for_maintenance(&cancellation).await {
                return Ok(());
            }
            let version = self.pool.version(self.key)?;
            if self.warm_one(&cancellation).await? {
                continue;
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                changed = poll_fn(|cx| self.pool.poll_capacity(self.key, version, cx)) => {
                    changed?;
                }
            }
        }
    }

    async fn warm_one(
        &self,
        cancellation: &CancellationToken
    ) -> Result<bool, FleetAcquireError<M::Error>> {
        let permit =
            match self.pool.claim_warm(self.key, self.options.max_live, self.options.min_idle) {
                Ok(permit) => permit,
                Err(orbit_pool::Error::CreationBudget { .. }) => return Ok(false),
                Err(error) => return Err(error.into())
            };
        let (local, execution, resource) =
            self.create_registered(ActiveRequestPolicy::Preserve, Some(cancellation)).await?;
        permit.finish();
        drop(local);
        let retained = execution.complete_idle(self.options.max_idle)?;
        if !retained {
            self.local.discard_idle_where(|client| client.resource == resource);
        }
        Ok(retained)
    }

    async fn create_registered(
        &self,
        active_policy: ActiveRequestPolicy,
        cancellation: Option<&CancellationToken>
    ) -> Result<
        (ClientLease<RegisteredManager<M>>, Execution, ResourceId),
        FleetAcquireError<M::Error>
    > {
        let mut local = match cancellation {
            Some(cancellation) => self.local.create_cancellable(active_policy, cancellation).await,
            None => self.local.create(active_policy).await
        }
        .map_err(map_local_error)?;
        let resource = local.resource;
        let execution = local
            .initial_execution
            .take()
            .expect("fresh fleet client carries its initial execution");
        Ok((local, execution, resource))
    }

    fn lease(
        &self,
        local: ClientLease<RegisteredManager<M>>,
        execution: Execution,
        resource: ResourceId,
        admission: Option<AdmissionPermit>
    ) -> FleetClientLease<M> {
        FleetClientLease {
            local: Some(local),
            execution: Some(execution),
            local_pool: self.local.clone(),
            resource,
            max_idle: self.options.max_idle,
            _admission: admission
        }
    }

    async fn acquire_admission(
        &self,
        deadline: Option<Instant>,
        cancellation: Option<&CancellationToken>
    ) -> Result<AdmissionPermit, FleetAcquireError<M::Error>> {
        loop {
            let version = self.pool.version(self.key)?;
            match self.pool.admit(self.key, self.options.max_concurrency) {
                Ok(permit) => return Ok(permit),
                Err(orbit_pool::Error::ConcurrencyLimit { .. }) => {
                    let _waiter = FleetWaiter::enter(&self.waiting, self.options.max_waiting)?;
                    let wait = poll_fn(|cx| self.pool.poll_capacity(self.key, version, cx));
                    self.wait_for_capacity(wait, deadline, cancellation).await?;
                }
                Err(error) => return Err(error.into())
            }
        }
    }

    /// Apply a directive to this process's admission gate.
    ///
    /// This does not claim to pause sibling processes. A fleet-wide admission
    /// controller needs a separately managed shared resource; `orbit-pool`
    /// deliberately contains no profile-policy bit.
    pub fn apply_local(
        &self,
        directive: PoolDirective
    ) {
        self.local.apply(directive);
    }

    /// Resume this process's policy-paused admission gate.
    pub fn resume_local(&self) -> bool {
        self.local.resume()
    }

    /// Explicitly cancel active local leases after admission has stopped.
    pub fn cancel_active_local(&self) -> bool {
        self.local.cancel_active()
    }

    async fn wait_for_capacity<F, T>(
        &self,
        wait: F,
        deadline: Option<Instant>,
        cancellation: Option<&CancellationToken>
    ) -> Result<(), FleetAcquireError<M::Error>>
    where
        F: std::future::Future<Output = Result<T, orbit_pool::Error>>
    {
        match (deadline, cancellation) {
            (Some(deadline), Some(cancellation)) => tokio::select! {
                result = wait => result.map(|_| ()).map_err(Into::into),
                _ = cancellation.cancelled() => Err(FleetAcquireError::Cancelled),
                _ = tokio::time::sleep_until(deadline) => Err(FleetAcquireError::Timeout(
                    self.options.acquire_timeout.expect("deadline has a configured duration")
                ))
            },
            (Some(deadline), None) => tokio::select! {
                result = wait => result.map(|_| ()).map_err(Into::into),
                _ = tokio::time::sleep_until(deadline) => Err(FleetAcquireError::Timeout(
                    self.options.acquire_timeout.expect("deadline has a configured duration")
                ))
            },
            (None, Some(cancellation)) => tokio::select! {
                result = wait => result.map(|_| ()).map_err(Into::into),
                _ = cancellation.cancelled() => Err(FleetAcquireError::Cancelled)
            },
            (None, None) => wait.await.map(|_| ()).map_err(Into::into)
        }
    }
}

fn map_local_error<E: std::error::Error + Send + Sync + 'static>(
    error: AcquireError<RegisteredError<E>>
) -> FleetAcquireError<E> {
    match error {
        AcquireError::Closed => FleetAcquireError::Closed,
        AcquireError::AtCapacity => FleetAcquireError::LocalCapacity,
        AcquireError::QueueFull { max_waiting } => FleetAcquireError::QueueFull { max_waiting },
        AcquireError::Timeout(duration) => FleetAcquireError::Timeout(duration),
        AcquireError::Cancelled => FleetAcquireError::Cancelled,
        AcquireError::Manager(error) => map_registered_error(error)
    }
}

fn map_registered_error<E: std::error::Error + 'static>(
    error: RegisteredError<E>
) -> FleetAcquireError<E> {
    match error {
        RegisteredError::Manager(error) => FleetAcquireError::Manager(error),
        RegisteredError::Orbit(error) => FleetAcquireError::Orbit(error)
    }
}

struct FleetWaiter<'a> {
    count: &'a AtomicUsize
}

impl<'a> FleetWaiter<'a> {
    fn enter<E: std::error::Error + 'static>(
        count: &'a AtomicUsize,
        max_waiting: usize
    ) -> Result<Self, FleetAcquireError<E>> {
        let admitted = count
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < max_waiting).then_some(current + 1)
            })
            .is_ok();
        if !admitted {
            return Err(FleetAcquireError::QueueFull { max_waiting });
        }
        Ok(Self { count })
    }
}

impl Drop for FleetWaiter<'_> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
#[path = "../tests/fleet.rs"]
mod tests;
