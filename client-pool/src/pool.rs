use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;

use crate::{AcquireError, ClientPolicy, OptionsError, PoolDirective, PoolOptions};

/// Creates and validates one protocol-specific outbound client.
#[async_trait]
pub trait ClientManager: Send + Sync + 'static {
    type Client: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Establish a fresh client. Called only after capacity is reserved.
    async fn create(&self) -> Result<Self::Client, Self::Error>;

    /// Prove that an idle client may be handed out again.
    async fn recycle(
        &self,
        client: &mut Self::Client
    ) -> Result<(), Self::Error>;
}

/// Current local ownership and queue counts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolStats {
    pub live: usize,
    pub idle: usize,
    pub leased: usize,
    pub creating: usize,
    pub waiting: usize,
    /// Requests that currently hold a client, including a client being created.
    pub in_flight: usize,
    pub state: PoolState
}

/// Admission and lifecycle state of one pool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PoolState {
    Open,
    Paused { reason: Arc<str> },
    Draining,
    Closed
}

/// Shutdown behavior declared by the owner of one active request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActiveRequestPolicy {
    /// Let the request finish naturally while pool drain waits for its lease.
    Preserve,
    /// Signal the request on drain, immediately or after the declared grace.
    CancelOnDrain(Option<std::time::Duration>)
}

struct ActiveCancellation {
    policy: ActiveRequestPolicy,
    token: CancellationToken
}

struct Entry<C> {
    client: C,
    created_at: Instant,
    idle_since: Instant
}

struct State<C> {
    idle: VecDeque<Entry<C>>,
    live: usize,
    creating: usize,
    waiting: usize,
    in_flight: usize,
    status: PoolState,
    next_active_id: u64,
    drain_started: Option<Instant>,
    drain_cancellations: HashMap<u64, ActiveCancellation>
}

struct Inner<M: ClientManager> {
    manager: M,
    options: PoolOptions,
    state: Mutex<State<M::Client>>,
    changed: Notify,
    admission_closed: CancellationToken,
    active_cancellation: CancellationToken
}

/// A bounded process-local pool of protocol-specific clients.
pub struct ClientPool<M: ClientManager> {
    inner: Arc<Inner<M>>
}

impl<M: ClientManager> Clone for ClientPool<M> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<M: ClientManager> ClientPool<M> {
    /// Construct an empty pool. This performs no network IO.
    pub fn new(
        manager: M,
        options: PoolOptions
    ) -> Result<Self, OptionsError> {
        options.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                manager,
                options,
                state: Mutex::new(State {
                    idle: VecDeque::new(),
                    live: 0,
                    creating: 0,
                    waiting: 0,
                    in_flight: 0,
                    status: PoolState::Open,
                    next_active_id: 0,
                    drain_started: None,
                    drain_cancellations: HashMap::new()
                }),
                changed: Notify::new(),
                admission_closed: CancellationToken::new(),
                active_cancellation: CancellationToken::new()
            })
        })
    }

    /// Acquire one client, creating it lazily or waiting for returned capacity.
    pub async fn acquire(
        &self,
        active_policy: ActiveRequestPolicy
    ) -> Result<ClientLease<M>, AcquireError<M::Error>> {
        self.acquire_inner(active_policy, None).await
    }

    /// Acquire one client while also observing caller-owned cancellation.
    pub async fn acquire_cancellable(
        &self,
        active_policy: ActiveRequestPolicy,
        cancellation: &CancellationToken
    ) -> Result<ClientLease<M>, AcquireError<M::Error>> {
        self.acquire_inner(active_policy, Some(cancellation)).await
    }

    async fn acquire_inner(
        &self,
        active_policy: ActiveRequestPolicy,
        cancellation: Option<&CancellationToken>
    ) -> Result<ClientLease<M>, AcquireError<M::Error>> {
        let deadline =
            self.inner.options.acquire_timeout.map(|duration| TokioInstant::now() + duration);
        let mut waiter = Waiter::new(self);

        loop {
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                return Err(AcquireError::Cancelled);
            }
            let notified = self.inner.changed.notified();
            let action = {
                let mut state = self.state();
                self.prune_expired(&mut state);
                match &state.status {
                    PoolState::Draining | PoolState::Closed => {
                        return Err(AcquireError::Closed);
                    }
                    PoolState::Paused { .. } => {
                        waiter.enter(&mut state)?;
                        Action::Wait
                    }
                    PoolState::Open => {
                        if state.in_flight >= self.inner.options.max_concurrency {
                            waiter.enter(&mut state)?;
                            Action::Wait
                        } else if let Some(entry) = state.idle.pop_front() {
                            state.in_flight += 1;
                            waiter.leave(&mut state);
                            Action::Reuse(entry)
                        } else if state.live < self.inner.options.max_live {
                            state.live += 1;
                            state.creating += 1;
                            state.in_flight += 1;
                            waiter.leave(&mut state);
                            Action::Create
                        } else {
                            waiter.enter(&mut state)?;
                            Action::Wait
                        }
                    }
                }
            };

            match action {
                Action::Reuse(mut entry) => {
                    match self
                        .operation(self.inner.manager.recycle(&mut entry.client), cancellation)
                        .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            self.retire_one();
                            return Err(AcquireError::Manager(error));
                        }
                        Err(interrupted) => {
                            self.retire_one();
                            return Err(interrupted.acquire_error());
                        }
                    }
                    return Ok(ClientLease::new(self.clone(), entry, active_policy));
                }
                Action::Create => {
                    match self.operation(self.inner.manager.create(), cancellation).await {
                        Ok(Ok(client)) => {
                            self.finish_create(true, true);
                            return Ok(ClientLease::new(
                                self.clone(),
                                Entry {
                                    client,
                                    created_at: Instant::now(),
                                    idle_since: Instant::now()
                                },
                                active_policy
                            ));
                        }
                        Ok(Err(error)) => {
                            self.finish_create(false, true);
                            return Err(AcquireError::Manager(error));
                        }
                        Err(interrupted) => {
                            self.finish_create(false, true);
                            return Err(interrupted.acquire_error());
                        }
                    }
                }
                Action::Wait => self.wait_for_change(notified, deadline, cancellation).await?
            }
        }
    }

    pub(crate) async fn create(
        &self,
        active_policy: ActiveRequestPolicy
    ) -> Result<ClientLease<M>, AcquireError<M::Error>> {
        self.create_inner(active_policy, None).await
    }

    pub(crate) async fn create_cancellable(
        &self,
        active_policy: ActiveRequestPolicy,
        cancellation: &CancellationToken
    ) -> Result<ClientLease<M>, AcquireError<M::Error>> {
        self.create_inner(active_policy, Some(cancellation)).await
    }

    async fn create_inner(
        &self,
        active_policy: ActiveRequestPolicy,
        cancellation: Option<&CancellationToken>
    ) -> Result<ClientLease<M>, AcquireError<M::Error>> {
        {
            let mut state = self.state();
            self.prune_expired(&mut state);
            if !matches!(&state.status, PoolState::Open) {
                return Err(AcquireError::Closed);
            }
            if state.live >= self.inner.options.max_live {
                return Err(AcquireError::AtCapacity);
            }
            if state.in_flight >= self.inner.options.max_concurrency {
                return Err(AcquireError::AtCapacity);
            }
            state.live += 1;
            state.creating += 1;
            state.in_flight += 1;
        }
        match self.operation(self.inner.manager.create(), cancellation).await {
            Ok(Ok(client)) => {
                self.finish_create(true, true);
                Ok(ClientLease::new(
                    self.clone(),
                    Entry { client, created_at: Instant::now(), idle_since: Instant::now() },
                    active_policy
                ))
            }
            Ok(Err(error)) => {
                self.finish_create(false, true);
                Err(AcquireError::Manager(error))
            }
            Err(interrupted) => {
                self.finish_create(false, true);
                Err(interrupted.acquire_error())
            }
        }
    }

    pub(crate) async fn acquire_idle_where(
        &self,
        predicate: impl Fn(&M::Client) -> bool,
        active_policy: ActiveRequestPolicy
    ) -> Result<Option<ClientLease<M>>, AcquireError<M::Error>> {
        self.acquire_idle_where_inner(predicate, active_policy, None).await
    }

    pub(crate) async fn acquire_idle_where_cancellable(
        &self,
        predicate: impl Fn(&M::Client) -> bool,
        active_policy: ActiveRequestPolicy,
        cancellation: &CancellationToken
    ) -> Result<Option<ClientLease<M>>, AcquireError<M::Error>> {
        self.acquire_idle_where_inner(predicate, active_policy, Some(cancellation)).await
    }

    async fn acquire_idle_where_inner(
        &self,
        predicate: impl Fn(&M::Client) -> bool,
        active_policy: ActiveRequestPolicy,
        cancellation: Option<&CancellationToken>
    ) -> Result<Option<ClientLease<M>>, AcquireError<M::Error>> {
        let mut entry = {
            let mut state = self.state();
            self.prune_expired(&mut state);
            if !matches!(&state.status, PoolState::Open) {
                return Err(AcquireError::Closed);
            }
            if state.in_flight >= self.inner.options.max_concurrency {
                return Err(AcquireError::AtCapacity);
            }
            let Some(index) = state.idle.iter().position(|entry| predicate(&entry.client)) else {
                return Ok(None);
            };
            state.in_flight += 1;
            state.idle.remove(index).expect("idle index came from this queue")
        };
        match self.operation(self.inner.manager.recycle(&mut entry.client), cancellation).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.retire_one();
                return Err(AcquireError::Manager(error));
            }
            Err(interrupted) => {
                self.retire_one();
                return Err(interrupted.acquire_error());
            }
        }
        Ok(Some(ClientLease::new(self.clone(), entry, active_policy)))
    }

    pub(crate) fn discard_idle_where(
        &self,
        predicate: impl Fn(&M::Client) -> bool
    ) -> bool {
        let mut state = self.state();
        let Some(index) = state.idle.iter().position(|entry| predicate(&entry.client)) else {
            return false;
        };
        let retired = state.idle.remove(index).expect("idle index came from this queue");
        state.live = state.live.saturating_sub(1);
        drop(state);
        drop(retired);
        self.inner.changed.notify_waiters();
        true
    }

    /// Maintain the declared warm floor until cancellation, then close and drain.
    pub async fn run(
        &self,
        cancellation: CancellationToken
    ) -> Result<(), M::Error> {
        loop {
            if cancellation.is_cancelled() {
                self.drain().await;
                return Ok(());
            }

            self.prune_now();
            if matches!(self.stats().state, PoolState::Draining | PoolState::Closed) {
                self.drain().await;
                return Ok(());
            }
            if self.reserve_warm_create() {
                match self.operation(self.inner.manager.create(), Some(&cancellation)).await {
                    Ok(Ok(client)) => self.finish_warm_create(client),
                    Ok(Err(error)) => {
                        self.finish_create(false, false);
                        return Err(error);
                    }
                    Err(_) => {
                        self.finish_create(false, false);
                        self.drain().await;
                        return Ok(());
                    }
                }
                continue;
            }

            let notified = self.inner.changed.notified();
            let deadline = self.next_retirement_deadline();
            match deadline {
                Some(deadline) => {
                    tokio::select! {
                        _ = cancellation.cancelled() => {},
                        _ = notified => {},
                        _ = tokio::time::sleep_until(TokioInstant::from_std(deadline)) => {}
                    }
                }
                None => {
                    tokio::select! {
                        _ = cancellation.cancelled() => {},
                        _ = notified => {}
                    }
                }
            }
        }
    }

    /// Reject new acquisitions and drop retained idle clients.
    pub fn close(&self) {
        let mut state = self.state();
        state.status = PoolState::Closed;
        let idle = state.idle.len();
        let retired: Vec<_> = state.idle.drain(..).collect();
        state.live = state.live.saturating_sub(idle);
        drop(state);
        drop(retired);
        self.inner.admission_closed.cancel();
        self.inner.changed.notify_waiters();
    }

    /// Stop admission and wait indefinitely for every leased or creating client to end.
    pub async fn drain(&self) {
        let immediate = {
            let mut state = self.state();
            if !matches!(&state.status, PoolState::Closed) {
                state.status = PoolState::Draining;
            }
            let drain_started = *state.drain_started.get_or_insert_with(Instant::now);
            let idle = state.idle.len();
            let retired: Vec<_> = state.idle.drain(..).collect();
            state.live = state.live.saturating_sub(idle);
            let immediate = take_due_cancellations(&mut state, drain_started);
            drop(state);
            drop(retired);
            immediate
        };
        for cancellation in immediate {
            cancellation.cancel();
        }
        self.inner.admission_closed.cancel();
        self.inner.changed.notify_waiters();
        loop {
            let notified = self.inner.changed.notified();
            let (due, deadline) = {
                let mut state = self.state();
                if state.live == 0 {
                    state.status = PoolState::Closed;
                    drop(state);
                    self.inner.changed.notify_waiters();
                    return;
                }
                let now = Instant::now();
                let due = take_due_cancellations(&mut state, now);
                let deadline = next_active_cancellation_deadline(&state);
                (due, deadline)
            };
            for cancellation in due {
                cancellation.cancel();
            }
            match deadline {
                Some(deadline) => {
                    tokio::select! {
                        _ = notified => {},
                        _ = tokio::time::sleep_until(TokioInstant::from_std(deadline)) => {}
                    }
                }
                None => notified.await
            }
        }
    }

    /// Pause new acquisitions without retiring retained or leased clients.
    pub fn pause(
        &self,
        reason: impl Into<Arc<str>>
    ) {
        let mut state = self.state();
        if matches!(&state.status, PoolState::Open | PoolState::Paused { .. }) {
            state.status = PoolState::Paused { reason: reason.into() };
        }
        drop(state);
        self.inner.changed.notify_waiters();
    }

    /// Resume a policy-paused pool. Closing and draining are irreversible.
    pub fn resume(&self) -> bool {
        let mut state = self.state();
        if !matches!(&state.status, PoolState::Paused { .. }) {
            return false;
        }
        state.status = PoolState::Open;
        drop(state);
        self.inner.changed.notify_waiters();
        true
    }

    /// Apply a protocol-neutral admission consequence.
    pub fn apply(
        &self,
        directive: PoolDirective
    ) {
        match directive {
            PoolDirective::Continue => {}
            PoolDirective::Pause { reason } => self.pause(reason)
        }
    }

    /// Explicitly cancel work holding a lease after admission has stopped.
    ///
    /// Graceful [`Self::drain`] never calls this. The runtime may call it only
    /// when its own policy has decided that active requests should no longer
    /// be preserved. Calling it while admission is open has no effect.
    pub fn cancel_active(&self) -> bool {
        if !matches!(&self.state().status, PoolState::Draining | PoolState::Closed) {
            return false;
        }
        self.inner.active_cancellation.cancel();
        true
    }

    fn register_active(
        &self,
        policy: ActiveRequestPolicy
    ) -> (CancellationToken, Option<u64>) {
        let token = self.inner.active_cancellation.child_token();
        if policy == ActiveRequestPolicy::Preserve {
            return (token, None);
        }

        let mut state = self.state();
        let id = state.next_active_id;
        state.next_active_id = state.next_active_id.wrapping_add(1);
        let cancel_now = match (policy, state.drain_started) {
            (ActiveRequestPolicy::CancelOnDrain(None), Some(_)) => true,
            (ActiveRequestPolicy::CancelOnDrain(Some(duration)), Some(started)) => {
                started.checked_add(duration).is_some_and(|deadline| deadline <= Instant::now())
            }
            _ => false
        };
        if cancel_now {
            token.cancel();
            (token, None)
        } else {
            state
                .drain_cancellations
                .insert(id, ActiveCancellation { policy, token: token.clone() });
            drop(state);
            self.inner.changed.notify_waiters();
            (token, Some(id))
        }
    }

    fn unregister_active(
        &self,
        registration: Option<u64>
    ) {
        let Some(id) = registration else {
            return;
        };
        self.state().drain_cancellations.remove(&id);
        self.inner.changed.notify_waiters();
    }

    pub(crate) async fn wait_until_open(
        &self,
        deadline: Option<TokioInstant>,
        cancellation: Option<&CancellationToken>
    ) -> Result<(), AcquireError<M::Error>> {
        let mut waiter = Waiter::new(self);
        loop {
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                return Err(AcquireError::Cancelled);
            }
            let notified = self.inner.changed.notified();
            {
                let mut state = self.state();
                match &state.status {
                    PoolState::Open => {
                        waiter.leave(&mut state);
                        return Ok(());
                    }
                    PoolState::Paused { .. } => waiter.enter(&mut state)?,
                    PoolState::Draining | PoolState::Closed => {
                        return Err(AcquireError::Closed);
                    }
                }
            }
            self.wait_for_change(notified, deadline, cancellation).await?;
        }
    }

    pub(crate) async fn wait_until_open_for_maintenance(
        &self,
        cancellation: &CancellationToken
    ) -> bool {
        loop {
            let notified = self.inner.changed.notified();
            match &self.state().status {
                PoolState::Open => return true,
                PoolState::Draining | PoolState::Closed => return false,
                PoolState::Paused { .. } => {}
            }
            tokio::select! {
                _ = notified => {},
                _ = cancellation.cancelled() => return false
            }
        }
    }

    pub fn stats(&self) -> PoolStats {
        let state = self.state();
        PoolStats {
            live: state.live,
            idle: state.idle.len(),
            leased: state.live.saturating_sub(state.idle.len() + state.creating),
            creating: state.creating,
            waiting: state.waiting,
            in_flight: state.in_flight,
            state: state.status.clone()
        }
    }

    fn state(&self) -> MutexGuard<'_, State<M::Client>> {
        self.inner.state.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    fn finish_create(
        &self,
        succeeded: bool,
        admitted: bool
    ) {
        let mut state = self.state();
        state.creating = state.creating.saturating_sub(1);
        if !succeeded {
            state.live = state.live.saturating_sub(1);
            if admitted {
                state.in_flight = state.in_flight.saturating_sub(1);
            }
        }
        drop(state);
        self.inner.changed.notify_waiters();
    }

    fn finish_warm_create(
        &self,
        client: M::Client
    ) {
        let now = Instant::now();
        let mut state = self.state();
        state.creating = state.creating.saturating_sub(1);
        if matches!(&state.status, PoolState::Draining | PoolState::Closed)
            || state.idle.len() >= self.inner.options.max_idle
        {
            state.live = state.live.saturating_sub(1);
        } else {
            state.idle.push_back(Entry { client, created_at: now, idle_since: now });
        }
        drop(state);
        self.inner.changed.notify_waiters();
    }

    fn reserve_warm_create(&self) -> bool {
        let mut state = self.state();
        if !matches!(&state.status, PoolState::Open)
            || state.idle.len() + state.creating >= self.inner.options.min_idle
            || state.live >= self.inner.options.max_live
        {
            return false;
        }
        state.live += 1;
        state.creating += 1;
        true
    }

    fn retire_one(&self) {
        let mut state = self.state();
        state.live = state.live.saturating_sub(1);
        state.in_flight = state.in_flight.saturating_sub(1);
        drop(state);
        self.inner.changed.notify_waiters();
    }

    fn return_entry(
        &self,
        mut entry: Entry<M::Client>
    ) {
        let now = Instant::now();
        let mut state = self.state();
        state.in_flight = state.in_flight.saturating_sub(1);
        let reusable = matches!(&state.status, PoolState::Open | PoolState::Paused { .. })
            && state.idle.len() < self.inner.options.max_idle
            && !expired(&entry, now, &self.inner.options);
        if reusable {
            entry.idle_since = now;
            state.idle.push_back(entry);
        } else {
            state.live = state.live.saturating_sub(1);
        }
        drop(state);
        self.inner.changed.notify_waiters();
    }

    fn prune_now(&self) {
        let mut state = self.state();
        self.prune_expired(&mut state);
    }

    fn prune_expired(
        &self,
        state: &mut State<M::Client>
    ) {
        let now = Instant::now();
        let before = state.idle.len();
        state.idle.retain(|entry| !expired(entry, now, &self.inner.options));
        state.live = state.live.saturating_sub(before - state.idle.len());
    }

    fn next_retirement_deadline(&self) -> Option<Instant> {
        let state = self.state();
        state.idle.iter().filter_map(|entry| retirement_deadline(entry, &self.inner.options)).min()
    }

    async fn wait_for_change<F>(
        &self,
        notified: F,
        deadline: Option<TokioInstant>,
        cancellation: Option<&CancellationToken>
    ) -> Result<(), AcquireError<M::Error>>
    where
        F: Future<Output = ()>
    {
        match (deadline, cancellation) {
            (Some(deadline), Some(cancellation)) => {
                tokio::select! {
                    _ = notified => Ok(()),
                    _ = cancellation.cancelled() => Err(AcquireError::Cancelled),
                    _ = tokio::time::sleep_until(deadline) => Err(AcquireError::Timeout(
                        self.inner.options.acquire_timeout.expect("deadline has duration")
                    ))
                }
            }
            (Some(deadline), None) => {
                tokio::select! {
                    _ = notified => Ok(()),
                    _ = tokio::time::sleep_until(deadline) => Err(AcquireError::Timeout(
                        self.inner.options.acquire_timeout.expect("deadline has duration")
                    ))
                }
            }
            (None, Some(cancellation)) => {
                tokio::select! {
                    _ = notified => Ok(()),
                    _ = cancellation.cancelled() => Err(AcquireError::Cancelled)
                }
            }
            (None, None) => {
                notified.await;
                Ok(())
            }
        }
    }

    async fn operation<F, T>(
        &self,
        operation: F,
        cancellation: Option<&CancellationToken>
    ) -> Result<T, OperationInterrupted>
    where
        F: Future<Output = T>
    {
        match cancellation {
            Some(cancellation) => tokio::select! {
                result = operation => Ok(result),
                _ = cancellation.cancelled() => Err(OperationInterrupted::Caller),
                _ = self.inner.admission_closed.cancelled() => {
                    Err(OperationInterrupted::AdmissionClosed)
                }
            },
            None => tokio::select! {
                result = operation => Ok(result),
                _ = self.inner.admission_closed.cancelled() => {
                    Err(OperationInterrupted::AdmissionClosed)
                }
            }
        }
    }
}

enum OperationInterrupted {
    Caller,
    AdmissionClosed
}

impl OperationInterrupted {
    fn acquire_error<E>(self) -> AcquireError<E> {
        match self {
            Self::Caller => AcquireError::Cancelled,
            Self::AdmissionClosed => AcquireError::Closed
        }
    }
}

enum Action<C> {
    Reuse(Entry<C>),
    Create,
    Wait
}

struct Waiter<'a, M: ClientManager> {
    pool: &'a ClientPool<M>,
    entered: bool
}

impl<'a, M: ClientManager> Waiter<'a, M> {
    fn new(pool: &'a ClientPool<M>) -> Self {
        Self { pool, entered: false }
    }

    fn enter(
        &mut self,
        state: &mut State<M::Client>
    ) -> Result<(), AcquireError<M::Error>> {
        if self.entered {
            return Ok(());
        }
        if state.waiting >= self.pool.inner.options.max_waiting {
            return Err(AcquireError::QueueFull {
                max_waiting: self.pool.inner.options.max_waiting
            });
        }
        state.waiting += 1;
        self.entered = true;
        Ok(())
    }

    fn leave(
        &mut self,
        state: &mut State<M::Client>
    ) {
        if self.entered {
            state.waiting = state.waiting.saturating_sub(1);
            self.entered = false;
        }
    }
}

impl<M: ClientManager> Drop for Waiter<'_, M> {
    fn drop(&mut self) {
        if !self.entered {
            return;
        }
        let mut state = self.pool.state();
        state.waiting = state.waiting.saturating_sub(1);
        drop(state);
        self.pool.inner.changed.notify_waiters();
    }
}

/// Exclusive checkout of one local client.
pub struct ClientLease<M: ClientManager> {
    pool: ClientPool<M>,
    entry: Option<Entry<M::Client>>,
    active_cancellation: CancellationToken,
    active_registration: Option<u64>
}

impl<M: ClientManager> ClientLease<M> {
    fn new(
        pool: ClientPool<M>,
        entry: Entry<M::Client>,
        active_policy: ActiveRequestPolicy
    ) -> Self {
        let (active_cancellation, active_registration) = pool.register_active(active_policy);
        Self { pool, entry: Some(entry), active_cancellation, active_registration }
    }

    /// Retire this client instead of returning it to the idle pool.
    pub fn discard(mut self) {
        self.entry.take();
        self.pool.unregister_active(self.active_registration.take());
        self.pool.retire_one();
    }

    /// Apply the application policy for one protocol-specific outcome.
    pub fn observe<O, P: ClientPolicy<O>>(
        &self,
        policy: &P,
        outcome: &O
    ) {
        self.pool.apply(policy.classify(outcome));
    }

    /// Optional cancellation for active work holding this lease.
    ///
    /// Its request policy controls whether drain signals it immediately, after
    /// an explicit grace period, or only through [`ClientPool::cancel_active`].
    pub fn cancellation_token(&self) -> CancellationToken {
        self.active_cancellation.clone()
    }
}

impl<M: ClientManager> Deref for ClientLease<M> {
    type Target = M::Client;

    fn deref(&self) -> &Self::Target {
        &self.entry.as_ref().expect("client lease is live").client
    }
}

impl<M: ClientManager> DerefMut for ClientLease<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entry.as_mut().expect("client lease is live").client
    }
}

impl<M: ClientManager> Drop for ClientLease<M> {
    fn drop(&mut self) {
        self.pool.unregister_active(self.active_registration.take());
        if let Some(entry) = self.entry.take() {
            self.pool.return_entry(entry);
        }
    }
}

fn take_due_cancellations<C>(
    state: &mut State<C>,
    now: Instant
) -> Vec<CancellationToken> {
    let Some(started) = state.drain_started else {
        return Vec::new();
    };
    let due: Vec<_> = state
        .drain_cancellations
        .iter()
        .filter_map(|(id, active)| {
            let due = match active.policy {
                ActiveRequestPolicy::Preserve => false,
                ActiveRequestPolicy::CancelOnDrain(None) => true,
                ActiveRequestPolicy::CancelOnDrain(Some(grace)) => {
                    started.checked_add(grace).is_some_and(|deadline| deadline <= now)
                }
            };
            due.then_some(*id)
        })
        .collect();
    due.into_iter()
        .filter_map(|id| state.drain_cancellations.remove(&id).map(|active| active.token))
        .collect()
}

fn next_active_cancellation_deadline<C>(state: &State<C>) -> Option<Instant> {
    let started = state.drain_started?;
    state
        .drain_cancellations
        .values()
        .filter_map(|active| match active.policy {
            ActiveRequestPolicy::CancelOnDrain(Some(grace)) => started.checked_add(grace),
            ActiveRequestPolicy::Preserve | ActiveRequestPolicy::CancelOnDrain(None) => None
        })
        .min()
}

fn expired<C>(
    entry: &Entry<C>,
    now: Instant,
    options: &PoolOptions
) -> bool {
    retirement_deadline(entry, options).is_some_and(|deadline| deadline <= now)
}

fn retirement_deadline<C>(
    entry: &Entry<C>,
    options: &PoolOptions
) -> Option<Instant> {
    let idle = options.idle_timeout.and_then(|duration| entry.idle_since.checked_add(duration));
    let lifetime = options.max_lifetime.and_then(|duration| entry.created_at.checked_add(duration));
    match (idle, lifetime) {
        (Some(idle), Some(lifetime)) => Some(idle.min(lifetime)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None
    }
}

#[cfg(test)]
#[path = "../tests/pool.rs"]
mod tests;
