//! Fleet-shared keyed locks over Orbit shared memory.
//!
//! Current ownership lives in a fixed-capacity state table. A dedicated
//! notified ring carries successful transitions for reactive consumers.

mod error;
mod fence;
mod layout;
mod protocol;
mod state;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use orbit_core::fleet::FleetLaneCursor;
use orbit_core::{Fleet, NetId64, RingLoss};

pub use error::{Error, Result};
pub use fence::{Fence, FenceToken};
pub use layout::{
    DefaultLockLayout, LOCK_EVENT_RING_CAPACITY, LOCK_EVENT_RING_KIND,
    LOCK_EVENT_RING_PAYLOAD_CAPACITY, LOCK_EVENT_RING_SPEC, LOCK_STATE_KIND, LockLayout,
};
pub use state::{LOCK_STATE_CAPACITY, LOCK_STATE_PAYLOAD_MAX};

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
pub use orbit_core::RingEventFd;

use layout::LockEventRecord;
use state::LockStateStore;

/// Typed namespace for lock keys.
pub trait LockType {
    const NAMESPACE: &'static str;
}

/// Resource identity used for contention.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LockKey {
    namespace: Bytes,
    label: Bytes,
}

impl LockKey {
    pub fn new<T: LockType>(label: impl Into<Bytes>) -> Self {
        Self {
            namespace: Bytes::from_static(T::NAMESPACE.as_bytes()),
            label: label.into(),
        }
    }

    /// Construct a dynamically namespaced key.
    pub fn from_parts(namespace: impl Into<Bytes>, label: impl Into<Bytes>) -> Self {
        Self {
            namespace: namespace.into(),
            label: label.into(),
        }
    }

    pub fn namespace(&self) -> &[u8] {
        &self.namespace
    }

    pub fn label(&self) -> &[u8] {
        &self.label
    }
}

/// Opaque caller-chosen identity for one lock tenure.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LockOwner(Bytes);

impl LockOwner {
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self(value.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Bytes> for LockOwner {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<String> for LockOwner {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}

impl From<&str> for LockOwner {
    fn from(value: &str) -> Self {
        Self(Bytes::copy_from_slice(value.as_bytes()))
    }
}

/// One current lock tenure returned by the authoritative table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockLease {
    pub lock_id: NetId64,
    pub key: LockKey,
    pub owner: LockOwner,
    /// Host-monotonic milliseconds at which this tenure began.
    pub acquired_at_ms: u64,
    /// Host-monotonic expiry deadline.
    pub expires_at_ms: u64,
    pub state_revision: u64,
}

impl LockLease {
    /// Strictly increasing token for successive lock tenures.
    pub fn fencing_token(&self) -> FenceToken {
        FenceToken::new(self.lock_id.counter())
    }

    pub fn is_expired_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms <= now_ms
    }
}

/// Result of one atomic acquisition attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockAcquire {
    Acquired(LockLease),
    Occupied(LockLease),
}

impl LockAcquire {
    pub fn acquired(self) -> Option<LockLease> {
        match self {
            Self::Acquired(lease) => Some(lease),
            Self::Occupied(_) => None,
        }
    }

    pub fn holder(&self) -> &LockLease {
        match self {
            Self::Acquired(lease) | Self::Occupied(lease) => lease,
        }
    }
}

/// One successful current-state change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockTransition {
    Acquired(LockLease),
    Renewed(LockLease),
    Released {
        lock_id: NetId64,
        key: LockKey,
        owner: LockOwner,
        state_revision: u64,
    },
}

impl LockTransition {
    pub fn lock_id(&self) -> NetId64 {
        match self {
            Self::Acquired(lease) | Self::Renewed(lease) => lease.lock_id,
            Self::Released { lock_id, .. } => *lock_id,
        }
    }

    pub fn key(&self) -> &LockKey {
        match self {
            Self::Acquired(lease) | Self::Renewed(lease) => &lease.key,
            Self::Released { key, .. } => key,
        }
    }

    pub fn owner(&self) -> &LockOwner {
        match self {
            Self::Acquired(lease) | Self::Renewed(lease) => &lease.owner,
            Self::Released { owner, .. } => owner,
        }
    }

    pub fn state_revision(&self) -> u64 {
        match self {
            Self::Acquired(lease) | Self::Renewed(lease) => lease.state_revision,
            Self::Released { state_revision, .. } => *state_revision,
        }
    }
}

/// Transition decoded from the notified ring.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockEvent {
    pub event_id: NetId64,
    pub transition: LockTransition,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockCursor {
    inner: FleetLaneCursor,
}

/// Result of advancing one lock-event cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LockPoll {
    pub events: Vec<LockEvent>,
    pub loss: RingLoss,
    pub malformed: u64,
}

impl LockPoll {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty() && self.loss.is_empty() && self.malformed == 0
    }
}

/// Fleet-shared lock handle. Clones share one event cursor.
#[derive(Clone)]
pub struct Lock<L: LockLayout = DefaultLockLayout> {
    fleet: Arc<Fleet>,
    state: LockStateStore<L>,
    cursor: Arc<Mutex<LockCursor>>,
}

impl Lock<DefaultLockLayout> {
    /// Subscribe after all currently committed transition events.
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        Self::open(fleet, false)
    }

    /// Replay retained transitions. Intended for tests and diagnostics.
    pub fn replay_retained(fleet: Arc<Fleet>) -> Result<Self> {
        Self::open(fleet, true)
    }
}

impl<L: LockLayout> Lock<L> {
    /// Subscribe with a custom storage layout.
    pub fn with_layout(fleet: Arc<Fleet>) -> Result<Self> {
        Self::open(fleet, false)
    }

    /// Replay retained transitions with a custom storage layout.
    /// Intended for tests and diagnostics.
    pub fn replay_layout(fleet: Arc<Fleet>) -> Result<Self> {
        Self::open(fleet, true)
    }

    fn open(fleet: Arc<Fleet>, replay_retained: bool) -> Result<Self> {
        layout::validate::<L>()?;
        let state = LockStateStore::new(&fleet);
        let cursor = LockCursor {
            inner: if replay_retained {
                fleet.lane_cursor_from_start::<LockEventRecord<L>>()
            } else {
                fleet.lane_cursor_at_head::<LockEventRecord<L>>()
            },
        };
        Ok(Self {
            fleet,
            state,
            cursor: Arc::new(Mutex::new(cursor)),
        })
    }

    pub fn try_acquire(
        &self,
        key: &LockKey,
        owner: &LockOwner,
        ttl: Duration,
    ) -> Result<LockAcquire> {
        let now_ms = monotonic_ms()?;
        let expires_at_ms = deadline(now_ms, ttl)?;
        self.state
            .acquire(key, owner, now_ms, expires_at_ms, |transition| {
                self.publish(transition).map(|_| ())
            })
    }

    /// Renew only if this exact tenure is still current.
    pub fn renew(&self, lease: &LockLease, ttl: Duration) -> Result<Option<LockLease>> {
        let now_ms = monotonic_ms()?;
        let expires_at_ms = deadline(now_ms, ttl)?;
        self.state
            .renew_id(lease, now_ms, expires_at_ms, |transition| {
                self.publish(transition).map(|_| ())
            })
    }

    /// Renew by a caller-retained owner identity.
    pub fn renew_owned(
        &self,
        key: &LockKey,
        owner: &LockOwner,
        ttl: Duration,
    ) -> Result<Option<LockLease>> {
        let now_ms = monotonic_ms()?;
        let expires_at_ms = deadline(now_ms, ttl)?;
        self.state
            .renew_owner(key, owner, now_ms, expires_at_ms, |transition| {
                self.publish(transition).map(|_| ())
            })
    }

    /// Release only if this exact tenure is still current.
    pub fn release(&self, lease: &LockLease) -> Result<bool> {
        let now_ms = monotonic_ms()?;
        self.state.release_id(lease, now_ms, |transition| {
            self.publish(transition).map(|_| ())
        })
    }

    /// Release only when the current owner bytes match.
    pub fn release_owned(&self, key: &LockKey, owner: &LockOwner) -> Result<bool> {
        let now_ms = monotonic_ms()?;
        self.state.release_owner(key, owner, now_ms, |transition| {
            self.publish(transition).map(|_| ())
        })
    }

    /// Release the current tenure without checking its owner.
    ///
    /// This is the explicit administrative escape hatch required by lock
    /// providers such as Laravel's `forceRelease`. Normal callers should use
    /// [`Self::release`] or [`Self::release_owned`].
    pub fn force_release(&self, key: &LockKey) -> Result<bool> {
        let now_ms = monotonic_ms()?;
        self.state.force_release(key, now_ms, |transition| {
            self.publish(transition).map(|_| ())
        })
    }

    /// Read authoritative current state for one key.
    pub fn current(&self, key: &LockKey) -> Result<Option<LockLease>> {
        self.state.current(key, monotonic_ms()?)
    }

    /// Restore the current tenure only when its owner bytes match.
    pub fn restore(&self, key: &LockKey, owner: &LockOwner) -> Result<Option<LockLease>> {
        Ok(self
            .current(key)?
            .filter(|lease| lease.owner.as_bytes() == owner.as_bytes()))
    }

    /// Drain every currently visible transition from all writer lanes.
    pub fn poll(&self) -> LockPoll {
        let raw = {
            let mut cursor = self
                .cursor
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.fleet
                .poll_lanes::<LockEventRecord<L>>(&mut cursor.inner)
        };
        let mut events = Vec::with_capacity(raw.frames.len());
        let mut malformed = 0;
        for frame in raw.frames {
            match protocol::decode(&frame) {
                Some(event) => events.push(event),
                None => malformed += 1,
            }
        }
        events.sort_by_key(|event| event.transition.state_revision());
        LockPoll {
            events,
            loss: raw.loss,
            malformed,
        }
    }

    /// Clear lock state and transition history during quiescent owner boot.
    pub fn reset_transport(&self) -> Result<()> {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.state.reset()?;
        self.fleet.reset_ring::<LockEventRecord<L>>()?;
        cursor.inner = self.fleet.lane_cursor_at_head::<LockEventRecord<L>>();
        Ok(())
    }

    /// Remove the state and event SHM objects.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        let ring = self.fleet.shm_ring::<LockEventRecord<L>>()?;
        let ring_result = ring.unlink();
        let state_result = self.state.unlink();
        ring_result?;
        state_result
    }

    /// Create this process' readiness fd for lock transitions.
    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    pub fn event_fd(&self) -> Result<RingEventFd> {
        self.fleet
            .ring_event_fd::<LockEventRecord<L>>()
            .map_err(Error::Io)
    }

    fn publish(&self, transition: &LockTransition) -> Result<LockEvent> {
        let (kind, payload) = protocol::encode::<L>(transition)?;
        let event_id = {
            #[cfg(any(target_os = "linux", target_os = "freebsd"))]
            {
                self.fleet.publish_notified::<LockEventRecord<L>>(
                    kind,
                    transition.state_revision(),
                    payload,
                )?
            }
            #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
            {
                self.fleet
                    .publish::<LockEventRecord<L>>(kind, transition.state_revision(), payload)
            }
        };
        Ok(LockEvent {
            event_id,
            transition: transition.clone(),
        })
    }
}

fn deadline(now_ms: u64, ttl: Duration) -> Result<u64> {
    if ttl.is_zero() {
        return Err(Error::TtlZero);
    }
    let ttl_ms = u64::try_from(ttl.as_millis().max(1)).map_err(|_| Error::TtlOverflow)?;
    now_ms.checked_add(ttl_ms).ok_or(Error::TtlOverflow)
}

#[cfg(unix)]
fn monotonic_ms() -> Result<u64> {
    let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, value.as_mut_ptr()) };
    if result != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    let value = unsafe { value.assume_init() };
    let seconds = u64::try_from(value.tv_sec).map_err(|_| Error::TtlOverflow)?;
    let nanos = u64::try_from(value.tv_nsec).map_err(|_| Error::TtlOverflow)?;
    Ok(seconds
        .saturating_mul(1_000)
        .saturating_add(nanos / 1_000_000))
}

#[cfg(not(unix))]
fn monotonic_ms() -> Result<u64> {
    use std::sync::LazyLock;
    use std::time::Instant;

    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    Ok(START.elapsed().as_millis().min(u128::from(u64::MAX)) as u64)
}
