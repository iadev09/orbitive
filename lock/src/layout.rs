use std::marker::PhantomData;

use orbit_core::{OrbitTyped, RingSpec, RingTopology};

use crate::{Error, Result};

pub const LOCK_STATE_KIND: u8 = 228;
pub const LOCK_EVENT_RING_KIND: u8 = 229;
pub const LOCK_EVENT_RING_SPEC: RingSpec = RingSpec::per_node(1_024, 1_024);

/// Physical Orbit surfaces used by one lock domain.
pub trait LockLayout: Send + Sync + 'static {
    /// Kind used to name the current-state SHM object and mint lock ids.
    const STATE_KIND: u8;
    /// Kind used by the notified transition ring.
    const EVENT_RING_KIND: u8;
    const EVENT_RING_SPEC: RingSpec;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultLockLayout;

impl LockLayout for DefaultLockLayout {
    const STATE_KIND: u8 = LOCK_STATE_KIND;
    const EVENT_RING_KIND: u8 = LOCK_EVENT_RING_KIND;
    const EVENT_RING_SPEC: RingSpec = LOCK_EVENT_RING_SPEC;
}

pub(crate) struct LockEventRecord<L>(PhantomData<L>);

impl<L> Clone for LockEventRecord<L> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<L: LockLayout> OrbitTyped for LockEventRecord<L> {
    const KIND: u8 = L::EVENT_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        L::EVENT_RING_SPEC.capacity,
        L::EVENT_RING_SPEC.payload_capacity,
    );
}

pub(crate) fn validate<L: LockLayout>() -> Result<()> {
    if L::STATE_KIND == L::EVENT_RING_KIND {
        return Err(Error::InvalidLayout(
            "state and event-ring kinds must differ",
        ));
    }
    if L::EVENT_RING_SPEC.topology != RingTopology::PerNode {
        return Err(Error::InvalidLayout(
            "the lock event ring must use per-node lanes",
        ));
    }
    if L::EVENT_RING_SPEC.capacity == 0 || !L::EVENT_RING_SPEC.capacity.is_power_of_two() {
        return Err(Error::InvalidLayout(
            "event-ring capacity must be a non-zero power of two",
        ));
    }
    if L::EVENT_RING_SPEC.payload_capacity < crate::protocol::EVENT_HEADER_LEN + 2 {
        return Err(Error::InvalidLayout(
            "event-ring payload cannot fit a lock event",
        ));
    }
    Ok(())
}
