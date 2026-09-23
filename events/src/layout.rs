//! Physical geometry of one event ring.
//!
//! The default layout is the fleet's own ring. A second layout names another
//! ring kind with its own capacity and frame size, so an embedding runtime
//! can keep, say, small service events and large application events apart
//! without the two competing for one retained window.

use std::marker::PhantomData;

use orbit_core::{OrbitTyped, RingSpec, RingTopology};

use crate::{Error, HEADER_LEN, Result};

/// Event frame payload limit for the default ring. This is the event lane's
/// own SHM payload capacity; non-Unix keeps the same contract so callers do
/// not accidentally rely on unbounded in-memory frames.
pub const EVENT_RING_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_EVENT_RING_CAPACITY"), 1_024);
pub const EVENT_RING_PAYLOAD_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_EVENT_RING_PAYLOAD_CAPACITY"), 512);
pub const EVENT_RING_SPEC: RingSpec =
    RingSpec::per_node(EVENT_RING_CAPACITY, EVENT_RING_PAYLOAD_CAPACITY);
pub const EVENT_PAYLOAD_MAX: usize = EVENT_RING_SPEC.payload_capacity;
pub const EVENT_RING_KIND: u8 = 220;

const _: () = assert!(EVENT_RING_CAPACITY.is_power_of_two());
const _: () = assert!(EVENT_RING_PAYLOAD_CAPACITY >= HEADER_LEN);
const _: () = assert!(EVENT_RING_PAYLOAD_CAPACITY <= u32::MAX as usize);

/// The Orbit ring one event bus publishes into and polls from.
pub trait EventLayout: Send + Sync + 'static {
    /// Kind of the per-node event ring.
    const RING_KIND: u8;
    /// Lane capacity and frame size of that ring.
    const RING_SPEC: RingSpec;
}

/// The fleet's default event ring: kind 220, geometry from
/// `ORBIT_EVENT_RING_CAPACITY` and `ORBIT_EVENT_RING_PAYLOAD_CAPACITY`.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultEventLayout;

impl EventLayout for DefaultEventLayout {
    const RING_KIND: u8 = EVENT_RING_KIND;
    const RING_SPEC: RingSpec = EVENT_RING_SPEC;
}

pub(crate) struct EventRecord<L>(PhantomData<L>);

impl<L> Clone for EventRecord<L> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<L: EventLayout> OrbitTyped for EventRecord<L> {
    // Hand-picked V0 kinds. Build-time KIND allocation will replace
    // these manual values later.
    const KIND: u8 = L::RING_KIND;
    const RING_SPEC: RingSpec =
        RingSpec::per_node(L::RING_SPEC.capacity, L::RING_SPEC.payload_capacity);
}

pub(crate) fn validate<L: EventLayout>() -> Result<()> {
    if L::RING_SPEC.topology != RingTopology::PerNode {
        return Err(Error::InvalidLayout("the event ring must use per-node lanes"));
    }
    if L::RING_SPEC.capacity == 0 || !L::RING_SPEC.capacity.is_power_of_two() {
        return Err(Error::InvalidLayout("event-ring capacity must be a non-zero power of two"));
    }
    if L::RING_SPEC.payload_capacity < HEADER_LEN {
        return Err(Error::InvalidLayout("event-ring payload cannot fit an event header"));
    }
    if L::RING_SPEC.payload_capacity > u32::MAX as usize {
        return Err(Error::InvalidLayout("event-ring payload exceeds the frame length field"));
    }
    Ok(())
}
