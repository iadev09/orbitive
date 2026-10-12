use std::marker::PhantomData;

use orbit_core::{OrbitTyped, RingSpec, RingTopology};

/// Arena-addressed mutation protocol; kind 200 is the retired ring-payload ABI.
pub const CACHE_MUTATION_RING_KIND: u8 = 194;
pub const CACHE_MUTATION_RING_CAPACITY: usize =
    orbit_core::compile::usize_from_env(
        option_env!("ORBIT_CACHE_MUTATION_RING_CAPACITY"),
        1_024,
    );
pub const CACHE_MUTATION_RING_PAYLOAD_CAPACITY: usize =
    orbit_core::compile::usize_from_env(
        option_env!("ORBIT_CACHE_MUTATION_RING_PAYLOAD_CAPACITY"),
        1_024,
    );
pub const CACHE_MUTATION_RING_SPEC: RingSpec = RingSpec::per_node(
    CACHE_MUTATION_RING_CAPACITY,
    CACHE_MUTATION_RING_PAYLOAD_CAPACITY,
);

/// Mutation geometry only. Values use the fleet arena, not a second ring.
pub trait CacheLayout: Send + Sync + 'static {
    const MUTATION_RING_KIND: u8;
    const MUTATION_RING_SPEC: RingSpec;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultCacheLayout;

impl CacheLayout for DefaultCacheLayout {
    const MUTATION_RING_KIND: u8 = CACHE_MUTATION_RING_KIND;
    const MUTATION_RING_SPEC: RingSpec = CACHE_MUTATION_RING_SPEC;
}

pub(crate) struct MutationRecord<L>(PhantomData<L>);

impl<L> Clone for MutationRecord<L> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<L: CacheLayout> OrbitTyped for MutationRecord<L> {
    const KIND: u8 = L::MUTATION_RING_KIND;
    const RING_SPEC: RingSpec = L::MUTATION_RING_SPEC;
}

pub(crate) fn validate<L: CacheLayout>() -> crate::Result<()> {
    if L::MUTATION_RING_KIND == orbit_arena::ARENA_KIND {
        return Err(crate::Error::InvalidLayout(
            "mutation kind must not alias the arena",
        ));
    }
    if L::MUTATION_RING_SPEC.topology != RingTopology::PerNode {
        return Err(crate::Error::InvalidLayout(
            "mutations must use per-node lanes",
        ));
    }
    if !L::MUTATION_RING_SPEC.capacity.is_power_of_two() {
        return Err(crate::Error::InvalidLayout(
            "mutation capacity must be a power of two",
        ));
    }
    if L::MUTATION_RING_SPEC.payload_capacity
        < crate::protocol::MIN_MUTATION_HEADER_LEN
        || L::MUTATION_RING_SPEC.payload_capacity > u32::MAX as usize
    {
        return Err(crate::Error::InvalidLayout(
            "mutation payload must fit the protocol header and u32",
        ));
    }
    Ok(())
}
