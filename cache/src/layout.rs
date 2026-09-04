use std::marker::PhantomData;

use orbit_core::{OrbitTyped, RingSpec, RingTopology};

pub const CACHE_MUTATION_RING_KIND: u8 = 200;
pub const CACHE_PAYLOAD_RING_KIND: u8 = 201;
pub const CACHE_MUTATION_RING_SPEC: RingSpec = RingSpec::per_node(1_024, 1_024);
pub const CACHE_PAYLOAD_RING_SPEC: RingSpec = RingSpec::per_node(1_024, 4_096);

/// Physical Orbit rings used by one fleet cache domain.
///
/// Kind and layout are wire contracts. Every peer joining the same fleet must
/// use the same values.
pub trait CacheLayout: Send + Sync + 'static {
    const MUTATION_RING_KIND: u8;
    const MUTATION_RING_SPEC: RingSpec;
    const PAYLOAD_RING_KIND: u8;
    const PAYLOAD_RING_SPEC: RingSpec;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultCacheLayout;

impl CacheLayout for DefaultCacheLayout {
    const MUTATION_RING_KIND: u8 = CACHE_MUTATION_RING_KIND;
    const MUTATION_RING_SPEC: RingSpec = CACHE_MUTATION_RING_SPEC;
    const PAYLOAD_RING_KIND: u8 = CACHE_PAYLOAD_RING_KIND;
    const PAYLOAD_RING_SPEC: RingSpec = CACHE_PAYLOAD_RING_SPEC;
}

pub(crate) struct MutationRecord<L>(PhantomData<L>);

impl<L> Clone for MutationRecord<L> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<L: CacheLayout> OrbitTyped for MutationRecord<L> {
    const KIND: u8 = L::MUTATION_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        L::MUTATION_RING_SPEC.capacity,
        L::MUTATION_RING_SPEC.payload_capacity,
    );
}

pub(crate) struct PayloadRecord<L>(PhantomData<L>);

impl<L> Clone for PayloadRecord<L> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<L: CacheLayout> OrbitTyped for PayloadRecord<L> {
    const KIND: u8 = L::PAYLOAD_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        L::PAYLOAD_RING_SPEC.capacity,
        L::PAYLOAD_RING_SPEC.payload_capacity,
    );
}

pub(crate) fn validate<L: CacheLayout>() -> crate::Result<()> {
    if L::MUTATION_RING_KIND == L::PAYLOAD_RING_KIND {
        return Err(crate::Error::InvalidLayout(
            "mutation and payload ring kinds must differ",
        ));
    }
    if L::MUTATION_RING_SPEC.topology != RingTopology::PerNode
        || L::PAYLOAD_RING_SPEC.topology != RingTopology::PerNode
    {
        return Err(crate::Error::InvalidLayout(
            "mutation and payload rings must use per-node lanes",
        ));
    }
    if L::MUTATION_RING_SPEC.capacity == 0 || L::PAYLOAD_RING_SPEC.capacity == 0 {
        return Err(crate::Error::InvalidLayout(
            "ring capacities must be greater than zero",
        ));
    }
    if !L::MUTATION_RING_SPEC.capacity.is_power_of_two()
        || !L::PAYLOAD_RING_SPEC.capacity.is_power_of_two()
    {
        return Err(crate::Error::InvalidLayout(
            "ring capacities must be powers of two",
        ));
    }
    if L::PAYLOAD_RING_SPEC.capacity > u32::MAX as usize {
        return Err(crate::Error::InvalidLayout(
            "payload ring capacity must fit the chunk-count field",
        ));
    }
    if L::MUTATION_RING_SPEC.payload_capacity < crate::protocol::MIN_MUTATION_HEADER_LEN {
        return Err(crate::Error::InvalidLayout(
            "mutation payload capacity cannot fit the protocol header",
        ));
    }
    if L::PAYLOAD_RING_SPEC.payload_capacity == 0 {
        return Err(crate::Error::InvalidLayout(
            "payload slots must hold at least one byte",
        ));
    }
    Ok(())
}
