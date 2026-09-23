/// Benchmark-backed default publication target.
///
/// The same-host SHM exchange benchmarks show 64 KiB as the useful
/// general-purpose boundary: it amortizes one descriptor/readiness cycle
/// without turning a normal payload into one very large credit reservation.
/// It is deterministic policy about our internal data path, not an
/// external-network heuristic and not wire geometry.
pub const DEFAULT_CHUNK_BYTES: usize = 64 * 1024;

/// Physical payload geometry available to one exchange direction.
///
/// Runtime policy chooses how many slots one publication should span. The
/// resulting chunk capacity is always an exact multiple of the physical slot
/// payload size: `slots * slot_payload_bytes`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ChunkGeometry {
    slot_payload_bytes: usize,
    slots_per_lane: usize
}

impl ChunkGeometry {
    pub const fn new(
        slot_payload_bytes: usize,
        slots_per_lane: usize
    ) -> Option<Self> {
        if slot_payload_bytes == 0 || slots_per_lane == 0 {
            None
        } else {
            Some(Self { slot_payload_bytes, slots_per_lane })
        }
    }

    /// Usable payload bytes in one physical slot. Slot metadata lives outside
    /// this capacity.
    pub const fn slot_payload_bytes(self) -> usize {
        self.slot_payload_bytes
    }

    /// Maximum slots one publication may reserve from this node's lane.
    pub const fn slots_per_lane(self) -> usize {
        self.slots_per_lane
    }

    /// Exact publication capacity for a runtime-selected slot count.
    pub const fn chunk_bytes(
        self,
        slots: usize
    ) -> Option<usize> {
        if slots == 0 || slots > self.slots_per_lane {
            None
        } else {
            self.slot_payload_bytes.checked_mul(slots)
        }
    }

    /// Convert a byte target into a valid runtime slot count. The target is
    /// rounded down to whole slots, but never below one slot, and clamped to
    /// the node lane.
    pub const fn slots_for_target(
        self,
        target_bytes: usize
    ) -> Option<usize> {
        if target_bytes == 0 {
            return None;
        }
        let slots = target_bytes / self.slot_payload_bytes;
        let slots = if slots == 0 { 1 } else { slots };
        Some(if slots > self.slots_per_lane { self.slots_per_lane } else { slots })
    }

    /// Effective chunk capacity for a runtime byte target. This is always
    /// `n * slot_payload_bytes` for a valid lane-bounded `n`.
    pub const fn chunk_bytes_for_target(
        self,
        target_bytes: usize
    ) -> Option<usize> {
        let slots = match self.slots_for_target(target_bytes) {
            Some(slots) => slots,
            None => return None
        };
        self.chunk_bytes(slots)
    }

    /// Plan a known payload using a runtime-selected slot count.
    pub const fn plan(
        self,
        data_bytes: usize,
        slots_per_chunk: usize
    ) -> Option<ChunkPlan> {
        let chunk_bytes = match self.chunk_bytes(slots_per_chunk) {
            Some(bytes) => bytes,
            None => return None
        };
        ChunkPlan::for_limit(data_bytes, self.slot_payload_bytes, chunk_bytes)
    }

    /// Plan a known payload from a runtime byte target after aligning it to
    /// the physical slot payload geometry.
    pub const fn plan_for_target(
        self,
        data_bytes: usize,
        target_bytes: usize
    ) -> Option<ChunkPlan> {
        let slots = match self.slots_for_target(target_bytes) {
            Some(slots) => slots,
            None => return None
        };
        self.plan(data_bytes, slots)
    }
}

/// A request/response-agnostic plan for splitting a known payload into chunks.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ChunkPlan {
    data_bytes: usize,
    chunk_bytes: usize,
    chunk_count: usize,
    slots_per_chunk: usize,
    tail_bytes: usize,
    tail_slots: usize
}

impl ChunkPlan {
    /// Plan with the benchmark-backed 64 KiB target.
    pub const fn new(
        data_bytes: usize,
        slot_bytes: usize
    ) -> Option<Self> {
        Self::for_limit(data_bytes, slot_bytes, DEFAULT_CHUNK_BYTES)
    }

    /// Plan with a runtime-selected number of physical slots per chunk.
    pub const fn for_slots(
        data_bytes: usize,
        slot_payload_bytes: usize,
        slots_per_chunk: usize
    ) -> Option<Self> {
        let geometry = match ChunkGeometry::new(slot_payload_bytes, slots_per_chunk) {
            Some(geometry) => geometry,
            None => return None
        };
        geometry.plan(data_bytes, slots_per_chunk)
    }

    pub(crate) const fn for_limit(
        data_bytes: usize,
        slot_bytes: usize,
        limit_bytes: usize
    ) -> Option<Self> {
        if slot_bytes == 0 || limit_bytes == 0 {
            return None;
        }
        let target = if limit_bytes < slot_bytes { slot_bytes } else { limit_bytes };
        let chunk_bytes = (target / slot_bytes) * slot_bytes;
        if data_bytes == 0 {
            return Some(Self {
                data_bytes,
                chunk_bytes,
                chunk_count: 0,
                slots_per_chunk: chunk_bytes / slot_bytes,
                tail_bytes: 0,
                tail_slots: 0
            });
        }
        let chunk_count = data_bytes.div_ceil(chunk_bytes);
        let remainder = data_bytes % chunk_bytes;
        let tail_bytes = if remainder == 0 { chunk_bytes } else { remainder };
        Some(Self {
            data_bytes,
            chunk_bytes,
            chunk_count,
            slots_per_chunk: chunk_bytes / slot_bytes,
            tail_bytes,
            tail_slots: tail_bytes.div_ceil(slot_bytes)
        })
    }

    pub const fn data_bytes(self) -> usize {
        self.data_bytes
    }

    /// Maximum bytes in each full publication.
    pub const fn chunk_bytes(self) -> usize {
        self.chunk_bytes
    }

    pub const fn chunk_count(self) -> usize {
        self.chunk_count
    }

    pub const fn slots_per_chunk(self) -> usize {
        self.slots_per_chunk
    }

    /// Exact bytes in the last publication, or zero for an empty payload.
    pub const fn tail_bytes(self) -> usize {
        self.tail_bytes
    }

    pub const fn tail_slots(self) -> usize {
        self.tail_slots
    }

    /// Exact length of chunk `index`, if it exists.
    pub const fn chunk_len(
        self,
        index: usize
    ) -> Option<usize> {
        if index >= self.chunk_count {
            None
        } else if index + 1 == self.chunk_count {
            Some(self.tail_bytes)
        } else {
            Some(self.chunk_bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_plan_uses_benchmark_chunk_and_exact_tail() {
        let plan = ChunkPlan::new(150 * 1024, 4 * 1024).expect("plan");
        assert_eq!(plan.chunk_bytes(), 64 * 1024);
        assert_eq!(plan.slots_per_chunk(), 16);
        assert_eq!(plan.chunk_count(), 3);
        assert_eq!(plan.tail_bytes(), 22 * 1024);
        assert_eq!(plan.tail_slots(), 6);
        assert_eq!(plan.chunk_len(0), Some(64 * 1024));
        assert_eq!(plan.chunk_len(2), Some(22 * 1024));
        assert_eq!(plan.chunk_len(3), None);
    }

    #[test]
    fn a_slot_larger_than_the_default_is_never_split_artificially() {
        let plan = ChunkPlan::new(1024 * 1024, 128 * 1024).expect("plan");
        assert_eq!(plan.chunk_bytes(), 128 * 1024);
        assert_eq!(plan.slots_per_chunk(), 1);
        assert_eq!(plan.chunk_count(), 8);
    }

    #[test]
    fn small_and_empty_payloads_are_well_defined() {
        let small = ChunkPlan::new(1024, 4 * 1024).expect("plan");
        assert_eq!(small.chunk_count(), 1);
        assert_eq!(small.tail_bytes(), 1024);
        assert_eq!(small.tail_slots(), 1);

        let empty = ChunkPlan::new(0, 4 * 1024).expect("plan");
        assert_eq!(empty.chunk_count(), 0);
        assert_eq!(empty.tail_bytes(), 0);
    }

    #[test]
    fn arena_plan_clamps_full_chunks_to_one_lane() {
        let spec = crate::exchange::PayloadArenaSpec::new(200, 4, 4 * 1024);
        let plan = spec.plan_chunks(1024 * 1024).expect("plan");
        assert_eq!(plan.chunk_bytes(), 16 * 1024);
        assert_eq!(plan.slots_per_chunk(), 4);
        assert_eq!(plan.chunk_count(), 64);
    }

    #[test]
    fn runtime_slot_count_defines_exact_chunk_capacity() {
        let geometry = ChunkGeometry::new(4096, 32).expect("geometry");

        assert_eq!(geometry.slot_payload_bytes(), 4096);
        assert_eq!(geometry.chunk_bytes(7), Some(7 * 4096));
        assert_eq!(geometry.chunk_bytes(0), None);
        assert_eq!(geometry.chunk_bytes(33), None);

        let plan = geometry.plan(70_000, 7).expect("plan");
        assert_eq!(plan.chunk_bytes(), 7 * 4096);
        assert_eq!(plan.slots_per_chunk(), 7);
        assert_eq!(plan.chunk_count(), 3);
    }

    #[test]
    fn byte_target_is_aligned_and_clamped_to_runtime_geometry() {
        let geometry = ChunkGeometry::new(4096, 8).expect("geometry");

        assert_eq!(geometry.slots_for_target(1), Some(1));
        assert_eq!(geometry.slots_for_target(10_000), Some(2));
        assert_eq!(geometry.slots_for_target(1024 * 1024), Some(8));
        assert_eq!(geometry.chunk_bytes_for_target(10_000), Some(8192));
        assert_eq!(geometry.chunk_bytes_for_target(1024 * 1024), Some(32_768));
        assert_eq!(geometry.slots_for_target(0), None);
    }
}
