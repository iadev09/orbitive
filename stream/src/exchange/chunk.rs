/// Benchmark-backed default publication target.
///
/// The same-host SHM exchange benchmarks show 64 KiB as the useful
/// general-purpose boundary: it amortizes one descriptor/readiness cycle
/// without turning a normal payload into one very large credit reservation.
/// It is deterministic policy about our internal data path, not an
/// external-network heuristic and not wire geometry.
pub const DEFAULT_CHUNK_BYTES: usize = 64 * 1024;

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
}
