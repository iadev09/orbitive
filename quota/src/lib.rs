//! Bounded GCRA quota admission, independent of HTTP and authentication.
//!
//! Each [`MemoryQuota::new`] creates an independent store. Clones share its
//! fixed table and budget. Admission never waits for a contended shard and
//! never evicts live debt. On Unix, ShmQuota shares admission across processes.

mod budget;
mod clock;
mod contract;
mod gcra;
mod memory;
#[cfg(unix)]
mod shm;

pub use contract::{
    CasBudget, Decision, Error, Geometry, Key, Policy, QuotaStore,
};
pub use memory::MemoryQuota;
#[cfg(unix)]
pub use shm::{ShmOptions, ShmQuota, ShmResource};

#[cfg(test)]
#[path = "../tests/allocator.rs"]
mod test_allocator;
