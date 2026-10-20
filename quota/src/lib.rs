//! Bounded GCRA quota admission, independent of HTTP and authentication.
//!
//! Each [`MemoryQuota::new`] creates an independent store. Clones share its
//! fixed table and budget. Admission never waits for a contended shard and
//! never evicts live debt. On Unix, ShmQuota shares admission across processes.
//! LocalGuard and FleetGuard permit bounded cold-key replacement under table
//! pressure; FleetQuota retains live debt instead of reclaiming it.

mod budget;
mod clock;
mod contract;
mod gcra;
mod memory;
mod retention;
#[cfg(unix)]
mod shm;

pub use contract::{
    Admission, CasBudget, Decision, Error, Geometry, Key, Policy, QuotaStore,
    Snapshot,
};
pub use memory::{LocalGuard, MemoryQuota};
/// Fleet-wide quota with strict live-debt retention; the same store as ShmQuota.
#[cfg(unix)]
pub use shm::ShmQuota as FleetQuota;
#[cfg(unix)]
pub use shm::{FleetGuard, ShmOptions, ShmQuota, ShmResource};

#[cfg(test)]
#[path = "../tests/allocator.rs"]
mod test_allocator;
