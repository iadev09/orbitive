//! Bounded GCRA quota admission, independent of HTTP and authentication.
//!
//! Each [`MemoryQuota::new`] creates an independent store. Clones share its
//! fixed table and budget. Admission never waits for a contended shard and
//! never evicts live debt. Shared-memory storage is not implemented yet.

mod clock;
mod contract;
mod gcra;
mod memory;

pub use contract::{
    CasBudget, Decision, Error, Geometry, Key, Policy, QuotaStore,
};
pub use memory::MemoryQuota;
