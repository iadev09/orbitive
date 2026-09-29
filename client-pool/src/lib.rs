//! Protocol-neutral outbound client pools for asynchronous applications.
//!
//! [`ClientPool`] owns process-local client objects. Creating the pool performs
//! no IO; [`ClientPool::acquire`] creates on demand and [`ClientPool::run`]
//! maintains the configured warm floor during the runtime phase.

mod error;
#[cfg(feature = "fleet")]
mod fleet;
mod options;
mod policy;
mod pool;

pub use error::{AcquireError, OptionsError};
#[cfg(feature = "fleet")]
pub use fleet::{
    FleetAcquireError, FleetClient, FleetClientLease, FleetClientPool, FleetRemoteClient,
    IncomingClientSession
};
#[cfg(feature = "fleet")]
pub use options::FleetPoolOptions;
pub use options::PoolOptions;
pub use policy::{ClientPolicy, PoolDirective};
pub use pool::{ActiveRequestPolicy, ClientLease, ClientManager, ClientPool, PoolState, PoolStats};
