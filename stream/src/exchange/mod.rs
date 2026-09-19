//! Paired request and response flows over one control channel and two
//! physically separate payload arenas.

mod protocol;

pub use protocol::{ChunkDescriptor, ControlEvent, ExchangeId, Flow, ResetCode};
