//! Paired request and response flows over one control channel and two
//! physically separate payload arenas.

mod arena;
mod protocol;

pub use arena::{
    PayloadArena, PayloadArenaSpec, PayloadChunk, segment_size_for as payload_segment_size_for,
};
pub use protocol::{ChunkDescriptor, ControlEvent, ExchangeId, Flow, ResetCode};
