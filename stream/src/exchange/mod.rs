//! Paired, symmetric byte flows over one control channel and shared or
//! directional payload arenas. Each endpoint owns an outbound sender and an
//! inbound receiver.

mod arena;
mod pair;
mod protocol;

pub use arena::{
    PayloadArena, PayloadArenaSpec, PayloadChunk, segment_size_for as payload_segment_size_for
};
pub use pair::{
    DispatchError, ExchangeEndpoint, ExchangePayloadSpec, ExchangeSpec, ExchangeTicket, Exchanges,
    FlowEvent, FlowHandler, PendingData, PendingStart, Receiver, Sender
};
pub use protocol::{ChunkDescriptor, ControlEvent, ExchangeId, Flow, ResetCode};
