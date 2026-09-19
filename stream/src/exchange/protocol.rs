use std::fmt;

use crate::StreamId;

pub(crate) const CONTROL_FRAME_BYTES: usize = 64;

const MAGIC: [u8; 4] = *b"OEXC";
const VERSION: u8 = 1;
const EVENT_START: u8 = 1;
const EVENT_DATA: u8 = 2;
const EVENT_FIN: u8 = 3;
const EVENT_RESET: u8 = 4;
const FLOW_REQUEST: u8 = 1;
const FLOW_RESPONSE: u8 = 2;
const HAS_PAYLOAD: u8 = 1;

/// The identity shared by both directions of one exchange.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExchangeId(StreamId);

impl ExchangeId {
    pub const fn from_stream_id(id: StreamId) -> Self {
        Self(id)
    }

    pub const fn stream_id(self) -> StreamId {
        self.0
    }
}

impl fmt::Display for ExchangeId {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The two independent byte flows paired by an [`ExchangeId`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Flow {
    Request,
    Response
}

impl Flow {
    const fn wire(self) -> u8 {
        match self {
            Self::Request => FLOW_REQUEST,
            Self::Response => FLOW_RESPONSE
        }
    }

    fn from_wire(value: u8) -> Option<Self> {
        match value {
            FLOW_REQUEST => Some(Self::Request),
            FLOW_RESPONSE => Some(Self::Response),
            _ => None
        }
    }
}

/// Application-defined reason attached to a reset.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ResetCode(u32);

impl ResetCode {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// One immutable payload publication. Slots are physical allocation units;
/// this descriptor is the variable-sized chunk decision that spans them.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ChunkDescriptor {
    exchange: ExchangeId,
    flow: Flow,
    chunk_id: u64,
    allocation_generation: u64,
    first_slot: u32,
    slot_count: u32,
    payload_len: u32,
    arena_kind: u8,
    owner_node: u16
}

impl ChunkDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        exchange: ExchangeId,
        flow: Flow,
        chunk_id: u64,
        allocation_generation: u64,
        first_slot: u32,
        slot_count: u32,
        payload_len: u32,
        arena_kind: u8,
        owner_node: u16
    ) -> Self {
        Self {
            exchange,
            flow,
            chunk_id,
            allocation_generation,
            first_slot,
            slot_count,
            payload_len,
            arena_kind,
            owner_node
        }
    }

    pub const fn exchange(self) -> ExchangeId {
        self.exchange
    }

    pub const fn flow(self) -> Flow {
        self.flow
    }

    pub const fn chunk_id(self) -> u64 {
        self.chunk_id
    }

    pub const fn allocation_generation(self) -> u64 {
        self.allocation_generation
    }

    pub const fn first_slot(self) -> u32 {
        self.first_slot
    }

    pub const fn slot_count(self) -> u32 {
        self.slot_count
    }

    pub const fn payload_len(self) -> u32 {
        self.payload_len
    }

    pub const fn arena_kind(self) -> u8 {
        self.arena_kind
    }

    pub const fn owner_node(self) -> u16 {
        self.owner_node
    }
}

/// A lossless control-plane event. Payload bytes live in the arena named by
/// the descriptor and never in this frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlEvent {
    Start { exchange: ExchangeId, flow: Flow, metadata: Option<ChunkDescriptor> },
    Data(ChunkDescriptor),
    Fin { exchange: ExchangeId, flow: Flow },
    Reset { exchange: ExchangeId, flow: Flow, code: ResetCode }
}

impl ControlEvent {
    pub const fn exchange(self) -> ExchangeId {
        match self {
            Self::Start { exchange, .. }
            | Self::Fin { exchange, .. }
            | Self::Reset { exchange, .. } => exchange,
            Self::Data(descriptor) => descriptor.exchange()
        }
    }

    pub const fn flow(self) -> Flow {
        match self {
            Self::Start { flow, .. } | Self::Fin { flow, .. } | Self::Reset { flow, .. } => flow,
            Self::Data(descriptor) => descriptor.flow()
        }
    }

    pub(crate) fn encode(self) -> [u8; CONTROL_FRAME_BYTES] {
        let mut frame = [0_u8; CONTROL_FRAME_BYTES];
        frame[..4].copy_from_slice(&MAGIC);
        frame[4] = VERSION;
        frame[6] = self.flow().wire();
        put_u64(&mut frame, 8, self.exchange().stream_id().net_id().raw());

        match self {
            Self::Start { metadata, .. } => {
                frame[5] = EVENT_START;
                if let Some(descriptor) = metadata {
                    frame[7] = HAS_PAYLOAD;
                    encode_descriptor(&mut frame, descriptor);
                }
            }
            Self::Data(descriptor) => {
                frame[5] = EVENT_DATA;
                frame[7] = HAS_PAYLOAD;
                encode_descriptor(&mut frame, descriptor);
            }
            Self::Fin { .. } => frame[5] = EVENT_FIN,
            Self::Reset { code, .. } => {
                frame[5] = EVENT_RESET;
                put_u32(&mut frame, 50, code.get());
            }
        }
        frame
    }

    pub(crate) fn decode(frame: &[u8; CONTROL_FRAME_BYTES]) -> Result<Self, &'static str> {
        if frame[..4] != MAGIC {
            return Err("wrong exchange control magic");
        }
        if frame[4] != VERSION {
            return Err("unsupported exchange control version");
        }
        let flow = Flow::from_wire(frame[6]).ok_or("unknown exchange flow")?;
        let exchange = ExchangeId::from_stream_id(StreamId::from_net_id(
            orbit_core::NetId64::from_raw(get_u64(frame, 8))
        ));
        let has_payload = frame[7] & HAS_PAYLOAD != 0;
        match frame[5] {
            EVENT_START => Ok(Self::Start {
                exchange,
                flow,
                metadata: has_payload.then(|| decode_descriptor(frame, exchange, flow))
            }),
            EVENT_DATA if has_payload => Ok(Self::Data(decode_descriptor(frame, exchange, flow))),
            EVENT_DATA => Err("data event has no payload descriptor"),
            EVENT_FIN if !has_payload => Ok(Self::Fin { exchange, flow }),
            EVENT_RESET if !has_payload => {
                Ok(Self::Reset { exchange, flow, code: ResetCode::new(get_u32(frame, 50)) })
            }
            EVENT_FIN | EVENT_RESET => Err("terminal event carries a payload descriptor"),
            _ => Err("unknown exchange event")
        }
    }
}

fn encode_descriptor(
    frame: &mut [u8; CONTROL_FRAME_BYTES],
    descriptor: ChunkDescriptor
) {
    put_u64(frame, 16, descriptor.chunk_id());
    put_u64(frame, 24, descriptor.allocation_generation());
    put_u32(frame, 32, descriptor.first_slot());
    put_u32(frame, 36, descriptor.slot_count());
    put_u32(frame, 40, descriptor.payload_len());
    frame[44] = descriptor.arena_kind();
    frame[48..50].copy_from_slice(&descriptor.owner_node().to_le_bytes());
}

fn decode_descriptor(
    frame: &[u8; CONTROL_FRAME_BYTES],
    exchange: ExchangeId,
    flow: Flow
) -> ChunkDescriptor {
    ChunkDescriptor::new(
        exchange,
        flow,
        get_u64(frame, 16),
        get_u64(frame, 24),
        get_u32(frame, 32),
        get_u32(frame, 36),
        get_u32(frame, 40),
        frame[44],
        u16::from_le_bytes(frame[48..50].try_into().expect("two bytes"))
    )
}

fn put_u32(
    frame: &mut [u8; CONTROL_FRAME_BYTES],
    at: usize,
    value: u32
) {
    frame[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(
    frame: &mut [u8; CONTROL_FRAME_BYTES],
    at: usize,
    value: u64
) {
    frame[at..at + 8].copy_from_slice(&value.to_le_bytes());
}

fn get_u32(
    frame: &[u8; CONTROL_FRAME_BYTES],
    at: usize
) -> u32 {
    u32::from_le_bytes(frame[at..at + 4].try_into().expect("four bytes"))
}

fn get_u64(
    frame: &[u8; CONTROL_FRAME_BYTES],
    at: usize
) -> u64 {
    u64::from_le_bytes(frame[at..at + 8].try_into().expect("eight bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exchange() -> ExchangeId {
        ExchangeId::from_stream_id(StreamId::from_net_id(orbit_core::NetId64::make(240, 3, 91)))
    }

    fn descriptor(flow: Flow) -> ChunkDescriptor {
        ChunkDescriptor::new(exchange(), flow, 7, 19, 31, 4, 777, 241, 5)
    }

    #[test]
    fn every_control_event_survives_its_fixed_frame() {
        let events = [
            ControlEvent::Start {
                exchange: exchange(),
                flow: Flow::Request,
                metadata: Some(descriptor(Flow::Request))
            },
            ControlEvent::Data(descriptor(Flow::Response)),
            ControlEvent::Fin { exchange: exchange(), flow: Flow::Request },
            ControlEvent::Reset {
                exchange: exchange(),
                flow: Flow::Response,
                code: ResetCode::new(503)
            }
        ];
        for event in events {
            assert_eq!(ControlEvent::decode(&event.encode()), Ok(event));
        }
    }

    #[test]
    fn control_frames_refuse_unknown_or_incomplete_events() {
        let mut frame = ControlEvent::Data(descriptor(Flow::Request)).encode();
        frame[7] = 0;
        assert_eq!(ControlEvent::decode(&frame), Err("data event has no payload descriptor"));
        frame = ControlEvent::Fin { exchange: exchange(), flow: Flow::Request }.encode();
        frame[6] = 99;
        assert_eq!(ControlEvent::decode(&frame), Err("unknown exchange flow"));
    }
}
