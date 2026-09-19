use std::fmt;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use orbit_core::{Fleet, NodeId};

use super::arena::Publication;
use super::protocol::CONTROL_FRAME_BYTES;
use super::{
    ChunkDescriptor, ChunkPlan, ControlEvent, ExchangeId, Flow, PayloadArena, PayloadArenaSpec,
    PayloadChunk, ResetCode
};
use crate::{Error, Incarnation, ReadHalf, Result, Side, StreamSpec, Streams, Ticket, WriteHalf};

/// How an exchange budgets payload memory for its two directions.
///
/// The shared form is the compact generic default. Directional arenas are for
/// adapters whose two flows have deliberately different budgets (for example
/// an upload and a streamed response) or must not consume each other's
/// credit.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExchangePayloadSpec {
    Shared(PayloadArenaSpec),
    Directional { a_to_b: PayloadArenaSpec, b_to_a: PayloadArenaSpec }
}

/// The physical resources used by an exchange table.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExchangeSpec {
    /// One lossless, bounded control table. Its two byte directions carry
    /// fixed-size typed events, never application payload bytes.
    pub control: StreamSpec,
    /// Payload memory may be shared by both directions or split into two
    /// independently bounded arenas.
    pub payload: ExchangePayloadSpec
}

impl ExchangeSpec {
    pub const fn new(
        control: StreamSpec,
        payload: PayloadArenaSpec
    ) -> Self {
        Self { control, payload: ExchangePayloadSpec::Shared(payload) }
    }

    pub const fn directional(
        control: StreamSpec,
        a_to_b: PayloadArenaSpec,
        b_to_a: PayloadArenaSpec
    ) -> Self {
        Self { control, payload: ExchangePayloadSpec::Directional { a_to_b, b_to_a } }
    }

    fn validate(self) -> Result<()> {
        if self.control.buffer_bytes < CONTROL_FRAME_BYTES {
            return Err(Error::Malformed(format!(
                "exchange control ring is {} bytes; one control frame is {CONTROL_FRAME_BYTES}",
                self.control.buffer_bytes
            )));
        }
        match self.payload {
            ExchangePayloadSpec::Shared(payload) if self.control.kind == payload.kind => {
                return Err(Error::Malformed(
                    "exchange control and payload need distinct SHM kinds".to_owned()
                ));
            }
            ExchangePayloadSpec::Directional { a_to_b, b_to_a }
                if self.control.kind == a_to_b.kind
                    || self.control.kind == b_to_a.kind
                    || a_to_b.kind == b_to_a.kind =>
            {
                return Err(Error::Malformed(
                    "exchange control and directional payloads need three distinct SHM kinds"
                        .to_owned()
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

/// The client representative's capability to open the other side of an
/// exchange. It may travel through an Orbit offer or any setup channel.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExchangeTicket(Ticket);

impl ExchangeTicket {
    pub const fn from_stream_ticket(ticket: Ticket) -> Self {
        Self(ticket)
    }

    pub const fn stream_ticket(self) -> Ticket {
        self.0
    }

    pub const fn id(self) -> ExchangeId {
        ExchangeId::from_stream_id(self.0.id)
    }
}

impl fmt::Display for ExchangeTicket {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ExchangeTicket {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        text.parse().map(Self)
    }
}

#[derive(Clone)]
enum PayloadArenas {
    Shared(PayloadArena),
    Directional { a_to_b: PayloadArena, b_to_a: PayloadArena }
}

impl PayloadArenas {
    fn for_flow(
        &self,
        flow: Flow
    ) -> PayloadArena {
        match (self, flow) {
            (Self::Shared(arena), _) => arena.clone(),
            (Self::Directional { a_to_b, .. }, Flow::AtoB) => a_to_b.clone(),
            (Self::Directional { b_to_a, .. }, Flow::BtoA) => b_to_a.clone()
        }
    }

    fn node_dead(
        &self,
        node: NodeId,
        incarnation: Incarnation
    ) {
        match self {
            Self::Shared(arena) => arena.node_dead(node, incarnation),
            Self::Directional { a_to_b, b_to_a } => {
                a_to_b.node_dead(node, incarnation);
                b_to_a.node_dead(node, incarnation);
            }
        }
    }

    fn reset_all(&self) {
        match self {
            Self::Shared(arena) => arena.reset_all(),
            Self::Directional { a_to_b, b_to_a } => {
                a_to_b.reset_all();
                b_to_a.reset_all();
            }
        }
    }

    #[cfg(unix)]
    fn unlink(&self) -> Result<()> {
        match self {
            Self::Shared(arena) => arena.unlink(),
            Self::Directional { a_to_b, b_to_a } => {
                a_to_b.unlink()?;
                b_to_a.unlink()
            }
        }
    }
}

/// One fleet's exchange transport: one duplex control table and either one
/// shared payload arena or one independently bounded arena per direction.
#[derive(Clone)]
pub struct Exchanges {
    control: Streams,
    payload: PayloadArenas
}

impl Exchanges {
    pub fn open(
        fleet: Arc<Fleet>,
        incarnation: Incarnation,
        spec: ExchangeSpec
    ) -> Result<Self> {
        spec.validate()?;
        let control = Streams::with_spec(Arc::clone(&fleet), incarnation, spec.control)?;
        let payload = match spec.payload {
            ExchangePayloadSpec::Shared(payload) => {
                PayloadArenas::Shared(PayloadArena::open(fleet, incarnation, payload)?)
            }
            ExchangePayloadSpec::Directional { a_to_b, b_to_a } => PayloadArenas::Directional {
                a_to_b: PayloadArena::open(Arc::clone(&fleet), incarnation, a_to_b)?,
                b_to_a: PayloadArena::open(fleet, incarnation, b_to_a)?
            }
        };
        Ok(Self { control, payload })
    }

    /// Create side A. Its sender writes A-to-B and its receiver reads B-to-A.
    /// The ticket opens side B.
    pub fn create(&self) -> Result<(ExchangeEndpoint, ExchangeTicket)> {
        let (endpoint, ticket) = self.control.create()?;
        Ok((self.endpoint(endpoint), ExchangeTicket(ticket)))
    }

    /// Open side B. Its sender writes B-to-A and its receiver reads A-to-B.
    /// Both directions have independent lifecycle and may start in either order.
    pub fn open_peer(
        &self,
        ticket: ExchangeTicket
    ) -> Result<ExchangeEndpoint> {
        let endpoint = self.control.open(ticket.0)?;
        Ok(self.endpoint(endpoint))
    }

    fn endpoint(
        &self,
        endpoint: crate::Endpoint
    ) -> ExchangeEndpoint {
        let id = ExchangeId::from_stream_id(endpoint.id());
        let (send_flow, receive_flow) = match endpoint.side() {
            Side::A => (Flow::AtoB, Flow::BtoA),
            Side::B => (Flow::BtoA, Flow::AtoB)
        };
        let (read, write) = endpoint.split();
        ExchangeEndpoint {
            sender: Sender(Producer::new(id, send_flow, self.payload.for_flow(send_flow), write)),
            receiver: Receiver(Consumer::new(
                id,
                receive_flow,
                self.payload.for_flow(receive_flow),
                read
            ))
        }
    }

    pub fn offer(
        &self,
        ticket: ExchangeTicket,
        to: NodeId
    ) -> Result<()> {
        self.control.offer(ticket.0, to)
    }

    pub fn take_offer(&self) -> Option<ExchangeTicket> {
        self.control.take_offer().map(ExchangeTicket)
    }

    /// Park until an exchange is offered to this node.
    pub fn blocking_take_offer(&self) -> Result<ExchangeTicket> {
        self.control.blocking_take_offer().map(ExchangeTicket)
    }

    /// Wait at most `timeout` for an offered exchange.
    #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    pub fn take_offer_timeout(
        &self,
        timeout: Duration
    ) -> Result<Option<ExchangeTicket>> {
        self.control.take_offer_timeout(timeout).map(|ticket| ticket.map(ExchangeTicket))
    }

    /// Task readiness for the next offered exchange.
    pub fn poll_take_offer(
        &self,
        cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Result<ExchangeTicket>> {
        match self.control.poll_take_offer(cx) {
            std::task::Poll::Ready(result) => std::task::Poll::Ready(result.map(ExchangeTicket)),
            std::task::Poll::Pending => std::task::Poll::Pending
        }
    }

    pub fn control(&self) -> &Streams {
        &self.control
    }

    /// Apply a confirmed process-death report to control ownership and every
    /// payload arena. This is never inferred from a replacement attaching.
    pub fn node_dead(
        &self,
        node: NodeId,
        incarnation: Incarnation
    ) {
        self.control.node_dead(node, incarnation);
        self.payload.node_dead(node, incarnation);
    }

    /// Quiescent-owner maintenance only. Attaching a replacement process
    /// never resets an exchange table or either payload arena implicitly.
    pub fn reset_all(&self) {
        self.control.reset_all();
        self.payload.reset_all();
    }

    /// Remove the control and payload SHM names. Existing mappings remain
    /// valid.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        self.control.unlink()?;
        self.payload.unlink()
    }
}

/// One held exchange side. A worker may hold endpoints on either side of many
/// exchanges at once; sender and receiver are capabilities, not worker roles.
pub struct ExchangeEndpoint {
    sender: Sender,
    receiver: Receiver
}

impl ExchangeEndpoint {
    pub fn id(&self) -> ExchangeId {
        self.sender.id()
    }

    pub fn sender(&mut self) -> &mut Sender {
        &mut self.sender
    }

    pub fn receiver(&mut self) -> &mut Receiver {
        &mut self.receiver
    }

    pub fn split(self) -> (Sender, Receiver) {
        (self.sender, self.receiver)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProducerState {
    New,
    Open,
    Terminal
}

struct Producer {
    exchange: ExchangeId,
    flow: Flow,
    arena: PayloadArena,
    control: WriteHalf,
    next_chunk: u64,
    state: ProducerState
}

impl Producer {
    fn new(
        exchange: ExchangeId,
        flow: Flow,
        arena: PayloadArena,
        control: WriteHalf
    ) -> Self {
        Self { exchange, flow, arena, control, next_chunk: 1, state: ProducerState::New }
    }

    fn id(&self) -> ExchangeId {
        self.exchange
    }

    fn start(
        &mut self,
        metadata: Option<&[u8]>
    ) -> Result<()> {
        if self.state != ProducerState::New {
            return Err(Error::Malformed("flow start was already sent".to_owned()));
        }
        if let Some(bytes) = metadata.filter(|bytes| !bytes.is_empty()) {
            let mut pending = self.reserve_start(bytes.len())?;
            pending.copy_from_slice(bytes);
            pending.commit()?;
        } else {
            self.send(ControlEvent::Start {
                exchange: self.exchange,
                flow: self.flow,
                metadata: None
            })?;
            self.state = ProducerState::Open;
        }
        Ok(())
    }

    fn data(
        &mut self,
        payload: &[u8]
    ) -> Result<ChunkDescriptor> {
        let mut pending = self.reserve_data(payload.len())?;
        pending.copy_from_slice(payload);
        pending.commit()
    }

    fn reserve_start(
        &mut self,
        payload_len: usize
    ) -> Result<PendingStart<'_>> {
        if self.state != ProducerState::New {
            return Err(Error::Malformed("flow start was already sent".to_owned()));
        }
        let publication = self.arena.reserve(payload_len)?;
        Ok(PendingStart { producer: self, publication })
    }

    fn reserve_data(
        &mut self,
        payload_len: usize
    ) -> Result<PendingData<'_>> {
        if self.state != ProducerState::Open {
            return Err(Error::Malformed("flow data requires an open flow".to_owned()));
        }
        let publication = self.arena.reserve(payload_len)?;
        Ok(PendingData { producer: self, publication })
    }

    fn commit_start(
        &mut self,
        publication: Publication
    ) -> Result<ChunkDescriptor> {
        let descriptor = self.descriptor(&publication);
        publication.make_live();
        self.send(ControlEvent::Start {
            exchange: self.exchange,
            flow: self.flow,
            metadata: Some(descriptor)
        })?;
        publication.mark_published();
        self.next_chunk += 1;
        self.state = ProducerState::Open;
        Ok(descriptor)
    }

    fn commit_data(
        &mut self,
        publication: Publication
    ) -> Result<ChunkDescriptor> {
        let descriptor = self.descriptor(&publication);
        publication.make_live();
        self.send(ControlEvent::Data(descriptor))?;
        publication.mark_published();
        self.next_chunk += 1;
        Ok(descriptor)
    }

    fn finish(&mut self) -> Result<()> {
        if self.state != ProducerState::Open {
            return Err(Error::Malformed("flow FIN requires an open flow".to_owned()));
        }
        self.send(ControlEvent::Fin { exchange: self.exchange, flow: self.flow })?;
        self.control.finish()?;
        self.state = ProducerState::Terminal;
        Ok(())
    }

    fn reset(
        &mut self,
        code: ResetCode
    ) -> Result<()> {
        if self.state == ProducerState::Terminal {
            return Err(Error::Closed);
        }
        self.send(ControlEvent::Reset { exchange: self.exchange, flow: self.flow, code })?;
        self.control.finish()?;
        self.state = ProducerState::Terminal;
        Ok(())
    }

    fn send(
        &self,
        event: ControlEvent
    ) -> Result<()> {
        self.control.try_write_exact(&event.encode())
    }

    fn descriptor(
        &self,
        publication: &Publication
    ) -> ChunkDescriptor {
        let (generation, first_slot, slot_count, payload_len, arena_kind, owner_node) =
            publication.coordinates();
        ChunkDescriptor::new(
            self.exchange,
            self.flow,
            self.next_chunk,
            generation,
            first_slot,
            slot_count,
            payload_len,
            arena_kind,
            owner_node
        )
    }
}

/// A flow-start payload reserved directly in the shared SHM arena. The bytes
/// become immutable and visible only when `commit` sends the matching start
/// event. Dropping this value returns the reserved slots.
pub struct PendingStart<'a> {
    producer: &'a mut Producer,
    publication: Publication
}

impl PendingStart<'_> {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.publication.as_mut_slice()
    }

    pub fn commit(self) -> Result<ChunkDescriptor> {
        self.producer.commit_start(self.publication)
    }

    /// Commit only the initialized prefix of a larger reservation. Any whole
    /// trailing slots that prefix does not need return to the arena first.
    pub fn commit_len(
        mut self,
        payload_len: usize
    ) -> Result<ChunkDescriptor> {
        self.publication.truncate(payload_len)?;
        self.producer.commit_start(self.publication)
    }
}

impl Deref for PendingStart<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.publication.as_slice()
    }
}

impl DerefMut for PendingStart<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

/// A data chunk reserved directly in the shared SHM arena. One commit publishes
/// the complete variable-sized chunk and one descriptor.
pub struct PendingData<'a> {
    producer: &'a mut Producer,
    publication: Publication
}

impl PendingData<'_> {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.publication.as_mut_slice()
    }

    pub fn commit(self) -> Result<ChunkDescriptor> {
        self.producer.commit_data(self.publication)
    }

    /// Commit only the initialized prefix of a larger reservation. This is
    /// the direct-read path for producers that learn the byte count from IO.
    pub fn commit_len(
        mut self,
        payload_len: usize
    ) -> Result<ChunkDescriptor> {
        self.publication.truncate(payload_len)?;
        self.producer.commit_data(self.publication)
    }
}

impl Deref for PendingData<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.publication.as_slice()
    }
}

impl DerefMut for PendingData<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConsumerState {
    New,
    Open,
    Terminal
}

struct Consumer {
    exchange: ExchangeId,
    flow: Flow,
    arena: PayloadArena,
    control: ReadHalf,
    next_chunk: u64,
    state: ConsumerState
}

impl Consumer {
    fn new(
        exchange: ExchangeId,
        flow: Flow,
        arena: PayloadArena,
        control: ReadHalf
    ) -> Self {
        Self { exchange, flow, arena, control, next_chunk: 1, state: ConsumerState::New }
    }

    fn id(&self) -> ExchangeId {
        self.exchange
    }

    fn next(&mut self) -> Result<FlowEvent> {
        if self.state == ConsumerState::Terminal {
            return Err(Error::Closed);
        }
        let mut frame = [0_u8; CONTROL_FRAME_BYTES];
        self.control.try_read_exact(&mut frame)?;
        let event =
            ControlEvent::decode(&frame).map_err(|reason| Error::Malformed(reason.to_owned()))?;
        if event.exchange() != self.exchange || event.flow() != self.flow {
            return Err(Error::Malformed(
                "control event belongs to another exchange flow".to_owned()
            ));
        }
        match event {
            ControlEvent::Start { metadata, .. } if self.state == ConsumerState::New => {
                let metadata =
                    metadata.map(|descriptor| self.take_chunk(descriptor)).transpose()?;
                self.state = ConsumerState::Open;
                Ok(FlowEvent::Start { metadata })
            }
            ControlEvent::Data(descriptor) if self.state == ConsumerState::Open => {
                let chunk = self.take_chunk(descriptor)?;
                Ok(FlowEvent::Data(chunk))
            }
            ControlEvent::Fin { .. } if self.state == ConsumerState::Open => {
                self.state = ConsumerState::Terminal;
                Ok(FlowEvent::Fin)
            }
            ControlEvent::Reset { code, .. } => {
                self.state = ConsumerState::Terminal;
                Ok(FlowEvent::Reset(code))
            }
            _ => Err(Error::Malformed("control event violates the flow lifecycle".to_owned()))
        }
    }

    fn take_chunk(
        &mut self,
        descriptor: ChunkDescriptor
    ) -> Result<PayloadChunk> {
        if descriptor.chunk_id() != self.next_chunk {
            return Err(Error::Malformed(format!(
                "flow expected chunk {}, received {}",
                self.next_chunk,
                descriptor.chunk_id()
            )));
        }
        let chunk = self.arena.read(descriptor)?;
        self.next_chunk += 1;
        Ok(chunk)
    }
}

/// The event program seen by a flow consumer.
pub enum FlowEvent {
    Start { metadata: Option<PayloadChunk> },
    Data(PayloadChunk),
    Fin,
    Reset(ResetCode)
}

/// Typed callbacks for one flow. A handler decides what its events mean; the
/// transport does not encode application sequencing policy.
pub trait FlowHandler {
    type Error;

    fn on_start(
        &mut self,
        metadata: Option<PayloadChunk>
    ) -> std::result::Result<(), Self::Error>;
    fn on_data(
        &mut self,
        chunk: PayloadChunk
    ) -> std::result::Result<(), Self::Error>;
    fn on_fin(&mut self) -> std::result::Result<(), Self::Error>;
    fn on_reset(
        &mut self,
        code: ResetCode
    ) -> std::result::Result<(), Self::Error>;
}

#[derive(Debug)]
pub enum DispatchError<E> {
    Transport(Error),
    Handler(E)
}

impl<E> From<Error> for DispatchError<E> {
    fn from(value: Error) -> Self {
        Self::Transport(value)
    }
}

fn dispatch<H: FlowHandler>(
    consumer: &mut Consumer,
    handler: &mut H
) -> std::result::Result<(), DispatchError<H::Error>> {
    match consumer.next()? {
        FlowEvent::Start { metadata } => handler.on_start(metadata),
        FlowEvent::Data(chunk) => handler.on_data(chunk),
        FlowEvent::Fin => handler.on_fin(),
        FlowEvent::Reset(code) => handler.on_reset(code)
    }
    .map_err(DispatchError::Handler)
}

/// The capability to publish one endpoint's outbound flow.
pub struct Sender(Producer);

impl Sender {
    pub fn id(&self) -> ExchangeId {
        self.0.id()
    }

    pub fn flow(&self) -> Flow {
        self.0.flow
    }

    /// Plan a known payload for this direction's actual arena geometry.
    pub fn plan_chunks(
        &self,
        data_bytes: usize
    ) -> Result<ChunkPlan> {
        self.0.arena.plan_chunks(data_bytes)
    }
    pub fn start(
        &mut self,
        metadata: Option<&[u8]>
    ) -> Result<()> {
        self.0.start(metadata)
    }

    /// Reserve start metadata directly in the shared payload arena. No start
    /// event is visible until the returned guard is committed.
    pub fn reserve_start(
        &mut self,
        payload_len: usize
    ) -> Result<PendingStart<'_>> {
        self.0.reserve_start(payload_len)
    }

    pub fn data(
        &mut self,
        payload: &[u8]
    ) -> Result<ChunkDescriptor> {
        self.0.data(payload)
    }

    /// Reserve one variable-sized data chunk directly in the payload arena.
    /// One commit emits one data descriptor.
    pub fn reserve_data(
        &mut self,
        payload_len: usize
    ) -> Result<PendingData<'_>> {
        self.0.reserve_data(payload_len)
    }

    pub fn finish(&mut self) -> Result<()> {
        self.0.finish()
    }

    pub fn reset(
        &mut self,
        code: ResetCode
    ) -> Result<()> {
        self.0.reset(code)
    }

    pub fn wait_writable(&self) -> Result<()> {
        self.0.control.wait_writable_exact(CONTROL_FRAME_BYTES)
    }

    pub fn poll_writable(
        &self,
        cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Result<()>> {
        self.0.control.poll_writable_exact(CONTROL_FRAME_BYTES, cx)
    }

    /// Wait for both one control frame and the payload credit needed by the
    /// next publication. Use zero for FIN, RESET, or a start without metadata.
    pub fn wait_ready(
        &self,
        payload_len: usize
    ) -> Result<()> {
        self.0.control.wait_writable_exact(CONTROL_FRAME_BYTES)?;
        if payload_len != 0 {
            self.0.arena.wait_available(payload_len)?;
        }
        Ok(())
    }

    /// Task readiness for the same combined control and payload capacity.
    /// Both sources register before `Pending` is returned.
    pub fn poll_ready(
        &self,
        payload_len: usize,
        cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Result<()>> {
        let control = self.0.control.poll_writable_exact(CONTROL_FRAME_BYTES, cx);
        let payload = if payload_len == 0 {
            std::task::Poll::Ready(Ok(()))
        } else {
            self.0.arena.poll_available(payload_len, cx)
        };
        match (control, payload) {
            (std::task::Poll::Ready(Err(error)), _) | (_, std::task::Poll::Ready(Err(error))) => {
                std::task::Poll::Ready(Err(error))
            }
            (std::task::Poll::Ready(Ok(())), std::task::Poll::Ready(Ok(()))) => {
                std::task::Poll::Ready(Ok(()))
            }
            _ => std::task::Poll::Pending
        }
    }
}

/// The capability to consume the peer endpoint's outbound flow.
pub struct Receiver(Consumer);

impl Receiver {
    pub fn id(&self) -> ExchangeId {
        self.0.id()
    }

    pub fn flow(&self) -> Flow {
        self.0.flow
    }

    pub fn try_next(&mut self) -> Result<FlowEvent> {
        self.0.next()
    }

    pub fn dispatch<H: FlowHandler>(
        &mut self,
        handler: &mut H
    ) -> std::result::Result<(), DispatchError<H::Error>> {
        dispatch(&mut self.0, handler)
    }

    pub fn wait_readable(&self) -> Result<()> {
        self.0.control.wait_readable()
    }

    pub fn poll_readable(
        &self,
        cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Result<()>> {
        self.0.control.poll_readable(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exchanges() -> Exchanges {
        Exchanges::open(
            Arc::new(Fleet::join("exchange-pair-test", 2).expect("fleet")),
            Incarnation::new(1),
            ExchangeSpec::new(StreamSpec::new(230, 8, 512), PayloadArenaSpec::new(231, 8, 64))
        )
        .expect("exchanges")
    }

    #[test]
    fn peer_can_send_before_the_first_flow_finishes() {
        let exchanges = exchanges();
        let (server, ticket) = exchanges.create().expect("server");
        let client = exchanges.open_peer(ticket).expect("client");
        let (mut request_out, mut response_in) = server.split();
        let (mut response_out, mut request_in) = client.split();

        request_out.start(Some(b"request-head")).expect("request start");
        let request_descriptor = request_out.data(&[7_u8; 192]).expect("request data");
        let request_metadata = match request_in.try_next().expect("request start event") {
            FlowEvent::Start { metadata: Some(metadata) } => metadata,
            _ => panic!("expected request start")
        };
        assert_eq!(&*request_metadata, b"request-head");
        let request_chunk = match request_in.try_next().expect("request data event") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected request data")
        };
        assert_eq!(request_descriptor.arena_kind(), 231);
        assert_eq!(request_descriptor.slot_count(), 3);

        // Both directions share one arena, but their flow lifecycles are
        // independent, so the peer can start before the first flow finishes.
        response_out.start(Some(b"response-head")).expect("early response start");
        let response_descriptor = response_out.data(b"response").expect("response data");
        assert_eq!(response_descriptor.arena_kind(), 231);
        assert!(matches!(response_in.try_next(), Ok(FlowEvent::Start { .. })));
        let response_chunk = match response_in.try_next().expect("response data event") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected response data")
        };
        assert_eq!(&*response_chunk, b"response");

        drop(request_chunk);
        request_out.finish().expect("request fin");
        assert!(matches!(request_in.try_next(), Ok(FlowEvent::Fin)));
        response_out.finish().expect("response fin");
        assert!(matches!(response_in.try_next(), Ok(FlowEvent::Fin)));
    }

    #[test]
    fn directional_payloads_keep_each_flows_credit_and_kind_separate() {
        let exchanges = Exchanges::open(
            Arc::new(Fleet::join("exchange-directional-test", 2).expect("fleet")),
            Incarnation::new(1),
            ExchangeSpec::directional(
                StreamSpec::new(220, 8, 512),
                PayloadArenaSpec::new(221, 1, 64),
                PayloadArenaSpec::new(222, 1, 128)
            )
        )
        .expect("directional exchanges");
        let (mut side_a, ticket) = exchanges.create().expect("side A");
        let mut side_b = exchanges.open_peer(ticket).expect("side B");

        side_a.sender().start(None).expect("A-to-B start");
        assert!(matches!(side_b.receiver().try_next(), Ok(FlowEvent::Start { .. })));
        side_b.sender().start(None).expect("B-to-A start");
        assert!(matches!(side_a.receiver().try_next(), Ok(FlowEvent::Start { .. })));

        let a = side_a.sender().data(&[1; 64]).expect("A-to-B payload");
        let b = side_b.sender().data(&[2; 128]).expect("B-to-A payload");
        assert_eq!(a.arena_kind(), 221);
        assert_eq!(b.arena_kind(), 222);

        let a_payload = match side_b.receiver().try_next().expect("A-to-B data") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected A-to-B data")
        };
        let b_payload = match side_a.receiver().try_next().expect("B-to-A data") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected B-to-A data")
        };
        assert_eq!(&*a_payload, &[1; 64]);
        assert_eq!(&*b_payload, &[2; 128]);
    }

    #[test]
    fn a_reserved_chunk_is_one_descriptor_over_many_slots() {
        let exchanges = Exchanges::open(
            Arc::new(Fleet::join("exchange-reservation-test", 2).expect("fleet")),
            Incarnation::new(1),
            ExchangeSpec::new(StreamSpec::new(233, 8, 512), PayloadArenaSpec::new(234, 8, 256))
        )
        .expect("exchanges");
        let (mut server, ticket) = exchanges.create().expect("server");
        let mut client = exchanges.open_peer(ticket).expect("client");

        let expected: Vec<u8> = (0..777).map(|index| (index % 251) as u8).collect();
        let descriptor = {
            let mut pending = server.sender().reserve_start(expected.len()).expect("reserve");
            pending.copy_from_slice(&expected);
            pending.commit().expect("commit")
        };
        assert_eq!(descriptor.payload_len(), 777);
        assert_eq!(descriptor.slot_count(), 4);
        let received = match client.receiver().try_next().expect("start") {
            FlowEvent::Start { metadata: Some(metadata) } => metadata,
            _ => panic!("expected start metadata")
        };
        assert_eq!(&*received, expected);
    }

    #[test]
    fn dropping_an_uncommitted_chunk_returns_credit_without_an_event() {
        let exchanges = exchanges();
        let (mut server, ticket) = exchanges.create().expect("server");
        let mut client = exchanges.open_peer(ticket).expect("client");
        server.sender().start(None).expect("start");
        assert!(matches!(client.receiver().try_next(), Ok(FlowEvent::Start { metadata: None })));

        let pending = server.sender().reserve_data(256).expect("reserve all slots");
        drop(pending);
        assert!(matches!(client.receiver().try_next(), Err(Error::WouldBlock)));
        assert!(server.sender().reserve_data(256).is_ok());
    }

    #[test]
    fn a_failed_control_commit_returns_its_payload_credit() {
        let exchanges = Exchanges::open(
            Arc::new(Fleet::join("exchange-failed-commit-test", 2).expect("fleet")),
            Incarnation::new(1),
            ExchangeSpec::new(
                StreamSpec::new(236, 8, CONTROL_FRAME_BYTES),
                PayloadArenaSpec::new(237, 1, 64)
            )
        )
        .expect("exchanges");
        let (mut server, ticket) = exchanges.create().expect("server");
        let mut client = exchanges.open_peer(ticket).expect("client");

        server.sender().start(None).expect("start fills control ring");
        let mut pending = server.sender().reserve_data(64).expect("reserve only payload slot");
        pending.fill(7);
        assert!(matches!(pending.commit(), Err(Error::WouldBlock)));

        assert!(matches!(client.receiver().try_next(), Ok(FlowEvent::Start { metadata: None })));
        let mut retry = server.sender().reserve_data(64).expect("failed commit returned credit");
        retry.fill(9);
        retry.commit().expect("retry commit");
        let received = match client.receiver().try_next().expect("retry data") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected retry data")
        };
        assert_eq!(&*received, &[9; 64]);
    }

    #[test]
    fn a_short_io_commit_returns_unused_trailing_slots() {
        let exchanges = exchanges();
        let (mut server, ticket) = exchanges.create().expect("server");
        let mut client = exchanges.open_peer(ticket).expect("client");
        server.sender().start(None).expect("start");
        assert!(matches!(client.receiver().try_next(), Ok(FlowEvent::Start { metadata: None })));

        let descriptor = {
            let mut pending = server.sender().reserve_data(256).expect("reserve IO capacity");
            pending[..37].fill(5);
            pending.commit_len(37).expect("commit bytes read")
        };
        assert_eq!(descriptor.payload_len(), 37);
        assert_eq!(descriptor.slot_count(), 1);

        let mut following = server.sender().reserve_data(192).expect("trailing slots returned");
        following.fill(9);
        following.commit().expect("commit following chunk");
        let first = match client.receiver().try_next().expect("short data") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected short data")
        };
        assert_eq!(&*first, &[5; 37]);
        let second = match client.receiver().try_next().expect("following data") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected following data")
        };
        assert_eq!(&*second, &[9; 192]);
    }

    #[test]
    fn reset_is_terminal_for_only_its_own_flow() {
        let exchanges = exchanges();
        let (server, ticket) = exchanges.create().expect("server");
        let client = exchanges.open_peer(ticket).expect("client");
        let (mut request_out, mut response_in) = server.split();
        let (mut response_out, mut request_in) = client.split();

        request_out.reset(ResetCode::new(41)).expect("request reset");
        assert!(matches!(
            request_in.try_next(),
            Ok(FlowEvent::Reset(code)) if code.get() == 41
        ));

        response_out.start(None).expect("response remains independent");
        assert!(matches!(response_in.try_next(), Ok(FlowEvent::Start { metadata: None })));
    }
}
