use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use orbit_core::{Fleet, NodeId};

use super::arena::Publication;
use super::protocol::CONTROL_FRAME_BYTES;
use super::{
    ChunkDescriptor, ControlEvent, ExchangeId, Flow, PayloadArena, PayloadArenaSpec, PayloadChunk,
    ResetCode,
};
use crate::{Error, Incarnation, ReadHalf, Result, StreamSpec, Streams, Ticket, WriteHalf};

/// The three physical resources used by an exchange table.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExchangeSpec {
    /// One lossless, bounded control table. Its two byte directions carry
    /// fixed-size typed events, never application payload bytes.
    pub control: StreamSpec,
    /// Request payload slots, shared by all exchanges in the fleet.
    pub request_payload: PayloadArenaSpec,
    /// Response payload slots, physically separate from request payloads.
    pub response_payload: PayloadArenaSpec,
}

impl ExchangeSpec {
    pub const fn new(
        control: StreamSpec,
        request_payload: PayloadArenaSpec,
        response_payload: PayloadArenaSpec,
    ) -> Self {
        Self { control, request_payload, response_payload }
    }

    fn validate(self) -> Result<()> {
        if self.control.buffer_bytes < CONTROL_FRAME_BYTES {
            return Err(Error::Malformed(format!(
                "exchange control ring is {} bytes; one control frame is {CONTROL_FRAME_BYTES}",
                self.control.buffer_bytes
            )));
        }
        if self.control.kind == self.request_payload.kind
            || self.control.kind == self.response_payload.kind
            || self.request_payload.kind == self.response_payload.kind
        {
            return Err(Error::Malformed(
                "exchange control, request payload and response payload need distinct SHM kinds"
                    .to_owned(),
            ));
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
        f: &mut fmt::Formatter<'_>,
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

/// One fleet's exchange transport: one control table and two directional
/// payload arenas.
#[derive(Clone)]
pub struct Exchanges {
    control: Streams,
    request_payload: PayloadArena,
    response_payload: PayloadArena,
}

impl Exchanges {
    pub fn open(
        fleet: Arc<Fleet>,
        incarnation: Incarnation,
        spec: ExchangeSpec,
    ) -> Result<Self> {
        spec.validate()?;
        let control = Streams::with_spec(Arc::clone(&fleet), incarnation, spec.control)?;
        let request_payload =
            PayloadArena::open(Arc::clone(&fleet), incarnation, spec.request_payload)?;
        let response_payload = PayloadArena::open(fleet, incarnation, spec.response_payload)?;
        Ok(Self { control, request_payload, response_payload })
    }

    /// Create the client-facing side (W1): request producer, response
    /// consumer. The ticket opens W2.
    pub fn create(&self) -> Result<(ServerExchange, ExchangeTicket)> {
        let (endpoint, ticket) = self.control.create()?;
        let id = ExchangeId::from_stream_id(endpoint.id());
        let (response_control, request_control) = endpoint.split();
        Ok((
            ServerExchange {
                request: RequestProducer(Producer::new(
                    id,
                    Flow::Request,
                    self.request_payload.clone(),
                    request_control,
                )),
                response: ResponseConsumer(Consumer::new(
                    id,
                    Flow::Response,
                    self.response_payload.clone(),
                    response_control,
                )),
            },
            ExchangeTicket(ticket),
        ))
    }

    /// Open the upstream-owning side (W2): request consumer, response
    /// producer. Response production is independent of request completion.
    pub fn open_client(
        &self,
        ticket: ExchangeTicket,
    ) -> Result<ClientExchange> {
        let endpoint = self.control.open(ticket.0)?;
        let id = ExchangeId::from_stream_id(endpoint.id());
        let (request_control, response_control) = endpoint.split();
        Ok(ClientExchange {
            request: RequestConsumer(Consumer::new(
                id,
                Flow::Request,
                self.request_payload.clone(),
                request_control,
            )),
            response: ResponseProducer(Producer::new(
                id,
                Flow::Response,
                self.response_payload.clone(),
                response_control,
            )),
        })
    }

    pub fn offer(
        &self,
        ticket: ExchangeTicket,
        to: NodeId,
    ) -> Result<()> {
        self.control.offer(ticket.0, to)
    }

    pub fn take_offer(&self) -> Option<ExchangeTicket> {
        self.control.take_offer().map(ExchangeTicket)
    }

    pub fn control(&self) -> &Streams {
        &self.control
    }

    /// Quiescent-owner maintenance only. Attaching a replacement process
    /// never resets an exchange table or either payload arena implicitly.
    pub fn reset_all(&self) {
        self.control.reset_all();
        self.request_payload.reset_all();
        self.response_payload.reset_all();
    }

    /// Remove all three SHM names. Existing mappings remain valid.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        self.control.unlink()?;
        self.request_payload.unlink()?;
        self.response_payload.unlink()
    }
}

/// W1, the client-facing representative.
pub struct ServerExchange {
    request: RequestProducer,
    response: ResponseConsumer,
}

impl ServerExchange {
    pub fn id(&self) -> ExchangeId {
        self.request.id()
    }

    pub fn request(&mut self) -> &mut RequestProducer {
        &mut self.request
    }

    pub fn response(&mut self) -> &mut ResponseConsumer {
        &mut self.response
    }

    pub fn split(self) -> (RequestProducer, ResponseConsumer) {
        (self.request, self.response)
    }
}

/// W2, the upstream-owning representative.
pub struct ClientExchange {
    request: RequestConsumer,
    response: ResponseProducer,
}

impl ClientExchange {
    pub fn id(&self) -> ExchangeId {
        self.response.id()
    }

    pub fn request(&mut self) -> &mut RequestConsumer {
        &mut self.request
    }

    pub fn response(&mut self) -> &mut ResponseProducer {
        &mut self.response
    }

    pub fn split(self) -> (RequestConsumer, ResponseProducer) {
        (self.request, self.response)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProducerState {
    New,
    Open,
    Terminal,
}

struct Producer {
    exchange: ExchangeId,
    flow: Flow,
    arena: PayloadArena,
    control: WriteHalf,
    next_chunk: u64,
    state: ProducerState,
}

impl Producer {
    fn new(
        exchange: ExchangeId,
        flow: Flow,
        arena: PayloadArena,
        control: WriteHalf,
    ) -> Self {
        Self { exchange, flow, arena, control, next_chunk: 1, state: ProducerState::New }
    }

    fn id(&self) -> ExchangeId {
        self.exchange
    }

    fn start(
        &mut self,
        metadata: Option<&[u8]>,
    ) -> Result<()> {
        if self.state != ProducerState::New {
            return Err(Error::Malformed("flow start was already sent".to_owned()));
        }
        let publication = match metadata.filter(|bytes| !bytes.is_empty()) {
            Some(bytes) => Some(self.arena.publish(bytes)?),
            None => None,
        };
        let descriptor = publication.as_ref().map(|publication| self.descriptor(publication));
        self.send(ControlEvent::Start {
            exchange: self.exchange,
            flow: self.flow,
            metadata: descriptor,
        })?;
        if let Some(publication) = publication {
            publication.mark_published();
            self.next_chunk += 1;
        }
        self.state = ProducerState::Open;
        Ok(())
    }

    fn data(
        &mut self,
        payload: &[u8],
    ) -> Result<ChunkDescriptor> {
        if self.state != ProducerState::Open {
            return Err(Error::Malformed("flow data requires an open flow".to_owned()));
        }
        let publication = self.arena.publish(payload)?;
        let descriptor = self.descriptor(&publication);
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
        code: ResetCode,
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
        event: ControlEvent,
    ) -> Result<()> {
        self.control.try_write_exact(&event.encode())
    }

    fn descriptor(
        &self,
        publication: &Publication,
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
            owner_node,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConsumerState {
    New,
    Open,
    Terminal,
}

struct Consumer {
    exchange: ExchangeId,
    flow: Flow,
    arena: PayloadArena,
    control: ReadHalf,
    next_chunk: u64,
    state: ConsumerState,
}

impl Consumer {
    fn new(
        exchange: ExchangeId,
        flow: Flow,
        arena: PayloadArena,
        control: ReadHalf,
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
                "control event belongs to another exchange flow".to_owned(),
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
            _ => Err(Error::Malformed("control event violates the flow lifecycle".to_owned())),
        }
    }

    fn take_chunk(
        &mut self,
        descriptor: ChunkDescriptor,
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
    Reset(ResetCode),
}

/// Typed callbacks for one flow. A handler decides what a request or response
/// event means; the transport does not encode application sequencing policy.
pub trait FlowHandler {
    type Error;

    fn on_start(
        &mut self,
        metadata: Option<PayloadChunk>,
    ) -> std::result::Result<(), Self::Error>;
    fn on_data(
        &mut self,
        chunk: PayloadChunk,
    ) -> std::result::Result<(), Self::Error>;
    fn on_fin(&mut self) -> std::result::Result<(), Self::Error>;
    fn on_reset(
        &mut self,
        code: ResetCode,
    ) -> std::result::Result<(), Self::Error>;
}

#[derive(Debug)]
pub enum DispatchError<E> {
    Transport(Error),
    Handler(E),
}

impl<E> From<Error> for DispatchError<E> {
    fn from(value: Error) -> Self {
        Self::Transport(value)
    }
}

fn dispatch<H: FlowHandler>(
    consumer: &mut Consumer,
    handler: &mut H,
) -> std::result::Result<(), DispatchError<H::Error>> {
    match consumer.next()? {
        FlowEvent::Start { metadata } => handler.on_start(metadata),
        FlowEvent::Data(chunk) => handler.on_data(chunk),
        FlowEvent::Fin => handler.on_fin(),
        FlowEvent::Reset(code) => handler.on_reset(code),
    }
    .map_err(DispatchError::Handler)
}

macro_rules! producer {
    ($name:ident, $flow:expr) => {
        pub struct $name(Producer);

        impl $name {
            pub fn id(&self) -> ExchangeId {
                self.0.id()
            }

            pub fn start(
                &mut self,
                metadata: Option<&[u8]>,
            ) -> Result<()> {
                debug_assert_eq!(self.0.flow, $flow);
                self.0.start(metadata)
            }

            pub fn data(
                &mut self,
                payload: &[u8],
            ) -> Result<ChunkDescriptor> {
                self.0.data(payload)
            }

            pub fn finish(&mut self) -> Result<()> {
                self.0.finish()
            }

            pub fn reset(
                &mut self,
                code: ResetCode,
            ) -> Result<()> {
                self.0.reset(code)
            }

            pub fn wait_writable(&self) -> Result<()> {
                self.0.control.wait_writable_exact(CONTROL_FRAME_BYTES)
            }

            pub fn poll_writable(
                &self,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<()>> {
                self.0.control.poll_writable_exact(CONTROL_FRAME_BYTES, cx)
            }

            /// Wait for both one control frame and the directional payload
            /// credit needed by the next publication. Use zero for FIN,
            /// RESET, or a start without metadata.
            pub fn wait_ready(&self, payload_len: usize) -> Result<()> {
                self.0.control.wait_writable_exact(CONTROL_FRAME_BYTES)?;
                if payload_len != 0 {
                    self.0.arena.wait_available(payload_len)?;
                }
                Ok(())
            }

            /// Task readiness for the same combined control and payload
            /// capacity. Both sources register before `Pending` is returned.
            pub fn poll_ready(
                &self,
                payload_len: usize,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<()>> {
                let control = self.0.control.poll_writable_exact(CONTROL_FRAME_BYTES, cx);
                let payload = if payload_len == 0 {
                    std::task::Poll::Ready(Ok(()))
                } else {
                    self.0.arena.poll_available(payload_len, cx)
                };
                match (control, payload) {
                    (std::task::Poll::Ready(Err(error)), _)
                    | (_, std::task::Poll::Ready(Err(error))) => {
                        std::task::Poll::Ready(Err(error))
                    }
                    (std::task::Poll::Ready(Ok(())), std::task::Poll::Ready(Ok(()))) => {
                        std::task::Poll::Ready(Ok(()))
                    }
                    _ => std::task::Poll::Pending,
                }
            }
        }
    };
}

macro_rules! consumer {
    ($name:ident, $flow:expr) => {
        pub struct $name(Consumer);

        impl $name {
            pub fn id(&self) -> ExchangeId {
                self.0.id()
            }

            pub fn try_next(&mut self) -> Result<FlowEvent> {
                debug_assert_eq!(self.0.flow, $flow);
                self.0.next()
            }

            pub fn dispatch<H: FlowHandler>(
                &mut self,
                handler: &mut H,
            ) -> std::result::Result<(), DispatchError<H::Error>> {
                dispatch(&mut self.0, handler)
            }

            pub fn wait_readable(&self) -> Result<()> {
                self.0.control.wait_readable()
            }

            pub fn poll_readable(
                &self,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<()>> {
                self.0.control.poll_readable(cx)
            }
        }
    };
}

producer!(RequestProducer, Flow::Request);
producer!(ResponseProducer, Flow::Response);
consumer!(RequestConsumer, Flow::Request);
consumer!(ResponseConsumer, Flow::Response);

#[cfg(test)]
mod tests {
    use super::*;

    fn exchanges() -> Exchanges {
        Exchanges::open(
            Arc::new(Fleet::join("exchange-pair-test", 2).expect("fleet")),
            Incarnation::new(1),
            ExchangeSpec::new(
                StreamSpec::new(230, 8, 512),
                PayloadArenaSpec::new(231, 4, 64),
                PayloadArenaSpec::new(232, 4, 64),
            ),
        )
        .expect("exchanges")
    }

    #[test]
    fn response_can_start_while_the_request_still_holds_every_request_slot() {
        let exchanges = exchanges();
        let (server, ticket) = exchanges.create().expect("server");
        let client = exchanges.open_client(ticket).expect("client");
        let (mut request_out, mut response_in) = server.split();
        let (mut request_in, mut response_out) = client.split();

        request_out.start(Some(b"request-head")).expect("request start");
        let request_descriptor = request_out.data(&[7_u8; 192]).expect("request data");
        let request_metadata = match request_in.try_next().expect("request start event") {
            FlowEvent::Start { metadata: Some(metadata) } => metadata,
            _ => panic!("expected request start"),
        };
        assert_eq!(&*request_metadata, b"request-head");
        let request_chunk = match request_in.try_next().expect("request data event") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected request data"),
        };
        assert_eq!(request_descriptor.arena_kind(), 231);
        assert_eq!(request_descriptor.slot_count(), 3);

        // Request metadata + body hold all four request slots. Response has
        // its own arena and its own lifecycle, so it starts immediately.
        response_out.start(Some(b"response-head")).expect("early response start");
        let response_descriptor = response_out.data(b"response").expect("response data");
        assert_eq!(response_descriptor.arena_kind(), 232);
        assert!(matches!(response_in.try_next(), Ok(FlowEvent::Start { .. })));
        let response_chunk = match response_in.try_next().expect("response data event") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected response data"),
        };
        assert_eq!(&*response_chunk, b"response");

        drop(request_chunk);
        request_out.finish().expect("request fin");
        assert!(matches!(request_in.try_next(), Ok(FlowEvent::Fin)));
        response_out.finish().expect("response fin");
        assert!(matches!(response_in.try_next(), Ok(FlowEvent::Fin)));
    }

    #[test]
    fn reset_is_terminal_for_only_its_own_flow() {
        let exchanges = exchanges();
        let (server, ticket) = exchanges.create().expect("server");
        let client = exchanges.open_client(ticket).expect("client");
        let (mut request_out, mut response_in) = server.split();
        let (mut request_in, mut response_out) = client.split();

        request_out.reset(ResetCode::new(41)).expect("request reset");
        assert!(matches!(
            request_in.try_next(),
            Ok(FlowEvent::Reset(code)) if code.get() == 41
        ));

        response_out.start(None).expect("response remains independent");
        assert!(matches!(response_in.try_next(), Ok(FlowEvent::Start { metadata: None })));
    }
}
