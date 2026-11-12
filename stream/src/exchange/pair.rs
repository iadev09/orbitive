use std::collections::HashSet;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orbit_core::{Fleet, NodeId};

use super::arena::{FlowKey, FlowWaitSpec, Publication, allowance_admits};
use super::protocol::CONTROL_FRAME_BYTES;
use super::{
    ChunkDescriptor, ChunkGeometry, ChunkPlan, ControlEvent,
    DEFAULT_CHUNK_BYTES, ExchangeId, Flow, PayloadArena, PayloadArenaSpec,
    PayloadChunk, ResetCode,
};
use crate::{
    Error, Incarnation, ReadHalf, Result, Side, StreamSpec, Streams, Ticket,
    WriteHalf, lock_unpoisoned,
};

/// How an exchange budgets payload memory for its two directions.
///
/// The shared form is the compact generic default. Directional arenas are for
/// adapters whose two flows have deliberately different budgets (for example
/// an upload and a streamed response) or must not consume each other's
/// credit.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExchangePayloadSpec {
    Shared(PayloadArenaSpec),
    Directional { a_to_b: PayloadArenaSpec, b_to_a: PayloadArenaSpec },
}

impl ExchangePayloadSpec {
    /// Physical payload arena used by one byte-flow direction.
    pub const fn for_flow(
        self,
        flow: Flow,
    ) -> PayloadArenaSpec {
        match (self, flow) {
            (Self::Shared(payload), _) => payload,
            (Self::Directional { a_to_b, .. }, Flow::AtoB) => a_to_b,
            (Self::Directional { b_to_a, .. }, Flow::BtoA) => b_to_a,
        }
    }
}

/// The physical resources used by an exchange table.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExchangeSpec {
    /// One lossless, bounded control table. Its two byte directions carry
    /// fixed-size typed events, never application payload bytes.
    pub control: StreamSpec,
    /// Payload memory may be shared by both directions or split into two
    /// independently bounded arenas.
    pub payload: ExchangePayloadSpec,
}

impl ExchangeSpec {
    pub const fn new(
        control: StreamSpec,
        payload: PayloadArenaSpec,
    ) -> Self {
        Self { control, payload: ExchangePayloadSpec::Shared(payload) }
    }

    pub const fn directional(
        control: StreamSpec,
        a_to_b: PayloadArenaSpec,
        b_to_a: PayloadArenaSpec,
    ) -> Self {
        Self {
            control,
            payload: ExchangePayloadSpec::Directional { a_to_b, b_to_a },
        }
    }

    /// Physical payload arena used by one byte-flow direction.
    pub const fn payload_for(
        self,
        flow: Flow,
    ) -> PayloadArenaSpec {
        self.payload.for_flow(flow)
    }

    /// Physical chunk geometry used by one byte-flow direction.
    pub const fn chunk_geometry(
        self,
        flow: Flow,
    ) -> Option<ChunkGeometry> {
        self.payload_for(flow).chunk_geometry()
    }

    fn validate(self) -> Result<()> {
        if self.control.buffer_bytes < CONTROL_FRAME_BYTES {
            return Err(Error::Malformed(format!(
                "exchange control ring is {} bytes; one control frame is {CONTROL_FRAME_BYTES}",
                self.control.buffer_bytes
            )));
        }
        match self.payload {
            ExchangePayloadSpec::Shared(payload)
                if self.control.kind == payload.kind =>
            {
                return Err(Error::Malformed(
                    "exchange control and payload need distinct SHM kinds"
                        .to_owned(),
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

#[derive(Clone)]
enum PayloadArenas {
    Shared(PayloadArena),
    Directional { a_to_b: PayloadArena, b_to_a: PayloadArena },
}

impl PayloadArenas {
    fn for_flow(
        &self,
        flow: Flow,
    ) -> PayloadArena {
        match (self, flow) {
            (Self::Shared(arena), _) => arena.clone(),
            (Self::Directional { a_to_b, .. }, Flow::AtoB) => a_to_b.clone(),
            (Self::Directional { b_to_a, .. }, Flow::BtoA) => b_to_a.clone(),
        }
    }

    fn node_dead(
        &self,
        node: NodeId,
        incarnation: Incarnation,
        dead_flows: &HashSet<(u64, u16)>,
    ) {
        match self {
            Self::Shared(arena) => {
                arena.node_dead_with_flows(node, incarnation, dead_flows)
            }
            Self::Directional { a_to_b, b_to_a } => {
                a_to_b.node_dead_with_flows(node, incarnation, dead_flows);
                b_to_a.node_dead_with_flows(node, incarnation, dead_flows);
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
    payload: PayloadArenas,
    flow_slots: Option<usize>,
}

impl Exchanges {
    pub fn open(
        fleet: Arc<Fleet>,
        incarnation: Incarnation,
        spec: ExchangeSpec,
    ) -> Result<Self> {
        spec.validate()?;
        let control =
            Streams::with_spec(Arc::clone(&fleet), incarnation, spec.control)?;
        let payload = match spec.payload {
            ExchangePayloadSpec::Shared(payload) => PayloadArenas::Shared(
                PayloadArena::open(fleet, incarnation, payload)?,
            ),
            ExchangePayloadSpec::Directional { a_to_b, b_to_a } => {
                PayloadArenas::Directional {
                    a_to_b: PayloadArena::open(
                        Arc::clone(&fleet),
                        incarnation,
                        a_to_b,
                    )?,
                    b_to_a: PayloadArena::open(fleet, incarnation, b_to_a)?,
                }
            }
        };
        Ok(Self { control, payload, flow_slots: None })
    }

    /// Bound outstanding payload independently in each exchange direction.
    /// By default a direction may use its producer's entire arena lane.
    pub fn with_flow_slots(
        mut self,
        slots: usize,
    ) -> Result<Self> {
        if slots == 0
            || slots > self.payload.for_flow(Flow::AtoB).slots_per_node()
            || slots > self.payload.for_flow(Flow::BtoA).slots_per_node()
        {
            return Err(Error::Malformed(
                "exchange flow credit must fit both payload lanes".to_owned(),
            ));
        }
        self.flow_slots = Some(slots);
        Ok(self)
    }

    fn creation_arena(
        &self,
        len: usize,
    ) -> Result<PayloadArena> {
        let arena = self.payload.for_flow(Flow::AtoB);
        if let Some(slots) = self.flow_slots {
            flow_slots_for(len, slots, arena.slot_size())?;
        }
        Ok(arena)
    }

    /// Create side A. Its sender writes A-to-B and its receiver reads B-to-A.
    /// The ticket opens side B.
    pub fn create(&self) -> Result<(ExchangeEndpoint, ExchangeTicket)> {
        let (endpoint, ticket) = self.control.create()?;
        Ok((self.endpoint(endpoint), ExchangeTicket(ticket)))
    }

    /// Whether side A can currently reserve its first flow payload.
    /// Session transports use this before reserving a remote resource so
    /// payload backpressure does not become an abandoned resource lease.
    pub fn create_payload_available(
        &self,
        payload_len: usize,
    ) -> Result<bool> {
        self.creation_arena(payload_len)?.is_available(payload_len)
    }

    /// Task readiness for side A's first flow payload credit.
    pub fn poll_create_payload_available(
        &self,
        payload_len: usize,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        self.creation_arena(payload_len)?.poll_available(payload_len, cx)
    }

    /// Open side B. Its sender writes B-to-A and its receiver reads A-to-B.
    /// Both directions have independent lifecycle and may start in either order.
    pub fn open_peer(
        &self,
        ticket: ExchangeTicket,
    ) -> Result<ExchangeEndpoint> {
        let endpoint = self.control.open(ticket.0)?;
        Ok(self.endpoint(endpoint))
    }

    fn endpoint(
        &self,
        endpoint: crate::Endpoint,
    ) -> ExchangeEndpoint {
        let id = ExchangeId::from_stream_id(endpoint.id());
        let (send_flow, receive_flow) = match endpoint.side() {
            Side::A => (Flow::AtoB, Flow::BtoA),
            Side::B => (Flow::BtoA, Flow::AtoB),
        };
        let (read, write) = endpoint.split();
        let unopened = (send_flow == Flow::AtoB).then(|| {
            Arc::new(UnopenedPeer {
                control: Arc::clone(&write.handle),
                arena: self.payload.for_flow(send_flow),
                unread: Mutex::new(Vec::new()),
            })
        });
        ExchangeEndpoint {
            sender: Sender(Producer::new(
                id,
                send_flow,
                self.payload.for_flow(send_flow),
                write,
                self.flow_slots,
                unopened.clone(),
            )),
            receiver: Receiver(Consumer::new(
                id,
                receive_flow,
                self.payload.for_flow(receive_flow),
                read,
                unopened,
            )),
        }
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

    /// Park until an exchange is offered to this node.
    pub fn blocking_take_offer(&self) -> Result<ExchangeTicket> {
        self.control.blocking_take_offer().map(ExchangeTicket)
    }

    /// Wait at most `timeout` for an offered exchange.
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    pub fn take_offer_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<ExchangeTicket>> {
        self.control
            .take_offer_timeout(timeout)
            .map(|ticket| ticket.map(ExchangeTicket))
    }

    /// Task readiness for the next offered exchange.
    pub fn poll_take_offer(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<ExchangeTicket>> {
        match self.control.poll_take_offer(cx) {
            std::task::Poll::Ready(result) => {
                std::task::Poll::Ready(result.map(ExchangeTicket))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
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
        incarnation: Incarnation,
    ) {
        let dead_flows = self
            .control
            .exchange_readers(node, incarnation)
            .map(|(id, side)| {
                let flow = match side {
                    Side::A => Flow::BtoA,
                    Side::B => Flow::AtoB,
                };
                (id.net_id().raw(), u16::from(flow.wire()))
            })
            .collect();
        self.control.node_dead(node, incarnation);
        self.payload.node_dead(node, incarnation, &dead_flows);
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
    receiver: Receiver,
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
    Terminal,
}

// Both local halves keep the ticket openable. If the writer finishes before
// the peer opens, the last local half seals an unclaimed ticket and reclaims
// the unread chunks. A claimed peer owns draining its own direction.
struct UnopenedPeer {
    control: Arc<crate::Handle>,
    arena: PayloadArena,
    unread: Mutex<Vec<ChunkDescriptor>>,
}

impl Drop for UnopenedPeer {
    fn drop(&mut self) {
        let unread =
            self.unread.get_mut().unwrap_or_else(|error| error.into_inner());
        if !unread.is_empty() && self.control.close_unclaimed_peer() {
            for descriptor in unread.drain(..) {
                self.arena.discard(descriptor);
            }
        }
    }
}

fn flow_slots_for(
    len: usize,
    slots: usize,
    slot_size: usize,
) -> Result<usize> {
    let capacity = slots * slot_size;
    if len > capacity {
        return Err(Error::PayloadTooLarge { len, capacity });
    }
    Ok(len.div_ceil(slot_size))
}

struct Producer {
    exchange: ExchangeId,
    flow: Flow,
    arena: PayloadArena,
    control: WriteHalf,
    /// The exchange status a credit driver reads while this sender is parked.
    status: crate::WriteStatus,
    next_chunk: u64,
    state: ProducerState,
    flow_slots: usize,
    published: Vec<ChunkDescriptor>,
    published_slots: usize,
    unopened: Option<Arc<UnopenedPeer>>,
}

impl Producer {
    fn new(
        exchange: ExchangeId,
        flow: Flow,
        arena: PayloadArena,
        control: WriteHalf,
        flow_slots: Option<usize>,
        unopened: Option<Arc<UnopenedPeer>>,
    ) -> Self {
        let flow_slots = flow_slots.unwrap_or_else(|| arena.slots_per_node());
        let status = control.write_status();
        Self {
            exchange,
            flow,
            arena,
            control,
            status,
            next_chunk: 1,
            state: ProducerState::New,
            flow_slots,
            published: Vec::new(),
            published_slots: 0,
            unopened,
        }
    }

    fn id(&self) -> ExchangeId {
        self.exchange
    }

    fn start(
        &mut self,
        metadata: Option<&[u8]>,
    ) -> Result<()> {
        if self.state != ProducerState::New {
            return Err(Error::Malformed(
                "flow start was already sent".to_owned(),
            ));
        }
        if let Some(bytes) = metadata.filter(|bytes| !bytes.is_empty()) {
            let mut pending = self.reserve_start(bytes.len())?;
            pending.copy_from_slice(bytes);
            pending.commit()?;
        } else {
            self.send(ControlEvent::Start {
                exchange: self.exchange,
                flow: self.flow,
                metadata: None,
            })?;
            self.state = ProducerState::Open;
        }
        Ok(())
    }

    fn data(
        &mut self,
        payload: &[u8],
    ) -> Result<ChunkDescriptor> {
        let mut pending = self.reserve_data(payload.len())?;
        pending.copy_from_slice(payload);
        pending.commit()
    }

    fn reserve_start(
        &mut self,
        payload_len: usize,
    ) -> Result<PendingStart<'_>> {
        if self.state != ProducerState::New {
            return Err(Error::Malformed(
                "flow start was already sent".to_owned(),
            ));
        }
        let publication = self.reserve(payload_len)?;
        Ok(PendingStart { producer: self, publication })
    }

    fn reserve_data(
        &mut self,
        payload_len: usize,
    ) -> Result<PendingData<'_>> {
        if self.state != ProducerState::Open {
            return Err(Error::Malformed(
                "flow data requires an open flow".to_owned(),
            ));
        }
        let publication = self.reserve(payload_len)?;
        Ok(PendingData { producer: self, publication })
    }

    fn required_slots(
        &self,
        len: usize,
    ) -> Result<usize> {
        flow_slots_for(len, self.flow_slots, self.arena.slot_size())
    }

    fn reserve(
        &mut self,
        len: usize,
    ) -> Result<Publication> {
        self.control.exchange_write_status()?;
        let count = self.required_slots(len)?;
        if self.published_slots + count > self.flow_slots {
            self.published.retain(|descriptor| self.arena.retains(*descriptor));
            self.published_slots =
                self.published.iter().map(|d| d.slot_count() as usize).sum();
        }
        if self.published_slots + count > self.flow_slots {
            return Err(Error::PayloadFull { requested_slots: count });
        }
        self.arena.reserve(len)
    }

    fn credit_ready(
        &self,
        len: usize,
    ) -> Result<bool> {
        self.control.exchange_write_status()?;
        let count = self.required_slots(len)?;
        if self.published_slots + count > self.flow_slots
            && !allowance_admits(
                &self.published,
                self.flow_slots,
                count,
                |descriptor| self.arena.retains(descriptor),
            )
        {
            return Ok(false);
        }
        self.arena.is_available(len)
    }

    fn credit_key(&self) -> FlowKey {
        (self.exchange.stream_id().net_id().raw(), u16::from(self.flow.wire()))
    }

    /// Task readiness for the payload credit of the next `len` bytes. The
    /// registration carries what this sender waits for, so the credit driver
    /// wakes it only when that is there.
    fn poll_credit(
        &self,
        len: usize,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        let demand = match self.required_slots(len) {
            Ok(demand) => demand,
            Err(error) => return std::task::Poll::Ready(Err(error)),
        };
        self.arena.poll_flow(
            FlowWaitSpec {
                key: self.credit_key(),
                status: &self.status,
                demand,
                limit: self.flow_slots,
                held: &self.published,
            },
            || self.credit_ready(len),
            cx,
        )
    }

    fn publish(
        &mut self,
        publication: Publication,
        descriptor: ChunkDescriptor,
        event: ControlEvent,
    ) -> Result<()> {
        publication.bind(self.exchange, self.flow);
        publication.make_live();
        self.send(event)?;
        publication.mark_published();
        self.published_slots += descriptor.slot_count() as usize;
        self.published.push(descriptor);

        // Either the departing reader saw the committed descriptor while
        // draining, or this check sees its close and returns the unread extent.
        if matches!(self.control.exchange_write_status(), Err(Error::PeerGone))
        {
            self.discard_unread();
        }
        self.next_chunk += 1;
        Ok(())
    }

    fn discard_unread(&mut self) {
        for descriptor in self.published.drain(..) {
            self.arena.discard(descriptor);
        }
        self.published_slots = 0;
    }

    fn commit_start(
        &mut self,
        publication: Publication,
    ) -> Result<ChunkDescriptor> {
        let descriptor = self.descriptor(&publication);
        self.publish(
            publication,
            descriptor,
            ControlEvent::Start {
                exchange: self.exchange,
                flow: self.flow,
                metadata: Some(descriptor),
            },
        )?;
        self.state = ProducerState::Open;
        Ok(descriptor)
    }

    fn commit_data(
        &mut self,
        publication: Publication,
    ) -> Result<ChunkDescriptor> {
        let descriptor = self.descriptor(&publication);
        self.publish(publication, descriptor, ControlEvent::Data(descriptor))?;
        Ok(descriptor)
    }

    fn finish(&mut self) -> Result<()> {
        if self.state != ProducerState::Open {
            return Err(Error::Malformed(
                "flow FIN requires an open flow".to_owned(),
            ));
        }
        self.send(ControlEvent::Fin {
            exchange: self.exchange,
            flow: self.flow,
        })?;
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
        self.control.reset_exchange(code.get())?;
        self.discard_unread();
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
        let (
            generation,
            first_slot,
            slot_count,
            payload_len,
            arena_kind,
            owner_node,
        ) = publication.coordinates();
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

impl Drop for Producer {
    fn drop(&mut self) {
        // A parked registration ends with its sender.
        self.arena.forget_flow(self.credit_key());
        if self.published.is_empty() {
            return;
        }
        if self.state != ProducerState::Terminal {
            self.control.reset();
            self.discard_unread();
        } else if matches!(
            self.control.exchange_write_status(),
            Err(Error::PeerGone)
        ) {
            self.discard_unread();
        } else if let Some(unopened) = &self.unopened
            && unopened.control.peer_is_unclaimed()
        {
            *lock_unpoisoned(&unopened.unread) =
                std::mem::take(&mut self.published);
        }
    }
}

/// A flow-start payload reserved directly in the shared SHM arena. The bytes
/// become immutable and visible only when `commit` sends the matching start
/// event. Dropping this value returns the reserved slots.
pub struct PendingStart<'a> {
    producer: &'a mut Producer,
    publication: Publication,
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
        payload_len: usize,
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
    publication: Publication,
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
        payload_len: usize,
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
    Terminal,
}

struct Consumer {
    exchange: ExchangeId,
    flow: Flow,
    arena: PayloadArena,
    control: ReadHalf,
    next_chunk: u64,
    state: ConsumerState,
    _unopened: Option<Arc<UnopenedPeer>>,
}

impl Consumer {
    fn new(
        exchange: ExchangeId,
        flow: Flow,
        arena: PayloadArena,
        control: ReadHalf,
        unopened: Option<Arc<UnopenedPeer>>,
    ) -> Self {
        Self {
            exchange,
            flow,
            arena,
            control,
            next_chunk: 1,
            state: ConsumerState::New,
            _unopened: unopened,
        }
    }

    fn id(&self) -> ExchangeId {
        self.exchange
    }

    fn next(&mut self) -> Result<FlowEvent> {
        if self.state == ConsumerState::Terminal {
            return Err(Error::Closed);
        }
        let mut frame = [0_u8; CONTROL_FRAME_BYTES];
        if let Err(error) = self.control.try_read_exact(&mut frame) {
            return self.read_error(error);
        }
        let event = ControlEvent::decode(&frame)
            .map_err(|reason| Error::Malformed(reason.to_owned()))?;
        if event.exchange() != self.exchange || event.flow() != self.flow {
            return Err(Error::Malformed(
                "control event belongs to another exchange flow".to_owned(),
            ));
        }
        match event {
            ControlEvent::Start { metadata, .. }
                if self.state == ConsumerState::New =>
            {
                let metadata = metadata
                    .map(|descriptor| self.take_chunk(descriptor))
                    .transpose();
                let metadata = match metadata {
                    Ok(metadata) => metadata,
                    Err(error) => return self.read_error(error),
                };
                self.state = ConsumerState::Open;
                Ok(FlowEvent::Start { metadata })
            }
            ControlEvent::Data(descriptor)
                if self.state == ConsumerState::Open =>
            {
                match self.take_chunk(descriptor) {
                    Ok(chunk) => Ok(FlowEvent::Data(chunk)),
                    Err(error) => self.read_error(error),
                }
            }
            ControlEvent::Fin { .. } if self.state == ConsumerState::Open => {
                self.state = ConsumerState::Terminal;
                Ok(FlowEvent::Fin)
            }
            ControlEvent::Reset { code, .. } => {
                self.state = ConsumerState::Terminal;
                Ok(FlowEvent::Reset(code))
            }
            _ => Err(Error::Malformed(
                "control event violates the flow lifecycle".to_owned(),
            )),
        }
    }

    fn read_error(
        &mut self,
        error: Error,
    ) -> Result<FlowEvent> {
        if let Some(code) = self.control.exchange_reset_code()? {
            self.state = ConsumerState::Terminal;
            return Ok(FlowEvent::Reset(ResetCode::new(code)));
        }
        Err(error)
    }

    fn cancel(&mut self) {
        if self.state == ConsumerState::Terminal {
            return;
        }
        self.state = ConsumerState::Terminal;
        self.control.stop_reading();
        if let Ok(node) = self.control.exchange_peer_node() {
            self.arena.notify_peer(node);
        }
        let mut frame = [0; CONTROL_FRAME_BYTES];
        while self.control.try_drain_exchange_frame(&mut frame).is_ok() {
            let Ok(event) = ControlEvent::decode(&frame) else { continue };
            if event.exchange() != self.exchange || event.flow() != self.flow {
                continue;
            }
            match event {
                ControlEvent::Start { metadata: Some(descriptor), .. }
                | ControlEvent::Data(descriptor) => {
                    // Claim with the same generation as a regular read. A
                    // concurrent writer abort can win, but a held chunk cannot.
                    drop(self.arena.read(descriptor));
                }
                _ => {}
            }
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

impl Drop for Consumer {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// The event program seen by a flow consumer.
pub enum FlowEvent {
    Start { metadata: Option<PayloadChunk> },
    Data(PayloadChunk),
    Fin,
    Reset(ResetCode),
}

/// Typed callbacks for one flow. A handler decides what its events mean; the
/// transport does not encode application sequencing policy.
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

/// The capability to publish one endpoint's outbound flow.
pub struct Sender(Producer);

impl Sender {
    pub fn id(&self) -> ExchangeId {
        self.0.id()
    }

    pub fn flow(&self) -> Flow {
        self.0.flow
    }

    /// Effective chunk geometry, including this direction's credit limit.
    pub fn chunk_geometry(&self) -> ChunkGeometry {
        ChunkGeometry::new(self.0.arena.slot_size(), self.0.flow_slots)
            .expect("opened exchange has nonzero flow credit")
    }

    /// Plan a known payload using the default chunk target.
    pub fn plan_chunks(
        &self,
        data_bytes: usize,
    ) -> Result<ChunkPlan> {
        self.chunk_geometry()
            .plan_for_target(data_bytes, DEFAULT_CHUNK_BYTES)
            .ok_or_else(|| {
                Error::Malformed(
                    "payload chunk plan exceeds flow credit".to_owned(),
                )
            })
    }

    /// Plan a known payload with a runtime-selected number of slots per chunk.
    pub fn plan_chunks_with(
        &self,
        data_bytes: usize,
        slots_per_chunk: usize,
    ) -> Result<ChunkPlan> {
        self.chunk_geometry().plan(data_bytes, slots_per_chunk)
            .ok_or_else(|| Error::Malformed(format!(
                "chunk slot count {slots_per_chunk} exceeds exchange flow credit {}", self.0.flow_slots,
            )))
    }

    pub fn start(
        &mut self,
        metadata: Option<&[u8]>,
    ) -> Result<()> {
        self.0.start(metadata)
    }

    /// Reserve start metadata directly in the shared payload arena. No start
    /// event is visible until the returned guard is committed.
    pub fn reserve_start(
        &mut self,
        payload_len: usize,
    ) -> Result<PendingStart<'_>> {
        self.0.reserve_start(payload_len)
    }

    pub fn data(
        &mut self,
        payload: &[u8],
    ) -> Result<ChunkDescriptor> {
        self.0.data(payload)
    }

    /// Reserve one variable-sized data chunk directly in the payload arena.
    /// One commit emits one data descriptor.
    pub fn reserve_data(
        &mut self,
        payload_len: usize,
    ) -> Result<PendingData<'_>> {
        self.0.reserve_data(payload_len)
    }

    pub fn finish(&mut self) -> Result<()> {
        self.0.finish()
    }

    /// Close an unused direction. The application already knows from its
    /// metadata that there is nothing to receive from this direction.
    pub fn finish_empty(&mut self) -> Result<()> {
        if self.0.state != ProducerState::New {
            return Err(Error::Malformed(
                "empty FIN requires an unused flow".to_owned(),
            ));
        }
        self.0.control.finish_quietly();
        self.0.state = ProducerState::Terminal;
        Ok(())
    }

    pub fn reset(
        &mut self,
        code: ResetCode,
    ) -> Result<()> {
        self.0.reset(code)
    }

    /// Observe cancellation while waiting on application work instead of a
    /// control write or a payload reservation. Clean FIN is not cancellation.
    pub async fn peer_closed(&mut self) -> Result<()> {
        self.0.control.peer_closed().await
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

    /// Wait for one control frame and any payload credit needed by the next
    /// publication. Use zero for FIN or a start without metadata.
    pub fn wait_ready(
        &self,
        payload_len: usize,
    ) -> Result<()> {
        if payload_len != 0 {
            self.0.arena.wait_credit(|| self.0.credit_ready(payload_len))?;
        }
        self.0.control.wait_writable_exact(CONTROL_FRAME_BYTES)?;
        self.0.control.exchange_write_status()
    }

    /// Task readiness for the same combined control and payload capacity.
    pub fn poll_ready(
        &self,
        payload_len: usize,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        self.0.control.exchange_write_status()?;
        let control =
            self.0.control.poll_writable_exact(CONTROL_FRAME_BYTES, cx);
        let payload = if payload_len == 0 {
            std::task::Poll::Ready(Ok(()))
        } else {
            self.0.poll_credit(payload_len, cx)
        };
        match (control, payload) {
            (std::task::Poll::Ready(Err(error)), _)
            | (_, std::task::Poll::Ready(Err(error))) => {
                std::task::Poll::Ready(Err(error))
            }
            (
                std::task::Poll::Ready(Ok(())),
                std::task::Poll::Ready(Ok(())),
            ) => std::task::Poll::Ready(self.0.control.exchange_write_status()),
            _ => std::task::Poll::Pending,
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

    /// Stop this direction and return published chunks that were not taken.
    /// Previously returned chunks keep their own credit until dropped.
    pub fn cancel(&mut self) {
        self.0.cancel();
    }

    /// Observe a reset independently of the task reading body descriptors.
    pub fn reset_signal(&self) -> crate::ResetSignal {
        self.0.control.reset_signal()
    }

    pub fn try_next(&mut self) -> Result<FlowEvent> {
        self.0.next()
    }

    pub fn dispatch<H: FlowHandler>(
        &mut self,
        handler: &mut H,
    ) -> std::result::Result<(), DispatchError<H::Error>> {
        dispatch(&mut self.0, handler)
    }

    pub fn wait_readable(&self) -> Result<()> {
        if self.0.state == ConsumerState::Terminal {
            return Err(Error::Closed);
        }
        self.0.control.wait_readable()
    }

    pub fn poll_readable(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        if self.0.state == ConsumerState::Terminal {
            return std::task::Poll::Ready(Err(Error::Closed));
        }
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
            ExchangeSpec::new(
                StreamSpec::new(230, 8, 512),
                PayloadArenaSpec::new(231, 8, 64),
            ),
        )
        .expect("exchanges")
    }

    fn take_data(receiver: &mut Receiver) -> bytes::Bytes {
        let Ok(FlowEvent::Data(chunk)) = receiver.try_next() else {
            panic!("expected data chunk");
        };
        bytes::Bytes::from_owner(chunk)
    }

    #[test]
    fn flow_credit_counts_slots_until_the_last_bytes_slice_drops() {
        let exchanges = exchanges().with_flow_slots(2).unwrap();
        let (mut a, ticket) = exchanges.create().unwrap();
        let mut b = exchanges.open_peer(ticket).unwrap();
        let (mut sibling, _) = exchanges.create().unwrap();
        a.sender().start(None).unwrap();
        b.sender().start(None).unwrap();
        sibling.sender().start(None).unwrap();
        assert!(matches!(b.receiver().try_next(), Ok(FlowEvent::Start { .. })));
        a.sender().data(&[7; 65]).unwrap();
        let body = take_data(b.receiver());
        let slice = body.slice(1..2);
        drop(body);

        assert!(matches!(
            a.sender().data(b"blocked"),
            Err(Error::PayloadFull { requested_slots: 1 })
        ));
        assert!(matches!(
            a.sender().data(&[0; 129]),
            Err(Error::PayloadTooLarge { capacity: 128, .. })
        ));
        assert!(a.sender().chunk_geometry().chunk_bytes(3).is_none());
        assert!(matches!(
            exchanges.create_payload_available(129),
            Err(Error::PayloadTooLarge { .. })
        ));
        sibling.sender().data(b"sibling can send").unwrap();
        b.sender().data(b"early response").unwrap();

        assert_eq!(&slice[..], &[7]);
        drop(slice);
        a.sender().data(&[8; 65]).unwrap();
        assert_eq!(&take_data(b.receiver())[..], &[8; 65]);
    }

    /// Counts its wakes and says so on a channel, so a test waits for a wake
    /// rather than for a time.
    struct Signal(Mutex<std::sync::mpsc::Sender<()>>);

    impl std::task::Wake for Signal {
        fn wake(self: Arc<Self>) {
            let _ = lock_unpoisoned(&self.0).send(());
        }
    }

    fn signal() -> (std::task::Waker, std::sync::mpsc::Receiver<()>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        (Arc::new(Signal(Mutex::new(sender))).into(), receiver)
    }

    /// Park `sender` on payload credit for one more slot.
    fn park(
        sender: &Sender,
        waker: &std::task::Waker,
    ) {
        let mut cx = std::task::Context::from_waker(waker);
        assert!(sender.poll_ready(64, &mut cx).is_pending());
    }

    // The driver takes a pass's records out under its lock and wakes them
    // after it: once A's wake arrives, the pass that woke it has decided about
    // B too, so B's registration still standing means B was not chosen.
    #[test]
    fn a_return_wakes_only_the_flow_it_frees() {
        let exchanges = exchanges().with_flow_slots(1).unwrap();
        let (mut a, ticket) = exchanges.create().unwrap();
        let mut a_peer = exchanges.open_peer(ticket).unwrap();
        let (mut b, ticket) = exchanges.create().unwrap();
        let mut b_peer = exchanges.open_peer(ticket).unwrap();
        for (sender, receiver) in [(&mut a, &mut a_peer), (&mut b, &mut b_peer)]
        {
            sender.sender().start(None).unwrap();
            assert!(matches!(
                receiver.receiver().try_next(),
                Ok(FlowEvent::Start { .. })
            ));
            sender.sender().data(&[1; 64]).unwrap();
        }
        let a_held = take_data(a_peer.receiver());
        let b_held = take_data(b_peer.receiver());
        let (a_waker, a_woken) = signal();
        let (b_waker, _b_woken) = signal();
        park(a.sender(), &a_waker);
        park(b.sender(), &b_waker);

        drop(a_held);
        a_woken.recv().unwrap();
        let a_key = a.sender().0.credit_key();
        let b_key = b.sender().0.credit_key();
        let arena = a.sender().0.arena.clone();
        assert!(!arena.is_waiting(a_key));
        assert!(arena.is_waiting(b_key));
        a.sender().data(&[2; 64]).unwrap();
        assert_eq!(&b_held[..], &[1; 64]);
    }

    #[test]
    fn a_cancelled_reader_wakes_its_sender_while_its_chunk_is_held() {
        let exchanges = exchanges().with_flow_slots(1).unwrap();
        let (mut a, ticket) = exchanges.create().unwrap();
        let mut peer = exchanges.open_peer(ticket).unwrap();
        a.sender().start(None).unwrap();
        assert!(matches!(
            peer.receiver().try_next(),
            Ok(FlowEvent::Start { .. })
        ));
        a.sender().data(&[1; 64]).unwrap();
        let held = take_data(peer.receiver());
        let (waker, woken) = signal();
        park(a.sender(), &waker);

        drop(peer);
        woken.recv().unwrap();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(matches!(
            a.sender().poll_ready(64, &mut cx),
            std::task::Poll::Ready(Err(Error::PeerGone))
        ));
        assert_eq!(&held[..], &[1; 64]);
    }

    #[test]
    fn reset_bypasses_a_full_ring_and_reclaims_only_unread_chunks() {
        let exchanges = Exchanges::open(
            Arc::new(Fleet::join("exchange-reset-full", 2).unwrap()),
            Incarnation::new(1),
            ExchangeSpec::new(
                StreamSpec::new(224, 8, CONTROL_FRAME_BYTES),
                PayloadArenaSpec::new(225, 2, 64),
            ),
        )
        .unwrap();
        let (mut a, ticket) = exchanges.create().unwrap();
        let mut b = exchanges.open_peer(ticket).unwrap();
        a.sender().start(None).unwrap();
        assert!(matches!(b.receiver().try_next(), Ok(FlowEvent::Start { .. })));
        a.sender().data(&[1; 64]).unwrap();
        let held = take_data(b.receiver());
        a.sender().data(&[2; 64]).unwrap();
        assert!(matches!(a.sender().finish(), Err(Error::WouldBlock)));
        a.sender().reset(ResetCode::new(0)).unwrap();
        assert!(
            matches!(b.receiver().try_next(), Ok(FlowEvent::Reset(code)) if code.get() == 0)
        );

        let (mut next, ticket) = exchanges.create().unwrap();
        let mut peer = exchanges.open_peer(ticket).unwrap();
        next.sender().start(Some(&[3; 64])).unwrap();
        assert!(matches!(
            peer.receiver().try_next(),
            Ok(FlowEvent::Start { .. })
        ));
        assert_eq!(&held[..], &[1; 64]);
    }

    #[test]
    fn receiver_cancel_drains_a_finished_sender_without_invalidating_held_data()
    {
        let exchanges = exchanges();
        let (a, ticket) = exchanges.create().unwrap();
        let (mut sender, _reverse) = a.split();
        let mut b = exchanges.open_peer(ticket).unwrap();
        sender.start(None).unwrap();
        assert!(matches!(b.receiver().try_next(), Ok(FlowEvent::Start { .. })));
        sender.data(&[1; 64]).unwrap();
        let held = take_data(b.receiver());
        sender.data(&[2; 448]).unwrap();
        sender.finish().unwrap();
        drop(sender);

        b.receiver().cancel();
        assert!(matches!(b.receiver().try_next(), Err(Error::Closed)));
        let (mut next, _) = exchanges.create().unwrap();
        next.sender().start(Some(&[3; 448])).unwrap();
        assert_eq!(&held[..], &[1; 64]);
    }

    #[test]
    fn finish_keeps_a_ticket_open_until_the_last_local_half_drops() {
        let exchanges = exchanges();
        let (a, ticket) = exchanges.create().unwrap();
        let (mut sender, reverse) = a.split();
        sender.start(Some(&[1; 512])).unwrap();
        sender.finish().unwrap();
        drop(sender);
        let mut late_peer = exchanges.open_peer(ticket).unwrap();
        assert!(
            matches!(late_peer.receiver().try_next(), Ok(FlowEvent::Start { metadata: Some(data) }) if data.as_slice() == [1; 512])
        );
        drop(late_peer);
        drop(reverse);

        let (mut abandoned, ticket) = exchanges.create().unwrap();
        abandoned.sender().start(Some(&[2; 512])).unwrap();
        abandoned.sender().finish().unwrap();
        drop(abandoned);
        assert!(matches!(exchanges.open_peer(ticket), Err(Error::Stale(_))));
        assert!(exchanges.create_payload_available(512).unwrap());
    }

    #[test]
    fn cancellation_racing_publication_does_not_lose_credit() {
        let exchanges = exchanges();
        for _ in 0..24 {
            let (a, ticket) = exchanges.create().unwrap();
            let b = exchanges.open_peer(ticket).unwrap();
            let (mut sender, _reverse) = a.split();
            let (_reverse, mut receiver) = b.split();
            sender.start(None).unwrap();

            let ready = std::sync::Barrier::new(2);
            let ready_ref = &ready;
            std::thread::scope(|scope| {
                let mut pending = sender.reserve_data(512).unwrap();
                pending.fill(7);
                let cancelled = scope.spawn(move || {
                    ready_ref.wait();
                    receiver.cancel();
                });
                ready.wait();
                assert!(matches!(
                    pending.commit(),
                    Ok(_) | Err(Error::PeerGone)
                ));
                cancelled.join().unwrap();
                assert!(exchanges.create_payload_available(512).unwrap());
            });
        }
    }

    #[tokio::test]
    async fn a_credit_wait_wakes_on_the_last_bytes_drop_or_reader_cancel() {
        use std::future::poll_fn;

        let exchanges = exchanges().with_flow_slots(1).unwrap();
        let (mut a, ticket) = exchanges.create().unwrap();
        let mut b = exchanges.open_peer(ticket).unwrap();
        a.sender().start(None).unwrap();
        assert!(matches!(b.receiver().try_next(), Ok(FlowEvent::Start { .. })));

        for cancel in [false, true] {
            a.sender().data(b"held").unwrap();
            let held = take_data(b.receiver());
            let (armed, registered) = tokio::sync::oneshot::channel();
            let mut armed = Some(armed);
            let sender = a.sender();
            let ready = async {
                poll_fn(|cx| {
                    let result = sender.poll_ready(1, cx);
                    if let Some(armed) = armed.take() {
                        assert!(result.is_pending());
                        armed.send(()).unwrap();
                    }
                    result
                })
                .await
            };
            let release = async {
                registered.await.unwrap();
                if cancel {
                    b.receiver().cancel();
                    Some(held)
                } else {
                    drop(held);
                    None
                }
            };
            let (result, still_held) =
                tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::join!(ready, release)
                })
                .await
                .expect("writer must wake");
            if cancel {
                assert!(matches!(result, Err(Error::PeerGone)));
                assert_eq!(&still_held.unwrap()[..], b"held");
            } else {
                assert!(matches!(result, Ok(())));
            }
        }
    }

    #[tokio::test]
    async fn cancellation_wakes_a_sender_waiting_on_application_work() {
        let exchanges = exchanges();
        let (mut a, ticket) = exchanges.create().unwrap();
        let mut b = exchanges.open_peer(ticket).unwrap();
        let (armed, registered) = tokio::sync::oneshot::channel();
        let mut armed = Some(armed);
        let mut closed = std::pin::pin!(a.sender().peer_closed());
        let wait = std::future::poll_fn(|cx| {
            let result = std::future::Future::poll(closed.as_mut(), cx);
            if let Some(armed) = armed.take() {
                assert!(result.is_pending());
                armed.send(()).unwrap();
            }
            result
        });
        let cancel = async {
            registered.await.unwrap();
            b.receiver().cancel();
            assert!(matches!(b.receiver().wait_readable(), Err(Error::Closed)));
        };
        let (result, ()) =
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(wait, cancel)
            })
            .await
            .expect("writer must wake");
        result.unwrap();
    }

    #[test]
    fn peer_can_send_before_the_first_flow_finishes() {
        let exchanges = exchanges();
        let (server, ticket) = exchanges.create().expect("server");
        let client = exchanges.open_peer(ticket).expect("client");
        let (mut request_out, mut response_in) = server.split();
        let (mut response_out, mut request_in) = client.split();

        request_out.start(Some(b"request-head")).expect("request start");
        let request_descriptor =
            request_out.data(&[7_u8; 192]).expect("request data");
        let request_metadata =
            match request_in.try_next().expect("request start event") {
                FlowEvent::Start { metadata: Some(metadata) } => metadata,
                _ => panic!("expected request start"),
            };
        assert_eq!(&*request_metadata, b"request-head");
        let request_chunk =
            match request_in.try_next().expect("request data event") {
                FlowEvent::Data(chunk) => chunk,
                _ => panic!("expected request data"),
            };
        assert_eq!(request_descriptor.arena_kind(), 231);
        assert_eq!(request_descriptor.slot_count(), 3);

        // Both directions share one arena, but their flow lifecycles are
        // independent, so the peer can start before the first flow finishes.
        response_out
            .start(Some(b"response-head"))
            .expect("early response start");
        let response_descriptor =
            response_out.data(b"response").expect("response data");
        assert_eq!(response_descriptor.arena_kind(), 231);
        assert!(matches!(response_in.try_next(), Ok(FlowEvent::Start { .. })));
        let response_chunk =
            match response_in.try_next().expect("response data event") {
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
    fn directional_payloads_keep_each_flows_credit_and_kind_separate() {
        let exchanges = Exchanges::open(
            Arc::new(
                Fleet::join("exchange-directional-test", 2).expect("fleet"),
            ),
            Incarnation::new(1),
            ExchangeSpec::directional(
                StreamSpec::new(220, 8, 512),
                PayloadArenaSpec::new(221, 1, 64),
                PayloadArenaSpec::new(222, 1, 128),
            ),
        )
        .expect("directional exchanges");
        let (mut side_a, ticket) = exchanges.create().expect("side A");
        let mut side_b = exchanges.open_peer(ticket).expect("side B");

        let request_geometry = side_a.sender().chunk_geometry();
        let response_geometry = side_b.sender().chunk_geometry();
        assert_eq!(request_geometry.slot_payload_bytes(), 64);
        assert_eq!(response_geometry.slot_payload_bytes(), 128);
        assert_eq!(request_geometry.chunk_bytes(1), Some(64));
        assert_eq!(response_geometry.chunk_bytes(1), Some(128));

        side_a.sender().start(None).expect("A-to-B start");
        assert!(matches!(
            side_b.receiver().try_next(),
            Ok(FlowEvent::Start { .. })
        ));
        side_b.sender().start(None).expect("B-to-A start");
        assert!(matches!(
            side_a.receiver().try_next(),
            Ok(FlowEvent::Start { .. })
        ));

        let a = side_a.sender().data(&[1; 64]).expect("A-to-B payload");
        let b = side_b.sender().data(&[2; 128]).expect("B-to-A payload");
        assert_eq!(a.arena_kind(), 221);
        assert_eq!(b.arena_kind(), 222);

        let a_payload = match side_b.receiver().try_next().expect("A-to-B data")
        {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected A-to-B data"),
        };
        let b_payload = match side_a.receiver().try_next().expect("B-to-A data")
        {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected B-to-A data"),
        };
        assert_eq!(&*a_payload, &[1; 64]);
        assert_eq!(&*b_payload, &[2; 128]);
    }

    #[test]
    fn a_reserved_chunk_is_one_descriptor_over_many_slots() {
        let exchanges = Exchanges::open(
            Arc::new(
                Fleet::join("exchange-reservation-test", 2).expect("fleet"),
            ),
            Incarnation::new(1),
            ExchangeSpec::new(
                StreamSpec::new(233, 8, 512),
                PayloadArenaSpec::new(234, 8, 256),
            ),
        )
        .expect("exchanges");
        let (mut server, ticket) = exchanges.create().expect("server");
        let mut client = exchanges.open_peer(ticket).expect("client");

        let expected: Vec<u8> =
            (0..777).map(|index| (index % 251) as u8).collect();
        let descriptor = {
            let mut pending =
                server.sender().reserve_start(expected.len()).expect("reserve");
            pending.copy_from_slice(&expected);
            pending.commit().expect("commit")
        };
        assert_eq!(descriptor.payload_len(), 777);
        assert_eq!(descriptor.slot_count(), 4);
        let received = match client.receiver().try_next().expect("start") {
            FlowEvent::Start { metadata: Some(metadata) } => metadata,
            _ => panic!("expected start metadata"),
        };
        assert_eq!(&*received, expected);
    }

    #[test]
    fn dropping_an_uncommitted_chunk_returns_credit_without_an_event() {
        let exchanges = exchanges();
        let (mut server, ticket) = exchanges.create().expect("server");
        let mut client = exchanges.open_peer(ticket).expect("client");
        server.sender().start(None).expect("start");
        assert!(matches!(
            client.receiver().try_next(),
            Ok(FlowEvent::Start { metadata: None })
        ));

        let pending =
            server.sender().reserve_data(256).expect("reserve all slots");
        drop(pending);
        assert!(matches!(client.receiver().try_next(), Err(Error::WouldBlock)));
        assert!(server.sender().reserve_data(256).is_ok());
    }

    #[test]
    fn a_failed_control_commit_returns_its_payload_credit() {
        let exchanges = Exchanges::open(
            Arc::new(
                Fleet::join("exchange-failed-commit-test", 2).expect("fleet"),
            ),
            Incarnation::new(1),
            ExchangeSpec::new(
                StreamSpec::new(236, 8, CONTROL_FRAME_BYTES),
                PayloadArenaSpec::new(237, 1, 64),
            ),
        )
        .expect("exchanges");
        let (mut server, ticket) = exchanges.create().expect("server");
        let mut client = exchanges.open_peer(ticket).expect("client");

        server.sender().start(None).expect("start fills control ring");
        let mut pending = server
            .sender()
            .reserve_data(64)
            .expect("reserve only payload slot");
        pending.fill(7);
        assert!(matches!(pending.commit(), Err(Error::WouldBlock)));

        assert!(matches!(
            client.receiver().try_next(),
            Ok(FlowEvent::Start { metadata: None })
        ));
        let mut retry = server
            .sender()
            .reserve_data(64)
            .expect("failed commit returned credit");
        retry.fill(9);
        retry.commit().expect("retry commit");
        let received = match client.receiver().try_next().expect("retry data") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected retry data"),
        };
        assert_eq!(&*received, &[9; 64]);
    }

    #[test]
    fn a_short_io_commit_returns_unused_trailing_slots() {
        let exchanges = exchanges();
        let (mut server, ticket) = exchanges.create().expect("server");
        let mut client = exchanges.open_peer(ticket).expect("client");
        server.sender().start(None).expect("start");
        assert!(matches!(
            client.receiver().try_next(),
            Ok(FlowEvent::Start { metadata: None })
        ));

        let descriptor = {
            let mut pending =
                server.sender().reserve_data(256).expect("reserve IO capacity");
            pending[..37].fill(5);
            pending.commit_len(37).expect("commit bytes read")
        };
        assert_eq!(descriptor.payload_len(), 37);
        assert_eq!(descriptor.slot_count(), 1);

        let mut following =
            server.sender().reserve_data(192).expect("trailing slots returned");
        following.fill(9);
        following.commit().expect("commit following chunk");
        let first = match client.receiver().try_next().expect("short data") {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected short data"),
        };
        assert_eq!(&*first, &[5; 37]);
        let second = match client.receiver().try_next().expect("following data")
        {
            FlowEvent::Data(chunk) => chunk,
            _ => panic!("expected following data"),
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
        assert!(matches!(
            response_in.try_next(),
            Ok(FlowEvent::Start { metadata: None })
        ));
    }
}
