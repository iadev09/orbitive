//! Bounded byte streams between two members of an Orbit fleet.
//!
//! A stream is a place with two ends. [`Streams::create`] takes a slot in
//! this process's lane, hands back the endpoint for side A and a [`Ticket`]
//! for side B; whoever in the fleet [`Streams::open`]s that ticket holds the
//! other end. Each direction is its own bounded ring: bytes go in at one end
//! and come out, in order and without loss, at the other; when the ring is
//! full the writer waits, when it is empty the reader waits. A writer that
//! is done shuts its direction down (FIN) and the reader drains to a clean
//! end; a writer that gives up resets it and the reader sees an error.
//!
//! Standalone the rings live in process memory; in a shared-memory fleet
//! they live in the fleet's segment and the two ends may be in different
//! processes. The address that travels is a [`NetId64`]: its kind is the
//! segment's, its node the creator's lane, its counter the slot and the
//! slot's generation; the [`Ticket`] around it adds the table's epoch. An
//! address kept past the stream's end, or past a reset, answers
//! [`Error::Stale`] instead of reaching whoever took the slot next.
//!
//! The blocking calls park on the direction's own change word through the
//! platform's shared address wait, like a cell. With the `tokio` feature the
//! halves implement `AsyncRead` and `AsyncWrite`; a poll that finds nothing
//! registers its waker and returns `Pending`, and one thread per process
//! turns the fleet's doorbell into those wakes. No thread and no descriptor
//! per stream.

use std::fmt;
use std::io;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::{Bytes, BytesMut};
use orbit_core::{Fleet, NetId64};

mod layout;
#[cfg(feature = "tokio")]
mod poll;
mod table;
mod wake;

use layout::{
    Direction, FLAG_FIN, FLAG_READER_GONE, FLAG_RESET, GENERATION_MASK, SIDE_CLAIMED, SIDE_FREE,
    SIDE_RELEASED, SLOT_BITS, SLOT_EMPTY, SLOT_LIVE, SLOT_MASK, Slot,
};
use orbit_core::NodeId;
use table::Table;
pub use table::{segment_size, segment_size_for};
use wake::Interest;

/// Reserved Orbit SHM kind for the default stream segment. Another table
/// names its own through a [`StreamSpec`].
pub const STREAM_KIND: u8 = 246;
/// Streams one fleet node can hold open at once, in the default spec.
///
/// Compile-time geometry: `ORBIT_STREAM_LANE_CAPACITY` in the application's
/// `.cargo/config.toml` overrides the default. A power of two, at most
/// 65 536. It is the *default* table's value; a [`StreamSpec`] gives
/// another table another one. Peers opening the same kind with a different
/// value are refused at the segment.
pub const STREAM_LANE_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_STREAM_LANE_CAPACITY"), 256);
/// Bytes each direction of a stream can hold before its writer waits, in
/// the default spec.
///
/// Compile-time geometry: `ORBIT_STREAM_BUFFER_BYTES`. A power of two. Part
/// of the same wire contract as the lane capacity, and like it a per-table
/// choice through [`StreamSpec`].
pub const STREAM_BUFFER_BYTES: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_STREAM_BUFFER_BYTES"), 64 * 1024);

/// Which segment a [`Streams`] table uses, and how big its lanes and
/// rings are.
///
/// One fleet can hold several independent stream tables. They are not
/// interchangeable: a relay carrying response bodies wants a ring sized to
/// a body, while a control channel carrying short frames is faster with a
/// small one, and a lane is a ceiling on concurrent streams per node. Each
/// names its own kind, and a kind is a fleet-wide identity — every process
/// opening it must pass the same geometry, and the segment's header
/// refuses a peer that does not. [`StreamSpec::DEFAULT`] is what
/// [`Streams::new`] opens; its values are the compile-time geometry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StreamSpec {
    /// The Orbit SHM kind, and with it the segment name.
    pub kind: u8,
    /// Streams one fleet node can hold open at once. A power of two, at
    /// most 65 536.
    pub lane_capacity: usize,
    /// Bytes each direction holds before its writer waits. A power of two.
    pub buffer_bytes: usize,
}

impl StreamSpec {
    pub const DEFAULT: Self = Self::new(STREAM_KIND, STREAM_LANE_CAPACITY, STREAM_BUFFER_BYTES);

    pub const fn new(kind: u8, lane_capacity: usize, buffer_bytes: usize) -> Self {
        Self { kind, lane_capacity, buffer_bytes }
    }

    /// The segment one node's streams occupy at most: every slot's two
    /// rings. Memory is backed as it is touched, but this is the ceiling
    /// a deployment is choosing when it picks a lane and a ring.
    pub const fn lane_bytes(&self) -> usize {
        self.lane_capacity * 2 * self.buffer_bytes
    }

    /// What the compile-time geometry used to assert. A spec is checked
    /// once, when its table is opened.
    fn validate(self) -> Result<()> {
        if self.lane_capacity == 0
            || self.buffer_bytes == 0
            || !self.lane_capacity.is_power_of_two()
            || !self.buffer_bytes.is_power_of_two()
            || self.lane_capacity > 1 << SLOT_BITS
            || self.buffer_bytes > u32::MAX as usize
        {
            return Err(Error::Malformed(format!(
                "stream spec kind={} lane_capacity={} buffer_bytes={}: both are powers of two, a lane holds at most {} streams and a ring at most {} bytes",
                self.kind,
                self.lane_capacity,
                self.buffer_bytes,
                1_usize << SLOT_BITS,
                u32::MAX
            )));
        }
        Ok(())
    }
}

impl Default for StreamSpec {
    fn default() -> Self {
        Self::DEFAULT
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    /// The address names a slot nothing occupies, or a generation that ended.
    Stale(StreamId),
    /// That side of the stream is already held, or was held and released.
    AlreadyClaimed(Ticket),
    /// Every slot in this process's lane is in use.
    Full {
        capacity: usize,
    },
    /// The text or id is not a stream address.
    Malformed(String),
    /// This write half was shut down; nothing more goes in.
    Closed,
    /// The other end reset the direction; buffered bytes were discarded.
    Reset,
    /// The other end's reader is gone; nothing written here will be read.
    PeerGone,
    /// Nothing to read, or no room to write, right now.
    WouldBlock,
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stale(id) => write!(f, "stream {id} has ended"),
            Self::AlreadyClaimed(ticket) => write!(f, "stream side {ticket} is already held"),
            Self::Full { capacity } => write!(f, "stream lane is full: capacity={capacity}"),
            Self::Malformed(text) => write!(f, "not a stream address: {text:?}"),
            Self::Closed => f.write_str("stream write half is shut down"),
            Self::Reset => f.write_str("stream direction was reset by the peer"),
            Self::PeerGone => f.write_str("stream peer reader is gone"),
            Self::WouldBlock => f.write_str("stream operation would block"),
            Self::Io(error) => write!(f, "Orbit stream io error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<Error> for io::Error {
    fn from(value: Error) -> Self {
        let kind = match &value {
            Error::Stale(_) | Error::AlreadyClaimed(_) => io::ErrorKind::NotConnected,
            Error::Full { .. } => io::ErrorKind::OutOfMemory,
            Error::Malformed(_) => io::ErrorKind::InvalidInput,
            Error::Closed => io::ErrorKind::BrokenPipe,
            Error::Reset => io::ErrorKind::ConnectionReset,
            Error::PeerGone => io::ErrorKind::BrokenPipe,
            Error::WouldBlock => io::ErrorKind::WouldBlock,
            Error::Io(error) => error.kind(),
        };
        io::Error::new(kind, value)
    }
}

/// The address of one stream: a [`NetId64`] whose node is the creator's
/// lane and whose counter is the slot and its generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamId(NetId64);

impl StreamId {
    pub const fn from_net_id(id: NetId64) -> Self {
        Self(id)
    }

    pub const fn net_id(self) -> NetId64 {
        self.0
    }

    pub const fn kind(self) -> u8 {
        self.0.kind()
    }

    /// The lane: the fleet node that created the stream.
    pub const fn node(self) -> u16 {
        self.0.node()
    }

    pub const fn slot(self) -> u32 {
        (self.0.counter() & SLOT_MASK) as u32
    }

    pub const fn generation(self) -> u32 {
        (self.0.counter() >> SLOT_BITS) as u32
    }

    fn make(kind: u8, node: u16, slot: u32, generation: u32) -> Self {
        Self(NetId64::make(
            kind,
            node,
            ((generation as u64) << SLOT_BITS) | (slot as u64 & SLOT_MASK),
        ))
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for StreamId {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        text.parse::<NetId64>()
            .map(Self)
            .map_err(|_| Error::Malformed(text.to_owned()))
    }
}

/// Which end of a stream a handle holds. The creator holds `A`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Side {
    A,
    B,
}

impl Side {
    const fn index(self) -> usize {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }

    /// The direction this side writes into.
    const fn write_direction(self) -> usize {
        self.index()
    }

    /// The direction this side reads from.
    const fn read_direction(self) -> usize {
        1 - self.index()
    }

    const fn letter(self) -> char {
        match self {
            Self::A => 'a',
            Self::B => 'b',
        }
    }
}

/// One end of a stream, in the form that crosses a setup channel: the
/// address, the side, and the epoch of the table the address was minted
/// in. Prints as `<address>/b/<epoch>`.
///
/// Two things keep a ticket from reaching the wrong memory: the slot
/// generation inside the address separates reuses of a slot within one
/// epoch, and the epoch separates before and after a quiescent reset or a
/// recreated segment, which reinstalls slots from generation one.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Ticket {
    pub id: StreamId,
    pub side: Side,
    pub epoch: u64,
}

impl fmt::Display for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.id, self.side.letter(), self.epoch)
    }
}

impl FromStr for Ticket {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        let malformed = || Error::Malformed(text.to_owned());
        let (rest, epoch) = text.rsplit_once('/').ok_or_else(malformed)?;
        let (id, side) = rest.rsplit_once('/').ok_or_else(malformed)?;
        let side = match side {
            "a" => Side::A,
            "b" => Side::B,
            _ => return Err(malformed()),
        };
        Ok(Self {
            id: id.parse().map_err(|_| malformed())?,
            side,
            epoch: epoch.parse().map_err(|_| malformed())?,
        })
    }
}

/// Which life of a process holds a side. A node id is a role that a
/// replacement process inherits; the incarnation tells the two apart, so a
/// late death report for the old process never touches the new one's
/// streams. Orbit does not mint it: the embedder knows what identifies one
/// live process (a start stamp, a supervisor generation) and supplies it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Incarnation(u64);

impl Incarnation {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The fleet's stream table. Cheap to clone; every clone in a process is
/// the same table, the same driver and the same wakers.
#[derive(Clone)]
pub struct Streams {
    table: Arc<Table>,
}

impl Streams {
    /// Open the fleet's table as this process's incarnation. Every side
    /// this process claims is stamped with it, and [`Streams::node_dead`]
    /// for the same node and incarnation is what ends those sides.
    pub fn new(fleet: Arc<Fleet>, incarnation: Incarnation) -> Result<Self> {
        Self::with_spec(fleet, incarnation, StreamSpec::DEFAULT)
    }

    /// Open the table `spec` names. Independent specs are independent
    /// tables: separate segments, separate lanes and rings, separate
    /// epochs, and a `reset_all` on one leaves the others alone. A process
    /// may hold as many as it has specs, but each under one incarnation.
    pub fn with_spec(
        fleet: Arc<Fleet>,
        incarnation: Incarnation,
        spec: StreamSpec,
    ) -> Result<Self> {
        spec.validate()?;
        Ok(Self {
            table: table::open(&fleet, incarnation, spec)?,
        })
    }

    /// The kind this table's segment lives under.
    pub fn kind(&self) -> u8 {
        self.table.kind()
    }

    /// The epoch every ticket minted from this table carries right now.
    pub fn epoch(&self) -> u64 {
        self.table.epoch()
    }

    /// Take a slot in this process's lane. The endpoint is side A; the
    /// ticket names side B and is what the other end opens.
    pub fn create(&self) -> Result<(Endpoint, Ticket)> {
        let (index, generation) = self.table.allocate()?;
        let id = StreamId::make(
            self.table.kind(),
            self.table.node(),
            (index % self.table.geometry().lane_capacity) as u32,
            generation,
        );
        let handle = Arc::new(Handle {
            table: Arc::clone(&self.table),
            index,
            id,
            side: Side::A,
        });
        Ok((
            Endpoint::new(handle),
            Ticket {
                id,
                side: Side::B,
                epoch: self.table.epoch(),
            },
        ))
    }

    /// Hold the side a ticket names. Each side can be held once per stream.
    pub fn open(&self, ticket: Ticket) -> Result<Endpoint> {
        let index = self.locate(ticket.id)?;
        if ticket.epoch != self.table.epoch() {
            return Err(Error::Stale(ticket.id));
        }
        let slot = &self.table.slots()[index];
        if !slot.is(ticket.id.generation()) {
            return Err(Error::Stale(ticket.id));
        }
        let claimed = &slot.claimed[ticket.side.index()];
        if claimed
            .compare_exchange(SIDE_FREE, SIDE_CLAIMED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::AlreadyClaimed(ticket));
        }
        // The slot may have ended between the check and the claim; give the
        // claim back rather than hold a side of whatever comes next.
        if !slot.is(ticket.id.generation()) {
            let _ = claimed.compare_exchange(
                SIDE_CLAIMED,
                SIDE_FREE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            return Err(Error::Stale(ticket.id));
        }
        slot.incarnation[ticket.side.index()].store(self.table.incarnation(), Ordering::Release);
        slot.node[ticket.side.index()].store(self.table.node(), Ordering::Release);
        // Whoever was waiting for this side to show up.
        for direction in &slot.directions {
            self.table.notify(index, slot, direction, true);
        }
        Ok(Endpoint::new(Arc::new(Handle {
            table: Arc::clone(&self.table),
            index,
            id: ticket.id,
            side: ticket.side,
        })))
    }

    /// Whether the address names a live stream right now.
    pub fn is_live(&self, id: StreamId) -> bool {
        self.locate(id)
            .map(|index| self.table.slots()[index].is(id.generation()))
            .unwrap_or(false)
    }

    /// Tell `node`'s process that side B of this stream is its to take:
    /// the slot is put in that node's offer bitmap and its doorbell rung.
    /// Discovery only; the ticket may just as well travel by any other
    /// channel. An offer is seen once, by that node, through
    /// [`Streams::take_offer`] and its waiting forms.
    pub fn offer(&self, ticket: Ticket, to: NodeId) -> Result<()> {
        let index = self.locate(ticket.id)?;
        if ticket.epoch != self.table.epoch()
            || !self.table.slots()[index].is(ticket.id.generation())
        {
            return Err(Error::Stale(ticket.id));
        }
        self.table.offer(index, to.get())
    }

    /// The next stream offered to this node, if any. A stream that ended
    /// between the offer and now is skipped.
    pub fn take_offer(&self) -> Option<Ticket> {
        while let Some(index) = self.table.take_offer() {
            if let Some(ticket) = self.ticket_for(index) {
                return Some(ticket);
            }
        }
        None
    }

    /// Park the thread until a stream is offered to this node.
    pub fn blocking_take_offer(&self) -> Result<Ticket> {
        loop {
            if let Some(ticket) = self.take_offer() {
                return Ok(ticket);
            }
            self.table.wait_offer()?;
        }
    }

    /// Readiness for a task: the next offer, or the waker is registered
    /// and `Pending` comes back.
    pub fn poll_take_offer(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Ticket>> {
        if let Some(ticket) = self.take_offer() {
            return std::task::Poll::Ready(Ok(ticket));
        }
        if let Err(error) = self.table.register_offer(cx.waker()) {
            return std::task::Poll::Ready(Err(error));
        }
        match self.take_offer() {
            Some(ticket) => std::task::Poll::Ready(Ok(ticket)),
            None => std::task::Poll::Pending,
        }
    }

    /// A confirmed death, reported by whoever supervises processes: every
    /// side that incarnation of `node` held, in any lane, is finished. Its
    /// unfinished writes become resets and its reads are gone, so a holder
    /// of the other side wakes with an error; that holder keeps the slot
    /// until it drops its handles, and only then is the slot reused. A
    /// report for another incarnation of the same node touches nothing.
    pub fn node_dead(&self, node: NodeId, incarnation: Incarnation) {
        self.table.node_dead(node.get(), incarnation.get());
    }

    /// Clear the table during quiescent owner boot and start a new epoch.
    /// Every handle and every ticket from before goes stale.
    pub fn reset_all(&self) {
        self.table.reset_all();
    }

    fn ticket_for(&self, index: usize) -> Option<Ticket> {
        let slot = &self.table.slots()[index];
        let generation = slot.generation.load(Ordering::Acquire);
        if !slot.is(generation) {
            return None;
        }
        let lane_capacity = self.table.geometry().lane_capacity;
        let id = StreamId::make(
            self.table.kind(),
            (index / lane_capacity) as u16,
            (index % lane_capacity) as u32,
            generation,
        );
        Some(Ticket {
            id,
            side: Side::B,
            epoch: self.table.epoch(),
        })
    }

    /// Remove the SHM object. Existing mappings stay valid until their
    /// processes release them.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        self.table.unlink()
    }

    fn locate(&self, id: StreamId) -> Result<usize> {
        let geometry = self.table.geometry();
        if id.kind() != self.table.kind()
            || usize::from(id.node()) >= geometry.fleet_capacity
            || id.slot() as usize >= geometry.lane_capacity
            || id.generation() == 0
            || id.generation() > GENERATION_MASK
        {
            return Err(Error::Malformed(id.to_string()));
        }
        Ok(usize::from(id.node()) * geometry.lane_capacity + id.slot() as usize)
    }
}

/// One held side. Dropped when both halves are gone; that releases the side.
struct Handle {
    table: Arc<Table>,
    index: usize,
    id: StreamId,
    side: Side,
}

impl Handle {
    fn slot(&self) -> Result<&Slot> {
        let slot = &self.table.slots()[self.index];
        if slot.is(self.id.generation()) {
            Ok(slot)
        } else {
            Err(Error::Stale(self.id))
        }
    }

    fn direction(slot: &Slot, direction: usize) -> &Direction {
        &slot.directions[direction]
    }

    /// One attempt at the direction this side writes into.
    fn try_write(&self, buf: &[u8]) -> Result<usize> {
        let slot = self.slot()?;
        let direction = Self::direction(slot, self.side.write_direction());
        let flags = direction.flags();
        if flags & FLAG_RESET != 0 {
            return Err(Error::Reset);
        }
        if flags & FLAG_FIN != 0 {
            return Err(Error::Closed);
        }
        if flags & FLAG_READER_GONE != 0 {
            return Err(Error::PeerGone);
        }
        let buffer_bytes = self.table.geometry().buffer_bytes;
        let head = direction.head.load(Ordering::Relaxed);
        let tail = direction.tail.load(Ordering::Acquire);
        let free = buffer_bytes - (head - tail) as usize;
        if buf.is_empty() {
            return Ok(0);
        }
        if free == 0 {
            return Err(Error::WouldBlock);
        }
        let len = buf.len().min(free);
        let base = self.table.buffer(self.index, self.side.write_direction());
        let start = (head as usize) & (buffer_bytes - 1);
        let first = len.min(buffer_bytes - start);
        // SAFETY: only this side writes this direction, and `[head, head+len)`
        // is free space the reader will not touch until `head` moves.
        unsafe {
            std::ptr::copy_nonoverlapping(buf.as_ptr(), base.add(start), first);
            std::ptr::copy_nonoverlapping(buf.as_ptr().add(first), base, len - first);
        }
        direction.head.store(head + len as u64, Ordering::SeqCst);
        // A reader parks only on an empty ring: it looks, registers, looks
        // again. Whether one could be parked is the tail *after* this
        // commit, never the one read before the copy — the reader may have
        // drained the ring and parked while we were copying into it, and
        // that older tail would say "nobody to wake" about a task asleep on
        // these very bytes. Each side orders its own store before the
        // other's load, so at least one of the two sees the other: the
        // reader sees the new head and does not park, or this sees the
        // drained tail and rings.
        std::sync::atomic::fence(Ordering::SeqCst);
        let drained = direction.tail.load(Ordering::SeqCst) >= head;
        self.table.notify(self.index, slot, direction, drained);
        Ok(len)
    }

    /// One attempt at the direction this side reads from. `Ok(0)` with a
    /// non-empty buffer is clean end of stream.
    fn try_read(&self, buf: &mut [u8]) -> Result<usize> {
        let slot = self.slot()?;
        let direction = Self::direction(slot, self.side.read_direction());
        let flags = direction.flags();
        if flags & FLAG_RESET != 0 {
            return Err(Error::Reset);
        }
        let head = direction.head.load(Ordering::Acquire);
        let tail = direction.tail.load(Ordering::Relaxed);
        let available = (head - tail) as usize;
        if buf.is_empty() {
            return Ok(0);
        }
        if available == 0 {
            if flags & FLAG_FIN != 0 {
                return Ok(0);
            }
            return Err(Error::WouldBlock);
        }
        let len = buf.len().min(available);
        let base = self.table.buffer(self.index, self.side.read_direction());
        let buffer_bytes = self.table.geometry().buffer_bytes;
        let start = (tail as usize) & (buffer_bytes - 1);
        let first = len.min(buffer_bytes - start);
        // SAFETY: `[tail, head)` was published by the writer's `Release`
        // store of `head`, acquired above, and is not rewritten until `tail`
        // moves past it.
        unsafe {
            std::ptr::copy_nonoverlapping(base.add(start), buf.as_mut_ptr(), first);
            std::ptr::copy_nonoverlapping(base, buf.as_mut_ptr().add(first), len - first);
        }
        direction.tail.store(tail + len as u64, Ordering::SeqCst);
        // The mirror image, and the other half of that agreement: a writer
        // parks only on a full ring, so the head read after this consume is
        // what says whether one is parked on the space just freed.
        std::sync::atomic::fence(Ordering::SeqCst);
        let head_now = direction.head.load(Ordering::SeqCst);
        let was_full = head_now - tail >= buffer_bytes as u64;
        self.table.notify(self.index, slot, direction, was_full);
        Ok(len)
    }

    fn set_flag(&self, direction_index: usize, flag: u8) {
        if let Ok(slot) = self.slot() {
            let direction = Self::direction(slot, direction_index);
            if direction.flags.fetch_or(flag, Ordering::SeqCst) & flag == 0 {
                self.table.notify(self.index, slot, direction, true);
            }
        }
    }

    /// Park until `ready` holds for the direction, or the stream ends.
    fn wait_until(&self, direction_index: usize, ready: impl Fn(&Direction) -> bool) -> Result<()> {
        loop {
            let slot = self.slot()?;
            let direction = Self::direction(slot, direction_index);
            if ready(direction) {
                return Ok(());
            }
            direction.waiters.fetch_add(1, Ordering::SeqCst);
            let since = direction.changes.load(Ordering::SeqCst);
            let outcome = if ready(direction) {
                Ok(())
            } else {
                wait_on(&direction.changes, since)
            };
            direction.waiters.fetch_sub(1, Ordering::SeqCst);
            outcome?;
        }
    }

    /// Check, register, check again: a change between the first look and
    /// the registration is not missed.
    fn poll_ready(
        &self,
        interest: Interest,
        ready: impl Fn(&Direction) -> bool,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        let slot = match self.slot() {
            Ok(slot) => slot,
            Err(error) => return std::task::Poll::Ready(Err(error)),
        };
        let direction = Self::direction(slot, interest.direction);
        if ready(direction) {
            return std::task::Poll::Ready(Ok(()));
        }
        if let Err(error) = self.table.register(self.index, interest, cx.waker()) {
            return std::task::Poll::Ready(Err(error));
        }
        if ready(direction) {
            std::task::Poll::Ready(Ok(()))
        } else {
            std::task::Poll::Pending
        }
    }
}

fn readable(direction: &Direction) -> bool {
    direction.head.load(Ordering::Acquire) != direction.tail.load(Ordering::Relaxed)
        || direction.flags() & (FLAG_FIN | FLAG_RESET) != 0
}

fn writable(direction: &Direction, buffer_bytes: usize) -> bool {
    let queued =
        (direction.head.load(Ordering::Relaxed) - direction.tail.load(Ordering::Acquire)) as usize;
    queued < buffer_bytes
        || direction.flags() & (FLAG_FIN | FLAG_RESET | FLAG_READER_GONE) != 0
}

impl Drop for Handle {
    /// Both halves are gone: give the side back. The slot empties once no
    /// side holds it any more, and every address to this generation goes
    /// stale.
    fn drop(&mut self) {
        let slot = &self.table.slots()[self.index];
        if !slot.is(self.id.generation()) {
            return;
        }
        // Only a side that is still ours is ours to release: a death
        // report may have finished it already.
        if slot.claimed[self.side.index()]
            .compare_exchange(
                SIDE_CLAIMED,
                SIDE_RELEASED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }
        // Side A is claimed at creation, so a free B was simply never
        // taken; a released or dead side is gone. Only a claimed side keeps
        // the slot alive.
        let other = slot.claimed[1 - self.side.index()].load(Ordering::SeqCst);
        if other != SIDE_CLAIMED {
            let _ = slot.state.compare_exchange(
                SLOT_LIVE,
                SLOT_EMPTY,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            for direction in &slot.directions {
                // A parked waiter with a handle to this generation wakes
                // and finds it stale.
                direction.changes.fetch_add(1, Ordering::SeqCst);
                if direction.waiters.load(Ordering::SeqCst) > 0 {
                    wake_on(&direction.changes);
                }
            }
            self.table.registry.wake(self.index);
        }
    }
}

/// Both ends of one side: what [`Streams::create`] and [`Streams::open`]
/// return. Reads and writes go through it directly, or [`Endpoint::split`]
/// gives the halves to two tasks.
pub struct Endpoint {
    read: ReadHalf,
    write: WriteHalf,
}

impl Endpoint {
    fn new(handle: Arc<Handle>) -> Self {
        Self {
            read: ReadHalf {
                handle: Arc::clone(&handle),
            },
            write: WriteHalf {
                handle,
                shut: false,
            },
        }
    }

    pub fn id(&self) -> StreamId {
        self.read.handle.id
    }

    pub fn side(&self) -> Side {
        self.read.handle.side
    }

    pub fn split(self) -> (ReadHalf, WriteHalf) {
        (self.read, self.write)
    }

    pub fn try_read(&self, buf: &mut [u8]) -> Result<usize> {
        self.read.try_read(buf)
    }

    pub fn blocking_read(&self, buf: &mut [u8]) -> Result<usize> {
        self.read.blocking_read(buf)
    }

    pub fn blocking_read_chunk(&self, max: usize) -> Result<Bytes> {
        self.read.blocking_read_chunk(max)
    }

    pub fn try_write(&self, buf: &[u8]) -> Result<usize> {
        self.write.try_write(buf)
    }

    pub fn blocking_write(&self, buf: &[u8]) -> Result<usize> {
        self.write.blocking_write(buf)
    }

    pub fn blocking_write_all(&self, buf: &[u8]) -> Result<()> {
        self.write.blocking_write_all(buf)
    }

    pub fn finish(&mut self) -> Result<()> {
        self.write.finish()
    }

    pub fn reset(&mut self) {
        self.write.reset()
    }
}

/// The reading end of one side. Dropping it tells the peer's writer that
/// nothing more will be read.
pub struct ReadHalf {
    handle: Arc<Handle>,
}

impl ReadHalf {
    pub fn id(&self) -> StreamId {
        self.handle.id
    }

    /// Read what is there now. `Ok(0)` is clean end of stream;
    /// [`Error::WouldBlock`] means nothing yet.
    pub fn try_read(&self, buf: &mut [u8]) -> Result<usize> {
        self.handle.try_read(buf)
    }

    /// Park until something can be read, then read it. `Ok(0)` is clean
    /// end of stream. Blocks the thread; an async runtime uses `AsyncRead`.
    pub fn blocking_read(&self, buf: &mut [u8]) -> Result<usize> {
        loop {
            match self.handle.try_read(buf) {
                Err(Error::WouldBlock) => self.wait_readable()?,
                other => return other,
            }
        }
    }

    /// Up to `max` bytes as one owned chunk; empty at clean end of stream.
    pub fn blocking_read_chunk(&self, max: usize) -> Result<Bytes> {
        let mut chunk = BytesMut::zeroed(max);
        let len = self.blocking_read(&mut chunk)?;
        chunk.truncate(len);
        Ok(chunk.freeze())
    }

    /// Park until a read would make progress or the direction ended.
    pub fn wait_readable(&self) -> Result<()> {
        self.handle
            .wait_until(self.handle.side.read_direction(), readable)
    }

    /// Readiness for a task: `Ready` when a read would make progress or the
    /// direction ended, otherwise the waker is registered and `Pending`
    /// comes back. Runtime-neutral; the `tokio` feature builds `AsyncRead`
    /// on it.
    pub fn poll_readable(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<()>> {
        self.handle.poll_ready(
            Interest {
                direction: self.handle.side.read_direction(),
                writer: false,
            },
            readable,
            cx,
        )
    }
}

impl Drop for ReadHalf {
    fn drop(&mut self) {
        self.handle
            .set_flag(self.handle.side.read_direction(), FLAG_READER_GONE);
    }
}

/// The writing end of one side. Dropping it without [`WriteHalf::finish`]
/// resets the direction, as a dropped socket would.
pub struct WriteHalf {
    handle: Arc<Handle>,
    shut: bool,
}

impl WriteHalf {
    pub fn id(&self) -> StreamId {
        self.handle.id
    }

    /// Write what fits now; [`Error::WouldBlock`] when the ring is full.
    pub fn try_write(&self, buf: &[u8]) -> Result<usize> {
        self.handle.try_write(buf)
    }

    /// Park until something fits, then write it. Blocks the thread.
    pub fn blocking_write(&self, buf: &[u8]) -> Result<usize> {
        loop {
            match self.handle.try_write(buf) {
                Err(Error::WouldBlock) => self.wait_writable()?,
                other => return other,
            }
        }
    }

    /// Write everything, waiting as needed. A failure part-way has already
    /// committed a prefix; nothing is replayed.
    pub fn blocking_write_all(&self, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            let written = self.blocking_write(buf)?;
            buf = &buf[written..];
        }
        Ok(())
    }

    /// No more bytes from this side (FIN); what is buffered still reaches
    /// the reader, then it sees a clean end. The other direction is
    /// unaffected. `AsyncWriteExt::shutdown` does the same.
    pub fn finish(&mut self) -> Result<()> {
        self.shut = true;
        self.handle
            .set_flag(self.handle.side.write_direction(), FLAG_FIN);
        Ok(())
    }

    /// Abandon the direction: the reader gets an error and buffered bytes
    /// are discarded.
    pub fn reset(&mut self) {
        self.shut = true;
        self.handle
            .set_flag(self.handle.side.write_direction(), FLAG_RESET);
    }

    /// Park until a write would make progress or the direction ended.
    pub fn wait_writable(&self) -> Result<()> {
        let buffer_bytes = self.handle.table.geometry().buffer_bytes;
        self.handle.wait_until(self.handle.side.write_direction(), |direction| {
            writable(direction, buffer_bytes)
        })
    }

    /// Readiness for a task: `Ready` when a write would make progress or
    /// the direction ended, otherwise the waker is registered and `Pending`
    /// comes back. Runtime-neutral; the `tokio` feature builds `AsyncWrite`
    /// on it.
    pub fn poll_writable(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<()>> {
        let buffer_bytes = self.handle.table.geometry().buffer_bytes;
        self.handle.poll_ready(
            Interest {
                direction: self.handle.side.write_direction(),
                writer: true,
            },
            |direction| writable(direction, buffer_bytes),
            cx,
        )
    }
}

impl Drop for WriteHalf {
    fn drop(&mut self) {
        if !self.shut {
            self.handle
                .set_flag(self.handle.side.write_direction(), FLAG_RESET);
        }
    }
}

/// Park until `word` no longer holds `expected`, through the platform's
/// shared address wait; where there is none, a short sleep and a re-check.
#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
pub(crate) fn wait_on(word: &AtomicU32, expected: u32) -> Result<()> {
    match orbit_core::sync::wait_word(word, expected) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::Unsupported => {
            std::thread::sleep(std::time::Duration::from_millis(1));
            Ok(())
        }
        Err(error) => Err(Error::Io(error)),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
pub(crate) fn wait_on(_word: &AtomicU32, _expected: u32) -> Result<()> {
    std::thread::sleep(std::time::Duration::from_millis(1));
    Ok(())
}

/// Wake everyone parked on `word`; nothing to do where nobody can park.
pub(crate) fn wake_on(word: &AtomicU32) {
    #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    let _ = orbit_core::sync::wake_word(word);
    #[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
    let _ = word;
}

pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use orbit_core::{Fleet, NodeId};

    use super::{
        Error, Incarnation, STREAM_BUFFER_BYTES, STREAM_LANE_CAPACITY, Side, Streams, Ticket,
    };

    fn streams(name: &'static str) -> Streams {
        Streams::new(Arc::new(Fleet::join(name, 2).unwrap()), Incarnation::new(1)).unwrap()
    }

    #[test]
    fn bytes_cross_in_order_and_end_cleanly() {
        let streams = streams("stream-order");
        let (mut a, ticket) = streams.create().unwrap();
        assert_eq!(ticket.side, Side::B);
        assert_eq!(ticket.id.node(), 0);
        assert_eq!(ticket.epoch, streams.epoch());
        let b = streams.open(ticket).unwrap();

        a.blocking_write_all(b"hello, ").unwrap();
        a.blocking_write_all(b"world").unwrap();
        a.finish().unwrap();

        let mut out = Vec::new();
        let mut buf = [0_u8; 4];
        loop {
            let n = b.blocking_read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, b"hello, world");
        // The other direction is its own thing.
        b.blocking_write_all(b"back").unwrap();
        assert_eq!(a.blocking_read_chunk(16).unwrap().as_ref(), b"back");
    }

    #[test]
    fn a_ticket_prints_and_parses() {
        let streams = streams("stream-ticket");
        let (_a, ticket) = streams.create().unwrap();
        let text = ticket.to_string();
        assert!(text.contains("/b/"), "{text}");
        assert_eq!(text.parse::<Ticket>().unwrap(), ticket);
        assert!(matches!("nope".parse::<Ticket>(), Err(Error::Malformed(_))));
        assert!(matches!("x/b".parse::<Ticket>(), Err(Error::Malformed(_))));
    }

    #[test]
    fn a_side_is_held_once_and_a_released_stream_goes_stale() {
        let streams = streams("stream-claim");
        let (a, ticket) = streams.create().unwrap();
        let b = streams.open(ticket).unwrap();
        assert!(matches!(
            streams.open(ticket),
            Err(Error::AlreadyClaimed(_))
        ));
        assert!(streams.is_live(ticket.id));

        drop(a);
        // A's write half went without finishing: B reads a reset.
        let mut buf = [0_u8; 1];
        assert!(matches!(b.blocking_read(&mut buf), Err(Error::Reset)));
        drop(b);
        assert!(!streams.is_live(ticket.id));
        assert!(matches!(streams.open(ticket), Err(Error::Stale(_))));

        // Once the lane wraps, the slot is reused under a new generation and
        // the old ticket stays stale.
        for _ in 0..STREAM_LANE_CAPACITY - 1 {
            drop(streams.create().unwrap());
        }
        let (_a2, ticket2) = streams.create().unwrap();
        assert_eq!(ticket2.id.slot(), ticket.id.slot());
        assert_ne!(ticket2.id.generation(), ticket.id.generation());
        assert!(matches!(streams.open(ticket), Err(Error::Stale(_))));
    }

    #[test]
    fn a_reset_starts_a_new_epoch_and_refuses_old_tickets() {
        let streams = streams("stream-epoch");
        let (_a, ticket) = streams.create().unwrap();
        let before = streams.epoch();
        streams.reset_all();
        assert!(streams.epoch() > before);
        // The slot is free again and reinstalled from generation one, which
        // is exactly what the old ticket names; the epoch tells them apart.
        let (_a2, ticket2) = streams.create().unwrap();
        assert_eq!(ticket2.id, ticket.id);
        assert_ne!(ticket2.epoch, ticket.epoch);
        assert!(matches!(streams.open(ticket), Err(Error::Stale(_))));
        assert!(streams.open(ticket2).is_ok());
    }

    #[test]
    fn a_slot_whose_generation_runs_out_is_not_reused() {
        // GENERATION_LIMIT is 4 under test: each slot can be installed four
        // times in one epoch, then it sits out until the next epoch.
        let streams = streams("stream-exhaust");
        let limit = super::layout::GENERATION_LIMIT as usize;
        for _ in 0..STREAM_LANE_CAPACITY * limit {
            drop(streams.create().unwrap());
        }
        assert!(matches!(streams.create(), Err(Error::Full { .. })));
        streams.reset_all();
        let (_a, ticket) = streams.create().unwrap();
        assert_eq!(ticket.id.generation(), 1);
    }

    #[test]
    fn a_full_ring_blocks_the_writer_and_wraps_correctly() {
        let streams = streams("stream-wrap");
        let (a, ticket) = streams.create().unwrap();
        let b = streams.open(ticket).unwrap();

        let filler = vec![7_u8; STREAM_BUFFER_BYTES];
        assert_eq!(a.try_write(&filler).unwrap(), STREAM_BUFFER_BYTES);
        assert!(matches!(a.try_write(b"x"), Err(Error::WouldBlock)));

        let mut buf = vec![0_u8; STREAM_BUFFER_BYTES - 5];
        assert_eq!(b.blocking_read(&mut buf).unwrap(), STREAM_BUFFER_BYTES - 5);
        // Room again; the next write wraps around the end of the ring.
        let tail = (0..STREAM_BUFFER_BYTES)
            .map(|i| i as u8)
            .collect::<Vec<_>>();
        assert_eq!(a.try_write(&tail).unwrap(), STREAM_BUFFER_BYTES - 5);

        let mut rest = vec![0_u8; 5];
        assert_eq!(b.blocking_read(&mut rest).unwrap(), 5);
        assert_eq!(rest, vec![7_u8; 5]);
        let mut wrapped = vec![0_u8; STREAM_BUFFER_BYTES];
        let mut got = 0;
        while got < STREAM_BUFFER_BYTES - 5 {
            got += b.blocking_read(&mut wrapped[got..]).unwrap();
        }
        assert_eq!(&wrapped[..got], &tail[..got]);
    }

    #[test]
    fn a_blocked_reader_is_woken_by_a_write_in_another_thread() {
        let streams = streams("stream-wake");
        let (a, ticket) = streams.create().unwrap();
        let b = streams.open(ticket).unwrap();
        let reader = std::thread::spawn(move || {
            let mut buf = [0_u8; 8];
            let n = b.blocking_read(&mut buf).unwrap();
            buf[..n].to_vec()
        });
        std::thread::sleep(Duration::from_millis(30));
        a.blocking_write_all(b"ping").unwrap();
        assert_eq!(reader.join().unwrap(), b"ping");
    }

    #[test]
    fn a_reader_gone_fails_the_writer_and_a_lane_fills_up() {
        let streams = streams("stream-gone");
        let (a, ticket) = streams.create().unwrap();
        let b = streams.open(ticket).unwrap();
        let (b_read, _b_write) = b.split();
        drop(b_read);
        assert!(matches!(a.blocking_write(b"nobody"), Err(Error::PeerGone)));

        let mut held = vec![a];
        for _ in 1..STREAM_LANE_CAPACITY {
            held.push(streams.create().unwrap().0);
        }
        assert!(matches!(streams.create(), Err(Error::Full { .. })));
    }

    #[test]
    fn a_death_report_ends_that_incarnations_sides_only() {
        let streams = streams("stream-dead");
        let (a, ticket) = streams.create().unwrap();
        let b = streams.open(ticket).unwrap();
        b.blocking_write_all(b"partial").unwrap();

        // Another incarnation of the same node: nothing happens.
        streams.node_dead(NodeId::ZERO, Incarnation::new(2));
        assert_eq!(a.blocking_read_chunk(16).unwrap().as_ref(), b"partial");

        // Our incarnation died (as a supervisor would report about a peer):
        // both sides here are stamped with it, so both end and the slot
        // empties at once. A parked reader wakes with an error either way;
        // the two-incarnation case, where the survivor keeps the slot and
        // reads a reset, is the shared-memory test.
        let reader = std::thread::spawn(move || {
            let mut buf = [0_u8; 8];
            a.blocking_read(&mut buf).map(|_| ())
        });
        std::thread::sleep(Duration::from_millis(30));
        streams.node_dead(NodeId::ZERO, Incarnation::new(1));
        assert!(matches!(
            reader.join().unwrap(),
            Err(Error::Reset | Error::Stale(_))
        ));
        assert!(matches!(
            b.try_write(b"x"),
            Err(Error::Reset | Error::Stale(_))
        ));
        assert!(matches!(
            streams.open(ticket),
            Err(Error::AlreadyClaimed(_) | Error::Stale(_))
        ));
        drop(b);
        assert!(!streams.is_live(ticket.id));
    }

    #[test]
    fn an_offer_is_taken_once_and_a_stale_offer_is_skipped() {
        let streams = streams("stream-offer");
        assert!(streams.take_offer().is_none());
        let (a, ticket) = streams.create().unwrap();
        streams.offer(ticket, NodeId::ZERO).unwrap();
        assert_eq!(streams.take_offer(), Some(ticket));
        assert!(streams.take_offer().is_none());

        let (gone, stale) = streams.create().unwrap();
        streams.offer(stale, NodeId::ZERO).unwrap();
        drop(gone);
        assert!(streams.take_offer().is_none());

        let taker = streams.clone();
        let waiter = std::thread::spawn(move || taker.blocking_take_offer());
        std::thread::sleep(Duration::from_millis(30));
        streams.offer(ticket, NodeId::ZERO).unwrap();
        assert_eq!(waiter.join().unwrap().unwrap(), ticket);
        drop(a);
    }

    #[test]
    fn a_contested_claim_goes_to_exactly_one_opener() {
        let streams = streams("stream-race");
        for _ in 0..50 {
            let (_a, ticket) = streams.create().unwrap();
            let openers = (0..4)
                .map(|_| {
                    let streams = streams.clone();
                    std::thread::spawn(move || streams.open(ticket).is_ok())
                })
                .collect::<Vec<_>>();
            let won = openers
                .into_iter()
                .map(|opener| opener.join().unwrap())
                .filter(|won| *won)
                .count();
            assert_eq!(won, 1);
        }
    }

    #[test]
    fn random_sizes_concatenate_exactly() {
        let streams = streams("stream-random");
        let (mut a, ticket) = streams.create().unwrap();
        let b = streams.open(ticket).unwrap();
        let payload = (0..300_000_u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect::<Vec<_>>();
        let expected = payload.clone();
        let writer = std::thread::spawn(move || {
            let mut rest = &payload[..];
            let mut step = 1_usize;
            while !rest.is_empty() {
                let len = step.min(rest.len());
                a.blocking_write_all(&rest[..len]).unwrap();
                rest = &rest[len..];
                step = (step * 7 + 13) % 9_001 + 1;
            }
            a.finish().unwrap();
        });
        let mut out = Vec::new();
        let mut step = 3_usize;
        loop {
            let chunk = b.blocking_read_chunk(step).unwrap();
            if chunk.is_empty() {
                break;
            }
            out.extend_from_slice(&chunk);
            step = (step * 5 + 11) % 4_099 + 1;
        }
        writer.join().unwrap();
        assert_eq!(out, expected);
    }
}
