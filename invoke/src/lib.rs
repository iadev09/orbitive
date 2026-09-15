//! Bounded, fleet-local invocation transport over a dedicated Orbit ring.
//!
//! This crate owns transport facts only: invocation identity, operation
//! routing, opaque payload bytes, retained cursors and loss reporting. It does
//! not know what an operation runs, who runs it, or what happens when it
//! fails. Those belong to the runtime adapter consuming an invocation.
//!
//! Publication proves only that a frame was committed. The ring is not a
//! durable queue and provides no acknowledgement, claim, retry, response or
//! exactly-once guarantee; cursors report overwritten frames as lag. A claim
//! or result protocol must add its own authoritative state rather than change
//! the meaning of this request ring.
//!
//! The `tokio` feature adds [`InvocationSubscription`], an async adapter that
//! waits on the ring's native readiness fd where the platform provides one and
//! polls otherwise.

#[cfg(feature = "tokio")]
use std::collections::VecDeque;
use std::sync::Arc;
#[cfg(feature = "tokio")]
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
use orbit_core::RingEventFd;
use orbit_core::fleet::FleetLaneCursor;
use orbit_core::{Fleet, NetId64, NodeId, OrbitEpoch, OrbitTyped, RingSpec};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Ring kind reserved for Orbit invocations.
pub const INVOCATION_RING_KIND: u8 = 241;
/// Number of retained invocation frames in each fleet-node lane.
pub const INVOCATION_RING_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_INVOKE_RING_CAPACITY"), 256);
/// Maximum bytes available to one invocation envelope.
pub const INVOCATION_RING_PAYLOAD_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_INVOKE_RING_PAYLOAD_CAPACITY"), 8_192);
pub const INVOCATION_RING_SPEC: RingSpec =
    RingSpec::per_node(INVOCATION_RING_CAPACITY, INVOCATION_RING_PAYLOAD_CAPACITY);
pub const INVOCATION_PAYLOAD_MAX: usize = INVOCATION_RING_SPEC.payload_capacity;

const FRAME_KIND_INVOCATION: u8 = 1;
const HEADER_LEN: usize = 2 + 4;
#[cfg(feature = "tokio")]
const FALLBACK_POLL_INTERVAL: Duration = Duration::from_millis(10);

const _: () = assert!(INVOCATION_RING_CAPACITY.is_power_of_two());
const _: () = assert!(INVOCATION_RING_PAYLOAD_CAPACITY >= HEADER_LEN);
const _: () = assert!(INVOCATION_RING_PAYLOAD_CAPACITY <= u32::MAX as usize);

#[derive(Clone, Debug)]
struct InvocationRecord;

impl OrbitTyped for InvocationRecord {
    const KIND: u8 = INVOCATION_RING_KIND;
    const RING_SPEC: RingSpec = INVOCATION_RING_SPEC;
}

/// Identity assigned when an invocation is committed to the ring.
///
/// Its embedded node is the submitting fleet node. It identifies the
/// invocation, not the executor that may later handle it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InvocationId(NetId64);

impl InvocationId {
    pub const fn from_net_id(id: NetId64) -> Self {
        Self(id)
    }

    pub const fn net_id(self) -> NetId64 {
        self.0
    }

    pub const fn node(self) -> u16 {
        self.0.node()
    }
}

impl std::fmt::Display for InvocationId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// One retained invocation with an opaque runtime-owned payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Invocation {
    pub id: InvocationId,
    pub operation: String,
    pub payload: Vec<u8>,
    /// Publication time carried by the Orbit frame version.
    pub submitted_at: OrbitEpoch,
}

/// A runtime-owned typed payload codec.
///
/// Orbit selects no serializer. Implementations may use a compact binary
/// schema, a language value codec or an existing framework wire format.
pub trait InvocationCodec: Sized {
    const OPERATION: &'static str;

    fn encode_invocation(&self) -> std::result::Result<Vec<u8>, String>;

    fn decode_invocation(payload: &[u8]) -> std::result::Result<Self, String>;
}

impl Invocation {
    pub fn decode<C: InvocationCodec>(&self) -> Result<C> {
        if self.operation != C::OPERATION {
            return Err(Error::OperationMismatch {
                expected: C::OPERATION,
                actual: self.operation.clone(),
            });
        }
        C::decode_invocation(&self.payload).map_err(Error::Codec)
    }
}

/// Result of advancing one caller-owned invocation cursor.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InvocationPoll {
    pub invocations: Vec<Invocation>,
    /// Frames no longer available or invalid for this invocation ring.
    pub lagged: u64,
}

impl InvocationPoll {
    pub fn is_empty(&self) -> bool {
        self.invocations.is_empty() && self.lagged == 0
    }
}

/// Caller-owned positions for every fleet-node lane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvocationCursor {
    inner: FleetLaneCursor,
}

/// Cheap fleet-local handle for publishing and polling invocations.
#[derive(Clone)]
pub struct InvocationBus {
    fleet: Arc<Fleet>,
}

impl InvocationBus {
    pub fn new(fleet: Arc<Fleet>) -> Self {
        Self { fleet }
    }

    pub fn node_id(&self) -> NodeId {
        self.fleet.node_id()
    }

    pub fn cursor_at_head(&self) -> InvocationCursor {
        InvocationCursor {
            inner: self.fleet.lane_cursor_at_head::<InvocationRecord>(),
        }
    }

    pub fn cursor_from_start(&self) -> InvocationCursor {
        InvocationCursor {
            inner: self.fleet.lane_cursor_from_start::<InvocationRecord>(),
        }
    }

    /// Subscribe to future invocations for one operation.
    ///
    /// Every subscription owns an independent cursor. This is filtering, not
    /// work claiming: executor placement remains the caller's responsibility.
    #[cfg(feature = "tokio")]
    pub fn subscribe(
        self: &Arc<Self>,
        operation: impl Into<String>,
    ) -> Result<InvocationSubscription> {
        let operation = operation.into();
        validate_operation(&operation)?;
        #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
        let event_fd = self.subscription_event_fd()?;
        Ok(InvocationSubscription {
            cursor: self.cursor_at_head(),
            bus: Arc::clone(self),
            operation,
            pending: VecDeque::new(),
            #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
            event_fd,
        })
    }

    /// Clear the invocation ring during quiescent owner boot.
    pub fn reset_ring(&self) -> Result<()> {
        self.fleet
            .reset_ring::<InvocationRecord>()
            .map_err(Error::Io)
    }

    /// Create this process' readiness fd for the invocation ring.
    #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    pub fn event_fd(&self) -> Result<RingEventFd> {
        self.fleet
            .ring_event_fd::<InvocationRecord>()
            .map_err(Error::Io)
    }

    #[cfg(all(
        feature = "tokio",
        any(target_os = "linux", target_os = "freebsd", target_os = "macos")
    ))]
    fn subscription_event_fd(&self) -> Result<Option<tokio::io::unix::AsyncFd<RingEventFd>>> {
        if !self.fleet.is_shm() {
            return Ok(None);
        }
        match self.event_fd() {
            Ok(event_fd) => Ok(Some(
                tokio::io::unix::AsyncFd::new(event_fd).map_err(Error::Io)?,
            )),
            #[cfg(target_os = "macos")]
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::Unsupported => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Commit a raw invocation. Success means publication, not execution.
    pub fn submit(&self, operation: &str, payload: &[u8]) -> Result<InvocationId> {
        validate_operation(operation)?;
        let submitted_at = OrbitEpoch::now();
        let frame = encode_frame(operation.as_bytes(), payload)?;
        #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
        let id = self
            .fleet
            .publish_notified::<InvocationRecord>(
                FRAME_KIND_INVOCATION,
                submitted_at.as_unix_ms(),
                frame,
            )
            .map_err(Error::Io)?;
        #[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
        let id = self.fleet.publish::<InvocationRecord>(
            FRAME_KIND_INVOCATION,
            submitted_at.as_unix_ms(),
            frame,
        );
        Ok(InvocationId(id))
    }

    pub fn submit_typed<C: InvocationCodec>(&self, invocation: &C) -> Result<InvocationId> {
        let payload = invocation.encode_invocation().map_err(Error::Codec)?;
        self.submit(C::OPERATION, &payload)
    }

    /// Advance every node lane once. Ordering is lane-local, not fleet-global.
    pub fn poll(&self, cursor: &mut InvocationCursor) -> InvocationPoll {
        let poll = self.fleet.poll_lanes::<InvocationRecord>(&mut cursor.inner);
        let mut lagged = poll.loss.total();
        let mut invocations = Vec::with_capacity(poll.frames.len());
        for frame in poll.frames {
            if frame.kind != FRAME_KIND_INVOCATION {
                lagged = lagged.saturating_add(1);
                continue;
            }
            let Some(decoded) = decode_frame(&frame.payload) else {
                lagged = lagged.saturating_add(1);
                continue;
            };
            let Ok(operation) = std::str::from_utf8(decoded.operation) else {
                lagged = lagged.saturating_add(1);
                continue;
            };
            invocations.push(Invocation {
                id: InvocationId(frame.id),
                operation: operation.to_owned(),
                payload: decoded.payload.to_vec(),
                submitted_at: OrbitEpoch::from_unix_ms(frame.ver),
            });
        }
        InvocationPoll {
            invocations,
            lagged,
        }
    }

    /// Poll one operation while still advancing past every ring frame.
    pub fn poll_operation(&self, cursor: &mut InvocationCursor, operation: &str) -> InvocationPoll {
        let mut poll = self.poll(cursor);
        poll.invocations
            .retain(|invocation| invocation.operation == operation);
        poll
    }
}

/// Async readiness adapter over one operation-filtered invocation cursor.
#[cfg(feature = "tokio")]
pub struct InvocationSubscription {
    bus: Arc<InvocationBus>,
    operation: String,
    cursor: InvocationCursor,
    pending: VecDeque<Invocation>,
    #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    event_fd: Option<tokio::io::unix::AsyncFd<RingEventFd>>,
}

#[cfg(feature = "tokio")]
impl InvocationSubscription {
    /// Wait for the next retained invocation.
    ///
    /// Cancellation is deliberately external: callers may select this future
    /// against the lifecycle token they own without teaching Orbit about that
    /// lifecycle.
    pub async fn receive(&mut self) -> Result<Invocation> {
        loop {
            if let Some(invocation) = self.pending.pop_front() {
                return Ok(invocation);
            }

            let poll = self.bus.poll_operation(&mut self.cursor, &self.operation);
            self.pending.extend(poll.invocations);
            if poll.lagged > 0 {
                return Err(Error::Lagged(poll.lagged));
            }
            if let Some(invocation) = self.pending.pop_front() {
                return Ok(invocation);
            }

            #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
            if let Some(event_fd) = &self.event_fd {
                let mut ready = event_fd.readable().await.map_err(Error::Io)?;
                event_fd.get_ref().drain().map_err(Error::Io)?;
                ready.clear_ready();
                continue;
            }

            tokio::time::sleep(FALLBACK_POLL_INTERVAL).await;
        }
    }
}

#[derive(Debug)]
pub enum Error {
    EmptyOperation,
    OperationTooLong(usize),
    FrameTooLarge {
        operation_len: usize,
        payload_len: usize,
        max: usize,
    },
    OperationMismatch {
        expected: &'static str,
        actual: String,
    },
    Lagged(u64),
    Codec(String),
    Io(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyOperation => formatter.write_str("invocation operation cannot be empty"),
            Self::OperationTooLong(len) => {
                write!(
                    formatter,
                    "invocation operation is too long: len={len} max=65535"
                )
            }
            Self::FrameTooLarge {
                operation_len,
                payload_len,
                max,
            } => write!(
                formatter,
                "invocation frame is too large: operation_len={operation_len} payload_len={payload_len} max={max}"
            ),
            Self::OperationMismatch { expected, actual } => write!(
                formatter,
                "invocation operation mismatch: expected {expected}, got {actual}"
            ),
            Self::Lagged(count) => {
                write!(
                    formatter,
                    "invocation subscription lagged by {count} frame(s)"
                )
            }
            Self::Codec(error) => write!(formatter, "invocation codec failed: {error}"),
            Self::Io(error) => write!(formatter, "invocation ring io error: {error}"),
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

struct DecodedFrame<'a> {
    operation: &'a [u8],
    payload: &'a [u8],
}

fn validate_operation(operation: &str) -> Result<()> {
    if operation.trim().is_empty() {
        return Err(Error::EmptyOperation);
    }
    if operation.len() > usize::from(u16::MAX) {
        return Err(Error::OperationTooLong(operation.len()));
    }
    Ok(())
}

fn encode_frame(operation: &[u8], payload: &[u8]) -> Result<Bytes> {
    let total = HEADER_LEN
        .checked_add(operation.len())
        .and_then(|len| len.checked_add(payload.len()))
        .ok_or(Error::FrameTooLarge {
            operation_len: operation.len(),
            payload_len: payload.len(),
            max: INVOCATION_PAYLOAD_MAX,
        })?;
    if operation.len() > usize::from(u16::MAX)
        || payload.len() > u32::MAX as usize
        || total > INVOCATION_PAYLOAD_MAX
    {
        return Err(Error::FrameTooLarge {
            operation_len: operation.len(),
            payload_len: payload.len(),
            max: INVOCATION_PAYLOAD_MAX,
        });
    }
    let mut frame = BytesMut::with_capacity(total);
    frame.put_u16_le(operation.len() as u16);
    frame.put_u32_le(payload.len() as u32);
    frame.put_slice(operation);
    frame.put_slice(payload);
    Ok(frame.freeze())
}

fn decode_frame(frame: &Bytes) -> Option<DecodedFrame<'_>> {
    if frame.len() < HEADER_LEN {
        return None;
    }
    let operation_len = u16::from_le_bytes(frame[0..2].try_into().ok()?) as usize;
    let payload_len = u32::from_le_bytes(frame[2..6].try_into().ok()?) as usize;
    let operation_end = HEADER_LEN.checked_add(operation_len)?;
    let payload_end = operation_end.checked_add(payload_len)?;
    if payload_end != frame.len() {
        return None;
    }
    Some(DecodedFrame {
        operation: &frame[HEADER_LEN..operation_end],
        payload: &frame[operation_end..payload_end],
    })
}
