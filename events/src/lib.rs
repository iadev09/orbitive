//! `FleetEventBus` — append-only event stream over one Orbit ring.
//!
//! Events are not cache entries and not metrics snapshots. Cache reads
//! ask "what is the newest value for this key?" Metrics reads ask "what
//! is the newest sample for each node?" Event reads ask "which frames
//! have appeared since my cursor?"
//!
//! One event SHM segment carries all topics, with one writer lane per fleet
//! node. The topic is carried inside the frame payload, so adding a new event
//! type does not allocate another SHM segment. Subscribers keep one cursor per
//! node lane and do not assume a total order across nodes.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use orbit_core::RingEventFd;
use orbit_core::fleet::FleetLaneCursor;
use orbit_core::{Fleet, NetId64, NodeId, OrbitTyped, RingSpec};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Failures produced by the Orbit event semantic layer.
#[derive(Debug)]
pub enum Error {
    /// A topic and payload envelope cannot fit in one configured event frame.
    FrameTooLarge {
        topic_len: usize,
        payload_len: usize,
        max_payload: usize,
    },
    /// The underlying Orbit ring operation failed.
    Io(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FrameTooLarge {
                topic_len,
                payload_len,
                max_payload,
            } => write!(
                f,
                "orbit event frame too large: topic_len={topic_len} payload_len={payload_len} max_payload={max_payload}"
            ),
            Self::Io(error) => write!(f, "orbit event io error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::FrameTooLarge { .. } => None,
        }
    }
}

/// Event frame payload limit for V0. This is the event lane's own SHM
/// payload capacity; non-Unix keeps the same contract so callers do not
/// accidentally rely on unbounded in-memory frames.
pub const EVENT_RING_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_EVENT_RING_CAPACITY"), 1_024);
pub const EVENT_RING_PAYLOAD_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_EVENT_RING_PAYLOAD_CAPACITY"), 512);
pub const EVENT_RING_SPEC: RingSpec =
    RingSpec::per_node(EVENT_RING_CAPACITY, EVENT_RING_PAYLOAD_CAPACITY);
pub const EVENT_PAYLOAD_MAX: usize = EVENT_RING_SPEC.payload_capacity;

const HEADER_LEN: usize = 2 + 2 + 8;
const FRAME_KIND_EVENT: u8 = 1;
pub const EVENT_RING_KIND: u8 = 220;

const _: () = assert!(EVENT_RING_CAPACITY.is_power_of_two());
const _: () = assert!(EVENT_RING_PAYLOAD_CAPACITY >= HEADER_LEN);
const _: () = assert!(EVENT_RING_PAYLOAD_CAPACITY <= u32::MAX as usize);

/// Dedicated per-node-lane ring kind for raw Orbit events.
#[derive(Clone, Debug)]
struct FleetEventRecord;

impl OrbitTyped for FleetEventRecord {
    // Hand-picked V0 kind. Build-time KIND allocation will replace
    // these manual values later.
    const KIND: u8 = EVENT_RING_KIND;
    const RING_SPEC: RingSpec = EVENT_RING_SPEC;
}

/// Cursor for one event subscriber.
///
/// The cursor stores one next counter per node lane. It is intentionally
/// caller-owned so different consumers can advance independently.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetEventCursor {
    inner: FleetLaneCursor,
}

impl FleetEventCursor {
    /// Start from the beginning of the ring history that is still
    /// available.
    pub const fn from_start() -> Self {
        Self {
            inner: FleetLaneCursor::from_counter(0),
        }
    }

    /// Start from a known next counter.
    pub const fn from_counter(next_counter: u64) -> Self {
        Self {
            inner: FleetLaneCursor::from_counter(next_counter),
        }
    }

    /// The lowest next counter among the node lanes.
    pub fn next_counter(&self) -> u64 {
        self.inner.minimum_next_counter()
    }
}

/// One decoded event from the shared event ring.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetEvent {
    pub id: NetId64,
    pub topic: String,
    pub payload: Vec<u8>,
    pub timestamp_ms: u64,
}

/// Result of polling an event cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FleetEventPoll {
    pub events: Vec<FleetEvent>,
    /// Number of counters that could not be delivered because the
    /// subscriber lagged past the ring capacity, a slot was empty, or a
    /// slot had already wrapped to another counter.
    pub lagged: u64,
}

impl FleetEventPoll {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty() && self.lagged == 0
    }
}

/// Fleet-shared raw event bus. Cheap to clone.
#[derive(Clone)]
pub struct FleetEventBus {
    fleet: Arc<Fleet>,
}

impl FleetEventBus {
    pub fn new(fleet: Arc<Fleet>) -> Self {
        Self { fleet }
    }

    pub fn node_id(&self) -> NodeId {
        self.fleet.node_id()
    }

    /// Cursor that starts after all events currently in the ring.
    /// Useful for subscribers that only want future events.
    pub fn cursor_at_head(&self) -> FleetEventCursor {
        FleetEventCursor {
            inner: self.fleet.lane_cursor_at_head::<FleetEventRecord>(),
        }
    }

    /// Cursor that starts at counter 0 and replays whatever history has
    /// not wrapped out of the ring.
    pub const fn cursor_from_start(&self) -> FleetEventCursor {
        let _ = self;
        FleetEventCursor::from_start()
    }

    /// Clear the shared event ring.
    ///
    /// Intended for owner-controlled boot-time cleanup before peer
    /// processes publish events into the ring. This prevents stale
    /// events from a previous process lifetime from being replayed or
    /// counted as current runtime state.
    pub fn reset_ring(&self) -> Result<()> {
        self.fleet
            .reset_ring::<FleetEventRecord>()
            .map_err(Error::Io)
    }

    /// Create this process' native fd readiness bridge for the event ring.
    ///
    /// The returned fd is local to this process. Publishers notify a shared
    /// generation after committing an event; the bridge converts that
    /// broadcast into local fd readiness suitable for epoll/kqueue/AsyncFd.
    /// Drain the fd, then poll the ring with this subscriber's cursor.
    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    pub fn event_fd(&self) -> Result<RingEventFd> {
        self.fleet
            .ring_event_fd::<FleetEventRecord>()
            .map_err(Error::Io)
    }

    /// Publish one event under `topic`.
    pub fn publish(&self, topic: &str, payload: &[u8]) -> Result<NetId64> {
        let timestamp_ms = now_ms();
        let frame = encode_frame(topic.as_bytes(), payload, timestamp_ms)?;
        #[cfg(any(target_os = "linux", target_os = "freebsd"))]
        let id = self
            .fleet
            .publish_notified::<FleetEventRecord>(FRAME_KIND_EVENT, timestamp_ms, frame)
            .map_err(Error::Io)?;
        #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
        let id = self
            .fleet
            .publish::<FleetEventRecord>(FRAME_KIND_EVENT, timestamp_ms, frame);
        Ok(id)
    }

    /// Poll all events since `cursor`, advancing the cursor to the
    /// current ring head. If the cursor has fallen behind the ring
    /// capacity, older overwritten counters are reported as `lagged`.
    pub fn poll(&self, cursor: &mut FleetEventCursor) -> FleetEventPoll {
        let ring_poll = self.fleet.poll_lanes::<FleetEventRecord>(&mut cursor.inner);
        let mut lagged = ring_poll.loss.total();
        let mut events = Vec::new();
        for frame in ring_poll.frames {
            let Some(decoded) = decode_frame(&frame.payload) else {
                lagged = lagged.saturating_add(1);
                continue;
            };
            events.push(FleetEvent {
                id: frame.id,
                topic: String::from_utf8_lossy(decoded.topic).into_owned(),
                payload: decoded.payload.to_vec(),
                timestamp_ms: decoded.timestamp_ms,
            });
        }

        FleetEventPoll { events, lagged }
    }

    /// Poll and keep only events whose topic matches `topic`.
    ///
    /// The cursor still advances past all events, including filtered
    /// topics. Use separate cursors for independent consumers.
    pub fn poll_topic(&self, cursor: &mut FleetEventCursor, topic: &str) -> FleetEventPoll {
        let mut poll = self.poll(cursor);
        poll.events.retain(|event| event.topic == topic);
        poll
    }
}

struct DecodedFrame<'a> {
    topic: &'a [u8],
    payload: &'a [u8],
    timestamp_ms: u64,
}

fn encode_frame(topic: &[u8], payload: &[u8], timestamp_ms: u64) -> Result<Bytes> {
    let total = HEADER_LEN + topic.len() + payload.len();
    if topic.len() > u16::MAX as usize
        || payload.len() > u16::MAX as usize
        || total > EVENT_PAYLOAD_MAX
    {
        return Err(Error::FrameTooLarge {
            topic_len: topic.len(),
            payload_len: payload.len(),
            max_payload: EVENT_PAYLOAD_MAX,
        });
    }

    let mut buf = BytesMut::with_capacity(total);
    buf.put_u16_le(topic.len() as u16);
    buf.put_u16_le(payload.len() as u16);
    buf.put_u64_le(timestamp_ms);
    buf.put_slice(topic);
    buf.put_slice(payload);
    Ok(buf.freeze())
}

fn decode_frame(payload: &Bytes) -> Option<DecodedFrame<'_>> {
    if payload.len() < HEADER_LEN {
        return None;
    }

    let topic_len = u16::from_le_bytes(payload[0..2].try_into().ok()?) as usize;
    let payload_len = u16::from_le_bytes(payload[2..4].try_into().ok()?) as usize;
    let timestamp_ms = u64::from_le_bytes(payload[4..12].try_into().ok()?);
    let topic_start = HEADER_LEN;
    let topic_end = topic_start.checked_add(topic_len)?;
    let payload_end = topic_end.checked_add(payload_len)?;
    if payload.len() < payload_end {
        return None;
    }

    Some(DecodedFrame {
        topic: &payload[topic_start..topic_end],
        payload: &payload[topic_end..payload_end],
        timestamp_ms,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::FleetEventBus;
    use orbit_core::Fleet;

    #[test]
    fn polls_events_since_cursor() {
        let fleet = Arc::new(Fleet::join("event_poll", 1).expect("fleet"));
        let bus = FleetEventBus::new(fleet);
        let mut cursor = bus.cursor_from_start();

        bus.publish("worker.booted", b"w1").expect("publish first");
        bus.publish("cache.reset", b"all").expect("publish second");

        let poll = bus.poll(&mut cursor);
        assert_eq!(poll.lagged, 0);
        assert_eq!(poll.events.len(), 2);
        assert_eq!(poll.events[0].topic, "worker.booted");
        assert_eq!(poll.events[0].payload, b"w1");
        assert_eq!(poll.events[1].topic, "cache.reset");
        assert_eq!(poll.events[1].payload, b"all");

        assert!(bus.poll(&mut cursor).is_empty());
    }

    #[test]
    fn cursor_at_head_only_reads_future_events() {
        let fleet = Arc::new(Fleet::join("event_head", 1).expect("fleet"));
        let bus = FleetEventBus::new(fleet);

        bus.publish("old", b"ignored").expect("publish old");
        let mut cursor = bus.cursor_at_head();
        bus.publish("new", b"seen").expect("publish new");

        let poll = bus.poll(&mut cursor);
        assert_eq!(poll.events.len(), 1);
        assert_eq!(poll.events[0].topic, "new");
    }

    #[test]
    fn topic_filter_advances_cursor() {
        let fleet = Arc::new(Fleet::join("event_topic", 1).expect("fleet"));
        let bus = FleetEventBus::new(fleet);
        let mut cursor = bus.cursor_from_start();

        bus.publish("a", b"1").expect("publish a");
        bus.publish("b", b"2").expect("publish b");

        let poll = bus.poll_topic(&mut cursor, "b");
        assert_eq!(poll.events.len(), 1);
        assert_eq!(poll.events[0].payload, b"2");
        assert!(bus.poll(&mut cursor).is_empty());
    }

    #[test]
    fn event_frame_contract_uses_the_compile_time_geometry() {
        let expected = orbit_core::compile::usize_from_env(
            option_env!("ORBIT_EVENT_RING_PAYLOAD_CAPACITY"),
            512,
        );
        assert_eq!(super::EVENT_PAYLOAD_MAX, expected);

        let largest_payload = vec![0_u8; super::EVENT_PAYLOAD_MAX - super::HEADER_LEN - 1];
        let frame = super::encode_frame(b"x", &largest_payload, 0).expect("frame must fit");
        assert_eq!(frame.len(), super::EVENT_PAYLOAD_MAX);

        let oversized_payload = vec![0_u8; largest_payload.len() + 1];
        assert!(matches!(
            super::encode_frame(b"x", &oversized_payload, 0),
            Err(super::Error::FrameTooLarge {
                max_payload,
                ..
            }) if max_payload == expected
        ));
    }

    #[test]
    fn reports_lag_when_cursor_falls_behind_capacity() {
        let fleet = Arc::new(Fleet::join("event_lag", 1).expect("fleet"));
        let bus = FleetEventBus::new(fleet);
        let mut cursor = bus.cursor_from_start();

        for value in 0..=super::EVENT_RING_SPEC.capacity {
            bus.publish("n", value.to_string().as_bytes())
                .expect("publish event");
        }

        let poll = bus.poll(&mut cursor);
        assert_eq!(poll.lagged, 1);
        assert_eq!(poll.events.len(), super::EVENT_RING_SPEC.capacity);
        assert_eq!(poll.events[0].payload, b"1");
    }
}
