//! Cursor and walking primitives for Orbit rings.
//!
//! This module owns the generic "walk counters from cursor toward head"
//! semantics shared by event streams, cache compaction, metrics, and
//! future ring-backed substrates. Semantic layers decode frames after
//! this layer has handled wraparound, in-flight slots, loss, and cursor
//! advance.

use crate::ring::Frame;

/// Result of reading one logical counter from a ring source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RingRead {
    /// The expected counter is fully committed and safe to consume.
    Ready(Frame),
    /// The counter has been claimed but may still be in flight.
    /// A cursor must stay on this counter and retry later.
    Pending,
    /// The counter can no longer be read from this slot.
    Unavailable,
}

/// Read-only source that can be walked by [`RingCursor`].
///
/// Implementors expose only the ring facts needed by the generic walker:
/// the ring kind, current head, fixed capacity, and counter-addressed
/// frame reads.
pub trait RingFrameSource {
    /// The `OrbitTyped::KIND` carried by this ring.
    fn kind(&self) -> u8;

    /// Monotonic visible head. Depending on topology, this counts counters
    /// reserved by writers or frames committed by writers.
    fn head(&self) -> u64;

    /// Fixed slot count for this ring.
    fn capacity(&self) -> usize;

    /// Read the frame currently occupying `counter % capacity`.
    fn read_at(&self, counter: u64) -> Option<Frame>;

    /// Classify the logical counter currently addressed by
    /// `counter % capacity`.
    ///
    /// Sources that can distinguish an in-flight claim should override
    /// this method. The compatibility default preserves the previous
    /// `read_at` behavior for external implementations.
    fn read_state_at(&self, counter: u64) -> RingRead {
        self.read_at(counter)
            .map_or(RingRead::Unavailable, RingRead::Ready)
    }
}

/// Caller-owned position in a ring walk.
///
/// The cursor stores the next counter a reader should attempt. Different
/// subscribers keep independent cursors over the same ring.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RingCursor {
    next_counter: u64,
}

impl RingCursor {
    /// Start at counter 0 and replay whatever ring history is still
    /// available.
    pub const fn from_start() -> Self {
        Self { next_counter: 0 }
    }

    /// Start from a known next counter.
    pub const fn from_counter(next_counter: u64) -> Self {
        Self { next_counter }
    }

    /// The next counter this cursor will read.
    pub const fn next_counter(self) -> u64 {
        self.next_counter
    }

    pub(crate) fn set_next_counter(&mut self, next_counter: u64) {
        self.next_counter = next_counter;
    }
}

/// Counters skipped while walking a ring.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RingLoss {
    /// Counters older than the current ring window.
    pub overwritten: u64,
    /// Counters inside the readable window whose slot was definitively
    /// unavailable, wrapped, corrupt, or carried an unexpected frame id.
    pub unavailable: u64,
}

impl RingLoss {
    pub const fn total(self) -> u64 {
        self.overwritten.saturating_add(self.unavailable)
    }

    pub const fn is_empty(self) -> bool {
        self.overwritten == 0 && self.unavailable == 0
    }
}

/// Result of walking a cursor toward a ring's visible head.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RingPoll {
    pub frames: Vec<Frame>,
    pub loss: RingLoss,
    pub from_counter: u64,
    pub to_counter: u64,
}

impl RingPoll {
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty() && self.loss.is_empty()
    }
}

/// Walk `cursor` toward the current visible head of `source`.
///
/// At most one ring window is inspected. If the cursor has fallen behind
/// the oldest available counter, the skipped counters are recorded as
/// overwritten and the walk resumes at the window floor. An in-flight
/// counter stops the walk without advancing past it.
pub fn poll_ring<S: RingFrameSource>(source: &S, cursor: &mut RingCursor) -> RingPoll {
    let head = source.head();
    let from_counter = cursor.next_counter();

    if from_counter >= head {
        cursor.set_next_counter(head);
        return RingPoll {
            from_counter,
            to_counter: head,
            ..RingPoll::default()
        };
    }

    let capacity = source.capacity() as u64;
    let oldest_available = head.saturating_sub(capacity);
    let mut next = from_counter;
    let mut loss = RingLoss::default();

    if next < oldest_available {
        loss.overwritten = oldest_available - next;
        next = oldest_available;
    }

    let kind = source.kind();
    let mut frames = Vec::new();
    while next < head {
        match source.read_state_at(next) {
            RingRead::Ready(frame) => {
                if frame.id.kind() != kind || frame.id.counter() != next {
                    loss.unavailable = loss.unavailable.saturating_add(1);
                } else {
                    frames.push(frame);
                }
                next = next.saturating_add(1);
            }
            RingRead::Pending => break,
            RingRead::Unavailable => {
                loss.unavailable = loss.unavailable.saturating_add(1);
                next = next.saturating_add(1);
            }
        }
    }

    cursor.set_next_counter(next);
    RingPoll {
        frames,
        loss,
        from_counter,
        to_counter: next,
    }
}
