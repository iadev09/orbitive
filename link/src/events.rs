//! Carry fleet events between two fleets that meet in a link segment.
//!
//! An edge and an application each have their own fleet and event ring, and
//! meet in the application's link segment. This module carries selected
//! events across it — ring, link, ring — and holds every rule that makes that
//! safe, so a downstream only says which segment and which ring:
//!
//! - **Forwarded once, at its origin.** An event is forwarded only by the
//!   process that published it: its `NetId64` names the publisher's node. A
//!   process that read it off the ring never forwards it again.
//! - **Received by one, published once.** The frame goes to one lane of the
//!   far side, and only that lane's owner reads it; it publishes the event
//!   into its own ring once, and the ring delivers it to its whole fleet.
//! - **Never back where it came from.** A republished event carries the
//!   receiver's node and would pass the origin rule; this bridge remembers
//!   what it republished and never sends it back over its own segment. A
//!   bridge on another segment may still forward it, which is how a hub
//!   relays between leaves.
//! - **Only where it is wanted.** Each lane and watcher publishes the routing
//!   keys its process can decode; an event goes only to a side where some
//!   process wants its key.
//!
//! The bridge owns no thread and no interval. [`LinkEvents::drive`] waits on
//! the ring's own readiness; the downstream runs it and hands event frames
//! from its lane reader to [`LinkEvents::accept`].

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use orbit_events::{
    DefaultEventLayout, EventLayout, FleetEvent, FleetEventBus,
    FleetEventCursor,
};

use crate::inbox::{INTEREST_BITS, INTEREST_WORDS};
use crate::{DirectoryWatcher, Error, LinkSegment, Result, frame};

/// What this feature's keys are tagged with in the shared interest table.
///
/// The table belongs to the link, not to events: whatever else crosses a link
/// one day records its interest in the same set. A key is hashed after this
/// tag, so an event topic and another feature's key that happen to be spelled
/// the same land on different bits. Both ends compute it from this constant;
/// a build without it reads none of the other side's interest.
const INTEREST_TAG: &[u8] = b"events\0";

/// What an event is matched by against the far side's interest.
#[derive(Clone, Copy)]
pub enum Routing {
    /// Its ring topic: one ring topic per event type.
    Topic,
    /// A key the downstream extracts, for an envelope that carries many
    /// domain topics under one ring topic. `None` means "never forward this
    /// one". The bridge never opens a payload itself.
    Key(fn(&FleetEvent) -> Option<Vec<u8>>),
}

/// One fleet's event ring bound to a link segment this process is part of.
pub struct LinkEvents<L: EventLayout = DefaultEventLayout> {
    segment: Arc<LinkSegment>,
    /// This process's own interest entry: its lane's or its watcher's.
    entry: usize,
    /// Which roles are the other fleet.
    far: fn(&str) -> bool,
    bus: Arc<FleetEventBus<L>>,
    routing: Routing,
    cursor: Mutex<FleetEventCursor>,
    /// Counters of events this bridge republished into its own ring.
    republished: Mutex<BTreeSet<u64>>,
    dropped: AtomicU64,
    lagged: AtomicU64,
}

impl<L: EventLayout> LinkEvents<L> {
    /// Bind through the lane this process holds in `segment`.
    pub fn for_lane(
        segment: &Arc<LinkSegment>,
        lane: usize,
        bus: Arc<FleetEventBus<L>>,
        far: fn(&str) -> bool,
        routing: Routing,
    ) -> Result<Self> {
        if segment.inbox().owner(lane)? != Some(std::process::id()) {
            return Err(Error::Malformed(
                "this process does not hold the lane",
            ));
        }
        Ok(Self::new(Arc::clone(segment), lane, bus, far, routing))
    }

    /// Bind through a directory watcher: for a process that holds no lane.
    /// It publishes interest and forwards what it publishes; it receives
    /// nothing, because only a lane takes frames.
    pub fn for_watcher(
        watcher: &DirectoryWatcher,
        bus: Arc<FleetEventBus<L>>,
        far: fn(&str) -> bool,
        routing: Routing,
    ) -> Self {
        let segment = Arc::clone(watcher.segment());
        let entry = segment.inbox().lanes() + watcher.slot();
        Self::new(segment, entry, bus, far, routing)
    }

    fn new(
        segment: Arc<LinkSegment>,
        entry: usize,
        bus: Arc<FleetEventBus<L>>,
        far: fn(&str) -> bool,
        routing: Routing,
    ) -> Self {
        let cursor = Mutex::new(bus.cursor_at_head());
        Self {
            segment,
            entry,
            far,
            bus,
            routing,
            cursor,
            republished: Mutex::new(BTreeSet::new()),
            dropped: AtomicU64::new(0),
            lagged: AtomicU64::new(0),
        }
    }

    /// Say which routing keys this process can decode, replacing what it said
    /// before. The far side forwards only these.
    pub fn interest<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
    ) {
        let mut bits = [0u64; INTEREST_WORDS];
        for key in keys {
            for bit in positions(INTEREST_TAG, key.as_ref()) {
                bits[bit / 64] |= 1 << (bit % 64);
            }
        }
        self.segment.inbox().write_interest(self.entry, &bits);
    }

    /// An event frame read from this process's lane: publish it into this
    /// fleet's ring, once, and hand it back.
    ///
    /// The ring carries it to every other process of the fleet. This process
    /// published it, and a process does not read its own publications back as
    /// news, so it delivers the returned topic and payload to itself.
    pub fn accept<'frame>(
        &self,
        frame: &'frame [u8],
    ) -> Result<(&'frame str, &'frame [u8])> {
        let (topic, payload) = frame::decode_event(frame)?;
        let id = self.bus.publish(topic, payload).map_err(Error::Events)?;
        self.republished
            .lock()
            .map_err(|_| Error::Malformed("link event memory is poisoned"))?
            .insert(id.counter());
        Ok((topic, payload))
    }

    /// Forward what this process published since the last look, wherever the
    /// far side wants it. [`Self::drive`] calls this on every ring wake.
    pub fn pump(&self) -> Result<()> {
        let mut cursor = self
            .cursor
            .lock()
            .map_err(|_| Error::Malformed("link event cursor is poisoned"))?;
        let poll = self.bus.poll(&mut cursor);
        if poll.lagged > 0 {
            self.lagged.fetch_add(poll.lagged, Ordering::Relaxed);
        }
        let own = self.bus.node_id().get();
        let mut frame = Vec::new();
        for event in poll.events {
            if event.id.node() != own || self.came_back(event.id.counter())? {
                continue;
            }
            let key = match self.routing {
                Routing::Topic => event.topic.as_bytes().to_vec(),
                Routing::Key(extract) => match extract(&event) {
                    Some(key) => key,
                    None => continue,
                },
            };
            if !self.far_wants(&key) {
                continue;
            }
            if self.send(&event, &mut frame).is_err() {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Forward until the ring stops answering. Waits on the ring's readiness,
    /// never on an interval; run it beside the lane reader and cancel it by
    /// dropping the future.
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    pub async fn drive(&self) -> Result<()> {
        let event_fd = self.bus.event_fd().map_err(Error::Events)?;
        // SAFETY: RingEventFd owns its stable descriptor until drop. This
        // bridge only drains it and never replaces the registered inner value.
        let ready = unsafe { tokio::io::unix::AsyncFd::register(event_fd) }
            .map_err(std::io::Error::from)
            .map_err(Error::Io)?;
        loop {
            self.pump()?;
            let mut guard = ready.readable().await.map_err(Error::Io)?;
            ready.get_ref().drain().map_err(Error::Io)?;
            guard.clear_ready();
        }
    }

    /// Events that could not be written to the far side: no lane ready, a full
    /// lane, or a frame too large for a slot.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Ring frames overwritten before this bridge read them.
    pub fn lagged(&self) -> u64 {
        self.lagged.load(Ordering::Relaxed)
    }

    /// Whether this own-node event is one this bridge republished, forgetting
    /// everything up to it: own counters arrive in order.
    fn came_back(
        &self,
        counter: u64,
    ) -> Result<bool> {
        let mut republished = self
            .republished
            .lock()
            .map_err(|_| Error::Malformed("link event memory is poisoned"))?;
        let ours = republished.contains(&counter);
        *republished = republished.split_off(&(counter + 1));
        Ok(ours)
    }

    /// Whether some process on the far side said it can decode `key`.
    fn far_wants(
        &self,
        key: &[u8],
    ) -> bool {
        let inbox = self.segment.inbox();
        let lanes = inbox.lanes();
        let mut union = [0u64; INTEREST_WORDS];
        let mut merge = |entry: usize| {
            for (word, bits) in union.iter_mut().zip(inbox.read_interest(entry))
            {
                *word |= bits;
            }
        };
        for lane in 0..lanes {
            if self.is_far_lane(lane) {
                merge(lane);
            }
        }
        for slot in 0..lanes {
            if inbox
                .watcher_role(slot)
                .ok()
                .flatten()
                .is_some_and(|role| (self.far)(&role))
            {
                merge(lanes + slot);
            }
        }
        positions(INTEREST_TAG, key)
            .iter()
            .all(|bit| union[bit / 64] & (1 << (bit % 64)) != 0)
    }

    fn is_far_lane(
        &self,
        lane: usize,
    ) -> bool {
        let inbox = self.segment.inbox();
        inbox.consumers(lane).is_ok_and(|consumers| consumers != 0)
            && inbox
                .role(lane)
                .ok()
                .flatten()
                .is_some_and(|role| (self.far)(&role))
    }

    /// Write one event to the idlest far lane that will take it.
    fn send(
        &self,
        event: &FleetEvent,
        frame: &mut Vec<u8>,
    ) -> Result<()> {
        let inbox = self.segment.inbox();
        let far: Vec<usize> =
            (0..inbox.lanes()).filter(|lane| self.is_far_lane(*lane)).collect();
        let Some((lane, _)) = inbox.ready(&far).into_iter().next() else {
            // Every far lane refused. One a killed process left keeps
            // refusing until it is taken back.
            for lane in far {
                self.segment.reclaim_if_gone(lane)?;
            }
            return Err(Error::NoLane);
        };
        frame::encode_event(&event.topic, &event.payload, frame)?;
        inbox.write(lane, frame)?;
        Ok(())
    }
}

impl<L: EventLayout> Drop for LinkEvents<L> {
    /// Interest belongs to a live bridge: take it back when the bridge goes.
    fn drop(&mut self) {
        self.segment.inbox().write_interest(self.entry, &[0; INTEREST_WORDS]);
    }
}

/// Two bit positions for a key under its feature's tag. FNV-1a, so every
/// process — whatever binary or compiler built it — computes the same
/// positions for the same bytes.
fn positions(
    tag: &[u8],
    key: &[u8],
) -> [usize; 2] {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in tag.iter().chain(key) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let bits = INTEREST_BITS as u64;
    [(hash % bits) as usize, ((hash >> 32) % bits) as usize]
}
