use std::marker::PhantomData;

use crate::OrbitTyped;
use crate::fleet::{Fleet, NodeId};
use crate::ring::cursor::{RingCursor, RingFrameSource, RingLoss, RingPoll, RingRead, poll_ring};
use crate::ring::{Frame, RingTopology};

struct FleetRingSource<'a, T: OrbitTyped> {
    fleet: &'a Fleet,
    _t: PhantomData<T>,
}

struct FleetLaneSource<'a, T: OrbitTyped> {
    fleet: &'a Fleet,
    node_id: NodeId,
    _t: PhantomData<T>,
}

impl<'a, T: OrbitTyped> FleetLaneSource<'a, T> {
    fn new(fleet: &'a Fleet, node_id: NodeId) -> Self {
        Self {
            fleet,
            node_id,
            _t: PhantomData,
        }
    }
}

impl<T: OrbitTyped> RingFrameSource for FleetLaneSource<'_, T> {
    fn kind(&self) -> u8 {
        T::KIND
    }

    fn head(&self) -> u64 {
        self.fleet.lane_head::<T>(self.node_id)
    }

    fn capacity(&self) -> usize {
        self.fleet.ring_capacity::<T>()
    }

    fn read_at(&self, counter: u64) -> Option<Frame> {
        self.fleet.read_lane_at::<T>(self.node_id, counter)
    }

    fn read_state_at(&self, counter: u64) -> RingRead {
        self.fleet.read_lane_state_at::<T>(self.node_id, counter)
    }
}

/// Caller-owned positions for every node lane of one ring type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetLaneCursor {
    lanes: Vec<RingCursor>,
    initial_counter: u64,
}

impl FleetLaneCursor {
    pub const fn from_counter(initial_counter: u64) -> Self {
        Self {
            lanes: Vec::new(),
            initial_counter,
        }
    }

    pub fn minimum_next_counter(&self) -> u64 {
        self.lanes
            .iter()
            .map(|cursor| cursor.next_counter())
            .min()
            .unwrap_or(self.initial_counter)
    }
}

/// Combined result of walking every node lane once.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FleetLanePoll {
    pub frames: Vec<Frame>,
    pub loss: RingLoss,
}

impl<'a, T: OrbitTyped> FleetRingSource<'a, T> {
    fn new(fleet: &'a Fleet) -> Self {
        Self {
            fleet,
            _t: PhantomData,
        }
    }
}

impl<T: OrbitTyped> RingFrameSource for FleetRingSource<'_, T> {
    fn kind(&self) -> u8 {
        T::KIND
    }

    fn head(&self) -> u64 {
        self.fleet.head::<T>()
    }

    fn capacity(&self) -> usize {
        self.fleet.ring_capacity::<T>()
    }

    fn read_at(&self, counter: u64) -> Option<Frame> {
        self.fleet.read_at::<T>(counter)
    }

    fn read_state_at(&self, counter: u64) -> RingRead {
        self.fleet.read_state_at::<T>(counter)
    }
}

impl Fleet {
    /// Cursor that starts after every counter currently claimed for `T`.
    /// Useful for subscribers that only want future writes.
    pub fn cursor_at_head<T: OrbitTyped>(&self) -> RingCursor {
        RingCursor::from_counter(self.head::<T>())
    }

    /// Cursor that starts at counter 0 for `T`.
    pub const fn cursor_from_start<T: OrbitTyped>(&self) -> RingCursor {
        let _ = self;
        let _ = PhantomData::<T>;
        RingCursor::from_start()
    }

    /// Walk `cursor` toward the current claim head for `T`, stopping at
    /// an in-flight counter and reporting definitive losses as
    /// [`crate::ring::cursor::RingLoss`].
    pub fn poll_ring<T: OrbitTyped>(&self, cursor: &mut RingCursor) -> RingPoll {
        poll_ring(&FleetRingSource::<T>::new(self), cursor)
    }

    /// Create one caller-owned cursor per physical node lane, starting at each
    /// lane's current head.
    pub fn lane_cursor_at_head<T: OrbitTyped>(&self) -> FleetLaneCursor {
        self.assert_per_node::<T>();
        let lanes = (0..self.fleet_size())
            .map(|node| RingCursor::from_counter(self.lane_head::<T>(NodeId::new(u16::from(node)))))
            .collect();
        FleetLaneCursor {
            lanes,
            initial_counter: 0,
        }
    }

    /// Create one caller-owned cursor per physical node lane, starting at
    /// counter zero.
    pub fn lane_cursor_from_start<T: OrbitTyped>(&self) -> FleetLaneCursor {
        self.assert_per_node::<T>();
        FleetLaneCursor {
            lanes: vec![RingCursor::from_start(); usize::from(self.fleet_size())],
            initial_counter: 0,
        }
    }

    /// Poll every physical node lane and combine the retained frames and loss
    /// counters into one result. Frames retain their writer node in `id`;
    /// callers that need semantic ordering across lanes must provide it.
    pub fn poll_lanes<T: OrbitTyped>(&self, cursor: &mut FleetLaneCursor) -> FleetLanePoll {
        self.assert_per_node::<T>();
        if cursor.lanes.is_empty() {
            cursor.lanes = vec![
                RingCursor::from_counter(cursor.initial_counter);
                usize::from(self.fleet_size())
            ];
        }
        assert_eq!(
            cursor.lanes.len(),
            usize::from(self.fleet_size()),
            "lane cursor belongs to a different fleet size"
        );

        let mut combined = FleetLanePoll::default();
        for (node, lane_cursor) in cursor.lanes.iter_mut().enumerate() {
            let source = FleetLaneSource::<T>::new(self, NodeId::new(node as u16));
            let poll = poll_ring(&source, lane_cursor);
            combined.frames.extend(poll.frames);
            combined.loss.overwritten = combined
                .loss
                .overwritten
                .saturating_add(poll.loss.overwritten);
            combined.loss.unavailable = combined
                .loss
                .unavailable
                .saturating_add(poll.loss.unavailable);
        }
        combined
    }

    fn assert_per_node<T: OrbitTyped>(&self) {
        assert_eq!(
            T::RING_SPEC.topology,
            RingTopology::PerNode,
            "lane cursor requires a per-node ring"
        );
    }
}
