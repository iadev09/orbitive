//! Duplex byte flow attached to an inbox frame.
//!
//! One stream slot carries both directions. The originator creates side A and
//! encodes the ticket in its application-defined inbox frame; the target opens
//! side B. A retained creator endpoint can be rearmed after side B releases
//! it, then announced in another frame to any target lane.

use std::sync::Arc;

use orbit_core::{Fleet, NodeId};
use orbit_stream::{Endpoint, Incarnation, Streams, Ticket};

use super::{LinkSpec, Reclaimed, Result};

/// This process's handle on a named link fleet's stream table.
pub struct LinkBodies {
    streams: Streams
}

impl LinkBodies {
    pub fn open(
        fleet: Arc<Fleet>,
        incarnation: u64,
        spec: LinkSpec
    ) -> Result<Self> {
        spec.validate()?;
        if fleet.fleet_capacity() != spec.fleet_capacity {
            return Err(super::Error::Malformed(
                "link stream fleet capacity differs from its inbox specification"
            ));
        }
        let streams = Streams::with_spec(fleet, Incarnation::new(incarnation), spec.streams)?;
        Ok(Self { streams })
    }

    /// Join the stream table under the same fleet name and lane index as the
    /// inbox participant.
    #[cfg(unix)]
    pub fn for_name(
        name: &str,
        lane_index: usize,
        incarnation: u64,
        spec: LinkSpec
    ) -> Result<Self> {
        super::check_name(name, spec)?;
        if lane_index >= spec.fleet_capacity as usize {
            return Err(super::Error::Malformed("link lane is outside the fleet capacity"));
        }
        let fleet = Fleet::join_shm_as(name, spec.fleet_capacity, NodeId::new(lane_index as u16))?;
        Self::open(Arc::new(fleet), incarnation, spec)
    }

    /// Create side A and the ticket an inbox frame carries to side B.
    pub fn create(&self) -> Result<(Endpoint, Ticket)> {
        Ok(self.streams.create()?)
    }

    /// Clear shared state for a retained creator endpoint and restore its
    /// local writer after an earlier FIN.
    pub fn rearm_endpoint(
        &self,
        endpoint: &mut Endpoint
    ) -> Result<()> {
        endpoint.rearm(&self.streams)?;
        Ok(())
    }

    /// Clear only the shared slot state.
    ///
    /// Prefer [`LinkBodies::rearm_endpoint`] when the retained endpoint sent
    /// FIN: a `WriteHalf` remembers FIN process-locally and must be restored
    /// together with the SHM directions.
    pub fn rearm_ticket(
        &self,
        ticket: &Ticket
    ) -> Result<()> {
        self.streams.rearm(ticket.id)?;
        Ok(())
    }

    pub fn accept(
        &self,
        ticket: Ticket
    ) -> Result<Endpoint> {
        Ok(self.streams.open(ticket)?)
    }

    pub fn is_live(
        &self,
        ticket: &Ticket
    ) -> bool {
        self.streams.is_live(ticket.id)
    }

    pub fn node_died(
        &self,
        node: NodeId,
        incarnation: u64
    ) {
        self.streams.node_dead(node, Incarnation::new(incarnation));
    }

    pub fn reclaim(
        &self,
        lanes: &[Reclaimed]
    ) {
        for lane in lanes {
            self.node_died(NodeId::new(lane.lane as u16), lane.incarnation);
        }
    }

    pub fn streams(&self) -> &Streams {
        &self.streams
    }

    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        self.streams.unlink()?;
        Ok(())
    }
}
