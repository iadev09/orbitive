//! The rendezvous: how a caller holding a lease reaches the owner that
//! will execute it.
//!
//! Every consumer of a remote lease needs the same few steps, and each of
//! them is a way to break the pool's contract: the lease has to reach the
//! owner, the owner has to accept it exactly once before trusting a byte
//! of what follows, and the unit has to come back however the session
//! ends. That is what lives here. What crosses afterwards — an HTTP
//! request, a FastCGI record, a worker's task — is the consumer's, and
//! this module never looks at it.
//!
//! The lease goes in first, in one frame of [`SESSION_FRAME`] bytes,
//! written into the stream *before* the offer rings the owner's doorbell.
//! So an owner that sees the offer always finds the frame already in the
//! ring: taking a session never parks, in any runtime.

use orbit_core::NodeId;
use orbit_stream::exchange::{
    ExchangeEndpoint, ExchangeTicket, Exchanges, FlowEvent, PayloadChunk
};
use orbit_stream::{ReadHalf, Streams, Ticket, WriteHalf};

use crate::{
    CreationPermit, Error, Execution, Incarnation, Lease, Limits, Plan, Policy, Pool, Reason,
    ResourceId, Result
};

/// Bytes the lease takes on the wire, ahead of the consumer's own first
/// byte. A stream whose ring cannot hold this much cannot carry a
/// session.
pub const SESSION_FRAME: usize = 32;

const MAGIC: [u8; 4] = *b"PSES";
const VERSION: u8 = 1;

/// The flow-start payload after the pool has consumed its lease prefix.
/// Keeping this value keeps the zero-copy metadata bytes alive; dropping it
/// returns their slots to the payload arena.
pub struct ExchangeSessionStart {
    payload: PayloadChunk
}

/// One ownership decision with the generic exchange rendezvous already
/// applied. This is an optional composition helper: callers with a more
/// specialized policy may continue using [`Pool::acquire`], [`Pool::accept`]
/// and [`Pool::open_exchange_session`] independently.
pub enum ExchangeSessionPlan {
    /// The selected resource belongs to this process and its exact lease has
    /// already been accepted. Use the local object directly.
    Local(Execution),
    /// The selected resource belongs to another process. Side A is open and
    /// its lease plus application metadata have been offered to that owner.
    Remote(ExchangeEndpoint),
    /// No reusable resource won; create and register one while this budget
    /// guard is held.
    Create(CreationPermit),
    /// Capacity may become available after this key version.
    Wait(u32),
    /// The caller's policy deliberately refused the acquisition.
    Reject(Reason)
}

impl ExchangeSessionStart {
    pub fn metadata(&self) -> &[u8] {
        &self.payload[SESSION_FRAME..]
    }

    pub fn payload(&self) -> &PayloadChunk {
        &self.payload
    }
}

fn encode(lease: &Lease) -> [u8; SESSION_FRAME] {
    let mut frame = [0_u8; SESSION_FRAME];
    frame[..4].copy_from_slice(&MAGIC);
    frame[4] = VERSION;
    frame[6..8].copy_from_slice(&lease.holder.get().to_le_bytes());
    frame[8..16].copy_from_slice(&lease.id.net_id().raw().to_le_bytes());
    frame[16..24].copy_from_slice(&lease.fence.to_le_bytes());
    frame[24..].copy_from_slice(&lease.holder_incarnation.get().to_le_bytes());
    frame
}

fn decode(frame: &[u8; SESSION_FRAME]) -> Result<Lease> {
    let word = |at: usize| u64::from_le_bytes(frame[at..at + 8].try_into().expect("eight bytes"));
    if frame[..4] != MAGIC || frame[4] != VERSION {
        return Err(Error::Malformed(format!(
            "not a pool session frame: magic={:?} version={}",
            &frame[..4],
            frame[4]
        )));
    }
    Ok(Lease {
        id: ResourceId::from_net_id(orbit_core::NetId64::from_raw(word(8))),
        fence: word(16),
        holder: NodeId::new(u16::from_le_bytes(frame[6..8].try_into().expect("two bytes"))),
        holder_incarnation: Incarnation::new(word(24))
    })
}

impl Pool {
    /// Apply one pool ownership decision and compose only the generic session
    /// mechanics. Protocol meaning stays with the caller: `metadata` is opaque,
    /// and `exchanges` may use either shared or directional payload arenas.
    pub fn acquire_exchange_session(
        &self,
        key: crate::Key,
        limits: &Limits,
        policy: &dyn Policy,
        exchanges: &Exchanges,
        metadata: &[u8]
    ) -> Result<ExchangeSessionPlan> {
        match self.acquire(key, limits, policy)? {
            Plan::LocalReuse(lease) => self.accept(lease).map(ExchangeSessionPlan::Local),
            Plan::RemoteReuse(lease) => self
                .open_exchange_session(lease, exchanges, metadata)
                .map(ExchangeSessionPlan::Remote),
            Plan::Create(permit) => Ok(ExchangeSessionPlan::Create(permit)),
            Plan::Wait(version) => Ok(ExchangeSessionPlan::Wait(version)),
            Plan::Reject(reason) => Ok(ExchangeSessionPlan::Reject(reason))
        }
    }

    /// Reach the owner of `lease` over `streams`: a stream of this node's
    /// own, the lease in its first frame, and the offer that tells the
    /// owner to take it. The halves that come back carry the consumer's
    /// bytes and nothing else.
    ///
    /// The stream table is the caller's choice, not this crate's: a
    /// session carrying response bodies and one carrying short commands
    /// want different rings, and a [`orbit_stream::StreamSpec`] is how
    /// that is said.
    ///
    /// Giving up is safe and frees nothing: dropping these halves resets
    /// the stream, the owner sees it, and the unit comes back only when
    /// the owner's [`Execution`] ends.
    pub fn open_session(
        &self,
        lease: Lease,
        streams: &Streams
    ) -> Result<(ReadHalf, WriteHalf)> {
        let (endpoint, ticket) = streams.create()?;
        let frame = encode(&lease);
        // One write into an empty ring, before the offer: the owner can
        // never see the offer without the frame behind it.
        match endpoint.try_write(&frame) {
            Ok(SESSION_FRAME) => {}
            Ok(short) => {
                return Err(Error::Malformed(format!(
                    "a stream whose ring took {short} of {SESSION_FRAME} bytes cannot carry a session"
                )));
            }
            Err(error) => return Err(error.into())
        }
        // The owner is the resource's lane, not the lease's holder: the
        // holder is this caller, and the frame carries it so the owner
        // knows whose reservation it is accepting.
        streams.offer(ticket, NodeId::new(lease.id.node()))?;
        Ok(endpoint.split())
    }

    /// Take a session this node was offered: read its lease, accept it —
    /// exactly once, by its fence — and hand over the bytes behind it.
    ///
    /// Readiness stays with the caller, which is why this takes a ticket
    /// rather than waiting for one: a blocking worker parks on
    /// [`Streams::blocking_take_offer`], a task on
    /// [`Streams::poll_take_offer`], and a process that carries other
    /// streams too decides for itself which offer is a session.
    ///
    /// A lease that was accepted already, aged out by `reconcile`, or
    /// taken on a generation that ended is refused here, before any of
    /// the consumer's bytes are read, and the caller's side of the stream
    /// ends with it.
    pub fn accept_session(
        &self,
        streams: &Streams,
        ticket: Ticket
    ) -> Result<(Execution, ReadHalf, WriteHalf)> {
        let endpoint = streams.open(ticket)?;
        let mut frame = [0_u8; SESSION_FRAME];
        let mut filled = 0;
        while filled < SESSION_FRAME {
            match endpoint.try_read(&mut frame[filled..]) {
                // The frame was committed before the offer, so the only
                // empty ring here is one whose peer is already gone.
                Ok(0) | Err(orbit_stream::Error::WouldBlock) => {
                    return Err(Error::Malformed(format!(
                        "a session offered {filled} of {SESSION_FRAME} lease bytes"
                    )));
                }
                Ok(read) => filled += read,
                Err(error) => return Err(error.into())
            }
        }
        let lease = decode(&frame)?;
        let execution = self.accept(lease)?;
        let (read, write) = endpoint.split();
        Ok((execution, read, write))
    }

    /// Create side A for a leased resource, put the lease ahead of the
    /// application's start metadata, then offer side B to the resource owner.
    /// Side A's outbound flow is already started when this returns.
    ///
    /// `RENDEZVOUS_BACKPRESSURE: NO_PHANTOM_RESERVATION` — failure before a
    /// successful offer must return the exact pending fence immediately. The
    /// owner cannot possibly accept an offer that was never published, so
    /// retaining that reservation until reconciliation would turn transport
    /// pressure into false pool exhaustion. This does not authorize cancelling
    /// a delivered or accepted lease.
    pub fn open_exchange_session(
        &self,
        lease: Lease,
        exchanges: &Exchanges,
        request_metadata: &[u8]
    ) -> Result<ExchangeEndpoint> {
        let opened = (|| {
            let (mut server, ticket) = exchanges.create()?;
            let frame = encode(&lease);
            let mut start =
                server.sender().reserve_start(SESSION_FRAME + request_metadata.len())?;
            start[..SESSION_FRAME].copy_from_slice(&frame);
            start[SESSION_FRAME..].copy_from_slice(request_metadata);
            start.commit()?;
            exchanges.offer(ticket, NodeId::new(lease.id.node()))?;
            Ok(server)
        })();
        if opened.is_err() {
            // No successful offer means the owner cannot accept this fence.
            // Return it immediately instead of occupying capacity until the
            // owner's abandoned-reservation grace expires.
            let _ = self.cancel_reservation(lease);
        }
        opened
    }

    /// Open side B from an offered exchange, validate and accept its lease
    /// before exposing later data, and return the application portion of the
    /// start metadata without copying it out of SHM.
    pub fn accept_exchange_session(
        &self,
        exchanges: &Exchanges,
        ticket: ExchangeTicket
    ) -> Result<(Execution, ExchangeEndpoint, ExchangeSessionStart)> {
        let mut client = exchanges.open_peer(ticket)?;
        let payload = match client.receiver().try_next()? {
            FlowEvent::Start { metadata: Some(payload) } if payload.len() >= SESSION_FRAME => {
                payload
            }
            FlowEvent::Start { .. } => {
                return Err(Error::Malformed(
                    "an exchange session start does not contain a lease frame".to_owned()
                ));
            }
            _ => {
                return Err(Error::Malformed(
                    "an exchange session did not begin with request start".to_owned()
                ));
            }
        };
        let mut frame = [0_u8; SESSION_FRAME];
        frame.copy_from_slice(&payload[..SESSION_FRAME]);
        let lease = decode(&frame)?;
        let execution = self.accept(lease)?;
        Ok((execution, client, ExchangeSessionStart { payload }))
    }

    /// Poll for the next offered exchange and validate its exact pool lease
    /// before returning anything to a protocol handler. Runtime services may
    /// use this directly without duplicating the take/open/accept sequence.
    pub fn poll_accept_exchange_session(
        &self,
        exchanges: &Exchanges,
        cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Result<(Execution, ExchangeEndpoint, ExchangeSessionStart)>> {
        match exchanges.poll_take_offer(cx) {
            std::task::Poll::Ready(Ok(ticket)) => {
                std::task::Poll::Ready(self.accept_exchange_session(exchanges, ticket))
            }
            std::task::Poll::Ready(Err(error)) => std::task::Poll::Ready(Err(Error::from(error))),
            std::task::Poll::Pending => std::task::Poll::Pending
        }
    }
}

impl From<orbit_stream::Error> for Error {
    fn from(value: orbit_stream::Error) -> Self {
        Self::Stream(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_survives_the_frame() {
        let lease = Lease {
            id: ResourceId::from_net_id(orbit_core::NetId64::make(247, 3, 0x1234_5678)),
            fence: 0x0102_0304_0506_0708,
            holder: NodeId::new(3),
            holder_incarnation: Incarnation::new(0x0A0B_0C0D)
        };
        let decoded = decode(&encode(&lease)).expect("a frame this crate wrote");
        assert_eq!(decoded.id, lease.id);
        assert_eq!(decoded.fence, lease.fence);
        assert_eq!(decoded.holder, lease.holder);
        assert_eq!(decoded.holder_incarnation, lease.holder_incarnation);
    }

    #[test]
    fn anything_else_is_refused_before_it_is_trusted() {
        let mut frame = encode(&Lease {
            id: ResourceId::from_net_id(orbit_core::NetId64::make(247, 1, 9)),
            fence: 1,
            holder: NodeId::new(1),
            holder_incarnation: Incarnation::new(1)
        });
        frame[4] = VERSION + 1;
        assert!(matches!(decode(&frame), Err(Error::Malformed(_))));
        frame[..4].copy_from_slice(b"HTTP");
        assert!(matches!(decode(&frame), Err(Error::Malformed(_))));
    }

    #[test]
    fn an_exchange_session_accepts_the_lease_before_request_data() {
        use std::sync::Arc;

        use orbit_core::Fleet;
        use orbit_stream::exchange::{ExchangeSpec, PayloadArenaSpec};
        use orbit_stream::{Incarnation as StreamIncarnation, StreamSpec};

        let fleet = Arc::new(Fleet::join("pool-exchange-session", 2).expect("fleet"));
        let pool = Pool::new(Arc::clone(&fleet), Incarnation::new(1)).expect("pool");
        pool.reset_all();
        let resource = pool.register(crate::Key::new(9), 1).expect("resource");
        let lease = pool.reserve(resource).expect("lease");
        let exchanges = Exchanges::open(
            fleet,
            StreamIncarnation::new(1),
            ExchangeSpec::new(StreamSpec::new(210, 4, 512), PayloadArenaSpec::new(211, 8, 64))
        )
        .expect("exchanges");
        exchanges.reset_all();

        let mut server = pool
            .open_exchange_session(lease, &exchanges, b"request headers")
            .expect("open session");
        let ticket = exchanges.take_offer().expect("offered exchange");
        let (execution, mut client, start) =
            pool.accept_exchange_session(&exchanges, ticket).expect("accept session");
        assert_eq!(start.metadata(), b"request headers");

        server.sender().data(b"body").expect("request body");
        let body = match client.receiver().try_next().expect("request data") {
            FlowEvent::Data(body) => body,
            _ => panic!("expected request data")
        };
        assert_eq!(&*body, b"body");
        drop((start, body, execution));
        assert!(pool.reserve(resource).is_ok());
    }

    #[test]
    fn a_session_that_cannot_publish_returns_its_reservation() {
        use std::sync::Arc;

        use orbit_core::Fleet;
        use orbit_stream::exchange::{ExchangeSpec, PayloadArenaSpec};
        use orbit_stream::{Incarnation as StreamIncarnation, StreamSpec};

        let fleet = Arc::new(Fleet::join("pool-exchange-session-full", 1).expect("fleet"));
        let pool = Pool::new(Arc::clone(&fleet), Incarnation::new(1)).expect("pool");
        pool.reset_all();
        let resource = pool.register(crate::Key::new(11), 1).expect("resource");
        let exchanges = Exchanges::open(
            fleet,
            StreamIncarnation::new(1),
            ExchangeSpec::new(StreamSpec::new(214, 4, 512), PayloadArenaSpec::new(215, 1, 64))
        )
        .expect("exchanges");
        exchanges.reset_all();

        let (mut occupying, _ticket) = exchanges.create().expect("occupying exchange");
        occupying.sender().start(Some(b"occupied")).expect("occupy the only payload slot");

        let lease = pool.reserve(resource).expect("lease");
        assert!(matches!(
            pool.open_exchange_session(lease, &exchanges, b"request"),
            Err(Error::Stream(orbit_stream::Error::PayloadFull { requested_slots: 1 }))
        ));
        let candidate = pool.candidates(crate::Key::new(11))[0];
        assert_eq!((candidate.reserved, candidate.active, candidate.free()), (0, 0, 1));
    }

    #[test]
    fn acquisition_helper_accepts_a_local_resource_without_an_exchange_hop() {
        use std::sync::Arc;

        use orbit_core::Fleet;
        use orbit_stream::exchange::{ExchangeSpec, PayloadArenaSpec};
        use orbit_stream::{Incarnation as StreamIncarnation, StreamSpec};

        let fleet = Arc::new(Fleet::join("pool-local-session-plan", 1).expect("fleet"));
        let pool = Pool::new(Arc::clone(&fleet), Incarnation::new(1)).expect("pool");
        pool.reset_all();
        let key = crate::Key::new(10);
        let resource = pool.register(key, 1).expect("resource");
        let exchanges = Exchanges::open(
            fleet,
            StreamIncarnation::new(1),
            ExchangeSpec::new(StreamSpec::new(212, 4, 512), PayloadArenaSpec::new(213, 8, 64))
        )
        .expect("exchanges");
        exchanges.reset_all();

        let plan = pool
            .acquire_exchange_session(
                key,
                &Limits { max_live: 1, attempts: 1 },
                &crate::LocalFirst,
                &exchanges,
                b"unused locally"
            )
            .expect("acquisition");
        let ExchangeSessionPlan::Local(execution) = plan else {
            panic!("a local candidate must stay on the local fast path")
        };
        assert_eq!(execution.lease().id, resource);
        assert!(exchanges.take_offer().is_none());
    }
}
