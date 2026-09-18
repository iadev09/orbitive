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
use orbit_stream::{ReadHalf, Streams, Ticket, WriteHalf};

use crate::{Error, Execution, Incarnation, Lease, Pool, ResourceId, Result};

/// Bytes the lease takes on the wire, ahead of the consumer's own first
/// byte. A stream whose ring cannot hold this much cannot carry a
/// session.
pub const SESSION_FRAME: usize = 32;

const MAGIC: [u8; 4] = *b"PSES";
const VERSION: u8 = 1;

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
        holder: NodeId::new(u16::from_le_bytes(
            frame[6..8].try_into().expect("two bytes"),
        )),
        holder_incarnation: Incarnation::new(word(24)),
    })
}

impl Pool {
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
    pub fn open_session(&self, lease: Lease, streams: &Streams) -> Result<(ReadHalf, WriteHalf)> {
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
            Err(error) => return Err(error.into()),
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
        ticket: Ticket,
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
                Err(error) => return Err(error.into()),
            }
        }
        let lease = decode(&frame)?;
        let execution = self.accept(lease)?;
        let (read, write) = endpoint.split();
        Ok((execution, read, write))
    }
}

impl From<orbit_stream::Error> for Error {
    fn from(value: orbit_stream::Error) -> Self {
        Self::Io(value.into())
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
            holder_incarnation: Incarnation::new(0x0A0B_0C0D),
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
            holder_incarnation: Incarnation::new(1),
        });
        frame[4] = VERSION + 1;
        assert!(matches!(decode(&frame), Err(Error::Malformed(_))));
        frame[..4].copy_from_slice(b"HTTP");
        assert!(matches!(decode(&frame), Err(Error::Malformed(_))));
    }
}
