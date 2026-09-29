//! Named, bounded same-host dispatch links between members of an Orbit fleet.
//!
//! A link combines two existing transport shapes:
//!
//! - one lossless inbox lane per fleet member for application-defined frames;
//! - one [`orbit_stream`] table for the duplex byte flow attached to a frame.
//!
//! The inbox is admission and discovery, not a retained event log: a full
//! lane refuses a frame instead of overwriting one, and a lane with no live
//! owner refuses it instead of accepting work nobody can answer. The stream
//! ticket travels inside the caller's frame, so this crate chooses no HTTP,
//! RPC, invocation or resource-lease encoding.
//!
//! The fleet name is the rendezvous. No descriptor is inherited or passed,
//! and application bytes remain in SHM. This is not an upstream protocol and
//! it does not own a connection pool. A caller may retain and rearm a stream
//! slot, which reuses bounded SHM capacity rather than a TCP connection or its
//! TLS/protocol state.

use std::fmt;

use orbit_stream::StreamSpec;

#[cfg(unix)]
mod bell;
mod body;
mod inbox;
#[cfg(all(unix, feature = "tokio"))]
mod receiver;
#[cfg(unix)]
mod segment;

pub use body::LinkBodies;
pub use inbox::{Admission, Inbox, InboxGeometry, LANE_NAME_MAX, LANE_ROLE_MAX, Reclaimed};
#[cfg(all(unix, feature = "tokio"))]
pub use receiver::InboxReceiver;
#[cfg(unix)]
pub use segment::{Joined, LinkSegment, LinkSegmentHandle, check_name};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Physical resources one named link fleet opens.
///
/// Kinds are supplied by the deployment and must be distinct. `orbit-link`
/// reserves no global default because several independent links may coexist
/// in one fleet namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinkSpec {
    pub fleet_capacity: u16,
    pub inbox_kind: u8,
    pub inbox: InboxGeometry,
    pub streams: StreamSpec
}

impl LinkSpec {
    pub const fn new(
        fleet_capacity: u16,
        inbox_kind: u8,
        inbox: InboxGeometry,
        streams: StreamSpec
    ) -> Self {
        Self { fleet_capacity, inbox_kind, inbox, streams }
    }

    pub const fn inbox_segment_bytes(self) -> usize {
        self.inbox.segment_bytes(self.fleet_capacity as usize)
    }

    fn validate(self) -> Result<()> {
        if self.fleet_capacity == 0 {
            return Err(Error::Malformed("link fleet capacity must not be zero"));
        }
        if self.inbox_kind == self.streams.kind {
            return Err(Error::Malformed("link inbox and stream table need distinct SHM kinds"));
        }
        self.inbox.validate()
    }
}

/// What can fail before a protocol adapter owns a dispatched frame.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    InboxFull,
    NoLane,
    FrameTooLarge,
    Malformed(&'static str),
    InvalidName { name: String, reason: String },
    RoleConflict { name: String, role: String, held_by: String, lane: usize, pid: u32 },
    Io(std::io::Error),
    Core(orbit_core::Error),
    Stream(orbit_stream::Error)
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        match self {
            Self::InboxFull => formatter.write_str("the target inbox lane is full or unowned"),
            Self::NoLane => formatter.write_str("every lane in the named fleet is held"),
            Self::FrameTooLarge => formatter.write_str("the frame does not fit in one inbox slot"),
            Self::Malformed(message) => formatter.write_str(message),
            Self::InvalidName { name, reason } => {
                write!(formatter, "{name:?} cannot name a link fleet: {reason}")
            }
            Self::RoleConflict { name, role, held_by, lane, pid } => write!(
                formatter,
                "{name:?} is already served by {held_by} on lane {lane} (pid {pid}); role {role:?} cannot join under the same name"
            ),
            Self::Io(error) => write!(formatter, "Orbit link IO error: {error}"),
            Self::Core(error) => write!(formatter, "Orbit link core error: {error}"),
            Self::Stream(error) => write!(formatter, "Orbit link stream error: {error}")
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Core(error) => Some(error),
            Self::Stream(error) => Some(error),
            _ => None
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<orbit_core::Error> for Error {
    fn from(error: orbit_core::Error) -> Self {
        Self::Core(error)
    }
}

impl From<orbit_stream::Error> for Error {
    fn from(error: orbit_stream::Error) -> Self {
        Self::Stream(error)
    }
}

/// Whether this host can park an inbox reader without polling.
pub fn parking_supported() -> bool {
    orbit_core::sync::supported()
}
