//! Error type for `orbit-core`. Deliberately small — this layer has few
//! independent failure modes.

use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    /// `Fleet::join` was called twice in the same process.
    AlreadyJoined { name: &'static str },

    /// Fleet size cannot be zero — Orbit's plurality requirement (see VISION §2).
    EmptyFleet,

    /// A node id must address one of the fleet's declared member slots.
    NodeOutsideFleet { node_id: u16, fleet_size: u8 },

    /// Shared-memory operation failed.
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyJoined { name } => {
                write!(f, "fleet '{name}' has already been joined in this process")
            }
            Self::EmptyFleet => {
                write!(f, "fleet_size must be ≥ 1; Orbit needs at least one member")
            }
            Self::NodeOutsideFleet {
                node_id,
                fleet_size,
            } => write!(f, "node_id {node_id} is outside fleet_size {fleet_size}"),
            Self::Io(err) => write!(f, "orbit io error: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}
