//! Error type for `orbit-core`. Deliberately small — this layer has few
//! independent failure modes.

use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    /// `Fleet::join` was called twice in the same process.
    AlreadyJoined { name: &'static str },

    /// Fleet capacity cannot be zero — Orbit needs at least one addressable node.
    EmptyFleet,

    /// A node id must address one of the fleet's reserved physical slots.
    NodeOutsideFleet { node_id: u16, fleet_capacity: u16 },

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
                write!(
                    f,
                    "fleet_capacity must be ≥ 1; Orbit needs at least one node lane"
                )
            }
            Self::NodeOutsideFleet {
                node_id,
                fleet_capacity,
            } => write!(
                f,
                "node_id {node_id} is outside fleet_capacity {fleet_capacity}"
            ),
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
