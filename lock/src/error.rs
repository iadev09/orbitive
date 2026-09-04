use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    InvalidLayout(&'static str),
    NamespaceEmpty,
    KeyEmpty,
    OwnerEmpty,
    EntryTooLarge {
        namespace_len: usize,
        key_len: usize,
        owner_len: usize,
        max_payload: usize,
    },
    StateFull {
        capacity: usize,
    },
    IdExhausted,
    TtlZero,
    TtlOverflow,
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLayout(message) => write!(f, "invalid lock layout: {message}"),
            Self::NamespaceEmpty => f.write_str("lock namespace must not be empty"),
            Self::KeyEmpty => f.write_str("lock key must not be empty"),
            Self::OwnerEmpty => f.write_str("lock owner must not be empty"),
            Self::EntryTooLarge {
                namespace_len,
                key_len,
                owner_len,
                max_payload,
            } => write!(
                f,
                "lock entry is too large: namespace_len={namespace_len} key_len={key_len} owner_len={owner_len} max_payload={max_payload}"
            ),
            Self::StateFull { capacity } => {
                write!(f, "lock state table is full: capacity={capacity}")
            }
            Self::IdExhausted => f.write_str("lock id counter is exhausted"),
            Self::TtlZero => f.write_str("lock TTL must be greater than zero"),
            Self::TtlOverflow => f.write_str("lock TTL deadline overflowed"),
            Self::Io(error) => write!(f, "orbit lock io error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
