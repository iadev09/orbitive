use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    StoreEmpty,
    StoreTooLarge { store_len: usize, max: usize },
    KeyEmpty,
    KeyTooLarge { key_len: usize, max: usize },
    ValueTooLarge { value_len: usize, max: usize },
    InvalidLayout(&'static str),
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoreEmpty => f.write_str("cache store must not be empty"),
            Self::StoreTooLarge { store_len, max } => {
                write!(f, "cache store is too large: {store_len} > {max}")
            }
            Self::KeyEmpty => f.write_str("cache key must not be empty"),
            Self::KeyTooLarge { key_len, max } => {
                write!(f, "cache key is too large: {key_len} > {max}")
            }
            Self::ValueTooLarge { value_len, max } => {
                write!(f, "cache value is too large: {value_len} > {max}")
            }
            Self::InvalidLayout(message) => write!(f, "invalid cache layout: {message}"),
            Self::Io(error) => write!(f, "orbit cache io error: {error}"),
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
