/// Authentication failures contain no credentials or decrypted claim data.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("invalid authentication configuration or claims")]
    InvalidInput,
    #[error("duplicate key id")]
    DuplicateKey,
    #[error("active key is missing")]
    MissingActiveKey,
    #[error("operating system randomness unavailable")]
    Randomness,
    #[error("invalid token")]
    InvalidToken,
    #[error("token exceeds the wire size limit")]
    TokenTooLarge,
    #[error("token expired")]
    Expired,
    #[error("token is not yet valid")]
    NotYetValid,
    #[error("required capability missing")]
    MissingCapability,
    #[error("token revoked")]
    Revoked,
    #[error("token already consumed")]
    Replayed,
    #[error("validation policy unavailable")]
    PolicyUnavailable,
    #[error("authentication state table is full")]
    StateFull,
    #[error("authentication table has an incompatible layout")]
    IncompatibleLayout,
    #[error("authentication table uses a different state protection policy")]
    IncompatibleProtection
}

pub type Result<T> = std::result::Result<T, Error>;
