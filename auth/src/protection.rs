//! Type-level selection of shared-state protection, independent of token crypto.

mod sealed {
    pub trait Sealed {}
}

/// One protection policy for a physical auth table. Implementations are sealed:
/// the selected type defines a fixed wire contract, not a negotiable algorithm.
pub trait StateProtection: sealed::Sealed + Send + Sync + 'static {
    #[doc(hidden)]
    const ID: u64;
}

/// Visible control metadata authenticated with full keyed BLAKE3 tags.
#[derive(Clone, Copy, Debug, Default)]
pub struct Blake3State;

/// Control metadata encrypted and authenticated with XChaCha20-Poly1305.
/// Verified plaintext remains in the worker's private local session cache.
#[derive(Clone, Copy, Debug, Default)]
pub struct EncryptedState;

impl sealed::Sealed for Blake3State {}
impl sealed::Sealed for EncryptedState {}
impl StateProtection for Blake3State {
    const ID: u64 = 1;
}
impl StateProtection for EncryptedState {
    const ID: u64 = 2;
}

/// No cryptographic protection of shared control records. Every SHM writer can
/// forge session/replay state. Warm checks trust the commit revision alone;
/// writers must publish a new revision for every mutation. Token AEAD, expiry
/// and live-state checks remain.
/// Choose explicitly only when all writers are trusted.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnprotectedState;
impl sealed::Sealed for UnprotectedState {}
impl StateProtection for UnprotectedState {
    const ID: u64 = 3;
}
