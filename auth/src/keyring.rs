use std::collections::BTreeMap;
use std::fmt;

use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::{Error, Result};

pub type KeyId = u32;

/// High-entropy 256-bit key material, zeroized on drop. Never use a password here.
pub struct SecretKey(pub(crate) Zeroizing<[u8; 32]>);

impl SecretKey {
    pub fn generate() -> Result<Self> {
        let mut bytes = Zeroizing::new([0; 32]);
        getrandom::fill(bytes.as_mut()).map_err(|_| Error::Randomness)?;
        Ok(Self(bytes))
    }

    /// Import key material from the application's secret manager.
    /// The caller owns any copies it makes before passing this array.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        f.write_str("SecretKey([REDACTED])")
    }
}

/// Immutable rotation snapshot: issue with the active key, validate with any retained key.
/// Replace the authority to rotate; removing or replacing a key invalidates cache namespaces.
pub struct Keyring {
    pub(crate) active: KeyId,
    pub(crate) keys: BTreeMap<KeyId, SecretKey>
}

impl Keyring {
    pub fn new(
        active: KeyId,
        keys: impl IntoIterator<Item = (KeyId, SecretKey)>
    ) -> Result<Self> {
        let mut retained = BTreeMap::new();
        for (id, key) in keys {
            if retained.insert(id, key).is_some() {
                return Err(Error::DuplicateKey);
            }
        }
        if !retained.contains_key(&active) {
            return Err(Error::MissingActiveKey);
        }
        Ok(Self { active, keys: retained })
    }
}

pub(crate) fn derive(
    key: &SecretKey,
    context: &[u8]
) -> Zeroizing<[u8; 32]> {
    let mut result = Zeroizing::new([0; 32]);
    Hkdf::<Sha256>::new(Some(b"orbit-auth/v1/hkdf-sha256"), key.0.as_ref())
        .expand(context, result.as_mut())
        .expect("32 bytes is a valid HKDF-SHA256 output length");
    result
}

/// Length prefixes make compound context labels unambiguous.
pub(crate) fn context(parts: &[&[u8]]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for part in parts {
        bytes.extend_from_slice(&(part.len() as u64).to_be_bytes());
        bytes.extend_from_slice(part);
    }
    bytes
}
