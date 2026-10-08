use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

/// Same-host fleet namespace, distinct from a process or application name.
///
/// One to eight ASCII letters, digits, hyphens or underscores, stored inline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FleetName {
    bytes: [u8; 8],
    len: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidFleetName;

impl fmt::Display for InvalidFleetName {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.write_str(
            "fleet name must contain 1–8 ASCII letters, digits, '-' or '_'",
        )
    }
}

impl std::error::Error for InvalidFleetName {}

impl FleetName {
    /// Namespace API contract, independent of platform-specific SHM limits.
    pub const MAX_LEN: usize = 8;

    pub fn new(name: impl AsRef<str>) -> Self {
        Self::try_new(name).expect("invalid fleet name")
    }

    pub fn try_new(name: impl AsRef<str>) -> Result<Self, InvalidFleetName> {
        let name = name.as_ref();
        if name.is_empty()
            || name.len() > Self::MAX_LEN
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(InvalidFleetName);
        }
        let mut bytes = [0; 8];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        Ok(Self { bytes, len: name.len() as u8 })
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..usize::from(self.len)])
            .expect("fleet name contains validated ASCII")
    }
}

impl FromStr for FleetName {
    type Err = InvalidFleetName;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::try_new(name)
    }
}

impl From<&str> for FleetName {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for FleetName {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

impl From<Arc<str>> for FleetName {
    fn from(name: Arc<str>) -> Self {
        Self::new(name)
    }
}

impl From<FleetName> for Arc<str> {
    fn from(name: FleetName) -> Self {
        Arc::from(name.as_str())
    }
}

impl From<FleetName> for String {
    fn from(name: FleetName) -> Self {
        name.as_str().to_owned()
    }
}

impl AsRef<str> for FleetName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for FleetName {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for FleetName {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
