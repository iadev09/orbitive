use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Maximum UTF-8 byte length of each realm, issuer, subject, audience or capability.
pub const MAX_LABEL_LEN: usize = 256;
pub const MAX_CAPABILITIES: usize = 64;

pub(crate) fn valid_label(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_LABEL_LEN && !value.chars().any(char::is_control)
}

macro_rules! label {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                if !valid_label(&value) {
                    return Err(Error::InvalidInput);
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = Error;
            fn try_from(value: String) -> Result<Self> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

label!(Audience, "Exact logical recipient; independent of DNS and transport.");
label!(Capability, "Exact application-defined permission; no implicit wildcards or hierarchy.");

/// Cryptographically separated credential uses, independent of extraction method.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    Session,
    Refresh,
    Access,
    WebSocket,
    Internal
}

impl Purpose {
    pub(crate) fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Session => b"session",
            Self::Refresh => b"refresh",
            Self::Access => b"access",
            Self::WebSocket => b"websocket",
            Self::Internal => b"internal"
        }
    }
}

/// Random 128-bit token identifier. Replay stores must also scope by realm and issuer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TokenId(pub [u8; 16]);

/// Issuer-authorized claims. All times are absolute Unix seconds; no implicit TTL or leeway.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Claims {
    pub subject: String,
    pub audience: Audience,
    pub purpose: Purpose,
    pub capabilities: BTreeSet<Capability>,
    pub issued_at: u64,
    pub not_before: u64,
    pub expires_at: u64
}

impl Claims {
    pub(crate) fn check_shape(&self) -> Result<()> {
        if !valid_label(&self.subject)
            || self.capabilities.len() > MAX_CAPABILITIES
            || self.issued_at > self.not_before
            || self.not_before >= self.expires_at
        {
            return Err(Error::InvalidInput);
        }
        Ok(())
    }
}

/// Authenticated identity snapshot. This value is not a transferable credential.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Principal {
    pub(crate) realm: String,
    pub(crate) issuer: String,
    pub(crate) jti: TokenId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session: Option<crate::fleet::SessionBinding>,
    pub(crate) claims: Claims
}

impl Principal {
    pub fn session_id(&self) -> Option<crate::SessionId> {
        self.session.as_ref().map(|session| session.id)
    }
    pub fn realm(&self) -> &str {
        &self.realm
    }
    pub fn issuer(&self) -> &str {
        &self.issuer
    }
    pub fn jti(&self) -> TokenId {
        self.jti
    }
    pub fn subject(&self) -> &str {
        &self.claims.subject
    }
    pub fn claims(&self) -> &Claims {
        &self.claims
    }
    pub fn has_capability(
        &self,
        capability: &Capability
    ) -> bool {
        self.claims.capabilities.contains(capability)
    }
}

/// Expected recipient and use come from trusted routing configuration, never the token.
#[derive(Clone, Debug)]
pub struct Validation {
    pub audience: Audience,
    pub purpose: Purpose,
    pub required_capabilities: BTreeSet<Capability>
}

/// Called after static validation on EVERY authentication, including cache hits.
///
/// A one-use policy must atomically consume `(realm, issuer, jti)` until `expires_at`.
/// Fail closed with `PolicyUnavailable` if its authoritative store is unavailable.
/// Revocation checks and application-specific current-state policy also belong here.
pub trait ValidationHook {
    fn check(
        &self,
        principal: &Principal,
        now: u64
    ) -> Result<()>;
}

/// Explicitly permits token reuse until expiry; provides no revocation or replay protection.
pub struct AllowReusable;

impl ValidationHook for AllowReusable {
    fn check(
        &self,
        _: &Principal,
        _: u64
    ) -> Result<()> {
        Ok(())
    }
}
