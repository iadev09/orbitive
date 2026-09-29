use std::sync::Arc;

/// Admission consequence derived from one protocol-specific outcome.
///
/// This type deliberately contains no HTTP status, header, retry, or decoding
/// knowledge. The application returns its own error to the triggering caller,
/// then may apply `Pause` to stop later acquisitions until it repairs the
/// profile and explicitly resumes the pool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PoolDirective {
    Continue,
    Pause { reason: Arc<str> }
}

impl PoolDirective {
    pub fn pause(reason: impl Into<Arc<str>>) -> Self {
        Self::Pause { reason: reason.into() }
    }
}

/// Application-owned classification from a domain outcome to pool admission.
///
/// `O` may be an HTTP response, a protocol frame, or an application result.
/// The pool never decodes it.
pub trait ClientPolicy<O>: Send + Sync {
    fn classify(
        &self,
        outcome: &O
    ) -> PoolDirective;
}
