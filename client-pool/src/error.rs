use std::time::Duration;

/// Invalid client-pool options.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum OptionsError {
    #[error("client pool max_live must be greater than zero")]
    ZeroMaxLive,
    #[error("client pool max_concurrency must be greater than zero")]
    ZeroMaxConcurrency,
    #[error("fleet client pool requires an Orbit pool spec with fleet concurrency enabled")]
    FleetConcurrencyDisabled,
    #[error(
        "client pool capacity must satisfy min_idle <= max_idle <= max_live; got {min_idle} <= {max_idle} <= {max_live}"
    )]
    CapacityOrder { min_idle: usize, max_idle: usize, max_live: usize },
    #[error("client pool attempts must be greater than zero")]
    ZeroAttempts,
    #[error("client pool {0} must be greater than zero when present")]
    ZeroDuration(&'static str)
}

/// Failure to acquire one client.
#[derive(Debug, thiserror::Error)]
pub enum AcquireError<E> {
    #[error("client pool is closed")]
    Closed,
    #[error("client pool wait queue is full (maximum {max_waiting})")]
    QueueFull { max_waiting: usize },
    #[error("client pool is at its physical capacity")]
    AtCapacity,
    #[error("timed out acquiring a client after {0:?}")]
    Timeout(Duration),
    #[error("client acquisition was cancelled")]
    Cancelled,
    #[error("client manager failed: {0}")]
    Manager(#[source] E)
}
