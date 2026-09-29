use std::time::Duration;

use crate::OptionsError;

/// Code-first operating policy for one process-local client pool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolOptions {
    pub(crate) min_idle: usize,
    pub(crate) max_idle: usize,
    pub(crate) max_live: usize,
    pub(crate) max_concurrency: usize,
    pub(crate) max_waiting: usize,
    pub(crate) acquire_timeout: Option<Duration>,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) max_lifetime: Option<Duration>
}

impl PoolOptions {
    /// Declare every capacity value explicitly. Constructing options performs no IO.
    pub const fn new(
        min_idle: usize,
        max_idle: usize,
        max_live: usize,
        max_concurrency: usize,
        max_waiting: usize
    ) -> Self {
        Self {
            min_idle,
            max_idle,
            max_live,
            max_concurrency,
            max_waiting,
            acquire_timeout: None,
            idle_timeout: None,
            max_lifetime: None
        }
    }

    /// Bound admission waits. `None` declares an indefinite wait.
    pub const fn acquire_timeout(
        mut self,
        timeout: Option<Duration>
    ) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Retire an unused client after the declared duration.
    pub const fn idle_timeout(
        mut self,
        timeout: Option<Duration>
    ) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Retire a client after the declared total lifetime.
    pub const fn max_lifetime(
        mut self,
        lifetime: Option<Duration>
    ) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), OptionsError> {
        validate(
            self.min_idle,
            self.max_idle,
            self.max_live,
            self.max_concurrency,
            self.acquire_timeout,
            self.idle_timeout,
            self.max_lifetime
        )
    }
}

/// Code-first operating policy for one fleet-wide client profile.
#[cfg(feature = "fleet")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FleetPoolOptions {
    pub(crate) min_idle: u32,
    pub(crate) max_idle: u32,
    pub(crate) max_live: u32,
    pub(crate) max_concurrency: u32,
    pub(crate) max_waiting: usize,
    pub(crate) acquire_timeout: Option<Duration>,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) max_lifetime: Option<Duration>,
    pub(crate) attempts: u32
}

#[cfg(feature = "fleet")]
impl FleetPoolOptions {
    /// Declare every capacity value and the lost-race attempt budget explicitly.
    pub const fn new(
        min_idle: u32,
        max_idle: u32,
        max_live: u32,
        max_concurrency: u32,
        max_waiting: usize,
        attempts: u32
    ) -> Self {
        Self {
            min_idle,
            max_idle,
            max_live,
            max_concurrency,
            max_waiting,
            acquire_timeout: None,
            idle_timeout: None,
            max_lifetime: None,
            attempts
        }
    }

    /// Bound admission waits. `None` declares an indefinite wait.
    pub const fn acquire_timeout(
        mut self,
        timeout: Option<Duration>
    ) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Retire a locally owned unused client after the declared duration.
    pub const fn idle_timeout(
        mut self,
        timeout: Option<Duration>
    ) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Retire a locally owned client after the declared total lifetime.
    pub const fn max_lifetime(
        mut self,
        lifetime: Option<Duration>
    ) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), OptionsError> {
        validate(
            self.min_idle as usize,
            self.max_idle as usize,
            self.max_live as usize,
            self.max_concurrency as usize,
            self.acquire_timeout,
            self.idle_timeout,
            self.max_lifetime
        )?;
        if self.attempts == 0 {
            return Err(OptionsError::ZeroAttempts);
        }
        Ok(())
    }

    pub(crate) fn local_options(&self) -> PoolOptions {
        PoolOptions::new(
            0,
            self.max_live as usize,
            self.max_live as usize,
            self.max_live as usize,
            self.max_waiting
        )
        .acquire_timeout(self.acquire_timeout)
        .idle_timeout(self.idle_timeout)
        .max_lifetime(self.max_lifetime)
    }
}

fn validate(
    min_idle: usize,
    max_idle: usize,
    max_live: usize,
    max_concurrency: usize,
    acquire_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>
) -> Result<(), OptionsError> {
    if max_live == 0 {
        return Err(OptionsError::ZeroMaxLive);
    }
    if max_concurrency == 0 {
        return Err(OptionsError::ZeroMaxConcurrency);
    }
    if min_idle > max_idle || max_idle > max_live {
        return Err(OptionsError::CapacityOrder { min_idle, max_idle, max_live });
    }
    for (name, value) in [
        ("acquire_timeout", acquire_timeout),
        ("idle_timeout", idle_timeout),
        ("max_lifetime", max_lifetime)
    ] {
        if value.is_some_and(|duration| duration.is_zero()) {
            return Err(OptionsError::ZeroDuration(name));
        }
    }
    Ok(())
}
