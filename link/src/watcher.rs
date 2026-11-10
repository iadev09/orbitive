//! Tokio-native directory watcher for a process that holds no lane.

use std::sync::Arc;

use orbit_core::shm::WatcherHold;

use crate::{LinkSegment, Result};

/// Wakes when a lane is claimed or released, without serving anything.
///
/// The directory is current state, kept in the table; this only says when to
/// look at it again. Several changes between two looks arrive as one, so
/// compare the lane identities themselves to learn each of them.
pub struct DirectoryWatcher {
    segment: Arc<LinkSegment>,
    slot: usize,
    socket: tokio::net::UnixDatagram,
    /// The directory generation this watcher last reported.
    directory: u64,
    /// Whether the pending pressure episode was already reported here.
    pressure_reported: bool,
    _hold: WatcherHold,
}

/// What [`DirectoryWatcher::next_change`] woke for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Watched {
    /// The lane directory moved to this generation.
    Directory(u64),
    /// A sender reported unmet demand: every receiver it tried was full.
    /// Reported once per episode; the owner ends it with
    /// [`DirectoryWatcher::take_pressure`] once it has acted.
    Pressure,
}

impl DirectoryWatcher {
    pub(crate) fn bind(
        segment: Arc<LinkSegment>,
        slot: usize,
        role: &str,
        hold: WatcherHold,
    ) -> Result<Self> {
        let socket = segment.inbox().bind_watcher(slot, role)?;
        let directory = segment.inbox().directory();
        Ok(Self {
            segment,
            slot,
            socket,
            directory,
            pressure_reported: false,
            _hold: hold,
        })
    }

    /// Wait until the directory moves, and return the generation it moved
    /// to. Cancellation loses nothing: an unreported change is reported by
    /// the next call.
    pub async fn next(&mut self) -> Result<u64> {
        let inbox = self.segment.inbox();
        loop {
            let seen = inbox.watcher_generation(self.slot)?;
            let directory = inbox.directory();
            if directory != self.directory {
                self.directory = directory;
                return Ok(directory);
            }

            inbox.disarm_watcher(self.slot);
            crate::bell::drain(&self.socket)?;
            inbox.arm_watcher(self.slot)?;
            if inbox.watcher_generation(self.slot)? != seen
                || inbox.directory() != self.directory
            {
                inbox.disarm_watcher(self.slot);
                continue;
            }

            self.socket.readable().await?;
            let mut scratch = [0u8; 64];
            while self.socket.try_recv(&mut scratch).is_ok() {}
        }
    }

    /// Wait until the directory moves or a pressure episode begins, and say
    /// which. A directory change is reported first when both are pending;
    /// the episode is reported once, however many senders reported it.
    pub async fn next_change(&mut self) -> Result<Watched> {
        let segment = Arc::clone(&self.segment);
        let inbox = segment.inbox();
        loop {
            let seen = inbox.watcher_generation(self.slot)?;
            if let Some(change) = self.observe() {
                return Ok(change);
            }

            inbox.disarm_watcher(self.slot);
            crate::bell::drain(&self.socket)?;
            inbox.arm_watcher(self.slot)?;
            if inbox.watcher_generation(self.slot)? != seen || self.observable()
            {
                inbox.disarm_watcher(self.slot);
                continue;
            }

            self.socket.readable().await?;
            let mut scratch = [0u8; 64];
            while self.socket.try_recv(&mut scratch).is_ok() {}
        }
    }

    /// End the pressure episode once the owner has acted, so the next report
    /// starts a new one and is reported again — even one that arrives before
    /// this watcher looks again. Returns whether one was pending.
    pub fn take_pressure(&mut self) -> bool {
        self.pressure_reported = false;
        self.segment.inbox().take_pressure()
    }

    fn observe(&mut self) -> Option<Watched> {
        let inbox = self.segment.inbox();
        let directory = inbox.directory();
        if directory != self.directory {
            self.directory = directory;
            return Some(Watched::Directory(directory));
        }
        let pending = inbox.pressure_pending();
        if pending && !self.pressure_reported {
            self.pressure_reported = true;
            return Some(Watched::Pressure);
        }
        if !pending {
            self.pressure_reported = false;
        }
        None
    }

    fn observable(&self) -> bool {
        let inbox = self.segment.inbox();
        inbox.directory() != self.directory
            || inbox.pressure_pending() != self.pressure_reported
    }

    /// The directory generation last reported, or the one current when the
    /// watcher was made.
    pub fn directory(&self) -> u64 {
        self.directory
    }

    pub fn slot(&self) -> usize {
        self.slot
    }

    pub fn segment(&self) -> &Arc<LinkSegment> {
        &self.segment
    }
}

impl Drop for DirectoryWatcher {
    fn drop(&mut self) {
        self.segment.inbox().unbind_watcher(self.slot);
    }
}
