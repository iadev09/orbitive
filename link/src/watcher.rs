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
    _hold: WatcherHold,
}

impl DirectoryWatcher {
    pub(crate) fn bind(
        segment: Arc<LinkSegment>,
        slot: usize,
        hold: WatcherHold,
    ) -> Result<Self> {
        let socket = segment.inbox().bind_watcher(slot)?;
        let directory = segment.inbox().directory();
        Ok(Self { segment, slot, socket, directory, _hold: hold })
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
