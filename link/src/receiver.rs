//! Tokio-native receiver for one claimed inbox lane.

use std::sync::Arc;

use crate::{LinkSegment, Result};

/// One lane's async reader.
///
/// The socket is a wake source, not a message source. [`Self::recv`] always
/// checks the shared-memory lane before arming and checks the lane generation
/// again before awaiting the descriptor, so a coalesced or stale datagram
/// cannot lose a frame.
pub struct InboxReceiver {
    segment: Arc<LinkSegment>,
    lane: usize,
    socket: tokio::net::UnixDatagram
}

impl InboxReceiver {
    pub(crate) fn bind(
        segment: Arc<LinkSegment>,
        lane: usize
    ) -> Result<Self> {
        let socket = segment.inbox().bind_receiver(lane)?;
        Ok(Self { segment, lane, socket })
    }

    /// Wait for and copy the next frame from this lane.
    ///
    /// Cancellation leaves the frame in SHM. A later call first reads the
    /// lane and therefore observes it even if the cancelled future had
    /// already consumed the coalescing descriptor wake.
    pub async fn recv(
        &mut self,
        out: &mut Vec<u8>
    ) -> Result<()> {
        let inbox = self.segment.inbox();
        loop {
            let seen = inbox.receiver_generation(self.lane)?;
            if inbox.read(self.lane, out)? {
                return Ok(());
            }

            inbox.arm_receiver(self.lane)?;
            if inbox.receiver_generation(self.lane)? != seen {
                inbox.disarm_receiver(self.lane);
                continue;
            }

            self.socket.readable().await?;
            let mut scratch = [0u8; 64];
            while self.socket.try_recv(&mut scratch).is_ok() {}
        }
    }

    pub fn lane(&self) -> usize {
        self.lane
    }
}

impl Drop for InboxReceiver {
    fn drop(&mut self) {
        self.segment.inbox().unbind_receiver(self.lane);
        self.segment.release_receiver(self.lane);
    }
}
