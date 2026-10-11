//! Tokio-native receiver for one claimed inbox lane.

use std::sync::Arc;

use crate::{LinkSegment, Result};

/// What [`InboxReceiver::next`] observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Received {
    /// A downstream frame was copied out of the lane.
    Frame,
    /// An orbit-link event frame was copied out of the lane; hand it to
    /// `LinkEvents::accept` (the `events` feature). Its first byte is in
    /// [`crate::RESERVED_FRAME_TAGS`], so a downstream decoder never sees it
    /// as one of its own.
    Event,
    /// The lane directory moved to this generation: a lane was claimed or
    /// released. Read [`crate::Inbox::serving`] or the lane identities to see
    /// which.
    Directory(u64),
}

/// One lane's async reader.
///
/// The socket is a wake source, not a message source. [`Self::recv`] always
/// checks the shared-memory lane before arming and checks the lane generation
/// again before awaiting the descriptor, so a coalesced or stale datagram
/// cannot lose a frame.
pub struct InboxReceiver {
    segment: Arc<LinkSegment>,
    lane: usize,
    socket: tokio::net::UnixDatagram,
    /// The directory generation this receiver last reported.
    directory: u64,
}

impl InboxReceiver {
    pub(crate) fn bind(
        segment: Arc<LinkSegment>,
        lane: usize,
    ) -> Result<Self> {
        let socket = segment.inbox().bind_receiver(lane)?;
        let directory = segment.inbox().directory();
        Ok(Self { segment, lane, socket, directory })
    }

    /// Wait for and copy the next frame from this lane, of either kind.
    ///
    /// Cancellation leaves the frame in SHM. A later call first reads the
    /// lane and therefore observes it even if the cancelled future had
    /// already consumed the coalescing descriptor wake.
    pub async fn recv(
        &mut self,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        loop {
            if !matches!(self.next(out).await?, Received::Directory(_)) {
                return Ok(());
            }
        }
    }

    /// Wait for the next frame or the next directory change, whichever comes
    /// first. A change is reported as the generation the directory moved to;
    /// several claims or releases between two looks arrive as one, so compare
    /// the directory itself to learn each of them. Cancellation loses
    /// neither: a frame stays in SHM, and an unreported change is reported
    /// next.
    pub async fn next(
        &mut self,
        out: &mut Vec<u8>,
    ) -> Result<Received> {
        let inbox = self.segment.inbox();
        loop {
            let directory = inbox.directory();
            if directory != self.directory {
                self.directory = directory;
                return Ok(Received::Directory(directory));
            }
            let seen = inbox.receiver_generation(self.lane)?;
            if inbox.read(self.lane, out)? {
                return Ok(if crate::frame::is_event(out) {
                    Received::Event
                } else {
                    Received::Frame
                });
            }

            inbox.arm_receiver(self.lane)?;
            if inbox.receiver_generation(self.lane)? != seen
                || inbox.directory() != self.directory
            {
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
