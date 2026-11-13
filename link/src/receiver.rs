//! Tokio-native receiver for one claimed inbox lane.

use std::sync::Arc;

use crate::{LinkSegment, Result};

/// What [`InboxReceiver::next`] observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Received {
    /// A downstream frame was copied out of the lane.
    Frame,
    /// A frame carrying one receiver-owned work admission.
    Work(crate::WorkTicket),
    /// Interested capacity changed; recheck before submitting work.
    Capacity(u64),
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
///
/// A writer rings only after taking the lane's arm, so every arm this
/// receiver finds taken is exactly one bell sent. It reads those and no more:
/// the socket is not asked whether it is empty.
pub struct InboxReceiver {
    segment: Arc<LinkSegment>,
    lane: usize,
    socket: Option<tokio::net::UnixDatagram>,
    /// The directory generation this receiver last reported.
    directory: u64,
    capacity: u64,
    /// Whether this receiver raised the lane's arm and has not taken it back.
    armed: bool,
    /// Bells sent for arms a writer took, and not read yet.
    owed: u32,
}

impl InboxReceiver {
    pub(crate) fn bind(
        segment: Arc<LinkSegment>,
        lane: usize,
    ) -> Result<Self> {
        let socket = segment.inbox().bind_receiver(lane)?;
        let directory = segment.inbox().directory();
        let capacity = segment.inbox().capacity_generation(lane)?;
        Ok(Self {
            segment,
            lane,
            socket: Some(socket),
            directory,
            capacity,
            armed: false,
            owed: 0,
        })
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
            match self.next(out).await? {
                Received::Directory(_) | Received::Capacity(_) => {}
                Received::Work(ticket) => {
                    self.segment.inbox().cancel_work(ticket);
                    return Err(crate::Error::Malformed(
                        "work frames require an admission-aware receiver",
                    ));
                }
                Received::Frame | Received::Event => return Ok(()),
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
        Ok(self.next_inner(Some(out), None).await?.expect("no read boundary"))
    }

    /// Read as usual until the cursor reaches a captured reservation boundary.
    /// Report progress even if reaching it only consumed cancelled turns.
    pub async fn next_until_read(
        &mut self,
        out: &mut Vec<u8>,
        boundary: u64,
    ) -> Result<Option<Received>> {
        self.next_inner(Some(out), Some(boundary)).await
    }

    /// Drive lane notifications after intake has stopped, without consuming
    /// work or control frames still in the inbox.
    pub async fn next_notification(&mut self) -> Result<Received> {
        Ok(self.next_inner(None, None).await?.expect("no read boundary"))
    }

    async fn next_inner(
        &mut self,
        mut out: Option<&mut Vec<u8>>,
        boundary: Option<u64>,
    ) -> Result<Option<Received>> {
        // A wait that was cancelled left its arm raised.
        self.lower();
        loop {
            let inbox = self.segment.inbox();
            let directory = inbox.directory();
            if directory != self.directory {
                self.directory = directory;
                return Ok(Some(Received::Directory(directory)));
            }
            let seen = inbox.receiver_generation(self.lane)?;
            let capacity = inbox.capacity_generation(self.lane)?;
            if capacity != self.capacity {
                self.capacity = capacity;
                return Ok(Some(Received::Capacity(capacity)));
            }
            if let Some(out) = out.as_deref_mut()
                && let Some(work) =
                    inbox.read_frame_before(self.lane, out, boundary)?
            {
                return Ok(Some(if let Some(ticket) = work {
                    Received::Work(ticket)
                } else if crate::frame::is_event(out) {
                    Received::Event
                } else {
                    Received::Frame
                }));
            }

            if let Some(boundary) = boundary
                && inbox.read_position(self.lane)? >= boundary
            {
                return Ok(None);
            }

            // Only the empty-to-wait transition touches the socket, and
            // only for bells known to have been sent: one from an earlier
            // arm must be read before another arm can bring a second.
            self.settle()?;
            let inbox = self.segment.inbox();
            inbox.arm_receiver(self.lane)?;
            self.armed = true;
            if inbox.receiver_generation(self.lane)? != seen
                || inbox.directory() != self.directory
            {
                self.lower();
                continue;
            }

            self.socket().readable().await?;
            self.lower();
            if self.owed == 0 {
                // Woken with the arm still raised: a bell this receiver did
                // not account for. Read until the socket says it is empty.
                crate::bell::drain(self.socket())?;
            }
        }
    }

    /// Take the arm back if it is raised. One a writer took first is one
    /// bell sent.
    fn lower(&mut self) {
        if std::mem::take(&mut self.armed)
            && !self.segment.inbox().take_receiver_arm(self.lane)
        {
            self.owed += 1;
        }
    }

    /// Read the bells known to have been sent, and no more. One that is not
    /// there yet is on its way and stays owed.
    fn settle(&mut self) -> std::io::Result<()> {
        if self.owed == 0 {
            return Ok(());
        }
        while self.owed > 0 && crate::bell::take_one(self.socket())? {
            self.owed -= 1;
        }
        crate::bell::forget_readiness(self.socket());
        Ok(())
    }

    fn socket(&self) -> &tokio::net::UnixDatagram {
        self.socket.as_ref().expect("a live receiver owns its socket")
    }

    fn close(
        &mut self,
        release: impl FnOnce(&LinkSegment, usize),
    ) {
        let Some(socket) = self.socket.take() else {
            return;
        };
        self.segment.inbox().unbind_receiver(self.lane);
        // The sender may still be connected to this socket. Close it before
        // another receiver can take the registry entry and arm the same lane.
        drop(socket);
        release(&self.segment, self.lane);
    }

    pub fn lane(&self) -> usize {
        self.lane
    }
}

#[cfg(test)]
#[path = "../tests/unit/receiver.rs"]
mod tests;

impl Drop for InboxReceiver {
    fn drop(&mut self) {
        self.close(LinkSegment::release_receiver);
    }
}
