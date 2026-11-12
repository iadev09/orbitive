//! Named descriptor wake for an inbox lane or a directory watcher.
//!
//! Frames remain in shared memory. The datagram contains one meaningless
//! byte and exists only so an async runtime can sleep on a descriptor instead
//! of waking a dedicated thread that then wakes the runtime.

use std::borrow::Cow;
use std::io;
use std::os::unix::net::{SocketAddr, UnixDatagram};
use std::sync::{Mutex, OnceLock, PoisonError};

/// Empty obsolete bells only when the receiver has no shared work left.
/// Callers disarm first and recheck the shared generation after rearming.
#[cfg(feature = "tokio")]
pub(crate) fn drain(socket: &tokio::net::UnixDatagram) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let mut scratch = [0u8; 64];
    loop {
        // Tokio may not have observed readiness yet when the generation
        // recheck avoided parking. Drain the kernel queue in that case too.
        // SAFETY: the socket owns this fd and scratch is writable for len bytes.
        let read = unsafe {
            libc::recv(
                socket.as_raw_fd(),
                scratch.as_mut_ptr().cast(),
                scratch.len(),
                libc::MSG_DONTWAIT,
            )
        };
        crate::probe::read(read >= 0);
        if read >= 0 {
            continue;
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => {
                // Let Tokio clear a cached readiness bit with its own IO
                // attempt. A late bell is harmless; consume it and finish
                // draining before rearming.
                let late = socket.try_recv(&mut scratch);
                crate::probe::read(late.is_ok());
                match late {
                    Ok(_) => continue,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(());
                    }
                    Err(error)
                        if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(error),
                }
            }
            _ => return Err(error),
        }
    }
}

/// Read one bell if one is there. The call goes to the socket itself: Tokio
/// may not have seen the bell arrive yet, and would answer for it.
#[cfg(feature = "tokio")]
pub(crate) fn take_one(socket: &tokio::net::UnixDatagram) -> io::Result<bool> {
    use std::os::fd::AsRawFd;

    let mut scratch = [0u8; 64];
    loop {
        // SAFETY: the socket owns this fd and scratch is writable for len bytes.
        let read = unsafe {
            libc::recv(
                socket.as_raw_fd(),
                scratch.as_mut_ptr().cast(),
                scratch.len(),
                libc::MSG_DONTWAIT,
            )
        };
        crate::probe::read(read >= 0);
        if read >= 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => return Ok(false),
            _ => return Err(error),
        }
    }
}

/// Drop Tokio's note that the socket is readable, without a call. For a
/// caller that has read every bell it knows was sent: the note is about bells
/// that are gone, and the next bell makes a new one.
#[cfg(feature = "tokio")]
pub(crate) fn forget_readiness(socket: &tokio::net::UnixDatagram) {
    let _ = socket.try_io(tokio::io::Interest::READABLE, || {
        Err::<(), io::Error>(io::ErrorKind::WouldBlock.into())
    });
}

/// Whether a send was refused because the far side's queue is full.
fn queue_full(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
        || error.raw_os_error() == Some(libc::ENOBUFS)
}

/// Who a datagram wakes. Lanes and watchers have separate addresses because a
/// named socket has exactly one binder.
#[derive(Clone, Copy)]
pub(crate) enum Doorbell {
    Lane(usize),
    Watcher(usize),
    /// A producer registration waiting for a reader to free a record.
    Producer(usize),
    /// The process waiting for the Link's owner to create it.
    Adopter,
}

impl Doorbell {
    fn suffix(self) -> String {
        match self {
            Self::Lane(lane) => lane.to_string(),
            Self::Watcher(slot) => format!("w{slot}"),
            Self::Producer(index) => format!("p{index}"),
            Self::Adopter => "a".to_owned(),
        }
    }
}

/// A socket connected to one lane's bell, and the lane epoch it was connected
/// under.
struct Connected {
    epoch: u64,
    socket: UnixDatagram,
}

pub(crate) struct Bell {
    prefix: String,
    sender: UnixDatagram,
    group: Option<orbit_core::shm::GroupAccess>,
    /// One entry for each lane this process may ring. A ring over a kept
    /// connection is a plain send; naming the lane again is for the first
    /// ring and for a bell that was replaced. Only the writer that took the
    /// lane's arm rings it, so a lane's entry is not contended.
    lanes: Box<[Mutex<Option<Connected>>]>,
    /// Each door's address, built on its first ring: a door's name is fixed
    /// for the bell's life, so a ring is one send rather than a name to
    /// format again. Indexed by [`Bell::door_index`].
    addresses: Box<[OnceLock<SocketAddr>]>,
}

impl Bell {
    #[cfg(test)]
    pub(crate) fn new(segment: &str) -> io::Result<Self> {
        Self::with_group(segment, None, 1)
    }

    /// `lanes` is how many lane bells this one may keep a connection to;
    /// zero for a bell that only binds.
    pub(crate) fn with_group(
        segment: &str,
        group: Option<orbit_core::shm::GroupAccess>,
        lanes: usize,
    ) -> io::Result<Self> {
        let sender = UnixDatagram::unbound()?;
        sender.set_nonblocking(true)?;
        let prefix = if group.is_some() {
            segment.to_owned()
        } else {
            Self::prefix(segment)
        };
        let addresses =
            (0..Self::door_count(lanes)).map(|_| OnceLock::new()).collect();
        let lanes = (0..lanes).map(|_| Mutex::new(None)).collect();
        Ok(Self { prefix, sender, group, lanes, addresses })
    }

    pub(crate) fn validate_group_address(
        access: &orbit_core::shm::GroupAccess,
        segment: &str,
        last_slot: usize,
    ) -> io::Result<()> {
        for door in Self::longest(last_slot) {
            SocketAddr::from_pathname(
                access.bell_path(segment, &door.suffix()),
            )?;
        }
        Ok(())
    }

    /// The longest names a table of `last_slot + 1` lanes binds: its last
    /// watcher and its last producer registration (two per lane).
    fn longest(last_slot: usize) -> [Doorbell; 2] {
        [Doorbell::Watcher(last_slot), Doorbell::Producer(2 * last_slot + 1)]
    }

    fn group_path(
        &self,
        door: Doorbell,
    ) -> Option<std::path::PathBuf> {
        self.group
            .as_ref()
            .map(|access| access.bell_path(&self.prefix, &door.suffix()))
    }

    fn prefix(segment: &str) -> String {
        // SAFETY: a plain syscall with no arguments.
        let uid = unsafe { libc::getuid() };
        let segment = segment.trim_start_matches('/');
        format!("orbit-link-{uid}/{segment}")
    }

    /// Check the longest lane, watcher or producer address before creating
    /// either SHM resource. A watcher suffix is one byte longer than the lane
    /// suffix; a producer index runs to twice the lane count.
    pub(crate) fn validate_address(
        segment: &str,
        last_slot: usize,
    ) -> io::Result<()> {
        let prefix = Self::prefix(segment);
        for door in Self::longest(last_slot) {
            Self::address_for(&prefix, door)?;
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn address(
        &self,
        door: Doorbell,
    ) -> io::Result<SocketAddr> {
        if let Some(path) = self.group_path(door) {
            return SocketAddr::from_pathname(path);
        }
        Self::address_for(&self.prefix, door)
    }

    #[cfg(target_os = "linux")]
    fn address_for(
        prefix: &str,
        door: Doorbell,
    ) -> io::Result<SocketAddr> {
        use std::os::linux::net::SocketAddrExt;
        SocketAddr::from_abstract_name(format!("{}.{}", prefix, door.suffix()))
    }

    #[cfg(not(target_os = "linux"))]
    fn path(
        &self,
        door: Doorbell,
    ) -> std::path::PathBuf {
        Self::path_for(&self.prefix, door)
    }

    #[cfg(not(target_os = "linux"))]
    fn path_for(
        prefix: &str,
        door: Doorbell,
    ) -> std::path::PathBuf {
        std::path::PathBuf::from(format!("/tmp/{}.{}", prefix, door.suffix()))
    }

    #[cfg(not(target_os = "linux"))]
    fn address(
        &self,
        door: Doorbell,
    ) -> io::Result<SocketAddr> {
        if let Some(path) = self.group_path(door) {
            return SocketAddr::from_pathname(path);
        }
        Self::address_for(&self.prefix, door)
    }

    #[cfg(not(target_os = "linux"))]
    fn address_for(
        prefix: &str,
        door: Doorbell,
    ) -> io::Result<SocketAddr> {
        SocketAddr::from_pathname(Self::path_for(prefix, door))
    }

    /// Ring a lane's bell over the socket connected to it, connecting first
    /// when there is none, when the lane has changed hands since (`epoch`),
    /// or when the send says the far side is gone.
    pub(crate) fn ring_lane(
        &self,
        lane: usize,
        epoch: u64,
    ) -> io::Result<()> {
        let Some(kept) = self.lanes.get(lane) else {
            return self.ring(Doorbell::Lane(lane));
        };
        let mut kept = kept.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(connected) = kept.as_ref()
            && connected.epoch == epoch
        {
            match crate::probe::ring(|| connected.socket.send(&[1])) {
                Ok(_) => return Ok(()),
                // A full queue is not a lost bell: the far side has not read
                // the ones it has.
                Err(error) if queue_full(&error) => return Err(error),
                // Closed or replaced: find the lane's bell again.
                Err(_) => {}
            }
        }
        *kept = None;
        let socket = UnixDatagram::unbound()?;
        socket.set_nonblocking(true)?;
        socket.connect_addr(&*self.door_address(Doorbell::Lane(lane))?)?;
        crate::probe::ring(|| socket.send(&[1]))?;
        *kept = Some(Connected { epoch, socket });
        Ok(())
    }

    pub(crate) fn ring(
        &self,
        door: Doorbell,
    ) -> io::Result<()> {
        let address = self.door_address(door)?;
        crate::probe::ring(|| self.sender.send_to_addr(&[1], &address))
            .map(|_| ())
    }

    /// Doors a bell of `lanes` lanes can ring: each lane, each watcher slot,
    /// two producer registrations per lane, and the adopter.
    fn door_count(lanes: usize) -> usize {
        if lanes == 0 { 0 } else { 4 * lanes + 1 }
    }

    fn door_index(
        &self,
        door: Doorbell,
    ) -> Option<usize> {
        let lanes = self.lanes.len();
        let index = match door {
            Doorbell::Lane(lane) if lane < lanes => lane,
            Doorbell::Watcher(slot) if slot < lanes => lanes + slot,
            Doorbell::Producer(index) if index < 2 * lanes => 2 * lanes + index,
            Doorbell::Adopter => 4 * lanes,
            _ => return None,
        };
        (index < self.addresses.len()).then_some(index)
    }

    /// The door's address, built once and kept. A name the platform refuses
    /// is not kept, so it is refused again on the next ring.
    fn door_address(
        &self,
        door: Doorbell,
    ) -> io::Result<Cow<'_, SocketAddr>> {
        let Some(kept) =
            self.door_index(door).and_then(|index| self.addresses.get(index))
        else {
            return self.address(door).map(Cow::Owned);
        };
        if let Some(address) = kept.get() {
            return Ok(Cow::Borrowed(address));
        }
        let address = self.address(door)?;
        Ok(Cow::Borrowed(kept.get_or_init(|| address)))
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn bind(
        &self,
        door: Doorbell,
    ) -> io::Result<UnixDatagram> {
        if let Some(access) = &self.group {
            return access.bind_bell(&self.group_path(door).unwrap());
        }
        #[cfg(not(target_os = "linux"))]
        {
            use std::os::unix::fs::DirBuilderExt;
            let path = self.path(door);
            if let Some(directory) = path.parent() {
                match std::fs::DirBuilder::new().mode(0o700).create(directory) {
                    Err(error)
                        if error.kind() != io::ErrorKind::AlreadyExists =>
                    {
                        return Err(error);
                    }
                    _ => {}
                }
            }
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        let socket = UnixDatagram::bind_addr(&self.address(door)?)?;
        socket.set_nonblocking(true)?;
        Ok(socket)
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn unbind(
        &self,
        door: Doorbell,
    ) {
        if let Some(path) = self.group_path(door) {
            let _ = std::fs::remove_file(path);
            return;
        }
        #[cfg(not(target_os = "linux"))]
        let _ = std::fs::remove_file(self.path(door));
        #[cfg(target_os = "linux")]
        let _ = door;
    }
}
