//! Named descriptor wake for an inbox lane or a directory watcher.
//!
//! Frames remain in shared memory. The datagram contains one meaningless
//! byte and exists only so an async runtime can sleep on a descriptor instead
//! of waking a dedicated thread that then wakes the runtime.

use std::io;
use std::os::unix::net::{SocketAddr, UnixDatagram};

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
                match socket.try_recv(&mut scratch) {
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

/// Who a datagram wakes. Lanes and watchers have separate addresses because a
/// named socket has exactly one binder.
#[derive(Clone, Copy)]
pub(crate) enum Doorbell {
    Lane(usize),
    Watcher(usize),
}

impl Doorbell {
    fn suffix(self) -> String {
        match self {
            Self::Lane(lane) => lane.to_string(),
            Self::Watcher(slot) => format!("w{slot}"),
        }
    }
}

pub(crate) struct Bell {
    prefix: String,
    sender: UnixDatagram,
    group: Option<orbit_core::shm::GroupAccess>,
}

impl Bell {
    #[cfg(test)]
    pub(crate) fn new(segment: &str) -> io::Result<Self> {
        Self::with_group(segment, None)
    }
    pub(crate) fn with_group(
        segment: &str,
        group: Option<orbit_core::shm::GroupAccess>,
    ) -> io::Result<Self> {
        let sender = UnixDatagram::unbound()?;
        sender.set_nonblocking(true)?;
        let prefix = if group.is_some() {
            segment.to_owned()
        } else {
            Self::prefix(segment)
        };
        Ok(Self { prefix, sender, group })
    }

    pub(crate) fn validate_group_address(
        access: &orbit_core::shm::GroupAccess,
        segment: &str,
        last_slot: usize,
    ) -> io::Result<()> {
        SocketAddr::from_pathname(
            access.bell_path(segment, &Doorbell::Watcher(last_slot).suffix()),
        )
        .map(|_| ())
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

    /// Check the longest lane or watcher address before creating either SHM
    /// resource. A watcher suffix is one byte longer than the lane suffix.
    pub(crate) fn validate_address(
        segment: &str,
        last_slot: usize,
    ) -> io::Result<()> {
        Self::address_for(&Self::prefix(segment), Doorbell::Watcher(last_slot))
            .map(|_| ())
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

    pub(crate) fn ring(
        &self,
        door: Doorbell,
    ) -> io::Result<()> {
        let address = self.address(door)?;
        self.sender.send_to_addr(&[1], &address).map(|_| ())
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
