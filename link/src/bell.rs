//! Named descriptor wake for an inbox lane or a directory watcher.
//!
//! Frames remain in shared memory. The datagram contains one meaningless
//! byte and exists only so an async runtime can sleep on a descriptor instead
//! of waking a dedicated thread that then wakes the runtime.

use std::io;
use std::os::unix::net::{SocketAddr, UnixDatagram};

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
}

impl Bell {
    pub(crate) fn new(segment: &str) -> io::Result<Self> {
        let sender = UnixDatagram::unbound()?;
        sender.set_nonblocking(true)?;
        Ok(Self { prefix: Self::prefix(segment), sender })
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
        #[cfg(not(target_os = "linux"))]
        let _ = std::fs::remove_file(self.path(door));
        #[cfg(target_os = "linux")]
        let _ = door;
    }
}
