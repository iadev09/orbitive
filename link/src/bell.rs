//! Named descriptor wake for an inbox lane.
//!
//! Frames remain in shared memory. The datagram contains one meaningless
//! byte and exists only so an async runtime can sleep on a descriptor instead
//! of waking a dedicated thread that then wakes the runtime.

use std::io;
use std::os::unix::net::{SocketAddr, UnixDatagram};

pub(crate) struct Bell {
    prefix: String,
    sender: UnixDatagram
}

impl Bell {
    pub(crate) fn new(segment: &str) -> io::Result<Self> {
        let sender = UnixDatagram::unbound()?;
        sender.set_nonblocking(true)?;
        // SAFETY: a plain syscall with no arguments.
        let uid = unsafe { libc::getuid() };
        let segment = segment.trim_start_matches('/');
        Ok(Self { prefix: format!("orbit-link-{uid}/{segment}"), sender })
    }

    #[cfg(target_os = "linux")]
    fn address(
        &self,
        lane: usize
    ) -> io::Result<SocketAddr> {
        use std::os::linux::net::SocketAddrExt;
        SocketAddr::from_abstract_name(format!("{}.{lane}", self.prefix))
    }

    #[cfg(not(target_os = "linux"))]
    fn path(
        &self,
        lane: usize
    ) -> std::path::PathBuf {
        std::path::PathBuf::from(format!("/tmp/{}.{lane}", self.prefix))
    }

    #[cfg(not(target_os = "linux"))]
    fn address(
        &self,
        lane: usize
    ) -> io::Result<SocketAddr> {
        SocketAddr::from_pathname(self.path(lane))
    }

    pub(crate) fn ring(
        &self,
        lane: usize
    ) {
        if let Ok(address) = self.address(lane) {
            let _ = self.sender.send_to_addr(&[1], &address);
        }
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn bind(
        &self,
        lane: usize
    ) -> io::Result<UnixDatagram> {
        #[cfg(not(target_os = "linux"))]
        {
            use std::os::unix::fs::DirBuilderExt;
            let path = self.path(lane);
            if let Some(directory) = path.parent() {
                match std::fs::DirBuilder::new().mode(0o700).create(directory) {
                    Err(error) if error.kind() != io::ErrorKind::AlreadyExists => {
                        return Err(error);
                    }
                    _ => {}
                }
            }
            let _ = std::fs::remove_file(&path);
        }
        let socket = UnixDatagram::bind_addr(&self.address(lane)?)?;
        socket.set_nonblocking(true)?;
        Ok(socket)
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn unbind(
        &self,
        lane: usize
    ) {
        #[cfg(not(target_os = "linux"))]
        let _ = std::fs::remove_file(self.path(lane));
        #[cfg(target_os = "linux")]
        let _ = lane;
    }
}
