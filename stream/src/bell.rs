//! A doorbell the async runtime answers itself.
//!
//! Experimental, behind `tokio-doorbell`. With the plain `tokio` feature a
//! doorbell is answered by one thread per process parked on the generation
//! word (`wake`): the writer wakes that thread, and the thread wakes the task.
//! Two wakes per crossing where a socket costs one, because the runtime's
//! reactor waits on descriptors and cannot park on a shared word.
//!
//! Here the process binds a Unix datagram socket named after its segment and
//! node, and a task on the runtime waits on it like any other socket. Before
//! it waits it sets [`ARMED`] in the node's `listening` word; a writer that
//! finds the bit set clears it and sends one byte to that name. The reactor
//! then wakes the task on a runtime worker, the task drains the pending bits,
//! and the tasks it wakes run there: one wake per crossing.
//!
//! The name is the only thing shared, so a process that joined on its own
//! reaches it as easily as a forked sibling — no descriptor is inherited or
//! passed. On Linux the name is abstract and leaves nothing behind; elsewhere
//! it is a socket file under `/tmp/orbit-bell-<uid>/`, removed on drop.
//!
//! The writer's half is compiled in every build: a process without the
//! feature must still ring a node whose listener has it.

use std::io;
use std::os::unix::net::{SocketAddr, UnixDatagram};
#[cfg(feature = "tokio-doorbell")]
use std::sync::atomic::Ordering;
#[cfg(feature = "tokio-doorbell")]
use std::sync::{Arc, Weak};

#[cfg(feature = "tokio-doorbell")]
use crate::layout::Doorbell;
#[cfg(feature = "tokio-doorbell")]
use crate::table::Table;
#[cfg(feature = "tokio-doorbell")]
use crate::wake::Doorstep;

/// Set in a doorbell's `listening` word while a socket listener waits. The
/// rest of the word counts the waiters parked on the generation.
pub(crate) const ARMED: u32 = 1 << 31;

/// Where a table's nodes are rung, and the socket it rings them from.
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
        Ok(Self { prefix: format!("orbit-bell-{uid}/{segment}"), sender })
    }

    #[cfg(target_os = "linux")]
    fn address(
        &self,
        node: usize
    ) -> io::Result<SocketAddr> {
        use std::os::linux::net::SocketAddrExt;
        SocketAddr::from_abstract_name(format!("{}.{node}", self.prefix))
    }

    #[cfg(not(target_os = "linux"))]
    fn path(
        &self,
        node: usize
    ) -> std::path::PathBuf {
        std::path::PathBuf::from(format!("/tmp/{}.{node}", self.prefix))
    }

    #[cfg(not(target_os = "linux"))]
    fn address(
        &self,
        node: usize
    ) -> io::Result<SocketAddr> {
        SocketAddr::from_pathname(self.path(node))
    }

    /// Ring `node`'s listener. A full queue already holds a wake for it, and
    /// a listener that is gone has nothing to be told, so neither is an
    /// error here.
    pub(crate) fn ring(
        &self,
        node: usize
    ) {
        if let Ok(address) = self.address(node) {
            let _ = self.sender.send_to_addr(&[1], &address);
        }
    }

    /// Bind this node's name. A file left by an earlier process on this node
    /// is removed first: one live process per node is the fleet's rule.
    #[cfg(feature = "tokio-doorbell")]
    fn bind(
        &self,
        node: usize
    ) -> io::Result<UnixDatagram> {
        #[cfg(not(target_os = "linux"))]
        {
            use std::os::unix::fs::DirBuilderExt;
            let path = self.path(node);
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
        let socket = UnixDatagram::bind_addr(&self.address(node)?)?;
        socket.set_nonblocking(true)?;
        Ok(socket)
    }

    #[cfg(feature = "tokio-doorbell")]
    fn unbind(
        &self,
        node: usize
    ) {
        #[cfg(not(target_os = "linux"))]
        let _ = std::fs::remove_file(self.path(node));
        #[cfg(target_os = "linux")]
        let _ = node;
    }
}

/// The task that answers this process's doorbell on the runtime.
#[cfg(feature = "tokio-doorbell")]
pub(crate) struct ReactorDriver {
    task: tokio::task::JoinHandle<()>,
    node: usize
}

#[cfg(feature = "tokio-doorbell")]
impl ReactorDriver {
    pub(crate) fn start(
        table: &Arc<Table>,
        bell: &Bell,
        runtime: &tokio::runtime::Handle
    ) -> io::Result<Self> {
        let node = usize::from(table.node());
        let socket = {
            let _entered = runtime.enter();
            tokio::net::UnixDatagram::from_std(bell.bind(node)?)?
        };
        table.own_doorbell().incarnation.store(table.incarnation(), Ordering::SeqCst);
        let weak = Arc::downgrade(table);
        let task = runtime.spawn(answer(weak, socket));
        Ok(Self { task, node })
    }

    pub(crate) fn stop(
        self,
        doorbell: &Doorbell,
        bell: &Bell
    ) {
        self.task.abort();
        doorbell.listening.fetch_and(!ARMED, Ordering::SeqCst);
        bell.unbind(self.node);
    }
}

/// Drain, arm, check, wait — the futex idiom with a socket as the park.
///
/// A writer bumps the generation and only then reads `listening`; this task
/// sets [`ARMED`] and only then reads the generation. Both sequentially
/// consistent, so either the writer sees the bit and sends, or this task
/// sees the bump and drains again: a wake is never lost between the two.
#[cfg(feature = "tokio-doorbell")]
async fn answer(
    table: Weak<Table>,
    socket: tokio::net::UnixDatagram
) {
    let mut scratch = [0u8; 64];
    loop {
        {
            // Never held across the wait: the table owns this task and
            // aborts it on drop.
            let Some(table) = table.upgrade() else { break };
            let doorbell = table.own_doorbell();
            loop {
                let seen = doorbell.generation.load(Ordering::SeqCst);
                table.drain();
                doorbell.listening.fetch_or(ARMED, Ordering::SeqCst);
                if doorbell.generation.load(Ordering::SeqCst) == seen {
                    break;
                }
            }
        }
        if socket.readable().await.is_err() {
            break;
        }
        while socket.try_recv(&mut scratch).is_ok() {}
    }
}
