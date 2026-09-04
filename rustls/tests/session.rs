//! Cross-process guarantees for Orbit-backed rustls sessions.

#![cfg(all(unix, feature = "rustls_0_23"))]

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, fork};
use orbit_core::{Fleet, NodeId};
use orbit_rustls::{FleetServerSessions, SessionDomain};
use rustls_0_23::server::StoresServerSessions;

fn fresh_name() -> &'static str {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFF;
    Box::leak(format!("t{pid:04x}{nonce:08x}{counter:02x}").into_boxed_str())
}

fn wait_child(pid: nix::unistd::Pid) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => return code,
            Ok(WaitStatus::Signaled(_, signal, _)) => panic!("child killed by {signal:?}"),
            Ok(WaitStatus::StillAlive) => {
                assert!(std::time::Instant::now() < deadline, "child timed out");
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(other) => panic!("unexpected child status: {other:?}"),
            Err(error) => panic!("waitpid failed: {error}"),
        }
    }
}

#[test]
fn stateful_tickets_have_one_cross_process_take_winner() {
    let name = fresh_name();
    let parent_fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("parent fleet"));
    let parent_sessions = FleetServerSessions::open(parent_fleet).expect("parent sessions");
    parent_sessions.reset().expect("reset session table");
    let parent_storage =
        parent_sessions.storage(SessionDomain::new("quic-public").expect("domain"));

    let (mut parent_pipe, mut child_pipe) = UnixStream::pair().expect("socket pair");
    match unsafe { fork() }.expect("fork") {
        ForkResult::Child => {
            drop(parent_pipe);
            let fleet = match Fleet::join_shm_as(name, 2, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(11),
            };
            let sessions = match FleetServerSessions::open(fleet) {
                Ok(sessions) => sessions,
                Err(_) => std::process::exit(12),
            };
            let storage = sessions.storage(SessionDomain::new("quic-public").expect("domain"));
            for ticket in 0_u64..128 {
                if child_pipe.write_all(&[1]).is_err() {
                    std::process::exit(13);
                }
                let mut start = [0; 1];
                if child_pipe.read_exact(&mut start).is_err() {
                    std::process::exit(14);
                }
                let won = storage.take(&ticket.to_le_bytes()).is_some();
                if child_pipe.write_all(&[u8::from(won)]).is_err() {
                    std::process::exit(15);
                }
            }
            std::process::exit(0);
        }
        ForkResult::Parent { child } => {
            drop(child_pipe);
            for ticket in 0_u64..128 {
                let key = ticket.to_le_bytes();
                let mut ready = [0; 1];
                parent_pipe.read_exact(&mut ready).expect("child ready");
                assert!(parent_storage.put(key.to_vec(), b"secret".to_vec()));
                parent_pipe.write_all(&[1]).expect("start child");
                let parent_won = parent_storage.take(&key).is_some();
                let mut child_result = [0; 1];
                parent_pipe
                    .read_exact(&mut child_result)
                    .expect("child result");

                assert_eq!(
                    usize::from(parent_won) + usize::from(child_result[0]),
                    1,
                    "exactly one process must consume ticket {ticket}"
                );
                assert_eq!(parent_storage.take(&key), None);
            }
            assert_eq!(wait_child(child), 0);
            parent_sessions.unlink().expect("unlink session table");
        }
    }
}
