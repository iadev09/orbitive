//! Kernel-released owner holds, exercised only on test-owned SHM.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use orbit_cell::{
    CELL_CAPACITY, CELL_TEXT_CAPACITY, CellId, Cells, Error, TextId,
};
use orbit_core::{Fleet, NodeId};

const CHILD_FLEET: &str = "ORBIT_CELL_TEST_FLEET";

fn cells(
    name: &str,
    node: u16,
) -> Cells {
    Cells::new(Arc::new(
        Fleet::join_shm_as(name, 2, NodeId::new(node)).unwrap(),
    ))
    .unwrap()
}

struct Peer(Child);

impl Peer {
    fn start(
        name: &str,
        ids: Option<(CellId, TextId)>,
    ) -> (Self, CellId, TextId) {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "cell_owner_child", "--nocapture"])
            .env(CHILD_FLEET, name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        if let Some((scalar, text)) = ids {
            command
                .env("ORBIT_CELL_TEST_SCALAR", scalar.to_string())
                .env("ORBIT_CELL_TEST_TEXT", text.to_string());
        }
        let mut peer = Self(command.spawn().unwrap());
        let mut output = BufReader::new(peer.0.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            assert_ne!(
                output.read_line(&mut line).unwrap(),
                0,
                "child exited before ready: {line}"
            );
            if let Some((_, ids)) = line.split_once("CELL_READY ") {
                let (scalar, text) = ids.trim().split_once(' ').unwrap();
                let ids = (scalar.parse().unwrap(), text.parse().unwrap());
                return (peer, ids.0, ids.1);
            }
            line.clear();
        }
    }

    fn kill_and_reap(&mut self) {
        self.0.kill().unwrap();
        assert!(!self.0.wait().unwrap().success());
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn cell_owner_child() {
    let Ok(name) = std::env::var(CHILD_FLEET) else { return };
    let cells = cells(&name, 1);
    let (scalar, text) = if let Ok(id) = std::env::var("ORBIT_CELL_TEST_SCALAR")
    {
        (
            cells.open::<u64>(id.parse().unwrap()).unwrap(),
            cells
                .open_text(
                    std::env::var("ORBIT_CELL_TEST_TEXT")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .unwrap(),
        )
    } else {
        (cells.allocate(7_u64).unwrap(), cells.allocate_text("old").unwrap())
    };
    println!("CELL_READY {} {}", scalar.id(), text.id());
    std::io::stdout().flush().unwrap();
    let mut byte = [0];
    // Parent explicitly kills this isolated helper. No Drop cleanup runs.
    std::io::stdin().read_exact(&mut byte).unwrap();
    drop((scalar, text));
}

#[test]
fn cell_reuse_requires_a_dead_owner_not_merely_a_released_id() {
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT: AtomicU32 = AtomicU32::new(0);
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let sequence = u16::try_from(NEXT.fetch_add(1, Ordering::Relaxed)).unwrap();
    let mut value = (u64::from(std::process::id()) << 16) | u64::from(sequence);
    let name: String = (0..8)
        .map(|_| {
            let byte = ALPHABET[(value & 63) as usize];
            value >>= 6;
            byte as char
        })
        .collect();
    let cells = cells(&name, 0);
    struct Cleanup(Cells);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.unlink();
        }
    }
    let _cleanup = Cleanup(cells.clone());
    let (mut child, scalar_id, text_id) = Peer::start(&name, None);
    let _scalars: Vec<_> =
        (1..CELL_CAPACITY).map(|_| cells.allocate(0_u64).unwrap()).collect();
    let _texts: Vec<_> = (1..CELL_TEXT_CAPACITY)
        .map(|_| cells.allocate_text("held").unwrap())
        .collect();
    cells.release(scalar_id).unwrap();
    cells.release_text(text_id).unwrap();
    assert!(matches!(cells.allocate(42_u64), Err(Error::Full { .. })));
    assert!(matches!(cells.allocate_text("new"), Err(Error::Full { .. })));

    child.kill_and_reap();
    let scalar = cells.allocate(42_u64).unwrap();
    let text = cells.allocate_text("new").unwrap();
    assert_eq!(scalar.id().index(), scalar_id.index());
    assert_eq!(text.id().index(), text_id.index());
    assert_ne!(scalar.id().generation(), scalar_id.generation());
    assert_ne!(text.id().generation(), text_id.generation());
    assert_eq!(scalar.load().unwrap(), 42);
    assert_eq!(text.load().unwrap(), "new");

    // Reusing the dead owner's registration must not retain its old pins or
    // clear this new live owner's pins when allocation scans under pressure.
    let (mut next, _, _) = Peer::start(&name, Some((scalar.id(), text.id())));
    cells.release(scalar.id()).unwrap();
    cells.release_text(text.id()).unwrap();
    drop((scalar, text));
    assert!(matches!(cells.allocate(99_u64), Err(Error::Full { .. })));
    assert!(matches!(cells.allocate_text("late"), Err(Error::Full { .. })));
    next.kill_and_reap();
    assert_eq!(cells.allocate(99_u64).unwrap().load().unwrap(), 99);
    assert_eq!(cells.allocate_text("late").unwrap().load().unwrap(), "late");
}
