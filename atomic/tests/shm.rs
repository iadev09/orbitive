//! The table lives in shared memory: another process opens the id and both
//! hold one value.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::process::Command;
use std::sync::Arc;

use orbit_atomic::{OrbitalId, Orbitals};
use orbit_core::{Fleet, NodeId};

const PEER_FLEET: &str = "ORBIT_ATOMIC_TEST_FLEET";
const PEER_ID: &str = "ORBIT_ATOMIC_TEST_ID";

fn orbitals(
    name: &str,
    node: u16,
) -> Orbitals {
    Orbitals::new(Arc::new(
        Fleet::join_shm_as(name, 2, NodeId::new(node)).unwrap(),
    ))
    .unwrap()
}

struct Cleanup(Orbitals);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = self.0.unlink();
    }
}

/// The other process. Does nothing unless the test below started it.
#[test]
fn peer_process() {
    let Ok(name) = std::env::var(PEER_FLEET) else { return };
    let id: OrbitalId = std::env::var(PEER_ID).unwrap().parse().unwrap();
    let shared = orbitals(&name, 1).open::<i64>(id).unwrap();
    assert_eq!(shared.fetch_add(2), 40);
}

#[test]
fn two_processes_hold_one_value() {
    let name = format!("oa{:x}", std::process::id());
    let owner = orbitals(&name, 0);
    let _cleanup = Cleanup(owner.clone());
    owner.reset_all().unwrap();
    let value = owner.allocate(40_i64).unwrap();

    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "peer_process"])
        .env(PEER_FLEET, &name)
        .env(PEER_ID, value.id().to_string())
        .status()
        .unwrap();
    assert!(status.success());
    // Already written, so this returns without parking.
    assert_eq!(value.wait(40).unwrap(), 42);
}

const PEER_NAMES: &str = "ORBIT_ATOMIC_TEST_NAMES";

/// The other process of the test below. It maps its own table and
/// directory, declares the name the first process installed, and writes.
#[test]
fn peer_declares() {
    let Ok(name) = std::env::var(PEER_NAMES) else { return };
    let orbitals = orbitals(&name, 1);
    let hits = orbitals.declare("hits", 0_i64).unwrap();
    assert_eq!(hits.fetch_add(2), 40, "the first declaration's value");
    let visitors = orbitals.declare_kept("visitors", 0_u64).unwrap();
    visitors.fetch_add(1);
}

#[test]
fn two_processes_declare_one_value() {
    let name = format!("on{:x}", std::process::id());
    let owner = orbitals(&name, 0);
    let _cleanup = Cleanup(owner.clone());
    owner.reset_all().unwrap();
    let hits = owner.declare("hits", 40_i64).unwrap();

    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "peer_declares"])
        .env(PEER_NAMES, &name)
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(hits.load(), 42);

    // The peer installed this one; it survives the owner's next boot.
    let visitors = owner.declare_kept("visitors", 0_u64).unwrap();
    assert_eq!(visitors.load(), 1);
    drop((hits, visitors));
    owner.reset_all().unwrap();
    assert_eq!(owner.declare_kept("visitors", 0_u64).unwrap().load(), 1);
    assert_eq!(owner.declare("hits", 0_i64).unwrap().load(), 0);
}
