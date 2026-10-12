#![cfg(unix)]

use orbit_arena::{ARENA_KIND, Arena, ArenaReader, Record};
use orbit_core::shm::{ShmRegion, fleet_lock_path, ring_segment_name};
use orbit_core::{Fleet, FleetObserver};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

struct Fixture {
    name: String,
    arena: Arena,
}

impl Fixture {
    fn new(readers: u16) -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let name = format!(
            "ar{:x}{:x}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let fleet = Arc::new(Fleet::join_shm(&name, readers).unwrap());
        Self { name, arena: Arena::new(fleet).unwrap() }
    }

    fn observer(&self) -> FleetObserver {
        FleetObserver::attach_existing(&self.name).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.arena.unlink().unwrap();
        let _ = std::fs::remove_file(fleet_lock_path(&self.name));
    }
}

#[test]
fn cache_explorer_pins_without_waiting_on_the_arena_writer_lock() {
    let fixture = Fixture::new(2);
    let address = fixture.arena.store(Record::body(b"original")).unwrap();
    // The diagnostic attach and read must succeed while a writer's lock is
    // already held. No writer lock is involved in row claim or value pinning.
    let (region, guard) = ShmRegion::open_or_create_locked(
        &ring_segment_name(&fixture.name, ARENA_KIND),
        64,
    )
    .unwrap();
    let reader = ArenaReader::attach_existing(&fixture.observer()).unwrap();
    let entry = reader.get_address(address).unwrap().unwrap();
    drop(guard);
    drop(region);
    fixture.arena.forget_address(address).unwrap();
    assert!(reader.get_address(address).unwrap().is_none());
    fixture.arena.store(Record::body(b"replacement")).unwrap();
    assert_eq!(entry.body(), b"original");
}

#[test]
fn cache_explorer_reader_capacity_does_not_reclaim_live_owners() {
    let fixture = Fixture::new(1);
    let address = fixture.arena.store(Record::body(b"still here")).unwrap();
    let error =
        ArenaReader::attach_existing(&fixture.observer()).err().unwrap();
    assert!(error.to_string().contains("reader capacity exhausted"));
    assert_eq!(
        fixture.arena.get_address(address).unwrap().body(),
        b"still here"
    );
}

#[test]
fn cache_explorer_reader_never_creates_an_absent_arena() {
    let fixture = Fixture::new(2);
    fixture.arena.unlink().unwrap();
    assert!(ArenaReader::attach_existing(&fixture.observer()).is_err());
    let name = ring_segment_name(&fixture.name, ARENA_KIND);
    assert_eq!(
        ShmRegion::validate_existing(&name, 64).unwrap(),
        orbit_core::shm::ShmValidation::Missing
    );
    // Drop also unlinks idempotently.
}

#[test]
fn cache_explorer_failed_attach_releases_its_fleet_hold() {
    let name = format!("af{:x}", std::process::id());
    let fleet = Arc::new(Fleet::join_shm(&name, 1).unwrap());
    let arena = Arena::new(fleet.clone()).unwrap();
    let observer = FleetObserver::attach_existing(&name).unwrap();
    assert!(ArenaReader::attach_existing(&observer).is_err());
    arena.unlink().unwrap();
    drop(arena);
    drop(fleet);
    let hold = orbit_core::shm::try_lock_fleet_exclusive(&name, observer.uid())
        .unwrap();
    assert!(hold.is_some(), "failed pin attachment leaked membership");
    drop(hold);
    std::fs::remove_file(fleet_lock_path(&name)).unwrap();
}
