use super::*;
use orbit_core::Fleet;
use orbit_core::shm::{fleet_lock_path, ring_segment_name};

#[test]
fn cache_explorer_uses_header_geometry_and_releases_invalid_span_pins() {
    let name = format!("ag{:x}", std::process::id());
    let fleet = Fleet::join_shm(&name, 1).unwrap();
    let segment = ring_segment_name(&name, ARENA_KIND);
    let slots = 8;
    let bytes = 256;
    let rows_offset = size_of::<Header>() + slots * size_of::<Slot>();
    let ring_offset = rows_offset + 64 + slots * size_of::<AtomicU32>();
    let (region, guard) =
        ShmRegion::open_or_create_locked(&segment, ring_offset + bytes)
            .unwrap();
    assert!(region.created());
    let mut header = Header::new(1);
    header.slots = slots as u32;
    header.bytes = bytes as u64;
    // SAFETY: isolated fresh fixture, sized for this canonical header/slot ABI.
    unsafe {
        std::ptr::write(region.as_ptr().cast::<Header>(), header);
        std::ptr::write_bytes(
            region.as_ptr().add(size_of::<Header>()),
            0,
            ring_offset + bytes - size_of::<Header>(),
        );
        std::ptr::copy_nonoverlapping(
            b"body".as_ptr(),
            region.as_ptr().add(ring_offset + 3),
            4,
        );
    }
    // SAFETY: the fixture index and row are within the allocated mapping.
    let slot = unsafe {
        &*region
            .as_ptr()
            .add(size_of::<Header>() + 7 * size_of::<Slot>())
            .cast::<Slot>()
    };
    let pins = unsafe {
        &*region
            .as_ptr()
            .add(rows_offset + 64 + 7 * size_of::<AtomicU32>())
            .cast::<AtomicU32>()
    };
    slot.body_offset.store(3, Ordering::Relaxed);
    slot.body_len.store(4, Ordering::Relaxed);
    slot.generation.store(1, Ordering::Relaxed);
    slot.state.store(SLOT_ADDRESSED, Ordering::SeqCst);
    let reader = ArenaReader::attach_existing(
        &FleetObserver::attach_existing(&name).unwrap(),
    )
    .unwrap();
    let address = Address { slot: 7, generation: 1 };
    let entry = reader.get_address(address).unwrap().unwrap();
    assert_eq!(entry.body(), b"body");
    assert_eq!(pins.load(Ordering::SeqCst), 1);
    drop(entry);
    assert_eq!(pins.load(Ordering::SeqCst), 0);
    slot.body_len.store(bytes as u64, Ordering::Relaxed);
    assert!(reader.get_address(address).is_err());
    assert_eq!(pins.load(Ordering::SeqCst), 0);
    assert!(
        reader
            .get_address(Address { slot: 8, generation: 1 })
            .unwrap()
            .is_none()
    );
    drop(reader);
    drop(guard);
    region.unlink().unwrap();
    drop(region);
    drop(fleet);
    std::fs::remove_file(fleet_lock_path(&name)).unwrap();
}
