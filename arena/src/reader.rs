//! Diagnostic reads through the existing arena pin protocol, never a writer.

use std::mem::{ManuallyDrop, size_of};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use orbit_core::FleetObserver;
use orbit_core::shm::{
    FleetMembership, LaneHold, ShmAccessPolicy, ShmRegion, hold_existing_fleet,
    ring_segment_name_for_uid, try_hold_lane,
};

use super::{
    ARENA_KIND, Address, Error, Header, MAGIC, Result, SLOT_ADDRESSED, Slot,
    VERSION,
};

/// An existing arena's reader capability: no store, reset or unlink methods.
///
/// Uses the production reader-pin protocol and consumes one existing owner
/// row. A full owner table is an error, not permission to reclaim a live row.
/// Reads never acquire the arena writer lock. Geometry comes from the existing
/// header, so a diagnostic tool need not share the producer's capacity profile.
pub struct ArenaReader {
    inner: Arc<ReaderInner>,
}

struct ReaderInner {
    region: ShmRegion,
    slots: usize,
    rows_offset: usize,
    row_stride: usize,
    ring_offset: usize,
    bytes: usize,
    reader: usize,
    pid: u32,
    claimed: bool,
    hold: Option<ManuallyDrop<LaneHold>>,
    membership: ManuallyDrop<FleetMembership>,
}

impl ReaderInner {
    fn active(
        &self,
        reader: usize,
    ) -> &AtomicU32 {
        // SAFETY: checked geometry bounds every row, aligned for AtomicU32.
        unsafe {
            &*self
                .region
                .as_ptr()
                .add(self.rows_offset + reader * self.row_stride)
                .cast()
        }
    }

    fn pin(
        &self,
        slot: usize,
    ) -> &AtomicU32 {
        // SAFETY: this owner's row contains the validated number of counters.
        unsafe {
            &*self
                .region
                .as_ptr()
                .add(
                    self.rows_offset
                        + self.reader * self.row_stride
                        + 64
                        + slot * size_of::<AtomicU32>(),
                )
                .cast()
        }
    }

    fn slot(
        &self,
        slot: usize,
    ) -> &Slot {
        // SAFETY: validated index geometry uses the canonical Slot layout.
        unsafe {
            &*self
                .region
                .as_ptr()
                .add(size_of::<Header>() + slot * size_of::<Slot>())
                .cast()
        }
    }
}

impl Drop for ReaderInner {
    fn drop(&mut self) {
        if self.pid == std::process::id() {
            // Entries retain this owner until all their pins have been dropped.
            if self.claimed {
                self.active(self.reader).store(0, Ordering::Release);
            }
            // SAFETY: the attaching process releases each hold exactly once.
            // A fork child must not unlock the parent's shared file description.
            unsafe {
                if let Some(hold) = &mut self.hold {
                    ManuallyDrop::drop(hold);
                }
                ManuallyDrop::drop(&mut self.membership);
            }
        }
    }
}

impl ArenaReader {
    pub fn attach_existing(observer: &FleetObserver) -> Result<Self> {
        if observer.uid()
            != FleetObserver::attach_existing(observer.name())?.uid()
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "arena pinning requires the owner's uid; run the reader as that user",
            )));
        }
        let membership = hold_existing_fleet(observer.name())?;
        let name = ring_segment_name_for_uid(
            observer.name(),
            ARENA_KIND,
            observer.uid(),
        );
        let region = ShmRegion::open_existing_read_write(
            &name,
            size_of::<Header>(),
            observer.uid(),
            ShmAccessPolicy::OwnerOnly,
        )?;
        // SAFETY: existing mapping is at least a Header and mmap is aligned.
        let header = unsafe { &*region.as_ptr().cast::<Header>() };
        if header.magic != MAGIC
            || header.version != VERSION
            || usize::from(header.header_size) != size_of::<Header>()
            || header.slot_size as usize != size_of::<Slot>()
        {
            return Err(invalid("incompatible arena header layout"));
        }
        let slots = header.slots as usize;
        let readers = u16::try_from(header.readers)
            .ok()
            .filter(|count| *count > 0)
            .ok_or_else(|| invalid("invalid arena reader count"))?;
        let bytes = usize::try_from(header.bytes)
            .ok()
            .filter(|bytes| *bytes > 0)
            .ok_or_else(|| invalid("invalid arena byte capacity"))?;
        if !slots.is_power_of_two() {
            return Err(invalid("invalid arena slot count"));
        }
        let rows_offset = slots
            .checked_mul(size_of::<Slot>())
            .and_then(|n| n.checked_add(size_of::<Header>()));
        let row_stride = slots
            .checked_mul(size_of::<AtomicU32>())
            .and_then(|n| n.checked_add(64));
        let (rows_offset, row_stride) = rows_offset
            .zip(row_stride)
            .ok_or_else(|| invalid("arena geometry overflow"))?;
        let ring_offset = row_stride
            .checked_mul(readers as usize)
            .and_then(|n| n.checked_add(rows_offset))
            .ok_or_else(|| invalid("arena geometry overflow"))?;
        if ring_offset.checked_add(bytes).is_none_or(|len| len > region.len()) {
            return Err(invalid("arena geometry exceeds the mapped segment"));
        }
        let mut inner = ReaderInner {
            region,
            slots,
            rows_offset,
            row_stride,
            ring_offset,
            bytes,
            reader: 0,
            pid: std::process::id(),
            claimed: false,
            hold: None,
            membership: ManuallyDrop::new(membership),
        };
        for reader in 0..readers as usize {
            if let Some(hold) = try_hold_lane(&name, reader)? {
                inner.reader = reader;
                inner.hold = Some(ManuallyDrop::new(hold));
                // The owner hold excludes a live reader and writer-side reclaim.
                // No reads are exposed until every stale pin has been cleared.
                for slot in 0..slots {
                    inner.pin(slot).swap(0, Ordering::SeqCst);
                }
                inner.active(reader).store(1, Ordering::Release);
                inner.claimed = true;
                return Ok(Self { inner: Arc::new(inner) });
            }
        }
        Err(Error::Io(std::io::Error::other("arena reader capacity exhausted")))
    }

    /// Pin only the exact generation, or report a miss after eviction/reuse.
    pub fn get_address(
        &self,
        address: Address,
    ) -> Result<Option<ReaderEntry>> {
        if self.inner.pid != std::process::id() {
            return Err(invalid("arena reader belongs to another process"));
        }
        let slot = address.slot as usize;
        if slot >= self.inner.slots {
            return Ok(None);
        }
        self.inner
            .pin(slot)
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |pins| {
                pins.checked_add(1)
            })
            .map_err(|_| invalid("arena reader pin count exhausted"))?;
        let mut entry =
            ReaderEntry { inner: Arc::clone(&self.inner), slot, body: (0, 0) };
        let record = self.inner.slot(slot);
        // SeqCst matches production readers: either eviction sees the pin or
        // this reader sees the retired state before borrowing any record bytes.
        if record.state.load(Ordering::SeqCst) != SLOT_ADDRESSED
            || record.generation.load(Ordering::SeqCst) != address.generation
        {
            return Ok(None);
        }
        let offset = record.body_offset.load(Ordering::Relaxed);
        let len = record.body_len.load(Ordering::Relaxed);
        if offset
            .checked_add(len)
            .is_none_or(|end| end > self.inner.bytes as u64)
        {
            return Err(invalid(
                "arena record span is outside the byte region",
            ));
        }
        entry.body = (offset as usize, len as usize);
        Ok(Some(entry))
    }
}

/// A pinned, immutable diagnostic body. Drop releases its existing reader pin.
pub struct ReaderEntry {
    inner: Arc<ReaderInner>,
    slot: usize,
    body: (usize, usize),
}

impl ReaderEntry {
    pub fn body(&self) -> &[u8] {
        assert_eq!(
            self.inner.pid,
            std::process::id(),
            "arena entry belongs to another process"
        );
        // SAFETY: the validated span is immutable while this entry's pin lives.
        unsafe {
            std::slice::from_raw_parts(
                self.inner
                    .region
                    .as_ptr()
                    .add(self.inner.ring_offset + self.body.0),
                self.body.1,
            )
        }
    }
}

impl Drop for ReaderEntry {
    fn drop(&mut self) {
        if self.inner.pid == std::process::id() {
            self.inner.pin(self.slot).fetch_sub(1, Ordering::SeqCst);
        }
    }
}

fn invalid(message: &str) -> Error {
    Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, message))
}

#[cfg(test)]
#[path = "../tests/reader_geometry/mod.rs"]
mod tests;
