//! Cold owner-incarnation metadata, separate from scalar handle ownership.

use std::sync::atomic::{AtomicU64, Ordering};

use orbit_core::shm::ShmRegion;

use super::{CELL_TEXT_CAPACITY, TextSlot, TextStateHeader};
use crate::{Error, Result};

#[repr(C, align(64))]
pub(super) struct Identity {
    token: AtomicU64,
}

pub(super) fn offset() -> usize {
    size_of::<TextStateHeader>() + CELL_TEXT_CAPACITY * size_of::<TextSlot>()
}

pub(super) fn pins_offset(owners: usize) -> Result<usize> {
    owners
        .checked_mul(size_of::<Identity>())
        .and_then(|size| offset().checked_add(size))
        .filter(|size| *size <= isize::MAX as usize)
        .ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "text owner geometry is too large",
            ))
        })
}

pub(super) fn identity(
    region: &ShmRegion,
    owner: usize,
) -> &Identity {
    // SAFETY: attach validates the complete owner geometry before calling us.
    unsafe {
        &*region.as_ptr().add(offset() + owner * size_of::<Identity>()).cast()
    }
}

pub(super) fn slots(region: &ShmRegion) -> &[TextSlot] {
    // SAFETY: the validated header fixes both slot count and layout.
    unsafe {
        std::slice::from_raw_parts(
            region.as_ptr().add(size_of::<TextStateHeader>()).cast(),
            CELL_TEXT_CAPACITY,
        )
    }
}

/// The caller retains both the structural lock and the exact owner row hold.
pub(super) fn recover(
    region: &ShmRegion,
    owner: usize,
) -> Result<usize> {
    let token = identity(region, owner).token.load(Ordering::Acquire);
    if token == 0 {
        return Ok(0);
    }
    if token as u32 != (owner as u32) + 1 {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid text writer owner identity",
        )));
    }
    Ok(slots(region).iter().filter(|slot| slot.recover(token)).count())
}

/// Reuse is fenced by the row hold; old descriptors must be gone BEFORE the
/// incarnation changes. A helper dying anywhere leaves the old token intact.
pub(super) fn claim(
    region: &ShmRegion,
    owner: usize,
) -> Result<()> {
    recover(region, owner)?;
    let identity = identity(region, owner);
    let old = identity.token.load(Ordering::Acquire);
    let incarnation = (old >> 32)
        .checked_add(1)
        .filter(|incarnation| *incarnation <= u64::from(u32::MAX))
        .ok_or(Error::OwnerExhausted)?;
    identity
        .token
        .store((incarnation << 32) | ((owner as u64) + 1), Ordering::Release);
    Ok(())
}

pub(super) fn token(
    region: &ShmRegion,
    owner: usize,
) -> u64 {
    identity(region, owner).token.load(Ordering::Acquire)
}

#[cfg(test)]
pub(super) fn exhaust(
    region: &ShmRegion,
    owner: usize,
) {
    identity(region, owner).token.store(
        (u64::from(u32::MAX) << 32) | (owner as u64 + 1),
        Ordering::Release,
    );
}
