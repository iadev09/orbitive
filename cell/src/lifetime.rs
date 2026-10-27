//! Cold-path handle ownership. Value operations never touch these counters.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

#[cfg(unix)]
use orbit_core::shm::{LaneHold, ShmRegion, try_hold_lane};
#[cfg(unix)]
use std::mem::ManuallyDrop;

use crate::{Error, Result};

#[derive(Clone)]
pub(crate) enum Owners {
    Memory(Arc<[AtomicU32]>),
    #[cfg(unix)]
    Shared(Arc<Shared>),
}

impl Owners {
    pub(crate) fn memory(capacity: usize) -> Self {
        Self::Memory((0..capacity).map(|_| AtomicU32::new(0)).collect())
    }

    /// Called under the table's structural lock, before publishing a handle.
    pub(crate) fn lease(
        &self,
        index: usize,
    ) -> Result<Arc<Lease>> {
        self.check_process()?;
        self.counter(index)
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .map_err(|_| Error::Overflow)?;
        Ok(Arc::new(Lease { owners: self.clone(), index }))
    }

    /// Called under the structural lock. A last Drop releases preceding IO.
    pub(crate) fn pinned(
        &self,
        index: usize,
    ) -> bool {
        match self {
            Self::Memory(counts) => counts[index].load(Ordering::Acquire) != 0,
            #[cfg(unix)]
            Self::Shared(shared) => (0..shared.owners).any(|owner| {
                shared.counts(owner)[index].load(Ordering::Acquire) != 0
            }),
        }
    }

    pub(crate) fn check_process(&self) -> Result<()> {
        #[cfg(unix)]
        if let Self::Shared(shared) = self
            && shared.pid != std::process::id()
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "open cell tables again after fork; inherited handles are not usable",
            )));
        }
        Ok(())
    }

    /// Only allocation pressure needs a death scan. No clocks or PID probes.
    /// The caller holds the structural lock throughout recovery and reuse.
    pub(crate) fn reclaim_dead(&self) -> Result<bool> {
        self.check_process()?;
        let mut reclaimed = false;
        #[cfg(unix)]
        if let Self::Shared(shared) = self {
            for owner in 0..shared.owners {
                if owner == shared.owner
                    || shared
                        .counts(owner)
                        .iter()
                        .all(|count| count.load(Ordering::Acquire) == 0)
                {
                    continue;
                }
                if let Some(_hold) = try_hold_lane(&shared.name, owner)? {
                    for count in shared.counts(owner) {
                        reclaimed |= count.swap(0, Ordering::AcqRel) != 0;
                    }
                }
            }
        }
        Ok(reclaimed)
    }

    fn counter(
        &self,
        index: usize,
    ) -> &AtomicU32 {
        match self {
            Self::Memory(counts) => &counts[index],
            #[cfg(unix)]
            Self::Shared(shared) => &shared.counts(shared.owner)[index],
        }
    }
}

pub(crate) struct Lease {
    owners: Owners,
    index: usize,
}

impl Drop for Lease {
    fn drop(&mut self) {
        // A fork child must not decrement the parent's owner row.
        if self.owners.check_process().is_ok() {
            self.owners.counter(self.index).fetch_sub(1, Ordering::Release);
        }
    }
}

#[cfg(unix)]
pub(crate) struct Shared {
    pub(crate) region: ShmRegion,
    name: String,
    owners: usize,
    capacity: usize,
    offset: usize,
    owner: usize,
    pid: u32,
    hold: ManuallyDrop<LaneHold>,
}

#[cfg(unix)]
impl Shared {
    /// The initialization/structural kernel lock must still be held. Holding
    /// the row's kernel lock proves exclusive ownership before clearing pins.
    pub(crate) fn claim_locked(
        region: ShmRegion,
        name: &str,
        owners: usize,
        capacity: usize,
        offset: usize,
    ) -> Result<Arc<Self>> {
        for owner in 0..owners {
            if let Some(hold) = try_hold_lane(name, owner)? {
                let shared = Self {
                    region,
                    name: name.to_owned(),
                    owners,
                    capacity,
                    offset,
                    owner,
                    pid: std::process::id(),
                    hold: ManuallyDrop::new(hold),
                };
                for count in shared.counts(owner) {
                    count.store(0, Ordering::Release);
                }
                return Ok(Arc::new(shared));
            }
        }
        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "cell handle owner capacity is exhausted",
        )))
    }

    pub(crate) fn is_current(
        &self,
        owners: usize,
    ) -> bool {
        self.pid == std::process::id() && self.owners == owners
    }

    fn counts(
        &self,
        owner: usize,
    ) -> &[AtomicU32] {
        // SAFETY: the owner count, capacity and total size are validated at
        // attach. Rows follow the slots, with cache-line aligned strides.
        unsafe {
            std::slice::from_raw_parts(
                self.region
                    .as_ptr()
                    .add(self.offset + owner * stride(self.capacity))
                    .cast(),
                self.capacity,
            )
        }
    }
}

#[cfg(unix)]
impl Drop for Shared {
    fn drop(&mut self) {
        if self.pid == std::process::id() {
            // Leases keep this mapping and kernel hold alive. At this point
            // all of this owner's pins are zero. A fork child never unlocks
            // its parent's inherited file description.
            unsafe { ManuallyDrop::drop(&mut self.hold) };
        }
    }
}

#[cfg(unix)]
const OWNER_CAPACITY: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_CELL_OWNER_CAPACITY"),
    0,
);
#[cfg(unix)]
const _: () = assert!(
    OWNER_CAPACITY <= u32::MAX as usize
        && (option_env!("ORBIT_CELL_OWNER_CAPACITY").is_none()
            || OWNER_CAPACITY != 0)
);

#[cfg(unix)]
pub(crate) fn owner_capacity(fleet: &orbit_core::Fleet) -> usize {
    if OWNER_CAPACITY == 0 {
        usize::from(fleet.fleet_capacity())
    } else {
        OWNER_CAPACITY
    }
}

#[cfg(unix)]
fn stride(capacity: usize) -> usize {
    (capacity * size_of::<AtomicU32>()).next_multiple_of(64)
}

#[cfg(unix)]
pub(crate) fn segment_size(
    offset: usize,
    capacity: usize,
    owners: usize,
) -> Result<usize> {
    if owners == 0 || owners > u32::MAX as usize {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "cell owner capacity must be nonzero and fit in u32",
        )));
    }
    stride(capacity)
        .checked_mul(owners)
        .and_then(|rows| offset.checked_add(rows))
        .filter(|size| *size <= isize::MAX as usize)
        .ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cell owner geometry is too large",
            ))
        })
}
