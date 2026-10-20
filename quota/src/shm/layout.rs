use std::mem::{align_of, size_of};
use std::sync::atomic::AtomicU64;

use portable_atomic::AtomicU128;

use crate::retention::Retention;
use crate::{Error, Geometry, Policy};

pub(super) const INITIALIZING: u64 = 1;
pub(super) const MAGIC: u64 = u64::from_le_bytes(*b"ORBQTA01");
pub(super) const READY: u64 = 2;
pub(super) const FROZEN: u64 = 1 << 63;
pub(super) const RETIRED: u64 = 1 << 62;
pub(super) const GENERATION: u64 = RETIRED - 1;

#[repr(C, align(64))]
pub(super) struct Header {
    pub magic: AtomicU64,
    pub state: AtomicU64,
    pub spec: [u8; 160],
    pub seed: [u8; 32],
}

#[repr(C, align(64))]
pub(super) struct Shard {
    pub watermark: AtomicU128,
}

#[repr(C, align(64))]
pub(super) struct Owner {
    pub generation: AtomicU64,
}

#[repr(C, align(16))]
pub(super) struct Bank {
    pub namespace_len: AtomicU64,
    pub subject_len: AtomicU64,
    pub tat: AtomicU128,
}

#[repr(C, align(64))]
pub(super) struct Slot {
    pub control: AtomicU64,
    pub banks: [Bank; 2],
}

/// Checked offsets only; no process address or Rust collection enters SHM.
pub(super) struct Layout {
    pub shards: usize,
    pub owners: usize,
    pub pins: usize,
    pub slots: usize,
    pub slot_stride: usize,
    pub recency: usize,
    pub size: usize,
}

fn add(
    a: usize,
    b: usize,
) -> Result<usize, Error> {
    a.checked_add(b).ok_or(Error::InvalidGeometry)
}

fn mul(
    a: usize,
    b: usize,
) -> Result<usize, Error> {
    a.checked_mul(b).ok_or(Error::InvalidGeometry)
}

fn align(
    value: usize,
    alignment: usize,
) -> Result<usize, Error> {
    Ok(add(value, alignment - 1)? & !(alignment - 1))
}

impl Layout {
    pub fn new(
        geometry: Geometry,
        owners: usize,
    ) -> Result<Self, Error> {
        Self::with_retention(geometry, owners, Retention::Strict)
    }

    pub fn with_retention(
        geometry: Geometry,
        owners: usize,
        retention: Retention,
    ) -> Result<Self, Error> {
        let shards = size_of::<Header>();
        let owner_offset =
            add(shards, mul(geometry.shards().get(), size_of::<Shard>())?)?;
        let pins = add(owner_offset, mul(owners, size_of::<Owner>())?)?;
        let slots = align(
            add(
                pins,
                mul(
                    mul(owners, geometry.slots().get())?,
                    size_of::<AtomicU64>(),
                )?,
            )?,
            align_of::<Slot>(),
        )?;
        let keys_end =
            add(size_of::<Slot>(), mul(2, geometry.key_bytes().get())?)?;
        let recency = align(keys_end, align_of::<AtomicU128>())?;
        // Strict mappings keep ABI v1's exact shape. Guard mappings append
        // one recency word per bank, committed by the same descriptor.
        let end = if retention == Retention::Guard {
            add(recency, mul(2, size_of::<AtomicU128>())?)?
        } else {
            keys_end
        };
        let slot_stride = align(end, align_of::<Slot>())?;
        let size = add(slots, mul(geometry.slots().get(), slot_stride)?)?;
        if size > isize::MAX as usize || size > i64::MAX as usize {
            return Err(Error::InvalidGeometry);
        }
        Ok(Self {
            shards,
            owners: owner_offset,
            pins,
            slots,
            slot_stride,
            recency,
            size,
        })
    }

    pub fn spec(
        &self,
        geometry: Geometry,
        policy: Policy,
        owners: usize,
        domain: [u8; 32],
        retention: Retention,
    ) -> [u8; 160] {
        let mut out = [0; 160];
        out[..8].copy_from_slice(b"ORBQUOTA");
        for (index, value) in [
            // Format 1 is strict quota; format 2 adds guard retention and
            // banked recency. Even equal-sized shapes must never mix modes.
            if retention == Retention::Strict { 1 } else { 2 },
            geometry.slots().get() as u64,
            geometry.shards().get() as u64,
            geometry.key_bytes().get() as u64,
            geometry.probes().get() as u64,
            owners as u64,
            policy.units().get(),
            policy.burst().get(),
        ]
        .into_iter()
        .enumerate()
        {
            out[8 + index * 8..16 + index * 8]
                .copy_from_slice(&value.to_le_bytes());
        }
        out[72..88].copy_from_slice(&policy.period().as_nanos().to_le_bytes());
        out[88..120].copy_from_slice(&domain);
        out[120..128].copy_from_slice(&(self.size as u64).to_le_bytes());
        out[128..136].copy_from_slice(&(self.slot_stride as u64).to_le_bytes());
        out[136..144].copy_from_slice(
            &(usize::from(cfg!(target_endian = "little")) as u64).to_le_bytes(),
        );
        // Hash protocol 1: keyed BLAKE3 over framed key components.
        out[144..152].copy_from_slice(&1_u64.to_le_bytes());
        out[152..160].copy_from_slice(&u64::from(usize::BITS).to_le_bytes());
        out
    }
}
