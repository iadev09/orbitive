use bytes::{BufMut, Bytes, BytesMut};
use orbit_core::{Frame, NetId64};

use crate::layout::LockLayout;
use crate::{Error, LockEvent, LockKey, LockLease, LockOwner, LockTransition, Result};

const MAGIC: u32 = 0x4F_4C_43_4B; // "OLCK"
const VERSION: u8 = 2;
pub(crate) const FRAME_KIND_ACQUIRED: u8 = 1;
pub(crate) const FRAME_KIND_RENEWED: u8 = 2;
pub(crate) const FRAME_KIND_RELEASED: u8 = 3;

// magic + version + namespace_len + key_len + owner_len + lock_id +
// acquired_at + expires_at
pub(crate) const EVENT_HEADER_LEN: usize = 4 + 1 + 2 + 2 + 2 + 8 + 8 + 8;

pub(crate) fn encode<L: LockLayout>(transition: &LockTransition) -> Result<(u8, Bytes)> {
    let key = transition.key();
    let owner = transition.owner();
    let max_payload = L::EVENT_RING_SPEC
        .payload_capacity
        .saturating_sub(EVENT_HEADER_LEN)
        .min(crate::state::LOCK_STATE_PAYLOAD_MAX);
    if key
        .namespace()
        .len()
        .saturating_add(key.label().len())
        .saturating_add(owner.as_bytes().len())
        > max_payload
    {
        return Err(Error::EntryTooLarge {
            namespace_len: key.namespace().len(),
            key_len: key.label().len(),
            owner_len: owner.as_bytes().len(),
            max_payload,
        });
    }

    let (frame_kind, acquired_at_ms, expires_at_ms) = match transition {
        LockTransition::Acquired(lease) => (
            FRAME_KIND_ACQUIRED,
            lease.acquired_at_ms,
            lease.expires_at_ms,
        ),
        LockTransition::Renewed(lease) => (
            FRAME_KIND_RENEWED,
            lease.acquired_at_ms,
            lease.expires_at_ms,
        ),
        LockTransition::Released { .. } => (FRAME_KIND_RELEASED, 0, 0),
    };
    let mut out = BytesMut::with_capacity(
        EVENT_HEADER_LEN + key.namespace().len() + key.label().len() + owner.as_bytes().len(),
    );
    out.put_u32_le(MAGIC);
    out.put_u8(VERSION);
    out.put_u16_le(key.namespace().len() as u16);
    out.put_u16_le(key.label().len() as u16);
    out.put_u16_le(owner.as_bytes().len() as u16);
    out.put_u64_le(transition.lock_id().raw());
    out.put_u64_le(acquired_at_ms);
    out.put_u64_le(expires_at_ms);
    out.put_slice(key.namespace());
    out.put_slice(key.label());
    out.put_slice(owner.as_bytes());
    Ok((frame_kind, out.freeze()))
}

pub(crate) fn decode(frame: &Frame) -> Option<LockEvent> {
    if frame.payload.len() < EVENT_HEADER_LEN {
        return None;
    }
    if u32::from_le_bytes(frame.payload[0..4].try_into().ok()?) != MAGIC
        || frame.payload[4] != VERSION
    {
        return None;
    }
    let namespace_len = u16::from_le_bytes(frame.payload[5..7].try_into().ok()?) as usize;
    let key_len = u16::from_le_bytes(frame.payload[7..9].try_into().ok()?) as usize;
    let owner_len = u16::from_le_bytes(frame.payload[9..11].try_into().ok()?) as usize;
    let lock_id = NetId64::from_raw(u64::from_le_bytes(frame.payload[11..19].try_into().ok()?));
    let acquired_at_ms = u64::from_le_bytes(frame.payload[19..27].try_into().ok()?);
    let expires_at_ms = u64::from_le_bytes(frame.payload[27..35].try_into().ok()?);
    let namespace_end = EVENT_HEADER_LEN.checked_add(namespace_len)?;
    let key_end = namespace_end.checked_add(key_len)?;
    let owner_end = key_end.checked_add(owner_len)?;
    if namespace_len == 0
        || key_len == 0
        || owner_len == 0
        || owner_end != frame.payload.len()
        || namespace_len
            .saturating_add(key_len)
            .saturating_add(owner_len)
            > crate::state::LOCK_STATE_PAYLOAD_MAX
        || frame.ver == 0
    {
        return None;
    }
    let key = LockKey::from_parts(
        frame.payload.slice(EVENT_HEADER_LEN..namespace_end),
        frame.payload.slice(namespace_end..key_end),
    );
    let owner = LockOwner::from(frame.payload.slice(key_end..owner_end));
    let transition = match frame.kind {
        FRAME_KIND_ACQUIRED | FRAME_KIND_RENEWED
            if acquired_at_ms > 0 && expires_at_ms > acquired_at_ms =>
        {
            let lease = LockLease {
                lock_id,
                key,
                owner,
                acquired_at_ms,
                expires_at_ms,
                state_revision: frame.ver,
            };
            if frame.kind == FRAME_KIND_ACQUIRED {
                LockTransition::Acquired(lease)
            } else {
                LockTransition::Renewed(lease)
            }
        }
        FRAME_KIND_RELEASED if acquired_at_ms == 0 && expires_at_ms == 0 => {
            LockTransition::Released {
                lock_id,
                key,
                owner,
                state_revision: frame.ver,
            }
        }
        _ => return None,
    };
    Some(LockEvent {
        event_id: frame.id,
        transition,
    })
}
