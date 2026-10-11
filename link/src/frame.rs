//! Frames orbit-link reserves for itself inside a lane.
//!
//! The link defines no application frame: what a downstream writes into a
//! lane is its own. A lane can still carry frames of this crate's features
//! beside the downstream's, so the first byte is shared: `0xF0..=0xFF`
//! belongs to orbit-link, everything below it to the downstream.

use std::ops::RangeInclusive;

/// First bytes orbit-link may use for its own frames. A downstream's frames
/// start with a byte outside this range.
pub const RESERVED_FRAME_TAGS: RangeInclusive<u8> = 0xf0..=0xff;

/// A fleet event carried between two fleets (the `events` feature).
pub const EVENT_FRAME_TAG: u8 = 0xf1;

/// Whether this frame belongs to orbit-link rather than to the downstream.
pub fn is_reserved(frame: &[u8]) -> bool {
    frame.first().is_some_and(|tag| RESERVED_FRAME_TAGS.contains(tag))
}

#[cfg_attr(not(feature = "tokio"), allow(dead_code))]
pub(crate) fn is_event(frame: &[u8]) -> bool {
    frame.first() == Some(&EVENT_FRAME_TAG)
}

/// Tag, topic length (u16 LE), topic, payload length (u32 LE), payload.
#[cfg(feature = "events")]
pub(crate) fn encode_event(
    topic: &str,
    payload: &[u8],
    out: &mut Vec<u8>,
) -> crate::Result<()> {
    let topic_len = u16::try_from(topic.len()).map_err(|_| {
        crate::Error::Malformed("an event topic is longer than 64 KiB")
    })?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| crate::Error::FrameTooLarge)?;
    out.clear();
    out.reserve(7 + topic.len() + payload.len());
    out.push(EVENT_FRAME_TAG);
    out.extend_from_slice(&topic_len.to_le_bytes());
    out.extend_from_slice(topic.as_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

#[cfg(feature = "events")]
pub(crate) fn decode_event(frame: &[u8]) -> crate::Result<(&str, &[u8])> {
    let malformed = crate::Error::Malformed("the event frame is malformed");
    let rest = frame
        .strip_prefix(&[EVENT_FRAME_TAG])
        .ok_or(crate::Error::Malformed("the frame is not an event frame"))?;
    let (len, rest) = rest.split_first_chunk::<2>().ok_or(malformed)?;
    let topic_len = u16::from_le_bytes(*len) as usize;
    if rest.len() < topic_len {
        return Err(crate::Error::Malformed("the event frame is malformed"));
    }
    let (topic, rest) = rest.split_at(topic_len);
    let topic = std::str::from_utf8(topic)
        .map_err(|_| crate::Error::Malformed("an event topic is not UTF-8"))?;
    let (len, payload) = rest
        .split_first_chunk::<4>()
        .ok_or(crate::Error::Malformed("the event frame is malformed"))?;
    if payload.len() != u32::from_le_bytes(*len) as usize {
        return Err(crate::Error::Malformed("the event frame is malformed"));
    }
    Ok((topic, payload))
}
