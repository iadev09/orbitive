//! Bind the lossless inbox layout to one named shared-memory segment.

use std::sync::Arc;

use orbit_core::shm::{LaneHold, ShmRegion, ring_segment_name, try_hold_lane};

use super::{Error, Inbox, LinkSpec, Reclaimed, Result};

/// A mapped inbox table and the region that keeps it mapped.
pub struct LinkSegment {
    region: ShmRegion,
    inbox: Inbox,
    name: String,
    spec: LinkSpec
}

/// A lane this process holds.
///
/// The kernel releases `hold` if the process dies. `reclaimed` identifies
/// lanes this join took back so their stream sides can be ended by exact
/// node/incarnation rather than by a timeout.
///
/// On orderly shutdown, call [`Inbox::release_lane`] before dropping `hold`.
/// An abrupt exit needs no cleanup: the kernel releases the hold, and the
/// next join reclaims the still-advertised lane.
pub struct Joined {
    pub lane: usize,
    pub hold: LaneHold,
    pub reclaimed: Vec<Reclaimed>
}

impl LinkSegment {
    /// Open or create one named link inbox.
    pub fn open(
        name: &str,
        spec: LinkSpec
    ) -> Result<Self> {
        spec.validate()?;
        check_name(name, spec)?;
        let segment_name = ring_segment_name(name, spec.inbox_kind);
        let bytes = spec.inbox_segment_bytes();
        let (region, lock) = ShmRegion::open_or_create_locked(&segment_name, bytes)?;
        let inbox = if region.created() {
            unsafe { Inbox::initialize(region.as_ptr(), spec.fleet_capacity as usize, spec.inbox)? }
        } else {
            unsafe { Inbox::attach(region.as_ptr(), spec.fleet_capacity as usize, spec.inbox)? }
        };
        drop(lock);
        Ok(Self { region, inbox, name: segment_name, spec })
    }

    /// Take a lane after reclaiming every claimed lane whose kernel hold is
    /// no longer owned by a live process.
    pub fn join(
        &self,
        service: &str,
        role: &str,
        incarnation: u64
    ) -> Result<Joined> {
        let mut reclaimed = Vec::new();
        for lane in 0..self.inbox.lanes() {
            if self.inbox.consumers(lane)? == 0 {
                continue;
            }
            if let Some(_gone) = self.hold(lane)?
                && let Some(lane) = self.inbox.reclaim(lane)?
            {
                reclaimed.push(lane);
            }
        }
        let (lane, hold) = self.inbox.join(service, role, incarnation, |lane| self.hold(lane))?;
        Ok(Joined { lane, hold, reclaimed })
    }

    fn hold(
        &self,
        lane: usize
    ) -> Result<Option<LaneHold>> {
        try_hold_lane(&self.name, lane).map_err(Error::Io)
    }

    pub fn inbox(&self) -> &Inbox {
        &self.inbox
    }

    pub fn spec(&self) -> LinkSpec {
        self.spec
    }

    pub fn created(&self) -> bool {
        self.region.created()
    }

    pub fn segment_bytes(&self) -> usize {
        self.region.len()
    }

    /// Remove the inbox name. Existing mappings remain valid.
    pub fn unlink(&self) -> Result<()> {
        self.region.unlink().map_err(Error::Io)
    }
}

/// A shareable lifecycle handle for registries and providers.
pub struct LinkSegmentHandle(Arc<LinkSegment>);

impl LinkSegmentHandle {
    pub fn new(segment: Arc<LinkSegment>) -> Self {
        Self(segment)
    }

    pub fn segment(&self) -> &Arc<LinkSegment> {
        &self.0
    }
}

#[cfg(target_os = "macos")]
const SHM_NAME_MAX: usize = 31;
#[cfg(not(target_os = "macos"))]
const SHM_NAME_MAX: usize = 255;

/// Validate the fleet name before opening either physical resource.
pub fn check_name(
    name: &str,
    spec: LinkSpec
) -> Result<()> {
    let invalid = |reason: String| Err(Error::InvalidName { name: name.to_owned(), reason });
    if name.is_empty() {
        return invalid("it is empty".into());
    }
    if let Some(bad) = name.chars().find(|character| {
        !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    }) {
        return invalid(format!("'{bad}' is not a letter, a digit, '-', '_' or '.'"));
    }
    let longest = ring_segment_name(name, spec.inbox_kind)
        .len()
        .max(ring_segment_name(name, spec.streams.kind).len());
    if longest > SHM_NAME_MAX {
        let room = name.len().saturating_sub(longest - SHM_NAME_MAX);
        return invalid(format!(
            "its segment name is {longest} bytes and this host takes {SHM_NAME_MAX}; at most {room} characters fit"
        ));
    }
    Ok(())
}
