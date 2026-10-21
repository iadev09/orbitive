//! Bind the lossless inbox layout to one named shared-memory segment.

#[cfg(feature = "tokio")]
use std::collections::HashSet;
use std::sync::Arc;
#[cfg(feature = "tokio")]
use std::sync::Mutex;

#[cfg(feature = "tokio")]
use orbit_core::shm::try_hold_watcher;
use orbit_core::shm::{LaneHold, ShmRegion, ring_segment_name, try_hold_lane};

use super::{Error, Inbox, LinkSpec, Reclaimed, Result};

/// A mapped inbox table and the region that keeps it mapped.
pub struct LinkSegment {
    region: ShmRegion,
    inbox: Inbox,
    name: String,
    fleet_name: String,
    spec: LinkSpec,
    group: Option<orbit_core::shm::GroupAccess>,
    #[cfg(feature = "tokio")]
    receivers: Mutex<HashSet<usize>>,
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
    pub reclaimed: Vec<Reclaimed>,
}

impl LinkSegment {
    /// Open or create one named link inbox.
    pub fn open(
        name: &str,
        spec: LinkSpec,
    ) -> Result<Self> {
        Self::open_inner(name, spec, None)
    }

    /// Open a dedicated group-shared Link, without sharing any fleet rings.
    /// The configured owner creates resources; other users attach existing ones.
    pub fn open_group(
        name: &str,
        spec: LinkSpec,
        access: orbit_core::shm::GroupAccess,
    ) -> Result<Self> {
        Self::open_inner(name, spec, Some(access))
    }

    fn open_inner(
        name: &str,
        spec: LinkSpec,
        group: Option<orbit_core::shm::GroupAccess>,
    ) -> Result<Self> {
        spec.validate()?;
        let segment_name = if let Some(access) = &group {
            validate_name(name)?;
            let segment = access.segment_name(name, spec.inbox_kind);
            crate::bell::Bell::validate_group_address(
                access,
                &segment,
                usize::from(spec.fleet_capacity - 1),
            )
            .map_err(Error::Io)?;
            access
                .prepare_locks(&segment, usize::from(spec.fleet_capacity))
                .map_err(Error::Io)?;
            // Provision streams before publishing a joinable inbox. Recovery
            // and body handles must use this exact access scope as well.
            orbit_stream::Streams::prepare_group(
                name,
                spec.fleet_capacity,
                spec.streams,
                access.clone(),
            )?;
            segment
        } else {
            check_name(name, spec)?;
            ring_segment_name(name, spec.inbox_kind)
        };
        let bytes = spec.inbox_segment_bytes();
        let (region, lock) = match &group {
            Some(access) => access.open_region(&segment_name, bytes)?,
            None => ShmRegion::open_or_create_locked(&segment_name, bytes)?,
        };
        let mut inbox = if region.created() {
            unsafe {
                Inbox::initialize(
                    region.as_ptr(),
                    spec.fleet_capacity as usize,
                    spec.inbox,
                )?
            }
        } else {
            unsafe {
                Inbox::attach(
                    region.as_ptr(),
                    spec.fleet_capacity as usize,
                    spec.inbox,
                )?
            }
        };
        inbox.install_bell_with_group(&segment_name, group.clone())?;
        drop(lock);
        Ok(Self {
            region,
            inbox,
            name: segment_name,
            fleet_name: name.to_owned(),
            spec,
            group,
            #[cfg(feature = "tokio")]
            receivers: Mutex::new(HashSet::new()),
        })
    }

    /// Take a lane after reclaiming every claimed lane whose kernel hold is
    /// no longer owned by a live process.
    pub fn join(
        &self,
        service: &str,
        role: &str,
        incarnation: u64,
    ) -> Result<Joined> {
        let mut reclaimed = Vec::new();
        for lane in 0..self.inbox.lanes() {
            let mut recovery = Ok(());
            if let Some(lane) = self.reclaim_if_gone_with(lane, |gone| {
                recovery = self.recover_streams(gone.lane, gone.incarnation);
            })? {
                recovery?;
                reclaimed.push(lane);
            }
        }
        let (lane, hold) =
            self.inbox.join(service, role, incarnation, |lane| {
                let Some(hold) = self.hold(lane)? else { return Ok(None) };
                // A withdrawn owner may have died during drain. End its sides
                // under the hold before its stamp is replaced by the new claim.
                if let Some((_, previous)) = self.inbox.last_owner(lane)? {
                    self.recover_streams(lane, previous)?;
                }
                Ok(Some(hold))
            })?;
        Ok(Joined { lane, hold, reclaimed })
    }

    fn recover_streams(
        &self,
        lane: usize,
        incarnation: u64,
    ) -> Result<()> {
        if let Some(access) = &self.group {
            return orbit_stream::Streams::node_dead_in_group(
                &self.fleet_name,
                self.spec.fleet_capacity,
                self.spec.streams,
                orbit_core::NodeId::new(lane as u16),
                orbit_stream::Incarnation::new(incarnation),
                access.clone(),
            )
            .map_err(Error::Stream);
        }
        orbit_stream::Streams::node_dead_in_shm(
            &self.fleet_name,
            self.spec.fleet_capacity,
            self.spec.streams,
            orbit_core::NodeId::new(lane as u16),
            orbit_stream::Incarnation::new(incarnation),
        )
        .map(|_| ())
        .map_err(Error::Stream)
    }

    /// Take back `lane` if it is claimed and its kernel hold is free: its
    /// owner died without releasing it. `None` when the lane is free or its
    /// owner is alive.
    ///
    /// A join does this for every lane. A writer refused by a lane asks it
    /// for that lane: a process killed outright leaves its lane claimed and
    /// filling, so until the next join every writer would keep finding it
    /// full. Reclaiming announces the directory, so watchers see the
    /// departure as they see an orderly one.
    pub fn reclaim_if_gone(
        &self,
        lane: usize,
    ) -> Result<Option<Reclaimed>> {
        self.reclaim_if_gone_with(lane, |_| {})
    }

    /// Run resource recovery while the reclaimed lane is still kernel-held,
    /// before another participant can publish a replacement on that index.
    pub fn reclaim_if_gone_with(
        &self,
        lane: usize,
        recover: impl FnOnce(&Reclaimed),
    ) -> Result<Option<Reclaimed>> {
        if self.inbox.consumers(lane)? == 0 {
            return Ok(None);
        }
        match self.hold(lane)? {
            Some(_gone) => {
                let gone = self.inbox.reclaim(lane)?;
                if let Some(gone) = &gone {
                    recover(gone);
                }
                Ok(gone)
            }
            None => Ok(None),
        }
    }

    /// Recover only lanes last owned by this exact process incarnation.
    /// The caller has observed process exit; the kernel hold additionally
    /// fences this scan against a concurrent claim. Withdrawn lanes retain
    /// their stamp so a process dying during graceful drain is covered too.
    pub fn reclaim_process(
        &self,
        pid: u32,
        incarnation: u64,
        mut recover: impl FnMut(&Reclaimed),
    ) -> Result<Vec<Reclaimed>> {
        let mut reclaimed = Vec::new();
        for lane in 0..self.inbox.lanes() {
            if self.inbox.last_owner(lane)? != Some((pid, incarnation)) {
                continue;
            }
            let Some(_hold) = self.hold(lane)? else { continue };
            if self.inbox.last_owner(lane)? != Some((pid, incarnation)) {
                continue;
            }
            if self.inbox.consumers(lane)? != 0 {
                self.inbox.release_lane(lane)?;
            }
            let gone = Reclaimed { lane, pid, incarnation };
            recover(&gone);
            reclaimed.push(gone);
        }
        Ok(reclaimed)
    }

    fn hold(
        &self,
        lane: usize,
    ) -> Result<Option<LaneHold>> {
        match &self.group {
            Some(access) => access.try_hold_lane(&self.name, lane),
            None => try_hold_lane(&self.name, lane),
        }
        .map_err(Error::Io)
    }

    pub fn inbox(&self) -> &Inbox {
        &self.inbox
    }

    /// Bind this process's claimed lane to the current Tokio reactor.
    ///
    /// One receiver owns one lane. Frames remain authoritative in SHM; the
    /// named datagram is only a coalescing wake that tells the task to inspect
    /// them. The receiver therefore loses neither a frame committed before it
    /// binds nor one committed while its future is cancelled.
    #[cfg(feature = "tokio")]
    pub fn receiver(
        self: &Arc<Self>,
        lane: usize,
    ) -> Result<crate::InboxReceiver> {
        self.inbox.wait_word(lane)?;
        let mut held = self.receivers.lock().map_err(|_| {
            Error::Malformed(
                "the process-local inbox receiver registry is poisoned",
            )
        })?;
        if !held.insert(lane) {
            return Err(Error::Malformed(
                "this process already receives the inbox lane",
            ));
        }
        drop(held);
        match crate::InboxReceiver::bind(Arc::clone(self), lane) {
            Ok(receiver) => Ok(receiver),
            Err(error) => {
                self.release_receiver(lane);
                Err(error)
            }
        }
    }

    /// Watch the lane directory without holding a lane.
    ///
    /// For a process that serves nothing but needs to know who joined or left
    /// — a master whose workers hold the lanes. The watcher takes the first
    /// free slot; a slot whose holder died is free, because the kernel
    /// released its hold. It is woken by the same claim and release that wake
    /// every lane, and never appears in the directory itself. `role` says
    /// what the watching process is, in a lane's words (`edge`, `php`), so a
    /// reader can tell which side of a link it is on.
    #[cfg(feature = "tokio")]
    pub fn watch(
        self: &Arc<Self>,
        role: &str,
    ) -> Result<crate::DirectoryWatcher> {
        for slot in 0..self.inbox.lanes() {
            let Some(hold) = match &self.group {
                Some(access) => access.try_hold_watcher(&self.name, slot),
                None => try_hold_watcher(&self.name, slot),
            }
            .map_err(Error::Io)?
            else {
                continue;
            };
            return crate::DirectoryWatcher::bind(
                Arc::clone(self),
                slot,
                role,
                hold,
            );
        }
        Err(Error::NoWatcher)
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn release_receiver(
        &self,
        lane: usize,
    ) {
        if let Ok(mut held) = self.receivers.lock() {
            held.remove(&lane);
        }
    }

    pub fn spec(&self) -> LinkSpec {
        self.spec
    }

    /// Open body streams in this Link's exact IPC scope, not its caller's UID.
    pub fn bodies(
        &self,
        lane: usize,
        incarnation: u64,
    ) -> Result<super::LinkBodies> {
        match &self.group {
            Some(access) => super::LinkBodies::for_group(
                &self.fleet_name,
                lane,
                incarnation,
                self.spec,
                access.clone(),
            ),
            None => super::LinkBodies::for_name(
                &self.fleet_name,
                lane,
                incarnation,
                self.spec,
            ),
        }
    }

    pub fn created(&self) -> bool {
        self.region.created()
    }

    pub fn segment_bytes(&self) -> usize {
        self.region.len()
    }

    /// Remove the inbox name. Existing mappings remain valid.
    pub fn unlink(&self) -> Result<()> {
        if let Some(access) = &self.group {
            return access.unlink_region(&self.region).map_err(Error::Io);
        }
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
    spec: LinkSpec,
) -> Result<()> {
    validate_name(name)?;
    let invalid = |reason: String| {
        Err(Error::InvalidName { name: name.to_owned(), reason })
    };
    let inbox_segment = ring_segment_name(name, spec.inbox_kind);
    let longest = inbox_segment
        .len()
        .max(ring_segment_name(name, spec.streams.kind).len());
    if longest > SHM_NAME_MAX {
        let room = name.len().saturating_sub(longest - SHM_NAME_MAX);
        return invalid(format!(
            "its segment name is {longest} bytes and this host takes {SHM_NAME_MAX}; at most {room} characters fit"
        ));
    }
    crate::bell::Bell::validate_address(
        &inbox_segment,
        usize::from(spec.fleet_capacity.saturating_sub(1)),
    )
    .map_err(|error| Error::InvalidName {
        name: name.to_owned(),
        reason: format!("its reactor socket address is invalid: {error}"),
    })?;
    Ok(())
}

fn validate_name(name: &str) -> Result<()> {
    let invalid = |reason: String| {
        Err(Error::InvalidName { name: name.to_owned(), reason })
    };
    if name.is_empty() {
        return invalid("it is empty".into());
    }
    if let Some(bad) = name.chars().find(|character| {
        !(character.is_ascii_alphanumeric()
            || matches!(character, '-' | '_' | '.'))
    }) {
        return invalid(format!(
            "'{bad}' is not a letter, a digit, '-', '_' or '.'"
        ));
    }
    Ok(())
}
