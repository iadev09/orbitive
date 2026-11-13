//! Bind the lossless inbox layout to one named shared-memory segment.

#[cfg(feature = "tokio")]
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[cfg(feature = "tokio")]
use orbit_core::shm::try_hold_watcher;
use orbit_core::shm::{
    LaneHold, ShmRegion, ring_segment_name, try_hold_lane, wait_lane_free,
};

use super::{Error, Inbox, LinkSpec, Reclaimed, Result};

/// A mapped inbox table and the region that keeps it mapped.
pub struct LinkSegment {
    region: ShmRegion,
    inbox: Inbox,
    name: String,
    fleet_name: String,
    spec: LinkSpec,
    group: Option<orbit_core::shm::GroupAccess>,
    /// Cold-open recovery is reported by the next successful join too.
    reclaimed: Mutex<Vec<Reclaimed>>,
    #[cfg(feature = "tokio")]
    receivers: Mutex<HashSet<usize>>,
    /// Bound the first time this producer waits for a record, then kept.
    #[cfg(feature = "tokio")]
    producer_bell: Mutex<Option<Arc<tokio::net::UnixDatagram>>>,
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
    segment: String,
    epoch: u64,
}

impl Joined {
    /// The inbox epoch owned by this membership.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub(crate) fn belongs_to(
        &self,
        segment: &LinkSegment,
    ) -> bool {
        self.segment == segment.name
            && segment.inbox.lane_epoch(self.lane).ok() == Some(self.epoch)
    }
}

impl LinkSegment {
    /// Open or create one named link inbox.
    pub fn open(
        name: &str,
        spec: LinkSpec,
    ) -> Result<Self> {
        Self::open_inner(name, spec, None, true)?
            .ok_or(Error::Malformed("a created Link inbox was not opened"))
    }

    /// Attach an existing owner-only link inbox and adopt the shape its
    /// owner declared, read from the inbox and stream-table headers. Never
    /// creates: `Ok(None)` when the inbox or its stream table does not exist
    /// yet, so a caller observes an absent owner instead of fixing the
    /// fleet's shape itself.
    pub fn adopt(
        name: &str,
        inbox_kind: u8,
        stream_kind: u8,
    ) -> Result<Option<Self>> {
        validate_name(name)?;
        let segment_name = ring_segment_name(name, inbox_kind);
        let Some((region, lock)) = ShmRegion::open_existing_locked(
            &segment_name,
            size_of::<crate::inbox::TableHeader>(),
        )?
        else {
            return Ok(None);
        };
        // SAFETY: the mapping holds a header, and its creator finished it
        // before releasing the initialization lock held here.
        let (fleet_capacity, inbox) =
            unsafe { Inbox::persisted_shape(region.as_ptr()) }?;
        drop(lock);
        drop(region);
        let Some((stream_fleet, streams)) =
            orbit_stream::Streams::persisted_spec(name, stream_kind)?
        else {
            return Ok(None);
        };
        if usize::from(stream_fleet) != fleet_capacity {
            return Err(Error::Malformed(
                "link stream fleet capacity differs from its inbox",
            ));
        }
        let fleet_capacity = u16::try_from(fleet_capacity).map_err(|_| {
            Error::Malformed("inbox fleet capacity must fit a nonzero u16")
        })?;
        let spec = LinkSpec::new(fleet_capacity, inbox_kind, inbox, streams);
        Self::open_inner(name, spec, None, false)
    }

    /// Open a dedicated group-shared Link, without sharing any fleet rings.
    /// The configured owner creates resources; other users attach existing ones.
    pub fn open_group(
        name: &str,
        spec: LinkSpec,
        access: orbit_core::shm::GroupAccess,
    ) -> Result<Self> {
        Self::open_inner(name, spec, Some(access), true)?
            .ok_or(Error::Malformed("a created Link inbox was not opened"))
    }

    /// `create: false` attaches existing owner-only resources only.
    fn open_inner(
        name: &str,
        spec: LinkSpec,
        group: Option<orbit_core::shm::GroupAccess>,
        create: bool,
    ) -> Result<Option<Self>> {
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
                .prepare_locks(&segment, 3 * usize::from(spec.fleet_capacity))
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
            None if create => {
                ShmRegion::open_or_create_locked(&segment_name, bytes)?
            }
            None => {
                match ShmRegion::open_existing_locked(&segment_name, bytes)? {
                    Some(opened) => opened,
                    None => return Ok(None),
                }
            }
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
        let mut segment = Self {
            region,
            inbox,
            name: segment_name,
            fleet_name: name.to_owned(),
            spec,
            group,
            reclaimed: Mutex::new(Vec::new()),
            #[cfg(feature = "tokio")]
            receivers: Mutex::new(HashSet::new()),
            #[cfg(feature = "tokio")]
            producer_bell: Mutex::new(None),
        };
        if let Err(error) = segment.register_producer() {
            if !matches!(error, Error::InboxFull) {
                return Err(error);
            }
            // No registration is needed to retire an abandoned inbox. Do
            // this before one bounded retry, otherwise retained references
            // can prevent the mapping needed to reach join-time recovery.
            let reclaimed = segment.recover_abandoned()?;
            *segment.reclaimed.get_mut().map_err(|_| {
                Error::Malformed("Link recovery observations are poisoned")
            })? = reclaimed;
            segment.register_producer()?;
        }
        drop(lock);
        if segment.region.created() && segment.group.is_none() {
            segment.inbox.ring_adopter();
        }
        Ok(Some(segment))
    }

    /// Wait until the owner of `name` has created its Link, then adopt it
    /// as [`LinkSegment::adopt`] does. For a process that runs the fleet's
    /// processes without creating their Link: it starts the process that
    /// creates it, then waits here.
    ///
    /// No deadline and no poll. The adopter binds its bell before it looks;
    /// the creator rings it once the inbox exists and again whenever a
    /// stream table is opened through the segment, and each ring means only
    /// "look again". One adopter per Link: a second gets
    /// [`Error::NoWatcher`]. Owner-only Links only.
    #[cfg(feature = "tokio")]
    pub async fn adopt_when_created(
        name: &str,
        inbox_kind: u8,
        stream_kind: u8,
    ) -> Result<Self> {
        validate_name(name)?;
        let segment_name = ring_segment_name(name, inbox_kind);
        let _hold = orbit_core::shm::try_hold_adopter(&segment_name)
            .map_err(Error::Io)?
            .ok_or(Error::NoWatcher)?;
        let bell = crate::bell::Bell::with_group(&segment_name, None, 0)
            .map_err(Error::Io)?;
        let socket = tokio::net::UnixDatagram::from_std(
            bell.bind(crate::bell::Doorbell::Adopter).map_err(Error::Io)?,
        )
        .map_err(Error::Io)?;
        struct Unbind(crate::bell::Bell);
        impl Drop for Unbind {
            fn drop(&mut self) {
                self.0.unbind(crate::bell::Doorbell::Adopter);
            }
        }
        let _unbind = Unbind(bell);
        loop {
            if let Some(segment) = Self::adopt(name, inbox_kind, stream_kind)? {
                return Ok(segment);
            }
            socket.readable().await.map_err(Error::Io)?;
            let mut scratch = [0u8; 64];
            while socket.try_recv(&mut scratch).is_ok() {}
        }
    }

    fn register_producer(&mut self) -> Result<()> {
        // Producer holds use a disjoint suffix of the lane-lock namespace.
        let lanes = usize::from(self.spec.fleet_capacity);
        let name = &self.name;
        let group = &self.group;
        self.inbox.register_producer(|index| {
            match group {
                Some(access) => access.try_hold_lane(name, lanes + index),
                None => try_hold_lane(name, lanes + index),
            }
            .map_err(Error::Io)
        })
    }

    /// Take a lane after reclaiming abandoned readers and queued work.
    /// Includes recovery performed while opening this mapping under producer
    /// registration pressure.
    pub fn join(
        &self,
        service: &str,
        role: &str,
        incarnation: u64,
    ) -> Result<Joined> {
        self.join_with_capacity(service, role, incarnation, None)
    }

    pub fn join_with_capacity(
        &self,
        service: &str,
        role: &str,
        incarnation: u64,
        capacity: Option<usize>,
    ) -> Result<Joined> {
        let recovered = self.recover_abandoned()?;
        let (lane, hold) = self.inbox.join_with_capacity(
            service,
            role,
            incarnation,
            capacity,
            |lane| {
                let Some(hold) = self.hold(lane)? else { return Ok(None) };
                // A withdrawn owner may have died during drain. End its sides
                // under the hold before its stamp is replaced by the new claim.
                if let Some((_, previous)) = self.inbox.last_owner(lane)? {
                    self.recover_streams(lane, previous)?;
                }
                Ok(Some(hold))
            },
        )?;
        let mut reclaimed =
            std::mem::take(&mut *self.reclaimed.lock().map_err(|_| {
                Error::Malformed("Link recovery observations are poisoned")
            })?);
        for gone in recovered {
            if !reclaimed.contains(&gone) {
                reclaimed.push(gone);
            }
        }
        let epoch = self.inbox.lane_epoch(lane)?;
        Ok(Joined { lane, hold, reclaimed, segment: self.name.clone(), epoch })
    }

    fn recover_abandoned(&self) -> Result<Vec<Reclaimed>> {
        self.recover_producers(None)?;
        let mut reclaimed = Vec::new();
        for lane in 0..self.inbox.lanes() {
            if let Some(gone) = self.recover_lane(lane, |gone| {
                self.recover_streams(gone.lane, gone.incarnation)
            })? {
                reclaimed.push(gone);
            }
        }
        Ok(reclaimed)
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

    /// Take back `lane` if its kernel hold is free and it is still claimed or
    /// retains queued work after withdrawal. `None` when there is nothing to
    /// retire or its reader is still alive, including during graceful drain.
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
        self.recover_lane(lane, |gone| {
            recover(gone);
            Ok(())
        })
    }

    fn recover_lane(
        &self,
        lane: usize,
        recover: impl FnOnce(&Reclaimed) -> Result<()>,
    ) -> Result<Option<Reclaimed>> {
        if self.inbox.consumers(lane)? == 0 && self.inbox.depth(lane)? == 0 {
            return Ok(None);
        }
        match self.hold(lane)? {
            Some(_gone) => {
                let gone = match self.inbox.reclaim(lane)? {
                    Some(gone) => Some(gone),
                    None => self.inbox.last_owner(lane)?.map(
                        |(pid, incarnation)| Reclaimed {
                            lane,
                            pid,
                            incarnation,
                        },
                    ),
                };
                if let Some(gone) = &gone {
                    recover(gone)?;
                }
                // The hold proves reader quiescence even for a lane already
                // withdrawn from routing. Retirement frees committed slot
                // references; it never cancels a still-live writer borrow.
                self.inbox.retire_lane(lane)?;
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
        self.recover_producers(Some(pid))?;
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
            self.inbox.retire_lane(lane)?;
            reclaimed.push(gone);
        }
        Ok(reclaimed)
    }

    /// Wait until the kernel has released the lane of this exited process, so
    /// that [`Self::reclaim_process`] can prove the death. An exit notification
    /// can arrive first. Blocks, and only a live holder prolongs it: a lane
    /// another process already took back returns at once.
    pub fn wait_lane_released(
        &self,
        lane: usize,
        pid: u32,
        incarnation: u64,
    ) -> Result<()> {
        if self.inbox.last_owner(lane)? != Some((pid, incarnation)) {
            return Ok(());
        }
        match &self.group {
            Some(access) => access.wait_lane_free(&self.name, lane),
            None => wait_lane_free(&self.name, lane),
        }
        .map_err(Error::Io)
    }

    /// Recover producer reservations after a native exit notification, or on
    /// a cold join. No PID liveness probe, timed scan or write-path lock.
    pub fn recover_producers(
        &self,
        pid: Option<u32>,
    ) -> Result<()> {
        self.inbox.recover_producers(pid, |index| {
            self.hold(self.inbox.lanes() + index)
        })
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

    /// Wait until a reader frees one of this producer's records, after a
    /// write returned [`Error::ProducerBusy`]; then write again.
    ///
    /// No deadline and no poll: the reader that frees a record rings this
    /// producer, and only while it waits. Returns at once when a record is
    /// already free. Several tasks may wait together; a ring wakes them all
    /// and each writes again. The bell is bound on first use and registers
    /// with the calling Tokio runtime, so a segment waits from one runtime.
    #[cfg(feature = "tokio")]
    pub async fn record_released(&self) -> Result<()> {
        let socket = {
            let mut bell = self.producer_bell.lock().map_err(|_| {
                Error::Malformed("Link producer bell is poisoned")
            })?;
            match &*bell {
                Some(socket) => Arc::clone(socket),
                None => {
                    let socket = Arc::new(self.inbox.bind_producer()?);
                    *bell = Some(Arc::clone(&socket));
                    socket
                }
            }
        };
        // Drain obsolete rings before arming, so the next readiness is new.
        let mut scratch = [0u8; 64];
        while socket.try_recv(&mut scratch).is_ok() {}
        if !self.inbox.wait_for_record()? {
            return Ok(());
        }
        socket.readable().await.map_err(Error::Io)?;
        while socket.try_recv(&mut scratch).is_ok() {}
        Ok(())
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
            None => {
                let bodies = super::LinkBodies::for_name(
                    &self.fleet_name,
                    lane,
                    incarnation,
                    self.spec,
                )?;
                // The stream table may be what an adopter still waits for.
                self.inbox.ring_adopter();
                Ok(bodies)
            }
        }
    }

    pub(crate) fn open_control_region(
        &self,
        kind: u8,
        size: usize,
    ) -> Result<(ShmRegion, orbit_core::shm::ShmRegionLock)> {
        if let Some(access) = &self.group {
            let name = access.segment_name(&self.fleet_name, kind);
            return access.open_region(&name, size).map_err(Error::Io);
        }
        ShmRegion::open_or_create_locked(
            &ring_segment_name(&self.fleet_name, kind),
            size,
        )
        .map_err(Error::Io)
    }

    pub(crate) fn lock_control_region(
        &self,
        kind: u8,
        region: &ShmRegion,
    ) -> Result<orbit_core::shm::ShmRegionLock> {
        if self.group.is_some() {
            let (_, lock) = self.open_control_region(kind, region.len())?;
            return Ok(lock);
        }
        region.lock_exclusive().map_err(Error::Io)
    }

    pub(crate) fn unlink_control_region(
        &self,
        region: &ShmRegion,
    ) -> Result<()> {
        match &self.group {
            Some(access) => access.unlink_region(region),
            None => region.unlink(),
        }
        .map_err(Error::Io)
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
#[cfg(feature = "tokio")]
impl Drop for LinkSegment {
    fn drop(&mut self) {
        if self.producer_bell.get_mut().is_ok_and(|bell| bell.is_some()) {
            self.inbox.unbind_producer();
        }
    }
}

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
