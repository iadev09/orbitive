//! Layout version 2 of the session table: a claim on each set, no lock on
//! the table.
//!
//! An operation touches one set, the one its key hashes to. It claims that
//! set with one compare-and-swap, works on the set's slots as their only
//! user, and releases the claim. Nothing waits and nothing asks the kernel:
//! an operation that finds its set claimed is a miss or a refused store,
//! which a lossy session store may answer. Nothing of a slot is read before
//! the claim, so a set's slots are only ever seen whole. Two stores of one
//! key meet in the same set and cannot both install it, which is what keeps
//! a ticket single use.
//!
//! A claim names its owner: the registration of the handle that opened the
//! table, as an index and a generation. A registration is held by an
//! exclusive lock on a file of its own, which the kernel releases when the
//! process is gone, however it goes. Whoever can take that lock has proof
//! nobody stands behind the registration; a claim it left is taken over while
//! the lock is held and the set is erased whole, since nothing in a set an
//! operation never finished can be trusted. An operation that dies while
//! erasing leaves a claim of its own, which is taken back the same way. No
//! clock is involved anywhere.
//!
//! A table is opened in the process that uses it. A handle inherited across
//! `fork` is not looked for: operating through one stays exclusive, since
//! parent and child then claim under one identity, and the lock stays held
//! until both are gone, so a claim is never taken from a live process. What
//! it costs is capacity: a set the child leaves claimed is not taken back
//! while the parent lives. The one thing a child must not do is unlock, and
//! dropping its copy of the handle does not.

use std::mem::{ManuallyDrop, size_of};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{io, ptr, slice};

use orbit_core::process::ProcessOwner;
use orbit_core::shm::{LaneHold, ShmRegion, try_hold_lane};

use super::{
    CAPACITY, SET_COUNT, SessionHeader, SessionSlot, WAYS, validate_header,
};

/// How many handles can have the table open at once. Each holds one
/// registration, whichever process it is in.
const OWNERS: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_RUSTLS_SESSION_OWNERS"),
    256,
);
const VERSION: u16 = 2;
const FREE: u64 = 0;
const GENERATION_BITS: u32 = 48;
const GENERATION_MAX: u64 = (1 << GENERATION_BITS) - 1;

const OWNERS_OFFSET: usize = size_of::<SessionHeader>();
const CLAIMS_OFFSET: usize =
    OWNERS_OFFSET + (OWNERS * size_of::<AtomicU64>()).next_multiple_of(64);
const SLOTS_OFFSET: usize = CLAIMS_OFFSET + SET_COUNT * size_of::<SetClaim>();

/// A set's claim, on a cache line of its own so that two sets in use do not
/// share one.
#[repr(C, align(64))]
struct SetClaim(AtomicU64);

/// The word a registration writes into a set it claims. Never [`FREE`].
const fn identity(
    owner: usize,
    generation: u64,
) -> u64 {
    ((owner as u64 + 1) << GENERATION_BITS) | generation
}

/// The registration a claim names, when it names one this layout has.
fn claimant(word: u64) -> Option<(usize, u64)> {
    let owner = usize::try_from((word >> GENERATION_BITS).checked_sub(1)?)
        .ok()
        .filter(|owner| *owner < OWNERS)?;
    Some((owner, word & GENERATION_MAX))
}

pub(super) struct ClaimTable<const KEY_CAPACITY: usize> {
    region: ShmRegion,
    name: Box<str>,
    /// The process that registered. A fork child inherits the description
    /// holding the registration's lock and must not unlock it.
    process: ProcessOwner,
    owner: usize,
    generation: u64,
    identity: u64,
    hold: ManuallyDrop<LaneHold>,
}

impl<const KEY_CAPACITY: usize> ClaimTable<KEY_CAPACITY> {
    pub(super) fn open_or_create(name: &str) -> io::Result<Self> {
        let process = ProcessOwner::current()?;
        let size = segment_size::<KEY_CAPACITY>();
        let (region, initialization) =
            ShmRegion::open_or_create_locked(name, size)?;
        let header = region.as_ptr().cast::<SessionHeader>();
        if region.created() {
            // SAFETY: the mapping covers `size` bytes, and the initialization
            // lock keeps every other opener out until the header is written.
            unsafe {
                ptr::write_bytes(region.as_ptr(), 0, size);
                ptr::write(
                    header,
                    SessionHeader::new::<KEY_CAPACITY>(VERSION, OWNERS),
                );
            }
        } else {
            // SAFETY: the mapping covers a header, which nothing writes after
            // its creation.
            validate_header::<KEY_CAPACITY>(
                name,
                unsafe { &*header },
                VERSION,
                OWNERS,
            )?;
        }
        drop(initialization);

        let (owner, generation, hold) = register(&region, name)?;
        let table = Self {
            region,
            name: name.into(),
            process,
            owner,
            generation,
            identity: identity(owner, generation),
            hold: ManuallyDrop::new(hold),
        };
        table.take_back_own();
        Ok(table)
    }

    /// Run `operation` on `set` as its only user, or return `None` when the
    /// set is claimed and was not taken back.
    pub(super) fn with_set<T>(
        &self,
        set: usize,
        recover: bool,
        operation: impl FnOnce(&mut [SessionSlot<KEY_CAPACITY>]) -> T,
    ) -> io::Result<Option<T>> {
        let Some(mut claimed) = self.claim(set, recover)? else {
            return Ok(None);
        };
        Ok(Some(operation(claimed.slots())))
    }

    pub(super) fn reset(&self) -> io::Result<()> {
        for set in 0..SET_COUNT {
            let Some(mut claimed) = self.claim(set, true)? else {
                return Err(io::Error::new(
                    io::ErrorKind::ResourceBusy,
                    format!(
                        "SHM segment {} is in use: session set {set} is claimed",
                        self.name
                    ),
                ));
            };
            claimed.erase();
        }
        Ok(())
    }

    pub(super) fn unlink(&self) -> io::Result<()> {
        self.region.unlink()
    }

    /// The whole table, for a test that is its only user.
    #[cfg(test)]
    pub(super) fn with_slots<T>(
        &self,
        operation: impl FnOnce(&mut [SessionSlot<KEY_CAPACITY>]) -> T,
    ) -> T {
        // SAFETY: the mapping covers every slot, and the caller is a test
        // with no operation running beside it.
        operation(unsafe { slice::from_raw_parts_mut(self.slot(0), CAPACITY) })
    }

    fn claim(
        &self,
        set: usize,
        recover: bool,
    ) -> io::Result<Option<Claimed<'_, KEY_CAPACITY>>> {
        self.check()?;
        match self.claim_of(set).compare_exchange(
            FREE,
            self.identity,
            Ordering::Acquire,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(Some(Claimed { table: self, set })),
            Err(observed) if recover => self.take_back(set, observed),
            Err(_) => Ok(None),
        }
    }

    /// Take `set` over from the registration `observed` names, if nobody
    /// stands behind it any more.
    fn take_back(
        &self,
        set: usize,
        observed: u64,
    ) -> io::Result<Option<Claimed<'_, KEY_CAPACITY>>> {
        let Some((owner, generation)) = claimant(observed) else {
            return Err(self.damaged(set));
        };
        if owner == self.owner {
            // Another thread of this handle is at work in the set.
            return Ok(None);
        }
        // The lock is kept until the set is erased, so the registration
        // cannot change hands in the middle of this.
        let Some(_gone) = try_hold_lane(&self.name, owner)? else {
            return Ok(None);
        };
        // A registration only ever moves forward, and a claim cannot name a
        // generation its registration has not reached.
        if self.generation_of(owner).load(Ordering::Acquire) < generation {
            return Err(self.damaged(set));
        }
        if self
            .claim_of(set)
            .compare_exchange(
                observed,
                self.identity,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_err()
        {
            // It was released, or taken back by someone else, in between.
            return Ok(None);
        }
        let mut claimed = Claimed { table: self, set };
        claimed.erase();
        tracing::debug!(
            target: "orbit_rustls::session_cache",
            set,
            owner,
            generation,
            "session set taken back from an owner that is gone"
        );
        Ok(Some(claimed))
    }

    /// Erase the sets an earlier holder of this registration left claimed.
    ///
    /// Holding the registration is the proof that they are abandoned, and
    /// nobody else can take them back while it is held.
    fn take_back_own(&self) {
        for set in 0..SET_COUNT {
            let claim = self.claim_of(set);
            let observed = claim.load(Ordering::Acquire);
            if claimant(observed).is_none_or(|(owner, _)| owner != self.owner) {
                continue;
            }
            if claim
                .compare_exchange(
                    observed,
                    self.identity,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                Claimed { table: self, set }.erase();
            }
        }
    }

    /// Refuse to operate under a registration that is no longer this
    /// handle's.
    ///
    /// Which process this is, is not asked here: that would be a system call
    /// on every operation, for a fact the embedder settles when it opens the
    /// table in the process that uses it.
    fn check(&self) -> io::Result<()> {
        if self.generation_of(self.owner).load(Ordering::Acquire)
            != self.generation
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {} lost this handle's owner registration",
                    self.name
                ),
            ));
        }
        Ok(())
    }

    fn damaged(
        &self,
        set: usize,
    ) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "SHM segment {} has a malformed claim on session set {set}",
                self.name
            ),
        )
    }

    fn generation_of(
        &self,
        owner: usize,
    ) -> &AtomicU64 {
        debug_assert!(owner < OWNERS);
        // SAFETY: the mapping covers `OWNERS` aligned words at this offset.
        unsafe { &*generation_row(&self.region, owner) }
    }

    fn claim_of(
        &self,
        set: usize,
    ) -> &AtomicU64 {
        debug_assert!(set < SET_COUNT);
        // SAFETY: the mapping covers `SET_COUNT` aligned claims at this
        // offset, and they are only ever accessed as atomics.
        unsafe {
            &(*self
                .region
                .as_ptr()
                .add(CLAIMS_OFFSET)
                .cast::<SetClaim>()
                .add(set))
            .0
        }
    }

    fn slot(
        &self,
        index: usize,
    ) -> *mut SessionSlot<KEY_CAPACITY> {
        debug_assert!(index <= CAPACITY);
        // SAFETY: the mapping covers `CAPACITY` aligned slots at this offset.
        unsafe {
            self.region
                .as_ptr()
                .add(SLOTS_OFFSET)
                .cast::<SessionSlot<KEY_CAPACITY>>()
                .add(index)
        }
    }
}

impl<const KEY_CAPACITY: usize> Drop for ClaimTable<KEY_CAPACITY> {
    fn drop(&mut self) {
        // Unlocking in a fork child would release the registration under the
        // parent, which shares the description. The child's copy stays locked
        // until that child is gone, which only delays a take back.
        if self.process.is_current() {
            // SAFETY: dropped once, here, and not used afterwards.
            unsafe { ManuallyDrop::drop(&mut self.hold) };
        }
    }
}

/// One set, held by this handle until dropped.
struct Claimed<'a, const KEY_CAPACITY: usize> {
    table: &'a ClaimTable<KEY_CAPACITY>,
    set: usize,
}

impl<const KEY_CAPACITY: usize> Claimed<'_, KEY_CAPACITY> {
    fn slots(&mut self) -> &mut [SessionSlot<KEY_CAPACITY>] {
        // SAFETY: the claim makes this handle the only user of the set's
        // slots, in this process and in every other, and the borrow of the
        // claim keeps a second view of them from being made.
        unsafe {
            slice::from_raw_parts_mut(self.table.slot(self.set * WAYS), WAYS)
        }
    }

    fn erase(&mut self) {
        self.slots().iter_mut().for_each(SessionSlot::clear);
    }
}

impl<const KEY_CAPACITY: usize> Drop for Claimed<'_, KEY_CAPACITY> {
    fn drop(&mut self) {
        self.table.claim_of(self.set).store(FREE, Ordering::Release);
    }
}

/// Take the first registration nobody holds and move it to its next
/// generation.
fn register(
    region: &ShmRegion,
    name: &str,
) -> io::Result<(usize, u64, LaneHold)> {
    for owner in 0..OWNERS {
        let Some(hold) = try_hold_lane(name, owner)? else {
            continue;
        };
        // SAFETY: the mapping covers `OWNERS` aligned words at this offset.
        // The hold keeps out the registration's last holder and anyone
        // taking its claims back.
        let row = unsafe { &*generation_row(region, owner) };
        let Some(generation) = row
            .load(Ordering::Acquire)
            .checked_add(1)
            .filter(|generation| *generation <= GENERATION_MAX)
        else {
            continue;
        };
        row.store(generation, Ordering::Release);
        return Ok((owner, generation, hold));
    }
    Err(io::Error::new(
        io::ErrorKind::ResourceBusy,
        format!(
            "SHM segment {name} has no free session-table owner registration: all {OWNERS} are held"
        ),
    ))
}

fn generation_row(
    region: &ShmRegion,
    owner: usize,
) -> *const AtomicU64 {
    // SAFETY: the caller keeps `owner` below `OWNERS`, inside the mapping.
    unsafe { region.as_ptr().add(OWNERS_OFFSET).cast::<AtomicU64>().add(owner) }
}

fn segment_size<const KEY_CAPACITY: usize>() -> usize {
    SLOTS_OFFSET + CAPACITY * size_of::<SessionSlot<KEY_CAPACITY>>()
}

const _: () = assert!(OWNERS > 0 && OWNERS < u16::MAX as usize);
const _: () = assert!(size_of::<SetClaim>() == 64);
const _: () = assert!(SLOTS_OFFSET.is_multiple_of(64));

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Barrier};

    use orbit_core::Fleet;

    use super::super::super::DEFAULT_SESSION_TTL;
    use super::super::{
        Backing, SERVER_KEY_MAX, ServerSessionPrimitive, entry_hash, set_index,
    };
    use super::*;

    type Table = ClaimTable<SERVER_KEY_MAX>;

    const DOMAIN: &[u8] = b"claims";

    /// A table of its own for one test, removed when the test ends.
    struct Segment(String);

    impl Segment {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            Self(format!(
                "/rtlc-{:x}-{:x}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ))
        }

        fn open(&self) -> ServerSessionPrimitive {
            let fleet =
                Arc::new(Fleet::join("tls-claim-unit", 1).expect("fleet"));
            ServerSessionPrimitive {
                backing: Backing::Claimed(
                    Table::open_or_create(&self.0).expect("table"),
                ),
                _fleet: fleet,
            }
        }
    }

    impl Drop for Segment {
        fn drop(&mut self) {
            if let Ok(table) = Table::open_or_create(&self.0) {
                let _ = table.unlink();
            }
        }
    }

    fn table(primitive: &ServerSessionPrimitive) -> &Table {
        match &primitive.backing {
            Backing::Claimed(table) => table,
            _ => unreachable!("these tests open claimed tables"),
        }
    }

    fn set_of(key: &[u8]) -> usize {
        set_index(entry_hash(DOMAIN, key))
    }

    /// Another key that lands in the set of `key`.
    fn neighbour(key: &[u8]) -> [u8; 8] {
        (0_u64..)
            .map(u64::to_le_bytes)
            .find(|other| other != key && set_of(other) == set_of(key))
            .expect("another key in the same set")
    }

    #[test]
    fn a_set_held_by_a_live_owner_is_a_miss_and_a_refused_store() {
        let segment = Segment::new();
        let (first, second) = (segment.open(), segment.open());
        let key = 1_u64.to_le_bytes();
        let ttl = DEFAULT_SESSION_TTL;
        assert!(first.put(DOMAIN, &key, b"secret", ttl).unwrap());

        let held = table(&first).claim(set_of(&key), false).unwrap();
        assert!(held.is_some());
        // Neither the other handle nor another thread of the same one gets
        // in, and a store does not take the set from an owner that is alive.
        for handle in [&first, &second] {
            assert_eq!(handle.take(DOMAIN, &key).unwrap(), None);
            assert_eq!(handle.get(DOMAIN, &key).unwrap(), None);
            assert!(!handle.put(DOMAIN, &key, b"other", ttl).unwrap());
        }
        assert_eq!(
            second.reset().unwrap_err().kind(),
            io::ErrorKind::ResourceBusy
        );
        drop(held);

        assert_eq!(second.take(DOMAIN, &key).unwrap(), Some(b"secret".into()));
        second.reset().unwrap();
    }

    #[test]
    fn a_claim_left_by_an_owner_that_is_gone_is_taken_back_and_erased() {
        let segment = Segment::new();
        let (gone, alive) = (segment.open(), segment.open());
        let key = 2_u64.to_le_bytes();
        let set = set_of(&key);
        let ttl = DEFAULT_SESSION_TTL;
        assert!(gone.put(DOMAIN, &key, b"secret", ttl).unwrap());

        // The owner dies inside an operation: its claim stays, with bytes in
        // the set that no committed entry accounts for.
        let mut held = table(&gone).claim(set, false).unwrap().expect("claim");
        held.slots()[WAYS - 1].value[9000..9010].fill(0xcc);
        std::mem::forget(held);
        drop(gone);

        // A read never asks the kernel, so it is still a miss.
        assert_eq!(alive.take(DOMAIN, &key).unwrap(), None);
        // A store does, takes the set back and leaves only what it stored.
        let other = neighbour(&key);
        assert!(alive.put(DOMAIN, &other, b"fresh", ttl).unwrap());
        assert_eq!(alive.get(DOMAIN, &key).unwrap(), None);
        table(&alive)
            .with_set(set, false, |slots| {
                let bytes: usize = slots
                    .iter()
                    .map(|slot| {
                        slot.value.iter().filter(|byte| **byte != 0).count()
                    })
                    .sum();
                assert_eq!(bytes, b"fresh".len());
            })
            .unwrap()
            .expect("the set is free again");
        assert_eq!(alive.take(DOMAIN, &other).unwrap(), Some(b"fresh".into()));
    }

    #[test]
    fn a_registration_erases_what_its_last_holder_left_claimed() {
        let segment = Segment::new();
        let gone = segment.open();
        let key = 3_u64.to_le_bytes();
        let set = set_of(&key);
        assert!(
            gone.put(DOMAIN, &key, b"secret", DEFAULT_SESSION_TTL).unwrap()
        );
        let (owner, generation) = (table(&gone).owner, table(&gone).generation);
        std::mem::forget(table(&gone).claim(set, false).unwrap());
        drop(gone);

        let next = segment.open();
        assert_eq!(table(&next).owner, owner);
        assert_eq!(table(&next).generation, generation + 1);
        // No store was needed: the set is free and holds nothing.
        assert_eq!(next.get(DOMAIN, &key).unwrap(), None);
        table(&next)
            .with_set(set, false, |slots| {
                assert!(
                    slots.iter().all(|slot| {
                        slot.value.iter().all(|byte| *byte == 0)
                    })
                );
            })
            .unwrap()
            .expect("the set is free again");
    }

    #[test]
    fn one_take_wins_however_many_race_for_a_ticket() {
        const TAKERS: usize = 4;
        const ROUNDS: u64 = 2_000;

        let segment = Segment::new();
        // Two handles, each shared by two threads: both kinds of contender.
        let handles = [Arc::new(segment.open()), Arc::new(segment.open())];
        let start = Arc::new(Barrier::new(TAKERS + 1));
        let done = Arc::new(Barrier::new(TAKERS + 1));
        let wins = Arc::new(AtomicUsize::new(0));

        let takers: Vec<_> = (0..TAKERS)
            .map(|taker| {
                let handle = handles[taker % handles.len()].clone();
                let (start, done, wins) =
                    (start.clone(), done.clone(), wins.clone());
                std::thread::spawn(move || {
                    for round in 0..ROUNDS {
                        start.wait();
                        let key = round.to_le_bytes();
                        if handle.take(DOMAIN, &key).unwrap().is_some() {
                            wins.fetch_add(1, Ordering::Relaxed);
                        }
                        done.wait();
                    }
                })
            })
            .collect();

        for round in 0..ROUNDS {
            let key = round.to_le_bytes();
            assert!(
                handles[0]
                    .put(DOMAIN, &key, b"ticket", DEFAULT_SESSION_TTL)
                    .unwrap()
            );
            wins.store(0, Ordering::Relaxed);
            start.wait();
            done.wait();
            assert_eq!(wins.load(Ordering::Relaxed), 1, "round {round}");
        }
        for taker in takers {
            taker.join().unwrap();
        }
    }

    #[test]
    fn a_fork_child_dropping_its_copy_does_not_release_the_registration() {
        use nix::sys::wait::{WaitStatus, waitpid};
        use nix::unistd::{ForkResult, fork};

        let segment = Segment::new();
        let (parent, other) = (segment.open(), segment.open());
        let key = 4_u64.to_le_bytes();
        let set = set_of(&key);

        // SAFETY: the child opens a fresh registration and exits directly.
        match unsafe { fork() }.expect("fork") {
            ForkResult::Child => {
                let fresh = segment.open();
                drop((parent, fresh));
                // SAFETY: leaves without running the harness's own exit path.
                unsafe { libc::_exit(0) };
            }
            ForkResult::Parent { child } => {
                assert_eq!(
                    waitpid(child, None).expect("wait"),
                    WaitStatus::Exited(child, 0)
                );
            }
        }

        // Had the child's drop unlocked the shared description, this store
        // would take the set from an owner that is alive.
        let held = table(&parent).claim(set, false).unwrap();
        assert!(held.is_some());
        assert!(!other.put(DOMAIN, &key, b"x", DEFAULT_SESSION_TTL).unwrap());
    }
}
