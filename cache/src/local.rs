use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use lru::{DefaultHasher, LruCache};

use crate::layout::CacheLayout;
use crate::protocol::{CacheMutation, CacheRevision};
use crate::transport::CacheTransport;

/// An immutable snapshot of the locally owned bytes and their revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    pub value: Bytes,
    pub revision: CacheRevision,
    pub expires_at_ms: Option<u64>,
}

impl CacheEntry {
    pub fn is_expired_at(
        &self,
        now_ms: u64,
    ) -> bool {
        self.expires_at_ms.is_some_and(|deadline| deadline <= now_ms)
    }
}

/// Honest result of consulting only this process' bounded L1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheRead {
    Hit(CacheEntry),
    /// The key is not currently resident. A higher layer may consult its
    /// backing store.
    Miss,
    /// Mutation history was lost or malformed. No local hit is served until
    /// a higher layer explicitly re-establishes a coherent snapshot.
    ResyncRequired,
}

/// One logical store's bounded, process-local view of a replicated store.
///
/// # Coherence invariant
///
/// For a key `K`, call every Put or Delete for `K` this view has applied or
/// refused *observed*. The view keeps two facts:
///
/// 1. If `K` has a slot, its value is at least as new as every observed
///    mutation for `K`: a mutation slot's revision is at or above each of
///    them, and a fill slot's token sequence is at or above each of their
///    sequences (the fill saw them all in its backing read, so a mutation at
///    the token's own sequence does not replace it).
/// 2. If `K` has no slot, every observed mutation for `K` has a sequence at
///    or below `max(floor, fence(K))`.
///
/// Admission follows from them. A mutation for `K` is applied when it is
/// above the floor and newer than `K`'s slot, or, with no slot, above `K`'s
/// fence; a fill at token `t` when `t` is at or above the floor and newer than
/// `K`'s slot, or, with no slot, at or above `K`'s fence. Everything that
/// removes a slot first records its revision in `K`'s fence or under the
/// floor, so fact 2 holds again. A Reset raises the floor to its sequence and
/// removes every slot at or below it, so what stays is newer than the Reset.
/// A read therefore never returns a value older than a mutation this view has
/// applied or refused for that key, and a delayed older mutation or a stale
/// fill cannot bring a value back.
///
/// A slot is its key's own fence, so forgetting one key never hides or
/// refuses another resident key: no read checks a floor. The floor is
/// store-wide and moves only with Reset and recovery, which remove every
/// slot they cover at once. Fences are per hash bucket and move with every
/// eviction, Delete, expiry and value that could not be kept; keys sharing a
/// bucket only refuse more.
///
/// # Tags
///
/// Call every tag invalidation this view has applied *observed* as well.
/// Two more facts hold for every tag `T`:
///
/// 3. Every slot carrying `T` is newer than every observed invalidation of
///    `T`, by the same rule as a mutation for its key (a fill slot at the
///    invalidation's own sequence read a backing store it had reached).
/// 4. Every observed invalidation of `T` has a sequence at or below the
///    fence of `T`'s bucket, the same fences keys use, indexed by `T`'s hash.
///
/// An invalidation raises its tags' fences and removes, through an index of
/// resident keys by tag, every slot carrying one that it covers, fencing
/// each key at its slot's own revision; that keeps 3 without a read ever
/// looking at a tag. A put carrying tags is admitted by its key first, as any
/// put; a put so admitted at or below one of its tags' fences is observed and
/// covered, so its key is forgotten at the put's sequence, which keeps 1 and
/// 2. A fill below one of its tags' fences is refused and changes nothing. A
/// tag sharing a bucket with another tag or key only refuses more.
#[derive(Clone)]
pub struct LocalCache {
    inner: Arc<Mutex<LocalState>>,
    budget: Arc<LocalBudget>,
    coherent: Arc<AtomicBool>,
}

struct LocalState {
    slots: LruCache<Arc<[u8]>, LocalSlot>,
    /// Store-wide: no mutation at or below it is admitted, and no fill below
    /// it. Raised by Reset and recovery.
    floor: u64,
    /// The highest sequence forgotten among the keys hashing to each bucket,
    /// and the highest invalidation among the tags hashing to it.
    /// One bucket per entry the store can hold, rounded up to a power of two.
    fences: Box<[u64]>,
    fence_hasher: DefaultHasher,
    /// Resident keys by tag. Allocates nothing until a tagged slot.
    tagged: TagIndex,
}

/// Exactly the resident slots carrying each tag. An emptied set is dropped,
/// so the index is bounded by the slots and their tags.
#[derive(Default)]
struct TagIndex(HashMap<u64, HashSet<Arc<[u8]>, DefaultHasher>, DefaultHasher>);

impl TagIndex {
    fn link(
        &mut self,
        key: &Arc<[u8]>,
        tags: &[u64],
    ) {
        for &tag in tags {
            self.0.entry(tag).or_default().insert(key.clone());
        }
    }

    fn unlink(
        &mut self,
        key: &[u8],
        tags: &[u64],
    ) {
        for &tag in tags {
            self.unlink_one(key, tag);
        }
    }

    fn unlink_one(
        &mut self,
        key: &[u8],
        tag: u64,
    ) {
        if let Some(members) = self.0.get_mut(&tag) {
            members.remove(key);
            if members.is_empty() {
                self.0.remove(&tag);
            }
        }
    }

    fn clear(&mut self) {
        // Free the tables too: what emptied the view was not a tag.
        self.0 = HashMap::default();
    }
}

pub(crate) struct LocalBudget {
    pub(crate) limit: usize,
    used: AtomicUsize,
}

impl LocalBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self { limit, used: AtomicUsize::new(0) }
    }

    fn reserve(
        &self,
        len: usize,
    ) -> bool {
        // Every insert holds the connection cursor, so reservations are
        // serialized. Anything else can only lower the charge, by dropping a
        // value.
        if len > self.limit - self.used.load(Ordering::Relaxed) {
            return false;
        }
        self.used.fetch_add(len, Ordering::Relaxed);
        true
    }
}

struct LocalValue {
    bytes: Bytes,
    budget: Arc<LocalBudget>,
}

impl Drop for LocalValue {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes.len(), Ordering::Relaxed);
    }
}

struct LocalSlot {
    value: LocalValue,
    revision: CacheRevision,
    expires_at_ms: Option<u64>,
    /// A backing fill, current as of every mutation up to its revision's
    /// sequence, rather than one mutation's value.
    fill: bool,
    /// Sorted, distinct tag hashes; empty, and unallocated, when untagged.
    tags: Box<[u64]>,
}

impl LocalSlot {
    /// Whether `revision` is newer than this slot's value.
    fn precedes(
        &self,
        revision: CacheRevision,
    ) -> bool {
        if self.fill {
            revision.sequence > self.revision.sequence
        } else {
            revision > self.revision
        }
    }
}

/// What a value claims about its revision when it is kept.
#[derive(Clone, Copy)]
enum Admission {
    /// A mutation: at or below a fence it may be a delayed older write.
    Mutation,
    /// A backing read: it saw every write up to its revision's sequence, so a
    /// fence at that sequence does not make it stale.
    Fill,
}

impl Admission {
    /// Whether a value at `sequence` is newer than `fence`.
    fn above(
        self,
        sequence: u64,
        fence: u64,
    ) -> bool {
        match self {
            Self::Mutation => sequence > fence,
            Self::Fill => sequence >= fence,
        }
    }
}

/// What to keep: the value's revision, expiry and tags.
struct Version {
    revision: CacheRevision,
    expires_at_ms: Option<u64>,
    tags: Box<[u64]>,
}

pub(crate) enum ApplyOutcome {
    Applied,
    Ignored,
    PayloadUnavailable { key: Bytes },
}

impl LocalCache {
    pub(crate) fn with_connection(
        capacity: NonZeroUsize,
        budget: Arc<LocalBudget>,
        coherent: Arc<AtomicBool>,
    ) -> Self {
        let buckets = capacity
            .get()
            .checked_next_power_of_two()
            .unwrap_or(1 << (usize::BITS - 1));
        Self {
            inner: Arc::new(Mutex::new(LocalState {
                slots: LruCache::new(capacity),
                floor: 0,
                fences: vec![0; buckets].into_boxed_slice(),
                fence_hasher: DefaultHasher::default(),
                tagged: TagIndex::default(),
            })),
            budget,
            coherent,
        }
    }

    pub fn read(
        &self,
        key: &[u8],
    ) -> CacheRead {
        self.read_with_clock(key, now_ms)
    }

    pub fn read_at(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> CacheRead {
        self.read_with_clock(key, || now_ms)
    }

    fn read_with_clock(
        &self,
        key: &[u8],
        now_ms: impl FnOnce() -> u64,
    ) -> CacheRead {
        let mut state =
            self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if !self.is_coherent() {
            return CacheRead::ResyncRequired;
        }
        let Some(slot) = state.slots.get_mut(key) else {
            return CacheRead::Miss;
        };
        if !slot.expires_at_ms.is_some_and(|deadline| deadline <= now_ms()) {
            return CacheRead::Hit(CacheEntry {
                value: slot.value.bytes.clone(),
                revision: slot.revision,
                expires_at_ms: slot.expires_at_ms,
            });
        }
        // Expiry is an honest miss. The fence keeps a delayed older Put from
        // resurrecting the value.
        let sequence = slot.revision.sequence;
        state.forget(key, sequence);
        CacheRead::Miss
    }

    /// Resident entries, including expired ones no read has reached yet.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|error| error.into_inner()).slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_coherent(&self) -> bool {
        self.coherent.load(Ordering::Acquire)
    }

    /// Clear uncertain local state after transport loss.
    pub fn require_resync(&self) {
        self.coherent.store(false, Ordering::Release);
        let mut state =
            self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let newest =
            state.slots.iter().map(|(_, slot)| slot.revision.sequence).max();
        if let Some(newest) = newest {
            state.raise_floor(newest);
        }
        state.clear();
    }

    pub(crate) fn recover_from_backing(
        &self,
        revision_floor: u64,
    ) {
        let mut state =
            self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.clear();
        state.raise_floor(revision_floor);
    }

    /// The ring restarts its sequences, so every fence and the floor restart
    /// with it.
    pub(crate) fn reset_after_transport_reset(&self) {
        let mut state =
            self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.clear();
        state.floor = 0;
        state.fences.fill(0);
    }

    pub(crate) fn apply<L: CacheLayout>(
        &self,
        transport: &CacheTransport<L>,
        mutation: CacheMutation,
        reclaim: impl FnMut() -> bool,
    ) -> ApplyOutcome {
        let mut state =
            self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if !self.is_coherent() {
            return ApplyOutcome::Ignored;
        }

        match mutation {
            CacheMutation::Put {
                store: _,
                key,
                revision,
                expires_at_ms,
                payload,
                tags,
            } => {
                if !state.admits(&key, revision, Admission::Mutation) {
                    return ApplyOutcome::Ignored;
                }
                if state.covered(&tags, revision, Admission::Mutation) {
                    // Applied and invalidated: the key is a miss at the
                    // Put's revision, without copying its payload.
                    state.forget(&key, revision.sequence);
                    return ApplyOutcome::Applied;
                }
                let Some(entry) =
                    transport.arena().get_address(payload.address).filter(
                        |entry| entry.body().len() as u64 == payload.value_len,
                    )
                else {
                    state.forget(&key, revision.sequence);
                    return ApplyOutcome::PayloadUnavailable { key };
                };
                state.put_value(
                    &self.budget,
                    &key,
                    entry.body(),
                    Version { revision, expires_at_ms, tags },
                    Admission::Mutation,
                    reclaim,
                );
                ApplyOutcome::Applied
            }
            CacheMutation::Delete { store: _, key, revision } => {
                if !state.admits(&key, revision, Admission::Mutation) {
                    return ApplyOutcome::Ignored;
                }
                state.forget(&key, revision.sequence);
                ApplyOutcome::Applied
            }
            CacheMutation::Reset { store: _, revision } => {
                // Fences do not gate a Reset: residents survive them, so a
                // delayed Reset may still have residents to remove.
                if state.reset(revision.sequence) {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::Ignored
                }
            }
            CacheMutation::InvalidateTags { store: _, tags, revision } => {
                // Like a Reset, a delayed invalidation still removes the
                // residents it covers.
                if state.invalidate(&tags, revision) {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::Ignored
                }
            }
        }
    }

    pub(crate) fn install_local(
        &self,
        key: &[u8],
        value: &[u8],
        revision: CacheRevision,
        expires_at_ms: Option<u64>,
        tags: Box<[u64]>,
        reclaim: impl FnMut() -> bool,
    ) {
        let mut state =
            self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if !self.is_coherent()
            || !state.admits(key, revision, Admission::Mutation)
        {
            return;
        }
        if state.covered(&tags, revision, Admission::Mutation) {
            // An invalidation allocated after this put already reached the
            // view, from another thread.
            state.forget(key, revision.sequence);
            return;
        }
        state.put_value(
            &self.budget,
            key,
            value,
            Version { revision, expires_at_ms, tags },
            Admission::Mutation,
            reclaim,
        );
    }

    /// Keep a value read from a backing store as current at `revision`.
    ///
    /// The revision is the last one allocated before the read, so the value
    /// has seen every backing write published up to it: a fence or floor at
    /// that very sequence leaves it current, while a slot this view already
    /// holds for the key at or above it makes the fill stale. A tag fence
    /// above it means an invalidation the backing read may predate. Publishes
    /// nothing; returns whether the value is kept.
    pub(crate) fn install_fill(
        &self,
        key: &[u8],
        value: &[u8],
        revision: CacheRevision,
        expires_at_ms: Option<u64>,
        tags: Box<[u64]>,
        reclaim: impl FnMut() -> bool,
    ) -> bool {
        let mut state =
            self.inner.lock().unwrap_or_else(|error| error.into_inner());
        self.is_coherent()
            && state.admits(key, revision, Admission::Fill)
            && !state.covered(&tags, revision, Admission::Fill)
            && state.put_value(
                &self.budget,
                key,
                value,
                Version { revision, expires_at_ms, tags },
                Admission::Fill,
                reclaim,
            )
    }

    pub(crate) fn evict_value(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .evict_value()
    }

    pub(crate) fn install_missing(
        &self,
        key: &[u8],
        revision: CacheRevision,
    ) {
        let mut state =
            self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if self.is_coherent()
            && state.admits(key, revision, Admission::Mutation)
        {
            state.forget(key, revision.sequence);
        }
    }

    pub(crate) fn reset_local(
        &self,
        revision: CacheRevision,
    ) {
        let mut state =
            self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if self.is_coherent() {
            state.reset(revision.sequence);
        }
    }

    pub(crate) fn invalidate_local(
        &self,
        tags: &[u64],
        revision: CacheRevision,
    ) {
        let mut state =
            self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if self.is_coherent() {
            state.invalidate(tags, revision);
        }
    }
}

/// The fence bucket of a key. The bucket count is a power of two.
fn key_bucket(
    hasher: &DefaultHasher,
    buckets: usize,
    key: &[u8],
) -> usize {
    hasher.hash_one(key) as usize & (buckets - 1)
}

/// The fence bucket of a tag, whose hash is already uniform.
fn tag_bucket(
    buckets: usize,
    tag: u64,
) -> usize {
    tag as usize & (buckets - 1)
}

fn raise(
    fence: &mut u64,
    sequence: u64,
) {
    *fence = (*fence).max(sequence);
}

impl LocalState {
    /// Whether `revision` is newer, for `key`, than everything this view has
    /// observed: the key's own slot when it has one, its fence when not.
    fn admits(
        &self,
        key: &[u8],
        revision: CacheRevision,
        admission: Admission,
    ) -> bool {
        let sequence = revision.sequence;
        admission.above(sequence, self.floor)
            && match self.slots.peek(key) {
                Some(slot) => slot.precedes(revision),
                None => admission.above(
                    sequence,
                    self.fences[key_bucket(
                        &self.fence_hasher,
                        self.fences.len(),
                        key,
                    )],
                ),
            }
    }

    /// Whether an invalidation this view has observed covers a value at
    /// `revision` carrying `tags`.
    fn covered(
        &self,
        tags: &[u64],
        revision: CacheRevision,
        admission: Admission,
    ) -> bool {
        tags.iter().any(|&tag| {
            !admission.above(
                revision.sequence,
                self.fences[tag_bucket(self.fences.len(), tag)],
            )
        })
    }

    fn raise_fence(
        &mut self,
        key: &[u8],
        sequence: u64,
    ) {
        let bucket = key_bucket(&self.fence_hasher, self.fences.len(), key);
        raise(&mut self.fences[bucket], sequence);
    }

    fn raise_floor(
        &mut self,
        sequence: u64,
    ) {
        self.floor = self.floor.max(sequence);
    }

    /// Empty the view and its tag index; fences and floor stay.
    fn clear(&mut self) {
        self.slots.clear();
        self.tagged.clear();
    }

    /// Remove `key`'s slot, if any, from the view and the tag index.
    fn take(
        &mut self,
        key: &[u8],
    ) -> Option<(Arc<[u8]>, LocalSlot)> {
        let (key, slot) = self.slots.pop_entry(key)?;
        self.tagged.unlink(&key, &slot.tags);
        Some((key, slot))
    }

    /// Drop `key`'s slot, if any, and fence `sequence`, which is at or above
    /// the slot's.
    fn forget(
        &mut self,
        key: &[u8],
        sequence: u64,
    ) {
        self.take(key);
        self.raise_fence(key, sequence);
    }

    /// Insert under a key known to be absent. Anything `push` returns is
    /// therefore a capacity eviction, which is fenced.
    fn insert_slot(
        &mut self,
        key: Arc<[u8]>,
        slot: LocalSlot,
    ) {
        self.tagged.link(&key, &slot.tags);
        if let Some((evicted_key, evicted)) = self.slots.push(key, slot) {
            self.tagged.unlink(&evicted_key, &evicted.tags);
            self.raise_fence(&evicted_key, evicted.revision.sequence);
        }
    }

    /// Keep `value` under `key`, admitted by the caller. The old slot is
    /// dropped before its replacement is charged, keeping its key buffer.
    /// Returns whether the value is kept; when it is not, the key is fenced.
    fn put_value(
        &mut self,
        budget: &Arc<LocalBudget>,
        key: &[u8],
        value: &[u8],
        version: Version,
        admission: Admission,
        mut reclaim: impl FnMut() -> bool,
    ) -> bool {
        let Version { revision, expires_at_ms, tags } = version;
        let old = self.take(key);
        let forgotten = match admission {
            // An observed mutation, newer than the old slot.
            Admission::Mutation => Some(revision.sequence),
            // A fill observed nothing; only the slot it replaced is forgotten.
            Admission::Fill => {
                old.as_ref().map(|(_, slot)| slot.revision.sequence)
            }
        };
        let key_buffer = old.map(|(key, _)| key);
        if value.len() <= budget.limit {
            // Evictions here fence other keys only: this key was admitted
            // against its own slot or fence, which they do not change.
            loop {
                if budget.reserve(value.len()) {
                    let slot = LocalSlot {
                        value: LocalValue {
                            bytes: Bytes::copy_from_slice(value),
                            budget: budget.clone(),
                        },
                        revision,
                        expires_at_ms,
                        fill: matches!(admission, Admission::Fill),
                        tags,
                    };
                    self.insert_slot(
                        key_buffer.unwrap_or_else(|| Arc::from(key)),
                        slot,
                    );
                    return true;
                }
                if !self.evict_value() && !reclaim() {
                    break;
                }
            }
        }
        if let Some(sequence) = forgotten {
            self.raise_fence(key, sequence);
        }
        false
    }

    /// Free the least recently used value, fencing its revision.
    fn evict_value(&mut self) -> bool {
        let Some((key, slot)) = self.slots.pop_lru() else { return false };
        self.tagged.unlink(&key, &slot.tags);
        self.raise_fence(&key, slot.revision.sequence);
        true
    }

    /// Refuse every mutation at or below `sequence` and drop every slot it
    /// covers. Slots above it are newer than the Reset and stay. A Reset at or
    /// below the floor covers nothing left: only fills at the floor's own
    /// sequence can be resident there, and they saw it.
    fn reset(
        &mut self,
        sequence: u64,
    ) -> bool {
        if sequence <= self.floor {
            return false;
        }
        self.floor = sequence;
        let Self { slots, tagged, .. } = self;
        slots.retain(|key, slot| {
            let keep = slot.revision.sequence > sequence;
            if !keep {
                tagged.unlink(key, &slot.tags);
            }
            keep
        });
        true
    }

    /// Raise each tag's fence to the invalidation and remove every resident
    /// carrying one that the invalidation is newer than, fencing its key at
    /// its own revision. Residents newer than the invalidation stay. One at
    /// or below the floor has nothing left to remove, and the floor already
    /// refuses everything it would fence.
    fn invalidate(
        &mut self,
        tags: &[u64],
        revision: CacheRevision,
    ) -> bool {
        if revision.sequence <= self.floor {
            return false;
        }
        let Self { slots, fences, fence_hasher, tagged, .. } = self;
        let buckets = fences.len();
        for &tag in tags {
            raise(&mut fences[tag_bucket(buckets, tag)], revision.sequence);
            let Some(mut members) = tagged.0.remove(&tag) else { continue };
            members.retain(|key| {
                // Every indexed key is resident; one that is not would only
                // be an index entry to drop.
                let newer = slots
                    .peek(key.as_ref())
                    .is_some_and(|slot| !slot.precedes(revision));
                if !newer
                    && let Some((key, slot)) = slots.pop_entry(key.as_ref())
                {
                    for &other in
                        slot.tags.iter().filter(|&&other| other != tag)
                    {
                        tagged.unlink_one(&key, other);
                    }
                    raise(
                        &mut fences[key_bucket(fence_hasher, buckets, &key)],
                        slot.revision.sequence,
                    );
                }
                newer
            });
            if !members.is_empty() {
                tagged.0.insert(tag, members);
            }
        }
        true
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(
        capacity: usize,
        bytes: usize,
    ) -> LocalCache {
        LocalCache::with_connection(
            NonZeroUsize::new(capacity).unwrap(),
            Arc::new(LocalBudget::new(bytes)),
            Arc::new(AtomicBool::new(true)),
        )
    }

    fn at(
        sequence: u64,
        id: u64,
    ) -> CacheRevision {
        CacheRevision {
            sequence,
            mutation_id: orbit_core::NetId64::from_raw(id),
        }
    }

    fn no_tags() -> Box<[u64]> {
        Box::default()
    }

    fn holds(
        cache: &LocalCache,
        key: &[u8],
        value: &[u8],
    ) -> bool {
        matches!(cache.read(key), CacheRead::Hit(entry) if entry.value.as_ref() == value)
    }

    #[test]
    fn read_checks_the_clock_only_for_a_value_with_expiry() {
        let cache = view(2, 16);
        cache.install_local(
            b"plain",
            b"value",
            at(1, 0),
            None,
            no_tags(),
            || false,
        );
        cache.install_local(
            b"ttl",
            b"value",
            at(1, 0),
            Some(10),
            no_tags(),
            || false,
        );

        let no_clock = || panic!("unexpected clock read");
        assert!(matches!(
            cache.read_with_clock(b"plain", no_clock),
            CacheRead::Hit(_)
        ));
        assert_eq!(cache.read_with_clock(b"absent", no_clock), CacheRead::Miss);
        assert!(matches!(cache.read_at(b"ttl", 9), CacheRead::Hit(_)));
        assert_eq!(cache.read_with_clock(b"ttl", || 10), CacheRead::Miss);
        assert_eq!(cache.read_with_clock(b"ttl", no_clock), CacheRead::Miss);
    }

    #[test]
    fn a_fill_is_kept_until_a_revision_after_it_says_otherwise() {
        let cache = view(4, 64);
        let token = |sequence| at(sequence, 0);

        assert!(cache.install_fill(
            b"cold",
            b"backing",
            token(3),
            None,
            no_tags(),
            || false
        ));
        assert!(matches!(cache.read(b"cold"), CacheRead::Hit(_)));

        // A delete this view applied after the token is newer than the fill;
        // one at the token's own sequence is in what the fill read.
        cache.install_missing(b"deleted", at(6, 1));
        assert!(!cache.install_fill(
            b"deleted",
            b"old",
            token(5),
            None,
            no_tags(),
            || false
        ));
        assert_eq!(cache.read(b"deleted"), CacheRead::Miss);
        assert!(cache.install_fill(
            b"deleted",
            b"new",
            token(6),
            None,
            no_tags(),
            || false
        ));

        // A floor at the token's sequence leaves the fill current; one past
        // it does not.
        cache.reset_local(at(7, 1));
        assert!(cache.install_fill(
            b"after",
            b"backing",
            token(7),
            None,
            no_tags(),
            || false
        ));
        assert!(!cache.install_fill(
            b"before",
            b"backing",
            token(6),
            None,
            no_tags(),
            || false
        ));

        // A mutation at the token's sequence is in what the fill read; one
        // published after the token replaces the fill.
        cache.install_local(
            b"after",
            b"seen",
            at(7, 2),
            None,
            no_tags(),
            || false,
        );
        assert!(holds(&cache, b"after", b"backing"));
        cache.install_local(
            b"after",
            b"newer",
            at(8, 1),
            None,
            no_tags(),
            || false,
        );
        assert!(holds(&cache, b"after", b"newer"));
    }

    #[test]
    fn an_eviction_fences_only_the_key_it_evicts() {
        let cache = view(2, 64);
        cache.install_local(
            b"older",
            b"value",
            at(1, 1),
            None,
            no_tags(),
            || false,
        );
        cache.install_local(
            b"newer",
            b"value",
            at(2, 1),
            None,
            no_tags(),
            || false,
        );
        // Read the older one, so the newer one is least recently used.
        assert!(holds(&cache, b"older", b"value"));

        // A third key evicts `newer`. `older`, below its revision, survives.
        cache.install_local(
            b"third",
            b"value",
            at(3, 1),
            None,
            no_tags(),
            || false,
        );
        assert_eq!(cache.len(), 2);
        assert!(holds(&cache, b"older", b"value"));
        assert!(holds(&cache, b"third", b"value"));
        assert_eq!(cache.read(b"newer"), CacheRead::Miss);

        // The evicted key refuses a delayed write and a stale fill at or
        // below the revision it had; a fill that saw it is kept.
        cache.install_local(
            b"newer",
            b"stale",
            at(2, 0),
            None,
            no_tags(),
            || false,
        );
        assert_eq!(cache.read(b"newer"), CacheRead::Miss);
        assert!(!cache.install_fill(
            b"newer",
            b"stale",
            at(1, 0),
            None,
            no_tags(),
            || false
        ));
        assert!(cache.install_fill(
            b"newer",
            b"backing",
            at(3, 0),
            None,
            no_tags(),
            || false
        ));
        assert!(holds(&cache, b"newer", b"backing"));
    }

    #[test]
    fn a_resident_is_fenced_by_its_own_revision_not_by_another_keys() {
        // A resident at 100 while another key's eviction fenced 50: a
        // delayed Put at 40 must leave the resident alone, and a delayed Put
        // at 70 must not replace it either.
        let cache = view(1, 64);
        cache.install_local(
            b"evicted",
            b"value",
            at(50, 1),
            None,
            no_tags(),
            || false,
        );
        cache.install_local(
            b"resident",
            b"current",
            at(100, 1),
            None,
            no_tags(),
            || false,
        );
        assert_eq!(cache.read(b"evicted"), CacheRead::Miss);
        cache.install_local(
            b"resident",
            b"old",
            at(40, 1),
            None,
            no_tags(),
            || false,
        );
        cache.install_local(
            b"resident",
            b"older",
            at(70, 1),
            None,
            no_tags(),
            || false,
        );
        assert!(holds(&cache, b"resident", b"current"));
    }

    #[test]
    fn deletes_of_absent_keys_take_no_capacity_and_still_fence() {
        let cache = view(2, 64);
        cache
            .install_local(b"a", b"value", at(1, 1), None, no_tags(), || false);
        cache
            .install_local(b"b", b"value", at(2, 1), None, no_tags(), || false);
        for sequence in 3..20 {
            cache.install_missing(
                format!("gone-{sequence}").as_bytes(),
                at(sequence, 1),
            );
        }
        assert!(holds(&cache, b"a", b"value"));
        assert!(holds(&cache, b"b", b"value"));
        // A delayed Put older than the Delete of an absent key is refused.
        cache.install_local(
            b"gone-9",
            b"stale",
            at(8, 1),
            None,
            no_tags(),
            || false,
        );
        assert_eq!(cache.read(b"gone-9"), CacheRead::Miss);
    }

    #[test]
    fn a_delayed_reset_removes_what_it_covers_and_keeps_what_is_newer() {
        let cache = view(2, 64);
        cache.install_local(
            b"low",
            b"value",
            at(5, 1),
            None,
            no_tags(),
            || false,
        );
        cache.install_local(
            b"high",
            b"value",
            at(9, 1),
            None,
            no_tags(),
            || false,
        );
        // Fence a key above the Reset, as an eviction or Delete would.
        cache.install_missing(b"other", at(10, 1));
        cache.reset_local(at(7, 1));
        assert_eq!(cache.read(b"low"), CacheRead::Miss);
        assert!(holds(&cache, b"high", b"value"));
        cache.install_local(
            b"low",
            b"stale",
            at(6, 1),
            None,
            no_tags(),
            || false,
        );
        assert_eq!(cache.read(b"low"), CacheRead::Miss);
    }
}
