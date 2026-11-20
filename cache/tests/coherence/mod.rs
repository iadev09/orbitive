use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;

use orbit_core::Fleet;

use crate::layout::MutationRecord;
use crate::{
    Cache, CacheMutation, CacheRead, DefaultCacheLayout, Fill, protocol,
};

#[test]
fn ttl_boundary_and_delayed_put_do_not_resurrect_an_expired_value() {
    let cache = Cache::<DefaultCacheLayout>::new(Arc::new(
        Fleet::join("example", 1).unwrap(),
    ))
    .unwrap();
    let store = cache.open_default_store().unwrap();
    let old = cache
        .transport()
        .publish_put(b"default", b"key", b"old", None)
        .unwrap();
    let current = cache
        .transport()
        .publish_put(
            b"default",
            b"key",
            b"current",
            Some(Duration::from_secs(1)),
        )
        .unwrap();
    let CacheMutation::Put { expires_at_ms: Some(deadline), .. } = &current
    else {
        panic!("expiry");
    };
    let deadline = *deadline;
    store.local().apply(cache.transport(), current, || false);
    let CacheRead::Hit(held) = store.read_at(b"key", deadline - 1) else {
        panic!("before TTL");
    };
    assert_eq!(store.read_at(b"key", deadline), CacheRead::Miss);
    assert_eq!(store.local().len(), 0);
    store.local().apply(cache.transport(), old, || false);
    assert_eq!(store.read_at(b"key", deadline), CacheRead::Miss);
    assert_eq!(held.value, b"current"[..]);
}

#[test]
fn delete_reset_and_l1_eviction_fence_delayed_puts_without_retaining_bodies() {
    let cache = Cache::<DefaultCacheLayout>::new(Arc::new(
        Fleet::join("example", 1).unwrap(),
    ))
    .unwrap();
    let store =
        cache.open_store(b"values", NonZeroUsize::new(1).unwrap()).unwrap();
    let put =
        cache.transport().publish_put(b"values", b"key", b"old", None).unwrap();
    store.local().apply(cache.transport(), put.clone(), || false);
    let CacheRead::Hit(held) = store.read(b"key") else {
        panic!("hit");
    };
    let delete = cache.transport().publish_delete(b"values", b"key").unwrap();
    store.local().apply(cache.transport(), delete, || false);
    store.local().apply(cache.transport(), put.clone(), || false);
    assert_eq!(store.read(b"key"), CacheRead::Miss);
    let reset = cache.transport().publish_reset(b"values").unwrap();
    store.local().apply(cache.transport(), reset, || false);
    store.local().apply(cache.transport(), put, || false);
    assert_eq!(store.read(b"key"), CacheRead::Miss);
    let stale = cache
        .transport()
        .publish_put(b"values", b"new", b"stale", None)
        .unwrap();
    store.put(b"new", b"new", None).unwrap();
    store.put(b"other", b"other", None).unwrap();
    // `other` evicted `new`; the floor that eviction set fences an older Put.
    store.local().apply(cache.transport(), stale, || false);
    assert_eq!(store.read(b"new"), CacheRead::Miss);
    cache.transport().arena().reset().unwrap();
    assert!(matches!(store.read(b"other"), CacheRead::Hit(_)));
    assert_eq!(held.value, b"old"[..]);
}

#[test]
fn recovery_serializes_with_a_reserved_publication_and_another_drain() {
    type Record = MutationRecord<DefaultCacheLayout>;
    let fleet = Arc::new(Fleet::join("cache_cut", 1).unwrap());
    let writer = Cache::<DefaultCacheLayout>::new(fleet.clone()).unwrap();
    let peer = Cache::<DefaultCacheLayout>::new(fleet.clone()).unwrap();
    let store = peer.open_default_store().unwrap();
    let publisher = writer.open_default_store().unwrap();
    publisher.put(b"old", b"visible", None).unwrap();
    assert_eq!(peer.poll().applied, 1);
    store.local().require_resync();

    let barrier = Barrier::new(3);
    let cut_sequence = std::thread::scope(|scope| {
        fleet
            .with_ring_publication::<Record, _>(|| {
                let sequence = fleet.next_ring_version::<Record>();
                scope.spawn(|| {
                    barrier.wait();
                    peer.poll();
                });
                scope.spawn(|| {
                    barrier.wait();
                    peer.recover_from_backing().unwrap();
                });
                barrier.wait();
                fleet.publish::<Record>(
                    protocol::FRAME_KIND_DELETE,
                    sequence,
                    protocol::encode_delete::<DefaultCacheLayout>(
                        b"default", b"old",
                    ),
                );
                sequence
            })
            .unwrap()
    });
    assert_eq!(store.read(b"old"), CacheRead::Miss);
    assert!(peer.poll().is_empty());
    let next = publisher.put(b"old", b"new", None).unwrap();
    assert!(next.sequence > cut_sequence);
    assert_eq!(peer.poll().applied, 1);
    assert!(
        matches!(store.read(b"old"), CacheRead::Hit(entry) if entry.value == b"new"[..])
    );
}

#[test]
fn a_tag_invalidation_refuses_older_tagged_puts_and_fills_it_covers() {
    let cache = Cache::<DefaultCacheLayout>::new(Arc::new(
        Fleet::join("example", 1).unwrap(),
    ))
    .unwrap();
    let store = cache.open_default_store().unwrap();
    let transport = cache.transport();
    let tags = |names: &[&str]| store.tag_key.hash_all(names).unwrap();
    let apply = |mutation| store.local().apply(transport, mutation, || false);
    let holds = |key: &[u8], value: &[u8]| matches!(store.read(key), CacheRead::Hit(entry) if entry.value == value);

    // `key` holds an untagged value and `other` one tagged `posts`; a newer
    // Put of `key` carrying `users` is delayed past an invalidation of it.
    let old = transport.publish_put(b"default", b"key", b"old", None).unwrap();
    let covered = transport
        .publish_put_tagged(
            b"default",
            b"key",
            b"stale",
            None,
            &tags(&["users"]),
        )
        .unwrap();
    let other = transport
        .publish_put_tagged(
            b"default",
            b"other",
            b"kept",
            None,
            &tags(&["posts"]),
        )
        .unwrap();
    let fill_token = store.fill_token();
    let invalidation = transport
        .publish_invalidate_tags(b"default", &tags(&["users"]))
        .unwrap();
    let after = store.fill_token();
    apply(old.clone());
    apply(other);
    assert!(holds(b"key", b"old"));
    apply(invalidation);

    // The delayed Put is newer than the resident, so the resident cannot
    // stay; it is covered, so it is not kept either. Neither can the
    // resident's own older Put come back.
    apply(covered);
    assert_eq!(store.read(b"key"), CacheRead::Miss);
    apply(old);
    assert_eq!(store.read(b"key"), CacheRead::Miss);
    assert!(holds(b"other", b"kept"));

    // A fill whose backing read may predate the invalidation is refused; one
    // that read after it is kept until a later invalidation reaches it. A
    // fill without tags is not reached by any.
    assert_eq!(
        store
            .fill_tagged(fill_token, b"filled", b"stale", None, &["users"])
            .unwrap(),
        Fill::Invalidated
    );
    assert_eq!(
        store
            .fill_tagged(after, b"filled", b"fresh", None, &["users"])
            .unwrap(),
        Fill::Kept
    );
    assert_eq!(
        store.fill(after, b"untagged", b"value", None).unwrap(),
        Fill::Kept
    );
    store.invalidate_tags(&["users"]).unwrap();
    assert_eq!(store.read(b"filled"), CacheRead::Miss);
    assert!(holds(b"untagged", b"value"));
    assert!(holds(b"other", b"kept"));
}
