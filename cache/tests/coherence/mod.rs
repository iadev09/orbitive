use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;

use orbit_core::Fleet;

use crate::layout::MutationRecord;
use crate::{Cache, CacheMutation, CacheRead, DefaultCacheLayout, protocol};

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
    store.put(b"new", b"new", None).unwrap();
    store.put(b"other", b"other", None).unwrap();
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
                    )
                    .unwrap(),
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
