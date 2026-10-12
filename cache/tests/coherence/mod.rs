use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use orbit_core::Fleet;

use crate::{Cache, CacheMutation, CacheRead, DefaultCacheLayout};

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
    store.local().apply(cache.transport(), current);
    let CacheRead::Hit(held) = store.read_at(b"key", deadline - 1) else {
        panic!("before TTL");
    };
    assert_eq!(store.read_at(b"key", deadline), CacheRead::Miss);
    assert_eq!(store.local().len(), 0);
    store.local().apply(cache.transport(), old);
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
    store.local().apply(cache.transport(), put.clone());
    let CacheRead::Hit(held) = store.read(b"key") else {
        panic!("hit");
    };
    let delete = cache.transport().publish_delete(b"values", b"key").unwrap();
    store.local().apply(cache.transport(), delete);
    store.local().apply(cache.transport(), put.clone());
    assert_eq!(store.read(b"key"), CacheRead::Miss);
    let reset = cache.transport().publish_reset(b"values").unwrap();
    store.local().apply(cache.transport(), reset);
    store.local().apply(cache.transport(), put);
    assert_eq!(store.read(b"key"), CacheRead::Miss);
    store.put(b"new", b"new", None).unwrap();
    store.put(b"other", b"other", None).unwrap();
    cache.transport().arena().reset().unwrap();
    assert_eq!(store.read(b"other"), CacheRead::Miss);
    assert_eq!(held.value, b"old"[..]);
}
