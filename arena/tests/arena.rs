use std::sync::Arc;

use orbit_core::Fleet;

use orbit_arena::{ARENA_BYTES, ARENA_RECORD_MAX, Arena, Error, Key, Record};

fn arena() -> Arena {
    Arena::new(Arc::new(Fleet::join("arena-test", 1).unwrap())).unwrap()
}

#[test]
fn put_get_replace_forget() {
    let arena = arena();
    let key = Key::new(7);
    assert!(arena.get(key).is_none());

    arena
        .put(key, Record::body(b"hello").encoded(b"h", 1).stamp(42).extra(5))
        .unwrap();
    let entry = arena.get(key).unwrap();
    assert_eq!(entry.body(), b"hello");
    assert_eq!(entry.encoded(), Some((&b"h"[..], 1)));
    assert_eq!((entry.stamp(), entry.extra()), (42, 5));
    drop(entry);

    arena.put(key, Record::body(b"world")).unwrap();
    let entry = arena.get(key).unwrap();
    assert_eq!(entry.body(), b"world");
    assert!(entry.encoded().is_none());
    assert_eq!(arena.len(), 1);
    drop(entry);

    assert!(arena.forget(key).unwrap());
    assert!(arena.get(key).is_none());
    assert!(!arena.forget(key).unwrap());
}

#[test]
fn the_ring_evicts_what_the_cursor_reaches_and_skips_what_is_pinned() {
    let arena = arena();
    let chunk = vec![0xab_u8; ARENA_RECORD_MAX];
    let records = ARENA_BYTES / ARENA_RECORD_MAX; // fills the ring exactly
    for n in 0..records {
        arena.put(Key::new(n as u128), Record::body(&chunk)).unwrap();
    }
    assert_eq!(arena.len(), records);

    // Pin the first record, then wrap: it must survive, the second must go.
    let pinned = arena.get(Key::new(0)).unwrap();
    arena.put(Key::new(1_000), Record::body(&chunk)).unwrap();
    assert_eq!(pinned.body().len(), ARENA_RECORD_MAX);
    assert!(arena.get(Key::new(0)).is_some(), "a pinned record is not evicted");
    assert!(arena.get(Key::new(1)).is_none(), "the cursor moved past the pin");
    drop(pinned);

    let too_big = vec![0_u8; ARENA_RECORD_MAX + 1];
    assert!(matches!(
        arena.put(Key::new(9), Record::body(&too_big)),
        Err(Error::TooLarge { .. })
    ));
}

#[test]
fn reset_empties_the_index_but_not_a_held_entry() {
    let arena = arena();
    arena.put(Key::new(1), Record::body(b"one")).unwrap();
    arena.put(Key::new(2), Record::body(b"two")).unwrap();
    let held = arena.get(Key::new(1)).unwrap();
    let generation = arena.generation();

    assert_eq!(arena.reset().unwrap(), 2);
    assert_eq!(arena.generation(), generation + 1);
    assert!(arena.get(Key::new(1)).is_none());
    assert!(arena.get(Key::new(2)).is_none());
    assert_eq!(held.body(), b"one");
    drop(held);

    // The retired slot is reusable once the reader is gone.
    arena.put(Key::new(1), Record::body(b"again")).unwrap();
    assert_eq!(arena.get(Key::new(1)).unwrap().body(), b"again");
}

#[test]
fn a_view_owns_the_pin() {
    let arena = arena();
    arena.put(Key::new(3), Record::body(b"body").encoded(b"enc", 2)).unwrap();
    let view = arena.get(Key::new(3)).unwrap().into_encoded().ok().unwrap();
    assert_eq!(view.as_ref(), b"enc");
    let view = arena.get(Key::new(3)).unwrap().into_body();
    assert_eq!(view.as_ref(), b"body");
}
