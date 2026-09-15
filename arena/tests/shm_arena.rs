//! The store lives in shared memory: a second fleet handle, as another
//! process would open it, reads the bytes the first one wrote, in place.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::sync::Arc;

use orbit_arena::{Arena, Key, Record};
use orbit_core::{Fleet, NodeId};

#[test]
fn a_second_mapping_reads_what_the_first_wrote() {
    let name: &'static str = Box::leak(format!("ar{:x}", std::process::id()).into_boxed_str());
    let owner = Arena::new(Arc::new(
        Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet"),
    ))
    .expect("owner arena");
    owner.reset().expect("quiescent reset");

    let key = Key::new(0xC0FFEE);
    owner
        .put(
            key,
            Record::body(b"identity")
                .encoded(b"encoded", 7)
                .stamp(9)
                .extra(8),
        )
        .expect("put");

    let peer = Arena::new(Arc::new(
        Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet"),
    ))
    .expect("peer arena");
    let entry = peer.get(key).expect("the peer sees the record");
    assert_eq!(entry.body(), b"identity");
    assert_eq!(entry.encoded(), Some((&b"encoded"[..], 7)));
    assert_eq!((entry.stamp(), entry.extra()), (9, 8));

    // Held by the peer, the record survives the owner's reset and put.
    assert_eq!(owner.reset().expect("reset"), 1);
    owner
        .put(Key::new(1), Record::body(b"after"))
        .expect("put after reset");
    assert_eq!(entry.body(), b"identity");
    drop(entry);
    assert!(peer.get(key).is_none());
    assert_eq!(peer.get(Key::new(1)).expect("new record").body(), b"after");

    owner.unlink().expect("unlink");
}
