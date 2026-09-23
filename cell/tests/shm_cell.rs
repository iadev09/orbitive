//! The table lives in shared memory: a second fleet handle, as another
//! process would open it, reaches the same cells by id.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::sync::Arc;

use orbit_cell::{Cells, Error};
use orbit_core::{Fleet, NodeId};

#[test]
fn a_second_mapping_reaches_the_same_cell_by_id() {
    let name: &'static str = Box::leak(format!("cl{:x}", std::process::id()).into_boxed_str());
    let owner =
        Cells::new(Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet")))
            .expect("owner cells");
    owner.reset_all().expect("quiescent reset");

    let counter = owner.allocate(40_i64).expect("allocate");
    let id = counter.id().to_string();

    let peer =
        Cells::new(Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet")))
            .expect("peer cells");
    let same = peer.open::<i64>(id.parse().expect("id")).expect("open");
    assert_eq!(same.fetch_add(2).expect("add"), 42);
    assert_eq!(counter.load().expect("load"), 42);

    counter.release().expect("release");
    assert!(matches!(same.load(), Err(Error::Stale(_))));

    let text = owner.allocate_text("shared ").expect("allocate text");
    let peer_text = peer.open_text(text.id()).expect("open text");
    peer_text.append("memory").expect("append");
    assert_eq!(text.load().expect("load"), "shared memory");

    owner.unlink().expect("unlink");
}
