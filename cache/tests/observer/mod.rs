use bytes::Bytes;
use orbit_arena::Address;
use orbit_core::NetId64;

use super::{RetainedSnapshot, apply};
use crate::{CacheMutation, CacheRevision, PayloadRef};

fn revision(sequence: u64) -> CacheRevision {
    CacheRevision {
        sequence,
        mutation_id: NetId64::make(
            crate::CACHE_MUTATION_RING_KIND,
            0,
            sequence,
        ),
    }
}

fn put(
    store: &'static [u8],
    key: &'static [u8],
    sequence: u64,
) -> CacheMutation {
    CacheMutation::Put {
        store: Bytes::from_static(store),
        key: Bytes::from_static(key),
        revision: revision(sequence),
        expires_at_ms: Some(1000),
        payload: PayloadRef {
            address: Address { slot: 0, generation: sequence },
            value_len: 12,
        },
    }
}

#[test]
fn cache_explorer_orders_mutations_and_keeps_store_resets_isolated() {
    let mut snapshot = RetainedSnapshot::default();
    apply(
        &mut snapshot,
        vec![
            put(b"a", b"latest", 7),
            put(b"b", b"keep", 1),
            put(b"a", b"old", 2),
            CacheMutation::Reset {
                store: Bytes::from_static(b"a"),
                revision: revision(4),
            },
            put(b"a", b"latest", 6),
            CacheMutation::Delete {
                store: Bytes::from_static(b"a"),
                key: Bytes::from_static(b"old"),
                revision: revision(3),
            },
        ],
    );
    let entries = &snapshot.stores[b"a".as_slice()];
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[b"latest".as_slice()].revision.sequence, 7);
    assert_eq!(snapshot.stores[b"b".as_slice()].len(), 1);
}

#[test]
fn cache_explorer_delete_leaves_an_observable_empty_store() {
    let mut snapshot = RetainedSnapshot::default();
    apply(
        &mut snapshot,
        vec![
            put(b"a", b"key", 1),
            CacheMutation::Delete {
                store: Bytes::from_static(b"a"),
                key: Bytes::from_static(b"key"),
                revision: revision(2),
            },
        ],
    );
    assert!(snapshot.stores[b"a".as_slice()].is_empty());
}
