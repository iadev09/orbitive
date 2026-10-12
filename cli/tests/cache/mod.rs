use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use clap::Parser;
use orbit_cache::{CACHE_MUTATION_RING_CAPACITY, Cache, DefaultCacheLayout};
use orbit_core::{Fleet, NodeId};

use super::{CacheArgs, ValueFormat, inspect, label, present, row};
use crate::{Cli, Command};

struct Fixture {
    name: String,
    cache: Cache<DefaultCacheLayout>,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let name = format!(
            "xc{:x}{:x}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let fleet =
            Arc::new(Fleet::join_shm_as(&name, 2, NodeId::ZERO).unwrap());
        Self { name, cache: Cache::new(fleet).unwrap() }
    }

    fn args(
        &self,
        extra: &[&str],
    ) -> CacheArgs {
        let mut argv = vec!["orbit", "cache", self.name.as_str()];
        argv.extend_from_slice(extra);
        match Cli::try_parse_from(argv).unwrap().command {
            Command::Cache(args) => args,
            _ => unreachable!(),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.cache.transport().unlink_rings().unwrap();
        self.cache.transport().arena().unlink().unwrap();
        let _ =
            std::fs::remove_file(orbit_core::shm::fleet_lock_path(&self.name));
    }
}

#[test]
fn cache_explorer_selection_requires_a_store_and_format_requires_a_key() {
    assert!(
        Cli::try_parse_from(["orbit", "cache", "example", "--key", "a"])
            .is_err()
    );
    assert!(
        Cli::try_parse_from(["orbit", "cache", "example", "--format", "json"])
            .is_err()
    );
    assert!(
        Cli::try_parse_from(["orbit", "cache", "example", "--json"]).is_ok()
    );
}

#[test]
fn cache_explorer_lists_metadata_and_reads_only_the_selected_value() {
    let fixture = Fixture::new();
    let transport = fixture.cache.transport();
    transport
        .publish_put(b"one", b"json", br#"{"enabled":true}"#, None)
        .unwrap();
    transport
        .publish_put(b"one", b"expired", b"old", Some(Duration::ZERO))
        .unwrap();
    transport.publish_put(b"two", b"binary", &[0xff, 0x00], None).unwrap();
    let report = inspect(&fixture.args(&["--json"])).unwrap();
    assert_eq!(report.entries.len(), 3);
    assert!(report.entries.iter().all(|row| row.value.is_none()));
    assert!(!report.authoritative);
    let selected =
        inspect(&fixture.args(&["--store", "one", "--key", "json"])).unwrap();
    assert_eq!(selected.entries.len(), 1);
    assert_eq!(
        selected.entries[0].value.as_ref().unwrap().data["enabled"],
        true
    );
    let snapshot = orbit_cache::observer::CacheObserver::attach_existing(
        &orbit_core::FleetObserver::attach_existing(&fixture.name).unwrap(),
    )
    .unwrap()
    .snapshot()
    .unwrap();
    let entry = &snapshot.stores[b"one".as_slice()][b"expired".as_slice()];
    // Check the exact expiry boundary without depending on wall-clock progress.
    let expired = row(b"one", entry, entry.expires_at_ms.unwrap());
    assert_eq!(expired.ttl_ms, Some(0));
    assert_eq!(expired.status, "expired");
    assert!(expired.value.is_none());
    let binary =
        inspect(&fixture.args(&["--store", "two", "--key", "binary"])).unwrap();
    assert_eq!(binary.entries[0].value.as_ref().unwrap().format, "hex");
    assert_eq!(binary.entries[0].value.as_ref().unwrap().data, "ff00");
    let encoded = serde_json::to_value(&selected).unwrap();
    assert_eq!(encoded["view"], "retained_history");
    assert_eq!(encoded["entries"][0]["size_bytes"], 16);
}

#[test]
fn cache_explorer_listing_does_not_claim_a_reader_row_or_publish() {
    let fixture = Fixture::new();
    fixture
        .cache
        .transport()
        .publish_put(b"one", b"a", b"value", None)
        .unwrap();
    let reader = orbit_arena::ArenaReader::attach_existing(
        &orbit_core::FleetObserver::attach_existing(&fixture.name).unwrap(),
    )
    .unwrap();
    let version = fixture.cache.transport().current_revision_sequence();
    assert_eq!(inspect(&fixture.args(&[])).unwrap().entries.len(), 1);
    assert_eq!(fixture.cache.transport().current_revision_sequence(), version);
    let error = inspect(&fixture.args(&["--store", "one", "--key", "a"]))
        .err()
        .unwrap();
    assert!(error.to_string().contains("reader capacity exhausted"));
    drop(reader);
    assert!(inspect(&fixture.args(&["--store", "one", "--key", "a"])).is_ok());
}

#[test]
fn cache_explorer_reports_retention_loss_instead_of_claiming_a_full_snapshot() {
    let fixture = Fixture::new();
    let transport = fixture.cache.transport();
    for _ in 0..=CACHE_MUTATION_RING_CAPACITY {
        transport.publish_delete(b"one", b"missing").unwrap();
    }
    let report = inspect(&fixture.args(&[])).unwrap();
    assert_eq!(report.overwritten, 1);
    assert!(!report.authoritative);
    assert!(report.entries.is_empty());
    assert!(
        inspect(&fixture.args(&["--store", "one", "--key", "missing"]))
            .is_err()
    );
}

#[test]
fn cache_explorer_formats_values_without_guessing_binary_types_or_terminal_controls()
 {
    assert_eq!(
        present(br#"{"a":[1,true]}"#, ValueFormat::Auto).unwrap().format,
        "json"
    );
    assert_eq!(present(b"hello", ValueFormat::Auto).unwrap().format, "text");
    assert_eq!(present(&[0xff], ValueFormat::Auto).unwrap().data, "ff");
    assert!(present(&[0xff], ValueFormat::Text).is_err());
    assert!(present(b"not json", ValueFormat::Json).is_err());
    assert_eq!(label(b"key\x1b[31m"), "hex:6b65791b5b33316d");
}
