use std::error::Error;
use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, ValueEnum};
use orbit_arena::ArenaReader;
use orbit_cache::observer::{CacheObserver, RetainedEntry};
use orbit_core::FleetObserver;
use serde::Serialize;
use serde_json::Value;

use crate::{TargetArgs, human_size, shm};

#[derive(Debug, Args)]
pub struct CacheArgs {
    #[command(flatten)]
    target: TargetArgs,
    /// Limit inspection to one logical store.
    #[arg(long)]
    store: Option<String>,
    /// Read only this selected key's value; requires --store.
    #[arg(long, requires = "store")]
    key: Option<String>,
    /// Emit one JSON document, including inspection limitations.
    #[arg(long)]
    json: bool,
    /// Selected value presentation; auto tries JSON, UTF-8, then hex.
    #[arg(long, value_enum, requires = "key")]
    format: Option<ValueFormat>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ValueFormat {
    Auto,
    Json,
    Text,
    Hex,
}

#[derive(Serialize)]
struct Report {
    fleet: String,
    uid: u32,
    view: &'static str,
    authoritative: bool,
    observed_at_ms: u64,
    overwritten: u64,
    unavailable: u64,
    malformed: u64,
    pending: u64,
    changed_during_scan: bool,
    stores: Vec<String>,
    entries: Vec<Row>,
}

#[derive(Serialize)]
struct Row {
    store: String,
    store_hex: String,
    key: String,
    key_hex: String,
    expires_at_ms: Option<u64>,
    ttl_ms: Option<u64>,
    size_bytes: u64,
    revision: u64,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<PresentedValue>,
}

#[derive(Serialize)]
struct PresentedValue {
    format: &'static str,
    data: Value,
}

pub fn run(args: CacheArgs) -> Result<(), Box<dyn Error>> {
    let report = inspect(&args)?;
    let mut stdout = io::stdout().lock();
    if args.json {
        serde_json::to_writer_pretty(&mut stdout, &report)?;
        writeln!(stdout)?;
    } else {
        render(&mut stdout, &report)?;
    }
    Ok(())
}

fn inspect(args: &CacheArgs) -> Result<Report, Box<dyn Error>> {
    let uid = args.target.uid.unwrap_or_else(shm::effective_uid);
    let fleet =
        FleetObserver::attach_existing_for_uid(&args.target.fleet, uid)?;
    // Protect both mappings against a concurrent clear/recreate before the
    // snapshot is read. Listing still claims no pin or membership.
    let reader = args
        .key
        .as_ref()
        .map(|_| ArenaReader::attach_existing(&fleet))
        .transpose()?;
    let observer = CacheObserver::attach_existing(&fleet)?;
    let snapshot = observer.snapshot()?;
    let now = u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?;
    let mut report = Report {
        fleet: args.target.fleet.clone(),
        uid,
        view: "retained_history",
        authoritative: false,
        observed_at_ms: now,
        overwritten: snapshot.loss.overwritten,
        unavailable: snapshot.loss.unavailable,
        malformed: snapshot.malformed,
        pending: snapshot.pending,
        changed_during_scan: snapshot.changed_during_scan,
        stores: Vec::new(),
        entries: Vec::new(),
    };
    for (store, entries) in &snapshot.stores {
        if args
            .store
            .as_ref()
            .is_some_and(|selected| selected.as_bytes() != store.as_ref())
        {
            continue;
        }
        report.stores.push(label(store));
        for (key, entry) in entries {
            if args
                .key
                .as_ref()
                .is_some_and(|selected| selected.as_bytes() != key.as_ref())
            {
                continue;
            }
            let mut row = row(store, entry, now);
            if args.key.is_some() && row.status != "expired" {
                // No pin at all for listing. For an explicit selection, copy
                // and drop the pin/owner before decoding or writing stdout.
                let bytes = read_value(
                    reader.as_ref().expect("selected value reader"),
                    entry,
                )?;
                if let Some(bytes) = bytes {
                    row.value = Some(present(
                        &bytes,
                        args.format.unwrap_or(ValueFormat::Auto),
                    )?);
                    row.status = "retained_value";
                } else {
                    row.status = "payload_unavailable";
                }
            }
            report.entries.push(row);
        }
    }
    if args.store.is_some() && report.stores.is_empty() {
        return Err("store is not represented in retained history; this does not prove the cache is empty".into());
    }
    if args.key.is_some() && report.entries.is_empty() {
        return Err("key is not represented in retained history; this does not prove a cache miss".into());
    }
    Ok(report)
}

fn read_value(
    reader: &ArenaReader,
    entry: &RetainedEntry,
) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
    let Some(value) = reader.get_address(entry.payload.address)? else {
        return Ok(None);
    };
    if value.body().len() as u64 != entry.payload.value_len {
        return Err(
            "arena body length differs from retained cache descriptor".into()
        );
    }
    Ok(Some(value.body().to_vec()))
}

fn row(
    store: &[u8],
    entry: &RetainedEntry,
    now: u64,
) -> Row {
    let expired = entry.expires_at_ms.is_some_and(|expiry| expiry <= now);
    Row {
        store: label(store),
        store_hex: hex(store),
        key: label(&entry.key),
        key_hex: hex(&entry.key),
        expires_at_ms: entry.expires_at_ms,
        ttl_ms: entry.expires_at_ms.map(|expiry| expiry.saturating_sub(now)),
        size_bytes: entry.payload.value_len,
        revision: entry.revision.sequence,
        status: if expired { "expired" } else { "retained_candidate" },
        value: None,
    }
}

fn present(
    bytes: &[u8],
    format: ValueFormat,
) -> Result<PresentedValue, Box<dyn Error>> {
    match format {
        ValueFormat::Auto => {
            if let Ok(data) = serde_json::from_slice::<Value>(bytes) {
                return Ok(PresentedValue { format: "json", data });
            }
            if let Ok(text) = std::str::from_utf8(bytes) {
                return Ok(PresentedValue {
                    format: "text",
                    data: Value::String(text.to_owned()),
                });
            }
            Ok(PresentedValue {
                format: "hex",
                data: Value::String(hex(bytes)),
            })
        }
        ValueFormat::Json => Ok(PresentedValue {
            format: "json",
            data: serde_json::from_slice(bytes)?,
        }),
        ValueFormat::Text => Ok(PresentedValue {
            format: "text",
            data: Value::String(std::str::from_utf8(bytes)?.to_owned()),
        }),
        ValueFormat::Hex => Ok(PresentedValue {
            format: "hex",
            data: Value::String(hex(bytes)),
        }),
    }
}

fn label(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) if text.chars().all(|ch| !ch.is_control()) => text.to_owned(),
        _ => format!("hex:{}", hex(bytes)),
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut text, "{byte:02x}").expect("String write");
    }
    text
}

fn render(
    output: &mut impl Write,
    report: &Report,
) -> io::Result<()> {
    writeln!(
        output,
        "Retained history only — not a complete or authoritative cache snapshot."
    )?;
    writeln!(
        output,
        "overwritten={} unavailable={} malformed={} pending={} changed_during_scan={}",
        report.overwritten,
        report.unavailable,
        report.malformed,
        report.pending,
        report.changed_during_scan
    )?;
    if report.entries.is_empty() {
        writeln!(
            output,
            "No retained Put candidates. Stores: {}",
            report.stores.join(", ")
        )?;
    } else {
        writeln!(
            output,
            "{:<20}  {:<32}  {:>12}  {:>12}  STATUS",
            "STORE", "KEY", "TTL (ms)", "SIZE"
        )?;
        for row in &report.entries {
            writeln!(
                output,
                "{:<20}  {:<32}  {:>12}  {:>12}  {}",
                row.store,
                row.key,
                row.ttl_ms.map_or_else(|| "none".into(), |ttl| ttl.to_string()),
                human_size(row.size_bytes),
                row.status
            )?;
            if let Some(value) = &row.value {
                writeln!(output, "Value ({}):", value.format)?;
                // JSON quoting keeps terminal control sequences inert.
                serde_json::to_writer_pretty(&mut *output, &value.data)?;
                writeln!(output)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/cache/mod.rs"]
mod tests;
