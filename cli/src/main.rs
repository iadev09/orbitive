#![cfg_attr(not(unix), allow(dead_code, unused_imports))]

#[cfg(not(unix))]
compile_error!("orbitive-cli currently requires a Unix target");

mod cache;
mod shm;

use std::error::Error;
use std::process::ExitCode;

use clap::{ArgGroup, Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "orbit",
    version,
    about = "Inspect and remove Orbit POSIX shared-memory objects",
    arg_required_else_help = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Explore retained cache keys; select a key to read its arena value.
    Cache(cache::CacheArgs),
    /// Discover fleets with existing shared-memory objects.
    Fleets(FleetsArgs),
    /// List shared-memory objects belonging to a fleet.
    List(ListArgs),
    /// Remove shared-memory objects belonging to a stopped fleet.
    Clear(ClearArgs),
}

#[derive(Debug, Args)]
struct FleetsArgs {
    /// Effective user id that owns the objects.
    #[arg(long, value_name = "UID")]
    uid: Option<u32>,
}

#[derive(Debug, Args)]
struct TargetArgs {
    /// Orbit fleet name embedded in the POSIX SHM object name.
    #[arg(value_name = "FLEET")]
    fleet: String,

    /// Effective user id that owns the objects.
    #[arg(long, value_name = "UID")]
    uid: Option<u32>,
}

#[derive(Debug, Args)]
struct ListArgs {
    #[command(flatten)]
    target: TargetArgs,

    /// Inspect only one Orbit kind instead of probing all 256 kinds.
    #[arg(long, value_name = "KIND")]
    kind: Option<u8>,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("scope")
        .required(true)
        .multiple(false)
        .args(["kind", "all"])
))]
struct ClearArgs {
    #[command(flatten)]
    target: TargetArgs,

    /// Remove one Orbit kind.
    #[arg(long, value_name = "KIND")]
    kind: Option<u8>,

    /// Remove every discovered kind for this fleet and uid.
    #[arg(long, requires = "yes")]
    all: bool,

    /// Confirm a fleet-wide clear.
    #[arg(long)]
    yes: bool,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn Error>> {
    match cli.command {
        Command::Cache(args) => cache::run(args),
        Command::Fleets(args) => fleets(args),
        Command::List(args) => list(args),
        Command::Clear(args) => clear(args),
    }
}

fn fleets(args: FleetsArgs) -> Result<(), Box<dyn Error>> {
    let uid = args.uid.unwrap_or_else(shm::effective_uid);
    let fleets = shm::discover_fleets(uid)?;
    if fleets.is_empty() {
        println!("No Orbit SHM fleets found for uid={uid}.");
        return Ok(());
    }
    println!("{:<24}  {:>8}  {:>12}", "FLEET", "SEGMENTS", "TOTAL SIZE");
    for fleet in fleets {
        println!(
            "{:<24}  {:>8}  {:>12}",
            fleet.name,
            fleet.segments,
            human_size(fleet.size)
        );
    }
    Ok(())
}

fn list(args: ListArgs) -> Result<(), Box<dyn Error>> {
    let uid = args.target.uid.unwrap_or_else(shm::effective_uid);
    let segments = shm::discover(&args.target.fleet, uid, args.kind)?;

    if segments.is_empty() {
        println!(
            "No Orbit SHM objects found for fleet={} uid={uid}.",
            args.target.fleet
        );
        return Ok(());
    }

    println!("{:>4}  {:>12}  NAME", "KIND", "SIZE");
    for segment in segments {
        println!(
            "{:>4}  {:>12}  {}",
            segment.kind,
            human_size(segment.size),
            segment.name
        );
    }

    Ok(())
}

fn clear(args: ClearArgs) -> Result<(), Box<dyn Error>> {
    let uid = args.target.uid.unwrap_or_else(shm::effective_uid);
    let segments = shm::discover(&args.target.fleet, uid, args.kind)?;

    if segments.is_empty() {
        println!(
            "No Orbit SHM objects found for fleet={} uid={uid}.",
            args.target.fleet
        );
        return Ok(());
    }

    // Members hold the fleet's lock for as long as they live; holding it
    // exclusively is the only way in, and there is no flag around it.
    let Some(_fleet_held) =
        orbit_core::shm::try_lock_fleet_exclusive(&args.target.fleet, uid)?
    else {
        return Err(format!(
            "fleet {} is running: a member holds its lock; stop the fleet before clearing",
            args.target.fleet
        )
        .into());
    };

    let mut removed = 0usize;
    let mut failures = Vec::new();

    for segment in &segments {
        match shm::unlink(segment, uid) {
            Ok(()) => {
                println!("removed {}", segment.name);
                removed += 1;
            }
            Err(error) => failures.push(format!("{}: {error}", segment.name)),
        }
    }

    println!("removed={removed} failed={}", failures.len());
    if failures.is_empty() { Ok(()) } else { Err(failures.join("; ").into()) }
}

fn human_size(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes_as_float = bytes as f64;
    if bytes_as_float >= GIB {
        format!("{:.2} GiB", bytes_as_float / GIB)
    } else if bytes_as_float >= MIB {
        format!("{:.2} MiB", bytes_as_float / MIB)
    } else if bytes_as_float >= KIB {
        format!("{:.2} KiB", bytes_as_float / KIB)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
#[path = "../tests/main/mod.rs"]
mod tests;
