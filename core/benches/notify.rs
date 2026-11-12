//! What a ring publish costs with each readiness form, on this kernel.
//!
//! One thread publishes 64-byte frames into a shared SHM ring as fast as it
//! can, through one of three calls:
//!
//! - `plain`: `Fleet::publish`, no wake at all. The floor.
//! - `notified`: `Fleet::publish_notified`, which bumps the generation and
//!   makes the platform wake call on every publish (cache, events, lock).
//! - `parked`: `Fleet::publish_notified_parked`, which wakes only when a
//!   reader is counted as parked.
//!
//! `reader` is `none` (nobody subscribed) or `busy` (one thread of this
//! process holds the matching readiness fd and drains it as fast as it
//! becomes readable). A row gives publisher wall and thread CPU per publish,
//! the whole process's CPU per publish (publisher, readiness driver and
//! reader), and voluntary context switches per publish.
//!
//! Run: `cargo bench -p orbit-core --bench notify -- [publishes] [samples]`.

use std::hint::black_box;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use orbit_core::{Fleet, NodeId, OrbitTyped, RingSpec};

const PAYLOAD: &[u8] = &[0x42; 64];

#[derive(Clone)]
struct Plain;
impl OrbitTyped for Plain {
    const KIND: u8 = 240;
    const RING_SPEC: RingSpec = RingSpec::new(4096, 64);
}
#[derive(Clone)]
struct Notified;
impl OrbitTyped for Notified {
    const KIND: u8 = 241;
    const RING_SPEC: RingSpec = RingSpec::new(4096, 64);
}
#[derive(Clone)]
struct Parked;
impl OrbitTyped for Parked {
    const KIND: u8 = 242;
    const RING_SPEC: RingSpec = RingSpec::new(4096, 64);
}

fn clock(id: libc::clockid_t) -> u64 {
    let mut time = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: time is writable.
    assert_eq!(unsafe { libc::clock_gettime(id, &mut time) }, 0);
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

fn switches() -> i64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    usage.ru_nvcsw
}

/// Wait without a deadline: the fd becomes readable because a publish woke it.
fn readable(fd: i32) {
    let mut poll = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
    loop {
        let result = unsafe { libc::poll(&mut poll, 1, -1) };
        if result > 0 {
            return;
        }
        assert!(
            std::io::Error::last_os_error().kind()
                == std::io::ErrorKind::Interrupted
        );
    }
}

struct Row {
    wall: f64,
    thread_cpu: f64,
    process_cpu: f64,
    switches: f64,
    reader_wakes: f64,
}

fn sample(
    fleet: &Fleet,
    mode: &str,
    reader: bool,
    publishes: u64,
) -> Row {
    let stop = Arc::new(AtomicBool::new(false));
    let wakes = Arc::new(AtomicU64::new(0));
    let handle = reader.then(|| {
        let fleet = fleet.clone();
        let stop = Arc::clone(&stop);
        let wakes = Arc::clone(&wakes);
        let mode = mode.to_owned();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || match mode.as_str() {
            "notified" => {
                let fd = fleet.ring_event_fd::<Notified>().unwrap();
                ready_tx.send(()).unwrap();
                while !stop.load(Ordering::Acquire) {
                    readable(fd.as_raw_fd());
                    fd.drain().unwrap();
                    black_box(fleet.read_head::<Notified>());
                    wakes.fetch_add(1, Ordering::Relaxed);
                }
            }
            "parked" => {
                let fd = fleet.ring_event_fd_parked::<Parked>().unwrap();
                ready_tx.send(()).unwrap();
                while !stop.load(Ordering::Acquire) {
                    readable(fd.as_raw_fd());
                    fd.drain().unwrap();
                    black_box(fleet.read_head::<Parked>());
                    wakes.fetch_add(1, Ordering::Relaxed);
                }
            }
            _ => unreachable!(),
        });
        ready_rx.recv().unwrap();
        handle
    });

    let publish = |ver: u64| match mode {
        "plain" => {
            fleet.publish::<Plain>(0, ver, Bytes::from_static(PAYLOAD));
        }
        "notified" => {
            fleet
                .publish_notified::<Notified>(
                    0,
                    ver,
                    Bytes::from_static(PAYLOAD),
                )
                .unwrap();
        }
        "parked" => {
            fleet
                .publish_notified_parked::<Parked>(
                    0,
                    ver,
                    Bytes::from_static(PAYLOAD),
                )
                .unwrap();
        }
        _ => unreachable!(),
    };

    let process = clock(libc::CLOCK_PROCESS_CPUTIME_ID);
    let thread = clock(libc::CLOCK_THREAD_CPUTIME_ID);
    let csw = switches();
    let started = Instant::now();
    for ver in 0..publishes {
        publish(black_box(ver));
    }
    let wall = started.elapsed().as_nanos() as f64;
    let thread = clock(libc::CLOCK_THREAD_CPUTIME_ID) - thread;
    let process = clock(libc::CLOCK_PROCESS_CPUTIME_ID) - process;
    let csw = switches() - csw;
    let reader_wakes = wakes.load(Ordering::Relaxed);
    if let Some(handle) = handle {
        stop.store(true, Ordering::Release);
        publish(publishes);
        handle.join().unwrap();
    }
    let n = publishes as f64;
    Row {
        wall: wall / n,
        thread_cpu: thread as f64 / n,
        process_cpu: process as f64 / n,
        switches: csw as f64 / n,
        reader_wakes: reader_wakes as f64 / n,
    }
}

fn main() {
    let args = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect::<Vec<_>>();
    let publishes: u64 =
        args.first().and_then(|v| v.parse().ok()).unwrap_or(2_000_000);
    let samples: usize = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(5);

    let name = format!("nf{:x}", std::process::id());
    let fleet = Fleet::join_shm_as(&name, 1, NodeId::new(0)).unwrap();
    println!(
        "| publish | reader | wall ns/publish per sample | median wall | publisher CPU ns | process CPU ns | switches/publish | reader wakes/publish |"
    );
    println!("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |");
    for (mode, reader) in [
        ("plain", false),
        ("notified", false),
        ("parked", false),
        ("notified", true),
        ("parked", true),
    ] {
        let _ = sample(&fleet, mode, reader, publishes / 10);
        let mut rows = (0..samples)
            .map(|_| sample(&fleet, mode, reader, publishes))
            .collect::<Vec<_>>();
        let list = rows
            .iter()
            .map(|row| format!("{:.1}", row.wall))
            .collect::<Vec<_>>()
            .join(" / ");
        rows.sort_by(|a, b| a.wall.total_cmp(&b.wall));
        let row = &rows[rows.len() / 2];
        println!(
            "| {mode} | {} | {list} | {:.1} | {:.1} | {:.1} | {:.4} | {:.4} |",
            if reader { "busy" } else { "none" },
            row.wall,
            row.thread_cpu,
            row.process_cpu,
            row.switches,
            row.reader_wakes,
        );
    }

    let _ = fleet.shm_ring::<Plain>().and_then(|r| r.unlink());
    let _ = fleet.shm_ring::<Notified>().and_then(|r| r.unlink());
    let _ = fleet.shm_ring::<Parked>().and_then(|r| r.unlink());
    drop(fleet);
    let prefix = format!("orbit-{name}-");
    for entry in std::fs::read_dir("/dev/shm").into_iter().flatten().flatten() {
        if entry.file_name().to_string_lossy().starts_with(&prefix) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}
