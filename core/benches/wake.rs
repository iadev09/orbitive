//! What one cross-thread wake costs on this kernel, with nothing else in it.
//!
//! Two threads hand a word back and forth through exactly the calls
//! `orbit-stream` uses -- `crate_sync::{wait_word, wake_word}` -- and
//! nothing else: no ring, no table, no async runtime. Whatever this costs is
//! the floor under every park orbit pays, and no design above it can go
//! faster than the kernel hands a thread back.
//!
//! `ru_nvcsw` says how many of the waits actually parked, so the per-park
//! figure is measured rather than assumed. macOS does not maintain it and
//! will report zero parks; read the round-trip column there.
//!
//! Run: `cargo bench -p orbit-core --bench wake -- [rounds]`. Taking this
//! on a new host before reading any figure from `orbit-stream` or
//! `orbit-pool` is worth the ten seconds: on one guest measured here it came
//! back at 22 µs a wake against another's 1.2 µs on the same hardware, which
//! is most of what a relayed request costs there and none of it ours.
use orbit_core::sync as crate_sync;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

fn parks() -> i64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    usage.ru_nvcsw as i64
}

fn main() {
    let rounds: u32 = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(50_000);

    // 0 means it is A's turn to act, 1 means B's.
    let turn = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicU32::new(0));

    let b = {
        let turn = Arc::clone(&turn);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            for _ in 0..rounds {
                while turn.load(Ordering::SeqCst) == 0 {
                    if done.load(Ordering::SeqCst) == 1 {
                        return;
                    }
                    let _ = crate_sync::wait_word(&turn, 0);
                }
                turn.store(0, Ordering::SeqCst);
                let _ = crate_sync::wake_word(&turn);
            }
        })
    };

    let before = parks();
    let started = Instant::now();
    for _ in 0..rounds {
        turn.store(1, Ordering::SeqCst);
        let _ = crate_sync::wake_word(&turn);
        while turn.load(Ordering::SeqCst) == 1 {
            let _ = crate_sync::wait_word(&turn, 1);
        }
    }
    let elapsed = started.elapsed();
    let parked = parks() - before;
    done.store(1, Ordering::SeqCst);
    let _ = crate_sync::wake_word(&turn);
    let _ = b.join();

    let trips = rounds as f64;
    let round_ns = elapsed.as_secs_f64() * 1e9 / trips;
    println!(
        "{rounds} round trips in {:.3}s\n  {:.2} µs per round trip (two wakes)\n  {:.2} µs per wake\n  {parked} parks on this thread, {:.2} per round trip{}",
        elapsed.as_secs_f64(),
        round_ns / 1000.0,
        round_ns / 2000.0,
        parked as f64 / trips,
        if parked == 0 { "  (kernel does not count them here)" } else { "" }
    );
}
