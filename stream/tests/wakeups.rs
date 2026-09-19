//! What a transfer costs in wakeups, and what the async adapters add.
//!
//! This is not a timing test. Time belongs to the machine — the same transfer
//! measured on a laptop, a UTM guest and a bare-metal Xeon came back at
//! 7.4 GB/s, 1.4 GB/s and 3.8 GB/s — but **the number of times a transfer
//! parks belongs to the design**, and it is the number that moves when the
//! adapters change. So the assertions are on parks, and they are the ones
//! that hold across hosts.
//!
//! What it pins, measured 2026-09-19 on a 40-core bare-metal Xeon:
//!
//! ```text
//!                    parks per ring-ful   bytes
//! blocking                      2.00      3.8 GB/s
//! through AsyncRead/AsyncWrite  9.38      1.4 GB/s
//! ```
//!
//! Five times the wakeups for a third of the throughput, and both halves of
//! that are the same fact: a blocking reader parks on the direction's own
//! word and is woken by the writer, while an async one goes through the
//! doorbell to a driver thread, which wakes a task, which wakes a runtime
//! worker. One hop is the design; three is the adapter.
//!
//! **This test is meant to be edited.** The bounds below are not a target,
//! they are where the adapters stood when they were last looked at. Change
//! how a wakeup reaches a task and these numbers move; the test will say so,
//! and the right response is to put the new numbers in the table above with
//! the date and tighten the bounds — not to raise them until it passes.
//!
//! `ru_nvcsw` is kept by Linux and FreeBSD and not by macOS, which reports
//! zero however much it parks. So the counts are asserted where they are
//! real, and the transfer itself is checked everywhere.
//!
//! One test in this file on purpose: `getrusage` counts the whole process,
//! so a second test running beside it would be counted into this one.

#![cfg(all(
    feature = "tokio",
    any(target_os = "linux", target_os = "freebsd", target_os = "macos")
))]

use std::sync::Arc;

use orbit_core::{Fleet, NodeId};
use orbit_stream::{Incarnation, STREAM_BUFFER_BYTES, Streams};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Ring-fuls to move. Enough that the wait path is entered many times and a
/// one-off cost cannot dominate; small enough to stay a test.
const RINGS: usize = 32;

/// Where the two paths stood when this was last measured, plus headroom for
/// a busy machine. Tight enough that another hop shows up as a failure.
const BLOCKING_MAX: f64 = 4.0;
const ASYNC_MAX: f64 = 14.0;

fn parks() -> i64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    usage.ru_nvcsw as i64
}

/// True where the kernel maintains `ru_nvcsw`; macOS leaves it at zero.
const COUNTED: bool = cfg!(any(target_os = "linux", target_os = "freebsd"));

fn fleet_name(tag: &str) -> &'static str {
    Box::leak(format!("wk{tag}{:x}", std::process::id()).into_boxed_str())
}

fn pair(name: &'static str) -> (Streams, Streams) {
    let near = Streams::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("fleet")),
        Incarnation::new(1),
    )
    .expect("streams");
    near.reset_all();
    let far = Streams::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("fleet")),
        Incarnation::new(2),
    )
    .expect("streams");
    (near, far)
}

#[test]
fn the_async_adapters_cost_more_wakeups_than_the_blocking_path() {
    let chunk = STREAM_BUFFER_BYTES;
    let total = chunk * RINGS;

    // --- blocking: one park per direction, on the direction's own word ---
    let (near, far) = pair(fleet_name("b"));
    let (endpoint, ticket) = near.create().expect("create");
    near.offer(ticket, NodeId::new(1)).expect("offer");
    let accepted = far.blocking_take_offer().expect("offer arrives");
    let accepted = far.open(accepted).expect("open");
    let (_, mut write) = endpoint.split();
    let (read, _) = accepted.split();

    let reader = std::thread::spawn(move || {
        let mut buf = vec![0_u8; chunk];
        let mut got = 0_usize;
        while got < total {
            match read.blocking_read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => got += n,
            }
        }
        got
    });
    let payload = vec![7_u8; chunk];
    let before = parks();
    for _ in 0..RINGS {
        write.blocking_write_all(&payload).expect("write");
    }
    write.finish().expect("finish");
    let moved = reader.join().expect("reader");
    let blocking = (parks() - before) as f64 / RINGS as f64;
    assert_eq!(moved, total, "the blocking path dropped bytes");
    let _ = near.unlink();

    // --- async: the doorbell, the driver, the task, the worker ---
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let (near, far) = pair(fleet_name("a"));
    let (endpoint, ticket) = near.create().expect("create");
    near.offer(ticket, NodeId::new(1)).expect("offer");
    let accepted = far.blocking_take_offer().expect("offer arrives");
    let accepted = far.open(accepted).expect("open");
    let (_, mut write) = endpoint.split();
    let (mut read, _) = accepted.split();

    let before = parks();
    let moved = runtime.block_on(async move {
        let reader = tokio::spawn(async move {
            let mut buf = vec![0_u8; chunk];
            let mut got = 0_usize;
            while got < total {
                match read.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => got += n,
                }
            }
            got
        });
        let payload = vec![7_u8; chunk];
        for _ in 0..RINGS {
            write.write_all(&payload).await.expect("write");
        }
        write.shutdown().await.expect("shutdown");
        reader.await.expect("reader")
    });
    let asynchronous = (parks() - before) as f64 / RINGS as f64;
    assert_eq!(moved, total, "the async path dropped bytes");
    let _ = near.unlink();

    println!(
        "parks per ring-ful of {chunk} bytes: blocking {blocking:.2}, async {asynchronous:.2}"
    );

    if !COUNTED {
        // macOS leaves `ru_nvcsw` at zero, so there is nothing to assert; the
        // transfers above still had to complete, which is the rest of it.
        return;
    }

    assert!(
        blocking <= BLOCKING_MAX,
        "the blocking path now parks {blocking:.2} times per ring-ful, over the {BLOCKING_MAX:.1} \
         it was held to. A blocking reader should park on the direction's own word and be woken \
         by the writer; more than that means another hop was added. Read the table at the top of \
         this file before changing the bound."
    );
    assert!(
        asynchronous <= ASYNC_MAX,
        "the async adapters now cost {asynchronous:.2} parks per ring-ful, over the {ASYNC_MAX:.1} \
         they were held to, against {blocking:.2} for the blocking path. Every park here is a \
         wakeup that a caller pays for and cannot see. Read the table at the top of this file: \
         the response is to record the new numbers and tighten the bound, not to raise it."
    );
}
