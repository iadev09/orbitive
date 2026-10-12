use std::sync::Arc;

use bytes::Bytes;
use orbit_arena::{ARENA_BYTES, ARENA_RECORD_MAX, Arena, Key, Record};
use orbit_core::Fleet;

#[test]
fn addresses_and_bytes_keep_one_generation_without_pinning_idle_metadata() {
    let fleet = Arc::new(Fleet::join("example", 1).unwrap());
    let arena = Arena::new(fleet.clone()).unwrap();
    // A Fleet clone may be put in a fresh Arc wrapper without separating
    // its private arena from the mutation rings it already shares.
    let same = Arena::new(Arc::new((*fleet).clone())).unwrap();
    let unrelated =
        Arena::new(Arc::new(Fleet::join("example", 1).unwrap())).unwrap();
    let address = arena.store(Record::body(b"original")).unwrap();
    assert!(unrelated.get_address(address).is_none());
    let first =
        Bytes::from_owner(arena.get_address(address).unwrap().into_body());
    let second =
        Bytes::from_owner(same.get_address(address).unwrap().into_body());
    assert_eq!(first.as_ptr(), second.as_ptr());
    arena.forget_address(address).unwrap();
    assert!(same.get_address(address).is_none());
    let next = arena.store(Record::body(b"replacement")).unwrap();
    assert_ne!(address, next);
    assert_eq!(first, b"original"[..]);
    arena.reset().unwrap();
    assert!(arena.get_address(next).is_none());
    assert_eq!(second, b"original"[..]);
}

#[test]
fn replacing_a_key_with_live_readers_never_rewrites_their_bytes() {
    let arena =
        Arena::new(Arc::new(Fleet::join("example", 1).unwrap())).unwrap();
    arena.put(Key::new(1), Record::body(b"old")).unwrap();
    let held = Bytes::from_owner(arena.get(Key::new(1)).unwrap().into_body());
    arena.put(Key::new(1), Record::body(b"new")).unwrap();
    assert_eq!(held, b"old"[..]);
    assert_eq!(arena.get(Key::new(1)).unwrap().body(), b"new");
}

#[test]
fn concurrent_key_replacement_never_tears_a_pinned_body() {
    let arena =
        Arena::new(Arc::new(Fleet::join("example", 1).unwrap())).unwrap();
    arena.put(Key::new(0), Record::body(&[0; 256])).unwrap();
    let start = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            start.wait();
            for value in 0..2000 {
                arena
                    .put(Key::new(0), Record::body(&[(value % 256) as u8; 256]))
                    .unwrap();
            }
        });
        start.wait();
        for _ in 0..4000 {
            if let Some(entry) = arena.get(Key::new(0)) {
                assert_eq!(entry.body().len(), 256);
                assert!(
                    entry.body().iter().all(|byte| *byte == entry.body()[0])
                );
            }
        }
    });
}

#[test]
fn empty_and_tiny_records_evict_under_index_pressure() {
    let arena =
        Arena::new(Arc::new(Fleet::join("example", 1).unwrap())).unwrap();
    let oldest = arena.store(Record::body(b"")).unwrap();
    for _ in 0..orbit_arena::ARENA_SLOTS {
        arena.store(Record::body(b"")).unwrap();
    }
    assert!(arena.get_address(oldest).is_none());
    assert_eq!(arena.len(), orbit_arena::ARENA_SLOTS);
    let recent: Vec<_> =
        (0..8).map(|_| arena.store(Record::body(b"")).unwrap()).collect();
    assert!(
        recent.into_iter().all(|address| arena.get_address(address).is_some())
    );
}

#[cfg(unix)]
#[test]
fn child_pin_owner() {
    use std::io::{Read, Write};
    let Ok(name) = std::env::var("ORBIT_ARENA_PIN_TEST") else {
        return;
    };
    let name = Box::leak(name.into_boxed_str());
    let fleet = Arc::new(Fleet::join_shm(name, 2).unwrap());
    let arena = Arena::new(fleet).unwrap();
    let count: usize =
        std::env::var("ORBIT_ARENA_PIN_COUNT").unwrap().parse().unwrap();
    let entries: Vec<_> = (0..count)
        .map(|key| arena.get(Key::new(key as u128)).unwrap())
        .collect();
    println!("PINNED");
    std::io::stdout().flush().unwrap();
    // Explicit parent signal/EOF, never a timeout or polling loop.
    let _ = std::io::stdin().read_exact(&mut [0]);
    drop(entries);
}

#[cfg(unix)]
fn pin_child(
    name: &str,
    count: usize,
) -> std::process::Child {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_pin_owner", "--nocapture"])
        .env("ORBIT_ARENA_PIN_TEST", name)
        .env("ORBIT_ARENA_PIN_COUNT", count.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(
            output.read_line(&mut line).unwrap(),
            0,
            "child failed before pinning"
        );
        if line.trim() == "PINNED" {
            break;
        }
    }
    child.stdout = Some(output.into_inner());
    child
}

#[cfg(unix)]
fn shared_arena() -> (String, Arena) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let name = format!(
        "arena_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let fleet = Arc::new(
        Fleet::join_shm(Box::leak(name.clone().into_boxed_str()), 2).unwrap(),
    );
    (name, Arena::new(fleet).unwrap())
}

#[cfg(unix)]
fn kill_pinned_child(mut child: std::process::Child) {
    use std::os::unix::process::ExitStatusExt;
    // Child::wait closes stdin. Keep it ourselves so SIGKILL cannot race an
    // EOF that would let the fixture release its pin normally before exit.
    let input = child.stdin.take();
    child.kill().unwrap();
    assert_eq!(child.wait().unwrap().signal(), Some(9));
    drop(input);
}

#[cfg(unix)]
#[test]
fn sigkill_reclaims_only_dead_owners_and_allocation_recovers_without_a_timer() {
    let (name, arena) = shared_arena();
    let count = ARENA_BYTES / ARENA_RECORD_MAX;
    let body = vec![7; ARENA_RECORD_MAX];
    for key in 0..count {
        arena.put(Key::new(key as u128), Record::body(&body)).unwrap();
    }
    let child = pin_child(&name, count);
    assert_eq!(
        arena.reclaim_dead_pins().unwrap(),
        0,
        "live child must not be reclaimed"
    );
    assert!(matches!(
        arena.put(Key::new(99), Record::body(&body)),
        Err(orbit_arena::Error::Pinned)
    ));
    kill_pinned_child(child);
    // Allocation itself detects abandoned pins after pressure, takes the
    // kernel ownership lock and recovers the room (no supervisor required).
    arena.put(Key::new(99), Record::body(&body)).unwrap();
    assert_eq!(arena.reclaim_dead_pins().unwrap(), 0);
    assert_eq!(arena.get(Key::new(99)).unwrap().body(), &body);
    arena.unlink().unwrap();
}

#[cfg(unix)]
#[test]
fn explicit_reclaim_clears_abandoned_pins_but_preserves_a_live_reader() {
    let (name, arena) = shared_arena();
    arena.put(Key::new(0), Record::body(b"value")).unwrap();
    let held = arena.get(Key::new(0)).unwrap();
    let child = pin_child(&name, 1);
    kill_pinned_child(child);
    assert_eq!(arena.reclaim_dead_pins().unwrap(), 1);
    arena.reset().unwrap();
    assert_eq!(held.body(), b"value");
    drop(held);
    // A new attachment takes the dead owner's row without clearing ours.
    arena.put(Key::new(0), Record::body(b"new")).unwrap();
    let mut replacement = pin_child(&name, 1);
    assert_eq!(arena.reclaim_dead_pins().unwrap(), 0);
    replacement.stdin.take();
    assert!(replacement.wait().unwrap().success());
    assert_eq!(arena.reclaim_dead_pins().unwrap(), 0);
    arena.unlink().unwrap();
}
