//! Replicated cache hits from several threads of one process.
//!
//! `get` runs one reader per process, so it cannot show what a hit's store
//! lock costs when threads share a store. Here `threads` readers hit 256
//! resident keys, either all on one store (`shared`: one `Mutex` for all of
//! them) or each on a store of its own on the same connection (`separate`:
//! the same work with no lock shared). The gap between the two at the same
//! thread count is what serializing on one store costs.
//!
//! Run: `cargo bench -p orbit-cache --bench read_contention -- [reads] [samples] [threads,...]`.

#[cfg(unix)]
mod unix {
    use std::hint::black_box;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Barrier};
    use std::time::Instant;

    use orbit_cache::{Cache, CacheRead, DefaultCacheLayout, Store};
    use orbit_core::{Fleet, NodeId};

    const KEYS: usize = 256;
    const VALUE: &[u8] = &[0x5a; 256];

    fn thread_cpu_ns() -> u64 {
        let mut time = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        // SAFETY: time is writable.
        let status = unsafe {
            libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time)
        };
        assert_eq!(status, 0);
        time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
    }

    fn keys() -> Vec<[u8; 8]> {
        (0..KEYS).map(|index| (index as u64).to_le_bytes()).collect()
    }

    fn open(
        cache: &Cache,
        name: &str,
    ) -> Store {
        let store = cache
            .open_store(name.as_bytes(), NonZeroUsize::new(KEYS).unwrap())
            .unwrap();
        for key in keys() {
            store.put(&key, VALUE, None).unwrap();
        }
        // Drain this store's mutations before the next store's, so the
        // connection's mutation ring never laps its own cursor.
        let batch = cache.poll();
        assert!(!batch.resync_required && batch.loss.is_empty());
        for key in keys() {
            assert!(matches!(store.read(&key), CacheRead::Hit(_)));
        }
        store
    }

    /// One sample: wall ns per read across all threads, and the mean
    /// thread CPU ns per read.
    fn sample(
        stores: &[Store],
        threads: usize,
        reads: usize,
    ) -> (f64, f64) {
        let barrier = Arc::new(Barrier::new(threads + 1));
        let handles = (0..threads)
            .map(|thread| {
                let store = stores[thread % stores.len()].clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let keys = keys();
                    barrier.wait();
                    let cpu = thread_cpu_ns();
                    let mut found = 0;
                    for index in 0..reads {
                        let key = &keys[(index + thread * 13) % KEYS];
                        if let CacheRead::Hit(_) =
                            black_box(store.read(black_box(key)))
                        {
                            found += 1;
                        }
                    }
                    assert_eq!(found, reads, "every read is a hit");
                    thread_cpu_ns() - cpu
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let started = Instant::now();
        let cpu: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let wall = started.elapsed().as_nanos() as f64;
        let total = (threads * reads) as f64;
        (wall / total, cpu as f64 / total)
    }

    pub fn main() {
        let args = std::env::args()
            .skip(1)
            .filter(|arg| arg != "--bench")
            .collect::<Vec<_>>();
        let reads: usize =
            args.first().and_then(|v| v.parse().ok()).unwrap_or(2_000_000);
        let samples: usize =
            args.get(1).and_then(|v| v.parse().ok()).unwrap_or(5);
        let counts: Vec<usize> = args
            .get(2)
            .map(|v| v.split(',').map(|s| s.parse().unwrap()).collect())
            .unwrap_or_else(|| vec![1, 2, 4, 8]);
        let max = *counts.iter().max().unwrap();

        let name = format!("rc{:x}", std::process::id());
        let fleet =
            Arc::new(Fleet::join_shm_as(&name, 1, NodeId::new(0)).unwrap());
        let cache =
            Cache::<DefaultCacheLayout>::new(Arc::clone(&fleet)).unwrap();
        let shared = vec![open(&cache, "shared")];
        let separate = (0..max)
            .map(|index| open(&cache, &format!("separate{index}")))
            .collect::<Vec<_>>();
        for store in shared.iter().chain(&separate) {
            for key in keys() {
                assert!(
                    matches!(store.read(&key), CacheRead::Hit(_)),
                    "every seeded key is resident"
                );
            }
        }

        println!(
            "| stores | threads | wall ns/read per sample | median wall ns/read | median CPU ns/read | reads/s (median) |"
        );
        println!("| --- | ---: | --- | ---: | ---: | ---: |");
        for &threads in &counts {
            for (label, stores) in
                [("shared", &shared[..]), ("separate", &separate[..threads])]
            {
                let _ = sample(stores, threads, reads / 10);
                let mut rows = (0..samples)
                    .map(|_| sample(stores, threads, reads))
                    .collect::<Vec<_>>();
                let list = rows
                    .iter()
                    .map(|(wall, _)| format!("{wall:.1}"))
                    .collect::<Vec<_>>()
                    .join(" / ");
                rows.sort_by(|a, b| a.0.total_cmp(&b.0));
                let (wall, _) = rows[rows.len() / 2];
                let mut cpu = rows.iter().map(|r| r.1).collect::<Vec<_>>();
                cpu.sort_by(f64::total_cmp);
                println!(
                    "| {label} | {threads} | {list} | {wall:.1} | {:.1} | {:.0} |",
                    cpu[cpu.len() / 2],
                    1e9 / wall
                );
            }
        }

        drop(shared);
        drop(separate);
        // Unlink by name, on every platform: segments outlive the process.
        cache.transport().unlink_rings().unwrap();
        cache.transport().arena().unlink().unwrap();
        drop(cache);
        drop(fleet);
    }
}

fn main() {
    #[cfg(unix)]
    unix::main();
}
