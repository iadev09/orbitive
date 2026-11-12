//! Replicated cache puts into a full store.
//!
//! A put of a new key into a full store evicts the store's least recently
//! used entry, and the eviction raises its shard's revision floor, which
//! looks through the shard for entries the floor now covers. Here one thread
//! puts into stores of a few capacities, either over keys already resident
//! (`overwrite`: no eviction) or over new keys (`evict`: one eviction per
//! put). The gap between the two at one capacity is what an eviction costs,
//! and how it grows with the capacity is what the floor's look costs.
//!
//! The connection's mutation ring is drained every `DRAIN` puts, as a
//! listener would, so it never laps its cursor; both cases pay it alike.
//!
//! Run: `cargo bench -p orbit-cache --bench eviction -- [puts] [samples] [capacities,...]`.

#[cfg(unix)]
mod unix {
    use std::hint::black_box;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Instant;

    use orbit_cache::{Cache, DefaultCacheLayout, Store};
    use orbit_core::{Fleet, NodeId};

    const VALUE: &[u8] = &[0x5a; 32];
    /// Well under the mutation ring's 1,024 records.
    const DRAIN: usize = 256;

    fn key(index: u64) -> [u8; 8] {
        index.to_le_bytes()
    }

    /// Fill a store to its capacity and drain the ring after it.
    fn open(
        cache: &Cache,
        name: &str,
        capacity: usize,
    ) -> Store {
        let store = cache
            .open_store(name.as_bytes(), NonZeroUsize::new(capacity).unwrap())
            .unwrap();
        for index in 0..capacity as u64 {
            store.put(&key(index), VALUE, None).unwrap();
            if index as usize % DRAIN == DRAIN - 1 {
                cache.poll();
            }
        }
        let batch = cache.poll();
        assert!(!batch.resync_required && batch.loss.is_empty());
        assert_eq!(store.local().len(), capacity, "the store is full");
        store
    }

    /// One sample: wall ns per put. `next` names the key of each put.
    fn sample(
        cache: &Cache,
        store: &Store,
        puts: usize,
        mut next: impl FnMut() -> u64,
    ) -> f64 {
        let started = Instant::now();
        for index in 0..puts {
            black_box(store.put(&key(next()), VALUE, None).unwrap());
            if index % DRAIN == DRAIN - 1 {
                let batch = cache.poll();
                assert!(!batch.resync_required, "the ring never laps");
            }
        }
        started.elapsed().as_nanos() as f64 / puts as f64
    }

    fn median(mut rows: Vec<f64>) -> (String, f64) {
        let list = rows
            .iter()
            .map(|row| format!("{row:.0}"))
            .collect::<Vec<_>>()
            .join(" / ");
        rows.sort_by(f64::total_cmp);
        (list, rows[rows.len() / 2])
    }

    pub fn main() {
        let args = std::env::args()
            .skip(1)
            .filter(|arg| arg != "--bench")
            .collect::<Vec<_>>();
        let puts: usize =
            args.first().and_then(|v| v.parse().ok()).unwrap_or(200_000);
        let samples: usize =
            args.get(1).and_then(|v| v.parse().ok()).unwrap_or(5);
        let capacities: Vec<usize> = args
            .get(2)
            .map(|v| v.split(',').map(|s| s.parse().unwrap()).collect())
            .unwrap_or_else(|| vec![256, 4_096, 65_536]);

        let name = format!("ev{:x}", std::process::id());
        let fleet =
            Arc::new(Fleet::join_shm_as(&name, 1, NodeId::new(0)).unwrap());
        let cache =
            Cache::<DefaultCacheLayout>::new(Arc::clone(&fleet)).unwrap();

        println!(
            "| case | capacity | wall ns/put per sample | median wall ns/put |"
        );
        println!("| --- | ---: | --- | ---: |");
        for &capacity in &capacities {
            let overwrite =
                open(&cache, &format!("overwrite{capacity}"), capacity);
            let mut resident = 0u64;
            let mut over = || {
                resident = (resident + 1) % capacity as u64;
                resident
            };
            let _ = sample(&cache, &overwrite, puts / 10, &mut over);
            let (list, wall) = median(
                (0..samples)
                    .map(|_| sample(&cache, &overwrite, puts, &mut over))
                    .collect(),
            );
            println!("| overwrite | {capacity} | {list} | {wall:.0} |");
            assert_eq!(overwrite.local().len(), capacity);
            drop(overwrite);

            let evict = open(&cache, &format!("evict{capacity}"), capacity);
            let mut fresh = capacity as u64;
            let mut new = || {
                fresh += 1;
                fresh
            };
            let _ = sample(&cache, &evict, puts / 10, &mut new);
            let (list, wall) = median(
                (0..samples)
                    .map(|_| sample(&cache, &evict, puts, &mut new))
                    .collect(),
            );
            println!("| evict | {capacity} | {list} | {wall:.0} |");
            assert!(evict.local().len() <= capacity, "capacity holds");
        }

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
