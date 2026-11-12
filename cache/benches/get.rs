#[cfg(unix)]
mod support;

#[cfg(unix)]
mod unix {
    use std::hint::black_box;
    use std::io::{self, BufRead, Write};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Instant;

    use orbit_cache::{
        Cache, CacheRead, DefaultCacheLayout, SharedCache, SharedStore, Store,
    };
    use orbit_core::{Fleet, NodeId};

    use crate::support::{BenchFleet, Worker, process_cpu_ns};

    const WORKERS: u16 = 16;
    const KEYS: usize = 256;
    const READS_PER_WORKER: usize = 5_000_000;
    const VALUE: &[u8] = &[0x5a; 256];

    enum CacheMode {
        Replicated(Cache, Store),
        Shm(SharedStore),
    }

    impl CacheMode {
        fn open(
            mode: &str,
            fleet: Arc<Fleet>,
        ) -> Self {
            match mode {
                "replicated" => {
                    let cache =
                        Cache::<DefaultCacheLayout>::new(fleet).unwrap();
                    let store = cache
                        .open_store(b"bench", NonZeroUsize::new(KEYS).unwrap())
                        .unwrap();
                    Self::Replicated(cache, store)
                }
                "shm" => Self::Shm(
                    SharedCache::new(fleet)
                        .unwrap()
                        .open_store(b"bench")
                        .unwrap(),
                ),
                _ => panic!("unknown cache mode {mode}"),
            }
        }

        fn seed(
            &self,
            keys: &[[u8; 8]],
        ) {
            for key in keys {
                match self {
                    Self::Replicated(_, store) => {
                        store.put(key, VALUE, None).unwrap();
                    }
                    Self::Shm(store) => store.put(key, VALUE, None).unwrap(),
                }
            }
        }

        fn prepare(
            &self,
            keys: &[[u8; 8]],
        ) {
            if let Self::Replicated(cache, _) = self {
                let batch = cache.poll();
                assert_eq!(batch.observed, KEYS);
                assert!(
                    batch.loss.is_empty()
                        && batch.payload_unavailable.is_empty()
                );
                assert_eq!(batch.malformed, 0);
                assert!(!batch.resync_required);
            }
            for key in keys {
                assert!(
                    matches!(self.read(key), CacheRead::Hit(entry) if entry.value.as_ref() == VALUE)
                );
            }
        }

        fn read(
            &self,
            key: &[u8],
        ) -> CacheRead {
            match self {
                Self::Replicated(_, store) => store.read(key),
                Self::Shm(store) => store.read(key),
            }
        }
    }

    fn keys(start: usize) -> Vec<[u8; 8]> {
        (start..start + KEYS)
            .map(|index| (index as u64).to_le_bytes())
            .collect()
    }

    fn worker(
        mode: &str,
        name: &str,
        node: u16,
    ) {
        let fleet = Arc::new(
            Fleet::join_shm_as(name, WORKERS, NodeId::new(node)).unwrap(),
        );
        let cache = CacheMode::open(mode, fleet);
        let hits = keys(0);
        let misses = keys(KEYS);
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "ready").unwrap();
        stdout.flush().unwrap();
        for command in io::stdin().lock().lines() {
            match command.unwrap().as_str() {
                "seed" => {
                    cache.seed(&hits);
                    writeln!(stdout, "seeded").unwrap();
                }
                "prepare" => {
                    cache.prepare(&hits);
                    writeln!(stdout, "prepared").unwrap();
                }
                phase @ ("hit" | "miss") => {
                    let keys = if phase == "hit" { &hits } else { &misses };
                    let cpu_start = process_cpu_ns();
                    let mut found = 0;
                    for index in 0..READS_PER_WORKER {
                        let key =
                            &keys[(index + usize::from(node) * 13) % KEYS];
                        match black_box(cache.read(black_box(key))) {
                            CacheRead::Hit(_) => found += 1,
                            CacheRead::Miss => {}
                            CacheRead::ResyncRequired => {
                                panic!("cache became incoherent")
                            }
                        }
                    }
                    let cpu_ns = process_cpu_ns() - cpu_start;
                    assert_eq!(
                        found,
                        if phase == "hit" { READS_PER_WORKER } else { 0 }
                    );
                    writeln!(stdout, "{cpu_ns}").unwrap();
                }
                command => panic!("unknown worker command {command}"),
            }
            stdout.flush().unwrap();
        }
    }

    fn run_mode(
        mode: &str,
        run: usize,
    ) {
        let fleet = BenchFleet::new(run);
        let mut workers = (0..WORKERS)
            .map(|node| Worker::start(mode, &fleet.0, node, &[]))
            .collect::<Vec<_>>();
        for worker in &mut workers {
            assert_eq!(worker.response(), "ready");
        }

        workers[0].send("seed");
        assert_eq!(workers[0].response(), "seeded");
        for worker in &mut workers {
            worker.send("prepare");
        }
        for worker in &mut workers {
            assert_eq!(worker.response(), "prepared");
        }

        let total = (usize::from(WORKERS) * READS_PER_WORKER) as f64;
        for phase in ["hit", "miss"] {
            let started = Instant::now();
            for worker in &mut workers {
                worker.send(phase);
            }
            let cpu_ns: u64 = workers
                .iter_mut()
                .map(|worker| worker.response().parse::<u64>().unwrap())
                .sum();
            let elapsed = started.elapsed().as_secs_f64();
            println!(
                "{run},{mode},{phase},{total:.0},{elapsed:.6},{:.0},{:.2}",
                total / elapsed,
                cpu_ns as f64 / total
            );
        }

        for worker in &mut workers {
            worker.finish();
        }
    }

    pub fn main() {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        if let [flag, mode, fleet, node] = args.as_slice() {
            assert_eq!(flag, "--worker");
            return worker(mode, fleet, node.parse().unwrap());
        }

        if !args.iter().any(|arg| arg == "--bench") {
            return;
        }

        println!(
            "run,mode,phase,operations,wall_seconds,gets_per_second,cpu_ns_per_get"
        );
        for (run, mode) in
            ["replicated", "shm", "shm", "replicated", "replicated", "shm"]
                .into_iter()
                .enumerate()
        {
            run_mode(mode, run + 1);
        }
    }
}

#[cfg(unix)]
fn main() {
    unix::main();
}

#[cfg(not(unix))]
fn main() {
    eprintln!("The cache get benchmark requires Unix shared memory.");
}
