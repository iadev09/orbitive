#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
mod support;

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
mod platform {
    use std::io::{self, Write};
    use std::num::NonZeroUsize;
    use std::os::fd::{AsRawFd, RawFd};
    use std::sync::Arc;
    use std::time::Duration;

    use orbit_cache::{
        Cache, CacheRead, DefaultCacheLayout, RingEventFd, SharedCache, Store,
    };
    use orbit_core::{Fleet, NodeId};

    use crate::support::{BenchFleet, Worker, clock_ns, process_cpu_ns};

    const WORKERS: usize = 16;
    const PUTS_PER_WORKER: usize = 128;
    const PUTS: usize = WORKERS * PUTS_PER_WORKER;
    const SECOND_NS: u64 = 1_000_000_000;
    const HOT_KEY: [u8; 8] = *b"hot-key!";
    const VALUE: [u8; 256] = [0x5a; 256];

    fn key(
        writer: usize,
        index: usize,
    ) -> [u8; 8] {
        ((writer * PUTS_PER_WORKER + index) as u64).to_le_bytes()
    }

    fn now_ns() -> u64 {
        clock_ns(libc::CLOCK_MONOTONIC)
    }

    struct Replicated {
        cache: Cache,
        store: Store,
        ready: RingEventFd,
        next: [usize; WORKERS],
        observed: Vec<u64>,
        loss: u64,
        malformed: u64,
        payload_unavailable: usize,
        probe_cpu: u64,
    }

    impl Replicated {
        fn new(fleet: Arc<Fleet>) -> Self {
            let cache = Cache::<DefaultCacheLayout>::new(fleet).unwrap();
            let store = cache
                .open_store(b"bench", NonZeroUsize::new(PUTS).unwrap())
                .unwrap();
            let ready = cache.event_fd().expect("native mutation readiness");
            Self {
                cache,
                store,
                ready,
                next: [0; WORKERS],
                observed: vec![0; PUTS],
                loss: 0,
                malformed: 0,
                payload_unavailable: 0,
                probe_cpu: 0,
            }
        }

        fn drain(
            &mut self,
            node: usize,
            hot_key: bool,
        ) {
            self.ready.drain().unwrap();
            let batch = self.cache.poll();
            let applied_at = now_ns();
            self.loss += batch.loss.total();
            self.malformed += batch.malformed;
            self.payload_unavailable += batch.payload_unavailable.len();
            if hot_key || batch.applied == 0 {
                return;
            }

            // The probe confirms which keys this drain made readable.
            // Use the drain completion timestamp so the probe scan itself
            // does not inflate the visibility latency for those keys.
            let probe_start = clock_ns(libc::CLOCK_THREAD_CPUTIME_ID);
            for writer in 0..WORKERS {
                if writer == node {
                    continue;
                }
                let next = &mut self.next[writer];
                while *next < PUTS_PER_WORKER {
                    let CacheRead::Hit(entry) =
                        self.store.read(&key(writer, *next))
                    else {
                        break;
                    };
                    assert_eq!(entry.value.as_ref(), &VALUE);
                    self.observed[writer * PUTS_PER_WORKER + *next] =
                        applied_at;
                    *next += 1;
                }
            }
            self.probe_cpu +=
                clock_ns(libc::CLOCK_THREAD_CPUTIME_ID) - probe_start;
        }
    }

    fn wait_ready(
        input: RawFd,
        ready: Option<RawFd>,
    ) -> (bool, bool) {
        let mut fds = [
            libc::pollfd { fd: input, events: libc::POLLIN, revents: 0 },
            libc::pollfd {
                fd: ready.unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        loop {
            // SAFETY: poll receives the valid mutable array and does not retain it.
            let count = unsafe {
                libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1)
            };
            if count >= 0 {
                return (fds[0].revents != 0, fds[1].revents != 0);
            }
            let error = io::Error::last_os_error();
            assert_eq!(
                error.kind(),
                io::ErrorKind::Interrupted,
                "poll: {error}"
            );
        }
    }

    fn read_signal(input: RawFd) -> bool {
        let mut byte = 0_u8;
        loop {
            // SAFETY: read writes at most one byte to byte and retains no pointer.
            let count = unsafe {
                libc::read(input, std::ptr::from_mut(&mut byte).cast(), 1)
            };
            match count {
                0 => return false,
                1 => {
                    assert_eq!(byte, 1);
                    return true;
                }
                _ => {
                    let error = io::Error::last_os_error();
                    assert_eq!(
                        error.kind(),
                        io::ErrorKind::Interrupted,
                        "read: {error}"
                    );
                }
            }
        }
    }

    fn worker(
        mode: &str,
        name: &str,
        node: usize,
        hot_key: bool,
    ) {
        let fleet = Arc::new(
            Fleet::join_shm_as(name, WORKERS as u16, NodeId::new(node as u16))
                .unwrap(),
        );
        let mut replicated =
            (mode == "replicated").then(|| Replicated::new(fleet.clone()));
        let shared = (mode == "shm").then(|| {
            SharedCache::new(fleet).unwrap().open_store(b"bench").unwrap()
        });
        let input = io::stdin();
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "ready").unwrap();
        stdout.flush().unwrap();

        let ready_fd = replicated.as_ref().map(|cache| cache.ready.as_raw_fd());
        let input_fd = input.as_raw_fd();
        let mut writes = Vec::with_capacity(PUTS_PER_WORKER);
        let cpu_start = process_cpu_ns();
        let mut last_published_revision = 0;
        let mut hot_value = VALUE;

        for index in 0..PUTS_PER_WORKER {
            let mutation = loop {
                let (write, mutation) = wait_ready(input_fd, ready_fd);
                if write {
                    if !read_signal(input_fd) {
                        return;
                    }
                    break mutation;
                }
                if mutation {
                    replicated.as_mut().unwrap().drain(node, hot_key);
                }
            };

            let writer_key = key(node, index);
            let (key, value) = if hot_key {
                hot_value[..8].copy_from_slice(&writer_key);
                (&HOT_KEY, &hot_value)
            } else {
                (&writer_key, &VALUE)
            };
            let started = now_ns();
            if let Some(cache) = &replicated {
                last_published_revision =
                    cache.store.put(key, value, None).unwrap().sequence;
            } else {
                shared.as_ref().unwrap().put(key, value, None).unwrap();
            }
            writes.push((started, now_ns()));
            if mutation {
                replicated.as_mut().unwrap().drain(node, hot_key);
            }
        }

        writeln!(stdout, "written").unwrap();
        stdout.flush().unwrap();
        loop {
            let (stop, mutation) = wait_ready(input_fd, ready_fd);
            if (stop || mutation)
                && let Some(cache) = &mut replicated
            {
                cache.drain(node, hot_key);
            }
            if stop {
                break;
            }
        }
        let cpu = process_cpu_ns() - cpu_start;

        let (loss, malformed, payload_unavailable, probe_cpu) =
            replicated.as_ref().map_or((0, 0, 0, 0), |cache| {
                (
                    cache.loss,
                    cache.malformed,
                    cache.payload_unavailable,
                    cache.probe_cpu,
                )
            });
        writeln!(
            stdout,
            "{cpu} {probe_cpu} {loss} {malformed} {payload_unavailable}"
        )
        .unwrap();
        for (started, finished) in writes {
            writeln!(stdout, "{started} {finished}").unwrap();
        }
        if hot_key {
            let entry = if let Some(cache) = replicated {
                cache.store.read(&HOT_KEY)
            } else {
                shared.unwrap().read(&HOT_KEY)
            };
            let CacheRead::Hit(entry) = entry else {
                panic!(
                    "worker {node} did not have a readable final hot key: {entry:?}"
                );
            };
            assert_eq!(entry.value.len(), VALUE.len());
            assert_eq!(entry.value[8..], VALUE[8..]);
            let id = u64::from_le_bytes(entry.value[..8].try_into().unwrap());
            let revision = entry.revision.sequence;
            writeln!(stdout, "{last_published_revision} {revision} {id}")
                .unwrap();
        } else if let Some(cache) = replicated {
            for time in cache.observed {
                writeln!(stdout, "{time}").unwrap();
            }
        } else {
            let cache = shared.unwrap();
            for writer in 0..WORKERS {
                for index in 0..PUTS_PER_WORKER {
                    let hit = matches!(cache.read(&key(writer, index)), CacheRead::Hit(entry) if entry.value.as_ref() == VALUE);
                    writeln!(stdout, "{}", u8::from(hit)).unwrap();
                }
            }
        }
        stdout.flush().unwrap();
    }

    fn parse<const N: usize>(line: &str) -> [u64; N] {
        let mut values =
            line.split_whitespace().map(|word| word.parse::<u64>().unwrap());
        let result = std::array::from_fn(|_| values.next().unwrap());
        assert!(values.next().is_none());
        result
    }

    fn p99(values: &mut [u64]) -> f64 {
        if values.is_empty() {
            return f64::NAN;
        }
        values.sort_unstable();
        values[(values.len() * 99).div_ceil(100) - 1] as f64 / 1_000.0
    }

    fn run(
        mode: &str,
        run: usize,
        rate: u64,
        hot_key: bool,
    ) {
        let fleet = BenchFleet::new(run);
        let options: &[_] = if hot_key { &["--hot-key"] } else { &[] };
        let mut workers = (0..WORKERS)
            .map(|node| Worker::start(mode, &fleet.0, node as u16, options))
            .collect::<Vec<_>>();
        for worker in &mut workers {
            assert_eq!(worker.response(), "ready");
        }
        let start = now_ns();
        let mut dispatched = [[0; WORKERS]; PUTS_PER_WORKER];
        for (round, dispatch_times) in dispatched.iter_mut().enumerate() {
            let scheduled = start + round as u64 * SECOND_NS / rate;
            let remaining = scheduled.saturating_sub(now_ns());
            if remaining > 0 {
                std::thread::sleep(Duration::from_nanos(remaining));
            }
            // Rotate the first process so pipe order does not favor one writer.
            for offset in 0..WORKERS {
                let node = (round + offset) % WORKERS;
                dispatch_times[node] = now_ns();
                workers[node].signal();
            }
        }
        for worker in &mut workers {
            assert_eq!(worker.response(), "written");
        }
        for worker in &mut workers {
            worker.send("finish");
        }

        let mut totals = [0; 5];
        let mut writes = Vec::with_capacity(PUTS);
        let mut observations = Vec::with_capacity(WORKERS);
        let mut final_reads = Vec::with_capacity(WORKERS);
        for worker in &mut workers {
            for (total, value) in
                totals.iter_mut().zip(parse::<5>(&worker.response()))
            {
                *total += value;
            }
            writes.extend(
                (0..PUTS_PER_WORKER).map(|_| parse::<2>(&worker.response())),
            );
            if hot_key {
                final_reads.push(parse::<3>(&worker.response()));
            } else {
                observations.push(
                    (0..PUTS)
                        .map(|_| worker.response().parse::<u64>().unwrap())
                        .collect::<Vec<_>>(),
                );
            }
            worker.finish();
        }

        let mut put = writes
            .iter()
            .map(|&[started, finished]| finished - started)
            .collect::<Vec<_>>();
        let mut late = writes
            .iter()
            .enumerate()
            .map(|(index, write)| {
                let node = index / PUTS_PER_WORKER;
                let round = index % PUTS_PER_WORKER;
                write[0] - dispatched[round][node]
            })
            .collect::<Vec<_>>();
        let offered_window =
            (PUTS_PER_WORKER as u64 * SECOND_NS).div_ceil(rate);
        let elapsed_ns = (writes.iter().map(|write| write[1]).max().unwrap()
            - start)
            .max(offered_window);
        let [cpu, probe_cpu, loss, malformed, payload_unavailable] = totals;
        if hot_key {
            let (expected_revision, expected_id) = if mode == "replicated" {
                let (writer, report) = final_reads
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, report)| report[0])
                    .unwrap();
                (
                    report[0],
                    (writer * PUTS_PER_WORKER + PUTS_PER_WORKER - 1) as u64,
                )
            } else {
                // The fresh SHM arena assigns one generation to each put.
                // The highest generation must belong to a worker's last put.
                (PUTS as u64, final_reads[0][2])
            };
            assert!(expected_id < PUTS as u64);
            assert_eq!(
                expected_id as usize % PUTS_PER_WORKER,
                PUTS_PER_WORKER - 1
            );
            let converged = final_reads
                .iter()
                .filter(|report| {
                    report[1..] == [expected_revision, expected_id]
                })
                .count();
            println!(
                "{run},{mode},{},{PUTS},{:.1},{:.1},{:.2},{:.2},{loss},{malformed},{payload_unavailable},{expected_revision},{},{converged}",
                rate * WORKERS as u64,
                PUTS as f64 * SECOND_NS as f64 / elapsed_ns as f64,
                cpu as f64 / PUTS as f64,
                p99(&mut put),
                p99(&mut late),
                expected_id as usize / PUTS_PER_WORKER,
            );
            assert_eq!(
                converged, WORKERS,
                "{mode} workers disagreed: {final_reads:?}"
            );
            return;
        }

        let mut visibility = Vec::with_capacity(PUTS);
        let mut missing = 0;
        let mut incomplete = 0;
        for (index, &[started, finished]) in writes.iter().enumerate() {
            let writer = index / PUTS_PER_WORKER;
            let mut latest = 0;
            let mut complete = true;
            for (reader, times) in observations.iter().enumerate() {
                if reader == writer {
                    continue;
                }
                let seen = times[index];
                if seen == 0 {
                    missing += 1;
                    complete = false;
                }
                latest = latest.max(seen);
            }
            if complete {
                visibility.push(if mode == "shm" {
                    finished - started
                } else {
                    latest - started
                });
            } else {
                incomplete += 1;
            }
        }
        println!(
            "{run},{mode},{},{PUTS},{:.1},{:.1},{:.1},{:.2},{:.2},{:.2},{loss},{malformed},{payload_unavailable},{missing},{incomplete}",
            rate * WORKERS as u64,
            PUTS as f64 * SECOND_NS as f64 / elapsed_ns as f64,
            (cpu - probe_cpu) as f64 / PUTS as f64,
            probe_cpu as f64 / PUTS as f64,
            p99(&mut put),
            p99(&mut visibility),
            p99(&mut late),
        );
    }

    pub fn main() {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        if let [flag, mode, fleet, node, options @ ..] = args.as_slice()
            && flag == "--worker"
        {
            let hot_key = options == ["--hot-key"];
            assert!(options.is_empty() || hot_key);
            return worker(mode, fleet, node.parse().unwrap(), hot_key);
        }
        if !args.iter().any(|arg| arg == "--bench" || arg == "--rate") {
            return;
        }
        let mut args = args.iter().filter(|arg| *arg != "--bench");
        assert_eq!(
            args.next().map(String::as_str),
            Some("--rate"),
            "pass --rate <puts-per-worker-per-second>"
        );
        let rate = args.next().unwrap().parse::<u64>().unwrap();
        assert!((1..=SECOND_NS / WORKERS as u64).contains(&rate));
        let hot_key = match args.next().map(String::as_str) {
            Some("--hot-key") => true,
            None => false,
            Some(option) => panic!("unknown benchmark option: {option}"),
        };
        assert!(args.next().is_none());
        if hot_key {
            println!(
                "run,mode,offered_puts_per_second,puts,puts_per_second,cpu_ns_per_put,put_p99_us,dispatch_to_put_p99_us,ring_loss,malformed,payload_unavailable,final_revision,final_writer,converged_workers"
            );
        } else {
            println!(
                "run,mode,offered_puts_per_second,puts,puts_per_second,cpu_ns_per_put,probe_cpu_ns_per_put,put_p99_us,visibility_p99_us,dispatch_to_put_p99_us,ring_loss,malformed,payload_unavailable,missing_peer_reads,incomplete_puts"
            );
        }
        for (run_id, mode) in
            ["replicated", "shm", "shm", "replicated", "replicated", "shm"]
                .into_iter()
                .enumerate()
        {
            run(mode, run_id + 1, rate, hot_key);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
fn main() {
    platform::main();
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "macos"
)))]
fn main() {
    eprintln!("The fleet write benchmark requires native ring readiness.");
}
