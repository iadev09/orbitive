use std::hint::black_box;
use std::io::{BufRead, BufReader, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion};
use orbit_core::FleetName;
use orbit_core::shm::{ShmRegion, ring_segment_name, try_lock_fleet_exclusive};
use orbit_quota::{
    CasBudget, Decision, Error, FleetGuard, FleetQuota, Geometry, Key,
    LocalGuard, Policy, QuotaStore, ShmOptions, ShmResource,
};

// Explicit workload settings, not library defaults or production advice.
const CAS_ATTEMPTS: usize = 64;
const RECOVERY_ATTEMPTS: usize = 0;
const KEY_BYTES: usize = 64;
const PROCESS_COUNTS: [usize; 4] = [1, 2, 4, 8];
const SMOKE_ATTEMPTS: u64 = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreKind {
    LocalGuard,
    FleetGuard,
    FleetQuota,
}

impl StoreKind {
    const ALL: [Self; 3] =
        [Self::LocalGuard, Self::FleetGuard, Self::FleetQuota];

    fn name(self) -> &'static str {
        match self {
            Self::LocalGuard => "local_guard",
            Self::FleetGuard => "fleet_guard",
            Self::FleetQuota => "fleet_quota",
        }
    }

    fn open(
        self,
        workload: Workload,
        resource: ShmResource,
        processes: usize,
    ) -> Box<dyn QuotaStore> {
        let geometry = workload.shape();
        let policy = workload.policy();
        let budget = CasBudget::new(size(CAS_ATTEMPTS));
        let options = ShmOptions::new(size(processes + 1), RECOVERY_ATTEMPTS);
        match self {
            Self::LocalGuard => {
                Box::new(LocalGuard::new(geometry, policy, budget).unwrap())
            }
            Self::FleetGuard => Box::new(
                FleetGuard::open(resource, geometry, policy, budget, options)
                    .unwrap(),
            ),
            Self::FleetQuota => Box::new(
                FleetQuota::open(resource, geometry, policy, budget, options)
                    .unwrap(),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Workload {
    SharedAllowed,
    SharedLimited,
    PerProcessAllowed,
    ChurnReuse(usize, usize),
    ChurnDebt(usize, usize),
    ChurnFull,
}

impl Workload {
    const ALL: [Self; 15] = [
        Self::SharedAllowed,
        Self::SharedLimited,
        Self::PerProcessAllowed,
        Self::ChurnReuse(64, 1),
        Self::ChurnReuse(64, 2),
        Self::ChurnReuse(64, 4),
        Self::ChurnReuse(256, 4),
        Self::ChurnReuse(256, 8),
        Self::ChurnReuse(256, 16),
        Self::ChurnReuse(1024, 16),
        Self::ChurnReuse(1024, 32),
        Self::ChurnReuse(1024, 64),
        Self::ChurnDebt(64, 1),
        Self::ChurnDebt(1024, 64),
        Self::ChurnFull,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::SharedAllowed => "shared_allowed",
            Self::SharedLimited => "shared_limited",
            Self::PerProcessAllowed => "per_process_allowed",
            Self::ChurnReuse(64, 1) => "churn_reuse",
            Self::ChurnReuse(64, 2) => "churn_reuse_shards_2",
            Self::ChurnReuse(64, 4) => "churn_reuse_shards_4",
            Self::ChurnReuse(256, 4) => "churn_reuse_slots_256_shards_4",
            Self::ChurnReuse(256, 8) => "churn_reuse_slots_256_shards_8",
            Self::ChurnReuse(256, 16) => "churn_reuse_slots_256_shards_16",
            Self::ChurnReuse(1024, 16) => "churn_reuse_slots_1024_shards_16",
            Self::ChurnReuse(1024, 32) => "churn_reuse_slots_1024_shards_32",
            Self::ChurnReuse(1024, 64) => "churn_reuse_slots_1024_shards_64",
            Self::ChurnReuse(_, _) => panic!("unsupported benchmark geometry"),
            Self::ChurnDebt(64, 1) => "churn_debt",
            Self::ChurnDebt(1024, 64) => "churn_debt_slots_1024_shards_64",
            Self::ChurnDebt(_, _) => panic!("unsupported benchmark geometry"),
            Self::ChurnFull => "churn_full",
        }
    }

    fn shape(self) -> Geometry {
        let slots = match self {
            Self::ChurnReuse(slots, _) | Self::ChurnDebt(slots, _) => slots,
            Self::ChurnFull => 16,
            _ => 64,
        };
        Geometry::new(
            size(slots),
            size(self.shards()),
            size(KEY_BYTES),
            size(16),
        )
        .unwrap()
    }

    fn shards(self) -> usize {
        match self {
            Self::ChurnReuse(_, shards) | Self::ChurnDebt(_, shards) => shards,
            _ => 1,
        }
    }

    fn policy(self) -> Policy {
        match self {
            Self::SharedLimited | Self::ChurnFull | Self::ChurnDebt(_, _) => {
                Policy::new(count(1), Duration::MAX, count(1)).unwrap()
            }
            Self::ChurnReuse(_, _) => {
                // One ns debt makes old records eligible for reuse without
                // sleeping or trusting caller-supplied timestamps.
                Policy::new(count(1), Duration::from_nanos(1), count(1))
                    .unwrap()
            }
            _ => Policy::new(
                count(1_000_000_000),
                Duration::from_secs(1),
                count(u64::MAX),
            )
            .unwrap(),
        }
    }
}

fn count(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}

fn size(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

// Each run owns an isolated, random fleet and exactly one kind. Production
// resources are never opened, cleared or discovered by this benchmark.
struct Resource(ShmResource, bool);

impl Resource {
    fn new(kind: StoreKind) -> Self {
        let mut bytes = [0; 4];
        getrandom::fill(&mut bytes).unwrap();
        let fleet =
            FleetName::new(format!("{:08x}", u32::from_le_bytes(bytes)));
        Self(ShmResource::new(fleet, 0), kind != StoreKind::LocalGuard)
    }
}

impl Drop for Resource {
    fn drop(&mut self) {
        if !self.1 {
            return;
        }
        let uid = unsafe { libc::geteuid() };
        let fleet = self.0.fleet();
        let Ok(Some(_hold)) = try_lock_fleet_exclusive(fleet.as_str(), uid)
        else {
            eprintln!("benchmark resource retained: fleet={fleet:?}");
            return;
        };
        let name = ring_segment_name(fleet.as_str(), self.0.kind());
        if let Ok(region) = ShmRegion::open_existing_read_write(
            &name,
            1,
            uid,
            Default::default(),
        ) {
            region.unlink().unwrap();
        }
        for path in orbit_core::shm::companion_lock_files(&name, uid).unwrap() {
            std::fs::remove_file(path).unwrap();
        }
        std::fs::remove_file(orbit_core::shm::fleet_lock_path_for_uid(
            fleet.as_str(),
            uid,
        ))
        .unwrap();
    }
}

struct Worker {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl Worker {
    fn spawn(
        resource: ShmResource,
        workload: Workload,
        processes: usize,
        id: usize,
        kind: StoreKind,
    ) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--quota-worker")
            .arg(resource.fleet().as_str())
            .arg(workload.name())
            .arg(processes.to_string())
            .arg(id.to_string())
            .arg(kind.name())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut worker = Self { child, input, output };
        assert_eq!(worker.line(), "READY");
        worker
    }

    fn send(
        &mut self,
        command: &str,
    ) {
        writeln!(self.input, "{command}").unwrap();
        self.input.flush().unwrap();
    }

    fn line(&mut self) -> String {
        let mut line = String::new();
        assert_ne!(
            self.output.read_line(&mut line).unwrap(),
            0,
            "worker exited"
        );
        line.trim_end().to_owned()
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Only this benchmark's child. Retain stdin until death is confirmed.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Peers {
    // Drop order: children, parent mapping, then exact resource cleanup.
    workers: Vec<Worker>,
    _quota: Box<dyn QuotaStore>,
    _resource: Resource,
}

impl Peers {
    fn new(
        workload: Workload,
        processes: usize,
        kind: StoreKind,
    ) -> Self {
        let resource = Resource::new(kind);
        let quota = kind.open(workload, resource.0, processes);
        seed_workload(quota.as_ref(), workload, processes);
        let workers = (0..processes)
            .map(|id| Worker::spawn(resource.0, workload, processes, id, kind))
            .collect();
        Self { workers, _quota: quota, _resource: resource }
    }

    fn batch(
        &mut self,
        attempts: u64,
        sample_every: Option<u64>,
    ) -> (Duration, Vec<Report>) {
        // Arm every peer before timing. GO/reply pipe traffic is timed;
        // process startup, independent opens and command parsing are not.
        for worker in &mut self.workers {
            worker
                .send(&format!("RUN {attempts} {}", sample_every.unwrap_or(0)));
        }
        for worker in &mut self.workers {
            assert_eq!(worker.line(), "ARMED");
        }
        let mut results = Vec::with_capacity(self.workers.len());
        let start = Instant::now();
        for worker in &mut self.workers {
            worker.send("GO");
        }
        for worker in &mut self.workers {
            let line = worker.line();
            let values: Vec<u64> = line
                .split_whitespace()
                .map(|part| part.parse().unwrap())
                .collect();
            let expected = if sample_every.is_some() { 6 + 4 * 64 } else { 6 };
            assert_eq!(values.len(), expected);
            let counts: [u64; 4] = values[..4].try_into().unwrap();
            assert_eq!(counts.iter().sum::<u64>(), attempts);
            let mut latency = [[0; 64]; 4];
            if sample_every.is_some() {
                for (row, values) in
                    latency.iter_mut().zip(values[6..].as_chunks::<64>().0)
                {
                    row.copy_from_slice(values);
                }
            }
            results.push(Report {
                counts,
                cpu_ns: [values[4], values[5]],
                latency,
            });
        }
        (start.elapsed(), results)
    }
}

#[derive(Debug)]
struct Report {
    counts: [u64; 4],
    cpu_ns: [u64; 2],
    // Per-outcome log2 nanosecond buckets; upper bounds, not exact percentiles.
    latency: [[u64; 64]; 4],
}

fn seed_workload(
    quota: &dyn QuotaStore,
    workload: Workload,
    processes: usize,
) {
    match workload {
        Workload::SharedAllowed | Workload::SharedLimited => {
            seed(quota, b"hot");
        }
        Workload::PerProcessAllowed => {
            for id in 0..processes {
                seed(quota, &(id as u64).to_le_bytes());
            }
        }
        Workload::ChurnFull => {
            // Probes cover the entire table, so all 16 slots are filled.
            for id in 0_u64..16 {
                seed(quota, &id.to_le_bytes());
            }
        }
        Workload::ChurnReuse(_, _) | Workload::ChurnDebt(_, _) => {}
    }
}

fn seed(
    quota: &dyn QuotaStore,
    subject: &[u8],
) {
    assert_eq!(
        quota
            .try_consume(Key::new(b"bench", subject).unwrap(), count(1))
            .unwrap(),
        Decision::Allowed
    );
}

fn worker(args: &[String]) {
    let resource = ShmResource::new(FleetName::new(&args[2]), 0);
    let workload =
        Workload::ALL.into_iter().find(|w| w.name() == args[3]).unwrap();
    let processes: usize = args[4].parse().unwrap();
    let id: u64 = args[5].parse().unwrap();
    let kind =
        StoreKind::ALL.into_iter().find(|kind| kind.name() == args[6]).unwrap();
    let quota = kind.open(workload, resource, processes);
    if kind == StoreKind::LocalGuard {
        seed_workload(quota.as_ref(), workload, processes);
    }
    let mut input = std::io::stdin().lock();
    let mut output = std::io::stdout().lock();
    writeln!(output, "READY").unwrap();
    output.flush().unwrap();
    let mut sequence = 0_u64;
    loop {
        let mut command = String::new();
        if input.read_line(&mut command).unwrap() == 0 {
            return;
        }
        let mut fields =
            command.trim().strip_prefix("RUN ").unwrap().split_whitespace();
        let attempts: u64 = fields.next().unwrap().parse().unwrap();
        let sample_every: u64 = fields.next().unwrap().parse().unwrap();
        assert!(fields.next().is_none());
        writeln!(output, "ARMED").unwrap();
        output.flush().unwrap();
        command.clear();
        assert_ne!(input.read_line(&mut command).unwrap(), 0);
        assert_eq!(command, "GO\n");
        let mut counts = [0_u64; 4];
        let mut latency = [[0_u64; 64]; 4];
        let cpu_start = cpu_time();
        for attempt in 0..attempts {
            let mut subject = [0; 16];
            subject[..8].copy_from_slice(&id.to_le_bytes());
            subject[8..].copy_from_slice(&sequence.to_le_bytes());
            let bytes: &[u8] = match workload {
                Workload::SharedAllowed | Workload::SharedLimited => b"hot",
                Workload::PerProcessAllowed => &subject[..8],
                _ => &subject,
            };
            let key = Key::new(b"bench", bytes).unwrap();
            let sampled =
                sample_every != 0 && attempt.is_multiple_of(sample_every);
            let start = sampled.then(Instant::now);
            let result = black_box(quota.try_consume(black_box(key), count(1)));
            let elapsed = start.map(|start| start.elapsed().as_nanos());
            let outcome = match result {
                Ok(Decision::Allowed) => 0,
                Ok(Decision::Limited { .. }) => 1,
                Err(Error::Busy) => 2,
                Err(Error::CapacityFull) => 3,
                result => panic!("unexpected benchmark outcome: {result:?}"),
            };
            counts[outcome] += 1;
            if let Some(ns) = elapsed {
                let bin = u64::try_from(ns).unwrap().max(1).ilog2() as usize;
                latency[outcome][bin] += 1;
            }
            sequence = sequence.checked_add(1).unwrap();
        }
        let cpu_end = cpu_time();
        assert_eq!(counts.iter().sum::<u64>(), attempts);
        match workload {
            Workload::SharedAllowed
            | Workload::PerProcessAllowed
            | Workload::ChurnReuse(_, _) => {
                assert_eq!(counts[1], 0);
                if !matches!(workload, Workload::ChurnReuse(_, _)) {
                    assert_eq!(counts[3], 0);
                }
            }
            Workload::SharedLimited => {
                assert_eq!(counts[0], 0);
                assert_eq!(counts[3], 0);
            }
            Workload::ChurnFull => {
                if kind == StoreKind::FleetQuota {
                    assert_eq!(counts[0], 0);
                }
                assert_eq!(counts[1], 0);
            }
            Workload::ChurnDebt(_, _) => {
                assert_eq!(counts[1], 0);
            }
        }
        write!(
            output,
            "{} {} {} {} {} {}",
            counts[0],
            counts[1],
            counts[2],
            counts[3],
            cpu_end[0] - cpu_start[0],
            cpu_end[1] - cpu_start[1]
        )
        .unwrap();
        if sample_every != 0 {
            for row in latency {
                for count in row {
                    write!(output, " {count}").unwrap();
                }
            }
        }
        writeln!(output).unwrap();
        output.flush().unwrap();
    }
}

fn cpu_time() -> [u64; 2] {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the complete output on success.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let usage = unsafe { usage.assume_init() };
    [usage.ru_utime, usage.ru_stime].map(|time| {
        u64::try_from(time.tv_sec)
            .unwrap()
            .checked_mul(1_000_000_000)
            .unwrap()
            .checked_add(u64::try_from(time.tv_usec).unwrap() * 1_000)
            .unwrap()
    })
}

fn percentile(
    histogram: &[u64; 64],
    percentile: u64,
) -> Option<u64> {
    let total: u64 = histogram.iter().sum();
    if total == 0 {
        return None;
    }
    let target = (total * percentile).div_ceil(100);
    let mut seen = 0;
    for (bin, count) in histogram.iter().enumerate() {
        seen += count;
        if seen >= target {
            return Some(if bin == 63 {
                u64::MAX
            } else {
                (1_u64 << (bin + 1)) - 1
            });
        }
    }
    unreachable!("histogram count covers target")
}

fn profile(args: &[String]) {
    // Cargo adds this harness flag even for a custom benchmark main.
    let args = if args.last().is_some_and(|arg| arg == "--bench") {
        &args[..args.len() - 1]
    } else {
        args
    };
    assert_eq!(
        args.len(),
        8,
        "--quota-profile STORE CASE PROCESSES ATTEMPTS SAMPLE_EVERY WARMUP_ATTEMPTS"
    );
    let kind =
        StoreKind::ALL.into_iter().find(|kind| kind.name() == args[2]).unwrap();
    let workload =
        Workload::ALL.into_iter().find(|case| case.name() == args[3]).unwrap();
    let processes: usize = args[4].parse().unwrap();
    let attempts: u64 = args[5].parse().unwrap();
    let stride: u64 = args[6].parse().unwrap();
    let warmup: u64 = args[7].parse().unwrap();
    assert!(processes > 0 && attempts > 0 && warmup > 0);
    let mut peers = Peers::new(workload, processes, kind);
    peers.batch(warmup, None);
    let (elapsed, reports) =
        peers.batch(attempts, (stride != 0).then_some(stride));
    let mut counts = [0_u64; 4];
    let mut cpu_ns = [0_u64; 2];
    let mut latency = [[0_u64; 64]; 4];
    for report in reports {
        for (sum, value) in counts.iter_mut().zip(report.counts) {
            *sum += value;
        }
        for (sum, value) in cpu_ns.iter_mut().zip(report.cpu_ns) {
            *sum += value;
        }
        for (sum, row) in latency.iter_mut().zip(report.latency) {
            for (sum, value) in sum.iter_mut().zip(row) {
                *sum += value;
            }
        }
    }
    let total: u64 = counts.iter().sum();
    println!(
        "quota_profile store={} case={} processes={processes} attempts={total} sample_every={stride} elapsed_ns={} allowed={} limited={} busy={} full={} allowed_per_sec={:.0} busy_percent={:.3} full_percent={:.3} user_cpu_ns_per_attempt={:.1} system_cpu_ns_per_attempt={:.1}",
        kind.name(),
        workload.name(),
        elapsed.as_nanos(),
        counts[0],
        counts[1],
        counts[2],
        counts[3],
        counts[0] as f64 / elapsed.as_secs_f64(),
        100.0 * counts[2] as f64 / total as f64,
        100.0 * counts[3] as f64 / total as f64,
        cpu_ns[0] as f64 / total as f64,
        cpu_ns[1] as f64 / total as f64
    );
    for (outcome, histogram) in
        ["allowed", "limited", "busy", "full"].into_iter().zip(latency)
    {
        let samples: u64 = histogram.iter().sum();
        println!(
            "quota_latency store={} case={} processes={processes} outcome={outcome} samples={samples} p50_upper_ns={:?} p95_upper_ns={:?} p99_upper_ns={:?}",
            kind.name(),
            workload.name(),
            percentile(&histogram, 50),
            percentile(&histogram, 95),
            percentile(&histogram, 99)
        );
    }
}

pub fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).is_some_and(|arg| arg == "--quota-worker") {
        worker(&args);
        return;
    }
    if args.get(1).is_some_and(|arg| arg == "--quota-profile") {
        profile(&args);
        return;
    }
    if args.get(1).is_some_and(|arg| arg == "--quota-smoke") {
        for kind in StoreKind::ALL {
            for workload in Workload::ALL {
                let mut peers = Peers::new(workload, 2, kind);
                // Two batches check persistent workers and non-repeating churn.
                for _ in 0..2 {
                    let (_, reports) = peers.batch(SMOKE_ATTEMPTS, None);
                    let outcomes: Vec<_> =
                        reports.iter().map(|report| report.counts).collect();
                    eprintln!(
                        "smoke store={} case={} outcomes={outcomes:?}",
                        kind.name(),
                        workload.name()
                    );
                }
            }
        }
        return;
    }
    let mut criterion = Criterion::default().configure_from_args();
    for kind in StoreKind::ALL {
        let group_name = match kind {
            StoreKind::FleetQuota => "shm_processes",
            StoreKind::FleetGuard => "fleet_guard_processes",
            StoreKind::LocalGuard => "local_guard_processes",
        };
        let mut group = criterion.benchmark_group(group_name);
        for workload in Workload::ALL {
            for processes in PROCESS_COUNTS {
                let mut totals = vec![[0_u128; 4]; processes];
                let mut elapsed = Duration::ZERO;
                let mut cpu_ns = [0_u128; 2];
                let mut peers = None;
                group.bench_function(
                    BenchmarkId::new(workload.name(), processes),
                    |b| {
                        let peers = peers.get_or_insert_with(|| {
                            Peers::new(workload, processes, kind)
                        });
                        b.iter_custom(|attempts| {
                            let (duration, outcomes) =
                                peers.batch(attempts, None);
                            elapsed += duration;
                            for (total, report) in
                                totals.iter_mut().zip(outcomes)
                            {
                                for (sum, value) in
                                    cpu_ns.iter_mut().zip(report.cpu_ns)
                                {
                                    *sum += u128::from(value);
                                }
                                for (total, count) in
                                    total.iter_mut().zip(report.counts)
                                {
                                    *total += u128::from(count);
                                }
                            }
                            duration / processes as u32
                        });
                    },
                );
                if elapsed.is_zero() {
                    continue;
                }
                let mut sum = [0_u128; 4];
                for counts in &totals {
                    for (sum, count) in sum.iter_mut().zip(counts) {
                        *sum += count;
                    }
                }
                let attempts = sum.iter().sum::<u128>() as f64;
                eprintln!(
                    "quota_cpu store={} case={} processes={processes} user_ns_per_attempt={:.1} system_ns_per_attempt={:.1}",
                    kind.name(),
                    workload.name(),
                    cpu_ns[0] as f64 / attempts,
                    cpu_ns[1] as f64 / attempts
                );
                eprintln!(
                    "shm_summary store={} case={} processes={processes} allowed={} limited={} busy={} full={} allowed_per_sec={:.0} limited_per_sec={:.0} busy_percent={:.3} full_percent={:.3} elapsed_ns={} per_process={totals:?} (columns=allowed,limited,busy,full; includes warmup/calibration)",
                    kind.name(),
                    workload.name(),
                    sum[0],
                    sum[1],
                    sum[2],
                    sum[3],
                    sum[0] as f64 / elapsed.as_secs_f64(),
                    sum[1] as f64 / elapsed.as_secs_f64(),
                    100.0 * sum[2] as f64 / attempts,
                    100.0 * sum[3] as f64 / attempts,
                    elapsed.as_nanos(),
                );
            }
        }
        group.finish();
    }
    criterion.final_summary();
}
