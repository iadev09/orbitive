use std::hint::black_box;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use criterion::{
    BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main,
};
use orbit_quota::{
    CasBudget, Decision, Error, Geometry, Key, MemoryQuota, Policy, QuotaStore,
};

fn count(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn geometry(
    slots: usize,
    probes: usize,
) -> Geometry {
    Geometry::new(
        NonZeroUsize::new(slots).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(64).unwrap(),
        NonZeroUsize::new(probes).unwrap(),
    )
    .unwrap()
}

fn available(geometry: Geometry) -> MemoryQuota {
    available_with_budget(geometry, 64)
}

fn available_with_budget(
    geometry: Geometry,
    attempts: usize,
) -> MemoryQuota {
    // Explicit benchmark policy, not a recommended production setting.
    let policy = Policy::new(
        count(1_000_000_000),
        Duration::from_secs(1),
        count(u64::MAX),
    )
    .unwrap();
    MemoryQuota::new(
        geometry,
        policy,
        CasBudget::new(NonZeroUsize::new(attempts).unwrap()),
    )
    .unwrap()
}

fn exhausted(geometry: Geometry) -> MemoryQuota {
    let policy = Policy::new(count(1), Duration::MAX, count(1)).unwrap();
    let quota = MemoryQuota::new(
        geometry,
        policy,
        CasBudget::new(NonZeroUsize::new(64).unwrap()),
    )
    .unwrap();
    assert_eq!(
        quota
            .try_consume(Key::new(b"bench", b"hot").unwrap(), count(1))
            .unwrap(),
        Decision::Allowed
    );
    quota
}

fn admission(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_admission");
    let key = Key::new(b"bench", b"hot").unwrap();
    group.bench_function("new_key_allowed", |b| {
        b.iter_batched_ref(
            || available(geometry(64, 4)),
            |quota| {
                assert_eq!(
                    black_box(quota.try_consume(black_box(key), count(1)))
                        .unwrap(),
                    Decision::Allowed
                )
            },
            BatchSize::SmallInput,
        );
    });
    for slots in [64, 4_096, 65_536] {
        let quota = available(geometry(slots, 4));
        quota.try_consume(key, count(1)).unwrap();
        group.bench_with_input(
            BenchmarkId::new("existing_allowed", slots),
            &quota,
            |b, quota| {
                b.iter(|| {
                    assert_eq!(
                        black_box(quota.try_consume(black_box(key), count(1)))
                            .unwrap(),
                        Decision::Allowed
                    )
                });
            },
        );
        let quota = exhausted(geometry(slots, 4));
        group.bench_with_input(
            BenchmarkId::new("existing_limited", slots),
            &quota,
            |b, quota| {
                b.iter(|| {
                    assert!(matches!(
                        black_box(quota.try_consume(black_box(key), count(1)))
                            .unwrap(),
                        Decision::Limited { .. }
                    ))
                });
            },
        );
    }
    for probes in [1, 4, 16] {
        let quota = exhausted(geometry(probes, probes));
        for index in 1..probes {
            quota
                .try_consume(
                    Key::new(b"fill", &index.to_le_bytes()).unwrap(),
                    count(1),
                )
                .unwrap();
        }
        let absent = Key::new(b"bench", b"absent").unwrap();
        group.bench_with_input(
            BenchmarkId::new("new_key_full", probes),
            &quota,
            |b, quota| {
                b.iter(|| {
                    assert!(matches!(
                        black_box(
                            quota.try_consume(black_box(absent), count(1))
                        ),
                        Err(Error::CapacityFull)
                    ))
                });
            },
        );
    }
    let quota = available(geometry(64, 4));
    let oversized = Key::new(b"bench", &[0; 65]).unwrap();
    group.bench_function("oversized_key", |b| {
        b.iter(|| {
            assert!(matches!(
                black_box(quota.try_consume(black_box(oversized), count(1))),
                Err(Error::KeyTooLarge)
            ))
        });
    });
    group.bench_function("weighted_allowed", |b| {
        b.iter(|| {
            assert_eq!(
                black_box(quota.try_consume(black_box(key), count(32)))
                    .unwrap(),
                Decision::Allowed
            )
        });
    });
    group.finish();
}

fn contention(c: &mut Criterion) {
    contention_case(c, "memory_hot_key_contention", available, &[2, 4, 8]);
    contention_case(
        c,
        "memory_hot_key_limited_contention",
        exhausted,
        &[2, 4, 8],
    );
    for attempts in [16, 32] {
        contention_case(
            c,
            &format!("memory_hot_key_cas_budget_{attempts}"),
            |geometry| available_with_budget(geometry, attempts),
            &[8],
        );
    }
}

fn contention_case(
    c: &mut Criterion,
    name: &str,
    make: impl Fn(Geometry) -> MemoryQuota,
    thread_counts: &[usize],
) {
    let mut group = c.benchmark_group(name);
    for &threads in thread_counts {
        let mut observed = vec![[0_u128; 3]; threads];
        let mut observed_elapsed = Duration::ZERO;
        group.bench_function(BenchmarkId::new("amortized_attempt", threads), |b| {
            b.iter_custom(|iterations| {
                let quota = make(geometry(64, 4));
                let barrier = Arc::new(Barrier::new(threads + 1));
                std::thread::scope(|scope| {
                    let mut workers = Vec::new();
                    for _ in 0..threads {
                        let quota = quota.clone();
                        let barrier = Arc::clone(&barrier);
                        workers.push(scope.spawn(move || {
                            let key = Key::new(b"bench", b"hot").unwrap();
                            let mut outcomes = [0_u64; 3];
                            barrier.wait();
                            for _ in 0..iterations {
                                match black_box(quota.try_consume(black_box(key), count(1))) {
                                    Ok(Decision::Allowed) => outcomes[0] += 1,
                                    Ok(Decision::Limited { .. }) => outcomes[1] += 1,
                                    Err(Error::Busy) => outcomes[2] += 1,
                                    result => panic!("unexpected benchmark outcome: {result:?}"),
                                }
                            }
                            outcomes
                        }));
                    }
                    let start = Instant::now();
                    barrier.wait();
                    let mut totals = [0_u64; 3];
                    let mut per_thread = Vec::with_capacity(threads);
                    for worker in workers {
                        let outcomes = worker.join().unwrap();
                        for (total, outcome) in totals.iter_mut().zip(outcomes) {
                            *total += outcome;
                        }
                        per_thread.push(outcomes);
                    }
                    let elapsed = start.elapsed();
                    assert_eq!(totals.iter().sum::<u64>(), iterations * threads as u64);
                    // Busy is not admitted throughput. The measured batch
                    // includes start/join overhead, not thread creation.
                    black_box(totals);
                    observed_elapsed += elapsed;
                    for (observed, outcomes) in observed.iter_mut().zip(per_thread) {
                        for (total, outcome) in observed.iter_mut().zip(outcomes) {
                            *total += u128::from(outcome);
                        }
                    }
                    // Criterion counts iterations per worker. Normalize wall
                    // time to total attempts, not per-request latency.
                    elapsed / threads as u32
                })
            });
        });
        if observed_elapsed.is_zero() {
            continue; // Criterion filtered this case out.
        }
        let totals =
            observed.iter().fold([0_u128; 3], |mut totals, outcomes| {
                for (total, outcome) in totals.iter_mut().zip(outcomes) {
                    *total += outcome;
                }
                totals
            });
        let allowed_per_sec = totals[0] as f64 / observed_elapsed.as_secs_f64();
        let limited_per_sec = totals[1] as f64 / observed_elapsed.as_secs_f64();
        let busy_percent =
            100.0 * totals[2] as f64 / totals.iter().sum::<u128>() as f64;
        eprintln!(
            "contention_summary case={name} threads={threads} allowed={} limited={} busy={} elapsed_ns={} allowed_per_sec={allowed_per_sec:.0} limited_per_sec={limited_per_sec:.0} busy_percent={busy_percent:.2} per_thread={observed:?} (columns=allowed,limited,busy; includes warmup/calibration)",
            totals[0],
            totals[1],
            totals[2],
            observed_elapsed.as_nanos(),
        );
    }
    group.finish();
}

criterion_group!(benches, admission, contention);
criterion_main!(benches);
