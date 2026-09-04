#[cfg(unix)]
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use criterion::Throughput;
use criterion::{Criterion, criterion_group, criterion_main};
#[cfg(unix)]
use orbit_cache::{Cache, CacheMutation, CacheRead, CacheTransport, DefaultCacheLayout};
#[cfg(unix)]
use orbit_core::{Fleet, NodeId};

#[cfg(unix)]
const WRITER_COUNT: usize = 4;
#[cfg(unix)]
const WRITES_PER_WRITER: usize = 128;
#[cfg(unix)]
const VALUE_LEN: usize = 256;
#[cfg(unix)]
const HOT_KEY: &[u8] = b"shared-hot-key";

#[cfg(unix)]
fn fresh_fleet_name() -> &'static str {
    let pid = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    Box::leak(format!("b{pid:04x}{nonce:08x}").into_boxed_str())
}

#[cfg(unix)]
fn multi_writer_same_key(criterion: &mut Criterion) {
    let fleet_name = fresh_fleet_name();
    let fleets = (0..WRITER_COUNT)
        .map(|node| {
            Arc::new(
                Fleet::join_shm_as(fleet_name, WRITER_COUNT as u8, NodeId::new(node as u16))
                    .expect("join benchmark fleet"),
            )
        })
        .collect::<Vec<_>>();

    let resetter = CacheTransport::<DefaultCacheLayout>::new(fleets[0].clone())
        .expect("create reset transport");
    resetter.reset_rings().expect("reset benchmark rings");

    let l1_capacity = NonZeroUsize::new(16).expect("benchmark L1 capacity is non-zero");
    let writers = fleets
        .iter()
        .map(|fleet| {
            let cache = Cache::<DefaultCacheLayout>::new(fleet.clone())
                .expect("create writer cache connection");
            Arc::new(
                cache
                    .open_store(b"default", l1_capacity)
                    .expect("create writer store"),
            )
        })
        .collect::<Vec<_>>();
    let observer_cache =
        Cache::<DefaultCacheLayout>::new(fleets[0].clone()).expect("create observer cache");
    let observer = observer_cache
        .open_store(b"default", l1_capacity)
        .expect("create observer store");
    let mutation_observer =
        CacheTransport::<DefaultCacheLayout>::new(fleets[0].clone()).expect("create observer");
    let mut mutation_cursor = mutation_observer.cursor_at_head();

    let values = (0..WRITER_COUNT)
        .map(|writer| vec![b'a' + writer as u8; VALUE_LEN])
        .collect::<Vec<_>>();
    let writes_per_batch = (WRITER_COUNT * WRITES_PER_WRITER) as u64;

    let mut group = criterion.benchmark_group("cache_shm");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(writes_per_batch));
    group.bench_function("four_writers_same_key", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut measured = Duration::ZERO;

            for _ in 0..iterations {
                let started = Instant::now();
                std::thread::scope(|scope| {
                    let handles = writers.iter().zip(&values).map(|(cache, value)| {
                        scope.spawn(move || {
                            for _ in 0..WRITES_PER_WRITER {
                                cache
                                    .put(HOT_KEY, value, None)
                                    .expect("publish benchmark value");
                            }
                        })
                    });

                    for handle in handles {
                        handle.join().expect("benchmark writer panicked");
                    }
                });
                measured += started.elapsed();

                let mutations = mutation_observer.poll(&mut mutation_cursor);
                assert_eq!(mutations.mutations.len(), WRITER_COUNT * WRITES_PER_WRITER);
                assert!(mutations.loss.is_empty());
                assert_eq!(mutations.malformed, 0);
                assert!(
                    mutations
                        .mutations
                        .windows(2)
                        .all(|pair| { pair[0].revision().sequence < pair[1].revision().sequence }),
                    "fleet-wide cache sequences must be unique"
                );
                let CacheMutation::Put {
                    revision: expected_revision,
                    payload,
                    ..
                } = mutations.mutations.last().expect("at least one mutation")
                else {
                    panic!("benchmark only publishes put mutations");
                };
                let expected_value = mutation_observer
                    .read_payload(*payload)
                    .expect("newest payload must remain available");

                let poll = observer_cache.poll();
                assert_eq!(poll.observed, WRITER_COUNT * WRITES_PER_WRITER);
                assert_eq!(poll.applied, poll.observed);
                assert_eq!(poll.ignored, 0);
                assert!(poll.loss.is_empty());
                assert_eq!(poll.malformed, 0);
                assert!(poll.payload_unavailable.is_empty());
                assert!(!poll.resync_required);

                let CacheRead::Hit(actual) = observer.read(HOT_KEY) else {
                    panic!("observer must converge on the shared key");
                };
                assert_eq!(actual.revision, *expected_revision);
                assert_eq!(actual.value, expected_value);
            }

            measured
        });
    });
    group.finish();

    observer_cache
        .transport()
        .unlink_rings()
        .expect("unlink benchmark rings");
}

#[cfg(not(unix))]
fn multi_writer_same_key(_: &mut Criterion) {}

criterion_group!(benches, multi_writer_same_key);
criterion_main!(benches);
