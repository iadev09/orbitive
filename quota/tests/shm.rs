use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Barrier;
use std::time::Duration;

use super::*;

#[path = "guards_shm.rs"]
mod guards;

struct Fixture(ShmResource);

impl Fixture {
    fn new() -> Self {
        let mut random = [0; 4];
        getrandom::fill(&mut random).unwrap();
        let fleet =
            FleetName::new(format!("{:08x}", u32::from_le_bytes(random)));
        Self(ShmResource::new(fleet, 0)) // Isolated test namespace; no reserved kind.
    }
    fn open(
        &self,
        clock: Arc<TestClock>,
        burst: u64,
        owners: usize,
        recovery: usize,
    ) -> ShmQuota {
        ShmQuota::with_clock(
            self.0,
            geometry(1),
            policy(burst),
            attempts(64),
            options(owners, recovery),
            Box::new(clock),
            [7; 32],
        )
        .unwrap()
    }
    fn name(&self) -> String {
        ring_segment_name(self.0.fleet.as_str(), self.0.kind)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let uid = unsafe { libc::geteuid() };
        let Ok(Some(_exclusive)) = orbit_core::shm::try_lock_fleet_exclusive(
            self.0.fleet.as_str(),
            uid,
        ) else {
            return;
        };
        // Exact isolated resource only, after every handle/member has left.
        let name = self.name();
        if let Ok(region) = ShmRegion::open_existing_read_write(
            &name,
            size_of::<Header>(),
            uid,
            Default::default(),
        ) {
            region.unlink().unwrap();
        }
        for path in orbit_core::shm::companion_lock_files(&name, uid).unwrap() {
            std::fs::remove_file(path).unwrap();
        }
        std::fs::remove_file(orbit_core::shm::fleet_lock_path_for_uid(
            self.0.fleet.as_str(),
            uid,
        ))
        .unwrap();
    }
}

#[derive(Default)]
struct TestClock(AtomicU64, AtomicU64);
impl Clock for Arc<TestClock> {
    fn now(&self) -> Result<u128, Error> {
        self.1.fetch_add(1, Ordering::Relaxed);
        Ok(u128::from(self.0.load(Ordering::Relaxed)))
    }
}
fn count(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}
fn attempts(n: usize) -> CasBudget {
    CasBudget::new(NonZeroUsize::new(n).unwrap())
}
fn options(
    owners: usize,
    recovery: usize,
) -> ShmOptions {
    ShmOptions::new(NonZeroUsize::new(owners).unwrap(), recovery)
}
fn geometry(slots: usize) -> Geometry {
    Geometry::new(
        NonZeroUsize::new(slots).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(32).unwrap(),
        NonZeroUsize::new(slots).unwrap(),
    )
    .unwrap()
}
fn policy(burst: u64) -> Policy {
    Policy::new(count(1), Duration::from_nanos(100), count(burst)).unwrap()
}
fn key() -> Key<'static> {
    Key::new(b"test", b"key").unwrap()
}
fn other() -> Key<'static> {
    Key::new(b"test", b"other").unwrap()
}
fn budget() -> Budget {
    Budget::new(attempts(64), 8)
}

#[test]
fn fleet_snapshots_use_committed_values_without_additional_clock_or_cas_work() {
    for retention in [Retention::Guard, Retention::Strict] {
        let plain_fixture = Fixture::new();
        let report_fixture = Fixture::new();
        let plain_clock = Arc::new(TestClock::default());
        let report_clock = Arc::new(TestClock::default());
        let open = |fixture: &Fixture, clock: &Arc<TestClock>, budget| {
            ShmQuota::with_retention(
                fixture.0,
                geometry(1),
                (policy(4), retention),
                attempts(budget),
                options(3, 3),
                Box::new(Arc::clone(clock)),
                [7; 32],
            )
            .unwrap()
        };
        let plain = open(&plain_fixture, &plain_clock, 64);
        let report = open(&report_fixture, &report_clock, 64);
        // An independent peer must report the debt committed by the first.
        let peer = open(&report_fixture, &report_clock, 64);
        for (index, (now, cost, remaining)) in [
            (0, 2, Some(2)),
            (25, 1, Some(1)),
            (25, 2, None),
            (1000, 1, Some(3)),
        ]
        .into_iter()
        .enumerate()
        {
            plain_clock.0.store(now, Ordering::Relaxed);
            report_clock.0.store(now, Ordering::Relaxed);
            // Opening a SHM handle validates its clock once. Compare admission
            // deltas, not lifetime totals (the reporting resource has a peer).
            let plain_reads = plain_clock.1.load(Ordering::Relaxed);
            let report_reads = report_clock.1.load(Ordering::Relaxed);
            let decision = plain.try_consume(key(), count(cost)).unwrap();
            let reported = if index % 2 == 0 { &report } else { &peer };
            let (result, allocations) = crate::test_allocator::measured(|| {
                reported.try_consume_with_snapshot(key(), count(cost))
            });
            let admission = result.unwrap();
            assert_eq!(allocations, 0);
            assert_eq!(admission.decision, decision);
            assert_eq!(
                admission.snapshot,
                remaining
                    .map(|remaining| crate::Snapshot { limit: 4, remaining })
            );
            assert_eq!(
                plain_clock.1.load(Ordering::Relaxed) - plain_reads,
                report_clock.1.load(Ordering::Relaxed) - report_reads
            );
        }
        // One attempt cannot install a key in either path. Do not assert exact
        // minimum-success budgets: weak CAS may fail spuriously on native CPUs.
        let first_fixture = Fixture::new();
        let second_fixture = Fixture::new();
        let clock = Arc::new(TestClock::default());
        let first = open(&first_fixture, &clock, 1);
        let second = open(&second_fixture, &clock, 1);
        assert!(matches!(first.try_consume(key(), count(1)), Err(Error::Busy)));
        assert!(matches!(
            second.try_consume_with_snapshot(key(), count(1)),
            Err(Error::Busy)
        ));
    }
}

#[test]
fn opens_share_committed_debt_and_reopen_does_not_reset() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let first = fixture.open(Arc::clone(&clock), 2, 3, 3);
    let second = fixture.open(Arc::clone(&clock), 2, 3, 3);
    assert_eq!(first.resource(), second.resource());
    assert_eq!(first.inner.seed, second.inner.seed);
    assert_eq!(first.inner.location(key()), second.inner.location(key()));
    assert_eq!(first.try_consume(key(), count(2)).unwrap(), Decision::Allowed);
    assert!(matches!(
        second.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
    drop(first);
    drop(second);
    let reopened = fixture.open(clock, 2, 3, 0);
    assert!(matches!(
        reopened.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
}

#[test]
fn namespace_boundaries_and_separate_resources_keep_independent_debt() {
    let fixture = Fixture::new();
    let other_resource = Fixture::new();
    let make = |resource| {
        ShmQuota::with_clock(
            resource,
            geometry(4),
            policy(1),
            attempts(64),
            options(1, 0),
            Box::new(Arc::new(TestClock::default())),
            [7; 32],
        )
        .unwrap()
    };
    let quota = make(fixture.0);
    let independent = make(other_resource.0);
    let left = Key::new(b"ab", b"c").unwrap();
    let right = Key::new(b"a", b"bc").unwrap();
    for key in [left, right] {
        assert_eq!(
            quota.try_consume(key, count(1)).unwrap(),
            Decision::Allowed
        );
        assert!(matches!(
            quota.try_consume(key, count(1)).unwrap(),
            Decision::Limited { .. }
        ));
    }
    assert_eq!(
        independent.try_consume(left, count(1)).unwrap(),
        Decision::Allowed
    );
}

#[test]
fn owner_capacity_is_explicit_clones_share_a_row_and_reuse_advances_generation()
{
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let first = fixture.open(Arc::clone(&clock), 1, 1, 0);
    let cloned = first.clone();
    let generation = first.inner.owner_generation;
    let open = || {
        ShmQuota::with_clock(
            fixture.0,
            geometry(1),
            policy(1),
            attempts(64),
            options(1, 0),
            Box::new(Arc::clone(&clock)),
            [7; 32],
        )
    };
    assert!(matches!(open(), Err(Error::CapacityFull)));
    drop(first);
    assert!(matches!(open(), Err(Error::CapacityFull)));
    drop(cloned);
    assert_eq!(open().unwrap().inner.owner_generation, generation + 1);
}

#[test]
fn incompatible_policy_domain_geometry_or_foreign_magic_never_reset_state() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let first = fixture.open(Arc::clone(&clock), 1, 2, 2);
    first.try_consume(key(), count(1)).unwrap();
    for (shape, limit, owners, domain) in [
        (geometry(1), policy(2), 2, [7; 32]),
        (geometry(1), policy(1), 3, [7; 32]),
        (geometry(1), policy(1), 2, [8; 32]),
        (
            Geometry::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(16).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap(),
            policy(1),
            2,
            [7; 32],
        ),
    ] {
        assert!(
            ShmQuota::with_clock(
                fixture.0,
                shape,
                limit,
                attempts(64),
                options(owners, 2),
                Box::new(Arc::clone(&clock)),
                domain
            )
            .is_err()
        );
    }
    assert!(matches!(
        first.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
    let foreign = Fixture::new();
    let size = Layout::new(geometry(1), 2).unwrap().size;
    let (region, hold) =
        ShmRegion::open_or_create_locked(&foreign.name(), size).unwrap();
    let magic = unsafe { &(*region.as_ptr().cast::<Header>()).magic };
    magic.store(123, Ordering::Release);
    drop(hold);
    assert!(
        ShmQuota::with_clock(
            foreign.0,
            geometry(1),
            policy(1),
            attempts(64),
            options(2, 2),
            Box::new(clock),
            [7; 32]
        )
        .is_err()
    );
    assert_eq!(magic.load(Ordering::Acquire), 123);
}

#[test]
fn initialization_recovers_only_tagged_unpublished_state() {
    let fixture = Fixture::new();
    let size = Layout::new(geometry(1), 2).unwrap().size;
    let (region, hold) =
        ShmRegion::open_or_create_locked(&fixture.name(), size).unwrap();
    unsafe {
        let header = region.as_ptr().cast::<Header>();
        (*header).magic.store(MAGIC, Ordering::Release);
        (*header).state.store(INITIALIZING, Ordering::Release);
        std::ptr::addr_of_mut!((*header).spec).write([0x55; 160]);
    }
    drop(hold);
    let quota = fixture.open(Arc::new(TestClock::default()), 1, 2, 2);
    assert_eq!(quota.try_consume(key(), count(1)).unwrap(), Decision::Allowed);
}

#[test]
fn live_pins_prevent_reuse_and_stale_generation_cannot_charge_replacement() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let first = fixture.open(Arc::clone(&clock), 1, 2, 2);
    let second = fixture.open(Arc::clone(&clock), 1, 2, 2);
    first.try_consume(key(), count(1)).unwrap();
    clock.0.store(100, Ordering::Relaxed);
    let pin = first.inner.pin(0, None, &mut budget()).unwrap().unwrap();
    let generation = pin.generation;
    assert!(matches!(second.try_consume(other(), count(1)), Err(Error::Busy)));
    assert_eq!(pin.bank().tat.load(Ordering::Acquire), 100);
    assert_eq!(first.try_consume(key(), count(1)).unwrap(), Decision::Allowed);
    drop(pin);
    clock.0.store(200, Ordering::Relaxed);
    assert_eq!(
        second.try_consume(other(), count(1)).unwrap(),
        Decision::Allowed
    );
    assert!(matches!(
        first.inner.pin(0, Some(generation), &mut budget()),
        Err(Error::Busy)
    ));
    assert_eq!(
        first.inner.counter(first.inner.owner, 0).load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        first.inner.slot(0).control.load(Ordering::SeqCst),
        generation + 1
    );
}

#[test]
fn pin_freeze_handshake_rejects_late_readers_and_unwind_restores_active_bank() {
    let fixture = Fixture::new();
    let quota = fixture.open(Arc::new(TestClock::default()), 1, 1, 0);
    quota.try_consume(key(), count(1)).unwrap();
    let inner = &quota.inner;
    let generation = inner.slot(0).control.load(Ordering::SeqCst);
    let mut work = budget();
    let _hold = inner.lock_shard(0, &mut work).unwrap();
    let frozen = inner.freeze(0, generation, &mut work).unwrap();
    assert!(matches!(inner.pin(0, None, &mut work), Err(Error::Busy)));
    // A delayed increment after the writer's scan cannot validate FROZEN.
    inner.counter(inner.owner, 0).fetch_add(1, Ordering::SeqCst);
    assert_ne!(inner.slot(0).control.load(Ordering::SeqCst), generation);
    inner.counter(inner.owner, 0).fetch_sub(1, Ordering::SeqCst);
    drop(frozen);
    assert_eq!(inner.slot(0).control.load(Ordering::SeqCst), generation);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let frozen = inner.freeze(0, generation, &mut budget()).unwrap();
        // Partial inactive write; no committed bank mutation.
        inner.slot(0).banks[((generation + 1) & 1) as usize]
            .namespace_len
            .store(u64::MAX, Ordering::Relaxed);
        let _keep = frozen;
        panic!("injected inactive-bank failure");
    }));
    assert!(result.is_err());
    assert!(matches!(
        quota.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
}

#[test]
fn exhausted_cas_budget_and_pin_counter_never_wrap_or_install() {
    let fixture = Fixture::new();
    let quota = ShmQuota::with_clock(
        fixture.0,
        geometry(1),
        policy(1),
        attempts(1),
        options(1, 0),
        Box::new(Arc::new(TestClock::default())),
        [7; 32],
    )
    .unwrap();
    assert!(matches!(quota.try_consume(key(), count(1)), Err(Error::Busy)));
    assert_eq!(quota.inner.slot(0).control.load(Ordering::SeqCst), 0);
    let counter = quota.inner.counter(quota.inner.owner, 0);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    counter.store(u64::MAX, Ordering::SeqCst);
    assert!(matches!(
        quota.inner.pin(0, None, &mut budget()),
        Err(Error::Busy)
    ));
    assert_eq!(counter.load(Ordering::SeqCst), u64::MAX);
    counter.store(0, Ordering::SeqCst);
}

#[test]
fn generation_exhaustion_retires_without_reusing_its_token() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let quota = fixture.open(Arc::clone(&clock), 1, 1, 0);
    quota.try_consume(key(), count(1)).unwrap();
    quota.inner.slot(0).control.store(GENERATION, Ordering::SeqCst); // Same active bank, maximal token.
    clock.0.store(100, Ordering::Relaxed);
    assert!(matches!(
        quota.try_consume(other(), count(1)),
        Err(Error::CapacityFull)
    ));
    assert_eq!(
        quota.inner.slot(0).control.load(Ordering::SeqCst),
        GENERATION | RETIRED
    );
}

#[test]
fn delayed_pin_cannot_validate_a_replaced_generation() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let quota = fixture.open(Arc::clone(&clock), 1, 1, 0);
    quota.try_consume(key(), count(1)).unwrap();
    let inner = &quota.inner;
    let observed = inner.slot(0).control.load(Ordering::SeqCst);
    clock.0.store(100, Ordering::Relaxed);
    let mut work = budget();
    let _hold = inner.lock_shard(0, &mut work).unwrap();
    let frozen = inner.freeze(0, observed, &mut work).unwrap();
    frozen.publish(other(), 200).unwrap();
    // Reader observed the descriptor before freezing, but publishes its pin
    // after the writer saw zero. Production validation must reject it.
    inner.counter(inner.owner, 0).fetch_add(1, Ordering::SeqCst);
    assert!(matches!(inner.validate_pin(0, observed, None), Err(Error::Busy)));
    assert_eq!(inner.counter(inner.owner, 0).load(Ordering::SeqCst), 0);
    assert_eq!(inner.slot(0).banks[0].tat.load(Ordering::Acquire), 200);
}

#[test]
fn mapped_handles_share_weighted_atomic_admission_without_overgrant() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let handles: Vec<_> =
        (0..8).map(|_| fixture.open(Arc::clone(&clock), 31, 8, 0)).collect();
    handles[0].try_consume(key(), count(1)).unwrap();
    let barrier = Barrier::new(8);
    let charged = std::thread::scope(|scope| {
        let workers: Vec<_> = handles
            .iter()
            .enumerate()
            .map(|(i, quota)| {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let cost = [2, 3, 5][i % 3];
                    let mut total = 0;
                    for _ in 0..128 {
                        match quota.try_consume(key(), count(cost)) {
                            Ok(Decision::Allowed) => total += cost,
                            Ok(Decision::Limited { .. }) | Err(Error::Busy) => {
                            }
                            result => panic!("unexpected outcome: {result:?}"),
                        }
                    }
                    total
                })
            })
            .collect();
        workers.into_iter().map(|w| w.join().unwrap()).sum::<u64>()
    });
    assert!(charged <= 30);
    for _ in charged + 1..31 {
        assert_eq!(
            handles[0].try_consume(key(), count(1)).unwrap(),
            Decision::Allowed
        );
    }
    assert!(matches!(
        handles[0].try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
}

#[test]
fn admission_and_replacement_allocate_nothing_and_hot_hits_ignore_structure_lock()
 {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let quota = fixture.open(Arc::clone(&clock), 2, 1, 0);
    let (results, allocations) = crate::test_allocator::measured(|| {
        [
            quota.try_consume(key(), count(1)),
            quota.try_consume(key(), count(1)),
            quota.try_consume(key(), count(1)),
            quota.try_consume(other(), count(1)),
        ]
    });
    assert_eq!(allocations, 0);
    assert_eq!(results[0].as_ref().unwrap(), &Decision::Allowed);
    assert!(matches!(results[2], Ok(Decision::Limited { .. })));
    assert!(matches!(results[3], Err(Error::CapacityFull)));
    let mut work = budget();
    let hold = quota.inner.lock_shard(0, &mut work).unwrap();
    assert!(matches!(
        quota.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
    drop(hold);
    clock.0.store(200, Ordering::Relaxed);
    let (result, allocations) = crate::test_allocator::measured(|| {
        quota.try_consume(other(), count(1))
    });
    assert_eq!(result.unwrap(), Decision::Allowed);
    assert_eq!(allocations, 0);
}

struct RunningChild {
    process: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
}

impl RunningChild {
    fn kill(&mut self) {
        self.process.kill().unwrap();
        // Child::wait closes its own stdin before reaping. Keep the control
        // pipe outside Child until death is confirmed, not merely requested.
        assert_eq!(self.process.wait().unwrap().signal(), Some(libc::SIGKILL));
    }

    fn resume(
        &mut self,
        command: &[u8],
    ) {
        self.input.as_mut().unwrap().write_all(command).unwrap();
    }
}

impl Drop for RunningChild {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn child(
    fixture: &Fixture,
    mode: &str,
) -> RunningChild {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "shm::tests::child_process", "--nocapture"])
        .env("ORBIT_QUOTA_TEST_FLEET", fixture.0.fleet.as_str())
        .env("ORBIT_QUOTA_TEST_MODE", mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut process = command.spawn().unwrap();
    let input = process.stdin.take();
    let output = BufReader::new(process.stdout.take().unwrap());
    let mut child = RunningChild { process, input, output };
    let expected = match fault_for_mode(mode) {
        Some(point) if mode != "recovery" => format!("{point:?}"),
        _ => "Armed".to_owned(),
    };
    wait_checkpoint(&mut child, &expected);
    child
}

fn wait_checkpoint(
    child: &mut RunningChild,
    expected: &str,
) {
    loop {
        let mut line = String::new();
        assert_ne!(
            child.output.read_line(&mut line).unwrap(),
            0,
            "child exited before checkpoint"
        );
        if let Some((_, point)) = line.split_once("QUOTA_CHECKPOINT:") {
            assert_eq!(point.trim(), expected, "wrong crash boundary");
            break;
        }
    }
}

fn fault_for_mode(mode: &str) -> Option<Fault> {
    let mode = mode.strip_prefix("guard-").unwrap_or(mode);
    Some(match mode {
        "race" => return None,
        "init" => Fault::Initializing,
        "ready" => Fault::BeforeReady,
        "pin" => Fault::Pinned,
        "frozen" => Fault::Frozen,
        "key" => Fault::KeyCopy,
        "metadata" => Fault::Metadata,
        "inactive" => Fault::BeforePublish,
        "publish" => Fault::Published,
        "charge" => Fault::TatCommitted,
        "recovery" => Fault::Recovering,
        _ => panic!("unknown child checkpoint"),
    })
}

#[test]
fn child_process() {
    let Ok(fleet) = std::env::var("ORBIT_QUOTA_TEST_FLEET") else {
        return;
    };
    let mode = std::env::var("ORBIT_QUOTA_TEST_MODE").unwrap();
    #[cfg(target_os = "freebsd")]
    if mode == "native" {
        let resource = ShmResource::new(FleetName::new(fleet), 0);
        let quota = ShmQuota::open(
            resource,
            geometry(1),
            Policy::new(count(1), Duration::MAX, count(2)).unwrap(),
            attempts(64),
            options(2, 2),
        )
        .unwrap();
        assert_eq!(
            quota.try_consume(key(), count(1)).unwrap(),
            Decision::Allowed
        );
        assert!(matches!(
            quota.try_consume(key(), count(1)).unwrap(),
            Decision::Limited { .. }
        ));
        println!("QUOTA_DOMAIN:{}", domain_hex());
        return;
    }
    let point = fault_for_mode(&mode);
    FAULT.with(|selected| selected.set(point));
    let clock = Arc::new(TestClock::default());
    let retention = if mode.starts_with("guard-") {
        Retention::Guard
    } else {
        Retention::Strict
    };
    let checkpoint = mode.strip_prefix("guard-").unwrap_or(&mode);
    if matches!(
        checkpoint,
        "frozen" | "key" | "metadata" | "inactive" | "publish" | "recovery"
    ) {
        clock.0.store(
            if retention == Retention::Guard { 1 } else { 200 },
            Ordering::Relaxed,
        );
    }
    let resource = ShmResource::new(FleetName::new(fleet), 0);
    let quota = ShmQuota::with_retention(
        resource,
        geometry(1),
        (policy(2), retention),
        attempts(64),
        options(3, 3),
        Box::new(clock),
        [7; 32],
    )
    .unwrap();
    match checkpoint {
        "pin" => {
            let _pin =
                quota.inner.pin(0, None, &mut budget()).unwrap().unwrap();
        }
        "charge" => {
            quota.try_consume(key(), count(1)).unwrap();
        }
        "frozen" | "key" | "metadata" | "inactive" | "publish" => {
            quota.try_consume(other(), count(1)).unwrap();
        }
        "init" | "ready" => {}
        "recovery" => {
            armed_checkpoint("recover\n"); // Before a different owner dies.
            quota.try_consume(other(), count(1)).unwrap();
        }
        "race" => {
            armed_checkpoint("start\n");
            let mut allowed = 0;
            for _ in 0..64 {
                match quota.try_consume(key(), count(1)) {
                    Ok(Decision::Allowed) => allowed += 1,
                    Ok(Decision::Limited { .. }) | Err(Error::Busy) => {}
                    result => panic!("unexpected outcome: {result:?}"),
                }
            }
            println!("QUOTA_RESULT:{allowed}");
        }
        _ => panic!("unknown child checkpoint"),
    }
}

pub(super) fn crash_checkpoint(point: Fault) -> ! {
    println!("QUOTA_CHECKPOINT:{point:?}");
    std::io::stdout().flush().unwrap();
    // A crash boundary has no resume command. EOF, input or an IO failure
    // terminates without unwinding guards or publishing any further state.
    let _ = std::io::stdin().read_exact(&mut [0]);
    std::process::exit(86);
}

fn armed_checkpoint(command: &str) {
    println!("QUOTA_CHECKPOINT:Armed");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    assert_ne!(std::io::stdin().read_line(&mut line).unwrap(), 0);
    assert_eq!(line, command);
}

#[test]
fn crash_control_pipe_cannot_resume_publication() {
    for send_input in [false, true] {
        let fixture = Fixture::new();
        let clock = Arc::new(TestClock::default());
        let quota = fixture.open(Arc::clone(&clock), 2, 3, 3);
        quota.try_consume(key(), count(1)).unwrap();
        clock.0.store(200, Ordering::Relaxed);
        let mut peer = child(&fixture, "inactive");
        if send_input {
            peer.resume(b"unexpected\n");
        } else {
            drop(peer.input.take());
        }
        assert_eq!(peer.process.wait().unwrap().code(), Some(86));
        assert_eq!(
            quota.inner.slot(0).control.load(Ordering::SeqCst),
            1 | FROZEN
        );
        assert_eq!(
            quota.inner.slot(0).banks[1].tat.load(Ordering::Acquire),
            100
        );
        assert_eq!(
            quota.try_consume(key(), count(2)).unwrap(),
            Decision::Allowed
        );
    }
}

#[test]
fn sigkill_reclaims_dead_pin_only_after_owner_hold_and_recovery_budget() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let quota = fixture.open(Arc::clone(&clock), 2, 3, 3);
    let no_recovery = fixture.open(Arc::clone(&clock), 2, 3, 0);
    quota.try_consume(key(), count(2)).unwrap();
    clock.0.store(200, Ordering::Relaxed);
    let mut peer = child(&fixture, "pin");
    assert!(matches!(quota.try_consume(other(), count(1)), Err(Error::Busy)));
    peer.kill();
    assert!(matches!(
        no_recovery.try_consume(other(), count(1)),
        Err(Error::Busy)
    ));
    assert_ne!(quota.inner.counter(2, 0).load(Ordering::SeqCst), 0);
    assert_eq!(
        quota.try_consume(other(), count(1)).unwrap(),
        Decision::Allowed
    );
    assert_eq!(quota.inner.counter(2, 0).load(Ordering::SeqCst), 0);
}

#[test]
fn sigkill_before_publication_preserves_active_bank_and_after_publication_keeps_charge()
 {
    for mode in ["frozen", "key", "metadata", "inactive", "publish"] {
        let fixture = Fixture::new();
        let clock = Arc::new(TestClock::default());
        let quota = fixture.open(Arc::clone(&clock), 2, 3, 3);
        quota.try_consume(key(), count(1)).unwrap();
        clock.0.store(200, Ordering::Relaxed);
        let mut peer = child(&fixture, mode);
        if mode != "publish" {
            assert!(matches!(
                quota.try_consume(key(), count(1)),
                Err(Error::Busy)
            ));
        }
        peer.kill();
        if mode != "publish" {
            // Recovery preserves the exact old committed charge, not just a
            // freshly refilled replacement bucket with equivalent allowance.
            assert_eq!(
                quota.inner.slot(0).banks[1].tat.load(Ordering::Acquire),
                100
            );
            assert_eq!(
                quota.try_consume(key(), count(2)).unwrap(),
                Decision::Allowed
            );
            assert!(matches!(
                quota.try_consume(key(), count(1)).unwrap(),
                Decision::Limited { .. }
            ));
        } else {
            assert_eq!(
                quota.try_consume(other(), count(1)).unwrap(),
                Decision::Allowed
            );
            assert!(matches!(
                quota.try_consume(other(), count(1)).unwrap(),
                Decision::Limited { .. }
            ));
            assert!(matches!(
                quota.try_consume(key(), count(1)),
                Err(Error::CapacityFull)
            ));
        }
    }
}

#[test]
fn sigkill_during_initialization_never_exposes_partial_table() {
    for mode in ["init", "ready"] {
        let fixture = Fixture::new();
        let mut peer = child(&fixture, mode);
        peer.kill();
        let quota = fixture.open(Arc::new(TestClock::default()), 2, 3, 3);
        assert_eq!(
            quota.try_consume(key(), count(2)).unwrap(),
            Decision::Allowed
        );
    }
}

#[test]
fn sigkill_after_tat_cas_keeps_committed_charge() {
    let fixture = Fixture::new();
    let quota = fixture.open(Arc::new(TestClock::default()), 2, 3, 3);
    quota.try_consume(key(), count(1)).unwrap();
    let mut peer = child(&fixture, "charge");
    peer.kill();
    assert!(matches!(
        quota.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
}

#[test]
fn independent_processes_share_one_atomic_burst() {
    let fixture = Fixture::new();
    let quota = fixture.open(Arc::new(TestClock::default()), 2, 3, 3);
    quota.try_consume(key(), count(1)).unwrap();
    let mut left = child(&fixture, "race");
    let mut right = child(&fixture, "race");
    for peer in [&mut left, &mut right] {
        peer.resume(b"start\n");
    }
    let mut total = 1;
    for peer in [&mut left, &mut right] {
        loop {
            let mut line = String::new();
            assert_ne!(peer.output.read_line(&mut line).unwrap(), 0);
            if let Some((_, result)) = line.split_once("QUOTA_RESULT:") {
                total += result.trim().parse::<u64>().unwrap();
                break;
            }
        }
        assert!(peer.process.wait().unwrap().success());
    }
    assert!(total <= 2);
    if total == 1 {
        assert_eq!(
            quota.try_consume(key(), count(1)).unwrap(),
            Decision::Allowed
        );
    }
    assert!(matches!(
        quota.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
}

#[test]
fn a_reclaimer_can_die_without_erasing_debt_or_leaving_a_stolen_hold() {
    reclaimer_crash(false);
}

#[test]
fn closed_crash_control_pipe_cannot_resume_recovery() {
    reclaimer_crash(true);
}

fn reclaimer_crash(close_control: bool) {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let quota = fixture.open(Arc::clone(&clock), 2, 3, 3);
    quota.try_consume(key(), count(2)).unwrap();
    clock.0.store(200, Ordering::Relaxed);
    let mut reclaimer = child(&fixture, "recovery"); // Holds owner row 1.
    let mut reader = child(&fixture, "pin"); // Pins from owner row 2.
    reader.kill();
    reclaimer.resume(b"recover\n");
    wait_checkpoint(&mut reclaimer, "Recovering"); // Row 2 held, not cleared.
    assert_eq!(quota.inner.slot(0).banks[1].tat.load(Ordering::Acquire), 200);
    assert_ne!(quota.inner.counter(2, 0).load(Ordering::SeqCst), 0);
    if close_control {
        drop(reclaimer.input.take());
        assert_eq!(reclaimer.process.wait().unwrap().code(), Some(86));
    } else {
        reclaimer.kill();
    }
    assert_eq!(
        quota.try_consume(other(), count(1)).unwrap(),
        Decision::Allowed
    );
    assert_eq!(quota.inner.counter(2, 0).load(Ordering::SeqCst), 0);
    assert_eq!(quota.inner.slot(0).banks[0].tat.load(Ordering::Acquire), 300);
}

#[test]
fn layout_rejects_overflow_before_opening_any_resource() {
    let shape = Geometry::new(
        NonZeroUsize::new(usize::MAX).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(32).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    )
    .unwrap();
    assert!(matches!(Layout::new(shape, 2), Err(Error::InvalidGeometry)));
}

#[test]
fn owner_generation_exhaustion_never_wraps() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let quota = fixture.open(Arc::clone(&clock), 1, 1, 0);
    quota.inner.owner(0).generation.store(u64::MAX, Ordering::Release);
    assert!(matches!(
        quota.try_consume(key(), count(1)),
        Err(Error::CorruptState)
    ));
    drop(quota);
    assert!(matches!(
        ShmQuota::with_clock(
            fixture.0,
            geometry(1),
            policy(1),
            attempts(64),
            options(1, 0),
            Box::new(clock),
            [7; 32]
        ),
        Err(Error::CapacityFull)
    ));
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
#[test]
fn public_open_binds_the_native_clock_domain_and_preserves_debt() {
    let fixture = Fixture::new();
    let limit = Policy::new(count(1), Duration::MAX, count(2)).unwrap();
    let first = ShmQuota::open(
        fixture.0,
        geometry(1),
        limit,
        attempts(64),
        options(2, 2),
    )
    .unwrap();
    let second = ShmQuota::open(
        fixture.0,
        geometry(1),
        limit,
        attempts(64),
        options(2, 0),
    )
    .unwrap();
    assert_eq!(first.try_consume(key(), count(2)).unwrap(), Decision::Allowed);
    assert!(matches!(
        second.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
}

#[cfg(target_os = "freebsd")]
fn domain_hex() -> String {
    domain::clock_domain()
        .unwrap()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_processes_share_native_clock_domain_and_quota_debt() {
    let fixture = Fixture::new();
    let limit = Policy::new(count(1), Duration::MAX, count(2)).unwrap();
    let quota = ShmQuota::open(
        fixture.0,
        geometry(1),
        limit,
        attempts(64),
        options(2, 2),
    )
    .unwrap();
    assert_eq!(quota.try_consume(key(), count(1)).unwrap(), Decision::Allowed);
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "shm::tests::child_process", "--nocapture"])
        .env("ORBIT_QUOTA_TEST_FLEET", fixture.0.fleet.as_str())
        .env("ORBIT_QUOTA_TEST_MODE", "native")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected = format!("QUOTA_DOMAIN:{}", domain_hex());
    assert!(String::from_utf8_lossy(&output.stdout).contains(&expected));
    assert!(matches!(
        quota.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
}
