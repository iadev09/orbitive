//! The first working loop, in two real processes: the owner registers a
//! test resource, the peer acquires capacity through the pool, brings the
//! lease to the owner over an `orbit-stream` stream with its input, the
//! owner validates the lease, accepts, runs the "work" (uppercasing), writes
//! the output back and only then completes the lease. A peer that gives up
//! mid-way frees nothing; the owner's completion does.
//!
//! The rendezvous itself is the pool's, through `open_session` and
//! `accept_session`: the lease crosses the stream in one frame and is
//! accepted exactly once before a byte of the payload is trusted. What
//! crosses afterwards — here, text to uppercase — is this test's.

#![cfg(all(
    feature = "pool-stream",
    any(target_os = "linux", target_os = "freebsd", target_os = "macos")
))]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use orbit_core::{Fleet, NodeId};
use orbit_pool::{Error, Incarnation, Key, Lease, Limits, LocalFirst, Plan, Pool, ResourceId};
use orbit_stream::{ReadHalf, Streams, WriteHalf};

const KEY: Key = Key::new(0x5EED);
const OWNER_INCARNATION: u64 = 10;
const PEER_INCARNATION: u64 = 11;

fn report(line: &str) {
    println!("ORBIT:{line}");
    std::io::stdout().flush().unwrap();
}

/// The owner process (node 0). Runs only when exec'd by a test below.
#[test]
fn pool_owner() {
    let Ok(name) = std::env::var("ORBIT_POOL_TEST_FLEET") else {
        return;
    };
    let name = Box::leak(name.into_boxed_str());
    let fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).unwrap());
    let pool = Pool::new(Arc::clone(&fleet), Incarnation::new(OWNER_INCARNATION)).unwrap();
    let streams = Streams::new(fleet, orbit_stream::Incarnation::new(OWNER_INCARNATION)).unwrap();
    // The test resource: capacity one, lives only here.
    let id = pool.register(KEY, 1).unwrap();
    report(&format!("registered {id}"));

    let mut executions = Vec::new();
    let mut permits = Vec::new();
    for line in std::io::stdin().lock().lines() {
        match line.unwrap().as_str() {
            // Take creation claims and hold them. The budget is the
            // fleet's, so what matters is what the other process sees.
            command if command.starts_with("claim ") => {
                let mut parts = command.split_whitespace().skip(1);
                let max_live: u32 = parts.next().unwrap().parse().unwrap();
                let wanted: usize = parts.next().unwrap().parse().unwrap();
                let mut taken = 0;
                for _ in 0..wanted {
                    match pool.claim_create(KEY, max_live) {
                        Ok(permit) => {
                            permits.push(permit);
                            taken += 1;
                        }
                        Err(Error::CreationBudget { .. }) => break,
                        Err(error) => panic!("{error}"),
                    }
                }
                report(&format!("claimed {taken}"));
            }
            "release" => {
                if let Some(permit) = permits.pop() {
                    permit.finish();
                }
                report("released");
            }
            // Serve one request end to end.
            "serve" => {
                let ticket = streams.blocking_take_offer().unwrap();
                let (execution, read, mut write) = pool.accept_session(&streams, ticket).unwrap();
                report(&format!("accepted {}", execution.lease().fence));
                let mut input = Vec::new();
                loop {
                    let chunk = read.blocking_read_chunk(4_096).unwrap();
                    if chunk.is_empty() {
                        break;
                    }
                    input.extend_from_slice(&chunk);
                }
                write
                    .blocking_write_all(&input.to_ascii_uppercase())
                    .unwrap();
                write.finish().unwrap();
                execution.complete();
                report(&format!("completed {}", input.len()));
            }
            // Accept, then keep the execution open until told to finish:
            // the peer's fate meanwhile must not free the resource.
            "hold" => {
                let ticket = streams.blocking_take_offer().unwrap();
                let (execution, read, _write) = pool.accept_session(&streams, ticket).unwrap();
                report(&format!("accepted {}", execution.lease().fence));
                // Drain what the peer sent until it goes away: its input
                // resets. The work is still "running" here until we say so.
                let outcome = loop {
                    match read.blocking_read_chunk(64) {
                        Ok(chunk) if chunk.is_empty() => break "eof",
                        Ok(_) => continue,
                        Err(orbit_stream::Error::Reset) => break "reset",
                        Err(_) => break "error",
                    }
                };
                report(&format!("peer {outcome}"));
                executions.push(execution);
            }
            "finish" => {
                if let Some(execution) = executions.pop() {
                    execution.complete();
                }
                report("finished");
            }
            "drop" => {
                report("dropped");
                return;
            }
            command => panic!("unknown command {command}"),
        }
    }
    // Stdin is not how this process ends. Under a parallel test binary the
    // pipe has been seen to close on its own, and a process that exits
    // there runs its destructors: a test that means to kill an owner
    // holding leases or creation claims would then be killing one that had
    // already given them back. It ends by command, or by the signal.
    report("stdin closed");
    loop {
        std::thread::sleep(Duration::from_secs(3_600));
    }
}

struct Owner {
    child: Child,
    lines: Receiver<String>,
    resource: ResourceId,
}

impl Owner {
    fn spawn(name: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "pool_owner", "--nocapture"])
            .env("ORBIT_POOL_TEST_FLEET", name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if tx.send(line.unwrap()).is_err() {
                    break;
                }
            }
        });
        let mut owner = Self {
            child,
            lines,
            resource: ResourceId::from_net_id(orbit_core::NetId64::make(0, 0, 0)),
        };
        let registered = owner.expect("registered");
        owner.resource = registered
            .strip_prefix("registered ")
            .unwrap()
            .parse()
            .unwrap();
        owner
    }

    fn expect(&self, message: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let line = self
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("owner failed or hung");
            if let Some(rest) = line.strip_prefix("ORBIT:")
                && rest.starts_with(message)
            {
                return rest.to_owned();
            }
        }
    }

    fn send(&mut self, command: &str) {
        writeln!(self.child.stdin.as_mut().unwrap(), "{command}").unwrap();
        self.child.stdin.as_mut().unwrap().flush().unwrap();
    }

    fn finish(&mut self) {
        self.send("drop");
        self.expect("dropped");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success());
                return;
            }
            assert!(Instant::now() < deadline, "owner did not exit");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Peer {
    pool: Pool,
    streams: Streams,
    name: &'static str,
}

impl Peer {
    /// The peer creates the segment so the owner process can join it.
    fn new(tag: &str) -> Self {
        let name: &'static str =
            Box::leak(format!("pp{tag}{:x}", std::process::id()).into_boxed_str());
        let fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).unwrap());
        let pool = Pool::new(Arc::clone(&fleet), Incarnation::new(PEER_INCARNATION)).unwrap();
        pool.reset_all();
        let streams =
            Streams::new(fleet, orbit_stream::Incarnation::new(PEER_INCARNATION)).unwrap();
        streams.reset_all();
        Self {
            pool,
            streams,
            name,
        }
    }

    /// Reserve through the pool, reach the owner with the lease, send the
    /// input; `finish` says whether the input is complete. Returns the
    /// lease and the peer's halves.
    fn dispatch(&self, input: &[u8], finish: bool) -> (Lease, ReadHalf, WriteHalf) {
        let limits = Limits {
            max_live: 1,
            attempts: 2,
        };
        let Plan::RemoteReuse(lease) = self.pool.acquire(KEY, &limits, &LocalFirst).unwrap() else {
            panic!("the owner's resource is remote to the peer");
        };
        let (read, mut write) = self.pool.open_session(lease, &self.streams).unwrap();
        write.blocking_write_all(input).unwrap();
        if finish {
            write.finish().unwrap();
        }
        (lease, read, write)
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.pool.unlink();
        let _ = self.streams.unlink();
    }
}

#[test]
fn a_remote_lease_is_executed_by_the_owner_over_a_stream() {
    let peer = Peer::new("s");
    let mut owner = Owner::spawn(peer.name);
    assert_eq!(peer.candidates(KEY).len(), 1);
    assert!(!peer.candidates(KEY)[0].local);

    owner.send("serve");
    let (lease, read, _write) = peer.dispatch(b"hello over the fleet", true);
    assert_eq!(
        owner.expect("accepted"),
        format!("accepted {}", lease.fence)
    );
    let mut output = Vec::new();
    loop {
        let chunk = read.blocking_read_chunk(64).unwrap();
        if chunk.is_empty() {
            break;
        }
        output.extend_from_slice(&chunk);
    }
    assert_eq!(output, b"HELLO OVER THE FLEET");
    owner.expect("completed");
    // Capacity is back only now, after the owner completed.
    assert!(peer.pool.reserve(owner.resource).is_ok());
    owner.finish();
}

#[test]
fn a_peer_that_gives_up_does_not_free_a_running_resource() {
    let peer = Peer::new("g");
    let mut owner = Owner::spawn(peer.name);

    owner.send("hold");
    let (lease, read, write) = peer.dispatch(b"partial", false);
    owner.expect("accepted");
    // The peer "times out" and drops its end mid-input: the owner sees a
    // reset, the work is still running there, and the resource stays busy.
    drop((read, write));
    owner.expect("peer reset");
    assert!(matches!(
        peer.pool.reserve(owner.resource),
        Err(Error::Busy(_))
    ));
    assert!(!peer.candidates(KEY).is_empty());
    assert_eq!(peer.candidates(KEY)[0].active, 1);
    let _ = lease;

    // Only the owner's completion gives the unit back.
    let since = peer.pool.version(KEY).unwrap();
    owner.send("finish");
    owner.expect("finished");
    assert!(peer.pool.wait_capacity(KEY, since).unwrap() > since);
    assert!(peer.pool.reserve(owner.resource).is_ok());
    owner.finish();
}

impl Peer {
    fn candidates(&self, key: Key) -> Vec<orbit_pool::Candidate> {
        self.pool.candidates(key)
    }
}

impl Owner {
    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

/// The owner dies while executing. Nothing in the segment changes by
/// itself; the supervisor's death reports end the owner's side of the
/// stream and close its resources, and the peer, parked on the stream,
/// wakes with a reset and finds nothing left to reuse.
#[test]
fn a_dead_owner_ends_the_stream_and_takes_its_resource_with_it() {
    let peer = Peer::new("k");
    let mut owner = Owner::spawn(peer.name);

    owner.send("hold");
    let (lease, read, write) = peer.dispatch(b"partial", false);
    owner.expect("accepted");
    let reader = std::thread::spawn(move || {
        let outcome = read.blocking_read_chunk(16);
        (read, write, outcome)
    });
    std::thread::sleep(Duration::from_millis(50));
    owner.kill();

    // Death alone: the resource still looks busy and the reader is parked.
    assert!(peer.candidates(KEY).len() == 1);
    assert!(matches!(
        peer.pool.reserve(owner.resource),
        Err(Error::Busy(_))
    ));

    peer.streams.node_dead(
        NodeId::ZERO,
        orbit_stream::Incarnation::new(OWNER_INCARNATION),
    );
    peer.pool
        .node_dead(NodeId::ZERO, Incarnation::new(OWNER_INCARNATION));
    let (_read, _write, outcome) = reader.join().unwrap();
    assert!(
        matches!(outcome, Err(orbit_stream::Error::Reset)),
        "{outcome:?}"
    );
    assert!(!peer.pool.is_current(lease));
    assert!(peer.candidates(KEY).is_empty());
    // The budget is free again: the next acquire would create.
    let limits = Limits {
        max_live: 1,
        attempts: 1,
    };
    assert!(matches!(
        peer.pool.acquire(KEY, &limits, &LocalFirst).unwrap(),
        Plan::Create(_)
    ));
}

/// The creation budget is the fleet's, not each process's: the ceiling
/// counts what the other process is already making. Until now this was
/// only ever exercised with both nodes in one process, where the counter
/// is the same memory reached the same way; here two processes race for
/// the last unit of it.
#[test]
fn a_creation_budget_is_fleet_wide_across_processes() {
    const MAX_LIVE: u32 = 4;
    let peer = Peer::new("b");
    let mut owner = Owner::spawn(peer.name);
    // The owner's registered resource is the first live unit of the four.
    assert_eq!(peer.pool.budget(KEY), (1, 0));

    owner.send(&format!("claim {MAX_LIVE} 2"));
    assert_eq!(owner.expect("claimed"), "claimed 2");
    assert_eq!(peer.pool.budget(KEY), (1, 2));

    // One unit is left for the whole fleet, and this process takes it.
    let permit = peer.pool.claim_create(KEY, MAX_LIVE).expect("the last unit");
    assert_eq!(peer.pool.budget(KEY), (1, 3));
    assert!(matches!(
        peer.pool.claim_create(KEY, MAX_LIVE),
        Err(Error::CreationBudget { .. })
    ));
    // The owner is at the same ceiling, from its own side of the segment.
    owner.send(&format!("claim {MAX_LIVE} 1"));
    assert_eq!(owner.expect("claimed"), "claimed 0");

    // Giving it back opens it for the other process, not for this one.
    permit.finish();
    assert_eq!(peer.pool.budget(KEY), (1, 2));
    owner.send(&format!("claim {MAX_LIVE} 1"));
    assert_eq!(owner.expect("claimed"), "claimed 1");
    assert_eq!(peer.pool.budget(KEY), (1, 3));
    owner.finish();
}

/// A process that dies holding creation claims does not take the fleet's
/// budget with it. Death alone changes nothing — the claims are still
/// counted — and the supervisor's report is what gives them back, along
/// with the resources that incarnation owned.
#[test]
fn a_dead_holders_creation_claims_come_back() {
    const MAX_LIVE: u32 = 3;
    let peer = Peer::new("c");
    let mut owner = Owner::spawn(peer.name);

    owner.send(&format!("claim {MAX_LIVE} 2"));
    assert_eq!(owner.expect("claimed"), "claimed 2");
    assert_eq!(peer.pool.budget(KEY), (1, 2));
    assert!(matches!(
        peer.pool.claim_create(KEY, MAX_LIVE),
        Err(Error::CreationBudget { .. })
    ));

    owner.kill();
    // Death alone gives nothing back: the units are still held by a
    // process that no longer exists.
    assert_eq!(peer.pool.budget(KEY), (1, 2));

    peer.pool
        .node_dead(NodeId::ZERO, Incarnation::new(OWNER_INCARNATION));
    assert_eq!(peer.pool.budget(KEY), (0, 0));
    peer.pool
        .claim_create(KEY, MAX_LIVE)
        .expect("the budget is free again")
        .finish();
}

/// The wait path across a process boundary: a caller at the ceiling parks
/// on the key's version, and the release that opens capacity happens in
/// another process. The version is taken before the attempt, so a release
/// between the two is seen by the wait instead of being missed.
#[test]
fn a_waiter_wakes_when_another_process_gives_its_claim_back() {
    const MAX_LIVE: u32 = 2;
    let peer = Peer::new("w");
    let mut owner = Owner::spawn(peer.name);

    owner.send(&format!("claim {MAX_LIVE} 1"));
    assert_eq!(owner.expect("claimed"), "claimed 1");
    let since = peer.pool.version(KEY).unwrap();
    assert!(matches!(
        peer.pool.claim_create(KEY, MAX_LIVE),
        Err(Error::CreationBudget { .. })
    ));

    let (woke, parked) = mpsc::channel();
    let pool = peer.pool.clone();
    std::thread::spawn(move || {
        let _ = woke.send(pool.wait_capacity(KEY, since));
    });
    std::thread::sleep(Duration::from_millis(50));
    owner.send("release");
    owner.expect("released");

    let version = parked
        .recv_timeout(Duration::from_secs(10))
        .expect("the waiter was left parked")
        .expect("wait_capacity");
    assert!(version > since);
    peer.pool
        .claim_create(KEY, MAX_LIVE)
        .expect("capacity came back")
        .finish();
    owner.finish();
}
