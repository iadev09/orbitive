//! The first working loop, in two real processes: the owner registers a
//! test resource, the peer acquires capacity through the pool, brings the
//! lease to the owner over an `orbit-stream` stream with its input, the
//! owner validates the lease, accepts, runs the "work" (uppercasing), writes
//! the output back and only then completes the lease. A peer that gives up
//! mid-way frees nothing; the owner's completion does.
//!
//! No transport lives in the pool: the stream is the existing crate, and
//! the lease crosses it as text in the first line.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use orbit_core::{Fleet, NodeId};
use orbit_pool::{Error, Incarnation, Key, Lease, Limits, LocalFirst, Plan, Pool, ResourceId};
use orbit_stream::{Streams, Ticket};

const KEY: Key = Key::new(0x5EED);
const OWNER_INCARNATION: u64 = 10;
const PEER_INCARNATION: u64 = 11;

fn report(line: &str) {
    println!("ORBIT:{line}");
    std::io::stdout().flush().unwrap();
}

/// The lease as it crosses the stream: one line, plain numbers.
fn encode_lease(lease: &Lease) -> String {
    format!(
        "{} {} {} {}\n",
        lease.id,
        lease.fence,
        lease.holder.get(),
        lease.holder_incarnation.get()
    )
}

fn decode_lease(line: &str) -> Lease {
    let mut parts = line.split_whitespace();
    Lease {
        id: parts.next().unwrap().parse::<ResourceId>().unwrap(),
        fence: parts.next().unwrap().parse().unwrap(),
        holder: NodeId::new(parts.next().unwrap().parse().unwrap()),
        holder_incarnation: Incarnation::new(parts.next().unwrap().parse().unwrap()),
    }
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
    for line in std::io::stdin().lock().lines() {
        match line.unwrap().as_str() {
            // Serve one request end to end.
            "serve" => {
                let ticket = streams.blocking_take_offer().unwrap();
                let mut endpoint = streams.open(ticket).unwrap();
                let mut header = Vec::new();
                loop {
                    let mut byte = [0_u8; 1];
                    assert_eq!(endpoint.blocking_read(&mut byte).unwrap(), 1);
                    if byte[0] == b'\n' {
                        break;
                    }
                    header.push(byte[0]);
                }
                let lease = decode_lease(std::str::from_utf8(&header).unwrap());
                let execution = pool.accept(lease).unwrap();
                report(&format!("accepted {}", lease.fence));
                let mut input = Vec::new();
                loop {
                    let chunk = endpoint.blocking_read_chunk(4_096).unwrap();
                    if chunk.is_empty() {
                        break;
                    }
                    input.extend_from_slice(&chunk);
                }
                endpoint
                    .blocking_write_all(&input.to_ascii_uppercase())
                    .unwrap();
                endpoint.finish().unwrap();
                execution.complete();
                report(&format!("completed {}", input.len()));
            }
            // Accept, then keep the execution open until told to finish:
            // the peer's fate meanwhile must not free the resource.
            "hold" => {
                let ticket = streams.blocking_take_offer().unwrap();
                let endpoint = streams.open(ticket).unwrap();
                let mut header = Vec::new();
                loop {
                    let mut byte = [0_u8; 1];
                    assert_eq!(endpoint.blocking_read(&mut byte).unwrap(), 1);
                    if byte[0] == b'\n' {
                        break;
                    }
                    header.push(byte[0]);
                }
                let lease = decode_lease(std::str::from_utf8(&header).unwrap());
                let execution = pool.accept(lease).unwrap();
                report(&format!("accepted {}", lease.fence));
                // Drain what the peer sent until it goes away: its input
                // resets. The work is still "running" here until we say so.
                let outcome = loop {
                    match endpoint.blocking_read_chunk(64) {
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
            "drop" => break,
            command => panic!("unknown command {command}"),
        }
    }
    report("dropped");
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

    /// Reserve through the pool, open a stream to the owner, send the
    /// lease and the input; `finish` says whether the input is complete.
    /// Returns the lease and the peer's endpoint.
    fn dispatch(&self, input: &[u8], finish: bool) -> (Lease, orbit_stream::Endpoint) {
        let limits = Limits {
            max_live: 1,
            attempts: 2,
        };
        let Plan::RemoteReuse(lease) = self.pool.acquire(KEY, &limits, &LocalFirst).unwrap() else {
            panic!("the owner's resource is remote to the peer");
        };
        let (mut endpoint, ticket) = self.streams.create().unwrap();
        self.streams.offer(ticket, NodeId::ZERO).unwrap();
        endpoint
            .blocking_write_all(encode_lease(&lease).as_bytes())
            .unwrap();
        endpoint.blocking_write_all(input).unwrap();
        if finish {
            endpoint.finish().unwrap();
        }
        let _ = ticket.to_string().parse::<Ticket>().unwrap();
        (lease, endpoint)
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
    let (lease, endpoint) = peer.dispatch(b"hello over the fleet", true);
    assert_eq!(
        owner.expect("accepted"),
        format!("accepted {}", lease.fence)
    );
    let mut output = Vec::new();
    loop {
        let chunk = endpoint.blocking_read_chunk(64).unwrap();
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
    let (lease, endpoint) = peer.dispatch(b"partial", false);
    owner.expect("accepted");
    // The peer "times out" and drops its end mid-input: the owner sees a
    // reset, the work is still running there, and the resource stays busy.
    drop(endpoint);
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
    let (lease, endpoint) = peer.dispatch(b"partial", false);
    owner.expect("accepted");
    let reader = std::thread::spawn(move || {
        let outcome = endpoint.blocking_read_chunk(16);
        (endpoint, outcome)
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
    let (_endpoint, outcome) = reader.join().unwrap();
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
