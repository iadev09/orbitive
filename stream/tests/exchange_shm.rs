#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use orbit_core::{Fleet, NodeId};
use orbit_stream::exchange::{
    ExchangeSpec, Exchanges, FlowEvent, PayloadArenaSpec,
};
use orbit_stream::{Incarnation, StreamSpec};

fn spec() -> ExchangeSpec {
    ExchangeSpec::new(
        StreamSpec::new(220, 8, 512),
        PayloadArenaSpec::new(221, 8, 256),
        PayloadArenaSpec::new(222, 8, 256),
    )
}

fn report(line: &str) {
    println!("ORBIT:{line}");
    std::io::stdout().flush().expect("flush peer report");
}

fn pattern(len: usize, salt: u8) -> Vec<u8> {
    (0..len).map(|index| (index as u8).wrapping_mul(31).wrapping_add(salt)).collect()
}

fn fill_pattern(bytes: &mut [u8], salt: u8) {
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index as u8).wrapping_mul(31).wrapping_add(salt);
    }
}

#[test]
fn exchange_peer() {
    let Ok(name) = std::env::var("ORBIT_EXCHANGE_TEST_FLEET") else {
        return;
    };
    let name = Box::leak(name.into_boxed_str());
    let exchanges = Exchanges::open(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet")),
        Incarnation::new(11),
        spec(),
    )
    .expect("peer exchanges");
    report("ready");

    let mut held_client = None;
    for line in std::io::stdin().lock().lines() {
        match line.expect("peer command").as_str() {
            "exchange" => {
                let ticket = exchanges.blocking_take_offer().expect("exchange offer");
                let mut client = exchanges.open_client(ticket).expect("client side");
                client.request().wait_readable().expect("request ready");
                let request = match client.request().try_next().expect("request start") {
                    FlowEvent::Start { metadata: Some(metadata) } => metadata,
                    _ => panic!("expected request start metadata"),
                };
                assert_eq!(&*request, pattern(777, 7));
                assert_eq!(request.descriptor().slot_count(), 4);

                let mut pending = client
                    .response()
                    .reserve_start(513)
                    .expect("reserve response start");
                fill_pattern(&mut pending, 19);
                let descriptor = pending.commit().expect("commit response start");
                assert_eq!(descriptor.slot_count(), 3);
                held_client = Some(client);
                report("exchanged");
            }
            "release" => {
                drop(held_client.take());
                report("released");
            }
            "drop" => break,
            command => panic!("unknown command {command}"),
        }
    }
    report("dropped");
}

struct Peer {
    child: Child,
    lines: Receiver<String>,
}

impl Peer {
    fn spawn(name: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", "exchange_peer", "--nocapture"])
            .env("ORBIT_EXCHANGE_TEST_FLEET", name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn exchange peer");
        let stdout = child.stdout.take().expect("peer stdout");
        let (sender, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if sender.send(line.expect("peer output")).is_err() {
                    break;
                }
            }
        });
        let peer = Self { child, lines };
        peer.expect("ready");
        peer
    }

    fn expect(&self, expected: &str) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let line = self
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("peer failed or hung");
            if line.strip_prefix("ORBIT:").is_some_and(|line| line == expected) {
                return;
            }
        }
    }

    fn send(&mut self, command: &str) {
        writeln!(self.child.stdin.as_mut().expect("peer stdin"), "{command}")
            .expect("send peer command");
        self.child.stdin.as_mut().expect("peer stdin").flush().expect("flush peer command");
    }

    fn finish(&mut self) {
        self.send("drop");
        self.expect("dropped");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().expect("peer status") {
                assert!(status.success());
                return;
            }
            assert!(Instant::now() < deadline, "peer did not exit");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn reserved_chunks_cross_processes_without_an_intermediate_payload_buffer() {
    let name: &'static str = Box::leak(format!("xp{:x}", std::process::id()).into_boxed_str());
    let owner = Exchanges::open(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet")),
        Incarnation::new(10),
        spec(),
    )
    .expect("owner exchanges");
    owner.reset_all();
    let mut peer = Peer::spawn(name);

    let (mut server, ticket) = owner.create().expect("server side");
    let mut pending = server.request().reserve_start(777).expect("reserve request start");
    fill_pattern(&mut pending, 7);
    let descriptor = pending.commit().expect("commit request start");
    assert_eq!(descriptor.slot_count(), 4);
    owner.offer(ticket, NodeId::new(1)).expect("offer exchange");
    peer.send("exchange");
    peer.expect("exchanged");

    server.response().wait_readable().expect("response ready");
    let response = match server.response().try_next().expect("response start") {
        FlowEvent::Start { metadata: Some(metadata) } => metadata,
        _ => panic!("expected response start metadata"),
    };
    assert_eq!(&*response, pattern(513, 19));
    assert_eq!(response.descriptor().slot_count(), 3);
    assert_ne!(descriptor.arena_kind(), response.descriptor().arena_kind());

    drop(response);
    peer.send("release");
    peer.expect("released");
    peer.finish();
    owner.unlink().expect("unlink exchange resources");
}

#[test]
fn paired_flows_cross_separate_shm_mappings() {
    let name: &'static str = Box::leak(format!("ex{:x}", std::process::id()).into_boxed_str());
    let owner = Exchanges::open(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet")),
        Incarnation::new(10),
        spec(),
    )
    .expect("owner exchanges");
    owner.reset_all();
    let peer = Exchanges::open(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet")),
        Incarnation::new(11),
        spec(),
    )
    .expect("peer exchanges");

    let (server, ticket) = owner.create().expect("server side");
    let client = peer.open_client(ticket).expect("client side");
    let (mut request_out, mut response_in) = server.split();
    let (mut request_in, mut response_out) = client.split();

    request_out.start(Some(b"request metadata")).expect("request start");
    request_out.data(&vec![5; 513]).expect("request data");
    let request_metadata = match request_in.try_next().expect("request start event") {
        FlowEvent::Start { metadata: Some(metadata) } => metadata,
        _ => panic!("expected request start"),
    };
    assert_eq!(&*request_metadata, b"request metadata");
    let request_data = match request_in.try_next().expect("request data event") {
        FlowEvent::Data(chunk) => chunk,
        _ => panic!("expected request data"),
    };
    assert_eq!(request_data.len(), 513);
    assert_eq!(request_data.descriptor().slot_count(), 3);

    response_out.start(Some(b"response metadata")).expect("response start");
    response_out.data(b"response data").expect("response data");
    let response_metadata = match response_in.try_next().expect("response start event") {
        FlowEvent::Start { metadata: Some(metadata) } => metadata,
        _ => panic!("expected response start"),
    };
    assert_eq!(&*response_metadata, b"response metadata");
    let response_data = match response_in.try_next().expect("response data event") {
        FlowEvent::Data(chunk) => chunk,
        _ => panic!("expected response data"),
    };
    assert_eq!(&*response_data, b"response data");
    assert_ne!(request_data.descriptor().arena_kind(), response_data.descriptor().arena_kind());

    drop((request_metadata, request_data, response_metadata, response_data));
    request_out.finish().expect("request fin");
    response_out.finish().expect("response fin");
    assert!(matches!(request_in.try_next(), Ok(FlowEvent::Fin)));
    assert!(matches!(response_in.try_next(), Ok(FlowEvent::Fin)));

    owner.unlink().expect("unlink exchange resources");
}
