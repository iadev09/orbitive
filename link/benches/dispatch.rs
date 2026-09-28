//! Complete named dispatch compared with an already addressed Unix socket
//! pair. Orbit includes target lookup, ticket encoding and parsing, inbox
//! publication, stream opening and a small duplex request/reply. The socket
//! pair needs no routing envelope because the pair itself is its address.
//!
//! Run: `cargo bench -p orbit-link --bench dispatch`

#![cfg(unix)]

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{Criterion, criterion_group, criterion_main};
use orbit_link::{InboxGeometry, LinkBodies, LinkSegment, LinkSpec};
use orbit_stream::{Endpoint, StreamSpec, Ticket};

const SPEC: LinkSpec =
    LinkSpec::new(2, 196, InboxGeometry::new(64, 256), StreamSpec::new(197, 64, 4 * 1024));
const REQUEST: &[u8] = b"small request body";
const RESPONSE: &[u8] = b"small response body";

fn fleet_name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("olb{:x}{:x}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}

struct Fixtures {
    segment: LinkSegment,
    caller: LinkBodies,
    target: LinkBodies
}

impl Fixtures {
    fn new() -> Self {
        let name = fleet_name();
        let segment = LinkSegment::open(&name, SPEC).expect("link inbox");
        segment.inbox().claim_lane(0, "caller", "edge", 1).expect("caller lane");
        segment.inbox().claim_lane(1, "service", "worker", 2).expect("target lane");
        let caller = LinkBodies::for_name(&name, 0, 1, SPEC).expect("caller bodies");
        caller.streams().reset_all();
        let target = LinkBodies::for_name(&name, 1, 2, SPEC).expect("target bodies");
        Self { segment, caller, target }
    }

    fn announce(
        &self,
        ticket: Ticket
    ) -> Ticket {
        let target = self.segment.inbox().targets("service")[0].0;
        self.segment.inbox().write(target, ticket.to_string().as_bytes()).expect("dispatch frame");
        let mut frame = Vec::new();
        assert!(self.segment.inbox().read(target, &mut frame).expect("read frame"));
        String::from_utf8(frame).expect("ticket text").parse().expect("ticket")
    }

    fn exchange(
        &self,
        creator: &Endpoint,
        ticket: Ticket
    ) {
        let peer = self.target.accept(self.announce(ticket)).expect("accept");
        creator.try_write_exact(REQUEST).expect("request write");
        let mut request = [0_u8; REQUEST.len()];
        peer.try_read_exact(&mut request).expect("request read");
        peer.try_write_exact(RESPONSE).expect("response write");
        let mut response = [0_u8; RESPONSE.len()];
        creator.try_read_exact(&mut response).expect("response read");
    }
}

impl Drop for Fixtures {
    fn drop(&mut self) {
        let _ = self.segment.unlink();
        let _ = self.caller.unlink();
    }
}

fn benches(criterion: &mut Criterion) {
    let fixtures = Fixtures::new();
    let mut group = criterion.benchmark_group("named_dispatch_small_duplex");

    group.bench_function("orbit-link-fresh-slot", |bencher| {
        bencher.iter(|| {
            let (creator, ticket) = fixtures.caller.create().expect("create");
            fixtures.exchange(&creator, ticket);
        });
    });

    let (mut retained, ticket) = fixtures.caller.create().expect("retained creator");
    let mut first = true;
    group.bench_function("orbit-link-rearmed-slot", |bencher| {
        bencher.iter(|| {
            if first {
                first = false;
            } else {
                fixtures.caller.rearm_endpoint(&mut retained).expect("rearm");
            }
            fixtures.exchange(&retained, ticket);
        });
    });

    group.bench_function("unix-stream-pair", |bencher| {
        bencher.iter(|| {
            let (mut caller, mut target) = UnixStream::pair().expect("socket pair");
            caller.write_all(REQUEST).expect("request write");
            let mut request = [0_u8; REQUEST.len()];
            target.read_exact(&mut request).expect("request read");
            target.write_all(RESPONSE).expect("response write");
            let mut response = [0_u8; RESPONSE.len()];
            caller.read_exact(&mut response).expect("response read");
        });
    });

    group.finish();
}

criterion_group!(dispatch, benches);
criterion_main!(dispatch);
