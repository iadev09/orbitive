#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::sync::Arc;

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
