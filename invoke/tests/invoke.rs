use std::sync::Arc;

use orbit_core::{Fleet, OrbitEpoch};
use orbit_invoke::*;

#[derive(Debug, Eq, PartialEq)]
struct Ping(Vec<u8>);

impl InvocationCodec for Ping {
    const OPERATION: &'static str = "test.ping.v1";

    fn encode_invocation(&self) -> std::result::Result<Vec<u8>, String> {
        Ok(self.0.clone())
    }

    fn decode_invocation(payload: &[u8]) -> std::result::Result<Self, String> {
        Ok(Self(payload.to_vec()))
    }
}

fn bus(name: &'static str) -> InvocationBus {
    InvocationBus::new(Arc::new(Fleet::join(name, 1).expect("fleet")))
}

#[test]
fn typed_invocation_round_trips_with_origin_identity() {
    let bus = bus("invoke-roundtrip");
    let mut cursor = bus.cursor_from_start();

    let id = bus.submit_typed(&Ping(b"hello".to_vec())).expect("submit");
    let poll = bus.poll(&mut cursor);

    assert_eq!(poll.lagged, 0);
    assert_eq!(poll.invocations.len(), 1);
    assert_eq!(poll.invocations[0].id, id);
    assert_eq!(id.node(), bus.node_id().get());
    assert_ne!(poll.invocations[0].submitted_at, OrbitEpoch::ZERO);
    assert_eq!(
        poll.invocations[0].decode::<Ping>().expect("decode"),
        Ping(b"hello".to_vec())
    );
}

#[test]
fn operation_filter_advances_past_other_invocations() {
    let bus = bus("invoke-filter");
    let mut cursor = bus.cursor_from_start();

    bus.submit("other.v1", b"ignored").expect("other");
    bus.submit(Ping::OPERATION, b"seen").expect("ping");

    let poll = bus.poll_operation(&mut cursor, Ping::OPERATION);
    assert_eq!(poll.invocations.len(), 1);
    assert_eq!(poll.invocations[0].payload, b"seen");
    assert!(bus.poll(&mut cursor).is_empty());
}

#[test]
fn rejects_invalid_and_oversized_envelopes() {
    let bus = bus("invoke-limits");
    assert!(matches!(
        bus.submit(" ", b"payload"),
        Err(Error::EmptyOperation)
    ));

    let payload = vec![0; INVOCATION_PAYLOAD_MAX];
    assert!(matches!(
        bus.submit("large.v1", &payload),
        Err(Error::FrameTooLarge { .. })
    ));
}

#[test]
fn reports_overwritten_invocations_as_lag() {
    let bus = bus("invoke-lag");
    let mut cursor = bus.cursor_from_start();

    for sequence in 0..=INVOCATION_RING_CAPACITY {
        bus.submit("sequence.v1", &sequence.to_le_bytes())
            .expect("submit");
    }

    let poll = bus.poll(&mut cursor);
    assert!(poll.lagged > 0);
    assert_eq!(poll.invocations.len(), INVOCATION_RING_CAPACITY);
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn subscription_keeps_retained_invocations_after_reporting_lag() {
    let bus = Arc::new(bus("invoke-subscription-lag"));
    let mut subscription = bus.clone().subscribe("sequence.v1").expect("subscribe");

    for sequence in 0..=INVOCATION_RING_CAPACITY {
        bus.submit("sequence.v1", &sequence.to_le_bytes())
            .expect("submit");
    }

    assert!(matches!(subscription.receive().await, Err(Error::Lagged(count)) if count > 0));
    let invocation = subscription
        .receive()
        .await
        .expect("first retained invocation");
    assert_eq!(invocation.operation, "sequence.v1");
    assert_eq!(invocation.payload, 1usize.to_le_bytes());
}
