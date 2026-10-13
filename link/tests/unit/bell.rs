use super::*;

#[test]
fn a_failed_send_keeps_the_next_wake_attempt_armed() {
    let segment = format!("/orbit-link-wake-test-{}", std::process::id());
    let bell = crate::bell::Bell::new(&segment).unwrap();
    let door = crate::bell::Doorbell::Lane(0);
    let reactor = AtomicU32::new(1);

    assert!(ring_armed(&reactor, &bell, door).is_err());
    assert_eq!(reactor.load(Ordering::SeqCst), 1);

    let socket = bell.bind(door).unwrap();
    ring_armed(&reactor, &bell, door).unwrap();
    assert_eq!(reactor.load(Ordering::SeqCst), 0);
    let mut byte = [0];
    assert_eq!(socket.recv(&mut byte).unwrap(), 1);
    assert_eq!(byte, [1]);

    drop(socket);
    bell.unbind(door);
}
