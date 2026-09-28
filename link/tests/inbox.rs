use std::sync::Arc;
use std::sync::atomic::Ordering;

use orbit_link::*;

/// A join in private memory: no other process can hold a lane, so every free
/// lane is this one's to hold.
fn join(
    inbox: &Inbox,
    service: &str,
    role: &str,
    incarnation: u64
) -> Result<usize> {
    inbox.join(service, role, incarnation, |_| Ok(Some(()))).map(|(lane, ())| lane)
}

/// A region that stands in for a mapped segment. Aligned to a cache line so
/// the layout's `#[repr(align(64))]` headers land where they say they do.
struct Region {
    ptr: *mut u8,
    layout: std::alloc::Layout
}

// Same contract as the mapped segment: the only mutable state is atomics.
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    fn new(bytes: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(bytes, 64).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null());
        Self { ptr, layout }
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        unsafe { std::alloc::dealloc(self.ptr, self.layout) }
    }
}

fn table(
    fleet_capacity: usize,
    geometry: InboxGeometry
) -> (Region, Inbox) {
    let region = Region::new(geometry.segment_bytes(fleet_capacity));
    let inbox = unsafe { Inbox::initialize(region.ptr, fleet_capacity, geometry).unwrap() };
    (region, inbox)
}

const SMALL: InboxGeometry = InboxGeometry::new(4, 64);

#[test]
fn a_frame_comes_back_as_it_went_in() {
    let (_region, inbox) = table(2, SMALL);
    inbox.claim_lane(1, "app", "asgi", 7).unwrap();

    inbox.write(1, b"GET /orders").unwrap();
    let mut out = Vec::new();
    assert!(inbox.read(1, &mut out).unwrap());
    assert_eq!(out, b"GET /orders");
    assert!(!inbox.read(1, &mut out).unwrap(), "the lane is empty again");
}

/// The whole reason this is not `orbit_core::Ring`: a request that does not
/// fit must be refused, never dropped on the floor while its caller waits.
#[test]
fn a_full_lane_refuses_instead_of_overwriting() {
    let (_region, inbox) = table(1, SMALL);
    inbox.claim_lane(0, "app", "asgi", 1).unwrap();

    for i in 0..SMALL.capacity {
        inbox.write(0, format!("frame {i}").as_bytes()).unwrap();
    }
    assert!(matches!(inbox.admits(0), Ok(Admission::Full)));
    assert!(matches!(inbox.write(0, b"one too many"), Err(Error::InboxFull)));

    // And the frames already there are untouched.
    let mut out = Vec::new();
    for i in 0..SMALL.capacity {
        assert!(inbox.read(0, &mut out).unwrap());
        assert_eq!(out, format!("frame {i}").as_bytes());
    }
}

#[test]
fn reading_makes_room_again() {
    let (_region, inbox) = table(1, SMALL);
    inbox.claim_lane(0, "app", "asgi", 1).unwrap();

    let mut out = Vec::new();
    for round in 0..10u32 {
        for i in 0..SMALL.capacity {
            inbox.write(0, format!("{round}:{i}").as_bytes()).unwrap();
        }
        assert!(matches!(inbox.write(0, b"full"), Err(Error::InboxFull)));
        for i in 0..SMALL.capacity {
            assert!(inbox.read(0, &mut out).unwrap());
            assert_eq!(out, format!("{round}:{i}").as_bytes());
        }
    }
}

/// Writing into a mailbox nobody empties leaves a request that is never
/// answered and a caller that waits for nothing. Refuse at the door.
#[test]
fn a_lane_with_no_consumer_is_refused() {
    let (_region, inbox) = table(2, SMALL);
    assert!(matches!(inbox.admits(1), Ok(Admission::NoConsumer)));
    assert!(inbox.write(1, b"nobody home").is_err());

    inbox.claim_lane(1, "app", "asgi", 3).unwrap();
    assert!(matches!(inbox.admits(1), Ok(Admission::Accepted)));
    inbox.write(1, b"somebody home").unwrap();

    inbox.release_lane(1).unwrap();
    assert!(matches!(inbox.admits(1), Ok(Admission::NoConsumer)));
}

/// What lets an edge pick a target by reading rather than guessing.
#[test]
fn depth_is_readable_before_choosing() {
    let (_region, inbox) = table(3, SMALL);
    inbox.claim_lane(0, "app", "asgi", 1).unwrap();
    inbox.claim_lane(1, "app", "asgi", 1).unwrap();
    inbox.claim_lane(2, "app", "asgi", 1).unwrap();

    inbox.write(0, b"a").unwrap();
    inbox.write(0, b"b").unwrap();
    inbox.write(2, b"c").unwrap();

    assert_eq!(inbox.depth(0).unwrap(), 2);
    assert_eq!(inbox.depth(1).unwrap(), 0);
    assert_eq!(inbox.depth(2).unwrap(), 1);

    let idlest = (0..3).min_by_key(|node| inbox.depth(*node).unwrap()).unwrap();
    assert_eq!(idlest, 1);
}

/// Lanes are mailboxes, not a shared queue: a frame addressed to one node is
/// invisible to every other.
#[test]
fn lanes_do_not_leak_into_each_other() {
    let (_region, inbox) = table(4, SMALL);
    for node in 0..4 {
        inbox.claim_lane(node, "app", "asgi", 1).unwrap();
    }
    inbox.write(2, b"for two").unwrap();

    let mut out = Vec::new();
    for node in [0, 1, 3] {
        assert!(!inbox.read(node, &mut out).unwrap(), "node {node} saw another lane's frame");
    }
    assert!(inbox.read(2, &mut out).unwrap());
    assert_eq!(out, b"for two");
}

/// A restarting owner starts from an empty mailbox. What it discards is work
/// addressed to an incarnation that is gone, whose writers gave up long ago.
#[test]
fn claiming_a_lane_clears_what_the_last_incarnation_left() {
    let (_region, inbox) = table(1, SMALL);
    inbox.claim_lane(0, "app", "asgi", 1).unwrap();
    inbox.write(0, b"before the restart").unwrap();
    assert_eq!(inbox.depth(0).unwrap(), 1);

    inbox.claim_lane(0, "app", "asgi", 2).unwrap();
    assert_eq!(inbox.depth(0).unwrap(), 0);
    assert_eq!(inbox.incarnation(0).unwrap(), 2);

    let mut out = Vec::new();
    assert!(!inbox.read(0, &mut out).unwrap());
}

#[test]
fn a_frame_larger_than_a_slot_is_refused() {
    let (_region, inbox) = table(1, SMALL);
    inbox.claim_lane(0, "app", "asgi", 1).unwrap();
    let oversized = vec![0u8; SMALL.payload_capacity + 1];
    assert!(matches!(inbox.write(0, &oversized), Err(Error::FrameTooLarge)));
}

#[test]
fn a_node_outside_the_fleet_is_refused() {
    let (_region, inbox) = table(2, SMALL);
    assert!(inbox.write(2, b"nowhere").is_err());
    assert!(inbox.depth(9).is_err());
}

/// Attaching is how a second process joins a table it did not create, so a
/// segment shaped differently from this build must be refused rather than
/// read as garbage.
#[test]
fn attaching_checks_the_header() {
    let region = Region::new(SMALL.segment_bytes(2));
    assert!(
        unsafe { Inbox::attach(region.ptr, 2, SMALL) }.is_err(),
        "zeroed bytes are not a table"
    );

    let inbox = unsafe { Inbox::initialize(region.ptr, 2, SMALL).unwrap() };
    inbox.claim_lane(0, "app", "asgi", 5).unwrap();
    let attached = unsafe { Inbox::attach(region.ptr, 2, SMALL).unwrap() };
    assert_eq!(attached.incarnation(0).unwrap(), 5);

    let wider = InboxGeometry::new(SMALL.capacity * 2, SMALL.payload_capacity);
    assert!(unsafe { Inbox::attach(region.ptr, 2, wider) }.is_err(), "geometry must match");
}

/// Writers finish out of order under contention. Nothing may be lost, nothing
/// duplicated, and no writer may be stalled by a slower one ahead of it.
#[test]
fn many_writers_one_reader_lose_nothing() {
    const WRITERS: usize = 8;
    const PER_WRITER: usize = 500;

    let geometry = InboxGeometry::new(16, 64);
    let region = Region::new(geometry.segment_bytes(1));
    let inbox = Arc::new(unsafe { Inbox::initialize(region.ptr, 1, geometry).unwrap() });
    inbox.claim_lane(0, "app", "asgi", 1).unwrap();

    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let inbox = Arc::clone(&inbox);
            std::thread::spawn(move || {
                for n in 0..PER_WRITER {
                    let frame = format!("{writer}:{n}");
                    // A full lane is a refusal, not a failure: retry until the
                    // reader has made room.
                    while matches!(inbox.write(0, frame.as_bytes()), Err(Error::InboxFull)) {
                        std::thread::yield_now();
                    }
                }
            })
        })
        .collect();

    let mut seen: Vec<Vec<usize>> = vec![Vec::new(); WRITERS];
    let mut out = Vec::new();
    let mut taken = 0;
    while taken < WRITERS * PER_WRITER {
        if inbox.read(0, &mut out).unwrap() {
            let text = String::from_utf8(out.clone()).unwrap();
            let (writer, n) = text.split_once(':').unwrap();
            seen[writer.parse::<usize>().unwrap()].push(n.parse().unwrap());
            taken += 1;
        } else {
            std::thread::yield_now();
        }
    }
    for writer in writers {
        writer.join().unwrap();
    }

    for (writer, numbers) in seen.iter().enumerate() {
        assert_eq!(numbers.len(), PER_WRITER, "writer {writer} lost or duplicated frames");
        // One writer's frames keep their own order even though the lane
        // interleaves every writer's.
        assert!(
            numbers.windows(2).all(|pair| pair[0] < pair[1]),
            "writer {writer} came back out of order"
        );
    }
    assert_eq!(inbox.depth(0).unwrap(), 0);
}

/// A parked reader needs a word that moves when a frame arrives — and only
/// then. The owner is the one waiter and the one reader: a word its own drain
/// moved would be stale by the time it parked on it, and every wake-up would
/// pay a park that returns at once. No writer waits for room; a full lane
/// refuses.
#[test]
fn the_wait_word_moves_on_a_commit_and_not_on_a_drain() {
    let (_region, inbox) = table(1, SMALL);
    inbox.claim_lane(0, "app", "asgi", 1).unwrap();

    let (word, before) = inbox.wait_word(0).unwrap();
    inbox.write(0, b"wake up").unwrap();
    assert_ne!(word.load(Ordering::Acquire), before);

    let after_write = word.load(Ordering::Acquire);
    let mut out = Vec::new();
    inbox.read(0, &mut out).unwrap();
    assert_eq!(
        word.load(Ordering::Acquire),
        after_write,
        "draining must not spoil the owner's own park token"
    );
}

/// Service processes start independently and know nothing about each
/// other. Two of them serving the same name must still end up on two lanes:
/// the claim is a compare-and-swap, so a race has a loser rather than two
/// winners sharing a mailbox.
#[test]
fn processes_that_do_not_know_each_other_take_different_lanes() {
    let (region, inbox) = table(8, SMALL);
    let inbox = Arc::new(inbox);
    let _region = region;

    let joining: Vec<_> = (0..8)
        .map(|_| {
            let inbox = Arc::clone(&inbox);
            std::thread::spawn(move || join(&inbox, "checkout", "asgi", 1).unwrap())
        })
        .collect();

    let mut lanes: Vec<usize> = joining.into_iter().map(|thread| thread.join().unwrap()).collect();
    lanes.sort_unstable();
    assert_eq!(lanes, (0..8).collect::<Vec<_>>(), "every lane taken exactly once");
    assert_eq!(inbox.targets("checkout").len(), 8);

    // And a full table says so rather than handing out a lane twice.
    assert!(matches!(join(&inbox, "checkout", "asgi", 1), Err(Error::NoLane)));
}

/// A lane its owner gave up is free again, and what the last owner left in it
/// does not reach the next one.
#[test]
fn a_released_lane_is_reused_without_its_old_frames() {
    let (_region, inbox) = table(2, SMALL);

    let lane = join(&inbox, "checkout", "asgi", 1).unwrap();
    inbox.write(lane, b"for the process that left").unwrap();
    inbox.release_lane(lane).unwrap();

    let reused = join(&inbox, "orders", "asgi", 2).unwrap();
    assert_eq!(reused, lane, "the free lane is the one that was given up");
    assert_eq!(inbox.service(lane).unwrap().as_deref(), Some("orders"));
    assert_eq!(inbox.depth(lane).unwrap(), 0, "the last owner's mail is not delivered to this one");
}

/// Two roles under one name would share an inbox and split its requests;
/// the second is refused at the door, and a second process of the same
/// role is not.
#[test]
fn a_name_served_by_another_role_is_refused() {
    let (_region, inbox) = table(4, SMALL);

    let python = join(&inbox, "api", "asgi", 1).unwrap();
    assert_eq!(inbox.role(python).unwrap().as_deref(), Some("asgi"));
    join(&inbox, "api", "asgi", 1).expect("another process of the same role scales the name");

    match join(&inbox, "api", "php", 1) {
        Err(Error::RoleConflict { held_by, role, .. }) => {
            assert_eq!((held_by.as_str(), role.as_str()), ("asgi", "php"));
        }
        other => panic!("expected a role conflict, got {other:?}")
    }
    assert_eq!(inbox.serving("api").len(), 2, "the refused join left no lane behind");
    join(&inbox, "orders", "php", 1).expect("another name is another service");
}
