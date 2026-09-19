use std::sync::Arc;

use orbit_cell::{CellId, Cells, Error};
use orbit_core::Fleet;

fn cells() -> Cells {
    Cells::new(Arc::new(Fleet::join("cell-test", 1).expect("fleet"))).expect("cells")
}

#[test]
fn a_cell_is_the_same_place_through_every_handle() {
    let cells = cells();
    let counter = cells.allocate(0_i64).expect("allocate");
    let elsewhere = cells.open::<i64>(counter.id()).expect("open by id");

    assert_eq!(counter.fetch_add(1).expect("add"), 1);
    assert_eq!(elsewhere.fetch_add(5).expect("add"), 6);
    assert_eq!(elsewhere.fetch_sub(2).expect("sub"), 4);
    assert_eq!(counter.load().expect("load"), 4);
    assert_eq!(counter.swap(10).expect("swap"), 4);
    assert_eq!(elsewhere.compare_exchange(10, 11).expect("cas"), Ok(10));
    assert_eq!(elsewhere.compare_exchange(10, 12).expect("cas"), Err(11));
}

#[test]
fn every_type_keeps_its_own_bits() {
    let cells = cells();
    let float = cells.allocate(0.5_f64).expect("float");
    assert_eq!(float.fetch_add(0.25).expect("add"), 0.75);
    let unsigned = cells.allocate(u64::MAX - 1).expect("unsigned");
    assert_eq!(unsigned.fetch_add(1).expect("add"), u64::MAX);
    assert!(matches!(unsigned.fetch_add(1), Err(Error::Overflow)));
    assert_eq!(unsigned.fetch_sub(u64::MAX).expect("sub"), 0);
    let flag = cells.allocate(false).expect("flag");
    assert!(!flag.set().expect("set"));
    assert!(flag.set().expect("set again"));
    assert!(flag.clear().expect("clear"));
}

#[test]
fn the_type_is_stamped_on_the_cell() {
    let cells = cells();
    let counter = cells.allocate(7_i64).expect("allocate");
    let wrong = cells.open::<f64>(counter.id());
    assert!(matches!(
        wrong,
        Err(Error::TypeMismatch {
            expected: "float",
            found: "int",
            ..
        })
    ));
}

#[test]
fn a_released_cell_goes_stale_and_its_slot_is_reused_under_a_new_generation() {
    let cells = cells();
    let first = cells.allocate(1_i64).expect("allocate");
    let id = first.id();
    let kept = cells.open::<i64>(id).expect("open");
    first.release().expect("release");

    assert!(matches!(kept.load(), Err(Error::Stale(stale)) if stale == id));
    assert!(matches!(cells.open::<i64>(id), Err(Error::Stale(_))));
    assert!(!cells.is_live(id));

    let mut fresh = None;
    for _ in 0..orbit_cell::CELL_CAPACITY {
        let cell = cells.allocate(2_i64).expect("allocate");
        if cell.id().index() == id.index() {
            fresh = Some(cell);
            break;
        }
    }
    let fresh = fresh.expect("the released slot is reused within one lap");
    assert_ne!(fresh.id().generation(), id.generation());
    assert!(
        matches!(kept.load(), Err(Error::Stale(_))),
        "the old handle still does not reach the new occupant"
    );
    assert_eq!(fresh.load().expect("load"), 2);
}

#[test]
fn a_full_table_says_so() {
    let cells = cells();
    let held: Vec<_> = (0..orbit_cell::CELL_CAPACITY)
        .map(|_| cells.allocate(0_i64).expect("allocate"))
        .collect();
    assert!(matches!(cells.allocate(0_i64), Err(Error::Full { .. })));
    held.into_iter()
        .next()
        .expect("one")
        .release()
        .expect("release");
    assert!(cells.allocate(0_i64).is_ok());
}

#[test]
fn ids_round_trip_through_text_and_bits() {
    let cells = cells();
    let cell = cells.allocate(3_i64).expect("allocate");
    let id = cell.id();
    let text = id.to_string();
    assert!(text.starts_with("cell:"));
    assert_eq!(text.parse::<CellId>().expect("parse"), id);
    assert_eq!(CellId::from_bits(id.to_bits()), id);
    assert!(matches!(
        "cell:x".parse::<CellId>(),
        Err(Error::Malformed(_))
    ));
    assert_eq!(
        cells
            .open::<i64>(text.parse().expect("parse"))
            .expect("open")
            .load()
            .expect("load"),
        3
    );
}

#[test]
fn text_cells_append_atomically_and_refuse_what_does_not_fit() {
    let cells = cells();
    let text = cells.allocate_text("ab").expect("allocate");
    let same = cells.open_text(text.id()).expect("open");
    assert_eq!(same.append("cd").expect("append"), 4);
    assert_eq!(text.load().expect("load"), "abcd");
    text.store("x").expect("store");
    assert_eq!(same.load().expect("load"), "x");

    let big = "y".repeat(orbit_cell::CELL_TEXT_MAX);
    assert!(matches!(same.append(&big), Err(Error::TooLong { .. })));
    assert_eq!(
        text.load().expect("load"),
        "x",
        "a refused append writes nothing"
    );
    assert!(cells.allocate_text(&format!("{big}z")).is_err());

    let id = text.id();
    assert!(id.to_string().starts_with("text:"));
    text.release().expect("release");
    assert!(matches!(same.load(), Err(Error::StaleText(stale)) if stale == id));
}

#[test]
fn text_readers_never_see_a_torn_write() {
    let cells = cells();
    let text = cells.allocate_text("aaaaaaaa").expect("allocate");
    let writer = cells.open_text(text.id()).expect("open");
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let thread = std::thread::spawn(move || {
        let mut flip = false;
        while !flag.load(std::sync::atomic::Ordering::Relaxed) {
            writer
                .store(if flip { "bbbbbbbb" } else { "aaaaaaaa" })
                .expect("store");
            flip = !flip;
        }
    });
    for _ in 0..5_000 {
        let seen = text.load().expect("load");
        assert!(
            seen == "aaaaaaaa" || seen == "bbbbbbbb",
            "torn read: {seen:?}"
        );
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    thread.join().expect("writer");
}

#[test]
fn racing_callers_divide_a_limit_without_ever_passing_it() {
    // What `fetch_add` plus a test cannot do. Eight threads take from one
    // cell until nothing is left; between them they must take the limit
    // exactly, and the cell must never hold more than it. `fetch_add` here
    // would overshoot by up to seven, because each caller's add lands
    // before it can learn it went too far.
    const LIMIT: i64 = 200_000;
    const THREADS: usize = 8;

    let cells = Arc::new(Cells::new(Arc::new(Fleet::join("cell-claim", 1).expect("fleet"))).expect("cells"));
    let counter = Arc::new(cells.allocate(0_i64).expect("allocate"));

    let takers: Vec<_> = (0..THREADS)
        .map(|n| {
            let counter = Arc::clone(&counter);
            // Uneven chunks on purpose: the last taker of each size has to
            // get a remainder rather than be refused.
            let chunk = [1_i64, 3, 7, 16][n % 4];
            std::thread::spawn(move || {
                let mut mine = 0_i64;
                while let Ok(orbit_cell::Claim::Took { taken, after }) =
                    counter.add_until(chunk, LIMIT)
                {
                    assert!(after <= LIMIT, "the cell passed the limit: {after}");
                    assert!(taken > 0 && taken <= chunk, "took {taken} of {chunk}");
                    mine += taken;
                }
                mine
            })
        })
        .collect();

    let shares: Vec<i64> = takers.into_iter().map(|t| t.join().expect("join")).collect();

    assert_eq!(shares.iter().sum::<i64>(), LIMIT, "the shares must add up to the limit");
    assert_eq!(counter.load().expect("load"), LIMIT, "the cell must hold exactly the limit");
    assert_eq!(
        counter.add_until(1, LIMIT).expect("claim"),
        orbit_cell::Claim::Exhausted,
        "a claim on a finished limit takes nothing"
    );
}
