use std::cell::RefCell;
use std::sync::{Arc, mpsc};

use orbit_cell::{CELL_CAPACITY, CELL_TEXT_CAPACITY, CellType, Cells, Error};
use orbit_core::Fleet;

thread_local! {
    static BEFORE_BITS: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
}

#[derive(Clone, Copy)]
struct Paused(u64);

impl CellType for Paused {
    const TAG: u8 = 200;
    const NAME: &'static str = "test";

    fn to_bits(self) -> u64 {
        let action = BEFORE_BITS.with(|slot| slot.borrow_mut().take());
        if let Some(action) = action {
            action();
        }
        self.0
    }

    fn from_bits(bits: u64) -> Self {
        Self(bits)
    }
}

fn cells() -> Cells {
    Cells::new(Arc::new(Fleet::join("example", 1).unwrap())).unwrap()
}

#[test]
fn an_inflight_cell_store_keeps_its_generation_until_it_finishes() {
    let cells = cells();
    let old = cells.allocate(Paused(7)).unwrap();
    let id = old.id();
    let _held: Vec<_> =
        (1..CELL_CAPACITY).map(|_| cells.allocate(0_u64).unwrap()).collect();
    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = mpsc::sync_channel(0);
    let writer = std::thread::spawn(move || {
        BEFORE_BITS.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
            }));
        });
        old.store(Paused(99))
    });

    entered_rx.recv().unwrap();
    cells.release(id).unwrap();
    assert!(!cells.is_live(id));
    assert!(matches!(cells.open::<Paused>(id), Err(Error::Stale(_))));
    let blocked = matches!(cells.allocate(42_u64), Err(Error::Full { .. }));
    // Always release and join the paused writer before reporting a failure.
    resume_tx.send(()).unwrap();
    writer.join().unwrap().unwrap();
    assert!(blocked, "a live old operation must prevent slot reuse");

    let fresh = cells.allocate(42_u64).unwrap();
    assert_eq!(fresh.id().index(), id.index());
    assert_ne!(fresh.id().generation(), id.generation());
    assert_eq!(fresh.load().unwrap(), 42);
}

#[test]
fn released_text_and_reset_cells_stay_pinned_until_the_last_handle_drops() {
    let cells = cells();
    let text = cells.allocate_text("old").unwrap();
    let id = text.id();
    let kept = cells.open_text(id).unwrap();
    let _texts: Vec<_> = (1..CELL_TEXT_CAPACITY)
        .map(|_| cells.allocate_text("held").unwrap())
        .collect();
    text.release().unwrap();
    assert!(matches!(kept.store("late"), Err(Error::StaleText(_))));
    assert!(matches!(cells.allocate_text("new"), Err(Error::Full { .. })));
    drop(kept);
    let fresh = cells.allocate_text("new").unwrap();
    assert_eq!(fresh.id().index(), id.index());
    assert_ne!(fresh.id().generation(), id.generation());
    assert_eq!(fresh.load().unwrap(), "new");

    let scalar = cells.allocate(7_u64).unwrap();
    let scalar_id = scalar.id();
    let held: Vec<_> =
        (1..CELL_CAPACITY).map(|_| cells.allocate(0_u64).unwrap()).collect();
    cells.reset_all().unwrap();
    assert!(matches!(scalar.load(), Err(Error::Stale(_))));
    assert!(matches!(fresh.load(), Err(Error::StaleText(_))));
    assert!(matches!(cells.allocate(42_u64), Err(Error::Full { .. })));
    assert!(matches!(cells.allocate_text("new"), Err(Error::Full { .. })));
    drop(scalar);
    let replacement = cells.allocate(42_u64).unwrap();
    assert_eq!(replacement.id().index(), scalar_id.index());
    assert_eq!(replacement.load().unwrap(), 42);
    drop(held);
}

/// An open takes no lock: it counts first and validates after. One that
/// finds its slot stale gives the count back, so the slot is reused; one
/// that holds a handle keeps its slot from reuse until the handle drops.
#[test]
fn a_failed_open_leaves_no_pin_and_a_held_one_keeps_its_slot() {
    let cells = cells();
    let mut held: Vec<_> = (0..CELL_CAPACITY)
        .map(|value| cells.allocate(value as i64).unwrap())
        .collect();
    assert!(matches!(cells.allocate(0_i64), Err(Error::Full { .. })));

    let stale = held.pop().unwrap();
    let stale_id = stale.id();
    stale.release().unwrap();
    assert!(matches!(cells.open::<i64>(stale_id), Err(Error::Stale(_))));
    let reused = cells.allocate(1_i64).expect("a failed open pins nothing");

    let pinned = held.pop().unwrap();
    let opened = cells.open::<i64>(pinned.id()).unwrap();
    pinned.release().unwrap();
    assert!(matches!(cells.allocate(2_i64), Err(Error::Full { .. })));
    drop(opened);
    let _after =
        cells.allocate(3_i64).expect("the last handle released the slot");
    drop(reused);
}

#[test]
fn a_scalar_opens_by_id_as_the_type_its_slot_holds() {
    use orbit_cell::Scalar;

    let cells = cells();
    let int = cells.allocate(7_i64).unwrap();
    let float = cells.allocate(0.5_f64).unwrap();
    let flag = cells.allocate(true).unwrap();
    assert!(
        matches!(cells.open_scalar(int.id()), Ok(Scalar::Int(cell)) if cell.load().unwrap() == 7)
    );
    assert!(
        matches!(cells.open_scalar(float.id()), Ok(Scalar::Float(cell)) if cell.load().unwrap() == 0.5)
    );
    assert!(
        matches!(cells.open_scalar(flag.id()), Ok(Scalar::Bool(cell)) if cell.load().unwrap())
    );

    let other = cells.allocate(Paused(1)).unwrap();
    assert!(matches!(
        cells.open_scalar(other.id()),
        Err(Error::TypeMismatch { .. })
    ));
    let gone = int.id();
    int.release().unwrap();
    assert!(matches!(cells.open_scalar(gone), Err(Error::Stale(_))));
}

/// Opens race reuse of the very slot they name; when every handle is gone,
/// every count an open took has been given back and the table fills again.
#[test]
fn concurrent_opens_racing_reuse_give_every_count_back() {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    let cells = cells();
    let first = cells.allocate(0_i64).unwrap();
    let current = Arc::new(Mutex::new(first.id()));
    first.release().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let openers: Vec<_> = (0..2)
        .map(|_| {
            let (cells, current, stop) =
                (cells.clone(), Arc::clone(&current), Arc::clone(&stop));
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let id = *current.lock().unwrap();
                    if let Ok(cell) = cells.open::<i64>(id) {
                        let _ = cell.load();
                    }
                }
            })
        })
        .collect();
    for value in 0..20_000_i64 {
        let cell = cells.allocate(value).unwrap();
        *current.lock().unwrap() = cell.id();
        cell.release().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    for opener in openers {
        opener.join().unwrap();
    }
    let filled: Vec<_> = (0..CELL_CAPACITY)
        .map(|value| cells.allocate(value as i64))
        .collect::<Result<_, _>>()
        .expect("no open left a count behind");
    assert_eq!(filled.len(), CELL_CAPACITY);
}
