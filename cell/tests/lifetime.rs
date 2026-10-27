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
