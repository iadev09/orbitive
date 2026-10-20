use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use super::*;

thread_local! {
    // Tracking is local to this test thread; parallel test setup is excluded.
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingAllocator;

fn record() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value + 1));
        }
    });
}

// SAFETY: every allocation operation delegates unchanged to System; the
// thread-local counter neither allocates nor changes pointers or layouts.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(
        &self,
        layout: Layout,
    ) -> *mut u8 {
        record();
        // SAFETY: forwarding the allocator caller's valid layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(
        &self,
        layout: Layout,
    ) -> *mut u8 {
        record();
        // SAFETY: forwarding the allocator caller's valid layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(
        &self,
        pointer: *mut u8,
        layout: Layout,
    ) {
        // SAFETY: the pointer and original layout are passed through unchanged.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: Layout,
        size: usize,
    ) -> *mut u8 {
        record();
        // SAFETY: forwarding the caller's allocation and requested new size.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn measured<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let result = operation();
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    (result, allocations)
}

#[test]
fn admission_paths_do_not_allocate() {
    let (quota, clock) = store(geometry(1, 1, 1), 2);
    let other = Key::new(b"other", b"subject").unwrap();
    let oversized = Key::new(b"test", &[0; 17]).unwrap();
    let (results, allocations) = measured(|| {
        [
            quota.try_consume(key(), count(1)),
            quota.try_consume(key(), count(1)),
            quota.try_consume(key(), count(1)),
            quota.try_consume(other, count(1)),
            quota.try_consume(oversized, count(1)),
            quota.try_consume(key(), count(3)),
        ]
    });
    assert_eq!(allocations, 0);
    let [allowed, existing, limited, full, large, cost] = results;
    assert_eq!(allowed.unwrap(), Decision::Allowed);
    assert_eq!(existing.unwrap(), Decision::Allowed);
    assert!(matches!(limited.unwrap(), Decision::Limited { .. }));
    assert!(matches!(full, Err(Error::CapacityFull)));
    assert!(matches!(large, Err(Error::KeyTooLarge)));
    assert!(matches!(cost, Err(Error::CostExceedsBurst)));

    clock.now.store(200, Ordering::Relaxed);
    let (result, allocations) = measured(|| quota.try_consume(other, count(1)));
    assert_eq!(result.unwrap(), Decision::Allowed);
    assert_eq!(allocations, 0);

    let mut budget = WorkBudget::new(cas_budget());
    let _guard = quota.inner.shards[0].slots[0].reserve(&mut budget).unwrap();
    let (result, allocations) = measured(|| quota.try_consume(other, count(1)));
    assert!(matches!(result, Err(Error::Busy)));
    assert_eq!(allocations, 0);
}

#[cfg(unix)]
#[test]
fn public_admission_with_host_clock_does_not_allocate() {
    let policy =
        Policy::new(count(1), Duration::from_secs(1), count(2)).unwrap();
    let quota =
        MemoryQuota::new(geometry(1, 1, 1), policy, cas_budget()).unwrap();
    let (results, allocations) = measured(|| {
        [quota.try_consume(key(), count(1)), quota.try_consume(key(), count(1))]
    });
    for result in results {
        assert_eq!(result.unwrap(), Decision::Allowed);
    }
    assert_eq!(allocations, 0);
}
