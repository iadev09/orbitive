use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
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

// SAFETY: forwarding System's contract unchanged; tracking is thread-local.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(
        &self,
        layout: Layout,
    ) -> *mut u8 {
        record();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(
        &self,
        layout: Layout,
    ) -> *mut u8 {
        record();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(
        &self,
        pointer: *mut u8,
        layout: Layout,
    ) {
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: Layout,
        size: usize,
    ) -> *mut u8 {
        record();
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

pub(crate) fn measured<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let result = operation();
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    (result, allocations)
}
