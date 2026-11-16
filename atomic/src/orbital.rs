//! The handle: a pointer to one slot and the table that keeps it mapped.

use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use crate::table::{Slot, Table};
use crate::{OrbitalId, Primitive, Result};

/// One shared value, typed. Cheap to clone; every clone, and every handle
/// any process opens from the same id, is the same 64 bits.
///
/// The operations are those of `std::sync::atomic` and cannot fail. The
/// memory ordering is not a parameter: every write is sequentially
/// consistent, which is what lets [`Self::wait`] never miss one.
pub struct Orbital<T: Primitive> {
    slot: NonNull<Slot>,
    id: OrbitalId,
    /// Keeps the mapping, or the in-memory slot array, that `slot` points
    /// into.
    table: Arc<Table>,
    _type: PhantomData<T>,
}

// SAFETY: the slot is atomics in memory the handle keeps alive.
unsafe impl<T: Primitive> Send for Orbital<T> {}
// SAFETY: as above.
unsafe impl<T: Primitive> Sync for Orbital<T> {}

impl<T: Primitive> Clone for Orbital<T> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot,
            id: self.id,
            table: Arc::clone(&self.table),
            _type: PhantomData,
        }
    }
}

impl<T: Primitive> fmt::Debug for Orbital<T> {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_tuple("Orbital").field(&self.id).finish()
    }
}

impl<T: Primitive> Orbital<T> {
    pub(crate) fn new(
        slot: NonNull<Slot>,
        id: OrbitalId,
        table: Arc<Table>,
    ) -> Self {
        Self { slot, id, table, _type: PhantomData }
    }

    #[inline]
    fn slot(&self) -> &Slot {
        // SAFETY: resolved when the handle was made, inside memory that
        // `table` keeps mapped for as long as the handle lives.
        unsafe { self.slot.as_ref() }
    }

    /// One write: the value word, then whether anyone waits.
    #[inline]
    fn write<R>(
        &self,
        operation: impl FnOnce(&AtomicU64) -> R,
    ) -> R {
        let slot = self.slot();
        let result = operation(&slot.value);
        slot.wake.changed();
        result
    }

    /// The address to hand to another process.
    pub fn id(&self) -> OrbitalId {
        self.id
    }

    #[inline]
    pub fn load(&self) -> T {
        T::from_word(self.slot().value.load(Ordering::Acquire))
    }

    #[inline]
    pub fn store(
        &self,
        value: T,
    ) {
        self.write(|word| word.store(value.into_word(), Ordering::SeqCst));
    }

    /// Replace the value and return what it was.
    #[inline]
    pub fn swap(
        &self,
        value: T,
    ) -> T {
        T::from_word(
            self.write(|word| word.swap(value.into_word(), Ordering::SeqCst)),
        )
    }

    /// Store `new` only if the value is still `current`: `Ok(previous)`
    /// when it was, `Err(actual)` when it held something else. An `f64` is
    /// compared by its bits, so `0.0` is not `-0.0` and a NaN equals only
    /// the same NaN.
    #[inline]
    pub fn compare_exchange(
        &self,
        current: T,
        new: T,
    ) -> Result<T, T> {
        let slot = self.slot();
        let exchanged = slot.value.compare_exchange(
            current.into_word(),
            new.into_word(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        if exchanged.is_ok() {
            slot.wake.changed();
        }
        exchanged.map(T::from_word).map_err(T::from_word)
    }

    /// Park until the value is no longer `current`, then return what it
    /// is. Returns at once when it already differs.
    ///
    /// Coalescing: any number of writes while parked are one wake, and a
    /// value that left `current` and came back before the caller looked is
    /// no change. An `f64` is compared by its bits. Blocking; an async
    /// runtime runs it where blocking is allowed.
    pub fn wait(
        &self,
        current: T,
    ) -> Result<T> {
        let slot = self.slot();
        let current = current.into_word();
        loop {
            let now = slot.value.load(Ordering::SeqCst);
            if now != current {
                return Ok(T::from_word(now));
            }
            slot.wake
                .wait_if(|| slot.value.load(Ordering::SeqCst) == current)?;
        }
    }

    /// [`Self::wait`], which a second word can end: `Ok(Some(value))` is a
    /// change, `Ok(None)` is the interrupt.
    ///
    /// This is not a timeout; nothing here measures time. The canceller
    /// sets `interrupt` and then calls [`Self::notify`], in that order, on
    /// any handle to the same value.
    pub fn wait_until(
        &self,
        current: T,
        interrupt: &AtomicBool,
    ) -> Result<Option<T>> {
        let slot = self.slot();
        let current = current.into_word();
        loop {
            if interrupt.load(Ordering::SeqCst) {
                return Ok(None);
            }
            let now = slot.value.load(Ordering::SeqCst);
            if now != current {
                return Ok(Some(T::from_word(now)));
            }
            slot.wake.wait_if(|| {
                slot.value.load(Ordering::SeqCst) == current
                    && !interrupt.load(Ordering::SeqCst)
            })?;
        }
    }

    /// Wake everyone parked on this value without writing to it. A waiter
    /// whose value has not changed and whose interrupt is not set parks
    /// again.
    pub fn notify(&self) {
        self.slot().wake.notify();
    }
}

macro_rules! integer {
    ($type:ty, $atomic:ty) => {
        impl Orbital<$type> {
            #[inline]
            fn integer<R>(
                &self,
                operation: impl FnOnce(&$atomic) -> R,
            ) -> R {
                // SAFETY: the value word is a live, aligned 64-bit atomic,
                // and both atomic integer types share its representation.
                self.write(|word| unsafe {
                    operation(<$atomic>::from_ptr(word.as_ptr().cast()))
                })
            }

            /// Add, wrapping on overflow, and return the previous value.
            #[inline]
            pub fn fetch_add(
                &self,
                value: $type,
            ) -> $type {
                self.integer(|word| word.fetch_add(value, Ordering::SeqCst))
            }

            /// Subtract, wrapping on overflow, and return the previous
            /// value.
            #[inline]
            pub fn fetch_sub(
                &self,
                value: $type,
            ) -> $type {
                self.integer(|word| word.fetch_sub(value, Ordering::SeqCst))
            }

            #[inline]
            pub fn fetch_and(
                &self,
                value: $type,
            ) -> $type {
                self.integer(|word| word.fetch_and(value, Ordering::SeqCst))
            }

            #[inline]
            pub fn fetch_or(
                &self,
                value: $type,
            ) -> $type {
                self.integer(|word| word.fetch_or(value, Ordering::SeqCst))
            }

            #[inline]
            pub fn fetch_xor(
                &self,
                value: $type,
            ) -> $type {
                self.integer(|word| word.fetch_xor(value, Ordering::SeqCst))
            }

            /// Keep the larger of the two and return the previous value.
            #[inline]
            pub fn fetch_max(
                &self,
                value: $type,
            ) -> $type {
                self.integer(|word| word.fetch_max(value, Ordering::SeqCst))
            }

            /// Keep the smaller of the two and return the previous value.
            #[inline]
            pub fn fetch_min(
                &self,
                value: $type,
            ) -> $type {
                self.integer(|word| word.fetch_min(value, Ordering::SeqCst))
            }
        }
    };
}

integer!(i64, AtomicI64);
integer!(u64, AtomicU64);

impl Orbital<f64> {
    /// Add and return the previous value. A compare-and-swap loop: floats
    /// have no fetch-add of their own.
    pub fn fetch_add(
        &self,
        value: f64,
    ) -> f64 {
        f64::from_bits(self.write(|word| {
            word.update(Ordering::SeqCst, Ordering::SeqCst, |bits| {
                (f64::from_bits(bits) + value).to_bits()
            })
        }))
    }
}

impl Orbital<bool> {
    #[inline]
    pub fn fetch_and(
        &self,
        value: bool,
    ) -> bool {
        self.write(|word| word.fetch_and(u64::from(value), Ordering::SeqCst))
            != 0
    }

    #[inline]
    pub fn fetch_or(
        &self,
        value: bool,
    ) -> bool {
        self.write(|word| word.fetch_or(u64::from(value), Ordering::SeqCst))
            != 0
    }

    #[inline]
    pub fn fetch_xor(
        &self,
        value: bool,
    ) -> bool {
        self.write(|word| word.fetch_xor(u64::from(value), Ordering::SeqCst))
            != 0
    }
}
