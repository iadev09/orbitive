//! Two-bank publication. No process lookup, pin RMW, or structural lock.

use std::sync::atomic::{
    AtomicU8, AtomicU16, AtomicU32, AtomicU64, Ordering, fence,
};

use super::{CELL_TEXT_MAX, TextId};
use crate::wake::Wake;
use crate::{Error, Result, SLOT_EMPTY, SLOT_OCCUPIED};

#[repr(C, align(64))]
struct Bank {
    len: AtomicU16,
    bytes: [AtomicU8; CELL_TEXT_MAX],
}

impl Bank {
    fn empty() -> Self {
        Self {
            len: AtomicU16::new(0),
            bytes: std::array::from_fn(|_| AtomicU8::new(0)),
        }
    }
}

#[repr(C, align(64))]
pub(super) struct TextSlot {
    pub(super) state: AtomicU8,
    pub(super) generation: AtomicU32,
    pub(super) writer: AtomicU64,
    /// Upper 63 bits: nonwrapping revision. Low bit: committed bank.
    pub(super) committed: AtomicU64,
    pub(super) wake: Wake,
    banks: [Bank; 2],
}

impl TextSlot {
    pub(super) fn empty() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            generation: AtomicU32::new(0),
            writer: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            wake: Wake::new(),
            banks: [Bank::empty(), Bank::empty()],
        }
    }

    pub(super) fn is(
        &self,
        id: TextId,
    ) -> bool {
        self.state.load(Ordering::Acquire) == SLOT_OCCUPIED
            && self.generation.load(Ordering::Relaxed) == id.generation()
    }

    /// Under the structural lock, with no handles or recovery using the slot.
    pub(super) fn install(
        &self,
        text: &[u8],
    ) -> u32 {
        let generation = self
            .generation
            .load(Ordering::Relaxed)
            .checked_add(1)
            .expect("exhausted generations are never reused");
        for bank in &self.banks {
            for (index, byte) in bank.bytes.iter().enumerate() {
                byte.store(
                    text.get(index).copied().unwrap_or(0),
                    Ordering::Relaxed,
                );
            }
            bank.len.store(text.len() as u16, Ordering::Relaxed);
        }
        self.committed.store(0, Ordering::Relaxed);
        self.writer.store(0, Ordering::Relaxed);
        self.wake.reset();
        self.generation.store(generation, Ordering::Relaxed);
        self.state.store(SLOT_OCCUPIED, Ordering::Release);
        generation
    }

    pub(super) fn copy(
        &self,
        id: TextId,
        output: &mut [u8],
    ) -> Result<usize> {
        let before = self.committed.load(Ordering::Acquire);
        let bank = &self.banks[(before & 1) as usize];
        let len = usize::from(bank.len.load(Ordering::Relaxed));
        #[cfg(test)]
        super::tests::at(super::tests::Phase::Read);
        for (target, source) in output.iter_mut().zip(&bank.bytes).take(len) {
            *target = source.load(Ordering::Relaxed);
        }
        // A byte read from a reused bank synchronizes with its release fence,
        // making the intervening commit visible to the final validation.
        fence(Ordering::Acquire);
        if self.committed.load(Ordering::Relaxed) != before {
            return Err(Error::Contended);
        }
        if len > CELL_TEXT_MAX {
            return Err(Error::Corrupt(id.to_string()));
        }
        Ok(len)
    }

    pub(super) fn writer(
        &self,
        token: u64,
    ) -> Result<Writer<'_>> {
        self.writer
            .compare_exchange(0, token, Ordering::Acquire, Ordering::Relaxed)
            .map_err(|_| Error::Contended)?;
        let writer = Writer { slot: self, token };
        #[cfg(test)]
        super::tests::at(super::tests::Phase::Claim);
        Ok(writer)
    }

    /// The caller holds the exact dead owner's kernel hold and structural
    /// lock. The latter stabilizes generation against physical slot reuse.
    pub(super) fn recover(
        &self,
        token: u64,
    ) -> bool {
        if token == 0 || self.writer.load(Ordering::Acquire) != token {
            return false;
        }
        // The dead writer did not release its owner word. Relay the latest
        // publication into the next owner's acquire without changing it.
        // This RMW belongs only to recovery, never the value hot path.
        self.committed.fetch_or(0, Ordering::AcqRel);
        // Retain the dead token AND its proof through wake. If this helper
        // dies, the same old row remains recoverable by the next helper.
        self.wake.notify();
        #[cfg(test)]
        super::tests::at(super::tests::Phase::RecoveredWake);
        self.writer
            .compare_exchange(token, 0, Ordering::Release, Ordering::Relaxed)
            .is_ok()
    }
}

pub(super) struct Writer<'a> {
    slot: &'a TextSlot,
    token: u64,
}

impl Writer<'_> {
    /// Append builds a complete new bank; the committed bank is never edited.
    pub(super) fn write(
        &self,
        bytes: &[u8],
        append: bool,
    ) -> Result<usize> {
        let before = self.slot.committed.load(Ordering::Acquire);
        let revision = (before >> 1)
            .checked_add(1)
            .filter(|revision| *revision <= u64::MAX >> 1)
            .ok_or(Error::RevisionExhausted)?;
        let source = &self.slot.banks[(before & 1) as usize];
        let offset = if append {
            usize::from(source.len.load(Ordering::Relaxed))
        } else {
            0
        };
        if offset > CELL_TEXT_MAX {
            return Err(Error::Corrupt("committed text length".into()));
        }
        let len = offset + bytes.len();
        if len > CELL_TEXT_MAX {
            return Err(Error::TooLong { len, max: CELL_TEXT_MAX });
        }
        let index = (before & 1) ^ 1;
        let target = &self.slot.banks[index as usize];
        fence(Ordering::Release);
        for (to, from) in target.bytes.iter().zip(&source.bytes).take(offset) {
            to.store(from.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        for (to, from) in target.bytes[offset..].iter().zip(bytes) {
            to.store(*from, Ordering::Relaxed);
            #[cfg(test)]
            super::tests::at(super::tests::Phase::Stage);
        }
        target.len.store(len as u16, Ordering::Relaxed);
        #[cfg(test)]
        super::tests::at(super::tests::Phase::Prepared);
        // SeqCst also participates in the registered-waiter/no-wake handshake.
        self.slot.committed.store((revision << 1) | index, Ordering::SeqCst);
        #[cfg(test)]
        super::tests::at(super::tests::Phase::Commit);
        Ok(len)
    }
}

impl Drop for Writer<'_> {
    fn drop(&mut self) {
        self.slot.wake.changed();
        #[cfg(test)]
        super::tests::at(super::tests::Phase::Woken);
        debug_assert_eq!(self.slot.writer.load(Ordering::Relaxed), self.token);
        // A helper cannot acquire this live process's hold. Normal release
        // needs no second CAS; only cold recovery compares the exact token.
        self.slot.writer.store(0, Ordering::Release);
    }
}

const _: () = assert!(
    size_of::<TextSlot>() == 64 + 2 * (2 + CELL_TEXT_MAX).next_multiple_of(64)
);
