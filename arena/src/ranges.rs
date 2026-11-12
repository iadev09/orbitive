//! An intrusive AVL tree ordered by (physical offset, slot). Writers alone
//! use it, under the arena lock. The original slot table remains authoritative.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};

use super::{
    ARENA_BYTES, Error, Header, Inner, Result, SLOT_EMPTY, SLOT_RETIRED, Slot,
};

const DIRTY: u32 = 1;
const READY: u32 = 2;
const NONE: u32 = 0;

#[repr(C)]
pub(super) struct Node {
    max_end: AtomicU64,
    left: AtomicU32,
    right: AtomicU32,
    height: AtomicU32,
}

pub(super) struct Ranges<'a> {
    header: &'a Header,
    slots: &'a [Slot],
}

impl<'a> Ranges<'a> {
    pub(super) fn begin(inner: &'a Inner) -> Result<Self> {
        let header = inner.header();
        let ranges = Self { header, slots: inner.slots() };
        let rebuild = header.range_state.load(Ordering::Acquire) != READY
            || header.range_generation.load(Ordering::Relaxed)
                != header.record_generation.load(Ordering::Relaxed);
        header.range_state.store(DIRTY, Ordering::Relaxed);
        // A writer killed after the first tree mutation must leave DIRTY.
        fence(Ordering::SeqCst);
        if rebuild {
            header.range_root.store(NONE, Ordering::Relaxed);
            for slot in ranges.slots {
                slot.range.height.store(0, Ordering::Relaxed);
            }
            for (index, slot) in ranges.slots.iter().enumerate() {
                let state = slot.state.load(Ordering::SeqCst);
                if state == SLOT_EMPTY
                    || (state == SLOT_RETIRED && !inner.pinned(index))
                {
                    continue;
                }
                let (start, end) = slot.span();
                if start > end || end > ARENA_BYTES as u64 {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "arena slot has an invalid byte range",
                    )));
                }
                ranges.insert(index);
            }
        }
        Ok(ranges)
    }

    pub(super) fn finish(
        self,
        generation: u64,
    ) {
        self.header.range_generation.store(generation, Ordering::Relaxed);
        self.header.range_state.store(READY, Ordering::Release);
    }

    pub(super) fn insert(
        &self,
        index: usize,
    ) {
        let id = index as u32 + 1;
        let node = self.node(id);
        node.left.store(NONE, Ordering::Relaxed);
        node.right.store(NONE, Ordering::Relaxed);
        self.refresh(id);
        let root = self.insert_at(self.root(), id);
        self.header.range_root.store(root, Ordering::Relaxed);
    }

    pub(super) fn remove(
        &self,
        index: usize,
    ) {
        let id = index as u32 + 1;
        let node = self.node(id);
        if node.height.load(Ordering::Relaxed) != 0 {
            let root = self.remove_at(self.root(), id);
            self.header.range_root.store(root, Ordering::Relaxed);
            node.height.store(0, Ordering::Relaxed);
        }
    }

    pub(super) fn overlap(
        &self,
        start: u64,
        end: u64,
        after: Option<(u64, usize)>,
    ) -> Option<usize> {
        let after = after.map(|(offset, index)| (offset, index as u32 + 1));
        self.overlap_at(self.root(), start, end, after)
            .map(|id| id as usize - 1)
    }

    fn overlap_at(
        &self,
        id: u32,
        start: u64,
        end: u64,
        after: Option<(u64, u32)>,
    ) -> Option<u32> {
        if self.max_end(id) <= start {
            return None;
        }
        let key = self.key(id);
        let node = self.node(id);
        if after.is_some_and(|after| key <= after) {
            return self.overlap_at(
                node.right.load(Ordering::Relaxed),
                start,
                end,
                after,
            );
        }
        if let Some(found) = self.overlap_at(
            node.left.load(Ordering::Relaxed),
            start,
            end,
            after,
        ) {
            return Some(found);
        }
        if key.0 >= end {
            return None;
        }
        if self.slot(id).span().1 > start {
            return Some(id);
        }
        self.overlap_at(node.right.load(Ordering::Relaxed), start, end, after)
    }

    fn insert_at(
        &self,
        root: u32,
        id: u32,
    ) -> u32 {
        if root == NONE {
            return id;
        }
        let child = if self.key(id) < self.key(root) {
            &self.node(root).left
        } else {
            &self.node(root).right
        };
        child.store(
            self.insert_at(child.load(Ordering::Relaxed), id),
            Ordering::Relaxed,
        );
        self.balance(root)
    }

    fn remove_at(
        &self,
        root: u32,
        id: u32,
    ) -> u32 {
        let node = self.node(root);
        let left = node.left.load(Ordering::Relaxed);
        let right = node.right.load(Ordering::Relaxed);
        if root == id {
            if left == NONE {
                return right;
            }
            if right == NONE {
                return left;
            }
            let (successor, rest) = self.take_first(right);
            let replacement = self.node(successor);
            replacement.left.store(left, Ordering::Relaxed);
            replacement.right.store(rest, Ordering::Relaxed);
            return self.balance(successor);
        }
        if self.key(id) < self.key(root) {
            node.left.store(self.remove_at(left, id), Ordering::Relaxed);
        } else {
            node.right.store(self.remove_at(right, id), Ordering::Relaxed);
        }
        self.balance(root)
    }

    fn take_first(
        &self,
        root: u32,
    ) -> (u32, u32) {
        let node = self.node(root);
        let left = node.left.load(Ordering::Relaxed);
        if left == NONE {
            return (root, node.right.load(Ordering::Relaxed));
        }
        let (first, rest) = self.take_first(left);
        node.left.store(rest, Ordering::Relaxed);
        (first, self.balance(root))
    }

    fn balance(
        &self,
        root: u32,
    ) -> u32 {
        let node = self.node(root);
        let left = node.left.load(Ordering::Relaxed);
        let right = node.right.load(Ordering::Relaxed);
        let left_height = self.height(left);
        let right_height = self.height(right);
        if left_height > right_height + 1 {
            let child = self.node(left);
            if self.height(child.right.load(Ordering::Relaxed))
                > self.height(child.left.load(Ordering::Relaxed))
            {
                node.left.store(self.rotate_left(left), Ordering::Relaxed);
            }
            self.rotate_right(root)
        } else if right_height > left_height + 1 {
            let child = self.node(right);
            if self.height(child.left.load(Ordering::Relaxed))
                > self.height(child.right.load(Ordering::Relaxed))
            {
                node.right.store(self.rotate_right(right), Ordering::Relaxed);
            }
            self.rotate_left(root)
        } else {
            self.refresh(root);
            root
        }
    }

    fn rotate_left(
        &self,
        id: u32,
    ) -> u32 {
        let node = self.node(id);
        let pivot = node.right.load(Ordering::Relaxed);
        let next = self.node(pivot);
        node.right.store(next.left.load(Ordering::Relaxed), Ordering::Relaxed);
        next.left.store(id, Ordering::Relaxed);
        self.refresh(id);
        self.refresh(pivot);
        pivot
    }

    fn rotate_right(
        &self,
        id: u32,
    ) -> u32 {
        let node = self.node(id);
        let pivot = node.left.load(Ordering::Relaxed);
        let next = self.node(pivot);
        node.left.store(next.right.load(Ordering::Relaxed), Ordering::Relaxed);
        next.right.store(id, Ordering::Relaxed);
        self.refresh(id);
        self.refresh(pivot);
        pivot
    }

    fn refresh(
        &self,
        id: u32,
    ) {
        let node = self.node(id);
        let left = node.left.load(Ordering::Relaxed);
        let right = node.right.load(Ordering::Relaxed);
        node.height.store(
            1 + self.height(left).max(self.height(right)),
            Ordering::Relaxed,
        );
        node.max_end.store(
            self.slot(id)
                .span()
                .1
                .max(self.max_end(left))
                .max(self.max_end(right)),
            Ordering::Relaxed,
        );
    }

    fn root(&self) -> u32 {
        self.header.range_root.load(Ordering::Relaxed)
    }

    fn height(
        &self,
        id: u32,
    ) -> u32 {
        if id == NONE {
            0
        } else {
            self.node(id).height.load(Ordering::Relaxed)
        }
    }

    fn max_end(
        &self,
        id: u32,
    ) -> u64 {
        if id == NONE {
            0
        } else {
            self.node(id).max_end.load(Ordering::Relaxed)
        }
    }

    fn key(
        &self,
        id: u32,
    ) -> (u64, u32) {
        (self.slot(id).body_offset.load(Ordering::Relaxed), id)
    }

    fn node(
        &self,
        id: u32,
    ) -> &Node {
        &self.slot(id).range
    }

    fn slot(
        &self,
        id: u32,
    ) -> &Slot {
        &self.slots[id as usize - 1]
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use orbit_core::Fleet;

    use super::Ranges;
    use crate::{ARENA_BYTES, ARENA_SLOTS, Arena, Record};

    #[test]
    fn overlap_matches_the_slot_scan_after_insertions_and_removals() {
        let arena =
            Arena::new(Arc::new(Fleet::join("ranges", 1).unwrap())).unwrap();
        let count = ARENA_SLOTS.min(128);
        let mut expected = Vec::new();
        for index in 0..count {
            let body = [1; 64];
            let address = arena
                .store(Record::body(&body[..(index * 29) % body.len()]))
                .unwrap();
            let index = address.slot as usize;
            let (start, end) = arena.inner.slots()[index].span();
            expected.push((start, end, index));
        }

        let _guard = arena.inner.lock().unwrap();
        let ranges = Ranges::begin(&arena.inner).unwrap();
        for step in 0..count {
            for query in 0..=count {
                let start = (query * 31) as u64;
                let end = start + (step % 41) as u64;
                let mut reference: Vec<_> = expected
                    .iter()
                    .filter(|&&(lo, hi, _)| hi > start && lo < end)
                    .map(|&(lo, _, index)| (lo, index))
                    .collect();
                reference.sort_unstable();

                let mut actual = Vec::new();
                let mut after = None;
                while let Some(index) = ranges.overlap(start, end, after) {
                    let position = arena.inner.slots()[index].span().0;
                    actual.push((position, index));
                    after = Some((position, index));
                }
                assert_eq!(actual, reference);
            }
            let remove = (step * 17) % expected.len();
            ranges.remove(expected.swap_remove(remove).2);
        }
        assert_eq!(ranges.overlap(0, ARENA_BYTES as u64, None), None);
    }

    #[test]
    fn a_new_slot_generation_invalidates_the_index() {
        let arena =
            Arena::new(Arc::new(Fleet::join("ranges", 1).unwrap())).unwrap();
        let header = arena.inner.header();
        {
            let _guard = arena.inner.lock().unwrap();
            Ranges::begin(&arena.inner).unwrap().finish(0);
        }
        // A v2 writer updates the original fields without touching the index.
        // The ordinary first pass has exactly that behavior.
        let first = arena.store(Record::body(b"old")).unwrap();
        let held = arena.get_address(first).unwrap();
        header.cursor.store(ARENA_BYTES as u64, Ordering::Relaxed);
        let next = arena.store(Record::body(b"new")).unwrap();
        assert_eq!(held.body(), b"old");
        assert_eq!(arena.get_address(next).unwrap().body(), b"new");
    }

    #[cfg(unix)]
    #[test]
    fn child_interrupted_range_update() {
        use std::io::{Read, Write};

        let Ok(name) = std::env::var("ORBIT_ARENA_RANGE_CRASH_TEST") else {
            return;
        };
        let index = std::env::var("ORBIT_ARENA_RANGE_CRASH_SLOT")
            .unwrap()
            .parse()
            .unwrap();
        let name = Box::leak(name.into_boxed_str());
        let arena =
            Arena::new(Arc::new(Fleet::join_shm(name, 2).unwrap())).unwrap();
        let _guard = arena.inner.lock().unwrap();
        let ranges = Ranges::begin(&arena.inner).unwrap();
        ranges.remove(index);
        println!("RANGE_REMOVED");
        std::io::stdout().flush().unwrap();
        let _ = std::io::stdin().read_exact(&mut [0]);
    }

    #[cfg(unix)]
    #[test]
    fn a_killed_writer_rebuilds_before_reusing_retired_pinned_bytes() {
        use std::io::BufRead;
        use std::os::unix::process::ExitStatusExt;
        use std::process::{Command, Stdio};

        let name = format!("ar_range_{}", std::process::id());
        let name = Box::leak(name.into_boxed_str());
        let arena =
            Arena::new(Arc::new(Fleet::join_shm(name, 2).unwrap())).unwrap();
        let first = arena.store(Record::body(b"first")).unwrap();
        let second = arena.store(Record::body(b"stale")).unwrap();
        let held = arena.get_address(first).unwrap();
        assert!(arena.forget_address(first).unwrap());
        let header = arena.inner.header();
        {
            let _guard = arena.inner.lock().unwrap();
            header.cursor.store(ARENA_BYTES as u64, Ordering::Relaxed);
            Ranges::begin(&arena.inner).unwrap().finish(second.generation);
        }

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "ranges::tests::child_interrupted_range_update",
                "--nocapture",
            ])
            .env("ORBIT_ARENA_RANGE_CRASH_TEST", &*name)
            .env("ORBIT_ARENA_RANGE_CRASH_SLOT", first.slot.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(output.read_line(&mut line).unwrap(), 0);
            if line.trim() == "RANGE_REMOVED" {
                break;
            }
        }
        let input = child.stdin.take();
        child.kill().unwrap();
        assert_eq!(child.wait().unwrap().signal(), Some(9));
        drop(input);

        let replacement = arena.store(Record::body(b"fresh")).unwrap();
        assert_eq!(held.body(), b"first");
        assert!(arena.get_address(first).is_none());
        assert!(arena.get_address(second).is_none());
        assert_eq!(arena.get_address(replacement).unwrap().body(), b"fresh");
        drop(held);
        arena.unlink().unwrap();
    }
}
