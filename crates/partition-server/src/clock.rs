//! CLOCK eviction, shared by the SST block cache and the memory tier.
//!
//! Items sit in a slot array; a caller keeps an item's index and touches it
//! in O(1). A new item starts unreferenced in a free slot — usually the one
//! the hand just left — or at the end; a touch sets its reference bit.
//! Evicting walks the hand over the array, skipping empty slots, clears each
//! set bit it passes and removes the first item found clear.

use std::cell::Cell;

struct Slot<T> {
    item: T,
    referenced: Cell<bool>,
}

pub(crate) struct Clock<T> {
    slots: Vec<Option<Slot<T>>>,
    free: Vec<usize>,
    hand: usize,
    len: usize,
}

impl<T> Clock<T> {
    pub(crate) fn new() -> Self {
        Clock { slots: Vec::new(), free: Vec::new(), hand: 0, len: 0 }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Store `item` unreferenced; returns its index.
    pub(crate) fn insert(&mut self, item: T) -> usize {
        let slot = Some(Slot { item, referenced: Cell::new(false) });
        self.len += 1;
        match self.free.pop() {
            Some(i) => {
                self.slots[i] = slot;
                i
            }
            None => {
                self.slots.push(slot);
                self.slots.len() - 1
            }
        }
    }

    pub(crate) fn get(&self, idx: usize) -> Option<&T> {
        self.slots.get(idx)?.as_ref().map(|s| &s.item)
    }

    pub(crate) fn get_mut(&mut self, idx: usize) -> Option<&mut T> {
        self.slots.get_mut(idx)?.as_mut().map(|s| &mut s.item)
    }

    /// Set the reference bit of the item at `idx`, if there is one.
    pub(crate) fn touch(&self, idx: usize) {
        if let Some(Some(s)) = self.slots.get(idx) {
            s.referenced.set(true);
        }
    }

    pub(crate) fn remove(&mut self, idx: usize) -> Option<T> {
        let s = self.slots.get_mut(idx)?.take()?;
        self.free.push(idx);
        self.len -= 1;
        Some(s.item)
    }

    /// Remove and return the first item the hand finds unreferenced, clearing
    /// the bits it passes; `keep` is passed over. None when nothing else is
    /// left.
    pub(crate) fn evict(&mut self, keep: Option<usize>) -> Option<(usize, T)> {
        if self.len == 0 {
            return None;
        }
        // One turn clears every bit, so a victim turns up within two.
        for _ in 0..2 * self.slots.len() + 1 {
            let h = self.hand;
            self.hand = (h + 1) % self.slots.len();
            if Some(h) == keep {
                continue;
            }
            match &self.slots[h] {
                None => {}
                Some(s) if s.referenced.replace(false) => {}
                Some(_) => return self.remove(h).map(|item| (h, item)),
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_in_insert_order_when_nothing_is_touched() {
        let mut c = Clock::new();
        for i in 0..4 {
            c.insert(i);
        }
        let order: Vec<i32> = (0..4).map(|_| c.evict(None).unwrap().1).collect();
        assert_eq!(order, [0, 1, 2, 3]);
        assert!(c.evict(None).is_none());
    }

    /// The hand clears a touched item and takes the next clear one; the
    /// touched one goes next time unless touched again.
    #[test]
    fn a_touch_buys_one_turn() {
        let mut c = Clock::new();
        let a = c.insert("a");
        c.insert("b");
        c.touch(a);
        assert_eq!(c.evict(None).unwrap().1, "b");
        c.insert("c");
        assert_eq!(c.evict(None).unwrap().1, "a", "a was cleared by the last pass");
    }

    /// A new item fills the slot the hand just left, so everything else, even
    /// what the hand just cleared, is looked at before it.
    #[test]
    fn a_new_item_in_the_freed_slot_waits_a_full_turn() {
        let mut c = Clock::new();
        let ids: Vec<usize> = (0..4).map(|i| c.insert(i)).collect();
        for &i in &ids {
            c.touch(i);
        }
        // Clears 0..3, evicts 0; the hand now points at 1.
        assert_eq!(c.evict(None).unwrap().1, 0);
        assert_eq!(c.insert(9), ids[0]);
        let order: Vec<i32> = (0..4).map(|_| c.evict(None).unwrap().1).collect();
        assert_eq!(order, [1, 2, 3, 9]);
    }

    #[test]
    fn keep_is_never_evicted() {
        let mut c = Clock::new();
        let a = c.insert(1);
        assert!(c.evict(Some(a)).is_none());
        c.insert(2);
        assert_eq!(c.evict(Some(a)).unwrap().1, 2);
        assert_eq!(c.get(a), Some(&1));
    }

    /// The hand passes empty slots, and freed slots are reused.
    #[test]
    fn empty_slots_are_skipped_and_reused() {
        let mut c = Clock::new();
        let ids: Vec<usize> = (0..5).map(|i| c.insert(i)).collect();
        assert_eq!(c.remove(ids[0]), Some(0));
        assert_eq!(c.remove(ids[2]), Some(2));
        assert_eq!(c.remove(ids[2]), None);
        assert_eq!(c.len(), 3);
        c.touch(ids[0]); // an empty slot: nothing to touch
        assert_eq!(c.evict(None).unwrap().1, 1);
        assert_eq!(c.evict(None).unwrap().1, 3);
        let n = c.insert(7);
        assert!(ids.contains(&n), "a freed slot is reused");
        let mut left: Vec<i32> = (0..2).map(|_| c.evict(None).unwrap().1).collect();
        left.sort();
        assert_eq!(left, [4, 7]);
        assert!(c.evict(None).is_none());
    }
}
