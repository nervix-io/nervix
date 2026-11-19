//! The ordered log for one set of readers.
//!
//! Layer: primitives.
//! - **Owns.** Tail-first publication, reader positions, and entry reclamation.
//! - **Depends on.** Selected atomics and shared ownership from the primitive boundary.
//! - **Must not know.** How a reader joins the set or how a writer obtains a reservation.

#[cfg(test)]
#[cfg(feature = "loom")]
#[path = "log/loom_models.rs"]
mod loom_models;

#[cfg(test)]
#[cfg(feature = "shuttle")]
#[path = "log/shuttle_checks.rs"]
mod shuttle_checks;

#[cfg(test)]
#[cfg(not(any(feature = "loom", feature = "shuttle")))]
#[path = "log/tests.rs"]
mod tests;

use std::{
    cell::UnsafeCell,
    collections::{BTreeMap, VecDeque},
    marker::PhantomData,
    num::{NonZeroU64, NonZeroUsize},
    ptr::{self, NonNull},
};

use meticulous::OptionExt as _;
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

use crate::channel::{SavedEntry, Standstill};

// The allocation and reuse scan are amortized over this many entries. A writer owns
// the positions in its active block, so it can prepare a slot without modifying a shared cursor.
const ENTRIES_PER_BLOCK: usize = 32;

pub(super) struct Entry<T> {
    pub(super) item: T,
    pub(super) weight: NonZeroU64,
}

struct Slot<T> {
    next: AtomicPtr<Self>,
    remaining: AtomicUsize,
    available: AtomicBool,
    entry: UnsafeCell<Option<Entry<T>>>,
}

// SAFETY: the exclusively owning writer initializes a vacant slot. After publication, the
// designated readers borrow its immutable entry. Only the last reader may take the entry, and
// access to the slot ends before the last reader of its successor returns it to its writer.
unsafe impl<T: Send + Sync> Sync for Slot<T> {}

impl<T> Slot<T> {
    fn empty() -> Self {
        Self {
            next: AtomicPtr::new(ptr::null_mut()),
            remaining: AtomicUsize::new(0),
            available: AtomicBool::new(true),
            entry: UnsafeCell::new(None),
        }
    }

    fn prepare(&self, entry: Entry<T>, readers: NonZeroUsize) {
        self.available.store(false, Ordering::Relaxed);
        self.next.store(ptr::null_mut(), Ordering::Relaxed);
        self.remaining.store(readers.get(), Ordering::Relaxed);
        // SAFETY: the writer owns the slot and acquired its previous release before reuse.
        unsafe { self.entry.get().write(Some(entry)) };
    }

    fn release(&self) {
        // The final advancement ended all references to this slot in its previous incarnation.
        self.available.store(true, Ordering::Release);
    }
}

struct Block<T> {
    next: AtomicPtr<Self>,
    owned: AtomicBool,
    slots: [Slot<T>; ENTRIES_PER_BLOCK],
}

impl<T> Block<T> {
    fn new() -> Self {
        Self {
            next: AtomicPtr::new(ptr::null_mut()),
            owned: AtomicBool::new(true),
            slots: std::array::from_fn(|_| Slot::empty()),
        }
    }

    fn available(&self) -> bool {
        self.slots
            .iter()
            .all(|slot| slot.available.load(Ordering::Acquire))
    }
}

/// Owns the backing allocations. Returning a slot makes it immediately reusable. Allocated
/// blocks stay at a stable address until the arena is destroyed.
pub(super) struct Arena<T> {
    blocks: AtomicPtr<Block<T>>,
    allocations: PhantomData<Box<Block<T>>>,
}

impl<T> Arena<T> {
    pub(super) fn new() -> Self {
        Self {
            blocks: AtomicPtr::new(ptr::null_mut()),
            allocations: PhantomData,
        }
    }

    fn claim_block(&self) -> NonNull<Block<T>> {
        // This slow allocation path visits at most the blocks allocated by the channel so far.
        // A block returned by a writer can still contain entries held by readers.
        let mut observed = self.blocks.load(Ordering::Acquire);
        let mut candidate = observed;
        while let Some(block) = NonNull::new(candidate) {
            // SAFETY: published blocks remain allocated for the life of the arena.
            let block_ref = unsafe { block.as_ref() };
            if !block_ref.owned.load(Ordering::Relaxed)
                && block_ref
                    .owned
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            {
                if block_ref.available() {
                    return block;
                }
                block_ref.owned.store(false, Ordering::Release);
            }
            candidate = block_ref.next.load(Ordering::Acquire);
        }

        let block = NonNull::from(Box::leak(Box::new(Block::new())));
        // SAFETY: the allocation is initialized and private until the first successful CAS.
        let block_ref = unsafe { block.as_ref() };
        loop {
            block_ref.next.store(observed, Ordering::Relaxed);
            match self.blocks.compare_exchange_weak(
                observed,
                block.as_ptr(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return block,
                Err(current) => observed = current,
            }
        }
    }
}

impl<T> Drop for Arena<T> {
    fn drop(&mut self) {
        let mut next = self.blocks.load(Ordering::Acquire);
        while !next.is_null() {
            // SAFETY: every block was allocated as a Box, joined this owning list once, and
            // is inaccessible to publishers or readers after the last arena owner leaves.
            let block = unsafe { Box::from_raw(next) };
            next = block.next.load(Ordering::Acquire);
        }
    }
}

pub(super) struct Writer<T> {
    arena: Arc<Arena<T>>,
    blocks: VecDeque<NonNull<Block<T>>>,
    offset: usize,
}

// SAFETY: a writer owns the allocation cursor and its block claims. Transferring that ownership
// leaves the block storage stable under the shared arena.
unsafe impl<T: Send + Sync> Send for Writer<T> {}

impl<T> Writer<T> {
    pub(super) fn new(arena: Arc<Arena<T>>) -> Self {
        // A sender starts with reusable space. In particular, a sender waking with its first
        // reservation can publish without allocating storage on the handoff path.
        let blocks = VecDeque::from([arena.claim_block()]);
        Self {
            arena,
            blocks,
            offset: 0,
        }
    }

    fn claim_slot(&mut self, entry: Entry<T>, readers: NonZeroUsize) -> NonNull<Slot<T>> {
        if self.offset == ENTRIES_PER_BLOCK {
            self.advance_block();
        }
        let block = self
            .blocks
            .back()
            .assured("the writer has selected an owned block");
        // SAFETY: this block stays allocated in the arena, and this writer owns its vacant slots.
        let slot = &unsafe { block.as_ref() }.slots[self.offset];
        self.offset = self
            .offset
            .checked_add(1)
            .assured("the offset points within a fixed-size block");
        slot.prepare(entry, readers);
        NonNull::from(slot)
    }

    fn advance_block(&mut self) {
        if let Some(block) = self.blocks.front() {
            // SAFETY: the arena keeps the block live, and the writer owns its reuse claim.
            if unsafe { block.as_ref() }.available() {
                let reusable = self
                    .blocks
                    .pop_front()
                    .verified("the front block was checked above");
                self.blocks.push_back(reusable);
                self.offset = 0;
                return;
            }
        }
        self.blocks.push_back(self.arena.claim_block());
        self.offset = 0;
    }
}

impl<T> Drop for Writer<T> {
    fn drop(&mut self) {
        for block in self.blocks.drain(..) {
            // SAFETY: the arena still owns every block, and this is its writer's final access.
            unsafe { block.as_ref() }
                .owned
                .store(false, Ordering::Release);
        }
    }
}

// Separate the producer tail and the reader retirement pointer across adjacent cache lines.
#[repr(align(128))]
struct Tail<T>(AtomicPtr<Slot<T>>);

#[repr(align(128))]
struct Retained<T>(AtomicPtr<Slot<T>>);

/// Every designated reader begins at the same sentinel and advances exactly once per item. A
/// departing reader must advance through this log's final item before discarding its position.
pub(super) struct Log<T> {
    arena: Arc<Arena<T>>,
    sentinel: Arc<Slot<T>>,
    tail: Tail<T>,
    first_retained: Retained<T>,
    readers: NonZeroUsize,
    unissued: AtomicUsize,
}

pub(super) struct Cursor<T> {
    log: Arc<Log<T>>,
    position: NonNull<Slot<T>>,
}

// SAFETY: only the owner of a cursor advances it. Its unspent reader share prevents the
// preceding slot from being reused until the cursor moves past it.
unsafe impl<T: Send + Sync> Send for Cursor<T> {}

pub(super) enum Read<T> {
    Item(ReadItem<T>),
    Empty,
    Publishing,
}

pub(super) struct ReadItem<T> {
    pub(super) item: Option<T>,
    // Returning the bytes before dropping the original protects the accounting if T::drop panics.
    pub(super) finished: Option<Entry<T>>,
}

pub(super) struct Contents<T> {
    pub(super) entries: Vec<SavedEntry<T>>,
    pub(super) next_by_address: BTreeMap<usize, usize>,
}

impl<T> Log<T> {
    pub(super) fn new(arena: Arc<Arena<T>>, readers: NonZeroUsize) -> Self {
        // The retained pointers share this allocation. Moving the log must not retag it
        // as exclusive ownership after a pointer has been published.
        let sentinel = Arc::new(Slot::empty());
        let pointer = NonNull::from(sentinel.as_ref()).as_ptr();
        Self {
            arena,
            sentinel,
            tail: Tail(AtomicPtr::new(pointer)),
            first_retained: Retained(AtomicPtr::new(pointer)),
            readers,
            unissued: AtomicUsize::new(readers.get()),
        }
    }

    /// The reserved reader shares keep the sentinel alive until each issued cursor passes it.
    pub(super) fn start(log: &Arc<Self>) -> Option<Cursor<T>> {
        let mut observed = log.unissued.load(Ordering::Relaxed);
        loop {
            let unissued = observed.checked_sub(1)?;
            match log.unissued.compare_exchange_weak(
                observed,
                unissued,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(Cursor {
                        log: log.clone(),
                        position: NonNull::from(log.sentinel.as_ref()),
                    });
                }
                Err(current) => observed = current,
            }
        }
    }

    pub(super) fn append(&self, writer: &mut Writer<T>, entry: Entry<T>) {
        assert!(
            Arc::ptr_eq(&self.arena, &writer.arena),
            "the writer and log use the same slot arena"
        );
        let next = writer.claim_slot(entry, self.readers);
        let previous = self.exchange(next);
        self.link(previous, next);
    }

    fn exchange(&self, next: NonNull<Slot<T>>) -> *mut Slot<T> {
        self.tail.0.swap(next.as_ptr(), Ordering::AcqRel)
    }

    fn link(&self, previous: *mut Slot<T>, next: NonNull<Slot<T>>) {
        // SAFETY: the exchange gave this producer the predecessor whose successor it alone
        // writes. A reader cannot advance past it until this store, so the predecessor stays live
        // throughout the store, even if another producer has already exchanged the tail again.
        unsafe { &*previous }
            .next
            .store(next.as_ptr(), Ordering::Release);
    }
}

impl<T: Clone> Cursor<T> {
    pub(super) fn try_read(&mut self, copy: bool) -> Read<T> {
        // SAFETY: the cursor's reader share in its successor keeps this predecessor live.
        let from = unsafe { self.position.as_ref() };
        let next = from.next.load(Ordering::Acquire);
        let Some(next) = NonNull::new(next) else {
            if self.log.tail.0.load(Ordering::Acquire) == self.position.as_ptr() {
                return Read::Empty;
            }
            return Read::Publishing;
        };
        // SAFETY: acquiring the predecessor's link publishes the whole entry. This reader's
        // share keeps the entry live until it has finished reading it.
        let slot = unsafe { next.as_ref() };
        let item = if copy {
            // SAFETY: every reader's shared access ends before releasing its count.
            let entry = unsafe { &*slot.entry.get() };
            let entry = entry
                .as_ref()
                .assured("the entry is still held by this reader's share");
            Some(entry.item.clone())
        } else {
            None
        };
        self.position = next;
        let last = self.log.readers == NonZeroUsize::MIN
            || slot.remaining.fetch_sub(1, Ordering::AcqRel) == 1;
        let finished = if last {
            // SAFETY: the last share acquires every other reader's release. The single-reader
            // path has exclusive access by construction. No reader borrows the entry now.
            let entry = unsafe { (&mut *slot.entry.get()).take() };
            // Every reader passed this predecessor, and the producer released its final use
            // through the forward link. The next slot stays anchored for subsequent reads.
            self.log
                .first_retained
                .0
                .store(next.as_ptr(), Ordering::Release);
            from.release();
            entry
        } else {
            None
        };
        Read::Item(ReadItem { item, finished })
    }
}

impl<T> Cursor<T> {
    pub(super) fn saved_address(&self, _proof: &Standstill) -> usize {
        self.position.as_ptr().addr()
    }
}

impl<T: Clone> Log<T> {
    pub(super) fn contents(&self, _proof: &Standstill) -> Contents<T> {
        let mut entries = Vec::new();
        let mut next_by_address = BTreeMap::new();
        let mut next = self.first_retained.0.load(Ordering::Acquire);
        while let Some(address) = NonNull::new(next) {
            // SAFETY: the exclusive controller acquires all handle releases before inspecting.
            // No writer, reader, or reclamation can access the chain while this view exists.
            let slot = unsafe { address.as_ref() };
            // SAFETY: the same standstill excludes changes to the entry during this clone.
            let value = unsafe { &*slot.entry.get() };
            if let Some(entry) = value {
                let remaining = NonZeroUsize::new(slot.remaining.load(Ordering::Relaxed))
                    .assured("an entry remains only while it has a reader");
                entries.push(SavedEntry {
                    item: entry.item.clone(),
                    weight: entry.weight,
                    remaining,
                });
            }
            next_by_address.insert(next.addr(), entries.len());
            next = slot.next.load(Ordering::Acquire);
        }
        Contents {
            entries,
            next_by_address,
        }
    }
}

impl<T> Drop for Log<T> {
    fn drop(&mut self) {
        let mut next = self.first_retained.0.load(Ordering::Acquire);
        while !next.is_null() {
            // SAFETY: no participants own a live cursor or producer reference to this log.
            // This follows only its retained chain, whose backing allocations the arena owns.
            let slot = unsafe { &*next };
            next = slot.next.load(Ordering::Acquire);
            // SAFETY: this is the final access to the slot in this log.
            let entry = unsafe { (&mut *slot.entry.get()).take() };
            slot.release();
            drop(entry);
        }
    }
}
