//! Allocation ownership across writer and log lifetimes.
//!
//! Layer: test harness.
//! - **Owns.** Reuse and destruction assertions for the arena.
//! - **Depends on.** The production log and writer.
//! - **Must not know.** How senders obtain charges.

use std::num::{NonZeroU64, NonZeroUsize};

use meticulous::OptionExt as _;
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::{Arena, ENTRIES_PER_BLOCK, Entry, Log, Read, Writer};

struct CountDrop(Arc<AtomicUsize>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn a_writer_reuses_returned_blocks_and_keeps_each_claim_exclusive() {
    let arena = Arc::new(Arena::new());
    let writer = Writer::<u8>::new(arena.clone());
    let original = writer.blocks[0];
    drop(writer);
    let second = Writer::new(arena.clone());
    assert_eq!(second.blocks[0], original);
    let third = Writer::new(arena);
    assert_ne!(third.blocks[0], original);
}

#[test]
fn draining_slots_allows_the_writer_to_reuse_its_own_blocks() {
    let arena = Arc::new(Arena::new());
    let mut writer = Writer::new(arena.clone());
    let log = Arc::new(Log::new(arena, NonZeroUsize::MIN));
    let mut reader = Log::start(&log).assured("one reader owns the first cursor");
    for item in 0..ENTRIES_PER_BLOCK * 3 {
        log.append(
            &mut writer,
            Entry {
                item,
                weight: NonZeroU64::MIN,
            },
        );
        let Read::Item(accepted) = reader.try_read(true) else {
            panic!("the reader must find the linked item");
        };
        assert_eq!(accepted.item, Some(item));
        assert!(accepted.finished.is_some());
    }
    assert_eq!(writer.blocks.len(), 2);
}

#[test]
fn dropping_a_log_releases_its_entries_while_the_arena_is_still_live() {
    let arena = Arc::new(Arena::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let mut writer = Writer::new(arena.clone());
    let log = Log::new(arena, NonZeroUsize::MIN);
    log.append(
        &mut writer,
        Entry {
            item: CountDrop(drops.clone()),
            weight: NonZeroU64::MIN,
        },
    );
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(log);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn the_arena_owns_even_an_initialized_slot_that_was_never_linked() {
    let arena = Arc::new(Arena::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let mut writer = Writer::new(arena.clone());
    writer.claim_slot(
        Entry {
            item: CountDrop(drops.clone()),
            weight: NonZeroU64::MIN,
        },
        NonZeroUsize::MIN,
    );
    drop(writer);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(arena);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}
