//! The reusable storage for whole quota decisions.
//!
//! Layer: primitives.
//! - **Owns.** Stable slot addresses and nonrepeating publication keys.
//! - **Depends on.** Selected atomics and the data of a quota decision.
//! - **Must not know.** Which decision an updater made or why.

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    publication::ArcSwap,
    sync::{
        StdArc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
};

use super::State;

// One block covers the benchmark's maximum producer and reader count together. Additional
// blocks grow the index in the same units and keep every published slot at a stable address.
const SLOTS_PER_BLOCK: usize = 64;
// The budget word's top bit identifies a decision represented directly in that word.
const GENERATION_LIMIT: u32 = u32::MAX >> 1;

#[derive(Clone, Copy)]
pub(super) struct Key {
    index: u32,
    generation: u32,
}

impl Key {
    pub(super) const INITIAL: Self = Self {
        index: 0,
        generation: 0,
    };

    pub(super) fn from_bits(bits: u64) -> Self {
        let [a, b, c, d, e, f, g, h] = bits.to_le_bytes();
        Self {
            index: u32::from_le_bytes([a, b, c, d]),
            generation: u32::from_le_bytes([e, f, g, h]),
        }
    }

    pub(super) fn bits(self) -> u64 {
        let [a, b, c, d] = self.index.to_le_bytes();
        let [e, f, g, h] = self.generation.to_le_bytes();
        u64::from_le_bytes([a, b, c, d, e, f, g, h])
    }

    pub(super) fn successor(self) -> Option<Self> {
        if self.generation == GENERATION_LIMIT {
            return None;
        }
        let generation = self.generation.checked_add(1)?;
        Some(Self { generation, ..self })
    }
}

struct Slot {
    available: AtomicBool,
    generation: AtomicU32,
    limit: AtomicU64,
    used: AtomicU64,
    waiting: AtomicUsize,
    closed: AtomicBool,
    readers: AtomicBool,
}

impl Slot {
    fn new(state: State) -> Self {
        Self {
            available: AtomicBool::new(false),
            generation: AtomicU32::new(0),
            limit: AtomicU64::new(state.limit.get()),
            used: AtomicU64::new(state.used),
            waiting: AtomicUsize::new(state.waiting),
            closed: AtomicBool::new(state.closed),
            readers: AtomicBool::new(state.readers),
        }
    }

    fn read(&self) -> State {
        // Seeing a later incarnation's field acquires the retirement of the observed key.
        // The subsequent key check then rejects the mixed observation.
        State {
            limit: NonZeroU64::new(self.limit.load(Ordering::Acquire))
                .assured("every incarnation of a slot has a positive limit"),
            used: self.used.load(Ordering::Acquire),
            waiting: self.waiting.load(Ordering::Acquire),
            closed: self.closed.load(Ordering::Acquire),
            readers: self.readers.load(Ordering::Acquire),
        }
    }

    fn write(&self, state: State) {
        self.limit.store(state.limit.get(), Ordering::Release);
        self.used.store(state.used, Ordering::Release);
        self.waiting.store(state.waiting, Ordering::Release);
        self.closed.store(state.closed, Ordering::Release);
        self.readers.store(state.readers, Ordering::Release);
    }
}

pub(super) struct Slots {
    inline: Block,
    overflow: ArcSwap<Vec<StdArc<Block>>>,
    allocated: AtomicU32,
}

type Block = [Slot; SLOTS_PER_BLOCK];

impl Slots {
    pub(super) fn new(initial: State) -> Self {
        Self {
            inline: std::array::from_fn(|_| Slot::new(initial)),
            overflow: ArcSwap::from_pointee(Vec::new()),
            allocated: AtomicU32::new(1),
        }
    }

    pub(super) fn read(&self, key: Key) -> State {
        self.with_slot(key.index, Slot::read)
            .assured("storage has been initialized before publishing its key")
    }

    pub(super) fn write(&self, key: Key, state: State) {
        self.with_slot(key.index, |slot| slot.write(state))
            .assured("a writer owns this already allocated slot");
    }

    pub(super) fn claim(&self) -> Key {
        let high_water = self.allocated.load(Ordering::Acquire);
        for index in 0..high_water {
            let claimed = self.with_slot(index, |slot| {
                if slot.available.load(Ordering::Relaxed)
                    && slot
                        .available
                        .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                {
                    return Some(slot.generation.load(Ordering::Relaxed));
                }
                None
            });
            if let Some(Some(generation)) = claimed {
                return Key { index, generation };
            }
        }

        let index = self.allocate_index(high_water);
        self.ensure_allocated(index);
        Key {
            index,
            generation: 0,
        }
    }

    fn allocate_index(&self, mut observed: u32) -> u32 {
        loop {
            let count = observed
                .checked_add(1)
                .assured("addressable budget publication slots exhausted");
            match self.allocated.compare_exchange_weak(
                observed,
                count,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return observed,
                Err(current) => observed = current,
            }
        }
    }

    pub(super) fn return_spare(&self, key: Key) {
        self.with_slot(key.index, |slot| {
            slot.generation.store(key.generation, Ordering::Relaxed);
            slot.available.store(true, Ordering::Release);
        })
        .assured("the recycled slot was allocated");
    }

    fn with_slot<R>(&self, index: u32, access: impl FnOnce(&Slot) -> R) -> Option<R> {
        let (block, offset) = Self::location(index);
        if let Some(block) = block {
            let index = self.overflow.load();
            return index.get(block).map(|slots| access(&slots[offset]));
        }
        Some(access(&self.inline[offset]))
    }

    fn ensure_allocated(&self, index: u32) {
        let Some(block) = Self::location(index).0 else {
            return;
        };
        let needed = block
            .checked_add(1)
            .assured("the index fits its block list");
        if self.overflow.load().len() >= needed {
            return;
        }
        self.overflow.rcu(|index| {
            if index.len() >= needed {
                return index.clone();
            }
            let mut grown = (**index).clone();
            let initial = State::new(NonZeroU64::MIN);
            grown.resize_with(needed, || {
                StdArc::new(std::array::from_fn(|_| Slot::new(initial)))
            });
            StdArc::new(grown)
        });
    }

    fn location(index: u32) -> (Option<usize>, usize) {
        let index = usize::try_from(index).assured("Nervix requires at least a 32-bit address");
        (
            (index / SLOTS_PER_BLOCK).checked_sub(1),
            index % SLOTS_PER_BLOCK,
        )
    }
}

#[cfg(test)]
#[cfg(not(any(feature = "loom", feature = "shuttle")))]
mod tests {
    use std::num::NonZeroU64;

    use meticulous::{OptionExt as _, ResultExt as _};

    use super::{GENERATION_LIMIT, Key, SLOTS_PER_BLOCK, Slots, State};

    #[test]
    fn slots_can_be_claimed_and_recycled_beyond_the_inline_block() {
        let initial = State::new(NonZeroU64::MAX);
        let slots = Slots::new(initial);
        let mut keys = Vec::new();
        for index in 1..=SLOTS_PER_BLOCK {
            let key = slots.claim();
            assert_eq!(usize::try_from(key.index), Ok(index));
            keys.push(key);
        }
        let overflow = *keys
            .last()
            .assured("the test claimed every inline slot and one more");
        let expected = State {
            used: 5,
            waiting: 3,
            closed: true,
            readers: false,
            ..initial
        };
        slots.write(overflow, expected);
        assert_eq!(slots.read(overflow), expected);
        let reuse = overflow
            .successor()
            .assured("this slot has only been used once");
        slots.return_spare(reuse);
        assert_eq!(slots.claim().bits(), reuse.bits());
        assert_ne!(slots.claim().bits(), reuse.bits());
        assert_eq!(slots.read(reuse), expected);
    }

    #[test]
    fn publication_keys_round_trip_and_retire_without_wrapping() {
        let last = Key {
            index: u32::MAX,
            generation: GENERATION_LIMIT,
        };
        let earlier = Key {
            generation: GENERATION_LIMIT - 1,
            ..last
        };
        let decoded = Key::from_bits(last.bits());
        assert!(
            last.bits() < (1 << 63),
            "slot keys leave room for the packed marker"
        );
        assert_eq!(decoded.index, last.index);
        assert_eq!(decoded.generation, last.generation);
        assert_eq!(earlier.successor().map(Key::bits), Some(last.bits()));
        assert!(last.successor().is_none());
    }

    #[test]
    fn addresses_distinguish_inline_slots_from_each_overflow_block() {
        let first = u32::try_from(SLOTS_PER_BLOCK).assured("the block fits its index");
        assert_eq!(Slots::location(1), (None, 1));
        assert_eq!(Slots::location(first), (Some(0), 0));
        assert_eq!(Slots::location(first + 2), (Some(0), 2));
        assert_eq!(Slots::location(first * 2), (Some(1), 0));
    }
}
