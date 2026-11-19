//! Export and restoration while the channel stands still.
//!
//! Layer: primitives.
//! - **Owns.** Exclusive inspection and validation of positions, weights and membership.
//! - **Depends on.** The control owner and its ordered log.
//! - **Must not know.** How the caller persists or transports an exported value.

use std::{
    collections::BTreeMap,
    num::{NonZeroU64, NonZeroUsize},
};

use meticulous::OptionExt as _;
use nervix_primitives::sync::{Arc, atomic::Ordering};

use super::{Channel, Standstill};
use crate::{
    generation::{Enter, Next, Position, ReaderSignal},
    log::{Entry, Writer},
    member::Member,
};

#[derive(Debug, PartialEq, Eq)]
pub struct SavedEntry<T> {
    pub item: T,
    pub weight: NonZeroU64,
    pub remaining: NonZeroUsize,
}

/// Positions index the next entry each reader needs. An index equal to the length is at
/// the end. Entries retain their original charges even after a capacity decrease.
#[derive(Debug, PartialEq, Eq)]
pub struct Snapshot<T, K> {
    pub capacity: NonZeroU64,
    pub closed: bool,
    pub entries: Vec<SavedEntry<T>>,
    pub positions: BTreeMap<K, usize>,
}

pub enum Capture<T, K> {
    Saved(Snapshot<T, K>),
    Busy,
}

pub enum Restore<T, K> {
    Restored(Channel<T, K>),
    InvalidPosition,
    InvalidRemaining,
    WeightOverflow,
}

impl<T: Clone, K: Clone + Ord> Channel<T, K> {
    /// Captures the complete content and every subscribed reader's position. The owner
    /// first releases pending senders with stop_reservations and drops or suspends all handles.
    pub fn capture(&mut self) -> Capture<T, K> {
        // Every handle releases the count after returning its final position and ending
        // publication. A read-modify-write acquires all those releases on the count. The
        // exclusive controller is the only way to create a new handle.
        if self.shared.access.fetch_or(0, Ordering::AcqRel) != 0 {
            return Capture::Busy;
        }
        self.finish_retirements();
        let proof = Standstill(());
        let mut entries = Vec::new();
        let mut positions = BTreeMap::new();
        let mut by_generation = BTreeMap::<u64, Vec<(&K, &Position<T>)>>::new();
        for (key, member) in &self.members {
            let position = member
                .at_standstill(&proof)
                .assured("every suspended reader returned its position before releasing access");
            by_generation
                .entry(position.generation.number)
                .or_default()
                .push((key, position));
        }
        if let Some((_, readers)) = by_generation.first_key_value() {
            let first = readers
                .first()
                .assured("a generation was indexed by its reader");
            let mut next = Some(first.1.generation.clone());
            while let Some(generation) = next {
                let base = entries.len();
                if let Some(content) = generation.contents(&proof) {
                    if let Some(readers) = by_generation.remove(&generation.number) {
                        for (key, position) in readers {
                            let address = position.cursor.saved_address(&proof);
                            let local = content
                                .next_by_address
                                .get(&address)
                                .assured("the reader's next share retains its cursor");
                            let index = base
                                .checked_add(*local)
                                .assured("the index fits in the captured entries");
                            positions.insert(key.clone(), index);
                        }
                    }
                    entries.extend(content.entries);
                }
                next = generation.successor();
            }
        }
        assert!(
            by_generation.is_empty(),
            "every position belongs to the generation chain"
        );
        Capture::Saved(Snapshot {
            capacity: self.capacity(),
            closed: self.closed,
            entries,
            positions,
        })
    }

    fn finish_retirements(&mut self) {
        let retiring = std::mem::take(&mut self.leaving);
        for mut reader in retiring.into_values() {
            loop {
                match reader.position.next(false, Some(reader.through)) {
                    Next::Ended => break,
                    Next::Item(item) => {
                        if let Some(finished) = item.finished {
                            self.shared
                                .waiting
                                .release(&mut self.budget, finished.weight);
                            drop(finished);
                        }
                    }
                    Next::Empty | Next::Publishing => {
                        unreachable!("no live appender remains in the sealed retirement range");
                    }
                }
            }
        }
    }

    pub fn restore(snapshot: Snapshot<T, K>) -> Restore<T, K> {
        let used = match snapshot.check() {
            Checked::Used(used) => used,
            Checked::InvalidPosition => return Restore::InvalidPosition,
            Checked::InvalidRemaining => return Restore::InvalidRemaining,
            Checked::WeightOverflow => return Restore::WeightOverflow,
        };
        let mut channel = Self::with_charge(snapshot.capacity, used);
        let readers: Vec<_> = snapshot
            .positions
            .into_iter()
            .map(|(key, index)| RestoredReader {
                key,
                index,
                signal: ReaderSignal::new(),
            })
            .collect();
        if !readers.is_empty() {
            let signals = readers.iter().map(|reader| reader.signal.clone()).collect();
            let generation = channel.install(signals);
            let mut writer = Writer::new(channel.shared.arena.clone());
            let Enter::Live(append) = generation.publication.try_enter() else {
                unreachable!("the restored generation is open and has a reader");
            };
            for saved in snapshot.entries {
                append.publish(
                    &mut writer,
                    Entry {
                        item: saved.item,
                        weight: saved.weight,
                    },
                );
            }
            drop(append);
            for saved in readers {
                let mut position = Position::new(generation.clone());
                for _ in 0..saved.index {
                    let Next::Item(item) = position.next(false, None) else {
                        unreachable!("the validated position lies within the restored log");
                    };
                    assert!(
                        item.finished.is_none(),
                        "the validated entries all retain a reader"
                    );
                }
                channel
                    .members
                    .insert(saved.key, Member::new(saved.signal, position));
            }
        }
        if snapshot.closed {
            channel.close();
        }
        Restore::Restored(channel)
    }
}

struct RestoredReader<K> {
    key: K,
    index: usize,
    signal: Arc<ReaderSignal>,
}

enum Checked {
    Used(u64),
    InvalidPosition,
    InvalidRemaining,
    WeightOverflow,
}

impl<T, K> Snapshot<T, K> {
    fn check(&self) -> Checked {
        let length = self
            .entries
            .len()
            .checked_add(1)
            .assured("the vector's element size bounds its length");
        let mut starting_here = vec![0_usize; length];
        for position in self.positions.values() {
            let Some(count) = starting_here.get_mut(*position) else {
                return Checked::InvalidPosition;
            };
            *count = count
                .checked_add(1)
                .assured("each reader owns a distinct map entry");
        }
        let mut readers = 0_usize;
        let mut used = 0_u64;
        for (entry, starts) in self.entries.iter().zip(starting_here) {
            readers = readers
                .checked_add(starts)
                .assured("each reader starts once");
            if readers != entry.remaining.get() {
                return Checked::InvalidRemaining;
            }
            let Some(next) = used.checked_add(entry.weight.get()) else {
                return Checked::WeightOverflow;
            };
            used = next;
        }
        Checked::Used(used)
    }
}
