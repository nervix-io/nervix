//! Membership boundaries in the ordered log.
//!
//! Layer: primitives.
//! - **Owns.** The readers covered by a log, the end of its publication period, and waiting
//!   readers' delivery notifications.
//! - **Depends on.** The underlying log and selected publication and waker primitives.
//! - **Must not know.** Reader identities or the budget associated with an item.

#[cfg(test)]
#[cfg(feature = "loom")]
#[path = "generation/loom_models.rs"]
mod loom_models;

use std::{
    num::NonZeroUsize,
    task::{Context, Poll, Waker},
};

use meticulous::OptionExt as _;
use nervix_primitives::{
    publication::ArcSwapOption,
    sync::{
        Arc, AtomicWaker, StdArc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    channel::Standstill,
    log::{Arena, Contents, Cursor, Entry, Log, Read, Writer},
};

// The same word records the seal, active appenders and sleeping readers. The lower word
// counts appenders and the upper word counts sleeping readers, with its top bit for the seal.
// Both counts cover billions of independent handles even on a 32-bit host.
const SEALED: u64 = 1 << (u64::BITS - 1);
const WAITER: u64 = 1 << u32::BITS;
const ACTIVE_MASK: u64 = WAITER - 1;
const WAITING_MASK: u64 = SEALED - WAITER;

pub(super) struct Generation<T> {
    pub(super) number: u64,
    pub(super) publication: StdArc<Publication<T>>,
    next: ArcSwapOption<Self>,
}

/// Senders retain the publication they last used. A publication owns just its own log.
/// The links between generations belong to the readers and controller.
pub(super) struct Publication<T> {
    log: Option<Arc<Log<T>>>,
    readers: Box<[Arc<ReaderSignal>]>,
    activity: AtomicU64,
}

pub(super) struct ReaderSignal {
    pub(super) waker: AtomicWaker,
}

impl ReaderSignal {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            waker: AtomicWaker::new(),
        })
    }
}

impl<T> Generation<T> {
    pub(super) fn new(
        number: u64,
        arena: &Arc<Arena<T>>,
        readers: Box<[Arc<ReaderSignal>]>,
    ) -> StdArc<Self> {
        let log =
            NonZeroUsize::new(readers.len()).map(|count| Arc::new(Log::new(arena.clone(), count)));
        StdArc::new(Self {
            number,
            publication: StdArc::new(Publication {
                log,
                readers,
                activity: AtomicU64::new(0),
            }),
            next: ArcSwapOption::empty(),
        })
    }

    pub(super) fn seal(&self, next: Option<StdArc<Self>>) {
        self.next.store(next);
        let published = &self.publication;
        let previous = published.activity.fetch_or(SEALED, Ordering::AcqRel);
        published.wake_waiting(previous);
    }

    pub(super) fn successor(&self) -> Option<StdArc<Self>> {
        self.next.load_full()
    }

    fn cursor(&self) -> Cursor<T> {
        let log = self
            .publication
            .log
            .as_ref()
            .assured("the reader belongs to this generation");
        Log::start(log).assured("the generation includes an unissued position for this reader")
    }
}

impl<T> Publication<T> {
    fn has_finished(&self) -> bool {
        self.activity.load(Ordering::Acquire) & !WAITING_MASK == SEALED
    }

    pub(super) fn try_enter(&self) -> Enter<'_, T> {
        let Some(log) = &self.log else {
            if self.activity.load(Ordering::Acquire) & SEALED != 0 {
                return Enter::Sealed;
            }
            return Enter::NoReaders;
        };
        let previous = self.activity.fetch_add(1, Ordering::AcqRel);
        assert_ne!(
            previous & ACTIVE_MASK,
            ACTIVE_MASK,
            "every publication has a distinct writer"
        );
        if previous & SEALED != 0 {
            self.end();
            return Enter::Sealed;
        }
        Enter::Live(Append {
            publication: self,
            log,
        })
    }

    fn end(&self) -> bool {
        let previous = self.activity.fetch_sub(1, Ordering::AcqRel);
        assert_ne!(
            previous & ACTIVE_MASK,
            0,
            "the lease raised the active count"
        );
        self.wake_waiting(previous)
    }

    fn wake_waiting(&self, previous: u64) -> bool {
        let waiting = previous & WAITING_MASK != 0;
        if waiting {
            for reader in &self.readers {
                reader.waker.wake();
            }
        }
        waiting
    }

    fn watch<'a>(
        publication: &StdArc<Self>,
        number: u64,
        reader: &'a ReaderSignal,
        waker: &Waker,
    ) -> Watch<'a, T> {
        reader.waker.register(waker);
        publication.arm_wait();
        Watch {
            publication: publication.clone(),
            number,
            reader,
        }
    }

    fn arm_wait(&self) {
        // The same read-modify-write both registers the waiter and acquires a completed
        // append, if any. The caller then rechecks the log.
        let previous = self.activity.fetch_add(WAITER, Ordering::AcqRel);
        assert_ne!(
            previous & WAITING_MASK,
            WAITING_MASK,
            "each waiter owns its own reader"
        );
    }

    fn disarm_wait(&self) {
        let previous = self.activity.fetch_sub(WAITER, Ordering::Relaxed);
        assert_ne!(previous & WAITING_MASK, 0, "this watch registered once");
    }
}

impl<T: Clone> Generation<T> {
    pub(super) fn contents(&self, proof: &Standstill) -> Option<Contents<T>> {
        self.publication.log.as_ref().map(|log| log.contents(proof))
    }
}

pub(super) enum Enter<'a, T> {
    Live(Append<'a, T>),
    Sealed,
    NoReaders,
}

pub(super) struct Append<'a, T> {
    publication: &'a Publication<T>,
    log: &'a Log<T>,
}

impl<T> Append<'_, T> {
    pub(super) fn publish(&self, writer: &mut Writer<T>, entry: Entry<T>) {
        self.log.append(writer, entry);
    }

    #[cfg(test)]
    #[cfg(feature = "shuttle")]
    pub(super) fn log(&self) -> &Log<T> {
        self.log
    }
}

impl<T> Drop for Append<'_, T> {
    fn drop(&mut self) {
        self.publication.end();
    }
}

pub(super) struct Watch<'a, T> {
    publication: StdArc<Publication<T>>,
    number: u64,
    reader: &'a ReaderSignal,
}

impl<T> Drop for Watch<'_, T> {
    fn drop(&mut self) {
        drop(self.reader.waker.take());
        self.publication.disarm_wait();
    }
}

pub(super) struct Position<T> {
    pub(super) generation: StdArc<Generation<T>>,
    pub(super) cursor: Cursor<T>,
}

impl<T> Position<T> {
    pub(super) fn new(generation: StdArc<Generation<T>>) -> Self {
        let cursor = generation.cursor();
        Self { generation, cursor }
    }
}

impl<T: Clone> Position<T> {
    pub(super) fn next(&mut self, copy: bool, stop_after: Option<u64>) -> Next<T> {
        loop {
            match self.cursor.try_read(copy) {
                Read::Item(item) => return Next::Item(item),
                Read::Publishing => return Next::Publishing,
                Read::Empty if !self.generation.publication.has_finished() => return Next::Empty,
                Read::Empty => {
                    // The sealing acquire precedes this recheck. An earlier read may have
                    // raced an appender that left just before the sealing count reached zero.
                    match self.cursor.try_read(copy) {
                        Read::Item(item) => return Next::Item(item),
                        Read::Publishing => return Next::Publishing,
                        Read::Empty => {}
                    }
                }
            }
            if stop_after == Some(self.generation.number) {
                return Next::Ended;
            }
            let Some(next) = self.generation.successor() else {
                return Next::Ended;
            };
            *self = Self::new(next);
        }
    }

    pub(super) fn poll_next<'a>(
        &mut self,
        reader: &'a ReaderSignal,
        watch: &mut Option<Watch<'a, T>>,
        cx: &Context<'_>,
        copy: bool,
        stop_after: Option<u64>,
    ) -> Poll<Option<crate::log::ReadItem<T>>> {
        let mut next = self.next(copy, stop_after);
        loop {
            match next {
                Next::Item(item) => {
                    drop(watch.take());
                    return Poll::Ready(Some(item));
                }
                Next::Ended => {
                    drop(watch.take());
                    return Poll::Ready(None);
                }
                Next::Empty | Next::Publishing => {}
            }
            let number = self.generation.number;
            if let Some(current) = watch
                && current.number == number
            {
                reader.waker.register(cx.waker());
            } else {
                drop(watch.take());
                *watch = Some(Publication::watch(
                    &self.generation.publication,
                    number,
                    reader,
                    cx.waker(),
                ));
            }
            next = self.next(copy, stop_after);
            if let (true, Next::Empty | Next::Publishing) =
                (self.generation.number == number, &next)
            {
                return Poll::Pending;
            }
        }
    }
}

pub(super) enum Next<T> {
    Item(crate::log::ReadItem<T>),
    Empty,
    Publishing,
    Ended,
}

#[cfg(test)]
#[cfg(not(any(feature = "loom", feature = "shuttle")))]
mod tests {
    use std::task::Waker;

    use nervix_primitives::sync::Arc;

    use super::{Arena, Enter, Generation, Publication, ReaderSignal};

    #[test]
    fn a_generation_seals_with_two_live_publications_and_finishes_after_both_leave() {
        let arena = Arc::new(Arena::<u8>::new());
        let generation = Generation::new(0, &arena, Box::new([ReaderSignal::new()]));
        let Enter::Live(first) = generation.publication.try_enter() else {
            panic!("the first publication enters the open generation");
        };
        let Enter::Live(second) = generation.publication.try_enter() else {
            panic!("the second publication enters the open generation");
        };
        generation.seal(None);
        assert!(matches!(generation.publication.try_enter(), Enter::Sealed));
        drop(first);
        assert!(!generation.publication.has_finished());
        drop(second);
        assert!(generation.publication.has_finished());
    }

    #[test]
    fn an_empty_membership_refuses_publication_and_still_observes_the_seal() {
        let arena = Arc::new(Arena::<u8>::new());
        let generation = Generation::new(0, &arena, Box::new([]));
        assert!(matches!(
            generation.publication.try_enter(),
            Enter::NoReaders
        ));
        generation.seal(None);
        assert!(matches!(generation.publication.try_enter(), Enter::Sealed));
    }

    #[test]
    fn dropping_a_watch_removes_its_waiter_from_the_publication_path() {
        let arena = Arc::new(Arena::<u8>::new());
        let signal = ReaderSignal::new();
        let generation = Generation::new(0, &arena, Box::new([signal.clone()]));
        let watch = Publication::watch(&generation.publication, 0, &signal, Waker::noop());
        drop(watch);
        let Enter::Live(append) = generation.publication.try_enter() else {
            panic!("the generation has a reader");
        };
        // Observe the decision made by the same completion called by Append::drop.
        std::mem::forget(append);
        assert!(!generation.publication.end());
    }
}
