//! The control, send and receive entry points of the weighted channel.
//!
//! Layer: primitives.
//! - **Owns.** Reader membership, handle lifetimes, and transfer of reservations to entries.
//! - **Depends on.** The ordered log, reservation owner and selected synchronization.
//! - **Must not know.** Item contents or the meaning of a reader identity.

mod snapshot;

#[cfg(test)]
#[cfg(feature = "loom")]
#[path = "channel/loom_models.rs"]
mod loom_models;

use std::{collections::BTreeMap, future::poll_fn, num::NonZeroU64};

use meticulous::OptionExt as _;
use nervix_primitives::{
    publication::ArcSwap,
    sync::{
        Arc, StdArc,
        atomic::{AtomicUsize, Ordering},
    },
    task::consume_budget,
};

pub use self::snapshot::{Capture, Restore, SavedEntry, Snapshot};
use crate::{
    budget::{self, Admission, Budget, State},
    generation::{Enter, Generation, Next, Position, Publication, ReaderSignal},
    log::{self, Entry},
    member::Member,
    waiters::Waiters,
};

/// The sole owner of membership and capacity changes. Senders and readers can operate on
/// separate threads while the owner changes membership.
pub struct Channel<T, K> {
    shared: Arc<Shared<T>>,
    members: BTreeMap<K, Arc<Member<T>>>,
    leaving: BTreeMap<K, Retirement<T>>,
    budget: budget::Writer,
    closed: bool,
    stopping: bool,
}

struct Shared<T> {
    arena: Arc<log::Arena<T>>,
    budget: Arc<Budget>,
    waiting: Waiters,
    current: ArcSwap<Generation<T>>,
    access: AtomicUsize,
}

impl<T> Shared<T> {
    fn start_access(&self) {
        self.access
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .assured("each active handle occupies independent memory");
    }

    fn end_access(&self) {
        self.access
            .fetch_sub(1, Ordering::AcqRel)
            .checked_sub(1)
            .assured("each dropped handle registered its own access");
    }
}

struct Retirement<T> {
    position: Box<Position<T>>,
    signal: Arc<ReaderSignal>,
    through: u64,
}

impl<T, K: Ord> Channel<T, K> {
    pub fn new(capacity: NonZeroU64) -> Self {
        Self::with_charge(capacity, 0)
    }

    fn with_charge(capacity: NonZeroU64, used: u64) -> Self {
        let arena = Arc::new(log::Arena::new());
        let generation = Generation::new(0, &arena, Box::new([]));
        let budget = Budget::new(State {
            readers: false,
            used,
            ..State::new(capacity)
        });
        Self {
            shared: Arc::new(Shared {
                arena,
                waiting: Waiters::new(budget.clone()),
                budget: budget.clone(),
                current: ArcSwap::from(generation),
                access: AtomicUsize::new(0),
            }),
            members: BTreeMap::new(),
            leaving: BTreeMap::new(),
            budget: Budget::writer(&budget),
            closed: false,
            stopping: false,
        }
    }

    pub fn sender(&self) -> Sender<T> {
        self.shared.start_access();
        Sender {
            shared: self.shared.clone(),
            publication: self.shared.current.load().publication.clone(),
            allocation: log::Writer::new(self.shared.arena.clone()),
            budget: Budget::writer(&self.shared.budget),
        }
    }

    pub fn subscribe(&mut self, key: K) -> ReaderAccess<T> {
        if self.stopping {
            return ReaderAccess::Closed;
        }
        if self.members.contains_key(&key) || self.leaving.contains_key(&key) {
            return ReaderAccess::Duplicate;
        }
        let signal = ReaderSignal::new();
        let readers = self
            .members
            .values()
            .map(|member| member.signal.clone())
            .chain(std::iter::once(signal.clone()))
            .collect();
        let generation = self.install(readers);
        let member = Member::new(signal, Position::new(generation));
        let position = member
            .take()
            .assured("a new reader owns its first position");
        self.members.insert(key, member.clone());
        ReaderAccess::Attached(Reader::new(&self.shared, member, position))
    }

    pub fn attach(&self, key: &K) -> ReaderAccess<T> {
        let Some(member) = self.members.get(key) else {
            return ReaderAccess::Missing;
        };
        let Some(position) = member.take() else {
            return ReaderAccess::Busy;
        };
        ReaderAccess::Attached(Reader::new(&self.shared, member.clone(), position))
    }

    fn install(&mut self, readers: Box<[Arc<ReaderSignal>]>) -> StdArc<Generation<T>> {
        let accepting = !readers.is_empty();
        let previous = self.shared.current.load_full();
        let number = previous
            .number
            .checked_add(1)
            .assured("one channel cannot change membership 2^64 times");
        let current = Generation::new(number, &self.shared.arena, readers);
        previous.seal(Some(current.clone()));
        self.shared.current.store(current.clone());
        self.budget.readers(accepting);
        self.shared.waiting.drive();
        current
    }

    pub fn resize(&mut self, capacity: NonZeroU64) {
        self.budget.resize(capacity);
        self.shared.waiting.drive();
    }

    pub fn capacity(&self) -> NonZeroU64 {
        self.shared.budget.state().limit
    }

    /// Counts both reservations and items whose final reader has yet to take them.
    pub fn charged(&self) -> u64 {
        self.shared.budget.state().used
    }
}

impl<T, K> Channel<T, K> {
    /// Seals publication for a standstill hand-out. Waiting senders get their items back.
    /// An unpublished grant returns its charge on cancellation or a refused publication.
    pub fn stop_reservations(&mut self) {
        if self.stopping {
            return;
        }
        self.stopping = true;
        self.shared.current.load().seal(None);
        self.budget.close();
        self.shared.waiting.drive();
    }

    /// Ends publication and lets readers take the entries accepted before the seal.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.stop_reservations();
    }
}

impl<T, K> Drop for Channel<T, K> {
    fn drop(&mut self) {
        self.close();
    }
}

/// A private witness issued after checking the exclusive owner's active-handle count.
pub(super) struct Standstill(());

impl<T: Clone, K: Clone + Ord> Channel<T, K> {
    /// Finishes a suspended reader's outstanding positions. A cancelled removal keeps its
    /// progress and can be resumed by calling this method with the same identity.
    pub async fn unsubscribe(&mut self, key: &K) -> Leave {
        if !self.leaving.contains_key(key) {
            let Some(member) = self.members.get(key) else {
                return Leave::Missing;
            };
            let Some(position) = member.take() else {
                return Leave::Busy;
            };
            let signal = member.signal.clone();
            let through = self.shared.current.load().number;
            self.leaving.insert(
                key.clone(),
                Retirement {
                    position,
                    signal,
                    through,
                },
            );
            self.members.remove(key);
            if !self.stopping {
                let readers = self
                    .members
                    .values()
                    .map(|member| member.signal.clone())
                    .collect();
                self.install(readers);
            }
        }
        let retirement = self.leaving.get_mut(key).assured("the removal is recorded");
        let mut watch = None;
        loop {
            consume_budget().await;
            let item = poll_fn(|cx| {
                retirement.position.poll_next(
                    &retirement.signal,
                    &mut watch,
                    cx,
                    false,
                    Some(retirement.through),
                )
            })
            .await;
            let Some(item) = item else {
                break;
            };
            if let Some(finished) = item.finished {
                self.shared
                    .waiting
                    .release(&mut self.budget, finished.weight);
                drop(finished);
            }
        }
        drop(watch);
        self.leaving.remove(key);
        Leave::Left
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leave {
    Left,
    Missing,
    Busy,
}

pub enum ReaderAccess<T> {
    Attached(Reader<T>),
    Duplicate,
    Missing,
    Busy,
    Closed,
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    publication: StdArc<Publication<T>>,
    allocation: log::Writer<T>,
    budget: budget::Writer,
}

pub enum Reserve<'a, T> {
    Reserved(Reservation<'a, T>),
    Oversized { limit: NonZeroU64 },
    NoReaders,
    Closed,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Send<T> {
    Published,
    Oversized {
        item: T,
        weight: NonZeroU64,
        limit: NonZeroU64,
    },
    NoReaders(T),
    Closed(T),
}

impl<T> Sender<T> {
    #[cold]
    fn refresh_publication(&mut self) -> bool {
        let current = self.shared.current.load();
        if !StdArc::ptr_eq(&self.publication, &current.publication) {
            self.publication = current.publication.clone();
            return true;
        }
        // The controller installs the successor on the old generation before sealing it.
        // If the shared pointer has yet to move, this edge identifies the replacement.
        let Some(next) = current.successor() else {
            return false;
        };
        self.publication = next.publication.clone();
        true
    }

    #[inline]
    pub async fn reserve(&mut self, weight: NonZeroU64) -> Reserve<'_, T> {
        let mut outcome = self.budget.try_reserve(weight);
        if outcome == Admission::Waiting {
            outcome = self.shared.waiting.wait(&mut self.budget, weight).await;
        }
        match outcome {
            Admission::Granted => Reserve::Reserved(Reservation {
                sender: self,
                outstanding: Some(weight),
            }),
            Admission::Oversized { limit } => Reserve::Oversized { limit },
            Admission::NoReaders => Reserve::NoReaders,
            Admission::Closed => Reserve::Closed,
            Admission::Waiting => unreachable!("the reservation future waits for its decision"),
        }
    }

    #[inline]
    pub async fn send(&mut self, item: T, weight: NonZeroU64) -> Send<T> {
        match self.reserve(weight).await {
            Reserve::Reserved(reservation) => reservation.publish(item),
            Reserve::Oversized { limit } => Send::Oversized {
                item,
                weight,
                limit,
            },
            Reserve::NoReaders => Send::NoReaders(item),
            Reserve::Closed => Send::Closed(item),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared.end_access();
    }
}

/// A grant owns the full charged weight until it publishes the item or is dropped.
pub struct Reservation<'a, T> {
    sender: &'a mut Sender<T>,
    outstanding: Option<NonZeroU64>,
}

impl<T> Reservation<'_, T> {
    #[inline]
    pub fn publish(mut self, item: T) -> Send<T> {
        loop {
            match self.sender.publication.try_enter() {
                Enter::Live(append) => {
                    let weight = self.outstanding.assured("the grant owns its full weight");
                    append.publish(&mut self.sender.allocation, Entry { item, weight });
                    self.outstanding = None;
                    drop(append);
                    return Send::Published;
                }
                Enter::NoReaders => return Send::NoReaders(item),
                Enter::Sealed => {}
            }
            if !self.sender.refresh_publication() {
                return Send::Closed(item);
            }
        }
    }
}

impl<T> Drop for Reservation<'_, T> {
    fn drop(&mut self) {
        if let Some(weight) = self.outstanding {
            self.sender
                .shared
                .waiting
                .release(&mut self.sender.budget, weight);
        }
    }
}

/// Dropping a handle saves its position. The owner can reattach it or finish its subscription.
pub struct Reader<T> {
    shared: Arc<Shared<T>>,
    member: Arc<Member<T>>,
    position: Option<Box<Position<T>>>,
    budget: budget::Writer,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Read<T> {
    Item(T),
    Empty,
    Publishing,
    Closed,
}

impl<T> Reader<T> {
    fn new(shared: &Arc<Shared<T>>, member: Arc<Member<T>>, position: Box<Position<T>>) -> Self {
        shared.start_access();
        Self {
            member,
            shared: shared.clone(),
            position: Some(position),
            budget: Budget::writer(&shared.budget),
        }
    }
}

impl<T: Clone> Reader<T> {
    #[inline]
    pub fn try_read(&mut self) -> Read<T> {
        let position = self
            .position
            .as_mut()
            .assured("an attached reader owns its position");
        match position.next(true, None) {
            Next::Item(item) => Read::Item(self.complete(item)),
            Next::Empty => Read::Empty,
            Next::Publishing => Read::Publishing,
            Next::Ended => Read::Closed,
        }
    }

    #[inline]
    pub async fn read(&mut self) -> Option<T> {
        match self.try_read() {
            Read::Item(item) => return Some(item),
            Read::Closed => return None,
            Read::Empty | Read::Publishing => {}
        }
        self.read_waiting().await
    }

    async fn read_waiting(&mut self) -> Option<T> {
        let mut watch = None;
        let position = self
            .position
            .as_mut()
            .assured("an attached reader owns its position");
        let item =
            poll_fn(|cx| position.poll_next(&self.member.signal, &mut watch, cx, true, None))
                .await?;
        drop(watch);
        Some(self.complete(item))
    }

    #[inline]
    fn complete(&mut self, result: log::ReadItem<T>) -> T {
        if let Some(finished) = result.finished {
            self.shared
                .waiting
                .release(&mut self.budget, finished.weight);
            drop(finished);
        }
        result.item.assured("the reader requested a copy")
    }
}

impl<T> Drop for Reader<T> {
    fn drop(&mut self) {
        let position = self
            .position
            .take()
            .assured("the reader returns its position once");
        self.member.suspend(position);
        self.shared.end_access();
    }
}

#[cfg(test)]
#[cfg(not(any(feature = "loom", feature = "shuttle")))]
mod tests {
    use std::{
        future::Future as _,
        num::NonZeroU64,
        task::{Context, Waker},
    };

    use nervix_primitives::sync::{
        Arc, StdArc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::{Capture, Channel, Enter, ReaderAccess};
    use crate::Send;

    #[test]
    fn an_empty_channel_starts_with_admission_disabled() {
        let channel = Channel::<u8, u8>::new(NonZeroU64::MIN);
        assert!(!channel.shared.budget.state().readers);
    }

    #[nervix_primitives::test]
    async fn capture_finishes_an_unsubscribe_that_was_cancelled_before_the_seal_drained() {
        let mut channel = Channel::<u8, _>::new(NonZeroU64::MIN);
        let ReaderAccess::Attached(reader) = channel.subscribe("reader") else {
            panic!("the new reader attaches");
        };
        drop(reader);
        let closing = channel.shared.current.load_full();
        let Enter::Live(publishing) = closing.publication.try_enter() else {
            panic!("the publication enters before the old log is sealed");
        };
        let mut leaving = Box::pin(channel.unsubscribe(&"reader"));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(leaving.as_mut().poll(&mut cx).is_pending());
        drop(leaving);
        assert!(matches!(
            channel.subscribe("reader"),
            ReaderAccess::Duplicate
        ));
        drop(publishing);
        let Capture::Saved(saved) = channel.capture() else {
            panic!("there are no remaining active handles");
        };
        assert!(saved.positions.is_empty());
        assert!(matches!(
            channel.subscribe("reader"),
            ReaderAccess::Attached(_)
        ));
    }

    #[nervix_primitives::test]
    async fn an_idle_sender_retains_only_its_last_log_and_can_follow_a_new_reader() {
        let mut channel = Channel::new(NonZeroU64::MIN);
        let old = StdArc::downgrade(&channel.shared.current.load());
        let mut sender = channel.sender();
        let ReaderAccess::Attached(mut reader) = channel.subscribe("reader") else {
            panic!("the new reader attaches");
        };
        assert!(old.upgrade().is_none());
        assert_eq!(sender.send(1, NonZeroU64::MIN).await, Send::Published);
        assert_eq!(reader.try_read(), super::Read::Item(1));
    }

    struct CountingItem(Arc<AtomicUsize>);

    impl Drop for CountingItem {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[nervix_primitives::test]
    async fn dropping_the_channel_frees_the_retained_position_of_a_suspended_reader() {
        let mut channel = Channel::new(NonZeroU64::MIN);
        let ReaderAccess::Attached(reader) = channel.subscribe("reader") else {
            panic!("the new reader attaches");
        };
        drop(reader);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut publisher = channel.sender();
        assert!(matches!(
            publisher
                .send(CountingItem(drops.clone()), NonZeroU64::MIN)
                .await,
            Send::Published
        ));
        drop(publisher);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(channel);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
