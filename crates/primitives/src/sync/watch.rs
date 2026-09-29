//! A watch channel for a Shuttle build: Tokio's semantics, with every registration and decision
//! read a point Shuttle can schedule around.
//!
//! A receiver sees a value once its version is newer than the version the receiver last marked as
//! seen. Subscribing marks the current version as seen, so it is the registration a lost update
//! hides behind: an owner that reads some state and only then subscribes never learns of a value
//! sent in between. Shuttle's own watch channel reads the version with no scheduling point before
//! it, so no schedule could place a send there. This channel takes a scheduling point immediately
//! before each read of the shared version that registers or decides something: subscribing,
//! [`Receiver::has_changed`], [`Sender::receiver_count`] and the send that checks it. It locks its
//! value with Shuttle's modeled lock, and a receiver waits on this crate's `Notify`, whose
//! registrations and notifications are scheduling points too.
//!
//! Dropping an endpoint is not a scheduling point, because a drop can run while Shuttle ends an
//! execution. The last receiver's drop still wakes [`Sender::closed`], and the last sender's drop
//! closes the channel and wakes every waiting receiver, as in Tokio.

use std::{
    fmt, mem, ops, panic,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use shuttle::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::notify::Notify;
use crate::scheduling;

/// The receiving half of a watch channel.
#[derive(Debug)]
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The version this receiver last marked as seen.
    seen: Version,
}

/// The sending half of a watch channel.
#[derive(Debug)]
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

/// A borrowed value, with whether its version is newer than the one the receiver had seen.
#[derive(Debug)]
pub struct Ref<'a, T> {
    inner: RwLockReadGuard<'a, T>,
    has_changed: bool,
}

pub mod error {
    //! Why a watch channel could not send or receive.

    use std::{error::Error, fmt};

    /// Every receiver was dropped; carries the value that could not be sent.
    #[derive(PartialEq, Eq, Clone, Copy)]
    pub struct SendError<T>(pub T);

    impl<T> fmt::Debug for SendError<T> {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.debug_struct("SendError").finish_non_exhaustive()
        }
    }

    impl<T> fmt::Display for SendError<T> {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "channel closed")
        }
    }

    impl<T> Error for SendError<T> {}

    /// Every sender was dropped.
    #[derive(Debug, Clone)]
    pub struct RecvError(pub(super) ());

    impl fmt::Display for RecvError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "channel closed")
        }
    }

    impl Error for RecvError {}
}

struct Shared<T> {
    value: RwLock<T>,
    state: State,
    // Real counters, like Tokio's: a count is read at a scheduling point the reader takes first,
    // and a drop that changes one takes no point of its own.
    receivers: AtomicUsize,
    senders: AtomicUsize,
    /// Wakes receivers waiting for a newer version or for the channel to close.
    changed: Notify,
    /// Wakes senders waiting for every receiver to be dropped.
    receivers_dropped: Notify,
}

impl<T: fmt::Debug> fmt::Debug for Shared<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load();
        formatter
            .debug_struct("Shared")
            .field("value", &self.value)
            .field("version", &state.version())
            .field("is_closed", &state.is_closed())
            .finish_non_exhaustive()
    }
}

/// The version of the value and whether the channel is closed, in one word so a receiver reads
/// both at once: the lowest bit is the closed flag, and every send adds two.
struct State(AtomicUsize);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Version(usize);

#[derive(Clone, Copy)]
struct Snapshot(usize);

const CLOSED: usize = 1;
const VERSION_STEP: usize = 2;

impl State {
    fn load(&self) -> Snapshot {
        Snapshot(self.0.load(Ordering::SeqCst))
    }

    /// A newer version. Versions are only compared for equality, so the count wraps as Tokio's
    /// does; a model sends far fewer than `usize::MAX / 2` values.
    fn advance_version(&self) {
        self.0.fetch_add(VERSION_STEP, Ordering::SeqCst);
    }

    fn close(&self) {
        self.0.fetch_or(CLOSED, Ordering::SeqCst);
    }
}

impl Snapshot {
    fn version(self) -> Version {
        Version(self.0 & !CLOSED)
    }

    fn is_closed(self) -> bool {
        self.0 & CLOSED == CLOSED
    }
}

/// A channel holding `init`, with one sender and one receiver that has seen `init`.
pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        value: RwLock::new(init),
        state: State(AtomicUsize::new(0)),
        receivers: AtomicUsize::new(1),
        senders: AtomicUsize::new(1),
        changed: Notify::new(),
        receivers_dropped: Notify::new(),
    });
    let sender = Sender {
        shared: Arc::clone(&shared),
    };
    let receiver = Receiver {
        shared,
        seen: Version(0),
    };
    (sender, receiver)
}

impl<T> Shared<T> {
    fn read(&self) -> RwLockReadGuard<'_, T> {
        match self.value.read() {
            Ok(guard) => guard,
            // A panic while the value is locked leaves it as the panicking send left it, which is
            // what Tokio's lock, which does not poison, gives the next reader too.
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn write(&self) -> RwLockWriteGuard<'_, T> {
        match self.value.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn receiver_count(&self) -> usize {
        self.receivers.load(Ordering::SeqCst)
    }
}

impl<T> Receiver<T> {
    fn subscribed(shared: Arc<Shared<T>>, seen: Version) -> Self {
        shared.receivers.fetch_add(1, Ordering::SeqCst);
        Self { shared, seen }
    }

    /// The current value, without marking it as seen.
    pub fn borrow(&self) -> Ref<'_, T> {
        let inner = self.shared.read();
        let has_changed = self.seen != self.shared.state.load().version();
        Ref { inner, has_changed }
    }

    /// The current value, marked as seen.
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        let inner = self.shared.read();
        let current = self.shared.state.load().version();
        let has_changed = self.seen != current;
        self.seen = current;
        Ref { inner, has_changed }
    }

    /// Whether a value newer than the one last marked as seen was sent. Fails once every sender
    /// was dropped.
    pub fn has_changed(&self) -> Result<bool, error::RecvError> {
        scheduling::point();
        let state = self.shared.state.load();
        if state.is_closed() {
            return Err(error::RecvError(()));
        }
        Ok(self.seen != state.version())
    }

    /// Wait for a value newer than the one last marked as seen, and mark it as seen. Fails once
    /// every sender was dropped and no unseen value is left.
    pub async fn changed(&mut self) -> Result<(), error::RecvError> {
        changed(&self.shared, &mut self.seen).await
    }

    /// Wait until the value satisfies `condition`, marking each value it checks as seen. Fails
    /// once every sender was dropped and no unseen value satisfies it.
    pub async fn wait_for(
        &mut self,
        mut condition: impl FnMut(&T) -> bool,
    ) -> Result<Ref<'_, T>, error::RecvError> {
        let mut closed = false;
        loop {
            {
                let inner = self.shared.read();
                let current = self.shared.state.load().version();
                let has_changed = self.seen != current;
                self.seen = current;
                if !closed || has_changed {
                    let checked =
                        panic::catch_unwind(panic::AssertUnwindSafe(|| condition(&inner)));
                    match checked {
                        Ok(true) => return Ok(Ref { inner, has_changed }),
                        Ok(false) => {}
                        Err(panicked) => {
                            drop(inner);
                            panic::resume_unwind(panicked);
                        }
                    }
                }
            }
            if closed {
                return Err(error::RecvError(()));
            }
            let waited = changed(&self.shared, &mut self.seen).await;
            closed = waited.is_err();
        }
    }

    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

/// Register for the next change, then read the version: a send after the read wakes the
/// registration, and a send before it shows as a newer version.
async fn changed<T>(shared: &Shared<T>, seen: &mut Version) -> Result<(), error::RecvError> {
    loop {
        let notified = shared.changed.notified();
        let state = shared.state.load();
        if *seen != state.version() {
            *seen = state.version();
            return Ok(());
        }
        if state.is_closed() {
            return Err(error::RecvError(()));
        }
        notified.await;
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self::subscribed(Arc::clone(&self.shared), self.seen)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let before = self.shared.receivers.fetch_sub(1, Ordering::SeqCst);
        if before == 1 {
            self.shared.receivers_dropped.notify_waiters_from_drop();
        }
    }
}

impl<T> Sender<T> {
    /// A sender of a channel holding `init`, which no receiver has subscribed to yet.
    pub fn new(init: T) -> Self {
        let (sender, _) = channel(init);
        sender
    }

    /// Send `value` to every receiver. Fails, returning it, when there is no receiver.
    pub fn send(&self, value: T) -> Result<(), error::SendError<T>> {
        scheduling::point();
        if self.shared.receiver_count() == 0 {
            return Err(error::SendError(value));
        }
        self.send_replace(value);
        Ok(())
    }

    /// Modify the value in place and notify every receiver.
    pub fn send_modify<F>(&self, modify: F)
    where
        F: FnOnce(&mut T),
    {
        self.send_if_modified(|value| {
            modify(value);
            true
        });
    }

    /// Modify the value in place, and notify every receiver when `modify` reports a change.
    pub fn send_if_modified<F>(&self, modify: F) -> bool
    where
        F: FnOnce(&mut T) -> bool,
    {
        {
            let mut value = self.shared.write();
            let modified = panic::catch_unwind(panic::AssertUnwindSafe(|| modify(&mut value)));
            match modified {
                Ok(true) => {}
                Ok(false) => return false,
                Err(panicked) => {
                    drop(value);
                    panic::resume_unwind(panicked);
                }
            }
            // Advanced while the value is locked, so a receiver holding the lock reads the version
            // of the value it borrowed.
            self.shared.state.advance_version();
        }
        self.shared.changed.notify_waiters();
        true
    }

    /// Replace the value, notify every receiver, and return the previous value. Succeeds without
    /// receivers, so a later subscriber sees the value.
    pub fn send_replace(&self, mut value: T) -> T {
        self.send_modify(|current| mem::swap(current, &mut value));
        value
    }

    /// The current value.
    pub fn borrow(&self) -> Ref<'_, T> {
        let inner = self.shared.read();
        Ref {
            inner,
            has_changed: false,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.receiver_count() == 0
    }

    /// Wait until every receiver is dropped.
    pub async fn closed(&self) {
        while self.receiver_count() > 0 {
            let notified = self.shared.receivers_dropped.notified();
            if self.receiver_count() == 0 {
                return;
            }
            notified.await;
        }
    }

    /// A receiver that has seen the current value.
    pub fn subscribe(&self) -> Receiver<T> {
        scheduling::point();
        let seen = self.shared.state.load().version();
        Receiver::subscribed(Arc::clone(&self.shared), seen)
    }

    pub fn receiver_count(&self) -> usize {
        scheduling::point();
        self.shared.receiver_count()
    }

    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::SeqCst);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: Default> Default for Sender<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let before = self.shared.senders.fetch_sub(1, Ordering::SeqCst);
        if before == 1 {
            self.shared.state.close();
            self.shared.changed.notify_waiters_from_drop();
        }
    }
}

impl<T> Ref<'_, T> {
    /// Whether this value is newer than the one the receiver had marked as seen.
    pub fn has_changed(&self) -> bool {
        self.has_changed
    }
}

impl<T> ops::Deref for Ref<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}
