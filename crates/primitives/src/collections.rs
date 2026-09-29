//! Concurrent collections, selected for the build's execution mode.
//!
//! A [`DashMap`] shards its entries behind locks. In a Shuttle build those locks are Shuttle's, so a
//! check observes every shard acquisition. A [`ConcurrentQueue`] is lock-free and opaque to every
//! model, so a Shuttle build runs each of its operations between two scheduling points: a check can
//! order an owner's use of the queue against other tasks, and the queue's own memory safety stays
//! unmodeled. The ordinary build re-exports both unchanged.

#[cfg(not(feature = "shuttle"))]
pub use concurrent_queue::ConcurrentQueue;
pub use concurrent_queue::{PopError, PushError};
#[cfg(not(feature = "shuttle"))]
pub use dashmap::DashMap;
#[cfg(feature = "shuttle")]
pub use scheduled::{ConcurrentQueue, DashMap};

pub mod dash_map {
    //! The entries of a [`DashMap`](super::DashMap).

    #[cfg(not(feature = "shuttle"))]
    pub use dashmap::mapref::entry::{Entry, OccupiedEntry, VacantEntry};
    #[cfg(feature = "shuttle")]
    pub use shuttle_dashmap::mapref::entry::{Entry, OccupiedEntry, VacantEntry};
}

#[cfg(feature = "shuttle")]
mod scheduled {
    use std::{
        hash::Hash,
        marker::PhantomData,
        ops::{Deref, DerefMut},
    };

    use crate::scheduling;

    /// A concurrent map whose lock operations are visible to Shuttle.
    ///
    /// The modeled DashMap does not use a configurable hasher. Retaining the hasher parameter in
    /// this boundary keeps production map types unchanged while deterministic builds model the
    /// synchronization rather than hashing behavior.
    #[derive(Debug)]
    pub struct DashMap<K, V, S = std::collections::hash_map::RandomState> {
        inner: shuttle_dashmap::DashMap<K, V>,
        hasher: PhantomData<fn() -> S>,
    }

    impl<K, V, S> DashMap<K, V, S>
    where
        K: Eq + Hash,
    {
        pub fn with_hasher(_hasher: S) -> Self {
            Self {
                inner: shuttle_dashmap::DashMap::new(),
                hasher: PhantomData,
            }
        }

        pub fn with_capacity_and_hasher(capacity: usize, _hasher: S) -> Self {
            Self {
                inner: shuttle_dashmap::DashMap::with_capacity(capacity),
                hasher: PhantomData,
            }
        }
    }

    impl<K, V> DashMap<K, V>
    where
        K: Eq + Hash,
    {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_capacity(capacity: usize) -> Self {
            Self {
                inner: shuttle_dashmap::DashMap::with_capacity(capacity),
                hasher: PhantomData,
            }
        }
    }

    impl<K, V, S> Default for DashMap<K, V, S>
    where
        K: Eq + Hash,
    {
        fn default() -> Self {
            Self {
                inner: shuttle_dashmap::DashMap::new(),
                hasher: PhantomData,
            }
        }
    }

    impl<K, V, S> Deref for DashMap<K, V, S> {
        type Target = shuttle_dashmap::DashMap<K, V>;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl<K, V, S> DerefMut for DashMap<K, V, S> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.inner
        }
    }

    /// A lock-free queue whose operations Shuttle can schedule around.
    #[derive(Debug)]
    pub struct ConcurrentQueue<T> {
        inner: concurrent_queue::ConcurrentQueue<T>,
    }

    impl<T> ConcurrentQueue<T> {
        pub fn bounded(capacity: usize) -> Self {
            Self {
                inner: concurrent_queue::ConcurrentQueue::bounded(capacity),
            }
        }

        pub fn unbounded() -> Self {
            Self {
                inner: concurrent_queue::ConcurrentQueue::unbounded(),
            }
        }

        pub fn push(&self, value: T) -> Result<(), concurrent_queue::PushError<T>> {
            scheduling::around(|| self.inner.push(value))
        }

        pub fn pop(&self) -> Result<T, concurrent_queue::PopError> {
            scheduling::around(|| self.inner.pop())
        }

        /// Close the queue. Returns whether this call closed it.
        pub fn close(&self) -> bool {
            scheduling::around(|| self.inner.close())
        }

        pub fn is_closed(&self) -> bool {
            scheduling::around(|| self.inner.is_closed())
        }

        pub fn len(&self) -> usize {
            scheduling::around(|| self.inner.len())
        }

        pub fn is_empty(&self) -> bool {
            scheduling::around(|| self.inner.is_empty())
        }
    }

    impl<'a, K, V, S> IntoIterator for &'a DashMap<K, V, S>
    where
        K: Eq + Hash + Clone,
    {
        type Item = shuttle_dashmap::mapref::multiple::RefMulti<'a, K, V>;
        type IntoIter = shuttle_dashmap::Iter<'a, K, V>;

        fn into_iter(self) -> Self::IntoIter {
            self.inner.iter()
        }
    }
}
