//! Concurrent collections, selected for the build's execution mode.
//!
//! A [`DashMap`] shards its entries behind locks. In a Shuttle build those locks are Shuttle's, so a
//! check observes every shard acquisition; the ordinary build re-exports DashMap unchanged.

#[cfg(not(feature = "shuttle"))]
pub use dashmap::DashMap;
#[cfg(feature = "shuttle")]
pub use scheduled::DashMap;

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
