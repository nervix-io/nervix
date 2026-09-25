//! Scheduler-visible synchronization around opaque third-party publication primitives.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The interfaces for opaque third-party synchronization primitives and the
//!   synchronous yield used by spin waits, with explicit scheduling points when Shuttle drives
//!   execution.
//! - **Depends on.** ArcSwap and DashMap in production and Shuttle only in deterministic feature
//!   builds.
//! - **Must not know.** What a published value represents or which runtime operation waits for it.

#[cfg(not(feature = "shuttle"))]
pub use arc_swap::{ArcSwap, ArcSwapOption, Guard, cache::Cache};
#[cfg(not(feature = "shuttle"))]
pub use dashmap::DashMap;
#[cfg(not(feature = "shuttle"))]
pub use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

/// A task handle that aborts its task when dropped.
#[cfg(feature = "shuttle")]
#[must_use = "dropping the handle aborts the task immediately"]
pub struct AbortOnDropHandle<T>(tokio::task::JoinHandle<T>);

#[cfg(feature = "shuttle")]
impl<T> AbortOnDropHandle<T> {
    pub fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self(handle)
    }

    /// End the task now, while keeping the handle to await its cancellation.
    pub fn abort(&self) {
        self.0.abort();
    }
}

#[cfg(feature = "shuttle")]
impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(feature = "shuttle")]
impl<T> std::future::Future for AbortOnDropHandle<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(context)
    }
}

/// Yield a synchronous spin wait through the active scheduler.
#[inline]
pub fn yield_now() {
    #[cfg(feature = "shuttle")]
    shuttle::thread::yield_now();

    #[cfg(not(feature = "shuttle"))]
    std::thread::yield_now();
}

#[cfg(feature = "shuttle")]
mod deterministic {
    use std::{
        hash::Hash,
        marker::PhantomData,
        ops::{Deref, DerefMut},
        sync::Arc,
    };

    pub use arc_swap::Guard;

    /// A cancellation token with the clone identity Tokio-util exposes in production.
    #[derive(Debug)]
    pub struct CancellationToken {
        inner: tokio_util::sync::CancellationToken,
        identity: Arc<()>,
    }

    impl CancellationToken {
        pub fn new() -> Self {
            Self {
                inner: tokio_util::sync::CancellationToken::new(),
                identity: Arc::new(()),
            }
        }
    }

    impl Clone for CancellationToken {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                identity: Arc::clone(&self.identity),
            }
        }
    }

    impl Default for CancellationToken {
        fn default() -> Self {
            Self::new()
        }
    }

    impl PartialEq for CancellationToken {
        fn eq(&self, other: &Self) -> bool {
            Arc::ptr_eq(&self.identity, &other.identity)
        }
    }

    impl Eq for CancellationToken {}

    impl Deref for CancellationToken {
        type Target = tokio_util::sync::CancellationToken;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    /// A concurrent map whose lock operations are visible to Shuttle.
    ///
    /// The modeled DashMap does not use a configurable hasher. Retaining the hasher parameter in
    /// this boundary keeps production map types unchanged while deterministic builds model the
    /// synchronization rather than hashing behavior.
    #[derive(Debug)]
    pub struct DashMap<K, V, S = std::collections::hash_map::RandomState> {
        inner: dashmap::DashMap<K, V>,
        hasher: PhantomData<fn() -> S>,
    }

    impl<K, V, S> DashMap<K, V, S>
    where
        K: Eq + Hash,
    {
        pub fn with_hasher(_hasher: S) -> Self {
            Self {
                inner: dashmap::DashMap::new(),
                hasher: PhantomData,
            }
        }

        pub fn with_capacity_and_hasher(capacity: usize, _hasher: S) -> Self {
            Self {
                inner: dashmap::DashMap::with_capacity(capacity),
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
                inner: dashmap::DashMap::with_capacity(capacity),
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
                inner: dashmap::DashMap::new(),
                hasher: PhantomData,
            }
        }
    }

    impl<K, V, S> Deref for DashMap<K, V, S> {
        type Target = dashmap::DashMap<K, V>;

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
        type Item = dashmap::mapref::multiple::RefMulti<'a, K, V>;
        type IntoIter = dashmap::Iter<'a, K, V>;

        fn into_iter(self) -> Self::IntoIter {
            self.inner.iter()
        }
    }

    /// Run one opaque publication operation between two scheduling points, so Shuttle can run
    /// another thread just before it and just after it. A value read this way may already be
    /// stale by the time its reader acts on it, exactly as it may be in production.
    fn scheduled<R>(operation: impl FnOnce() -> R) -> R {
        super::yield_now();
        let result = operation();
        super::yield_now();
        result
    }

    /// An atomic `Arc` publication whose opaque operations are visible to Shuttle.
    #[derive(Debug)]
    pub struct ArcSwap<T> {
        inner: arc_swap::ArcSwap<T>,
    }

    impl<T> ArcSwap<T> {
        pub fn from_pointee(value: T) -> Self {
            Self {
                inner: arc_swap::ArcSwap::from_pointee(value),
            }
        }

        pub fn load(&self) -> Guard<Arc<T>> {
            scheduled(|| self.inner.load())
        }

        pub fn load_full(&self) -> Arc<T> {
            scheduled(|| self.inner.load_full())
        }

        pub fn store(&self, value: Arc<T>) {
            scheduled(|| self.inner.store(value));
        }

        pub fn compare_and_swap(&self, current: &Arc<T>, new: Arc<T>) -> Guard<Arc<T>> {
            scheduled(|| self.inner.compare_and_swap(current, new))
        }

        pub fn rcu<R>(&self, update: impl FnMut(&Arc<T>) -> R) -> Arc<T>
        where
            R: Into<Arc<T>>,
        {
            scheduled(|| self.inner.rcu(update))
        }
    }

    impl<T> From<Arc<T>> for ArcSwap<T> {
        fn from(value: Arc<T>) -> Self {
            Self {
                inner: arc_swap::ArcSwap::from(value),
            }
        }
    }

    /// An optional atomic `Arc` publication whose opaque operations are visible to Shuttle.
    #[derive(Debug)]
    pub struct ArcSwapOption<T> {
        inner: arc_swap::ArcSwapOption<T>,
    }

    impl<T> ArcSwapOption<T> {
        pub fn empty() -> Self {
            Self {
                inner: arc_swap::ArcSwapOption::empty(),
            }
        }

        pub fn load(&self) -> Guard<Option<Arc<T>>> {
            scheduled(|| self.inner.load())
        }

        pub fn load_full(&self) -> Option<Arc<T>> {
            scheduled(|| self.inner.load_full())
        }

        pub fn store(&self, value: Option<Arc<T>>) {
            scheduled(|| self.inner.store(value));
        }
    }

    impl<T> From<Option<Arc<T>>> for ArcSwapOption<T> {
        fn from(value: Option<Arc<T>>) -> Self {
            Self {
                inner: arc_swap::ArcSwapOption::from(value),
            }
        }
    }

    /// Shuttle's cache reloads through the wrapper on every access so every observation is a
    /// scheduling point. The production build re-exports ArcSwap's original pointer cache.
    pub struct Cache<A, T> {
        source: A,
        cached: T,
    }

    impl<A, T> Cache<A, Arc<T>>
    where
        A: Deref<Target = ArcSwap<T>>,
    {
        pub fn new(source: A) -> Self {
            let cached = source.load_full();
            Self { source, cached }
        }

        pub fn load(&mut self) -> &Arc<T> {
            self.cached = self.source.load_full();
            &self.cached
        }
    }
}

#[cfg(feature = "shuttle")]
pub use deterministic::{ArcSwap, ArcSwapOption, Cache, CancellationToken, DashMap, Guard};
