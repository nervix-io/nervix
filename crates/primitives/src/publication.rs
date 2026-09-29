//! Atomic publication of a shared value, selected for the build's execution mode.
//!
//! An `ArcSwap` publishes a whole value that readers load without locking, and a `Cache` keeps a
//! reader's last load until the published pointer changes. ArcSwap's internals are opaque to every
//! model. A Shuttle build therefore runs each load, store, compare-and-swap and read-copy-update
//! between two scheduling points, so a check can order an owner's use of a published value against
//! other threads; it cannot establish ArcSwap's own memory safety. The ordinary build re-exports
//! ArcSwap unchanged.

#[cfg(feature = "shuttle")]
pub use arc_swap::Guard;
#[cfg(not(feature = "shuttle"))]
pub use arc_swap::{ArcSwap, ArcSwapOption, Guard, cache::Cache};

#[cfg(feature = "shuttle")]
mod scheduled {
    use std::{ops::Deref, sync::Arc};

    use arc_swap::Guard;

    /// Run one opaque publication operation between two scheduling points, so Shuttle can run
    /// another thread just before it and just after it. A value read this way may already be
    /// stale by the time its reader acts on it, exactly as it may be in production.
    fn scheduled<R>(operation: impl FnOnce() -> R) -> R {
        shuttle::thread::yield_now();
        let result = operation();
        shuttle::thread::yield_now();
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

        pub fn rcu<R>(&self, update: impl FnMut(&Option<Arc<T>>) -> R) -> Option<Arc<T>>
        where
            R: Into<Option<Arc<T>>>,
        {
            scheduled(|| self.inner.rcu(update))
        }
    }

    impl<T> Default for ArcSwapOption<T> {
        fn default() -> Self {
            Self::empty()
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
pub use scheduled::{ArcSwap, ArcSwapOption, Cache};
