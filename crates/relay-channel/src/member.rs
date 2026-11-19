//! A reader's position while it has no active handle.
//!
//! Layer: primitives.
//! - **Owns.** The transfer of sole ownership of a reader position.
//! - **Depends on.** Its log position and the selected atomic pointer.
//! - **Must not know.** Which identity the caller uses for the reader.

use std::{marker::PhantomData, ptr, ptr::NonNull};

use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicPtr, Ordering},
};

use crate::{
    channel::Standstill,
    generation::{Position, ReaderSignal},
};

pub(super) struct Member<T> {
    pub(super) signal: Arc<ReaderSignal>,
    saved: AtomicPtr<Position<T>>,
    owns: PhantomData<Box<Position<T>>>,
}

// SAFETY: a position always belongs to the atomic slot or to one handle. The standstill
// inspection runs only when the exclusive controller has ruled out every handle.
unsafe impl<T: Send + Sync> Sync for Member<T> {}

impl<T> Member<T> {
    pub(super) fn new(signal: Arc<ReaderSignal>, position: Position<T>) -> Arc<Self> {
        Arc::new(Self {
            signal,
            saved: AtomicPtr::new(Box::into_raw(Box::new(position))),
            owns: PhantomData,
        })
    }

    pub(super) fn take(&self) -> Option<Box<Position<T>>> {
        let raw = self.saved.swap(ptr::null_mut(), Ordering::AcqRel);
        let owned = NonNull::new(raw)?;
        // SAFETY: the exchange acquired the sole position recorded in this member.
        Some(unsafe { Box::from_raw(owned.as_ptr()) })
    }

    pub(super) fn suspend(&self, position: Box<Position<T>>) {
        // A later handle or the exclusive controller acquires this position through take().
        debug_assert!(self.saved.load(Ordering::Relaxed).is_null());
        self.saved.store(Box::into_raw(position), Ordering::Release);
    }

    pub(super) fn at_standstill<'a>(&'a self, _proof: &'a Standstill) -> Option<&'a Position<T>> {
        let stored = self.saved.load(Ordering::Acquire);
        let stored = NonNull::new(stored)?;
        // SAFETY: the exclusive controller proved that there is no handle that can take or
        // change this position until this view ends.
        Some(unsafe { stored.as_ref() })
    }
}

impl<T> Drop for Member<T> {
    fn drop(&mut self) {
        drop(self.take());
    }
}
