//! Real primitives that stay outside every model, in every mode.
//!
//! Some state has to be real even in a build that runs a model checker. A runner keeps statistics
//! across the many model executions it starts, and a model execution cannot own them. An external
//! library can require the exact type of the library it was written against. A thread the model
//! does not run, such as a timer thread the operating system schedules, cannot touch a modeled
//! primitive at all.
//!
//! Every use of this module is a permission: `crates/primitives/unmodeled-permissions.toml` names
//! the file, the items it uses, the owner that needs them, why a real primitive is required and
//! what that leaves unverified, and `just validate-primitive-boundary` rejects any use without one.
//! A real primitive supports runner bookkeeping, external interoperability, or state a check
//! excludes from its claim. It never carries the protocol under test, chooses its branches, supplies
//! its wakeups, or establishes an ordering an assertion relies on.
//!
//! The atomics are portable; the rest is the `native` capability, like the selected families.

pub mod sync {
    //! Real synchronization primitives.

    /// Tokio's own lock and channels, for an external library whose interface takes them.
    #[cfg(feature = "native")]
    pub use tokio::sync::{Mutex, mpsc, watch};

    pub mod atomic {
        //! The standard library's atomics, whatever the execution mode.
        //!
        //! [`Ordering`] is the standard library's in every mode, so it is the same type as the
        //! selected one.

        pub use std::sync::atomic::{
            AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr,
            AtomicU8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering,
        };
    }
}

#[cfg(feature = "native")]
pub mod runtime {
    //! Tokio's own runtime, for an owner whose runtime is its external contract rather than
    //! something a model runs.

    pub use tokio::runtime::{Builder, Runtime};
}

#[cfg(feature = "native")]
pub mod task {
    //! Tokio's own tasks, for work that runs on the runtime of an external library and waits on
    //! its real primitives.

    pub use tokio::task::{consume_budget, spawn};
}

#[cfg(feature = "native")]
pub mod time {
    //! Tokio's own timer, for a task of [`task`](super::task) that runs on the runtime of an
    //! external library.

    pub use tokio::time::sleep;
}

/// Tokio's own `select!`, for a task of [`task`] that waits on real primitives.
#[cfg(feature = "native")]
pub use tokio::select;
