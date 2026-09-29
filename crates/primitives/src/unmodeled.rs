//! Real primitives that stay outside every model, in every mode.
//!
//! Some state has to be real even in a build that runs a model checker. A runner keeps statistics
//! across the many model executions it starts, and a model execution cannot own them. An external
//! library can require the exact standard-library type. A thread the model does not run, such as
//! a timer thread the operating system schedules, cannot touch a modeled primitive at all.
//!
//! Every use of this module is a permission: `crates/primitives/unmodeled-permissions.toml` names
//! the file, the items it uses, the owner that needs them, why a real primitive is required and
//! what that leaves unverified, and `just validate-primitive-boundary` rejects any use without one.
//! A real primitive supports runner bookkeeping, external interoperability, or state a check
//! excludes from its claim. It never carries the protocol under test, chooses its branches, supplies
//! its wakeups, or establishes an ordering an assertion relies on.

pub mod sync {
    //! Real synchronization primitives.

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
