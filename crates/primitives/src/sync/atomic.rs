//! Atomic values, the orderings their operations take, and fences, selected together.
//!
//! The three are one family: an ordering argument means something only to the atomic it is passed
//! to, and a fence orders only the operations of the same backend. Every Nervix atomic, including a
//! counter or flag a vocabulary type exposes, comes from here, so a modeled build observes all of
//! them rather than only the ones an owner remembered to switch.
//!
//! Every operation keeps the ordering its caller requested in ordinary execution. Shuttle treats
//! every ordering as `SeqCst` and makes each operation a scheduling point, so it explores
//! interleavings but proves nothing about a weaker ordering. Loom explores the reorderings the C11
//! model allows for `Relaxed`, `Acquire` and `Release`, within the limits its documentation states,
//! and is the backend a memory-ordering claim is checked under.
//!
//! The surface is the part every backend provides: the atomic types, [`Ordering`] and [`fence`].
//! An operation one backend lacks, such as the standard library's `as_ptr`, fails to compile in
//! that mode instead of silently using a real primitive.
//!
//! A selected atomic belongs to the model execution that constructs it, so none lives in a `static`
//! or is constructed in a const context. Loom's constructors are not `const`, and a static would
//! carry one execution's state into the next. State that must outlive every execution is a real
//! atomic from [`crate::unmodeled`].

#[cfg(not(any(feature = "loom", feature = "shuttle")))]
pub use std::sync::atomic::{
    AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8,
    AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence,
};

#[cfg(feature = "loom")]
pub use loom::sync::atomic::{
    AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8,
    AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence,
};
// A graph that enables both modes has already failed to compile with a diagnostic naming them;
// selecting one backend here keeps that the only error it reports.
#[cfg(all(feature = "shuttle", not(feature = "loom")))]
pub use shuttle::sync::atomic::{
    AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8,
    AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence,
};
