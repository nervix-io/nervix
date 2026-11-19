//! A shared broadcast log with exact weighted admission.
//!
//! Layer: primitives.
//! - **Owns.** Reservation, delivery, capacity, and the lifetime of channel entries.
//! - **Depends on.** The selected synchronization primitives and invariant checks.
//! - **Must not know.** How the caller measures an item, who reads it, or what it delivers.

mod budget;
mod channel;
mod generation;
mod log;
mod member;
mod waiters;

pub use channel::{
    Capture, Channel, Leave, Read, Reader, ReaderAccess, Reservation, Reserve, Restore, SavedEntry,
    Send, Sender, Snapshot,
};

#[cfg(test)]
#[cfg(not(any(feature = "loom", feature = "shuttle")))]
mod tests;

#[cfg(test)]
#[cfg(feature = "shuttle")]
mod shuttle_checks;
