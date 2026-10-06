//! The wakeups of the operations waiting for a stream of one peer.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** One wakeup for each pool class and request subquota of a peer's streams, where a
//!   released stream reports itself.
//! - **Depends on.** The pool classes and request subquotas, and the primitive boundary's `Notify`.
//! - **Must not know.** Connections, slots, deadlines, or what an operation sends.

use nervix_primitives::sync::Notify;
use strum::EnumCount as _;

use super::{PoolClass, RequestSubquota};

/// One wakeup for each class and subquota of a peer's streams.
///
/// A stream's slot serves only operations of its own class and subquota, so a released stream
/// wakes a waiter of exactly those: a single notification for every waiter of the transport could
/// wake one that cannot use the slot while the one that can sleeps on until its deadline.
pub(super) struct StreamReleases([[Notify; RequestSubquota::COUNT]; PoolClass::COUNT]);

impl StreamReleases {
    pub(super) fn new() -> Self {
        Self(std::array::from_fn(|_| {
            std::array::from_fn(|_| Notify::new())
        }))
    }

    pub(super) fn of(&self, class: PoolClass, subquota: RequestSubquota) -> &Notify {
        &self.0[class.index()][subquota.index()]
    }
}
