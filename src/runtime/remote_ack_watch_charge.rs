//! The relay memory a node charges for the acknowledgement watches of a batch another node
//! delivered to it, and the hold that keeps such a delivery back until that memory has room.
//!
//! Layer: data plane.
//!
//! - **Owns.** The fixed charge of one acknowledgement watch and of the task that multiplexes a
//!   batch's watches, the hold of an unadmitted delivery while the relay budget cannot take that
//!   charge, and the return of each watch's share as that watch ends.
//! - **Depends on.** The execution budgets and the relay memory hold.
//! - **Must not know.** The interconnect protocol, relays, branches, or the rows a watch reports.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a receiver charges the acknowledgement watches of every relay delivery it admits"
    )
)]

use std::future::Future;

use nervix_execution::{AdmissionError, Reservation};

use super::{
    relay_memory_hold::{RelayMemoryHold, RelayMemoryHolding},
    remote_dispatch::REMOTE_RELAY_TOTAL_TIMEOUT,
    *,
};

/// The relay memory one acknowledgement watch holds: its row's poll future, its queue node, its ACK
/// root and its registration fit within it.
const WATCH_BYTES: u64 = 1024;

/// The relay memory of the one task that multiplexes a batch's watches, with its scheduler
/// allocation, however many rows the batch carries.
const TASK_BYTES: u64 = 4096;

/// The relay memory the acknowledgement watches of one admitted batch hold.
///
/// It covers one share per watch and one for the task that polls them. A watch's share returns to
/// the budget as soon as that watch ends, so the charge measures the watches still polled rather
/// than the batch they arrived in; the task's share returns when the charge is dropped.
#[derive(Debug)]
pub(super) struct RemoteAckWatchCharge {
    reservation: Reservation,
    watches: usize,
}

/// How holding a delivery for its watches' charge ended without a refusal.
#[derive(Debug)]
pub(super) enum RemoteAckWatchHold {
    /// The relay budget took the charge.
    Charged(RemoteAckWatchCharge),
    /// The delivery stopped being admissible while it was held, so it needs no charge.
    Cancelled,
}

impl RemoteAckWatchCharge {
    /// What `watches` acknowledgement watches of one batch cost the relay budget.
    fn bytes(watches: NonZeroUsize) -> u64 {
        let watches: u64 = watches.get().arch_into();
        let watch_bytes = watches.checked_mul(WATCH_BYTES).assured(
            "one relay frame carries no more registrations than its bounded encoded metadata \
             holds, so their watches' charge fits in u64",
        );
        watch_bytes
            .checked_add(TASK_BYTES)
            .assured("a bounded frame's watch charge leaves room for one task's share in u64")
    }

    /// Charge the relay budget for `watches` acknowledgement watches, holding the delivery back
    /// while the budget has no room for them.
    ///
    /// A held delivery is neither admitted nor refused: its sender keeps waiting for the admission,
    /// and the hold takes the charge once the budget has room. It ends without a charge as soon as
    /// `cancelled` resolves. Watches the whole budget could never hold are refused at once, and a
    /// hold still without room once it has lasted as long as a sender waits for an admission is
    /// refused with the budget's last refusal.
    pub(super) async fn hold(
        executor: &Executor,
        watches: NonZeroUsize,
        cancelled: impl Future<Output = ()>,
    ) -> Result<RemoteAckWatchHold, Report<AdmissionError>> {
        let deadline = Instant::now()
            .checked_add(REMOTE_RELAY_TOTAL_TIMEOUT)
            .assured("the fixed relay admission bound fits the monotonic clock");
        let holding = RelayMemoryHold::new(Self::bytes(watches), deadline)
            .charge(executor, cancelled)
            .await?;
        let RelayMemoryHolding::Charged(reservation) = holding else {
            return Ok(RemoteAckWatchHold::Cancelled);
        };
        Ok(RemoteAckWatchHold::Charged(Self {
            reservation,
            watches: watches.get(),
        }))
    }

    /// Return the share of one watch that ended. The task's share stays until the charge drops.
    pub(super) fn release_watch(self) -> Self {
        let Self {
            reservation,
            watches,
        } = self;
        let watches = watches
            .checked_sub(1)
            .assured("a watcher returns each watch's share once, when that watch ends");
        let (ended, kept) = reservation.split(WATCH_BYTES).assured(
            "the charge holds one share for every watch it still covers, beside the task's",
        );
        drop(ended);
        Self {
            reservation: kept,
            watches,
        }
    }

    /// The relay memory this charge holds now.
    #[cfg(test)]
    pub(super) fn bytes_held(&self) -> u64 {
        self.reservation.bytes()
    }
}

#[cfg(test)]
#[path = "remote_ack_watch_charge_tests.rs"]
mod tests;
