//! A charge of the relay memory budget that waits for room instead of being refused.
//!
//! Layer: data plane.
//!
//! - **Owns.** The wait of relay work for room in the relay budget, how often it reads the budget
//!   while it waits, how the wait ends when its work is cancelled, and the bound it waits within.
//! - **Depends on.** The execution budgets and the primitive boundary's timers.
//! - **Must not know.** What the charge pays for: acknowledgement watches, record storage, relays,
//!   deliveries or the interconnect protocol.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "remote deliveries charge their acknowledgement watches and record storage \
                  through this hold"
    )
)]

use std::future::Future;

use nervix_execution::{AdmissionError, MemoryClass, Reservation};

use super::*;

/// How long held work waits before it reads the relay budget again.
///
/// The hold reads the budget instead of waiting in its queue. A place in that queue takes every
/// byte the class releases until the place is satisfied, so a hold queued there would keep the
/// node's other relay work behind it, including the work whose completion returns the room it
/// waits for. Nothing notifies the hold of a release either: every relay operation on the node
/// releases into this class, and none of them should pay for telling a hold.
pub(super) const RECHECK_INTERVAL: Duration = Duration::from_millis(10);

/// A charge of `bytes` of relay memory that waits for room until `deadline`.
#[derive(Debug, Clone, Copy)]
pub(super) struct RelayMemoryHold {
    bytes: u64,
    deadline: Instant,
}

/// How a hold for relay memory ended without a refusal.
#[derive(Debug)]
pub(super) enum RelayMemoryHolding {
    /// The relay budget took the charge.
    Charged(Reservation),
    /// The work stopped needing the charge while it was held.
    Cancelled,
}

impl RelayMemoryHold {
    pub(super) fn new(bytes: u64, deadline: Instant) -> Self {
        Self { bytes, deadline }
    }

    /// Charge the relay budget, holding the work back while the budget has no room for it.
    ///
    /// Held work is neither charged nor refused: it takes its charge once the budget has room. It
    /// ends without a charge as soon as `cancelled` resolves. A charge the whole budget could never
    /// hold is refused at once, and one still without room at its deadline is refused with the
    /// budget's last refusal.
    pub(super) async fn charge(
        self,
        executor: &Executor,
        cancelled: impl Future<Output = ()>,
    ) -> Result<RelayMemoryHolding, Report<AdmissionError>> {
        let mut cancelled = std::pin::pin!(cancelled);
        loop {
            nervix_primitives::task::consume_budget().await;
            let memory = executor.memory(MemoryClass::Relay);
            let room = memory
                .capacity_bytes
                .checked_sub(memory.reserved_bytes)
                .assured("a budget never reserves more than its capacity");
            let expired = Instant::now() >= self.deadline;
            // Only a charge that fits, one the class could never hold, or one whose hold is over
            // asks the class. The class counts a refusal for each charge it turns away, and a
            // held charge has not been turned away.
            if room >= self.bytes || self.bytes > memory.capacity_bytes || expired {
                match executor.try_reserve(MemoryClass::Relay, self.bytes) {
                    Ok(reservation) => return Ok(RelayMemoryHolding::Charged(reservation)),
                    Err(refusal) => {
                        let exhausted = matches!(
                            refusal.current_context(),
                            AdmissionError::BudgetExhausted { .. }
                        );
                        // Other relay work took the room between the read and the request; the
                        // hold goes on unless it is over.
                        if expired || !exhausted {
                            return Err(refusal);
                        }
                    }
                }
            }
            let recheck = Instant::now()
                .checked_add(RECHECK_INTERVAL)
                .assured("the fixed recheck interval fits the monotonic clock")
                .min(self.deadline);
            nervix_primitives::select! {
                biased;
                () = &mut cancelled => return Ok(RelayMemoryHolding::Cancelled),
                () = sleep_until(recheck) => {}
            }
        }
    }
}
