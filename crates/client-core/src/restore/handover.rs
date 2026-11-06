//! The final archive frame and the ending of its restore attempt.
//!
//! Layer: edges.
//! - **Owns.** One attempt's permission to hand its final declared archive chunk to transport.
//! - **Depends on.** The selected atomic boundary.
//! - **Must not know.** Archive bytes, transport scheduling, server admission, or graph execution.
//!
//! The private atomic encoding is open (0), handed over (1), or finished (2). Only open can
//! grant handover; ending consumes either live state once. These tags are representation, not
//! operational policy, and never leave this owner.

use nervix_primitives::sync::atomic::{AtomicU8, Ordering};

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the private handover encoding reserves 0 for permission to forward the final \
                  frame"
    )
)]
const OPEN: u8 = 0;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the private handover encoding reserves 1 for a final frame whose forwarding was \
                  permitted"
    )
)]
const HANDED_OVER: u8 = 1;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the private handover encoding reserves 2 for an ended attempt that denies any \
                  later final frame"
    )
)]
const FINISHED: u8 = 2;

const _: () = assert!(OPEN != HANDED_OVER && OPEN != FINISHED && HANDED_OVER != FINISHED);

pub(super) struct RestoreHandover {
    state: AtomicU8,
}

impl RestoreHandover {
    pub(super) fn new() -> Self {
        Self {
            state: AtomicU8::new(OPEN),
        }
    }

    /// The final frame is forwarded only when its handover precedes the attempt's ending.
    pub(super) fn try_handover(&self) -> bool {
        // Both decisions modify this one atomic. No payload is published and no task is woken.
        self.state
            .compare_exchange(OPEN, HANDED_OVER, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// Ends this attempt once, retaining whether its complete archive may reach the peer.
    pub(super) fn finish(&self) -> bool {
        self.state.swap(FINISHED, Ordering::Relaxed) == HANDED_OVER
    }
}
