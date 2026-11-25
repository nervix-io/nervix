//! How a receiver's acknowledgement watch stands with the run of the node that registered the
//! acknowledgement it reports, as the receiver's live view and membership show that run.
//!
//! Layer: data plane.
//!
//! - **Owns.** The watch's view of its registrar's run, undiscovered or seen, and what each look
//!   decides: report, or end the watch.
//! - **Depends on.** Process identities, the monotonic clock and the delivery's admission bound.
//! - **Must not know.** Gossip, the failure detector, acknowledgement trees, the interconnect or the
//!   rows a watch reports.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a receiver looks at its registrar's run at every report of every \
                  acknowledgement watch"
    )
)]

use super::*;

/// Where a receiver's acknowledgement watch stands with the run of the node that registered the
/// acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegistrarPresence {
    /// The run has not appeared in the live view yet. A run that has only just joined reaches
    /// this node over the transport before gossip lists it, so the watch reports to it until
    /// `deadline`, the admission bound of the delivery that carried the acknowledgement.
    Undiscovered { deadline: Instant },
    /// The live view listed the run at an earlier look.
    Seen,
}

/// What a watch does after one look at its registrar's run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegistrarLook {
    /// Report the acknowledgement to the registrar.
    Report,
    /// End the watch: nothing it could still send reaches a run that waits for it.
    Release,
}

impl RegistrarPresence {
    /// A watch that starts at `now`, before its registrar's run has appeared in the live view,
    /// carried by a delivery whose admission bound is `discovery_bound`.
    pub(super) fn undiscovered(now: Instant, discovery_bound: Duration) -> Self {
        let deadline = now
            .checked_add(discovery_bound)
            .assured("a validated admission bound fits the monotonic clock");
        Self::Undiscovered { deadline }
    }

    /// Looks at the registrar's run, `registrar`, when the live view shows `live` for its node,
    /// membership `keeps_node` or no longer lists the node, at `now`.
    ///
    /// A newer run of the node holds none of the acknowledgement, and a node membership removed
    /// is gone, so either ends the watch. A run the live view misses while membership keeps its
    /// node, as a failure detector that suspects a slow node shows it, is reported to, since
    /// membership, not the live view, decides that a node left.
    pub(super) fn look(
        &mut self,
        registrar: ClusterNodeIncarnation,
        live: Option<ClusterNodeIncarnation>,
        keeps_node: bool,
        now: Instant,
    ) -> RegistrarLook {
        if let Some(incarnation) = live {
            if incarnation > registrar {
                return RegistrarLook::Release;
            }
            if incarnation == registrar {
                *self = Self::Seen;
                return RegistrarLook::Report;
            }
        }
        // The live view misses the run, or lists only an earlier run of its node, which transport
        // admission can precede gossip learning.
        match *self {
            Self::Undiscovered { deadline } => {
                if now >= deadline {
                    return RegistrarLook::Release;
                }
                RegistrarLook::Report
            }
            Self::Seen => {
                if keeps_node {
                    return RegistrarLook::Report;
                }
                RegistrarLook::Release
            }
        }
    }
}

#[cfg(test)]
#[path = "remote_ack_registrar_tests.rs"]
mod tests;
