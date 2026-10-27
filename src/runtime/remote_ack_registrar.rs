//! How a receiver's acknowledgement watch stands with the run of the node that registered the
//! acknowledgement it reports, as the receiver's live view shows that run.
//!
//! Layer: data plane.
//!
//! - **Owns.** The watch's view of its registrar's run, undiscovered, present or missing since a
//!   known instant, and what each look at the live view decides: report, hold the acknowledgement
//!   while the run is missing, or end the watch.
//! - **Depends on.** Process identities, the monotonic clock and the registering node's silence
//!   bound.
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

use super::{
    remote_dispatch::{
        REMOTE_ACK_SILENCE_SWEEP_INTERVAL, REMOTE_ACK_SILENCE_TIMEOUT,
        REMOTE_RELAY_INSTANTIATION_WAIT,
    },
    *,
};

/// Where a receiver's acknowledgement watch stands with the run of the node that registered the
/// acknowledgement, as the receiver's live view shows that run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegistrarPresence {
    /// The run has not appeared in the live view yet, and may until `deadline`.
    Undiscovered { deadline: Instant },
    /// The run was in the live view at the watch's last look.
    Present,
    /// The run, seen live before, has been missing from the live view since `since`.
    Missing { since: Instant },
}

/// What a watch does after one look at its registrar's run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegistrarLook {
    /// Report the acknowledgement to the registrar.
    Report,
    /// Keep the acknowledgement and report nothing while the registrar's run is missing.
    Hold,
    /// End the watch: nothing it could still send reaches a run that waits for it.
    Release,
}

impl RegistrarPresence {
    /// A watch that starts at `now`, before its registrar's run has appeared in the live view.
    pub(super) fn undiscovered(now: Instant) -> Self {
        let deadline = now
            .checked_add(REMOTE_RELAY_INSTANTIATION_WAIT)
            .assured("the bounded registrar discovery grace fits the monotonic clock");
        Self::Undiscovered { deadline }
    }

    /// How long a run the watch has seen live may stay missing from the live view before the
    /// watch ends. A registering node whose run goes on has failed the acknowledgement by then: it
    /// fails one whose receiver it has not heard from for its silence bound, at its first sweep
    /// after that bound.
    pub(super) fn missing_grace() -> Duration {
        REMOTE_ACK_SILENCE_TIMEOUT
            .checked_add(REMOTE_ACK_SILENCE_SWEEP_INTERVAL)
            .assured("the silence bound and one sweep interval are a few seconds each")
    }

    /// Looks at the registrar's run, `registrar`, when the live view shows `live` for its node at
    /// `now`.
    pub(super) fn look(
        &mut self,
        registrar: ClusterNodeIncarnation,
        live: Option<ClusterNodeIncarnation>,
        now: Instant,
    ) -> RegistrarLook {
        let Some(incarnation) = live else {
            return self.missed(now);
        };
        if incarnation != registrar {
            // Another run of the node replaced the registrar, and nothing in it holds this
            // acknowledgement.
            return RegistrarLook::Release;
        }
        *self = Self::Present;
        RegistrarLook::Report
    }

    /// A look at `now` that found no run of the registrar's node in the live view.
    fn missed(&mut self, now: Instant) -> RegistrarLook {
        match *self {
            // A run that has only just joined may not be in this node's live view yet, while the
            // transport already reaches it.
            Self::Undiscovered { deadline } => {
                if now >= deadline {
                    return RegistrarLook::Release;
                }
                RegistrarLook::Report
            }
            // A failure detector that suspects a slow node drops its run from the live view while
            // the run goes on, so a run seen live is held for rather than given up.
            Self::Present => {
                *self = Self::Missing { since: now };
                RegistrarLook::Hold
            }
            Self::Missing { since } => {
                let missing_for = now
                    .checked_duration_since(since)
                    .assured("the watch read `since` on the monotonic clock at an earlier look");
                if missing_for >= Self::missing_grace() {
                    return RegistrarLook::Release;
                }
                RegistrarLook::Hold
            }
        }
    }
}

#[cfg(test)]
#[path = "remote_ack_registrar_tests.rs"]
mod tests;
