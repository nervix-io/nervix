//! The bounded response window of one endpoint intake.
//!
//! Layer: data plane.
//! - **Owns.** Request-window admission, acknowledgement outcomes, their public projection and the
//!   outcome an ingestor counts them under.
//! - **Depends on.** Validated endpoint plans, shared client outcomes, the endpoint request outcome
//!   vocabulary and execution primitives.
//! - **Must not know.** HTTP framing, source registration, configuration Models or graph decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns endpoint response admission")
)]

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClientOutcomeUncertainty, ClientProcessingFailure, ClientSubmissionOutcome,
    ClientSubmissionRefusal,
};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::EndpointRequestPlan;
use crate::metrics::EndpointIntakeOutcome;

pub(super) enum EndpointRequestPolicy {
    NoAck,
    Ack(EndpointRequestWindow),
}

pub(super) struct EndpointRequestWindow {
    capacity: Arc<EndpointRequestCapacity>,
    max: usize,
    pub(super) timeout: Duration,
    pub(super) retry_after: Option<Duration>,
}

/// The outstanding responses of a logical ingestor, shared across replacements during a pause.
#[derive(Debug, Default)]
pub(super) struct EndpointRequestCapacity {
    outstanding: AtomicUsize,
}

pub(super) struct EndpointResponseSlot<'a> {
    capacity: &'a EndpointRequestCapacity,
}

impl Drop for EndpointResponseSlot<'_> {
    fn drop(&mut self) {
        self.capacity.outstanding.fetch_sub(1, Ordering::Relaxed);
    }
}

impl EndpointRequestPolicy {
    pub(super) fn new(plan: &EndpointRequestPlan, capacity: &Arc<EndpointRequestCapacity>) -> Self {
        match plan {
            EndpointRequestPlan::NoAck => Self::NoAck,
            EndpointRequestPlan::Ack {
                max,
                timeout,
                retry_after,
            } => Self::Ack(EndpointRequestWindow {
                capacity: capacity.clone(),
                max: max.get(),
                timeout: *timeout,
                retry_after: *retry_after,
            }),
        }
    }
    pub(super) fn has_capacity(&self) -> bool {
        match self {
            Self::NoAck => true,
            Self::Ack(window) => window.capacity.outstanding.load(Ordering::Relaxed) < window.max,
        }
    }
}

impl EndpointRequestWindow {
    pub(super) fn try_admit(&self) -> Option<EndpointResponseSlot<'_>> {
        let mut outstanding = self.capacity.outstanding.load(Ordering::Relaxed);
        while outstanding < self.max {
            let next = outstanding
                .checked_add(1)
                .assured("the window bounds the outstanding count");
            match self.capacity.outstanding.compare_exchange_weak(
                outstanding,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(EndpointResponseSlot {
                        capacity: &self.capacity,
                    });
                }
                Err(current) => outstanding = current,
            }
        }
        None
    }

    #[cfg(all(test, feature = "shuttle"))]
    fn available_slots(&self) -> usize {
        nervix_primitives::expect_lint!(
            nervix::clamped_arithmetic,
            "a replacement can reduce the maximum below the outstanding count, leaving no slots",
            self.max
                .saturating_sub(self.capacity.outstanding.load(Ordering::Relaxed))
        )
    }

    /// Holds the response slot until graph work completes, the intake ends, or its physical
    /// deadline expires. The caller supplies those events; checks drive the same owner directly.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the host supplies graph work and the intake's ending and configured deadline"
        )
    )]
    #[cfg(all(test, feature = "shuttle"))]
    pub(super) async fn run_until(
        &self,
        ending: impl Future<Output = ()>,
        deadline: impl Future<Output = ()>,
        dispatch: impl Future<Output = EndpointRequestOutcome>,
    ) -> EndpointRequestOutcome {
        let Some(_permit) = self.try_admit() else {
            return EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::WindowFull);
        };
        Self::run_admitted_until(ending, deadline, dispatch).await
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "waits for graph work, the intake's ending and the request's deadline"
        )
    )]
    pub(super) async fn run_admitted_until(
        ending: impl Future<Output = ()>,
        deadline: impl Future<Output = ()>,
        dispatch: impl Future<Output = EndpointRequestOutcome>,
    ) -> EndpointRequestOutcome {
        nervix_primitives::select! {
            biased;
            outcome = dispatch => outcome,
            _ = ending => EndpointRequestOutcome::Interrupted,
            _ = deadline => EndpointRequestOutcome::ProcessingFailed(ClientProcessingFailure::AckTimedOut),
        }
    }
}

/// The local refusal of an input is distinct from a failure after downstream admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointRequestOutcome {
    Completed,
    Refused(EndpointInputRefusal),
    NotAdmitted(EndpointAdmissionRefusal),
    ProcessingFailed(ClientProcessingFailure),
    Interrupted,
}

/// What refused the input of a request before any of it entered the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointInputRefusal {
    /// The ingestor's codec refused the payload, which the ingestor consumed as a decode rejection.
    Codec,
    /// A route of the ingestor refused the input its codec decoded.
    Route,
}

/// Why an acknowledged endpoint ingestor did not admit a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointAdmissionRefusal {
    /// The ingestor is quiesced and refuses new requests.
    Suspended,
    /// Every slot of the ingestor's response window is taken.
    WindowFull,
    /// The node's extension workers had no room to unfold the payload.
    UnfoldingRefused,
    /// The endpoint's pause buffer has no space for this request.
    PauseBufferFull,
    /// The response deadline elapsed before a buffered request entered the graph.
    PauseExpired,
    /// The intake ended before the buffered request entered the graph.
    IntakeEnded,
}

impl EndpointAdmissionRefusal {
    /// The refusal the request's sender learns, in the client outcome vocabulary: the window and
    /// the extension workers are both capacity the node lacks right now.
    fn submission(self) -> ClientSubmissionRefusal {
        match self {
            Self::Suspended | Self::PauseExpired | Self::IntakeEnded => {
                ClientSubmissionRefusal::Suspended
            }
            Self::WindowFull | Self::UnfoldingRefused | Self::PauseBufferFull => {
                ClientSubmissionRefusal::Busy
            }
        }
    }
}

impl EndpointRequestOutcome {
    /// Representatives of the replies a signaling protocol must encode.
    pub(super) fn replies() -> [Self; 9] {
        [
            Self::Completed,
            Self::Refused(EndpointInputRefusal::Codec),
            Self::Refused(EndpointInputRefusal::Route),
            Self::NotAdmitted(EndpointAdmissionRefusal::Suspended),
            Self::NotAdmitted(EndpointAdmissionRefusal::WindowFull),
            Self::NotAdmitted(EndpointAdmissionRefusal::UnfoldingRefused),
            Self::ProcessingFailed(ClientProcessingFailure::AckTimedOut),
            Self::ProcessingFailed(ClientProcessingFailure::Rejected),
            Self::Interrupted,
        ]
    }

    pub(crate) fn submission(self) -> ClientSubmissionOutcome {
        match self {
            Self::Completed => ClientSubmissionOutcome::Completed,
            Self::Refused(_) => {
                ClientSubmissionOutcome::ProcessingFailed(ClientProcessingFailure::Rejected)
            }
            Self::NotAdmitted(refusal) => {
                ClientSubmissionOutcome::NotAdmitted(refusal.submission())
            }
            Self::ProcessingFailed(failure) => ClientSubmissionOutcome::ProcessingFailed(failure),
            Self::Interrupted => {
                ClientSubmissionOutcome::OutcomeUnknown(ClientOutcomeUncertainty::Interrupted)
            }
        }
    }

    pub(crate) fn pause_buffer_full(self) -> bool {
        matches!(
            self,
            Self::NotAdmitted(EndpointAdmissionRefusal::PauseBufferFull)
        )
    }

    /// The outcome the ingestor counts the request under.
    pub(super) fn counted(self) -> EndpointIntakeOutcome {
        match self {
            Self::Completed => EndpointIntakeOutcome::Accepted,
            Self::Refused(EndpointInputRefusal::Codec) => EndpointIntakeOutcome::DecodeRejected,
            Self::Refused(EndpointInputRefusal::Route)
            | Self::NotAdmitted(EndpointAdmissionRefusal::Suspended) => {
                EndpointIntakeOutcome::Refused
            }
            Self::NotAdmitted(
                EndpointAdmissionRefusal::WindowFull | EndpointAdmissionRefusal::PauseBufferFull,
            ) => EndpointIntakeOutcome::Busy,
            Self::NotAdmitted(EndpointAdmissionRefusal::UnfoldingRefused) => {
                EndpointIntakeOutcome::NotAdmitted
            }
            Self::NotAdmitted(
                EndpointAdmissionRefusal::PauseExpired | EndpointAdmissionRefusal::IntakeEnded,
            ) => EndpointIntakeOutcome::NotAdmitted,
            Self::ProcessingFailed(_) | Self::Interrupted => {
                EndpointIntakeOutcome::ProcessingFailed
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EndpointRequestReply {
    pub(crate) outcome: EndpointRequestOutcome,
    /// The declared retry delay, used only for an unavailable response.
    pub(crate) retry_after: Option<Duration>,
}

impl EndpointRequestReply {
    /// HTTP and signaling use the labels of the one client batch-outcome vocabulary.
    pub(crate) fn value(self) -> serde_json::Value {
        let outcome = self.outcome.submission();
        serde_json::json!({ "outcome": outcome.class_label(), "cause": outcome.cause_label() })
    }

    pub(crate) fn body(self) -> Vec<u8> {
        serde_json::to_vec(&self.value())
            .assured("the outcome projection contains only two JSON strings")
    }
}

#[cfg(test)]
#[path = "endpoint_acknowledgement_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "endpoint_acknowledgement_shuttle_tests.rs"]
mod shuttle_tests;
