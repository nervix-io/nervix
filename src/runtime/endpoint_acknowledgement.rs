//! The bounded response window of one endpoint intake.
//!
//! Layer: data plane.
//! - **Owns.** Request-window admission, acknowledgement outcomes, their public projection and the
//!   outcome an ingestor counts them under.
//! - **Depends on.** Validated endpoint plans, shared client outcomes, the endpoint request outcome
//!   vocabulary and primitive semaphores.
//! - **Must not know.** HTTP framing, source registration, configuration Models or graph decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns endpoint response admission")
)]

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_models::{
    ClientOutcomeUncertainty, ClientProcessingFailure, ClientSubmissionOutcome,
    ClientSubmissionRefusal,
};
use nervix_primitives::sync::{Semaphore, SemaphorePermit};

use super::EndpointRequestPlan;
use crate::metrics::EndpointIntakeOutcome;

pub(super) enum EndpointRequestPolicy {
    NoAck,
    Ack(EndpointRequestWindow),
}

pub(super) struct EndpointRequestWindow {
    permits: Semaphore,
    pub(super) timeout: Duration,
    pub(super) retry_after: Duration,
}

impl From<&EndpointRequestPlan> for EndpointRequestPolicy {
    fn from(plan: &EndpointRequestPlan) -> Self {
        match plan {
            EndpointRequestPlan::NoAck => Self::NoAck,
            EndpointRequestPlan::Ack {
                max,
                timeout,
                retry_after,
            } => Self::Ack(EndpointRequestWindow {
                permits: Semaphore::new(max.get()),
                timeout: *timeout,
                retry_after: *retry_after,
            }),
        }
    }
}

impl EndpointRequestPolicy {
    pub(super) fn has_capacity(&self) -> bool {
        match self {
            Self::NoAck => true,
            Self::Ack(window) => window.permits.available_permits() > 0,
        }
    }
}

impl EndpointRequestWindow {
    pub(super) fn try_admit(&self) -> Option<SemaphorePermit<'_>> {
        self.permits.try_acquire().ok()
    }

    /// Holds the response slot until graph work completes, the intake ends, or its physical
    /// deadline expires. The caller supplies those events; checks drive the same owner directly.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the host supplies graph work and the intake's ending and configured deadline"
        )
    )]
    pub(super) async fn run_until(
        &self,
        ending: impl Future<Output = ()>,
        deadline: impl Future<Output = ()>,
        dispatch: impl Future<Output = EndpointRequestOutcome>,
    ) -> EndpointRequestOutcome {
        let Some(_permit) = self.try_admit() else {
            return EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::WindowFull);
        };
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
}

impl EndpointAdmissionRefusal {
    /// The refusal the request's sender learns, in the client outcome vocabulary: the window and
    /// the extension workers are both capacity the node lacks right now.
    fn submission(self) -> ClientSubmissionRefusal {
        match self {
            Self::Suspended => ClientSubmissionRefusal::Suspended,
            Self::WindowFull | Self::UnfoldingRefused => ClientSubmissionRefusal::Busy,
        }
    }
}

impl EndpointRequestOutcome {
    /// Every outcome a request can reach, whose replies a signaling protocol must encode.
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

    /// The outcome the ingestor counts the request under.
    pub(super) fn counted(self) -> EndpointIntakeOutcome {
        match self {
            Self::Completed => EndpointIntakeOutcome::Accepted,
            Self::Refused(EndpointInputRefusal::Codec) => EndpointIntakeOutcome::DecodeRejected,
            Self::Refused(EndpointInputRefusal::Route)
            | Self::NotAdmitted(EndpointAdmissionRefusal::Suspended) => {
                EndpointIntakeOutcome::Refused
            }
            Self::NotAdmitted(EndpointAdmissionRefusal::WindowFull) => EndpointIntakeOutcome::Busy,
            Self::NotAdmitted(EndpointAdmissionRefusal::UnfoldingRefused) => {
                EndpointIntakeOutcome::NotAdmitted
            }
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
    pub(crate) retry_after: Duration,
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
