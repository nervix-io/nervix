//! Request outcome representation checks.
//!
//! Layer: test harness.
//! - **Owns.** Exact public outcome objects and the bounded body's representation property.
//! - **Depends on.** The production endpoint response projection and client outcome vocabulary.
//! - **Must not know.** HTTP sockets, source publication or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "checks endpoint outcome representation")
)]

use super::*;

/// This is a one-way projection: local refusal and downstream rejection deliberately have the
/// same client cause, and a full window and full extension workers are both `busy`. Retry delay is
/// an HTTP header, not part of this object.
#[test]
fn bolero_endpoint_bodies_preserve_the_complete_client_outcome_object() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            for byte in bytes {
                let (outcome, class, cause) = match byte % 9 {
                    0 => (EndpointRequestOutcome::Completed, "completed", "none"),
                    1 => (
                        EndpointRequestOutcome::Refused(EndpointInputRefusal::Codec),
                        "processing_failed",
                        "rejected",
                    ),
                    2 => (
                        EndpointRequestOutcome::Refused(EndpointInputRefusal::Route),
                        "processing_failed",
                        "rejected",
                    ),
                    3 => (
                        EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::WindowFull),
                        "not_admitted",
                        "busy",
                    ),
                    4 => (
                        EndpointRequestOutcome::NotAdmitted(
                            EndpointAdmissionRefusal::UnfoldingRefused,
                        ),
                        "not_admitted",
                        "busy",
                    ),
                    5 => (
                        EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::Suspended),
                        "not_admitted",
                        "suspended",
                    ),
                    6 => (
                        EndpointRequestOutcome::ProcessingFailed(
                            ClientProcessingFailure::AckTimedOut,
                        ),
                        "processing_failed",
                        "ack_timeout",
                    ),
                    7 => (
                        EndpointRequestOutcome::ProcessingFailed(ClientProcessingFailure::Rejected),
                        "processing_failed",
                        "rejected",
                    ),
                    _ => (
                        EndpointRequestOutcome::Interrupted,
                        "outcome_unknown",
                        "interrupted",
                    ),
                };
                let reply = EndpointRequestReply {
                    outcome,
                    retry_after: Duration::from_secs(u64::from(*byte)),
                };
                let encoded = reply.body();
                let decoded: serde_json::Value = serde_json::from_slice(&encoded)
                    .assured("every projected object is valid JSON");
                assert_eq!(
                    decoded,
                    serde_json::json!({ "outcome": class, "cause": cause })
                );
                assert!(
                    encoded.len() <= 64,
                    "the fixed labels have a bounded encoding"
                );
            }
        });
}

/// Every outcome a request can reach is counted under exactly one endpoint request outcome, which
/// separates what the client vocabulary deliberately merges.
#[test]
fn every_request_outcome_is_counted_under_its_endpoint_outcome() {
    let counted = EndpointRequestOutcome::replies().map(EndpointRequestOutcome::counted);
    assert_eq!(
        counted,
        [
            EndpointIntakeOutcome::Accepted,
            EndpointIntakeOutcome::DecodeRejected,
            EndpointIntakeOutcome::Refused,
            EndpointIntakeOutcome::Refused,
            EndpointIntakeOutcome::Busy,
            EndpointIntakeOutcome::NotAdmitted,
            EndpointIntakeOutcome::ProcessingFailed,
            EndpointIntakeOutcome::ProcessingFailed,
            EndpointIntakeOutcome::ProcessingFailed,
        ]
    );
}
