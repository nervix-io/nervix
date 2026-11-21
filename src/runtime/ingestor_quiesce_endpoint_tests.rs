//! Endpoint pause buffer checks.
//!
//! Layer: test harness.
//! - **Owns.** Assertions about retained endpoint payloads and the active pause response policy.
//! - **Depends on.** The production quiesce control and the runtime's test fixtures.
//! - **Must not know.** Transport framing or socket delivery.

use nervix_connector::IngestMetadataRow;
use nervix_models::{EndpointPauseResponse, IngestQuiesceMode, Timestamp};

use super::{
    BufferedIngestMetadata, BufferedIngestPayload, EndpointPauseIntake, IngestorQuiesceCause,
    IngestorQuiesceIntake,
};
use crate::runtime::{
    Runtime,
    test_fixtures::{domain, named, test_ingestor_quiesce_control},
};

#[test]
fn endpoint_quiesce_buffer_rejects_overflow_without_discarding_acknowledged_payloads() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let ingestor = named("paused_ingestor");
    let control = test_ingestor_quiesce_control(
        &runtime,
        &domain,
        &ingestor,
        IngestQuiesceMode::EndpointBuffer {
            max_size: "5B".to_string(),
            response: nervix_models::EndpointPauseResponse::Immediate,
        },
    );
    control.engage(IngestorQuiesceCause::EntityHold);

    assert!(matches!(
        control.intake(
            BufferedIngestPayload::new(
                b"kept",
                BufferedIngestMetadata::without_headers(),
                Timestamp::from_unix_nanos(1),
            ),
            true,
        ),
        IngestorQuiesceIntake::Buffered
    ));
    assert!(matches!(
        control.intake(
            BufferedIngestPayload::new(
                b"no",
                BufferedIngestMetadata::without_headers(),
                Timestamp::from_unix_nanos(2),
            ),
            true,
        ),
        IngestorQuiesceIntake::Rejected
    ));
    assert_eq!(control.counters().buffered_records, 1);
    assert_eq!(control.counters().rejected_total, 1);
    assert_eq!(control.counters().dropped_total, 0);

    control.release(IngestorQuiesceCause::EntityHold);
    assert_eq!(
        control
            .take_buffered()
            .expect("the acknowledged payload must remain buffered")
            .finish()
            .payload(),
        b"kept"
    );
}

#[test]
fn a_replacement_can_still_hold_replies_under_the_pauses_original_policy() {
    let runtime = Runtime::default();
    let control = test_ingestor_quiesce_control(
        &runtime,
        &domain("default"),
        &named("paused_ingestor"),
        IngestQuiesceMode::EndpointBuffer {
            max_size: "1KiB".to_string(),
            response: EndpointPauseResponse::AfterResume,
        },
    );
    control.engage(IngestorQuiesceCause::EntityHold);
    control.update_declared_mode(
        &IngestQuiesceMode::Reject {
            retry_after: "1s".to_string(),
        },
        true,
    );

    assert!(control.has_endpoint_pause_replies());
    let metadata = IngestMetadataRow::headers(
        &nervix_connector::NoIngestHeaders,
        &nervix_connector::NoIngestMetadata(4),
    );
    let EndpointPauseIntake::Waiting(wait) =
        control.pause_endpoint(b"held", metadata, Timestamp::from_unix_nanos(1), None)
    else {
        panic!("the pause still holds the response");
    };
    control.release(IngestorQuiesceCause::EntityHold);
    assert!(control.has_endpoint_pause_replies(), "replay is pending");
    drop(wait);
    assert!(!control.has_endpoint_pause_replies());
}
