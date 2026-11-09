//! The runtime ingestor host's acknowledgement and lifecycle observations.
//!
//! Layer: test harness.
//! - **Owns.** Checks that the production host records every observation into its unit's retained
//!   children, reports a failed and a timed-out tree as distinct events, and projects its transient
//!   status only while it lives.
//! - **Depends on.** The production runtime ingestor host and the extension-worker domain fixture.
//! - **Must not know.** Broker drivers, the ingestor loops or the metric registry's internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::{
    extension_tests::{UnfoldingDomain, WAITING_INTAKE},
    *,
};

/// The rendered `source:` lines of the fixture's ingestor unit.
fn source_lines(fixture: &UnfoldingDomain) -> Vec<String> {
    let unit = DomainNodeUnitRef::new(
        DomainNodeRef::node_in(
            fixture.domain.clone(),
            ModelKind::Ingestor,
            fixture.ingestor.clone(),
        ),
        Unit::SINGLE,
    );
    fixture
        .runtime
        .inner
        .metrics
        .describe_ingestor_interactions(&unit)
}

/// Whether `lines` hold the line of `family` whose selector ends with `selector_end` and whose
/// rendered value is `value`.
fn shows(lines: &[String], family: &str, selector_end: &str, value: &str) -> bool {
    let prefix = format!("{family}{{");
    let suffix = format!("{selector_end}}} {value}");
    lines
        .iter()
        .any(|line| line.starts_with(&prefix) && line.ends_with(&suffix))
}

/// The next error the runtime reports, within a bound a single in-process report never needs.
async fn next_error(events: &mut broadcast::Receiver<RuntimeEvent>) -> String {
    let event = nervix_primitives::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("the host reports its event at once")
        .expect("the bus keeps every event of this check");
    let RuntimeEvent::Error(message) = event;
    message
}

#[nervix_primitives::test]
async fn the_runtime_host_counts_every_observation_and_names_a_timed_out_tree_a_timeout() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut events = fixture.runtime.events().subscribe();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    host.record_acknowledgement(&IngestorAcknowledgementOutcome::Acknowledged);
    host.record_acknowledgement(&IngestorAcknowledgementOutcome::Failed {
        reason: "the emitter refused the record".to_string(),
    });
    host.record_acknowledgement(&IngestorAcknowledgementOutcome::TimedOut {
        silence: Duration::from_secs(2),
    });
    host.record_acknowledgement(&IngestorAcknowledgementOutcome::Shutdown);
    host.record_transport_settlement(
        IngestorSettlementOperation::Reject,
        IngestorSettlementOutcome::Succeeded,
        Duration::from_millis(5),
    );
    host.record_resume(IngestorResumeOutcome::Failed);
    host.record_suspend(IngestorSuspendCause::Rejection);
    host.record_unconfirmed_stop(2);

    assert_eq!(
        next_error(&mut events).await,
        "ingestor ACK chain failed for ingestor 'event_source' in domain 'default': the emitter \
         refused the record"
    );
    assert_eq!(
        next_error(&mut events).await,
        "ingestor ACK chain for ingestor 'event_source' in domain 'default' timed out: no \
         acknowledgement progress within ACK TIMEOUT 2s"
    );
    let lines = source_lines(&fixture);
    for outcome in ["acknowledged", "failed", "timed_out", "shutdown"] {
        assert!(
            shows(
                &lines,
                "nervix_ingestor_ack_outcomes_total",
                &format!(",outcome=\"{outcome}\""),
                "total=1"
            ),
            "{outcome} is counted once: {lines:#?}"
        );
    }
    assert!(shows(
        &lines,
        "nervix_ingestor_transport_acknowledgements_total",
        ",operation=\"reject\",outcome=\"succeeded\"",
        "total=1"
    ));
    assert!(shows(
        &lines,
        "nervix_ingestor_transport_acknowledgement_duration_seconds",
        ",operation=\"reject\"",
        "count=1 sum_seconds=0.005"
    ));
    assert!(shows(
        &lines,
        "nervix_ingestor_resumes_total",
        ",outcome=\"failed\"",
        "total=1"
    ));
    assert!(shows(
        &lines,
        "nervix_ingestor_suspends_total",
        ",cause=\"rejection\"",
        "total=1"
    ));
    assert!(shows(
        &lines,
        "nervix_ingestor_unconfirmed_stop_positions_total",
        "",
        "total=2"
    ));

    host.record_transient_error("the broker is gone".to_string(), Duration::from_millis(100));
    assert!(shows(
        &source_lines(&fixture),
        "nervix_ingestor_transient_failure",
        "",
        "value=1"
    ));
    host.clear_transient_error();
    assert!(shows(
        &source_lines(&fixture),
        "nervix_ingestor_transient_failure",
        "",
        "value=0"
    ));
    host.record_transient_error("the broker is gone".to_string(), Duration::from_millis(100));
    drop(host);
    assert!(
        shows(
            &source_lines(&fixture),
            "nervix_ingestor_transient_failure",
            "",
            "value=0"
        ),
        "an ended host no longer projects the failure its unit still publishes"
    );

    fixture.finish().await;
}
