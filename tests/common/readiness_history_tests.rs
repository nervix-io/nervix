//! Regressions for the bounded history of one readiness wait.
//!
//! Layer: test harness.
//! - **Owns.** Synthetic readiness probe outcomes recorded at chosen times, and the checks that
//!   the history groups them into runs, keeps the last outcome whole, and stays bounded.
//! - **Depends on.** The node-liveness owner's readiness history and probe outcomes, and the
//!   status request failures a probe can end with.
//! - **Must not know.** How a probe reaches a node, or what a startup decides from its history.

use std::time::Duration;

use error_stack::Report;

use crate::{
    node_liveness::{ReadinessHistory, ReadinessProbeOutcome, ResponseKind},
    status_request::{StatusOperation, StatusRequestError},
};

/// A probe whose status request was still opening its session when its deadline passed.
fn session_pending() -> ReadinessProbeOutcome {
    ReadinessProbeOutcome::RequestFailed(Report::new(StatusRequestError::DeadlinePassed {
        operation: StatusOperation::OpenSession,
        budget: Duration::from_secs(10),
    }))
}

/// A probe whose status session ended before its command result arrived.
fn session_ended() -> ReadinessProbeOutcome {
    ReadinessProbeOutcome::RequestFailed(Report::new(StatusRequestError::SessionEnded))
}

#[test]
fn a_history_without_probes_says_none_completed() {
    let history = ReadinessHistory::new();

    assert_eq!(history.to_string(), "no readiness probe completed");
    assert_eq!(history.last().to_string(), "no readiness probe completed");
}

#[test]
fn consecutive_probes_of_one_kind_form_one_run() {
    let mut history = ReadinessHistory::new();

    history.record(Duration::from_millis(1_000), session_pending());
    history.record(Duration::from_millis(11_000), session_pending());
    history.record(Duration::from_millis(21_000), session_pending());
    history.record(
        Duration::from_millis(21_500),
        ReadinessProbeOutcome::UnsuccessfulResponse {
            response_kind: ResponseKind::Failed,
            message: "the default user is not yet installed".to_string(),
            diagnostics: Vec::new(),
        },
    );

    assert_eq!(
        history.to_string(),
        "1.0s–21.0s status request: session establishment pending at its deadline ×3; 21.5s–21.5s \
         non-ready response (Failed) ×1"
    );
    assert_eq!(
        history.last().to_string(),
        "non-ready response (Failed): the default user is not yet installed; 0 diagnostic(s)"
    );
}

#[test]
fn a_kind_that_returns_starts_a_new_run() {
    let mut history = ReadinessHistory::new();

    history.record(Duration::from_secs(1), session_ended());
    history.record(Duration::from_secs(2), session_pending());
    history.record(Duration::from_secs(3), session_ended());

    assert_eq!(
        history.to_string(),
        "1.0s–1.0s status request: response receive failed ×1; 2.0s–2.0s status request: session \
         establishment pending at its deadline ×1; 3.0s–3.0s status request: response receive \
         failed ×1"
    );
}

#[test]
fn a_long_history_keeps_its_first_and_latest_runs() {
    let mut history = ReadinessHistory::new();

    // Thirty runs of one probe each, alternating between two kinds of answer.
    for second in 1..=30_u64 {
        let outcome = if second % 2 == 1 {
            session_pending()
        } else {
            session_ended()
        };
        history.record(Duration::from_secs(second), outcome);
    }

    let description = history.to_string();
    assert_eq!(description.matches(" ×1").count(), 16, "{description}");
    assert!(
        description.starts_with(
            "1.0s–1.0s status request: session establishment pending at its deadline ×1"
        ),
        "{description}"
    );
    assert!(
        description.contains(
            "8.0s–8.0s status request: response receive failed ×1; 14 runs of 14 probes omitted; \
             23.0s–23.0s"
        ),
        "{description}"
    );
    assert!(
        description.ends_with("30.0s–30.0s status request: response receive failed ×1"),
        "{description}"
    );
}
