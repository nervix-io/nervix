//! Tests of cut-phase measurements and their typed timeout projection.

use std::time::Duration;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{CommandExecutionReference, DomainName};
use nervix_primitives::time::Instant;

use super::*;

fn progress_at(started: Instant) -> CutProgress {
    CutProgress::at(
        DomainName::parse("cut_measurement").assured("the test domain is valid"),
        CaptureContext::Coordinator {
            execution: CommandExecutionReference::parse("00000000-0000-7000-8000-000000000001")
                .assured("the execution reference is valid"),
        },
        CutPhase::MutationLease,
        started,
    )
}

#[test]
fn a_capture_timeout_retains_every_completed_phase_and_its_own_age() {
    let started = Instant::now();
    let mut progress = progress_at(started);
    progress.enter_at(
        CutPhase::ConfigurationRead,
        started + Duration::from_secs(1),
    );
    progress.enter_at(CutPhase::OpeningCounters, started + Duration::from_secs(3));
    progress.enter_at(CutPhase::Drain, started + Duration::from_secs(4));
    progress.enter_at(CutPhase::Capture, started + Duration::from_secs(7));

    let error = progress.timeout_at(started + Duration::from_secs(10));
    let BackupError::CaptureTimeout {
        domain,
        phase,
        elapsed,
        phase_elapsed,
        completed,
    } = &error
    else {
        panic!("a cut expiry is a typed capture timeout");
    };
    assert_eq!(domain.as_str(), "cut_measurement");
    assert_eq!(*phase, CutPhase::Capture);
    assert_eq!(*elapsed, Duration::from_secs(10));
    assert_eq!(*phase_elapsed, Duration::from_secs(3));
    assert_eq!(
        completed.phases,
        vec![
            PhaseTiming {
                phase: CutPhase::MutationLease,
                elapsed: Duration::from_secs(1)
            },
            PhaseTiming {
                phase: CutPhase::ConfigurationRead,
                elapsed: Duration::from_secs(2)
            },
            PhaseTiming {
                phase: CutPhase::OpeningCounters,
                elapsed: Duration::from_secs(1)
            },
            PhaseTiming {
                phase: CutPhase::Drain,
                elapsed: Duration::from_secs(3)
            },
        ]
    );
    assert_eq!(
        format!("{}", Report::new(error)),
        "timed out waiting to capture domain 'cut_measurement': phase=capture, elapsed=10s, \
         phase_elapsed=3s, completed=[mutation_lease=1s, configuration_read=2s, \
         opening_counters=1s, drain=3s]"
    );
}

#[test]
fn a_lease_timeout_has_no_completed_phase_and_keeps_its_typed_cause() {
    let started = Instant::now();
    let progress = progress_at(started);
    let cause = std::io::Error::from(std::io::ErrorKind::Interrupted);
    let report =
        Report::new(cause).change_context(progress.timeout_at(started + Duration::from_secs(5)));
    let BackupError::CaptureTimeout {
        phase,
        elapsed,
        phase_elapsed,
        completed,
        ..
    } = report.current_context()
    else {
        panic!("lease admission expiry carries the cut timeout");
    };
    assert_eq!(*phase, CutPhase::MutationLease);
    assert_eq!(*elapsed, Duration::from_secs(5));
    assert_eq!(*phase_elapsed, Duration::from_secs(5));
    assert!(completed.phases.is_empty());
    assert_eq!(
        report
            .downcast_ref::<std::io::Error>()
            .map(std::io::Error::kind),
        Some(std::io::ErrorKind::Interrupted)
    );
}

#[test]
fn a_drain_timeout_keeps_the_outstanding_work_report() {
    let mut progress = progress_at(Instant::now());
    progress.enter(CutPhase::Drain);
    let report = Report::new(DomainAlterError::BackupQuiesceTimeout {
        domain: progress.domain.clone(),
        details: "publishing_emitters=1".to_string(),
    });
    let report = progress.drain_failure(report);
    let BackupError::CaptureTimeout { phase, .. } = report.current_context() else {
        panic!("the drain's budget expiry is attributed to its phase");
    };
    assert_eq!(*phase, CutPhase::Drain);
    let Some(DomainAlterError::BackupQuiesceTimeout { domain, details }) =
        report.downcast_ref::<DomainAlterError>()
    else {
        panic!("the typed drain report remains beneath the cut timeout");
    };
    assert_eq!(domain, &progress.domain);
    assert_eq!(details, "publishing_emitters=1");
    assert!(format!("{report:#}").contains("publishing_emitters=1"));
}

#[test]
fn a_pause_failure_keeps_its_cause_and_failure_class() {
    let mut progress = progress_at(Instant::now());
    progress.enter(CutPhase::Drain);
    let report = Report::new(DomainAlterError::PauseDomain {
        domain: progress.domain.clone(),
    });
    let report = progress.drain_failure(report);
    let BackupError::CaptureDomain { domain, .. } = report.current_context() else {
        panic!("a refused pause remains a capture failure");
    };
    assert_eq!(domain, &progress.domain);
    let Some(DomainAlterError::PauseDomain { domain }) = report.downcast_ref::<DomainAlterError>()
    else {
        panic!("the typed pause cause remains available");
    };
    assert_eq!(domain, &progress.domain);
}

#[test]
fn a_pause_application_the_deadline_ended_is_the_cut_timeout_and_keeps_the_wait_it_ended() {
    let mut progress = progress_at(Instant::now());
    progress.enter(CutPhase::Drain);
    let wait = crate::runtime::ApplicationWait::Readiness { revision: 115 };
    let report = Report::new(crate::runtime::RuntimeError::ApplicationDeadline { wait })
        .change_context(DomainAlterError::BackupPauseTimeout {
            domain: progress.domain.clone(),
        });
    let report = progress.drain_failure(report);
    let BackupError::CaptureTimeout { phase, .. } = report.current_context() else {
        panic!("a pause application the cut's deadline ended is attributed to its drain phase");
    };
    assert_eq!(*phase, CutPhase::Drain);
    let Some(DomainAlterError::BackupPauseTimeout { domain }) =
        report.downcast_ref::<DomainAlterError>()
    else {
        panic!("the typed pause application report remains beneath the cut timeout");
    };
    assert_eq!(domain, &progress.domain);
    let Some(crate::runtime::RuntimeError::ApplicationDeadline { wait: ended }) =
        report.downcast_ref::<crate::runtime::RuntimeError>()
    else {
        panic!("the wait the deadline ended remains beneath the pause application");
    };
    assert_eq!(*ended, wait);
    assert!(
        format!("{report:#}").contains("every live node to ready runtime revision 115"),
        "the rendered refusal names the wait the deadline ended: {report:#}"
    );
}
