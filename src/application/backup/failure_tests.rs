//! Tests of the command outcome projected from a backup attempt's typed failure.
//!
//! Layer: test harness.
//! - **Owns.** Authority-loss recovery and definitive cut/storage refusal assertions.
//! - **Depends on.** The production backup response boundary and application fixtures.
//! - **Must not know.** Client retry scheduling or archive representations.

use meticulous::ResultExt as _;
use nervix_consensus::LeadershipObservation;
use nervix_recovery::Discarded as _;

use super::*;
use crate::application::{
    domain_lifecycle::DomainAlterError,
    test_fixtures::{TestService, build_test_service, test_execution_reference},
};

#[nervix_primitives::test]
async fn a_consensus_refusal_beneath_capture_and_lifecycle_contexts_leaves_the_backup_applying() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(false).await;
    let reference = test_execution_reference();
    let domain = DomainName::parse("capture").assured("the test domain is valid");
    let cause = || {
        Report::new(ConsensusError::LeadershipLost {
            observed: LeadershipObservation::UnconfirmedLease { term: 7 },
        })
    };
    let failures = [
        cause()
            .change_context(DomainAlterError::PauseDomain {
                domain: domain.clone(),
            })
            .change_context(BackupError::CaptureDomain {
                domain: domain.clone(),
                reason: "domain pause or drain failed".to_string(),
            }),
        cause()
            .change_context(DomainAlterError::ResumeDomain {
                domain: domain.clone(),
            })
            .change_context(BackupError::CaptureDomain {
                domain: domain.clone(),
                reason: "domain resume failed".to_string(),
            }),
        cause().change_context(BackupError::CaptureDomain {
            domain,
            reason: "mutation lease release failed".to_string(),
        }),
    ];
    for report in failures {
        nervix_primitives::task::consume_budget().await;
        let cause_chain = format!("{report:#}");
        let response = service.backup_error_response(&reference, report).await;
        assert_eq!(
            response.disposition,
            CommandDisposition::OutcomeUnknown(OutcomeUnknownCause::LeadershipLost)
        );
        assert!(
            response
                .message
                .starts_with("the leader could not confirm its lease in term 7")
        );
        assert!(response.message.contains(reference.as_str()));
        assert!(response.message.contains(&cause_chain));
        assert!(
            response
                .message
                .contains("retry it with the same reference")
        );
        assert_eq!(response.diagnostics.len(), 1);
        assert_eq!(response.diagnostics[0].message, response.message);
    }
    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway database may already be gone");
}

#[nervix_primitives::test]
async fn a_changed_coordinator_leaves_the_backup_applying() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(false).await;
    let reference = test_execution_reference();
    let report = Report::new(BackupError::CoordinatorChanged {
        domain: DomainName::parse("capture").assured("the test domain is valid"),
    });
    let response = service.backup_error_response(&reference, report).await;
    assert_eq!(
        response.disposition,
        CommandDisposition::OutcomeUnknown(OutcomeUnknownCause::LeadershipLost)
    );
    assert!(response.message.contains("lost its leader tenure"));
    assert!(response.message.contains(reference.as_str()));
    assert!(
        response
            .message
            .contains("retry it with the same reference")
    );
    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway database may already be gone");
}

#[nervix_primitives::test]
async fn a_cut_budget_expiry_keeps_its_terminal_failure_when_the_lease_was_last_refused() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(false).await;
    let report = Report::new(ConsensusError::LeadershipLost {
        observed: LeadershipObservation::UnconfirmedLease { term: 7 },
    })
    .change_context(BackupError::CaptureTimeout {
        domain: DomainName::parse("capture").assured("the test domain is valid"),
        phase: CutPhase::MutationLease,
        elapsed: Duration::from_secs(30),
        phase_elapsed: Duration::from_secs(30),
        completed: CompletedPhases::default(),
    });
    let response = service
        .backup_error_response(&test_execution_reference(), report)
        .await;
    assert_eq!(response.disposition, CommandDisposition::Failed);
    assert!(
        response
            .message
            .contains("phase=mutation_lease, elapsed=30s")
    );
    assert!(response.message.contains("could not confirm its lease"));
    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway database may already be gone");
}

#[nervix_primitives::test]
async fn a_storage_refusal_keeps_its_terminal_failure_and_complete_cause() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(false).await;
    let cause = std::io::Error::from(std::io::ErrorKind::StorageFull);
    let cause_message = cause.to_string();
    let report = Report::new(cause)
        .change_context(ConsensusError::Storage)
        .change_context(BackupError::CaptureDomain {
            domain: DomainName::parse("capture").assured("the test domain is valid"),
            reason: "mutation lease release failed".to_string(),
        });
    let response = service
        .backup_error_response(&test_execution_reference(), report)
        .await;
    assert_eq!(response.disposition, CommandDisposition::Failed);
    assert!(response.message.contains("mutation lease release failed"));
    assert!(response.message.contains("storage"));
    assert!(response.message.contains(&cause_message));
    assert_eq!(response.diagnostics[0].message, response.message);
    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway database may already be gone");
}
