//! Applied-owner checks for orphaned pause recovery.
//!
//! Layer: test harness.
//! - **Owns.** Lease, adoption, restore-gate and domain-lifetime preservation assertions.
//! - **Depends on.** The production applied mutation and current typed consensus records.
//! - **Must not know.** Network behavior, session routing or reference-count ordering.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "tests conditional pause recovery through its applied owner"
    )
)]

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    CommandExecutionReference, DomainConfig, DomainPace, DomainStartPoint, DomainState, Timestamp,
    UserName,
};

use super::*;
use crate::{
    DomainMutationError, DomainMutationOwner, ReplicatedTransaction, TransactionActivity,
    TransactionCommitPlanHeader, apply_consensus_command,
};

struct Fixture {
    state: StateMachineData,
    domain: DomainName,
    mutation: DomainMutationLease,
}

impl Fixture {
    fn new() -> Self {
        let domain = DomainName::parse("tenant").assured("the fixture domain is an identifier");
        let reference = CommandExecutionReference::parse("018bcfe5-6800-7000-8000-000000000001")
            .assured("the fixture reference is a UUIDv7");
        let mutation = DomainMutationLease::recorded(DomainMutationOwner::command(reference), 11);
        let mut state = StateMachineData::default();
        state
            .domain_mutations
            .insert(domain.clone(), mutation.clone());
        state.domains.insert(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                },
                status: DomainStatus::Paused,
                start_version: 7,
                last_start: DomainStartPoint::Resume,
                clock: None,
                stop_cause: None,
            },
        );
        Self {
            state,
            domain,
            mutation,
        }
    }

    fn recover(&mut self, mutation: &DomainMutationLease) -> AppliedConsensusCommand {
        apply_consensus_command(
            &mut self.state,
            &ConsensusCommand::RecoverOrphanedModelPause {
                domain_id: self.domain.clone(),
                mutation: Box::new(mutation.clone()),
            },
        )
    }

    fn paused(&self) -> bool {
        self.state
            .domains
            .get(&self.domain)
            .assured("the fixture domain remains present")
            .status
            == DomainStatus::Paused
    }

    fn adopt(&mut self, domain: DomainName) {
        let user = UserName::parse("operator").assured("the fixture owner is an identifier");
        let activity = TransactionActivity::from_timeout(
            Timestamp::from_unix_nanos(1),
            Duration::from_secs(60),
        );
        let mut transaction =
            ReplicatedTransaction::open("adopting".into(), domain.clone(), user.clone(), activity);
        let decision = crate::transaction::test_commit_plan("adopting", 0);
        let mutation = if domain == self.domain {
            Some(self.mutation.clone())
        } else {
            None
        };
        transaction
            .start_commit(
                &user,
                activity,
                12,
                mutation,
                TransactionCommitPlanHeader {
                    preview: decision.preview,
                    step_count: 0,
                },
            )
            .assured("the current open transaction can enter COMMITTING");
        self.state
            .transactions
            .insert(transaction.id.clone(), transaction);
    }
}

#[test]
fn recovery_changes_only_the_pause_and_repeats_as_unchanged() {
    let mut fixture = Fixture::new();
    let mut expected = fixture
        .state
        .domains
        .get(&fixture.domain)
        .assured("fixture domain")
        .clone();
    expected.status = DomainStatus::Running;
    let schedule = fixture.state.schedule.clone();
    let mutation = fixture.mutation.clone();
    let recovered = fixture.recover(&mutation);
    assert_eq!(
        recovered.response,
        ConsensusResponse::OrphanedPauseRecovery(OrphanedPauseRecovery::Resumed)
    );
    assert!(recovered.domains_changed);
    assert!(!recovered.schedule_changed);
    assert_eq!(fixture.state.domains.get(&fixture.domain), Some(&expected));
    assert_eq!(fixture.state.schedule, schedule);
    let repeated = fixture.recover(&mutation);
    assert_eq!(
        repeated.response,
        ConsensusResponse::OrphanedPauseRecovery(OrphanedPauseRecovery::Unchanged)
    );
    assert!(!repeated.domains_changed);
}

#[test]
fn a_replaced_fence_is_refused_even_when_no_pause_remains() {
    for status in [
        DomainStatus::Paused,
        DomainStatus::Running,
        DomainStatus::Stopped,
    ] {
        let mut fixture = Fixture::new();
        let mutation = fixture.mutation.clone();
        fixture
            .state
            .domains
            .get_mut(&fixture.domain)
            .assured("fixture domain")
            .status = status.clone();
        let replacement = DomainMutationLease::recorded(mutation.owner().clone(), 12);
        fixture
            .state
            .domain_mutations
            .insert(fixture.domain.clone(), replacement);
        let recovered = fixture.recover(&mutation);
        assert!(matches!(
            recovered.response,
            ConsensusResponse::Conflict(ConsensusConflict::DomainMutation { .. })
        ));
        assert!(!recovered.domains_changed);
        assert_eq!(
            fixture
                .state
                .domains
                .get(&fixture.domain)
                .assured("fixture domain")
                .status,
            status
        );
    }
}

#[test]
fn a_released_lease_is_not_authority_to_recover_a_pause() {
    let mut fixture = Fixture::new();
    let mutation = fixture.mutation.clone();
    fixture.state.domain_mutations.remove(&fixture.domain);
    let recovered = fixture.recover(&mutation);
    let ConsensusResponse::Conflict(ConsensusConflict::DomainMutation { cause }) =
        recovered.response
    else {
        panic!("the released fence must be refused");
    };
    assert!(matches!(*cause, DomainMutationError::FenceLost { .. }));
    assert!(fixture.paused());
}

#[test]
fn a_committing_transaction_retains_its_pause_and_ordinary_cleanup_can_resume_it() {
    let mut fixture = Fixture::new();
    fixture.adopt(fixture.domain.clone());
    let mutation = fixture.mutation.clone();
    let recovered = fixture.recover(&mutation);
    assert_eq!(
        recovered.response,
        ConsensusResponse::OrphanedPauseRecovery(OrphanedPauseRecovery::TransactionActive {
            transaction: "adopting".into()
        },)
    );
    assert!(fixture.paused());
    let resumed = apply_consensus_command(
        &mut fixture.state,
        &ConsensusCommand::ResumeDomain {
            domain_id: fixture.domain.clone(),
            mutation: Some(Box::new(mutation)),
        },
    );
    assert_eq!(resumed.response, ConsensusResponse::Applied);
    assert!(!fixture.paused());
}

#[test]
fn another_domains_committing_transaction_does_not_hold_this_pause() {
    let mut fixture = Fixture::new();
    fixture.adopt(DomainName::parse("another").assured("the other domain is an identifier"));
    let mutation = fixture.mutation.clone();
    assert_eq!(
        fixture.recover(&mutation).response,
        ConsensusResponse::OrphanedPauseRecovery(OrphanedPauseRecovery::Resumed)
    );
}

#[test]
fn an_incomplete_restore_keeps_its_activation_gate() {
    let mut fixture = Fixture::new();
    let mutation = fixture.mutation.clone();
    let DomainMutationOwner::Command(reference) = mutation.owner() else {
        panic!("the fixture models an admitted command");
    };
    fixture.state.domain_restore_installations.insert(
        fixture.domain.clone(),
        crate::restore::DomainRestoreInstallation::Pending {
            execution: reference.clone(),
        },
    );
    let recovered = fixture.recover(&mutation);
    assert!(matches!(
        recovered.response,
        ConsensusResponse::Conflict(ConsensusConflict::RestoreInstallation { .. })
    ));
    assert!(fixture.paused());
    assert!(
        fixture
            .state
            .domain_restore_installations
            .contains_key(&fixture.domain)
    );
}

#[cfg(feature = "shuttle")]
mod shuttle_checks {
    use nervix_primitives::{
        sync::{Arc, blocking::Mutex},
        thread,
    };

    use super::*;

    #[test]
    fn shuttle_transaction_adoption_and_reconcile_fence_delayed_recovery() {
        nervix_model_harness::shuttle::check_interleavings(|| {
            let fixture = Arc::new(Mutex::new(Fixture::new()));
            let recovery = thread::spawn({
                let fixture = fixture.clone();
                move || {
                    let mutation = fixture.lock().mutation.clone();
                    thread::yield_now();
                    fixture.lock().recover(&mutation);
                }
            });
            let reconcile = thread::spawn({
                let fixture = fixture.clone();
                move || {
                    let mut fixture = fixture.lock();
                    let domain_id = fixture.domain.clone();
                    let outcome = apply_consensus_command(
                        &mut fixture.state,
                        &ConsensusCommand::ResumeDomain {
                            domain_id,
                            mutation: None,
                        },
                    );
                    assert!(matches!(outcome.response, ConsensusResponse::Conflict(_)));
                }
            });
            {
                let mut fixture = fixture.lock();
                let domain = fixture.domain.clone();
                fixture.adopt(domain.clone());
                let mutation = fixture.mutation.clone();
                let paused = apply_consensus_command(
                    &mut fixture.state,
                    &ConsensusCommand::PauseDomain {
                        domain_id: domain,
                        mutation: Some(Box::new(mutation)),
                    },
                );
                assert_eq!(paused.response, ConsensusResponse::Applied);
            }
            recovery.join().assured("recovery participant completes");
            reconcile.join().assured("reconcile participant completes");
            assert!(
                fixture.lock().paused(),
                "a committing transaction's adopted pause survives every stale recovery"
            );
        });
    }

    #[test]
    fn shuttle_a_new_lease_fences_a_delayed_recovery() {
        nervix_model_harness::shuttle::check_interleavings(|| {
            let fixture = Arc::new(Mutex::new(Fixture::new()));
            let recovery = thread::spawn({
                let fixture = fixture.clone();
                move || {
                    let mutation = fixture.lock().mutation.clone();
                    thread::yield_now();
                    fixture.lock().recover(&mutation);
                }
            });
            {
                let mut fixture = fixture.lock();
                let domain = fixture.domain.clone();
                let replacement =
                    DomainMutationLease::recorded(fixture.mutation.owner().clone(), 12);
                fixture
                    .state
                    .domain_mutations
                    .insert(domain.clone(), replacement.clone());
                let paused = apply_consensus_command(
                    &mut fixture.state,
                    &ConsensusCommand::PauseDomain {
                        domain_id: domain,
                        mutation: Some(Box::new(replacement)),
                    },
                );
                assert_eq!(paused.response, ConsensusResponse::Applied);
            }
            recovery.join().assured("recovery participant completes");
            assert!(
                fixture.lock().paused(),
                "an earlier lease cannot release the replacement owner's pause"
            );
        });
    }
}
