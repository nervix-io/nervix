//! Settling transaction application and recovering its observed quiescence.
//!
//! Layer: control plane.
//!
//! - **Owns.** Completion of a replicated applying step and recovery of its recorded pauses.
//! - **Depends on.** Transaction consensus state, runtime revision application and impact Models.
//! - **Must not know.** Session protocol presentation or transaction planning.

use error_stack::Report;
use nervix_consensus::{
    ConsensusError, ConsensusTransactionError, ReplicatedTransaction,
    TransactionApplicationOutcome, TransactionApplyingStep, TransactionState,
    TransactionStepEffect,
};
use nervix_models::{
    ActualExecutionStepImpact, DomainName, DomainStatus, ImpactAttribution, ImpactNodeCoverage,
    ModelKind, PauseRequirement, QuiesceLevel, RebuildImpact, RebuildReason,
    TransactionOperationRange,
};
use tokio::time::{Duration, sleep};
use tracing::warn;

use super::{TransactionApplicationAttempt, TransactionCommitError, TransactionStepImpactRecorder};
use crate::{
    application::{domain_clock::current_timestamp, session_service::SessionServiceImpl},
    runtime::RuntimeError,
};

impl SessionServiceImpl {
    pub(super) async fn complete_transaction_application(
        &self,
        transaction: &ReplicatedTransaction,
        applying: &TransactionApplyingStep,
    ) -> Result<TransactionApplicationAttempt, Report<TransactionCommitError>> {
        let actual = TransactionStepImpactRecorder::from_actual(
            applying.result.operation_range().first(),
            applying.result.impact.actual().clone(),
        );
        let application_failure = if !applying.result.result.success {
            None
        } else {
            match &applying.effect {
                Some(TransactionStepEffect::CreateResourceCatalog { .. }) | None => {
                    if self.wait_for_authoritative_visibility().await.is_err() {
                        sleep(Duration::from_millis(100)).await;
                        return Ok(TransactionApplicationAttempt::Retry);
                    }
                    None
                }
                Some(_) => match self
                    .wait_for_runtime_revision(applying.effect_revision)
                    .await
                {
                    Ok(()) => None,
                    Err(error)
                        if matches!(
                            error.current_context(),
                            RuntimeError::RuntimeRevisionPreparation { .. }
                                | RuntimeError::RuntimeRevisionReadiness { .. }
                        ) =>
                    {
                        match self.apply_current_cluster_state().await {
                            Ok(()) => {
                                if self
                                    .wait_for_runtime_revision(applying.effect_revision)
                                    .await
                                    .is_err()
                                {
                                    sleep(Duration::from_millis(100)).await;
                                    return Ok(TransactionApplicationAttempt::Retry);
                                }
                                None
                            }
                            Err(
                                RuntimeError::RuntimeRevisionPreparation { .. }
                                | RuntimeError::RuntimeRevisionReadiness { .. },
                            ) => {
                                sleep(Duration::from_millis(100)).await;
                                return Ok(TransactionApplicationAttempt::Retry);
                            }
                            Err(error) => Some(format!(
                                "transaction '{}' committed the effect beginning at statement {}, \
                                 but it failed to become usable: {error}",
                                transaction.id,
                                applying.result.operation_range().first()
                            )),
                        }
                    }
                    Err(error) => Some(format!(
                        "transaction '{}' committed the effect beginning at statement {}, but it \
                         failed to become usable: {error}",
                        transaction.id,
                        applying.result.operation_range().first()
                    )),
                },
            }
        };
        if !matches!(
            applying.effect,
            Some(TransactionStepEffect::CreateResourceCatalog { .. }) | None
        ) {
            self.record_runtime_recovery_expansions(
                &actual,
                applying.result.operation_range(),
                applying.effect_revision,
            )
            .await;
        }
        Box::pin(self.recover_applying_quiescence(transaction, &actual)).await?;
        let completed = self
            .record_transaction_application_completion(
                transaction,
                application_failure,
                Some(actual.snapshot()),
            )
            .await?;

        Ok(TransactionApplicationAttempt::Completed(Box::new(
            completed,
        )))
    }

    async fn recover_applying_quiescence(
        &self,
        transaction: &ReplicatedTransaction,
        actual: &TransactionStepImpactRecorder,
    ) -> Result<(), Report<TransactionCommitError>> {
        if let Some(attempt) = actual.pending_domain_attempt() {
            let domain = &transaction.domain;
            let status = self
                .inner
                .consensus
                .current_domain(domain)
                .await
                .map(|state| state.status);
            if matches!(status, Some(DomainStatus::Paused)) {
                if let Err(error) = self.apply_current_cluster_state().await {
                    actual.fail(
                        attempt,
                        nervix_models::ImpactDiagnosticKind::Recovery,
                        error.to_string(),
                    );
                    return Err(Report::new(error).change_context(
                        TransactionCommitError::RecoverQuiescence {
                            id: transaction.id.clone(),
                        },
                    ));
                }
                if let Err(error) = self.wait_for_paused_domain_drain(domain).await {
                    actual.fail(
                        attempt,
                        nervix_models::ImpactDiagnosticKind::Recovery,
                        error.to_string(),
                    );
                    return Err(
                        error.change_context(TransactionCommitError::RecoverQuiescence {
                            id: transaction.id.clone(),
                        }),
                    );
                }
                if let Err(error) = self
                    .resume_domain_after_alter_with_impact(
                        domain,
                        transaction.domain_mutation(),
                        Some((actual, attempt)),
                    )
                    .await
                {
                    return Err(
                        error.change_context(TransactionCommitError::RecoverQuiescence {
                            id: transaction.id.clone(),
                        }),
                    );
                }
            } else {
                actual.release(attempt);
            }
        }
        for attempt in actual.pending_entity_attempts() {
            actual.uncertain(
                attempt,
                nervix_models::ImpactDiagnosticKind::Recovery,
                "the original coordinator left while the gate was engaged; its fenced lease owns \
                 cleanup",
            );
        }
        Ok(())
    }

    pub(in crate::application) async fn record_runtime_recovery_expansions(
        &self,
        actual: &TransactionStepImpactRecorder,
        operations: TransactionOperationRange,
        revision: u64,
    ) {
        let attribution = ImpactAttribution::for_range(operations);
        for expansion in self.inner.runtime.recovery_expansions(revision) {
            tokio::task::consume_budget().await;
            let rebuilds = expansion.scope.into_iter().map(|node| RebuildImpact {
                node: ImpactNodeCoverage::all_executions(node),
                reason: RebuildReason::Recovery,
                attribution: attribution.clone(),
            });
            actual.record_recovery_expansion(
                PauseRequirement::Domain {
                    domain: expansion.domain.clone(),
                },
                rebuilds,
            );
            warn!(
                domain = expansion.domain.as_str(),
                reason = expansion.reason,
                revision,
                "recorded runtime recovery expansion in transaction impact"
            );
        }
    }

    pub(in crate::application) async fn apply_current_cluster_state_recording_recovery(
        &self,
        actual: &TransactionStepImpactRecorder,
        transaction: &ReplicatedTransaction,
    ) -> Option<RuntimeError> {
        if let Err(error) = self.apply_current_cluster_state().await {
            return Some(error);
        }
        if let TransactionState::Committing(progress) = &transaction.state
            && let Some(applying) = &progress.applying
        {
            self.record_runtime_recovery_expansions(
                actual,
                applying.result.operation_range(),
                applying.effect_revision,
            )
            .await;
        }
        None
    }

    /// How an applying step that found no other failure ends. A successful model step that
    /// creates, changes, or drops a VHOST first waits until every live HTTPS listener installed its
    /// runtime revision. When one could not, a step that did not pause is rolled back by the entry
    /// that records its failure, and a paused step, which has already resumed, keeps its committed
    /// models like any other activation failure.
    async fn https_listener_outcome(
        &self,
        transaction: &ReplicatedTransaction,
        applying: &TransactionApplyingStep,
    ) -> TransactionApplicationOutcome {
        let Some(TransactionStepEffect::ReplaceDomainSchedule { .. }) = &applying.effect else {
            return TransactionApplicationOutcome::Applied;
        };
        let planned = applying.result.impact.planned();
        if !applying.result.result.success
            || !planned.effects.changes_configuration_of(ModelKind::Vhost)
        {
            return TransactionApplicationOutcome::Applied;
        }
        let Err(failure) =
            Box::pin(self.wait_for_https_listener_installation(applying.effect_revision)).await
        else {
            return TransactionApplicationOutcome::Applied;
        };
        let domain = &transaction.domain;
        let error = format!(
            "committed transaction model step in domain '{}' failed HTTPS listener activation: \
             {failure}",
            domain.as_str()
        );
        self.broadcast_error(error.clone());
        if planned.pause.level() != QuiesceLevel::Dynamic {
            return TransactionApplicationOutcome::Failed { error };
        }
        let inputs = Box::pin(self.inner.consensus.domain_planning_inputs(domain)).await;
        TransactionApplicationOutcome::RolledBack {
            error: format!("{error}; the model batch was rolled back"),
            inputs: Box::new(inputs),
        }
    }

    /// Makes the schedule a rolled-back step restored usable: every node applies it, and every
    /// HTTPS listener presents the restored certificates again. The failure is already recorded,
    /// so a node that cannot follow is reported rather than changing the outcome.
    async fn apply_rolled_back_transaction_step(&self, domain: &DomainName) {
        let restored_revision = self.inner.consensus.current_runtime_revision().await;
        if let Err(error) = self.apply_current_cluster_state().await {
            self.broadcast_error(format!(
                "failed to apply the models restored in domain '{}': {error}",
                domain.as_str()
            ));
            return;
        }
        if let Err(error) = self
            .wait_for_https_listener_installation(restored_revision)
            .await
        {
            self.broadcast_error(format!(
                "the HTTPS listener TLS configuration restored in domain '{}' did not install: \
                 {error}",
                domain.as_str()
            ));
        }
    }

    /// Records how the applying step of `transaction` ended. A step without an application failure
    /// is settled by its HTTPS listeners first, so a VHOST change that no listener could install
    /// is never recorded as applied.
    pub(in crate::application) async fn record_transaction_application_completion(
        &self,
        transaction: &ReplicatedTransaction,
        application_failure: Option<String>,
        actual: Option<ActualExecutionStepImpact>,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let applying = match &transaction.state {
            TransactionState::Committing(progress) => progress.applying.as_ref(),
            TransactionState::Open(_) | TransactionState::Finished(_) => None,
        }
        .ok_or_else(|| {
            Report::new(TransactionCommitError::InvalidProgress {
                id: transaction.id.clone(),
            })
        })?;
        let outcome = match application_failure {
            Some(error) => TransactionApplicationOutcome::Failed { error },
            None => Box::pin(self.https_listener_outcome(transaction, applying)).await,
        };
        let actual = actual.unwrap_or_else(|| applying.result.impact.actual().clone());
        let rolls_back = matches!(outcome, TransactionApplicationOutcome::RolledBack { .. });
        let completed = self
            .inner
            .consensus
            .complete_transaction_application(
                transaction.id.clone(),
                applying.result.first_statement(),
                current_timestamp(),
                actual,
                outcome,
            )
            .await
            .map_err(|error| Report::new(TransactionCommitError::Proposal(error)))?;
        if rolls_back {
            Box::pin(self.apply_rolled_back_transaction_step(&transaction.domain)).await;
        }

        if let TransactionState::Finished(finished) = &completed.state {
            loop {
                tokio::task::consume_budget().await;
                if self
                    .wait_for_authoritative_revision(finished.outcome_revision)
                    .await
                    .is_ok()
                {
                    break;
                }
                if self.inner.consensus.current_leader().await.as_ref()
                    != Some(self.inner.consensus.local_node_id())
                {
                    return Err(Report::new(TransactionCommitError::Proposal(
                        ConsensusTransactionError::Consensus(ConsensusError::LeadershipLost {
                            leader_id: self.inner.consensus.current_leader().await,
                        }),
                    )));
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
        Ok(completed)
    }
}
