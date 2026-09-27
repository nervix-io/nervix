//! Leader-side decisions about refused WASM guest-state lifetimes.
//!
//! Layer: control plane.
//!
//! - **Owns.** Whether a refused lifetime still has its one recovery attempt, the durable record of
//!   that decision, and the coordinated reset an admitted attempt drives.
//! - **Depends on.** Consensus schedules, the interconnect, and the coordinated reset operation.
//! - **Must not know.** Guest memory, snapshot bytes, or how a branch discovered its state was
//!   refused.

use error_stack::{Report, ResultExt as _};
use nervix_interconnect::{
    RecoverWasmProcessorStateRequest, RecoverWasmProcessorStateResponse, RemoteOperationSubject,
    WasmStateResetTarget,
};
use nervix_models::{
    CommandExecutionReference, DomainName, DomainStatus, ModelKind, ModelName, NodeRef,
    WasmRejectedStatePolicy, WasmSavedStateRejection, WasmStateGeneration,
    WasmStateRecoveryAdmission, WasmStateRecoveryOutcome, WasmStateResetReason,
    WasmStateResetScope,
};
use tracing::info;

use super::{AppError, session_service::SessionServiceImpl};
use crate::runtime::{Runtime, WasmStateRecoveryRequest};

#[derive(Debug, thiserror::Error)]
pub(in crate::application) enum WasmStateRecoveryError {
    #[error("domain '{}' is not running", .domain.as_str())]
    DomainNotRunning { domain: DomainName },
    #[error("WASM processor '{}' does not exist in domain '{}'", .processor.as_str(), .domain.as_str())]
    ProcessorUnavailable {
        domain: DomainName,
        processor: ModelName,
    },
    #[error(
        "WASM processor '{}' does not opt in to discarding a rejected snapshot",
        .processor.as_str()
    )]
    PolicyPreservesState { processor: ModelName },
    #[error("recovery target does not match WASM processor '{}' branching", .processor.as_str())]
    InvalidTarget { processor: ModelName },
    #[error(
        "reported guest-state generation of WASM processor '{}' is not a generation number",
        .processor.as_str()
    )]
    InvalidGeneration { processor: ModelName },
    #[error(
        "WASM processor '{}' already spent the recovery attempt of guest-state generation \
         {generation}",
        .processor.as_str()
    )]
    Exhausted {
        processor: ModelName,
        generation: WasmStateGeneration,
    },
    #[error("failed to record the recovery attempt of WASM processor '{}'", .processor.as_str())]
    Record { processor: ModelName },
    #[error("failed to replace the rejected guest state of WASM processor '{}'", .processor.as_str())]
    Reset { processor: ModelName },
    #[error("failed to reach the leader that decides WASM processor '{}' recovery", .processor.as_str())]
    Leader { processor: ModelName },
}

/// One reported refusal, resolved against the committed schedule.
enum WasmStateRecoveryPlan {
    /// The lifetime the owner was refused is still the one the schedule names for that branch, so
    /// it can spend its attempt.
    Refused {
        entity: NodeRef,
        scope: WasmStateResetScope,
        generation: WasmStateGeneration,
    },
    /// The branch has already left the lifetime that was refused, so there is nothing to discard.
    /// A report that raced a transition lands here rather than discarding the lifetime that
    /// replaced it.
    Superseded,
}

impl SessionServiceImpl {
    /// Answer refused guest-state lifetimes this node's branches raise, for as long as it serves.
    ///
    /// The owner raises; the leader decides. A node that is not the leader forwards, so one refused
    /// lifetime reaches exactly one decision wherever its branch happens to run.
    pub(super) fn register_wasm_state_recovery_coordinator(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        let mut requests = self.inner.runtime.attach_wasm_state_recovery_coordinator();
        let service = self.clone();
        self.inner.service_tasks.spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                let request = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    request = requests.recv() => request,
                };
                let Some(request) = request else {
                    break;
                };
                service.answer_wasm_state_recovery(request).await;
            }
        });
    }

    /// Decide one refused lifetime and report what the decision was.
    async fn answer_wasm_state_recovery(&self, request: WasmStateRecoveryRequest) {
        let placement = request.placement().clone();
        let outcome = self.decide_wasm_state_recovery(&request).await;
        if let Err(error) = outcome {
            self.inner
                .runtime
                .report_wasm_state_recovery_failure(&placement, &format!("{error:#}"));
        }
        // The decision is durable, so releasing the raise cannot turn a spent attempt into a second
        // reset. It only lets a branch that is refused again ask once more and be told the same.
        self.inner
            .runtime
            .release_raised_wasm_state_recovery(&placement);
    }

    async fn decide_wasm_state_recovery(
        &self,
        request: &WasmStateRecoveryRequest,
    ) -> error_stack::Result<(), WasmStateRecoveryError> {
        let domain = request.domain().clone();
        let processor = request.processor().clone();
        let target = request.target();
        if let Some(leader) = self.inner.consensus.current_leader().await
            && &leader != self.inner.consensus.local_node_id()
        {
            let response = self
                .inner
                .interconnect
                .request(
                    &leader,
                    RecoverWasmProcessorStateRequest {
                        domain,
                        processor: processor.clone(),
                        target,
                        generation: request.generation().into(),
                        rejection: request.rejection(),
                    },
                )
                .await
                .change_context_lazy(|| WasmStateRecoveryError::Leader {
                    processor: processor.clone(),
                })?;
            return response
                .result
                .map_err(Report::new)
                .change_context_lazy(|| WasmStateRecoveryError::Reset {
                    processor: processor.clone(),
                });
        }
        self.recover_wasm_processor_state(
            &domain,
            &processor,
            target,
            request.generation(),
            request.rejection(),
        )
        .await
    }

    pub(super) fn register_wasm_state_recovery_interconnect_handler(
        &self,
    ) -> error_stack::Result<(), AppError> {
        let service = self.clone();
        self.inner
            .interconnect
            .register_handler::<RecoverWasmProcessorStateRequest, _, _>(move |_context, request| {
                let service = service.clone();
                async move {
                    let subject = RemoteOperationSubject::entity(
                        &request.domain,
                        ModelKind::WasmProcessor,
                        request.processor.clone(),
                    );
                    let result = match WasmStateGeneration::try_from(request.generation) {
                        Ok(generation) => {
                            service
                                .recover_wasm_processor_state(
                                    &request.domain,
                                    &request.processor,
                                    request.target,
                                    generation,
                                    request.rejection,
                                )
                                .await
                        }
                        Err(error) => Err(Report::new(error).change_context(
                            WasmStateRecoveryError::InvalidGeneration {
                                processor: request.processor.clone(),
                            },
                        )),
                    };
                    RecoverWasmProcessorStateResponse {
                        result: result.map_err(|error| {
                            nervix_interconnect::RemoteOperationFailure::failed(
                                subject,
                                format!("{error:#}"),
                            )
                        }),
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)
    }

    /// Spend the one recovery attempt the refused lifetime of `target` is worth.
    ///
    /// `refused` names the lifetime the reporting owner was handed. It is checked against the one
    /// the committed schedule names, so a report that raced a transition is answered with that
    /// transition instead of discarding the lifetime that replaced it.
    pub(in crate::application) async fn recover_wasm_processor_state(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        target: WasmStateResetTarget,
        refused: WasmStateGeneration,
        rejection: WasmSavedStateRejection,
    ) -> error_stack::Result<(), WasmStateRecoveryError> {
        let plan = self
            .plan_wasm_state_recovery(domain, processor, target.clone(), refused)
            .await?;
        let WasmStateRecoveryPlan::Refused {
            entity,
            scope,
            generation,
        } = plan
        else {
            return Ok(());
        };
        let admission = self
            .admit_wasm_state_recovery(domain, processor, &entity, scope, generation, rejection)
            .await?;
        let request = match admission {
            WasmStateRecoveryAdmission::Admitted(request)
            | WasmStateRecoveryAdmission::Resumed(request) => request,
            WasmStateRecoveryAdmission::AlreadyRecovered => return Ok(()),
            WasmStateRecoveryAdmission::Exhausted => {
                return Err(Report::new(WasmStateRecoveryError::Exhausted {
                    processor: processor.clone(),
                    generation,
                }));
            }
        };

        let reset = self
            .reset_wasm_processor_state(
                domain,
                processor,
                request.clone(),
                target,
                WasmStateResetReason::RejectedSnapshot,
                None,
            )
            .await
            .change_context_lazy(|| WasmStateRecoveryError::Reset {
                processor: processor.clone(),
            });
        let outcome = match &reset {
            Ok(()) => WasmStateRecoveryOutcome::Recovered,
            Err(_) => WasmStateRecoveryOutcome::Failed,
        };
        let recorded = self
            .settle_wasm_state_recovery(domain, processor, &entity, &scope, &request, outcome)
            .await;

        // A reset that failed and an attempt whose outcome was not recorded are both failures, and
        // the unrecorded outcome is the one that decides what happens next: without it the branch's
        // next record asks for another reset. Report it, carrying the reset's own failure.
        let Err(failure) = reset else {
            return recorded;
        };
        match recorded {
            Ok(()) => Err(failure),
            Err(unrecorded) => Err(unrecorded.attach_printable(format!("{failure:#}"))),
        }
    }

    async fn plan_wasm_state_recovery(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        target: WasmStateResetTarget,
        refused: WasmStateGeneration,
    ) -> error_stack::Result<WasmStateRecoveryPlan, WasmStateRecoveryError> {
        let inputs = self.inner.consensus.domain_planning_inputs(domain).await;
        let unavailable = || WasmStateRecoveryError::ProcessorUnavailable {
            domain: domain.clone(),
            processor: processor.clone(),
        };
        let Some(state) = inputs.state() else {
            return Err(Report::new(unavailable()));
        };
        if state.status != DomainStatus::Running {
            return Err(Report::new(WasmStateRecoveryError::DomainNotRunning {
                domain: domain.clone(),
            }));
        }
        let entity = NodeRef::new(ModelKind::WasmProcessor, processor.clone());
        let Some(schedule) = inputs.schedule() else {
            return Err(Report::new(unavailable()));
        };
        let Some(node) = schedule.nodes.get(&entity) else {
            return Err(Report::new(unavailable()));
        };
        let wasm = node
            .wasm_processor()
            .ok_or_else(|| Report::new(unavailable()))?;
        if wasm.rejected_state_policy != WasmRejectedStatePolicy::Reset {
            return Err(Report::new(WasmStateRecoveryError::PolicyPreservesState {
                processor: processor.clone(),
            }));
        }
        let scope = Self::resolve_wasm_state_recovery_scope(processor, target)?;
        let generations = node
            .wasm_state_generations()
            .ok_or_else(|| Report::new(unavailable()))?;
        let generation = match &scope {
            WasmStateResetScope::Unbranched => generations.of_branch(None),
            WasmStateResetScope::Branch(branch) => generations.of_branch(Some(branch)),
            WasmStateResetScope::AllBranches => {
                return Err(Report::new(WasmStateRecoveryError::InvalidTarget {
                    processor: processor.clone(),
                }));
            }
        };
        if generation != refused {
            return Ok(WasmStateRecoveryPlan::Superseded);
        }
        Ok(WasmStateRecoveryPlan::Refused {
            entity,
            scope,
            generation,
        })
    }

    /// The scope one refused branch selects. A refusal always names one execution, so the
    /// all-branches target a coordinated reset accepts is not a recovery target.
    fn resolve_wasm_state_recovery_scope(
        processor: &ModelName,
        target: WasmStateResetTarget,
    ) -> error_stack::Result<WasmStateResetScope, WasmStateRecoveryError> {
        match target {
            WasmStateResetTarget::Unbranched => Ok(WasmStateResetScope::Unbranched),
            WasmStateResetTarget::Branch(fields) => Runtime::wasm_state_reset_branch_scope(
                processor, fields,
            )
            .change_context_lazy(|| WasmStateRecoveryError::InvalidTarget {
                processor: processor.clone(),
            }),
            WasmStateResetTarget::AllBranches => {
                Err(Report::new(WasmStateRecoveryError::InvalidTarget {
                    processor: processor.clone(),
                }))
            }
        }
    }

    /// Record that this refused lifetime is spending its attempt, before anything is reset.
    ///
    /// Recording first is what makes a failure that happens before the new generation is published
    /// still count: the attempt is already durable, so the branch that keeps being refused is told
    /// the budget is spent instead of asking for another reset.
    async fn admit_wasm_state_recovery(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        entity: &NodeRef,
        scope: WasmStateResetScope,
        generation: WasmStateGeneration,
        rejection: WasmSavedStateRejection,
    ) -> error_stack::Result<WasmStateRecoveryAdmission, WasmStateRecoveryError> {
        let record = || WasmStateRecoveryError::Record {
            processor: processor.clone(),
        };
        let inputs = self.inner.consensus.domain_planning_inputs(domain).await;
        let mut schedule = inputs
            .schedule()
            .cloned()
            .ok_or_else(|| Report::new(record()))?;
        let node = schedule
            .nodes
            .get_mut(entity)
            .ok_or_else(|| Report::new(record()))?;
        let admission = node
            .admit_wasm_state_recovery(scope, generation, rejection)
            .ok_or_else(|| Report::new(record()))?;
        let WasmStateRecoveryAdmission::Admitted(_) = &admission else {
            return Ok(admission);
        };
        self.inner
            .consensus
            .replace_domain_schedule(inputs, Some(schedule), None)
            .await
            .change_context_lazy(record)?;
        info!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            scope = scope.kind(),
            %generation,
            %rejection,
            "WASM rejected-state recovery admitted"
        );
        Ok(admission)
    }

    /// Record what the attempt achieved, so the next refusal of the same lifetime is answered from
    /// committed state rather than by resetting again.
    async fn settle_wasm_state_recovery(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        entity: &NodeRef,
        scope: &WasmStateResetScope,
        request: &CommandExecutionReference,
        outcome: WasmStateRecoveryOutcome,
    ) -> error_stack::Result<(), WasmStateRecoveryError> {
        let record = || WasmStateRecoveryError::Record {
            processor: processor.clone(),
        };
        let inputs = self.inner.consensus.domain_planning_inputs(domain).await;
        let mut schedule = inputs
            .schedule()
            .cloned()
            .ok_or_else(|| Report::new(record()))?;
        let node = schedule
            .nodes
            .get_mut(entity)
            .ok_or_else(|| Report::new(record()))?;
        if !node.settle_wasm_state_recovery(scope, request, outcome) {
            // Another coordinator settled the same attempt first, and it settled it with the same
            // reset's outcome. There is nothing left to publish.
            return Ok(());
        }
        self.inner
            .consensus
            .replace_domain_schedule(inputs, Some(schedule), None)
            .await
            .change_context_lazy(record)?;
        info!(
            domain = domain.as_str(),
            processor = processor.as_str(),
            scope = scope.kind(),
            %outcome,
            "WASM rejected-state recovery settled"
        );
        Ok(())
    }
}
