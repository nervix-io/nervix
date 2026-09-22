//! Leader-side coordination of durable WASM guest-state lifetime replacement.
//!
//! Layer: control plane.
//!
//! - **Owns.** Reset validation, cluster fencing, owner preparation, generation publication, and
//!   the readiness publication after the fresh initial checkpoint is durable.
//! - **Depends on.** Consensus schedules, entity gates, the interconnect, and the runtime command
//!   that replaces branch-local guest instances.
//! - **Must not know.** Guest memory layout, checkpoint bytes, relay queues, or NSPL syntax.

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use futures_util::future::try_join_all;
use meticulous::OptionExt as _;
use nervix_consensus::{DomainMutationLease, DomainPlanningInputs};
use nervix_interconnect::{
    CoordinateWasmStateResetRequest, CoordinateWasmStateResetResponse, EntityGatePurpose,
    RemoteOperationSubject, WasmStateResetRuntimeAction as RemoteWasmStateResetRuntimeAction,
    WasmStateResetRuntimeRequest as RemoteWasmStateResetRuntimeRequest,
    WasmStateResetRuntimeResponse as RemoteWasmStateResetRuntimeResponse, WasmStateResetTarget,
};
use nervix_models::{
    ClusterNodeName, CommandExecutionReference, DomainName, DomainSchedule, DomainStatus,
    ModelKind, ModelName, NodeRef, RelayName, RemoteRuntimeField, ResolvedBranching,
    WasmStateResetPhase, WasmStateResetScope,
};
#[cfg(feature = "testing")]
use nervix_recovery::NoReceiver as _;

use super::{AppError, entity_gate::ClusterEntityGate, session_service::SessionServiceImpl};
use crate::runtime::{Runtime, WasmStateResetPreparation};

#[derive(Debug, thiserror::Error)]
pub(in crate::application) enum WasmStateResetError {
    #[error("another alteration is active in domain '{}'", .domain.as_str())]
    ConcurrentAlter { domain: DomainName },
    #[error("domain '{}' does not exist", .domain.as_str())]
    DomainUnavailable { domain: DomainName },
    #[error("domain '{}' is not running", .domain.as_str())]
    DomainNotRunning { domain: DomainName },
    #[error("WASM processor '{}' does not exist in domain '{}'", .processor.as_str(), .domain.as_str())]
    ProcessorUnavailable {
        domain: DomainName,
        processor: ModelName,
    },
    #[error("WASM processor '{}' has no execution owner in domain '{}'", .processor.as_str(), .domain.as_str())]
    OwnerUnavailable {
        domain: DomainName,
        processor: ModelName,
    },
    #[error("reset target does not match WASM processor '{}' branching in domain '{}'", .processor.as_str(), .domain.as_str())]
    InvalidTarget {
        domain: DomainName,
        processor: ModelName,
    },
    #[error("reset request '{}' conflicts with the reset already publishing for WASM processor '{}'", .request, .processor.as_str())]
    RequestConflict {
        processor: ModelName,
        request: CommandExecutionReference,
    },
    #[error("failed to establish the reset fence for WASM processor '{}'", .processor.as_str())]
    Gate { processor: ModelName },
    #[error("failed to prepare fresh guest state for WASM processor '{}'", .processor.as_str())]
    Prepare { processor: ModelName },
    #[error("failed to publish a new guest-state generation for WASM processor '{}'", .processor.as_str())]
    Publish { processor: ModelName },
    #[error("failed to abort the unpublished guest-state reset for WASM processor '{}'", .processor.as_str())]
    Abort { processor: ModelName },
    #[error("WASM processor '{}' reset was committed but its new lifetime is not usable", .processor.as_str())]
    CommittedNotUsable { processor: ModelName },
}

struct WasmStateResetPlan {
    inputs: DomainPlanningInputs,
    schedule: DomainSchedule,
    owner: ClusterNodeName,
    relays: Vec<RelayName>,
    entity: NodeRef,
    scope: WasmStateResetScope,
    branch_key: Option<Vec<RemoteRuntimeField>>,
    published: bool,
    ready: bool,
}

impl SessionServiceImpl {
    pub(super) fn register_wasm_state_reset_interconnect_handler(
        &self,
    ) -> error_stack::Result<(), AppError> {
        let service = self.clone();
        self.inner
            .interconnect
            .register_handler::<RemoteWasmStateResetRuntimeRequest, _, _>(
                move |_context, request| {
                    let service = service.clone();
                    async move {
                        RemoteWasmStateResetRuntimeResponse {
                            result: service
                                .handle_wasm_state_reset_runtime_request(request)
                                .await,
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let service = self.clone();
        self.inner
            .interconnect
            .register_handler::<CoordinateWasmStateResetRequest, _, _>(move |_context, request| {
                let service = service.clone();
                async move {
                    let subject = RemoteOperationSubject::entity(
                        &request.domain,
                        ModelKind::WasmProcessor,
                        request.processor.clone(),
                    );
                    let result = service
                        .reset_wasm_processor_state(
                            &request.domain,
                            &request.processor,
                            request.request,
                            request.target,
                            None,
                        )
                        .await
                        .map_err(|error| {
                            nervix_interconnect::RemoteOperationFailure::failed(
                                subject,
                                format!("{error:#}"),
                            )
                        });
                    CoordinateWasmStateResetResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)
    }

    #[cfg(feature = "testing")]
    pub(super) fn register_wasm_state_reset_test_coordinator(
        &self,
        fault_injection: &crate::fault_injection::FaultInjection,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        let mut reset_requests = fault_injection
            .register_wasm_state_reset_coordinator(self.inner.consensus.local_node_id().clone());
        let service = self.clone();
        self.inner.service_tasks.spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                let request = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    request = reset_requests.recv() => request,
                };
                let Some(request) = request else {
                    break;
                };
                let result = service
                    .handle_wasm_state_reset_test_request(
                        request.domain,
                        request.processor,
                        request.reference,
                        request.target,
                    )
                    .await;
                request
                    .response
                    .send(result)
                    .means_peer_left("WASM state reset test requester");
            }
        });
    }

    #[cfg(feature = "testing")]
    async fn handle_wasm_state_reset_test_request(
        &self,
        domain: DomainName,
        processor: ModelName,
        reference: CommandExecutionReference,
        target: crate::fault_injection::WasmStateResetRequestTarget,
    ) -> error_stack::Result<(), crate::fault_injection::WasmStateResetRequestError> {
        let target = match target {
            crate::fault_injection::WasmStateResetRequestTarget::Unbranched => {
                WasmStateResetTarget::Unbranched
            }
            crate::fault_injection::WasmStateResetRequestTarget::Branch(branch) => {
                WasmStateResetTarget::Branch(branch)
            }
            crate::fault_injection::WasmStateResetRequestTarget::AllBranches => {
                WasmStateResetTarget::AllBranches
            }
        };
        let failed = || crate::fault_injection::WasmStateResetRequestError::ResetFailed {
            processor: processor.clone(),
        };
        if let Some(leader) = self.inner.consensus.current_leader().await
            && &leader != self.inner.consensus.local_node_id()
        {
            let response = self
                .inner
                .interconnect
                .request(
                    &leader,
                    CoordinateWasmStateResetRequest {
                        domain,
                        processor: processor.clone(),
                        request: reference,
                        target,
                    },
                )
                .await
                .change_context_lazy(failed)?;
            return response
                .result
                .map_err(Report::new)
                .change_context_lazy(failed);
        }
        self.reset_wasm_processor_state(&domain, &processor, reference, target, None)
            .await
            .change_context_lazy(failed)
    }

    pub(in crate::application) async fn reset_wasm_processor_state(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        request: CommandExecutionReference,
        target: WasmStateResetTarget,
        mutation: Option<&DomainMutationLease>,
    ) -> error_stack::Result<(), WasmStateResetError> {
        let Some(_alter_guard) = self.inner.runtime.try_begin_domain_alter(domain) else {
            return Err(Report::new(WasmStateResetError::ConcurrentAlter {
                domain: domain.clone(),
            }));
        };
        let mut plan = self
            .plan_wasm_state_reset(domain, processor, &request, target.clone())
            .await?;
        // A Publishing schedule whose owner failed its initial checkpoint cannot pass the normal
        // cluster-wide application barrier: that same owner is the node the barrier is waiting
        // for. Its processor scope is already fenced by the published reset, so the matching
        // request re-enters coordination directly and repairs that revision under a new cluster
        // gate. Every other request first catches this node up in the usual way.
        if !plan.published {
            self.apply_current_cluster_state()
                .await
                .change_context_lazy(|| WasmStateResetError::Prepare {
                    processor: processor.clone(),
                })?;
            plan = self
                .plan_wasm_state_reset(domain, processor, &request, target)
                .await?;
        }
        let purpose = EntityGatePurpose::WasmStateReset(plan.scope);
        let deadline = tokio::time::Instant::now()
            .checked_add(self.inner.runtime.entity_gate_deadline())
            .assured("the configured entity-gate duration stays within the monotonic clock");
        let gate = self
            .engage_cluster_entity_gates(
                domain,
                &plan.relays,
                std::slice::from_ref(&plan.entity),
                purpose,
                deadline,
            )
            .await
            .change_context_lazy(|| WasmStateResetError::Gate {
                processor: processor.clone(),
            })?;

        // Engagement publishes a branch-selective gate and waits for every dispatch permit that
        // entered that scope before publication. The processor command then drains its accepted
        // mailbox before replacing selected branch tasks, so waiting on domain-wide counters here
        // would unnecessarily stop sibling branches.
        let result = self
            .reset_wasm_processor_state_while_gated(
                domain, processor, &request, plan, &gate, mutation,
            )
            .await;
        self.release_cluster_entity_gates(gate).await;
        result
    }

    async fn plan_wasm_state_reset(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        request: &CommandExecutionReference,
        target: WasmStateResetTarget,
    ) -> error_stack::Result<WasmStateResetPlan, WasmStateResetError> {
        let inputs = self.inner.consensus.domain_planning_inputs(domain).await;
        let Some(state) = inputs.state() else {
            return Err(Report::new(WasmStateResetError::DomainUnavailable {
                domain: domain.clone(),
            }));
        };
        if state.status != DomainStatus::Running {
            return Err(Report::new(WasmStateResetError::DomainNotRunning {
                domain: domain.clone(),
            }));
        }
        let Some(schedule) = inputs.schedule().cloned() else {
            return Err(Report::new(WasmStateResetError::ProcessorUnavailable {
                domain: domain.clone(),
                processor: processor.clone(),
            }));
        };
        let entity = NodeRef::new(ModelKind::WasmProcessor, processor.clone());
        let Some(node) = schedule.nodes.get(&entity) else {
            return Err(Report::new(WasmStateResetError::ProcessorUnavailable {
                domain: domain.clone(),
                processor: processor.clone(),
            }));
        };
        let Some(wasm) = node.wasm_processor() else {
            return Err(Report::new(WasmStateResetError::ProcessorUnavailable {
                domain: domain.clone(),
                processor: processor.clone(),
            }));
        };
        let (scope, branch_key) = Self::resolve_wasm_state_reset_target(
            domain,
            processor,
            node.resolved_branching.as_ref(),
            target,
        )?;
        let mut published = false;
        let mut ready = false;
        if let Some(current) = node.wasm_state_reset() {
            if current.request() == request {
                if current.scope() != &scope {
                    return Err(Report::new(WasmStateResetError::RequestConflict {
                        processor: processor.clone(),
                        request: request.clone(),
                    }));
                }
                published = true;
                ready = current.phase() == WasmStateResetPhase::Ready;
            } else if current.phase() == WasmStateResetPhase::Publishing {
                return Err(Report::new(WasmStateResetError::RequestConflict {
                    processor: processor.clone(),
                    request: request.clone(),
                }));
            }
        }
        let owner = node.execution_node().cloned().ok_or_else(|| {
            Report::new(WasmStateResetError::OwnerUnavailable {
                domain: domain.clone(),
                processor: processor.clone(),
            })
        })?;
        let relays = wasm.from.relays().to_vec();
        Ok(WasmStateResetPlan {
            inputs,
            schedule,
            owner,
            relays,
            entity,
            scope,
            branch_key,
            published,
            ready,
        })
    }

    fn resolve_wasm_state_reset_target(
        domain: &DomainName,
        processor: &ModelName,
        branching: Option<&ResolvedBranching>,
        target: WasmStateResetTarget,
    ) -> error_stack::Result<
        (WasmStateResetScope, Option<Vec<RemoteRuntimeField>>),
        WasmStateResetError,
    > {
        let invalid = || WasmStateResetError::InvalidTarget {
            domain: domain.clone(),
            processor: processor.clone(),
        };
        match (branching, target) {
            (Some(ResolvedBranching::Unbranched), WasmStateResetTarget::Unbranched) => {
                Ok((WasmStateResetScope::Unbranched, None))
            }
            (
                Some(ResolvedBranching::Branched { schema, .. }),
                WasmStateResetTarget::AllBranches,
            ) if !schema.fields.is_empty() => Ok((WasmStateResetScope::AllBranches, None)),
            (
                Some(ResolvedBranching::Branched { schema, .. }),
                WasmStateResetTarget::Branch(fields),
            ) if !schema.fields.is_empty() => {
                let supplied = fields
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<BTreeSet<_>>();
                let expected = schema
                    .fields
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<BTreeSet<_>>();
                if supplied.len() != fields.len() || supplied != expected {
                    return Err(Report::new(invalid()));
                }
                let scope = Runtime::wasm_state_reset_branch_scope(processor, fields.clone())
                    .change_context_lazy(invalid)?;
                Ok((scope, Some(fields)))
            }
            _ => Err(Report::new(invalid())),
        }
    }

    async fn reset_wasm_processor_state_while_gated(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        request: &CommandExecutionReference,
        mut plan: WasmStateResetPlan,
        gate: &ClusterEntityGate,
        mutation: Option<&DomainMutationLease>,
    ) -> error_stack::Result<(), WasmStateResetError> {
        if plan.ready {
            return self
                .activate_wasm_state_reset_schedule(
                    gate,
                    &plan.owner,
                    processor,
                    request,
                    plan.scope,
                )
                .await
                .change_context_lazy(|| WasmStateResetError::CommittedNotUsable {
                    processor: processor.clone(),
                });
        }

        self.send_wasm_state_reset_action(
            &plan.owner,
            gate,
            processor,
            request,
            plan.scope,
            RemoteWasmStateResetRuntimeAction::Prepare {
                branch_key: plan.branch_key.clone(),
                published: plan.published,
            },
        )
        .await
        .change_context_lazy(|| WasmStateResetError::Prepare {
            processor: processor.clone(),
        })?;

        if !plan.published {
            let node = plan
                .schedule
                .nodes
                .get_mut(&plan.entity)
                .verified("the reset plan retained the WASM node it resolved");
            let began = node.begin_wasm_state_reset(request.clone(), plan.scope);
            if !began {
                self.abort_unpublished_wasm_state_reset(
                    &plan.owner,
                    gate,
                    processor,
                    request,
                    plan.scope,
                )
                .await?;
                return Err(Report::new(WasmStateResetError::Publish {
                    processor: processor.clone(),
                }));
            }
            #[cfg(feature = "testing")]
            if self
                .inner
                .runtime
                .take_armed_schedule_publication_fault(domain)
            {
                self.abort_unpublished_wasm_state_reset(
                    &plan.owner,
                    gate,
                    processor,
                    request,
                    plan.scope,
                )
                .await?;
                return Err(Report::new(WasmStateResetError::Publish {
                    processor: processor.clone(),
                }));
            }
            let publication = self
                .inner
                .consensus
                .replace_domain_schedule(plan.inputs.clone(), Some(plan.schedule), mutation)
                .await;
            if let Err(error) = publication {
                let observed = self.inner.consensus.domain_planning_inputs(domain).await;
                if Self::schedule_has_wasm_reset(
                    observed.schedule(),
                    &plan.entity,
                    request,
                    plan.scope,
                    WasmStateResetPhase::Publishing,
                ) {
                    plan.published = true;
                } else {
                    self.abort_unpublished_wasm_state_reset(
                        &plan.owner,
                        gate,
                        processor,
                        request,
                        plan.scope,
                    )
                    .await?;
                    return Err(
                        Report::new(error).change_context(WasmStateResetError::Publish {
                            processor: processor.clone(),
                        }),
                    );
                }
            } else {
                plan.published = true;
            }
        }

        self.activate_wasm_state_reset_schedule(gate, &plan.owner, processor, request, plan.scope)
            .await
            .change_context_lazy(|| WasmStateResetError::CommittedNotUsable {
                processor: processor.clone(),
            })?;

        let ready_inputs = self.inner.consensus.domain_planning_inputs(domain).await;
        let ready_already_committed = Self::schedule_has_wasm_reset(
            ready_inputs.schedule(),
            &plan.entity,
            request,
            plan.scope,
            WasmStateResetPhase::Ready,
        );
        if !ready_already_committed {
            let mut ready_schedule = ready_inputs.schedule().cloned().ok_or_else(|| {
                Report::new(WasmStateResetError::CommittedNotUsable {
                    processor: processor.clone(),
                })
            })?;
            let node = ready_schedule.nodes.get_mut(&plan.entity).ok_or_else(|| {
                Report::new(WasmStateResetError::CommittedNotUsable {
                    processor: processor.clone(),
                })
            })?;
            if !node.complete_wasm_state_reset(request) {
                return Err(Report::new(WasmStateResetError::CommittedNotUsable {
                    processor: processor.clone(),
                }));
            }
            #[cfg(feature = "testing")]
            if self
                .inner
                .runtime
                .take_armed_schedule_publication_fault(domain)
            {
                return Err(Report::new(WasmStateResetError::CommittedNotUsable {
                    processor: processor.clone(),
                }));
            }
            if let Err(error) = self
                .inner
                .consensus
                .replace_domain_schedule(ready_inputs, Some(ready_schedule), mutation)
                .await
            {
                let observed = self.inner.consensus.domain_planning_inputs(domain).await;
                if !Self::schedule_has_wasm_reset(
                    observed.schedule(),
                    &plan.entity,
                    request,
                    plan.scope,
                    WasmStateResetPhase::Ready,
                ) {
                    return Err(Report::new(error).change_context(
                        WasmStateResetError::CommittedNotUsable {
                            processor: processor.clone(),
                        },
                    ));
                }
            }
        }

        self.activate_wasm_state_reset_schedule(gate, &plan.owner, processor, request, plan.scope)
            .await
            .change_context_lazy(|| WasmStateResetError::CommittedNotUsable {
                processor: processor.clone(),
            })
    }

    fn schedule_has_wasm_reset(
        schedule: Option<&DomainSchedule>,
        entity: &NodeRef,
        request: &CommandExecutionReference,
        scope: WasmStateResetScope,
        phase: WasmStateResetPhase,
    ) -> bool {
        let Some(schedule) = schedule else {
            return false;
        };
        let Some(node) = schedule.nodes.get(entity) else {
            return false;
        };
        let Some(reset) = node.wasm_state_reset() else {
            return false;
        };
        reset.request() == request && reset.scope() == &scope && reset.phase() == phase
    }

    async fn abort_unpublished_wasm_state_reset(
        &self,
        owner: &ClusterNodeName,
        gate: &ClusterEntityGate,
        processor: &ModelName,
        request: &CommandExecutionReference,
        scope: WasmStateResetScope,
    ) -> error_stack::Result<(), WasmStateResetError> {
        self.send_wasm_state_reset_action(
            owner,
            gate,
            processor,
            request,
            scope,
            RemoteWasmStateResetRuntimeAction::Abort,
        )
        .await
        .change_context_lazy(|| WasmStateResetError::Abort {
            processor: processor.clone(),
        })
    }

    async fn activate_wasm_state_reset_schedule(
        &self,
        gate: &ClusterEntityGate,
        owner: &ClusterNodeName,
        processor: &ModelName,
        request: &CommandExecutionReference,
        scope: WasmStateResetScope,
    ) -> error_stack::Result<(), WasmStateResetError> {
        let nodes = gate.nodes();
        let replica_activations =
            nodes
                .iter()
                .filter(|node| *node != owner)
                .map(|node| async move {
                    self.send_wasm_state_reset_action(
                        node,
                        gate,
                        processor,
                        request,
                        scope,
                        RemoteWasmStateResetRuntimeAction::ActivateCommittedSchedule,
                    )
                    .await
                    .change_context_lazy(|| {
                        WasmStateResetError::CommittedNotUsable {
                            processor: processor.clone(),
                        }
                    })
                });
        try_join_all(replica_activations).await?;
        self.send_wasm_state_reset_action(
            owner,
            gate,
            processor,
            request,
            scope,
            RemoteWasmStateResetRuntimeAction::ActivateCommittedSchedule,
        )
        .await
        .change_context_lazy(|| WasmStateResetError::CommittedNotUsable {
            processor: processor.clone(),
        })
    }

    async fn send_wasm_state_reset_action(
        &self,
        node: &ClusterNodeName,
        gate: &ClusterEntityGate,
        processor: &ModelName,
        request: &CommandExecutionReference,
        scope: WasmStateResetScope,
        action: RemoteWasmStateResetRuntimeAction,
    ) -> error_stack::Result<(), WasmStateResetError> {
        if node == self.inner.consensus.local_node_id() {
            return match action {
                RemoteWasmStateResetRuntimeAction::Prepare {
                    branch_key,
                    published,
                } => {
                    let preparation = WasmStateResetPreparation::from_remote(
                        processor,
                        request.clone(),
                        scope,
                        branch_key,
                        published,
                    )
                    .change_context_lazy(|| WasmStateResetError::Prepare {
                        processor: processor.clone(),
                    })?;
                    self.inner
                        .runtime
                        .prepare_wasm_state_reset(
                            &gate.coordination,
                            &gate.domain,
                            processor,
                            preparation,
                        )
                        .await
                        .change_context_lazy(|| WasmStateResetError::Prepare {
                            processor: processor.clone(),
                        })
                }
                RemoteWasmStateResetRuntimeAction::ActivateCommittedSchedule => {
                    self.inner
                        .runtime
                        .verify_wasm_state_reset_gate(
                            &gate.coordination,
                            &gate.domain,
                            processor,
                            scope,
                        )
                        .change_context_lazy(|| WasmStateResetError::Gate {
                            processor: processor.clone(),
                        })?;
                    self.apply_committed_wasm_state_reset_schedule_locally()
                        .await
                        .change_context_lazy(|| WasmStateResetError::CommittedNotUsable {
                            processor: processor.clone(),
                        })
                }
                RemoteWasmStateResetRuntimeAction::Abort => self
                    .inner
                    .runtime
                    .abort_wasm_state_reset(
                        &gate.coordination,
                        &gate.domain,
                        processor,
                        request.clone(),
                        scope,
                    )
                    .await
                    .change_context_lazy(|| WasmStateResetError::Abort {
                        processor: processor.clone(),
                    }),
            };
        }
        self.inner
            .interconnect
            .request(
                node,
                RemoteWasmStateResetRuntimeRequest {
                    coordination: gate.coordination.clone(),
                    domain: gate.domain.clone(),
                    processor: processor.clone(),
                    request: request.clone(),
                    scope,
                    action,
                },
            )
            .await
            .change_context_lazy(|| WasmStateResetError::CommittedNotUsable {
                processor: processor.clone(),
            })?
            .result
            .map_err(|failure| {
                Report::new(WasmStateResetError::CommittedNotUsable {
                    processor: processor.clone(),
                })
                .attach_printable(failure)
            })
    }

    pub(in crate::application) async fn handle_wasm_state_reset_runtime_request(
        &self,
        request: RemoteWasmStateResetRuntimeRequest,
    ) -> Result<(), nervix_interconnect::RemoteOperationFailure> {
        let subject = RemoteOperationSubject::entity(
            &request.domain,
            ModelKind::WasmProcessor,
            request.processor.clone(),
        );
        let result = match request.action {
            RemoteWasmStateResetRuntimeAction::Prepare {
                branch_key,
                published,
            } => {
                let preparation = WasmStateResetPreparation::from_remote(
                    &request.processor,
                    request.request,
                    request.scope,
                    branch_key,
                    published,
                );
                match preparation {
                    Ok(preparation) => self
                        .inner
                        .runtime
                        .prepare_wasm_state_reset(
                            &request.coordination,
                            &request.domain,
                            &request.processor,
                            preparation,
                        )
                        .await
                        .map_err(|error| format!("{error:#}")),
                    Err(error) => Err(format!("{error:#}")),
                }
            }
            RemoteWasmStateResetRuntimeAction::ActivateCommittedSchedule => {
                let verification = self.inner.runtime.verify_wasm_state_reset_gate(
                    &request.coordination,
                    &request.domain,
                    &request.processor,
                    request.scope,
                );
                match verification {
                    Ok(()) => self
                        .apply_committed_wasm_state_reset_schedule_locally()
                        .await
                        .map_err(|error| format!("{error:#}")),
                    Err(error) => Err(format!("{error:#}")),
                }
            }
            RemoteWasmStateResetRuntimeAction::Abort => self
                .inner
                .runtime
                .abort_wasm_state_reset(
                    &request.coordination,
                    &request.domain,
                    &request.processor,
                    request.request,
                    request.scope,
                )
                .await
                .map_err(|error| format!("{error:#}")),
        };
        result
            .map_err(|reason| nervix_interconnect::RemoteOperationFailure::failed(subject, reason))
    }

    /// Apply the committed reset schedule to this node without joining the ordinary cluster-wide
    /// runtime-revision barrier. The reset coordinator orders every live replica before the owner,
    /// and a failed owner checkpoint is precisely what prevents that ordinary barrier from
    /// completing. The background runtime applicator remains the sole publisher of this node's
    /// prepared and ready revision observations.
    async fn apply_committed_wasm_state_reset_schedule_locally(
        &self,
    ) -> error_stack::Result<(), crate::runtime::RuntimeError> {
        let state = self.inner.consensus.current_runtime_state().await;
        self.inner
            .runtime
            .apply_cluster_state(
                self.inner.consensus.local_node_id(),
                state.revision,
                &state.domains,
                &state.domain_clock_authorities,
                &state.schedule,
            )
            .await
            .map_err(Report::new)
    }
}
