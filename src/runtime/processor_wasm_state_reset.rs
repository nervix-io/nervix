//! A processor task's part in a coordinated WASM guest-state reset.
//!
//! Layer: data plane.
//!
//! - **Owns.** Selecting the branches a reset's scope reaches, preparing their replacement
//!   lifetimes, committing or aborting the prepared reset, and completing it once the schedule
//!   marks it ready.
//! - **Depends on.** Planned processor templates, concrete branch runtimes, runtime persistence and
//!   the processor's branch task lifecycle.
//! - **Must not know.** Plexus text, control-plane transactions, consensus, or connector protocols.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "processor branch creation binds the retained plan and its branch-local \
                  execution task"
    )
)]

use error_stack::ResultExt as _;

use super::{processor_branch_task::*, *};

pub(super) struct ProcessorWasmStateResetContext<'a> {
    runtime: &'a Runtime,
    domain: &'a DomainName,
    template: &'a BranchInstanceTemplate,
}

impl<'a> ProcessorWasmStateResetContext<'a> {
    pub(super) fn new(
        runtime: &'a Runtime,
        domain: &'a DomainName,
        template: &'a BranchInstanceTemplate,
    ) -> Self {
        Self {
            runtime,
            domain,
            template,
        }
    }
}

pub(super) fn processor_reset_target_keys(
    processor: &ModelName,
    template: &BranchInstanceTemplate,
    instances: &ProcessorBranches,
    scope: WasmStateResetScope,
    branch_key: Option<BranchKey>,
) -> error_stack::Result<Vec<Option<BranchKey>>, WasmStateResetRuntimeError> {
    if template.owner.node.kind != ModelKind::WasmProcessor {
        return Err(Report::new(WasmStateResetRuntimeError::NotWasmProcessor {
            processor: processor.clone(),
        }));
    }
    match (template.branch.as_ref(), scope, branch_key) {
        (None, WasmStateResetScope::Unbranched, None) => Ok(vec![None]),
        (Some(_), WasmStateResetScope::Branch(selected), Some(branch))
            if branch.fingerprint() == selected
                && instances.contains_key(&Some(branch.clone())) =>
        {
            Ok(vec![Some(branch)])
        }
        (Some(_), WasmStateResetScope::Branch(selected), Some(branch))
            if branch.fingerprint() == selected =>
        {
            Err(Report::new(WasmStateResetRuntimeError::BranchUnavailable {
                processor: processor.clone(),
            }))
        }
        (Some(_), WasmStateResetScope::Branch(selected), None) => Ok(instances
            .snapshot_entries()
            .into_iter()
            .map(|entry| entry.key)
            .filter(|key| {
                key.as_ref()
                    .is_some_and(|branch| branch.fingerprint() == selected)
            })
            .collect()),
        (Some(_), WasmStateResetScope::AllBranches, None) => Ok(instances
            .snapshot_entries()
            .into_iter()
            .map(|entry| entry.key)
            .collect()),
        _ => Err(Report::new(WasmStateResetRuntimeError::InvalidScope {
            processor: processor.clone(),
        })),
    }
}

async fn restore_processor_wasm_state_reset_branches(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
    instances: &mut ProcessorBranches,
    branches: &mut Vec<PreparedWasmStateResetBranch>,
) -> error_stack::Result<(), WasmStateResetRuntimeError> {
    let processor = ModelName::from(&template.owner.node.identifier);
    for mut branch in branches.drain(..) {
        nervix_primitives::task::consume_budget().await;
        let Some(handoff) = branch.previous.take() else {
            continue;
        };
        let restored_at = handoff.restored_at;
        let key = branch.key.clone();
        let task = nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "the missing or replacement concrete branch installs one retained task under its \
             branch key and incarnation",
            spawn_processor_branch_task(
                ProcessorRuntimeContext::new(runtime.clone(), domain.clone()),
                template,
                key.clone(),
                handoff.pending_materialized,
                ProcessorBranchLifetime::Restored,
                handoff.incarnation,
            )
        )
        .await
        .change_context_lazy(|| WasmStateResetRuntimeError::RestoreAfterAbort {
            processor: processor.clone(),
        })?;
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "the concrete branch creation transition registers its one metric identity",
            runtime.observe_branch_instance_created(domain, template.branch.as_ref(), &key)
        );
        instances.insert_changed(key, restored_at, ProcessorBranchSlot::Live(task));
    }
    Ok(())
}

impl ProcessorWasmStateResetContext<'_> {
    pub(super) async fn prepare(
        &self,
        instances: &mut ProcessorBranches,
        preparation: WasmStateResetPreparation,
        prepared: &mut Option<PreparedWasmStateReset>,
    ) -> error_stack::Result<(), WasmStateResetRuntimeError> {
        let runtime = self.runtime;
        let domain = self.domain;
        let template = self.template;
        let WasmStateResetPreparation {
            request,
            scope,
            branch_key,
            published,
            reason: _,
        } = preparation;
        let processor = ModelName::from(&template.owner.node.identifier);
        if let Some(current) = prepared.as_mut() {
            if !current.matches(&request, scope) {
                return Err(Report::new(WasmStateResetRuntimeError::RequestConflict {
                    processor,
                }));
            }
            current.published |= published;
            return Ok(());
        }

        let targets =
            processor_reset_target_keys(&processor, template, instances, scope, branch_key)?;
        let mut branches = Vec::with_capacity(targets.len());
        for key in targets {
            nervix_primitives::task::consume_budget().await;
            // A released key's state moves to the unit claiming it, whose own reset covers it.
            if let Some(slot) = instances.get(&key)
                && slot.live().is_none()
            {
                continue;
            }
            let previous = if let Some(entry) = instances.remove(&key) {
                nervix_primitives::expect_lint!(
                    nervix::lifecycle_call,
                    "the concrete branch ending or detach transition withdraws its exact metric \
                     identity",
                    runtime.observe_branch_instance_removed(
                        domain,
                        template.branch.as_ref(),
                        &key,
                        None,
                    )
                );
                let (response, receiver) = oneshot::channel();
                nervix_primitives::expect_lint!(
                    nervix::lifecycle_call,
                    "the eviction, reset or ownership transfer transition ends its exact retained \
                     branch task lifetime",
                    stop_processor_branch_task(
                        domain,
                        processor.clone(),
                        &key,
                        entry
                            .live()
                            .verified("the loop skipped every released key before removing it"),
                        ProcessorBranchStopMode::Handoff(response),
                    )
                )
                .await;
                match receiver.await {
                    Ok(handoff) => Some(handoff),
                    Err(error) => {
                        let restore = restore_processor_wasm_state_reset_branches(
                            runtime,
                            domain,
                            template,
                            instances,
                            &mut branches,
                        )
                        .await;
                        restore?;
                        return Err(Report::new(WasmStateResetRuntimeError::StopBranch {
                            processor,
                        })
                        .attach_printable(error));
                    }
                }
            } else {
                None
            };
            let initial_state = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "the state-generation reset constructs its one fresh guest before publishing the \
                 replacement save",
                template.prepare_fresh_wasm_state(runtime, domain, key.clone())
            )
            .await;
            match initial_state {
                Ok(initial_state) => branches.push(PreparedWasmStateResetBranch {
                    key,
                    initial_state: Some(initial_state),
                    previous,
                    activated: false,
                }),
                Err(error) => {
                    branches.push(PreparedWasmStateResetBranch {
                        key,
                        initial_state: None,
                        previous,
                        activated: false,
                    });
                    restore_processor_wasm_state_reset_branches(
                        runtime,
                        domain,
                        template,
                        instances,
                        &mut branches,
                    )
                    .await?;
                    return Err(error);
                }
            }
        }
        *prepared = Some(PreparedWasmStateReset {
            request,
            scope,
            published,
            branches,
        });
        Ok(())
    }
}

pub(super) async fn abort_processor_wasm_state_reset(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
    instances: &mut ProcessorBranches,
    request: &CommandExecutionReference,
    prepared: &mut Option<PreparedWasmStateReset>,
) -> error_stack::Result<(), WasmStateResetRuntimeError> {
    let processor = ModelName::from(&template.owner.node.identifier);
    let Some(current) = prepared.as_ref() else {
        return Ok(());
    };
    if &current.request != request || current.published {
        return Err(Report::new(WasmStateResetRuntimeError::RequestConflict {
            processor,
        }));
    }
    let mut current = prepared
        .take()
        .verified("the reset preparation was observed immediately before this take");
    restore_processor_wasm_state_reset_branches(
        runtime,
        domain,
        template,
        instances,
        &mut current.branches,
    )
    .await
}

impl ProcessorWasmStateResetContext<'_> {
    pub(super) async fn commit(
        &self,
        instances: &mut ProcessorBranches,
        last_persisted_lru_lsm: &mut u64,
        reset: &nervix_models::WasmStateReset,
        prepared: &mut Option<PreparedWasmStateReset>,
    ) -> error_stack::Result<(), WasmStateResetRuntimeError> {
        let runtime = self.runtime;
        let domain = self.domain;
        let template = self.template;
        let processor = ModelName::from(&template.owner.node.identifier);
        if prepared.is_none() {
            self.prepare(
                instances,
                WasmStateResetPreparation {
                    request: reset.request().clone(),
                    scope: *reset.scope(),
                    branch_key: None,
                    published: true,
                    reason: reset.reason(),
                },
                prepared,
            )
            .await?;
        }
        let current = prepared
            .as_mut()
            .verified("the preparation above either returned an error or installed reset state");
        if !current.matches(reset.request(), *reset.scope()) {
            return Err(Report::new(WasmStateResetRuntimeError::RequestConflict {
                processor,
            }));
        }
        current.published = true;

        let execution_now = nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "this task or concrete branch transition attaches its retained domain clock before \
             executing in the selected lifetime",
            runtime.bind_domain_clock(domain)
        )
        .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
            processor: processor.clone(),
        })?
        .snapshot()
        .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
            processor: processor.clone(),
        })?
        .now();
        for branch in &mut current.branches {
            nervix_primitives::task::consume_budget().await;
            if branch.activated {
                continue;
            }
            let key = branch.key.clone();
            let task = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "the missing or replacement concrete branch installs one retained task under its \
                 branch key and incarnation",
                spawn_processor_branch_task(
                    ProcessorRuntimeContext::new(runtime.clone(), domain.clone()),
                    template,
                    key.clone(),
                    VecDeque::new(),
                    ProcessorBranchLifetime::Appeared,
                    instances.next_incarnation(),
                )
            )
            .await
            .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
                processor: processor.clone(),
            })?;
            nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "the concrete branch creation transition registers its one metric identity",
                runtime.observe_branch_instance_created(domain, template.branch.as_ref(), &key)
            );
            instances.insert_changed(key, execution_now, ProcessorBranchSlot::Live(task));
            branch.activated = true;
        }
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "a committed guest-state reset offers the recreated branch lifecycle to the replicas \
             once, as the new generation begins",
            publish_branch_instance_lru_snapshot(runtime, domain, template, instances)
        )
        .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
            processor: processor.clone(),
        })?;
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "explicit checkpoint cadence or terminal teardown captures one branch lifecycle \
             generation; the record path does not invoke it",
            persist_branch_instance_lru_snapshot(
                runtime,
                domain,
                template,
                instances,
                last_persisted_lru_lsm,
            )
        )
        .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
            processor: processor.clone(),
        })?;
        let branch_lru =
            branch_lru_placement(runtime, domain, template).change_context_lazy(|| {
                WasmStateResetRuntimeError::InitialCheckpoint {
                    processor: processor.clone(),
                }
            })?;
        let branch_lru_deadline = Instant::now()
            .checked_add(WASM_CHECKPOINT_DEADLINE)
            .assured("the configured WASM checkpoint deadline stays within Instant");
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "the state-generation reset confirms its exact branch lifecycle before publishing the \
             replacement save",
            runtime.confirm_branch_lru_checkpoint(
                &branch_lru,
                instances.version(),
                branch_lru_deadline
            )
        )
        .await
        .change_context_lazy(|| WasmStateResetRuntimeError::InitialCheckpoint {
            processor: processor.clone(),
        })?;

        for branch in &mut current.branches {
            nervix_primitives::task::consume_budget().await;
            let Some(initial_state) = branch.initial_state.as_ref() else {
                continue;
            };
            nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "the state-generation reset installs and commits its one initial replacement save",
                runtime.commit_wasm_state_reset_checkpoint(
                    domain,
                    &processor,
                    template.owner.unit,
                    branch.key.clone(),
                    initial_state.clone(),
                )
            )
            .await?;
            branch.initial_state = None;
        }
        for branch in &mut current.branches {
            branch.previous.take();
        }
        Ok(())
    }
}

pub(super) fn complete_processor_wasm_state_reset(
    processor: &ModelName,
    reset: &nervix_models::WasmStateReset,
    fence: &mut Option<nervix_models::WasmStateReset>,
    prepared: &mut Option<PreparedWasmStateReset>,
) -> error_stack::Result<(), WasmStateResetRuntimeError> {
    if let Some(current) = fence.as_ref()
        && (current.request() != reset.request() || current.scope() != reset.scope())
    {
        return Err(Report::new(WasmStateResetRuntimeError::RequestConflict {
            processor: processor.clone(),
        }));
    }
    if let Some(current) = prepared.as_ref()
        && !current.matches(reset.request(), *reset.scope())
    {
        return Err(Report::new(WasmStateResetRuntimeError::RequestConflict {
            processor: processor.clone(),
        }));
    }
    *prepared = None;
    *fence = None;
    Ok(())
}
