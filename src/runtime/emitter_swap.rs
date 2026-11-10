//! Swapping the tasks of one emitter while a schedule's entity swap applies.
//!
//! Layer: data plane.
//! - **Owns.** Stopping the outgoing task of each emitter unit to swap, moving its relay consumer
//!   registrations to the incoming plan, and spawning the unit's replacement task where it runs.
//! - **Depends on.** The installed domain execution, the decided emitter plan and its served relay
//!   units, and the emitter task supervisor.
//! - **Must not know.** Placement decisions, connector drivers or the schedule's other entities.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;

use super::{emitter_supervision::EmitterInput, *};
use crate::emitter_execution_plan::EmitterInputUnits;

impl Runtime {
    /// Replaces the tasks of `entity`'s units in `domain` with tasks of the emitter `revision`
    /// plans. A changed emitter (`replace_all`) replaces every unit; a reassignment only the units
    /// whose local placement changed, so the other instances keep their tasks and connectors.
    pub(super) async fn swap_emitter_entity(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        revision: &Arc<ExecutionRevision>,
        replace_all: bool,
        local_node_id: Option<&ClusterNodeName>,
    ) -> error_stack::Result<(), RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        let emitter_name = EmitterName::from(&entity.identifier);
        let Some(desired_emitter) = revision.emitters.emitter(&emitter_name).cloned() else {
            return Err(Report::new(ExecutionBuildError::MissingDesiredNode {
                node: entity.clone(),
            })
            .change_context(build()));
        };
        let desired_node = revision
            .nodes
            .get(entity)
            .assured("the emitter plan was decided from this same schedule");
        let emitter_swap_unavailable = || {
            Report::new(ExecutionBuildError::ExecutionUnavailable {
                step: ExecutionStep::EmitterSwap,
            })
            .change_context(build())
        };
        let old_emitter = {
            let Some(execution) = self.inner.executions.get(domain) else {
                return Err(emitter_swap_unavailable());
            };
            let Some(old_emitter) = execution.revision.emitters.emitter(&emitter_name).cloned()
            else {
                return Err(Report::new(ExecutionBuildError::MissingInstalledNode {
                    node: entity.clone(),
                })
                .change_context(build()));
            };
            old_emitter
        };
        let units =
            self.processor_units_to_swap(domain, entity, revision, replace_all, local_node_id);
        for unit in units {
            nervix_primitives::task::consume_budget().await;
            let owner = NodeUnitRef::new(entity.clone(), unit);
            let old_task = {
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(emitter_swap_unavailable());
                };
                execution.emitter_tasks.remove(&owner)
            };
            let had_old_task = old_task.is_some();
            if let Some(old_task) = old_task
                && let Err(failure) = old_task.stop(self.domain_drain_timeout()).await
            {
                let drain = failure.drain_description();
                let (error, old_task) = failure.into_parts();
                if let Some(mut execution) = self.inner.executions.get_mut(domain) {
                    execution.emitter_tasks.insert(owner, old_task);
                }
                return Err(error
                    .change_context(ExecutionBuildError::StopEmitter {
                        emitter: emitter_name,
                        drain,
                    })
                    .change_context(build()));
            }

            let executes_locally =
                Self::scheduled_unit_executes_locally(desired_node, unit, local_node_id)
                    && !desired_emitter.is_idle(unit);
            /// What spawning the swapped emitter unit's task needs from the domain
            /// execution, taken while it is borrowed so the spawn itself runs without
            /// holding that borrow.
            struct EmitterSpawnInputs {
                shutdown: watch::Sender<bool>,
                codecs: HashMap<CodecName, Arc<CompiledCodec>>,
                deps: EmitterTaskDeps,
                inputs: Vec<EmitterInput>,
            }

            let spawn = {
                let Some(execution) = self.inner.executions.get_mut(domain) else {
                    return Err(emitter_swap_unavailable());
                };
                if had_old_task {
                    for input in &old_emitter.inputs {
                        let Some(services) = execution.relay_services.get(&input.relay) else {
                            continue;
                        };
                        match old_emitter.input_units(unit, input) {
                            EmitterInputUnits::WholeRelay => {
                                services.remove_local_runtime_consumer(old_emitter.mode);
                            }
                            EmitterInputUnits::Units(relay_units) => {
                                for relay_unit in relay_units {
                                    services.remove_local_unit_runtime_consumer(
                                        old_emitter.mode,
                                        relay_unit,
                                    );
                                }
                            }
                        }
                    }
                }
                if !executes_locally {
                    None
                } else {
                    let mut inputs = Vec::new();
                    for input in &desired_emitter.inputs {
                        let Some(services) = execution.relay_services.get(&input.relay) else {
                            return Err(Report::new(ExecutionBuildError::MissingRelayBoundary {
                                relay: input.relay.clone(),
                            })
                            .change_context(build()));
                        };
                        match desired_emitter.input_units(unit, input) {
                            EmitterInputUnits::WholeRelay => inputs.push(EmitterInput {
                                relay: input.relay.clone(),
                                relay_unit: None,
                                fan_in: services.add_local_runtime_consumer(desired_emitter.mode),
                            }),
                            EmitterInputUnits::Units(relay_units) => {
                                for relay_unit in relay_units {
                                    inputs.push(EmitterInput {
                                        relay: input.relay.clone(),
                                        relay_unit: Some(relay_unit),
                                        fan_in: services.add_local_unit_runtime_consumer(
                                            desired_emitter.mode,
                                            relay_unit,
                                        ),
                                    });
                                }
                            }
                        }
                    }
                    let deps = self
                        .emitter_task_deps(
                            ExecutionBuildDeps::from_routing(domain, &execution),
                            &desired_emitter,
                        )
                        .change_context_lazy(build)?;
                    Some(EmitterSpawnInputs {
                        shutdown: execution.shutdown.clone(),
                        codecs: execution.codecs.clone(),
                        deps,
                        inputs,
                    })
                }
            };
            if let Some(spawn) = spawn {
                let task = self
                    .spawn_emitter_task(
                        EmitterTaskBuildDeps {
                            domain,
                            shutdown_tx: &spawn.shutdown,
                            codecs: &spawn.codecs,
                            deps: spawn.deps,
                        },
                        desired_emitter.as_ref().clone(),
                        unit,
                        spawn.inputs,
                    )
                    .change_context_lazy(build)?;
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(emitter_swap_unavailable());
                };
                execution.emitter_tasks.insert(owner, task);
            }
            #[cfg(feature = "testing")]
            if had_old_task && !executes_locally {
                self.inner
                    .fault_injection
                    .pause_emitter_swap_after_detach_if_armed(domain, &emitter_name)
                    .await;
            }
        }
        Ok(())
    }
}
