//! Coordinated replacement of branch-local WASM guest-state lifetimes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Fresh guest initialization, the prepared reset residue held by a processor
//!   supervisor, and durable initial checkpoints for a newly published state generation.
//! - **Depends on.** Planned processor templates, branch runtimes, guest execution, and replicated
//!   WASM state.
//! - **Must not know.** NSPL text, consensus mutations, administrative protocols, or scheduling
//!   policy.

use error_stack::{Report, ResultExt as _};

use super::*;

#[derive(Debug, thiserror::Error)]
pub(crate) enum WasmStateResetRuntimeError {
    #[error("WASM processor '{}' reset does not own its entity gate", .processor.as_str())]
    GateNotHeld { processor: ModelName },
    #[error("WASM processor '{}' is not running on this node", .processor.as_str())]
    ProcessorUnavailable { processor: ModelName },
    #[error("processor '{}' is not a WASM processor", .processor.as_str())]
    NotWasmProcessor { processor: ModelName },
    #[error(
        "WASM processor '{}' reset target does not match its branch declaration",
        .processor.as_str()
    )]
    InvalidScope { processor: ModelName },
    #[error("WASM processor '{}' has no active execution for the selected branch", .processor.as_str())]
    BranchUnavailable { processor: ModelName },
    #[error("WASM processor '{}' reset branch key is invalid", .processor.as_str())]
    InvalidBranchKey { processor: ModelName },
    #[error(
        "WASM processor '{}' is already preparing another state reset",
        .processor.as_str()
    )]
    RequestConflict { processor: ModelName },
    #[error(
        "failed to stop a selected branch of WASM processor '{}' at the reset boundary",
        .processor.as_str()
    )]
    StopBranch { processor: ModelName },
    #[error(
        "failed to initialize fresh guest state for WASM processor '{}'",
        .processor.as_str()
    )]
    FreshInitialization { processor: ModelName },
    #[error(
        "failed to persist the initial reset checkpoint of WASM processor '{}'",
        .processor.as_str()
    )]
    InitialCheckpoint { processor: ModelName },
    #[error(
        "failed to restore WASM processor '{}' after its reset was aborted",
        .processor.as_str()
    )]
    RestoreAfterAbort { processor: ModelName },
    #[error("WASM processor '{}' reset command channel closed", .processor.as_str())]
    CommandUnavailable { processor: ModelName },
    #[error("WASM processor '{}' dropped its reset command response", .processor.as_str())]
    ResponseDropped { processor: ModelName },
}

#[derive(Debug)]
pub(crate) struct WasmStateResetPreparation {
    pub(super) request: CommandExecutionReference,
    pub(super) scope: WasmStateResetScope,
    pub(super) branch_key: Option<BranchKey>,
    pub(super) published: bool,
}

impl WasmStateResetPreparation {
    pub(crate) fn from_remote(
        processor: &ModelName,
        request: CommandExecutionReference,
        scope: WasmStateResetScope,
        branch_key: Option<Vec<RemoteRuntimeField>>,
        published: bool,
    ) -> error_stack::Result<Self, WasmStateResetRuntimeError> {
        let branch_key = BranchKey::from_remote_key(branch_key).map_err(|reason| {
            Report::new(WasmStateResetRuntimeError::InvalidBranchKey {
                processor: processor.clone(),
            })
            .attach_printable(reason)
        })?;
        Ok(Self {
            request,
            scope,
            branch_key,
            published,
        })
    }
}

pub(super) struct PreparedWasmStateResetBranch {
    pub(super) key: Option<BranchKey>,
    pub(super) initial_state: Option<Vec<u8>>,
    pub(super) previous: Option<ProcessorBranchHandoff>,
    pub(super) activated: bool,
}

pub(super) struct PreparedWasmStateReset {
    pub(super) request: CommandExecutionReference,
    pub(super) scope: WasmStateResetScope,
    pub(super) published: bool,
    pub(super) branches: Vec<PreparedWasmStateResetBranch>,
}

impl PreparedWasmStateReset {
    pub(super) fn matches(
        &self,
        request: &CommandExecutionReference,
        scope: WasmStateResetScope,
    ) -> bool {
        &self.request == request && self.scope == scope
    }
}

impl BranchInstanceTemplate {
    /// Instantiate a selected branch without a saved state and capture the state its fresh guest
    /// reports. The returned bytes stay in memory until a new generation has been published; this
    /// method never writes them under the generation it is replacing.
    pub(super) async fn prepare_fresh_wasm_state(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        key: Option<BranchKey>,
    ) -> error_stack::Result<Vec<u8>, WasmStateResetRuntimeError> {
        let processor = ModelName::from(&self.source);
        let fresh = || WasmStateResetRuntimeError::FreshInitialization {
            processor: processor.clone(),
        };
        let mut branch = self
            .instantiate(runtime, domain, key.clone())
            .change_context_lazy(fresh)?
            .into_inner();
        branch.refresh_domain_routing().change_context_lazy(fresh)?;
        let template = self.processors.get(&processor).ok_or_else(|| {
            Report::new(WasmStateResetRuntimeError::NotWasmProcessor {
                processor: processor.clone(),
            })
        })?;
        let RelayProcessorOperationTemplate::WasmProcessor {
            output_routes,
            resource,
            resource_version,
            file,
            limits,
            compiled,
            ..
        } = &template.operation
        else {
            return Err(Report::new(WasmStateResetRuntimeError::NotWasmProcessor {
                processor,
            }));
        };
        let input_relay = template.input_relays.first().ok_or_else(|| {
            Report::new(WasmStateResetRuntimeError::FreshInitialization {
                processor: processor.clone(),
            })
        })?;
        let input_schema = branch
            .relay_schema(input_relay)
            .change_context_lazy(fresh)?;
        let mut output_schemas = Vec::with_capacity(output_routes.routes.len());
        for output in &output_routes.routes {
            tokio::task::consume_budget().await;
            let schema = branch
                .relay_schema(&output.output_relay)
                .change_context_lazy(fresh)?;
            output_schemas.push((output.output_relay.clone(), schema));
        }
        let compiled = match compiled {
            Some(compiled) => compiled.clone(),
            None => runtime
                .compile_wasm_processor_module(
                    domain,
                    &processor,
                    resource,
                    *resource_version,
                    file,
                )
                .await
                .change_context_lazy(fresh)?,
        };
        if runtime
            .inner
            .fault_injection
            .wasm_state_reset_fresh_initialization_fails()
        {
            return Err(Report::new(fresh()));
        }
        let execution_now = branch
            .domain_clock
            .snapshot()
            .change_context_lazy(fresh)?
            .now();
        let init = WasmBranchInit {
            domain_name: domain.as_str().to_string(),
            domain_type: "runtime".to_string(),
            branch_key: key
                .as_ref()
                .map(|branch| branch.as_str().as_bytes().to_vec()),
            input_schema: input_schema.wasm_processor_schema(input_relay.as_str().to_string()),
            output_schemas: output_schemas
                .iter()
                .map(|(relay, schema)| schema.wasm_processor_schema(relay.as_str().to_string()))
                .collect(),
        };
        let module = WasmBranchModule {
            processor: processor.clone(),
            branch: key,
            resource: ResourceId::new(domain.clone(), resource.clone(), *resource_version),
            file: file.clone(),
        };
        let mut live = compiled
            .instantiate_branch(module, *limits, init, execution_now, None)
            .await
            .change_context_lazy(fresh)?;
        live.guest
            .save_state_in_context(nervix_wasm::WasmExecutionContext::new(execution_now))
            .await
            .map_err(|error| live.module.guest_failure(error, None))
            .change_context_lazy(fresh)
    }
}

impl Runtime {
    pub(crate) fn wasm_state_reset_branch_scope(
        processor: &ModelName,
        fields: Vec<RemoteRuntimeField>,
    ) -> error_stack::Result<WasmStateResetScope, WasmStateResetRuntimeError> {
        let branch = BranchKey::from_remote_key(Some(fields)).map_err(|reason| {
            Report::new(WasmStateResetRuntimeError::InvalidBranchKey {
                processor: processor.clone(),
            })
            .attach_printable(reason)
        })?;
        let branch = branch.verified("a present remote key decodes to a present branch key");
        Ok(WasmStateResetScope::Branch(branch.fingerprint()))
    }

    fn wasm_state_reset_commands(
        &self,
        domain: &DomainName,
        processor: &ModelName,
    ) -> error_stack::Result<mpsc::Sender<ProcessorNodeCommand>, WasmStateResetRuntimeError> {
        let commands = self.inner.executions.get(domain).and_then(|execution| {
            execution
                .node_tasks
                .get(&NodeRef::new(ModelKind::WasmProcessor, processor.clone()))
                .map(|task| task.commands.clone())
        });
        commands.ok_or_else(|| {
            Report::new(WasmStateResetRuntimeError::ProcessorUnavailable {
                processor: processor.clone(),
            })
        })
    }

    pub(crate) fn verify_wasm_state_reset_gate(
        &self,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
        processor: &ModelName,
        scope: WasmStateResetScope,
    ) -> error_stack::Result<(), WasmStateResetRuntimeError> {
        let entity = NodeRef::new(ModelKind::WasmProcessor, processor.clone());
        if self.entity_gate_operation_owns_entity(
            coordination,
            domain,
            &entity,
            EntityGatePurpose::WasmStateReset(scope),
        ) {
            return Ok(());
        }
        Err(Report::new(WasmStateResetRuntimeError::GateNotHeld {
            processor: processor.clone(),
        }))
    }

    pub(crate) async fn prepare_wasm_state_reset(
        &self,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
        processor: &ModelName,
        preparation: WasmStateResetPreparation,
    ) -> error_stack::Result<(), WasmStateResetRuntimeError> {
        self.verify_wasm_state_reset_gate(coordination, domain, processor, preparation.scope)?;
        let commands = self.wasm_state_reset_commands(domain, processor)?;
        let (response, receiver) = oneshot::channel();
        commands
            .send(ProcessorNodeCommand::PrepareWasmStateReset {
                preparation,
                response,
            })
            .await
            .map_err(|_| {
                Report::new(WasmStateResetRuntimeError::CommandUnavailable {
                    processor: processor.clone(),
                })
            })?;
        receiver.await.map_err(|_| {
            Report::new(WasmStateResetRuntimeError::ResponseDropped {
                processor: processor.clone(),
            })
        })?
    }

    pub(crate) async fn abort_wasm_state_reset(
        &self,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
        processor: &ModelName,
        request: CommandExecutionReference,
        scope: WasmStateResetScope,
    ) -> error_stack::Result<(), WasmStateResetRuntimeError> {
        self.verify_wasm_state_reset_gate(coordination, domain, processor, scope)?;
        let commands = self.wasm_state_reset_commands(domain, processor)?;
        let (response, receiver) = oneshot::channel();
        commands
            .send(ProcessorNodeCommand::AbortWasmStateReset { request, response })
            .await
            .map_err(|_| {
                Report::new(WasmStateResetRuntimeError::CommandUnavailable {
                    processor: processor.clone(),
                })
            })?;
        receiver.await.map_err(|_| {
            Report::new(WasmStateResetRuntimeError::ResponseDropped {
                processor: processor.clone(),
            })
        })?
    }

    /// Make a fresh snapshot the first committed checkpoint of the selected branch's currently
    /// published generation. This uses the same local durability and replica-confirmation boundary
    /// as an ordinary callback checkpoint.
    pub(super) async fn commit_wasm_state_reset_checkpoint(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        key: Option<BranchKey>,
        bytes: Vec<u8>,
    ) -> error_stack::Result<(), WasmStateResetRuntimeError> {
        let failed = || WasmStateResetRuntimeError::InitialCheckpoint {
            processor: processor.clone(),
        };
        let placement = self
            .state_placement(
                domain,
                RuntimeStateKind::WasmProcessor,
                ModelKind::WasmProcessor,
                processor,
                key,
            )
            .change_context_lazy(failed)?;
        let state = self
            .replicated_wasm_processor_state(placement)
            .change_context_lazy(failed)?;
        let boundary = self
            .wasm_checkpoint_boundary(&state)
            .change_context_lazy(failed)?;
        let captured = state.capture(bytes, boundary);
        let deadline = Instant::now()
            .checked_add(WASM_CHECKPOINT_DEADLINE)
            .assured("the configured WASM checkpoint deadline stays within Instant");
        let durable = self
            .persist_wasm_checkpoint(&state, captured, deadline)
            .await
            .change_context_lazy(failed)?;
        let completed = self
            .confirm_wasm_checkpoint(&state, durable, deadline)
            .await
            .change_context_lazy(failed)?;
        state.commit(completed);
        Ok(())
    }
}
