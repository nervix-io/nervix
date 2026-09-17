//! Layer: data plane.
//! Owns: WASM processor guest state construction, persistence, replica quorum and handoff restore
//! validation.
//! May depend on: replicated WASM guest state, the state store, compiled WASM modules and schedules.
//! Must not know: control-plane transactions, NSPL parsing, or edge protocols.

use super::*;

impl Runtime {
    pub(super) async fn prepare_ownership_handoff_wasm_guests(
        &self,
        domain: &DomainName,
        scheduled: &ScheduledNode,
        checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
    ) -> OwnershipHandoffResult<()> {
        let Some(processor) = scheduled.wasm_processor() else {
            return Ok(());
        };
        let input_relay = processor.from.first().ok_or_else(|| {
            OwnershipHandoffError::state(format!(
                "wasm processor '{}' has no input relay while preparing ownership handoff",
                processor.name.as_str()
            ))
        })?;
        let (input_schema, output_schemas) = {
            let execution = self.inner.executions.get(domain).ok_or_else(|| {
                OwnershipHandoffError::state(format!(
                    "domain '{}' has no execution while preparing wasm ownership handoff",
                    domain.as_str()
                ))
            })?;
            let input_schema = execution
                .relay_schemas
                .get(input_relay)
                .cloned()
                .ok_or_else(|| {
                    OwnershipHandoffError::state(format!(
                        "wasm processor '{}' input relay '{}' has no runtime schema",
                        processor.name.as_str(),
                        input_relay.as_str()
                    ))
                })?;
            let output_schemas = processor
                .output_routes
                .outputs()
                .map(|output| {
                    let schema = execution.relay_schemas.get(&output.relay).cloned();
                    let Some(schema) = schema else {
                        return Err(OwnershipHandoffError::state(format!(
                            "wasm processor '{}' output relay '{}' has no runtime schema",
                            processor.name.as_str(),
                            output.relay.as_str()
                        )));
                    };
                    Ok((output.relay.clone(), schema))
                })
                .collect::<OwnershipHandoffResult<Vec<_>>>()?;
            (input_schema, output_schemas)
        };
        let restore = || OwnershipHandoffError::WasmRestore {
            processor: processor.name.clone().into(),
        };
        let compiled = self
            .compile_wasm_processor_module(
                domain,
                &processor.name,
                &processor.resource,
                processor.resource_version,
                &processor.file,
            )
            .await
            .change_context_lazy(restore)?;
        let domain_clock = self
            .bind_domain_clock(domain)
            .change_context_lazy(restore)?;
        let pinned = ResourceId::new(
            domain.clone(),
            processor.resource.clone(),
            processor.resource_version,
        );
        for (placement, snapshot) in checkpoints {
            tokio::task::consume_budget().await;
            if placement.state != RuntimeStateKind::WasmProcessor {
                continue;
            }
            let init = WasmBranchInit {
                domain_name: domain.as_str().to_string(),
                domain_type: "runtime".to_string(),
                branch_key: placement
                    .branch_key
                    .as_ref()
                    .map(|key| key.as_str().as_bytes().to_vec()),
                input_schema: input_schema.wasm_processor_schema(input_relay.as_str().to_string()),
                output_schemas: output_schemas
                    .iter()
                    .map(|(relay, schema)| schema.wasm_processor_schema(relay.as_str().to_string()))
                    .collect(),
            };
            let execution_now = domain_clock.snapshot().change_context_lazy(restore)?.now();
            let module = WasmBranchModule {
                processor: processor.name.clone().into(),
                branch: placement.branch_key.clone(),
                resource: pinned.clone(),
                file: processor.file.clone(),
            };
            let saved = RestorableGuestState::of_snapshot(snapshot);
            compiled
                .instantiate_branch(module, processor.limits, init, execution_now, saved)
                .await
                .change_context_lazy(restore)?;
        }
        Ok(())
    }

    pub(in crate::runtime) async fn wait_for_wasm_processor_replica_quorum(
        &self,
        state: &ReplicatedWasmProcessorState,
        lsm: u64,
    ) -> error_stack::Result<(), StateReplicationError> {
        if state.required_replica_acks == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            if state.replica_quorum_satisfied(lsm) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(Report::new(StateReplicationError::ReplicaQuorum {
                    placement: state.placement.clone(),
                    lsm,
                    required_acks: state.required_replica_acks,
                }));
            }
            tokio::select! {
                _ = state.replication_notify.notified() => {}
                _ = sleep_until(deadline) => {}
            }
        }
    }

    /// Persist the guest state a WASM processor branch saved and wait until its replicas hold it.
    pub(in crate::runtime) async fn persist_wasm_processor_snapshot(
        &self,
        state: &ReplicatedWasmProcessorState,
        saved: &WasmGuestState,
    ) -> error_stack::Result<(), StateReplicationError> {
        if let Some(store) = &self.inner.state_store {
            store
                .persist_latest_snapshot(&state.placement, saved.revision(), saved.bytes())
                .map_err(Report::new)
                .change_context(StateReplicationError::Persist {
                    placement: state.placement.clone(),
                    lsm: saved.revision(),
                })?;
            state.record_persisted(saved.revision());
            self.notify_runtime_state_replicas(&state.placement, saved.revision());
        }
        self.wait_for_wasm_processor_replica_quorum(state, saved.revision())
            .await
    }

    pub(in crate::runtime) fn replicated_wasm_processor_state(
        &self,
        placement: RuntimeStatePlacement,
        replica_nodes: Vec<ClusterNodeName>,
        required_replica_acks: usize,
    ) -> Result<Arc<ReplicatedWasmProcessorState>, RuntimePersistenceError> {
        let transferred = self.take_transferred_runtime_state_snapshot(&placement);
        if transferred.is_none()
            && let Some(existing) = self.inner.replicated_wasm_processor_states.get(&placement)
        {
            return Ok(existing.clone());
        }
        let initial = match transferred {
            Some(snapshot) => Some(snapshot),
            None => self
                .stored_runtime_state_snapshot(&placement)
                .map_err(|error| error.current_context().clone())?,
        };
        let state = Arc::new(ReplicatedWasmProcessorState::new(
            placement.clone(),
            replica_nodes,
            required_replica_acks,
            initial,
        )?);
        self.inner
            .replicated_wasm_processor_states
            .insert(placement, state.clone());
        Ok(state)
    }
}
