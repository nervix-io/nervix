//! Layer: data plane.
//! Owns: WASM processor guest state construction, the boundary a guest-state checkpoint has to
//! reach, writing a checkpoint to stable storage, waiting for its replicas, and handoff restore
//! validation.
//! May depend on: replicated WASM guest state, the state store, compiled WASM modules and schedules.
//! Must not know: control-plane transactions, NSPL parsing, or edge protocols.

use super::*;

/// How often a checkpoint waiting for its replicas reads the schedule again, so a changed
/// assignment or owner re-plans or fails the checkpoint without waiting for its deadline.
const WASM_CHECKPOINT_REPLAN_INTERVAL: Duration = Duration::from_millis(100);

impl Runtime {
    /// Wait until every assigned replica has installed the branch lifecycle that authorizes a
    /// branch checkpoint. A reset uses this before offering its first guest checkpoint, so a
    /// replica can never reject that checkpoint merely because the lifecycle announcement raced
    /// it.
    pub(in crate::runtime) async fn confirm_branch_lru_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        lsm: u64,
        deadline: Instant,
    ) -> error_stack::Result<(), StateReplicationError> {
        if !self.runtime_state_placement_is_current(placement) {
            return Err(Report::new(StateReplicationError::Superseded {
                placement: placement.clone(),
            }));
        }
        let Some(dispatcher) = self.inner.remote_dispatcher.load_full() else {
            return Ok(());
        };
        let replicas = {
            let execution = self
                .inner
                .executions
                .get(&placement.domain)
                .ok_or_else(|| {
                    Report::new(StateReplicationError::Superseded {
                        placement: placement.clone(),
                    })
                })?;
            let node = execution
                .schedule
                .nodes
                .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
                .ok_or_else(|| {
                    Report::new(StateReplicationError::Superseded {
                        placement: placement.clone(),
                    })
                })?;
            if !node.is_primary_on(dispatcher.local_node_id()) {
                return Err(Report::new(StateReplicationError::Superseded {
                    placement: placement.clone(),
                }));
            }
            node.replica_nodes()
                .into_iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        };
        if replicas.is_empty() {
            return Ok(());
        }
        loop {
            tokio::task::consume_budget().await;
            let awaiting = match self
                .inner
                .pending_state_checkpoint_announcements
                .get(placement)
            {
                Some(pending) => replicas
                    .iter()
                    .filter(|replica| {
                        pending
                            .replica_progress
                            .get(*replica)
                            .is_none_or(|progress| *progress < lsm)
                    })
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                // The announcement owner removes this entry only after every assigned replica
                // acknowledged its target LSM.
                None => return Ok(()),
            };
            if awaiting.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Report::new(StateReplicationError::ReplicaConfirmation {
                    placement: placement.clone(),
                    lsm,
                    awaiting: AwaitedReplicas(awaiting),
                }));
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

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
            if placement.state.kind() != RuntimeStateKind::WasmProcessor {
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

    /// The boundary a checkpoint of `state` captured now has to reach: this node's stable storage,
    /// and the stable storage of every replica the committed schedule assigns to the processor.
    ///
    /// A branch task can outlive the schedule it was built from: an owner replacement or a state
    /// transition publishes a new generation, or another owner, while its last callback is still
    /// running. Nothing restores the lifetime a checkpoint of such a branch describes, so it is
    /// refused before anything is persisted or replicated.
    pub(in crate::runtime) fn wasm_checkpoint_boundary(
        &self,
        state: &ReplicatedWasmProcessorState,
    ) -> error_stack::Result<WasmCheckpointBoundary, StateReplicationError> {
        let placement = &state.placement;
        let superseded = || {
            Report::new(StateReplicationError::Superseded {
                placement: placement.clone(),
            })
        };
        if !self.runtime_state_placement_is_current(placement) {
            return Err(superseded());
        }
        let dispatcher = self.inner.remote_dispatcher.load();
        // A runtime that has not joined a cluster executes every node it runs, with no replicas.
        let Some(dispatcher) = dispatcher.as_deref() else {
            return Ok(WasmCheckpointBoundary::LocalStorage);
        };
        let Some(execution) = self.inner.executions.get(&placement.domain) else {
            return Err(superseded());
        };
        let node = NodeRef::new(placement.kind, placement.identifier.clone());
        let Some(scheduled) = execution.schedule.nodes.get(&node) else {
            return Err(superseded());
        };
        if !scheduled.executes_on(dispatcher.local_node_id()) {
            return Err(superseded());
        }
        let replicas = scheduled
            .replica_nodes()
            .into_iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        Ok(WasmCheckpointBoundary::assigned(replicas))
    }

    /// Write a captured checkpoint to this node's stable storage, and offer it to the replicas its
    /// boundary names once it is there.
    pub(in crate::runtime) async fn persist_wasm_checkpoint(
        &self,
        state: &ReplicatedWasmProcessorState,
        captured: CapturedWasmCheckpoint,
        deadline: Instant,
    ) -> error_stack::Result<LocallyDurableWasmCheckpoint, StateReplicationError> {
        let placement = &state.placement;
        let revision = captured.revision();
        let failed = || StateReplicationError::Persist {
            placement: placement.clone(),
            lsm: revision,
        };
        if self.inner.fault_injection.wasm_checkpoint_storage_fails() {
            return Err(Report::new(failed()));
        }
        // A runtime without a state store, which only unit tests construct, has no stable storage
        // for a checkpoint to reach.
        if let Some(store) = self.inner.state_store.as_ref() {
            let written = tokio::time::timeout_at(
                deadline,
                store.persist_wasm_checkpoint(placement, captured.saved()),
            )
            .await;
            match written {
                Ok(written) => written.change_context_lazy(failed)?,
                Err(_elapsed) => {
                    return Err(Report::new(failed()).attach_printable(
                        "the checkpoint deadline passed before its write reached stable storage",
                    ));
                }
            }
        }
        let durable = state.record_locally_durable(captured);
        if let WasmCheckpointBoundary::Replicas(_) = durable.boundary() {
            self.notify_runtime_state_replicas(placement, revision);
        }
        Ok(durable)
    }

    /// Wait until every replica the checkpoint's boundary names holds it on its stable storage.
    ///
    /// The replicas follow the committed schedule while the checkpoint waits, so a replaced replica
    /// is replaced in the wait, but a checkpoint never completes with fewer replicas than it was
    /// captured for, and never after this node stopped executing the processor.
    pub(in crate::runtime) async fn confirm_wasm_checkpoint(
        &self,
        state: &ReplicatedWasmProcessorState,
        durable: LocallyDurableWasmCheckpoint,
        deadline: Instant,
    ) -> error_stack::Result<CompletedWasmCheckpoint, StateReplicationError> {
        let placement = &state.placement;
        let revision = durable.revision();
        let WasmCheckpointBoundary::Replicas(captured_replicas) = durable.boundary().clone() else {
            return Ok(durable.completed());
        };
        let mut replicas = captured_replicas;
        loop {
            tokio::task::consume_budget().await;
            let progressed = state.replica_progress_signal().notified();
            tokio::pin!(progressed);
            progressed.as_mut().enable();
            let assigned = self.wasm_checkpoint_boundary(state)?;
            replicas = replicas.followed(assigned).map_err(|shrunk| {
                Report::new(StateReplicationError::ReplicaPlanShrunk {
                    placement: placement.clone(),
                    lsm: revision,
                    required: shrunk.required,
                    assigned: shrunk.assigned,
                })
            })?;
            let awaiting = state.replicas_awaiting(&replicas, revision);
            if awaiting.is_empty() {
                return Ok(durable.completed());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(Report::new(StateReplicationError::ReplicaConfirmation {
                    placement: placement.clone(),
                    lsm: revision,
                    awaiting: AwaitedReplicas(awaiting),
                }));
            }
            let recheck = now
                .checked_add(WASM_CHECKPOINT_REPLAN_INTERVAL)
                .assured("a recheck interval of a fraction of a second stays within Instant")
                .min(deadline);
            tokio::select! {
                _ = &mut progressed => {}
                _ = sleep_until(recheck) => {}
            }
        }
    }

    pub(in crate::runtime) fn replicated_wasm_processor_state(
        &self,
        placement: RuntimeStatePlacement,
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
            initial,
        ));
        self.inner
            .replicated_wasm_processor_states
            .insert(placement, state.clone());
        Ok(state)
    }
}
