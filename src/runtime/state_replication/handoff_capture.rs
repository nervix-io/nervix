//! Capture of the runtime state an ownership handoff transfers.
//!
//! Layer: data plane.
//!
//! - **Owns.** The final checkpoint of a frozen entity's live state and the persisted checkpoints
//!   its ownership handoff carries.
//! - **Depends on.** The entity's scheduled node task, replicated runtime-state carriers and the
//!   state store.
//! - **Must not know.** Plexus parsing, schedule planning or external connector configuration.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "an ownership handoff checkpoints and reads the frozen entity's state generation"
    )
)]

use super::*;

impl Runtime {
    pub(crate) async fn capture_selected_ownership_handoff_state(
        &self,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
        entity: &NodeRef,
        unit: nervix_models::Unit,
        base_schedule_fingerprint: [u8; 32],
        selection: nervix_interconnect::OwnershipHandoffSelection,
    ) -> OwnershipHandoffResult<Vec<nervix_interconnect::OwnershipHandoffCheckpoint>> {
        if !self.entity_gate_operation_owns_unit(
            coordination,
            domain,
            &NodeUnitRef::new(entity.clone(), unit),
            EntityGatePurpose::OwnershipHandoff,
        ) {
            return Err(OwnershipHandoffError::participant(format!(
                "coordination identity '{coordination}' does not hold the ownership gate for {} \
                 '{}'",
                entity.kind.as_str(),
                entity.identifier.as_str()
            )));
        }
        self.verify_local_handoff_schedule(domain, base_schedule_fingerprint)?;
        match selection {
            nervix_interconnect::OwnershipHandoffSelection::Unit => {
                self.capture_frozen_ownership_handoff_state(domain, entity, unit)
                    .await
            }
            nervix_interconnect::OwnershipHandoffSelection::IngestorUnits {
                selection,
                checkpoints,
            } => {
                self.project_captured_ingestor_unit_state(
                    domain,
                    entity,
                    unit,
                    selection,
                    checkpoints,
                )
                .await
            }
        }
    }

    async fn capture_frozen_ownership_handoff_state(
        &self,
        domain: &DomainName,
        entity: &NodeRef,
        unit: nervix_models::Unit,
    ) -> OwnershipHandoffResult<Vec<nervix_interconnect::OwnershipHandoffCheckpoint>> {
        let scheduled = self.ownership_handoff_scheduled_node(domain, entity)?;
        let source_is_passive = self
            .inner
            .executions
            .get(domain)
            .is_some_and(|execution| execution.passive_only);
        let matches_entity = |placement: &RuntimeStatePlacement| {
            placement.domain == *domain
                && placement.kind == entity.kind
                && placement.identifier == entity.identifier
                && placement.unit == unit
        };
        let final_branch_lru = if entity.kind.is_processor() {
            let commands = {
                let execution = self.inner.executions.get(domain).ok_or_else(|| {
                    OwnershipHandoffError::checkpoint(format!(
                        "domain '{}' has no active execution while checkpointing {} '{}'",
                        domain.as_str(),
                        entity.kind.as_str(),
                        entity.identifier.as_str()
                    ))
                })?;
                let commands = execution
                    .node_tasks
                    .get(&NodeUnitRef::new(entity.clone(), unit))
                    .map(|task| task.commands.clone());
                if commands.is_none() && !execution.passive_only {
                    return Err(OwnershipHandoffError::checkpoint(format!(
                        "{} '{}' has no local task while checkpointing ownership",
                        entity.kind.as_str(),
                        entity.identifier.as_str()
                    )));
                }
                commands
            };
            match commands {
                Some(commands) => Some(
                    ScheduledNodeTask::checkpoint_via(
                        &commands,
                        ProcessorCheckpointPurpose::OwnershipHandoff,
                    )
                    .await?,
                ),
                None => None,
            }
        } else {
            self.checkpoint_entrypoint_branch_lifecycle(domain, entity, unit)
                .await?
        };
        let mut checkpoints = Vec::new();
        let mut described = Vec::new();
        // Branch states leave their maps before they are encoded, so an encode never holds a map
        // shard that a branch appearing or leaving elsewhere has to write.
        let mut deduplicators = Vec::new();
        for state in self.inner.replicated_deduplicator_states.iter() {
            if matches_entity(state.key()) {
                deduplicators.push((state.key().clone(), state.value().clone()));
            }
        }
        for (placement, state) in deduplicators {
            let snapshot = state
                .latest_snapshot()
                .change_context(OwnershipHandoffError::Persistence)?;
            checkpoints.push((placement, snapshot));
        }
        let kafka_offsets = self
            .inner
            .replicated_kafka_offset_states
            .iter()
            .filter(|state| matches_entity(state.key()))
            .map(|state| {
                (
                    state.key().clone(),
                    ReplicatedKafkaOffsetState::read(state.value()),
                )
            })
            .collect::<Vec<_>>();
        for (placement, state) in kafka_offsets {
            nervix_primitives::task::consume_budget().await;
            let _capture = state.start_coordination().capture_guard().await;
            checkpoints.push((
                placement,
                state
                    .latest_snapshot()
                    .change_context(OwnershipHandoffError::Persistence)?,
            ));
        }
        let materialized = self
            .inner
            .replicated_materialized_stream_states
            .iter()
            .filter(|state| matches_entity(state.key()))
            .map(|state| {
                (
                    state.key().clone(),
                    ReplicatedMaterializedRelayState::read(state.value()),
                )
            })
            .collect::<Vec<_>>();
        for (placement, state) in materialized {
            nervix_primitives::task::consume_budget().await;
            let sealed = state
                .seal_after(&self.inner.executor, &self.inner.snapshot_staging, None)
                .await
                .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?
                .ok_or_else(|| {
                    OwnershipHandoffError::checkpoint(
                        "the materialized relay state produced no checkpoint generation",
                    )
                })?;
            checkpoints.push((
                placement,
                sealed
                    .into_persisted_entry(&self.inner.executor)
                    .await
                    .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?,
            ));
        }
        let mut windows = Vec::new();
        for state in self.inner.replicated_window_processor_states.iter() {
            if matches_entity(state.key()) {
                windows.push((state.key().clone(), state.value().clone()));
            }
        }
        for (placement, state) in windows {
            nervix_primitives::task::consume_budget().await;
            let snapshot = state
                .latest_snapshot(&self.inner.executor)
                .await
                .change_context(OwnershipHandoffError::Persistence)?;
            checkpoints.push((placement, snapshot));
        }
        let mut wasm_processors = Vec::new();
        for state in self.inner.replicated_wasm_processor_states.iter() {
            if matches_entity(state.key()) {
                wasm_processors.push((state.key().clone(), state.value().clone()));
            }
        }
        for (placement, state) in wasm_processors {
            // Describe and release each committed guest save before reading the next branch.
            // A handoff with many large branches keeps its inventory, not all save allocations.
            let descriptor = self
                .describe_state_checkpoint(&placement, state.latest_snapshot())
                .await
                .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?;
            described.push((placement, descriptor));
        }
        for state in self.inner.replicated_branch_aggregated_states.iter() {
            if matches_entity(state.key()) {
                checkpoints.push((
                    state.key().clone(),
                    state
                        .latest_snapshot(&self.inner.metrics)
                        .change_context(OwnershipHandoffError::Persistence)?,
                ));
            }
        }
        if Self::node_has_branch_lifecycle(entity.kind) {
            let branch_lru = self
                .state_placement_for_unit(
                    domain,
                    RuntimeStateKind::BranchLru,
                    entity.kind,
                    entity.identifier.clone(),
                    unit,
                    None,
                )
                .change_context_lazy(|| OwnershipHandoffError::StatePlacement {
                    kind: entity.kind,
                    identifier: entity.identifier.clone(),
                })?;
            match final_branch_lru {
                Some(snapshot) => checkpoints.push((branch_lru.clone(), snapshot)),
                None => {
                    if let Some(held) = self.held_branch_lifecycle(&branch_lru) {
                        checkpoints.push((branch_lru.clone(), held.snapshot().clone()));
                    } else if let Some(store) = self.inner.state_store.as_ref()
                        && let Some(snapshot) = store
                            .latest_snapshot(&branch_lru)
                            .change_context(OwnershipHandoffError::Persistence)?
                    {
                        checkpoints.push((branch_lru.clone(), snapshot));
                    }
                }
            }
            if source_is_passive
                && !checkpoints
                    .iter()
                    .any(|(placement, _)| placement == &branch_lru)
            {
                checkpoints.push((
                    branch_lru,
                    PersistedRuntimeStateEntry {
                        lsm: 0,
                        payload: encode_branch_lru_snapshot(&[]).map_err(|error| {
                            OwnershipHandoffError::checkpoint(error.to_string())
                        })?,
                    },
                ));
            }
        }
        let expected =
            self.expected_ownership_handoff_placements(domain, &scheduled, unit, &checkpoints)?;
        checkpoints.retain(|(placement, _)| expected.contains(placement));
        described.retain(|(placement, _)| expected.contains(placement));
        let captured = checkpoints
            .iter()
            .map(|(placement, _)| placement.clone())
            .chain(described.iter().map(|(placement, _)| placement.clone()))
            .collect::<HashSet<_>>();
        for placement in expected.difference(&captured) {
            nervix_primitives::task::consume_budget().await;
            let latest = self.uncaptured_handoff_checkpoint(placement).await?;
            let snapshot = if let Some(snapshot) = latest {
                snapshot
            } else if source_is_passive {
                self.empty_ownership_handoff_snapshot(placement)?
            } else {
                return Err(OwnershipHandoffError::checkpoint(format!(
                    "final {:?} checkpoint is missing for {} '{}'",
                    placement.state,
                    entity.kind.as_str(),
                    entity.identifier.as_str()
                )));
            };
            checkpoints.push((placement.clone(), snapshot));
        }
        checkpoints.sort_by(|(left, _), (right, _)| {
            u8::from(left.state.kind())
                .cmp(&u8::from(right.state.kind()))
                .then_with(|| {
                    left.branch_key
                        .as_ref()
                        .map(BranchKey::as_str)
                        .cmp(&right.branch_key.as_ref().map(BranchKey::as_str))
                })
        });
        for (placement, snapshot) in &checkpoints {
            if placement.state.kind() == RuntimeStateKind::WasmProcessor {
                // A WASM branch's handoff checkpoint is its committed checkpoint, which already
                // reached stable storage and every replica before it was committed. Writing it again
                // could replace a newer checkpoint this node holds with an older revision.
                continue;
            }
            if placement.state.kind() == RuntimeStateKind::BranchLru {
                self.persist_branch_lru_snapshot(placement.clone(), snapshot.clone())
                    .change_context(OwnershipHandoffError::Persistence)?;
            } else if let Some(store) = self.inner.state_store.as_ref() {
                store
                    .persist_latest_snapshot(placement, snapshot.lsm, &snapshot.payload)
                    .change_context(OwnershipHandoffError::Persistence)?;
                self.announce_stored_checkpoint(placement, snapshot.lsm);
            }
        }
        for (placement, snapshot) in checkpoints {
            let descriptor = self
                .describe_state_checkpoint(&placement, snapshot)
                .await
                .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?;
            described.push((placement, descriptor));
        }
        described.sort_by(|(left, _), (right, _)| {
            u8::from(left.state.kind())
                .cmp(&u8::from(right.state.kind()))
                .then_with(|| {
                    left.branch_key
                        .as_ref()
                        .map(BranchKey::as_str)
                        .cmp(&right.branch_key.as_ref().map(BranchKey::as_str))
                })
        });
        Ok(described
            .into_iter()
            .map(
                |(placement, snapshot)| nervix_interconnect::OwnershipHandoffCheckpoint {
                    placement: placement.to_remote(),
                    snapshot,
                },
            )
            .collect())
    }

    /// The checkpoint a handoff carries for an expected placement that no walk of the frozen
    /// entity's state captured.
    ///
    /// A stateful inferencer branch keeps its tensors only in its routed publication, which the
    /// freeze has just made current: it reflects every input the source admitted before the hold,
    /// while the store holds at most the last periodic snapshot. The capture encodes that
    /// publication, and falls back to the store only for a branch without a routed state, as in a
    /// stopped domain. Every other placement carries the newest checkpoint on this node's storage.
    pub(super) async fn uncaptured_handoff_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> OwnershipHandoffResult<Option<PersistedRuntimeStateEntry>> {
        if placement.state.kind() == RuntimeStateKind::Inferencer {
            return self
                .capture_state_checkpoint(placement, None)
                .await
                .change_context(OwnershipHandoffError::Persistence);
        }
        let Some(store) = self.inner.state_store.as_ref() else {
            return Ok(None);
        };
        store
            .latest_snapshot(placement)
            .change_context(OwnershipHandoffError::Persistence)
    }
}
