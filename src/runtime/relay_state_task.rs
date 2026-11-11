//! The task that keeps one materialized relay unit's rows on the node that owns the unit.
//!
//! Layer: data plane.
//!
//! - **Owns.** The state task's lifetime and stop, applying the unit's batches to its replicated
//!   materialized state as the latest record by timestamp, expiring and evicting the unit's
//!   branches by the relay's TTL and `MAX INSTANCES`, and, while the relay's branch changes its
//!   partition count, the order in which the unit's claims, parked batches and holder steps apply.
//! - **Depends on.** The relay's fan-in, its replicated materialized state, the relay interaction,
//!   the domain clock, the unit's ownership freeze and its partition moves.
//! - **Must not know.** Plexus text, transaction planning, consensus, or how a materialized read
//!   reaches the rows.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "relay membership and channel installation establish retained concrete branch \
                  and delivery handles"
    )
)]

use std::ops::ControlFlow;

use nervix_interconnect::{RemoteOperationFailure, RemoteOperationSubject};

use super::{
    domain_clock::{DomainClockAccessError, DomainClockAccessResult},
    materialized_partition_moves::{
        MaterializedBranches, MaterializedPartitionMoves, MovingRelayUnit, ParkedRowBatch,
        RelayStateCommand, RowAdmission, SettledRows,
    },
    partition_claims::UnitPartitionTransition,
    relay_boundary::{RelayRetention, RelayTaskKind, RelayTaskStopError},
    relay_interaction::RELAY_INTERACTION_COMMAND_CAPACITY,
    *,
};

/// One materialized relay unit's runtime task: the relay and unit it serves, the replicated state
/// it maintains, the branch retention limits it enforces, and the fan-in it consumes.
pub(super) struct RelayStateTaskSpec {
    pub(super) relay: RelayName,
    /// The unit whose keys this task materializes. Its fan-in registers with this partition's
    /// fixed audience, so sibling partitions occupy none of its queue capacity.
    pub(super) unit: Unit,
    pub(super) partitioning: RelayPartitioning,
    pub(super) state: MaterializedRelayStateOriginator,
    pub(super) retention: RelayRetention,
    pub(super) receiver: RelayRuntimeFanIn,
    /// The partition transition the unit executes in, while its branch is in one.
    pub(super) transition: Option<UnitPartitionTransition>,
    /// The `CAPACITY` of the relay's owner buffers, which bounds the batches the unit parks while
    /// their keys' rows are claimed.
    pub(super) parked_capacity: usize,
}

pub(super) struct RelayStateTask {
    pub(super) unit: Unit,
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: JoinHandle<()>,
    /// The claim steps other units of the relay ask of this one.
    pub(super) commands: mpsc::Sender<RelayStateCommand>,
}

impl RelayStateTask {
    pub(super) async fn stop(
        mut self,
        grace: Duration,
    ) -> error_stack::Result<(), RelayTaskStopError> {
        self.shutdown.send_replace(true);
        match nervix_primitives::time::timeout(grace, &mut self.task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(Report::new(error).change_context(RelayTaskStopError::Join {
                task: RelayTaskKind::State,
            })),
            Err(_) => {
                self.task.abort();
                self.task.join_after_shutdown("relay state").await;
                Err(Report::new(RelayTaskStopError::DrainTimeout {
                    task: RelayTaskKind::State,
                    grace,
                }))
            }
        }
    }
}

/// What one materialized relay unit's state task owns between events.
struct RelayStateRun {
    runtime: Runtime,
    domain: DomainName,
    relay: RelayName,
    unit: Unit,
    partitioning: RelayPartitioning,
    state: MaterializedRelayStateOriginator,
    branches: MaterializedBranches,
    branch_capacity: Option<NonZeroUsize>,
    clock: DomainClock,
    moves: Option<MaterializedPartitionMoves>,
}

impl RelayStateRun {
    /// The domain time now, as the unit's domain clock tells it.
    fn now(&self) -> DomainClockAccessResult<Timestamp> {
        let snapshot = self.clock.snapshot()?;
        Ok(snapshot.now())
    }

    /// What the unit reports, and refuses the batch it holds with, once it lost its clock.
    fn lost_clock(&self, error: &Report<DomainClockAccessError>) -> String {
        format!(
            "materialized relay '{}' in domain '{}' lost its clock: {error:#}",
            self.relay.as_str(),
            self.domain.as_str(),
        )
    }

    /// Deletes the rows of every branch idle for longer than `branch_ttl`.
    async fn expire(&mut self, branch_ttl: Duration) -> ControlFlow<()> {
        let now = match self.now() {
            Ok(now) => now,
            Err(error) => {
                self.runtime.events().report_error(self.lost_clock(&error));
                return ControlFlow::Break(());
            }
        };
        for (key, _) in self.branches.expire(now, branch_ttl) {
            nervix_primitives::task::consume_budget().await;
            if let Err(error) = self
                .runtime
                .delete_materialized_stream_key(&mut self.state, &key)
            {
                warn!(
                    domain = self.domain.as_str(),
                    relay = self.relay.as_str(),
                    error = %format_args!("{error:#}"),
                    "materialized relay assignment changed during branch expiration"
                );
                return ControlFlow::Break(());
            }
        }
        if let Some(moves) = self.moves.as_mut() {
            moves.retain_held(&self.branches);
        }
        ControlFlow::Continue(())
    }

    /// Takes one batch of the relay: applies it, unless its key's row may lie in another unit and
    /// it waits for that row's claim.
    async fn receive(
        &mut self,
        batch: RelayRecordBatch,
        work: Option<NodeQuiesceWorkGuard>,
    ) -> ControlFlow<()> {
        // The fan-in carries the batches of every unit this node owns and of the units whose
        // owners route to its consumers; another unit's keys are that unit's state, never this
        // one's.
        if self.partitioning.unit_of(&batch.key).ok() != Some(self.unit) {
            return ControlFlow::Continue(());
        }
        let parked = ParkedRowBatch { batch, work };
        let admitted = match self.moves.as_mut() {
            Some(moves) => {
                let held = self.branches.contains_key(&parked.batch.key);
                moves.admit(&self.runtime, held, parked)
            }
            None => RowAdmission::Apply(parked),
        };
        match admitted {
            RowAdmission::Apply(parked) => self.apply(parked.batch).await,
            RowAdmission::Parked => ControlFlow::Continue(()),
        }
    }

    /// Records the activity of `key` at `now`, evicting the least recently active branches past the
    /// relay's `MAX INSTANCES`.
    async fn track(&mut self, key: &Option<BranchKey>, now: Timestamp) -> ControlFlow<()> {
        self.branches
            .get_or_try_create_with(key.clone(), now, |_, _| {
                Ok::<(), std::convert::Infallible>(())
            })
            .assured("the tracking closure's error type is Infallible");
        let Some(branch_capacity) = self.branch_capacity else {
            return ControlFlow::Continue(());
        };
        for (evicted_key, _) in self.branches.evict_lru_to_capacity(branch_capacity) {
            nervix_primitives::task::consume_budget().await;
            if let Err(error) = self
                .runtime
                .delete_materialized_stream_key(&mut self.state, &evicted_key)
            {
                warn!(
                    domain = self.domain.as_str(),
                    relay = self.relay.as_str(),
                    error = %format_args!("{error:#}"),
                    "materialized relay assignment changed during branch eviction"
                );
                return ControlFlow::Break(());
            }
        }
        if let Some(moves) = self.moves.as_mut() {
            moves.retain_held(&self.branches);
        }
        ControlFlow::Continue(())
    }

    /// Applies one batch to the unit's rows, keeping each branch's latest record by timestamp.
    async fn apply(&mut self, batch: RelayRecordBatch) -> ControlFlow<()> {
        let branch_key = batch.key.clone();
        let now = match self.now() {
            Ok(now) => now,
            Err(error) => {
                let reason = self.lost_clock(&error);
                self.runtime.events().report_error(reason.clone());
                for ack in batch.acks.iter() {
                    ack.no_ack(reason.clone());
                }
                return ControlFlow::Break(());
            }
        };
        self.track(&branch_key, now).await?;
        let messages = match batch.try_into_messages() {
            Ok(messages) => messages,
            Err(error_and_batch) => {
                let error = error_and_batch.error;
                warn!(
                    domain = self.domain.as_str(),
                    relay = self.relay.as_str(),
                    scope = %BranchScope::from(&branch_key),
                    error = %format_args!("{error:#}"),
                    "failed to decode scheduled materialized relay batch"
                );
                return ControlFlow::Continue(());
            }
        };
        let records = messages.into_iter().map(|message| message.record);
        if let Err(error) = self
            .runtime
            .apply_materialized_stream_records(&mut self.state, &branch_key, records)
            .await
        {
            warn!(
                domain = self.domain.as_str(),
                relay = self.relay.as_str(),
                error = %format_args!("{error:#}"),
                "materialized relay assignment changed while applying a batch"
            );
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    /// Handles one claim step another unit asks of this one, or one claim of this unit's progress.
    async fn command(&mut self, command: RelayStateCommand) -> ControlFlow<()> {
        match command {
            RelayStateCommand::PartitionKey(request) => {
                let now = match self.now() {
                    Ok(now) => now,
                    Err(error) => {
                        self.runtime.events().report_error(self.lost_clock(&error));
                        return ControlFlow::Break(());
                    }
                };
                match self.moves.as_mut() {
                    Some(moves) => {
                        moves
                            .answer_holder(
                                &self.runtime,
                                &mut self.state,
                                &mut self.branches,
                                request,
                                now,
                            )
                            .await;
                    }
                    None => {
                        let subject = RemoteOperationSubject::entity(
                            &self.domain,
                            ModelKind::Relay,
                            ModelName::from(&self.relay),
                        );
                        request
                            .response
                            .send(Err(RemoteOperationFailure::not_ready(subject)))
                            .means_peer_left("partition claimant");
                    }
                }
                ControlFlow::Continue(())
            }
            RelayStateCommand::PartitionClaim(progress) => {
                let Some(moves) = self.moves.as_mut() else {
                    return ControlFlow::Continue(());
                };
                let settled = moves.progress(&self.runtime, progress);
                self.settle(settled).await
            }
            RelayStateCommand::TransitionRetired => self.retire().await,
        }
    }

    /// Ends the unit's part in its branch's partition transition, which retired. The leader
    /// retires a transition once no unit holds or claims a row outside its partition, so a claim
    /// still in flight finds no row: the batches it parked apply here, in arrival order. A
    /// claimant still in the transition elsewhere is told this unit is not ready until it retires
    /// too.
    async fn retire(&mut self) -> ControlFlow<()> {
        let Some(mut retired) = self.moves.take() else {
            return ControlFlow::Continue(());
        };
        for parked in retired.drain_parked() {
            nervix_primitives::task::consume_budget().await;
            let ParkedRowBatch { batch, work } = parked;
            self.apply(batch).await?;
            drop(work);
        }
        ControlFlow::Continue(())
    }

    /// Installs a claimed row before anything else of its key, then applies the batches its claim
    /// released, in arrival order.
    async fn settle(&mut self, settled: SettledRows) -> ControlFlow<()> {
        let SettledRows { claimed, batches } = settled;
        if let Some(claimed) = claimed {
            let branch = Some(claimed.key.clone());
            let now = match self.now() {
                Ok(now) => now,
                Err(error) => {
                    self.runtime.events().report_error(self.lost_clock(&error));
                    return ControlFlow::Break(());
                }
            };
            self.track(&branch, now).await?;
            if let Err(error) = self
                .runtime
                .apply_materialized_stream_records(&mut self.state, &branch, [claimed.row.clone()])
                .await
            {
                warn!(
                    domain = self.domain.as_str(),
                    relay = self.relay.as_str(),
                    error = %format_args!("{error:#}"),
                    "materialized relay assignment changed while installing a claimed row"
                );
                return ControlFlow::Break(());
            }
            let revision = self.state.read().current_lsm();
            if let Some(moves) = self.moves.as_mut() {
                moves.installed(&self.runtime, self.state.read(), claimed, revision);
            }
        }
        for parked in batches {
            nervix_primitives::task::consume_budget().await;
            self.apply(parked.batch).await?;
        }
        ControlFlow::Continue(())
    }

    /// Refuses every batch still waiting for its key's claim when the unit ends: its key's row stays
    /// in its holder, and its source sends it again.
    fn end(&mut self) {
        let Some(moves) = self.moves.as_mut() else {
            return;
        };
        for parked in moves.drain_parked() {
            for ack in parked.batch.acks.iter() {
                ack.no_ack("the materialized relay unit stopped before its key's claim settled");
            }
            drop(parked.work);
        }
    }
}

impl Runtime {
    pub(in crate::runtime) fn spawn_relay_state_task(
        &self,
        domain: &DomainName,
        spec: RelayStateTaskSpec,
    ) -> RelayStateTask {
        let RelayStateTaskSpec {
            relay,
            unit,
            partitioning,
            state,
            retention:
                RelayRetention {
                    branch_ttl,
                    branch_capacity,
                },
            receiver,
            transition,
            parked_capacity,
        } = spec;
        let runtime = self.clone();
        let domain = domain.clone();
        let expiration_scan_interval = self.inner.branch_instance_expiration_scan_interval;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (commands, command_rx) = mpsc::channel(RELAY_INTERACTION_COMMAND_CAPACITY);
        // While its branch's keys move between units, the unit's claim tasks report through its own
        // command channel. It stops on shutdown, never on its commands closing.
        let claim_reports = if transition.is_some() {
            Some(commands.clone())
        } else {
            None
        };
        let quiesce_counters =
            self.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Relay, &relay));
        let force_flush = self.force_flush_participant(&domain, quiesce_counters.clone());
        let task = nervix_primitives::task::spawn(async move {
            let interaction_input = RelayInteractionInput::immediate(relay.clone(), receiver);
            let mut interaction = RelayInteraction::with_commands(
                vec![interaction_input],
                shutdown_rx,
                Some(force_flush),
                Some(quiesce_counters),
                command_rx,
            )
            .verified(
                "the registry validated this input, and a non-empty input list builds an \
                 interaction",
            );
            let relay_model_name = ModelName::from(&relay);
            let ownership_entity =
                DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay_model_name.clone());
            let freeze_watch = OwnershipHandoffFreezeWatch::for_unit(
                &runtime,
                DomainNodeUnitRef::new(ownership_entity, unit),
            );
            let clock = match runtime.bind_domain_clock(&domain) {
                Ok(clock) => clock,
                Err(error) => {
                    runtime.events().report_error(format!(
                        "materialized relay '{}' in domain '{}' could not bind its clock: \
                         {error:#}",
                        relay.as_str(),
                        domain.as_str()
                    ));
                    return;
                }
            };
            let activated_at = match clock.snapshot() {
                Ok(snapshot) => snapshot.now(),
                Err(error) => {
                    runtime.events().report_error(format!(
                        "materialized relay '{}' in domain '{}' could not read its activation \
                         time: {error:#}",
                        relay.as_str(),
                        domain.as_str(),
                    ));
                    return;
                }
            };
            let branches = state.read().restored_branch_instances(activated_at);
            let moves = match (transition, claim_reports) {
                (Some(transition), Some(reports)) => {
                    let held = branches.snapshot_entries();
                    Some(MaterializedPartitionMoves::begin(
                        &runtime,
                        MovingRelayUnit {
                            domain: domain.clone(),
                            entity: NodeRef::new(ModelKind::Relay, relay_model_name),
                            schema: state.read().schema().clone(),
                            transition,
                        },
                        held.iter().map(|entry| &entry.key),
                        reports,
                        parked_capacity,
                    ))
                }
                _ => None,
            };
            let mut run = RelayStateRun {
                runtime: runtime.clone(),
                domain: domain.clone(),
                relay: relay.clone(),
                unit,
                partitioning,
                state,
                branches,
                branch_capacity,
                clock,
                moves,
            };
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            'state_task: loop {
                nervix_primitives::task::consume_budget().await;
                let frozen = freeze_watch.observe().is_frozen();
                if !frozen
                    && let Some(branch_ttl) = branch_ttl
                    && Instant::now() >= next_expiration_scan
                {
                    if run.expire(branch_ttl).await.is_break() {
                        break 'state_task;
                    }
                    next_expiration_scan = Instant::now() + expiration_scan_interval;
                    continue;
                }
                if let Some(moves) = run.moves.as_mut()
                    && moves
                        .next_wake()
                        .is_some_and(|wake_at| Instant::now() >= wake_at)
                {
                    moves.wake(&run.runtime, run.state.read());
                    continue;
                }
                let mut sleep = if frozen {
                    Some((run.runtime.entity_gate_deadline() / 1200).max(Duration::from_nanos(1)))
                } else {
                    branch_ttl.map(|_| {
                        next_expiration_scan
                            .checked_duration_since(Instant::now())
                            .unwrap_or(Duration::ZERO)
                    })
                };
                if let Some(wake_at) = run.moves.as_ref().and_then(|moves| moves.next_wake()) {
                    let until = wake_at
                        .checked_duration_since(Instant::now())
                        .unwrap_or(Duration::ZERO);
                    sleep = Some(match sleep {
                        Some(sleep) => sleep.min(until),
                        None => until,
                    });
                }
                let mut wake = RuntimeWake::never();
                if let Some(sleep) = sleep {
                    match RuntimeWake::after(sleep) {
                        Ok(scheduled) => wake = scheduled,
                        Err(error) => {
                            warn!(
                                domain = domain.as_str(),
                                relay = relay.as_str(),
                                error = %format_args!("{error:#}"),
                                "materialized relay state task could not schedule its next scan"
                            );
                            break 'state_task;
                        }
                    }
                }
                // A unit whose parked batches fill their bound takes no more until a claim
                // releases some; its relay holds the rest back.
                let parked_full = match run.moves.as_ref() {
                    Some(moves) => moves.is_full(),
                    None => false,
                };
                let work = match interaction.next_with_input(wake, !parked_full).await {
                    Ok(work) => work,
                    Err(failure) => {
                        let reason = format!("{:#}", failure.error);
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            error = %reason,
                            "materialized relay interaction failed"
                        );
                        failure.acks.no_ack(format!(
                            "state task for relay '{}' failed: {reason}",
                            relay.as_str()
                        ));
                        continue;
                    }
                };
                let (event, work) = work.into_parts();
                let flow = match event {
                    RelayInteractionEvent::Batch {
                        relay: input_relay,
                        batch,
                    } => {
                        debug_assert_eq!(input_relay, relay);
                        run.receive(batch, work).await
                    }
                    RelayInteractionEvent::Wake => ControlFlow::Continue(()),
                    RelayInteractionEvent::ForceFlush(completion) => {
                        completion.complete();
                        ControlFlow::Continue(())
                    }
                    RelayInteractionEvent::Command(command) => run.command(command).await,
                    RelayInteractionEvent::Stopped(reason) => {
                        debug!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            ?reason,
                            "materialized relay interaction stopped"
                        );
                        ControlFlow::Break(())
                    }
                };
                if flow.is_break() {
                    break 'state_task;
                }
            }
            run.end();
        });
        RelayStateTask {
            unit,
            shutdown,
            task,
            commands,
        }
    }
}

#[cfg(test)]
#[path = "relay_state_task_tests.rs"]
mod tests;
