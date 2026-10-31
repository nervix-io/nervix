//! Layer: data plane.
//! Owns: persistence and replica polling tasks for runtime state.
//! May depend on: runtime state carriers, the state store, interconnect, and vocabulary models.
//! Must not know: control-plane transactions, Plexus parsing, or edge protocols.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "replica task and placement installation establish ownership-bound execution \
                  lifetimes"
    )
)]

use std::future::Future;

use nervix_primitives::sync::DropGuard;

use super::*;

/// The tasks that write this node's checkpoints of runtime state: the snapshot task of each
/// deduplicator and window branch, and the [`CheckpointWriter`] of the state each scheduled node's
/// placement keeps on this node.
///
/// Each task outlives the tasks that change its state. A branch's snapshot task still writes what
/// the branch published last once its branch task is gone, whether that task stopped or was ended
/// at its stop grace, and a placement's checkpoint writer makes its last write only once every task
/// that changes the placement's state has ended. No such task can therefore own one to its end, so
/// the runtime does, and none outlives the runtime with the node's storage open. Closing the owner
/// tells every task to write its state now and end; waiting for it returns once each of them has
/// finished.
///
/// Each task is also counted for its domain. A domain's stop ends every task of the domain's
/// execution, which leaves each branch's snapshot task making its last write, then tells each
/// placement's checkpoint writer to make its own, and waits for the domain's tasks before it
/// returns, so a stopped domain's checkpoints are on disk when its stop completes.
#[derive(Default)]
pub(in crate::runtime) struct StateSnapshotTasks {
    tracker: TaskTracker,
    stopping: CancellationToken,
    /// The snapshot tasks of every domain that has one. Each tracker is closed as it is created,
    /// so waiting for it returns as soon as it counts no task.
    domains: DashMap<DomainName, TaskTracker, RandomState>,
}

/// A tracker that counts its domain's snapshot tasks, closed so that waiting for it ends when no
/// task is left rather than when an owner closes it.
fn closed_domain_tracker() -> TaskTracker {
    let tracker = TaskTracker::new();
    tracker.close();
    tracker
}

impl StateSnapshotTasks {
    /// The signal a snapshot task waits for beside its own branch's stop.
    pub(in crate::runtime) fn stopping(&self) -> CancellationToken {
        self.stopping.clone()
    }

    /// Start `task`, the snapshot task of a branch or a placement of `domain`.
    pub(in crate::runtime) fn spawn(
        &self,
        domain: &DomainName,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> JoinHandle<()> {
        // The task counts for its domain while the domain's entry is held, so a stop that finds the
        // domain's tracker empty and removes it never removes a tracker this task joins.
        let domain_tasks = self
            .domains
            .entry(domain.clone())
            .or_insert_with(closed_domain_tracker);
        let task = domain_tasks.track_future(task);
        drop(domain_tasks);
        self.tracker.spawn(task)
    }

    /// Start `writer`, the checkpoint writer of a placement of `domain`, with the signal that tells
    /// it to make its last write. The returned handle sends that signal when the domain's stop
    /// tells it to, or when the placement drops it.
    pub(in crate::runtime) fn spawn_checkpoint_writer<F>(
        &self,
        domain: &DomainName,
        writer: impl FnOnce(LastWriteSignal) -> F,
    ) -> CheckpointWriter
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let placement_stopped = CancellationToken::new();
        let signal = LastWriteSignal {
            placement_stopped: placement_stopped.clone(),
            runtime_stopping: self.stopping(),
        };
        let task = self.spawn(domain, writer(signal));
        CheckpointWriter {
            last_write: placement_stopped.drop_guard(),
            task,
        }
    }

    pub(in crate::runtime) fn close(&self) {
        self.stopping.cancel();
        self.tracker.close();
    }

    pub(in crate::runtime) async fn wait(&self) {
        self.tracker.wait().await;
    }

    /// Wait until no snapshot task of `domain` is left. The caller has ended every task of the
    /// domain's execution and told each of its checkpoint writers to make its last write, so each
    /// task this waits for is making that write.
    pub(in crate::runtime) async fn wait_for_domain(&self, domain: &DomainName) {
        let Some(domain_tasks) = self.domains.get(domain) else {
            return;
        };
        let tasks = domain_tasks.value().clone();
        // The shard is released before the wait, which lasts as long as the slowest write.
        drop(domain_tasks);
        tasks.wait().await;
        self.domains.remove_if(domain, |_, tasks| tasks.is_empty());
    }
}

/// What tells a placement's checkpoint writer to make its last write: the placement's stop, sent
/// once every task that changes the placement's state has ended or when the placement is dropped,
/// or the stop of the runtime.
pub(in crate::runtime) struct LastWriteSignal {
    placement_stopped: CancellationToken,
    runtime_stopping: CancellationToken,
}

impl LastWriteSignal {
    /// Returns once either stop has arrived. Both tell the writer the same thing, so the order in
    /// which they are polled decides nothing, and a fixed order asks no scheduler, a model
    /// checker's included, for a random choice it would have to replay.
    pub(in crate::runtime) async fn arrived(&self) {
        nervix_primitives::select! {
            biased;
            _ = self.placement_stopped.cancelled() => {}
            _ = self.runtime_stopping.cancelled() => {}
        }
    }
}

/// The handle a placement keeps on the writer of its state's checkpoints, a task the runtime's
/// [`StateSnapshotTasks`] owns and counts for the placement's domain.
///
/// The writer writes the state on every snapshot interval, and once more when the handle tells it
/// to or is dropped: that is the write a domain's stop waits for. A reassignment that replaces the
/// placement's runtime ends the writer without it.
pub(in crate::runtime) struct CheckpointWriter {
    last_write: DropGuard,
    task: JoinHandle<()>,
}

impl CheckpointWriter {
    /// Tells the writer to make its last write. The runtime owns the writer, so whoever needs that
    /// write waits for it through [`StateSnapshotTasks::wait_for_domain`].
    pub(in crate::runtime) fn write_last(self) {
        let Self { last_write, task } = self;
        drop(last_write);
        drop(task);
    }

    /// Ends the writer without its last write.
    pub(in crate::runtime) async fn abort(self) {
        let Self { last_write, task } = self;
        task.abort();
        task.join_after_shutdown("placement checkpoint writer")
            .await;
        // The writer has ended, so the signal this guard would send reaches nobody.
        drop(last_write.disarm());
    }
}

impl Runtime {
    /// Start the checkpoint writer of a Kafka ingestor's domain offsets, which writes them on every
    /// snapshot interval and once more when its [`CheckpointWriter`] tells it to: once its domain's
    /// stop has ended every task that changes them.
    pub(in crate::runtime) fn spawn_kafka_offset_snapshot_task(
        &self,
        state: KafkaOffsetStatePersistence,
    ) -> Option<CheckpointWriter> {
        let store = self.inner.state_store.as_ref()?.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let runtime = self.clone();
        let domain = state.read().placement().domain.clone();
        Some(self.inner.state_snapshot_tasks.spawn_checkpoint_writer(
            &domain,
            move |last_write| async move {
                let flush_latest_snapshot =
                    |state: &KafkaOffsetStatePersistence,
                     store: &RuntimeStateStore|
                     -> error_stack::Result<Option<u64>, RuntimePersistenceError> {
                        if !state.is_dirty() {
                            return Ok(None);
                        }
                        let snapshot = state.read().latest_snapshot()?;
                        if snapshot.lsm <= state.last_persisted_lsm() {
                            return Ok(None);
                        }
                        store.persist_latest_snapshot(
                            state.read().placement(),
                            snapshot.lsm,
                            &snapshot.payload,
                        )?;
                        state.record_persisted(snapshot.lsm);
                        Ok(Some(snapshot.lsm))
                    };
                loop {
                    nervix_primitives::task::consume_budget().await;
                    nervix_primitives::select! {
                        _ = last_write.arrived() => break,
                        _ = sleep(snapshot_interval) => {
                            match flush_latest_snapshot(&state, &store) {
                                Ok(Some(lsm)) => {
                                    let offsets = state.read();
                                    runtime.announce_checkpoint(
                                        offsets.placement(), offsets.replication(), lsm,
                                    );
                                }
                                Ok(None) => {}
                                Err(error) => warn!(error = format!("{error:#}"), "failed to persist kafka offset snapshot"),
                            }
                        }
                    }
                }
                let placement = state.read().placement().clone();
                runtime.wait_for_slow_last_write(&placement).await;
                match flush_latest_snapshot(&state, &store) {
                    Ok(Some(lsm)) => {
                        let offsets = state.read();
                        runtime.announce_checkpoint(offsets.placement(), offsets.replication(), lsm);
                    }
                    Ok(None) => {}
                    Err(error) => warn!(
                        error = format!("{error:#}"),
                        "failed to flush kafka offset snapshot during shutdown"
                    ),
                }
            },
        ))
    }

    /// Spawn the task that keeps one branch state current for its replicas and on disk.
    ///
    /// At least once per replication poll interval it asks the branch task to publish state that
    /// changed, so replicas follow the branch without anything reading its live state. On every
    /// snapshot interval, and once more when the branch stops, it also persists what was published
    /// last, including when the branch task is gone and can no longer publish.
    ///
    /// The task runs under the runtime's [`StateSnapshotTasks`], not under its branch task, so a
    /// runtime that stops waits for that last write. The handle this returns only lets a branch
    /// that stops in time wait for it too; dropping it leaves the task with the runtime.
    pub(in crate::runtime) fn spawn_published_branch_state_snapshot_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: PublishedBranchState,
        snapshot_requests: mpsc::Sender<ProcessorSnapshotRequest>,
    ) -> Option<JoinHandle<()>> {
        let store = self.inner.state_store.as_ref()?.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let publication_interval =
            snapshot_interval.min(self.inner.state_replication_poll_interval);
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let runtime_stopping = self.inner.state_snapshot_tasks.stopping();
        let domain = state.placement().domain.clone();
        Some(self.inner.state_snapshot_tasks.spawn(&domain, async move {
            let mut next_persist = Instant::now() + snapshot_interval;
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            if let Err(error) = state.request_publication(&snapshot_requests).await {
                                warn!(
                                    kind = state.placement().kind.as_str(),
                                    error = %format_args!("{error:#}"),
                                    "failed to publish branch state during shutdown"
                                );
                            }
                            runtime.persist_stopped_branch_state(&state, &store).await;
                            break;
                        }
                    }
                    // A stopping runtime has ended its branch tasks, so nothing publishes for this
                    // state any more and what was published last is what a restart restores.
                    _ = runtime_stopping.cancelled() => {
                        runtime.persist_stopped_branch_state(&state, &store).await;
                        break;
                    }
                    _ = sleep(publication_interval) => {
                        if let Err(error) = state.request_publication(&snapshot_requests).await {
                            warn!(
                                kind = state.placement().kind.as_str(),
                                error = %format_args!("{error:#}"),
                                "failed to publish branch state"
                            );
                        }
                        if Instant::now() < next_persist {
                            continue;
                        }
                        next_persist = Instant::now() + snapshot_interval;
                        match state
                            .persist_published(
                                &store,
                                &runtime.inner.executor,
                                &runtime.inner.snapshot_staging,
                            )
                            .await
                        {
                            Ok(Some(lsm)) => runtime.announce_checkpoint(
                                state.placement(), state.replication(), lsm,
                            ),
                            Ok(None) => {}
                            Err(error) => warn!(
                                kind = state.placement().kind.as_str(),
                                error = %format_args!("{error:#}"),
                                "failed to persist branch state snapshot"
                            ),
                        }
                    }
                }
            }
        }))
    }

    /// Waits out the delay a scenario gave the last write of `placement`'s state as its domain
    /// stops, as storage slow to take a large checkpoint would.
    async fn wait_for_slow_last_write(&self, placement: &RuntimeStatePlacement) {
        if let Some(delay) = self.inner.fault_injection.stopped_state_write_delay(
            &placement.domain,
            placement.kind,
            &placement.identifier,
        ) {
            sleep(delay).await;
        }
    }

    /// Write what a branch that has stopped published last, and offer the checkpoint to its
    /// replicas.
    async fn persist_stopped_branch_state(
        &self,
        state: &PublishedBranchState,
        store: &RuntimeStateStore,
    ) {
        self.wait_for_slow_last_write(state.placement()).await;
        let persisted = state
            .persist_published(store, &self.inner.executor, &self.inner.snapshot_staging)
            .await;
        match persisted {
            Ok(Some(lsm)) => self.announce_checkpoint(state.placement(), state.replication(), lsm),
            Ok(None) => {}
            Err(error) => warn!(
                kind = state.placement().kind.as_str(),
                error = %format_args!("{error:#}"),
                "failed to flush branch state snapshot during shutdown"
            ),
        }
    }

    /// Start the checkpoint writer of a materialized relay's records on this node, its owner or one
    /// of its replicas, which writes them on every snapshot interval and once more when its
    /// [`CheckpointWriter`] tells it to: once its domain's stop has ended the relay's state task,
    /// which applies what the relay still delivers as it stops, and every other task that changes
    /// the records.
    pub(in crate::runtime) fn spawn_materialized_stream_snapshot_task(
        &self,
        state: MaterializedRelayStatePersistence,
    ) -> Option<CheckpointWriter> {
        let store = self.inner.state_store.as_ref()?.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let runtime = self.clone();
        let executor = self.inner.executor.clone();
        let domain = state.read().placement().domain.clone();
        Some(self.inner.state_snapshot_tasks.spawn_checkpoint_writer(
            &domain,
            move |last_write| async move {
                let flush_latest_snapshot =
                    async |state: &MaterializedRelayStatePersistence,
                           store: &Arc<RuntimeStateStore>| {
                        if !state.is_dirty() {
                            return Ok(None);
                        }
                        let last_persisted = state.last_persisted_lsm();
                        let sealed = state
                            .read()
                            .seal_after(
                                &executor,
                                &runtime.inner.snapshot_staging,
                                Some(last_persisted),
                            )
                            .await;
                        let sealed = match sealed {
                            Ok(Some(sealed)) => sealed,
                            Ok(None) => return Ok(None),
                            Err(error) => {
                                return Err(
                                    error.change_context(RuntimePersistenceError::EncodeState)
                                );
                            }
                        };
                        let revision = sealed.descriptor.revision;
                        let placement = state.read().placement().clone();
                        store
                            .publish_checkpoint_artifact(&placement, revision, sealed.artifact)
                            .await?;
                        state.record_persisted(revision);
                        Ok::<Option<u64>, Report<RuntimePersistenceError>>(Some(revision))
                    };
                loop {
                    nervix_primitives::task::consume_budget().await;
                    nervix_primitives::select! {
                        _ = last_write.arrived() => break,
                        _ = sleep(snapshot_interval) => {
                            match flush_latest_snapshot(&state, &store).await {
                                Ok(Some(lsm)) => {
                                    let materialized = state.read();
                                    runtime.announce_checkpoint(
                                        materialized.placement(), materialized.replication(), lsm,
                                    );
                                }
                                Ok(None) => {}
                                Err(error) => warn!(error = format!("{error:#}"), "failed to persist materialized relay snapshot"),
                            }
                        }
                    }
                }
                let placement = state.read().placement().clone();
                runtime.wait_for_slow_last_write(&placement).await;
                match flush_latest_snapshot(&state, &store).await {
                    Ok(Some(lsm)) => {
                        let materialized = state.read();
                        runtime.announce_checkpoint(
                            materialized.placement(),
                            materialized.replication(),
                            lsm,
                        );
                    }
                    Ok(None) => {}
                    Err(error) => warn!(
                        error = format!("{error:#}"),
                        "failed to flush materialized relay snapshot during shutdown"
                    ),
                }
            },
        ))
    }

    /// Start the checkpoint writer of a node's branch-aggregated state on the cluster node that
    /// executes it, which writes it on every snapshot interval and once more when its
    /// [`CheckpointWriter`] tells it to: once its domain's stop has ended every task that changes
    /// it.
    pub(in crate::runtime) fn spawn_branch_aggregated_snapshot_task(
        &self,
        state: Arc<ReplicatedBranchAggregatedState>,
    ) -> Option<CheckpointWriter> {
        let store = self.inner.state_store.as_ref()?.clone();
        let metrics = self.inner.metrics.clone();
        let snapshot_interval = self.inner.state_snapshot_interval;
        let runtime = self.clone();
        let domain = state.placement.domain.clone();
        Some(self.inner.state_snapshot_tasks.spawn_checkpoint_writer(
            &domain,
            move |last_write| async move {
                let flush_latest_snapshot =
                    |state: &ReplicatedBranchAggregatedState,
                     metrics: &RuntimeMetrics,
                     store: &RuntimeStateStore| {
                        let Some(snapshot) = state.snapshot_to_persist(metrics)? else {
                            return Ok(None);
                        };
                        store.persist_latest_snapshot(
                            &state.placement,
                            snapshot.lsm,
                            &snapshot.payload,
                        )?;
                        state.persisted(snapshot.lsm);
                        Ok::<Option<u64>, Report<RuntimePersistenceError>>(Some(snapshot.lsm))
                    };
                loop {
                    nervix_primitives::task::consume_budget().await;
                    nervix_primitives::select! {
                        _ = last_write.arrived() => break,
                        _ = sleep(snapshot_interval) => {
                            match flush_latest_snapshot(&state, &metrics, &store) {
                                Ok(Some(lsm)) => runtime.announce_checkpoint(
                                    &state.placement, state.replication(), lsm,
                                ),
                                Ok(None) => {}
                                Err(error) => warn!(error = %format_args!("{error:#}"), "failed to persist branch-aggregated state snapshot"),
                            }
                        }
                    }
                }
                runtime.wait_for_slow_last_write(&state.placement).await;
                match flush_latest_snapshot(&state, &metrics, &store) {
                    Ok(Some(lsm)) => {
                        runtime.announce_checkpoint(&state.placement, state.replication(), lsm);
                    }
                    Ok(None) => {}
                    Err(error) => warn!(
                        error = %format_args!("{error:#}"),
                        "failed to flush branch-aggregated state snapshot during shutdown"
                    ),
                }
            },
        ))
    }

    pub(in crate::runtime) fn spawn_kafka_offset_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: KafkaOffsetSnapshotInstaller,
    ) -> Option<JoinHandle<()>> {
        let offsets = state.read();
        let primary_node = offsets.primary_node()?;
        let placement = offsets.placement().clone();
        let poll_interval = self.inner.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(nervix_primitives::task::spawn(async move {
            let offsets = state.read();
            let mut initial_sync_pending = true;
            loop {
                nervix_primitives::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        offsets.replication(),
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                let after_lsm = offsets.current_lsm();
                let synchronized = nervix_primitives::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    synchronized = runtime.sync_kafka_offsets_from(
                        &primary_node, &state, after_lsm,
                    ) => synchronized,
                };
                match synchronized {
                    Ok(lsm) => {
                        let dispatcher = runtime.inner.remote_dispatcher.load_full();
                        if let Some(dispatcher) = dispatcher
                            && let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: placement.to_remote(),
                                                lsm,
                                            },
                                        ),
                                    ),
                                )
                                .await
                        {
                            warn!(node_id = %dispatcher.local_node_id(), error = %format_args!("{error:#}"), "failed to acknowledge replicated kafka offset snapshot");
                        }
                    }
                    Err(error) => {
                        warn!(error = %format_args!("{error:#}"), "failed to sync replicated kafka offsets");
                    }
                }
            }
        }))
    }

    /// Spawn the replica task that keeps one branch-keyed entity current on this node: the
    /// entity's branch lifecycle and, for a deduplicator, window or WASM processor, the state of
    /// each of its branches.
    ///
    /// The task retains the entity's lifecycle handle and owns everything it learns of the
    /// entity's branch checkpoints, so each round asks the owner only what changed since the
    /// previous one. It runs a round when it starts, when the owner announces a checkpoint, and
    /// once every replication poll interval, which catches up a checkpoint whose announcement was
    /// lost.
    pub(in crate::runtime) fn spawn_branch_state_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        domain: &DomainName,
        node: &ExecutionNode,
    ) -> error_stack::Result<Option<JoinHandle<()>>, StateIdentityError> {
        let branch_states = match node.kind() {
            ModelKind::Deduplicator => Some(RuntimeStateKind::Deduplicator),
            ModelKind::WasmProcessor => Some(RuntimeStateKind::WasmProcessor),
            ModelKind::WindowProcessor => Some(RuntimeStateKind::WindowProcessor),
            ModelKind::Ingestor
            | ModelKind::Reingestor
            | ModelKind::Inferencer
            | ModelKind::Junction
            | ModelKind::Correlator
            | ModelKind::Reorderer => None,
            _ => return Ok(None),
        };
        let Some(primary_node) = node.execution_node().cloned() else {
            return Ok(None);
        };
        let branch_lru = self.state_placement(
            domain,
            RuntimeStateKind::BranchLru,
            node.kind(),
            node.identifier.clone(),
            None,
        )?;
        let poll_interval = self.inner.state_replication_poll_interval;
        let lifecycle = self.replicated_branch_lifecycle(&branch_lru);
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Ok(Some(nervix_primitives::task::spawn(async move {
            if let Err(error) = runtime.restore_replica_branch_lifecycle(&branch_lru, &lifecycle) {
                warn!(error = %format_args!("{error:#}"), "failed to read the stored replicated branch lifecycle");
            }
            let owner = RemoteStateOwner::new(runtime.clone(), primary_node);
            let mut checkpoints = ReplicaBranchCheckpoints::default();
            let mut initial_sync_pending = true;
            loop {
                nervix_primitives::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        lifecycle.replication(),
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                runtime
                    .catch_up_replica_branches(
                        &owner,
                        &branch_lru,
                        &lifecycle,
                        &mut checkpoints,
                        branch_states,
                    )
                    .await;
            }
        })))
    }

    pub(in crate::runtime) fn spawn_materialized_stream_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: MaterializedRelaySnapshotInstaller,
    ) -> Option<JoinHandle<()>> {
        let primary_node = state.read().primary_node()?;
        let poll_interval = self.inner.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(nervix_primitives::task::spawn(async move {
            let materialized = state.read();
            let mut initial_sync_pending = true;
            loop {
                nervix_primitives::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        materialized.replication(),
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                let after_lsm = materialized.current_lsm();
                match runtime
                    .install_materialized_snapshot_from(
                        &primary_node,
                        materialized,
                        &state,
                        Some(after_lsm),
                    )
                    .await
                {
                    Ok(Some(revision)) => {
                        runtime.inner.materialized_state_changed.notify_waiters();
                        let dispatcher = runtime.inner.remote_dispatcher.load_full();
                        if let Some(dispatcher) = dispatcher
                            && let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: materialized.placement().to_remote(),
                                                lsm: revision,
                                            },
                                        ),
                                    ),
                                )
                                .await
                        {
                            warn!(node_id = %dispatcher.local_node_id(), error = %format_args!("{error:#}"), "failed to acknowledge replicated materialized relay snapshot");
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(error = %format_args!("{error:#}"), "failed to sync replicated materialized relay state");
                    }
                }
            }
        }))
    }

    pub(in crate::runtime) fn spawn_branch_aggregated_replica_poll_task(
        &self,
        shutdown_tx: &watch::Sender<bool>,
        state: Arc<ReplicatedBranchAggregatedState>,
    ) -> Option<JoinHandle<()>> {
        let primary_node = state.primary_node()?;
        let poll_interval = self.inner.state_replication_poll_interval;
        let runtime = self.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(nervix_primitives::task::spawn(async move {
            let mut initial_sync_pending = true;
            loop {
                nervix_primitives::task::consume_budget().await;
                if !runtime
                    .wait_for_state_replica_sync_trigger(
                        &mut shutdown_rx,
                        state.replication(),
                        poll_interval,
                        initial_sync_pending,
                    )
                    .await
                {
                    break;
                }
                initial_sync_pending = false;
                let after_lsm = state.current_lsm.current();
                match runtime
                    .request_state_sync_with_timeout(
                        &primary_node,
                        &state.placement,
                        Some(after_lsm),
                        nervix_interconnect::StateSyncRequest::TIMEOUT,
                    )
                    .await
                {
                    Ok(Some(snapshot)) => {
                        if let Err(error) = state.apply_snapshot(
                            &runtime.inner.metrics,
                            snapshot.lsm,
                            &snapshot.payload,
                        ) {
                            warn!(
                                error = format!("{error:#}"),
                                "failed to apply replicated branch-aggregated state snapshot"
                            );
                            continue;
                        }
                        let dispatcher = runtime.inner.remote_dispatcher.load_full();
                        if let Some(dispatcher) = dispatcher
                            && let Err(error) = dispatcher
                                .dispatch(
                                    &primary_node,
                                    Envelope::Control(
                                        nervix_interconnect::ControlEnvelope::StateReplicationAck(
                                            nervix_interconnect::StateReplicationAck {
                                                placement: state.placement.to_remote(),
                                                lsm: snapshot.lsm,
                                            },
                                        ),
                                    ),
                                )
                                .await
                        {
                            warn!(node_id = %dispatcher.local_node_id(), error = %format_args!("{error:#}"), "failed to acknowledge replicated branch-aggregated state snapshot");
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(error = %format_args!("{error:#}"), "failed to sync replicated branch-aggregated state");
                    }
                }
            }
        }))
    }

    /// Installs what every runtime state of `schedule`'s domain is keyed by: each node's schema
    /// fingerprint and, for a WASM processor, the guest-state generation of every branch.
    ///
    /// A node the schedule still carries keeps its identity throughout: each one is written before
    /// any stale node is dropped, so a concurrent [`Self::state_placement`] always resolves to the
    /// state the node already owns. Emptying the domain first would expose a window where a
    /// scheduled node has no identity, and a placement resolved in that window cannot address the
    /// state the node owns. Relocation rebuilds these while the relocating node's own state task is
    /// still reading them, which is exactly when that window is observed.
    pub(in crate::runtime) fn state_assignment_for_unit(
        &self,
        unit: &DomainNodeUnitRef,
    ) -> SharedStateAssignment {
        if let Some(slot) = self.inner.state_identities.get(unit) {
            return slot.clone();
        }
        // Schedule application is the single registration owner; tasks bind only after publication.
        let slot = Arc::new(ArcSwapOption::empty());
        self.inner
            .state_identities
            .insert(unit.clone(), slot.clone());
        self.inner
            .state_replication_routing
            .register_unit_assignment(unit, &slot);
        slot
    }

    #[cfg(any(test, feature = "benchmarks"))]
    pub(in crate::runtime) fn publish_state_assignment(
        &self,
        node: DomainNodeRef,
        assignment: ScheduledStateAssignment,
    ) {
        self.publish_unit_state_assignment(DomainNodeUnitRef::single(node), assignment);
    }

    pub(in crate::runtime) fn publish_unit_state_assignment(
        &self,
        unit: DomainNodeUnitRef,
        assignment: ScheduledStateAssignment,
    ) {
        self.state_assignment_for_unit(&unit)
            .store(Some(StdArc::new(assignment)));
    }

    pub(in crate::runtime) fn install_state_identities(&self, revision: &ExecutionRevision) {
        self.install_state_identities_from_nodes(&revision.domain, revision.nodes.values());
    }

    fn install_state_identities_from_nodes<'a>(
        &self,
        domain: &DomainName,
        nodes: impl Iterator<Item = &'a ExecutionNode>,
    ) {
        let start_version = match self.inner.domains.get(domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        let mut scheduled = HashSet::default();
        for node in nodes {
            let node_ref =
                DomainNodeRef::node_in(domain.clone(), node.kind(), node.identifier.clone());
            for (unit, execution) in &node.units {
                let executors = execution
                    .assigned_nodes
                    .iter()
                    .filter(|owner| {
                        node.executes_on_every_cluster_node
                            || execution
                                .primary_node
                                .as_ref()
                                .is_none_or(|primary| primary == *owner)
                    })
                    .cloned()
                    .collect();
                let replicas = execution.replica_nodes().into_iter().cloned().collect();
                let key = DomainNodeUnitRef::new(node_ref.clone(), *unit);
                self.publish_unit_state_assignment(
                    key.clone(),
                    ScheduledStateAssignment {
                        identity: ScheduledStateIdentity {
                            schema_fingerprint: Self::state_schema_fingerprint(node, start_version),
                            wasm_state_generations: execution.wasm_state_generations.clone(),
                        },
                        checkpoint_owners: Some(CheckpointOwners {
                            primary: execution.execution_node().cloned(),
                            executors,
                            replicas,
                        }),
                    },
                );
                scheduled.insert(key);
            }
        }
        self.retain_state_identities(domain, &scheduled);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_schedule_state_identities(&self, schedule: &DomainSchedule) {
        let nodes = schedule
            .nodes
            .values()
            .map(|node| ExecutionNode::from_scheduled(node, schedule))
            .collect::<Vec<_>>();
        self.install_state_identities_from_nodes(&schedule.domain, nodes.iter());
    }

    /// Before cluster assignment, preserve an existing guest-state generation while installing
    /// the schema identities of the unplaced revision.
    pub(in crate::runtime) fn install_state_identities_from_unplaced_revision(
        &self,
        domain: &DomainName,
        revision: &ExecutionRevision,
    ) {
        self.install_state_identities_from_unplaced_nodes(domain, revision.nodes.values());
    }

    fn install_state_identities_from_unplaced_nodes<'a>(
        &self,
        domain: &DomainName,
        nodes: impl Iterator<Item = &'a ExecutionNode>,
    ) {
        let start_version = match self.inner.domains.get(domain) {
            Some(state) => state.start_version,
            None => 0,
        };
        let mut active = HashSet::default();
        for node in nodes {
            let node_ref =
                DomainNodeRef::node_in(domain.clone(), node.kind(), node.identifier.clone());
            let schema_fingerprint = Self::state_schema_fingerprint(node, start_version);
            for unit in node.units.keys() {
                let key = DomainNodeUnitRef::new(node_ref.clone(), *unit);
                let slot = self.state_assignment_for_unit(&key);
                let mut assignment = match slot.load_full() {
                    Some(current) => (*current).clone(),
                    None => ScheduledStateAssignment {
                        identity: ScheduledStateIdentity {
                            schema_fingerprint,
                            wasm_state_generations: None,
                        },
                        checkpoint_owners: None,
                    },
                };
                assignment.identity.schema_fingerprint = schema_fingerprint;
                slot.store(Some(StdArc::new(assignment)));
                active.insert(key);
            }
        }
        self.retain_state_identities(domain, &active);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_state_identities_from_graph(
        &self,
        domain: &DomainName,
        nodes: &[ScheduledNode],
    ) {
        let schedule = DomainSchedule::new(domain.clone(), nodes.to_vec(), Vec::new());
        let nodes = schedule
            .nodes
            .values()
            .map(|node| ExecutionNode::from_scheduled(node, &schedule))
            .collect::<Vec<_>>();
        self.install_state_identities_from_unplaced_nodes(domain, nodes.iter());
    }

    /// The fingerprint every schema-bound runtime state of `node` is keyed by in a domain started
    /// `start_version` times.
    ///
    /// Materialized relay state also belongs to the domain start that began it, so a START resets
    /// it: its fingerprint covers the start version beside the node's schemas.
    fn state_schema_fingerprint(node: &ExecutionNode, start_version: u64) -> SchemaFingerprint {
        if !node.materialized_relay {
            return node.schema_fingerprint;
        }
        node.schema_fingerprint.materialized_at(start_version)
    }

    pub(in crate::runtime) fn clear_state_identities(&self, domain: &DomainName) {
        self.retain_state_identities(domain, &HashSet::default());
    }

    /// Drops the identities of `domain` that `keep` no longer names, leaving the rest in place.
    fn retain_state_identities(&self, domain: &DomainName, keep: &HashSet<DomainNodeUnitRef>) {
        let stale = self
            .inner
            .state_identities
            .iter()
            .filter_map(|entry| {
                (&entry.key().entity.domain == domain && !keep.contains(entry.key()))
                    .then(|| entry.key().clone())
            })
            .collect::<Vec<_>>();
        for key in stale {
            if let Some(slot) = self.inner.state_identities.get(&key) {
                slot.store(None);
            }
            self.inner.state_identities.remove(&key);
            self.inner.state_replication_routing.withdraw_unit(&key);
        }
    }

    /// The placement of runtime state of `state` for `branch_key`, in the identity the committed
    /// schedule publishes for its node.
    ///
    /// Branch-aggregated metrics and Kafka offsets depend on no schema and are placed without one.
    /// Every other kind is placed under the schema fingerprint the schedule publishes for the node,
    /// and WASM guest state also in the generation it names for the branch, so neither can be
    /// placed for a node that no schedule has published them for.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the typed model-name conversion")
    )]
    pub(in crate::runtime) fn state_placement(
        &self,
        domain: &DomainName,
        state: RuntimeStateKind,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        branch_key: Option<BranchKey>,
    ) -> error_stack::Result<RuntimeStatePlacement, StateIdentityError> {
        self.state_placement_for_unit(
            domain,
            state,
            kind,
            identifier,
            nervix_models::Unit::Single,
            branch_key,
        )
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "a record, frame or checkpoint poll resolves one unit's published state \
                      identity"
        )
    )]
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the typed model-name conversion")
    )]
    pub(in crate::runtime) fn state_placement_for_unit(
        &self,
        domain: &DomainName,
        state: RuntimeStateKind,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        unit: nervix_models::Unit,
        branch_key: Option<BranchKey>,
    ) -> error_stack::Result<RuntimeStatePlacement, StateIdentityError> {
        let identifier = identifier.into();
        let state = match state {
            RuntimeStateKind::BranchAggregated => RuntimeState::BranchAggregated,
            RuntimeStateKind::KafkaOffset => RuntimeState::KafkaOffset,
            RuntimeStateKind::Correlator
            | RuntimeStateKind::Deduplicator
            | RuntimeStateKind::MaterializedRelay
            | RuntimeStateKind::WasmProcessor
            | RuntimeStateKind::WindowProcessor
            | RuntimeStateKind::BranchLru => {
                let node = DomainNodeUnitRef::new(
                    DomainNodeRef::node_in(domain.clone(), kind, identifier.clone()),
                    unit,
                );
                let assignment = self
                    .inner
                    .state_replication_routing
                    .assignment_for_unit(&node)
                    .and_then(|slot| slot.load_full());
                let Some(assignment) = assignment else {
                    return Err(Report::new(
                        StateIdentityError::SchemaFingerprintUnpublished {
                            domain: domain.clone(),
                            kind,
                            identifier,
                        },
                    ));
                };
                let branch = branch_key.as_ref().map(BranchKey::fingerprint);
                let Some(state) = assignment.identity.state_of(state, branch.as_ref()) else {
                    return Err(Report::new(StateIdentityError::GenerationUnpublished {
                        domain: domain.clone(),
                        kind,
                        identifier,
                    }));
                };
                state
            }
        };
        Ok(RuntimeStatePlacement {
            domain: domain.clone(),
            state,
            kind,
            identifier,
            unit,
            branch_key,
        })
    }

    /// Whether `placement` still names the state the committed schedule keys this node's state by.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "this owner is reached by recurring record, frame, acknowledgement or \
                      state-poll work"
        )
    )]
    pub(in crate::runtime) fn runtime_state_placement_is_current(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> bool {
        let Some(slot) = self.inner.state_replication_routing.assignment(placement) else {
            return false;
        };
        let Some(assignment) = slot.load_full() else {
            return false;
        };
        assignment.names(placement)
    }

    pub(in crate::runtime) fn purge_stale_runtime_state(
        &self,
        domain: &DomainName,
    ) -> Result<(), error_stack::Report<RuntimePersistenceError>> {
        self.inner.state_replication_routing.purge_stale(domain);
        let stale_deduplicators = self
            .inner
            .replicated_deduplicator_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_deduplicators {
            self.inner.replicated_deduplicator_states.remove(&placement);
        }
        let stale_materialized = self
            .inner
            .replicated_materialized_stream_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_materialized {
            self.inner
                .replicated_materialized_stream_states
                .remove(&placement);
        }
        let stale_windows = self
            .inner
            .replicated_window_processor_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_windows {
            self.inner
                .replicated_window_processor_states
                .remove(&placement);
        }
        let stale_wasm = self
            .inner
            .replicated_wasm_processor_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_wasm {
            self.inner
                .replicated_wasm_processor_states
                .remove(&placement);
        }
        let stale_offsets = self
            .inner
            .replicated_kafka_offset_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_offsets {
            self.inner.replicated_kafka_offset_states.remove(&placement);
        }
        let stale_aggregates = self
            .inner
            .replicated_branch_aggregated_states
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_aggregates {
            self.inner
                .replicated_branch_aggregated_states
                .remove(&placement);
        }
        let stale_presences = self
            .inner
            .relay_branch_presences
            .iter()
            .filter_map(|entry| {
                let placement = entry.key();
                (&placement.domain == domain && !self.runtime_state_placement_is_current(placement))
                    .then(|| placement.clone())
            })
            .collect::<Vec<_>>();
        for placement in stale_presences {
            self.inner.relay_branch_presences.remove(&placement);
        }

        if let Some(store) = self.inner.state_store.as_ref() {
            let current = self
                .inner
                .state_identities
                .iter()
                .filter_map(|entry| {
                    let key = entry.key();
                    if &key.entity.domain != domain {
                        return None;
                    }
                    let assignment = entry.load_full()?;
                    Some((
                        nervix_models::NodeUnitRef::new(key.entity.node.clone(), key.unit),
                        assignment.identity.clone(),
                    ))
                })
                .collect::<HashMap<_, _>>();
            store.purge_stale_state_identities(domain, &current)?;
        }
        Ok(())
    }

    pub(in crate::runtime) fn clear_runtime_state_for_domain(&self, domain: &DomainName) {
        self.inner.state_replication_routing.retire_domain(domain);
        let placements = self
            .inner
            .replicated_deduplicator_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner.replicated_deduplicator_states.remove(&placement);
        }
        let placements = self
            .inner
            .replicated_kafka_offset_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner.replicated_kafka_offset_states.remove(&placement);
        }
        let placements = self
            .inner
            .replicated_materialized_stream_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner
                .replicated_materialized_stream_states
                .remove(&placement);
        }
        let placements = self
            .inner
            .replicated_window_processor_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner
                .replicated_window_processor_states
                .remove(&placement);
        }
        let placements = self
            .inner
            .replicated_wasm_processor_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner
                .replicated_wasm_processor_states
                .remove(&placement);
        }
        let placements = self
            .inner
            .replicated_branch_aggregated_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner
                .replicated_branch_aggregated_states
                .remove(&placement);
        }
        self.inner
            .replicated_branch_lifecycles
            .retain(|placement, _| &placement.domain != domain);
        self.inner
            .passive_runtime_state_snapshots
            .retain(|placement, _| &placement.domain != domain);
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "tasks_shuttle_tests.rs"]
mod shuttle_tests;
