//! Ingestor branch lifecycle and concrete branch execution lanes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Route batching, concrete branch instantiation, branch-local FIFO dispatch, and
//!   branch TTL and capacity eviction.
//! - **Depends on.** Validated branch templates, relay boundaries, processors, execution
//!   admission, and runtime state persistence.
//! - **Must not know.** NSPL text, control-plane transactions, consensus, or connector protocols.

use super::*;

pub(super) const BRANCH_INSTANCE_EXPIRATION_SCAN_INTERVAL: Duration = Duration::from_secs(30);

pub(super) struct BranchRuntime {
    pub(super) key: Option<BranchKey>,
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) domain_clock: DomainClock,
    pub(super) source_kind: ModelKind,
    pub(super) source: RelayName,
    pub(super) root_relay: RelayName,
    pub(super) error_policies: ErrorPolicies,
    pub(super) relays: HashMap<RelayName, ConcreteRelayRuntime>,
    pub(super) materialized_states: HashMap<RelayName, MaterializedRelayStateOriginator>,
    pub(super) relay_state_epoch: Option<u64>,
    pub(super) processors: HashMap<ModelName, RelayProcessorNode>,
}

#[derive(Debug)]
pub(super) struct PendingMaterializedBatch {
    pub(super) input_relay: RelayName,
    pub(super) batch: Option<RelayRecordBatch>,
    pub(super) required_wait: Option<AckRequiredWaitGuard>,
}

pub(super) struct MaterializedBatchWaitContext<'a> {
    pub(super) shutdown_rx: &'a mut watch::Receiver<bool>,
    pub(super) wait_for_required_state: bool,
    pub(super) quiesce_work: Option<&'a mut NodeQuiesceWorkGuard>,
}

impl PendingMaterializedBatch {
    pub(super) fn new(input_relay: RelayName, batch: RelayRecordBatch) -> Self {
        let required_wait = AckRequiredWaitGuard::new(batch.acks.iter());
        Self {
            input_relay,
            batch: Some(batch),
            required_wait: Some(required_wait),
        }
    }

    pub(super) fn into_parts(mut self) -> (RelayName, RelayRecordBatch) {
        drop(self.required_wait.take());
        let batch = self
            .batch
            .take()
            .verified("the pending entry holds its batch from construction until this take");
        (self.input_relay.clone(), batch)
    }
}

impl Drop for PendingMaterializedBatch {
    fn drop(&mut self) {
        if let Some(batch) = &self.batch {
            let reason = format!(
                "node stopped while waiting for required materialized state at relay '{}'",
                self.input_relay
            );
            for ack in &batch.acks {
                ack.no_ack(reason.clone());
            }
        }
    }
}

pub(super) fn output_error_policies(
    policy: &MessageErrorPolicy,
    general: GeneralErrorPolicy,
) -> ErrorPolicies {
    ErrorPolicies {
        message: policy.clone(),
        general,
    }
}

pub(super) fn internal_processor_error_policies(general: GeneralErrorPolicy) -> ErrorPolicies {
    ErrorPolicies {
        message: MessageErrorPolicy::Log,
        general,
    }
}

pub(super) struct BranchExecutionRuntime {
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) sender: mpsc::Sender<BranchedEntrypointInput>,
    pub(super) checkpoints:
        mpsc::Sender<oneshot::Sender<OwnershipHandoffResult<PersistedRuntimeStateEntry>>>,
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

pub(super) struct IngestorRouteRuntime {
    pub(super) sender: mpsc::Sender<BranchedEntrypointInput>,
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: parking_lot::Mutex<Option<JoinHandle<()>>>,
    pub(super) branch_runtime: Arc<BranchExecutionRuntime>,
}

pub(super) struct PendingIngestorRouteBatch {
    pub(super) batches: Vec<RelayRecordBatch>,
    pub(super) estimated_bytes: u64,
    pub(super) flush_timer: BranchBufferTimer,
}

pub(super) struct IngestorRouteTask {
    pub(super) runtime_handle: Runtime,
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) template: IngestorRouteTemplate,
    pub(super) branch_sender: mpsc::Sender<BranchedEntrypointInput>,
    pub(super) pending: HashMap<Option<BranchKey>, PendingIngestorRouteBatch>,
}

pub(super) struct BranchExecutionDispatchContext<'a> {
    pub(super) runtime_handle: &'a Runtime,
    pub(super) domain: &'a DomainName,
    pub(super) ingestor: &'a IngestorName,
    pub(super) graph: &'a SharedActiveGraph,
    pub(super) template: &'a BranchInstanceTemplate,
    pub(super) now: Timestamp,
}

struct BranchDispatchCompletion {
    key: Option<BranchKey>,
    acks: Vec<AckSet>,
    result: Result<Option<Timestamp>, tokio::task::JoinError>,
}

type PendingBranchDispatch = BoxFuture<'static, BranchDispatchCompletion>;

struct QueuedBranchDispatch {
    received_at: Timestamp,
    batch: BranchedEntrypointInput,
}

#[derive(Default)]
struct BranchDispatchLanes {
    active: HashSet<Option<BranchKey>>,
    queued: HashMap<Option<BranchKey>, VecDeque<QueuedBranchDispatch>>,
    pending: FuturesUnordered<PendingBranchDispatch>,
}

impl BranchDispatchLanes {
    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn queue(&mut self, key: Option<BranchKey>, received_at: Timestamp, batch: RelayRecordBatch) {
        self.queued
            .entry(key)
            .or_default()
            .push_back(QueuedBranchDispatch { received_at, batch });
    }

    fn take_next(&mut self, key: &Option<BranchKey>) -> Option<QueuedBranchDispatch> {
        let queue = self.queued.get_mut(key)?;
        let next = queue.pop_front();
        let empty = queue.is_empty();
        if empty {
            self.queued.remove(key);
        }
        next
    }

    fn reject_queued(&mut self, key: &Option<BranchKey>, reason: &str) {
        let Some(queued) = self.queued.remove(key) else {
            return;
        };
        for dispatch in queued {
            for ack in dispatch.batch.acks.iter() {
                ack.no_ack(reason.to_string());
            }
        }
    }
}

impl BranchRuntime {
    pub(super) async fn evict(&mut self) {
        for processor in self.processors.values_mut() {
            processor.drop_collected_inputs("processor branch was evicted");
        }
    }

    pub(super) async fn reconcile_materialized_state_membership(&mut self, relay: &RelayName) {
        let current_epoch = self
            .runtime
            .relay_state_epoch(&self.domain)
            .load(Ordering::Acquire);
        if self.relay_state_epoch == Some(current_epoch) {
            return;
        }
        let desired_relays = match self.runtime.inner.executions.get(&self.domain) {
            Some(execution) => execution
                .materialized_stream_specs
                .keys()
                .filter(|relay| {
                    !execution
                        .materialized_stream_owner_nodes
                        .get(*relay)
                        .is_some_and(Option::is_some)
                })
                .cloned()
                .collect::<HashSet<_>>(),
            None => HashSet::default(),
        };
        self.materialized_states
            .retain(|identifier, _| desired_relays.contains(identifier));
        if desired_relays.contains(relay) && !self.materialized_states.contains_key(relay) {
            let schema = if let Some(execution) = self.runtime.inner.executions.get(&self.domain)
                && let Some(spec) = execution.materialized_stream_specs.get(relay)
            {
                spec.schema.clone()
            } else {
                warn!(
                    domain = self.domain.as_str(),
                    relay = relay.as_str(),
                    "failed to reconcile materialized relay without its schema"
                );
                return;
            };
            let placement = self.runtime.state_placement(
                &self.domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                self.key.clone(),
            );
            if let Err(error) = self
                .runtime
                .prepare_materialized_stream_restore(&placement, &schema)
                .await
            {
                warn!(
                    domain = self.domain.as_str(),
                    relay = relay.as_str(),
                    error = %error,
                    "failed to open the persisted branch-local materialized relay snapshot"
                );
                return;
            }
            match self.runtime.replicated_materialized_stream_state(
                placement,
                schema,
                None,
                Vec::new(),
                None,
            ) {
                Ok(mut assignment) => {
                    let Some(state) = assignment.originator.take() else {
                        warn!(
                            domain = self.domain.as_str(),
                            relay = relay.as_str(),
                            "branch-local materialized relay lacks authoritative state access"
                        );
                        return;
                    };
                    self.materialized_states.insert(relay.clone(), state);
                }
                Err(error) => {
                    warn!(
                        domain = self.domain.as_str(),
                        relay = relay.as_str(),
                        error = %error,
                        "failed to reconcile materialized relay membership"
                    );
                    return;
                }
            }
        }
        self.relay_state_epoch = Some(current_epoch);
    }

    pub(super) async fn materialize_stream_batch(
        &mut self,
        relay: &RelayName,
        batch: &RelayRecordBatch,
    ) {
        if self.runtime.relay_is_cluster_scheduled(&self.domain, relay) {
            return;
        }
        self.reconcile_materialized_state_membership(relay).await;
        let Some(state) = self.materialized_states.get(relay) else {
            return;
        };
        let messages = match batch.detached().try_into_messages() {
            Ok(messages) => messages,
            Err(error_and_batch) => {
                let (error, _) = *error_and_batch;
                warn!(
                    domain = self.domain.as_str(),
                    relay = relay.as_str(),
                    branch = branch_key_display(&self.key),
                    error = %error,
                    "failed to decode branch-local materialized state batch"
                );
                return;
            }
        };
        for message in messages {
            tokio::task::consume_budget().await;
            if let Err(error) = self.runtime.update_materialized_stream_last_by_timestamp(
                state,
                &batch.key,
                &message.record,
            ) {
                warn!(
                    domain = self.domain.as_str(),
                    relay = relay.as_str(),
                    branch = branch_key_display(&self.key),
                    error = %error,
                    "materialized relay assignment changed while applying a branch-local batch"
                );
                return;
            }
        }
    }

    pub(super) fn processor_has_pending_materialized(&self, processor_id: &ModelName) -> bool {
        self.processors
            .get(processor_id)
            .is_some_and(|processor| !processor.pending_materialized.is_empty())
    }

    pub(super) fn snapshot_processor_live_state(
        &mut self,
        processor_id: &ModelName,
    ) -> Result<(), String> {
        let Some(mut processor) = self.processors.remove(processor_id) else {
            return Ok(());
        };
        let result = processor.snapshot_live_state(self);
        self.processors.insert(processor_id.clone(), processor);
        result
    }

    pub(super) async fn checkpoint_processor_live_state(
        &mut self,
        processor_id: &ModelName,
        execution_now: Timestamp,
    ) -> OwnershipHandoffResult<()> {
        let Some(mut processor) = self.processors.remove(processor_id) else {
            return Ok(());
        };
        let result = processor.checkpoint_live_state(self, execution_now).await;
        self.processors.insert(processor_id.clone(), processor);
        result
    }

    pub(super) async fn retry_processor_pending_materialized(
        &mut self,
        graph: &SharedActiveGraph,
        processor_id: &ModelName,
    ) {
        let Some(mut processor) = self.processors.remove(processor_id) else {
            return;
        };
        let pending_count = processor.pending_materialized.len();
        for _ in 0..pending_count {
            let Some(pending) = processor.pending_materialized.pop_front() else {
                break;
            };
            let (incoming_relay, batch) = pending.into_parts();
            processor.execute(graph, self, &incoming_relay, batch).await;
        }
        self.processors.insert(processor_id.clone(), processor);
    }

    pub(super) async fn retry_materialized_waiters(
        &mut self,
        graph: &SharedActiveGraph,
        updated_relay: &RelayName,
    ) {
        let processor_ids = self
            .processors
            .iter()
            .filter(|(_, processor)| {
                processor
                    .materialized_state
                    .iter()
                    .any(|dependency| &dependency.relay == updated_relay)
                    && !processor.pending_materialized.is_empty()
            })
            .map(|(identifier, _)| identifier.clone())
            .collect::<Vec<_>>();
        for processor_id in processor_ids {
            let Some(mut processor) = self.processors.remove(&processor_id) else {
                continue;
            };
            let pending_count = processor.pending_materialized.len();
            for _ in 0..pending_count {
                let Some(pending) = processor.pending_materialized.pop_front() else {
                    break;
                };
                let (incoming_relay, batch) = pending.into_parts();
                processor.execute(graph, self, &incoming_relay, batch).await;
            }
            self.processors.insert(processor_id, processor);
        }
    }

    pub(super) async fn dispatch(&mut self, graph: &SharedActiveGraph, batch: RelayRecordBatch) {
        let root_relay = self.root_relay.clone();
        self.runtime
            .inner
            .metrics
            .observe_global_node_sent(NodeBatchObservation {
                domain: &self.domain,
                kind: self.source_kind,
                node: &ModelName::from(&self.source),
                relay: &root_relay,
                physical_node_id: self
                    .runtime
                    .inner
                    .remote_dispatch
                    .local_node_id
                    .read()
                    .as_ref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: batch.domain_timestamp(),
            });
        self.runtime.inner.metrics.observe_branch_node_sent(
            branch_key_display(&self.key),
            NodeBatchObservation {
                domain: &self.domain,
                kind: self.source_kind,
                node: &ModelName::from(&self.source),
                relay: &root_relay,
                physical_node_id: self
                    .runtime
                    .inner
                    .remote_dispatch
                    .local_node_id
                    .read()
                    .as_ref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: batch.domain_timestamp(),
            },
        );
        self.runtime.mark_branch_aggregated_metrics_updated(
            &self.domain,
            self.source_kind,
            &self.source,
        );
        if self
            .dispatch_stream(graph, &root_relay, &batch)
            .await
            .is_err()
        {
            let reason = "branched root relay dispatch failed".to_string();
            if self.source_kind == ModelKind::Ingestor {
                self.runtime.handle_general_error_for_acks(
                    &self.domain,
                    self.source_kind,
                    &self.source,
                    &self.error_policies,
                    batch.acks.iter(),
                    reason,
                );
            } else {
                self.runtime.handle_internal_processor_error_for_acks(
                    &self.domain,
                    self.source_kind,
                    &self.source,
                    &self.error_policies,
                    batch.acks.iter(),
                    reason,
                );
            }
            return;
        }
        for ack in batch.acks.iter() {
            ack.ack_success();
        }
    }

    pub(super) fn dispatch_stream<'a>(
        &'a mut self,
        graph: &'a SharedActiveGraph,
        relay: &'a RelayName,
        batch: &'a RelayRecordBatch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RelayDispatchResult> + Send + 'a>> {
        Box::pin(async move {
            let Some(runtime_stream) = self.relays.get_mut(relay) else {
                return Err(Box::new(batch.clone()));
            };
            runtime_stream.dispatch_boundary(batch).await?;
            self.materialize_stream_batch(relay, batch).await;
            self.retry_materialized_waiters(graph, relay).await;

            Ok(())
        })
    }

    pub(super) async fn execute_processor_input(
        &mut self,
        graph: &SharedActiveGraph,
        processor_id: &ModelName,
        incoming_relay: &RelayName,
        batch: RelayRecordBatch,
    ) {
        let Some(mut processor) = self.processors.remove(processor_id) else {
            for ack in batch.acks.iter() {
                ack.no_ack("processor is not instantiated for this branch");
            }
            return;
        };
        let delivery_observation = batch.delivery_observation(current_timestamp());
        let physical_node_id = self
            .runtime
            .inner
            .remote_dispatch
            .local_node_id
            .read()
            .clone();
        self.runtime
            .inner
            .metrics
            .observe_global_node_received(NodeBatchObservation {
                domain: &self.domain,
                kind: processor.kind,
                node: &processor.processor,
                relay: incoming_relay,
                physical_node_id: physical_node_id.as_ref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: delivery_observation.domain_timestamp,
            });
        self.runtime.inner.metrics.observe_branch_node_received(
            branch_key_display(&self.key),
            NodeBatchObservation {
                domain: &self.domain,
                kind: processor.kind,
                node: &processor.processor,
                relay: incoming_relay,
                physical_node_id: physical_node_id.as_ref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: delivery_observation.domain_timestamp,
            },
        );
        self.runtime.mark_branch_aggregated_metrics_updated(
            &self.domain,
            processor.kind,
            &processor.processor,
        );
        for seconds in delivery_observation.latency_seconds {
            self.runtime
                .inner
                .metrics
                .observe_global_delivery_latency_at_domain_time(NodeLatencyObservation {
                    domain: &self.domain,
                    kind: processor.kind,
                    node: &processor.processor,
                    relay: incoming_relay,
                    physical_node_id: physical_node_id.as_ref(),
                    seconds,
                    domain_timestamp: delivery_observation.domain_timestamp,
                });
            self.runtime.inner.metrics.observe_branch_delivery_latency(
                branch_key_display(&self.key),
                NodeLatencyObservation {
                    domain: &self.domain,
                    kind: processor.kind,
                    node: &processor.processor,
                    relay: incoming_relay,
                    physical_node_id: physical_node_id.as_ref(),
                    seconds,
                    domain_timestamp: delivery_observation.domain_timestamp,
                },
            );
        }
        processor
            .accept_input(graph, self, incoming_relay, batch)
            .await;
        self.processors.insert(processor_id.clone(), processor);
    }

    pub(super) async fn flush_processor_collected_inputs(
        &mut self,
        graph: &SharedActiveGraph,
        processor_id: &ModelName,
    ) {
        let Some(mut processor) = self.processors.remove(processor_id) else {
            return;
        };
        processor.flush_all_collected_inputs(graph, self).await;
        self.processors.insert(processor_id.clone(), processor);
    }

    pub(super) async fn dispatch_output(
        &mut self,
        graph: &SharedActiveGraph,
        output: &RelayProcessorOutputNode,
        source_kind: ModelKind,
        source: &ModelName,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.runtime
            .inner
            .metrics
            .observe_global_node_sent(NodeBatchObservation {
                domain: &self.domain,
                kind: source_kind,
                node: &ModelName::from(source),
                relay: &output.relay,
                physical_node_id: self
                    .runtime
                    .inner
                    .remote_dispatch
                    .local_node_id
                    .read()
                    .as_ref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: batch.domain_timestamp(),
            });
        self.runtime.inner.metrics.observe_branch_node_sent(
            branch_key_display(&self.key),
            NodeBatchObservation {
                domain: &self.domain,
                kind: source_kind,
                node: &ModelName::from(source),
                relay: &output.relay,
                physical_node_id: self
                    .runtime
                    .inner
                    .remote_dispatch
                    .local_node_id
                    .read()
                    .as_ref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: batch.domain_timestamp(),
            },
        );
        self.runtime
            .mark_branch_aggregated_metrics_updated(&self.domain, source_kind, source);
        self.dispatch_stream(graph, &output.relay, batch).await
    }

    pub(super) async fn tick(&mut self, graph: &SharedActiveGraph, now: Timestamp) {
        let processor_ids = self.processors.keys().cloned().collect::<Vec<_>>();
        for processor_id in processor_ids {
            let Some(mut processor) = self.processors.remove(&processor_id) else {
                continue;
            };
            processor.tick(graph, self, now).await;
            self.processors.insert(processor_id, processor);
        }
    }

    pub(super) async fn force_flush(&mut self, graph: &SharedActiveGraph, now: Timestamp) {
        let processor_ids = self.processors.keys().cloned().collect::<Vec<_>>();
        for processor_id in processor_ids {
            tokio::task::consume_budget().await;
            let Some(mut processor) = self.processors.remove(&processor_id) else {
                continue;
            };
            processor.flush_all_collected_inputs(graph, self).await;
            processor.flush_guest_buffers(graph, self, now).await;
            let current = graph.load_full();
            processor.refresh(
                &self.runtime,
                &self.domain,
                current.as_ref().map(StdArc::clone),
            );
            processor.flush_route_buffers(graph, self, now).await;
            processor.tick(graph, self, now).await;
            self.processors.insert(processor_id, processor);
        }
    }

    pub(super) fn next_deadline(&self) -> Option<Timestamp> {
        self.processors
            .values()
            .filter_map(RelayProcessorNode::next_deadline)
            .min()
    }

    pub(super) fn buffer_deadlines(&self) -> Vec<BranchBufferDeadline> {
        self.processors
            .values()
            .flat_map(RelayProcessorNode::buffer_deadlines)
            .collect()
    }

    pub(super) fn buffer_deadline_due(
        &self,
        snapshot: &DomainExecutionSnapshot,
    ) -> BranchBufferTimingResult<bool> {
        for processor in self.processors.values() {
            if processor.buffer_deadline_due(&self.domain_clock, snapshot)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl IngestorRouteTask {
    pub(super) fn handle_general_error(&self, acks: &[AckSet], reason: String) {
        if self.template.branch.source_kind == ModelKind::Ingestor {
            self.runtime_handle.handle_general_error_for_acks(
                &self.domain,
                self.template.branch.source_kind,
                &self.ingestor,
                &self.template.branch.error_policies,
                acks.iter(),
                reason,
            );
        } else {
            self.runtime_handle
                .handle_internal_processor_error_for_acks(
                    &self.domain,
                    self.template.branch.source_kind,
                    &self.ingestor,
                    &self.template.branch.error_policies,
                    acks.iter(),
                    reason,
                );
        }
    }

    pub(super) async fn prepare_input(
        &self,
        input: BranchedEntrypointInput,
    ) -> Vec<RelayRecordBatch> {
        let input_batch = match branched_entrypoint_batch_from_inputs_blocking(vec![input]).await {
            Ok(batch) => batch,
            Err((error, acks)) => {
                self.handle_general_error(
                    &acks,
                    format!(
                        "{} '{}' failed to build route input batch: {}",
                        self.template.branch.source_kind.as_str(),
                        self.ingestor.as_str(),
                        error
                    ),
                );
                return Vec::new();
            }
        };
        let branch_plan = match branched_branch_plan_blocking(input_batch.clone()).await {
            Ok(plan) => plan,
            Err(error) => {
                self.handle_general_error(
                    &input_batch.acks,
                    format!(
                        "{} '{}' failed to evaluate output branch assignments: {}",
                        self.template.branch.source_kind.as_str(),
                        self.ingestor.as_str(),
                        error
                    ),
                );
                return Vec::new();
            }
        };
        if self.template.branch.source_kind == ModelKind::Ingestor {
            let row_count = input_batch.batch.batch().num_rows();
            let row_bytes = input_batch
                .batch
                .batch()
                .columns()
                .iter()
                .map(|column| {
                    let Ok(bytes) = column.to_data().get_slice_memory_size() else {
                        return u64::MAX;
                    };
                    bytes.arch_into()
                })
                .fold(0_u64, u64::saturating_add)
                .checked_div(row_count.arch_into())
                .unwrap_or_default();
            for (key, row) in &branch_plan.valid_rows {
                let Some(metadata) = input_batch.metadata.get(*row) else {
                    continue;
                };
                self.runtime_handle
                    .inner
                    .metrics
                    .observe_branch_node_without_stream_received(
                        branch_key_display(key),
                        NodeWithoutRelayObservation {
                            domain: &self.domain,
                            kind: self.template.branch.source_kind,
                            node: &ModelName::from(&self.template.branch.source),
                            physical_node_id: self
                                .runtime_handle
                                .inner
                                .remote_dispatch
                                .local_node_id
                                .read()
                                .as_ref(),
                            messages: 1,
                            bytes: row_bytes,
                            domain_timestamp: Some(metadata.ingested_at_high_watermark()),
                        },
                    );
                self.runtime_handle.mark_branch_aggregated_metrics_updated(
                    &self.domain,
                    self.template.branch.source_kind,
                    &self.template.branch.source,
                );
            }
        }

        let mut batch_builds = FuturesUnordered::new();
        for selection in branch_plan.selections {
            tokio::task::consume_budget().await;
            batch_builds.push(branched_branch_filter_blocking(
                input_batch.clone(),
                selection,
                self.template.ack_boundary,
            ));
        }
        let mut prepared = Vec::new();
        while let Some(batch_result) = futures_util::StreamExt::next(&mut batch_builds).await {
            tokio::task::consume_budget().await;
            match batch_result {
                Ok((_, batch)) => prepared.push(batch),
                Err((error, acks)) => self.handle_general_error(
                    &acks,
                    format!(
                        "{} '{}' failed to prepare output branch batch: {}",
                        self.template.branch.source_kind.as_str(),
                        self.ingestor.as_str(),
                        error
                    ),
                ),
            }
        }
        prepared
    }

    pub(super) async fn flush_key(&mut self, key: &Option<BranchKey>) {
        let Some(pending) = self.pending.remove(key) else {
            return;
        };
        let acks = pending
            .batches
            .iter()
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect::<Vec<_>>();
        let batch = match RelayRecordBatch::concat(pending.batches) {
            Ok(batch) => batch,
            Err(error) => {
                self.handle_general_error(
                    &acks,
                    format!(
                        "{} '{}' failed to concatenate output route batch: {}",
                        self.template.branch.source_kind.as_str(),
                        self.ingestor.as_str(),
                        error
                    ),
                );
                return;
            }
        };
        if let Err(error) = self.branch_sender.send(batch).await {
            let batch = error.0;
            self.handle_general_error(
                &batch.acks,
                format!(
                    "{} '{}' failed to forward prepared batch for relay '{}'",
                    self.template.branch.source_kind.as_str(),
                    self.ingestor.as_str(),
                    self.template.branch.root_relay.as_str()
                ),
            );
        }
    }

    pub(super) async fn accept(
        &mut self,
        input: BranchedEntrypointInput,
        domain_clock: &DomainClock,
    ) {
        for batch in self.prepare_input(input).await {
            tokio::task::consume_budget().await;
            let key = batch.key.clone();
            let estimated_bytes = batch.estimated_bytes();
            if !self.pending.contains_key(&key) {
                let snapshot = match domain_clock.snapshot() {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        self.handle_general_error(
                            &batch.acks,
                            format!(
                                "{} '{}' could not read the domain clock while starting an output \
                                 flush: {error}",
                                self.template.branch.source_kind.as_str(),
                                self.ingestor.as_str(),
                            ),
                        );
                        continue;
                    }
                };
                let mut flush_timer = BranchBufferTimer::default();
                if let Err(error) =
                    flush_timer.arm_flush(self.template.flush_policy, domain_clock, &snapshot)
                {
                    self.handle_general_error(
                        &batch.acks,
                        format!(
                            "{} '{}' could not start an output flush deadline: {error}",
                            self.template.branch.source_kind.as_str(),
                            self.ingestor.as_str(),
                        ),
                    );
                    continue;
                }
                self.pending.insert(
                    key.clone(),
                    PendingIngestorRouteBatch {
                        batches: Vec::new(),
                        estimated_bytes: 0,
                        flush_timer,
                    },
                );
            }
            let pending = self
                .pending
                .get_mut(&key)
                .verified("the branch buffer is inserted above when it is absent");
            pending.estimated_bytes = pending
                .estimated_bytes
                .checked_add(estimated_bytes)
                .assured("both counts estimate bytes of batches this node already holds");
            pending.batches.push(batch);
            if self
                .template
                .flush_policy
                .size_boundary_reached(pending.estimated_bytes)
            {
                self.flush_key(&key).await;
            }
        }
    }

    pub(super) async fn flush_due(
        &mut self,
        domain_clock: &DomainClock,
    ) -> BranchBufferTimingResult<()> {
        let snapshot = domain_clock
            .snapshot()
            .map_err(|error| error.change_context(BranchBufferTimingError::LogicalDeadline))?;
        let mut keys = Vec::new();
        for (key, pending) in &self.pending {
            if pending.flush_timer.is_due(domain_clock, &snapshot)? {
                keys.push(key.clone());
            }
        }
        for key in keys {
            tokio::task::consume_budget().await;
            self.flush_key(&key).await;
        }
        Ok(())
    }

    pub(super) async fn flush_all(&mut self) {
        let keys = self.pending.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            tokio::task::consume_budget().await;
            self.flush_key(&key).await;
        }
    }

    pub(super) fn flush_deadlines(&self) -> Vec<BranchBufferDeadline> {
        self.pending
            .values()
            .filter_map(|pending| pending.flush_timer.deadline())
            .collect()
    }

    fn pending_acks(&self) -> Vec<AckSet> {
        self.pending
            .values()
            .flat_map(|pending| &pending.batches)
            .flat_map(|batch| batch.acks.iter().cloned())
            .collect()
    }

    pub(super) async fn run(
        mut self,
        mut input: mpsc::Receiver<BranchedEntrypointInput>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let domain_clock = match self.runtime_handle.bind_domain_clock(&self.domain) {
            Ok(clock) => clock,
            Err(error) => {
                self.runtime_handle.events().report_error(format!(
                    "{} '{}' in domain '{}' could not bind its route flush clock: {error}",
                    self.template.branch.source_kind.as_str(),
                    self.ingestor.as_str(),
                    self.domain.as_str(),
                ));
                return;
            }
        };
        loop {
            tokio::task::consume_budget().await;
            let flush_deadlines = self.flush_deadlines();
            let has_flush_deadlines = !flush_deadlines.is_empty();
            tokio::select! {
                biased;
                // A signalled stop and a dropped sender both mean the owner is gone, and this
                // arm drains and finishes either way, so the outcome carries nothing to read.
                _ = shutdown_rx.changed() => {
                    input.close();
                    while let Some(message) = input.recv().await {
                        tokio::task::consume_budget().await;
                        self.accept(message, &domain_clock).await;
                    }
                    self.flush_all().await;
                    break;
                }
                result = wait_for_branch_buffer_deadlines(&domain_clock, flush_deadlines),
                    if has_flush_deadlines =>
                {
                    if let Err(error) = result {
                        let acks = self.pending_acks();
                        self.handle_general_error(
                            &acks,
                            format!(
                                "{} '{}' could not wait for an output flush deadline: {error}",
                                self.template.branch.source_kind.as_str(),
                                self.ingestor.as_str(),
                            ),
                        );
                        self.flush_all().await;
                        break;
                    }
                    if let Err(error) = self.flush_due(&domain_clock).await {
                        let acks = self.pending_acks();
                        self.handle_general_error(
                            &acks,
                            format!(
                                "{} '{}' could not inspect an output flush deadline: {error}",
                                self.template.branch.source_kind.as_str(),
                                self.ingestor.as_str(),
                            ),
                        );
                        self.flush_all().await;
                        break;
                    }
                }
                message = input.recv() => {
                    let Some(message) = message else {
                        self.flush_all().await;
                        break;
                    };
                    self.accept(message, &domain_clock).await;
                }
            }
        }
    }
}

impl IngestorRouteRuntime {
    pub(super) fn new(
        runtime_handle: Runtime,
        domain: DomainName,
        ingestor: IngestorName,
        graph: SharedActiveGraph,
        template: IngestorRouteTemplate,
        expiration_scan_interval: Duration,
    ) -> Arc<Self> {
        let branch_runtime = BranchExecutionRuntime::new(
            runtime_handle.clone(),
            domain.clone(),
            ingestor.clone(),
            graph,
            template.branch.clone(),
            expiration_scan_interval,
        );
        let (sender, input) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let runtime = Arc::new(Self {
            sender,
            shutdown,
            task: parking_lot::Mutex::new(None),
            branch_runtime: branch_runtime.clone(),
        });
        let task = tokio::spawn(
            IngestorRouteTask {
                runtime_handle,
                domain,
                ingestor,
                template,
                branch_sender: branch_runtime.sender(),
                pending: HashMap::default(),
            }
            .run(input, shutdown_rx),
        );
        *runtime.task.lock() = Some(task);
        runtime
    }

    pub(super) fn sender(&self) -> mpsc::Sender<BranchedEntrypointInput> {
        self.sender.clone()
    }

    pub(super) async fn shutdown(&self) {
        self.shutdown.send_replace(true);
        let task = self.task.lock().take();
        if let Some(task) = task {
            task.join_after_shutdown("branch entrypoint").await;
        }
        self.branch_runtime.shutdown().await;
    }
}

impl BranchExecutionRuntime {
    async fn enqueue_prepared_inputs(
        context: BranchExecutionDispatchContext<'_>,
        instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
        inputs: Vec<BranchedEntrypointInput>,
        lanes: &mut BranchDispatchLanes,
    ) {
        let BranchExecutionDispatchContext {
            runtime_handle,
            domain,
            ingestor,
            graph,
            template,
            now,
        } = context;
        if inputs.is_empty() {
            return;
        }

        for message in inputs {
            tokio::task::consume_budget().await;
            let key = message.key.clone();
            let instance = match instances.get_or_try_create_with(key.clone(), now, |key| {
                template.instantiate(runtime_handle, domain, key.clone())
            }) {
                Ok(instance) => instance,
                Err(error) => {
                    let reason = format!(
                        "failed to instantiate branch '{}': {}",
                        branch_key_display(&key),
                        error
                    );
                    if template.source_kind == ModelKind::Ingestor {
                        runtime_handle.handle_general_error_for_acks(
                            domain,
                            template.source_kind,
                            ingestor,
                            &template.error_policies,
                            message.acks.iter(),
                            reason,
                        );
                    } else {
                        runtime_handle.handle_internal_processor_error_for_acks(
                            domain,
                            template.source_kind,
                            ingestor,
                            &template.error_policies,
                            message.acks.iter(),
                            reason,
                        );
                    }
                    continue;
                }
            };
            if instance.created {
                runtime_handle.observe_branch_instance_created(
                    domain,
                    template.branch.as_ref(),
                    &key,
                );
                debug!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    key = branch_key_display(&key),
                    "created branch runtime"
                );
            }
            if let Some(max_instances) = template.branch_max_instances {
                let evicted = evict_branch_instance_instances_to_capacity(
                    runtime_handle,
                    domain,
                    ingestor,
                    template.branch.as_ref(),
                    max_instances,
                    instances,
                )
                .await;
                for evicted_key in evicted {
                    lanes.reject_queued(
                        &evicted_key,
                        "branch generation was evicted before queued dispatch",
                    );
                }
            }
            if !lanes.active.insert(key.clone()) {
                lanes.queue(key, now, message);
                continue;
            }
            let state = instance.state.clone();
            let graph = graph.clone();
            let dispatch_key = key.clone();
            let dispatch_acks = message.acks.clone();
            let (started, started_rx) = oneshot::channel();
            let handle = AbortOnDropHandle::new(tokio::spawn(async move {
                let mut branch = state.lock().await;
                started
                    .send(())
                    .means_shutdown("branch lifecycle dispatch scheduler");
                branch.dispatch(&graph, message).await;
                branch.next_deadline()
            }));
            started_rx
                .await
                .assured("the spawned dispatch signals after acquiring its infallible branch lock");
            lanes.pending.push(Box::pin(async move {
                BranchDispatchCompletion {
                    key: dispatch_key,
                    acks: dispatch_acks,
                    result: handle.await,
                }
            }));
        }
    }

    async fn finish_dispatch(
        context: BranchExecutionDispatchContext<'_>,
        instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
        lanes: &mut BranchDispatchLanes,
        next_deadline: &mut Option<Timestamp>,
        completion: BranchDispatchCompletion,
    ) {
        let BranchExecutionDispatchContext {
            runtime_handle,
            domain,
            ingestor,
            graph,
            template,
            ..
        } = context;
        let key = completion.key.clone();
        Self::handle_dispatch_completion(
            runtime_handle,
            domain,
            ingestor,
            template,
            next_deadline,
            completion,
        );
        let was_active = lanes.active.remove(&key);
        debug_assert!(was_active, "completed branch dispatch must own its lane");
        let Some(next) = lanes.take_next(&key) else {
            return;
        };
        Self::enqueue_prepared_inputs(
            BranchExecutionDispatchContext {
                runtime_handle,
                domain,
                ingestor,
                graph,
                template,
                now: next.received_at,
            },
            instances,
            vec![next.batch],
            lanes,
        )
        .await;
    }

    fn handle_dispatch_completion(
        runtime_handle: &Runtime,
        domain: &DomainName,
        ingestor: &IngestorName,
        template: &BranchInstanceTemplate,
        next_deadline: &mut Option<Timestamp>,
        completion: BranchDispatchCompletion,
    ) {
        match completion.result {
            Ok(deadline) => {
                record_next_branch_instance_branch_deadline(next_deadline, deadline);
            }
            Err(error) => {
                runtime_handle.handle_internal_processor_error_for_acks(
                    domain,
                    template.source_kind,
                    ingestor,
                    &template.error_policies,
                    completion.acks.iter(),
                    format!(
                        "branch '{}' dispatch task failed: {}",
                        branch_key_display(&completion.key),
                        error
                    ),
                );
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn dispatch_prepared_inputs(
        context: BranchExecutionDispatchContext<'_>,
        instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
        inputs: Vec<BranchedEntrypointInput>,
    ) -> Option<Timestamp> {
        let BranchExecutionDispatchContext {
            runtime_handle,
            domain,
            ingestor,
            graph,
            template,
            now,
        } = context;
        let mut lanes = BranchDispatchLanes::default();
        Self::enqueue_prepared_inputs(
            BranchExecutionDispatchContext {
                runtime_handle,
                domain,
                ingestor,
                graph,
                template,
                now,
            },
            instances,
            inputs,
            &mut lanes,
        )
        .await;
        let mut next_deadline = None;
        while let Some(completion) = lanes.pending.next().await {
            tokio::task::consume_budget().await;
            Self::finish_dispatch(
                BranchExecutionDispatchContext {
                    runtime_handle,
                    domain,
                    ingestor,
                    graph,
                    template,
                    now,
                },
                instances,
                &mut lanes,
                &mut next_deadline,
                completion,
            )
            .await;
        }
        next_deadline
    }

    pub(super) fn new(
        runtime_handle: Runtime,
        domain: DomainName,
        ingestor: IngestorName,
        graph: SharedActiveGraph,
        template: BranchInstanceTemplate,
        expiration_scan_interval: Duration,
    ) -> Arc<Self> {
        // input from ingestor/re-ingestor
        let (sender, mut input) = mpsc::channel(1);
        let (checkpoints, mut checkpoint_requests) = mpsc::channel(1);
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let runtime = Arc::new(Self {
            domain: domain.clone(),
            ingestor: ingestor.clone(),
            sender,
            checkpoints,
            shutdown,
            task: parking_lot::Mutex::new(None),
        });
        runtime_handle.register_branch_lifecycle_metrics(&domain, template.branch.as_ref());

        let task = tokio::spawn(async move {
            let domain_clock = match runtime_handle.bind_domain_clock(&domain) {
                Ok(clock) => clock,
                Err(error) => {
                    runtime_handle.events().report_error(format!(
                        "branch runtime for ingestor '{}' in domain '{}' could not bind its \
                         clock: {error}",
                        ingestor.as_str(),
                        domain.as_str(),
                    ));
                    return;
                }
            };
            let mut instances =
                BranchInstanceRegistry::<Option<BranchKey>, Mutex<BranchRuntime>>::new();
            let mut last_persisted_lru_lsm = match restore_branch_instance_lru_snapshot(
                &runtime_handle,
                &domain,
                &template,
                &mut instances,
            ) {
                Ok(lsm) => lsm,
                Err(error) => {
                    warn!(
                        domain = domain.as_str(),
                        ingestor = ingestor.as_str(),
                        error = %error,
                        "failed to restore branch lru snapshot"
                    );
                    0
                }
            };
            if let Some(max_instances) = template.branch_max_instances {
                drop(
                    evict_branch_instance_instances_to_capacity(
                        &runtime_handle,
                        &domain,
                        &ingestor,
                        template.branch.as_ref(),
                        max_instances,
                        &mut instances,
                    )
                    .await,
                );
            }
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            let mut next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();
            let now = match domain_clock.snapshot() {
                Ok(snapshot) => snapshot.now(),
                Err(error) => {
                    runtime_handle.events().report_error(format!(
                        "branch runtime for ingestor '{}' in domain '{}' lost its clock: {error}",
                        ingestor.as_str(),
                        domain.as_str(),
                    ));
                    return;
                }
            };
            let mut next_branch_deadline =
                tick_due_branch_instance_branches(&graph, now, &instances).await;
            let ownership_entity = DomainNodeRef::node_in(
                domain.clone(),
                template.source_kind,
                ModelName::from(&template.source),
            );
            let mut checkpoint_requests_open = true;
            let mut lanes = BranchDispatchLanes::default();

            loop {
                tokio::task::consume_budget().await;
                let ownership_frozen =
                    runtime_handle.ownership_handoff_entity_is_frozen(&ownership_entity);
                let now = match domain_clock.snapshot() {
                    Ok(snapshot) => snapshot.now(),
                    Err(error) => {
                        runtime_handle.events().report_error(format!(
                            "branch runtime for ingestor '{}' in domain '{}' lost its clock: \
                             {error}",
                            ingestor.as_str(),
                            domain.as_str(),
                        ));
                        break;
                    }
                };
                let mut did_scheduled_work = false;
                if !ownership_frozen && Instant::now() >= next_expiration_scan {
                    if let Some(branch_ttl) = template.branch_ttl {
                        let expired = expire_branch_instance_instances(
                            &runtime_handle,
                            &domain,
                            &ingestor,
                            template.branch.as_ref(),
                            now,
                            branch_ttl,
                            &mut instances,
                        )
                        .await;
                        for expired_key in expired {
                            lanes.reject_queued(
                                &expired_key,
                                "branch generation expired before queued dispatch",
                            );
                        }
                    }
                    next_expiration_scan = Instant::now() + expiration_scan_interval;
                    did_scheduled_work = true;
                }
                if Instant::now() >= next_lru_snapshot {
                    if let Err(error) = persist_branch_instance_lru_snapshot(
                        &runtime_handle,
                        &domain,
                        &template,
                        &instances,
                        &mut last_persisted_lru_lsm,
                    ) {
                        warn!(
                            domain = domain.as_str(),
                            ingestor = ingestor.as_str(),
                            error = %error,
                            "failed to persist branch lru snapshot"
                        );
                    }
                    next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();
                    did_scheduled_work = true;
                }
                if lanes.is_empty()
                    && !ownership_frozen
                    && next_branch_deadline.is_some_and(|deadline| deadline <= now)
                {
                    next_branch_deadline =
                        tick_due_branch_instance_branches(&graph, now, &instances).await;
                    did_scheduled_work = true;
                }
                if did_scheduled_work {
                    continue;
                }

                let sleep_duration = if ownership_frozen {
                    OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL
                } else {
                    let expiration_sleep = next_expiration_scan
                        .checked_duration_since(Instant::now())
                        .unwrap_or(Duration::ZERO);
                    let branch_sleep = match next_branch_deadline {
                        Some(deadline) => {
                            match wall_duration_until_domain_deadline(
                                &runtime_handle,
                                &domain,
                                now,
                                deadline,
                            ) {
                                Ok(duration) => Some(duration),
                                Err(error) => {
                                    runtime_handle.events().report_error(format!(
                                        "branch runtime for ingestor '{}' in domain '{}' lost its \
                                         clock: {error}",
                                        ingestor.as_str(),
                                        domain.as_str(),
                                    ));
                                    break;
                                }
                            }
                        }
                        None => None,
                    };
                    let until_next_deadline = match branch_sleep {
                        Some(branch_sleep) => expiration_sleep.min(branch_sleep),
                        None => expiration_sleep,
                    };
                    until_next_deadline.min(
                        next_lru_snapshot
                            .checked_duration_since(Instant::now())
                            .unwrap_or(Duration::ZERO),
                    )
                };
                tokio::select! {
                    biased;
                    checkpoint = checkpoint_requests.recv(), if checkpoint_requests_open && lanes.is_empty() => {
                        let Some(checkpoint) = checkpoint else {
                            checkpoint_requests_open = false;
                            continue;
                        };
                        let placement = branch_lru_placement(&runtime_handle, &domain, &template);
                        let result = match encode_branch_lru_snapshot(&instances.snapshot_entries()) {
                            Ok(payload) => {
                                let snapshot = PersistedRuntimeStateEntry {
                                    lsm: instances.version(),
                                    schema_fingerprint: placement.schema_fingerprint,
                                    payload,
                                };
                                match runtime_handle.persist_branch_lru_snapshot(
                                    placement.clone(),
                                    snapshot.clone(),
                                ) {
                                    Ok(()) => {
                                        last_persisted_lru_lsm = snapshot.lsm;
                                        Ok(snapshot)
                                    }
                                    Err(error) => Err(OwnershipHandoffError::persistence(
                                        error.current_context().clone(),
                                    )),
                                }
                            }
                            Err(error) => Err(OwnershipHandoffError::checkpoint(error)),
                        };
                        checkpoint
                            .send(result)
                            .means_peer_left("branch lifecycle checkpoint requester");
                    }
                    completion = lanes.pending.next(), if !lanes.is_empty() => {
                        let Some(completion) = completion else {
                            continue;
                        };
                        Self::finish_dispatch(
                            BranchExecutionDispatchContext {
                                runtime_handle: &runtime_handle,
                                domain: &domain,
                                ingestor: &ingestor,
                                graph: &graph,
                                template: &template,
                                now,
                            },
                            &mut instances,
                            &mut lanes,
                            &mut next_branch_deadline,
                            completion,
                        )
                        .await;
                    }
                    message = input.recv(), if !ownership_frozen => {
                        let Some(message) = message else {
                            while let Some(completion) = lanes.pending.next().await {
                                tokio::task::consume_budget().await;
                                Self::finish_dispatch(
                                    BranchExecutionDispatchContext {
                                        runtime_handle: &runtime_handle,
                                        domain: &domain,
                                        ingestor: &ingestor,
                                        graph: &graph,
                                        template: &template,
                                        now,
                                    },
                                    &mut instances,
                                    &mut lanes,
                                    &mut next_branch_deadline,
                                    completion,
                                )
                                .await;
                            }
                            break;
                        };
                        Self::enqueue_prepared_inputs(
                            BranchExecutionDispatchContext {
                                runtime_handle: &runtime_handle,
                                domain: &domain,
                                ingestor: &ingestor,
                                graph: &graph,
                                template: &template,
                                now,
                            },
                            &mut instances,
                            vec![message],
                            &mut lanes,
                        )
                        .await;
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            input.close();
                            while let Some(message) = input.recv().await {
                                tokio::task::consume_budget().await;
                                let drain_now = match runtime_handle
                                    .current_stream_expiration_time(&domain)
                                {
                                    Ok(now) => now,
                                    Err(error) => {
                                        let reason = format!(
                                            "branch runtime for ingestor '{}' in domain '{}' lost \
                                             its clock while draining: {error}",
                                            ingestor.as_str(),
                                            domain.as_str(),
                                        );
                                        if template.source_kind == ModelKind::Ingestor {
                                            runtime_handle.handle_general_error_for_acks(
                                                &domain,
                                                template.source_kind,
                                                &ingestor,
                                                &template.error_policies,
                                                message.acks.iter(),
                                                reason,
                                            );
                                        } else {
                                            runtime_handle.handle_internal_processor_error_for_acks(
                                                &domain,
                                                template.source_kind,
                                                &ingestor,
                                                &template.error_policies,
                                                message.acks.iter(),
                                                reason,
                                            );
                                        }
                                        continue;
                                    }
                                };
                                Self::enqueue_prepared_inputs(
                                    BranchExecutionDispatchContext {
                                        runtime_handle: &runtime_handle,
                                        domain: &domain,
                                        ingestor: &ingestor,
                                        graph: &graph,
                                        template: &template,
                                        now: drain_now,
                                    },
                                    &mut instances,
                                    vec![message],
                                    &mut lanes,
                                )
                                .await;
                            }
                            while let Some(completion) = lanes.pending.next().await {
                                tokio::task::consume_budget().await;
                                Self::finish_dispatch(
                                    BranchExecutionDispatchContext {
                                        runtime_handle: &runtime_handle,
                                        domain: &domain,
                                        ingestor: &ingestor,
                                        graph: &graph,
                                        template: &template,
                                        now,
                                    },
                                    &mut instances,
                                    &mut lanes,
                                    &mut next_branch_deadline,
                                    completion,
                                )
                                .await;
                            }
                            break;
                        }
                    }
                    _ = runtime_handle.inner.ownership_handoff_freeze_changed.notified(), if ownership_frozen => {}
                    _ = sleep(sleep_duration) => {}
                }
            }

            if let Err(error) = persist_branch_instance_lru_snapshot(
                &runtime_handle,
                &domain,
                &template,
                &instances,
                &mut last_persisted_lru_lsm,
            ) {
                warn!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    error = %error,
                    "failed to persist final branch lru snapshot"
                );
            }
            shutdown_all_branch_instance_instances(
                &runtime_handle,
                &domain,
                &ingestor,
                template.branch.as_ref(),
                &mut instances,
            )
            .await;
        });
        *runtime.task.lock() = Some(task);
        runtime
    }

    pub(super) async fn checkpoint(&self) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
        let (response, receiver) = oneshot::channel();
        self.checkpoints.send(response).await.map_err(|_| {
            OwnershipHandoffError::checkpoint(format!(
                "{} '{}' branch lifecycle task is unavailable",
                self.domain.as_str(),
                self.ingestor.as_str()
            ))
        })?;
        receiver.await.map_err(|_| {
            OwnershipHandoffError::checkpoint(format!(
                "{} '{}' branch lifecycle task dropped its checkpoint response",
                self.domain.as_str(),
                self.ingestor.as_str()
            ))
        })?
    }

    pub(super) fn sender(&self) -> mpsc::Sender<BranchedEntrypointInput> {
        self.sender.clone()
    }

    pub(super) async fn shutdown(&self) {
        const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

        self.shutdown.send_replace(true);
        let Some(mut task) = self.task.lock().take() else {
            return;
        };

        match tokio::time::timeout(SHUTDOWN_GRACE_PERIOD, &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if error.is_cancelled() {
                    warn!(
                        domain = self.domain.as_str(),
                        ingestor = self.ingestor.as_str(),
                        "branched ingestor task was cancelled"
                    );
                } else {
                    warn!(
                        domain = self.domain.as_str(),
                        ingestor = self.ingestor.as_str(),
                        error = %error,
                        "branched ingestor task join failed"
                    );
                }
            }
            Err(_) => {
                warn!(
                    domain = self.domain.as_str(),
                    ingestor = self.ingestor.as_str(),
                    grace_period = %humantime::format_duration(SHUTDOWN_GRACE_PERIOD),
                    "branched ingestor task exceeded shutdown grace period; aborting"
                );
                task.abort();
                if let Err(error) = task.await
                    && !error.is_cancelled()
                {
                    warn!(
                        domain = self.domain.as_str(),
                        ingestor = self.ingestor.as_str(),
                        error = %error,
                        "aborted branched ingestor task join failed"
                    );
                }
            }
        }
    }
}

pub(super) async fn expire_branch_instance_instances(
    runtime: &Runtime,
    domain: &DomainName,
    ingestor: &IngestorName,
    branch: Option<&BranchName>,
    now: Timestamp,
    expiration_after: Duration,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) -> Vec<Option<BranchKey>> {
    let mut expired_keys = Vec::new();
    for (key, state) in instances.expire(now, expiration_after) {
        runtime.observe_branch_instance_removed(
            domain,
            branch,
            &key,
            Some(BranchEvictionReason::Ttl),
        );
        runtime.invalidate_branch_relay_generation(domain, &key);
        let mut branch = state.lock().await;
        branch.evict().await;
        debug!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            key = branch_key_display(&key),
            "expired branched processor root"
        );
        expired_keys.push(key);
    }
    expired_keys
}

pub(super) async fn evict_branch_instance_instances_to_capacity(
    runtime: &Runtime,
    domain: &DomainName,
    ingestor: &IngestorName,
    branch: Option<&BranchName>,
    max_instances: NonZeroUsize,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) -> Vec<Option<BranchKey>> {
    let mut evicted_keys = Vec::new();
    for (key, state) in instances.evict_lru_to_capacity(max_instances) {
        runtime.observe_branch_instance_removed(
            domain,
            branch,
            &key,
            Some(BranchEvictionReason::Lru),
        );
        runtime.invalidate_branch_relay_generation(domain, &key);
        let mut branch = state.lock().await;
        branch.evict().await;
        debug!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            key = branch_key_display(&key),
            max_instances,
            "evicted branch runtime by lru"
        );
        evicted_keys.push(key);
    }
    evicted_keys
}

pub(super) async fn shutdown_all_branch_instance_instances(
    runtime: &Runtime,
    domain: &DomainName,
    ingestor: &IngestorName,
    branch: Option<&BranchName>,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) {
    for (key, state) in instances.drain() {
        runtime.observe_branch_instance_removed(domain, branch, &key, None);
        drop(state);
        debug!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            key = branch_key_display(&key),
            "stopped branch runtime"
        );
    }
}

pub(super) fn branch_lru_placement(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
) -> RuntimeStatePlacement {
    runtime.state_placement(
        domain,
        RuntimeStateKind::BranchLru,
        template.source_kind,
        &template.source,
        None,
    )
}

pub(super) fn restore_branch_instance_lru_snapshot(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) -> Result<u64, String> {
    let placement = branch_lru_placement(runtime, domain, template);
    let snapshot = runtime
        .take_restorable_branch_lru_snapshot(&placement)
        .map_err(|error| error.to_string())?;
    let Some(snapshot) = snapshot else {
        return Ok(0);
    };
    for (key, last_ingestion) in decode_branch_lru_snapshot(&snapshot.payload)? {
        let state = template.instantiate(runtime, domain, key.clone())?;
        runtime.observe_branch_instance_created(domain, template.branch.as_ref(), &key);
        instances.insert_restored(key, last_ingestion, state);
    }
    instances.set_version(snapshot.lsm);
    Ok(snapshot.lsm)
}

pub(super) fn persist_branch_instance_lru_snapshot<V>(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
    instances: &BranchInstanceRegistry<Option<BranchKey>, V>,
    last_persisted_lsm: &mut u64,
) -> Result<(), String> {
    let lsm = instances.version();
    if lsm <= *last_persisted_lsm {
        return Ok(());
    }
    let placement = branch_lru_placement(runtime, domain, template);
    let payload = encode_branch_lru_snapshot(&instances.snapshot_entries())?;
    runtime
        .persist_branch_lru_snapshot(
            placement.clone(),
            PersistedRuntimeStateEntry {
                lsm,
                schema_fingerprint: placement.schema_fingerprint,
                payload,
            },
        )
        .map_err(|error| error.to_string())?;
    *last_persisted_lsm = lsm;
    Ok(())
}

pub(super) async fn tick_due_branch_instance_branches(
    graph: &SharedActiveGraph,
    now: Timestamp,
    instances: &BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) -> Option<Timestamp> {
    let mut next = None;
    for instance in instances.states() {
        let mut branch = instance.lock().await;
        if branch
            .next_deadline()
            .is_some_and(|deadline| deadline <= now)
        {
            branch.tick(graph, now).await;
        }
        record_next_branch_instance_branch_deadline(&mut next, branch.next_deadline());
    }
    next
}

pub(super) fn record_next_branch_instance_branch_deadline(
    next: &mut Option<Timestamp>,
    candidate: Option<Timestamp>,
) {
    if let Some(candidate) = candidate {
        *next = Some(match *next {
            Some(current) => current.min(candidate),
            None => candidate,
        });
    }
}

pub(super) fn wall_duration_until_domain_deadline(
    runtime: &Runtime,
    domain: &DomainName,
    now: Timestamp,
    deadline: Timestamp,
) -> DomainClockAccessResult<Duration> {
    runtime
        .bind_domain_clock(domain)?
        .physical_duration_until(now, deadline)
}

pub(super) async fn flush_branch_junction(
    context: JunctionFlushContext<'_>,
    forwarded: RelayRecordBatch,
) {
    let JunctionFlushContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        input_relays,
        output_routes,
        materialized_values,
        execution_now,
    } = context;
    if let Some(acks) = dispatch_processor_outputs(
        ProcessorOutputDispatchContext {
            graph,
            branch,
            node_kind,
            source_kind: ModelKind::Junction,
            processor,
            error_policies,
            input_relays,
            filter_source: ProcessorOutputFilterSource::InputRelays,
            materialized_state: ProcessorMaterializedState::Admitted(materialized_values),
            execution_now,
        },
        output_routes,
        forwarded,
    )
    .await
    {
        for ack in acks {
            ack.ack_success();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use nervix_interconnect::EntityGatePurpose;
    use nervix_models::{IngestorName, ModelKind, ModelName, NodeRef, ParseAsType, RelayName};
    use triomphe::Arc;

    use super::*;
    use crate::{
        runtime_ack::{AckOutcome, AckRootTracker, AckSet},
        runtime_schema::{RuntimeValue, test_runtime_row},
    };
    #[test]
    fn pending_materialized_batches_remain_visible_in_entity_drain_status() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let processor = named::<ModelName>("wait_for_customer");
        let input_relay = named::<RelayName>("orders");
        let template = junction_branch_template(processor.as_str(), input_relay.as_str());
        let mut branch = template
            .instantiate(&runtime, &domain, None)
            .expect("junction branch should instantiate")
            .into_inner();
        branch
            .processors
            .get_mut(&processor)
            .expect("junction processor should exist")
            .pending_materialized
            .push_back(PendingMaterializedBatch::new(
                input_relay,
                quiesce_test_batch(),
            ));
        let counters =
            runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Junction, &processor));
        let mut gauges = BranchQuiesceGauges::new(counters.clone());

        gauges.observe(&branch, &processor);

        assert_eq!(counters.collected_inputs.load(Ordering::Acquire), 0);
        assert_eq!(counters.pending_materialized.load(Ordering::Acquire), 1);
        let status = runtime.entity_drain_status(
            &domain,
            &[],
            &[NodeRef {
                kind: ModelKind::Junction,
                identifier: processor.clone(),
            }],
            EntityGatePurpose::ModelAlteration,
        );
        assert_eq!(status.node_work_items, 1);
        assert!(!status.is_drained());

        let handoff_status = runtime.entity_drain_status(
            &domain,
            &[],
            &[NodeRef {
                kind: ModelKind::Junction,
                identifier: processor,
            }],
            EntityGatePurpose::OwnershipHandoff,
        );
        assert_eq!(handoff_status.node_work_items, 0);
        assert!(handoff_status.is_drained());

        drop(gauges);
        assert_eq!(counters.outstanding_work(), 0);
    }

    #[test]
    fn blocking_materialized_wait_is_excluded_only_from_ownership_handoff_work() {
        let counters = Arc::new(NodeQuiesceCounters::default());
        let mut work = NodeQuiesceWorkGuard::begin(counters.clone());

        assert_eq!(
            counters.outstanding_work_for(EntityGatePurpose::ModelAlteration),
            1
        );
        assert_eq!(
            counters.outstanding_work_for(EntityGatePurpose::OwnershipHandoff),
            1
        );

        work.park_for_required_materialized_state();
        assert_eq!(counters.mailbox_and_in_flight.load(Ordering::Acquire), 0);
        assert_eq!(counters.pending_materialized.load(Ordering::Acquire), 1);
        assert_eq!(
            counters.outstanding_work_for(EntityGatePurpose::ModelAlteration),
            1
        );
        assert_eq!(
            counters.outstanding_work_for(EntityGatePurpose::OwnershipHandoff),
            0
        );

        work.resume_from_required_materialized_state();
        assert_eq!(counters.mailbox_and_in_flight.load(Ordering::Acquire), 1);
        assert_eq!(counters.pending_materialized.load(Ordering::Acquire), 0);
        drop(work);
        assert_eq!(counters.outstanding_work(), 0);
    }

    #[tokio::test]
    async fn dropping_pending_materialized_batch_nacks_its_ack_root() {
        let tracker = Arc::new(AckRootTracker::default());
        let (acks, completion) = AckSet::tracked_root(tracker.clone());
        let pending = PendingMaterializedBatch::new(
            named("orders"),
            RelayRecordBatch::single(
                test_schema(&[("value", ParseAsType::I64)]),
                None,
                test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
                acks,
            )
            .expect("pending materialized test batch should build"),
        );

        assert_eq!(tracker.outstanding(), 1);
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
        drop(pending);

        assert_eq!(
            completion.wait().await,
            AckOutcome::NoAck(
                "node stopped while waiting for required materialized state at relay 'orders'"
                    .to_string()
            )
        );
        assert_eq!(tracker.outstanding(), 0);
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    }

    #[test]
    fn required_wait_ack_does_not_block_ownership_handoff_drain_status() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named::<IngestorName>("orders_source");
        let tracker = Arc::new(AckRootTracker::default());
        runtime.inner.in_flight_by_ingestor.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone()),
            tracker.clone(),
        );
        let (acks, _completion) = AckSet::tracked_root(tracker);
        let _required_wait = acks.required_wait_guard();
        let affected = [NodeRef {
            kind: ModelKind::Ingestor,
            identifier: ModelName::from(&ingestor),
        }];

        let alteration = runtime.entity_drain_status(
            &domain,
            &[],
            &affected,
            EntityGatePurpose::ModelAlteration,
        );
        assert_eq!(alteration.outstanding_acks, 1);
        assert!(!alteration.is_drained());

        let handoff = runtime.entity_drain_status(
            &domain,
            &[],
            &affected,
            EntityGatePurpose::OwnershipHandoff,
        );
        assert_eq!(handoff.outstanding_acks, 0);
        assert!(handoff.is_drained());
    }
}
