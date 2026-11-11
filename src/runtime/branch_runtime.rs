//! Ingestor branch lifecycle and concrete branch execution lanes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Route batching, concrete branch instantiation, branch-local FIFO dispatch, and
//!   branch TTL and capacity eviction.
//! - **Depends on.** Validated branch templates, relay boundaries, processors, execution
//!   admission, domain force-flush generations, node quiesce accounting, and runtime state
//!   persistence.
//! - **Must not know.** Plexus text, control-plane transactions, consensus, or connector protocols.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "branch task construction and teardown retain the branch state; admitted \
                  processing declares its own contract"
    )
)]

use error_stack::ResultExt as _;

use super::*;

pub(super) const BRANCH_INSTANCE_EXPIRATION_SCAN_INTERVAL: Duration = Duration::from_secs(30);

/// Every way a branched entrypoint fails to take one input into the concrete branch it belongs to,
/// before the input's routes buffer it. The ingestor or reingestor names itself when it reports
/// the failure, and the acknowledgements of the input fail with it.
#[derive(Debug, thiserror::Error)]
pub(super) enum BranchEntrypointError {
    #[error("could not read the domain time of accepted input")]
    AcceptedInputClock,
    #[error("failed to instantiate the branch runtime ({branch})")]
    Instantiate { branch: BranchScope },
    #[error("the branch dispatch task failed ({branch})")]
    DispatchTask { branch: BranchScope },
    #[error("failed to prepare the route input")]
    PrepareRouteInput,
    #[error("failed to prepare an output branch batch")]
    PrepareBranchBatch,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "branch lanes retain the concrete branch while consuming admitted input and \
                  scheduled ticks"
    )
)]
pub(super) struct BranchRuntime {
    pub(super) key: Option<BranchKey>,
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) routing: Option<DomainRoutingCache>,
    pub(super) routing_snapshot: Option<StdArc<DomainRoutingSnapshot>>,
    pub(super) domain_clock: DomainClock,
    pub(super) owner: NodeUnitRef,
    pub(super) root_relay: RelayName,
    pub(super) error_policies: ErrorPolicies,
    pub(super) relays: HashMap<RelayName, ConcreteRelayRuntime>,
    pub(super) materialized_states: HashMap<RelayName, MaterializedRelayStateOriginator>,
    pub(super) relay_state_epoch: Option<u64>,
    pub(super) processors: HashMap<ModelName, RelayProcessorNode>,
    pub(super) metrics: BranchRuntimeMetrics,
}

pub(super) struct BranchRuntimeMetrics {
    pub(super) source_dirty: BranchMetricsMark,
    pub(super) processor_dirty: HashMap<ModelName, BranchMetricsMark>,
    pub(super) source: BatchMetricsHandle,
    pub(super) source_input: Option<MessageMetricsHandle>,
    pub(super) processor_inputs: HashMap<ModelName, HashMap<RelayName, NodeInputMetricsHandle>>,
    pub(super) processor_outputs: HashMap<ModelName, HashMap<RelayName, BatchMetricsHandle>>,
}

#[derive(Debug)]
pub(super) struct PendingMaterializedBatch {
    pub(super) input_relay: RelayName,
    pub(super) batch: Option<RelayRecordBatch>,
    pub(super) required_wait: Option<AckParkGuard>,
}

pub(super) struct MaterializedBatchWaitContext<'a> {
    pub(super) shutdown_rx: &'a mut watch::Receiver<bool>,
    pub(super) wait_for_required_state: bool,
    pub(super) quiesce_work: Option<&'a mut NodeQuiesceWorkGuard>,
}

/// The per-domain handles one batch's materialized-dependency resolution reads.
///
/// Every task that resolves dependencies per batch — emitter, reingestor and branch — binds these
/// once when it starts, so resolution borrows what its caller already holds instead of resolving
/// the domain's routing and clock again for every batch.
pub(super) struct MaterializedDomainHandles<'a> {
    pub(super) routing: &'a mut DomainRoutingCache,
    pub(super) domain_clock: &'a DomainClock,
    pub(super) domain: &'a DomainName,
}

/// A message taken from its processor's `REQUIRED WAIT` queue to be re-evaluated, still holding
/// the park that keeps its acknowledgement out of drains and ownership handoffs.
pub(super) struct ParkedMaterializedBatch {
    pub(super) input_relay: RelayName,
    pub(super) batch: RelayRecordBatch,
    pub(super) required_wait: AckParkGuard,
}

impl PendingMaterializedBatch {
    pub(super) fn new(input_relay: RelayName, batch: RelayRecordBatch) -> Self {
        let required_wait = AckParkGuard::new(batch.acks.iter());
        Self::parked_again(input_relay, batch, required_wait)
    }

    /// Queues `batch` with `required_wait`, the park it already holds, so a message that waits
    /// again keeps its acknowledgement parked from one wait to the next.
    pub(super) fn parked_again(
        input_relay: RelayName,
        batch: RelayRecordBatch,
        required_wait: AckParkGuard,
    ) -> Self {
        Self {
            input_relay,
            batch: Some(batch),
            required_wait: Some(required_wait),
        }
    }

    /// Takes the message out of the queue to be re-evaluated, keeping its park.
    pub(super) fn into_parked(mut self) -> ParkedMaterializedBatch {
        let batch = self
            .batch
            .take()
            .assured("the pending entry holds its batch from construction until this take");
        let required_wait = self
            .required_wait
            .take()
            .assured("the pending entry holds its park from construction until this take");
        ParkedMaterializedBatch {
            input_relay: self.input_relay.clone(),
            batch,
            required_wait,
        }
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

pub(super) fn internal_processor_error_policies(general: GeneralErrorPolicy) -> ErrorPolicies {
    ErrorPolicies {
        message: MessageErrorPolicy::Log,
        general,
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "branch lanes retain the concrete branch while consuming admitted input and \
                  scheduled ticks"
    )
)]
pub(super) struct BranchExecutionRuntime {
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) sender: mpsc::Sender<BranchedEntrypointInput>,
    pub(super) checkpoints:
        mpsc::Sender<oneshot::Sender<OwnershipHandoffResult<PersistedRuntimeStateEntry>>>,
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: nervix_primitives::sync::blocking::Mutex<Option<JoinHandle<()>>>,
}

pub(super) struct IngestorRouteRuntime {
    pub(super) sender: mpsc::Sender<BranchedEntrypointInput>,
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: nervix_primitives::sync::blocking::Mutex<Option<JoinHandle<()>>>,
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
    /// Reports the depth of `pending` to this route's owning node, so a drain sees route output
    /// that has left the ingest group but has not yet reached branch execution.
    pub(super) quiesce: OutputBufferQuiesceGauge,
}

pub(super) struct BranchExecutionDispatchContext<'a> {
    pub(super) runtime_handle: &'a Runtime,
    pub(super) domain: &'a DomainName,
    pub(super) ingestor: &'a IngestorName,
    pub(super) template: &'a BranchInstanceTemplate,
    pub(super) domain_clock: &'a DomainClock,
}

struct BranchDispatchCompletion {
    key: Option<BranchKey>,
    acks: Vec<AckSet>,
    result: Result<Option<Timestamp>, nervix_primitives::task::JoinError>,
}

type PendingBranchDispatch = BoxFuture<'static, BranchDispatchCompletion>;

struct QueuedBranchDispatch {
    batch: BranchedEntrypointInput,
}

#[derive(Default)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "branch lanes retain the concrete branch while consuming admitted input and \
                  scheduled ticks"
    )
)]
struct BranchDispatchLanes {
    active: HashSet<Option<BranchKey>>,
    queued: HashMap<Option<BranchKey>, VecDeque<QueuedBranchDispatch>>,
    pending: FuturesUnordered<PendingBranchDispatch>,
}

impl BranchDispatchLanes {
    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn queue(&mut self, key: Option<BranchKey>, batch: RelayRecordBatch) {
        self.queued
            .entry(key)
            .or_default()
            .push_back(QueuedBranchDispatch { batch });
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
    pub(super) fn refresh_domain_routing(&mut self) -> Result<(), Report<DomainRoutingError>> {
        if self.routing.is_none() {
            self.routing = self.runtime.domain_routing_cache(&self.domain);
        }
        let Some(routing) = self.routing.as_mut() else {
            return Err(Report::new(DomainRoutingError::DomainNotInstantiated {
                domain: self.domain.clone(),
            }));
        };
        let current = routing.load();
        let changed = self
            .routing_snapshot
            .as_ref()
            .is_none_or(|snapshot| !StdArc::ptr_eq(snapshot, current));
        if changed {
            self.routing_snapshot = Some(current.clone());
        }
        Ok(())
    }

    pub(super) fn domain_routing(
        &self,
    ) -> Result<&DomainRoutingSnapshot, Report<DomainRoutingError>> {
        self.routing_snapshot.as_deref().ok_or_else(|| {
            Report::new(DomainRoutingError::SnapshotNotResolved {
                domain: self.domain.clone(),
            })
        })
    }

    pub(super) fn relay_schema(
        &self,
        relay: &RelayName,
    ) -> Result<Arc<CompiledSchema>, Report<DomainRoutingError>> {
        relay_schema_for_routing(self.domain_routing()?, &self.domain, relay)
    }

    pub(super) async fn evict(&mut self) {
        for (relay, state) in &mut self.materialized_states {
            if let Err(error) = self
                .runtime
                .delete_materialized_stream_key(state, &self.key)
            {
                warn!(domain = self.domain.as_str(), relay = relay.as_str(),
                    scope = %BranchScope::from(&self.key), error = %format_args!("{error:#}"),
                    "materialized assignment changed during branch eviction");
            }
        }
        self.materialized_states.clear();
        for relay in self.relays.values_mut() {
            relay.retire();
        }
        for processor in self.processors.values_mut() {
            processor.drop_collected_inputs("processor branch was evicted");
            processor.reset_window_state();
            processor.evict_inferencer_state();
        }
    }

    pub(super) async fn reconcile_materialized_state_membership(&mut self, relay: &RelayName) {
        let current_epoch = match self.domain_routing() {
            Ok(routing) => routing.relay_state_epoch.load(Ordering::Acquire),
            Err(_) => return,
        };
        if self.relay_state_epoch == Some(current_epoch) {
            if self.materialized_states.contains_key(relay) {
                return;
            }
            if self
                .domain_routing()
                .is_ok_and(|routing| !routing.materialized_stream_specs.contains_key(relay))
            {
                return;
            }
        }
        // A schedule publishes its routing before it advances the epoch, so routing read after the
        // epoch is at least as new as the epoch this reconciliation records. The routing the branch
        // read for an earlier batch may predate it.
        self.refresh_domain_routing().discarded(
            "a domain whose routing is not resolved has no materialized relays to reconcile, \
             which reading the routing below reports",
        );
        let (desired_relays, relay_schema, relay_unit) = match self.domain_routing() {
            Ok(routing) => {
                let desired_relays = routing
                    .materialized_stream_specs
                    .keys()
                    .filter(|relay| {
                        !routing
                            .materialized_stream_owner_nodes
                            .get(*relay)
                            .is_some_and(RelayOwnerRoutes::is_placed)
                    })
                    .cloned()
                    .collect::<HashSet<_>>();
                let relay_schema = routing
                    .materialized_stream_specs
                    .get(relay)
                    .map(|spec| spec.schema.clone());
                let relay_unit = routing.relay_partitioning(relay).unit_of(&self.key);
                (desired_relays, relay_schema, Some(relay_unit))
            }
            Err(_) => (HashSet::default(), None, None),
        };
        self.materialized_states
            .retain(|identifier, _| desired_relays.contains(identifier));
        if desired_relays.contains(relay) && !self.materialized_states.contains_key(relay) {
            let Some(schema) = relay_schema else {
                warn!(
                    domain = self.domain.as_str(),
                    relay = relay.as_str(),
                    "failed to reconcile materialized relay without its schema"
                );
                return;
            };
            // The state of this branch belongs to the unit of the relay that owns its key.
            let unit = match relay_unit {
                Some(Ok(unit)) => unit,
                Some(Err(error)) => {
                    warn!(
                        domain = self.domain.as_str(),
                        relay = relay.as_str(),
                        error = %format_args!("{error:#}"),
                        "failed to place the branch-local materialized relay state"
                    );
                    return;
                }
                None => return,
            };
            let placement = match self.runtime.state_placement_for_unit(
                &self.domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                unit,
                self.key.clone(),
            ) {
                Ok(placement) => placement,
                Err(error) => {
                    warn!(
                        domain = self.domain.as_str(),
                        relay = relay.as_str(),
                        error = %format_args!("{error:#}"),
                        "failed to place the branch-local materialized relay state"
                    );
                    return;
                }
            };
            if let Err(error) = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "first installation of this concrete branch restores its materialized placement \
                 before publishing the retained state",
                self.runtime
                    .prepare_materialized_stream_restore(&placement, &schema)
            )
            .await
            {
                warn!(
                    domain = self.domain.as_str(),
                    relay = relay.as_str(),
                    error = format!("{error:#}"),
                    "failed to open the persisted branch-local materialized relay snapshot"
                );
                return;
            }
            let mut assignment = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "first installation of this concrete branch binds its materialized placement and \
                 retains the resulting state",
                self.runtime.replicated_materialized_stream_state(
                    placement,
                    schema,
                    None,
                    Vec::new(),
                    None,
                )
            );
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
        self.relay_state_epoch = Some(current_epoch);
    }

    pub(super) async fn materialize_stream_batch(
        &mut self,
        relay: &RelayName,
        batch: &RelayRecordBatch,
    ) {
        if self
            .domain_routing()
            .ok()
            .and_then(|routing| routing.materialized_stream_owner_nodes.get(relay))
            .is_some_and(RelayOwnerRoutes::is_placed)
        {
            return;
        }
        self.reconcile_materialized_state_membership(relay).await;
        let Some(state) = self.materialized_states.get_mut(relay) else {
            return;
        };
        let messages = match batch.detached().try_into_messages() {
            Ok(messages) => messages,
            Err(error_and_batch) => {
                let error = error_and_batch.error;
                warn!(
                    domain = self.domain.as_str(),
                    relay = relay.as_str(),
                    scope = %BranchScope::from(&self.key),
                    error = %format_args!("{error:#}"),
                    "failed to decode branch-local materialized state batch"
                );
                return;
            }
        };
        let records = messages.into_iter().map(|message| message.record);
        if let Err(error) = self
            .runtime
            .apply_materialized_stream_records(state, &batch.key, records)
            .await
        {
            warn!(
                domain = self.domain.as_str(),
                relay = relay.as_str(),
                scope = %BranchScope::from(&self.key),
                error = %format_args!("{error:#}"),
                "materialized relay assignment changed while applying a branch-local batch"
            );
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
    ) -> error_stack::Result<(), ProcessorLiveStateError> {
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
        purpose: ProcessorCheckpointPurpose,
    ) -> OwnershipHandoffResult<()> {
        let Some(mut processor) = self.processors.remove(processor_id) else {
            return Ok(());
        };
        let result = processor
            .checkpoint_live_state(self, execution_now, purpose)
            .await;
        self.processors.insert(processor_id.clone(), processor);
        result
    }

    pub(super) async fn retry_processor_pending_materialized(&mut self, processor_id: &ModelName) {
        if let Err(error) = self.refresh_domain_routing() {
            warn!(error = %format_args!("{error:#}"), "failed to refresh routing for pending processor work");
            return;
        }
        let Some(mut processor) = self.processors.remove(processor_id) else {
            return;
        };
        processor.retry_parked(self).await;
        self.processors.insert(processor_id.clone(), processor);
    }

    pub(super) async fn retry_materialized_waiters(&mut self, updated_relay: &RelayName) {
        if let Err(error) = self.refresh_domain_routing() {
            warn!(error = %format_args!("{error:#}"), "failed to refresh routing for materialized-state waiters");
            return;
        }
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
            nervix_primitives::task::consume_budget().await;
            let Some(mut processor) = self.processors.remove(&processor_id) else {
                continue;
            };
            processor.retry_parked(self).await;
            self.processors.insert(processor_id, processor);
        }
    }

    fn rebind_input_origin(&mut self, key: &Option<BranchKey>) {
        if BranchKey::same_origin(&self.key, key) {
            return;
        }
        self.key = key.clone();
        for relay in self.relays.values_mut() {
            if relay.key.is_some() {
                relay.rebind_key(key.clone());
            }
        }
    }

    pub(super) async fn dispatch(&mut self, batch: RelayRecordBatch) {
        self.rebind_input_origin(&batch.key);
        if let Err(error) = self.refresh_domain_routing() {
            for ack in &batch.acks {
                ack.no_ack(error.to_string());
            }
            return;
        }
        let root_relay = self.root_relay.clone();
        if let Some(source_input) = &self.metrics.source_input {
            source_input.observe(
                batch.message_count(),
                batch.estimated_bytes(),
                batch.domain_timestamp(),
            );
        }
        self.metrics.source.observe(
            batch.message_count(),
            batch.estimated_bytes(),
            batch.domain_timestamp(),
        );
        self.metrics.source_dirty.mark();
        if self.dispatch_stream(&root_relay, &batch).await.is_err() {
            let error = Report::new(RouteOutputError::Forward { relay: root_relay });
            if self.owner.node.kind == ModelKind::Ingestor {
                self.runtime.handle_general_error_for_acks(
                    &self.domain,
                    self.owner.node.kind,
                    &self.owner.node.identifier,
                    &self.error_policies,
                    batch.acks.iter(),
                    &error,
                );
            } else {
                self.runtime.handle_internal_processor_error_for_acks(
                    &self.domain,
                    self.owner.node.kind,
                    &self.owner.node.identifier,
                    &self.error_policies,
                    batch.acks.iter(),
                    &error,
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
        relay: &'a RelayName,
        batch: &'a RelayRecordBatch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RelayDispatchResult> + Send + 'a>> {
        Box::pin(async move {
            let Some(runtime_stream) = self.relays.get_mut(relay) else {
                return Err(Box::new(batch.clone()));
            };
            runtime_stream.dispatch_boundary(batch).await?;
            self.materialize_stream_batch(relay, batch).await;
            self.retry_materialized_waiters(relay).await;

            Ok(())
        })
    }

    pub(super) async fn execute_processor_input(
        &mut self,
        processor_id: &ModelName,
        incoming_relay: &RelayName,
        batch: RelayRecordBatch,
    ) {
        self.rebind_input_origin(&batch.key);
        if let Err(error) = self.refresh_domain_routing() {
            for ack in &batch.acks {
                ack.no_ack(error.to_string());
            }
            return;
        }
        let Some(mut processor) = self.processors.remove(processor_id) else {
            for ack in batch.acks.iter() {
                ack.no_ack("processor is not instantiated for this branch");
            }
            return;
        };
        // One accepted unit of domain work reads its clock once. Delivery latency compares the
        // batch's domain ingestion watermarks against this same instant, so it stays inside the
        // domain's logical coordinate instead of mixing in wall time.
        let snapshot = match self.domain_clock.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.runtime.handle_internal_processor_error_for_acks(
                    &self.domain,
                    processor.kind,
                    &processor.processor,
                    &processor.error_policies,
                    batch.acks.iter(),
                    &error.change_context(RelayProcessorError::ExecutionTime),
                );
                self.processors.insert(processor_id.clone(), processor);
                return;
            }
        };
        let input_metrics =
            self.metrics.processor_inputs.get(processor_id).verified(
                "the branch template resolves metrics for every processor before spawning",
            );
        let input_metrics = input_metrics.get(incoming_relay).verified(
            "the branch template resolves every declared processor input before spawning",
        );
        input_metrics.observe_delivery(&batch.delivery_observation(snapshot.now()));
        self.metrics
            .processor_dirty
            .get(processor_id)
            .verified("the branch binds a mark for each processor")
            .mark();
        processor
            .accept_input(self, incoming_relay, batch, &snapshot)
            .await;
        self.processors.insert(processor_id.clone(), processor);
    }

    pub(super) async fn flush_processor_collected_inputs(&mut self, processor_id: &ModelName) {
        let Some(mut processor) = self.processors.remove(processor_id) else {
            return;
        };
        processor.flush_all_collected_inputs(self).await;
        self.processors.insert(processor_id.clone(), processor);
    }

    pub(super) async fn dispatch_output(
        &mut self,
        output: &RelayProcessorOutputNode,
        _source_kind: ModelKind,
        source: &ModelName,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let output_metrics = self.metrics.processor_outputs.get(source).verified(
            "the branch template resolves output metrics for every processor before spawning",
        );
        let output_metrics = output_metrics.get(&output.relay).verified(
            "the branch template resolves every declared processor output before spawning",
        );
        output_metrics.observe(
            batch.message_count(),
            batch.estimated_bytes(),
            batch.domain_timestamp(),
        );
        self.metrics
            .processor_dirty
            .get(source)
            .verified("the branch binds a mark for each processor")
            .mark();
        self.dispatch_stream(&output.relay, batch).await
    }

    pub(super) async fn tick(&mut self, snapshot: &DomainExecutionSnapshot) {
        if let Err(error) = self.refresh_domain_routing() {
            warn!(error = %format_args!("{error:#}"), "failed to refresh routing for processor tick");
            return;
        }
        let processor_ids = self.processors.keys().cloned().collect::<Vec<_>>();
        for processor_id in processor_ids {
            let Some(mut processor) = self.processors.remove(&processor_id) else {
                continue;
            };
            processor.tick(self, snapshot).await;
            self.processors.insert(processor_id, processor);
        }
    }

    pub(super) async fn force_flush(&mut self, snapshot: &DomainExecutionSnapshot) {
        if let Err(error) = self.refresh_domain_routing() {
            warn!(error = %format_args!("{error:#}"), "failed to refresh routing for processor flush");
            return;
        }
        let now = snapshot.now();
        let routing =
            self.routing_snapshot.as_ref().cloned().verified(
                "the routing refresh above returned unless it installed a current snapshot",
            );
        let processor_ids = self.processors.keys().cloned().collect::<Vec<_>>();
        for processor_id in processor_ids {
            nervix_primitives::task::consume_budget().await;
            let Some(mut processor) = self.processors.remove(&processor_id) else {
                continue;
            };
            processor.flush_all_collected_inputs(self).await;
            processor.flush_guest_buffers(self, now).await;
            processor.refresh(&routing);
            processor.flush_route_buffers(self, now).await;
            processor.tick(self, snapshot).await;
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
    /// Routes a failure of the route this task buffers through the entrypoint's own error policy.
    pub(super) fn handle_general_error<C: error_stack::Context>(
        &self,
        acks: &[AckSet],
        error: &Report<C>,
    ) {
        if self.template.branch.owner.node.kind == ModelKind::Ingestor {
            self.runtime_handle.handle_general_error_for_acks(
                &self.domain,
                self.template.branch.owner.node.kind,
                &self.ingestor,
                &self.template.branch.error_policies,
                acks.iter(),
                error,
            );
        } else {
            self.runtime_handle
                .handle_internal_processor_error_for_acks(
                    &self.domain,
                    self.template.branch.owner.node.kind,
                    &self.ingestor,
                    &self.template.branch.error_policies,
                    acks.iter(),
                    error,
                );
        }
    }

    pub(super) async fn prepare_input(
        &self,
        input: BranchedEntrypointInput,
    ) -> Vec<RelayRecordBatch> {
        let branches = match prepare_branched_entrypoint_input(
            self.runtime_handle.executor(),
            input,
            self.template.ack_boundary,
        )
        .await
        {
            Ok(branches) => branches,
            Err(failure) => {
                self.handle_general_error(
                    &failure.preserved,
                    &failure
                        .error
                        .change_context(BranchEntrypointError::PrepareRouteInput),
                );
                return Vec::new();
            }
        };
        let mut prepared = Vec::with_capacity(branches.len());
        for branch in branches {
            nervix_primitives::task::consume_budget().await;
            match branch {
                Ok(batch) => prepared.push(batch),
                Err(failure) => self.handle_general_error(
                    &failure.preserved,
                    &failure
                        .error
                        .change_context(BranchEntrypointError::PrepareBranchBatch),
                ),
            }
        }
        prepared
    }

    pub(super) async fn flush_key(&mut self, key: &Option<BranchKey>) {
        let Some(pending) = self.pending.remove(key) else {
            return;
        };
        self.quiesce.remove_batches(pending.batches.len());
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
                    &error.change_context(RouteOutputError::Concatenate {
                        relay: self.template.branch.root_relay.clone(),
                    }),
                );
                return;
            }
        };
        if let Err(error) = self.branch_sender.send(batch).await {
            let batch = error.0;
            self.handle_general_error(
                &batch.acks,
                &Report::new(RouteOutputError::Forward {
                    relay: self.template.branch.root_relay.clone(),
                }),
            );
        }
    }

    pub(super) async fn accept(
        &mut self,
        input: BranchedEntrypointInput,
        domain_clock: &DomainClock,
    ) {
        for batch in self.prepare_input(input).await {
            nervix_primitives::task::consume_budget().await;
            let key = batch.key.clone();
            let estimated_bytes = batch.estimated_bytes();
            if !self.pending.contains_key(&key) {
                #[cfg(feature = "testing")]
                self.runtime_handle
                    .fault_injection()
                    .pause_ingestor_branch_buffer_if_armed(
                        &self.domain,
                        &self.template.branch.owner.node,
                        key.as_ref().map(BranchKey::fingerprint),
                    )
                    .await;
                let snapshot = match domain_clock.snapshot() {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        self.handle_general_error(
                            &batch.acks,
                            &error.change_context(RouteOutputError::BufferClock),
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
                        &error.change_context(RouteOutputError::StartFlushDeadline {
                            relay: self.template.branch.root_relay.clone(),
                        }),
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
            let buffered_bytes = pending.estimated_bytes;
            self.quiesce.add_batch();
            if self
                .template
                .flush_policy
                .size_boundary_reached(buffered_bytes)
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
            nervix_primitives::task::consume_budget().await;
            self.flush_key(&key).await;
        }
        Ok(())
    }

    pub(super) async fn flush_all(&mut self) {
        let keys = self.pending.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            nervix_primitives::task::consume_budget().await;
            self.flush_key(&key).await;
        }
    }

    /// Releases every branch buffer for one force-flush generation.
    ///
    /// A generation covers the route input this node already holds, so the messages that are
    /// ready on the channel are accepted into their branch buffers before those buffers are
    /// released. The cut is the count the channel holds when the generation arrives: input that
    /// lands after it belongs to the next generation, so a producing source cannot extend one
    /// flush indefinitely.
    async fn force_flush(
        &mut self,
        input: &mut mpsc::Receiver<BranchedEntrypointInput>,
        domain_clock: &DomainClock,
    ) {
        let ready = input.len();
        for _ in 0..ready {
            nervix_primitives::task::consume_budget().await;
            let Ok(message) = input.try_recv() else {
                break;
            };
            self.accept(message, domain_clock).await;
        }
        self.flush_all().await;
    }

    /// Accepts everything the owner already handed this route, then releases every buffer.
    ///
    /// Closing the channel first is what bounds the drain. A source that publishes afterwards
    /// sees a closed route and reports the failure through its own error policy, rather than
    /// adding to a buffer that nothing will publish.
    async fn drain_and_flush(
        &mut self,
        input: &mut mpsc::Receiver<BranchedEntrypointInput>,
        domain_clock: &DomainClock,
    ) {
        input.close();
        while let Some(message) = input.recv().await {
            nervix_primitives::task::consume_budget().await;
            self.accept(message, domain_clock).await;
        }
        self.flush_all().await;
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
        mut force_flush: DomainForceFlushParticipant,
    ) {
        let domain_clock = match self.runtime_handle.bind_domain_clock(&self.domain) {
            Ok(clock) => clock,
            Err(error) => {
                self.runtime_handle.events().report_error(format!(
                    "{} '{}' in domain '{}' could not bind its route flush clock: {error:#}",
                    self.template.branch.owner.node.kind.as_str(),
                    self.ingestor.as_str(),
                    self.domain.as_str(),
                ));
                return;
            }
        };
        let ownership_entity = DomainNodeRef::node_in(
            self.domain.clone(),
            self.template.branch.owner.node.kind,
            ModelName::from(&self.template.branch.owner.node.identifier),
        );
        let ownership_freeze = OwnershipHandoffFreezeWatch::for_unit(
            &self.runtime_handle,
            DomainNodeUnitRef::new(ownership_entity, self.template.branch.owner.unit),
        );
        loop {
            nervix_primitives::task::consume_budget().await;
            self.runtime_handle
                .inner
                .fault_injection
                .pause_ingestor_route_poll_if_armed(
                    &self.domain,
                    self.template.branch.owner.node.kind,
                    &RelayName::from(&self.template.branch.owner.node.identifier),
                )
                .await;
            let freeze = ownership_freeze.observe();
            let ownership_frozen = freeze.is_frozen();
            let flush_deadlines = self.flush_deadlines();
            let has_flush_deadlines = !flush_deadlines.is_empty();
            nervix_primitives::select! {
                biased;
                // A signalled stop and a dropped sender both mean the owner is gone, and this
                // arm drains and finishes either way, so the outcome carries nothing to read.
                _ = shutdown_rx.changed() => {
                    self.drain_and_flush(&mut input, &domain_clock).await;
                    break;
                }
                completion = force_flush.changed(), if !ownership_frozen => {
                    let Ok(completion) = completion else {
                        // The domain's coordinator is gone, so this domain is being torn down and
                        // no later generation can arrive. Release everything this route holds
                        // rather than leaving it for a generation that cannot be requested.
                        self.drain_and_flush(&mut input, &domain_clock).await;
                        break;
                    };
                    self.force_flush(&mut input, &domain_clock).await;
                    completion.complete();
                }
                // A frozen entity keeps its obligation outstanding rather than publishing into a
                // branch runtime whose state is being captured. Waking on the freeze change is
                // what re-enables the arm above.
                _ = freeze.changed(), if ownership_frozen => {}
                result = wait_for_branch_buffer_deadlines(&domain_clock, flush_deadlines),
                    if has_flush_deadlines =>
                {
                    if let Err(error) = result {
                        let acks = self.pending_acks();
                        self.handle_general_error(
                            &acks,
                            &error.change_context(RouteOutputError::WaitFlushDeadline),
                        );
                        self.flush_all().await;
                        break;
                    }
                    if let Err(error) = self.flush_due(&domain_clock).await {
                        let acks = self.pending_acks();
                        let relay = self.template.branch.root_relay.clone();
                        self.handle_general_error(
                            &acks,
                            &error.change_context(RouteOutputError::InspectFlushDeadline { relay }),
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
        template: IngestorRouteTemplate,
        expiration_scan_interval: Duration,
    ) -> Arc<Self> {
        let branch_runtime = BranchExecutionRuntime::new(
            runtime_handle.clone(),
            domain.clone(),
            ingestor.clone(),
            template.branch.clone(),
            expiration_scan_interval,
        );
        let (sender, input) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let runtime = Arc::new(Self {
            sender,
            shutdown,
            task: nervix_primitives::sync::blocking::Mutex::new(None),
            branch_runtime: branch_runtime.clone(),
        });
        // The obligation is registered before the task starts, so a generation requested between
        // the spawn and the first poll is still owed by this route rather than missed.
        let quiesce = OutputBufferQuiesceGauge::new(
            runtime_handle.unit_quiesce_counters(&domain, template.branch.owner.clone()),
        );
        let force_flush = runtime_handle.force_flush_participant(&domain, quiesce.counters());
        let task = nervix_primitives::task::spawn(
            IngestorRouteTask {
                runtime_handle,
                domain,
                ingestor,
                template,
                branch_sender: branch_runtime.sender(),
                pending: HashMap::default(),
                quiesce,
            }
            .run(input, shutdown_rx, force_flush),
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
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the branch lane serializes its own state while consuming admitted input",
            key = "one concrete branch instance",
            bound = "one admitted batch, expiry, eviction or tick is processed by its retained \
                     branch owner"
        )
    )]
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
            template,
            domain_clock,
        } = context;
        if inputs.is_empty() {
            return;
        }

        // Branch activity is the domain time at which these inputs were accepted. The supervisor
        // reads it here, after the wait that delivered them, so an idle supervisor never records
        // the activity of a live branch at the instant it started waiting.
        let accepted_at = match domain_clock.snapshot() {
            Ok(snapshot) => snapshot.now(),
            Err(error) => {
                Self::report_dispatch_error(
                    runtime_handle,
                    domain,
                    ingestor,
                    template,
                    inputs.iter().flat_map(|message| message.acks.iter()),
                    &error.change_context(BranchEntrypointError::AcceptedInputClock),
                );
                return;
            }
        };

        for message in inputs {
            nervix_primitives::task::consume_budget().await;
            let key = message.key.clone();
            if (key
                .as_ref()
                .is_some_and(|key| key.external_partition().is_some())
                || template
                    .partition_transition
                    .as_ref()
                    .is_some_and(|transition| transition.needs_key_lookup()))
                && instances
                    .stored_key(&key)
                    .is_some_and(|stored| !BranchKey::same_origin(stored, &key))
            {
                instances.replace_key(key.clone());
            }
            let instance = if let Some(state) = instances.touch(&key, accepted_at) {
                GetOrCreateBranchInstance {
                    state,
                    created: false,
                }
            } else {
                let incarnation = instances.next_incarnation();
                let state = match nervix_primitives::expect_lint!(
                    nervix::lifecycle_call,
                    "the missing concrete branch is installed once under its branch key and \
                     incarnation before batch dispatch",
                    template.instantiate(runtime_handle, domain, key.clone(), incarnation)
                )
                .await
                {
                    Ok(state) => state,
                    Err(error) => {
                        Self::report_dispatch_error(
                            runtime_handle,
                            domain,
                            ingestor,
                            template,
                            message.acks.iter(),
                            &error.change_context(BranchEntrypointError::Instantiate {
                                branch: BranchScope::from(&key),
                            }),
                        );
                        continue;
                    }
                };
                let state = instances.insert_changed(key.clone(), accepted_at, state);
                GetOrCreateBranchInstance {
                    state,
                    created: true,
                }
            };
            if instance.created {
                nervix_primitives::expect_lint!(
                    nervix::lifecycle_call,
                    "the concrete branch creation transition registers its one metric identity",
                    runtime_handle.observe_branch_instance_created(
                        domain,
                        template.branch.as_ref(),
                        &key,
                    )
                );
                debug!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    scope = %BranchScope::from(&key),
                    "created branch runtime"
                );
            }
            if let Some(max_instances) = template.branch_max_instances {
                let evicted = evict_branch_instance_instances_to_capacity(
                    runtime_handle,
                    domain,
                    ingestor,
                    template,
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
                lanes.queue(key, message);
                continue;
            }
            let state = instance.state.clone();
            let dispatch_key = key.clone();
            let dispatch_acks = message.acks.clone();
            let (started, started_rx) = oneshot::channel();
            let handle = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
                let mut branch = state.lock().await;
                started
                    .send(())
                    .means_shutdown("branch lifecycle dispatch scheduler");
                branch.dispatch(message).await;
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
            template,
            domain_clock,
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
                template,
                domain_clock,
            },
            instances,
            vec![next.batch],
            lanes,
        )
        .await;
    }

    /// Routes one dispatch failure through the entrypoint's own error policy.
    ///
    /// Ingestors own a general error policy for their own intake, while every other branch
    /// entrypoint reports through the internal processor policy.
    fn report_dispatch_error<'a, C: error_stack::Context>(
        runtime_handle: &Runtime,
        domain: &DomainName,
        ingestor: &IngestorName,
        template: &BranchInstanceTemplate,
        acks: impl IntoIterator<Item = &'a AckSet>,
        error: &Report<C>,
    ) {
        if template.owner.node.kind == ModelKind::Ingestor {
            runtime_handle.handle_general_error_for_acks(
                domain,
                template.owner.node.kind,
                ingestor,
                &template.error_policies,
                acks,
                error,
            );
        } else {
            runtime_handle.handle_internal_processor_error_for_acks(
                domain,
                template.owner.node.kind,
                ingestor,
                &template.error_policies,
                acks,
                error,
            );
        }
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
                    template.owner.node.kind,
                    ingestor,
                    &template.error_policies,
                    completion.acks.iter(),
                    &Report::new(error).change_context(BranchEntrypointError::DispatchTask {
                        branch: BranchScope::from(&completion.key),
                    }),
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
            template,
            domain_clock,
        } = context;
        let mut lanes = BranchDispatchLanes::default();
        Self::enqueue_prepared_inputs(
            BranchExecutionDispatchContext {
                runtime_handle,
                domain,
                ingestor,
                template,
                domain_clock,
            },
            instances,
            inputs,
            &mut lanes,
        )
        .await;
        let mut next_deadline = None;
        while let Some(completion) = lanes.pending.next().await {
            nervix_primitives::task::consume_budget().await;
            Self::finish_dispatch(
                BranchExecutionDispatchContext {
                    runtime_handle,
                    domain,
                    ingestor,
                    template,
                    domain_clock,
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

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "install or terminate the one branch lifecycle task and its join handle"
        )
    )]
    pub(super) fn new(
        runtime_handle: Runtime,
        domain: DomainName,
        ingestor: IngestorName,
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
            task: nervix_primitives::sync::blocking::Mutex::new(None),
        });
        runtime_handle.register_branch_lifecycle_metrics(&domain, template.branch.as_ref());

        #[cfg_attr(
            nervix_lint,
            nervix::context(
                recurring,
                reason = "the branch lifecycle task dispatches each admitted batch and consumes \
                          maintenance polls"
            )
        )]
        let task = nervix_primitives::task::spawn(async move {
            let domain_clock = match nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "this task or concrete branch transition attaches its retained domain clock \
                 before executing in the selected lifetime",
                runtime_handle.bind_domain_clock(&domain)
            ) {
                Ok(clock) => clock,
                Err(error) => {
                    runtime_handle.events().report_error(format!(
                        "branch runtime for ingestor '{}' in domain '{}' could not bind its \
                         clock: {error:#}",
                        ingestor.as_str(),
                        domain.as_str(),
                    ));
                    return;
                }
            };
            let mut instances =
                BranchInstanceRegistry::<Option<BranchKey>, Mutex<BranchRuntime>>::new();
            let mut last_persisted_lru_lsm = match nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "task startup restores one branch lifecycle generation before accepting batches",
                restore_branch_instance_lru_snapshot(
                    &runtime_handle,
                    &domain,
                    &template,
                    &mut instances,
                )
            )
            .await
            {
                Ok(lsm) => lsm,
                Err(error) => {
                    warn!(
                        domain = domain.as_str(),
                        ingestor = ingestor.as_str(),
                        error = %format_args!("{error:#}"),
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
                        &template,
                        max_instances,
                        &mut instances,
                    )
                    .await,
                );
            }
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            let mut next_lru_snapshot = Instant::now() + runtime_handle.state_snapshot_interval();
            let restored_snapshot = match domain_clock.snapshot() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    runtime_handle.events().report_error(format!(
                        "branch runtime for ingestor '{}' in domain '{}' lost its clock: {error:#}",
                        ingestor.as_str(),
                        domain.as_str(),
                    ));
                    return;
                }
            };
            let mut next_branch_deadline =
                tick_due_branch_instance_branches(&restored_snapshot, &instances).await;
            let ownership_entity = DomainNodeRef::node_in(
                domain.clone(),
                template.owner.node.kind,
                ModelName::from(&template.owner.node.identifier),
            );
            let mut checkpoint_requests_open = true;
            let mut lanes = BranchDispatchLanes::default();

            let ownership_freeze = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "this task binds its exact ownership freeze publication before entering its \
                 recurring loop",
                OwnershipHandoffFreezeWatch::for_unit(
                    &runtime_handle,
                    DomainNodeUnitRef::new(ownership_entity, template.owner.unit)
                )
            );
            loop {
                nervix_primitives::task::consume_budget().await;
                let freeze = ownership_freeze.observe();
                let ownership_frozen = freeze.is_frozen();
                let snapshot = match domain_clock.snapshot() {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        runtime_handle.events().report_error(format!(
                            "branch runtime for ingestor '{}' in domain '{}' lost its clock: \
                             {error:#}",
                            ingestor.as_str(),
                            domain.as_str(),
                        ));
                        break;
                    }
                };
                let now = snapshot.now();
                let mut did_scheduled_work = false;
                if !ownership_frozen && Instant::now() >= next_expiration_scan {
                    if let Some(branch_ttl) = template.branch_ttl {
                        let expired = expire_branch_instance_instances(
                            &runtime_handle,
                            &domain,
                            &ingestor,
                            &template,
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
                    if let Err(error) = nervix_primitives::expect_lint!(
                        nervix::lifecycle_call,
                        "explicit checkpoint cadence or terminal teardown captures one branch \
                         lifecycle generation; the record path does not invoke it",
                        persist_branch_instance_lru_snapshot(
                            &runtime_handle,
                            &domain,
                            &template,
                            &instances,
                            &mut last_persisted_lru_lsm,
                        )
                    ) {
                        warn!(
                            domain = domain.as_str(),
                            ingestor = ingestor.as_str(),
                            error = %format_args!("{error:#}"),
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
                        tick_due_branch_instance_branches(&snapshot, &instances).await;
                    did_scheduled_work = true;
                }
                if did_scheduled_work {
                    continue;
                }

                // Maintenance wakeups are physical, so their interval is a plain monotonic sleep.
                // The branch's own deadline is logical and is awaited on the domain clock below.
                let maintenance_sleep = if ownership_frozen {
                    OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL
                } else {
                    next_expiration_scan
                        .checked_duration_since(Instant::now())
                        .unwrap_or(Duration::ZERO)
                        .min(
                            next_lru_snapshot
                                .checked_duration_since(Instant::now())
                                .unwrap_or(Duration::ZERO),
                        )
                };
                let awaited_branch_deadline = if ownership_frozen {
                    None
                } else {
                    next_branch_deadline.map(|deadline| domain_clock.deadline_at(deadline))
                };
                nervix_primitives::select! {
                    biased;
                    checkpoint = checkpoint_requests.recv(), if checkpoint_requests_open && lanes.is_empty() => {
                        let Some(checkpoint) = checkpoint else {
                            checkpoint_requests_open = false;
                            continue;
                        };
                        let result = nervix_primitives::expect_lint!(nervix::lifecycle_call, "the admitted checkpoint command captures one branch lifecycle generation", checkpoint_branch_instance_lru_snapshot(
                            &runtime_handle,
                            &domain,
                            &template,
                            &instances,
                        ));
                        if let Ok(snapshot) = &result {
                            last_persisted_lru_lsm = snapshot.lsm;
                        }
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
                                template: &template,
                                domain_clock: &domain_clock,
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
                                nervix_primitives::task::consume_budget().await;
                                Self::finish_dispatch(
                                    BranchExecutionDispatchContext {
                                        runtime_handle: &runtime_handle,
                                        domain: &domain,
                                        ingestor: &ingestor,
                                        template: &template,
                                        domain_clock: &domain_clock,
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
                                template: &template,
                                domain_clock: &domain_clock,
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
                                nervix_primitives::task::consume_budget().await;
                                Self::enqueue_prepared_inputs(
                                    BranchExecutionDispatchContext {
                                        runtime_handle: &runtime_handle,
                                        domain: &domain,
                                        ingestor: &ingestor,
                                        template: &template,
                                        domain_clock: &domain_clock,
                                    },
                                    &mut instances,
                                    vec![message],
                                    &mut lanes,
                                )
                                .await;
                            }
                            while let Some(completion) = lanes.pending.next().await {
                                nervix_primitives::task::consume_budget().await;
                                Self::finish_dispatch(
                                    BranchExecutionDispatchContext {
                                        runtime_handle: &runtime_handle,
                                        domain: &domain,
                                        ingestor: &ingestor,
                                        template: &template,
                                        domain_clock: &domain_clock,
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
                    _ = freeze.changed(), if ownership_frozen => {}
                    result = wait_for_branch_deadline(
                        &domain_clock,
                        awaited_branch_deadline.clone(),
                    ), if awaited_branch_deadline.is_some() => {
                        if let Err(error) = result {
                            runtime_handle.events().report_error(format!(
                                "branch runtime for ingestor '{}' in domain '{}' could not wait \
                                 for a branch deadline: {error:#}",
                                ingestor.as_str(),
                                domain.as_str(),
                            ));
                            break;
                        }
                    }
                    _ = sleep(maintenance_sleep) => {}
                }
            }

            if let Err(error) = nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "explicit checkpoint cadence or terminal teardown captures one branch lifecycle \
                 generation; the record path does not invoke it",
                persist_branch_instance_lru_snapshot(
                    &runtime_handle,
                    &domain,
                    &template,
                    &instances,
                    &mut last_persisted_lru_lsm,
                )
            ) {
                warn!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    error = %format_args!("{error:#}"),
                    "failed to persist final branch lru snapshot"
                );
            }
            nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "terminal source teardown ends the exact retained concrete branch lifetimes",
                shutdown_all_branch_instance_instances(
                    &runtime_handle,
                    &domain,
                    &ingestor,
                    template.branch.as_ref(),
                    &mut instances,
                )
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

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "install or terminate the one branch lifecycle task and its join handle"
        )
    )]
    pub(super) async fn shutdown(&self) {
        const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

        self.shutdown.send_replace(true);
        let Some(mut task) = self.task.lock().take() else {
            return;
        };

        match nervix_primitives::time::timeout(SHUTDOWN_GRACE_PERIOD, &mut task).await {
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

#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "the branch lane serializes its own state while consuming admitted input",
        key = "one concrete branch instance",
        bound = "one admitted batch, expiry, eviction or tick is processed by its retained branch \
                 owner"
    )
)]
pub(super) async fn expire_branch_instance_instances(
    runtime: &Runtime,
    domain: &DomainName,
    ingestor: &IngestorName,
    template: &BranchInstanceTemplate,
    now: Timestamp,
    expiration_after: Duration,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) -> Vec<Option<BranchKey>> {
    let mut expired_keys = Vec::new();
    for (key, state) in instances.expire(now, expiration_after) {
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "the concrete branch ending or detach transition withdraws its exact metric identity",
            runtime.observe_branch_instance_removed(
                domain,
                template.branch.as_ref(),
                &key,
                Some(BranchEvictionReason::Ttl),
            )
        );
        template.retire_branch_channels(&key);
        let mut branch = state.lock().await;
        branch.evict().await;
        debug!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            scope = %BranchScope::from(&key),
            "expired branched processor root"
        );
        expired_keys.push(key);
    }
    expired_keys
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "the branch lane serializes its own state while consuming admitted input",
        key = "one concrete branch instance",
        bound = "one admitted batch, expiry, eviction or tick is processed by its retained branch \
                 owner"
    )
)]
pub(super) async fn evict_branch_instance_instances_to_capacity(
    runtime: &Runtime,
    domain: &DomainName,
    ingestor: &IngestorName,
    template: &BranchInstanceTemplate,
    max_instances: NonZeroUsize,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) -> Vec<Option<BranchKey>> {
    let mut evicted_keys = Vec::new();
    for (key, state) in instances.evict_lru_to_capacity(max_instances) {
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "the concrete branch ending or detach transition withdraws its exact metric identity",
            runtime.observe_branch_instance_removed(
                domain,
                template.branch.as_ref(),
                &key,
                Some(BranchEvictionReason::Lru),
            )
        );
        template.retire_branch_channels(&key);
        let mut branch = state.lock().await;
        branch.evict().await;
        debug!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            scope = %BranchScope::from(&key),
            max_instances,
            "evicted branch runtime by lru"
        );
        evicted_keys.push(key);
    }
    evicted_keys
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "this operation installs, snapshots or retires retained execution state at an \
                  explicit lifetime boundary"
    )
)]
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
            scope = %BranchScope::from(&key),
            "stopped branch runtime"
        );
    }
}

pub(super) fn branch_lru_placement(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
) -> error_stack::Result<RuntimeStatePlacement, StateIdentityError> {
    runtime.state_placement_for_unit(
        domain,
        RuntimeStateKind::BranchLru,
        template.owner.node.kind,
        &template.owner.node.identifier,
        template.owner.unit,
        None,
    )
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "this operation installs, snapshots or retires retained execution state at an \
                  explicit lifetime boundary"
    )
)]
pub(super) async fn restore_branch_instance_lru_snapshot(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) -> error_stack::Result<u64, BranchLruSnapshotError> {
    /// One branch the lifecycle names, built and not installed yet.
    struct RestoredBranch {
        lifetime: BranchInstanceSnapshotEntry<Option<BranchKey>>,
        state: Mutex<BranchRuntime>,
    }

    let placement = branch_lru_placement(runtime, domain, template)
        .change_context(BranchLruSnapshotError::Unplaced)?;
    let lifecycle = runtime
        .restorable_branch_lru_snapshot(&placement)
        .change_context(BranchLruSnapshotError::Read)?;
    let Some(lifecycle) = lifecycle else {
        return Ok(0);
    };
    // Every branch is built before any is installed, and the lifecycle is released only once all
    // of them are, so a failed restore leaves both as they were.
    let lifetimes = lifecycle.branches()?;
    let mut restored = Vec::with_capacity(lifetimes.len());
    for (entry, lifetime) in lifetimes.into_iter().enumerate() {
        nervix_primitives::task::consume_budget().await;
        let state = template
            .instantiate(runtime, domain, lifetime.key.clone(), lifetime.incarnation)
            .await
            .change_context(BranchLruSnapshotError::Restore { entry })?;
        restored.push(RestoredBranch { lifetime, state });
    }
    for branch in restored {
        let lifetime = branch.lifetime;
        runtime.observe_branch_instance_created(domain, template.branch.as_ref(), &lifetime.key);
        instances.insert_restored(
            lifetime.key,
            lifetime.last_ingestion,
            lifetime.incarnation,
            branch.state,
        );
    }
    let lsm = lifecycle.lsm();
    instances.set_version(lsm);
    runtime.release_restored_branch_lru_snapshot(lifecycle);
    Ok(lsm)
}

/// Persist the branch lifecycle checkpoint `instances` form now and hand it to the ownership
/// handoff that asked for it.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "this operation installs, snapshots or retires retained execution state at an \
                  explicit lifetime boundary"
    )
)]
fn checkpoint_branch_instance_lru_snapshot<V>(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
    instances: &BranchInstanceRegistry<Option<BranchKey>, V>,
) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
    let placement = branch_lru_placement(runtime, domain, template).change_context_lazy(|| {
        OwnershipHandoffError::StatePlacement {
            kind: template.owner.node.kind,
            identifier: ModelName::from(&template.owner.node.identifier),
        }
    })?;
    let _publication = runtime.backup_publication(domain);
    let payload = encode_branch_lru_snapshot(&instances.snapshot_entries())
        .map_err(|error| OwnershipHandoffError::checkpoint(error.to_string()))?;
    let snapshot = PersistedRuntimeStateEntry {
        lsm: instances.version(),
        payload,
    };
    runtime
        .persist_branch_lru_snapshot(placement, snapshot.clone())
        .change_context(OwnershipHandoffError::Persistence)?;
    Ok(snapshot)
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "this operation installs, snapshots or retires retained execution state at an \
                  explicit lifetime boundary"
    )
)]
pub(super) fn persist_branch_instance_lru_snapshot<V>(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
    instances: &BranchInstanceRegistry<Option<BranchKey>, V>,
    last_persisted_lsm: &mut u64,
) -> error_stack::Result<(), BranchLruSnapshotError> {
    let lsm = instances.version();
    if lsm <= *last_persisted_lsm {
        return Ok(());
    }
    let placement = branch_lru_placement(runtime, domain, template)
        .change_context(BranchLruSnapshotError::Unplaced)?;
    let _publication = runtime.backup_publication(domain);
    let payload = encode_branch_lru_snapshot(&instances.snapshot_entries())?;
    runtime
        .persist_branch_lru_snapshot(
            placement.clone(),
            PersistedRuntimeStateEntry { lsm, payload },
        )
        .change_context(BranchLruSnapshotError::Persist { lsm })?;
    *last_persisted_lsm = lsm;
    Ok(())
}

/// Offer the branch lifecycle of `instances` to the node's replicas at once, without writing it to
/// storage: the periodic lifecycle snapshot persists it.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a branch that appears offers the entity's lifecycle to its replicas once, as \
                  one branch lifetime begins"
    )
)]
pub(super) fn publish_branch_instance_lru_snapshot<V>(
    runtime: &Runtime,
    domain: &DomainName,
    template: &BranchInstanceTemplate,
    instances: &BranchInstanceRegistry<Option<BranchKey>, V>,
) -> error_stack::Result<(), BranchLruSnapshotError> {
    let placement = branch_lru_placement(runtime, domain, template)
        .change_context(BranchLruSnapshotError::Unplaced)?;
    let _publication = runtime.backup_publication(domain);
    let payload = encode_branch_lru_snapshot(&instances.snapshot_entries())?;
    let snapshot = PersistedRuntimeStateEntry {
        lsm: instances.version(),
        payload,
    };
    runtime.publish_branch_lru_snapshot(placement, snapshot);
    Ok(())
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "the branch lane serializes its own state while consuming admitted input",
        key = "one concrete branch instance",
        bound = "one admitted batch, expiry, eviction or tick is processed by its retained branch \
                 owner"
    )
)]
pub(super) async fn tick_due_branch_instance_branches(
    snapshot: &DomainExecutionSnapshot,
    instances: &BranchInstanceRegistry<Option<BranchKey>, Mutex<BranchRuntime>>,
) -> Option<Timestamp> {
    let mut next = None;
    for instance in instances.states() {
        let mut branch = instance.lock().await;
        if branch
            .next_deadline()
            .is_some_and(|deadline| deadline <= snapshot.now())
        {
            branch.tick(snapshot).await;
        }
        record_next_branch_instance_branch_deadline(&mut next, branch.next_deadline());
    }
    next
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "branch lanes retain the concrete branch while consuming admitted input and \
                  scheduled ticks"
    )
)]
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

pub(super) async fn flush_branch_junction(
    context: JunctionFlushContext<'_>,
    forwarded: RelayRecordBatch,
) {
    let JunctionFlushContext {
        branch,
        node_kind,
        processor,
        error_policies,
        output_routes,
        materialized_values,
        execution_now,
    } = context;
    if let Some(acks) = dispatch_processor_outputs(
        ProcessorOutputDispatchContext {
            branch,
            node_kind,
            entity_kind: ModelKind::Junction,
            processor,
            error_policies,
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "branch_runtime_tests.rs"]
mod tests;
