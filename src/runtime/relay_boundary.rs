use super::*;

pub(super) const RELAY_BUFFER_DIRECTION_CONCRETE: &str = "concrete";

/// A count NSPL configures, narrowed to the width this node addresses memory with.
///
/// Both halves of the narrowing hold before it runs: the Models keep these counts non-zero, and
/// the supported targets address memory at least as wide as the `u64` they are written as.
pub(super) fn addressable_count(configured: NonZeroU64) -> NonZeroUsize {
    NonZeroUsize::new(configured.get().arch_into())
        .assured("a non-zero configured count is still non-zero at this target's pointer width")
}

#[derive(Debug)]
pub(super) struct RelayPresence {
    pub(super) last_seen_at: parking_lot::Mutex<Timestamp>,
}

#[derive(Debug, Clone)]
pub(super) struct RelayRegistry {
    pub(super) presences: Arc<DashMap<Option<BranchKey>, Arc<RelayPresence>, RandomState>>,
}

impl RelayRegistry {
    pub(super) fn new() -> Self {
        Self {
            presences: Arc::new(DashMap::default()),
        }
    }

    pub(super) fn touch(&self, key: &Option<BranchKey>, now: Timestamp) {
        if let Some(existing) = self.presences.get(key) {
            *existing.last_seen_at.lock() = now;
            return;
        }
        self.presences.insert(
            key.clone(),
            Arc::new(RelayPresence {
                last_seen_at: parking_lot::Mutex::new(now),
            }),
        );
    }

    pub(super) fn contains_key(&self, key: &Option<BranchKey>) -> bool {
        self.presences.contains_key(key)
    }

    pub(super) fn remove(&self, key: &Option<BranchKey>) {
        self.presences.remove(key);
    }

    pub(super) fn clear(&self) {
        self.presences.clear();
    }

    pub(super) fn keys(&self) -> Vec<String> {
        let mut keys = self
            .presences
            .iter()
            .filter_map(|entry| entry.key().as_ref().map(|key| key.as_str().to_string()))
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }
}

pub(super) struct ConcreteRelayRuntime {
    pub(super) key: Option<BranchKey>,
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) relay: RelayName,
    pub(super) registry: RelayRegistry,
    pub(super) services: Arc<RelayBoundaryServices>,
}

pub(super) struct ConcreteRelayRuntimeBuild {
    pub(super) key: Option<BranchKey>,
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) relay: RelayName,
    pub(super) registry: RelayRegistry,
    pub(super) services: Arc<RelayBoundaryServices>,
}

#[derive(Debug)]
pub(super) struct RelayBoundaryServices {
    pub(super) fanout: RelayBoundaryFanout,
    pub(super) attached_runtime_consumer_count: AtomicUsize,
    pub(super) detached_runtime_consumer_count: AtomicUsize,
    pub(super) remote_runtime_consumers: ArcSwap<Vec<RemoteRuntimeConsumer>>,
    pub(super) remote_dispatcher: Option<Arc<RemoteDispatcher>>,
    pub(super) owner_node: RwLock<Option<ClusterNodeName>>,
    pub(super) ingress_slot: Mutex<()>,
    pub(super) outbound_slots: DashMap<String, Arc<Mutex<()>>, RandomState>,
}

impl std::fmt::Debug for ConcreteRelayRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcreteStreamRuntime")
            .field("domain", &self.domain)
            .field("relay", &self.relay)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(super) struct RelayBoundaryBuilder {
    pub(super) fanout: RelayBoundaryFanout,
    pub(super) attached_runtime_consumer_count: usize,
    pub(super) detached_runtime_consumer_count: usize,
    pub(super) registry: RelayRegistry,
    pub(super) remote_runtime_consumers: Vec<RemoteRuntimeConsumer>,
}

#[derive(Debug)]
pub(super) struct RelayConsumerFanout {
    pub(super) dispatch_gate: Arc<RelayDispatchGate>,
    pub(super) owner_buffer: RwLock<Option<Arc<RelayBroadcast<RelayRecordBatch>>>>,
    pub(super) owner_capacity: AtomicUsize,
    pub(super) owner_pending_batches: Arc<AtomicUsize>,
    pub(super) subscriptions: RelayBroadcast<RelayRecordBatch>,
    pub(super) attached_runtime_consumers: RelayBroadcast<RelayRecordBatch>,
    pub(super) detached_runtime_consumers: RelayBroadcast<RelayRecordBatch>,
}

pub(super) struct RelayOwnerAdmission {
    pub(super) pending_batches: Arc<AtomicUsize>,
    pub(super) accepted: bool,
}

impl RelayOwnerAdmission {
    pub(super) fn new(pending_batches: Arc<AtomicUsize>) -> Self {
        pending_batches.fetch_add(1, Ordering::AcqRel);
        Self {
            pending_batches,
            accepted: false,
        }
    }

    pub(super) fn accept(mut self) {
        self.accepted = true;
    }
}

impl Drop for RelayOwnerAdmission {
    fn drop(&mut self) {
        if !self.accepted {
            let previous = self.pending_batches.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(previous > 0, "relay owner pending batch count underflow");
        }
    }
}

pub(super) struct RelayOwnerBatchCompletion {
    pub(super) pending_batches: Arc<AtomicUsize>,
}

impl Drop for RelayOwnerBatchCompletion {
    fn drop(&mut self) {
        let previous = self.pending_batches.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "relay owner pending batch count underflow");
    }
}

#[derive(Debug)]
pub(super) struct BranchCollapseNode {
    pub(super) fanout: RelayConsumerFanout,
}

#[derive(Debug, Clone)]
pub(super) enum RelayBoundaryFanout {
    Direct(Arc<RelayConsumerFanout>),
    BranchCollapse(Arc<BranchCollapseNode>),
}

/// One destination's share of a relay fanout.
///
/// The body is a parameter rather than something this builds, because the fanout encodes it once
/// and every destination carries a handle to that one allocation. Only the target relay and the
/// acknowledgement obligations differ between destinations.
pub(super) struct RoutedDelivery<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) consumer: &'a RemoteRuntimeConsumer,
    pub(super) batch: &'a RelayRecordBatch,
    pub(super) batch_ipc: ChargedBytes,
    pub(super) acks: Vec<Option<RemoteAckRegistration>>,
}

pub(super) fn routed_payload(delivery: RoutedDelivery<'_>) -> RelayPayload {
    RelayPayload {
        kind: RelayPayloadKind::Routed,
        domain: delivery.domain.clone(),
        relay: delivery.consumer.relay.clone(),
        key: BranchKey::to_remote_key(&delivery.batch.key),
        batch_ipc: delivery.batch_ipc,
        metadata: delivery
            .batch
            .metadata
            .iter()
            .map(RuntimeRecordMetadata::to_remote)
            .collect(),
        acks: delivery.acks,
        admission: None,
    }
}

#[derive(Debug, Clone)]
pub(super) struct RemoteRuntimeConsumer {
    pub(super) node_id: ClusterNodeName,
    pub(super) relay: RelayName,
    pub(super) mode: AckMode,
}

pub(super) struct RelayOwnerTask {
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: JoinHandle<()>,
}

pub(super) struct RelayOwnerBranchState {
    pub(super) registry: RelayRegistry,
    pub(super) instances: BranchInstanceRegistry<Option<BranchKey>, ()>,
    pub(super) capacity: Option<NonZeroUsize>,
}

pub(super) struct RelayStateTask {
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: JoinHandle<()>,
}

impl RelayStateTask {
    pub(super) async fn stop(mut self, grace: Duration) -> Result<(), String> {
        self.shutdown.send_replace(true);
        match tokio::time::timeout(grace, &mut self.task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(format!("relay state task failed: {error}")),
            Err(_) => {
                self.task.abort();
                self.task.join_after_shutdown("relay state").await;
                Err(format!(
                    "relay state task did not drain within {}",
                    humantime::format_duration(grace)
                ))
            }
        }
    }
}

impl RelayOwnerTask {
    pub(super) async fn stop(mut self, grace: Duration) -> Result<(), String> {
        self.shutdown.send_replace(true);
        match tokio::time::timeout(grace, &mut self.task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(format!("relay owner task failed: {error}")),
            Err(_) => {
                self.task.abort();
                self.task.join_after_shutdown("relay owner").await;
                Err(format!(
                    "relay owner task did not drain within {}",
                    humantime::format_duration(grace)
                ))
            }
        }
    }
}

pub(super) type RelayBoundaryFanoutMap = DashMap<DomainNodeRef, RelayBoundaryFanout, RandomState>;

pub(super) type RelayRuntimeConsumerReceiver = RelaySubscriptionReceiver<RelayRecordBatch>;

pub(super) struct RelayRuntimeFanIn {
    pub(super) receiver: RelayRuntimeConsumerReceiver,
}

impl RelayConsumerFanout {
    pub(super) fn with_capacity(capacity: NonZeroUsize) -> Self {
        let dispatch_capacity = NonZeroUsize::MIN;
        Self {
            dispatch_gate: Arc::new(RelayDispatchGate::new()),
            owner_buffer: RwLock::new(None),
            owner_capacity: AtomicUsize::new(capacity.get()),
            owner_pending_batches: Arc::new(AtomicUsize::new(0)),
            subscriptions: RelayBroadcast::with_capacity(dispatch_capacity),
            attached_runtime_consumers: RelayBroadcast::with_capacity(dispatch_capacity),
            detached_runtime_consumers: RelayBroadcast::with_capacity(dispatch_capacity),
        }
    }

    pub(super) fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        self.subscriptions.new_receiver()
    }

    pub(super) fn set_capacity(&self, capacity: NonZeroUsize) {
        self.owner_capacity.store(capacity.get(), Ordering::Release);
        if let Some(buffer) = self.owner_buffer.read().as_ref() {
            buffer.set_capacity(capacity);
        }
    }

    pub(super) fn activate_owner_buffer(&self) -> RelayRuntimeConsumerReceiver {
        let capacity = NonZeroUsize::new(self.owner_capacity.load(Ordering::Acquire))
            .verified("relay capacity is validated as nonzero before it is stored");
        let buffer = Arc::new(RelayBroadcast::with_capacity(capacity));
        let receiver = buffer.new_receiver();
        *self.owner_buffer.write() = Some(buffer);
        receiver
    }

    pub(super) fn deactivate_owner_buffer(&self) {
        *self.owner_buffer.write() = None;
    }

    pub(super) fn owner_buffer(&self) -> Option<Arc<RelayBroadcast<RelayRecordBatch>>> {
        self.owner_buffer.read().clone()
    }

    pub(super) fn owner_buffer_len(&self) -> Option<(usize, usize)> {
        self.owner_buffer()
            .map(|buffer| (buffer.len(), buffer.capacity()))
    }

    pub(super) fn begin_owner_admission(&self) -> RelayOwnerAdmission {
        RelayOwnerAdmission::new(self.owner_pending_batches.clone())
    }

    pub(super) fn begin_owner_batch_completion(&self) -> RelayOwnerBatchCompletion {
        RelayOwnerBatchCompletion {
            pending_batches: self.owner_pending_batches.clone(),
        }
    }

    pub(super) fn runtime_consumer_receiver_for_mode(
        &self,
        mode: AckMode,
    ) -> RelayRuntimeConsumerReceiver {
        self.runtime_consumer_broadcast_for_mode(mode)
            .new_receiver()
    }

    pub(super) fn dispatch_gate(&self) -> Arc<RelayDispatchGate> {
        self.dispatch_gate.clone()
    }

    pub(super) fn runtime_consumer_buffer_len(&self) -> usize {
        self.attached_runtime_consumers
            .len()
            .checked_add(self.detached_runtime_consumers.len())
            .assured("both counts are lengths of collections this node holds in memory")
    }

    pub(super) fn outstanding_work_len(&self) -> usize {
        self.owner_pending_batches
            .load(Ordering::Acquire)
            .checked_add(self.runtime_consumer_buffer_len())
            .assured("both counts total batches this node already holds in memory")
    }

    pub(super) fn runtime_consumer_broadcast_for_mode(
        &self,
        mode: AckMode,
    ) -> &RelayBroadcast<RelayRecordBatch> {
        match mode {
            AckMode::Attached => &self.attached_runtime_consumers,
            AckMode::Detached => &self.detached_runtime_consumers,
        }
    }

    pub(super) async fn fanout_subscriptions(&self, batch: &RelayRecordBatch) {
        if self.subscriptions.receiver_count() == 0 {
            return;
        }
        self.subscriptions
            .broadcast(batch.detached())
            .await
            .means_peer_left("relay subscription");
    }

    pub(super) async fn dispatch_runtime_consumers(
        &self,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let attached_receiver_count = self
            .runtime_consumer_broadcast_for_mode(AckMode::Attached)
            .receiver_count();
        if attached_runtime_consumer_count > 0
            && attached_receiver_count < attached_runtime_consumer_count
        {
            for ack in batch.acks.iter() {
                ack.no_ack("runtime consumer unavailable for attached delivery");
            }
            return Err(Box::new(batch.clone()));
        }
        if attached_receiver_count > 0 {
            let attached = batch.attached_for_receivers(attached_receiver_count);
            if let Err(error) = self
                .runtime_consumer_broadcast_for_mode(AckMode::Attached)
                .broadcast(attached)
                .await
            {
                let failed = error.0;
                for ack in failed.acks.iter() {
                    ack.no_ack("runtime consumer unavailable for attached delivery");
                }
                return Err(Box::new(batch.clone()));
            }
        }

        let detached_receiver_count = self
            .runtime_consumer_broadcast_for_mode(AckMode::Detached)
            .receiver_count();
        if detached_runtime_consumer_count > 0
            && detached_receiver_count < detached_runtime_consumer_count
        {
            warn!("detached runtime consumer receiver is unavailable");
        }
        if detached_receiver_count > 0 {
            let detached = batch.detached();
            if let Err(error) = self
                .runtime_consumer_broadcast_for_mode(AckMode::Detached)
                .broadcast(detached)
                .await
            {
                warn!(
                    error = %error,
                    "detached runtime consumer relay broadcast failed"
                );
            }
        }

        Ok(())
    }
}

impl BranchCollapseNode {
    pub(super) fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            fanout: RelayConsumerFanout::with_capacity(capacity),
        }
    }

    pub(super) fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        self.fanout.subscription_receiver()
    }

    pub(super) fn set_capacity(&self, capacity: NonZeroUsize) {
        self.fanout.set_capacity(capacity);
    }

    pub(super) fn runtime_consumer_receiver_for_mode(
        &self,
        mode: AckMode,
    ) -> RelayRuntimeConsumerReceiver {
        self.fanout.runtime_consumer_receiver_for_mode(mode)
    }

    pub(super) async fn fanout_subscriptions(&self, batch: &RelayRecordBatch) {
        self.fanout.fanout_subscriptions(batch).await;
    }

    pub(super) async fn dispatch_runtime_consumers(
        &self,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout
            .dispatch_runtime_consumers(
                attached_runtime_consumer_count,
                detached_runtime_consumer_count,
                batch,
            )
            .await
    }
}

impl RelayBoundaryFanout {
    pub(super) fn direct_with_capacity(capacity: NonZeroUsize) -> Self {
        Self::Direct(Arc::new(RelayConsumerFanout::with_capacity(capacity)))
    }

    pub(super) fn branch_collapse_with_capacity(capacity: NonZeroUsize) -> Self {
        Self::BranchCollapse(Arc::new(BranchCollapseNode::with_capacity(capacity)))
    }

    pub(super) fn uses_branch_collapse(&self) -> bool {
        match self {
            Self::Direct(_) => false,
            Self::BranchCollapse(_) => true,
        }
    }

    pub(super) fn set_capacity(&self, capacity: NonZeroUsize) {
        match self {
            Self::Direct(fanout) => fanout.set_capacity(capacity),
            Self::BranchCollapse(branch_collapse) => branch_collapse.set_capacity(capacity),
        }
    }

    pub(super) fn activate_owner_buffer(&self) -> RelayRuntimeConsumerReceiver {
        match self {
            Self::Direct(fanout) => fanout.activate_owner_buffer(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.activate_owner_buffer(),
        }
    }

    pub(super) fn deactivate_owner_buffer(&self) {
        match self {
            Self::Direct(fanout) => fanout.deactivate_owner_buffer(),
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.fanout.deactivate_owner_buffer();
            }
        }
    }

    pub(super) fn owner_buffer(&self) -> Option<Arc<RelayBroadcast<RelayRecordBatch>>> {
        match self {
            Self::Direct(fanout) => fanout.owner_buffer(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.owner_buffer(),
        }
    }

    pub(super) fn owner_buffer_len(&self) -> Option<(usize, usize)> {
        match self {
            Self::Direct(fanout) => fanout.owner_buffer_len(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.owner_buffer_len(),
        }
    }

    pub(super) fn begin_owner_admission(&self) -> RelayOwnerAdmission {
        match self {
            Self::Direct(fanout) => fanout.begin_owner_admission(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.begin_owner_admission(),
        }
    }

    pub(super) fn begin_owner_batch_completion(&self) -> RelayOwnerBatchCompletion {
        match self {
            Self::Direct(fanout) => fanout.begin_owner_batch_completion(),
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.fanout.begin_owner_batch_completion()
            }
        }
    }

    pub(super) fn dispatch_gate(&self) -> Arc<RelayDispatchGate> {
        match self {
            Self::Direct(fanout) => fanout.dispatch_gate(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.dispatch_gate(),
        }
    }

    pub(super) fn outstanding_work_len(&self) -> usize {
        match self {
            Self::Direct(fanout) => fanout.outstanding_work_len(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.outstanding_work_len(),
        }
    }

    pub(super) fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        match self {
            Self::Direct(fanout) => fanout.subscription_receiver(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.subscription_receiver(),
        }
    }

    pub(super) fn runtime_consumer_receiver_for_mode(
        &self,
        mode: AckMode,
    ) -> RelayRuntimeConsumerReceiver {
        match self {
            Self::Direct(fanout) => fanout.runtime_consumer_receiver_for_mode(mode),
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.runtime_consumer_receiver_for_mode(mode)
            }
        }
    }

    pub(super) async fn fanout_subscriptions(&self, batch: &RelayRecordBatch) {
        match self {
            Self::Direct(fanout) => fanout.fanout_subscriptions(batch).await,
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.fanout_subscriptions(batch).await;
            }
        }
    }

    pub(super) async fn dispatch_runtime_consumers(
        &self,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        match self {
            Self::Direct(fanout) => {
                fanout
                    .dispatch_runtime_consumers(
                        attached_runtime_consumer_count,
                        detached_runtime_consumer_count,
                        batch,
                    )
                    .await
            }
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse
                    .dispatch_runtime_consumers(
                        attached_runtime_consumer_count,
                        detached_runtime_consumer_count,
                        batch,
                    )
                    .await
            }
        }
    }
}

impl RelayRuntimeFanIn {
    pub(super) fn new(receiver: RelayRuntimeConsumerReceiver) -> Self {
        Self { receiver }
    }

    pub(super) async fn recv(&mut self) -> Option<RelayRecordBatch> {
        tokio::task::consume_budget().await;
        match self.receiver.recv().await {
            Ok(batch) => Some(batch),
            Err(async_broadcast::RecvError::Overflowed(_)) => {
                unreachable!("relay broadcasts are backpressured and must not overflow")
            }
            Err(async_broadcast::RecvError::Closed) => None,
        }
    }

    pub(super) fn try_recv(&mut self) -> Result<RelayRecordBatch, async_broadcast::TryRecvError> {
        self.receiver.try_recv()
    }

    pub(super) fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<RelayRecordBatch>> {
        match self.receiver.poll_recv(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => std::task::Poll::Ready(Some(batch)),
            std::task::Poll::Ready(Some(Err(async_broadcast::RecvError::Overflowed(_)))) => {
                unreachable!("relay broadcasts are backpressured and must not overflow")
            }
            std::task::Poll::Ready(Some(Err(async_broadcast::RecvError::Closed)) | None) => {
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    pub(super) fn pending_len(&self) -> usize {
        self.receiver.len()
    }

    /// Polls one relay batch while keeping quiesce accounting continuous across dequeue.
    pub(super) fn poll_recv_with_quiesce(
        &mut self,
        cx: &mut std::task::Context<'_>,
        counters: Option<&Arc<NodeQuiesceCounters>>,
    ) -> std::task::Poll<Option<(RelayRecordBatch, Option<NodeQuiesceWorkGuard>)>> {
        let work = counters.map(|counters| NodeQuiesceWorkGuard::begin(counters.clone()));
        match self.poll_recv(cx) {
            std::task::Poll::Ready(Some(batch)) => std::task::Poll::Ready(Some((batch, work))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    /// Tries one relay batch while keeping quiesce accounting continuous across dequeue.
    pub(super) fn try_recv_with_quiesce(
        &mut self,
        counters: Option<&Arc<NodeQuiesceCounters>>,
    ) -> Result<(RelayRecordBatch, Option<NodeQuiesceWorkGuard>), async_broadcast::TryRecvError>
    {
        let work = counters.map(|counters| NodeQuiesceWorkGuard::begin(counters.clone()));
        self.try_recv().map(|batch| (batch, work))
    }
}

#[derive(Debug)]
pub(super) struct ExpiringRelayState {
    pub(super) registry: RelayRegistry,
}

/// The branch retention enforced by a relay owner. It is derived from the relay's branch so an
/// owner can start directly from the published schedule.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct RelayRetention {
    pub(super) branch_ttl: Option<Duration>,
    pub(super) branch_capacity: Option<NonZeroUsize>,
}

impl RelayRetention {
    pub(super) fn from_schedule(
        domain: &DomainName,
        schedule: &DomainSchedule,
        relay: &RelayName,
    ) -> Result<Self, RuntimeError> {
        let Some(Model::Relay(model)) = schedule
            .nodes
            .values()
            .find(|node| {
                node.kind() == ModelKind::Relay && node.identifier == ModelName::from(&*relay)
            })
            .map(|node| node.config.as_ref())
        else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("missing relay '{}'", relay.as_str()),
            });
        };
        let Some(branch) = model.branching.branch() else {
            return Ok(Self::default());
        };
        let branch_model = schedule
            .nodes
            .values()
            .find_map(|node| {
                let Model::Branch(candidate) = node.config.as_ref() else {
                    return None;
                };
                (&candidate.name == branch).then_some(candidate)
            })
            .ok_or_else(|| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "missing branch '{}' for relay '{}'",
                    branch.as_str(),
                    relay.as_str()
                ),
            })?;
        let branch_ttl = humantime::parse_duration(&branch_model.ttl).map_err(|error| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "invalid branch ttl '{}' for relay '{}': {error}",
                    branch_model.ttl,
                    relay.as_str()
                ),
            }
        })?;
        let branch_capacity = branch_model
            .eviction
            .as_ref()
            .map(|eviction| addressable_count(eviction.max_instances()));
        Ok(Self {
            branch_ttl: Some(branch_ttl),
            branch_capacity,
        })
    }
}

/// One materialized relay's runtime task: the relay it serves, the replicated state it maintains,
/// the branch retention limits it enforces, and the fan-in it consumes.
pub(super) struct RelayStateTaskSpec {
    pub(super) relay: RelayName,
    pub(super) state: MaterializedRelayStateOriginator,
    pub(super) retention: RelayRetention,
    pub(super) receiver: RelayRuntimeFanIn,
}

impl ExpiringRelayState {
    pub(super) fn new() -> Self {
        Self {
            registry: RelayRegistry::new(),
        }
    }

    pub(super) fn touch(&self, key: &Option<BranchKey>, now: Timestamp) {
        self.registry.touch(key, now);
    }

    pub(super) fn contains_key(&self, key: &Option<BranchKey>) -> bool {
        self.registry.contains_key(key)
    }

    pub(super) fn remove(&self, key: &Option<BranchKey>) {
        self.registry.remove(key);
    }
}

impl RelayBoundaryServices {
    pub(super) fn new(
        fanout: RelayBoundaryFanout,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        remote_runtime_consumers: Vec<RemoteRuntimeConsumer>,
        remote_dispatcher: Option<Arc<RemoteDispatcher>>,
    ) -> Self {
        Self {
            fanout,
            attached_runtime_consumer_count: AtomicUsize::new(attached_runtime_consumer_count),
            detached_runtime_consumer_count: AtomicUsize::new(detached_runtime_consumer_count),
            remote_runtime_consumers: ArcSwap::from_pointee(remote_runtime_consumers),
            remote_dispatcher,
            owner_node: RwLock::new(None),
            ingress_slot: Mutex::new(()),
            outbound_slots: DashMap::default(),
        }
    }

    pub(super) fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        self.fanout.subscription_receiver()
    }

    pub(super) fn replace_owner_node(&self, owner_node: Option<ClusterNodeName>) {
        *self.owner_node.write() = owner_node;
    }

    pub(super) fn is_owned_by(&self, node_id: Option<&ClusterNodeName>) -> bool {
        self.owner_node
            .read()
            .as_ref()
            .is_none_or(|owner| Some(owner) == node_id)
    }

    pub(super) fn activate_owner_buffer(&self) -> RelayRuntimeFanIn {
        RelayRuntimeFanIn::new(self.fanout.activate_owner_buffer())
    }

    pub(super) fn deactivate_owner_buffer(&self) {
        self.fanout.deactivate_owner_buffer();
    }

    pub(super) fn outbound_slot(&self, node_id: &ClusterNodeName) -> Arc<Mutex<()>> {
        self.outbound_slots
            .entry(node_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(super) async fn enqueue_owner_batch(
        &self,
        metrics: &RuntimeMetrics,
        domain: &DomainName,
        relay: &RelayName,
        physical_node_id: Option<&ClusterNodeName>,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let dispatch_gate = self.fanout.dispatch_gate();
        let _dispatch_permit = dispatch_gate.acquire_dispatch().await;
        let Some(buffer) = self.fanout.owner_buffer() else {
            for ack in batch.acks.iter() {
                ack.no_ack("relay owner buffer is unavailable");
            }
            return Err(Box::new(batch.clone()));
        };
        let admission = self.fanout.begin_owner_admission();
        if let Err(error) = buffer.broadcast(batch.attached()).await {
            for ack in error.0.acks.iter() {
                ack.no_ack("relay owner buffer is unavailable");
            }
            return Err(Box::new(batch.clone()));
        }
        admission.accept();
        self.observe_owner_buffer_length(
            metrics,
            domain,
            relay,
            physical_node_id,
            batch.key.as_ref(),
        );
        Ok(())
    }

    pub(super) fn begin_owner_batch_completion(&self) -> RelayOwnerBatchCompletion {
        self.fanout.begin_owner_batch_completion()
    }

    pub(super) async fn dispatch_to_owner(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let dispatch_gate = self.fanout.dispatch_gate();
        let _dispatch_permit = dispatch_gate.acquire_dispatch().await;
        let Some(owner_node) = self.owner_node.read().clone() else {
            for ack in batch.acks.iter() {
                ack.no_ack("relay owner is unavailable");
            }
            return Err(Box::new(batch.clone()));
        };
        let Some(dispatcher) = &self.remote_dispatcher else {
            for ack in batch.acks.iter() {
                ack.no_ack("remote dispatcher is unavailable for relay owner delivery");
            }
            return Err(Box::new(batch.clone()));
        };
        let Some(local_node_id) = dispatcher.local_node_id() else {
            for ack in batch.acks.iter() {
                ack.no_ack("local node id is unavailable for relay owner delivery");
            }
            return Err(Box::new(batch.clone()));
        };
        let _slot = self.ingress_slot.lock().await;
        let batch_ipc = match batch.batch.encode_arrow_ipc(dispatcher.executor()).await {
            Ok(bytes) => bytes,
            Err(error) => {
                for ack in batch.acks.iter() {
                    ack.no_ack(error.to_string());
                }
                return Err(Box::new(batch.clone()));
            }
        };
        let mut registered_ack_ids = Vec::new();
        let remote_acks = batch
            .acks
            .iter()
            .map(|ack| {
                if ack.is_empty() {
                    return None;
                }
                let ack_id = dispatcher.next_ack_id();
                dispatcher.register_pending_ack(ack_id, RemoteDispatcher::forwarded_ack(ack));
                registered_ack_ids.push(ack_id);
                Some(RemoteAckRegistration {
                    ack_id,
                    reply_node_id: local_node_id.clone(),
                })
            })
            .collect::<Vec<_>>();
        let admission_result = dispatcher
            .dispatch_admitted_relay_payload(
                &owner_node,
                RelayPayload {
                    kind: RelayPayloadKind::Ingress,
                    domain: domain.clone(),
                    relay: relay.clone(),
                    key: BranchKey::to_remote_key(&batch.key),
                    batch_ipc,
                    metadata: batch
                        .metadata
                        .iter()
                        .map(RuntimeRecordMetadata::to_remote)
                        .collect(),
                    acks: remote_acks,
                    admission: None,
                },
            )
            .await;
        if let Err(reason) = admission_result {
            for ack_id in registered_ack_ids {
                dispatcher.clear_pending_ack(ack_id);
            }
            for ack in batch.acks.iter() {
                ack.no_ack(reason.clone());
            }
            return Err(Box::new(batch.clone()));
        }
        Ok(())
    }

    pub(super) fn observe_owner_buffer_length(
        &self,
        metrics: &RuntimeMetrics,
        domain: &DomainName,
        relay: &RelayName,
        physical_node_id: Option<&ClusterNodeName>,
        branch_key: Option<&BranchKey>,
    ) {
        let Some((len, capacity)) = self.fanout.owner_buffer_len() else {
            return;
        };
        let observation = RelayBufferObservation {
            domain,
            relay,
            physical_node_id,
            direction: RELAY_BUFFER_DIRECTION_CONCRETE,
            len,
            capacity,
        };
        metrics.observe_global_relay_buffer_len(observation);
        if let Some(branch_key) = branch_key {
            metrics.observe_branch_relay_buffer_len(branch_key.as_str(), observation);
        }
    }

    pub(super) async fn fanout_local_subscriptions(&self, batch: &RelayRecordBatch) {
        self.fanout.fanout_subscriptions(batch).await;
    }

    pub(super) async fn fanout_remote_subscriptions(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        batch: &RelayRecordBatch,
    ) {
        let Some(dispatcher) = &self.remote_dispatcher else {
            return;
        };
        let remote_runtime_consumers = self.remote_runtime_consumers.load_full();
        let excluded_nodes = remote_runtime_consumers
            .iter()
            .map(|consumer| consumer.node_id.clone())
            .collect::<BTreeSet<_>>();
        dispatcher
            .dispatch_subscription_fanout(self, domain, relay, &batch.detached(), &excluded_nodes)
            .await;
    }

    pub(super) async fn dispatch_local_runtime_consumers(
        &self,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout
            .dispatch_runtime_consumers(
                self.attached_runtime_consumer_count.load(Ordering::Acquire),
                self.detached_runtime_consumer_count.load(Ordering::Acquire),
                batch,
            )
            .await
    }

    pub(super) async fn dispatch_remote_runtime_consumers(
        &self,
        domain: &DomainName,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let remote_runtime_consumers = self.remote_runtime_consumers.load_full();
        if remote_runtime_consumers.is_empty() {
            return Ok(());
        }
        let Some(dispatcher) = &self.remote_dispatcher else {
            if remote_runtime_consumers
                .iter()
                .any(|consumer| consumer.mode == AckMode::Attached)
            {
                for ack in batch.acks.iter() {
                    ack.no_ack("remote dispatcher unavailable for attached delivery");
                }
                return Err(Box::new(batch.clone()));
            }
            return Ok(());
        };
        // Every consumer of this relay receives the same columns; only the acknowledgement
        // obligations differ, so the body is encoded once for the whole fanout and each
        // destination carries a handle to that one allocation.
        //
        // The encode happens inside the first destination's outbound slot rather than ahead of
        // the loop. A destination's slot is what orders the batches published to it, and an
        // encode is long enough that hoisting it out lets a later batch finish serializing first
        // and reach the slot ahead of an earlier one, delivering the relay out of order.
        let mut encoded_body: Option<ChargedBytes> = None;
        for consumer in remote_runtime_consumers.iter() {
            tokio::task::consume_budget().await;
            let outbound_slot = self.outbound_slot(&consumer.node_id);
            let _slot = outbound_slot.lock().await;
            let batch_ipc = match encoded_body.clone() {
                Some(bytes) => bytes,
                None => match batch.batch.encode_arrow_ipc(dispatcher.executor()).await {
                    Ok(bytes) => {
                        encoded_body = Some(bytes.clone());
                        bytes
                    }
                    Err(error) => {
                        if remote_runtime_consumers
                            .iter()
                            .any(|consumer| consumer.mode == AckMode::Attached)
                        {
                            for ack in batch.acks.iter() {
                                ack.no_ack(error.to_string());
                            }
                            return Err(Box::new(batch.clone()));
                        }
                        warn!(
                            error = %error,
                            "failed to serialize detached remote relay batch"
                        );
                        return Ok(());
                    }
                },
            };
            let remote_batch = match consumer.mode {
                AckMode::Attached => batch.attached(),
                AckMode::Detached => batch.detached(),
            };
            let remote_acks = if consumer.mode == AckMode::Attached {
                let Some(local_node_id) = dispatcher.local_node_id() else {
                    for ack in remote_batch.acks.iter() {
                        ack.no_ack("local node id is unavailable for attached remote delivery");
                    }
                    return Err(Box::new(batch.clone()));
                };
                remote_batch
                    .acks
                    .iter()
                    .map(|ack| {
                        let ack_id = dispatcher.next_ack_id();
                        dispatcher.register_pending_ack(ack_id, ack.clone());
                        Some(RemoteAckRegistration {
                            ack_id,
                            reply_node_id: local_node_id.clone(),
                        })
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![None; remote_batch.acks.len()]
            };
            let result = dispatcher
                .dispatch_admitted_relay_payload(
                    &consumer.node_id,
                    routed_payload(RoutedDelivery {
                        domain,
                        consumer,
                        batch: &remote_batch,
                        batch_ipc: batch_ipc.clone(),
                        acks: remote_acks.clone(),
                    }),
                )
                .await;

            match (consumer.mode, result) {
                (AckMode::Attached, Ok(())) => {}
                (AckMode::Attached, Err(error)) => {
                    for (ack_set, remote_ack) in remote_batch.acks.iter().zip(remote_acks.iter()) {
                        if let Some(remote_ack) = remote_ack {
                            dispatcher.clear_pending_ack(remote_ack.ack_id);
                        }
                        ack_set.no_ack(error.clone());
                    }
                    return Err(Box::new(batch.clone()));
                }
                (AckMode::Detached, Err(error)) => {
                    warn!(
                        error = %error,
                        target_node = %consumer.node_id,
                        "detached remote delivery failed"
                    );
                }
                (AckMode::Detached, Ok(())) => {}
            }
        }

        Ok(())
    }

    pub(super) fn add_local_runtime_consumer(&self, mode: AckMode) -> RelayRuntimeFanIn {
        match mode {
            AckMode::Attached => {
                self.attached_runtime_consumer_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            AckMode::Detached => {
                self.detached_runtime_consumer_count
                    .fetch_add(1, Ordering::AcqRel);
            }
        }
        RelayRuntimeFanIn::new(self.fanout.runtime_consumer_receiver_for_mode(mode))
    }

    pub(super) fn remove_local_runtime_consumer(&self, mode: AckMode) {
        let counter = match mode {
            AckMode::Attached => &self.attached_runtime_consumer_count,
            AckMode::Detached => &self.detached_runtime_consumer_count,
        };
        let previous = counter.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "relay runtime consumer count underflow");
    }

    pub(super) fn replace_remote_runtime_consumers(&self, consumers: Vec<RemoteRuntimeConsumer>) {
        self.remote_runtime_consumers.store(StdArc::new(consumers));
    }

    pub(super) async fn fanout_owner_batch(
        &self,
        metrics: &RuntimeMetrics,
        domain: &DomainName,
        relay: &RelayName,
        physical_node_id: Option<&ClusterNodeName>,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout_local_subscriptions(batch).await;
        self.fanout_remote_subscriptions(domain, relay, batch).await;
        self.observe_owner_buffer_length(
            metrics,
            domain,
            relay,
            physical_node_id,
            batch.key.as_ref(),
        );
        self.dispatch_local_runtime_consumers(batch).await?;
        self.dispatch_remote_runtime_consumers(domain, batch).await
    }

    pub(super) async fn inject_remote_message(
        &self,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout_local_subscriptions(batch).await;
        self.dispatch_local_runtime_consumers(batch).await
    }
}

impl ConcreteRelayRuntime {
    pub(super) fn new(build: ConcreteRelayRuntimeBuild) -> Self {
        let ConcreteRelayRuntimeBuild {
            key,
            runtime,
            domain,
            relay,
            registry,
            services,
        } = build;
        Self {
            runtime,
            domain,
            relay,
            registry,
            services,
            key,
        }
    }

    pub(super) async fn dispatch_boundary(
        &mut self,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        debug_assert_eq!(&self.key, &batch.key);
        self.runtime
            .ingest_stream_boundary_message(
                &self.domain,
                &self.relay,
                &self.registry,
                &self.services,
                batch,
            )
            .await
    }
}

impl RelayBoundaryBuilder {
    pub(super) fn runtime_consumer_receiver_for_mode(
        &mut self,
        mode: AckMode,
    ) -> RelayRuntimeConsumerReceiver {
        match mode {
            AckMode::Attached => {
                self.attached_runtime_consumer_count += 1;
            }
            AckMode::Detached => {
                self.detached_runtime_consumer_count += 1;
            }
        }
        self.fanout.runtime_consumer_receiver_for_mode(mode)
    }

    pub(super) fn runtime_consumer_fan_in_for_mode(&mut self, mode: AckMode) -> RelayRuntimeFanIn {
        RelayRuntimeFanIn::new(self.runtime_consumer_receiver_for_mode(mode))
    }
}

pub(crate) fn scheduled_relay_owner_nodes(
    schedule: &DomainSchedule,
    relay: &RelayName,
) -> Vec<ClusterNodeName> {
    let owner = schedule
        .nodes
        .get(&NodeRef::new(ModelKind::Relay, ModelName::from(relay)))
        .and_then(ScheduledNode::execution_node);
    match owner {
        Some(owner) => vec![owner.clone()],
        None => Vec::new(),
    }
}

impl Runtime {
    pub(in crate::runtime) fn current_stream_expiration_time(
        &self,
        domain: &DomainName,
    ) -> DomainClockAccessResult<Timestamp> {
        Ok(self.domain_execution_snapshot(domain)?.now())
    }

    pub(in crate::runtime) fn touch_stream_key(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        key: &Option<BranchKey>,
        now: Timestamp,
    ) {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        );
        if let Some(state) = self.inner.expiring_stream_states.get(&placement) {
            state.touch(key, now);
        }
    }

    pub(in crate::runtime) fn remove_stream_key_presence(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        key: &Option<BranchKey>,
    ) {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        );
        if let Some(state) = self.inner.expiring_stream_states.get(&placement) {
            state.remove(key);
        }
    }

    pub(super) async fn fanout_relay_owner_batch(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        services: &RelayBoundaryServices,
        branches: &mut RelayOwnerBranchState,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let physical_node_id = self.inner.remote_dispatch.local_node_id.read().clone();
        let now = match self.current_stream_expiration_time(domain) {
            Ok(now) => now,
            Err(error) => {
                let reason = format!(
                    "relay '{}' in domain '{}' could not read domain time: {error}",
                    relay.as_str(),
                    domain.as_str(),
                );
                self.events().report_error(reason.clone());
                for ack in batch.acks.iter() {
                    ack.no_ack(reason.clone());
                }
                return Err(Box::new(batch.clone()));
            }
        };
        branches.registry.touch(&batch.key, now);
        self.touch_stream_key(domain, relay, &batch.key, now);
        branches
            .instances
            .get_or_try_create_with(batch.key.clone(), now, |_| {
                Ok::<(), std::convert::Infallible>(())
            })
            .assured("the tracking closure's error type is Infallible");
        if let Some(capacity) = branches.capacity {
            for (evicted_key, _) in branches.instances.evict_lru_to_capacity(capacity) {
                branches.registry.remove(&evicted_key);
                self.remove_stream_key_presence(domain, relay, &evicted_key);
            }
        }
        self.inner.metrics.observe_global_stream_received(
            domain,
            relay,
            self.inner.remote_dispatch.local_node_id.read().as_ref(),
            batch.message_count(),
            batch.estimated_bytes(),
            batch.domain_timestamp(),
        );
        self.inner.metrics.observe_branch_stream_received(
            branch_key_display(&batch.key),
            RelayBatchObservation {
                domain,
                relay,
                physical_node_id: self.inner.remote_dispatch.local_node_id.read().as_ref(),
                messages: batch.message_count(),
                bytes: batch.estimated_bytes(),
                domain_timestamp: batch.domain_timestamp(),
            },
        );
        self.mark_branch_aggregated_metrics_updated(domain, ModelKind::Relay, relay);
        let result = services
            .fanout_owner_batch(
                &self.inner.metrics,
                domain,
                relay,
                physical_node_id.as_ref(),
                batch,
            )
            .await;
        batch.ack_success();
        result
    }

    pub(in crate::runtime) fn expiring_stream_state(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Arc<ExpiringRelayState> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        );
        if let Some(existing) = self.inner.expiring_stream_states.get(&placement) {
            return existing.clone();
        }
        let state = Arc::new(ExpiringRelayState::new());
        self.inner
            .expiring_stream_states
            .insert(placement, state.clone());
        state
    }

    pub(in crate::runtime) fn clear_expiring_stream_states_for_domain(&self, domain: &DomainName) {
        let relays = self
            .inner
            .expiring_stream_states
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in relays {
            self.inner.expiring_stream_states.remove(&placement);
        }
    }

    pub(crate) async fn subscribe_stream(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<RelaySubscriptionReceiver<RelayRecordBatch>, RuntimeError> {
        let Some(execution) = self.inner.executions.get(domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        if !execution.relay_registries.contains_key(relay) {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        }
        let Some(services) = execution.relay_services.get(relay) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        Ok(services.subscription_receiver())
    }

    pub(in crate::runtime) fn relay_is_cluster_scheduled(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> bool {
        self.inner.executions.get(domain).is_some_and(|execution| {
            execution
                .schedule
                .nodes
                .values()
                .find(|node| {
                    node.kind() == ModelKind::Relay && node.identifier == ModelName::from(&*relay)
                })
                .and_then(ScheduledNode::execution_node)
                .is_some()
        })
    }

    pub(in crate::runtime) fn spawn_relay_owner_task(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        registry: RelayRegistry,
        services: Arc<RelayBoundaryServices>,
        retention: RelayRetention,
    ) -> RelayOwnerTask {
        let mut receiver = services.activate_owner_buffer();
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let runtime = self.clone();
        let domain = domain.clone();
        let relay = relay.clone();
        let RelayRetention {
            branch_ttl,
            branch_capacity,
        } = retention;
        let expiration_scan_interval = self.inner.branch_instance_expiration_scan_interval;
        let task = tokio::spawn(async move {
            let mut branches = RelayOwnerBranchState {
                registry,
                instances: BranchInstanceRegistry::new(),
                capacity: branch_capacity,
            };
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    biased;
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    batch = receiver.recv() => {
                        let Some(batch) = batch else {
                            break;
                        };
                        let _completion = services.begin_owner_batch_completion();
                        runtime
                            .fanout_relay_owner_batch(
                                &domain,
                                &relay,
                                &services,
                                &mut branches,
                                &batch,
                            )
                            .await
                            .discarded(
                                "the batch is acknowledged inside the fanout, and the owner task \
                                 has no second consumer for a rejected copy",
                            );
                    }
                    _ = async {
                        if branch_ttl.is_some() {
                            sleep_until(next_expiration_scan).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    } => {
                        let now = match runtime.current_stream_expiration_time(&domain) {
                            Ok(now) => now,
                            Err(error) => {
                                runtime.events().report_error(format!(
                                    "relay '{}' in domain '{}' lost its clock: {error}",
                                    relay.as_str(),
                                    domain.as_str(),
                                ));
                                break;
                            }
                        };
                        for (expired_key, _) in branches.instances.expire(
                            now,
                            branch_ttl.verified("this select branch only arms while a branch TTL is configured"),
                        ) {
                            branches.registry.remove(&expired_key);
                            runtime.remove_stream_key_presence(&domain, &relay, &expired_key);
                        }
                        next_expiration_scan = Instant::now() + expiration_scan_interval;
                    }
                }
            }

            loop {
                tokio::task::consume_budget().await;
                let batch = match receiver.try_recv() {
                    Ok(batch) => batch,
                    Err(
                        async_broadcast::TryRecvError::Empty
                        | async_broadcast::TryRecvError::Closed,
                    ) => {
                        break;
                    }
                    Err(async_broadcast::TryRecvError::Overflowed(_)) => {
                        unreachable!("relay owner buffer is backpressured and must not overflow")
                    }
                };
                let _completion = services.begin_owner_batch_completion();
                runtime
                    .fanout_relay_owner_batch(&domain, &relay, &services, &mut branches, &batch)
                    .await
                    .discarded(
                        "the batch is acknowledged inside the fanout, and the owner task has no \
                         second consumer for a rejected copy",
                    );
            }
            services.observe_owner_buffer_length(
                &runtime.inner.metrics,
                &domain,
                &relay,
                runtime.inner.remote_dispatch.local_node_id.read().as_ref(),
                None,
            );
            services.deactivate_owner_buffer();
            branches.registry.clear();
            runtime.inner.metrics.remove_relay(&domain, &relay);
        });
        RelayOwnerTask { shutdown, task }
    }

    pub(in crate::runtime) fn spawn_relay_state_task(
        &self,
        domain: &DomainName,
        spec: RelayStateTaskSpec,
    ) -> RelayStateTask {
        let RelayStateTaskSpec {
            relay,
            state,
            retention:
                RelayRetention {
                    branch_ttl,
                    branch_capacity,
                },
            receiver,
        } = spec;
        let runtime = self.clone();
        let domain = domain.clone();
        let expiration_scan_interval = self.inner.branch_instance_expiration_scan_interval;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let quiesce_counters =
            self.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Relay, &relay));
        let force_flush = self.force_flush_participant(&domain, quiesce_counters.clone());
        let task = tokio::spawn(async move {
            let interaction_input = RelayInteractionInput::new(relay.clone(), receiver, None);
            let mut interaction = RelayInteraction::new(
                vec![interaction_input],
                shutdown_rx,
                Some(force_flush),
                Some(quiesce_counters),
            )
            .verified(
                "the registry validated this input, and a non-empty input list builds an \
                 interaction",
            );
            let mut branch_instances = BranchInstanceRegistry::<Option<BranchKey>, ()>::new();
            let relay_model_name = ModelName::from(&relay);
            let ownership_entity =
                DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay_model_name);
            let mut restored_branches = state.read().restored_branch_watermarks();
            restored_branches.sort_by_key(|(_, last_ingestion)| *last_ingestion);
            for (key, last_ingestion) in restored_branches {
                branch_instances.insert_restored(key, last_ingestion, ());
            }
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            'state_task: loop {
                tokio::task::consume_budget().await;
                if !runtime.ownership_handoff_entity_is_frozen(&ownership_entity)
                    && let Some(branch_ttl) = branch_ttl
                    && Instant::now() >= next_expiration_scan
                {
                    let now = match runtime.current_stream_expiration_time(&domain) {
                        Ok(now) => now,
                        Err(error) => {
                            runtime.events().report_error(format!(
                                "materialized relay '{}' in domain '{}' lost its clock: {error}",
                                relay.as_str(),
                                domain.as_str(),
                            ));
                            break 'state_task;
                        }
                    };
                    for (key, _) in branch_instances.expire(now, branch_ttl) {
                        tokio::task::consume_budget().await;
                        if let Err(error) = runtime.delete_materialized_stream_key(&state, &key) {
                            warn!(
                                domain = domain.as_str(),
                                relay = relay.as_str(),
                                error = %error,
                                "materialized relay assignment changed during branch expiration"
                            );
                            break 'state_task;
                        }
                    }
                    next_expiration_scan = Instant::now() + expiration_scan_interval;
                    continue;
                }
                let expiration_sleep =
                    if runtime.ownership_handoff_entity_is_frozen(&ownership_entity) {
                        Some(OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL)
                    } else {
                        branch_ttl.map(|_| {
                            next_expiration_scan
                                .checked_duration_since(Instant::now())
                                .unwrap_or(Duration::ZERO)
                        })
                    };
                let wake_at = expiration_sleep.map(|sleep| Instant::now() + sleep);
                let work = match interaction.next(wake_at).await {
                    Ok(work) => work,
                    Err(error) => {
                        if let Some(acks) = error.acks() {
                            acks.no_ack(format!(
                                "state task for relay '{}' failed to collect input: {error}",
                                relay.as_str()
                            ));
                        }
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            error = %error,
                            "materialized relay interaction failed"
                        );
                        continue;
                    }
                };
                let (event, _work) = work.into_parts();
                let batch = match event {
                    RelayInteractionEvent::Batch {
                        relay: input_relay,
                        batch,
                    } => {
                        debug_assert_eq!(input_relay, relay);
                        batch
                    }
                    RelayInteractionEvent::Wake => continue,
                    RelayInteractionEvent::ForceFlush(completion) => {
                        completion.complete();
                        continue;
                    }
                    RelayInteractionEvent::Command(command) => match command {},
                    RelayInteractionEvent::Stopped(reason) => {
                        debug!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            ?reason,
                            "materialized relay interaction stopped"
                        );
                        break;
                    }
                };
                let branch_key = batch.key.clone();
                let now = match runtime.current_stream_expiration_time(&domain) {
                    Ok(now) => now,
                    Err(error) => {
                        let reason = format!(
                            "materialized relay '{}' in domain '{}' lost its clock: {error}",
                            relay.as_str(),
                            domain.as_str(),
                        );
                        runtime.events().report_error(reason.clone());
                        for ack in batch.acks.iter() {
                            ack.no_ack(reason.clone());
                        }
                        break;
                    }
                };
                branch_instances
                    .get_or_try_create_with(branch_key.clone(), now, |_| {
                        Ok::<(), std::convert::Infallible>(())
                    })
                    .assured("the tracking closure's error type is Infallible");
                if let Some(branch_capacity) = branch_capacity {
                    for (evicted_key, _) in branch_instances.evict_lru_to_capacity(branch_capacity)
                    {
                        tokio::task::consume_budget().await;
                        if let Err(error) =
                            runtime.delete_materialized_stream_key(&state, &evicted_key)
                        {
                            warn!(
                                domain = domain.as_str(),
                                relay = relay.as_str(),
                                error = %error,
                                "materialized relay assignment changed during branch eviction"
                            );
                            break 'state_task;
                        }
                    }
                }
                let messages = match batch.try_into_messages() {
                    Ok(messages) => messages,
                    Err(error_and_batch) => {
                        let (error, _) = *error_and_batch;
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            branch = branch_key_display(&branch_key),
                            error = %error,
                            "failed to decode scheduled materialized relay batch"
                        );
                        continue;
                    }
                };
                for message in messages {
                    tokio::task::consume_budget().await;
                    if let Err(error) = runtime.update_materialized_stream_last_by_timestamp(
                        &state,
                        &branch_key,
                        &message.record,
                    ) {
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            error = %error,
                            "materialized relay assignment changed while applying a batch"
                        );
                        break 'state_task;
                    }
                }
            }
        });
        RelayStateTask { shutdown, task }
    }
}

#[cfg(test)]
mod tests;
