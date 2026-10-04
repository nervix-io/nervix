//! Per-relay execution boundaries and branch-local runtime ownership.
//!
//! Layer: data plane.
//!
//! - **Owns.** Relay fan-out, branch-local ingress and outbound ordering, concrete branch
//!   retention, owner tasks, and materialized relay state.
//! - **Depends on.** Validated execution graphs, compiled schemas, execution admission, and the
//!   interconnect dispatcher.
//! - **Must not know.** NSPL text, transactions, consensus operations, or connector internals.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "relay membership and channel installation establish retained concrete branch \
                  and delivery handles"
    )
)]

use super::*;

#[path = "relay_channels.rs"]
mod channels;
use channels::{RelayBranchChannels, RelayChannels};

#[cfg(all(test, feature = "shuttle"))]
#[path = "relay_boundary_shuttle_tests.rs"]
mod shuttle_tests;

pub(super) const RELAY_BUFFER_DIRECTION_CONCRETE: &str = "concrete";
const RELAY_CHANNEL_IDLE_ROTATION: Duration = Duration::from_secs(300);

/// A count NSPL configures, narrowed to the width this node addresses memory with.
///
/// Both halves of the narrowing hold before it runs: the Models keep these counts non-zero, and
/// the supported targets address memory at least as wide as the `u64` they are written as.
pub(super) fn addressable_count(configured: NonZeroU64) -> NonZeroUsize {
    NonZeroUsize::new(configured.get().arch_into())
        .assured("a non-zero configured count is still non-zero at this target's pointer width")
}

/// The concrete branches this node's owner of one relay holds, as `DESCRIBE`, materialized reads
/// and the console's graph observe them. The relay's owner task alone changes it, and only when a
/// branch appears, is evicted or expires, or the owner starts or stops.
pub(super) type RelayBranchPresence = Arc<BranchPresence<BranchKey>>;

pub(super) struct ConcreteRelayRuntime {
    pub(super) key: Option<BranchKey>,
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) relay: RelayName,
    pub(super) services: Arc<RelayBoundaryServices>,
    channels: RelayProducerChannels,
}

enum RelayProducerChannels {
    Bound(StdArc<RelayBranchChannels>),
    Ended,
}

pub(super) struct ConcreteRelayRuntimeBuild {
    pub(super) key: Option<BranchKey>,
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) relay: RelayName,
    pub(super) services: Arc<RelayBoundaryServices>,
}

#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "retained relay handles route each admitted record and keep channel ordering and \
                  fences"
    )
)]
pub(super) struct RelayBoundaryServices {
    pub(super) fanout: RelayBoundaryFanout,
    pub(super) attached_runtime_consumer_count: AtomicUsize,
    pub(super) detached_runtime_consumer_count: AtomicUsize,
    pub(super) remote_runtime_consumers: ArcSwap<Vec<RemoteRuntimeConsumer>>,
    pub(super) remote_dispatcher: Option<StdArc<RemoteDispatcher>>,
    pub(super) owner_node: ArcSwapOption<ClusterNodeName>,
    /// The concrete branches this node's owner of the relay holds, shared with the relay's state
    /// placement so materialized reads observe the same membership across execution rebuilds.
    pub(super) branch_presence: RelayBranchPresence,
    channels: RelayChannels,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(super) struct RelayOutboundChannel {
    node_id: ClusterNodeName,
    relay: RelayName,
    kind: RelayPayloadKind,
}

#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "retained relay handles route each admitted record and keep channel ordering and \
                  fences"
    )
)]
pub(super) struct RelayOutboundSlot {
    pub(super) gate: Mutex<()>,
    sequence: nervix_primitives::sync::blocking::Mutex<RelayOutboundSequence>,
    cancellation: CancellationToken,
}

#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "retained relay handles route each admitted record and keep channel ordering and \
                  fences"
    )
)]
struct RelayOutboundSequence {
    channel_incarnation: [u8; 16],
    next_sequence: u64,
    last_delivery_at: Option<Instant>,
}

impl RelayOutboundSequence {
    fn reopen(&mut self) {
        self.channel_incarnation = uuid::Uuid::now_v7().into_bytes();
        self.next_sequence = 0;
        self.last_delivery_at = None;
    }
}

impl RelayOutboundSlot {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_cancellation(CancellationToken::new())
    }

    fn with_cancellation(cancellation: CancellationToken) -> Self {
        Self {
            gate: Mutex::new(()),
            sequence: nervix_primitives::sync::blocking::Mutex::new(RelayOutboundSequence {
                channel_incarnation: uuid::Uuid::now_v7().into_bytes(),
                next_sequence: 0,
                last_delivery_at: None,
            }),
            cancellation,
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "one retained channel serializes an admitted delivery until completion or \
                      retirement",
            key = "delivery channel lifetime",
            bound = "one gate holder; queued delivery is cancelled when that exact lifetime ends"
        )
    )]
    pub(super) async fn lock_for_delivery(
        &self,
    ) -> Option<nervix_primitives::sync::MutexGuard<'_, ()>> {
        nervix_primitives::select! {
            biased;
            _ = self.cancellation.cancelled() => None,
            guard = self.gate.lock() => {
                if self.cancellation.is_cancelled() { None } else { Some(guard) }
            }
        }
    }

    pub(super) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the retained channel serializes sequence and publication changes",
            key = "delivery channel incarnation and sequence",
            bound = "one admitted delivery is serialized against channel replacement and its \
                     dispatch gate"
        )
    )]
    pub(super) fn reopen_delivery_channel(&self) {
        self.sequence.lock().reopen();
    }

    pub(super) fn next_delivery(&self) -> RelayDelivery {
        self.next_delivery_at(Instant::now())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the retained channel serializes sequence and publication changes",
            key = "delivery channel incarnation and sequence",
            bound = "one admitted delivery is serialized against channel replacement and its \
                     dispatch gate"
        )
    )]
    fn next_delivery_at(&self, now: Instant) -> RelayDelivery {
        let mut channel = self.sequence.lock();
        if let Some(last_delivery_at) = channel.last_delivery_at
            && now
                .checked_duration_since(last_delivery_at)
                .assured("a relay channel's monotonic delivery time does not move backwards")
                >= RELAY_CHANNEL_IDLE_ROTATION
        {
            channel.reopen();
        }
        let sequence = channel.next_sequence;
        channel.next_sequence = sequence
            .checked_add(1)
            .assured("a process cannot deliver u64::MAX batches before a channel rotates");
        channel.last_delivery_at = Some(now);
        RelayDelivery {
            channel_incarnation: channel.channel_incarnation,
            sequence,
        }
    }
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
    pub(super) branch_presence: RelayBranchPresence,
    pub(super) remote_runtime_consumers: Vec<RemoteRuntimeConsumer>,
}

#[derive(Debug)]
pub(super) struct RelayConsumerFanout {
    pub(super) dispatch_gate: Arc<RelayDispatchGate>,
    /// Branch-scoped fences published under a short whole-relay fence. Dispatch reads one immutable
    /// set and takes permits only from entries selecting its branch.
    branch_dispatch_gates: Arc<BranchRelayDispatchGates>,
    pub(super) owner_buffer: ArcSwapOption<RelayOwnerBuffer>,
    pub(super) owner_capacity: AtomicUsize,
    pub(super) owner_pending_batches: Arc<AtomicUsize>,
    pub(super) subscriptions: RelaySubscriptions,
    pub(super) attached_runtime_consumers: RelayBroadcast<RelayRecordBatch>,
    pub(super) detached_runtime_consumers: RelayBroadcast<RelayRecordBatch>,
}

#[derive(Debug, Clone)]
pub(super) struct BranchRelayDispatchGate {
    id: u64,
    scope: WasmStateResetScope,
    gate: Arc<RelayDispatchGate>,
}

#[derive(Debug)]
struct BranchRelayDispatchGates {
    entries: ArcSwap<Vec<BranchRelayDispatchGate>>,
    next_id: AtomicU64,
}

impl BranchRelayDispatchGates {
    fn remove(&self, id: u64) {
        self.entries.rcu(|current| {
            current
                .iter()
                .filter(|entry| entry.id != id)
                .cloned()
                .collect::<Vec<_>>()
        });
    }
}

/// One branch-scoped relay fence. Removing its immutable registry entry and releasing its gate
/// admits the selected branch again; sibling branches never wait on this lease after publication.
#[derive(Debug)]
pub(in crate::runtime) struct BranchRelayDispatchGateLease {
    gates: Arc<BranchRelayDispatchGates>,
    id: u64,
    gate: Option<RelayDispatchGateLease>,
}

impl Drop for BranchRelayDispatchGateLease {
    fn drop(&mut self) {
        self.gates.remove(self.id);
        self.gate.take();
    }
}

#[derive(Debug)]
pub(super) struct RelayOwnerBuffer {
    batches: RelayBroadcast<RelayRecordBatch>,
    metrics: RelayMetricsHandle,
}

impl RelayOwnerBuffer {
    fn with_capacity(capacity: NonZeroUsize, metrics: RelayMetricsHandle) -> Self {
        Self {
            batches: RelayBroadcast::with_capacity(capacity),
            metrics,
        }
    }

    fn new_receiver(&self) -> RelayRuntimeConsumerReceiver {
        self.batches.new_receiver()
    }

    fn len(&self) -> usize {
        self.batches.len()
    }

    fn capacity(&self) -> usize {
        self.batches.capacity()
    }

    fn set_capacity(&self, capacity: NonZeroUsize) {
        self.batches.set_capacity(capacity);
    }

    fn observe_length(&self) {
        self.metrics.observe_buffer(self.len(), self.capacity());
    }
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
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "retained relay handles route each admitted record and keep channel ordering and \
                  fences"
    )
)]
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
    pub(super) delivery: RelayDelivery,
    pub(super) domain: &'a DomainName,
    pub(super) consumer: &'a RemoteRuntimeConsumer,
    pub(super) batch: &'a RelayRecordBatch,
    pub(super) batch_ipc: ChargedBytes,
    pub(super) acks: Vec<Option<RemoteAckRegistration>>,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "retained relay handles route each admitted record and keep channel ordering and \
                  fences"
    )
)]
pub(super) fn routed_payload(delivery: RoutedDelivery<'_>) -> RelayPayload {
    RelayPayload {
        delivery: delivery.delivery,
        kind: RelayPayloadKind::Routed,
        domain: delivery.domain.clone(),
        relay: delivery.consumer.relay.clone(),
        key: BranchKey::to_remote_key(&delivery.batch.key),
        batch_ipc: delivery.batch_ipc,
        metadata: delivery.batch.metadata.to_remote(),
        acks: delivery.acks,
        admission: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RemoteRuntimeConsumer {
    pub(super) node_id: ClusterNodeName,
    pub(super) relay: RelayName,
    pub(super) mode: AckMode,
}

/// How a batch reaches this node's runtime consumers of a relay, which decides what reaching no
/// attached consumer here means.
#[derive(Debug, Clone, Copy)]
pub(super) enum RuntimeConsumerDispatch {
    /// The relay owner fans out a batch of its own relay. Its remote consumers can take what no
    /// local consumer does, and a relay that no attached consumer reads leaves nothing to wait for.
    Owner,
    /// The owner of the relay on another node routed the batch here, because its schedule places an
    /// attached consumer of the relay on this node.
    Routed,
}

impl RuntimeConsumerDispatch {
    /// Whether a batch that carries record acknowledgements must reach an attached consumer on this
    /// node.
    ///
    /// A routed batch that finds none was routed for a consumer that has since left the node, as
    /// when a forced recovery moves it off a node the scheduler judged unavailable while the batch
    /// was in flight. Nothing here can complete the work its acknowledgements stand for, so
    /// completing them would report records no consumer processed.
    fn requires_local_attached_consumer(self) -> bool {
        match self {
            Self::Owner => false,
            Self::Routed => true,
        }
    }
}

pub(super) struct RelayOwnerTask {
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: JoinHandle<()>,
}

pub(super) struct RelayOwnerBranchState {
    /// The concrete branches this owner holds, each with its metric series, and the presence it
    /// publishes for them. Dropping it releases the presence.
    pub(super) instances: OwnedBranches<BranchKey, RelayMetricRecorders>,
    pub(super) global_metrics: RelayMetricsHandle,
    pub(super) physical_node_id: Option<ClusterNodeName>,
    pub(super) capacity: Option<NonZeroUsize>,
    /// The clock this owner reads its expiration time from, bound on first use and then held.
    ///
    /// Binding resolves the domain's execution through a map every other task family reads once,
    /// and the handle stays valid for as long as the owner runs: rebuilding a domain execution
    /// stops its relay owners and spawns new ones.
    domain_clock: Option<DomainClock>,
}

impl RelayOwnerBranchState {
    /// The domain time this owner admits branches at and expires them by.
    fn expiration_time(
        &mut self,
        runtime: &Runtime,
        domain: &DomainName,
    ) -> DomainClockAccessResult<Timestamp> {
        if self.domain_clock.is_none() {
            self.domain_clock = Some(runtime.bind_domain_clock(domain)?);
        }
        let clock = self
            .domain_clock
            .as_ref()
            .verified("the binding above installed this owner's domain clock");
        Ok(clock.snapshot()?.now())
    }
}

pub(super) struct RelayStateTask {
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy, strum::Display)]
pub(super) enum RelayTaskKind {
    #[strum(serialize = "relay state")]
    State,
    #[strum(serialize = "relay owner")]
    Owner,
}

#[derive(Debug, Error)]
pub(super) enum RelayTaskStopError {
    #[error("{task} task failed")]
    Join { task: RelayTaskKind },
    #[error("{task} task did not drain within {grace:?}")]
    DrainTimeout {
        task: RelayTaskKind,
        grace: Duration,
    },
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

impl RelayOwnerTask {
    pub(super) async fn stop(
        mut self,
        grace: Duration,
    ) -> error_stack::Result<(), RelayTaskStopError> {
        self.shutdown.send_replace(true);
        match nervix_primitives::time::timeout(grace, &mut self.task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(Report::new(error).change_context(RelayTaskStopError::Join {
                task: RelayTaskKind::Owner,
            })),
            Err(_) => {
                self.task.abort();
                self.task.join_after_shutdown("relay owner").await;
                Err(Report::new(RelayTaskStopError::DrainTimeout {
                    task: RelayTaskKind::Owner,
                    grace,
                }))
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
            branch_dispatch_gates: Arc::new(BranchRelayDispatchGates {
                entries: ArcSwap::from_pointee(Vec::new()),
                next_id: AtomicU64::new(0),
            }),
            owner_buffer: ArcSwapOption::empty(),
            owner_capacity: AtomicUsize::new(capacity.get()),
            owner_pending_batches: Arc::new(AtomicUsize::new(0)),
            subscriptions: RelaySubscriptions::new(),
            attached_runtime_consumers: RelayBroadcast::with_capacity(dispatch_capacity),
            detached_runtime_consumers: RelayBroadcast::with_capacity(dispatch_capacity),
        }
    }

    #[cfg(test)]
    pub(super) fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        self.subscriptions.receivers().new_receiver()
    }

    pub(super) fn set_capacity(&self, capacity: NonZeroUsize) {
        self.owner_capacity.store(capacity.get(), Ordering::Release);
        let owner_buffer = self.owner_buffer.load();
        if let Some(buffer) = owner_buffer.as_deref() {
            buffer.set_capacity(capacity);
        }
    }

    pub(super) fn activate_owner_buffer(
        &self,
        metrics: RelayMetricsHandle,
    ) -> RelayRuntimeConsumerReceiver {
        let capacity = NonZeroUsize::new(self.owner_capacity.load(Ordering::Acquire))
            .verified("relay capacity is validated as nonzero before it is stored");
        let buffer = StdArc::new(RelayOwnerBuffer::with_capacity(capacity, metrics));
        let receiver = buffer.new_receiver();
        self.owner_buffer.store(Some(buffer));
        receiver
    }

    pub(super) fn deactivate_owner_buffer(&self) {
        self.owner_buffer.store(None);
    }

    /// The owner buffer held in full, for a caller that keeps it across an await.
    pub(super) fn owner_buffer(&self) -> Option<StdArc<RelayOwnerBuffer>> {
        self.owner_buffer.load_full()
    }

    pub(super) fn owner_buffer_len(&self) -> Option<(usize, usize)> {
        let owner_buffer = self.owner_buffer.load();
        owner_buffer
            .as_deref()
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

    async fn acquire_branch_dispatch_gates(
        &self,
        key: &Option<BranchKey>,
    ) -> Vec<OwnedRelayDispatchPermit> {
        let fingerprint = key.as_ref().map(BranchKey::fingerprint);
        let gates = self.branch_dispatch_gates.entries.load_full();
        let mut permits = Vec::new();
        for scoped in gates.iter() {
            nervix_primitives::task::consume_budget().await;
            if scoped.scope.contains(fingerprint.as_ref()) {
                permits.push(RelayDispatchGate::acquire_owned(&scoped.gate).await);
            }
        }
        permits
    }

    #[allow(deprecated)] // until try_update is stabilized
    async fn engage_branch_dispatch_gate(
        &self,
        scope: WasmStateResetScope,
        deadline: Instant,
        reason: &str,
    ) -> Option<BranchRelayDispatchGateLease> {
        let branch_gate = Arc::new(RelayDispatchGate::new());
        let mut branch_lease =
            RelayDispatchGateLease::engage(branch_gate.clone(), deadline, reason.to_string());
        let mut publication_fence = RelayDispatchGateLease::engage(
            self.dispatch_gate.clone(),
            deadline,
            reason.to_string(),
        );
        if !publication_fence.wait_quiescent().await {
            return None;
        }
        #[allow(deprecated)] // until try_update is stabilized
        let id = self
            .branch_dispatch_gates
            .next_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .assured("a relay cannot publish 2^64 branch-scoped gate leases");
        self.branch_dispatch_gates.entries.rcu(|current| {
            let mut next = Vec::with_capacity(
                current
                    .len()
                    .checked_add(1)
                    .assured("a process cannot hold usize::MAX branch-scoped relay gates"),
            );
            next.extend(current.iter().cloned());
            next.push(BranchRelayDispatchGate {
                id,
                scope,
                gate: branch_gate.clone(),
            });
            next
        });
        drop(publication_fence);
        if !branch_lease.wait_quiescent().await {
            self.remove_branch_dispatch_gate(id);
            return None;
        }
        Some(BranchRelayDispatchGateLease {
            gates: self.branch_dispatch_gates.clone(),
            id,
            gate: Some(branch_lease),
        })
    }

    fn remove_branch_dispatch_gate(&self, id: u64) {
        self.branch_dispatch_gates.remove(id);
    }

    pub(super) fn dispatch_is_fenced(&self) -> bool {
        self.dispatch_gate.is_engaged()
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
        self.subscriptions.broadcast(batch.detached()).await;
    }

    pub(super) async fn dispatch_runtime_consumers(
        &self,
        dispatch: RuntimeConsumerDispatch,
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
        // A consumer that leaves after this count was read keeps the share the broadcast below
        // reserves for it unresolved, which fails the batch as well.
        if attached_receiver_count == 0
            && dispatch.requires_local_attached_consumer()
            && batch.carries_record_acknowledgements()
        {
            for ack in batch.acks.iter() {
                ack.no_ack(
                    "no attached consumer of the relay runs on the node the batch was routed to",
                );
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
                let failed = error.batch;
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

    #[cfg(test)]
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
        dispatch: RuntimeConsumerDispatch,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout
            .dispatch_runtime_consumers(
                dispatch,
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

    pub(super) fn activate_owner_buffer(
        &self,
        metrics: RelayMetricsHandle,
    ) -> RelayRuntimeConsumerReceiver {
        match self {
            Self::Direct(fanout) => fanout.activate_owner_buffer(metrics),
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.fanout.activate_owner_buffer(metrics)
            }
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

    pub(super) fn owner_buffer(&self) -> Option<StdArc<RelayOwnerBuffer>> {
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

    pub(super) async fn engage_branch_dispatch_gate(
        &self,
        scope: WasmStateResetScope,
        deadline: Instant,
        reason: &str,
    ) -> Option<BranchRelayDispatchGateLease> {
        match self {
            Self::Direct(fanout) => {
                fanout
                    .engage_branch_dispatch_gate(scope, deadline, reason)
                    .await
            }
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse
                    .fanout
                    .engage_branch_dispatch_gate(scope, deadline, reason)
                    .await
            }
        }
    }

    async fn acquire_branch_dispatch_gates(
        &self,
        key: &Option<BranchKey>,
    ) -> Vec<OwnedRelayDispatchPermit> {
        match self {
            Self::Direct(fanout) => fanout.acquire_branch_dispatch_gates(key).await,
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse
                    .fanout
                    .acquire_branch_dispatch_gates(key)
                    .await
            }
        }
    }

    pub(super) fn dispatch_is_fenced(&self) -> bool {
        match self {
            Self::Direct(fanout) => fanout.dispatch_is_fenced(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.dispatch_is_fenced(),
        }
    }

    pub(super) fn outstanding_work_len(&self) -> usize {
        match self {
            Self::Direct(fanout) => fanout.outstanding_work_len(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.outstanding_work_len(),
        }
    }

    #[cfg(test)]
    pub(super) fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        match self {
            Self::Direct(fanout) => fanout.subscription_receiver(),
            Self::BranchCollapse(branch_collapse) => branch_collapse.subscription_receiver(),
        }
    }

    /// The session subscribers of this relay.
    pub(super) fn subscriptions(&self) -> &RelaySubscriptions {
        match self {
            Self::Direct(fanout) => &fanout.subscriptions,
            Self::BranchCollapse(branch_collapse) => &branch_collapse.fanout.subscriptions,
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
        dispatch: RuntimeConsumerDispatch,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        match self {
            Self::Direct(fanout) => {
                fanout
                    .dispatch_runtime_consumers(
                        dispatch,
                        attached_runtime_consumer_count,
                        detached_runtime_consumer_count,
                        batch,
                    )
                    .await
            }
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse
                    .dispatch_runtime_consumers(
                        dispatch,
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
        nervix_primitives::task::consume_budget().await;
        self.receiver.recv().await
    }

    pub(super) fn try_recv(&mut self) -> RelayTryRecv<RelayRecordBatch> {
        self.receiver.try_recv()
    }

    pub(super) fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<RelayRecordBatch>> {
        self.receiver.poll_recv(cx)
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
    ) -> RelayTryRecv<(RelayRecordBatch, Option<NodeQuiesceWorkGuard>)> {
        let work = counters.map(|counters| NodeQuiesceWorkGuard::begin(counters.clone()));
        match self.try_recv() {
            RelayTryRecv::Batch(batch) => RelayTryRecv::Batch((batch, work)),
            RelayTryRecv::Empty => RelayTryRecv::Empty,
            RelayTryRecv::Closed => RelayTryRecv::Closed,
        }
    }
}

/// A relay owner consumes the retention decision made with the rest of its domain plan.
pub(super) use crate::registry::PlannedRelayRetention as RelayRetention;

/// One materialized relay's runtime task: the relay it serves, the replicated state it maintains,
/// the branch retention limits it enforces, and the fan-in it consumes.
pub(super) struct RelayStateTaskSpec {
    pub(super) relay: RelayName,
    pub(super) state: MaterializedRelayStateOriginator,
    pub(super) retention: RelayRetention,
    pub(super) receiver: RelayRuntimeFanIn,
}

impl RelayBoundaryServices {
    pub(super) fn new(
        fanout: RelayBoundaryFanout,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        remote_runtime_consumers: Vec<RemoteRuntimeConsumer>,
        remote_dispatcher: Option<StdArc<RemoteDispatcher>>,
        branch_presence: RelayBranchPresence,
    ) -> Self {
        Self {
            fanout,
            attached_runtime_consumer_count: AtomicUsize::new(attached_runtime_consumer_count),
            detached_runtime_consumer_count: AtomicUsize::new(detached_runtime_consumer_count),
            remote_runtime_consumers: ArcSwap::from_pointee(remote_runtime_consumers),
            remote_dispatcher,
            owner_node: ArcSwapOption::empty(),
            branch_presence,
            channels: RelayChannels::default(),
        }
    }

    #[cfg(test)]
    pub(super) fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        self.fanout.subscription_receiver()
    }

    pub(super) fn replace_owner_node(&self, owner_node: Option<ClusterNodeName>) {
        if self.owner_node.load().as_deref() == owner_node.as_ref() {
            return;
        }
        self.channels.replace_owner();
        self.owner_node.store(owner_node.map(StdArc::new));
    }

    pub(super) fn dispatch_is_fenced(&self) -> bool {
        self.fanout.dispatch_is_fenced()
    }

    pub(super) fn is_owned_by(&self, node_id: Option<&ClusterNodeName>) -> bool {
        let owner_node = self.owner_node.load();
        owner_node
            .as_deref()
            .is_none_or(|owner| Some(owner) == node_id)
    }

    pub(super) fn activate_owner_buffer(&self, metrics: RelayMetricsHandle) -> RelayRuntimeFanIn {
        RelayRuntimeFanIn::new(self.fanout.activate_owner_buffer(metrics))
    }

    pub(super) fn deactivate_owner_buffer(&self) {
        self.fanout.deactivate_owner_buffer();
    }

    pub(super) fn ingress_slot(&self, branch: &Option<BranchKey>) -> StdArc<RelayOutboundSlot> {
        self.channels.bind(branch).ingress()
    }

    pub(super) fn outbound_slot(
        &self,
        node_id: &ClusterNodeName,
        relay: &RelayName,
        kind: RelayPayloadKind,
        branch: &Option<BranchKey>,
    ) -> StdArc<RelayOutboundSlot> {
        self.channels.bind(branch).outbound(RelayOutboundChannel {
            node_id: node_id.clone(),
            relay: relay.clone(),
            kind,
        })
    }

    pub(super) fn subscription_channels(
        &self,
        branch: &Option<BranchKey>,
        snapshot: StdArc<crate::runtime::SubscriptionInterestIndex>,
        domain: &DomainName,
        relay: &RelayName,
    ) -> StdArc<channels::SubscriptionChannels> {
        self.channels
            .bind(branch)
            .subscriptions(snapshot, domain, relay)
    }

    pub(super) fn retire_channels(&self) {
        self.channels.retire_all();
    }

    pub(super) fn remove_branch_slots(&self, branch: &Option<BranchKey>) {
        self.channels.retire(branch);
    }

    pub(super) async fn enqueue_owner_batch(
        &self,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let dispatch_gate = self.fanout.dispatch_gate();
        let _dispatch_permit = dispatch_gate.acquire_dispatch().await;
        let _branch_dispatch_permits = self.fanout.acquire_branch_dispatch_gates(&batch.key).await;
        let Some(buffer) = self.fanout.owner_buffer() else {
            for ack in batch.acks.iter() {
                ack.no_ack("relay owner buffer is unavailable");
            }
            return Err(Box::new(batch.clone()));
        };
        let admission = self.fanout.begin_owner_admission();
        if let Err(error) = buffer.batches.broadcast(batch.attached()).await {
            for ack in error.batch.acks.iter() {
                ack.no_ack("relay owner buffer is unavailable");
            }
            return Err(Box::new(batch.clone()));
        }
        admission.accept();
        buffer.observe_length();
        Ok(())
    }

    pub(super) fn begin_owner_batch_completion(&self) -> RelayOwnerBatchCompletion {
        self.fanout.begin_owner_batch_completion()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the retained channel serializes sequence and publication changes",
            key = "delivery channel incarnation and sequence",
            bound = "one admitted delivery is serialized against channel replacement and its \
                     dispatch gate"
        )
    )]
    pub(super) async fn dispatch_to_owner(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.dispatch_to_owner_on(domain, relay, batch, self.ingress_slot(&batch.key))
            .await
    }

    async fn dispatch_to_owner_on(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        batch: &RelayRecordBatch,
        ingress_slot: StdArc<RelayOutboundSlot>,
    ) -> RelayDispatchResult {
        let dispatch_gate = self.fanout.dispatch_gate();
        let _dispatch_permit = dispatch_gate.acquire_dispatch().await;
        let _branch_dispatch_permits = self.fanout.acquire_branch_dispatch_gates(&batch.key).await;
        let Some(owner_node) = self.owner_node.load_full() else {
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
        let Some(_slot) = ingress_slot.lock_for_delivery().await else {
            for ack in &batch.acks {
                ack.no_ack("relay channel was retired");
            }
            return Err(Box::new(batch.clone()));
        };
        let batch_ipc = match batch.batch.encode_arrow_ipc(dispatcher.executor()).await {
            Ok(bytes) => bytes,
            Err(error) => {
                for ack in batch.acks.iter() {
                    ack.no_ack(error.to_string());
                }
                return Err(Box::new(batch.clone()));
            }
        };
        let delivery = ingress_slot.next_delivery();
        let mut remote_acks = Vec::with_capacity(batch.acks.len());
        for ack in &batch.acks {
            if ack.is_empty() {
                remote_acks.push(None);
                continue;
            }
            let registration =
                dispatcher.register_pending_ack(&owner_node, RemoteDispatcher::forwarded_ack(ack));
            remote_acks.push(Some(registration));
        }
        let admission_result = dispatcher
            .dispatch_admitted_relay_payload(
                &owner_node,
                RelayPayload {
                    delivery,
                    kind: RelayPayloadKind::Ingress,
                    domain: domain.clone(),
                    relay: relay.clone(),
                    key: BranchKey::to_remote_key(&batch.key),
                    batch_ipc,
                    metadata: batch.metadata.to_remote(),
                    acks: remote_acks.clone(),
                    admission: None,
                },
                &ingress_slot,
            )
            .await;
        if let Err(error) = admission_result {
            let reason = error.to_string();
            for registration in remote_acks.iter().flatten() {
                dispatcher.clear_pending_ack(registration);
            }
            for ack in batch.acks.iter() {
                ack.no_ack(reason.clone());
            }
            return Err(Box::new(batch.clone()));
        }
        dispatcher.admit_pending_acks(&remote_acks);
        Ok(())
    }

    pub(super) fn observe_owner_buffer_length(&self, metrics: &RelayMetricsHandle) {
        let Some((len, capacity)) = self.fanout.owner_buffer_len() else {
            return;
        };
        metrics.observe_buffer(len, capacity);
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
        dispatch: RuntimeConsumerDispatch,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout
            .dispatch_runtime_consumers(
                dispatch,
                self.attached_runtime_consumer_count.load(Ordering::Acquire),
                self.detached_runtime_consumer_count.load(Ordering::Acquire),
                batch,
            )
            .await
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the retained channel serializes sequence and publication changes",
            key = "delivery channel incarnation and sequence",
            bound = "one admitted delivery is serialized against channel replacement and its \
                     dispatch gate"
        )
    )]
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
            nervix_primitives::task::consume_budget().await;
            let outbound_slot = self.outbound_slot(
                &consumer.node_id,
                &consumer.relay,
                RelayPayloadKind::Routed,
                &batch.key,
            );
            let Some(_slot) = outbound_slot.lock_for_delivery().await else {
                for ack in batch.acks.iter() {
                    ack.no_ack("relay channel lifetime ended");
                }
                return Err(Box::new(batch.clone()));
            };
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
                remote_batch
                    .acks
                    .iter()
                    .map(|ack| {
                        Some(dispatcher.register_pending_ack(&consumer.node_id, ack.clone()))
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![None; remote_batch.acks.len()]
            };
            let delivery = outbound_slot.next_delivery();
            let result = dispatcher
                .dispatch_admitted_relay_payload(
                    &consumer.node_id,
                    routed_payload(RoutedDelivery {
                        delivery,
                        domain,
                        consumer,
                        batch: &remote_batch,
                        batch_ipc: batch_ipc.clone(),
                        acks: remote_acks.clone(),
                    }),
                    &outbound_slot,
                )
                .await;

            match (consumer.mode, result) {
                (AckMode::Attached, Ok(())) => {
                    dispatcher.admit_pending_acks(&remote_acks);
                }
                (AckMode::Attached, Err(error)) => {
                    let reason = error.to_string();
                    for (ack_set, remote_ack) in remote_batch.acks.iter().zip(remote_acks.iter()) {
                        if let Some(remote_ack) = remote_ack {
                            dispatcher.clear_pending_ack(remote_ack);
                        }
                        ack_set.no_ack(reason.clone());
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
        if *self.remote_runtime_consumers.load().as_ref() == consumers {
            return;
        }
        self.channels.replace_destinations();
        self.remote_runtime_consumers.store(StdArc::new(consumers));
    }

    pub(super) async fn fanout_owner_batch(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        batch: &RelayRecordBatch,
        fault_injection: &ConfiguredFaultInjection,
    ) -> RelayDispatchResult {
        fault_injection
            .pause_owner_relay_fanout_if_armed(domain)
            .await;
        let gate = self.fanout.dispatch_gate();
        let Some(_dispatch_permit) = gate.try_acquire_dispatch() else {
            for ack in batch.acks.iter() {
                ack.no_ack("relay routing changed before owner fan-out");
            }
            batch.ack_success();
            return Err(Box::new(batch.clone()));
        };
        self.fanout_local_subscriptions(batch).await;
        self.fanout_remote_subscriptions(domain, relay, batch).await;
        if let Err(failed) = self
            .dispatch_local_runtime_consumers(RuntimeConsumerDispatch::Owner, batch)
            .await
        {
            batch.ack_success();
            return Err(failed);
        }
        let result = self.dispatch_remote_runtime_consumers(domain, batch).await;
        batch.ack_success();
        result
    }

    /// Hands a batch another node's relay owner routed here to the runtime consumers this node runs
    /// for the relay. A batch that carries record acknowledgements fails them when no attached
    /// consumer of the relay runs here any more, so its source delivers it again along the owner's
    /// current routes rather than committing a record no consumer processed.
    pub(super) async fn inject_remote_message(
        &self,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        self.fanout_local_subscriptions(batch).await;
        self.dispatch_local_runtime_consumers(RuntimeConsumerDispatch::Routed, batch)
            .await
    }
}

impl ConcreteRelayRuntime {
    pub(super) fn new(build: ConcreteRelayRuntimeBuild) -> Self {
        let ConcreteRelayRuntimeBuild {
            key,
            runtime,
            domain,
            relay,
            services,
        } = build;
        let channels = RelayProducerChannels::Bound(services.channels.bind(&key));
        Self {
            runtime,
            domain,
            relay,
            services,
            key,
            channels,
        }
    }

    pub(super) fn retire(&mut self) {
        self.channels = RelayProducerChannels::Ended;
    }

    pub(super) async fn dispatch_boundary(
        &mut self,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        debug_assert_eq!(&self.key, &batch.key);
        let RelayProducerChannels::Bound(channels) = &mut self.channels else {
            for ack in batch.acks.iter() {
                ack.no_ack("relay producer lifetime ended");
            }
            return Err(Box::new(batch.clone()));
        };
        // The receiver's relay branch may expire independently of this producer. Its explicit
        // invalidation is a cold rebind; ending this producer never permits such a rebind.
        if channels.is_retired() {
            *channels = self.services.channels.bind(&self.key);
            if channels.is_retired() {
                for ack in batch.acks.iter() {
                    ack.no_ack("relay service lifetime ended");
                }
                return Err(Box::new(batch.clone()));
            }
        }
        if self.runtime.owns_relay(&self.services) {
            return self.services.enqueue_owner_batch(batch).await;
        }
        self.services
            .dispatch_to_owner_on(&self.domain, &self.relay, batch, channels.ingress())
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

impl Runtime {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "retained relay handles route each admitted record and keep channel ordering \
                      and fences"
        )
    )]
    pub(in crate::runtime) fn invalidate_branch_relay_generation(
        &self,
        domain: &DomainName,
        key: &Option<BranchKey>,
    ) {
        let Some(routing) = self.domain_routing(domain) else {
            return;
        };
        let routing = routing.load();
        for services in routing.relay_services.values() {
            services.remove_branch_slots(key);
        }
    }

    /// Whether this node owns the relay `services` serve. A relay whose schedule names no owner is
    /// owned wherever it runs.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "retained relay handles route each admitted record and keep channel ordering \
                      and fences"
        )
    )]
    pub(in crate::runtime) fn owns_relay(&self, services: &RelayBoundaryServices) -> bool {
        let dispatcher = self.inner.remote_dispatcher.load();
        services.is_owned_by(dispatcher.as_deref().map(RemoteDispatcher::local_node_id))
    }

    pub(super) async fn fanout_relay_owner_batch(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        services: &RelayBoundaryServices,
        branches: &mut RelayOwnerBranchState,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let now = match branches.expiration_time(self, domain) {
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
        let physical_node_id = branches.physical_node_id.as_ref();
        let admission = branches
            .instances
            .admit(
                batch.key.as_ref(),
                now,
                branches.capacity,
                |branch_key, _| {
                    Ok::<RelayMetricRecorders, std::convert::Infallible>(
                        self.inner.metrics.resolve_relay_metric_recorders(
                            domain,
                            relay,
                            physical_node_id,
                            RELAY_BUFFER_DIRECTION_CONCRETE,
                            Some(branch_key.as_str()),
                        ),
                    )
                },
            )
            .assured("the metrics resolver's error type is Infallible");
        let metrics = match admission.branch {
            Some(recorders) => RelayMetricsHandle::from_recorders(recorders),
            None => branches.global_metrics.clone(),
        };
        for (evicted_key, _) in admission.evicted {
            self.invalidate_branch_relay_generation(domain, &Some(evicted_key));
        }
        metrics.observe_batch(
            batch.message_count(),
            batch.estimated_bytes(),
            batch.domain_timestamp(),
        );
        services.observe_owner_buffer_length(&metrics);
        let result = services
            .fanout_owner_batch(domain, relay, batch, &self.inner.fault_injection)
            .await;
        self.inner
            .fault_injection
            .mark_owner_relay_fanout_complete(domain);
        result
    }

    /// The branch presence of `relay`, kept with the relay-wide materialized state in the lifetime
    /// the committed schedule publishes for it, so materialized reads observe the membership of
    /// whichever owner the relay has on this node.
    pub(in crate::runtime) fn relay_branch_presence(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> error_stack::Result<RelayBranchPresence, StateIdentityError> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        )?;
        if let Some(existing) = self.inner.relay_branch_presences.get(&placement) {
            return Ok(existing.clone());
        }
        let presence = Arc::new(BranchPresence::new());
        self.inner
            .relay_branch_presences
            .insert(placement, presence.clone());
        Ok(presence)
    }

    pub(in crate::runtime) fn clear_relay_branch_presences_for_domain(&self, domain: &DomainName) {
        let relays = self
            .inner
            .relay_branch_presences
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|placement| &placement.domain == domain)
            .collect::<Vec<_>>();
        for placement in relays {
            self.inner.relay_branch_presences.remove(&placement);
        }
    }

    /// Attaches a session subscriber to `relay`, provided this node executes the relay with the
    /// definition the subscriber describes its rows by. The receiver ends as soon as the relay is
    /// redefined or withdrawn, before any batch of another definition could reach it.
    pub(crate) async fn subscribe_stream(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        expected: &RelaySubscriptionDefinition,
    ) -> Result<RelaySubscriptionReceiver<RelayRecordBatch>, RuntimeError> {
        let Some(routing) = self.domain_routing(domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        let routing = routing.load();
        let Some(services) = routing.relay_services.get(relay) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        match services.fanout.subscriptions().attach(expected) {
            Ok(receiver) => Ok(receiver),
            Err(RelaySubscriptionRefusal::NotDeclared) => Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            }),
            Err(RelaySubscriptionRefusal::Redefined) => Err(RuntimeError::RelayRedefined {
                domain: domain.clone(),
                relay: relay.clone(),
            }),
        }
    }

    pub(in crate::runtime) fn spawn_relay_owner_task(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        services: Arc<RelayBoundaryServices>,
        retention: RelayRetention,
    ) -> RelayOwnerTask {
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let dispatcher = self.inner.remote_dispatcher.load();
        let physical_node_id = dispatcher
            .as_deref()
            .map(RemoteDispatcher::local_node_id)
            .cloned();
        let global_metrics = self.inner.metrics.resolve_relay_metrics(
            domain,
            relay,
            physical_node_id.as_ref(),
            RELAY_BUFFER_DIRECTION_CONCRETE,
            None,
        );
        let mut receiver = services.activate_owner_buffer(global_metrics.clone());
        let runtime = self.clone();
        let domain = domain.clone();
        let relay = relay.clone();
        let RelayRetention {
            branch_ttl,
            branch_capacity,
        } = retention;
        let expiration_scan_interval = self.inner.branch_instance_expiration_scan_interval;
        // Claiming before the task starts makes this owner the presence's only publisher from here
        // on: a predecessor that is still ending can no longer change what observers read.
        let instances = OwnedBranches::claim(services.branch_presence.clone());
        let task = nervix_primitives::task::spawn(async move {
            let mut branches = RelayOwnerBranchState {
                instances,
                global_metrics,
                physical_node_id,
                capacity: branch_capacity,
                domain_clock: None,
            };
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
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
                        let now = match branches.expiration_time(&runtime, &domain) {
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
                            runtime.invalidate_branch_relay_generation(&domain, &Some(expired_key));
                        }
                        next_expiration_scan = Instant::now() + expiration_scan_interval;
                    }
                }
            }

            loop {
                nervix_primitives::task::consume_budget().await;
                let batch = match receiver.try_recv() {
                    RelayTryRecv::Batch(batch) => batch,
                    RelayTryRecv::Empty | RelayTryRecv::Closed => {
                        break;
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
            services.observe_owner_buffer_length(&branches.global_metrics);
            services.deactivate_owner_buffer();
            // Releasing the owner's branches publishes an empty presence before the relay's metrics
            // are removed, unless a successor already claimed the presence.
            drop(branches);
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
        let task = nervix_primitives::task::spawn(async move {
            let interaction_input = RelayInteractionInput::immediate(relay.clone(), receiver);
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
            let freeze_watch = OwnershipHandoffFreezeWatch::new(&runtime, ownership_entity);
            let clock = match runtime.bind_domain_clock(&domain) {
                Ok(clock) => clock,
                Err(error) => {
                    runtime.events().report_error(format!(
                        "materialized relay '{}' in domain '{}' could not bind its clock: {error}",
                        relay.as_str(),
                        domain.as_str()
                    ));
                    return;
                }
            };
            let mut restored_branches = state.read().restored_branch_watermarks();
            restored_branches.sort_by_key(|(_, last_ingestion)| *last_ingestion);
            for (key, last_ingestion) in restored_branches {
                branch_instances.insert_changed(key, last_ingestion, ());
            }
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            'state_task: loop {
                nervix_primitives::task::consume_budget().await;
                if !freeze_watch.observe().is_frozen()
                    && let Some(branch_ttl) = branch_ttl
                    && Instant::now() >= next_expiration_scan
                {
                    let now = match clock.snapshot().map(|snapshot| snapshot.now()) {
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
                        nervix_primitives::task::consume_budget().await;
                        runtime.invalidate_branch_relay_generation(&domain, &key);
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
                let expiration_sleep = if freeze_watch.observe().is_frozen() {
                    Some(OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL)
                } else {
                    branch_ttl.map(|_| {
                        next_expiration_scan
                            .checked_duration_since(Instant::now())
                            .unwrap_or(Duration::ZERO)
                    })
                };
                let mut wake = RuntimeWake::never();
                if let Some(sleep) = expiration_sleep {
                    match RuntimeWake::after(sleep) {
                        Ok(scheduled) => wake = scheduled,
                        Err(error) => {
                            warn!(
                                domain = domain.as_str(),
                                relay = relay.as_str(),
                                error = %error,
                                "materialized relay state task could not schedule its next scan"
                            );
                            break 'state_task;
                        }
                    }
                }
                let work = match interaction.next(wake).await {
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
                let now = match clock.snapshot().map(|snapshot| snapshot.now()) {
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
                    .get_or_try_create_with(branch_key.clone(), now, |_, _| {
                        Ok::<(), std::convert::Infallible>(())
                    })
                    .assured("the tracking closure's error type is Infallible");
                if let Some(branch_capacity) = branch_capacity {
                    for (evicted_key, _) in branch_instances.evict_lru_to_capacity(branch_capacity)
                    {
                        nervix_primitives::task::consume_budget().await;
                        runtime.invalidate_branch_relay_generation(&domain, &evicted_key);
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
                        let error = error_and_batch.error;
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
                let records = messages.into_iter().map(|message| message.record);
                if let Err(error) = runtime
                    .apply_materialized_stream_records(&state, &branch_key, records)
                    .await
                {
                    warn!(
                        domain = domain.as_str(),
                        relay = relay.as_str(),
                        error = %error,
                        "materialized relay assignment changed while applying a batch"
                    );
                    break 'state_task;
                }
            }
        });
        RelayStateTask { shutdown, task }
    }
}

#[cfg(test)]
#[path = "relay_boundary_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "relay_branch_presence_shuttle_tests.rs"]
mod branch_presence_shuttle_tests;
