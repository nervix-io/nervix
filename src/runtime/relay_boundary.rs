//! Per-relay execution boundaries and branch-local runtime ownership.
//!
//! Layer: data plane.
//!
//! - **Owns.** Relay fan-out, branch-local ingress and outbound ordering, concrete branch
//!   retention, owner tasks, and materialized relay state.
//! - **Depends on.** Validated execution graphs, compiled schemas, execution admission, and the
//!   interconnect dispatcher.
//! - **Must not know.** Plexus text, transactions, consensus operations, or connector internals.

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

use super::{
    relay_channel::{RelayAudience, RelayConsumerSelection, RelayPublication},
    *,
};

#[path = "relay_channels.rs"]
mod channels;
use channels::{RelayBranchChannels, RelayChannels};

#[path = "relay_boundary_routed.rs"]
mod routed;
pub(super) use routed::{
    RemoteRuntimeConsumer, RoutedDelivery, RuntimeConsumerDispatch, routed_payload,
};

#[cfg(all(test, feature = "shuttle"))]
#[path = "relay_boundary_shuttle_tests.rs"]
mod shuttle_tests;

pub(super) const RELAY_BUFFER_DIRECTION_CONCRETE: &str = "concrete";
/// A channel that carried no batch this long, counted from its opening or its last batch, opens a
/// fresh incarnation for its next batch. It stays below the receiver's watermark retention, so a
/// channel its sender resumes still finds its watermark there, and a receiver's sweep of watermarks
/// unreconciled for the whole retention raises its retired-channel floor over a live channel's next
/// batch only when that batch is delayed for longer than the difference.
const RELAY_CHANNEL_IDLE_ROTATION: Duration = Duration::from_secs(300);
const _: () = assert!(
    RELAY_CHANNEL_IDLE_ROTATION.as_nanos()
        < nervix_interconnect::RELAY_CHANNEL_RETENTION.as_nanos(),
    "an idle relay channel rotates before its receiver's watermark retention ends"
);

/// A count Plexus configures, narrowed to the width this node addresses memory with.
///
/// Both halves of the narrowing hold before it runs: the Models keep these counts non-zero, and
/// the supported targets address memory at least as wide as the `u64` they are written as.
pub(super) fn addressable_count(configured: NonZeroU64) -> NonZeroUsize {
    NonZeroUsize::new(configured.get().arch_into())
        .assured("a non-zero configured count is still non-zero at this target's pointer width")
}

/// The concrete branches this node's owner of one unit of a relay holds, as `DESCRIBE`,
/// materialized reads and the console's graph observe them. The unit's owner task alone changes it,
/// and only when a branch appears, is evicted or expires, or the owner starts or stops.
pub(super) type RelayBranchPresence = Arc<BranchPresence<BranchKey>>;

/// The branch presence of every unit of one relay, and how the relay divides its keys among those
/// units, fixed when the relay's services are built. A key's branch is present only in the
/// presence of the unit that owns it.
#[derive(Debug, Clone)]
pub(super) struct RelayBranchPresences {
    partitioning: RelayPartitioning,
    by_unit: BTreeMap<Unit, RelayBranchPresence>,
}

impl RelayBranchPresences {
    pub(super) fn new(
        partitioning: RelayPartitioning,
        by_unit: BTreeMap<Unit, RelayBranchPresence>,
    ) -> Self {
        Self {
            partitioning,
            by_unit,
        }
    }

    /// The presence of a relay that is not divided into partitions: its one `Single` unit's.
    #[cfg(test)]
    pub(super) fn single(presence: RelayBranchPresence) -> Self {
        Self {
            partitioning: RelayPartitioning::Whole,
            by_unit: BTreeMap::from([(Unit::SINGLE, presence)]),
        }
    }

    /// One fresh presence for each unit of a relay divided as `partitioning`, for a relay this
    /// node does not own.
    pub(super) fn fresh(partitioning: RelayPartitioning) -> Self {
        Self {
            partitioning,
            by_unit: partitioning
                .units()
                .into_iter()
                .map(|unit| (unit, Arc::new(BranchPresence::new())))
                .collect(),
        }
    }

    /// How the relay divides its keys among the units these presences belong to.
    pub(super) fn partitioning(&self) -> RelayPartitioning {
        self.partitioning
    }

    pub(super) fn of(&self, unit: Unit) -> Option<&RelayBranchPresence> {
        self.by_unit.get(&unit)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&Unit, &RelayBranchPresence)> {
        self.by_unit.iter()
    }
}

/// Where the owner of each unit of one relay runs, as the committed schedule places it. A unit the
/// table does not name is owned wherever the relay runs, as on a graph no schedule has placed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct RelayOwnerRoutes {
    by_unit: BTreeMap<Unit, ClusterNodeName>,
}

impl RelayOwnerRoutes {
    pub(super) fn new(by_unit: BTreeMap<Unit, ClusterNodeName>) -> Self {
        Self { by_unit }
    }

    pub(super) fn owner_of(&self, unit: Unit) -> Option<&ClusterNodeName> {
        self.by_unit.get(&unit)
    }

    /// The owner table of a relay that is not divided into partitions, owned on `owner`.
    #[cfg(test)]
    pub(super) fn single(owner: ClusterNodeName) -> Self {
        Self {
            by_unit: BTreeMap::from([(Unit::SINGLE, owner)]),
        }
    }

    /// Whether a committed schedule places the relay's owners at all. Every unit of a relay is
    /// placed by the same schedule, so either every unit has an owner here or none has.
    pub(super) fn is_placed(&self) -> bool {
        !self.by_unit.is_empty()
    }

    /// The units whose owner differs between this table and `other`.
    fn changed_units(&self, other: &Self) -> Vec<Unit> {
        let mut units = self.by_unit.keys().copied().collect::<BTreeSet<_>>();
        units.extend(other.by_unit.keys().copied());
        units
            .into_iter()
            .filter(|unit| self.by_unit.get(unit) != other.by_unit.get(unit))
            .collect()
    }
}

pub(super) struct ConcreteRelayRuntime {
    pub(super) key: Option<BranchKey>,
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) relay: RelayName,
    pub(super) services: Arc<RelayBoundaryServices>,
    /// The unit of the relay that owns this producer's branch, resolved once when the producer is
    /// bound, or why the branch names none.
    unit: Result<Unit, RelayPartitionError>,
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
    unit_runtime_consumers: BTreeMap<Unit, UnitRuntimeConsumers>,
    pub(super) remote_runtime_consumers: ArcSwap<Vec<RemoteRuntimeConsumer>>,
    pub(super) remote_dispatcher: Option<StdArc<RemoteDispatcher>>,
    /// How the relay's concrete keys are divided among its units, as its branch declares.
    pub(super) partitioning: RelayPartitioning,
    /// Where each unit's owner runs, published whole whenever the committed schedule moves one.
    pub(super) owners: ArcSwap<RelayOwnerRoutes>,
    /// The concrete branches this node's owner of each unit holds, shared with the unit's state
    /// placement so materialized reads observe the same membership across execution rebuilds.
    pub(super) branch_presences: RelayBranchPresences,
    /// Record ACK roots retain this domain drain tracker after the relay service's borrow ends.
    pub(super) domain_ack_tracker: Arc<AckRootTracker>,
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
    /// A version 7 UUID. Every one this process creates sorts after the ones it created before,
    /// because uuid orders them through one process-wide context, and a receiver relies on that
    /// order to refuse a channel it forgot.
    channel_incarnation: [u8; 16],
    next_sequence: u64,
    /// When the incarnation opened or last carried a batch. Idleness counts from the opening too,
    /// so a channel that opened and carried nothing never sends its first batch under an
    /// incarnation older than the idle rotation.
    last_active_at: Instant,
}

impl RelayOutboundSequence {
    fn opened_at(now: Instant) -> Self {
        Self {
            channel_incarnation: uuid::Uuid::now_v7().into_bytes(),
            next_sequence: 0,
            last_active_at: now,
        }
    }

    fn reopen(&mut self, now: Instant) {
        *self = Self::opened_at(now);
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
            sequence: nervix_primitives::sync::blocking::Mutex::new(
                RelayOutboundSequence::opened_at(Instant::now()),
            ),
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
        let mut channel = self.sequence.lock();
        channel.reopen(Instant::now());
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
        // Activity recorded after `now` was read, such as a reopening, leaves no idle time.
        if let Some(idle) = now.checked_duration_since(channel.last_active_at)
            && idle >= RELAY_CHANNEL_IDLE_ROTATION
        {
            channel.reopen(now);
        }
        let sequence = channel.next_sequence;
        channel.next_sequence = sequence
            .checked_add(1)
            .assured("a process cannot deliver u64::MAX batches before a channel rotates");
        channel.last_active_at = channel.last_active_at.max(now);
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
    pub(super) unit_runtime_consumer_counts: BTreeMap<Unit, (usize, usize)>,
    pub(super) branch_presences: RelayBranchPresences,
    pub(super) remote_runtime_consumers: Vec<RemoteRuntimeConsumer>,
}

#[derive(Debug)]
pub(super) struct RelayConsumerFanout {
    pub(super) dispatch_gate: Arc<RelayDispatchGate>,
    /// Branch-scoped fences published under a short whole-relay fence. Dispatch reads one immutable
    /// set and takes permits only from entries selecting its branch.
    branch_dispatch_gates: Arc<BranchRelayDispatchGates>,
    /// The owner buffer of every unit of this relay this node owns. Each owner task publishes its
    /// unit's buffer when it starts and withdraws it when it ends, and producers read the buffer of
    /// their unit from the published table.
    pub(super) owner_buffers: ArcSwap<RelayOwnerBuffers>,
    pub(super) owner_capacity: AtomicUsize,
    pub(super) transit: Arc<RelayTransit>,
    pub(super) subscriptions: RelaySubscriptions,
    pub(super) attached_runtime_consumers: RelayBroadcast<RelayRecordBatch>,
    pub(super) detached_runtime_consumers: RelayBroadcast<RelayRecordBatch>,
    /// Lifecycle-only registration. Delivery retains the selected audience in its services.
    unit_audiences:
        nervix_primitives::sync::blocking::Mutex<BTreeMap<Unit, Arc<UnitRuntimeAudiences>>>,
}

#[derive(Debug)]
struct UnitRuntimeAudiences {
    dispatch_gate: Arc<RelayDispatchGate>,
    transit: Arc<RelayTransit>,
    attached: RelayAudience<RelayRecordBatch>,
    detached: RelayAudience<RelayRecordBatch>,
}

#[derive(Debug)]
struct UnitRuntimeConsumers {
    audiences: Arc<UnitRuntimeAudiences>,
    attached: AtomicUsize,
    detached: AtomicUsize,
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

/// The owner buffers one node holds for the units of a relay it owns, keyed by unit and published
/// whole. A relay without partitions holds at most its `Single` unit's buffer; a partitioned relay
/// holds one buffer, of the relay's whole `CAPACITY`, for each partition this node owns.
#[derive(Debug, Default)]
pub(super) struct RelayOwnerBuffers {
    by_unit: BTreeMap<Unit, StdArc<RelayOwnerBuffer>>,
}

impl RelayOwnerBuffers {
    fn get(&self, unit: Unit) -> Option<&StdArc<RelayOwnerBuffer>> {
        self.by_unit.get(&unit)
    }

    fn with(&self, unit: Unit, buffer: StdArc<RelayOwnerBuffer>) -> Self {
        let mut by_unit = self.by_unit.clone();
        by_unit.insert(unit, buffer);
        Self { by_unit }
    }

    /// This table without `unit`'s buffer, when the published buffer is still `buffer`.
    fn without(&self, unit: Unit, buffer: &StdArc<RelayOwnerBuffer>) -> Self {
        let mut by_unit = self.by_unit.clone();
        if by_unit
            .get(&unit)
            .is_some_and(|published| StdArc::ptr_eq(published, buffer))
        {
            by_unit.remove(&unit);
        }
        Self { by_unit }
    }

    fn buffers(&self) -> std::collections::btree_map::Values<'_, Unit, StdArc<RelayOwnerBuffer>> {
        self.by_unit.values()
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
            owner_buffers: ArcSwap::from_pointee(RelayOwnerBuffers::default()),
            owner_capacity: AtomicUsize::new(capacity.get()),
            transit: Arc::new(RelayTransit::default()),
            subscriptions: RelaySubscriptions::new(),
            attached_runtime_consumers: RelayBroadcast::with_capacity(dispatch_capacity),
            detached_runtime_consumers: RelayBroadcast::with_capacity(dispatch_capacity),
            unit_audiences: Default::default(),
        }
    }

    #[cfg(test)]
    pub(super) fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        self.subscriptions.receivers().new_receiver()
    }

    pub(super) fn set_capacity(&self, capacity: NonZeroUsize) {
        self.owner_capacity.store(capacity.get(), Ordering::Release);
        let owner_buffers = self.owner_buffers.load();
        for buffer in owner_buffers.buffers() {
            buffer.set_capacity(capacity);
        }
    }

    /// Publishes a buffer for `unit`, of the relay's current capacity, and returns the receiver its
    /// owner task drains together with the buffer it withdraws when it ends.
    pub(super) fn activate_owner_buffer(
        &self,
        unit: Unit,
        metrics: RelayMetricsHandle,
    ) -> (RelayRuntimeConsumerReceiver, StdArc<RelayOwnerBuffer>) {
        let capacity = NonZeroUsize::new(self.owner_capacity.load(Ordering::Acquire))
            .verified("relay capacity is validated as nonzero before it is stored");
        let buffer = StdArc::new(RelayOwnerBuffer::with_capacity(capacity, metrics));
        let receiver = buffer.new_receiver();
        self.owner_buffers
            .rcu(|current| current.with(unit, buffer.clone()));
        (receiver, buffer)
    }

    /// Withdraws `unit`'s buffer while it is still `buffer`, so an ending owner never withdraws the
    /// buffer a successor of the same unit already published.
    pub(super) fn deactivate_owner_buffer(&self, unit: Unit, buffer: &StdArc<RelayOwnerBuffer>) {
        self.owner_buffers
            .rcu(|current| current.without(unit, buffer));
    }

    /// The owner buffer of `unit` held in full, for a caller that keeps it across an await.
    pub(super) fn owner_buffer(&self, unit: Unit) -> Option<StdArc<RelayOwnerBuffer>> {
        self.owner_buffers.load().get(unit).cloned()
    }

    pub(super) fn owner_buffer_len(&self, unit: Unit) -> Option<(usize, usize)> {
        let owner_buffers = self.owner_buffers.load();
        owner_buffers
            .get(unit)
            .map(|buffer| (buffer.len(), buffer.capacity()))
    }

    #[cfg(all(test, any(feature = "shuttle", feature = "loom")))]
    pub(super) fn begin_owner_admission(&self) -> RelayOwnerAdmission {
        RelayOwnerAdmission::new(self.transit.clone())
    }

    #[cfg(all(test, any(feature = "shuttle", feature = "loom")))]
    pub(super) fn begin_owner_batch_completion(&self) -> RelayOwnerBatchCompletion {
        RelayOwnerBatchCompletion::new(self.transit.clone())
    }

    pub(super) fn runtime_consumer_receiver_for_mode(
        &self,
        mode: AckMode,
    ) -> RelayRuntimeConsumerReceiver {
        self.runtime_consumer_broadcast_for_mode(mode)
            .new_receiver()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "unit registration retains its audiences before delivery starts"
        )
    )]
    fn unit_audiences(&self, unit: Unit) -> Arc<UnitRuntimeAudiences> {
        let mut audiences = self.unit_audiences.lock();
        audiences
            .entry(unit)
            .or_insert_with(|| {
                Arc::new(UnitRuntimeAudiences {
                    dispatch_gate: Arc::new(RelayDispatchGate::new()),
                    transit: Arc::new(RelayTransit::within(self.transit.clone())),
                    attached: self.attached_runtime_consumers.new_audience().assured(
                        "each audience is retained in memory before another identity is allocated",
                    ),
                    detached: self.detached_runtime_consumers.new_audience().assured(
                        "each audience is retained in memory before another identity is allocated",
                    ),
                })
            })
            .clone()
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
        self.transit
            .held()
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
        self.dispatch_publications(
            dispatch,
            attached_runtime_consumer_count,
            detached_runtime_consumer_count,
            self.attached_runtime_consumers.publication(),
            || self.detached_runtime_consumers.publication(),
            batch,
        )
        .await
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the two local publication callbacks borrow their retained \
                                   common or unit fan-out and only read its immutable \
                                   registration snapshot")
    )]
    async fn dispatch_publications<'a, Selection: RelayConsumerSelection<RelayRecordBatch>>(
        &self,
        dispatch: RuntimeConsumerDispatch,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        attached: RelayPublication<'_, RelayRecordBatch, Selection>,
        detached_publication: impl Fn() -> RelayPublication<'a, RelayRecordBatch, Selection>,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        // The shares reserved for the attached consumers and the deliveries to them come from the
        // same registration snapshot, so a consumer that registers meanwhile never receives the
        // batch without a share of its own.
        let attached_receiver_count = attached.receivers();
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
            let shares = batch.attached_for_receivers(attached_receiver_count);
            if let Err(error) = attached.broadcast(shares).await {
                let failed = error.batch;
                for ack in failed.acks.iter() {
                    ack.no_ack("runtime consumer unavailable for attached delivery");
                }
                return Err(Box::new(batch.clone()));
            }
        }

        // Detached observation and publication both start after attached admission. Keep their
        // separate reads so a consumer joining between the count and broadcast receives the batch.
        let detached_receiver_count = detached_publication().receivers();
        if detached_runtime_consumer_count > 0
            && detached_receiver_count < detached_runtime_consumer_count
        {
            warn!("detached runtime consumer receiver is unavailable");
        }
        if detached_receiver_count > 0
            && let Err(error) = detached_publication().broadcast(batch.detached()).await
        {
            warn!(
                error = %error,
                "detached runtime consumer relay broadcast failed"
            );
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
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "a selected-unit drain observes retained transit and dedicated consumer \
                      queues"
        )
    )]
    pub(super) fn unit_outstanding_work_len(&self, units: &BTreeSet<Unit>) -> usize {
        let audiences = self.consumers().unit_audiences.lock();
        units
            .iter()
            .filter_map(|unit| audiences.get(unit))
            .map(|audience| {
                audience
                    .transit
                    .held()
                    .checked_add(audience.attached.dedicated_len())
                    .and_then(|count| count.checked_add(audience.detached.dedicated_len()))
                    .assured("the counts total batches retained in memory")
            })
            .sum()
    }

    fn consumers(&self) -> &RelayConsumerFanout {
        match self {
            Self::Direct(fanout) => fanout,
            Self::BranchCollapse(collapse) => &collapse.fanout,
        }
    }

    /// Lifecycle binding of the fixed dispatch fence for one partition. Record delivery retains
    /// this gate through its services and never returns to the audience registry.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "installation and handoff retain the partition dispatch gate before use"
        )
    )]
    pub(super) fn unit_dispatch_gate(&self, unit: Unit) -> Option<Arc<RelayDispatchGate>> {
        if unit == Unit::SINGLE {
            return None;
        }
        Some(self.consumers().unit_audiences(unit).dispatch_gate.clone())
    }
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
        unit: Unit,
        metrics: RelayMetricsHandle,
    ) -> (RelayRuntimeConsumerReceiver, StdArc<RelayOwnerBuffer>) {
        match self {
            Self::Direct(fanout) => fanout.activate_owner_buffer(unit, metrics),
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.fanout.activate_owner_buffer(unit, metrics)
            }
        }
    }

    pub(super) fn deactivate_owner_buffer(&self, unit: Unit, buffer: &StdArc<RelayOwnerBuffer>) {
        match self {
            Self::Direct(fanout) => fanout.deactivate_owner_buffer(unit, buffer),
            Self::BranchCollapse(branch_collapse) => {
                branch_collapse.fanout.deactivate_owner_buffer(unit, buffer);
            }
        }
    }

    pub(super) fn owner_buffer(&self, unit: Unit) -> Option<StdArc<RelayOwnerBuffer>> {
        match self {
            Self::Direct(fanout) => fanout.owner_buffer(unit),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.owner_buffer(unit),
        }
    }

    pub(super) fn owner_buffer_len(&self, unit: Unit) -> Option<(usize, usize)> {
        match self {
            Self::Direct(fanout) => fanout.owner_buffer_len(unit),
            Self::BranchCollapse(branch_collapse) => branch_collapse.fanout.owner_buffer_len(unit),
        }
    }

    /// The batches this relay holds in transit on this node.
    pub(super) fn transit(&self) -> &Arc<RelayTransit> {
        match self {
            Self::Direct(fanout) => &fanout.transit,
            Self::BranchCollapse(branch_collapse) => &branch_collapse.fanout.transit,
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

impl RelayBoundaryServices {
    fn transit_for_unit(&self, unit: Unit) -> &Arc<RelayTransit> {
        match self.unit_runtime_consumers.get(&unit) {
            Some(consumers) => &consumers.audiences.transit,
            None => self.fanout.transit(),
        }
    }
    fn unit_dispatch_gate(&self, unit: Unit) -> Option<&Arc<RelayDispatchGate>> {
        self.unit_runtime_consumers
            .get(&unit)
            .map(|consumers| &consumers.audiences.dispatch_gate)
    }
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "execution binding retains all declared unit audiences and gates once"
        )
    )]
    pub(super) fn new(
        fanout: RelayBoundaryFanout,
        attached_runtime_consumer_count: usize,
        detached_runtime_consumer_count: usize,
        remote_runtime_consumers: Vec<RemoteRuntimeConsumer>,
        remote_dispatcher: Option<StdArc<RemoteDispatcher>>,
        branch_presences: RelayBranchPresences,
        domain_ack_tracker: Arc<AckRootTracker>,
    ) -> Self {
        let partitioning = branch_presences.partitioning();
        let unit_runtime_consumers = partitioning
            .units()
            .into_iter()
            .filter(|unit| *unit != Unit::SINGLE)
            .map(|unit| {
                (
                    unit,
                    UnitRuntimeConsumers {
                        audiences: fanout.consumers().unit_audiences(unit),
                        attached: AtomicUsize::new(0),
                        detached: AtomicUsize::new(0),
                    },
                )
            })
            .collect();
        Self {
            fanout,
            attached_runtime_consumer_count: AtomicUsize::new(attached_runtime_consumer_count),
            detached_runtime_consumer_count: AtomicUsize::new(detached_runtime_consumer_count),
            unit_runtime_consumers,
            remote_runtime_consumers: ArcSwap::from_pointee(remote_runtime_consumers),
            remote_dispatcher,
            partitioning,
            owners: ArcSwap::from_pointee(RelayOwnerRoutes::default()),
            branch_presences,
            domain_ack_tracker,
            channels: RelayChannels::for_units(&partitioning.units()),
        }
    }

    /// The unit that owns the branch `key` names, refusing a keyless batch of a partitioned relay.
    pub(super) fn unit_of(
        &self,
        key: &Option<BranchKey>,
    ) -> error_stack::Result<Unit, RelayPartitionError> {
        self.partitioning.unit_of(key)
    }

    /// Whether this node's owner of the unit that owns `key` holds `key`'s branch. A key that names
    /// no unit of the relay is held by none.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "DESCRIBE and materialized reads observe one key's published branch presence"
        )
    )]
    pub(super) fn holds_branch(&self, key: &Option<BranchKey>) -> bool {
        let Ok(unit) = self.partitioning.unit_of(key) else {
            return false;
        };
        match self.branch_presences.of(unit) {
            Some(presence) => presence.contains(key.as_ref()),
            None => false,
        }
    }

    /// The fingerprint of every concrete branch this node's owners of the relay's units hold.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "the console's graph enumerates the published presence of each unit"
        )
    )]
    pub(super) fn held_branch_fingerprints(&self) -> BTreeSet<BranchKeyFingerprint> {
        let mut fingerprints = BTreeSet::new();
        for (_, presence) in self.branch_presences.iter() {
            let membership = presence.load();
            fingerprints.extend(membership.branches().map(BranchKey::fingerprint));
        }
        fingerprints
    }

    #[cfg(test)]
    pub(super) fn subscription_receiver(&self) -> RelaySubscriptionReceiver<RelayRecordBatch> {
        self.fanout.subscription_receiver()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "schedule publication replaces the relay's owner route"
        )
    )]
    pub(super) fn replace_owner_routes(&self, routes: RelayOwnerRoutes) {
        let current = self.owners.load_full();
        let changed = current.changed_units(&routes);
        if changed.is_empty() {
            return;
        }
        // Only the branches of a unit whose owner moved retire their channels; every other unit's
        // producers keep delivering to the owner they already reach.
        self.channels.replace_owner_of(&changed);
        self.owners.store(StdArc::new(routes));
    }

    /// A committed forced move cannot drain through its fenced former owner. Retire only a
    /// changed route's preceding channels while its dispatch gate is closed; normal publication
    /// creates the successor generation after the swap has installed its consumers.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "forced schedule installation fences changed routes"
        )
    )]
    pub(super) fn fence_routing_change(
        &self,
        routes: &RelayOwnerRoutes,
        consumers: &[RemoteRuntimeConsumer],
    ) {
        let mut changed = self.owners.load().changed_units(routes);
        changed.extend(self.changed_consumer_units(consumers));
        changed.sort_unstable();
        changed.dedup();
        if !changed.is_empty() {
            self.channels.fence_routing_of(&changed);
        }
    }

    pub(super) fn dispatch_is_fenced(&self, unit: Unit) -> bool {
        self.fanout.dispatch_is_fenced()
            || self
                .unit_dispatch_gate(unit)
                .is_some_and(|gate| gate.is_engaged())
    }

    /// Whether `node_id` owns `unit`. A unit no schedule places is owned wherever it runs.
    pub(super) fn is_owned_by(&self, node_id: Option<&ClusterNodeName>, unit: Unit) -> bool {
        let owners = self.owners.load();
        owners
            .owner_of(unit)
            .is_none_or(|owner| Some(owner) == node_id)
    }

    /// The cluster node the owner of `unit` runs on, when a schedule places it.
    pub(super) fn owner_of(&self, unit: Unit) -> Option<ClusterNodeName> {
        self.owners.load().owner_of(unit).cloned()
    }

    pub(super) fn activate_owner_buffer(
        &self,
        unit: Unit,
        metrics: RelayMetricsHandle,
    ) -> (RelayRuntimeFanIn, StdArc<RelayOwnerBuffer>) {
        let (receiver, buffer) = self.fanout.activate_owner_buffer(unit, metrics);
        (RelayRuntimeFanIn::new(receiver), buffer)
    }

    pub(super) fn deactivate_owner_buffer(&self, unit: Unit, buffer: &StdArc<RelayOwnerBuffer>) {
        self.fanout.deactivate_owner_buffer(unit, buffer);
    }

    pub(super) fn ingress_slot(
        &self,
        branch: &Option<BranchKey>,
        unit: Unit,
    ) -> StdArc<RelayOutboundSlot> {
        self.channels.bind(branch, unit).ingress()
    }

    pub(super) fn outbound_slot(
        &self,
        node_id: &ClusterNodeName,
        relay: &RelayName,
        kind: RelayPayloadKind,
        branch: &Option<BranchKey>,
        unit: Unit,
    ) -> StdArc<RelayOutboundSlot> {
        self.channels
            .bind(branch, unit)
            .outbound(RelayOutboundChannel {
                node_id: node_id.clone(),
                relay: relay.clone(),
                kind,
            })
    }

    pub(super) fn subscription_channels(
        &self,
        branch: &Option<BranchKey>,
        unit: Unit,
        snapshot: StdArc<crate::runtime::SubscriptionInterestIndex>,
        domain: &DomainName,
        relay: &RelayName,
    ) -> StdArc<channels::SubscriptionChannels> {
        self.channels
            .bind(branch, unit)
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
        unit: Unit,
    ) -> RelayDispatchResult {
        let _unit_permit = match self.unit_dispatch_gate(unit) {
            Some(gate) => Some(gate.acquire_dispatch().await),
            None => None,
        };
        let dispatch_gate = self.fanout.dispatch_gate();
        let _dispatch_permit = dispatch_gate.acquire_dispatch().await;
        let _branch_dispatch_permits = self.fanout.acquire_branch_dispatch_gates(&batch.key).await;
        let Some(buffer) = self.fanout.owner_buffer(unit) else {
            for ack in batch.acks.iter() {
                ack.no_ack("relay owner buffer is unavailable");
            }
            return Err(Box::new(batch.clone()));
        };
        let admission = RelayOwnerAdmission::new(self.transit_for_unit(unit).clone());
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

    pub(super) fn begin_owner_batch_completion(&self, unit: Unit) -> RelayOwnerBatchCompletion {
        RelayOwnerBatchCompletion::new(self.transit_for_unit(unit).clone())
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
        unit: Unit,
    ) -> RelayDispatchResult {
        self.dispatch_to_owner_on(
            domain,
            relay,
            unit,
            batch,
            self.ingress_slot(&batch.key, unit),
        )
        .await
    }

    /// Delivers `batch` to the relay's owner through `ingress_slot`.
    ///
    /// The owner reports the batch's records alive to their sources once it has admitted them, and
    /// not before. Until then this node holds them on their sources' behalf, so it reports them
    /// alive itself while the delivery waits for its gates, its channel, the storage of its
    /// records, a grant or the owner's admission. An ingestor's `ACK TIMEOUT` therefore measures how
    /// long its records make no progress, not how long they take to reach their owner.
    async fn dispatch_to_owner_on(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        unit: Unit,
        batch: &RelayRecordBatch,
        ingress_slot: StdArc<RelayOutboundSlot>,
    ) -> RelayDispatchResult {
        batch
            .keep_alive_while_pending(self.deliver_to_owner_on(
                domain,
                relay,
                unit,
                batch,
                ingress_slot,
            ))
            .await
    }

    async fn deliver_to_owner_on(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        unit: Unit,
        batch: &RelayRecordBatch,
        ingress_slot: StdArc<RelayOutboundSlot>,
    ) -> RelayDispatchResult {
        let _unit_permit = match self.unit_dispatch_gate(unit) {
            Some(gate) => Some(gate.acquire_dispatch().await),
            None => None,
        };
        let dispatch_gate = self.fanout.dispatch_gate();
        let _dispatch_permit = dispatch_gate.acquire_dispatch().await;
        let _branch_dispatch_permits = self.fanout.acquire_branch_dispatch_gates(&batch.key).await;
        let Some(owner_node) = self.owner_of(unit) else {
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
        // The delivery's records are stored before its body is encoded, so a delivery that waits
        // for room to store them holds no encoded body meanwhile.
        let deadline = RemoteDispatcher::delivery_deadline();
        let forwarded = batch
            .acks
            .iter()
            .map(RemoteDispatcher::forwarded_ack)
            .collect();
        let storage = dispatcher
            .store_pending_acks(
                &owner_node,
                forwarded,
                deadline,
                ingress_slot.cancellation().cancelled(),
            )
            .await;
        let mut remote_acks = match storage {
            Ok(RemoteAckStorage::Stored(registrations)) => registrations,
            Ok(RemoteAckStorage::Cancelled) => {
                for ack in &batch.acks {
                    ack.no_ack("relay channel was retired");
                }
                return Err(Box::new(batch.clone()));
            }
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    scope = %BranchScope::from(&batch.key),
                    target_node = %owner_node,
                    error = %format_args!("{error:#}"),
                    "failed to store the acknowledgements of a relay batch for its owner"
                );
                let reason = format!("{error:#}");
                for ack in &batch.acks {
                    ack.no_ack(reason.clone());
                }
                return Err(Box::new(batch.clone()));
            }
        };
        let batch_ipc = match batch.batch.encode_arrow_ipc(dispatcher.executor()).await {
            Ok(bytes) => bytes,
            Err(error) => {
                for registration in remote_acks.registrations().iter().flatten() {
                    dispatcher.clear_pending_ack(registration);
                }
                for ack in batch.acks.iter() {
                    ack.no_ack(format!("{error:#}"));
                }
                return Err(Box::new(batch.clone()));
            }
        };
        let delivery = ingress_slot.next_delivery();
        let admission_result = dispatcher
            .dispatch_admitted_relay_payload(
                &owner_node,
                RelayPayload {
                    delivery,
                    kind: RelayPayloadKind::Ingress,
                    domain: domain.clone(),
                    relay: relay.clone(),
                    unit,
                    key: BranchKey::to_remote_key(&batch.key),
                    batch_ipc,
                    metadata: batch.metadata.to_remote(),
                    acks: remote_acks.registrations().to_vec(),
                    admission: None,
                },
                &ingress_slot,
                deadline,
                GrantCapacityRefusals::Hold,
            )
            .await;
        if let Err(error) = admission_result {
            warn!(
                domain = domain.as_str(),
                relay = relay.as_str(),
                scope = %BranchScope::from(&batch.key),
                target_node = %owner_node,
                error = %format_args!("{error:#}"),
                "failed to admit relay batch on its owner"
            );
            let reason = format!("{error:#}");
            for registration in remote_acks.registrations().iter().flatten() {
                dispatcher.clear_pending_ack(registration);
            }
            for ack in batch.acks.iter() {
                ack.no_ack(reason.clone());
            }
            return Err(Box::new(batch.clone()));
        }
        remote_acks.admit();
        Ok(())
    }

    pub(super) fn observe_owner_buffer_length(&self, unit: Unit, metrics: &RelayMetricsHandle) {
        let Some((len, capacity)) = self.fanout.owner_buffer_len(unit) else {
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
        unit: Unit,
        batch: &RelayRecordBatch,
    ) {
        let Some(dispatcher) = &self.remote_dispatcher else {
            return;
        };
        let remote_runtime_consumers = self.remote_runtime_consumers.load_full();
        let excluded_nodes = remote_runtime_consumers
            .iter()
            .filter(|consumer| consumer.mode_for(unit).is_some())
            .map(|consumer| consumer.node_id.clone())
            .collect::<BTreeSet<_>>();
        dispatcher
            .dispatch_subscription_fanout(
                self,
                domain,
                relay,
                unit,
                &batch.detached(),
                &excluded_nodes,
            )
            .await;
    }

    pub(super) async fn dispatch_local_runtime_consumers(
        &self,
        dispatch: RuntimeConsumerDispatch,
        unit: Unit,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        if let Some(consumers) = self.unit_runtime_consumers.get(&unit) {
            let attached = self
                .attached_runtime_consumer_count
                .load(Ordering::Acquire)
                .checked_add(consumers.attached.load(Ordering::Acquire))
                .assured("registered consumers are held in memory");
            let detached = self
                .detached_runtime_consumer_count
                .load(Ordering::Acquire)
                .checked_add(consumers.detached.load(Ordering::Acquire))
                .assured("registered consumers are held in memory");
            return self
                .fanout
                .consumers()
                .dispatch_publications(
                    dispatch,
                    attached,
                    detached,
                    consumers.audiences.attached.publication(),
                    || consumers.audiences.detached.publication(),
                    batch,
                )
                .await;
        }
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
        unit: Unit,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let remote_runtime_consumers = self.remote_runtime_consumers.load_full();
        if remote_runtime_consumers.is_empty() {
            return Ok(());
        }
        // A consumer's node reports the batch's records alive to their sources once it has admitted
        // them, and not before, so this node reports them alive while their deliveries wait.
        batch
            .keep_alive_while_pending(self.deliver_to_remote_runtime_consumers(
                domain,
                unit,
                batch,
                &remote_runtime_consumers,
            ))
            .await
    }

    async fn deliver_to_remote_runtime_consumers(
        &self,
        domain: &DomainName,
        unit: Unit,
        batch: &RelayRecordBatch,
        remote_runtime_consumers: &[RemoteRuntimeConsumer],
    ) -> RelayDispatchResult {
        let Some(dispatcher) = &self.remote_dispatcher else {
            if remote_runtime_consumers
                .iter()
                .any(|consumer| consumer.mode_for(unit) == Some(AckMode::Attached))
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
            let Some(mode) = consumer.mode_for(unit) else {
                continue;
            };
            nervix_primitives::task::consume_budget().await;
            let outbound_slot = self.outbound_slot(
                &consumer.node_id,
                &consumer.relay,
                RelayPayloadKind::Routed,
                &batch.key,
                unit,
            );
            let Some(_slot) = outbound_slot.lock_for_delivery().await else {
                for ack in batch.acks.iter() {
                    ack.no_ack("relay channel lifetime ended");
                }
                return Err(Box::new(batch.clone()));
            };
            let remote_batch = match mode {
                AckMode::Attached => batch.attached(),
                AckMode::Detached => batch.detached(),
            };
            // The records are stored before the body is encoded, so the first destination that
            // waits for room to store them holds no encoded body meanwhile.
            let deadline = RemoteDispatcher::delivery_deadline();
            let mut registration_owner = if mode == AckMode::Attached {
                let storage = dispatcher
                    .store_pending_acks(
                        &consumer.node_id,
                        remote_batch.acks.to_vec(),
                        deadline,
                        outbound_slot.cancellation().cancelled(),
                    )
                    .await;
                match storage {
                    Ok(RemoteAckStorage::Stored(owner)) => Some(owner),
                    Ok(RemoteAckStorage::Cancelled) => {
                        for ack in batch.acks.iter() {
                            ack.no_ack("relay channel lifetime ended");
                        }
                        return Err(Box::new(batch.clone()));
                    }
                    Err(error) => {
                        let reason = format!("{error:#}");
                        for ack in &remote_batch.acks {
                            ack.no_ack(reason.clone());
                        }
                        return Err(Box::new(batch.clone()));
                    }
                }
            } else {
                None
            };
            let batch_ipc = match encoded_body.clone() {
                Some(bytes) => bytes,
                None => match batch.batch.encode_arrow_ipc(dispatcher.executor()).await {
                    Ok(bytes) => {
                        encoded_body = Some(bytes.clone());
                        bytes
                    }
                    Err(error) => {
                        if let Some(owner) = &registration_owner {
                            for registration in owner.registrations().iter().flatten() {
                                dispatcher.clear_pending_ack(registration);
                            }
                        }
                        if remote_runtime_consumers
                            .iter()
                            .any(|consumer| consumer.mode_for(unit) == Some(AckMode::Attached))
                        {
                            for ack in batch.acks.iter() {
                                ack.no_ack(format!("{error:#}"));
                            }
                            return Err(Box::new(batch.clone()));
                        }
                        warn!(
                            error = %format_args!("{error:#}"),
                            "failed to serialize detached remote relay batch"
                        );
                        return Ok(());
                    }
                },
            };
            let remote_acks = match &registration_owner {
                Some(owner) => owner.registrations().to_vec(),
                None => vec![None; remote_batch.acks.len()],
            };
            let delivery = outbound_slot.next_delivery();
            let result = dispatcher
                .dispatch_admitted_relay_payload(
                    &consumer.node_id,
                    routed_payload(RoutedDelivery {
                        delivery,
                        domain,
                        unit,
                        consumer,
                        batch: &remote_batch,
                        batch_ipc: batch_ipc.clone(),
                        acks: remote_acks.clone(),
                    }),
                    &outbound_slot,
                    deadline,
                    GrantCapacityRefusals::Hold,
                )
                .await;

            match (mode, result) {
                (AckMode::Attached, Ok(())) => {
                    registration_owner
                        .as_mut()
                        .verified("attached delivery registers an acknowledgement owner")
                        .admit();
                }
                (AckMode::Attached, Err(error)) => {
                    let reason = format!("{error:#}");
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
                        error = %format_args!("{error:#}"),
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

    pub(super) fn with_unit_runtime_consumer_counts(
        self,
        counts: BTreeMap<Unit, (usize, usize)>,
    ) -> Self {
        for (unit, (attached, detached)) in counts {
            let consumers = self
                .unit_runtime_consumers
                .get(&unit)
                .verified("scoped registrations name a unit declared by the relay");
            consumers.attached.store(attached, Ordering::Release);
            consumers.detached.store(detached, Ordering::Release);
        }
        self
    }

    pub(super) fn add_local_unit_runtime_consumer(
        &self,
        mode: AckMode,
        unit: Unit,
    ) -> RelayRuntimeFanIn {
        if unit == Unit::SINGLE {
            return self.add_local_runtime_consumer(mode);
        }
        let consumers = self
            .unit_runtime_consumers
            .get(&unit)
            .verified("the validated processor and its input share partition units");
        let (count, audience) = match mode {
            AckMode::Attached => (&consumers.attached, &consumers.audiences.attached),
            AckMode::Detached => (&consumers.detached, &consumers.audiences.detached),
        };
        count
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .assured("registered consumers occupy memory");
        RelayRuntimeFanIn::new(audience.new_receiver())
    }

    pub(super) fn remove_local_unit_runtime_consumer(&self, mode: AckMode, unit: Unit) {
        if unit == Unit::SINGLE {
            self.remove_local_runtime_consumer(mode);
            return;
        }
        let consumers = self
            .unit_runtime_consumers
            .get(&unit)
            .verified("the installed consumer belongs to a declared partition");
        let count = match mode {
            AckMode::Attached => &consumers.attached,
            AckMode::Detached => &consumers.detached,
        };
        count
            .fetch_sub(1, Ordering::AcqRel)
            .checked_sub(1)
            .verified("an installed consumer is removed once");
    }

    pub(super) fn remove_local_runtime_consumer(&self, mode: AckMode) {
        let counter = match mode {
            AckMode::Attached => &self.attached_runtime_consumer_count,
            AckMode::Detached => &self.detached_runtime_consumer_count,
        };
        let previous = counter.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "relay runtime consumer count underflow");
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "schedule publication replaces the relay's consumer routes"
        )
    )]
    pub(super) fn replace_remote_runtime_consumers(&self, consumers: Vec<RemoteRuntimeConsumer>) {
        if *self.remote_runtime_consumers.load().as_ref() == consumers {
            return;
        }
        self.channels
            .replace_owner_of(&self.changed_consumer_units(&consumers));
        self.remote_runtime_consumers.store(StdArc::new(consumers));
    }

    fn changed_consumer_units(&self, next: &[RemoteRuntimeConsumer]) -> Vec<Unit> {
        let current = self.remote_runtime_consumers.load();
        self.partitioning
            .units()
            .into_iter()
            .filter(|unit| {
                let before = current.iter().filter_map(|consumer| {
                    consumer
                        .mode_for(*unit)
                        .map(|mode| (&consumer.node_id, &consumer.relay, mode))
                });
                let after = next.iter().filter_map(|consumer| {
                    consumer
                        .mode_for(*unit)
                        .map(|mode| (&consumer.node_id, &consumer.relay, mode))
                });
                !before.eq(after)
            })
            .collect()
    }

    pub(super) async fn fanout_owner_batch(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        unit: Unit,
        batch: &RelayRecordBatch,
        fault_injection: &ConfiguredFaultInjection,
    ) -> RelayDispatchResult {
        fault_injection
            .pause_owner_relay_fanout_if_armed(domain)
            .await;
        fault_injection
            .pause_owner_relay_partition_fanout_if_armed(domain, relay, unit)
            .await;
        let _unit_permit = if let Some(gate) = self.unit_dispatch_gate(unit) {
            let Some(permit) = gate.try_acquire_dispatch() else {
                for ack in batch.acks.iter() {
                    ack.no_ack("relay partition routing changed before owner fan-out");
                }
                batch.ack_success();
                return Err(Box::new(batch.clone()));
            };
            Some(permit)
        } else {
            None
        };
        let gate = self.fanout.dispatch_gate();
        let Some(_dispatch_permit) = gate.try_acquire_dispatch() else {
            for ack in batch.acks.iter() {
                ack.no_ack("relay routing changed before owner fan-out");
            }
            batch.ack_success();
            return Err(Box::new(batch.clone()));
        };
        self.fanout_local_subscriptions(batch).await;
        self.fanout_remote_subscriptions(domain, relay, unit, batch)
            .await;
        if let Err(failed) = self
            .dispatch_local_runtime_consumers(RuntimeConsumerDispatch::Owner, unit, batch)
            .await
        {
            batch.ack_success();
            return Err(failed);
        }
        let result = self
            .dispatch_remote_runtime_consumers(domain, unit, batch)
            .await;
        batch.ack_success();
        result
    }

    /// Hands a batch another node's relay owner routed here to the runtime consumers this node runs
    /// for the relay. A batch that carries record acknowledgements fails them when no attached
    /// consumer of the relay runs here any more, so its source delivers it again along the owner's
    /// current routes rather than committing a record no consumer processed.
    pub(super) async fn inject_remote_message(
        &self,
        unit: Unit,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let _unit_permit = if let Some(gate) = self.unit_dispatch_gate(unit) {
            let Some(permit) = gate.try_acquire_dispatch() else {
                for ack in batch.acks.iter() {
                    ack.no_ack("relay partition routing changed before routed delivery");
                }
                return Err(Box::new(batch.clone()));
            };
            Some(permit)
        } else {
            None
        };
        self.fanout_local_subscriptions(batch).await;
        // The routed batch enters this relay on this node here, and the relay holds it until its
        // consumers here have it.
        let _routed = RelayRoutedAdmission::new(self.transit_for_unit(unit));
        self.dispatch_local_runtime_consumers(RuntimeConsumerDispatch::Routed, unit, batch)
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
        let unit = services
            .partitioning
            .unit_of(&key)
            .map_err(|refusal| refusal.current_context().clone());
        let channels = match &unit {
            Ok(unit) => RelayProducerChannels::Bound(services.channels.bind(&key, *unit)),
            Err(_) => RelayProducerChannels::Ended,
        };
        Self {
            runtime,
            domain,
            relay,
            services,
            key,
            unit,
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
        let unit = match &self.unit {
            Ok(unit) => *unit,
            Err(refusal) => {
                let reason = refusal.to_string();
                for ack in batch.acks.iter() {
                    ack.no_ack(reason.clone());
                }
                return Err(Box::new(batch.clone()));
            }
        };
        let RelayProducerChannels::Bound(channels) = &mut self.channels else {
            for ack in batch.acks.iter() {
                ack.no_ack("relay producer lifetime ended");
            }
            return Err(Box::new(batch.clone()));
        };
        // The receiver's relay branch may expire independently of this producer. Its explicit
        // invalidation is a cold rebind; ending this producer never permits such a rebind.
        if channels.is_retired() {
            *channels = self.services.channels.bind(&self.key, unit);
            if channels.is_retired() {
                for ack in batch.acks.iter() {
                    ack.no_ack("relay service lifetime ended");
                }
                return Err(Box::new(batch.clone()));
            }
        }
        if self.runtime.owns_relay_unit(&self.services, unit) {
            return self.services.enqueue_owner_batch(batch, unit).await;
        }
        self.services
            .dispatch_to_owner_on(&self.domain, &self.relay, unit, batch, channels.ingress())
            .await
    }
}

impl RelayBoundaryBuilder {
    pub(super) fn runtime_consumer_fan_in_for_unit(
        &mut self,
        mode: AckMode,
        unit: Unit,
    ) -> RelayRuntimeFanIn {
        if unit == Unit::SINGLE {
            return self.runtime_consumer_fan_in_for_mode(mode);
        }
        let counts = self.unit_runtime_consumer_counts.entry(unit).or_default();
        let audiences = self.fanout.consumers().unit_audiences(unit);
        let (count, audience) = match mode {
            AckMode::Attached => (&mut counts.0, &audiences.attached),
            AckMode::Detached => (&mut counts.1, &audiences.detached),
        };
        *count = count
            .checked_add(1)
            .assured("registered consumers occupy memory");
        RelayRuntimeFanIn::new(audience.new_receiver())
    }
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
    /// Whether this node owns `unit` of the relay `services` serve. A unit whose schedule names no
    /// owner is owned wherever it runs.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "retained relay handles route each admitted record and keep channel ordering \
                      and fences"
        )
    )]
    pub(in crate::runtime) fn owns_relay_unit(
        &self,
        services: &RelayBoundaryServices,
        unit: Unit,
    ) -> bool {
        let dispatcher = self.inner.remote_dispatcher.load();
        services.is_owned_by(
            dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
            unit,
        )
    }

    pub(super) async fn fanout_relay_owner_batch(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        unit: Unit,
        services: &RelayBoundaryServices,
        branches: &mut RelayOwnerBranchState,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let now = match branches.expiration_time(self, domain) {
            Ok(now) => now,
            Err(error) => {
                let reason = format!(
                    "relay '{}' in domain '{}' could not read domain time: {error:#}",
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
                            unit,
                            physical_node_id,
                            RELAY_BUFFER_DIRECTION_CONCRETE,
                            Some(branch_key.fingerprint()),
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
            services.remove_branch_slots(&Some(evicted_key));
        }
        metrics.observe_batch(
            batch.message_count(),
            batch.estimated_bytes(),
            batch.domain_timestamp(),
        );
        services.observe_owner_buffer_length(unit, &metrics);
        let result = services
            .fanout_owner_batch(domain, relay, unit, batch, &self.inner.fault_injection)
            .await;
        self.inner
            .fault_injection
            .mark_owner_relay_fanout_complete(domain);
        result
    }

    /// The branch presence of every unit of `relay`: the one kept with the unit's state placement
    /// for each unit `shared` selects, and a fresh one for every other unit.
    pub(in crate::runtime) fn relay_branch_presences(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        partitioning: RelayPartitioning,
        shared: impl Fn(Unit) -> bool,
    ) -> error_stack::Result<RelayBranchPresences, StateIdentityError> {
        let mut by_unit = BTreeMap::new();
        for unit in partitioning.units() {
            let presence = if shared(unit) {
                self.relay_branch_presence(domain, relay, unit)?
            } else {
                Arc::new(BranchPresence::new())
            };
            by_unit.insert(unit, presence);
        }
        Ok(RelayBranchPresences::new(partitioning, by_unit))
    }

    /// The branch presence of `unit` of `relay`, kept with the unit's materialized state in the
    /// lifetime the committed schedule publishes for it, so materialized reads observe the
    /// membership of whichever owner the unit has on this node.
    pub(in crate::runtime) fn relay_branch_presence(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        unit: Unit,
    ) -> error_stack::Result<RelayBranchPresence, StateIdentityError> {
        let placement = self.state_placement_for_unit(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            unit,
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
    ) -> error_stack::Result<RelaySubscriptionReceiver<RelayRecordBatch>, RuntimeError> {
        let Some(routing) = self.domain_routing(domain) else {
            return Err(Report::new(RuntimeError::relay_not_instantiated(
                domain, relay,
            )));
        };
        let routing = routing.load();
        let Some(services) = routing.relay_services.get(relay) else {
            return Err(Report::new(RuntimeError::relay_not_instantiated(
                domain, relay,
            )));
        };
        match services.fanout.subscriptions().attach(expected) {
            Ok(receiver) => Ok(receiver),
            Err(RelaySubscriptionRefusal::NotDeclared) => Err(Report::new(
                RuntimeError::relay_not_instantiated(domain, relay),
            )),
            Err(RelaySubscriptionRefusal::Redefined) => {
                Err(Report::new(RuntimeError::RelayRedefined {
                    domain: domain.clone(),
                    relay: relay.clone(),
                }))
            }
        }
    }

    /// Starts the owner task of `unit` of `relay` on this node: its buffer, its branch presence and
    /// retention, its metric series and its fan-out. A partitioned relay has one such task for each
    /// partition this node owns, and each enforces `CAPACITY`, TTL and `MAX INSTANCES` on the keys
    /// of its own partition only.
    pub(in crate::runtime) fn spawn_relay_owner_task(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        unit: Unit,
        services: Arc<RelayBoundaryServices>,
        retention: RelayRetention,
    ) -> error_stack::Result<RelayOwnerTask, ExecutionBuildError> {
        let Some(presence) = services.branch_presences.of(unit).cloned() else {
            return Err(Report::new(ExecutionBuildError::RelayUnitNotScheduled {
                relay: relay.clone(),
                unit,
            }));
        };
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let dispatcher = self.inner.remote_dispatcher.load();
        let physical_node_id = dispatcher
            .as_deref()
            .map(RemoteDispatcher::local_node_id)
            .cloned();
        let global_metrics = self.inner.metrics.resolve_relay_metrics(
            domain,
            relay,
            unit,
            physical_node_id.as_ref(),
            RELAY_BUFFER_DIRECTION_CONCRETE,
            None,
        );
        let (mut receiver, owner_buffer) =
            services.activate_owner_buffer(unit, global_metrics.clone());
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
        let instances = OwnedBranches::claim(presence);
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
                        let _completion = services.begin_owner_batch_completion(unit);
                        runtime
                            .fanout_relay_owner_batch(
                                &domain,
                                &relay,
                                unit,
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
                                    "relay '{}' in domain '{}' lost its clock: {error:#}",
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
                            services.remove_branch_slots(&Some(expired_key));
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
                let _completion = services.begin_owner_batch_completion(unit);
                runtime
                    .fanout_relay_owner_batch(
                        &domain,
                        &relay,
                        unit,
                        &services,
                        &mut branches,
                        &batch,
                    )
                    .await
                    .discarded(
                        "the batch is acknowledged inside the fanout, and the owner task has no \
                         second consumer for a rejected copy",
                    );
            }
            services.observe_owner_buffer_length(unit, &branches.global_metrics);
            services.deactivate_owner_buffer(unit, &owner_buffer);
            // Releasing the owner's branches publishes an empty presence before the unit's metrics
            // are removed, unless a successor already claimed the presence.
            drop(branches);
            runtime
                .inner
                .metrics
                .remove_relay_unit(&domain, &relay, unit);
        });
        Ok(RelayOwnerTask { shutdown, task })
    }
}

#[cfg(test)]
#[path = "relay_boundary_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "relay_boundary_keepalive_tests.rs"]
mod keepalive_tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "relay_branch_presence_shuttle_tests.rs"]
mod branch_presence_shuttle_tests;
