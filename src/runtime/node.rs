//! The handle every task on this node carries, and the state it holds.
//!
//! Layer: data plane.
//!
//! - **Owns.** The `Runtime` handle, the single `Arc` of node state behind it, and the graph each
//!   domain currently runs.
//! - **Depends on.** Every subsystem whose state the node holds.
//! - **Must not know.** How any of that state is used; the subsystems own their own behaviour.

use super::*;

pub(in crate::runtime) type SharedActiveGraph = StdArc<ArcSwapOption<ActiveGraph>>;

pub const DEFAULT_TEMP_DIR: &str = "/tmp";

/// The handle every task, ingestor, emitter, and connector carries. It is one `Arc` over the
/// node's state, so passing the runtime into a spawned task costs a single refcount rather than
/// one per piece of state the node owns.
#[derive(Clone)]
pub struct Runtime {
    pub(in crate::runtime) inner: Arc<RuntimeInner>,
}

/// Everything one Nervix node owns for as long as it runs. These fields are reached only through
/// a `Runtime` handle and therefore hold their values directly. The few that keep an `Arc` of
/// their own have a second owner that outlives the handle's borrow, and each names that owner.
pub(in crate::runtime) struct RuntimeInner {
    /// Also held by the entity gate's deadline task, which releases an expired lease long after
    /// the call that engaged it returned.
    pub(in crate::runtime) ingestors: Arc<DashMap<DomainNodeRef, IngestorRuntime, RandomState>>,
    /// Also held by the entity gate's deadline task, alongside `ingestors`.
    pub(in crate::runtime) ingestor_quiescence:
        Arc<DashMap<DomainNodeRef, Arc<IngestorQuiesceControl>, RandomState>>,
    pub(in crate::runtime) ingestors_paused_for_memory_pressure: AtomicBool,
    pub(in crate::runtime) ingestor_transient_errors: DashMap<DomainNodeRef, String, RandomState>,
    pub(in crate::runtime) ingestor_reconnect_backoffs:
        DashMap<DomainNodeRef, RuntimeReconnectStatus, RandomState>,
    pub(in crate::runtime) ingestor_readiness:
        DashMap<DomainNodeRef, IngestorReadiness, RandomState>,
    pub(in crate::runtime) emitter_transient_errors: DashMap<DomainNodeRef, String, RandomState>,
    pub(in crate::runtime) emitter_retry_statuses:
        DashMap<DomainNodeRef, EmitterRetryStatus, RandomState>,
    pub(in crate::runtime) emitter_confirmation_waits:
        DashMap<DomainNodeRef, Arc<AtomicUsize>, RandomState>,
    /// One connector instance per named client on this node, keyed by the client it belongs to and
    /// held open by the emitters and ingestors leasing it.
    pub(in crate::runtime) shared_clients:
        DashMap<DomainNodeRef, shared_clients::SharedClientSlot, RandomState>,
    /// The graph nodes that have asked a shared client for a connection and not yet been given
    /// one, keyed by the waiting node rather than the client it waits on.
    pub(in crate::runtime) pool_waits:
        DashMap<DomainNodeRef, shared_clients::PoolWait, RandomState>,
    pub(in crate::runtime) executions: DashMap<DomainName, DomainExecution, RandomState>,
    pub(in crate::runtime) message_error_routes:
        DashMap<MessageErrorRouteKey, Arc<MessageErrorRouteRuntime>, RandomState>,
    pub(in crate::runtime) compiled_domain_udfs:
        DashMap<DomainName, CompiledDomainUdfs, RandomState>,
    pub(in crate::runtime) schedule_apply_lock: Mutex<()>,
    pub(in crate::runtime) applied_cluster_revision: AtomicU64,
    pub(in crate::runtime) domain_instantiation_errors: DashMap<DomainName, String, RandomState>,
    pub(in crate::runtime) domains: DashMap<DomainName, RuntimeDomainState, RandomState>,
    pub(in crate::runtime) domain_status_changed: watch::Sender<u64>,
    pub(in crate::runtime) in_flight_by_domain:
        DashMap<DomainName, Arc<AckRootTracker>, RandomState>,
    pub(in crate::runtime) in_flight_by_ingestor:
        DashMap<DomainNodeRef, Arc<AckRootTracker>, RandomState>,
    pub(in crate::runtime) generator_activity_by_domain:
        DashMap<DomainName, Arc<AtomicUsize>, RandomState>,
    pub(in crate::runtime) emitter_buffers: DashMap<DomainNodeRef, Arc<AtomicUsize>, RandomState>,
    pub(in crate::runtime) force_flush_by_domain:
        DashMap<DomainName, Arc<DomainForceFlush>, RandomState>,
    pub(in crate::runtime) node_quiesce_counters:
        DashMap<DomainNodeRef, Arc<NodeQuiesceCounters>, RandomState>,
    /// Also held by the entity gate's deadline task, alongside `ingestors`.
    pub(in crate::runtime) entity_gate_holds:
        Arc<DashMap<EntityGateHoldKey, EntityAlterHold, RandomState>>,
    /// Also held by the entity gate deadline task so a failed handoff resumes state timers when
    /// its lease expires.
    pub(in crate::runtime) frozen_ownership_handoff_entities:
        Arc<DashMap<DomainNodeRef, (), RandomState>>,
    /// Also held by branch tasks waiting for a handoff freeze to end.
    pub(in crate::runtime) ownership_handoff_freeze_changed: Arc<Notify>,
    /// Also held by every outstanding `DomainAlterGuard`, which clears its entry on drop.
    pub(in crate::runtime) active_domain_alters:
        Arc<DashMap<DomainName, ActiveDomainAlter, RandomState>>,
    pub(in crate::runtime) state_schema_fingerprints: DashMap<DomainNodeRef, [u8; 32], RandomState>,
    pub(in crate::runtime) domain_graphs: DashMap<DomainName, SharedActiveGraph, RandomState>,
    pub(in crate::runtime) endpoint_bindings:
        DashMap<HttpRouteKey, Vec<EndpointIngestBinding>, RandomState>,
    /// Instantiated endpoint routes keyed by the host and path an inbound request carries, so
    /// request routing never scans domain executions or their configured routes.
    pub(in crate::runtime) routed_endpoints:
        DashMap<HttpRouteKey, RoutedEndpointsByDomain, RandomState>,
    pub(in crate::runtime) relay_boundary_fanouts: RelayBoundaryFanoutMap,
    pub(in crate::runtime) events: RuntimeEvents,
    /// The test harness keeps another handle to the same injected state and arms it while this
    /// node runs. Normal builds store a zero-sized marker here.
    pub(in crate::runtime) fault_injection: ConfiguredFaultInjection,
    pub(in crate::runtime) resource_store: RwLock<Option<Arc<ResourceStore>>>,
    pub(in crate::runtime) resource_versions: RwLock<ResourceVersionStatus>,
    pub(in crate::runtime) remote_dispatcher: RwLock<Option<Arc<RemoteDispatcher>>>,
    /// Also held by the attached `RemoteDispatcher`, which must allocate correlation ids from the
    /// same registry the runtime resolves incoming acknowledgements against.
    pub(in crate::runtime) remote_dispatch: Arc<RemoteDispatchRegistry>,
    /// Cancels remote acknowledgement watchers once this runtime has drained its domain tasks.
    pub(in crate::runtime) remote_ack_watcher_shutdown: CancellationToken,
    /// Owns acknowledgement progress tasks so none can retain an interconnect after shutdown.
    pub(in crate::runtime) remote_ack_watcher_tasks: TaskTracker,
    pub(in crate::runtime) state_checkpoint_notifications:
        DashMap<RuntimeStatePlacement, Arc<Notify>, RandomState>,
    pub(in crate::runtime) pending_state_replica_syncs:
        DashMap<RuntimeStatePlacement, PendingStateReplicaSync, RandomState>,
    pub(in crate::runtime) pending_state_checkpoint_announcements:
        DashMap<RuntimeStatePlacement, PendingStateCheckpointAnnouncement, RandomState>,
    /// Owns replica synchronization and checkpoint announcement work that outlives the event that
    /// scheduled it.
    pub(in crate::runtime) state_replication_tasks: TaskTracker,
    pub(in crate::runtime) passive_runtime_state_snapshots:
        DashMap<RuntimeStatePlacement, PersistedRuntimeStateEntry, RandomState>,
    pub(in crate::runtime) replicated_branch_lru_snapshots:
        DashMap<RuntimeStatePlacement, PersistedRuntimeStateEntry, RandomState>,
    pub(in crate::runtime) prepared_runtime_state_handoffs:
        DashMap<DomainNodeRef, PreparedRuntimeStateHandoff, RandomState>,
    pub(in crate::runtime) activated_runtime_state_handoffs:
        DashMap<DomainNodeRef, ActivatedRuntimeStateHandoff, RandomState>,
    pub(in crate::runtime) prepared_forced_runtime_state_recoveries:
        DashMap<DomainNodeRef, PreparedForcedRuntimeStateRecovery, RandomState>,
    pub(in crate::runtime) prepared_runtime_state_snapshots:
        DashMap<RuntimeStatePlacement, PreparedRuntimeStateSnapshot, RandomState>,
    pub(in crate::runtime) expiring_stream_states:
        DashMap<RuntimeStatePlacement, Arc<ExpiringRelayState>, RandomState>,
    pub(in crate::runtime) latest_resource_versions: DashMap<DomainResourceKey, u64, RandomState>,
    pub(in crate::runtime) replicated_deduplicator_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedDeduplicatorState>, RandomState>,
    pub(in crate::runtime) replicated_kafka_offset_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedKafkaOffsetState>, RandomState>,
    pub(in crate::runtime) replicated_materialized_stream_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedMaterializedRelayState>, RandomState>,
    /// Sealed materialized snapshots that have been opened and are waiting for the state they
    /// belong to to be built. Opening one is bulk work, so it happens on a path that can wait and
    /// the synchronous construction consumes the result.
    pub(in crate::runtime) restored_materialized_stream_states:
        DashMap<RuntimeStatePlacement, RestoredMaterializedSnapshot, RandomState>,
    pub(in crate::runtime) relay_state_epochs: DashMap<DomainName, Arc<AtomicU64>, RandomState>,
    pub(in crate::runtime) materialized_state_changed: Notify,
    pub(in crate::runtime) replicated_window_processor_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedWindowProcessorState>, RandomState>,
    pub(in crate::runtime) replicated_wasm_processor_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedWasmProcessorState>, RandomState>,
    pub(in crate::runtime) replicated_branch_aggregated_states:
        DashMap<RuntimeStatePlacement, Arc<ReplicatedBranchAggregatedState>, RandomState>,
    pub(in crate::runtime) wasm_runtime: WasmRuntime,
    pub(in crate::runtime) branch_instance_expiration_scan_interval: Duration,
    pub(in crate::runtime) state_store: Option<Arc<RuntimeStateStore>>,
    /// The bounded disk incoming sealed snapshots land on before they are verified and opened.
    pub(in crate::runtime) snapshot_staging: SnapshotStaging,
    pub(in crate::runtime) state_snapshot_interval: Duration,
    pub(in crate::runtime) state_replication_poll_interval: Duration,
    pub(in crate::runtime) domain_drain_timeout: Duration,
    pub(in crate::runtime) entity_gate_deadline: Duration,
    pub(in crate::runtime) temp_dir: PathBuf,
    /// The node's bounded execution and transient-memory admission. Every variable-size encode,
    /// decode, validation and hash the runtime performs is submitted through it, so none of them
    /// occupies an async worker and none of them allocates before it is charged.
    pub(in crate::runtime) executor: Executor,
    pub(in crate::runtime) metrics: RuntimeMetrics,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}
