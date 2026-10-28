//! Fault controls exposed to the server test harness.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** The shared state used to arm deterministic server failures, pauses, timing
//!   overrides, listener-address overrides, and test scheduling.
//! - **Depends on.** Vocabulary identities, connector test-control contracts, execution-sensitive
//!   notification primitives, and the decision layer's test scheduler mode.
//! - **Must not know.** Product configuration, Plexus syntax, connector drivers, or runtime
//!   ownership beyond the typed values needed to select a seam.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "the testing harness owns failure seams and observations; these are not product \
                  graph state"
    )
)]

use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use ahash::RandomState;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector_syslog::SyslogListenerFault;
use nervix_execution::Executor;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, CommandExecutionReference,
    DomainName, DomainNodeRef, EmitterName, IngestorName, ModelKind, ModelName, RelayName,
    RemoteRuntimeField, RestoreStep,
};
use nervix_primitives::{
    collections::DashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        blocking::RwLock,
        broadcast, mpsc, oneshot, watch,
    },
};
use nervix_recovery::NoReceiver as _;

use crate::registry::SchedulerMode;

mod backup_cut;
#[cfg(test)]
mod backup_cut_tests;
mod consensus;
mod domain_clock;
mod domain_resume;
mod entity_drain;
#[cfg(test)]
mod entity_drain_tests;
mod execution;
mod handoff_gate_link;
mod live_view_gap;
#[cfg(test)]
mod live_view_gap_tests;
mod materialized_fetch_hold;
mod model_publication;
mod pause;
mod relay_memory;
mod relocation_commit;
mod resource_discovery;
mod resource_publication;
#[cfg(test)]
mod resource_publication_tests;
mod restarted_voter;
mod restore_publication;
#[cfg(test)]
mod restore_publication_tests;
mod restore_staging;
#[cfg(test)]
mod restore_staging_tests;
mod shutdown_handoff;
mod standalone_transaction;
#[cfg(test)]
mod syslog_tests;
mod transaction_cleanup;
mod wasm_reset_publication;

pub use backup_cut::BackupCutBoundary;
use backup_cut::{BackupCutGate, BackupCutGateKey};
use domain_clock::DomainClockProgressPausePoint;
pub use domain_resume::DomainResumeBoundary;
use entity_drain::ForcedEntityDrainTimeout;
pub(crate) use entity_drain::ObservedEntityDrain;
use execution::NodeExecution;
use handoff_gate_link::HandoffGateLink;
use live_view_gap::{LiveViewGap, LiveViewGapState};
use model_publication::ModelPublicationRefusal;
pub use model_publication::ModelPublicationRefusalKind;
use pause::TestPause;
use relay_memory::RelayMemoryFill;
use relocation_commit::RelocationCommitRefusal;
use resource_publication::ResourcePublicationRefusal;
use restarted_voter::{StartupObservationEnd, StartupVoterGossip};
use restore_publication::RestorePublicationFailure;
pub use restore_staging::RestoreStagingKind;
use restore_staging::{RestoreStagingFailure, RestoreStagingTarget};
pub use standalone_transaction::StandaloneTransactionBoundary;
use wasm_reset_publication::{WasmResetPublicationAuthorityLoss, WasmResetPublicationFailure};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmitterFaultMode {
    Fail,
    Stall,
}

/// A consensus storage failure a scenario armed for a node before that node started.
///
/// Storage belongs to the node, so the failure can only be armed once the node builds it. Keeping
/// the record afterwards is what lets a scenario tell a failure that has fired from one that was
/// never armed at all.
#[derive(Debug)]
enum StartupConsensusFault {
    Pending {
        operation: String,
        boundary: nervix_consensus::StorageBoundary,
    },
    Armed,
}

/// Whether a node moves what its node database holds in memory into on-disk tables as it stops.
///
/// A scenario arms the move before a node stops and reads afterwards whether the stopping node
/// performed it, so a restart that proves nothing because the move never happened cannot pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeDatabaseFlush {
    Armed,
    Flushed,
}

/// The entity of one domain whose state a backup captures.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CapturedStateOwner {
    domain: DomainName,
    entity: ModelName,
}

/// One cloneable handle for every fault and override a server test can inject.
///
/// Every clone points at the same state, so a Cucumber world can arm a seam after handing the
/// handle to every node without keeping parallel injector objects in sync.
#[derive(Clone, Debug)]
pub struct FaultInjection {
    inner: Arc<FaultInjectionState>,
}

#[derive(Debug)]
struct FaultInjectionState {
    emitter_faults: DashMap<String, EmitterFaultMode, RandomState>,
    failed_ingestors: DashMap<String, (), RandomState>,
    unavailable_emitter_clients: DashMap<String, (), RandomState>,
    /// One-shot, emitter-scoped emitter stalls: the next write of the emitter keeps the emitter's answers
    /// for this many of its records, and the answers for the rest are lost.
    stalled_emitter_connectors: DashMap<String, usize, RandomState>,
    failed_schedule_publications: DashMap<String, (), RandomState>,
    wasm_reset_publication_authority_losses:
        DashMap<DomainName, WasmResetPublicationAuthorityLoss, RandomState>,
    failed_wasm_reset_publications: DashMap<DomainName, WasmResetPublicationFailure, RandomState>,
    /// One-shot, domain-scoped rejections consumed before any entity gate engages.
    failed_entity_gate_engagements: DashMap<DomainName, (), RandomState>,
    /// One-shot, domain-scoped transport failures of the next ownership-handoff gate engagement
    /// request a coordinator sends to a remote node. Each keeps whether it was delivered, so a
    /// scenario proves the failure happened instead of assuming it.
    handoff_engagement_transport_failures:
        DashMap<DomainName, HandoffEngagementTransportFailure, RandomState>,
    /// Coordinator-to-node links that lose every ownership-handoff gate engagement request sent
    /// over them, each with how many requests it lost.
    lost_handoff_gate_engagements: DashMap<HandoffGateLink, usize, RandomState>,
    /// One-shot, domain-scoped leader refusals of the next relocation commit, each keeping whether
    /// it was delivered.
    refused_relocation_commits: DashMap<DomainName, RelocationCommitRefusal, RandomState>,
    /// One-shot, domain-scoped leader refusals of the next transaction model step publication and
    /// of the pause release after it, each keeping how far it was delivered.
    refused_model_publications: DashMap<DomainName, ModelPublicationRefusal, RandomState>,
    /// Domain-scoped failures met at the entity drain observation boundary, each refusing the drain
    /// that met it first whenever the execution owning it observes that drain again.
    forced_entity_drain_timeouts: DashMap<DomainName, ForcedEntityDrainTimeout, RandomState>,
    /// A stale drain observation on one node omits already buffered relay-owner batches.
    stale_owner_buffer_drain_reports: DashMap<(DomainName, ClusterNodeName), (), RandomState>,
    /// One-shot, domain-scoped timeouts consumed after the durable domain pause engages.
    forced_domain_drain_timeouts: DashMap<DomainName, (), RandomState>,
    /// One-shot entity-swap failures consumed by the selected runtime node and domain.
    failed_entity_schedule_swaps: DashMap<EntityScheduleSwapFailureKey, (), RandomState>,
    transaction_binding_drops: DashMap<ClusterNodeName, (), RandomState>,
    /// One-shot waits, keyed by a stopping node, that the leader spends before it releases the
    /// cordon that node's own graceful shutdown drain set.
    delayed_shutdown_cordon_releases: DashMap<ClusterNodeName, Duration, RandomState>,
    /// Waits, keyed by an ingestor, that its source hosts spend between learning that a batch was
    /// acknowledged through the graph and letting the source confirm the batch's positions on its
    /// transport, as a transport slow to answer an acknowledgement would.
    delayed_source_confirmations: DashMap<DomainNodeRef, Duration, RandomState>,
    /// Waits, keyed by the node whose state is written, that a snapshot task spends before the last
    /// write it makes as that state stops, as storage slow to take a large checkpoint would: the
    /// snapshot task of each branch of a deduplicator or window processor once the branch stopped,
    /// and the checkpoint writer of the state the node's placement keeps, such as a materialized
    /// relay's records, once its domain's stop told it to write.
    delayed_stopped_state_writes: DashMap<DomainNodeRef, Duration, RandomState>,
    /// Waits, keyed by a relay processor, that the processor spends before each read of its
    /// materialized dependencies, as a read slow to reach the node that holds the state would.
    delayed_materialized_dependency_reads: DashMap<DomainNodeRef, Duration, RandomState>,
    /// Holds, keyed by node, of that node's next materialized snapshot fetch on its first received
    /// chunk until the node's restore checkpoint maintenance asks for bulk memory.
    materialized_snapshot_fetch_holds: DashMap<ClusterNodeName, Arc<TestPause>, RandomState>,
    /// One-shot installation failures, consumed by the next resource version a node installs.
    failed_resource_installations: DashMap<ClusterNodeName, (), RandomState>,
    /// Refusals of a node's next uploaded-version publication, each keeping whether a publication
    /// met it and whether the node's reconciliation took the refused upload up.
    refused_resource_publications:
        DashMap<ClusterNodeName, watch::Sender<ResourcePublicationRefusal>, RandomState>,
    /// Failures of every staging of one kind of a restored domain's checkpoints until cleared,
    /// each keeping whether a staging met it.
    failed_restore_checkpoint_stagings:
        DashMap<RestoreStagingTarget, RestoreStagingFailure, RandomState>,
    /// Failures of every durable publication of a restored domain's state until cleared, each
    /// keeping whether a publication met it.
    failed_restore_state_publications: DashMap<DomainName, RestorePublicationFailure, RandomState>,
    /// One-shot interruptions of a backup's next native metadata section of an entity, after
    /// this many of its entries.
    native_metadata_interruptions: DashMap<CapturedStateOwner, u64, RandomState>,
    /// One-shot interruptions of a backup's next guest save after this many transfer chunks.
    guest_save_interruptions: DashMap<CapturedStateOwner, u64, RandomState>,
    /// One-shot failures, consumed by the next HTTPS listener configuration a node installs.
    failed_https_listener_installations: DashMap<ClusterNodeName, (), RandomState>,
    consensus_probes: DashMap<ClusterNodeName, ConsensusProbeState, RandomState>,
    /// Consensus storage failures armed as each named node builds its storage, before it answers
    /// any Raft traffic.
    startup_consensus_faults: DashMap<ClusterNodeName, StartupConsensusFault, RandomState>,
    /// Nodes armed to move their node database into on-disk tables as they stop, and whether
    /// they did.
    node_database_flushes: DashMap<ClusterNodeName, NodeDatabaseFlush, RandomState>,
    executions: DashMap<ClusterNodeName, NodeExecution, RandomState>,
    failed_health_responders: DashMap<ClusterNodeName, (), RandomState>,
    failed_health_links: DashMap<HealthResponsePauseKey, (), RandomState>,
    /// A blocked peer drops gossip requests until the scenario restores its links.
    blocked_gossip_nodes: DashMap<ClusterNodeName, Duration, RandomState>,
    /// Restart regressions retain one relayed heartbeat until the first scheduling pass finishes.
    startup_voter_gossip: DashMap<ClusterNodeName, StartupVoterGossip, RandomState>,
    /// Whether a restart fixture waits for the restarted cluster's leader to end its startup voter
    /// observation, and whether one has.
    startup_observation_end: watch::Sender<StartupObservationEnd>,
    /// Application health handlers clone a pause so it remains alive after its map guard drops.
    health_response_pauses: DashMap<HealthResponsePauseKey, Arc<TestPause>, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    command_pauses: DashMap<CommandPausePoint, Arc<TestPause>, RandomState>,
    /// The gates every backup cut of a domain waits at, by domain and boundary. Runtime and
    /// harness waiters clone a gate so it remains alive after its map guard drops.
    backup_cut_gates: DashMap<BackupCutGateKey, Arc<BackupCutGate>, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    entity_gate_pauses: DashMap<String, Arc<TestPause>, RandomState>,
    /// A remote node pauses after engaging its gate but before replying to the coordinator.
    entity_gate_response_pauses: DashMap<EntityGateResponsePauseKey, Arc<TestPause>, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    remote_relay_admission_pauses:
        DashMap<RemoteRelayAdmissionPauseKey, Arc<TestPause>, RandomState>,
    /// A receiver pauses after it admitted a remote relay batch and returned the admission to the
    /// batch's owner, before it hands the batch to its local runtime consumers.
    remote_relay_dispatch_pauses: DashMap<String, Arc<TestPause>, RandomState>,
    /// One-shot losses of the terminal record acknowledgement a node returns to the node that
    /// registered it. Each keeps whether it fired, so a scenario waits for the loss instead of
    /// assuming it.
    lost_remote_acknowledgements:
        DashMap<RemoteAcknowledgementLink, watch::Sender<RemoteAcknowledgementLoss>, RandomState>,
    /// Gaps a scenario opens in the live view one node's acknowledgement watchers read. Each keeps
    /// whether a watcher read it, so a scenario waits for the read instead of assuming it.
    live_view_gaps: DashMap<LiveViewGap, watch::Sender<LiveViewGapState>, RandomState>,
    /// Nodes whose relay memory a scenario fills, at once or just before their relay work reaches
    /// an armed point, and the memory each fill holds.
    relay_memory_fills: DashMap<ClusterNodeName, RelayMemoryFill, RandomState>,
    /// The execution budgets each node's relay work charges, registered as the node joins its
    /// cluster so a scenario can fill its relay memory at once.
    relay_memory_owners: DashMap<ClusterNodeName, Executor, RandomState>,
    /// The owner pauses one buffered batch before it acquires a dispatch permit and loads routes.
    owner_relay_fanout_pauses: DashMap<String, Arc<TestPause>, RandomState>,
    /// The owner waits this long before it fans out each buffered batch of a domain's relays, so
    /// the rows its sources admitted reach their consumers slowly, as on a loaded node.
    owner_relay_fanout_delays: DashMap<String, Duration, RandomState>,
    /// A local schedule swap pauses after removing one attached emitter from its relay.
    emitter_swap_after_detach_pauses:
        DashMap<(DomainName, EmitterName), Arc<TestPause>, RandomState>,
    /// A source pauses once inside dispatch so a test can engage quiesce while its loop awaits.
    ingestor_dispatch_pauses: DashMap<DomainNodeRef, Arc<TestPause>, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    ownership_handoff_preparation_pauses: DashMap<String, Arc<TestPause>, RandomState>,
    /// A destination pauses after making a handoff preparation durable but before returning its
    /// response, reproducing an ambiguous prepare RPC outcome.
    ownership_handoff_prepare_response_pauses: DashMap<String, Arc<TestPause>, RandomState>,
    /// Runtime and harness waiters clone a pause so it remains alive after its map guard drops.
    domain_clock_progress_pauses:
        DashMap<DomainClockProgressPausePoint, Arc<TestPause>, RandomState>,
    /// One-shot, node-scoped refusals of the next domain-clock authority reconciliation the node
    /// proposes as leader, consumed before the proposal reaches Raft.
    domain_clock_authority_proposal_rejections: DashMap<ClusterNodeName, (), RandomState>,
    /// A WASM processor branch holds its next guest-state checkpoint at one window, so a scenario
    /// can end the node while the checkpoint is exactly that far.
    wasm_checkpoint_pauses: DashMap<WasmCheckpointPausePoint, Arc<TestPause>, RandomState>,
    /// Wall time already elapsed when the next newly supplied mapping for a domain starts.
    domain_clock_initial_elapsed: DashMap<DomainName, Duration, RandomState>,
    state_replica_polling_paused: AtomicBool,
    /// While set, every node drops every runtime-state checkpoint announcement it receives, as if
    /// each was lost on the way.
    state_checkpoint_announcements_lost: AtomicBool,
    /// While set, every node's WASM guest-state checkpoint fails to reach its stable storage.
    wasm_checkpoint_storage_failing: AtomicBool,
    /// While set, every coordinated WASM reset fails while building its fresh guest instance.
    wasm_state_reset_fresh_initialization_failing: AtomicBool,
    /// While set, every node refuses to install a runtime-state checkpoint it replicates.
    state_replica_installation_failing: AtomicBool,
    syslog_ingestor_bind_ips: DashMap<ClusterNodeName, IpAddr, RandomState>,
    /// A scenario can fail one stream accept on a selected bound address, and observe that the
    /// source consumed the fault before checking its established connections. The armed request
    /// remains available until acknowledged; another request cannot overwrite it before then.
    syslog_listener_faults: watch::Sender<Option<SyslogListenerFault>>,
    branch_instance_expiration_scan_interval: RwLock<Option<Duration>>,
    domain_drain_timeout: RwLock<Option<Duration>>,
    entity_gate_deadline: RwLock<Option<Duration>>,
    scheduler_mode: RwLock<SchedulerMode>,
    leadership_transfers: broadcast::Sender<LeadershipTransferRequest>,
    wasm_state_reset_coordinators:
        DashMap<ClusterNodeName, mpsc::Sender<WasmStateResetRequest>, RandomState>,
}

pub(crate) struct WasmStateResetRequest {
    pub(crate) domain: DomainName,
    pub(crate) processor: ModelName,
    pub(crate) reference: CommandExecutionReference,
    pub(crate) target: WasmStateResetRequestTarget,
    pub(crate) response: oneshot::Sender<error_stack::Result<(), WasmStateResetRequestError>>,
}

pub(crate) enum WasmStateResetRequestTarget {
    Unbranched,
    Branch(Vec<RemoteRuntimeField>),
    AllBranches,
}

#[derive(Debug, thiserror::Error)]
pub enum WasmStateResetRequestError {
    #[error("node '{}' has no WASM state reset coordinator", .node.as_str())]
    CoordinatorUnavailable { node: ClusterNodeName },
    #[error("node '{}' stopped its WASM state reset coordinator", .node.as_str())]
    CoordinatorStopped { node: ClusterNodeName },
    #[error("node '{}' dropped the WASM state reset response", .node.as_str())]
    ResponseDropped { node: ClusterNodeName },
    #[error("WASM processor '{}' state reset failed", .processor.as_str())]
    ResetFailed { processor: ModelName },
}

struct ConsensusProbe {
    observer: nervix_consensus::Observer,
}

#[derive(Debug, Default)]
struct ConsensusProbeState {
    /// Running nodes register their observer; stopped nodes retain the test probe and its controls.
    probe: Option<ConsensusProbe>,
    test_probe: nervix_consensus::ConsensusTestProbe,
}

impl std::fmt::Debug for ConsensusProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusProbe").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct RemoteRelayAdmissionPauseKey {
    domain: String,
    branch: Option<String>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct HealthResponsePauseKey {
    probing_node: ClusterNodeName,
    responding_node: ClusterNodeName,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct EntityGateResponsePauseKey {
    node: ClusterNodeName,
    domain: DomainName,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct EntityScheduleSwapFailureKey {
    node: ClusterNodeName,
    domain: DomainName,
}

/// The direction a record acknowledgement travels: from the node that resolved it back to the
/// node that registered it.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct RemoteAcknowledgementLink {
    resolver: ClusterNodeName,
    registrar: ClusterNodeName,
}

/// Whether the one terminal record acknowledgement a scenario armed a link to lose was lost yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteAcknowledgementLoss {
    Armed,
    Lost,
}

/// Whether the transport failure a scenario armed for a domain's next ownership-handoff gate
/// engagement request was delivered yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandoffEngagementTransportFailure {
    Armed,
    Delivered,
}

/// The command boundary a test controls without racing an election against a request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CommandPausePoint {
    TransactionCleanup {
        node: ClusterNodeName,
        domain: DomainName,
    },
    StandaloneTransaction {
        node: ClusterNodeName,
        domain: DomainName,
        boundary: StandaloneTransactionBoundary,
    },
    DomainResume {
        node: ClusterNodeName,
        domain: DomainName,
        boundary: DomainResumeBoundary,
    },
    Admission(ClusterNodeName),
    RuntimeInstallation(ClusterNodeName),
    InitialClusterParameters(ClusterNodeName),
    ForcedRecoveryMissingSchedule(ClusterNodeName),
    RuntimePreparation(ClusterNodeName),
    ApplicationRevisionResponse(ClusterNodeName),
    ReferenceLookup(ClusterNodeName),
    DurableAdmission(ClusterNodeName),
    Finalization(ClusterNodeName),
    RelocationPublication(DomainName),
    ResponseDelivery(ClusterNodeName),
    ResourceInstallation(ClusterNodeName),
    ResourceSourceDiscovery(ClusterNodeName),
    RestoreStep {
        node_id: ClusterNodeName,
        step: RestoreStep,
    },
    BackupDownload(ClusterNodeName),
    RestoreStatePublication {
        domain: DomainName,
        coordinator: ClusterNodeName,
    },
    RestoreBranchStateConversion(DomainName),
    IngestorRoutePoll(DomainNodeRef),
    OwnershipHandoffFlush(DomainName),
    OwnershipHandoffActivation(DomainName),
    GracefulDrain(ClusterNodeName),
    TransactionCommit {
        node_id: ClusterNodeName,
        domain: String,
        completed_statements: usize,
    },
}

/// How far one WASM guest-state checkpoint has come when a scenario holds it.
///
/// Every window lies after the callback's output was dispatched and before the success
/// acknowledgements the callback decided are released, so each names one crash window of the
/// checkpoint's completion boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumString, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum WasmCheckpointWindow {
    /// The callback's output is dispatched and the guest has not saved its state yet.
    BeforeCapture,
    /// The guest saved its state and the checkpoint has its revision, but none of it is on stable
    /// storage.
    AfterCapture,
    /// The checkpoint is on this node's stable storage and its replicas have not confirmed it.
    AfterLocalDurability,
    /// The checkpoint reached its whole boundary, and is neither committed nor has it released the
    /// acknowledgements it holds.
    BeforeAcknowledgement,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WasmCheckpointPausePoint {
    domain: DomainName,
    processor: ModelName,
    window: WasmCheckpointWindow,
}

#[derive(Clone, Debug)]
pub(crate) struct LeadershipTransferRequest {
    pub from_node_id: ClusterNodeName,
    pub to_node_id: ClusterNodeName,
}

impl Default for FaultInjection {
    fn default() -> Self {
        let (leadership_transfers, _) = broadcast::channel(16);
        let (syslog_listener_faults, _) = watch::channel(None);
        let (startup_observation_end, _) = watch::channel(StartupObservationEnd::Unarmed);
        Self {
            inner: Arc::new(FaultInjectionState {
                emitter_faults: DashMap::default(),
                failed_ingestors: DashMap::default(),
                unavailable_emitter_clients: DashMap::default(),
                stalled_emitter_connectors: DashMap::default(),
                failed_schedule_publications: DashMap::default(),
                wasm_reset_publication_authority_losses: DashMap::default(),
                failed_wasm_reset_publications: DashMap::default(),
                failed_entity_gate_engagements: DashMap::default(),
                handoff_engagement_transport_failures: DashMap::default(),
                lost_handoff_gate_engagements: DashMap::default(),
                refused_relocation_commits: DashMap::default(),
                refused_model_publications: DashMap::default(),
                forced_entity_drain_timeouts: DashMap::default(),
                stale_owner_buffer_drain_reports: DashMap::default(),
                forced_domain_drain_timeouts: DashMap::default(),
                failed_entity_schedule_swaps: DashMap::default(),
                transaction_binding_drops: DashMap::default(),
                delayed_shutdown_cordon_releases: DashMap::default(),
                delayed_source_confirmations: DashMap::default(),
                delayed_stopped_state_writes: DashMap::default(),
                delayed_materialized_dependency_reads: DashMap::default(),
                materialized_snapshot_fetch_holds: DashMap::default(),
                failed_resource_installations: DashMap::default(),
                refused_resource_publications: DashMap::default(),
                failed_restore_checkpoint_stagings: DashMap::default(),
                failed_restore_state_publications: DashMap::default(),
                native_metadata_interruptions: DashMap::default(),
                guest_save_interruptions: DashMap::default(),
                failed_https_listener_installations: DashMap::default(),
                consensus_probes: DashMap::default(),
                startup_consensus_faults: DashMap::default(),
                node_database_flushes: DashMap::default(),
                executions: DashMap::default(),
                failed_health_responders: DashMap::default(),
                failed_health_links: DashMap::default(),
                blocked_gossip_nodes: DashMap::default(),
                startup_voter_gossip: DashMap::default(),
                startup_observation_end,
                health_response_pauses: DashMap::default(),
                command_pauses: DashMap::default(),
                backup_cut_gates: DashMap::default(),
                entity_gate_pauses: DashMap::default(),
                entity_gate_response_pauses: DashMap::default(),
                remote_relay_admission_pauses: DashMap::default(),
                remote_relay_dispatch_pauses: DashMap::default(),
                lost_remote_acknowledgements: DashMap::default(),
                live_view_gaps: DashMap::default(),
                relay_memory_fills: DashMap::default(),
                relay_memory_owners: DashMap::default(),
                owner_relay_fanout_pauses: DashMap::default(),
                owner_relay_fanout_delays: DashMap::default(),
                emitter_swap_after_detach_pauses: DashMap::default(),
                ingestor_dispatch_pauses: DashMap::default(),
                ownership_handoff_preparation_pauses: DashMap::default(),
                ownership_handoff_prepare_response_pauses: DashMap::default(),
                domain_clock_progress_pauses: DashMap::default(),
                domain_clock_authority_proposal_rejections: DashMap::default(),
                wasm_checkpoint_pauses: DashMap::default(),
                domain_clock_initial_elapsed: DashMap::default(),
                state_replica_polling_paused: AtomicBool::new(false),
                state_checkpoint_announcements_lost: AtomicBool::new(false),
                wasm_checkpoint_storage_failing: AtomicBool::new(false),
                wasm_state_reset_fresh_initialization_failing: AtomicBool::new(false),
                state_replica_installation_failing: AtomicBool::new(false),
                syslog_ingestor_bind_ips: DashMap::default(),
                syslog_listener_faults,
                branch_instance_expiration_scan_interval: RwLock::new(None),
                domain_drain_timeout: RwLock::new(None),
                entity_gate_deadline: RwLock::new(None),
                scheduler_mode: RwLock::new(SchedulerMode::default()),
                leadership_transfers,
                wasm_state_reset_coordinators: DashMap::default(),
            }),
        }
    }
}

impl FaultInjection {
    /// Interrupt the next branch lifecycle or Kafka offset section of `entity` that a backup of
    /// `domain` streams, once, after it has asked for `entries` of its entries.
    pub fn interrupt_native_metadata_capture(
        &self,
        domain: DomainName,
        entity: ModelName,
        entries: u64,
    ) {
        self.inner
            .native_metadata_interruptions
            .insert(CapturedStateOwner { domain, entity }, entries);
    }

    /// Interrupt the next guest save of `entity` that a backup of `domain` streams, once, after
    /// writing `chunks` of its bytes.
    pub fn interrupt_guest_save_capture(&self, domain: DomainName, entity: ModelName, chunks: u64) {
        self.inner
            .guest_save_interruptions
            .insert(CapturedStateOwner { domain, entity }, chunks);
    }

    /// Hold the restore of `domain` after its models are applied and its branch lifecycles are
    /// staged, before it converts its first archived deduplicator keyspace or window.
    pub fn pause_restore_branch_state_conversion(&self, domain: DomainName) {
        self.arm_command_pause(CommandPausePoint::RestoreBranchStateConversion(domain));
    }

    pub async fn wait_for_restore_branch_state_conversion_pause(&self, domain: &DomainName) {
        self.wait_for_command_pause(&CommandPausePoint::RestoreBranchStateConversion(
            domain.clone(),
        ))
        .await;
    }

    pub fn release_restore_branch_state_conversion_pause(&self, domain: &DomainName) {
        self.release_command_pause(&CommandPausePoint::RestoreBranchStateConversion(
            domain.clone(),
        ));
    }

    pub(crate) async fn pause_restore_branch_state_conversion_if_armed(&self, domain: &DomainName) {
        self.pause_command_if_armed(CommandPausePoint::RestoreBranchStateConversion(
            domain.clone(),
        ))
        .await;
    }

    pub fn pause_restore_state_publication(
        &self,
        domain: DomainName,
        coordinator: ClusterNodeName,
    ) {
        self.arm_command_pause(CommandPausePoint::RestoreStatePublication {
            domain,
            coordinator,
        });
    }

    pub async fn wait_for_restore_state_publication(
        &self,
        domain: &DomainName,
        coordinator: &ClusterNodeName,
    ) {
        self.wait_for_command_pause(&CommandPausePoint::RestoreStatePublication {
            domain: domain.clone(),
            coordinator: coordinator.clone(),
        })
        .await;
    }

    pub fn release_restore_state_publication(
        &self,
        domain: &DomainName,
        coordinator: &ClusterNodeName,
    ) {
        self.release_command_pause(&CommandPausePoint::RestoreStatePublication {
            domain: domain.clone(),
            coordinator: coordinator.clone(),
        });
    }

    pub(crate) async fn pause_restore_state_publication_if_armed(
        &self,
        domain: &DomainName,
        coordinator: &ClusterNodeName,
    ) {
        let point = CommandPausePoint::RestoreStatePublication {
            domain: domain.clone(),
            coordinator: coordinator.clone(),
        };
        let Some(pause) = self
            .inner
            .command_pauses
            .get(&point)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        if pause.claimed.swap(true, Ordering::AcqRel) {
            return;
        }
        pause.reach();
        pause.wait_until_released().await;
        // Retain the observation until the scenario sees the authority refusal after release.
    }

    pub(crate) fn mark_restore_state_publication_refused(
        &self,
        domain: &DomainName,
        coordinator: &ClusterNodeName,
    ) {
        if let Some(pause) =
            self.inner
                .command_pauses
                .get(&CommandPausePoint::RestoreStatePublication {
                    domain: domain.clone(),
                    coordinator: coordinator.clone(),
                })
        {
            pause.mark_delivered();
        }
    }

    pub async fn wait_for_restore_state_publication_refusal(
        &self,
        domain: &DomainName,
        coordinator: &ClusterNodeName,
    ) {
        let point = CommandPausePoint::RestoreStatePublication {
            domain: domain.clone(),
            coordinator: coordinator.clone(),
        };
        let pause = self
            .inner
            .command_pauses
            .get(&point)
            .assured("the scenario armed this publication pause")
            .clone();
        pause.wait_until_delivered().await;
        self.inner.command_pauses.remove(&point);
    }

    pub(crate) fn native_metadata_capture_interruption(
        &self,
        domain: &DomainName,
        entity: &ModelName,
    ) -> Option<u64> {
        let owner = CapturedStateOwner {
            domain: domain.clone(),
            entity: entity.clone(),
        };
        let (_, entries) = self.inner.native_metadata_interruptions.remove(&owner)?;
        Some(entries)
    }

    pub(crate) fn guest_save_capture_interruption(
        &self,
        domain: &DomainName,
        entity: &ModelName,
    ) -> Option<u64> {
        let owner = CapturedStateOwner {
            domain: domain.clone(),
            entity: entity.clone(),
        };
        let (_, chunks) = self.inner.guest_save_interruptions.remove(&owner)?;
        Some(chunks)
    }

    pub(crate) fn register_wasm_state_reset_coordinator(
        &self,
        node: ClusterNodeName,
    ) -> mpsc::Receiver<WasmStateResetRequest> {
        let (sender, receiver) = mpsc::channel(1);
        self.inner
            .wasm_state_reset_coordinators
            .insert(node, sender);
        receiver
    }

    pub async fn reset_wasm_processor_branch(
        &self,
        node: &ClusterNodeName,
        domain: DomainName,
        processor: ModelName,
        reference: CommandExecutionReference,
        branch: Vec<RemoteRuntimeField>,
    ) -> error_stack::Result<(), WasmStateResetRequestError> {
        self.request_wasm_state_reset(
            node,
            domain,
            processor,
            reference,
            WasmStateResetRequestTarget::Branch(branch),
        )
        .await
    }

    pub async fn reset_unbranched_wasm_processor(
        &self,
        node: &ClusterNodeName,
        domain: DomainName,
        processor: ModelName,
        reference: CommandExecutionReference,
    ) -> error_stack::Result<(), WasmStateResetRequestError> {
        self.request_wasm_state_reset(
            node,
            domain,
            processor,
            reference,
            WasmStateResetRequestTarget::Unbranched,
        )
        .await
    }

    pub async fn reset_all_wasm_processor_branches(
        &self,
        node: &ClusterNodeName,
        domain: DomainName,
        processor: ModelName,
        reference: CommandExecutionReference,
    ) -> error_stack::Result<(), WasmStateResetRequestError> {
        self.request_wasm_state_reset(
            node,
            domain,
            processor,
            reference,
            WasmStateResetRequestTarget::AllBranches,
        )
        .await
    }

    async fn request_wasm_state_reset(
        &self,
        node: &ClusterNodeName,
        domain: DomainName,
        processor: ModelName,
        reference: CommandExecutionReference,
        target: WasmStateResetRequestTarget,
    ) -> error_stack::Result<(), WasmStateResetRequestError> {
        let sender = match self.inner.wasm_state_reset_coordinators.get(node) {
            Some(sender) => sender.clone(),
            None => {
                return Err(Report::new(
                    WasmStateResetRequestError::CoordinatorUnavailable { node: node.clone() },
                ));
            }
        };
        let (response, receiver) = oneshot::channel();
        sender
            .send(WasmStateResetRequest {
                domain,
                processor,
                reference,
                target,
                response,
            })
            .await
            .map_err(|_| {
                Report::new(WasmStateResetRequestError::CoordinatorStopped { node: node.clone() })
            })?;
        receiver.await.map_err(|_| {
            Report::new(WasmStateResetRequestError::ResponseDropped { node: node.clone() })
        })?
    }

    pub fn unregister_consensus(&self, node: &ClusterNodeName) {
        if let Some(mut state) = self.inner.consensus_probes.get_mut(node) {
            state.probe = None;
        }
    }

    pub(crate) fn register_consensus(
        &self,
        node: ClusterNodeName,
        consensus: &nervix_consensus::Consensus,
    ) {
        let mut state = self.inner.consensus_probes.entry(node).or_default();
        state.probe = Some(ConsensusProbe {
            observer: consensus.observer(),
        });
    }

    pub(crate) fn consensus_test_probe(
        &self,
        node: &ClusterNodeName,
    ) -> nervix_consensus::ConsensusTestProbe {
        let test_probe = self
            .inner
            .consensus_probes
            .entry(node.clone())
            .or_default()
            .test_probe
            .clone();
        // This probe is handed to storage before consensus starts, so a failure armed here is in
        // place for the first installation the node is asked to perform.
        if let Some(mut startup) = self.inner.startup_consensus_faults.get_mut(node) {
            let pending = std::mem::replace(&mut *startup, StartupConsensusFault::Armed);
            if let StartupConsensusFault::Pending {
                operation,
                boundary,
            } = pending
            {
                test_probe.storage_fault().fail_next(operation, boundary);
            }
        }
        test_probe
    }

    pub fn block_consensus_connectivity(&self, node: ClusterNodeName) {
        let state = self.inner.consensus_probes.entry(node).or_default();
        state.test_probe.block_connectivity();
    }

    pub fn restore_consensus_connectivity(&self, node: &ClusterNodeName) {
        let state = self.inner.consensus_probes.entry(node.clone()).or_default();
        state.test_probe.restore_connectivity();
    }

    pub fn consensus_observer(&self, node: &ClusterNodeName) -> nervix_consensus::Observer {
        use meticulous::OptionExt as _;
        let state = self
            .inner
            .consensus_probes
            .get(node)
            .verified("the harness registered this node before observing consensus");
        state
            .probe
            .as_ref()
            .verified("the harness started this node before observing consensus")
            .observer
            .clone()
    }

    pub fn consensus_append_stream_open_count(
        &self,
        node: &ClusterNodeName,
        target: &ClusterNodeName,
    ) -> u64 {
        use meticulous::OptionExt as _;
        let state = self
            .inner
            .consensus_probes
            .get(node)
            .verified("the harness registered this node before observing consensus");
        let _running_probe = state
            .probe
            .as_ref()
            .verified("the harness started this node before observing consensus");
        state.test_probe.append_stream_open_count(target)
    }

    pub fn fail_consensus_storage(
        &self,
        node: &ClusterNodeName,
        operation: String,
        boundary: nervix_consensus::StorageBoundary,
    ) {
        use meticulous::OptionExt as _;
        let state = self
            .inner
            .consensus_probes
            .get(node)
            .verified("the harness registered this node before injecting storage failure");
        let _running_probe = state
            .probe
            .as_ref()
            .verified("the harness started this node before injecting storage failure");
        state
            .test_probe
            .storage_fault()
            .fail_next(operation, boundary);
    }

    /// Delay every consensus storage commit on this node, including after its next registration.
    pub fn set_consensus_storage_commit_delay(&self, node: &ClusterNodeName, delay: Duration) {
        let state = self.inner.consensus_probes.entry(node.clone()).or_default();
        state.test_probe.set_storage_commit_delay(delay);
    }

    /// Arm a consensus storage failure for a node that has not started yet.
    ///
    /// Storage a running node already owns cannot be reached before its first installation, so a
    /// scenario that interrupts one arms the failure while the node is stopped.
    pub fn fail_consensus_storage_on_start(
        &self,
        node: &ClusterNodeName,
        operation: String,
        boundary: nervix_consensus::StorageBoundary,
    ) {
        self.inner.startup_consensus_faults.insert(
            node.clone(),
            StartupConsensusFault::Pending {
                operation,
                boundary,
            },
        );
    }

    /// Whether the failure armed for this node as it started has since stopped its storage.
    pub fn consensus_storage_failure_fired(&self, node: &ClusterNodeName) -> bool {
        use meticulous::OptionExt as _;
        let armed = self
            .inner
            .startup_consensus_faults
            .get(node)
            .verified("the scenario armed this node before observing its failure");
        if let StartupConsensusFault::Pending { .. } = &*armed {
            return false;
        }
        let state = self
            .inner
            .consensus_probes
            .get(node)
            .verified("an armed node registered its storage as it started");
        let _running_probe = state
            .probe
            .as_ref()
            .verified("the harness started this node before observing its storage failure");
        !state.test_probe.storage_fault().is_armed()
    }

    /// Arm `node` to move everything its node database holds in memory into on-disk tables the next
    /// time it stops, once every owner of that database has stopped. A long-running node reaches the
    /// same layout when journal rotation flushes its memtables, so the next start reads every record
    /// back from a table rather than from the journal.
    pub fn flush_node_database_on_stop(&self, node: ClusterNodeName) {
        self.inner
            .node_database_flushes
            .insert(node, NodeDatabaseFlush::Armed);
    }

    /// Whether `node` moved its node database into on-disk tables when it last stopped while armed.
    pub fn node_database_flushed(&self, node: &ClusterNodeName) -> bool {
        let flush = self.inner.node_database_flushes.get(node);
        matches!(flush.as_deref(), Some(NodeDatabaseFlush::Flushed))
    }

    /// Called by a stopping node with its node database once every owner of the database has
    /// stopped. An armed node moves every keyspace's memtable into an on-disk table and records
    /// that it did; a failure leaves the node armed, which the scenario reports.
    pub(crate) fn flush_node_database_if_armed(
        &self,
        node: &ClusterNodeName,
        database: &fjall::Database,
    ) {
        let armed = self.inner.node_database_flushes.get(node);
        let Some(NodeDatabaseFlush::Armed) = armed.as_deref() else {
            return;
        };
        drop(armed);
        for name in database.list_keyspace_names() {
            if let Err(error) = Self::flush_keyspace(database, &name) {
                tracing::error!(
                    node = %node,
                    keyspace = %&*name,
                    error = %error,
                    "failed to move a node database keyspace into an on-disk table"
                );
                return;
            }
        }
        self.inner
            .node_database_flushes
            .insert(node.clone(), NodeDatabaseFlush::Flushed);
    }

    /// Moves the memtable of the keyspace `name` into an on-disk table and waits until it is there.
    fn flush_keyspace(database: &fjall::Database, name: &str) -> Result<(), fjall::Error> {
        let keyspace = database.keyspace(name, fjall::KeyspaceCreateOptions::default)?;
        keyspace.rotate_memtable_and_wait()
    }

    pub fn fail_emitter(&self, emitter: &str) {
        self.inner
            .emitter_faults
            .insert(emitter.to_ascii_lowercase(), EmitterFaultMode::Fail);
    }

    pub fn stall_emitter(&self, emitter: &str) {
        self.inner
            .emitter_faults
            .insert(emitter.to_ascii_lowercase(), EmitterFaultMode::Stall);
    }

    pub fn clear_emitter_fault(&self, emitter: &str) {
        self.inner
            .emitter_faults
            .remove(&emitter.to_ascii_lowercase());
    }

    pub fn clear_all_emitter_faults(&self) {
        self.inner.emitter_faults.clear();
    }

    pub fn fail_ingestor(&self, ingestor: &str) {
        self.inner
            .failed_ingestors
            .insert(ingestor.to_ascii_lowercase(), ());
    }

    pub fn clear_ingestor_fault(&self, ingestor: &str) {
        self.inner
            .failed_ingestors
            .remove(&ingestor.to_ascii_lowercase());
    }

    pub fn fail_emitter_client_unavailable(&self, emitter: &str) {
        self.inner
            .unavailable_emitter_clients
            .insert(emitter.to_ascii_lowercase(), ());
    }

    pub fn clear_emitter_client_fault(&self, emitter: &str) {
        self.inner
            .unavailable_emitter_clients
            .remove(&emitter.to_ascii_lowercase());
    }

    /// Makes the emitter of `emitter` stall after it resolved `resolved` records of its next write.
    ///
    /// The emitter still writes every record, but the emitter receives only its answers for the first
    /// `resolved` records in the order it handed them over, as it does from a broker that stops
    /// answering after it accepted the rest. The write then fails the way an unanswered one does,
    /// so the emitter cannot tell which of the other records landed.
    pub fn stall_emitter_connector_after_resolving(&self, emitter: &str, resolved: usize) {
        self.inner
            .stalled_emitter_connectors
            .insert(emitter.to_ascii_lowercase(), resolved);
    }

    /// Fails the next schedule publication for a domain so a test can observe recovery after the
    /// committed model mutation's new schedule does not reach the cluster.
    pub fn fail_next_schedule_publication(&self, domain: &str) {
        self.inner
            .failed_schedule_publications
            .insert(domain.to_ascii_lowercase(), ());
    }

    /// Rejects the next entity gate before any node can engage it.
    pub fn fail_next_entity_gate_engagement(&self, domain: DomainName) {
        self.inner.failed_entity_gate_engagements.insert(domain, ());
    }

    /// Fails the next ownership-handoff gate engagement request a coordinator sends to a remote
    /// node in `domain` in transport, as a broken interconnect stream would, without sending it.
    pub fn break_next_handoff_engagement_transport(&self, domain: DomainName) {
        self.inner
            .handoff_engagement_transport_failures
            .insert(domain, HandoffEngagementTransportFailure::Armed);
    }

    /// Whether the transport failure armed for `domain`'s next ownership-handoff gate engagement
    /// request was delivered.
    pub fn handoff_engagement_transport_failure_delivered(&self, domain: &DomainName) -> bool {
        let Some(failure) = self.inner.handoff_engagement_transport_failures.get(domain) else {
            return false;
        };
        *failure == HandoffEngagementTransportFailure::Delivered
    }

    /// Simulates an incomplete drain observation so a scenario can exercise the local schedule
    /// fence independently of the coordinator's normal quiescence check.
    pub fn report_no_owner_buffered_batches_for_entity_drain(
        &self,
        domain: DomainName,
        node: ClusterNodeName,
    ) {
        self.inner
            .stale_owner_buffer_drain_reports
            .insert((domain, node), ());
    }

    /// Forces the next domain drain to time out after the durable pause has engaged.
    pub fn force_next_domain_drain_timeout(&self, domain: DomainName) {
        self.inner.forced_domain_drain_timeouts.insert(domain, ());
    }

    /// Fails the next incremental entity swap for `domain` on `node`.
    pub fn fail_next_entity_schedule_swap_on(&self, node: ClusterNodeName, domain: DomainName) {
        self.inner
            .failed_entity_schedule_swaps
            .insert(EntityScheduleSwapFailureKey { node, domain }, ());
    }

    pub fn drop_transaction_bindings_on(&self, node_id: ClusterNodeName) {
        self.inner.transaction_binding_drops.insert(node_id, ());
    }

    /// Makes the leader wait `delay` before it releases the cordon that `stopping_node`'s own
    /// graceful shutdown drain set, the next time it releases one, as a loaded worker whose
    /// consensus writes take longer than usual would.
    pub fn delay_shutdown_cordon_release_of(
        &self,
        stopping_node: ClusterNodeName,
        delay: Duration,
    ) {
        self.inner
            .delayed_shutdown_cordon_releases
            .insert(stopping_node, delay);
    }

    /// Makes every source host of `ingestor` wait `delay` after a batch was acknowledged through
    /// the graph before its source confirms the batch's positions on its transport, as a transport
    /// slow to answer an acknowledgement would. A host reads the delay as it starts, so a scenario
    /// arms it before the ingestor starts.
    pub fn delay_source_confirmation(&self, ingestor: DomainNodeRef, delay: Duration) {
        self.inner
            .delayed_source_confirmations
            .insert(ingestor, delay);
    }

    /// Makes every snapshot task that writes the state of `node` wait `delay` before the last
    /// write it makes as that state stops, as storage slow to take a large checkpoint would: the
    /// snapshot task of every branch of a deduplicator or window processor, after its branch
    /// stopped, and the checkpoint writer of the state `node`'s placement keeps, such as a
    /// materialized relay's records, on each cluster node that keeps it, after its domain's stop
    /// told it to write. A wait longer than a stop grace leaves that write running after the task
    /// that owned the state was ended.
    pub fn delay_stopped_state_writes(&self, node: DomainNodeRef, delay: Duration) {
        self.inner.delayed_stopped_state_writes.insert(node, delay);
    }

    /// Makes `processor`, a relay processor, wait `delay` before each read of its materialized
    /// dependencies, as a read slow to reach the node that holds the state would. Every read waits,
    /// including each re-evaluation of a message the processor parked on `REQUIRED WAIT`.
    pub fn delay_materialized_dependency_reads(&self, processor: DomainNodeRef, delay: Duration) {
        self.inner
            .delayed_materialized_dependency_reads
            .insert(processor, delay);
    }

    /// Fails the next resource version `node_id` installs, after the node installed and published
    /// the version and before the upload is promoted, so a scenario can produce a failed version
    /// whose archive is present.
    pub fn fail_next_resource_installation_on(&self, node_id: ClusterNodeName) {
        self.inner.failed_resource_installations.insert(node_id, ());
    }

    /// Fails the next HTTPS listener configuration `node_id` installs, after it loaded the
    /// certificates and before it replaces the configuration its listener serves.
    pub fn fail_next_https_listener_installation_on(&self, node_id: ClusterNodeName) {
        self.inner
            .failed_https_listener_installations
            .insert(node_id, ());
    }

    pub fn arm_health_response_pause(
        &self,
        probing_node: ClusterNodeName,
        responding_node: ClusterNodeName,
    ) {
        self.inner.health_response_pauses.insert(
            HealthResponsePauseKey {
                probing_node,
                responding_node,
            },
            Arc::new(TestPause::default()),
        );
    }

    /// Holds every revision-query reply from this node until the scenario releases it. Local
    /// revision publication and gossip continue while the replies retain their sampled values.
    pub fn pause_application_revision_responses_from(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::ApplicationRevisionResponse(node_id));
    }

    pub async fn wait_for_application_revision_response_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::ApplicationRevisionResponse(
            node_id.clone(),
        ))
        .await;
    }

    pub fn release_application_revision_responses_from(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::ApplicationRevisionResponse(
            node_id.clone(),
        ));
    }

    pub(crate) async fn pause_application_revision_response_if_armed(
        &self,
        node_id: &ClusterNodeName,
    ) {
        let point = CommandPausePoint::ApplicationRevisionResponse(node_id.clone());
        let Some(pause) = self
            .inner
            .command_pauses
            .get(&point)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
    }

    pub fn fail_health_responses_from(&self, responding_node: ClusterNodeName) {
        self.inner
            .failed_health_responders
            .insert(responding_node, ());
    }

    pub fn fail_health_responses_between(
        &self,
        probing_node: ClusterNodeName,
        responding_node: ClusterNodeName,
    ) {
        self.inner.failed_health_links.insert(
            HealthResponsePauseKey {
                probing_node,
                responding_node,
            },
            (),
        );
    }

    pub fn restore_health_responses_between(
        &self,
        probing_node: &ClusterNodeName,
        responding_node: &ClusterNodeName,
    ) {
        self.inner
            .failed_health_links
            .remove(&HealthResponsePauseKey {
                probing_node: probing_node.clone(),
                responding_node: responding_node.clone(),
            });
    }

    pub fn block_gossip_for_node(&self, node: ClusterNodeName, send_delay: Duration) {
        self.inner.blocked_gossip_nodes.insert(node, send_delay);
    }

    pub fn restore_gossip_for_node(&self, node: &ClusterNodeName) {
        self.inner.blocked_gossip_nodes.remove(node);
    }

    pub(crate) fn gossip_exchange_is_blocked(
        &self,
        sending_node: &ClusterNodeName,
        receiving_node: &ClusterNodeName,
    ) -> bool {
        self.inner.blocked_gossip_nodes.contains_key(sending_node)
            || self.inner.blocked_gossip_nodes.contains_key(receiving_node)
            || self.startup_voter_gossip_exchange_is_blocked(sending_node, receiving_node)
    }

    pub(crate) fn gossip_send_delay(&self, destination: &ClusterNodeName) -> Option<Duration> {
        self.inner
            .blocked_gossip_nodes
            .get(destination)
            .map(|delay| *delay.value())
    }

    pub async fn wait_for_health_response_pause(
        &self,
        probing_node: &ClusterNodeName,
        responding_node: &ClusterNodeName,
    ) {
        let pause = self.health_response_pause(probing_node, responding_node);
        pause.wait_until_reached().await;
    }

    pub fn release_health_response_pause(
        &self,
        probing_node: &ClusterNodeName,
        responding_node: &ClusterNodeName,
    ) {
        let pause = self.health_response_pause(probing_node, responding_node);
        pause.release();
    }

    pub fn release_all_health_responses(&self) {
        for pause in &self.inner.health_response_pauses {
            pause.release();
        }
        self.inner.health_response_pauses.clear();
        self.inner.failed_health_responders.clear();
    }

    pub fn pause_command_admission_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::Admission(node_id));
    }

    /// Holds initial live-policy discovery after the public observation listeners start.
    pub fn pause_initial_cluster_parameters_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::InitialClusterParameters(node_id));
    }

    pub async fn wait_for_initial_cluster_parameters_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::InitialClusterParameters(
            node_id.clone(),
        ))
        .await;
    }

    pub fn release_initial_cluster_parameters_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::InitialClusterParameters(
            node_id.clone(),
        ));
    }

    /// Holds the next runtime installation the node begins, inside its installation sequence and
    /// before it synchronizes any domain lifecycle or schedule of the admitted revision.
    pub fn pause_runtime_installation_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::RuntimeInstallation(node_id));
    }

    pub async fn wait_for_runtime_installation_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::RuntimeInstallation(node_id.clone()))
            .await;
    }

    pub fn release_runtime_installation_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::RuntimeInstallation(node_id.clone()));
    }

    /// Resume a held initial installation only after forced recovery observes its missing
    /// schedule. The observation precedes release, so the fixture never races two scenario steps
    /// against the recovery budget.
    pub fn resume_installation_after_missing_recovery_schedule(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::ForcedRecoveryMissingSchedule(node_id));
    }

    pub async fn wait_for_missing_recovery_schedule(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::ForcedRecoveryMissingSchedule(
            node_id.clone(),
        ))
        .await;
    }

    pub(crate) fn missing_recovery_schedule_if_armed(&self, node_id: &ClusterNodeName) {
        let point = CommandPausePoint::ForcedRecoveryMissingSchedule(node_id.clone());
        let pause = match self.inner.command_pauses.get(&point) {
            Some(pause) => pause.value().clone(),
            None => return,
        };
        if pause.claimed.swap(true, Ordering::AcqRel) {
            return;
        }
        pause.reach();
        self.release_runtime_installation_pause(node_id);
        // Keep the one-shot observation until the scenario observes it or finishes cleanup.
    }

    pub fn pause_runtime_preparation_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::RuntimePreparation(node_id));
    }

    pub async fn wait_for_runtime_preparation_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::RuntimePreparation(node_id.clone()))
            .await;
    }

    pub fn release_runtime_preparation_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::RuntimePreparation(node_id.clone()));
    }

    pub async fn wait_for_command_admission_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::Admission(node_id.clone()))
            .await;
    }

    pub fn release_command_admission_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::Admission(node_id.clone()));
    }

    /// Holds a command after its leader-local reference lookup and before its replicated proposal.
    pub fn pause_command_reference_lookup_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::ReferenceLookup(node_id));
    }

    pub async fn wait_for_command_reference_lookup_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::ReferenceLookup(node_id.clone()))
            .await;
    }

    pub fn release_command_reference_lookup_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::ReferenceLookup(node_id.clone()));
    }

    /// Holds the next persistent command after its applying record is committed and before its
    /// effect begins.
    pub fn pause_command_after_durable_admission_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::DurableAdmission(node_id));
    }

    pub async fn wait_for_command_durable_admission_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::DurableAdmission(node_id.clone()))
            .await;
    }

    pub fn release_command_durable_admission_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::DurableAdmission(node_id.clone()));
    }

    /// Holds the next persistent command whose effects returned a terminal result, before the
    /// node proposes that outcome for its execution reference.
    pub fn pause_command_finalization_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::Finalization(node_id));
    }

    pub async fn wait_for_command_finalization_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::Finalization(node_id.clone()))
            .await;
    }

    pub fn release_command_finalization_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::Finalization(node_id.clone()));
    }

    /// Holds a relocation after it has computed its target schedule but before it validates and
    /// publishes the captured planning inputs.
    pub fn pause_relocation_publication(&self, domain: DomainName) {
        self.arm_command_pause(CommandPausePoint::RelocationPublication(domain));
    }

    pub async fn wait_for_relocation_publication_pause(&self, domain: &DomainName) {
        self.wait_for_command_pause(&CommandPausePoint::RelocationPublication(domain.clone()))
            .await;
    }

    pub fn release_relocation_publication_pause(&self, domain: &DomainName) {
        self.release_command_pause(&CommandPausePoint::RelocationPublication(domain.clone()));
    }

    /// Holds the next completed command after its result exists but before either public session
    /// transport can deliver it.
    pub fn pause_command_response_delivery_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::ResponseDelivery(node_id));
    }

    pub async fn wait_for_command_response_delivery_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::ResponseDelivery(node_id.clone()))
            .await;
    }

    pub fn release_command_response_delivery_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::ResponseDelivery(node_id.clone()));
    }

    pub fn release_all_command_pauses(&self) {
        for pause in &self.inner.command_pauses {
            pause.release();
        }
        self.inner.command_pauses.clear();
    }

    pub fn pause_resource_installation_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::ResourceInstallation(node_id));
    }

    pub async fn wait_for_resource_installation_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::ResourceInstallation(node_id.clone()))
            .await;
    }

    pub fn release_resource_installation_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::ResourceInstallation(node_id.clone()));
    }

    /// Holds the next restore on `node_id` just before it applies `step`, once.
    pub fn pause_restore_step_on(&self, node_id: ClusterNodeName, step: RestoreStep) {
        self.arm_command_pause(CommandPausePoint::RestoreStep { node_id, step });
    }

    pub async fn wait_for_restore_step_pause(&self, node_id: &ClusterNodeName, step: &RestoreStep) {
        self.wait_for_command_pause(&CommandPausePoint::RestoreStep {
            node_id: node_id.clone(),
            step: step.clone(),
        })
        .await;
    }

    pub fn release_restore_step_pause(&self, node_id: &ClusterNodeName, step: &RestoreStep) {
        self.release_command_pause(&CommandPausePoint::RestoreStep {
            node_id: node_id.clone(),
            step: step.clone(),
        });
    }

    /// Holds the next backup archive download on `node_id`, once, after the node opened the
    /// retained archive and before it streams any frame of it, so the archive is not collected.
    pub fn pause_backup_download_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::BackupDownload(node_id));
    }

    pub async fn wait_for_backup_download_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::BackupDownload(node_id.clone()))
            .await;
    }

    pub fn release_backup_download_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::BackupDownload(node_id.clone()));
    }

    pub fn pause_transaction_commit_after(
        &self,
        node_id: ClusterNodeName,
        domain: impl Into<String>,
        completed_statements: usize,
    ) {
        self.arm_command_pause(CommandPausePoint::TransactionCommit {
            node_id,
            domain: domain.into().to_ascii_lowercase(),
            completed_statements,
        });
    }

    pub async fn wait_for_transaction_commit_pause(
        &self,
        node_id: &ClusterNodeName,
        domain: &str,
        completed_statements: usize,
    ) {
        self.wait_for_command_pause(&CommandPausePoint::TransactionCommit {
            node_id: node_id.clone(),
            domain: domain.to_ascii_lowercase(),
            completed_statements,
        })
        .await;
    }

    pub fn release_transaction_commit_pause(
        &self,
        node_id: &ClusterNodeName,
        domain: &str,
        completed_statements: usize,
    ) {
        self.release_command_pause(&CommandPausePoint::TransactionCommit {
            node_id: node_id.clone(),
            domain: domain.to_ascii_lowercase(),
            completed_statements,
        });
    }

    pub fn pause_entity_gate(&self, domain: impl Into<String>) {
        self.inner.entity_gate_pauses.insert(
            domain.into().to_ascii_lowercase(),
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_entity_gate_pause(&self, domain: &str) {
        let key = domain.to_ascii_lowercase();
        let pause = self.entity_gate_pause(&key);
        pause.wait_until_reached().await;
    }

    pub fn release_entity_gate_pause(&self, domain: &str) {
        let pause = self.entity_gate_pause(&domain.to_ascii_lowercase());
        pause.release();
    }

    pub fn pause_entity_gate_response_on(&self, node: ClusterNodeName, domain: DomainName) {
        self.inner.entity_gate_response_pauses.insert(
            EntityGateResponsePauseKey { node, domain },
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_entity_gate_response_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
    ) {
        let key = EntityGateResponsePauseKey {
            node: node.clone(),
            domain: domain.clone(),
        };
        let pause = self.entity_gate_response_pause(&key);
        pause.wait_until_reached().await;
    }

    pub fn release_entity_gate_response_pause(&self, node: &ClusterNodeName, domain: &DomainName) {
        let key = EntityGateResponsePauseKey {
            node: node.clone(),
            domain: domain.clone(),
        };
        let pause = self.entity_gate_response_pause(&key);
        pause.release();
    }

    pub fn pause_remote_relay_admission(&self, domain: impl Into<String>) {
        self.pause_remote_relay_admission_for_branch(domain, None);
    }

    pub fn pause_remote_relay_admission_for_branch(
        &self,
        domain: impl Into<String>,
        branch: Option<String>,
    ) {
        self.inner.remote_relay_admission_pauses.insert(
            RemoteRelayAdmissionPauseKey {
                domain: domain.into().to_ascii_lowercase(),
                branch,
            },
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_remote_relay_admission_pause(&self, domain: &str) {
        self.wait_for_remote_relay_admission_pause_for_branch(domain, None)
            .await;
    }

    pub async fn wait_for_remote_relay_admission_pause_for_branch(
        &self,
        domain: &str,
        branch: Option<&str>,
    ) {
        let key = RemoteRelayAdmissionPauseKey {
            domain: domain.to_ascii_lowercase(),
            branch: branch.map(str::to_string),
        };
        let pause = self.remote_relay_admission_pause(&key);
        pause.wait_until_reached().await;
    }

    pub fn release_remote_relay_admission_pause(&self, domain: &str) {
        self.release_remote_relay_admission_pause_for_branch(domain, None);
    }

    pub fn release_remote_relay_admission_pause_for_branch(
        &self,
        domain: &str,
        branch: Option<&str>,
    ) {
        let key = RemoteRelayAdmissionPauseKey {
            domain: domain.to_ascii_lowercase(),
            branch: branch.map(str::to_string),
        };
        let pause = self.remote_relay_admission_pause(&key);
        pause.release();
    }

    pub fn pause_remote_relay_dispatch(&self, domain: impl Into<String>) {
        self.inner.remote_relay_dispatch_pauses.insert(
            domain.into().to_ascii_lowercase(),
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_remote_relay_dispatch_pause(&self, domain: &str) {
        let pause = self.remote_relay_dispatch_pause(&domain.to_ascii_lowercase());
        pause.wait_until_reached().await;
    }

    pub fn release_remote_relay_dispatch_pause(&self, domain: &str) {
        let pause = self.remote_relay_dispatch_pause(&domain.to_ascii_lowercase());
        pause.release();
    }

    /// Loses the next terminal record acknowledgement `resolver` returns to `registrar`. The
    /// resolving node drops it instead of sending it, as a link that loses every attempt of the
    /// send does, and reports nothing more about that record.
    pub fn lose_next_remote_acknowledgement(
        &self,
        resolver: ClusterNodeName,
        registrar: ClusterNodeName,
    ) {
        let link = RemoteAcknowledgementLink {
            resolver,
            registrar,
        };
        let (loss, _) = watch::channel(RemoteAcknowledgementLoss::Armed);
        self.inner.lost_remote_acknowledgements.insert(link, loss);
    }

    /// Waits until the loss armed from `resolver` to `registrar` has taken an acknowledgement.
    pub async fn wait_for_lost_remote_acknowledgement(
        &self,
        resolver: &ClusterNodeName,
        registrar: &ClusterNodeName,
    ) {
        let link = RemoteAcknowledgementLink {
            resolver: resolver.clone(),
            registrar: registrar.clone(),
        };
        let mut loss = {
            let armed = self
                .inner
                .lost_remote_acknowledgements
                .get(&link)
                .assured("a scenario waits only for a loss it armed");
            armed.subscribe()
        };
        loss.wait_for(|loss| *loss == RemoteAcknowledgementLoss::Lost)
            .await
            .assured("the armed loss keeps its sender for the rest of the scenario");
    }

    pub fn pause_owner_relay_fanout(&self, domain: impl Into<String>) {
        self.inner.owner_relay_fanout_pauses.insert(
            domain.into().to_ascii_lowercase(),
            Arc::new(TestPause::default()),
        );
    }

    /// Makes the owner of each of `domain`'s relays wait `delay` before it fans out every batch.
    pub fn delay_owner_relay_fanout(&self, domain: impl Into<String>, delay: Duration) {
        self.inner
            .owner_relay_fanout_delays
            .insert(domain.into().to_ascii_lowercase(), delay);
    }

    pub async fn wait_for_owner_relay_fanout_pause(&self, domain: &str) {
        let pause = self.owner_relay_fanout_pause(&domain.to_ascii_lowercase());
        pause.wait_until_reached().await;
    }

    pub fn release_owner_relay_fanout_pause(&self, domain: &str) {
        let pause = self.owner_relay_fanout_pause(&domain.to_ascii_lowercase());
        pause.release();
    }

    pub async fn wait_for_owner_relay_fanout_completion(&self, domain: &str) {
        let key = domain.to_ascii_lowercase();
        let pause = self.owner_relay_fanout_pause(&key);
        pause.wait_until_delivered().await;
        self.inner.owner_relay_fanout_pauses.remove(&key);
    }

    pub fn pause_emitter_swap_after_detach(&self, domain: DomainName, emitter: EmitterName) {
        self.inner
            .emitter_swap_after_detach_pauses
            .insert((domain, emitter), Arc::new(TestPause::default()));
    }

    pub async fn wait_for_emitter_swap_after_detach_pause(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) {
        let pause = self.emitter_swap_after_detach_pause(domain, emitter);
        pause.wait_until_reached().await;
    }

    pub fn release_emitter_swap_after_detach_pause(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) {
        let pause = self.emitter_swap_after_detach_pause(domain, emitter);
        pause.release();
    }

    /// Holds one route after accepting its input and before observing handoff or flush state.
    pub fn pause_ingestor_route_poll(&self, ingestor: DomainNodeRef) {
        self.arm_command_pause(CommandPausePoint::IngestorRoutePoll(ingestor));
    }

    pub async fn wait_for_ingestor_route_poll_pause(&self, ingestor: &DomainNodeRef) {
        self.wait_for_command_pause(&CommandPausePoint::IngestorRoutePoll(ingestor.clone()))
            .await;
    }

    pub fn release_ingestor_route_poll_pause(&self, ingestor: &DomainNodeRef) {
        self.release_command_pause(&CommandPausePoint::IngestorRoutePoll(ingestor.clone()));
    }

    pub(crate) async fn pause_ingestor_route_poll_if_armed(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        source: &RelayName,
    ) {
        let ingestor = DomainNodeRef::node_in(domain.clone(), kind, ModelName::from(source));
        self.pause_command_if_armed(CommandPausePoint::IngestorRoutePoll(ingestor))
            .await;
    }

    /// Holds handoff after publishing its force-flush generation, before declaring the hold ready.
    pub fn pause_ownership_handoff_flush(&self, domain: DomainName) {
        self.arm_command_pause(CommandPausePoint::OwnershipHandoffFlush(domain));
    }

    pub async fn wait_for_ownership_handoff_flush_pause(&self, domain: &DomainName) {
        self.wait_for_command_pause(&CommandPausePoint::OwnershipHandoffFlush(domain.clone()))
            .await;
    }

    pub fn release_ownership_handoff_flush_pause(&self, domain: &DomainName) {
        self.release_command_pause(&CommandPausePoint::OwnershipHandoffFlush(domain.clone()));
    }

    pub(crate) async fn pause_ownership_handoff_flush_if_armed(&self, domain: &DomainName) {
        self.pause_command_if_armed(CommandPausePoint::OwnershipHandoffFlush(domain.clone()))
            .await;
    }

    pub fn pause_ingestor_dispatch(&self, ingestor: DomainNodeRef) {
        self.inner
            .ingestor_dispatch_pauses
            .insert(ingestor, Arc::new(TestPause::default()));
    }

    pub async fn wait_for_ingestor_dispatch_pause(&self, ingestor: &DomainNodeRef) {
        let pause = self
            .inner
            .ingestor_dispatch_pauses
            .get(ingestor)
            .assured("the scenario arms this ingestor's dispatch pause before waiting")
            .value()
            .clone();
        pause.wait_until_reached().await;
    }

    pub fn release_ingestor_dispatch_pause(&self, ingestor: &DomainNodeRef) {
        let pause = self
            .inner
            .ingestor_dispatch_pauses
            .get(ingestor)
            .assured("the scenario arms this ingestor's dispatch pause before releasing it")
            .value()
            .clone();
        pause.release();
    }

    pub fn pause_ownership_handoff_after_preparation(&self, domain: impl Into<String>) {
        self.inner.ownership_handoff_preparation_pauses.insert(
            domain.into().to_ascii_lowercase(),
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_ownership_handoff_preparation_pause(&self, domain: &str) {
        let key = domain.to_ascii_lowercase();
        let pause = self.ownership_handoff_preparation_pause(&key);
        pause.wait_until_reached().await;
    }

    pub fn release_ownership_handoff_preparation_pause(&self, domain: &str) {
        let pause = self.ownership_handoff_preparation_pause(&domain.to_ascii_lowercase());
        pause.release();
    }

    pub fn pause_ownership_handoff_prepare_response(&self, domain: impl Into<String>) {
        self.inner.ownership_handoff_prepare_response_pauses.insert(
            domain.into().to_ascii_lowercase(),
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_ownership_handoff_prepare_response_pause(&self, domain: &str) {
        let key = domain.to_ascii_lowercase();
        let pause = self.ownership_handoff_prepare_response_pause(&key);
        pause.wait_until_reached().await;
    }

    pub fn release_ownership_handoff_prepare_response_pause(&self, domain: &str) {
        let pause = self.ownership_handoff_prepare_response_pause(&domain.to_ascii_lowercase());
        pause.release();
    }

    /// Hold the next guest-state checkpoint of `processor` at `window`, on whichever node and branch
    /// reaches it first. The pause is one-shot: every later checkpoint passes the window, including
    /// the checkpoints of a node that restarted while the first one was held.
    pub fn pause_wasm_checkpoint(
        &self,
        domain: DomainName,
        processor: ModelName,
        window: WasmCheckpointWindow,
    ) {
        self.inner.wasm_checkpoint_pauses.insert(
            WasmCheckpointPausePoint {
                domain,
                processor,
                window,
            },
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_wasm_checkpoint_pause(
        &self,
        domain: DomainName,
        processor: ModelName,
        window: WasmCheckpointWindow,
    ) {
        let point = WasmCheckpointPausePoint {
            domain,
            processor,
            window,
        };
        let pause = self.wasm_checkpoint_pause(&point);
        pause.wait_until_reached().await;
    }

    /// Release every held WASM guest-state checkpoint, so a checkpoint whose node still runs
    /// continues from its window.
    pub fn release_all_wasm_checkpoint_pauses(&self) {
        for pause in self.inner.wasm_checkpoint_pauses.iter() {
            pause.value().release();
        }
    }

    pub fn pause_state_replica_polling(&self) {
        self.inner
            .state_replica_polling_paused
            .store(true, Ordering::Release);
    }

    /// Make every node drop every runtime-state checkpoint announcement it receives, so replicas
    /// learn of new checkpoints only through their own catch-up rounds.
    pub fn lose_state_checkpoint_announcements(&self) {
        self.inner
            .state_checkpoint_announcements_lost
            .store(true, Ordering::Release);
    }

    /// Make every node's WASM guest-state checkpoint fail to reach its stable storage, as a failing
    /// disk would, until [`Self::restore_wasm_checkpoint_storage`].
    pub fn fail_wasm_checkpoint_storage(&self) {
        self.inner
            .wasm_checkpoint_storage_failing
            .store(true, Ordering::Release);
    }

    pub fn restore_wasm_checkpoint_storage(&self) {
        self.inner
            .wasm_checkpoint_storage_failing
            .store(false, Ordering::Release);
    }

    /// Make fresh guest construction fail before a reset publishes its new generation.
    pub fn fail_wasm_state_reset_fresh_initialization(&self) {
        self.inner
            .wasm_state_reset_fresh_initialization_failing
            .store(true, Ordering::Release);
    }

    pub fn restore_wasm_state_reset_fresh_initialization(&self) {
        self.inner
            .wasm_state_reset_fresh_initialization_failing
            .store(false, Ordering::Release);
    }

    /// Make every node refuse to install the runtime-state checkpoints it replicates, until
    /// [`Self::restore_state_replica_installation`].
    pub fn fail_state_replica_installation(&self) {
        self.inner
            .state_replica_installation_failing
            .store(true, Ordering::Release);
    }

    pub fn restore_state_replica_installation(&self) {
        self.inner
            .state_replica_installation_failing
            .store(false, Ordering::Release);
    }

    pub fn set_syslog_ingestor_bind_ip(&self, node_id: ClusterNodeName, host: IpAddr) {
        self.inner.syslog_ingestor_bind_ips.insert(node_id, host);
    }

    pub fn fail_next_syslog_stream_accept(&self, bind_addr: String) -> watch::Receiver<bool> {
        let (fired, observed) = watch::channel(false);
        assert!(
            self.inner.syslog_listener_faults.receiver_count() > 0,
            "a Syslog source must be subscribed before its listener fault is armed"
        );
        self.inner.syslog_listener_faults.send_modify(|pending| {
            if let Some(fault) = pending {
                assert!(
                    *fault.fired.borrow(),
                    "observe the armed Syslog listener fault before arming another"
                );
            }
            *pending = Some(SyslogListenerFault { bind_addr, fired });
        });
        observed
    }

    pub(crate) fn syslog_listener_faults(&self) -> watch::Sender<Option<SyslogListenerFault>> {
        self.inner.syslog_listener_faults.clone()
    }

    pub fn set_branch_instance_expiration_scan_interval(&self, interval: Duration) {
        *self.inner.branch_instance_expiration_scan_interval.write() = Some(interval);
    }

    pub fn set_domain_drain_timeout(&self, timeout: Duration) {
        *self.inner.domain_drain_timeout.write() = Some(timeout);
    }

    pub fn set_entity_gate_deadline(&self, deadline: Duration) {
        *self.inner.entity_gate_deadline.write() = Some(deadline);
    }

    pub fn set_scheduler_mode(&self, mode: SchedulerMode) {
        *self.inner.scheduler_mode.write() = mode;
    }

    pub fn request_leadership_transfer(
        &self,
        from_node_id: ClusterNodeName,
        to_node_id: ClusterNodeName,
    ) {
        self.inner
            .leadership_transfers
            .send(LeadershipTransferRequest {
                from_node_id,
                to_node_id,
            })
            .means_shutdown("leadership transfer watcher");
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario selects one retained emitter fault mode",
            key = "one test emitter",
            bound = "one keyed read; the guard ends with the synchronous predicate"
        )
    )]
    pub(crate) fn emitter_should_fail(&self, emitter: &EmitterName) -> bool {
        self.inner
            .emitter_faults
            .get(&emitter.as_str().to_ascii_lowercase())
            .is_some_and(|mode| *mode == EmitterFaultMode::Fail)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario selects one retained emitter stall mode",
            key = "one test emitter",
            bound = "one keyed read; the guard ends with the synchronous predicate"
        )
    )]
    pub(crate) fn emitter_should_stall(&self, emitter: &EmitterName) -> bool {
        self.inner
            .emitter_faults
            .get(&emitter.as_str().to_ascii_lowercase())
            .is_some_and(|mode| *mode == EmitterFaultMode::Stall)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario selects one retained ingestor failure seam",
            key = "one test ingestor",
            bound = "one keyed presence read with no retained guard"
        )
    )]
    pub(crate) fn ingestor_is_failed(&self, ingestor: &IngestorName) -> bool {
        self.inner
            .failed_ingestors
            .contains_key(&ingestor.as_str().to_ascii_lowercase())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario selects one retained emitter unavailability seam",
            key = "one test emitter",
            bound = "one keyed presence read with no retained guard"
        )
    )]
    pub(crate) fn emitter_client_is_unavailable(&self, emitter: &EmitterName) -> bool {
        self.inner
            .unavailable_emitter_clients
            .contains_key(&emitter.as_str().to_ascii_lowercase())
    }

    /// Consumes the emitter stall armed for the next write of `emitter`: how many of its records the
    /// emitter resolves before it stalls.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario consumes its single armed emitter-write stall",
            key = "one test emitter",
            bound = "one keyed removal; no guard survives the operation"
        )
    )]
    pub(crate) fn take_emitter_connector_stall(&self, emitter: &EmitterName) -> Option<usize> {
        self.inner
            .stalled_emitter_connectors
            .remove(&emitter.as_str().to_ascii_lowercase())
            .map(|(_, resolved)| resolved)
    }

    /// Consumes an armed fault so the rollback publication can still reach the cluster.
    pub(crate) fn take_armed_schedule_publication_fault(&self, domain: &DomainName) -> bool {
        self.inner
            .failed_schedule_publications
            .remove(&domain.as_str().to_ascii_lowercase())
            .is_some()
    }

    pub(crate) fn take_failed_entity_gate_engagement(&self, domain: &DomainName) -> bool {
        self.inner
            .failed_entity_gate_engagements
            .remove(domain)
            .is_some()
    }

    /// Delivers the armed transport failure of `domain`'s next ownership-handoff gate engagement
    /// request once, and records that it was delivered.
    pub(crate) fn take_handoff_engagement_transport_failure(&self, domain: &DomainName) -> bool {
        let Some(mut failure) = self
            .inner
            .handoff_engagement_transport_failures
            .get_mut(domain)
        else {
            return false;
        };
        if *failure != HandoffEngagementTransportFailure::Armed {
            return false;
        }
        *failure = HandoffEngagementTransportFailure::Delivered;
        true
    }

    pub(crate) fn report_no_owner_buffered_batches(
        &self,
        domain: &DomainName,
        node: &ClusterNodeName,
    ) -> bool {
        self.inner
            .stale_owner_buffer_drain_reports
            .contains_key(&(domain.clone(), node.clone()))
    }

    pub(crate) fn take_forced_domain_drain_timeout(&self, domain: &DomainName) -> bool {
        self.inner
            .forced_domain_drain_timeouts
            .remove(domain)
            .is_some()
    }

    pub(crate) fn take_failed_entity_schedule_swap(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
    ) -> bool {
        self.inner
            .failed_entity_schedule_swaps
            .remove(&EntityScheduleSwapFailureKey {
                node: node.clone(),
                domain: domain.clone(),
            })
            .is_some()
    }

    /// Consumes an armed installation failure for `node_id` at its next installation.
    pub(crate) fn take_armed_resource_installation_failure(
        &self,
        node_id: &ClusterNodeName,
    ) -> bool {
        self.inner
            .failed_resource_installations
            .remove(node_id)
            .is_some()
    }

    /// Consumes an armed listener failure for `node_id` at its next HTTPS listener installation.
    pub(crate) fn take_armed_https_listener_installation_failure(
        &self,
        node_id: &ClusterNodeName,
    ) -> bool {
        self.inner
            .failed_https_listener_installations
            .remove(node_id)
            .is_some()
    }

    /// Consumes an armed drop for `node_id` when the node handles its next transaction command.
    pub(crate) fn take_armed_transaction_binding_drop(&self, node_id: &ClusterNodeName) -> bool {
        self.inner
            .transaction_binding_drops
            .remove(node_id)
            .is_some()
    }

    /// Consumes the wait armed for the release of `stopping_node`'s shutdown drain cordon, when
    /// the leader is about to release it.
    pub(crate) fn take_shutdown_cordon_release_delay(
        &self,
        stopping_node: &ClusterNodeName,
    ) -> Option<Duration> {
        let (_, delay) = self
            .inner
            .delayed_shutdown_cordon_releases
            .remove(stopping_node)?;
        Some(delay)
    }

    pub(crate) fn source_confirmation_delay(&self, ingestor: &DomainNodeRef) -> Option<Duration> {
        let delay = self.inner.delayed_source_confirmations.get(ingestor)?;
        Some(*delay)
    }

    pub(crate) fn stopped_state_write_delay(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        entity: &ModelName,
    ) -> Option<Duration> {
        let node = DomainNodeRef::node_in(domain.clone(), kind, entity.clone());
        let delay = self.inner.delayed_stopped_state_writes.get(&node)?;
        Some(*delay)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "a scenario-owned dependency read delay is read before every materialized \
                      dependency read of the processor it names",
            key = "one test processor",
            bound = "one keyed read; its guard ends before the test-controlled wait"
        )
    )]
    pub(crate) async fn delay_materialized_dependency_read_if_armed(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        processor: &ModelName,
    ) {
        let processor = DomainNodeRef::node_in(domain.clone(), kind, processor.clone());
        let delay = self
            .inner
            .delayed_materialized_dependency_reads
            .get(&processor)
            .map(|delay| *delay.value());
        if let Some(delay) = delay {
            nervix_primitives::time::sleep(delay).await;
        }
    }

    pub(crate) async fn pause_transaction_commit_after_progress_if_armed(
        &self,
        node_id: &ClusterNodeName,
        domain: &DomainName,
        completed_statements: usize,
    ) {
        self.pause_command_if_armed(CommandPausePoint::TransactionCommit {
            node_id: node_id.clone(),
            domain: domain.as_str().to_ascii_lowercase(),
            completed_statements,
        })
        .await;
    }

    pub(crate) async fn pause_command_admission_if_armed(&self, node_id: &ClusterNodeName) {
        self.pause_command_if_armed(CommandPausePoint::Admission(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_initial_cluster_parameters_if_armed(
        &self,
        node_id: &ClusterNodeName,
    ) {
        self.pause_command_if_armed(CommandPausePoint::InitialClusterParameters(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_runtime_installation_if_armed(&self, node_id: &ClusterNodeName) {
        self.pause_command_if_armed(CommandPausePoint::RuntimeInstallation(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_runtime_preparation_if_armed(&self, node_id: &ClusterNodeName) {
        self.pause_command_if_armed(CommandPausePoint::RuntimePreparation(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_command_reference_lookup_if_armed(&self, node_id: &ClusterNodeName) {
        self.pause_command_if_armed(CommandPausePoint::ReferenceLookup(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_command_response_delivery_if_armed(&self, node_id: &ClusterNodeName) {
        self.pause_command_if_armed(CommandPausePoint::ResponseDelivery(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_command_after_durable_admission_if_armed(
        &self,
        node_id: &ClusterNodeName,
    ) {
        self.pause_command_if_armed(CommandPausePoint::DurableAdmission(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_command_finalization_if_armed(&self, node_id: &ClusterNodeName) {
        self.pause_command_if_armed(CommandPausePoint::Finalization(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_relocation_publication_if_armed(&self, domain: &DomainName) {
        self.pause_command_if_armed(CommandPausePoint::RelocationPublication(domain.clone()))
            .await;
    }

    pub(crate) async fn pause_resource_installation_if_armed(&self, node_id: &ClusterNodeName) {
        self.pause_command_if_armed(CommandPausePoint::ResourceInstallation(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_restore_step_if_armed(
        &self,
        node_id: &ClusterNodeName,
        step: &RestoreStep,
    ) {
        self.pause_command_if_armed(CommandPausePoint::RestoreStep {
            node_id: node_id.clone(),
            step: step.clone(),
        })
        .await;
    }

    pub(crate) async fn pause_backup_download_if_armed(&self, node_id: &ClusterNodeName) {
        self.pause_command_if_armed(CommandPausePoint::BackupDownload(node_id.clone()))
            .await;
    }

    pub(crate) async fn pause_health_response_if_armed(
        &self,
        probing_node: &ClusterNodeName,
        responding_node: &ClusterNodeName,
    ) {
        let key = HealthResponsePauseKey {
            probing_node: probing_node.clone(),
            responding_node: responding_node.clone(),
        };
        let Some(pause) = self
            .inner
            .health_response_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.health_response_pauses.remove(&key);
    }

    pub(crate) fn health_response_identity(
        &self,
        probing_node: &ClusterNodeName,
        responding_node: ClusterNodeIdentity,
    ) -> ClusterNodeIdentity {
        let link = HealthResponsePauseKey {
            probing_node: probing_node.clone(),
            responding_node: responding_node.node_id().clone(),
        };
        if !self.inner.failed_health_links.contains_key(&link)
            && !self
                .inner
                .failed_health_responders
                .contains_key(responding_node.node_id())
        {
            return responding_node;
        }

        let current_incarnation = responding_node.incarnation().get();
        let failed_incarnation = if current_incarnation == 0 { 1 } else { 0 };
        ClusterNodeIdentity::new(
            responding_node.node_id().clone(),
            ClusterNodeIncarnation::new(failed_incarnation),
        )
    }

    pub(crate) async fn pause_entity_gate_if_armed(&self, domain: &DomainName) {
        let key = domain.as_str().to_ascii_lowercase();
        let Some(pause) = self
            .inner
            .entity_gate_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.entity_gate_pauses.remove(&key);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "one scenario-owned ingestor dispatch pause is claimed once",
            key = "one test domain and ingestor",
            bound = "one keyed read and removal; guards end before the test-controlled wait"
        )
    )]
    pub(crate) async fn pause_ingestor_dispatch_if_armed(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        let Some(pause) = self
            .inner
            .ingestor_dispatch_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        if pause.claimed.swap(true, Ordering::SeqCst) {
            return;
        }
        pause.reach();
        pause.wait_until_released().await;
        self.inner.ingestor_dispatch_pauses.remove(&key);
    }

    pub(crate) async fn pause_entity_gate_response_if_armed(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
    ) {
        let key = EntityGateResponsePauseKey {
            node: node.clone(),
            domain: domain.clone(),
        };
        let Some(pause) = self
            .inner
            .entity_gate_response_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.entity_gate_response_pauses.remove(&key);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "one scenario-owned admission pause chooses an exact or domain selector",
            key = "one test domain and optional branch",
            bound = "at most two keyed reads and one removal; guards end before the \
                     test-controlled wait"
        )
    )]
    pub(crate) async fn pause_remote_relay_admission_if_armed(
        &self,
        domain: &DomainName,
        branch: Option<&str>,
    ) {
        let exact_key = RemoteRelayAdmissionPauseKey {
            domain: domain.as_str().to_ascii_lowercase(),
            branch: branch.map(str::to_string),
        };
        let exact_pause = self
            .inner
            .remote_relay_admission_pauses
            .get(&exact_key)
            .map(|pause| pause.value().clone());
        let (key, pause) = if let Some(pause) = exact_pause {
            (exact_key, pause)
        } else if exact_key.branch.is_some() {
            let domain_key = RemoteRelayAdmissionPauseKey {
                domain: exact_key.domain,
                branch: None,
            };
            let Some(pause) = self
                .inner
                .remote_relay_admission_pauses
                .get(&domain_key)
                .map(|pause| pause.value().clone())
            else {
                return;
            };
            (domain_key, pause)
        } else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.remote_relay_admission_pauses.remove(&key);
    }

    /// Hold a guest-state checkpoint of `processor` at `window` when a scenario armed that window
    /// and no other checkpoint has claimed it.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "one scenario-owned guest checkpoint window is claimed once",
            key = "one test domain, processor and checkpoint window",
            bound = "one keyed read; its guard ends before the test-controlled wait"
        )
    )]
    pub(crate) async fn pause_wasm_checkpoint_if_armed(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        window: WasmCheckpointWindow,
    ) {
        let point = WasmCheckpointPausePoint {
            domain: domain.clone(),
            processor: processor.clone(),
            window,
        };
        let Some(pause) = self
            .inner
            .wasm_checkpoint_pauses
            .get(&point)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        if pause.claimed.swap(true, Ordering::AcqRel) {
            return;
        }
        pause.reach();
        pause.wait_until_released().await;
    }

    /// Whether the terminal record acknowledgement `resolver` is about to return to `registrar` is
    /// lost on the way. Only the first acknowledgement on an armed link is lost.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the scenario drops only the first acknowledgement on an armed link",
            key = "one test resolver and registrar link",
            bound = "one keyed read and a terminal retained watch transition; no guard across \
                     await"
        )
    )]
    pub(crate) fn loses_remote_acknowledgement(
        &self,
        resolver: &ClusterNodeName,
        registrar: &ClusterNodeName,
    ) -> bool {
        let link = RemoteAcknowledgementLink {
            resolver: resolver.clone(),
            registrar: registrar.clone(),
        };
        let Some(loss) = self.inner.lost_remote_acknowledgements.get(&link) else {
            return false;
        };
        loss.send_if_modified(|loss| {
            if *loss == RemoteAcknowledgementLoss::Lost {
                return false;
            }
            *loss = RemoteAcknowledgementLoss::Lost;
            true
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "one scenario-owned remote dispatch pause is consumed at its selected seam",
            key = "one test domain",
            bound = "one keyed read and removal; guards end before the test-controlled wait"
        )
    )]
    pub(crate) async fn pause_remote_relay_dispatch_if_armed(&self, domain: &DomainName) {
        let key = domain.as_str().to_ascii_lowercase();
        let Some(pause) = self
            .inner
            .remote_relay_dispatch_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.remote_relay_dispatch_pauses.remove(&key);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "a scenario-owned relay fanout delay is read for every batch, and one \
                      scenario-owned relay fanout pause is claimed once",
            key = "one test domain",
            bound = "two keyed reads; each guard ends before its test-controlled wait"
        )
    )]
    pub(crate) async fn pause_owner_relay_fanout_if_armed(&self, domain: &DomainName) {
        let key = domain.as_str().to_ascii_lowercase();
        let delay = self
            .inner
            .owner_relay_fanout_delays
            .get(&key)
            .map(|delay| *delay.value());
        if let Some(delay) = delay {
            nervix_primitives::time::sleep(delay).await;
        }
        let Some(pause) = self
            .inner
            .owner_relay_fanout_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        if pause.claimed.swap(true, Ordering::AcqRel) {
            return;
        }
        pause.reach();
        pause.wait_until_released().await;
    }

    pub(crate) fn mark_owner_relay_fanout_complete(&self, domain: &DomainName) {
        let key = domain.as_str().to_ascii_lowercase();
        if let Some(pause) = self.inner.owner_relay_fanout_pauses.get(&key) {
            pause.mark_delivered();
        }
    }

    pub(crate) async fn pause_emitter_swap_after_detach_if_armed(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) {
        let key = (domain.clone(), emitter.clone());
        let Some(pause) = self
            .inner
            .emitter_swap_after_detach_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        if pause.claimed.swap(true, Ordering::AcqRel) {
            return;
        }
        pause.reach();
        pause.wait_until_released().await;
        self.inner.emitter_swap_after_detach_pauses.remove(&key);
    }

    pub(crate) async fn pause_ownership_handoff_after_preparation_if_armed(
        &self,
        domain: &DomainName,
    ) {
        let key = domain.as_str().to_ascii_lowercase();
        let Some(pause) = self
            .inner
            .ownership_handoff_preparation_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.ownership_handoff_preparation_pauses.remove(&key);
    }

    pub(crate) async fn pause_ownership_handoff_prepare_response_if_armed(
        &self,
        domain: &DomainName,
    ) {
        let key = domain.as_str().to_ascii_lowercase();
        let Some(pause) = self
            .inner
            .ownership_handoff_prepare_response_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner
            .ownership_handoff_prepare_response_pauses
            .remove(&key);
    }

    pub(crate) fn syslog_ingestor_bind_addr(
        &self,
        node_id: &ClusterNodeName,
        configured: &str,
    ) -> String {
        let Some(host) = self
            .inner
            .syslog_ingestor_bind_ips
            .get(node_id)
            .map(|host| *host.value())
        else {
            return configured.to_string();
        };
        let Ok(mut addr) = configured.parse::<SocketAddr>() else {
            return configured.to_string();
        };
        addr.set_ip(host);
        addr.to_string()
    }

    pub(crate) fn branch_instance_expiration_scan_interval(&self) -> Option<Duration> {
        *self.inner.branch_instance_expiration_scan_interval.read()
    }

    #[doc(hidden)]
    pub fn domain_drain_timeout(&self) -> Option<Duration> {
        *self.inner.domain_drain_timeout.read()
    }

    pub(crate) fn state_replica_polling_is_paused(&self) -> bool {
        self.inner
            .state_replica_polling_paused
            .load(Ordering::Acquire)
    }

    pub(crate) fn state_checkpoint_announcements_are_lost(&self) -> bool {
        self.inner
            .state_checkpoint_announcements_lost
            .load(Ordering::Acquire)
    }

    pub(crate) fn wasm_checkpoint_storage_fails(&self) -> bool {
        self.inner
            .wasm_checkpoint_storage_failing
            .load(Ordering::Acquire)
    }

    pub(crate) fn wasm_state_reset_fresh_initialization_fails(&self) -> bool {
        self.inner
            .wasm_state_reset_fresh_initialization_failing
            .load(Ordering::Acquire)
    }

    pub(crate) fn state_replica_installation_fails(&self) -> bool {
        self.inner
            .state_replica_installation_failing
            .load(Ordering::Acquire)
    }

    pub(crate) fn entity_gate_deadline(&self) -> Option<Duration> {
        *self.inner.entity_gate_deadline.read()
    }

    pub(crate) fn scheduler_mode(&self) -> SchedulerMode {
        *self.inner.scheduler_mode.read()
    }

    pub(crate) fn subscribe_leadership_transfers(
        &self,
    ) -> broadcast::Receiver<LeadershipTransferRequest> {
        self.inner.leadership_transfers.subscribe()
    }

    fn arm_command_pause(&self, point: CommandPausePoint) {
        self.inner
            .command_pauses
            .insert(point, Arc::new(TestPause::default()));
    }

    fn command_pause(&self, point: &CommandPausePoint) -> Arc<TestPause> {
        let Some(pause) = self.inner.command_pauses.get(point) else {
            panic!("command pause at {point:?} is not armed");
        };
        pause.value().clone()
    }

    fn release_command_pause(&self, point: &CommandPausePoint) {
        let pause = self.command_pause(point);
        pause.release();
    }

    async fn wait_for_command_pause(&self, point: &CommandPausePoint) {
        let pause = self.command_pause(point);
        pause.wait_until_reached().await;
    }

    async fn pause_command_if_armed(&self, point: CommandPausePoint) {
        let Some(pause) = self
            .inner
            .command_pauses
            .get(&point)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        if pause.claimed.swap(true, Ordering::AcqRel) {
            return;
        }
        pause.reach();
        pause.wait_until_released().await;
        self.inner.command_pauses.remove(&point);
    }

    fn health_response_pause(
        &self,
        probing_node: &ClusterNodeName,
        responding_node: &ClusterNodeName,
    ) -> Arc<TestPause> {
        let key = HealthResponsePauseKey {
            probing_node: probing_node.clone(),
            responding_node: responding_node.clone(),
        };
        let Some(pause) = self.inner.health_response_pauses.get(&key) else {
            panic!(
                "health response pause from '{probing_node}' to '{responding_node}' is not armed"
            );
        };
        pause.value().clone()
    }

    fn entity_gate_pause(&self, key: &str) -> Arc<TestPause> {
        let Some(pause) = self.inner.entity_gate_pauses.get(key) else {
            panic!("entity gate pause for domain '{key}' is not armed");
        };
        pause.value().clone()
    }

    fn entity_gate_response_pause(&self, key: &EntityGateResponsePauseKey) -> Arc<TestPause> {
        let Some(pause) = self.inner.entity_gate_response_pauses.get(key) else {
            panic!(
                "entity gate response pause for domain '{}' on node '{}' is not armed",
                key.domain.as_str(),
                key.node
            );
        };
        pause.value().clone()
    }

    fn remote_relay_admission_pause(&self, key: &RemoteRelayAdmissionPauseKey) -> Arc<TestPause> {
        let Some(pause) = self.inner.remote_relay_admission_pauses.get(key) else {
            panic!(
                "remote relay admission pause for domain '{}' and branch {:?} is not armed",
                key.domain, key.branch
            );
        };
        pause.value().clone()
    }

    fn remote_relay_dispatch_pause(&self, key: &str) -> Arc<TestPause> {
        let Some(pause) = self.inner.remote_relay_dispatch_pauses.get(key) else {
            panic!("remote relay dispatch pause for domain '{key}' is not armed");
        };
        pause.value().clone()
    }

    fn owner_relay_fanout_pause(&self, key: &str) -> Arc<TestPause> {
        let Some(pause) = self.inner.owner_relay_fanout_pauses.get(key) else {
            panic!("relay owner fan-out pause for domain '{key}' is not armed");
        };
        pause.value().clone()
    }

    fn emitter_swap_after_detach_pause(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) -> Arc<TestPause> {
        let Some(pause) = self
            .inner
            .emitter_swap_after_detach_pauses
            .get(&(domain.clone(), emitter.clone()))
        else {
            panic!(
                "emitter swap pause for '{}' in domain '{}' is not armed",
                emitter.as_str(),
                domain.as_str()
            );
        };
        pause.value().clone()
    }

    fn ownership_handoff_preparation_pause(&self, key: &str) -> Arc<TestPause> {
        let Some(pause) = self.inner.ownership_handoff_preparation_pauses.get(key) else {
            panic!("ownership handoff preparation pause for domain '{key}' is not armed");
        };
        pause.value().clone()
    }

    fn ownership_handoff_prepare_response_pause(&self, key: &str) -> Arc<TestPause> {
        let Some(pause) = self
            .inner
            .ownership_handoff_prepare_response_pauses
            .get(key)
        else {
            panic!("ownership handoff prepare response pause for domain '{key}' is not armed");
        };
        pause.value().clone()
    }

    fn wasm_checkpoint_pause(&self, point: &WasmCheckpointPausePoint) -> Arc<TestPause> {
        let Some(pause) = self.inner.wasm_checkpoint_pauses.get(point) else {
            panic!("WASM checkpoint pause at {point:?} is not armed");
        };
        pause.value().clone()
    }
}
