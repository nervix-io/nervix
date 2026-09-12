//! Raft replication of the control-plane state a Nervix cluster agrees on.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The log store, the state machine over the replicated state, snapshotting, leadership
//!   observation, granting operation capabilities, and the rkyv Raft protocol carried between
//!   peers.
//! - **Depends on.** The vocabulary for the state it replicates, `fjall` for storage, and the
//!   authenticated interconnect for peer transport.
//! - **Must not know.** What the replicated state means. Domain lifecycle, transactions, validation
//!   and scheduling belong above; this crate agrees on values and hands them back.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Cursor},
    path::Path,
    sync::{
        Arc as StdArc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use error_stack::Report;
use fjall::{Database, Keyspace};
use futures_util::StreamExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::{HandlerRegistrationError, Transport};
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, ClusterSchedule,
    DomainClockAuthority, DomainClockState, DomainName, DomainPace, DomainSchedule,
    DomainStartPoint, DomainState, DomainStatus, ResourceName, ResourceNodeStatus, ResourceUpload,
    ResourceUploadKey, ResourceVersion, ResourceVersionStatus, Statement, UserName,
};
use nervix_recovery::Discarded as _;
pub use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::{
    BasicNode, Config, LogId, Raft, RaftNetworkFactory, Snapshot, SnapshotMeta, StoredMembership,
    Vote,
    error::{ClientWriteError, RPCError, RaftError, StreamingError},
    metrics::RaftServerMetrics,
    network::{RPCOption, RaftNetworkV2},
    type_config::{
        alias::{CommittedLeaderIdOf, EntryOf, LeaderIdOf, WatchReceiverOf},
        async_runtime::watch::WatchReceiver,
    },
};
use parking_lot::Mutex;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{
    sync::{Mutex as AsyncMutex, broadcast, watch},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tracing::{error, info};
use triomphe::Arc;

mod durable_batch;
mod records;
mod replication;
mod retention;
mod snapshot;
pub use retention::RaftRetentionPolicy;
pub use snapshot::SealedSnapshot;
mod storage;
mod storage_fault;

use records::{Records, ResourceRecords, ScheduleRecords};
use replication::AppendPath;
#[cfg(test)]
use storage::FjallLogReader;
use storage::FjallStore;
mod transaction;

#[cfg(any(test, feature = "testing"))]
pub use storage_fault::{StorageBoundary, StorageFault, StoragePause};
mod wire;

pub use transaction::{
    FinishedTransaction, ReplicatedTransaction, TransactionCommandResult, TransactionCommitAdvance,
    TransactionCommitProgress, TransactionDiagnostic, TransactionMutationError,
    TransactionMutationResponse, TransactionOutcome, TransactionQueueLimits, TransactionState,
    TransactionStatement, TransactionStepEffect, TransactionStepResult,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ConsensusCommand {
    ReplaceDomainSchedule {
        domain: DomainName,
        expected_schedule: Option<Box<DomainSchedule>>,
        schedule: Option<Box<DomainSchedule>>,
    },
    ApplyAutomaticDomainSchedule {
        fence: AutomaticScheduleFence,
        domain: DomainName,
        expected_schedule: Option<Box<DomainSchedule>>,
        schedule: Option<Box<DomainSchedule>>,
    },
    PutDomainAndSchedule {
        expected_domain: Option<Box<DomainState>>,
        expected_schedule: Option<Box<DomainSchedule>>,
        domain: Box<DomainState>,
        schedule: Option<Box<DomainSchedule>>,
    },
    PutDomain {
        domain: Box<DomainState>,
    },
    StartDomain {
        domain_id: DomainName,
        start: DomainStartPoint,
        clock: Option<DomainClockState>,
        authority: Option<ClusterNodeIdentity>,
    },
    StopDomain {
        domain_id: DomainName,
    },
    PauseDomain {
        domain_id: DomainName,
    },
    ResumeDomain {
        domain_id: DomainName,
    },
    ReconcileDomainClockAuthority {
        domain_id: DomainName,
        expected_start_version: u64,
        expected_authority: DomainClockAuthority,
        owner: Option<ClusterNodeIdentity>,
    },
    CreateUser {
        user: Box<UserCredentials>,
    },
    CreateResourceCatalog {
        domain: DomainName,
        identifier: ResourceName,
    },
    BeginResourceUpload {
        key: Box<ResourceUploadKey>,
    },
    PublishResourceUpload {
        key: Box<ResourceUploadKey>,
        resource: Box<ResourceVersion>,
        replica: Box<ResourceNodeStatus>,
    },
    PutResourceReplica {
        replica: Box<ResourceNodeStatus>,
    },
    SetNodeCordoned {
        node_id: ClusterNodeName,
        cordoned: bool,
    },
    OpenTransaction {
        transaction: Box<ReplicatedTransaction>,
        max_open_transactions: usize,
    },
    QueueTransactionStatement {
        id: String,
        owner: UserName,
        domain: DomainName,
        at: nervix_models::Timestamp,
        statement: Box<TransactionStatement>,
        limits: TransactionQueueLimits,
    },
    TouchTransaction {
        id: String,
        owner: UserName,
        at: nervix_models::Timestamp,
    },
    StartTransactionCommit {
        id: String,
        owner: UserName,
        at: nervix_models::Timestamp,
    },
    AdvanceTransactionCommit {
        id: String,
        expected_next_statement: usize,
        next_statement: usize,
        at: nervix_models::Timestamp,
        result: Box<TransactionStepResult>,
        effect: Option<Box<TransactionStepEffect>>,
        completion: Option<TransactionOutcome>,
    },
    FinishEmptyTransactionCommit {
        id: String,
        at: nervix_models::Timestamp,
    },
    RevertTransaction {
        id: String,
        owner: UserName,
        at: nervix_models::Timestamp,
    },
    ExpireTransaction {
        id: String,
        at: nervix_models::Timestamp,
        idle_before: nervix_models::Timestamp,
    },
    RemoveFinishedTransactions {
        finished_before: nervix_models::Timestamp,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ConsensusResponse {
    Applied,
    Conflict(String),
    Transaction(Box<TransactionMutationResponse>),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct UserCredentials {
    pub name: UserName,
    pub password_hash: String,
}

impl std::fmt::Display for ConsensusCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReplaceDomainSchedule {
                domain, schedule, ..
            } => {
                if schedule.is_some() {
                    write!(f, "replace-domain-schedule:{}", domain.as_str())
                } else {
                    write!(f, "clear-domain-schedule:{}", domain.as_str())
                }
            }
            Self::ApplyAutomaticDomainSchedule {
                domain, schedule, ..
            } => {
                if schedule.is_some() {
                    write!(f, "apply-automatic-domain-schedule:{}", domain.as_str())
                } else {
                    write!(f, "clear-automatic-domain-schedule:{}", domain.as_str())
                }
            }
            Self::PutDomainAndSchedule { domain, .. } => {
                write!(f, "put-domain-and-schedule:{}", domain.id.as_str())
            }
            Self::PutDomain { domain } => write!(f, "put-domain:{}", domain.id.as_str()),
            Self::StartDomain { domain_id, .. } => write!(f, "start-domain:{}", domain_id.as_str()),
            Self::StopDomain { domain_id } => write!(f, "stop-domain:{}", domain_id.as_str()),
            Self::PauseDomain { domain_id } => write!(f, "pause-domain:{}", domain_id.as_str()),
            Self::ResumeDomain { domain_id } => write!(f, "resume-domain:{}", domain_id.as_str()),
            Self::ReconcileDomainClockAuthority { domain_id, .. } => {
                write!(f, "reconcile-domain-clock-authority:{}", domain_id.as_str())
            }
            Self::CreateUser { user } => write!(f, "create-user:{}", user.name.as_str()),
            Self::CreateResourceCatalog { domain, identifier } => {
                write!(
                    f,
                    "create-resource-catalog:{}.{}",
                    domain.as_str(),
                    identifier.as_str()
                )
            }
            Self::BeginResourceUpload { key } => {
                write!(
                    f,
                    "begin-resource-upload:{}.{}:{}:{}",
                    key.domain.as_str(),
                    key.identifier.as_str(),
                    key.owner.as_str(),
                    key.identity
                )
            }
            Self::PublishResourceUpload { key, resource, .. } => write!(
                f,
                "publish-resource-upload:{}.{}@{}:{}:{}",
                resource.id.domain.as_str(),
                resource.id.identifier.as_str(),
                resource.id.version,
                key.owner.as_str(),
                key.identity
            ),
            Self::PutResourceReplica { replica } => write!(
                f,
                "put-resource-replica:{}.{}@{}:{}",
                replica.key.domain.as_str(),
                replica.key.identifier.as_str(),
                replica.key.version,
                replica.key.node_id
            ),
            Self::SetNodeCordoned { node_id, cordoned } => {
                if *cordoned {
                    write!(f, "cordon-node:{node_id}")
                } else {
                    write!(f, "uncordon-node:{node_id}")
                }
            }
            Self::OpenTransaction { transaction, .. } => {
                write!(f, "open-transaction:{}", transaction.id)
            }
            Self::QueueTransactionStatement { id, .. } => {
                write!(f, "queue-transaction-statement:{id}")
            }
            Self::TouchTransaction { id, .. } => write!(f, "touch-transaction:{id}"),
            Self::StartTransactionCommit { id, .. } => {
                write!(f, "start-transaction-commit:{id}")
            }
            Self::AdvanceTransactionCommit {
                id,
                expected_next_statement,
                next_statement,
                ..
            } => write!(
                f,
                "advance-transaction-commit:{id}:{expected_next_statement}-{next_statement}"
            ),
            Self::FinishEmptyTransactionCommit { id, .. } => {
                write!(f, "finish-empty-transaction-commit:{id}")
            }
            Self::RevertTransaction { id, .. } => write!(f, "revert-transaction:{id}"),
            Self::ExpireTransaction { id, .. } => write!(f, "expire-transaction:{id}"),
            Self::RemoveFinishedTransactions { .. } => f.write_str("remove-finished-transactions"),
        }
    }
}

impl std::fmt::Display for ConsensusResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Applied => f.write_str("ok"),
            Self::Conflict(reason) => write!(f, "conflict:{reason}"),
            Self::Transaction(response) => match &response.result {
                Ok(transaction) => write!(f, "transaction:{}", transaction.id),
                Err(error) => write!(f, "transaction-error:{error}"),
            },
        }
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = ConsensusCommand,
        R = ConsensusResponse,
        NodeId = ClusterNodeName,
        Node = BasicNode
);

type NervixRaft = Raft<TypeConfig, FjallStore>;
pub type Node = BasicNode;
pub type LogIdOf = LogId<CommittedLeaderIdOf<TypeConfig>>;
pub type VoteOf = Vote<LeaderIdOf<TypeConfig>>;
pub type StoredMembershipOf =
    StoredMembership<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node>;
pub type SnapshotOf =
    Snapshot<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node, SealedSnapshot>;

const MEMBERSHIP_MUTATION_TIMEOUT: Duration = Duration::from_secs(10);
/// How many consensus transitions a session can fall behind before the bus drops the oldest.
const CONSENSUS_EVENT_CAPACITY: usize = 256;
/// How often a mutation held back by log retention rechecks for reclaimed space.
const RETENTION_ADMISSION_POLL: Duration = Duration::from_millis(50);
/// How long one complete snapshot transfer may take.
const SNAPSHOT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);

static NEXT_SNAPSHOT_TRANSFER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct ConsensusSettings {
    pub cluster_name: String,
    pub node_id: ClusterNodeName,
    pub interconnect_advertise_addr: String,
    pub interconnect: Transport,
    pub executor: nervix_execution::Executor,
    pub raft_heartbeat_interval: Duration,
    pub raft_election_timeout_min: Duration,
    pub raft_election_timeout_max: Duration,
    pub raft_retention: RaftRetentionPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipNode {
    pub node_id: ClusterNodeName,
    pub incarnation: ClusterNodeIncarnation,
    pub grpc_advertise_addr: String,
    pub web_console_advertise_addr: String,
    pub interconnect_advertise_addr: String,
}

impl GossipNode {
    pub fn identity(&self) -> ClusterNodeIdentity {
        ClusterNodeIdentity::new(self.node_id.clone(), self.incarnation)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GossipState {
    pub live_nodes: Vec<GossipNode>,
    pub dead_node_ids: BTreeSet<ClusterNodeName>,
}

impl GossipState {
    fn admission_candidates(&self) -> impl Iterator<Item = &GossipNode> {
        self.live_nodes
            .iter()
            .filter(|node| !self.dead_node_ids.contains(&node.node_id))
    }

    pub fn live_identities(&self) -> BTreeSet<ClusterNodeIdentity> {
        let mut current = BTreeMap::<ClusterNodeName, ClusterNodeIdentity>::new();
        for node in self.admission_candidates() {
            let identity = node.identity();
            let replace = match current.get(&node.node_id) {
                Some(observed) => observed.incarnation() < identity.incarnation(),
                None => true,
            };
            if replace {
                current.insert(node.node_id.clone(), identity);
            }
        }
        current.into_values().collect()
    }

    fn latest_admission_candidates(&self) -> BTreeMap<ClusterNodeName, GossipNode> {
        let mut current = BTreeMap::<ClusterNodeName, GossipNode>::new();
        for node in self.admission_candidates() {
            let replace = match current.get(&node.node_id) {
                Some(observed) => observed.incarnation < node.incarnation,
                None => true,
            };
            if replace {
                current.insert(node.node_id.clone(), node.clone());
            }
        }
        current
    }
}

/// What one node keeps of its Raft log at a point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftLogRetention {
    /// The highest index whose entry has been removed, once a snapshot covered it.
    pub purged_index: Option<u64>,
    /// The highest index the node's current snapshot covers.
    pub snapshot_index: Option<u64>,
    /// The highest index the node's log holds.
    pub last_log_index: Option<u64>,
    /// What the retained log occupies in node-owned storage.
    pub retained_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ConsensusRuntimeState {
    pub revision: u64,
    pub schedule: ClusterSchedule,
    pub domains: BTreeMap<DomainName, DomainState>,
    pub domain_clock_authorities: BTreeMap<DomainName, DomainClockAuthority>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct LeaderTenure {
    leader_id: ClusterNodeName,
    term: u64,
}

impl LeaderTenure {
    pub fn leader_id(&self) -> &ClusterNodeName {
        &self.leader_id
    }

    pub fn term(&self) -> u64 {
        self.term
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AutomaticScheduleFence {
    leader_tenure: LeaderTenure,
    input_revision: Option<u64>,
}

impl AutomaticScheduleFence {
    pub fn leader_tenure(&self) -> &LeaderTenure {
        &self.leader_tenure
    }

    pub fn input_revision(&self) -> Option<u64> {
        self.input_revision
    }
}

#[derive(Debug, Clone)]
pub struct AutomaticScheduleInput {
    runtime_state: ConsensusRuntimeState,
    fence: AutomaticScheduleFence,
}

impl AutomaticScheduleInput {
    pub fn runtime_state(&self) -> &ConsensusRuntimeState {
        &self.runtime_state
    }

    pub fn fence(&self) -> AutomaticScheduleFence {
        self.fence.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MembershipSnapshot {
    voters: BTreeSet<ClusterNodeName>,
    nodes: BTreeMap<ClusterNodeName, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MembershipMutation {
    AddLearner {
        node_id: ClusterNodeName,
        address: String,
        refresh: bool,
    },
    ChangeVoters {
        voters: BTreeSet<ClusterNodeName>,
    },
}

impl MembershipSnapshot {
    fn automatic_mutations(&self, gossip: &GossipState) -> Vec<MembershipMutation> {
        let mut mutations = Vec::new();
        let mut desired_voters = self.voters.clone();
        for node in gossip.latest_admission_candidates().into_values() {
            if node.interconnect_advertise_addr.is_empty() {
                continue;
            }

            let known_address = self.nodes.get(&node.node_id);
            let is_voter = self.voters.contains(&node.node_id);
            if !is_voter || known_address != Some(&node.interconnect_advertise_addr) {
                let refresh = known_address.is_some()
                    && known_address != Some(&node.interconnect_advertise_addr);
                mutations.push(MembershipMutation::AddLearner {
                    node_id: node.node_id.clone(),
                    address: node.interconnect_advertise_addr,
                    refresh,
                });
            }
            desired_voters.insert(node.node_id);
        }

        if desired_voters != self.voters {
            mutations.push(MembershipMutation::ChangeVoters {
                voters: desired_voters,
            });
        }
        mutations
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct StateMachineData {
    last_applied_log_id: Option<LogIdOf>,
    // Independently retained revisions share unchanged membership.
    last_membership: Arc<StoredMembershipOf>,
    runtime_revision: u64,
    schedule: ScheduleRecords,
    domains: Records<DomainName, DomainState>,
    domain_clock_authorities: Records<DomainName, DomainClockAuthority>,
    users: Records<UserName, UserCredentials>,
    resources: ResourceRecords,
    cordoned_node_ids: Records<ClusterNodeName, ()>,
    transactions: Records<String, ReplicatedTransaction>,
}

impl StateMachineData {
    fn record_runtime_revision(&mut self, revision: u64, applied: &AppliedConsensusCommand) {
        if applied.schedule_changed || applied.domains_changed {
            self.runtime_revision = revision;
        }
    }

    fn replace_domain_schedule(&mut self, domain: &DomainName, schedule: Option<&DomainSchedule>) {
        let Some(domain_schedule) = schedule else {
            self.schedule.domains.remove(domain);
            return;
        };
        self.schedule
            .domains
            .insert(domain.clone(), domain_schedule.clone());
    }

    fn advance_domain_clock_authority(
        &mut self,
        domain: &DomainName,
        owner: Option<ClusterNodeIdentity>,
    ) {
        let current = self
            .domain_clock_authorities
            .get(domain)
            .cloned()
            .unwrap_or_else(DomainClockAuthority::initial);
        let next = current.checked_reassign(owner).assured(
            "a domain-clock authority cannot be reassigned 2^64 times in the lifetime of a cluster",
        );
        self.domain_clock_authorities.insert(domain.clone(), next);
    }

    fn commit_domain_start(
        &mut self,
        domain_id: &DomainName,
        start: &DomainStartPoint,
        clock: &Option<DomainClockState>,
        authority: &Option<ClusterNodeIdentity>,
    ) -> bool {
        let Some(domain) = self.domains.get_mut(domain_id) else {
            return false;
        };
        domain.status = DomainStatus::Running;
        domain.start_version = domain
            .start_version
            .checked_add(1)
            .assured("a domain cannot be started 2^64 times in the lifetime of a cluster");
        domain.last_start = start.clone();
        domain.clock = clock.clone();
        let paced = matches!(domain.config.pace, DomainPace::Paced);

        if paced {
            self.advance_domain_clock_authority(domain_id, authority.clone());
        } else {
            self.domain_clock_authorities.remove(domain_id);
        }
        true
    }

    fn commit_domain_stop(&mut self, domain_id: &DomainName) -> bool {
        let Some(domain) = self.domains.get_mut(domain_id) else {
            return false;
        };
        domain.status = DomainStatus::Stopped;
        domain.clock = None;
        let paced = matches!(domain.config.pace, DomainPace::Paced);

        if paced {
            self.advance_domain_clock_authority(domain_id, None);
        } else {
            self.domain_clock_authorities.remove(domain_id);
        }
        true
    }

    fn reconcile_domain_clock_authority(
        &mut self,
        domain_id: &DomainName,
        expected_start_version: u64,
        expected_authority: &DomainClockAuthority,
        owner: &Option<ClusterNodeIdentity>,
    ) -> bool {
        let current_domain = self.domains.get(domain_id);
        let current_authority = self
            .domain_clock_authorities
            .get(domain_id)
            .cloned()
            .unwrap_or_else(DomainClockAuthority::initial);
        let eligible = current_domain.is_some_and(|domain| {
            domain.start_version == expected_start_version
                && !matches!(domain.status, DomainStatus::Stopped)
                && matches!(domain.config.pace, DomainPace::Paced)
        }) && &current_authority == expected_authority;
        if !eligible || expected_authority.owner() == owner.as_ref() {
            return false;
        }

        self.advance_domain_clock_authority(domain_id, owner.clone());
        true
    }
}

/// A snapshot a peer is staging into this node's own snapshot storage.
///
/// Sections arrive one at a time and are sealed as they complete, so the transfer never holds more
/// than one section in memory regardless of how large the snapshot is.
struct IncomingSnapshotTransfer {
    transfer_id: u64,
    generation: u64,
    vote: VoteOf,
    meta: SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node>,
    section_count: u32,
    total_bytes: u64,
    /// The section being assembled, and how many of its declared bytes have arrived.
    section: PendingSection,
    staged_sections: u32,
    staged_bytes: u64,
}

struct PendingSection {
    index: u32,
    declared_bytes: u64,
    bytes: Vec<u8>,
}

/// One completed section, taken out of the transfer so it can be sealed outside the lock.
struct StagedSection {
    generation: u64,
    index: u32,
    bytes: Vec<u8>,
}

impl IncomingSnapshotTransfer {
    fn new(
        transfer_id: u64,
        generation: u64,
        vote: VoteOf,
        meta: SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node>,
        section_count: u32,
        total_bytes: u64,
    ) -> Self {
        Self {
            transfer_id,
            generation,
            vote,
            meta,
            section_count,
            total_bytes,
            section: PendingSection {
                index: 0,
                declared_bytes: 0,
                bytes: Vec::new(),
            },
            staged_sections: 0,
            staged_bytes: 0,
        }
    }

    fn verify_id(&self, transfer_id: u64) -> Result<(), Report<SnapshotTransferError>> {
        if self.transfer_id != transfer_id {
            return Err(Report::new(SnapshotTransferError::Superseded {
                expected: self.transfer_id,
                actual: transfer_id,
            }));
        }
        Ok(())
    }

    /// Take one chunk, and return the section it completed.
    fn append_chunk(
        &mut self,
        transfer_id: u64,
        chunk: SnapshotChunkPart,
        section_limit: u64,
        chunk_limit: usize,
    ) -> Result<Option<StagedSection>, Report<SnapshotTransferError>> {
        self.verify_id(transfer_id)?;
        if chunk.bytes.len() > chunk_limit {
            return Err(Report::new(SnapshotTransferError::ChunkTooLarge {
                actual: chunk.bytes.len(),
                limit: chunk_limit,
            }));
        }
        if chunk.section_bytes > section_limit {
            return Err(Report::new(SnapshotTransferError::SectionTooLarge {
                actual: chunk.section_bytes,
                limit: section_limit,
            }));
        }
        if chunk.section_index != self.staged_sections || chunk.section_index >= self.section_count
        {
            return Err(Report::new(SnapshotTransferError::WrongSection {
                expected: self.staged_sections,
                actual: chunk.section_index,
            }));
        }
        if chunk.offset == 0 {
            self.section = PendingSection {
                index: chunk.section_index,
                declared_bytes: chunk.section_bytes,
                bytes: Vec::new(),
            };
        }
        if self.section.index != chunk.section_index
            || self.section.declared_bytes != chunk.section_bytes
        {
            return Err(Report::new(SnapshotTransferError::WrongSection {
                expected: self.section.index,
                actual: chunk.section_index,
            }));
        }
        let received = u64::try_from(self.section.bytes.len())
            .assured("supported targets have a pointer width no larger than u64");
        if chunk.offset != received {
            return Err(Report::new(SnapshotTransferError::WrongOffset {
                expected: received,
                actual: chunk.offset,
            }));
        }
        let chunk_bytes = u64::try_from(chunk.bytes.len())
            .assured("supported targets have a pointer width no larger than u64");
        let end = received.checked_add(chunk_bytes).ok_or_else(|| {
            Report::new(SnapshotTransferError::ExceedsDeclaredLength {
                declared: self.section.declared_bytes,
            })
        })?;
        if end > self.section.declared_bytes {
            return Err(Report::new(SnapshotTransferError::ExceedsDeclaredLength {
                declared: self.section.declared_bytes,
            }));
        }
        self.section
            .bytes
            .try_reserve(chunk.bytes.len())
            .map_err(|_| Report::new(SnapshotTransferError::Allocation { requested: end }))?;
        self.section.bytes.extend_from_slice(&chunk.bytes);
        if end < self.section.declared_bytes {
            return Ok(None);
        }
        self.staged_sections = self
            .staged_sections
            .checked_add(1)
            .ok_or_else(|| Report::new(SnapshotTransferError::TooManySections))?;
        self.staged_bytes = self.staged_bytes.checked_add(end).ok_or_else(|| {
            Report::new(SnapshotTransferError::ExceedsDeclaredLength {
                declared: self.total_bytes,
            })
        })?;
        if self.staged_bytes > self.total_bytes {
            return Err(Report::new(SnapshotTransferError::ExceedsDeclaredLength {
                declared: self.total_bytes,
            }));
        }
        Ok(Some(StagedSection {
            generation: self.generation,
            index: self.section.index,
            bytes: std::mem::take(&mut self.section.bytes),
        }))
    }

    fn complete(
        self,
        transfer_id: u64,
    ) -> Result<CompletedSnapshotTransfer, Report<SnapshotTransferError>> {
        self.verify_id(transfer_id)?;
        if self.staged_sections != self.section_count || self.staged_bytes != self.total_bytes {
            return Err(Report::new(SnapshotTransferError::Incomplete {
                expected: self.total_bytes,
                actual: self.staged_bytes,
            }));
        }
        Ok(CompletedSnapshotTransfer {
            generation: self.generation,
            vote: self.vote,
            meta: self.meta,
            section_count: self.section_count,
            total_bytes: self.total_bytes,
        })
    }
}

struct CompletedSnapshotTransfer {
    generation: u64,
    vote: VoteOf,
    meta: SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node>,
    section_count: u32,
    total_bytes: u64,
}

/// One arriving chunk of one snapshot section.
struct SnapshotChunkPart {
    section_index: u32,
    section_bytes: u64,
    offset: u64,
    bytes: Vec<u8>,
}

#[derive(Debug, Error)]
enum SnapshotTransferError {
    #[error("node '{peer}' has no active snapshot transfer")]
    Missing { peer: ClusterNodeName },
    #[error("snapshot transfer {actual} superseded transfer {expected}")]
    Superseded { expected: u64, actual: u64 },
    #[error("snapshot chunk offset {actual} differs from expected offset {expected}")]
    WrongOffset { expected: u64, actual: u64 },
    #[error("snapshot chunk names section {actual}, expected section {expected}")]
    WrongSection { expected: u32, actual: u32 },
    #[error("snapshot section declares {actual} bytes, exceeding the {limit}-byte limit")]
    SectionTooLarge { actual: u64, limit: u64 },
    #[error("snapshot transfer staged more sections than it declared")]
    TooManySections,
    #[error("snapshot chunk contains {actual} bytes, exceeding the {limit}-byte limit")]
    ChunkTooLarge { actual: usize, limit: usize },
    #[error("snapshot transfer would exceed its declared {declared}-byte length")]
    ExceedsDeclaredLength { declared: u64 },
    #[error("snapshot transfer ended at {actual} bytes, expected {expected}")]
    Incomplete { expected: u64, actual: u64 },
    #[error("snapshot transfer could not allocate its first {requested} bytes")]
    Allocation { requested: u64 },
    #[error("raft rejected the completed snapshot: {0}")]
    Install(String),
    #[error("the arriving snapshot section could not be sealed: {0}")]
    Stage(String),
}

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("consensus storage failed: {0}")]
    Storage(#[source] io::Error),
    #[error("consensus storage failed: {0}")]
    RaftStorage(#[source] openraft::StorageError<TypeConfig>),
    #[error("raft startup failed")]
    Startup,
    #[error("failed to create raft client endpoint")]
    Endpoint,
    #[error("raft transport failed")]
    Transport,
    #[error("{0}")]
    Write(String),
    #[error("consensus state changed: {0}")]
    Conflict(String),
    #[error("raft returned an unexpected response to a state mutation")]
    UnexpectedResponse,
    #[error("raft proposal lost leadership")]
    LeadershipLost { leader_id: Option<ClusterNodeName> },
    #[error("node '{0}' is not a raft member")]
    NodeNotFound(String),
    #[error("cannot remove the local leader node '{0}'")]
    RemoveLocalLeader(String),
    #[error("cannot remove the last raft voter '{0}'")]
    RemoveLastVoter(String),
    #[error("timed out changing raft membership after {timeout:?}: {operation}")]
    MembershipChangeTimeout {
        operation: String,
        timeout: Duration,
    },
    #[error(
        "the retained raft log holds {retained} bytes against a {cap}-byte cap and did not \
         reclaim within {waited:?}"
    )]
    LogRetentionSaturated {
        retained: u64,
        cap: u64,
        waited: Duration,
    },
}

#[derive(Debug, Error)]
enum ProtocolOriginError {
    #[error(
        "authenticated node '{authenticated}' cannot submit {operation} for declared origin \
         '{declared}'"
    )]
    Mismatch {
        operation: &'static str,
        authenticated: ClusterNodeName,
        declared: ClusterNodeName,
    },
}

fn validate_protocol_origin(
    authenticated: &ClusterNodeName,
    declared: &ClusterNodeName,
    operation: &'static str,
) -> Result<(), Report<ProtocolOriginError>> {
    if authenticated == declared {
        return Ok(());
    }
    Err(Report::new(ProtocolOriginError::Mismatch {
        operation,
        authenticated: authenticated.clone(),
        declared: declared.clone(),
    }))
}

impl From<RaftError<TypeConfig, ClientWriteError<TypeConfig>>> for ConsensusError {
    fn from(error: RaftError<TypeConfig, ClientWriteError<TypeConfig>>) -> Self {
        match error {
            RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
                Self::LeadershipLost {
                    leader_id: forward.leader_id,
                }
            }
            RaftError::Fatal(openraft::error::Fatal::StorageError(error)) => {
                Self::RaftStorage(error)
            }
            error => Self::Write(format!("raft write failed: {error}")),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConsensusTransactionError {
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    #[error(transparent)]
    Mutation(#[from] TransactionMutationError),
    #[error("raft returned an invalid transaction response")]
    InvalidResponse,
}

/// Owns the consensus runtime lifecycle and grants operation-specific capabilities.
///
/// Keep this owner at cluster startup and shutdown. Give consumers only the capability
/// they need; cloning a capability preserves that capability's authority.
///
/// ```
/// use nervix_consensus::{Administrator, Consensus, Observer, Proposer, ProtocolReceiver};
/// fn grant_capabilities(consensus: &Consensus) {
///     let observer: Observer = consensus.observer();
///     let proposer: Proposer = consensus.proposer();
///     let receiver: ProtocolReceiver = consensus.protocol_receiver();
///     let administrator: Administrator = consensus.administrator();
///     let read_only: Observer = proposer.observer();
///     let local_node = proposer.local_node_id();
///     let admin_observation: Observer = administrator.observer();
/// }
/// ```
pub struct Consensus {
    inner: Arc<ConsensusState>,
}

/// Reads replicated state, leadership, membership, and their change notifications.
///
/// Queries return the state currently applied on this node. Reads may lag the leader;
/// they provide no linearizable-read barrier.
///
/// Observation cannot propose commands:
/// ```compile_fail
/// use nervix_consensus::Observer;
/// use nervix_models::DomainName;
/// async fn mutate(observer: Observer, domain: DomainName) {
///     observer.stop_domain(domain).await;
/// }
/// ```
/// Observation cannot process Raft protocol traffic:
/// ```compile_fail
/// use nervix_consensus::{Observer, TypeConfig, VoteRequest};
/// async fn receive(observer: Observer, vote: VoteRequest<TypeConfig>) {
///     observer.vote(vote).await;
/// }
/// ```
/// Observation cannot acquire administration:
/// ```compile_fail
/// use nervix_consensus::Observer;
/// fn escalate(observer: Observer) {
///     observer.administrator();
/// }
/// ```
/// Its shared state is private:
/// ```compile_fail
/// use nervix_consensus::Observer;
/// fn access_shared_state(observer: Observer) {
///     let state = observer.inner;
/// }
/// ```
/// Observation cannot convert into proposal authority:
/// ```compile_fail
/// use nervix_consensus::{Observer, Proposer};
/// fn escalate(observer: Observer) {
///     let proposer: Proposer = observer.into();
/// }
/// ```
#[derive(Clone)]
pub struct Observer {
    inner: Arc<ConsensusState>,
}

/// Retained Raft leadership and membership observation.
pub struct RaftTopologyWatcher {
    state: WatchReceiverOf<TypeConfig, RaftServerMetrics<TypeConfig>>,
}

impl RaftTopologyWatcher {
    /// Wait for a server-state change, returning `false` after the Raft core has stopped.
    pub async fn changed(&mut self) -> bool {
        match self.state.changed().await {
            Ok(()) => true,
            Err(_) => false,
        }
    }
}

/// Proposes replicated commands and includes read-only observation.
///
/// This capability authorizes proposal attempts. Leadership may change after an earlier
/// check, so Raft acceptance determines whether each proposal succeeds. A proposal rejected
/// after leadership is lost returns [`ConsensusError::LeadershipLost`] with the leader ID
/// when Raft knows it.
///
/// A proposer cannot change Raft membership:
/// ```compile_fail
/// use nervix_consensus::Proposer;
/// use nervix_models::ClusterNodeName;
/// async fn administer(proposer: Proposer, node: ClusterNodeName) {
///     proposer.drop_node(&node).await;
/// }
/// ```
/// A proposer cannot process Raft protocol traffic:
/// ```compile_fail
/// use nervix_consensus::{Proposer, TypeConfig, VoteRequest};
/// async fn receive(proposer: Proposer, vote: VoteRequest<TypeConfig>) {
///     proposer.vote(vote).await;
/// }
/// ```
#[derive(Clone)]
pub struct Proposer {
    observer: Observer,
}

/// Receives Raft protocol traffic without proposal or administration authority.
///
/// Protocol receivers cannot propose commands:
/// ```compile_fail
/// use nervix_consensus::ProtocolReceiver;
/// use nervix_models::DomainName;
/// async fn propose(receiver: ProtocolReceiver, domain: DomainName) {
///     receiver.stop_domain(domain).await;
/// }
/// ```
/// Protocol receivers cannot administer membership:
/// ```compile_fail
/// use nervix_consensus::ProtocolReceiver;
/// use nervix_models::ClusterNodeName;
/// async fn administer(receiver: ProtocolReceiver, node: ClusterNodeName) {
///     receiver.drop_node(&node).await;
/// }
/// ```
#[derive(Clone)]
pub struct ProtocolReceiver {
    inner: Arc<ConsensusState>,
}

/// Initializes and changes membership, reconciles peers, and transfers leadership.
///
/// Administration does not grant replicated-command proposals:
/// ```compile_fail
/// use nervix_consensus::Administrator;
/// use nervix_models::DomainName;
/// async fn propose(administrator: Administrator, domain: DomainName) {
///     administrator.stop_domain(domain).await;
/// }
/// ```
/// Administration does not grant protocol receipt:
/// ```compile_fail
/// use nervix_consensus::{Administrator, TypeConfig, VoteRequest};
/// async fn receive(administrator: Administrator, vote: VoteRequest<TypeConfig>) {
///     administrator.vote(vote).await;
/// }
/// ```
#[derive(Clone)]
pub struct Administrator {
    inner: Arc<ConsensusState>,
}

struct ConsensusState {
    raft: NervixRaft,
    // The Raft runtime independently owns the store as both log storage and state machine.
    store: FjallStore,
    local_node_id: ClusterNodeName,
    interconnect_advertise_addr: String,
    interconnect: Transport,
    raft_retention: RaftRetentionPolicy,
    membership_mutation: AsyncMutex<()>,
    incoming_snapshots: Mutex<BTreeMap<ClusterNodeName, IncomingSnapshotTransfer>>,
    events: ConsensusEvents,
    metrics_task: Mutex<Option<JoinHandle<()>>>,
    retention_task: Mutex<Option<JoinHandle<()>>>,
}

/// The consensus event bus, and the one way a Raft transition reaches an attached session.
///
/// A transition is already the node's own record of itself, which is why publishing goes through
/// [`Self::report`] rather than through the sender directly: the log and the bus carry the same
/// text, and the log carries it whether or not anyone is attached. Subscribers here are live
/// sessions only, so a node serving none is the ordinary case rather than a failure, and the
/// send's outcome adds nothing the `info` line has not already recorded.
#[derive(Clone)]
struct ConsensusEvents {
    sender: broadcast::Sender<String>,
}

impl ConsensusEvents {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(CONSENSUS_EVENT_CAPACITY);
        Self { sender }
    }

    /// Record a transition of this node's consensus state and offer it to attached sessions.
    fn report(&self, message: String) {
        info!("{message}");
        self.sender
            .send(message)
            .discarded("the info line above is this transition's record, attached session or not");
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.sender.subscribe()
    }
}

impl std::ops::Deref for Proposer {
    type Target = Observer;

    fn deref(&self) -> &Self::Target {
        &self.observer
    }
}

impl Consensus {
    #[cfg(feature = "testing")]
    pub fn storage_fault(&self) -> StorageFault {
        self.inner.store.inner.faults.clone()
    }

    pub fn observer(&self) -> Observer {
        Observer {
            inner: self.inner.clone(),
        }
    }

    pub fn proposer(&self) -> Proposer {
        Proposer {
            observer: self.observer(),
        }
    }

    pub fn protocol_receiver(&self) -> ProtocolReceiver {
        ProtocolReceiver {
            inner: self.inner.clone(),
        }
    }

    pub fn administrator(&self) -> Administrator {
        Administrator {
            inner: self.inner.clone(),
        }
    }

    pub async fn open(
        path: impl AsRef<Path>,
        settings: ConsensusSettings,
    ) -> Result<Self, ConsensusError> {
        let path = path.as_ref().to_path_buf();
        let reservation = settings
            .executor
            .reserve(nervix_execution::MemoryClass::Management, 4096)
            .await
            .map_err(|error| ConsensusError::Storage(io::Error::other(error)))?;
        let db = settings
            .executor
            .run_storage(
                nervix_execution::StorageClass::Consensus,
                reservation,
                move |_, _| Database::builder(path).open(),
            )
            .await
            .map_err(|error| ConsensusError::Storage(io::Error::other(error)))?
            .map_err(|error| ConsensusError::Storage(io::Error::other(error)))?;
        Self::from_database(db, settings).await
    }

    pub async fn from_database(
        db: Database,
        settings: ConsensusSettings,
    ) -> Result<Self, ConsensusError> {
        let store = FjallStore::from_database(db, settings.executor.clone())
            .await
            .map_err(ConsensusError::Storage)?;
        let retention = settings.raft_retention;
        let config = StdArc::new(
            Config {
                cluster_name: settings.cluster_name,
                heartbeat_interval: u64::try_from(settings.raft_heartbeat_interval.as_millis())
                    .unwrap_or(u64::MAX),
                election_timeout_min: u64::try_from(settings.raft_election_timeout_min.as_millis())
                    .unwrap_or(u64::MAX),
                election_timeout_max: u64::try_from(settings.raft_election_timeout_max.as_millis())
                    .unwrap_or(u64::MAX),
                // The log reader fills a batch to the append target and never splits a command,
                // so the byte bound comes from storage. This only caps how many entries one
                // batch may gather before that bound is reached.
                max_payload_entries: replication::MAX_APPEND_BATCH_ENTRIES,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                    retention.snapshot_entry_threshold,
                ),
                max_in_snapshot_log_to_keep: retention.covered_entries_retained,
                // One snapshot moves section by section under one deadline, so this covers the
                // whole transfer rather than one chunk of it.
                install_snapshot_timeout: u64::try_from(SNAPSHOT_TRANSFER_TIMEOUT.as_millis())
                    .unwrap_or(u64::MAX),
                ..Default::default()
            }
            .validate()
            .map_err(|_| ConsensusError::Startup)?,
        );

        let network = NetworkFactory {
            local_node_id: settings.node_id.clone(),
            interconnect: settings.interconnect.clone(),
            executor: settings.executor.clone(),
        };
        let raft = Raft::new(
            settings.node_id.clone(),
            config,
            network,
            store.clone(),
            store.clone(),
        )
        .await
        .map_err(|_| ConsensusError::Startup)?;
        let events = ConsensusEvents::new();
        let metrics_raft = raft.clone();
        let metrics_events = events.clone();
        let metrics_task = tokio::spawn(async move {
            let mut rx = metrics_raft.metrics();
            let mut last_transition = None;
            loop {
                tokio::task::consume_budget().await;
                if rx.changed().await.is_err() {
                    break;
                }
                let metrics = rx.borrow_watched().clone();
                let transition = RaftTransition {
                    state: format!("{:?}", metrics.state),
                    term: metrics.current_term,
                    leader: metrics.current_leader.clone(),
                };
                if last_transition.as_ref() != Some(&transition) {
                    let leader = match &transition.leader {
                        Some(leader) => leader.as_str(),
                        None => "(none)",
                    };
                    let last_applied = match metrics.last_applied {
                        Some(last_applied) => last_applied.index.to_string(),
                        None => "(none)".to_string(),
                    };
                    let summary = format!(
                        "raft transition: state={} leader={leader} term={} last_log_index={} \
                         last_applied={last_applied}",
                        transition.state,
                        transition.term,
                        metrics.last_log_index.unwrap_or_default(),
                    );
                    metrics_events.report(summary);
                    last_transition = Some(transition);
                }
            }
        });

        let retention_task = tokio::spawn(
            retention::RetentionTask::new(raft.clone(), store.clone(), retention).run(),
        );
        let consensus = Self {
            inner: Arc::new(ConsensusState {
                raft,
                store,
                local_node_id: settings.node_id,
                interconnect_advertise_addr: settings.interconnect_advertise_addr,
                interconnect: settings.interconnect,
                raft_retention: retention,
                membership_mutation: AsyncMutex::new(()),
                incoming_snapshots: Mutex::new(BTreeMap::new()),
                events,
                metrics_task: Mutex::new(Some(metrics_task)),
                retention_task: Mutex::new(Some(retention_task)),
            }),
        };
        consensus
            .register_protocol_handlers()
            .map_err(|_| ConsensusError::Startup)?;
        Ok(consensus)
    }

    fn register_protocol_handlers(&self) -> Result<(), Report<HandlerRegistrationError>> {
        let receiver = self.protocol_receiver();
        self.inner
            .interconnect
            .register_handler::<wire::HeartbeatRequest, _, _>(move |context, request| {
                let receiver = receiver.clone();
                async move {
                    validate_protocol_origin(
                        context.peer_node_id(),
                        request.0.origin_node_id(),
                        "a heartbeat",
                    )
                    .map_err(wire::ConsensusRequestError::invalid_origin)?;
                    let request = match request.0.into_request() {
                        Ok(request) => request,
                        Err(error) => {
                            return Err(wire::ConsensusRequestError::invalid_request(error));
                        }
                    };
                    receiver
                        .append_entries(request)
                        .await
                        .map(wire::AppendEntriesResponseRecord::from)
                        .map_err(wire::ConsensusRequestError::raft)
                }
            })?;

        let receiver = self.protocol_receiver();
        self.inner
            .interconnect
            .register_duplex_handler::<wire::OpenAppendStream, _, _>(
                move |context, request, items| {
                    let receiver = receiver.clone();
                    async move {
                        validate_protocol_origin(
                            context.peer_node_id(),
                            &request.leader_node_id,
                            "an append stream",
                        )
                        .map_err(|error| {
                            nervix_interconnect::StreamHandlerError::new(error.to_string())
                        })?;
                        let answers =
                            receiver.answer_append_stream(context.peer_node_id().clone(), items);
                        Ok(nervix_interconnect::DuplexResponses::new(answers.map(Ok)))
                    }
                },
            )?;

        let receiver = self.protocol_receiver();
        self.inner
            .interconnect
            .register_handler::<wire::RequestVote, _, _>(move |context, request| {
                let receiver = receiver.clone();
                async move {
                    validate_protocol_origin(
                        context.peer_node_id(),
                        request.0.origin_node_id(),
                        "a vote request",
                    )
                    .map_err(wire::ConsensusRequestError::invalid_origin)?;
                    receiver
                        .vote(request.0.into_request())
                        .await
                        .map(wire::VoteResponseRecord::from)
                        .map_err(wire::ConsensusRequestError::raft)
                }
            })?;

        let receiver = self.protocol_receiver();
        self.inner
            .interconnect
            .register_handler::<wire::BeginSnapshotTransfer, _, _>(move |context, request| {
                let receiver = receiver.clone();
                async move {
                    validate_protocol_origin(
                        context.peer_node_id(),
                        request.origin_node_id(),
                        "a snapshot transfer",
                    )
                    .map_err(wire::ConsensusRequestError::invalid_origin)?;
                    let transfer = request
                        .into_start()
                        .map_err(wire::ConsensusRequestError::invalid_request)?;
                    receiver
                        .begin_snapshot_transfer(
                            context.peer_node_id().clone(),
                            transfer.transfer_id,
                            transfer.vote,
                            transfer.meta,
                            transfer.section_count,
                            transfer.total_bytes,
                        )
                        .map_err(wire::ConsensusRequestError::snapshot_transfer)
                }
            })?;

        let receiver = self.protocol_receiver();
        self.inner
            .interconnect
            .register_handler::<wire::SnapshotChunk, _, _>(move |context, request| {
                let receiver = receiver.clone();
                async move {
                    receiver
                        .append_snapshot_chunk(
                            context.peer_node_id(),
                            request.transfer_id,
                            SnapshotChunkPart {
                                section_index: request.section_index,
                                section_bytes: request.section_bytes,
                                offset: request.offset,
                                bytes: request.bytes,
                            },
                        )
                        .await
                        .map_err(wire::ConsensusRequestError::snapshot_transfer)
                }
            })?;

        let receiver = self.protocol_receiver();
        self.inner
            .interconnect
            .register_handler::<wire::FinishSnapshotTransfer, _, _>(move |context, request| {
                let receiver = receiver.clone();
                async move {
                    receiver
                        .finish_snapshot_transfer(context.peer_node_id(), request.transfer_id)
                        .await
                        .map(wire::SnapshotResponseRecord::from)
                        .map_err(wire::ConsensusRequestError::snapshot_transfer)
                }
            })?;

        let receiver = self.protocol_receiver();
        self.inner
            .interconnect
            .register_handler::<wire::TransferLeadership, _, _>(move |context, request| {
                let receiver = receiver.clone();
                async move {
                    validate_protocol_origin(
                        context.peer_node_id(),
                        request.origin_node_id(),
                        "a leadership transfer",
                    )
                    .map_err(wire::ConsensusRequestError::invalid_origin)?;
                    receiver
                        .transfer_leader(request.into_request())
                        .await
                        .map(wire::TransferLeadershipResponse::from)
                        .map_err(wire::ConsensusRequestError::raft)
                }
            })?;

        Ok(())
    }

    pub async fn shutdown(&self) {
        self.inner.raft.shutdown().await.discarded(
            "openraft joins its core task inside this call and always answers Ok; the core's own \
             outcome is not exposed here",
        );
        self.inner.store.wait_for_idle().await.assured(
            "the live consensus executor owns the ordered worker and its no-op barrier cannot fail",
        );
        let metrics_task = self.inner.metrics_task.lock().take();
        let retention_task = self.inner.retention_task.lock().take();
        for (name, handle) in [("metrics", metrics_task), ("retention", retention_task)] {
            let Some(handle) = handle else {
                continue;
            };
            handle.abort();
            // The abort makes a cancellation the expected outcome and it says nothing new. A panic
            // is the opposite: the task died on its own and this join is the last place that fact
            // exists.
            if let Err(error) = handle.await
                && !error.is_cancelled()
            {
                error!(%error, "consensus {name} task panicked before shutdown could join it");
            }
        }
    }
}

impl Observer {
    pub fn subscribe_events(&self) -> broadcast::Receiver<String> {
        self.inner.events.subscribe()
    }

    pub fn subscribe_schedule(&self) -> watch::Receiver<u64> {
        self.inner.store.inner.schedule_tx.subscribe()
    }

    pub fn subscribe_domains(&self) -> watch::Receiver<u64> {
        self.inner.store.inner.domain_tx.subscribe()
    }

    pub fn subscribe_resources(&self) -> watch::Receiver<u64> {
        self.inner.store.inner.resource_tx.subscribe()
    }

    pub fn subscribe_transactions(&self) -> watch::Receiver<u64> {
        self.inner.store.inner.transaction_tx.subscribe()
    }

    /// Observe retained leadership and membership changes without exposing OpenRaft to callers.
    pub fn subscribe_topology(&self) -> RaftTopologyWatcher {
        RaftTopologyWatcher {
            state: self.inner.raft.server_metrics(),
        }
    }

    pub async fn current_schedule(&self) -> ClusterSchedule {
        (&self.inner.store.inner.state().schedule).into()
    }
    pub async fn current_domains(&self) -> BTreeMap<DomainName, DomainState> {
        (&self.inner.store.inner.state().domains).into()
    }
    pub async fn current_transactions(&self) -> BTreeMap<String, ReplicatedTransaction> {
        (&self.inner.store.inner.state().transactions).into()
    }
    pub async fn current_transaction(&self, id: &str) -> Option<ReplicatedTransaction> {
        self.inner.store.inner.state().transactions.get(id).cloned()
    }
    pub async fn current_runtime_state(&self) -> ConsensusRuntimeState {
        let state = self.inner.store.inner.state();
        ConsensusRuntimeState {
            revision: state.runtime_revision,
            schedule: (&state.schedule).into(),
            domains: (&state.domains).into(),
            domain_clock_authorities: (&state.domain_clock_authorities).into(),
        }
    }
    pub async fn current_domain(&self, domain_id: &DomainName) -> Option<DomainState> {
        self.inner
            .store
            .inner
            .state()
            .domains
            .get(domain_id)
            .cloned()
    }
    /// What this node currently keeps of its Raft log, and what covers it.
    pub fn raft_log_retention(&self) -> RaftLogRetention {
        let metrics = self.inner.raft.metrics().borrow_watched().clone();
        RaftLogRetention {
            purged_index: metrics.purged.map(|log_id| log_id.index),
            snapshot_index: metrics.snapshot.map(|log_id| log_id.index),
            last_log_index: metrics.last_log_index,
            retained_bytes: self.inner.store.retained_log_bytes(),
        }
    }

    pub async fn current_users(&self) -> BTreeMap<UserName, UserCredentials> {
        (&self.inner.store.inner.state().users).into()
    }
    pub async fn current_user(&self, user: &UserName) -> Option<UserCredentials> {
        self.inner.store.inner.state().users.get(user).cloned()
    }
    pub async fn current_resources(&self) -> ResourceVersionStatus {
        (&self.inner.store.inner.state().resources).into()
    }
    pub async fn cordoned_node_ids(&self) -> BTreeSet<ClusterNodeName> {
        self.inner
            .store
            .inner
            .state()
            .cordoned_node_ids
            .keys()
            .cloned()
            .collect()
    }

    pub fn local_node_id(&self) -> &ClusterNodeName {
        &self.inner.local_node_id
    }

    pub async fn current_leader(&self) -> Option<ClusterNodeName> {
        self.inner.raft.current_leader().await
    }

    pub async fn status_lines(&self) -> Vec<String> {
        let metrics = self.inner.raft.metrics().borrow_watched().clone();
        let mut lines = Vec::new();
        lines.push(format!("raft.id: {}", self.inner.local_node_id));
        let current_leader = match metrics.current_leader {
            Some(leader) => leader.to_string(),
            None => "(none)".to_string(),
        };
        lines.push(format!("raft.current_leader: {current_leader}"));
        lines.push(format!("raft.current_term: {}", metrics.current_term));
        lines.push(format!("raft.state: {:?}", metrics.state));
        let cordoned = self.cordoned_node_ids().await;
        lines.push(format!(
            "raft.cordoned_nodes: {}",
            if cordoned.is_empty() {
                "(none)".to_string()
            } else {
                cordoned.into_iter().collect::<Vec<_>>().join(",")
            }
        ));
        lines.push(format!(
            "raft.last_log_index: {}",
            metrics.last_log_index.unwrap_or_default()
        ));
        let last_applied = match metrics.last_applied {
            Some(last_applied) => last_applied.index.to_string(),
            None => "(none)".to_string(),
        };
        lines.push(format!("raft.last_applied: {last_applied}"));
        lines.push("raft.membership:".to_string());
        for (node_id, node) in metrics.membership_config.nodes() {
            let role = if metrics
                .membership_config
                .membership()
                .voter_ids()
                .any(|id| id == *node_id)
            {
                "voter"
            } else {
                "learner"
            };
            lines.push(format!("- {node_id} [{role}] {}", node.addr));
        }
        lines
    }

    pub async fn domain_status_lines(&self) -> Vec<String> {
        let domains = self.current_domains().await;
        if domains.is_empty() {
            return vec!["- none".to_string()];
        }

        let mut lines = Vec::new();
        for domain in domains.into_values() {
            let line = if let nervix_models::DomainPace::Paced = domain.config.pace {
                format!(
                    "- {} status={:?} pace={} period={} skew={}",
                    domain.id.as_str(),
                    domain.status,
                    domain.config.pace.as_ref(),
                    domain.config.period,
                    domain.config.skew
                )
            } else {
                format!(
                    "- {} status={:?} pace={}",
                    domain.id.as_str(),
                    domain.status,
                    domain.config.pace.as_ref()
                )
            };
            lines.push(line);
        }
        lines
    }

    pub async fn membership_nodes(&self) -> BTreeMap<ClusterNodeName, String> {
        let metrics = self.inner.raft.metrics().borrow_watched().clone();
        metrics
            .membership_config
            .nodes()
            .map(|(node_id, node)| (node_id.clone(), node.addr.clone()))
            .collect()
    }

    pub async fn membership_voter_ids(&self) -> BTreeSet<ClusterNodeName> {
        let metrics = self.inner.raft.metrics().borrow_watched().clone();
        metrics.membership_config.membership().voter_ids().collect()
    }

    pub async fn live_voter_ids(
        &self,
        live_node_ids: impl IntoIterator<Item = ClusterNodeName>,
    ) -> Vec<ClusterNodeName> {
        let voters = self.membership_voter_ids().await;
        let mut live_voters = live_node_ids
            .into_iter()
            .filter(|node_id| voters.contains(node_id))
            .collect::<Vec<_>>();
        live_voters.sort();
        live_voters.dedup();
        live_voters
    }

    pub async fn schedulable_live_voter_ids(
        &self,
        live_node_ids: impl IntoIterator<Item = ClusterNodeName>,
    ) -> Vec<ClusterNodeName> {
        let cordoned = self.cordoned_node_ids().await;
        self.live_voter_ids(live_node_ids)
            .await
            .into_iter()
            .filter(|node_id| !cordoned.contains(node_id))
            .collect()
    }
}

impl Proposer {
    pub fn observer(&self) -> Observer {
        self.observer.clone()
    }

    pub async fn automatic_schedule_input(
        &self,
    ) -> Result<AutomaticScheduleInput, Report<ConsensusError>> {
        let before = self.inner.raft.metrics().borrow_watched().clone();
        if before.current_leader.as_ref() != Some(&self.inner.local_node_id) {
            return Err(Report::new(ConsensusError::LeadershipLost {
                leader_id: before.current_leader,
            }));
        }

        let state = self.inner.store.inner.state();
        let after = self.inner.raft.metrics().borrow_watched().clone();
        if after.current_leader.as_ref() != Some(&self.inner.local_node_id)
            || after.current_term != before.current_term
        {
            return Err(Report::new(ConsensusError::LeadershipLost {
                leader_id: after.current_leader,
            }));
        }

        let input_revision = state
            .last_applied_log_id
            .as_ref()
            .map(|log_id| log_id.index);
        Ok(AutomaticScheduleInput {
            runtime_state: ConsensusRuntimeState {
                revision: state.runtime_revision,
                schedule: (&state.schedule).into(),
                domains: (&state.domains).into(),
                domain_clock_authorities: (&state.domain_clock_authorities).into(),
            },
            fence: AutomaticScheduleFence {
                leader_tenure: LeaderTenure {
                    leader_id: self.inner.local_node_id.clone(),
                    term: before.current_term,
                },
                input_revision,
            },
        })
    }

    pub async fn replace_domain_schedule(
        &self,
        domain: DomainName,
        expected_schedule: Option<DomainSchedule>,
        schedule: Option<DomainSchedule>,
    ) -> Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::ReplaceDomainSchedule {
                domain,
                expected_schedule: expected_schedule.map(Box::new),
                schedule: schedule.map(Box::new),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => Ok(()),
            ConsensusResponse::Conflict(reason) => Err(ConsensusError::Conflict(reason)),
            ConsensusResponse::Transaction(_) => Err(ConsensusError::UnexpectedResponse),
        }
    }

    pub async fn apply_automatic_domain_schedule(
        &self,
        fence: AutomaticScheduleFence,
        domain: DomainName,
        expected_schedule: Option<DomainSchedule>,
        schedule: Option<DomainSchedule>,
    ) -> Result<(), Report<ConsensusError>> {
        if fence.leader_tenure.leader_id != self.inner.local_node_id {
            return Err(Report::new(ConsensusError::LeadershipLost {
                leader_id: self.inner.raft.current_leader().await,
            }));
        }

        let response = self
            .inner
            .raft
            .client_write(ConsensusCommand::ApplyAutomaticDomainSchedule {
                fence,
                domain,
                expected_schedule: expected_schedule.map(Box::new),
                schedule: schedule.map(Box::new),
            })
            .await
            .map_err(|error| Report::new(ConsensusError::from(error)))?;
        match response.data {
            ConsensusResponse::Applied => Ok(()),
            ConsensusResponse::Conflict(reason) => {
                Err(Report::new(ConsensusError::Conflict(reason)))
            }
            ConsensusResponse::Transaction(_) => {
                Err(Report::new(ConsensusError::UnexpectedResponse))
            }
        }
    }

    pub async fn put_domain(&self, domain: DomainState) -> Result<(), ConsensusError> {
        self.inner
            .client_write(ConsensusCommand::PutDomain {
                domain: Box::new(domain),
            })
            .await
            .map(|_| ())
    }

    pub async fn put_domain_and_schedule(
        &self,
        expected_domain: Option<DomainState>,
        expected_schedule: Option<DomainSchedule>,
        domain: DomainState,
        schedule: Option<DomainSchedule>,
    ) -> Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::PutDomainAndSchedule {
                expected_domain: expected_domain.map(Box::new),
                expected_schedule: expected_schedule.map(Box::new),
                domain: Box::new(domain),
                schedule: schedule.map(Box::new),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => Ok(()),
            ConsensusResponse::Conflict(reason) => Err(ConsensusError::Conflict(reason)),
            ConsensusResponse::Transaction(_) => Err(ConsensusError::UnexpectedResponse),
        }
    }

    pub async fn start_domain(
        &self,
        domain_id: DomainName,
        start: DomainStartPoint,
        clock: Option<DomainClockState>,
        authority: Option<ClusterNodeIdentity>,
    ) -> Result<(), ConsensusError> {
        self.inner
            .client_write(ConsensusCommand::StartDomain {
                domain_id,
                start,
                clock,
                authority,
            })
            .await
            .map(|_| ())
    }

    pub async fn stop_domain(&self, domain_id: DomainName) -> Result<(), ConsensusError> {
        self.inner
            .client_write(ConsensusCommand::StopDomain { domain_id })
            .await
            .map(|_| ())
    }

    pub async fn reconcile_domain_clock_authority(
        &self,
        domain_id: DomainName,
        expected_start_version: u64,
        expected_authority: DomainClockAuthority,
        owner: Option<ClusterNodeIdentity>,
    ) -> Result<(), Report<ConsensusError>> {
        let written = self
            .inner
            .client_write(ConsensusCommand::ReconcileDomainClockAuthority {
                domain_id,
                expected_start_version,
                expected_authority,
                owner,
            })
            .await;
        match written {
            Ok(_) => Ok(()),
            Err(error) => Err(Report::new(error)),
        }
    }

    pub async fn pause_domain(&self, domain_id: DomainName) -> Result<(), ConsensusError> {
        self.inner
            .client_write(ConsensusCommand::PauseDomain { domain_id })
            .await
            .map(|_| ())
    }

    pub async fn resume_domain(&self, domain_id: DomainName) -> Result<(), ConsensusError> {
        self.inner
            .client_write(ConsensusCommand::ResumeDomain { domain_id })
            .await
            .map(|_| ())
    }

    pub async fn create_user(&self, user: UserCredentials) -> Result<(), ConsensusError> {
        self.inner
            .client_write(ConsensusCommand::CreateUser {
                user: Box::new(user),
            })
            .await
            .map(|_| ())
    }

    pub async fn begin_resource_upload(
        &self,
        key: ResourceUploadKey,
    ) -> Result<ResourceUpload, Report<ConsensusError>> {
        let response = self
            .inner
            .client_write(ConsensusCommand::BeginResourceUpload {
                key: Box::new(key.clone()),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => self
                .current_resources()
                .await
                .upload(&key)
                .cloned()
                .ok_or_else(|| Report::new(ConsensusError::UnexpectedResponse)),
            ConsensusResponse::Conflict(reason) => {
                Err(Report::new(ConsensusError::Conflict(reason)))
            }
            ConsensusResponse::Transaction(_) => {
                Err(Report::new(ConsensusError::UnexpectedResponse))
            }
        }
    }

    pub async fn create_resource_catalog(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
    ) -> Result<(), ConsensusError> {
        self.inner
            .client_write(ConsensusCommand::CreateResourceCatalog {
                domain: domain.clone(),
                identifier: identifier.clone(),
            })
            .await
            .map(|_| ())
    }

    pub async fn publish_resource_upload(
        &self,
        key: ResourceUploadKey,
        resource: ResourceVersion,
        replica: ResourceNodeStatus,
    ) -> Result<ResourceUpload, Report<ConsensusError>> {
        let response = self
            .inner
            .client_write(ConsensusCommand::PublishResourceUpload {
                key: Box::new(key.clone()),
                resource: Box::new(resource),
                replica: Box::new(replica),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => self
                .current_resources()
                .await
                .upload(&key)
                .cloned()
                .ok_or_else(|| Report::new(ConsensusError::UnexpectedResponse)),
            ConsensusResponse::Conflict(reason) => {
                Err(Report::new(ConsensusError::Conflict(reason)))
            }
            ConsensusResponse::Transaction(_) => {
                Err(Report::new(ConsensusError::UnexpectedResponse))
            }
        }
    }

    pub async fn put_resource_replica(
        &self,
        replica: ResourceNodeStatus,
    ) -> Result<(), ConsensusError> {
        self.inner
            .client_write(ConsensusCommand::PutResourceReplica {
                replica: Box::new(replica),
            })
            .await
            .map(|_| ())
    }

    pub async fn set_node_cordoned(
        &self,
        node_id: ClusterNodeName,
        cordoned: bool,
    ) -> Result<(), ConsensusError> {
        self.inner
            .client_write(ConsensusCommand::SetNodeCordoned { node_id, cordoned })
            .await
            .map(|_| ())
    }

    async fn write_transaction(
        &self,
        command: ConsensusCommand,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        let response = self.inner.client_write(command).await?;
        let ConsensusResponse::Transaction(response) = response.data else {
            return Err(ConsensusTransactionError::InvalidResponse);
        };
        response.result.map_err(Into::into)
    }

    pub async fn open_transaction(
        &self,
        transaction: ReplicatedTransaction,
        max_open_transactions: usize,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::OpenTransaction {
            transaction: Box::new(transaction),
            max_open_transactions,
        })
        .await
    }

    pub async fn queue_transaction_statement(
        &self,
        id: String,
        owner: UserName,
        domain: DomainName,
        at: nervix_models::Timestamp,
        statement: TransactionStatement,
        limits: TransactionQueueLimits,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::QueueTransactionStatement {
            id,
            owner,
            domain,
            at,
            statement: Box::new(statement),
            limits,
        })
        .await
    }

    pub async fn touch_transaction(
        &self,
        id: String,
        owner: UserName,
        at: nervix_models::Timestamp,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::TouchTransaction { id, owner, at })
            .await
    }

    pub async fn start_transaction_commit(
        &self,
        id: String,
        owner: UserName,
        at: nervix_models::Timestamp,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::StartTransactionCommit { id, owner, at })
            .await
    }

    pub async fn advance_transaction_commit(
        &self,
        advance: TransactionCommitAdvance,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        let TransactionCommitAdvance {
            id,
            expected_next_statement,
            next_statement,
            at,
            result,
            effect,
            completion,
        } = advance;
        self.write_transaction(ConsensusCommand::AdvanceTransactionCommit {
            id,
            expected_next_statement,
            next_statement,
            at,
            result: Box::new(result),
            effect: effect.map(Box::new),
            completion,
        })
        .await
    }

    pub async fn finish_empty_transaction_commit(
        &self,
        id: String,
        at: nervix_models::Timestamp,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::FinishEmptyTransactionCommit { id, at })
            .await
    }

    pub async fn revert_transaction(
        &self,
        id: String,
        owner: UserName,
        at: nervix_models::Timestamp,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::RevertTransaction { id, owner, at })
            .await
    }

    pub async fn expire_transaction(
        &self,
        id: String,
        at: nervix_models::Timestamp,
        idle_before: nervix_models::Timestamp,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::ExpireTransaction {
            id,
            at,
            idle_before,
        })
        .await
    }

    pub async fn remove_finished_transactions(
        &self,
        finished_before: nervix_models::Timestamp,
    ) -> Result<(), ConsensusError> {
        self.inner
            .client_write(ConsensusCommand::RemoveFinishedTransactions { finished_before })
            .await
            .map(|_| ())
    }
}

impl Administrator {
    pub fn observer(&self) -> Observer {
        Observer {
            inner: self.inner.clone(),
        }
    }

    pub async fn maybe_initialize(&self) -> Result<bool, ConsensusError> {
        let _membership_mutation = self.inner.membership_mutation.lock().await;
        if self
            .inner
            .store
            .has_raft_state()
            .await
            .map_err(ConsensusError::Storage)?
        {
            return Ok(false);
        }

        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.inner.local_node_id.clone(),
            BasicNode::new(self.inner.interconnect_advertise_addr.clone()),
        );
        self.inner
            .raft
            .initialize(nodes)
            .await
            .map(|_| ())
            .map_err(|_| ConsensusError::Startup)?;
        self.inner.events.report(format!(
            "raft initialized with single-node membership {}",
            self.inner.local_node_id
        ));
        Ok(true)
    }

    pub async fn reconcile_nodes(&self, gossip: GossipState) -> Result<(), ConsensusError> {
        let _membership_mutation = self.inner.membership_mutation.lock().await;
        let leader = self.inner.raft.current_leader().await;
        if leader.as_ref() != Some(&self.inner.local_node_id) {
            return Ok(());
        }

        let before = self.effective_membership();
        let mutations = before.automatic_mutations(&gossip);
        for mutation in mutations {
            tokio::task::consume_budget().await;
            match mutation {
                MembershipMutation::AddLearner {
                    node_id,
                    address,
                    refresh,
                } => {
                    let operation = if refresh {
                        format!("refresh learner '{node_id}' at {address}")
                    } else if before.nodes.contains_key(&node_id) {
                        format!("wait for learner '{node_id}' to catch up at {address}")
                    } else {
                        format!("add learner '{node_id}' at {address}")
                    };
                    self.inner.events.report(format!("raft {operation}"));
                    let admission = timeout(
                        MEMBERSHIP_MUTATION_TIMEOUT,
                        self.inner
                            .raft
                            .add_learner(node_id, BasicNode::new(address), true),
                    )
                    .await;
                    let result = match admission {
                        Ok(result) => result,
                        Err(_) => {
                            let observed = self.effective_membership();
                            return Err(self.membership_timeout(operation, &observed));
                        }
                    };
                    result.map_err(ConsensusError::from)?;
                }
                MembershipMutation::ChangeVoters { voters } => {
                    let operation = format!("promote voters {voters:?}");
                    let membership = timeout(
                        MEMBERSHIP_MUTATION_TIMEOUT,
                        self.inner.raft.change_membership(voters.clone(), true),
                    )
                    .await;
                    let result = match membership {
                        Ok(result) => result,
                        Err(_) => {
                            let observed = self.effective_membership();
                            if observed.voters == voters {
                                continue;
                            }
                            return Err(self.membership_timeout(operation, &observed));
                        }
                    };
                    result.map_err(ConsensusError::from)?;
                }
            }
        }

        let after = self.effective_membership();
        if before.voters != after.voters {
            self.inner
                .events
                .report(format!("raft membership updated: {:?}", after.voters));
        }
        Ok(())
    }

    pub async fn drop_node(&self, node_id: &ClusterNodeName) -> Result<(), ConsensusError> {
        if *node_id == self.inner.local_node_id {
            return Err(ConsensusError::RemoveLocalLeader(node_id.to_string()));
        }

        let _membership_mutation = self.inner.membership_mutation.lock().await;
        let before = self.effective_membership();
        if !before.nodes.contains_key(node_id) {
            return Err(ConsensusError::NodeNotFound(node_id.to_string()));
        }

        let mut desired_voters = before.voters;
        let was_voter = desired_voters.remove(node_id);
        if was_voter && desired_voters.is_empty() {
            return Err(ConsensusError::RemoveLastVoter(node_id.to_string()));
        }

        let operation = format!("remove node '{node_id}'");
        let membership = timeout(
            MEMBERSHIP_MUTATION_TIMEOUT,
            self.inner
                .raft
                .change_membership(desired_voters.clone(), false),
        )
        .await;
        let result = match membership {
            Ok(result) => result,
            Err(_) => {
                let observed = self.effective_membership();
                if !observed.nodes.contains_key(node_id) {
                    self.inner
                        .events
                        .report(format!("raft node removed after timed wait: {node_id}"));
                    return Ok(());
                }
                return Err(self.membership_timeout(operation, &observed));
            }
        };
        result.map_err(ConsensusError::from)?;

        self.inner
            .events
            .report(format!("raft node removed: {node_id}"));
        Ok(())
    }

    fn effective_membership(&self) -> MembershipSnapshot {
        let metrics = self.inner.raft.metrics().borrow_watched().clone();
        MembershipSnapshot {
            voters: metrics.membership_config.membership().voter_ids().collect(),
            nodes: metrics
                .membership_config
                .nodes()
                .map(|(node_id, node)| (node_id.clone(), node.addr.clone()))
                .collect(),
        }
    }

    fn membership_timeout(
        &self,
        operation: String,
        observed: &MembershipSnapshot,
    ) -> ConsensusError {
        self.inner.events.report(format!(
            "raft membership wait timed out after {MEMBERSHIP_MUTATION_TIMEOUT:?}: {operation}; \
             observed effective voters {:?} and nodes {:?}",
            observed.voters, observed.nodes
        ));
        ConsensusError::MembershipChangeTimeout {
            operation,
            timeout: MEMBERSHIP_MUTATION_TIMEOUT,
        }
    }

    pub async fn transfer_leadership_to(
        &self,
        target_node_id: ClusterNodeName,
    ) -> Result<(), openraft::error::Fatal<TypeConfig>> {
        self.inner
            .raft
            .trigger()
            .transfer_leader(target_node_id)
            .await
    }
}

impl ConsensusState {
    /// Propose one command once reclaiming the log has left room for it.
    ///
    /// A node whose retained log has reached its cap holds new mutations back rather than growing
    /// past the bound. Reads, health and recovery continue throughout, and a node whose
    /// reclamation genuinely cannot keep up fails the mutation instead of waiting forever.
    async fn client_write(
        &self,
        command: ConsensusCommand,
    ) -> Result<openraft::raft::ClientWriteResponse<TypeConfig>, ConsensusError> {
        let cap = self.raft_retention.retained_log_cap_bytes;
        if self.store.retained_log_bytes() > cap {
            let deadline = self.raft_retention.retention_admission_timeout;
            let reclaimed = timeout(deadline, async {
                loop {
                    tokio::task::consume_budget().await;
                    tokio::time::sleep(RETENTION_ADMISSION_POLL).await;
                    if self.store.retained_log_bytes() <= cap {
                        return;
                    }
                }
            })
            .await;
            if reclaimed.is_err() {
                return Err(ConsensusError::LogRetentionSaturated {
                    retained: self.store.retained_log_bytes(),
                    cap,
                    waited: deadline,
                });
            }
        }
        self.raft
            .client_write(command)
            .await
            .map_err(ConsensusError::from)
    }
}

impl ProtocolReceiver {
    pub async fn append_entries(
        &self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RaftError<TypeConfig>> {
        self.inner.raft.append_entries(req).await
    }

    pub async fn vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>> {
        self.inner.raft.vote(req).await
    }

    pub async fn transfer_leader(
        &self,
        req: TransferLeaderRequest<TypeConfig>,
    ) -> Result<TransferLeaderResponse<TypeConfig>, openraft::error::Fatal<TypeConfig>> {
        self.inner.raft.handle_transfer_leader(req).await
    }

    fn begin_snapshot_transfer(
        &self,
        peer: ClusterNodeName,
        transfer_id: u64,
        vote: VoteOf,
        meta: SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node>,
        section_count: u32,
        total_bytes: u64,
    ) -> Result<(), Report<SnapshotTransferError>> {
        let generation = self.inner.store.claim_snapshot_generation();
        let transfer = IncomingSnapshotTransfer::new(
            transfer_id,
            generation,
            vote,
            meta,
            section_count,
            total_bytes,
        );
        // A peer that restarts a transfer abandons whatever the previous one staged.
        self.discard_snapshot_transfer(&peer);
        self.inner.incoming_snapshots.lock().insert(peer, transfer);
        Ok(())
    }

    /// Take one chunk and, when it completes a section, seal that section in node-owned storage.
    async fn append_snapshot_chunk(
        &self,
        peer: &ClusterNodeName,
        transfer_id: u64,
        chunk: SnapshotChunkPart,
    ) -> Result<(), Report<SnapshotTransferError>> {
        let limits = self.inner.store.limits();
        let completed = {
            let mut transfers = self.inner.incoming_snapshots.lock();
            let transfer = transfers.get_mut(peer).ok_or_else(|| {
                Report::new(SnapshotTransferError::Missing { peer: peer.clone() })
            })?;
            transfer.append_chunk(
                transfer_id,
                chunk,
                limits.snapshot_section_bytes.as_u64(),
                snapshot_chunk_bytes(&limits),
            )?
        };
        let Some(section) = completed else {
            return Ok(());
        };
        self.inner
            .store
            .stage_snapshot_section(section.generation, section.index, section.bytes)
            .await
            .map_err(|error| Report::new(SnapshotTransferError::Stage(error.to_string())))
    }

    async fn finish_snapshot_transfer(
        &self,
        peer: &ClusterNodeName,
        transfer_id: u64,
    ) -> Result<SnapshotResponse<TypeConfig>, Report<SnapshotTransferError>> {
        let transfer = {
            let mut transfers = self.inner.incoming_snapshots.lock();
            let Some(current) = transfers.get(peer) else {
                return Err(Report::new(SnapshotTransferError::Missing {
                    peer: peer.clone(),
                }));
            };
            current.verify_id(transfer_id)?;
            transfers
                .remove(peer)
                .verified("the transfer was found for this peer immediately above")
        };
        let transfer = transfer.complete(transfer_id)?;
        let snapshot = self
            .inner
            .store
            .open_staged_snapshot(snapshot::SnapshotManifest {
                generation: transfer.generation,
                last_applied_log_id: transfer.meta.last_log_id.clone(),
                last_membership: Arc::new(transfer.meta.last_membership.clone()),
                section_count: transfer.section_count,
                total_bytes: transfer.total_bytes,
            });
        // The authoritative check belongs to Raft: it revalidates the vote and whether this
        // snapshot still applies before anything is published.
        self.inner
            .raft
            .install_full_snapshot(
                transfer.vote,
                Snapshot {
                    meta: transfer.meta,
                    snapshot,
                },
            )
            .await
            .map_err(|error| Report::new(SnapshotTransferError::Install(error.to_string())))
    }

    /// Drop a transfer a peer abandoned, releasing the generation it staged.
    fn discard_snapshot_transfer(&self, peer: &ClusterNodeName) {
        let abandoned = self.inner.incoming_snapshots.lock().remove(peer);
        if let Some(abandoned) = abandoned {
            self.inner
                .store
                .abandon_snapshot_generation(abandoned.generation);
        }
    }
}

#[derive(Clone)]
struct NetworkFactory {
    local_node_id: ClusterNodeName,
    interconnect: Transport,
    executor: nervix_execution::Executor,
}

#[derive(Clone)]
struct NetworkClient {
    local_node_id: ClusterNodeName,
    target: ClusterNodeName,
    interconnect: Transport,
    executor: nervix_execution::Executor,
    append_path: AppendPath,
}

impl NetworkFactory {
    fn client(&self, target: ClusterNodeName, append_path: AppendPath) -> NetworkClient {
        NetworkClient {
            local_node_id: self.local_node_id.clone(),
            target,
            interconnect: self.interconnect.clone(),
            executor: self.executor.clone(),
            append_path,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: ClusterNodeName, _node: &Node) -> Self::Network {
        self.client(target, AppendPath::Replication)
    }

    /// Leader liveness probes keep their own management connection, so a saturated append stream
    /// cannot delay the lease that keeps this leader in office.
    async fn new_heartbeat_client(
        &mut self,
        target: ClusterNodeName,
        _node: &Node,
    ) -> Self::Network {
        self.client(target, AppendPath::Heartbeat)
    }

    async fn new_snapshot_client(
        &mut self,
        target: ClusterNodeName,
        _node: &Node,
    ) -> Self::Network {
        self.client(target, AppendPath::Replication)
    }
}

fn io_error(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(err.to_string())
}

fn next_snapshot_transfer_id() -> io::Result<u64> {
    NEXT_SNAPSHOT_TRANSFER_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| io::Error::other("snapshot transfer id space is exhausted"))
}

/// The application body one snapshot chunk submits. A section is carried as more chunks, never as
/// a larger one.
fn snapshot_chunk_bytes(limits: &nervix_execution::OperationLimits) -> usize {
    usize::try_from(limits.bulk_chunk_bytes.as_u64())
        .assured("a configured bulk chunk fits the address space it is buffered in")
}

fn snapshot_request_timeout(deadline: Instant) -> io::Result<Duration> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "snapshot deadline elapsed"))?;
    if remaining.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "snapshot deadline elapsed",
        ));
    }
    Ok(remaining)
}

fn storage_decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, io::Error> {
    ciborium::from_reader(Cursor::new(bytes)).map_err(io_error)
}

fn unreachable_err<E: std::error::Error + Send + Sync + 'static>(
    err: E,
) -> openraft::error::Unreachable<TypeConfig> {
    openraft::error::Unreachable::new(&err)
}

impl RaftNetworkV2<TypeConfig> for NetworkClient {
    type SnapshotData = SealedSnapshot;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let record = wire::AppendEntriesRecord::from_request(rpc);
        let response = self
            .interconnect
            .request_with_timeout(
                &self.target,
                wire::HeartbeatRequest(record),
                replication::append_deadline(&option),
            )
            .await
            .map_err(io_error)
            .map_err(unreachable_err)?
            .map_err(io_error)
            .map_err(unreachable_err)?;
        Ok(response.into_response())
    }

    /// Carry appends on the path this client was built for.
    ///
    /// A replication client opens one ordered stream to its follower and pipelines every batch
    /// over it. A heartbeat client keeps OpenRaft's one-probe-at-a-time shape on the management
    /// pool, where a saturated append stream cannot reach it.
    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        option: RPCOption,
    ) -> openraft::base::BoxFuture<
        's,
        Result<
            openraft::base::BoxStream<
                's,
                Result<openraft::raft::StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>,
            >,
            RPCError<TypeConfig>,
        >,
    >
    where
        S: futures_util::Stream<Item = AppendEntriesRequest<TypeConfig>>
            + openraft::OptionalSend
            + Unpin
            + 'static,
    {
        match self.append_path {
            AppendPath::Heartbeat => {
                openraft::network::stream_append_sequential(self, input, option)
            }
            AppendPath::Replication => {
                let interconnect = self.interconnect.clone();
                let executor = self.executor.clone();
                let local_node_id = self.local_node_id.clone();
                let target = self.target.clone();
                Box::pin(async move {
                    let answers = replication::open_append_stream(
                        &interconnect,
                        &executor,
                        &local_node_id,
                        &target,
                        input,
                        option,
                    )
                    .await?;
                    Ok(answers)
                })
            }
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let rpc_timeout = option.hard_ttl();
        let response = self
            .interconnect
            .request_with_timeout(
                &self.target,
                wire::RequestVote(wire::VoteRequestRecord::from_request(rpc)),
                rpc_timeout,
            )
            .await
            .map_err(io_error)
            .map_err(unreachable_err)?;
        let response = response.map_err(io_error).map_err(unreachable_err)?;
        Ok(response.into_response())
    }

    /// Send one sealed snapshot generation, section by section.
    ///
    /// Only one section is held in memory at a time, so a snapshot larger than the transfer budget
    /// still moves. Cancellation is honoured between every chunk.
    async fn full_snapshot(
        &mut self,
        vote: VoteOf,
        snapshot: Snapshot<
            CommittedLeaderIdOf<TypeConfig>,
            ClusterNodeName,
            Node,
            Self::SnapshotData,
        >,
        cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
        + openraft::OptionalSend
        + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let rpc_timeout = option.hard_ttl();
        let deadline = Instant::now().checked_add(rpc_timeout).ok_or_else(|| {
            StreamingError::from(unreachable_err(io::Error::other(
                "snapshot deadline exceeds the monotonic clock range",
            )))
        })?;
        let transfer_id = next_snapshot_transfer_id()
            .map_err(unreachable_err)
            .map_err(StreamingError::from)?;
        let Snapshot { meta, snapshot } = snapshot;
        let manifest = snapshot.manifest().clone();
        let chunk_bytes = snapshot_chunk_bytes(self.executor.limits());
        let transfer = async {
            self.interconnect
                .request_with_timeout(
                    &self.target,
                    wire::BeginSnapshotTransfer::from_parts(
                        transfer_id,
                        vote,
                        meta,
                        manifest.section_count,
                        manifest.total_bytes,
                    ),
                    snapshot_request_timeout(deadline).map_err(unreachable_err)?,
                )
                .await
                .map_err(io_error)
                .map_err(unreachable_err)?
                .map_err(io_error)
                .map_err(unreachable_err)?;

            for section_index in 0..manifest.section_count {
                tokio::task::consume_budget().await;
                let section = snapshot
                    .section(section_index)
                    .await
                    .map_err(unreachable_err)?;
                let section_bytes = u64::try_from(section.len())
                    .assured("supported targets have a pointer width no larger than u64");
                let mut offset = 0_u64;
                for chunk in section.chunks(chunk_bytes) {
                    tokio::task::consume_budget().await;
                    self.interconnect
                        .request_with_timeout(
                            &self.target,
                            wire::SnapshotChunk {
                                transfer_id,
                                section_index,
                                section_bytes,
                                offset,
                                bytes: chunk.to_vec(),
                            },
                            snapshot_request_timeout(deadline).map_err(unreachable_err)?,
                        )
                        .await
                        .map_err(io_error)
                        .map_err(unreachable_err)?
                        .map_err(io_error)
                        .map_err(unreachable_err)?;
                    let chunk_len = u64::try_from(chunk.len())
                        .assured("supported targets have a pointer width no larger than u64");
                    offset = offset
                        .checked_add(chunk_len)
                        .assured("chunks are slices of one section whose length fits in u64");
                }
            }

            self.interconnect
                .request_with_timeout(
                    &self.target,
                    wire::FinishSnapshotTransfer { transfer_id },
                    snapshot_request_timeout(deadline).map_err(unreachable_err)?,
                )
                .await
                .map_err(io_error)
                .map_err(unreachable_err)?
                .map(wire::SnapshotResponseRecord::into_response)
                .map_err(io_error)
                .map_err(unreachable_err)
        };
        tokio::pin!(transfer);
        tokio::pin!(cancel);
        tokio::select! {
            closed = &mut cancel => Err(StreamingError::Closed(closed)),
            result = &mut transfer => result.map_err(StreamingError::from),
        }
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        let rpc_timeout = option.hard_ttl();
        let response = self
            .interconnect
            .request_with_timeout(
                &self.target,
                wire::TransferLeadership::from_request(req),
                rpc_timeout,
            )
            .await
            .map_err(io_error)
            .map_err(unreachable_err)?;
        Ok(response
            .map(wire::TransferLeadershipResponse::into_response)
            .map_err(io_error)
            .map_err(unreachable_err)?)
    }
}

#[derive(Debug, Default)]
struct StateMachineChanges {
    schedule_changed: bool,
    domains_changed: bool,
    resources_changed: bool,
    transactions_changed: bool,
}

#[derive(Debug)]
struct AppliedConsensusCommand {
    response: ConsensusResponse,
    schedule_changed: bool,
    domains_changed: bool,
    resources_changed: bool,
    transactions_changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppliedEntryContext {
    leader_term: u64,
    input_revision: Option<u64>,
}

impl AppliedConsensusCommand {
    fn applied(changes: StateMachineChanges) -> Self {
        Self {
            response: ConsensusResponse::Applied,
            schedule_changed: changes.schedule_changed,
            domains_changed: changes.domains_changed,
            resources_changed: changes.resources_changed,
            transactions_changed: changes.transactions_changed,
        }
    }

    fn conflict(reason: String) -> Self {
        Self {
            response: ConsensusResponse::Conflict(reason),
            schedule_changed: false,
            domains_changed: false,
            resources_changed: false,
            transactions_changed: false,
        }
    }

    fn transaction(
        result: Result<ReplicatedTransaction, TransactionMutationError>,
        changes: StateMachineChanges,
    ) -> Self {
        Self {
            response: ConsensusResponse::Transaction(Box::new(TransactionMutationResponse {
                result,
            })),
            schedule_changed: changes.schedule_changed,
            domains_changed: changes.domains_changed,
            resources_changed: changes.resources_changed,
            transactions_changed: changes.transactions_changed,
        }
    }
}

#[cfg(test)]
fn apply_consensus_command(
    state: &mut StateMachineData,
    command: &ConsensusCommand,
) -> AppliedConsensusCommand {
    apply_consensus_command_at(
        state,
        command,
        AppliedEntryContext {
            leader_term: 0,
            input_revision: None,
        },
    )
}

fn apply_consensus_command_at(
    state: &mut StateMachineData,
    command: &ConsensusCommand,
    context: AppliedEntryContext,
) -> AppliedConsensusCommand {
    let mut changes = StateMachineChanges::default();
    match command {
        ConsensusCommand::ReplaceDomainSchedule {
            domain,
            expected_schedule,
            schedule,
        } => {
            if state.schedule.domain(domain) != expected_schedule.as_deref() {
                return AppliedConsensusCommand::conflict(format!(
                    "domain '{}' schedule changed",
                    domain.as_str()
                ));
            }
            state.replace_domain_schedule(domain, schedule.as_deref());
            changes.schedule_changed = true;
        }
        ConsensusCommand::ApplyAutomaticDomainSchedule {
            fence,
            domain,
            expected_schedule,
            schedule,
        } => {
            if context.leader_term != fence.leader_tenure.term {
                return AppliedConsensusCommand::conflict(
                    "automatic schedule decision leader tenure changed".to_string(),
                );
            }
            if context.input_revision != fence.input_revision {
                return AppliedConsensusCommand::conflict(
                    "automatic schedule decision input revision changed".to_string(),
                );
            }
            if state.schedule.domain(domain) != expected_schedule.as_deref() {
                return AppliedConsensusCommand::conflict(format!(
                    "domain '{}' schedule changed",
                    domain.as_str()
                ));
            }
            state.replace_domain_schedule(domain, schedule.as_deref());
            changes.schedule_changed = true;
        }
        ConsensusCommand::PutDomainAndSchedule {
            expected_domain,
            expected_schedule,
            domain,
            schedule,
        } => {
            if state.domains.get(&domain.id) != expected_domain.as_deref() {
                return AppliedConsensusCommand::conflict(format!(
                    "domain '{}' configuration changed",
                    domain.id.as_str()
                ));
            }
            if state.schedule.domain(&domain.id) != expected_schedule.as_deref() {
                return AppliedConsensusCommand::conflict(format!(
                    "domain '{}' schedule changed",
                    domain.id.as_str()
                ));
            }
            state
                .domains
                .insert(domain.id.clone(), domain.as_ref().clone());
            state.replace_domain_schedule(&domain.id, schedule.as_deref());
            changes.domains_changed = true;
            changes.schedule_changed = true;
        }
        ConsensusCommand::PutDomain { domain } => {
            state.domains.insert(domain.id.clone(), (**domain).clone());
            changes.domains_changed = true;
        }
        ConsensusCommand::StartDomain {
            domain_id,
            start,
            clock,
            authority,
        } => {
            changes.domains_changed = state.commit_domain_start(domain_id, start, clock, authority);
        }
        ConsensusCommand::StopDomain { domain_id } => {
            changes.domains_changed = state.commit_domain_stop(domain_id);
        }
        ConsensusCommand::PauseDomain { domain_id } => {
            if let Some(domain) = state.domains.get_mut(domain_id)
                && let DomainStatus::Running = domain.status
            {
                domain.status = DomainStatus::Paused;
                changes.domains_changed = true;
            }
        }
        ConsensusCommand::ResumeDomain { domain_id } => {
            if let Some(domain) = state.domains.get_mut(domain_id)
                && let DomainStatus::Paused = domain.status
            {
                domain.status = DomainStatus::Running;
                changes.domains_changed = true;
            }
        }
        ConsensusCommand::ReconcileDomainClockAuthority {
            domain_id,
            expected_start_version,
            expected_authority,
            owner,
        } => {
            changes.domains_changed = state.reconcile_domain_clock_authority(
                domain_id,
                *expected_start_version,
                expected_authority,
                owner,
            );
        }
        ConsensusCommand::CreateUser { user } => {
            if !state.users.contains_key(&user.name) {
                state.users.insert(user.name.clone(), user.as_ref().clone());
            }
        }
        ConsensusCommand::CreateResourceCatalog { domain, identifier } => {
            state.resources.ensure_catalog(domain, identifier);
            changes.resources_changed = true;
        }
        ConsensusCommand::BeginResourceUpload { key } => {
            if let Err(reason) = state.resources.begin_upload(key) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            changes.resources_changed = true;
        }
        ConsensusCommand::PublishResourceUpload {
            key,
            resource,
            replica,
        } => {
            if let Err(reason) = state.resources.publish_upload(key, resource, replica) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            changes.resources_changed = true;
        }
        ConsensusCommand::PutResourceReplica { replica } => {
            state
                .resources
                .replicas
                .insert(replica.key.clone(), replica.as_ref().clone());
            changes.resources_changed = true;
        }
        ConsensusCommand::SetNodeCordoned { node_id, cordoned } => {
            if *cordoned {
                state.cordoned_node_ids.insert(node_id.clone(), ());
            } else {
                state.cordoned_node_ids.remove(node_id);
            }
        }
        ConsensusCommand::OpenTransaction {
            transaction,
            max_open_transactions,
        } => {
            let result = if state.transactions.contains_key(&transaction.id) {
                Err(TransactionMutationError::AlreadyExists {
                    id: transaction.id.clone(),
                })
            } else if state
                .transactions
                .values()
                .filter(|transaction| transaction.state.is_live())
                .count()
                >= *max_open_transactions
            {
                Err(TransactionMutationError::OpenLimit {
                    limit: *max_open_transactions,
                })
            } else {
                let transaction = transaction.as_ref().clone();
                state
                    .transactions
                    .insert(transaction.id.clone(), transaction.clone());
                changes.transactions_changed = true;
                Ok(transaction)
            };
            return AppliedConsensusCommand::transaction(result, changes);
        }
        ConsensusCommand::QueueTransactionStatement {
            id,
            owner,
            domain,
            at,
            statement,
            limits,
        } => {
            let result = mutate_transaction(state, id, |transaction| {
                transaction.queue(owner, domain, *at, statement.as_ref().clone(), *limits)
            });
            changes.transactions_changed = result.is_ok();
            return AppliedConsensusCommand::transaction(result, changes);
        }
        ConsensusCommand::TouchTransaction { id, owner, at } => {
            let result = mutate_transaction(state, id, |transaction| transaction.touch(owner, *at));
            changes.transactions_changed = result.is_ok();
            return AppliedConsensusCommand::transaction(result, changes);
        }
        ConsensusCommand::StartTransactionCommit { id, owner, at } => {
            let result = mutate_transaction(state, id, |transaction| {
                transaction.start_commit(owner, *at)
            });
            changes.transactions_changed = result.is_ok();
            return AppliedConsensusCommand::transaction(result, changes);
        }
        ConsensusCommand::AdvanceTransactionCommit {
            id,
            expected_next_statement,
            next_statement,
            at,
            result,
            effect,
            completion,
        } => {
            let mut transaction = match state.transactions.get(id).cloned() {
                Some(transaction) => transaction,
                None => {
                    return AppliedConsensusCommand::transaction(
                        Err(TransactionMutationError::Unknown { id: id.clone() }),
                        changes,
                    );
                }
            };
            if let Err(error) = transaction.advance(
                *expected_next_statement,
                *next_statement,
                *at,
                result.as_ref().clone(),
                completion.clone(),
            ) {
                return AppliedConsensusCommand::transaction(Err(error), changes);
            }
            if let Some(effect) = effect {
                if let Err(error) = validate_transaction_step_effect(
                    state,
                    state.transactions.get(id).verified(
                        "the branch above resolved this transaction id in the same replicated \
                         state",
                    ),
                    *expected_next_statement,
                    *next_statement,
                    result,
                    effect,
                    completion.as_ref(),
                ) {
                    return AppliedConsensusCommand::transaction(Err(error), changes);
                }
                apply_transaction_step_effect(state, &transaction.domain, effect, &mut changes);
            } else if let Err(error) = validate_transaction_step_without_effect(
                state.transactions.get(id).verified(
                    "the branch above resolved this transaction id in the same replicated state",
                ),
                *expected_next_statement,
                *next_statement,
                result,
                completion.as_ref(),
            ) {
                return AppliedConsensusCommand::transaction(Err(error), changes);
            }
            state.transactions.insert(id.clone(), transaction.clone());
            changes.transactions_changed = true;
            return AppliedConsensusCommand::transaction(Ok(transaction), changes);
        }
        ConsensusCommand::FinishEmptyTransactionCommit { id, at } => {
            let result = mutate_transaction(state, id, |transaction| {
                transaction.finish_empty_commit(*at)
            });
            changes.transactions_changed = result.is_ok();
            return AppliedConsensusCommand::transaction(result, changes);
        }
        ConsensusCommand::RevertTransaction { id, owner, at } => {
            let result =
                mutate_transaction(state, id, |transaction| transaction.revert(owner, *at));
            changes.transactions_changed = result.is_ok();
            return AppliedConsensusCommand::transaction(result, changes);
        }
        ConsensusCommand::ExpireTransaction {
            id,
            at,
            idle_before,
        } => {
            let Some(mut transaction) = state.transactions.get(id).cloned() else {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::Unknown { id: id.clone() }),
                    changes,
                );
            };
            match transaction.expire(*at, *idle_before) {
                Ok(expired) => {
                    if expired {
                        state.transactions.insert(id.clone(), transaction.clone());
                        changes.transactions_changed = true;
                    }
                    return AppliedConsensusCommand::transaction(Ok(transaction), changes);
                }
                Err(error) => {
                    return AppliedConsensusCommand::transaction(Err(error), changes);
                }
            }
        }
        ConsensusCommand::RemoveFinishedTransactions { finished_before } => {
            let before = state.transactions.len();
            state.transactions.retain(|_, transaction| {
                let TransactionState::Finished(finished) = &transaction.state else {
                    return true;
                };
                finished.finished_at > *finished_before
            });
            changes.transactions_changed = state.transactions.len() != before;
        }
    }
    AppliedConsensusCommand::applied(changes)
}

fn mutate_transaction(
    state: &mut StateMachineData,
    id: &str,
    mutation: impl FnOnce(&mut ReplicatedTransaction) -> Result<(), TransactionMutationError>,
) -> Result<ReplicatedTransaction, TransactionMutationError> {
    let Some(transaction) = state.transactions.get_mut(id) else {
        return Err(TransactionMutationError::Unknown { id: id.to_string() });
    };
    mutation(transaction)?;
    Ok(transaction.clone())
}

fn validate_transaction_step_contract<'a>(
    transaction: &'a ReplicatedTransaction,
    first_statement: usize,
    next_statement: usize,
    result: &TransactionStepResult,
    completion: Option<&TransactionOutcome>,
) -> Result<&'a [TransactionStatement], TransactionMutationError> {
    let statements = transaction
        .statements
        .get(first_statement..next_statement)
        .ok_or_else(|| TransactionMutationError::EffectMismatch {
            id: transaction.id.clone(),
        })?;
    let success = result.result.success;
    let completion_matches = match completion {
        None => success && next_statement < transaction.statements.len(),
        Some(TransactionOutcome::Committed) => {
            success && next_statement == transaction.statements.len()
        }
        Some(TransactionOutcome::Failed {
            failing_step,
            error,
        }) => !success && *failing_step == first_statement && error == &result.result.message,
        Some(TransactionOutcome::Reverted | TransactionOutcome::Expired) => false,
    };
    if statements.is_empty() || !completion_matches {
        return Err(TransactionMutationError::EffectMismatch {
            id: transaction.id.clone(),
        });
    }
    Ok(statements)
}

fn validate_transaction_step_without_effect(
    transaction: &ReplicatedTransaction,
    first_statement: usize,
    next_statement: usize,
    result: &TransactionStepResult,
    completion: Option<&TransactionOutcome>,
) -> Result<(), TransactionMutationError> {
    let statements = validate_transaction_step_contract(
        transaction,
        first_statement,
        next_statement,
        result,
        completion,
    )?;
    if !result.result.success
        || statements
            .iter()
            .all(|statement| statement.statement.is_model_mutation())
    {
        return Ok(());
    }
    if statements.len() != 1 {
        return Err(TransactionMutationError::EffectMismatch {
            id: transaction.id.clone(),
        });
    }
    let statement = &statements[0];
    let valid_no_effect = match &statement.statement {
        Statement::CreateResource(create) => create.if_not_exists && result.result.already_existed,
        Statement::AlterDomain(_) => true,
        _ => false,
    };
    if valid_no_effect {
        Ok(())
    } else {
        Err(TransactionMutationError::EffectMismatch {
            id: transaction.id.clone(),
        })
    }
}

fn validate_transaction_step_effect(
    state: &StateMachineData,
    transaction: &ReplicatedTransaction,
    first_statement: usize,
    next_statement: usize,
    result: &TransactionStepResult,
    effect: &TransactionStepEffect,
    completion: Option<&TransactionOutcome>,
) -> Result<(), TransactionMutationError> {
    let statements = validate_transaction_step_contract(
        transaction,
        first_statement,
        next_statement,
        result,
        completion,
    )?;
    if !result.result.success {
        return Err(TransactionMutationError::EffectMismatch {
            id: transaction.id.clone(),
        });
    }
    let effect_matches = match effect {
        TransactionStepEffect::ReplaceDomainSchedule { domain, .. } => {
            &transaction.domain == domain
                && statements
                    .iter()
                    .all(|statement| statement.statement.is_model_mutation())
        }
        TransactionStepEffect::PutDomainAndSchedule {
            expected_domain,
            domain,
            ..
        } => {
            statements.len() == 1
                && transaction.domain == domain.id
                && expected_domain.id == domain.id
                && matches!(statements[0].statement, Statement::AlterDomain(_))
        }
        TransactionStepEffect::StartDomain { domain_id, .. } => {
            statements.len() == 1
                && &transaction.domain == domain_id
                && matches!(statements[0].statement, Statement::StartDomain(_))
        }
        TransactionStepEffect::StopDomain { domain_id, .. } => {
            statements.len() == 1
                && &transaction.domain == domain_id
                && matches!(statements[0].statement, Statement::StopDomain(_))
        }
        TransactionStepEffect::CreateResourceCatalog { identifier } => {
            statements.len() == 1
                && matches!(
                    &statements[0].statement,
                    Statement::CreateResource(create) if &create.identifier == identifier
                )
        }
    };
    if !effect_matches {
        return Err(TransactionMutationError::EffectMismatch {
            id: transaction.id.clone(),
        });
    }

    let conflict = match effect {
        TransactionStepEffect::ReplaceDomainSchedule {
            domain,
            expected_schedule,
            ..
        } => (state.schedule.domain(domain) != expected_schedule.as_deref())
            .then(|| format!("domain '{}' schedule changed", domain.as_str())),
        TransactionStepEffect::PutDomainAndSchedule {
            expected_domain,
            expected_schedule,
            ..
        } => {
            if state.domains.get(&expected_domain.id) != Some(expected_domain.as_ref()) {
                Some(format!(
                    "domain '{}' configuration changed",
                    expected_domain.id.as_str()
                ))
            } else if state.schedule.domain(&expected_domain.id) != expected_schedule.as_deref() {
                Some(format!(
                    "domain '{}' schedule changed",
                    expected_domain.id.as_str()
                ))
            } else {
                None
            }
        }
        TransactionStepEffect::StartDomain {
            domain_id,
            expected_start_version,
            ..
        } => match state.domains.get(domain_id) {
            Some(domain)
                if matches!(domain.status, DomainStatus::Stopped)
                    && domain.start_version == *expected_start_version =>
            {
                None
            }
            Some(_) => Some(format!(
                "domain '{}' start state changed",
                domain_id.as_str()
            )),
            None => Some(format!("domain '{}' no longer exists", domain_id.as_str())),
        },
        TransactionStepEffect::StopDomain {
            domain_id,
            expected_start_version,
        } => match state.domains.get(domain_id) {
            Some(domain)
                if !matches!(domain.status, DomainStatus::Stopped)
                    && domain.start_version == *expected_start_version =>
            {
                None
            }
            Some(_) => Some(format!(
                "domain '{}' stop state changed",
                domain_id.as_str()
            )),
            None => Some(format!("domain '{}' no longer exists", domain_id.as_str())),
        },
        TransactionStepEffect::CreateResourceCatalog { identifier } => state
            .resources
            .is_declared(&transaction.domain, identifier)
            .then(|| {
                format!(
                    "resource '{}' now exists in domain '{}'",
                    identifier.as_str(),
                    transaction.domain.as_str()
                )
            }),
    };
    match conflict {
        Some(reason) => Err(TransactionMutationError::StepConflict {
            id: transaction.id.clone(),
            reason,
        }),
        None => Ok(()),
    }
}

fn apply_transaction_step_effect(
    state: &mut StateMachineData,
    domain: &DomainName,
    effect: &TransactionStepEffect,
    changes: &mut StateMachineChanges,
) {
    match effect {
        TransactionStepEffect::ReplaceDomainSchedule {
            domain, schedule, ..
        } => {
            state.replace_domain_schedule(domain, schedule.as_deref());
            changes.schedule_changed = true;
        }
        TransactionStepEffect::PutDomainAndSchedule {
            domain, schedule, ..
        } => {
            state
                .domains
                .insert(domain.id.clone(), domain.as_ref().clone());
            state.replace_domain_schedule(&domain.id, schedule.as_deref());
            changes.domains_changed = true;
            changes.schedule_changed = true;
        }
        TransactionStepEffect::StartDomain {
            domain_id,
            start,
            clock,
            authority,
            ..
        } => {
            changes.domains_changed = state.commit_domain_start(domain_id, start, clock, authority);
        }
        TransactionStepEffect::StopDomain { domain_id, .. } => {
            changes.domains_changed = state.commit_domain_stop(domain_id);
        }
        TransactionStepEffect::CreateResourceCatalog { identifier } => {
            state.resources.ensure_catalog(domain, identifier);
            changes.resources_changed = true;
        }
    }
}

/// The raft state a node last reported, compared as a whole so a repeated metrics update that
/// changes nothing is not logged again.
#[derive(PartialEq, Eq)]
struct RaftTransition {
    state: String,
    term: u64,
    leader: Option<ClusterNodeName>,
}

fn read_key<T: DeserializeOwned>(keyspace: &Keyspace, key: &[u8]) -> io::Result<Option<T>> {
    let Some(bytes) = keyspace.get(key).map_err(io_error)? else {
        return Ok(None);
    };
    storage_decode(bytes.as_ref()).map(Some)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        ops::RangeInclusive,
    };

    use fjall::Database;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        ClusterNodeIdentity, ClusterNodeIncarnation, DomainClockAuthority, DomainClockState,
        DomainConfig, DomainName, DomainPace, DomainSchedule, DomainStartPoint, DomainState,
        DomainStatus, DomainTimeRate, ResourceId, ResourceName, ResourceNodeState,
        ResourceNodeStatus, ResourceReplicaKey, ResourceUploadIdentity, ResourceUploadKey,
        ResourceUploadState, ResourceVersion, ResourceVersionCounter, ResourceVersionStatus,
        Statement, Timestamp,
    };
    use openraft::{
        entry::RaftEntry,
        storage::{RaftLogReader, RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
        type_config::alias::{CommittedLeaderIdOf, EntryOf, LeaderIdOf},
        vote::RaftLeaderIdExt,
    };
    use tempfile::tempdir;

    use super::{
        AppliedEntryContext, AutomaticScheduleFence, ClusterSchedule, ConsensusCommand,
        ConsensusResponse, FjallLogReader, FjallStore, GossipNode, GossipState, LeaderTenure,
        MembershipMutation, MembershipSnapshot, ProtocolOriginError, ResourceRecords,
        StateMachineChanges, StateMachineData, TransactionCommandResult, TransactionMutationError,
        TransactionOutcome, TransactionStatement, TransactionStepEffect, TransactionStepResult,
        TypeConfig, UserCredentials, apply_consensus_command, apply_consensus_command_at,
        apply_transaction_step_effect, io_error, storage_decode, validate_protocol_origin,
    };
    use crate::{
        ClusterNodeName, ConsensusError, LogIdOf, ReplicatedTransaction, TransactionQueueLimits,
        UserName, VoteOf,
    };

    fn domain(raw: &str) -> DomainName {
        DomainName::try_from(raw).expect("valid domain")
    }

    #[test]
    fn raft_request_origin_must_match_the_authenticated_peer()
    -> Result<(), Box<dyn std::error::Error>> {
        let authenticated = ClusterNodeName::parse("node-1")?;
        let declared = ClusterNodeName::parse("node-2")?;
        validate_protocol_origin(&authenticated, &authenticated, "replication")?;

        let error = validate_protocol_origin(&authenticated, &declared, "replication")
            .expect_err("a peer must not submit Raft work for another node");
        assert!(matches!(
            error.current_context(),
            ProtocolOriginError::Mismatch {
                operation: "replication",
                authenticated: actual_authenticated,
                declared: actual_declared,
            } if actual_authenticated == &authenticated && actual_declared == &declared
        ));
        Ok(())
    }

    #[test]
    fn proposal_leadership_loss_retains_known_and_unknown_leaders()
    -> Result<(), Box<dyn std::error::Error>> {
        let node = ClusterNodeName::parse("node-2")?;
        for leader_id in [Some(node), None] {
            let error =
                super::RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(
                    openraft::error::ForwardToLeader::<TypeConfig> {
                        leader_id: leader_id.clone(),
                        leader_node: None,
                    },
                ));
            let mapped = ConsensusError::from(error);
            let ConsensusError::LeadershipLost { leader_id: actual } = mapped else {
                panic!("a proposal rejected by a non-leader must preserve leadership loss");
            };
            assert_eq!(actual, leader_id);
        }
        Ok(())
    }

    #[test]
    fn proposal_runtime_failure_retains_write_diagnostic() {
        let error = super::RaftError::Fatal(openraft::error::Fatal::<TypeConfig>::Stopped);
        let mapped = ConsensusError::from(error);
        let ConsensusError::Write(message) = mapped else {
            panic!("a stopped Raft runtime must report a write failure");
        };
        assert_eq!(message, "raft write failed: raft stopped");
    }

    #[test]
    fn transaction_errors_preserve_consensus_and_mutation_outcomes()
    -> Result<(), Box<dyn std::error::Error>> {
        let leader = ClusterNodeName::parse("node-2")?;
        let consensus = super::ConsensusTransactionError::from(ConsensusError::LeadershipLost {
            leader_id: Some(leader.clone()),
        });
        assert!(matches!(
            consensus,
            super::ConsensusTransactionError::Consensus(ConsensusError::LeadershipLost {
                leader_id: Some(actual),
            }) if actual == leader
        ));

        let mutation = TransactionMutationError::Unknown {
            id: "tx-1".to_string(),
        };
        let error = super::ConsensusTransactionError::from(mutation.clone());
        assert!(matches!(
            error,
            super::ConsensusTransactionError::Mutation(actual) if actual == mutation
        ));
        Ok(())
    }

    #[test]
    fn dead_gossip_nodes_are_not_membership_admission_candidates() {
        let state = GossipState {
            live_nodes: vec![
                GossipNode {
                    node_id: ClusterNodeName::parse("node-2").expect("valid name"),
                    incarnation: ClusterNodeIncarnation::new(2),
                    grpc_advertise_addr: String::new(),
                    web_console_advertise_addr: String::new(),
                    interconnect_advertise_addr: String::new(),
                },
                GossipNode {
                    node_id: ClusterNodeName::parse("node-3").expect("valid name"),
                    incarnation: ClusterNodeIncarnation::new(3),
                    grpc_advertise_addr: String::new(),
                    web_console_advertise_addr: String::new(),
                    interconnect_advertise_addr: String::new(),
                },
            ],
            dead_node_ids: [ClusterNodeName::parse("node-3").expect("valid node name")]
                .into_iter()
                .collect(),
        };

        assert_eq!(
            state
                .admission_candidates()
                .map(|node| node.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node-2"]
        );
    }

    #[test]
    fn live_identities_require_the_newest_nondead_node_incarnation() {
        let gossip_node = |name: &str, incarnation| GossipNode {
            node_id: ClusterNodeName::parse(name).expect("valid node name"),
            incarnation: ClusterNodeIncarnation::new(incarnation),
            grpc_advertise_addr: String::new(),
            web_console_advertise_addr: String::new(),
            interconnect_advertise_addr: String::new(),
        };
        let state = GossipState {
            live_nodes: vec![
                gossip_node("node-1", 10),
                gossip_node("node-1", 11),
                gossip_node("node-2", 20),
            ],
            dead_node_ids: BTreeSet::from([
                ClusterNodeName::parse("node-2").expect("valid node name")
            ]),
        };

        assert_eq!(
            state.live_identities(),
            BTreeSet::from([node_identity("node-1", 11)])
        );
    }

    #[test]
    fn observed_learner_retries_catch_up_before_promotion() {
        let first = ClusterNodeName::parse("node-1").assured("the test node name is valid");
        let joining = ClusterNodeName::parse("node-2").assured("the test node name is valid");
        let gossip = GossipState {
            live_nodes: vec![GossipNode {
                node_id: joining.clone(),
                incarnation: ClusterNodeIncarnation::new(2),
                grpc_advertise_addr: String::new(),
                web_console_advertise_addr: String::new(),
                interconnect_advertise_addr: "https://node-2.test:7443".to_string(),
            }],
            dead_node_ids: BTreeSet::new(),
        };
        let membership = MembershipSnapshot {
            voters: BTreeSet::from([first.clone()]),
            nodes: BTreeMap::from([
                (first.clone(), "https://node-1.test:7443".to_string()),
                (joining.clone(), "https://node-2.test:7443".to_string()),
            ]),
        };

        assert_eq!(
            membership.automatic_mutations(&gossip),
            vec![
                MembershipMutation::AddLearner {
                    node_id: joining.clone(),
                    address: "https://node-2.test:7443".to_string(),
                    refresh: false,
                },
                MembershipMutation::ChangeVoters {
                    voters: BTreeSet::from([first, joining]),
                },
            ]
        );
    }

    #[test]
    fn changed_endpoint_is_refreshed_before_membership_promotion() {
        let first = ClusterNodeName::parse("node-1").assured("the test node name is valid");
        let joining = ClusterNodeName::parse("node-2").assured("the test node name is valid");
        let current_address = "https://node-2.test:7443".to_string();
        let replacement_address = "https://node-2.test:8443".to_string();
        let gossip = GossipState {
            live_nodes: vec![GossipNode {
                node_id: joining.clone(),
                incarnation: ClusterNodeIncarnation::new(3),
                grpc_advertise_addr: String::new(),
                web_console_advertise_addr: String::new(),
                interconnect_advertise_addr: replacement_address.clone(),
            }],
            dead_node_ids: BTreeSet::new(),
        };
        let membership = MembershipSnapshot {
            voters: BTreeSet::from([first.clone()]),
            nodes: BTreeMap::from([
                (first.clone(), "https://node-1.test:7443".to_string()),
                (joining.clone(), current_address),
            ]),
        };

        assert_eq!(
            membership.automatic_mutations(&gossip),
            vec![
                MembershipMutation::AddLearner {
                    node_id: joining.clone(),
                    address: replacement_address,
                    refresh: true,
                },
                MembershipMutation::ChangeVoters {
                    voters: BTreeSet::from([first, joining]),
                },
            ]
        );
    }

    fn domain_schedule(raw: &str) -> DomainSchedule {
        DomainSchedule::new(domain(raw), Vec::new(), Vec::new())
    }

    fn running_domain_state(raw: &str) -> DomainState {
        DomainState {
            id: domain(raw),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".to_string(),
                skew: "0ms".to_string(),
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: DomainStatus::Running,
            start_version: 7,
            last_start: DomainStartPoint::Resume,
            clock: None,
        }
    }

    fn node_identity(raw: &str, incarnation: u64) -> ClusterNodeIdentity {
        ClusterNodeIdentity::new(
            ClusterNodeName::parse(raw).expect("valid node name"),
            ClusterNodeIncarnation::new(incarnation),
        )
    }

    #[test]
    fn committed_clock_authority_revisions_fence_transfer_and_stop() {
        let domain_id = domain("paced");
        let mut stopped = running_domain_state("paced");
        stopped.config.pace = DomainPace::Paced;
        stopped.status = DomainStatus::Stopped;
        stopped.start_version = 0;
        let mut state = StateMachineData::default();
        state.domains.insert(domain_id.clone(), stopped);
        let first_owner = node_identity("node-1", 10);
        let second_owner = node_identity("node-2", 20);
        let mapping = DomainClockState::new(
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(1_000),
            DomainTimeRate::ONE,
        );

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::StartDomain {
                domain_id: domain_id.clone(),
                start: DomainStartPoint::At {
                    timestamp: Timestamp::from_unix_nanos(1_000),
                    time_rate: DomainTimeRate::ONE,
                },
                clock: Some(mapping.clone()),
                authority: Some(first_owner.clone()),
            },
        );
        let first = state
            .domain_clock_authorities
            .get(&domain_id)
            .cloned()
            .expect("START commits an authority");
        assert_eq!(first.revision().get(), 1);
        assert_eq!(first.owner(), Some(&first_owner));
        assert_eq!(
            state
                .domains
                .get(&domain_id)
                .and_then(|domain| domain.clock.as_ref()),
            Some(&mapping)
        );

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReconcileDomainClockAuthority {
                domain_id: domain_id.clone(),
                expected_start_version: 1,
                expected_authority: first.clone(),
                owner: Some(second_owner.clone()),
            },
        );
        let transferred = state
            .domain_clock_authorities
            .get(&domain_id)
            .cloned()
            .expect("the transfer commits its fence");
        assert_eq!(transferred.revision().get(), 2);
        assert_eq!(transferred.owner(), Some(&second_owner));
        assert_eq!(
            state
                .domains
                .get(&domain_id)
                .and_then(|domain| domain.clock.as_ref()),
            Some(&mapping),
            "authority transfer must preserve the committed mapping"
        );

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReconcileDomainClockAuthority {
                domain_id: domain_id.clone(),
                expected_start_version: 1,
                expected_authority: first,
                owner: Some(first_owner),
            },
        );
        assert_eq!(
            state.domain_clock_authorities.get(&domain_id),
            Some(&transferred),
            "a superseded reconciliation must not replace the committed authority"
        );

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::StopDomain {
                domain_id: domain_id.clone(),
            },
        );
        let stopped = state
            .domain_clock_authorities
            .get(&domain_id)
            .expect("STOP retains the revocation fence");
        assert_eq!(stopped.revision().get(), 3);
        assert!(matches!(stopped, DomainClockAuthority::Unassigned { .. }));
    }

    #[test]
    fn direct_and_transactional_lifecycle_commit_the_same_clock_state() {
        let domain_id = domain("paced");
        let mut stopped = running_domain_state("paced");
        stopped.config.pace = DomainPace::Paced;
        stopped.status = DomainStatus::Stopped;
        stopped.start_version = 0;
        let mut direct = StateMachineData::default();
        direct.domains.insert(domain_id.clone(), stopped);
        let mut transactional = direct.clone();
        let owner = node_identity("node-1", 10);
        let start = DomainStartPoint::At {
            timestamp: Timestamp::from_unix_nanos(1_000),
            time_rate: DomainTimeRate::ONE,
        };
        let mapping = DomainClockState::new(
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(1_000),
            DomainTimeRate::ONE,
        );

        apply_consensus_command(
            &mut direct,
            &ConsensusCommand::StartDomain {
                domain_id: domain_id.clone(),
                start: start.clone(),
                clock: Some(mapping.clone()),
                authority: Some(owner.clone()),
            },
        );
        let mut changes = StateMachineChanges::default();
        apply_transaction_step_effect(
            &mut transactional,
            &domain_id,
            &TransactionStepEffect::StartDomain {
                domain_id: domain_id.clone(),
                expected_start_version: 0,
                start,
                clock: Some(mapping),
                authority: Some(owner),
            },
            &mut changes,
        );
        assert!(changes.domains_changed);
        assert_eq!(transactional.domains, direct.domains);
        assert_eq!(
            transactional.domain_clock_authorities,
            direct.domain_clock_authorities
        );

        apply_consensus_command(
            &mut direct,
            &ConsensusCommand::StopDomain {
                domain_id: domain_id.clone(),
            },
        );
        let mut changes = StateMachineChanges::default();
        apply_transaction_step_effect(
            &mut transactional,
            &domain_id,
            &TransactionStepEffect::StopDomain {
                domain_id: domain_id.clone(),
                expected_start_version: 1,
            },
            &mut changes,
        );
        assert!(changes.domains_changed);
        assert_eq!(transactional.domains, direct.domains);
        assert_eq!(
            transactional.domain_clock_authorities,
            direct.domain_clock_authorities
        );
    }

    fn resource_version(domain_id: &str, identifier: &str, version: u64) -> ResourceVersion {
        ResourceVersion {
            id: ResourceId::new(
                domain(domain_id),
                ResourceName::parse(identifier).expect("valid resource name"),
                version,
            ),
            root_checksum: format!("root-{version}"),
            manifest_checksum: format!("manifest-{version}"),
            file_count: 2,
            total_bytes: 128,
            archive_bytes: 2048,
            created_at: nervix_models::Timestamp::from_unix_nanos(42),
            created_by_node: ClusterNodeName::parse("node-1").expect("valid name"),
        }
    }

    fn temp_database() -> Database {
        let dir = tempdir().expect("tempdir");
        Database::builder(dir.keep())
            .open()
            .expect("database should open")
    }

    fn committed_leader(term: u64) -> CommittedLeaderIdOf<TypeConfig> {
        <LeaderIdOf<TypeConfig> as RaftLeaderIdExt>::new_committed(
            term,
            ClusterNodeName::parse("node-1").expect("valid name"),
        )
    }

    fn blank_log_entries(term: u64, indexes: RangeInclusive<u64>) -> Vec<EntryOf<TypeConfig>> {
        let leader = committed_leader(term);
        indexes
            .map(|index| {
                <EntryOf<TypeConfig> as RaftEntry>::new_blank(LogIdOf::new(leader.clone(), index))
            })
            .collect()
    }

    async fn read_log_indexes(reader: &mut FjallLogReader) -> Vec<u64> {
        reader
            .try_get_log_entries(..)
            .await
            .expect("log should read")
            .iter()
            .map(|entry| entry.log_id.index)
            .collect()
    }

    #[test]
    fn consensus_command_display_distinguishes_replace_and_clear() {
        let replace = ConsensusCommand::ReplaceDomainSchedule {
            domain: domain("tenant"),
            expected_schedule: None,
            schedule: Some(Box::new(domain_schedule("tenant"))),
        };
        let clear = ConsensusCommand::ReplaceDomainSchedule {
            domain: domain("tenant"),
            expected_schedule: Some(Box::new(domain_schedule("tenant"))),
            schedule: None,
        };

        assert_eq!(replace.to_string(), "replace-domain-schedule:tenant");
        assert_eq!(clear.to_string(), "clear-domain-schedule:tenant");
        assert_eq!(ConsensusResponse::Applied.to_string(), "ok");
    }

    #[test]
    fn encode_decode_roundtrip_and_invalid_bytes_fail() {
        let command = ConsensusCommand::ReplaceDomainSchedule {
            domain: domain("tenant"),
            expected_schedule: None,
            schedule: Some(Box::new(domain_schedule("tenant"))),
        };

        let bytes = crate::durable_batch::DurableBatch::encode(&command, 1024)
            .expect("command should encode");
        let decoded: ConsensusCommand = storage_decode(&bytes).expect("command should decode");
        assert_eq!(decoded, command);

        let err =
            storage_decode::<ConsensusCommand>(b"not-cbor").expect_err("invalid bytes must fail");
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn io_error_uses_display_message() {
        let err = io_error("raft transport failed");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(err.to_string(), "raft transport failed");
    }

    #[test]
    fn apply_consensus_command_replaces_sorts_and_clears_schedule() {
        let mut state = StateMachineData {
            schedule: ClusterSchedule::from_iter([domain_schedule("zeta")]).into(),
            ..Default::default()
        };

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                domain: domain("alpha"),
                expected_schedule: None,
                schedule: Some(Box::new(domain_schedule("alpha"))),
            },
        );
        assert_eq!(
            state
                .schedule
                .domains
                .keys()
                .map(DomainName::as_str)
                .collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                domain: domain("alpha"),
                expected_schedule: Some(Box::new(domain_schedule("alpha"))),
                schedule: Some(Box::new(domain_schedule("alpha"))),
            },
        );
        assert_eq!(state.schedule.domains.len(), 2);

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                domain: domain("zeta"),
                expected_schedule: Some(Box::new(domain_schedule("zeta"))),
                schedule: None,
            },
        );
        assert_eq!(
            state
                .schedule
                .domains
                .keys()
                .map(DomainName::as_str)
                .collect::<Vec<_>>(),
            vec!["alpha"]
        );
    }

    #[test]
    fn apply_consensus_command_updates_domain_and_schedule_atomically() {
        let mut state = StateMachineData::default();
        let mut domain_state = running_domain_state("tenant");
        domain_state.config.placement = nervix_models::PlacementPolicy::RequireColocation;
        let schedule = domain_schedule("tenant");

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::PutDomainAndSchedule {
                expected_domain: None,
                expected_schedule: None,
                domain: Box::new(domain_state.clone()),
                schedule: Some(Box::new(schedule.clone())),
            },
        );

        assert_eq!(state.domains.get(&domain("tenant")), Some(&domain_state));
        assert_eq!(state.schedule.domain(&domain("tenant")), Some(&schedule));
    }

    #[test]
    fn schedule_publication_rejects_a_changed_base() {
        let committed = domain_schedule("tenant");
        let mut state = StateMachineData {
            schedule: ClusterSchedule::from_iter([committed.clone()]).into(),
            ..Default::default()
        };

        let applied = apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                domain: domain("tenant"),
                expected_schedule: None,
                schedule: None,
            },
        );

        assert_eq!(
            applied.response,
            ConsensusResponse::Conflict("domain 'tenant' schedule changed".to_string())
        );
        assert_eq!(state.schedule.domain(&domain("tenant")), Some(&committed));
        assert!(!applied.schedule_changed);
    }

    #[test]
    fn automatic_schedule_publication_rejects_a_stale_leader_tenure() {
        let proposed = domain_schedule("tenant");
        let mut state = StateMachineData::default();
        let applied = apply_consensus_command_at(
            &mut state,
            &ConsensusCommand::ApplyAutomaticDomainSchedule {
                fence: AutomaticScheduleFence {
                    leader_tenure: LeaderTenure {
                        leader_id: ClusterNodeName::parse("node-1")
                            .assured("the test node name is valid"),
                        term: 7,
                    },
                    input_revision: Some(40),
                },
                domain: domain("tenant"),
                expected_schedule: None,
                schedule: Some(Box::new(proposed)),
            },
            AppliedEntryContext {
                leader_term: 8,
                input_revision: Some(40),
            },
        );

        assert_eq!(
            applied.response,
            ConsensusResponse::Conflict(
                "automatic schedule decision leader tenure changed".to_string()
            )
        );
        assert_eq!(state.schedule.domains.len(), 0);
        assert!(!applied.schedule_changed);
    }

    #[test]
    fn automatic_schedule_publication_rejects_a_stale_input_revision() {
        let proposed = domain_schedule("tenant");
        let mut state = StateMachineData::default();
        let applied = apply_consensus_command_at(
            &mut state,
            &ConsensusCommand::ApplyAutomaticDomainSchedule {
                fence: AutomaticScheduleFence {
                    leader_tenure: LeaderTenure {
                        leader_id: ClusterNodeName::parse("node-1")
                            .assured("the test node name is valid"),
                        term: 7,
                    },
                    input_revision: Some(40),
                },
                domain: domain("tenant"),
                expected_schedule: None,
                schedule: Some(Box::new(proposed)),
            },
            AppliedEntryContext {
                leader_term: 7,
                input_revision: Some(41),
            },
        );

        assert_eq!(
            applied.response,
            ConsensusResponse::Conflict(
                "automatic schedule decision input revision changed".to_string()
            )
        );
        assert_eq!(state.schedule.domains.len(), 0);
        assert!(!applied.schedule_changed);
    }

    #[test]
    fn automatic_schedule_publication_applies_a_current_fence() {
        let proposed = domain_schedule("tenant");
        let mut state = StateMachineData::default();
        let applied = apply_consensus_command_at(
            &mut state,
            &ConsensusCommand::ApplyAutomaticDomainSchedule {
                fence: AutomaticScheduleFence {
                    leader_tenure: LeaderTenure {
                        leader_id: ClusterNodeName::parse("node-1")
                            .assured("the test node name is valid"),
                        term: 7,
                    },
                    input_revision: Some(40),
                },
                domain: domain("tenant"),
                expected_schedule: None,
                schedule: Some(Box::new(proposed.clone())),
            },
            AppliedEntryContext {
                leader_term: 7,
                input_revision: Some(40),
            },
        );

        assert_eq!(applied.response, ConsensusResponse::Applied);
        assert_eq!(state.schedule.domain(&domain("tenant")), Some(&proposed));
        assert!(applied.schedule_changed);
    }

    #[test]
    fn runtime_revision_advances_only_for_runtime_state_changes() {
        let mut state = StateMachineData::default();
        let domain_change = apply_consensus_command(
            &mut state,
            &ConsensusCommand::PutDomain {
                domain: Box::new(running_domain_state("tenant")),
            },
        );
        state.record_runtime_revision(41, &domain_change);
        assert_eq!(state.runtime_revision, 41);

        let user_change = apply_consensus_command(
            &mut state,
            &ConsensusCommand::CreateUser {
                user: Box::new(UserCredentials {
                    name: UserName::parse("app_user").expect("valid user name"),
                    password_hash: "argon2-hash".to_string(),
                }),
            },
        );
        state.record_runtime_revision(42, &user_change);
        assert_eq!(state.runtime_revision, 41);

        let schedule_change = apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                domain: domain("tenant"),
                expected_schedule: None,
                schedule: Some(Box::new(domain_schedule("tenant"))),
            },
        );
        state.record_runtime_revision(43, &schedule_change);
        assert_eq!(state.runtime_revision, 43);
    }

    #[test]
    fn transaction_step_effect_and_progress_are_applied_once() {
        let owner = UserName::parse("app_user").expect("valid owner");
        let domain_id = domain("tenant");
        let mut stopped = running_domain_state("tenant");
        stopped.status = DomainStatus::Stopped;
        stopped.start_version = 0;
        let mut state = StateMachineData::default();
        state.domains.insert(domain_id.clone(), stopped);

        let transaction = ReplicatedTransaction::open(
            "tx-1".to_string(),
            domain_id.clone(),
            owner.clone(),
            nervix_models::Timestamp::from_unix_nanos(1),
        );
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::OpenTransaction {
                transaction: Box::new(transaction),
                max_open_transactions: 4,
            },
        );
        for (at, statement) in [
            Statement::StartDomain(nervix_models::StartDomain {
                start: DomainStartPoint::Resume,
            }),
            Statement::StopDomain(nervix_models::StopDomain),
        ]
        .into_iter()
        .enumerate()
        {
            apply_consensus_command(
                &mut state,
                &ConsensusCommand::QueueTransactionStatement {
                    id: "tx-1".to_string(),
                    owner: owner.clone(),
                    domain: domain_id.clone(),
                    at: nervix_models::Timestamp::from_unix_nanos(
                        i64::try_from(at)
                            .unwrap_or_default()
                            .checked_add(2)
                            .assured("the test clock counts from zero"),
                    ),
                    statement: Box::new(TransactionStatement {
                        source: "statement".to_string(),
                        statement,
                    }),
                    limits: TransactionQueueLimits {
                        max_statements: 4,
                        max_source_bytes: 1024,
                    },
                },
            );
        }
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::StartTransactionCommit {
                id: "tx-1".to_string(),
                owner,
                at: nervix_models::Timestamp::from_unix_nanos(4),
            },
        );

        let first_step = ConsensusCommand::AdvanceTransactionCommit {
            id: "tx-1".to_string(),
            expected_next_statement: 0,
            next_statement: 1,
            at: nervix_models::Timestamp::from_unix_nanos(5),
            result: Box::new(TransactionStepResult {
                first_statement: 0,
                statement_count: 1,
                quiesce_level: None,
                planned_relocations: None,
                result: TransactionCommandResult {
                    success: true,
                    message: "started".to_string(),
                    diagnostics: Vec::new(),
                    already_existed: false,
                },
            }),
            effect: Some(Box::new(TransactionStepEffect::StartDomain {
                domain_id: domain_id.clone(),
                expected_start_version: 0,
                start: DomainStartPoint::Resume,
                clock: None,
                authority: None,
            })),
            completion: None,
        };
        apply_consensus_command(&mut state, &first_step);
        let running = state.domains.get(&domain_id).expect("domain remains");
        assert_eq!(running.status, DomainStatus::Running);
        assert_eq!(running.start_version, 1);
        let transaction = state.transactions.get("tx-1").expect("transaction remains");
        assert_eq!(transaction.completed_statement_count(), 1);

        let duplicate = apply_consensus_command(&mut state, &first_step);
        let ConsensusResponse::Transaction(response) = duplicate.response else {
            panic!("duplicate transaction step must return a transaction response");
        };
        assert!(matches!(
            response.result,
            Err(TransactionMutationError::ProgressConflict { .. })
        ));
        assert_eq!(
            state
                .domains
                .get(&domain_id)
                .expect("domain remains")
                .start_version,
            1
        );

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdvanceTransactionCommit {
                id: "tx-1".to_string(),
                expected_next_statement: 1,
                next_statement: 2,
                at: nervix_models::Timestamp::from_unix_nanos(6),
                result: Box::new(TransactionStepResult {
                    first_statement: 1,
                    statement_count: 1,
                    quiesce_level: None,
                    planned_relocations: None,
                    result: TransactionCommandResult {
                        success: false,
                        message: "validation failed".to_string(),
                        diagnostics: Vec::new(),
                        already_existed: false,
                    },
                }),
                effect: None,
                completion: Some(TransactionOutcome::Failed {
                    failing_step: 1,
                    error: "validation failed".to_string(),
                }),
            },
        );
        let transaction = state.transactions.get("tx-1").expect("tombstone remains");
        assert!(matches!(
            transaction.finished_outcome(),
            Some(TransactionOutcome::Failed {
                failing_step: 1,
                ..
            })
        ));
        assert_eq!(transaction.commit_results().len(), 2);
    }

    #[test]
    fn pause_and_resume_preserve_domain_start_state() {
        let domain = domain("tenant");
        let original = running_domain_state("tenant");
        let mut state = StateMachineData::default();
        state.domains.insert(domain.clone(), original.clone());

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::PauseDomain {
                domain_id: domain.clone(),
            },
        );
        let paused = state.domains.get(&domain).expect("domain should remain");
        assert_eq!(paused.status, DomainStatus::Paused);
        assert_eq!(paused.start_version, original.start_version);
        assert_eq!(paused.last_start, original.last_start);

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ResumeDomain {
                domain_id: domain.clone(),
            },
        );
        let resumed = state.domains.get(&domain).expect("domain should remain");
        assert_eq!(resumed.status, DomainStatus::Running);
        assert_eq!(resumed.start_version, original.start_version);
        assert_eq!(resumed.last_start, original.last_start);
    }

    #[test]
    fn apply_consensus_command_tracks_resource_versions_and_replicas() {
        let mut state = StateMachineData {
            resources: ResourceRecords::default(),
            ..Default::default()
        };

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::CreateResourceCatalog {
                domain: domain("tenant"),
                identifier: ResourceName::parse("fraud_model").expect("valid resource name"),
            },
        );
        let upload_key = ResourceUploadKey::new(
            UserName::parse("uploader").expect("valid user name"),
            domain("tenant"),
            ResourceName::parse("fraud_model").expect("valid resource name"),
            ResourceUploadIdentity::parse("retry-one").expect("valid upload identity"),
        );
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::BeginResourceUpload {
                key: Box::new(upload_key.clone()),
            },
        );
        assert_eq!(
            ResourceVersionStatus::from(&state.resources)
                .next_version_by_resource
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![ResourceVersionCounter {
                domain: domain("tenant"),
                identifier: ResourceName::parse("fraud_model").expect("valid resource name"),
                next_version: 2,
            }]
        );
        assert!(
            !state.resources.is_declared(
                &domain("other"),
                &ResourceName::parse("fraud_model").expect("valid resource name")
            ),
            "a resource declared in one domain must not appear in another"
        );

        let version = resource_version("tenant", "fraud_model", 1);
        let replica = ResourceNodeStatus {
            key: ResourceReplicaKey::new(
                domain("tenant"),
                ResourceName::parse("fraud_model").expect("valid resource name"),
                1,
                ClusterNodeName::parse("node-2").expect("valid name"),
            ),
            state: ResourceNodeState::Ready,
            root_checksum: Some("root-1".to_string()),
            last_verified_at: Some(nervix_models::Timestamp::from_unix_nanos(77)),
            source_node_id: Some(ClusterNodeName::parse("node-1").expect("valid name")),
            error: None,
        };
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::PublishResourceUpload {
                key: Box::new(upload_key.clone()),
                resource: Box::new(version.clone()),
                replica: Box::new(replica.clone()),
            },
        );
        let resources = ResourceVersionStatus::from(&state.resources);
        assert_eq!(
            resources.versions.iter().cloned().collect::<Vec<_>>(),
            vec![version]
        );
        assert_eq!(
            resources.replicas.iter().cloned().collect::<Vec<_>>(),
            vec![replica]
        );
        let upload = resources
            .upload(&upload_key)
            .expect("the durable upload outcome should remain addressable");
        assert_eq!(upload.version, 1);
        assert_eq!(
            upload.state,
            ResourceUploadState::Published {
                root_checksum: "root-1".to_string(),
            }
        );

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::BeginResourceUpload {
                key: Box::new(upload_key),
            },
        );
        assert_eq!(
            ResourceVersionStatus::from(&state.resources).versions.len(),
            1,
            "reusing an upload identity must not allocate another version"
        );
    }

    #[test]
    fn apply_consensus_command_tracks_cordoned_nodes() {
        let mut state = StateMachineData::default();

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::SetNodeCordoned {
                node_id: ClusterNodeName::parse("node-2").expect("valid name"),
                cordoned: true,
            },
        );
        assert!(state.cordoned_node_ids.contains_key("node-2"));

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::SetNodeCordoned {
                node_id: ClusterNodeName::parse("node-2").expect("valid name"),
                cordoned: false,
            },
        );
        assert!(!state.cordoned_node_ids.contains_key("node-2"));
    }

    #[test]
    fn apply_consensus_command_tracks_users_without_overwriting_existing_password_hash() {
        let mut state = StateMachineData::default();
        let name = UserName::parse("app_user").expect("valid user name");

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::CreateUser {
                user: Box::new(UserCredentials {
                    name: name.clone(),
                    password_hash: "argon2-hash-v1".to_string(),
                }),
            },
        );
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::CreateUser {
                user: Box::new(UserCredentials {
                    name: name.clone(),
                    password_hash: "argon2-hash-v2".to_string(),
                }),
            },
        );

        assert_eq!(
            state
                .users
                .get(&name)
                .map(|user| user.password_hash.as_str()),
            Some("argon2-hash-v1")
        );
    }

    /// The handle `get_log_reader` returns reads the log and the vote and carries nothing else.
    ///
    /// `AmbiguousIfImpl` has one impl that covers every type and one per mutating storage trait.
    /// Only the blanket impl can apply to a reader, so the marker below infers to `()`. Were the
    /// reader ever to gain log storage or state machine authority, two impls would apply and
    /// inference here would fail.
    const _: fn() = || {
        fn reads_the_raft_log<T: RaftLogReader<TypeConfig>>() {}
        reads_the_raft_log::<FjallLogReader>();

        trait AmbiguousIfImpl<Marker> {
            fn probe() {}
        }
        impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
        impl<T: ?Sized + RaftLogStorage<TypeConfig>> AmbiguousIfImpl<u8> for T {}
        impl<T: ?Sized + RaftStateMachine<TypeConfig>> AmbiguousIfImpl<u16> for T {}

        let _ = <FjallLogReader as AmbiguousIfImpl<_>>::probe;
    };

    #[tokio::test]
    async fn log_reader_returns_the_requested_range_in_index_order() {
        let mut store =
            FjallStore::from_database(temp_database(), nervix_execution::Executor::default())
                .await
                .expect("store should open");
        let vote = VoteOf::new(4, ClusterNodeName::parse("node-1").expect("valid name"));
        RaftLogStorage::<TypeConfig>::save_vote(&mut store, &vote)
            .await
            .expect("vote should save");
        RaftLogStorageExt::<TypeConfig>::blocking_append(&mut store, blank_log_entries(4, 1..=5))
            .await
            .expect("entries should append");

        let mut reader = RaftLogStorage::<TypeConfig>::get_log_reader(&mut store).await;

        assert_eq!(read_log_indexes(&mut reader).await, vec![1, 2, 3, 4, 5]);
        assert_eq!(
            reader
                .try_get_log_entries(2..=4)
                .await
                .expect("bounded range should read")
                .iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(
            reader
                .try_get_log_entries(4..)
                .await
                .expect("open range should read")
                .iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>(),
            vec![4, 5]
        );
        assert!(
            reader
                .try_get_log_entries(6..9)
                .await
                .expect("range beyond the log should read")
                .is_empty()
        );
        assert_eq!(
            reader.read_vote().await.expect("vote should read"),
            Some(vote)
        );
    }

    #[tokio::test]
    async fn log_reader_observes_writes_the_storage_owner_makes() {
        let mut store =
            FjallStore::from_database(temp_database(), nervix_execution::Executor::default())
                .await
                .expect("store should open");
        let mut reader = RaftLogStorage::<TypeConfig>::get_log_reader(&mut store).await;
        assert!(
            reader
                .read_vote()
                .await
                .expect("vote should read")
                .is_none()
        );
        assert!(read_log_indexes(&mut reader).await.is_empty());

        let vote = VoteOf::new(2, ClusterNodeName::parse("node-1").expect("valid name"));
        RaftLogStorage::<TypeConfig>::save_vote(&mut store, &vote)
            .await
            .expect("vote should save");
        RaftLogStorageExt::<TypeConfig>::blocking_append(&mut store, blank_log_entries(2, 1..=6))
            .await
            .expect("entries should append");
        assert_eq!(
            reader.read_vote().await.expect("vote should read"),
            Some(vote)
        );
        assert_eq!(read_log_indexes(&mut reader).await, vec![1, 2, 3, 4, 5, 6]);

        let leader = committed_leader(2);
        RaftLogStorage::<TypeConfig>::truncate_after(
            &mut store,
            Some(LogIdOf::new(leader.clone(), 4)),
        )
        .await
        .expect("log should truncate");
        assert_eq!(read_log_indexes(&mut reader).await, vec![1, 2, 3, 4]);

        RaftLogStorage::<TypeConfig>::purge(&mut store, LogIdOf::new(leader, 2))
            .await
            .expect("log should purge");
        assert_eq!(read_log_indexes(&mut reader).await, vec![3, 4]);

        let state = RaftLogStorage::<TypeConfig>::get_log_state(&mut store)
            .await
            .expect("log state should read");
        assert_eq!(state.last_purged_log_id.map(|log_id| log_id.index), Some(2));
        assert_eq!(state.last_log_id.map(|log_id| log_id.index), Some(4));
    }
}

#[cfg(test)]
mod durability_tests {
    use openraft::{entry::EntryPayload, storage::RaftStateMachine as _, vote::RaftLeaderId as _};

    use super::*;

    #[tokio::test]
    async fn failed_application_does_not_publish_state_or_applied_position()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let database = Database::builder(directory.path()).open()?;
        let mut store =
            FjallStore::from_database(database, nervix_execution::Executor::default()).await?;
        let mut notifications = store.inner.resource_tx.subscribe();
        let domain = DomainName::try_from("durability")?;
        let identifier = ResourceName::parse("artifact")?;
        let command = ConsensusCommand::CreateResourceCatalog { domain, identifier };
        store
            .inner
            .faults
            .fail_next(command.to_string(), StorageBoundary::BeforeCommit);
        let entry = EntryOf::<TypeConfig> {
            log_id: LogIdOf::new(
                CommittedLeaderIdOf::<TypeConfig>::new(1, ClusterNodeName::parse("node-1")?),
                1,
            ),
            payload: EntryPayload::Normal(command),
        };
        let result = store
            .apply(futures_util::stream::iter([Ok((entry, None))]))
            .await;
        assert!(result.is_err());
        assert!(!notifications.has_changed()?);
        assert_eq!(store.applied_state().await?.0, None);
        assert_eq!(
            *store.inner.state_machine.read(),
            StateMachineData::default()
        );
        notifications.borrow_and_update();
        Ok(())
    }
}
