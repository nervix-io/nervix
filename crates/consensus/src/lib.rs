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
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::Transport;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, ClusterSchedule,
    DomainClockAuthority, DomainClockState, DomainName, DomainPace, DomainSchedule,
    DomainStartPoint, DomainState, DomainStatus, ResourceName, ResourceNodeStatus, ResourceVersion,
    ResourceVersionStatus, Statement, UserName,
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
    network::{RPCOption, RaftNetworkV2},
    type_config::{
        alias::{CommittedLeaderIdOf, EntryOf, LeaderIdOf},
        async_runtime::watch::WatchReceiver,
    },
};
use parking_lot::Mutex;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{
    sync::{RwLock, broadcast, watch},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tracing::{error, info};
use triomphe::Arc;

mod durable_batch;
mod records;
mod storage;
mod storage_fault;

use records::{Records, ResourceRecords, ScheduleRecords};
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
    AdvanceResourceVersion {
        domain: DomainName,
        identifier: ResourceName,
    },
    PutResourceVersion {
        resource: Box<ResourceVersion>,
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
            Self::AdvanceResourceVersion { domain, identifier } => {
                write!(
                    f,
                    "advance-resource-version:{}.{}",
                    domain.as_str(),
                    identifier.as_str()
                )
            }
            Self::PutResourceVersion { resource } => write!(
                f,
                "put-resource-version:{}.{}@{}",
                resource.id.domain.as_str(),
                resource.id.identifier.as_str(),
                resource.id.version
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
    Snapshot<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node, Cursor<Vec<u8>>>;

const HEARTBEAT_ERROR_REPORT_MIN_INTERVAL: Duration = Duration::from_secs(10);
/// How many consensus transitions a session can fall behind before the bus drops the oldest.
const CONSENSUS_EVENT_CAPACITY: usize = 256;
const SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;
static NEXT_SNAPSHOT_TRANSFER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct ConsensusSettings {
    pub cluster_name: String,
    pub node_id: ClusterNodeName,
    pub interconnect_advertise_addr: String,
    pub interconnect: Transport,
    pub executor: nervix_execution::Executor,
    pub node_unavailability_timeout: Duration,
    pub raft_heartbeat_interval: Duration,
    pub raft_election_timeout_min: Duration,
    pub raft_election_timeout_max: Duration,
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
}

#[derive(Debug, Clone)]
pub struct ConsensusRuntimeState {
    pub revision: u64,
    pub schedule: ClusterSchedule,
    pub domains: BTreeMap<DomainName, DomainState>,
    pub domain_clock_authorities: BTreeMap<DomainName, DomainClockAuthority>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredSnapshotData {
    meta: SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node>,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct PeerHealth {
    unavailable_since: Option<Instant>,
    last_reported_unavailable_at: Option<Instant>,
}

struct IncomingSnapshotTransfer {
    transfer_id: u64,
    vote: VoteOf,
    meta: SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node>,
    total_bytes: u64,
    snapshot: Vec<u8>,
}

struct CompletedSnapshotTransfer {
    vote: VoteOf,
    meta: SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node>,
    snapshot: Vec<u8>,
}

impl IncomingSnapshotTransfer {
    fn new(
        transfer_id: u64,
        vote: VoteOf,
        meta: SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node>,
        total_bytes: u64,
    ) -> Result<Self, Report<SnapshotTransferError>> {
        usize::try_from(total_bytes)
            .map_err(|_| Report::new(SnapshotTransferError::UnaddressableLength { total_bytes }))?;
        Ok(Self {
            transfer_id,
            vote,
            meta,
            total_bytes,
            snapshot: Vec::new(),
        })
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

    fn append_chunk(
        &mut self,
        transfer_id: u64,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<(), Report<SnapshotTransferError>> {
        self.verify_id(transfer_id)?;
        if bytes.len() > SNAPSHOT_CHUNK_BYTES {
            return Err(Report::new(SnapshotTransferError::ChunkTooLarge {
                actual: bytes.len(),
                limit: SNAPSHOT_CHUNK_BYTES,
            }));
        }
        let expected_offset = u64::try_from(self.snapshot.len())
            .assured("supported targets have a pointer width no larger than u64");
        if offset != expected_offset {
            return Err(Report::new(SnapshotTransferError::WrongOffset {
                expected: expected_offset,
                actual: offset,
            }));
        }
        let chunk_bytes = u64::try_from(bytes.len())
            .assured("supported targets have a pointer width no larger than u64");
        let end = expected_offset.checked_add(chunk_bytes).ok_or_else(|| {
            Report::new(SnapshotTransferError::ExceedsDeclaredLength {
                declared: self.total_bytes,
            })
        })?;
        if end > self.total_bytes {
            return Err(Report::new(SnapshotTransferError::ExceedsDeclaredLength {
                declared: self.total_bytes,
            }));
        }
        self.snapshot
            .try_reserve(bytes.len())
            .map_err(|_| Report::new(SnapshotTransferError::Allocation { requested: end }))?;
        self.snapshot.extend_from_slice(&bytes);
        Ok(())
    }

    fn complete(
        self,
        transfer_id: u64,
    ) -> Result<CompletedSnapshotTransfer, Report<SnapshotTransferError>> {
        self.verify_id(transfer_id)?;
        let actual = u64::try_from(self.snapshot.len())
            .assured("supported targets have a pointer width no larger than u64");
        if actual != self.total_bytes {
            return Err(Report::new(SnapshotTransferError::Incomplete {
                expected: self.total_bytes,
                actual,
            }));
        }
        Ok(CompletedSnapshotTransfer {
            vote: self.vote,
            meta: self.meta,
            snapshot: self.snapshot,
        })
    }
}

#[derive(Debug, Error)]
enum SnapshotTransferError {
    #[error("node '{peer}' has no active snapshot transfer")]
    Missing { peer: ClusterNodeName },
    #[error("snapshot transfer {actual} superseded transfer {expected}")]
    Superseded { expected: u64, actual: u64 },
    #[error("snapshot transfer declares an unaddressable {total_bytes}-byte length")]
    UnaddressableLength { total_bytes: u64 },
    #[error("snapshot chunk offset {actual} differs from expected offset {expected}")]
    WrongOffset { expected: u64, actual: u64 },
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
    node_unavailability_timeout: Duration,
    peer_health: RwLock<BTreeMap<ClusterNodeName, PeerHealth>>,
    incoming_snapshots: Mutex<BTreeMap<ClusterNodeName, IncomingSnapshotTransfer>>,
    events: ConsensusEvents,
    metrics_task: Mutex<Option<JoinHandle<()>>>,
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
        let config = StdArc::new(
            Config {
                cluster_name: settings.cluster_name,
                heartbeat_interval: u64::try_from(settings.raft_heartbeat_interval.as_millis())
                    .unwrap_or(u64::MAX),
                election_timeout_min: u64::try_from(settings.raft_election_timeout_min.as_millis())
                    .unwrap_or(u64::MAX),
                election_timeout_max: u64::try_from(settings.raft_election_timeout_max.as_millis())
                    .unwrap_or(u64::MAX),
                // A single consensus command may use the full interconnect command-byte budget.
                // Replicate one entry per request so catch-up cannot exceed that bounded payload.
                max_payload_entries: 1,
                snapshot_policy: openraft::SnapshotPolicy::Never,
                ..Default::default()
            }
            .validate()
            .map_err(|_| ConsensusError::Startup)?,
        );

        let network = NetworkFactory {
            interconnect: settings.interconnect.clone(),
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

        let consensus = Self {
            inner: Arc::new(ConsensusState {
                raft,
                store,
                local_node_id: settings.node_id,
                interconnect_advertise_addr: settings.interconnect_advertise_addr,
                interconnect: settings.interconnect,
                node_unavailability_timeout: settings.node_unavailability_timeout,
                peer_health: RwLock::new(BTreeMap::new()),
                incoming_snapshots: Mutex::new(BTreeMap::new()),
                events,
                metrics_task: Mutex::new(Some(metrics_task)),
            }),
        };
        consensus.register_protocol_handlers()?;
        Ok(consensus)
    }

    fn register_protocol_handlers(&self) -> Result<(), ConsensusError> {
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
            })
            .map_err(|_| ConsensusError::Startup)?;

        let receiver = self.protocol_receiver();
        self.inner
            .interconnect
            .register_handler::<wire::ReplicateRequest, _, _>(move |context, request| {
                let receiver = receiver.clone();
                async move {
                    validate_protocol_origin(
                        context.peer_node_id(),
                        request.0.origin_node_id(),
                        "replication",
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
            })
            .map_err(|_| ConsensusError::Startup)?;

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
            })
            .map_err(|_| ConsensusError::Startup)?;

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
                            transfer.total_bytes,
                        )
                        .map_err(wire::ConsensusRequestError::snapshot_transfer)
                }
            })
            .map_err(|_| ConsensusError::Startup)?;

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
                            request.offset,
                            request.bytes,
                        )
                        .map_err(wire::ConsensusRequestError::snapshot_transfer)
                }
            })
            .map_err(|_| ConsensusError::Startup)?;

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
            })
            .map_err(|_| ConsensusError::Startup)?;

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
            })
            .map_err(|_| ConsensusError::Startup)?;

        self.inner
            .interconnect
            .register_handler::<wire::HealthCheck, _, _>(|_, _| async {})
            .map_err(|_| ConsensusError::Startup)?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.inner.raft.shutdown().await.discarded(
            "openraft joins its core task inside this call and always answers Ok; the core's own \
             outcome is not exposed here",
        );
        let handle = self.inner.metrics_task.lock().take();
        if let Some(handle) = handle {
            handle.abort();
            // The abort makes a cancellation the expected outcome and it says nothing new. A panic
            // is the opposite: the metrics task died on its own and this join is the last place
            // that fact exists.
            if let Err(error) = handle.await
                && !error.is_cancelled()
            {
                error!(%error, "consensus metrics task panicked before shutdown could join it");
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
        lines.push(format!(
            "raft.node_unavailability_timeout: {:?}",
            self.inner.node_unavailability_timeout
        ));
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

    pub async fn replace_domain_schedule(
        &self,
        domain: DomainName,
        expected_schedule: Option<DomainSchedule>,
        schedule: Option<DomainSchedule>,
    ) -> Result<(), ConsensusError> {
        let response = self
            .inner
            .raft
            .client_write(ConsensusCommand::ReplaceDomainSchedule {
                domain,
                expected_schedule: expected_schedule.map(Box::new),
                schedule: schedule.map(Box::new),
            })
            .await
            .map_err(ConsensusError::from)?;
        match response.data {
            ConsensusResponse::Applied => Ok(()),
            ConsensusResponse::Conflict(reason) => Err(ConsensusError::Conflict(reason)),
            ConsensusResponse::Transaction(_) => Err(ConsensusError::UnexpectedResponse),
        }
    }

    pub async fn put_domain(&self, domain: DomainState) -> Result<(), ConsensusError> {
        self.inner
            .raft
            .client_write(ConsensusCommand::PutDomain {
                domain: Box::new(domain),
            })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
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
            .raft
            .client_write(ConsensusCommand::PutDomainAndSchedule {
                expected_domain: expected_domain.map(Box::new),
                expected_schedule: expected_schedule.map(Box::new),
                domain: Box::new(domain),
                schedule: schedule.map(Box::new),
            })
            .await
            .map_err(ConsensusError::from)?;
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
            .raft
            .client_write(ConsensusCommand::StartDomain {
                domain_id,
                start,
                clock,
                authority,
            })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
    }

    pub async fn stop_domain(&self, domain_id: DomainName) -> Result<(), ConsensusError> {
        self.inner
            .raft
            .client_write(ConsensusCommand::StopDomain { domain_id })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
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
            .raft
            .client_write(ConsensusCommand::ReconcileDomainClockAuthority {
                domain_id,
                expected_start_version,
                expected_authority,
                owner,
            })
            .await;
        match written {
            Ok(_) => Ok(()),
            Err(error) => Err(Report::new(ConsensusError::from(error))),
        }
    }

    pub async fn pause_domain(&self, domain_id: DomainName) -> Result<(), ConsensusError> {
        self.inner
            .raft
            .client_write(ConsensusCommand::PauseDomain { domain_id })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
    }

    pub async fn resume_domain(&self, domain_id: DomainName) -> Result<(), ConsensusError> {
        self.inner
            .raft
            .client_write(ConsensusCommand::ResumeDomain { domain_id })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
    }

    pub async fn create_user(&self, user: UserCredentials) -> Result<(), ConsensusError> {
        self.inner
            .raft
            .client_write(ConsensusCommand::CreateUser {
                user: Box::new(user),
            })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
    }

    pub async fn allocate_resource_version(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
    ) -> Result<u64, ConsensusError> {
        let resources = self.current_resources().await;
        if !resources.is_declared(domain, identifier) {
            return Err(ConsensusError::Write(format!(
                "resource '{}' does not exist in domain '{}'",
                identifier.as_str(),
                domain.as_str()
            )));
        }
        self.inner
            .raft
            .client_write(ConsensusCommand::AdvanceResourceVersion {
                domain: domain.clone(),
                identifier: identifier.clone(),
            })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)?;

        let resources = self.current_resources().await;
        Ok(resources.latest_version(domain, identifier).unwrap_or(1))
    }

    pub async fn create_resource_catalog(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
    ) -> Result<(), ConsensusError> {
        self.inner
            .raft
            .client_write(ConsensusCommand::CreateResourceCatalog {
                domain: domain.clone(),
                identifier: identifier.clone(),
            })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
    }

    pub async fn put_resource_version(
        &self,
        resource: ResourceVersion,
    ) -> Result<(), ConsensusError> {
        self.inner
            .raft
            .client_write(ConsensusCommand::PutResourceVersion {
                resource: Box::new(resource),
            })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
    }

    pub async fn put_resource_replica(
        &self,
        replica: ResourceNodeStatus,
    ) -> Result<(), ConsensusError> {
        self.inner
            .raft
            .client_write(ConsensusCommand::PutResourceReplica {
                replica: Box::new(replica),
            })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
    }

    pub async fn set_node_cordoned(
        &self,
        node_id: ClusterNodeName,
        cordoned: bool,
    ) -> Result<(), ConsensusError> {
        self.inner
            .raft
            .client_write(ConsensusCommand::SetNodeCordoned { node_id, cordoned })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
    }

    async fn write_transaction(
        &self,
        command: ConsensusCommand,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        let response = self
            .inner
            .raft
            .client_write(command)
            .await
            .map_err(ConsensusError::from)?;
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
            .raft
            .client_write(ConsensusCommand::RemoveFinishedTransactions { finished_before })
            .await
            .map(|_| ())
            .map_err(ConsensusError::from)
    }
}

impl Administrator {
    pub fn observer(&self) -> Observer {
        Observer {
            inner: self.inner.clone(),
        }
    }

    pub async fn maybe_initialize(&self) -> Result<bool, ConsensusError> {
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
        let leader = self.inner.raft.current_leader().await;
        if leader.as_ref() != Some(&self.inner.local_node_id) {
            return Ok(());
        }

        let metrics = self.inner.raft.metrics().borrow_watched().clone();
        let current_voters = metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect::<BTreeSet<_>>();
        let mut desired_voters = current_voters.clone();
        let mut added_learner = false;

        for node in gossip.admission_candidates() {
            tokio::task::consume_budget().await;
            if node.interconnect_advertise_addr.is_empty() {
                continue;
            }

            let known_node = metrics
                .membership_config
                .membership()
                .get_node(&node.node_id)
                .cloned();
            if known_node.is_none()
                || known_node.as_ref().map(|known| &known.addr)
                    != Some(&node.interconnect_advertise_addr)
            {
                let add_message = if known_node.is_some() {
                    format!(
                        "raft refreshing learner {} address to {}",
                        node.node_id, node.interconnect_advertise_addr
                    )
                } else {
                    format!(
                        "raft adding learner {} at {}",
                        node.node_id, node.interconnect_advertise_addr
                    )
                };
                self.inner.events.report(add_message);
                self.inner
                    .raft
                    .add_learner(
                        node.node_id.clone(),
                        BasicNode::new(node.interconnect_advertise_addr.clone()),
                        true,
                    )
                    .await
                    .map_err(|_| ConsensusError::Transport)?;
                added_learner = true;
            }

            desired_voters.insert(node.node_id.clone());
        }

        let membership_nodes = metrics
            .membership_config
            .nodes()
            .map(|(node_id, node)| (node_id.clone(), node.addr.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut unavailable = Vec::new();
        for node_id in current_voters.iter() {
            tokio::task::consume_budget().await;
            if node_id == &self.inner.local_node_id {
                continue;
            }

            let Some(_) = membership_nodes.get(node_id) else {
                continue;
            };

            let chitchat_unavailable = gossip.dead_node_ids.contains(node_id);
            let healthcheck_unavailable = self.ping_peer(node_id).await.is_err();
            unavailable.push((
                node_id.clone(),
                chitchat_unavailable || healthcheck_unavailable,
            ));
        }

        {
            let mut peer_health = self.inner.peer_health.write().await;

            for (node_id, is_unavailable) in unavailable {
                let entry = peer_health.entry(node_id.clone()).or_insert(PeerHealth {
                    unavailable_since: None,
                    last_reported_unavailable_at: None,
                });

                if is_unavailable {
                    let unavailable_since =
                        entry.unavailable_since.get_or_insert_with(Instant::now);
                    let should_report = unavailable_since.elapsed()
                        >= self.inner.node_unavailability_timeout
                        && entry.last_reported_unavailable_at.is_none_or(|last| {
                            last.elapsed() >= HEARTBEAT_ERROR_REPORT_MIN_INTERVAL
                        });
                    if should_report {
                        let message = format!(
                            "raft peer {} remains unavailable for {:?} (threshold {:?})",
                            node_id,
                            unavailable_since.elapsed(),
                            self.inner.node_unavailability_timeout
                        );
                        self.inner.events.report(message);
                        entry.last_reported_unavailable_at = Some(Instant::now());
                    }
                } else {
                    entry.unavailable_since = None;
                    entry.last_reported_unavailable_at = None;
                }
            }

            peer_health.retain(|node_id, _| {
                current_voters.contains(node_id) || desired_voters.contains(node_id)
            });
        }

        if !added_learner && desired_voters == current_voters {
            return Ok(());
        }

        self.inner
            .raft
            .change_membership(desired_voters.clone(), true)
            .await
            .map_err(|_| ConsensusError::Transport)?;
        let after = self
            .inner
            .raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .voter_ids()
            .collect::<BTreeSet<_>>();
        if current_voters != after {
            self.inner
                .events
                .report(format!("raft membership updated: {after:?}"));
        }
        Ok(())
    }

    async fn ping_peer(&self, node_id: &ClusterNodeName) -> Result<(), ConsensusError> {
        self.inner
            .interconnect
            .request(node_id, wire::HealthCheck)
            .await
            .map_err(|_| ConsensusError::Transport)
    }

    pub async fn drop_node(&self, node_id: &ClusterNodeName) -> Result<(), ConsensusError> {
        if *node_id == self.inner.local_node_id {
            return Err(ConsensusError::RemoveLocalLeader(node_id.to_string()));
        }

        let metrics = self.inner.raft.metrics().borrow_watched().clone();
        let member_ids = metrics
            .membership_config
            .nodes()
            .map(|(member_id, _)| member_id.clone())
            .collect::<BTreeSet<_>>();
        if !member_ids.contains(node_id) {
            return Err(ConsensusError::NodeNotFound(node_id.to_string()));
        }

        let mut desired_voters = metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect::<BTreeSet<_>>();
        let was_voter = desired_voters.remove(node_id);
        if was_voter && desired_voters.is_empty() {
            return Err(ConsensusError::RemoveLastVoter(node_id.to_string()));
        }

        let membership_change_timeout = self
            .inner
            .node_unavailability_timeout
            .max(Duration::from_secs(5))
            * 2;
        timeout(
            membership_change_timeout,
            self.inner
                .raft
                .change_membership(desired_voters.clone(), false),
        )
        .await
        .map_err(|_| ConsensusError::MembershipChangeTimeout {
            operation: format!("remove node '{node_id}'"),
            timeout: membership_change_timeout,
        })?
        .map_err(|error| {
            if let RaftError::APIError(ClientWriteError::ForwardToLeader(_)) = error {
                ConsensusError::from(error)
            } else {
                ConsensusError::Transport
            }
        })?;

        self.inner
            .events
            .report(format!("raft node removed: {node_id}"));
        Ok(())
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
        total_bytes: u64,
    ) -> Result<(), Report<SnapshotTransferError>> {
        let transfer = IncomingSnapshotTransfer::new(transfer_id, vote, meta, total_bytes)?;
        self.inner.incoming_snapshots.lock().insert(peer, transfer);
        Ok(())
    }

    fn append_snapshot_chunk(
        &self,
        peer: &ClusterNodeName,
        transfer_id: u64,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<(), Report<SnapshotTransferError>> {
        let mut transfers = self.inner.incoming_snapshots.lock();
        let transfer = transfers
            .get_mut(peer)
            .ok_or_else(|| Report::new(SnapshotTransferError::Missing { peer: peer.clone() }))?;
        transfer.append_chunk(transfer_id, offset, bytes)
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
        self.install_full_snapshot(transfer.vote, transfer.meta, transfer.snapshot)
            .await
            .map_err(|error| Report::new(SnapshotTransferError::Install(error.to_string())))
    }

    async fn install_full_snapshot(
        &self,
        vote: VoteOf,
        meta: openraft::SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node>,
        snapshot: Vec<u8>,
    ) -> Result<SnapshotResponse<TypeConfig>, openraft::error::Fatal<TypeConfig>> {
        self.inner
            .raft
            .install_full_snapshot(
                vote,
                Snapshot {
                    meta,
                    snapshot: Cursor::new(snapshot),
                },
            )
            .await
    }
}

#[derive(Clone)]
struct NetworkFactory {
    interconnect: Transport,
}

#[derive(Clone)]
struct NetworkClient {
    target: ClusterNodeName,
    interconnect: Transport,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: ClusterNodeName, _node: &Node) -> Self::Network {
        Self::Network {
            target,
            interconnect: self.interconnect.clone(),
        }
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
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let rpc_timeout = option.hard_ttl();
        let record = wire::AppendEntriesRecord::from_request(rpc);
        let response = if record.is_heartbeat() {
            self.interconnect
                .request_with_timeout(&self.target, wire::HeartbeatRequest(record), rpc_timeout)
                .await
        } else {
            self.interconnect
                .request_with_timeout(&self.target, wire::ReplicateRequest(record), rpc_timeout)
                .await
        }
        .map_err(io_error)
        .map_err(unreachable_err)?
        .map_err(io_error)
        .map_err(unreachable_err)?;
        Ok(response.into_response())
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

    async fn full_snapshot(
        &mut self,
        vote: VoteOf,
        snapshot: SnapshotOf,
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
        let snapshot = snapshot.into_inner();
        let total_bytes = u64::try_from(snapshot.len())
            .assured("supported targets have a pointer width no larger than u64");
        let transfer = async {
            self.interconnect
                .request_with_timeout(
                    &self.target,
                    wire::BeginSnapshotTransfer::from_parts(transfer_id, vote, meta, total_bytes),
                    snapshot_request_timeout(deadline).map_err(unreachable_err)?,
                )
                .await
                .map_err(io_error)
                .map_err(unreachable_err)?
                .map_err(io_error)
                .map_err(unreachable_err)?;

            let mut offset = 0_u64;
            for chunk in snapshot.chunks(SNAPSHOT_CHUNK_BYTES) {
                tokio::task::consume_budget().await;
                self.interconnect
                    .request_with_timeout(
                        &self.target,
                        wire::SnapshotChunk {
                            transfer_id,
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
                let chunk_bytes = u64::try_from(chunk.len())
                    .assured("supported targets have a pointer width no larger than u64");
                offset = offset
                    .checked_add(chunk_bytes)
                    .assured("snapshot chunks are slices of one Vec whose length fits in u64");
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

fn apply_consensus_command(
    state: &mut StateMachineData,
    command: &ConsensusCommand,
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
        ConsensusCommand::AdvanceResourceVersion { domain, identifier } => {
            state.resources.advance_version(domain, identifier);
            changes.resources_changed = true;
        }
        ConsensusCommand::PutResourceVersion { resource } => {
            state
                .resources
                .versions
                .insert(resource.id.clone(), resource.as_ref().clone());
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
    use std::{collections::BTreeSet, ops::RangeInclusive};

    use fjall::Database;
    use meticulous::OptionExt as _;
    use nervix_models::{
        ClusterNodeIdentity, ClusterNodeIncarnation, DomainClockAuthority, DomainClockState,
        DomainConfig, DomainName, DomainPace, DomainSchedule, DomainStartPoint, DomainState,
        DomainStatus, DomainTimeRate, ResourceId, ResourceName, ResourceNodeState,
        ResourceNodeStatus, ResourceReplicaKey, ResourceVersion, ResourceVersionCounter,
        ResourceVersionStatus, Statement, Timestamp,
    };
    use openraft::{
        entry::RaftEntry,
        storage::{RaftLogReader, RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
        type_config::alias::{CommittedLeaderIdOf, EntryOf, LeaderIdOf},
        vote::RaftLeaderIdExt,
    };
    use tempfile::tempdir;

    use super::{
        ClusterSchedule, ConsensusCommand, ConsensusResponse, FjallLogReader, FjallStore,
        GossipNode, GossipState, ProtocolOriginError, ResourceRecords, StateMachineChanges,
        StateMachineData, TransactionCommandResult, TransactionMutationError, TransactionOutcome,
        TransactionStatement, TransactionStepEffect, TransactionStepResult, TypeConfig,
        UserCredentials, apply_consensus_command, apply_transaction_step_effect, io_error,
        storage_decode, validate_protocol_origin,
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
            &ConsensusCommand::AdvanceResourceVersion {
                domain: domain("tenant"),
                identifier: ResourceName::parse("fraud_model").expect("valid resource name"),
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
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::PutResourceVersion {
                resource: Box::new(version.clone()),
            },
        );
        assert_eq!(
            state
                .resources
                .versions
                .values()
                .cloned()
                .collect::<Vec<_>>(),
            vec![version.clone()]
        );

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
            &ConsensusCommand::PutResourceReplica {
                replica: Box::new(replica.clone()),
            },
        );
        assert_eq!(
            state
                .resources
                .replicas
                .values()
                .cloned()
                .collect::<Vec<_>>(),
            vec![replica]
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
