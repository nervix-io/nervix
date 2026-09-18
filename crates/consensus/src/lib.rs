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
    future::Future,
    io,
    path::{Path, PathBuf},
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
    CoordinationIdentity, DomainClockAuthority, DomainClockState, DomainName, DomainSchedule,
    DomainStartPoint, DomainState, DomainStatus, NodeEndpoint, NodeServiceUrl, ResourceId,
    ResourceName, ResourceNodeStatus, ResourceUpload, ResourceUploadKey, ResourceVersion,
    ResourceVersionStatus, Statement, TransactionImpactReport, UserName,
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
    raft::{
        ReadPolicy,
        linearizable_read::{Linearizer, ReadLogId},
    },
    type_config::{
        alias::{CommittedLeaderIdOf, EntryOf, LeaderIdOf, WatchReceiverOf},
        async_runtime::watch::WatchReceiver,
    },
};
use parking_lot::Mutex;
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Place, Serialize as RkyvSerialize,
    rancor::Fallible,
    ser::{Allocator, Writer},
    vec::{ArchivedVec, VecResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};
use serde::{Deserialize, Serialize};
use sorted_vec::SortedSet;
use thiserror::Error;
use tokio::{
    sync::{Mutex as AsyncMutex, broadcast, watch},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tracing::{error, info};
use triomphe::Arc;

mod command_execution;
mod connectivity_fault;
mod domain_mutation;
mod durable_batch;
mod raft_record;
mod records;
mod replication;
mod retention;
mod snapshot;
pub use command_execution::{
    CommandExecution, CommandExecutionChildResult, CommandExecutionDiagnostic,
    CommandExecutionEffect, CommandExecutionResult, CommandExecutionResultKind,
    CommandExecutionState, CommandExecutionTransactionStatus,
};
pub use domain_mutation::{DomainMutationLease, DomainMutationOwner, DomainMutationRecoveryFence};
pub use retention::RaftRetentionPolicy;
pub use snapshot::{SealedSnapshot, SnapshotRetention};
mod storage;
mod storage_fault;
mod transaction_plan;
mod transaction_report;

use records::{Records, ResourceRecords, ScheduleRecords};
use replication::AppendPath;
#[cfg(test)]
use storage::FjallLogReader;
use storage::FjallStore;
mod transaction;
use connectivity_fault::ConnectivityFault;
use domain_mutation::{DomainMutationAdmission, DomainMutationError};
#[cfg(any(test, feature = "testing"))]
pub use storage_fault::{StorageBoundary, StorageFault, StoragePause};
use transaction_report::TransactionReportRecords;
mod wire;

pub use transaction::{
    FinishedTransaction, ReplicatedTransaction, TransactionActivity, TransactionApplyingStep,
    TransactionCommandResult, TransactionCommitAdmissionFailure, TransactionCommitAdvance,
    TransactionCommitPlan, TransactionCommitPlanHeader, TransactionCommitPlanStep,
    TransactionCommitProgress, TransactionCommitStepKind, TransactionDiagnostic,
    TransactionEntityGatePlan, TransactionModelTransition, TransactionMutationError,
    TransactionMutationResponse, TransactionOutcome, TransactionQueueAdmission,
    TransactionQueueLimits, TransactionQueueRequest, TransactionState, TransactionStatement,
    TransactionStatementRequest, TransactionStepEffect, TransactionStepResult,
};
pub use transaction_plan::{
    FrozenTransactionCommitStep, TransactionCommitAdmissionPlan, TransactionCommitPlanBuildError,
    TransactionCommitPlanReadError, TransactionCommitPlanStoreError,
    TransactionScheduleEligibility,
};
pub use transaction_report::{
    TransactionReportArchive, TransactionReportArchiveError, TransactionReportReadError,
    TransactionReportStoreError,
};

/// A sorted set archived as a vector so vocabulary types need no second archived ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[serde(transparent)]
struct PlanningInputSet<T: Ord>(#[rkyv(with = PlanningInputSetAsVec)] SortedSet<T>);

#[derive(Debug)]
struct PlanningInputSetAsVec;

impl<T> ArchiveWith<SortedSet<T>> for PlanningInputSetAsVec
where
    T: Archive + Ord,
{
    type Archived = ArchivedVec<T::Archived>;
    type Resolver = VecResolver;

    fn resolve_with(field: &SortedSet<T>, resolver: Self::Resolver, out: Place<Self::Archived>) {
        ArchivedVec::resolve_from_len(field.len(), resolver, out);
    }
}

impl<T, S> SerializeWith<SortedSet<T>, S> for PlanningInputSetAsVec
where
    T: RkyvSerialize<S> + Ord,
    S: Fallible + Allocator + Writer + ?Sized,
{
    fn serialize_with(
        field: &SortedSet<T>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, S::Error> {
        ArchivedVec::<T::Archived>::serialize_from_iter::<T, _, _>(field.iter(), serializer)
    }
}

impl<T, D> DeserializeWith<ArchivedVec<T::Archived>, SortedSet<T>, D> for PlanningInputSetAsVec
where
    T: Archive + Ord,
    T::Archived: RkyvDeserialize<T, D>,
    D: Fallible + ?Sized,
{
    fn deserialize_with(
        field: &ArchivedVec<T::Archived>,
        deserializer: &mut D,
    ) -> Result<SortedSet<T>, D::Error> {
        let mut values = SortedSet::new();
        for archived in field.iter() {
            values.find_or_insert(archived.deserialize(deserializer)?);
        }
        Ok(values)
    }
}

impl<T: Ord> PlanningInputSet<T> {
    fn as_slice(&self) -> &[T] {
        &self.0
    }

    fn contains(&self, value: &T) -> bool {
        self.0.contains(value)
    }
}

impl<T: Ord> FromIterator<T> for PlanningInputSet<T> {
    fn from_iter<I: IntoIterator<Item = T>>(values: I) -> Self {
        Self(values.into_iter().collect())
    }
}

impl<'de, T> Deserialize<'de> for PlanningInputSet<T>
where
    T: Deserialize<'de> + Ord,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let values = Vec::<T>::deserialize(deserializer)?;
        Ok(Self(values.into_iter().collect()))
    }
}

/// The resource inputs from one domain that can affect a control-plane plan.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainResourcePlanningInputs {
    resources: PlanningInputSet<ResourceName>,
    completed_versions: PlanningInputSet<ResourceId>,
}

impl DomainResourcePlanningInputs {
    pub fn resources(&self) -> &[ResourceName] {
        self.resources.as_slice()
    }

    pub fn completed_versions(&self) -> &[ResourceId] {
        self.completed_versions.as_slice()
    }
}

/// Membership and operator eligibility inputs consumed by schedule decisions.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ScheduleTopologyInputs {
    members: PlanningInputSet<ClusterNodeName>,
    voters: PlanningInputSet<ClusterNodeName>,
    cordoned: PlanningInputSet<ClusterNodeName>,
}

impl ScheduleTopologyInputs {
    pub fn members(&self) -> &[ClusterNodeName] {
        self.members.as_slice()
    }

    pub fn voters(&self) -> &[ClusterNodeName] {
        self.voters.as_slice()
    }

    pub fn cordoned(&self) -> &[ClusterNodeName] {
        self.cordoned.as_slice()
    }
}

/// Domain-owned authoritative inputs captured together for one plan.
///
/// Values are compared directly at the replicated apply boundary. There is deliberately no Raft
/// log revision: writes that do not change these inputs cannot make an otherwise current plan
/// stale.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainPlanningInputs {
    domain: DomainName,
    state: Option<Box<DomainState>>,
    resources: DomainResourcePlanningInputs,
    schedule: Option<Box<DomainSchedule>>,
    topology: ScheduleTopologyInputs,
}

impl DomainPlanningInputs {
    pub fn domain(&self) -> &DomainName {
        &self.domain
    }

    pub fn state(&self) -> Option<&DomainState> {
        self.state.as_deref()
    }

    pub fn resources(&self) -> &DomainResourcePlanningInputs {
        &self.resources
    }

    pub fn schedule(&self) -> Option<&DomainSchedule> {
        self.schedule.as_deref()
    }

    pub fn topology(&self) -> &ScheduleTopologyInputs {
        &self.topology
    }

    /// Derive the authoritative state a plan expects after its own pause transition.
    pub fn after_domain_pause(mut self) -> Self {
        if let Some(state) = self.state.as_deref_mut() {
            state.status = DomainStatus::Paused;
        }
        self
    }

    /// Derive the authoritative inputs expected after this plan publishes a schedule.
    pub fn after_schedule(mut self, schedule: Option<DomainSchedule>) -> Self {
        self.schedule = schedule.map(Box::new);
        self
    }

    /// Derive the authoritative inputs expected after this plan replaces domain state and schedule.
    pub fn after_domain_update(
        mut self,
        state: DomainState,
        schedule: Option<DomainSchedule>,
    ) -> Self {
        self.state = Some(Box::new(state));
        self.schedule = schedule.map(Box::new);
        self
    }

    /// Derive the authoritative inputs expected after this plan creates a resource catalog entry.
    pub fn after_resource_catalog(mut self, resource: ResourceName) -> Self {
        self.resources.resources.0.find_or_insert(resource);
        self
    }
}

/// Domain-owned authoritative inputs and the detailed resource state captured from one
/// state-machine read for transaction planning.
#[derive(Debug, Clone)]
pub struct TransactionControlSnapshot {
    pub planning_inputs: DomainPlanningInputs,
    pub resources: ResourceVersionStatus,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ConsensusCommand {
    AdmitCommandExecution {
        execution: Box<CommandExecution>,
        mutation_domains: BTreeSet<DomainName>,
    },
    AcquireCommandDomainMutation {
        reference: nervix_models::CommandExecutionReference,
        owner: UserName,
        request_digest: [u8; 32],
        domain: DomainName,
    },
    FinishCommandExecution {
        reference: nervix_models::CommandExecutionReference,
        owner: UserName,
        request_digest: [u8; 32],
        at: nervix_models::Timestamp,
        result: Box<CommandExecutionResult>,
    },
    ExpireCommandExecutions {
        finished_before: nervix_models::Timestamp,
        at: nervix_models::Timestamp,
    },
    ReplaceDomainSchedule {
        inputs: Box<DomainPlanningInputs>,
        schedule: Option<Box<DomainSchedule>>,
        mutation: Option<Box<DomainMutationLease>>,
    },
    UpdateKafkaPartitionSchedule {
        inputs: Box<DomainPlanningInputs>,
        schedule: Box<DomainSchedule>,
    },
    ApplyAutomaticDomainSchedule {
        fence: AutomaticScheduleFence,
        inputs: Box<DomainPlanningInputs>,
        schedule: Option<Box<DomainSchedule>>,
    },
    /// Orders ownership-handoff reconciliation after every schedule proposal inherited by this
    /// leader. The command intentionally changes no model; its committed log position is the
    /// authority participants wait to apply before classifying durable preparations.
    ReconcileOwnershipHandoffPreparations {
        authority: CoordinationIdentity,
    },
    PutDomainAndSchedule {
        inputs: Box<DomainPlanningInputs>,
        domain: Box<DomainState>,
        schedule: Option<Box<DomainSchedule>>,
        mutation: Option<Box<DomainMutationLease>>,
    },
    PutDomain {
        domain: Box<DomainState>,
        mutation: Option<Box<DomainMutationLease>>,
    },
    StartDomain {
        domain_id: DomainName,
        start: DomainStartPoint,
        clock: Option<DomainClockState>,
        authority: Option<ClusterNodeIdentity>,
        mutation: Option<Box<DomainMutationLease>>,
    },
    StopDomain {
        domain_id: DomainName,
        mutation: Option<Box<DomainMutationLease>>,
    },
    PauseDomain {
        domain_id: DomainName,
        mutation: Option<Box<DomainMutationLease>>,
    },
    ResumeDomain {
        domain_id: DomainName,
        mutation: Option<Box<DomainMutationLease>>,
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
        root_checksum: String,
    },
    PublishResourceUpload {
        key: Box<ResourceUploadKey>,
        resource: Box<ResourceVersion>,
        replica: Box<ResourceNodeStatus>,
    },
    PutResourceReplica {
        replica: Box<ResourceNodeStatus>,
    },
    CompleteResourceUpload {
        key: Box<ResourceUploadKey>,
    },
    FailResourceUpload {
        key: Box<ResourceUploadKey>,
        reason: String,
    },
    SetNodeCordoned {
        node_id: ClusterNodeName,
        cordoned: bool,
    },
    FenceNodeAdmission {
        identity: ClusterNodeIdentity,
    },
    OpenTransaction {
        transaction: Box<ReplicatedTransaction>,
        max_open_transactions: usize,
    },
    QueueTransactionStatement {
        id: String,
        owner: UserName,
        domain: DomainName,
        activity: TransactionActivity,
        statement: Box<TransactionStatement>,
        report: Box<TransactionReportArchive>,
        limits: TransactionQueueLimits,
    },
    TouchTransaction {
        id: String,
        owner: UserName,
        activity: TransactionActivity,
    },
    StartTransactionCommit {
        id: String,
        owner: UserName,
        activity: TransactionActivity,
        expected_preview: nervix_models::TransactionPreviewIdentity,
        report: Box<TransactionReportArchive>,
        plan: Box<TransactionCommitAdmissionPlan>,
    },
    FailTransactionCommitAdmission {
        failure: Box<TransactionCommitAdmissionFailure>,
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
    CompleteTransactionApplication {
        id: String,
        expected_next_statement: usize,
        at: nervix_models::Timestamp,
        application_failure: Option<String>,
    },
    FinishEmptyTransactionCommit {
        id: String,
        at: nervix_models::Timestamp,
    },
    RevertTransaction {
        id: String,
        owner: UserName,
        activity: TransactionActivity,
    },
    ExpireTransaction {
        id: String,
        at: nervix_models::Timestamp,
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
            Self::AdmitCommandExecution { execution, .. } => {
                write!(f, "admit-command-execution:{}", execution.reference)
            }
            Self::AcquireCommandDomainMutation {
                reference, domain, ..
            } => write!(
                f,
                "acquire-command-domain-mutation:{reference}:{}",
                domain.as_str()
            ),
            Self::FinishCommandExecution { reference, .. } => {
                write!(f, "finish-command-execution:{reference}")
            }
            Self::ExpireCommandExecutions { .. } => f.write_str("expire-command-executions"),
            Self::ReplaceDomainSchedule {
                inputs, schedule, ..
            } => {
                if schedule.is_some() {
                    write!(f, "replace-domain-schedule:{}", inputs.domain().as_str())
                } else {
                    write!(f, "clear-domain-schedule:{}", inputs.domain().as_str())
                }
            }
            Self::UpdateKafkaPartitionSchedule { inputs, .. } => {
                write!(
                    f,
                    "update-kafka-partition-schedule:{}",
                    inputs.domain().as_str()
                )
            }
            Self::ApplyAutomaticDomainSchedule {
                inputs, schedule, ..
            } => {
                if schedule.is_some() {
                    write!(
                        f,
                        "apply-automatic-domain-schedule:{}",
                        inputs.domain().as_str()
                    )
                } else {
                    write!(
                        f,
                        "clear-automatic-domain-schedule:{}",
                        inputs.domain().as_str()
                    )
                }
            }
            Self::ReconcileOwnershipHandoffPreparations { authority } => {
                write!(f, "reconcile-ownership-handoff-preparations:{authority}")
            }
            Self::PutDomainAndSchedule { domain, .. } => {
                write!(f, "put-domain-and-schedule:{}", domain.id.as_str())
            }
            Self::PutDomain { domain, .. } => write!(f, "put-domain:{}", domain.id.as_str()),
            Self::StartDomain { domain_id, .. } => write!(f, "start-domain:{}", domain_id.as_str()),
            Self::StopDomain { domain_id, .. } => write!(f, "stop-domain:{}", domain_id.as_str()),
            Self::PauseDomain { domain_id, .. } => write!(f, "pause-domain:{}", domain_id.as_str()),
            Self::ResumeDomain { domain_id, .. } => {
                write!(f, "resume-domain:{}", domain_id.as_str())
            }
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
            Self::BeginResourceUpload { key, .. } => {
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
                replica.key.node
            ),
            Self::CompleteResourceUpload { key } => {
                write!(f, "complete-resource-upload:{}", key.identity)
            }
            Self::FailResourceUpload { key, .. } => {
                write!(f, "fail-resource-upload:{}", key.identity)
            }
            Self::SetNodeCordoned { node_id, cordoned } => {
                if *cordoned {
                    write!(f, "cordon-node:{node_id}")
                } else {
                    write!(f, "uncordon-node:{node_id}")
                }
            }
            Self::FenceNodeAdmission { identity } => {
                write!(f, "fence-node-admission:{identity}")
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
            Self::FailTransactionCommitAdmission { failure } => {
                write!(f, "fail-transaction-commit-admission:{}", failure.id)
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
            Self::CompleteTransactionApplication {
                id,
                expected_next_statement,
                ..
            } => write!(
                f,
                "complete-transaction-application:{id}:{expected_next_statement}"
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
    pub interconnect_advertise_addr: NodeEndpoint,
    pub interconnect: Transport,
    pub executor: nervix_execution::Executor,
    pub raft_heartbeat_interval: Duration,
    pub raft_election_timeout_min: Duration,
    pub raft_election_timeout_max: Duration,
    pub raft_retention: RaftRetentionPolicy,
}

/// One node as cluster discovery currently describes it.
///
/// Each advertised endpoint is present only once the node has published a value this build accepts.
/// Discovery converges field by field, so a node can be seen before any of them arrives, and a node
/// whose interconnect endpoint is still unavailable is never a membership admission candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipNode {
    pub node_id: ClusterNodeName,
    pub incarnation: ClusterNodeIncarnation,
    pub terminating: bool,
    /// Where clients reach this node's session service.
    pub client_url: Option<NodeServiceUrl>,
    /// Where operators reach this node's web console.
    pub console_url: Option<NodeServiceUrl>,
    /// Where peers reach this node's interconnect listener.
    pub interconnect_endpoint: Option<NodeEndpoint>,
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
        self.latest_admission_candidates()
            .into_values()
            .map(|node| node.identity())
            .collect()
    }

    pub fn live_node_ids(&self) -> BTreeSet<ClusterNodeName> {
        self.latest_admission_candidates().into_keys().collect()
    }

    pub fn placement_candidate_node_ids(&self) -> BTreeSet<ClusterNodeName> {
        let current = self.latest_admission_candidates();
        let mut candidates = BTreeSet::new();
        for (node_id, node) in current {
            if !node.terminating {
                candidates.insert(node_id);
            }
        }
        candidates
    }

    pub fn latest_nodes_by_id(&self) -> BTreeMap<ClusterNodeName, GossipNode> {
        self.latest_nodes(self.live_nodes.iter())
    }

    fn latest_nodes<'a>(
        &self,
        nodes: impl IntoIterator<Item = &'a GossipNode>,
    ) -> BTreeMap<ClusterNodeName, GossipNode> {
        let mut current = BTreeMap::<ClusterNodeName, GossipNode>::new();
        for node in nodes {
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

    fn latest_admission_candidates(&self) -> BTreeMap<ClusterNodeName, GossipNode> {
        self.latest_nodes(self.admission_candidates())
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
    /// Encoded bytes held by the current log entries.
    pub retained_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ConsensusRuntimeState {
    pub revision: u64,
    pub schedule: ClusterSchedule,
    pub domains: BTreeMap<DomainName, DomainState>,
    pub domain_clock_authorities: BTreeMap<DomainName, DomainClockAuthority>,
}

/// A coherent runtime state read after this process applied a quorum-confirmed Raft log boundary.
#[derive(Debug, Clone)]
pub struct AdmittedRuntimeState {
    committed_log_index: u64,
    runtime_state: ConsensusRuntimeState,
}

impl AdmittedRuntimeState {
    pub fn committed_log_index(&self) -> u64 {
        self.committed_log_index
    }

    pub fn into_runtime_state(self) -> ConsensusRuntimeState {
        self.runtime_state
    }
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
}

impl AutomaticScheduleFence {
    pub fn leader_tenure(&self) -> &LeaderTenure {
        &self.leader_tenure
    }
}

#[derive(Debug, Clone)]
pub struct AutomaticScheduleInput {
    runtime_state: ConsensusRuntimeState,
    planning_inputs: BTreeMap<DomainName, DomainPlanningInputs>,
    topology: ScheduleTopologyInputs,
    fence: AutomaticScheduleFence,
}

impl AutomaticScheduleInput {
    pub fn runtime_state(&self) -> &ConsensusRuntimeState {
        &self.runtime_state
    }

    pub fn fence(&self) -> AutomaticScheduleFence {
        self.fence.clone()
    }

    pub fn planning_inputs(&self, domain: &DomainName) -> Option<&DomainPlanningInputs> {
        self.planning_inputs.get(domain)
    }

    pub fn topology(&self) -> &ScheduleTopologyInputs {
        &self.topology
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MembershipSnapshot {
    voters: BTreeSet<ClusterNodeName>,
    /// The address Raft holds for each member, in the `BasicNode` form openraft stores.
    nodes: BTreeMap<ClusterNodeName, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MembershipMutation {
    AddLearner {
        node_id: ClusterNodeName,
        endpoint: NodeEndpoint,
        refresh: bool,
    },
    ChangeVoters {
        voters: BTreeSet<ClusterNodeName>,
    },
}

impl MembershipSnapshot {
    fn automatic_mutations(
        &self,
        gossip: &GossipState,
        admission_fences: &BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
    ) -> Vec<MembershipMutation> {
        let mut mutations = Vec::new();
        let mut desired_voters = self.voters.clone();
        for node in gossip.latest_admission_candidates().into_values() {
            let Some(endpoint) = node.interconnect_endpoint else {
                continue;
            };
            let is_fenced = match admission_fences.get(&node.node_id) {
                Some(incarnation) => node.incarnation <= *incarnation,
                None => false,
            };
            if is_fenced {
                continue;
            }

            let advertised = endpoint.to_string();
            let known_address = self.nodes.get(&node.node_id);
            let is_voter = self.voters.contains(&node.node_id);
            let address_changed = known_address != Some(&advertised);
            if !is_voter || address_changed {
                let refresh = known_address.is_some() && address_changed;
                mutations.push(MembershipMutation::AddLearner {
                    node_id: node.node_id.clone(),
                    endpoint,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    node_admission_fences: Records<ClusterNodeName, ClusterNodeIncarnation>,
    domain_mutations: Records<DomainName, DomainMutationLease>,
    transactions: Records<String, ReplicatedTransaction>,
    transaction_commit_plans: transaction_plan::TransactionCommitPlanRecords,
    transaction_reports: TransactionReportRecords,
    command_executions: Records<nervix_models::CommandExecutionReference, CommandExecution>,
}

impl StateMachineData {
    fn domain_resource_planning_inputs(&self, domain: &DomainName) -> DomainResourcePlanningInputs {
        let status = ResourceVersionStatus::from(&self.resources);
        let resources = status
            .next_version_by_resource
            .iter()
            .filter(|counter| counter.domain == *domain)
            .map(|counter| counter.identifier.clone())
            .collect();
        let completed_versions = status
            .uploads
            .completed_versions()
            .filter(|id| id.domain == *domain)
            .cloned()
            .collect();
        DomainResourcePlanningInputs {
            resources,
            completed_versions,
        }
    }

    fn schedule_topology_inputs(&self) -> ScheduleTopologyInputs {
        let membership = self.last_membership.membership();
        let voters: PlanningInputSet<ClusterNodeName> = membership.voter_ids().collect();
        let cordoned = self
            .cordoned_node_ids
            .keys()
            .filter(|node_id| voters.contains(node_id))
            .cloned()
            .collect();
        ScheduleTopologyInputs {
            members: membership
                .nodes()
                .map(|(node_id, _)| node_id.clone())
                .collect(),
            voters,
            cordoned,
        }
    }

    fn domain_planning_inputs(&self, domain: &DomainName) -> DomainPlanningInputs {
        DomainPlanningInputs {
            domain: domain.clone(),
            state: self.domains.get(domain).cloned().map(Box::new),
            resources: self.domain_resource_planning_inputs(domain),
            schedule: self.schedule.domain(domain).cloned().map(Box::new),
            topology: self.schedule_topology_inputs(),
        }
    }

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
        let paced = domain.config.pace.is_paced();

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
        let paced = domain.config.pace.is_paced();

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
                && domain.config.pace.is_paced()
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
    #[error("failed to establish a linearizable consensus read")]
    LinearizableRead,
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
    #[error("cannot identify the current incarnation of raft member '{0}'")]
    NodeIncarnationUnknown(String),
    #[error("cannot drop process '{expected}' because the current process is '{observed}'")]
    NodeIncarnationChanged {
        expected: ClusterNodeIdentity,
        observed: ClusterNodeIdentity,
    },
    #[error("cannot drop live node '{0}'; stop the node before removing it")]
    RemoveLiveNode(String),
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
    interconnect_advertise_addr: NodeEndpoint,
    interconnect: Transport,
    connectivity: ConnectivityFault,
    raft_retention: RaftRetentionPolicy,
    membership_mutation: AsyncMutex<()>,
    incoming_snapshots: Mutex<BTreeMap<ClusterNodeName, IncomingSnapshotTransfer>>,
    events: ConsensusEvents,
    metrics_task: Mutex<Option<JoinHandle<()>>>,
    retention_task: Mutex<Option<JoinHandle<()>>>,
}

#[cfg(feature = "testing")]
#[derive(Clone, Debug, Default)]
pub struct ConsensusTestProbe {
    /// The harness and Raft network clients retain this node's controls and observations together.
    inner: Arc<ConsensusTestProbeState>,
}

#[cfg(feature = "testing")]
#[derive(Debug, Default)]
struct ConsensusTestProbeState {
    /// `FjallStore` retains a clone after construction so live delay and failure controls continue
    /// to reach the storage worker without borrowing this probe.
    storage_fault: StorageFault,
    append_stream_opens: Mutex<BTreeMap<ClusterNodeName, u64>>,
    connectivity: ConnectivityFault,
}

#[cfg(feature = "testing")]
impl ConsensusTestProbe {
    pub fn storage_fault(&self) -> StorageFault {
        self.inner.storage_fault.clone()
    }

    /// Delay every completed consensus storage sync; a zero duration disables the delay.
    pub fn set_storage_commit_delay(&self, delay: Duration) {
        self.inner.storage_fault.set_after_sync_delay(delay);
    }

    pub fn append_stream_open_count(&self, target: &ClusterNodeName) -> u64 {
        let counts = self.inner.append_stream_opens.lock();
        match counts.get(target) {
            Some(count) => *count,
            None => 0,
        }
    }

    pub fn block_connectivity(&self) {
        self.inner.connectivity.block();
    }

    pub fn restore_connectivity(&self) {
        self.inner.connectivity.restore();
    }

    fn record_append_stream_open(&self, target: &ClusterNodeName) {
        let mut counts = self.inner.append_stream_opens.lock();
        let current = match counts.get(target) {
            Some(current) => *current,
            None => 0,
        };
        let Some(next) = current.checked_add(1) else {
            // At u64::MAX this is already a sufficient lower bound for every bounded append-stream
            // assertion, so retaining it preserves the observation's meaning.
            return;
        };
        counts.insert(target.clone(), next);
    }
}

trait AppendStreamOpenRecorder: Clone + Send + Sync + 'static {
    fn record_append_stream_open(&self, target: &ClusterNodeName);
    fn connectivity_fault(&self) -> ConnectivityFault;
}

impl AppendStreamOpenRecorder for () {
    fn record_append_stream_open(&self, _target: &ClusterNodeName) {}

    fn connectivity_fault(&self) -> ConnectivityFault {
        ConnectivityFault::default()
    }
}

#[cfg(feature = "testing")]
impl AppendStreamOpenRecorder for ConsensusTestProbe {
    fn record_append_stream_open(&self, target: &ClusterNodeName) {
        ConsensusTestProbe::record_append_stream_open(self, target);
    }

    fn connectivity_fault(&self) -> ConnectivityFault {
        self.inner.connectivity.clone()
    }
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
        let db = Self::open_database(path.as_ref().to_path_buf(), &settings.executor)
            .await
            .map_err(ConsensusError::Storage)?;
        let store = FjallStore::from_database(db, settings.executor.clone())
            .await
            .map_err(ConsensusError::Storage)?;
        Self::from_store(store, settings, ())
            .await
            .map_err(|_| ConsensusError::Startup)
    }

    #[cfg(feature = "testing")]
    pub async fn open_with_test_probe(
        path: impl AsRef<Path>,
        settings: ConsensusSettings,
        test_probe: ConsensusTestProbe,
    ) -> Result<Self, Report<ConsensusError>> {
        let db = Self::open_database(path.as_ref().to_path_buf(), &settings.executor)
            .await
            .map_err(ConsensusError::Storage)?;
        let store = FjallStore::from_database_with_storage_fault(
            db,
            settings.executor.clone(),
            test_probe.storage_fault(),
        )
        .await
        .map_err(ConsensusError::Storage)?;
        Self::from_store(store, settings, test_probe).await
    }

    async fn open_database(
        path: PathBuf,
        executor: &nervix_execution::Executor,
    ) -> io::Result<Database> {
        let reservation = executor
            .reserve(nervix_execution::MemoryClass::Management, 4096)
            .await
            .map_err(io::Error::other)?;
        executor
            .run_storage(
                nervix_execution::StorageClass::Consensus,
                reservation,
                move |_, _| Database::builder(path).open().map_err(io::Error::other),
            )
            .await
            .map_err(io::Error::other)?
    }

    async fn from_store<Recorder>(
        store: FjallStore,
        settings: ConsensusSettings,
        append_stream_open_recorder: Recorder,
    ) -> Result<Self, Report<ConsensusError>>
    where
        Recorder: AppendStreamOpenRecorder,
    {
        let retention = settings.raft_retention;
        let heartbeat_interval =
            u64::try_from(settings.raft_heartbeat_interval.as_millis()).unwrap_or(u64::MAX);
        let election_timeout_min =
            u64::try_from(settings.raft_election_timeout_min.as_millis()).unwrap_or(u64::MAX);
        // A follower that acknowledged replication sent within the last heartbeat interval already
        // knows what a heartbeat would tell it, so sustained writes send none. The window never
        // takes more than half of the gap before the minimum election timeout, which keeps one
        // interval plus the window below that timeout and leaves the rest for delivery.
        let heartbeat_min_interval = match election_timeout_min.checked_sub(heartbeat_interval) {
            Some(election_gap) => heartbeat_interval.min(election_gap / 2),
            // `validate` rejects an election timeout that does not exceed the heartbeat interval.
            None => 0,
        };
        let config = StdArc::new(
            Config {
                cluster_name: settings.cluster_name,
                heartbeat_interval,
                heartbeat_min_interval: Some(heartbeat_min_interval),
                election_timeout_min,
                election_timeout_max: u64::try_from(settings.raft_election_timeout_max.as_millis())
                    .unwrap_or(u64::MAX),
                // A restarted voter may time out before discovery reconnects it to a healthy
                // leader. Pre-vote lets the current quorum reject that stale candidate without
                // advancing the term and tearing down the leader's replication streams.
                enable_pre_vote: Some(true),
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

        let connectivity = append_stream_open_recorder.connectivity_fault();
        let network = NetworkFactory {
            local_node_id: settings.node_id.clone(),
            interconnect: settings.interconnect.clone(),
            executor: settings.executor.clone(),
            connectivity: connectivity.clone(),
            append_stream_open_recorder,
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
                connectivity,
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
            .register_handler::<wire::RuntimeAdmissionRead, _, _>(move |_context, _request| {
                let receiver = receiver.clone();
                async move {
                    receiver
                        .inner
                        .connectivity
                        .check()
                        .map_err(wire::ConsensusRequestError::raft)?;
                    receiver
                        .runtime_admission_read()
                        .await
                        .map(|read_log_id| read_log_id.log_id().clone().into())
                        .map_err(wire::ConsensusRequestError::raft)
                }
            })?;

        let receiver = self.protocol_receiver();
        self.inner
            .interconnect
            .register_handler::<wire::HeartbeatRequest, _, _>(move |context, request| {
                let receiver = receiver.clone();
                async move {
                    receiver
                        .inner
                        .connectivity
                        .check()
                        .map_err(wire::ConsensusRequestError::raft)?;
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
                        receiver.inner.connectivity.check().map_err(|error| {
                            nervix_interconnect::StreamHandlerError::new(error.to_string())
                        })?;
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
            .register_handler::<wire::RequestPreVote, _, _>(move |context, request| {
                let receiver = receiver.clone();
                async move {
                    receiver
                        .inner
                        .connectivity
                        .check()
                        .map_err(wire::ConsensusRequestError::raft)?;
                    validate_protocol_origin(
                        context.peer_node_id(),
                        request.0.origin_node_id(),
                        "a pre-vote request",
                    )
                    .map_err(wire::ConsensusRequestError::invalid_origin)?;
                    receiver
                        .pre_vote(request.0.into_request())
                        .await
                        .map(wire::VoteResponseRecord::from)
                        .map_err(wire::ConsensusRequestError::raft)
                }
            })?;

        let receiver = self.protocol_receiver();
        self.inner
            .interconnect
            .register_handler::<wire::RequestVote, _, _>(move |context, request| {
                let receiver = receiver.clone();
                async move {
                    receiver
                        .inner
                        .connectivity
                        .check()
                        .map_err(wire::ConsensusRequestError::raft)?;
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
                    receiver
                        .inner
                        .connectivity
                        .check()
                        .map_err(wire::ConsensusRequestError::raft)?;
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
                        .inner
                        .connectivity
                        .check()
                        .map_err(wire::ConsensusRequestError::raft)?;
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
                        .inner
                        .connectivity
                        .check()
                        .map_err(wire::ConsensusRequestError::raft)?;
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
                    receiver
                        .inner
                        .connectivity
                        .check()
                        .map_err(wire::ConsensusRequestError::raft)?;
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

    pub fn subscribe_applied(&self) -> watch::Receiver<u64> {
        self.inner.store.inner.applied_tx.subscribe()
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

    /// Capture every authoritative state-machine input used to plan work in one domain.
    pub async fn domain_planning_inputs(&self, domain: &DomainName) -> DomainPlanningInputs {
        self.inner
            .store
            .inner
            .state()
            .domain_planning_inputs(domain)
    }

    pub async fn transaction_control_snapshot(
        &self,
        domain: &DomainName,
    ) -> TransactionControlSnapshot {
        let state = self.inner.store.inner.state();
        TransactionControlSnapshot {
            planning_inputs: state.domain_planning_inputs(domain),
            resources: (&state.resources).into(),
        }
    }
    pub async fn current_revision(&self) -> u64 {
        let state = self.inner.store.inner.state();
        match &state.last_applied_log_id {
            Some(log_id) => log_id.index,
            None => 0,
        }
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
    pub async fn current_transaction_report(
        &self,
        identity: &nervix_models::TransactionPreviewIdentity,
    ) -> error_stack::Result<TransactionImpactReport, TransactionReportReadError> {
        self.inner
            .store
            .inner
            .state()
            .transaction_reports
            .report(identity)
    }
    pub async fn current_transaction_commit_step(
        &self,
        transaction_id: &str,
        index: usize,
    ) -> error_stack::Result<FrozenTransactionCommitStep, TransactionCommitPlanReadError> {
        self.inner
            .store
            .inner
            .state()
            .transaction_commit_plans
            .step(transaction_id, index)
    }
    pub async fn current_domain_mutation(
        &self,
        domain: &DomainName,
    ) -> Option<DomainMutationLease> {
        self.inner
            .store
            .inner
            .state()
            .domain_mutations
            .get(domain)
            .cloned()
    }
    pub async fn current_command_execution(
        &self,
        reference: &nervix_models::CommandExecutionReference,
    ) -> Option<CommandExecution> {
        self.inner
            .store
            .inner
            .state()
            .command_executions
            .get(reference)
            .cloned()
    }
    pub async fn current_command_executions(
        &self,
    ) -> BTreeMap<nervix_models::CommandExecutionReference, CommandExecution> {
        (&self.inner.store.inner.state().command_executions).into()
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

    /// Return the applied log index of the newest runtime-state change.
    pub async fn current_runtime_revision(&self) -> u64 {
        self.inner.store.inner.state().runtime_revision
    }

    /// Wait until this observer has applied runtime state newer than `revision`.
    pub async fn wait_for_runtime_revision_after(&self, revision: u64) -> Option<u64> {
        let mut applied = self.subscribe_applied();
        loop {
            tokio::task::consume_budget().await;
            let current_revision = self.current_runtime_revision().await;
            if current_revision > revision {
                return Some(current_revision);
            }
            if applied.changed().await.is_err() {
                return None;
            }
        }
    }

    /// Establish a strict read boundary with the current leader, apply through it locally, and
    /// then take one coherent runtime-state snapshot.
    pub async fn admitted_runtime_state(
        &self,
    ) -> Result<AdmittedRuntimeState, Report<ConsensusError>> {
        let leader = self.inner.raft.current_leader().await.ok_or_else(|| {
            Report::new(ConsensusError::LinearizableRead).attach_printable("no current leader")
        })?;
        let read_log_id = if leader == self.inner.local_node_id {
            self.inner
                .raft
                .ensure_linearizable(ReadPolicy::ReadIndex)
                .await
                .map_err(|error| {
                    Report::new(ConsensusError::LinearizableRead)
                        .attach_printable(error.to_string())
                })?
        } else {
            self.inner.connectivity.check().map_err(|error| {
                Report::new(ConsensusError::LinearizableRead).attach_printable(error.to_string())
            })?;
            let response = self
                .inner
                .interconnect
                .request(&leader, wire::RuntimeAdmissionRead)
                .await
                .map_err(|error| {
                    Report::new(ConsensusError::LinearizableRead)
                        .attach_printable(error.to_string())
                })?;
            let log_id = response
                .map_err(|error| {
                    Report::new(ConsensusError::LinearizableRead)
                        .attach_printable(error.to_string())
                })?
                .into_log_id();
            let read_log_id = ReadLogId::from_log_id(log_id);
            Linearizer::new(leader, read_log_id.clone(), None)
                .await_ready(&self.inner.raft)
                .await
                .map_err(|error| {
                    Report::new(ConsensusError::LinearizableRead)
                        .attach_printable(error.to_string())
                })?;
            read_log_id
        };
        let runtime_state = self.current_runtime_state().await;
        Ok(AdmittedRuntimeState {
            committed_log_index: read_log_id.index(),
            runtime_state,
        })
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
    /// What this node is keeping in snapshot storage, beside the log that snapshot covers.
    pub fn snapshot_retention(&self) -> SnapshotRetention {
        self.inner.store.snapshot_retention()
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
            let line = if let nervix_models::DomainPace::Paced { period, skew } = domain.config.pace
            {
                format!(
                    "- {} status={:?} pace={} period={} skew={}",
                    domain.id.as_str(),
                    domain.status,
                    domain.config.pace.as_ref(),
                    period,
                    skew
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

    pub async fn admit_command_execution(
        &self,
        execution: CommandExecution,
        mutation_domains: BTreeSet<DomainName>,
    ) -> Result<CommandExecution, Report<ConsensusError>> {
        let reference = execution.reference.clone();
        let response = self
            .inner
            .client_write(ConsensusCommand::AdmitCommandExecution {
                execution: Box::new(execution),
                mutation_domains,
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => self
                .current_command_execution(&reference)
                .await
                .ok_or_else(|| Report::new(ConsensusError::UnexpectedResponse)),
            ConsensusResponse::Conflict(reason) => {
                Err(Report::new(ConsensusError::Conflict(reason)))
            }
            ConsensusResponse::Transaction(_) => {
                Err(Report::new(ConsensusError::UnexpectedResponse))
            }
        }
    }

    pub async fn acquire_command_domain_mutation(
        &self,
        reference: nervix_models::CommandExecutionReference,
        owner: UserName,
        request_digest: [u8; 32],
        domain: DomainName,
    ) -> Result<CommandExecution, Report<ConsensusError>> {
        let response = self
            .inner
            .client_write(ConsensusCommand::AcquireCommandDomainMutation {
                reference: reference.clone(),
                owner,
                request_digest,
                domain,
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => self
                .current_command_execution(&reference)
                .await
                .ok_or_else(|| Report::new(ConsensusError::UnexpectedResponse)),
            ConsensusResponse::Conflict(reason) => {
                Err(Report::new(ConsensusError::Conflict(reason)))
            }
            ConsensusResponse::Transaction(_) => {
                Err(Report::new(ConsensusError::UnexpectedResponse))
            }
        }
    }

    pub async fn finish_command_execution(
        &self,
        reference: nervix_models::CommandExecutionReference,
        owner: UserName,
        request_digest: [u8; 32],
        at: nervix_models::Timestamp,
        result: CommandExecutionResult,
    ) -> Result<CommandExecution, Report<ConsensusError>> {
        let response = self
            .inner
            .client_write(ConsensusCommand::FinishCommandExecution {
                reference: reference.clone(),
                owner,
                request_digest,
                at,
                result: Box::new(result),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => self
                .current_command_execution(&reference)
                .await
                .ok_or_else(|| Report::new(ConsensusError::UnexpectedResponse)),
            ConsensusResponse::Conflict(reason) => {
                Err(Report::new(ConsensusError::Conflict(reason)))
            }
            ConsensusResponse::Transaction(_) => {
                Err(Report::new(ConsensusError::UnexpectedResponse))
            }
        }
    }

    pub async fn expire_command_executions(
        &self,
        finished_before: nervix_models::Timestamp,
        at: nervix_models::Timestamp,
    ) -> Result<(), Report<ConsensusError>> {
        self.inner
            .client_write(ConsensusCommand::ExpireCommandExecutions {
                finished_before,
                at,
            })
            .await
            .map(|_| ())
            .map_err(Report::new)
    }

    /// Commits a barrier after schedule proposals this leader may have inherited, returning the
    /// revision participants must apply before classifying ownership-handoff preparations.
    pub async fn establish_ownership_handoff_reconciliation(
        &self,
        authority: CoordinationIdentity,
    ) -> Result<u64, Report<ConsensusError>> {
        if authority.coordinator() != &self.inner.local_node_id {
            return Err(Report::new(ConsensusError::Conflict(
                "ownership handoff reconciliation authority is not the local node".to_string(),
            )));
        }
        let response = self
            .inner
            .client_write(ConsensusCommand::ReconcileOwnershipHandoffPreparations { authority })
            .await?;
        let revision = response.log_id.index;
        match response.data {
            ConsensusResponse::Applied => Ok(revision),
            ConsensusResponse::Conflict(reason) => {
                Err(Report::new(ConsensusError::Conflict(reason)))
            }
            ConsensusResponse::Transaction(_) => {
                Err(Report::new(ConsensusError::UnexpectedResponse))
            }
        }
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

        let domains: BTreeMap<DomainName, DomainState> = (&state.domains).into();
        let planning_inputs = domains
            .keys()
            .map(|domain| (domain.clone(), state.domain_planning_inputs(domain)))
            .collect();
        let topology = state.schedule_topology_inputs();
        Ok(AutomaticScheduleInput {
            runtime_state: ConsensusRuntimeState {
                revision: state.runtime_revision,
                schedule: (&state.schedule).into(),
                domains,
                domain_clock_authorities: (&state.domain_clock_authorities).into(),
            },
            planning_inputs,
            topology,
            fence: AutomaticScheduleFence {
                leader_tenure: LeaderTenure {
                    leader_id: self.inner.local_node_id.clone(),
                    term: before.current_term,
                },
            },
        })
    }

    pub async fn replace_domain_schedule(
        &self,
        inputs: DomainPlanningInputs,
        schedule: Option<DomainSchedule>,
        mutation: Option<&DomainMutationLease>,
    ) -> Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::ReplaceDomainSchedule {
                inputs: Box::new(inputs),
                schedule: schedule.map(Box::new),
                mutation: mutation.cloned().map(Box::new),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => Ok(()),
            ConsensusResponse::Conflict(reason) => Err(ConsensusError::Conflict(reason)),
            ConsensusResponse::Transaction(_) => Err(ConsensusError::UnexpectedResponse),
        }
    }

    /// Publish connector-owned Kafka partition metadata against the schedule it was derived from.
    ///
    /// This narrow update is allowed while a domain mutation owns broader model work. The broader
    /// plan carries its own captured inputs and will conflict if this metadata changed its base.
    pub async fn update_kafka_partition_schedule(
        &self,
        inputs: DomainPlanningInputs,
        schedule: DomainSchedule,
    ) -> error_stack::Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::UpdateKafkaPartitionSchedule {
                inputs: Box::new(inputs),
                schedule: Box::new(schedule),
            })
            .await
            .map_err(Report::new)?;
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

    pub async fn apply_automatic_domain_schedule(
        &self,
        fence: AutomaticScheduleFence,
        inputs: DomainPlanningInputs,
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
                inputs: Box::new(inputs),
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

    pub async fn put_domain(
        &self,
        domain: DomainState,
        mutation: Option<&DomainMutationLease>,
    ) -> Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::PutDomain {
                domain: Box::new(domain),
                mutation: mutation.cloned().map(Box::new),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => Ok(()),
            ConsensusResponse::Conflict(reason) => Err(ConsensusError::Conflict(reason)),
            ConsensusResponse::Transaction(_) => Err(ConsensusError::UnexpectedResponse),
        }
    }

    pub async fn put_domain_and_schedule(
        &self,
        inputs: DomainPlanningInputs,
        domain: DomainState,
        schedule: Option<DomainSchedule>,
        mutation: Option<&DomainMutationLease>,
    ) -> Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::PutDomainAndSchedule {
                inputs: Box::new(inputs),
                domain: Box::new(domain),
                schedule: schedule.map(Box::new),
                mutation: mutation.cloned().map(Box::new),
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
        mutation: Option<&DomainMutationLease>,
    ) -> Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::StartDomain {
                domain_id,
                start,
                clock,
                authority,
                mutation: mutation.cloned().map(Box::new),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => Ok(()),
            ConsensusResponse::Conflict(reason) => Err(ConsensusError::Conflict(reason)),
            ConsensusResponse::Transaction(_) => Err(ConsensusError::UnexpectedResponse),
        }
    }

    pub async fn stop_domain(
        &self,
        domain_id: DomainName,
        mutation: Option<&DomainMutationLease>,
    ) -> Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::StopDomain {
                domain_id,
                mutation: mutation.cloned().map(Box::new),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => Ok(()),
            ConsensusResponse::Conflict(reason) => Err(ConsensusError::Conflict(reason)),
            ConsensusResponse::Transaction(_) => Err(ConsensusError::UnexpectedResponse),
        }
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
            Ok(response) => match response.data {
                ConsensusResponse::Applied => Ok(()),
                ConsensusResponse::Conflict(reason) => {
                    Err(Report::new(ConsensusError::Conflict(reason)))
                }
                ConsensusResponse::Transaction(_) => {
                    Err(Report::new(ConsensusError::UnexpectedResponse))
                }
            },
            Err(error) => Err(Report::new(error)),
        }
    }

    pub async fn pause_domain(
        &self,
        domain_id: DomainName,
        mutation: Option<&DomainMutationLease>,
    ) -> Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::PauseDomain {
                domain_id,
                mutation: mutation.cloned().map(Box::new),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => Ok(()),
            ConsensusResponse::Conflict(reason) => Err(ConsensusError::Conflict(reason)),
            ConsensusResponse::Transaction(_) => Err(ConsensusError::UnexpectedResponse),
        }
    }

    pub async fn resume_domain(
        &self,
        domain_id: DomainName,
        mutation: Option<&DomainMutationLease>,
    ) -> Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::ResumeDomain {
                domain_id,
                mutation: mutation.cloned().map(Box::new),
            })
            .await?;
        match response.data {
            ConsensusResponse::Applied => Ok(()),
            ConsensusResponse::Conflict(reason) => Err(ConsensusError::Conflict(reason)),
            ConsensusResponse::Transaction(_) => Err(ConsensusError::UnexpectedResponse),
        }
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
        root_checksum: String,
    ) -> Result<ResourceUpload, Report<ConsensusError>> {
        let response = self
            .inner
            .client_write(ConsensusCommand::BeginResourceUpload {
                key: Box::new(key.clone()),
                root_checksum,
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

    pub async fn complete_resource_upload(
        &self,
        key: ResourceUploadKey,
    ) -> Result<ResourceUpload, Report<ConsensusError>> {
        let response = self
            .inner
            .client_write(ConsensusCommand::CompleteResourceUpload {
                key: Box::new(key.clone()),
            })
            .await?;
        self.resource_upload_response(response, &key).await
    }

    pub async fn fail_resource_upload(
        &self,
        key: ResourceUploadKey,
        reason: String,
    ) -> Result<ResourceUpload, Report<ConsensusError>> {
        let response = self
            .inner
            .client_write(ConsensusCommand::FailResourceUpload {
                key: Box::new(key.clone()),
                reason,
            })
            .await?;
        self.resource_upload_response(response, &key).await
    }

    async fn resource_upload_response(
        &self,
        response: openraft::raft::ClientWriteResponse<TypeConfig>,
        key: &ResourceUploadKey,
    ) -> Result<ResourceUpload, Report<ConsensusError>> {
        match response.data {
            ConsensusResponse::Applied => self
                .current_resources()
                .await
                .upload(key)
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

    pub async fn set_node_cordoned(
        &self,
        node_id: ClusterNodeName,
        cordoned: bool,
    ) -> Result<(), Report<ConsensusError>> {
        self.inner
            .client_write(ConsensusCommand::SetNodeCordoned { node_id, cordoned })
            .await
            .map(|_| ())
            .map_err(Report::new)
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
        request: TransactionQueueRequest,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        let TransactionQueueRequest {
            id,
            owner,
            domain,
            activity,
            statement,
            report,
            limits,
        } = request;
        self.write_transaction(ConsensusCommand::QueueTransactionStatement {
            id,
            owner,
            domain,
            activity,
            statement: Box::new(statement),
            report: Box::new(report),
            limits,
        })
        .await
    }

    pub async fn touch_transaction(
        &self,
        id: String,
        owner: UserName,
        activity: TransactionActivity,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::TouchTransaction {
            id,
            owner,
            activity,
        })
        .await
    }

    pub async fn start_transaction_commit(
        &self,
        id: String,
        owner: UserName,
        activity: TransactionActivity,
        expected_preview: nervix_models::TransactionPreviewIdentity,
        report: TransactionReportArchive,
        plan: TransactionCommitAdmissionPlan,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::StartTransactionCommit {
            id,
            owner,
            activity,
            expected_preview,
            report: Box::new(report),
            plan: Box::new(plan),
        })
        .await
    }

    pub async fn fail_transaction_commit_admission(
        &self,
        failure: TransactionCommitAdmissionFailure,
    ) -> error_stack::Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::FailTransactionCommitAdmission {
            failure: Box::new(failure),
        })
        .await
        .map_err(Report::new)
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

    pub async fn complete_transaction_application(
        &self,
        id: String,
        expected_next_statement: usize,
        at: nervix_models::Timestamp,
        application_failure: Option<String>,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::CompleteTransactionApplication {
            id,
            expected_next_statement,
            at,
            application_failure,
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
        activity: TransactionActivity,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::RevertTransaction {
            id,
            owner,
            activity,
        })
        .await
    }

    pub async fn expire_transaction(
        &self,
        id: String,
        at: nervix_models::Timestamp,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::ExpireTransaction { id, at })
            .await
    }

    pub async fn remove_finished_transactions(
        &self,
        finished_before: nervix_models::Timestamp,
    ) -> Result<(), Report<ConsensusError>> {
        self.inner
            .client_write(ConsensusCommand::RemoveFinishedTransactions { finished_before })
            .await
            .map(|_| ())
            .map_err(Report::new)
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
            BasicNode::new(self.inner.interconnect_advertise_addr.to_string()),
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

    pub async fn reconcile_nodes(
        &self,
        gossip: impl Future<Output = GossipState>,
    ) -> Result<(), ConsensusError> {
        let _membership_mutation = self.inner.membership_mutation.lock().await;
        let gossip = gossip.await;
        let leader = self.inner.raft.current_leader().await;
        if leader.as_ref() != Some(&self.inner.local_node_id) {
            return Ok(());
        }

        let before = self.effective_membership();
        let state = self.inner.store.inner.state();
        let admission_fences = (&state.node_admission_fences).into();
        let mutations = before.automatic_mutations(&gossip, &admission_fences);
        for mutation in mutations {
            tokio::task::consume_budget().await;
            match mutation {
                MembershipMutation::AddLearner {
                    node_id,
                    endpoint,
                    refresh,
                } => {
                    let operation = if refresh {
                        format!("refresh learner '{node_id}' at {endpoint}")
                    } else if before.nodes.contains_key(&node_id) {
                        format!("wait for learner '{node_id}' to catch up at {endpoint}")
                    } else {
                        format!("add learner '{node_id}' at {endpoint}")
                    };
                    self.inner.events.report(format!("raft {operation}"));
                    let admission = timeout(
                        MEMBERSHIP_MUTATION_TIMEOUT,
                        self.inner.raft.add_learner(
                            node_id,
                            BasicNode::new(endpoint.to_string()),
                            true,
                        ),
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

    pub async fn drop_node(
        &self,
        identity: &ClusterNodeIdentity,
        availability: impl Future<Output = GossipState>,
    ) -> Result<(), ConsensusError> {
        let node_id = identity.node_id();
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

        let availability = availability.await;
        if !availability.dead_node_ids.contains(node_id) {
            return Err(ConsensusError::RemoveLiveNode(node_id.to_string()));
        }
        let mut latest_nodes = availability.latest_nodes_by_id();
        let Some(node) = latest_nodes.remove(node_id) else {
            return Err(ConsensusError::NodeIncarnationUnknown(node_id.to_string()));
        };
        let observed_identity = node.identity();
        if &observed_identity != identity {
            return Err(ConsensusError::NodeIncarnationChanged {
                expected: identity.clone(),
                observed: observed_identity,
            });
        }
        let response = self
            .inner
            .client_write(ConsensusCommand::FenceNodeAdmission {
                identity: identity.clone(),
            })
            .await?;
        if response.data != ConsensusResponse::Applied {
            return Err(ConsensusError::UnexpectedResponse);
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
    async fn runtime_admission_read(
        &self,
    ) -> Result<
        ReadLogId<TypeConfig>,
        openraft::error::RaftError<TypeConfig, openraft::error::LinearizableReadError<TypeConfig>>,
    > {
        self.inner
            .raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
    }

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

    pub async fn pre_vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>> {
        self.inner.raft.pre_vote(req).await
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
struct NetworkFactory<Recorder> {
    local_node_id: ClusterNodeName,
    interconnect: Transport,
    executor: nervix_execution::Executor,
    connectivity: ConnectivityFault,
    append_stream_open_recorder: Recorder,
}

#[derive(Clone)]
struct NetworkClient<Recorder> {
    local_node_id: ClusterNodeName,
    target: ClusterNodeName,
    interconnect: Transport,
    executor: nervix_execution::Executor,
    append_path: AppendPath,
    connectivity: ConnectivityFault,
    append_stream_open_recorder: Recorder,
}

impl<Recorder> NetworkFactory<Recorder>
where
    Recorder: AppendStreamOpenRecorder,
{
    fn client(&self, target: ClusterNodeName, append_path: AppendPath) -> NetworkClient<Recorder> {
        NetworkClient {
            local_node_id: self.local_node_id.clone(),
            target,
            interconnect: self.interconnect.clone(),
            executor: self.executor.clone(),
            append_path,
            connectivity: self.connectivity.clone(),
            append_stream_open_recorder: self.append_stream_open_recorder.clone(),
        }
    }
}

impl<Recorder> RaftNetworkFactory<TypeConfig> for NetworkFactory<Recorder>
where
    Recorder: AppendStreamOpenRecorder,
{
    type Network = NetworkClient<Recorder>;

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

fn storage_decode<T: durable_batch::StorageDecode>(bytes: &[u8]) -> Result<T, io::Error> {
    T::decode_record(bytes)
}

fn unreachable_err<E: std::error::Error + Send + Sync + 'static>(
    err: E,
) -> openraft::error::Unreachable<TypeConfig> {
    openraft::error::Unreachable::new(&err)
}

impl<Recorder> RaftNetworkV2<TypeConfig> for NetworkClient<Recorder>
where
    Recorder: AppendStreamOpenRecorder,
{
    type SnapshotData = SealedSnapshot;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.connectivity.check().map_err(unreachable_err)?;
        let record = wire::AppendEntriesRecord::from_request(rpc);
        let response = self
            .interconnect
            .request_with_timeout(
                &self.target,
                wire::HeartbeatRequest(record),
                replication::heartbeat_deadline(&option),
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
    /// over it, under the stream's own idle bound rather than OpenRaft's heartbeat-derived soft
    /// TTL. A heartbeat client keeps OpenRaft's one-probe-at-a-time shape and deadline on the
    /// management pool, where a saturated append stream cannot reach it.
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
        if let Err(error) = self.connectivity.check() {
            return Box::pin(async move { Err(RPCError::Unreachable(unreachable_err(error))) });
        }
        match self.append_path {
            AppendPath::Heartbeat => {
                openraft::network::stream_append_sequential(self, input, option)
            }
            AppendPath::Replication => {
                let interconnect = self.interconnect.clone();
                let executor = self.executor.clone();
                let local_node_id = self.local_node_id.clone();
                let target = self.target.clone();
                let append_stream_open_recorder = self.append_stream_open_recorder.clone();
                Box::pin(async move {
                    let answers = replication::open_append_stream(
                        &interconnect,
                        &executor,
                        &local_node_id,
                        &target,
                        input,
                    )
                    .await?;
                    append_stream_open_recorder.record_append_stream_open(&target);
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
        self.connectivity.check().map_err(unreachable_err)?;
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

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.connectivity.check().map_err(unreachable_err)?;
        let rpc_timeout = option.hard_ttl();
        let response = self
            .interconnect
            .request_with_timeout(
                &self.target,
                wire::RequestPreVote(wire::VoteRequestRecord::from_request(rpc)),
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
        self.connectivity
            .check()
            .map_err(unreachable_err)
            .map_err(StreamingError::from)?;
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
        self.connectivity.check().map_err(unreachable_err)?;
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
    apply_consensus_command_at(state, command, AppliedEntryContext { leader_term: 0 })
}

fn domain_mutation_recovery_fence(state: &StateMachineData) -> DomainMutationRecoveryFence {
    let revision = match &state.last_applied_log_id {
        Some(log_id) => log_id.index,
        None => 0,
    };
    DomainMutationRecoveryFence::at_revision(revision)
}

fn admit_domain_mutation(
    state: &mut StateMachineData,
    domain: &DomainName,
    owner: &DomainMutationOwner,
) -> error_stack::Result<DomainMutationLease, DomainMutationError> {
    let admission = DomainMutationAdmission::decide(
        state.domain_mutations.get(domain),
        owner,
        domain_mutation_recovery_fence(state),
    );
    let Some(lease) = admission.clone().into_admitted() else {
        return Err(Report::new(DomainMutationError::Conflict {
            domain: domain.clone(),
            owner: admission.lease().owner().clone(),
        }));
    };
    state.domain_mutations.insert(domain.clone(), lease.clone());
    Ok(lease)
}

fn admit_domain_mutations(
    state: &mut StateMachineData,
    domains: &BTreeSet<DomainName>,
    owner: &DomainMutationOwner,
) -> error_stack::Result<BTreeMap<DomainName, DomainMutationLease>, DomainMutationError> {
    let recovery_fence = domain_mutation_recovery_fence(state);
    let mut admitted = BTreeMap::new();
    for domain in domains {
        let admission = DomainMutationAdmission::decide(
            state.domain_mutations.get(domain),
            owner,
            recovery_fence,
        );
        let Some(lease) = admission.clone().into_admitted() else {
            return Err(Report::new(DomainMutationError::Conflict {
                domain: domain.clone(),
                owner: admission.lease().owner().clone(),
            }));
        };
        admitted.insert(domain.clone(), lease);
    }
    for (domain, lease) in &admitted {
        state.domain_mutations.insert(domain.clone(), lease.clone());
    }
    Ok(admitted)
}

fn validate_domain_mutation(
    state: &StateMachineData,
    domain: &DomainName,
    requested: Option<&DomainMutationLease>,
) -> error_stack::Result<(), DomainMutationError> {
    let current = state.domain_mutations.get(domain);
    match (current, requested) {
        (None, None) => Ok(()),
        (Some(current), Some(requested)) if current == requested => Ok(()),
        (Some(current), _) => Err(Report::new(DomainMutationError::Conflict {
            domain: domain.clone(),
            owner: current.owner().clone(),
        })),
        (None, Some(requested)) => Err(Report::new(DomainMutationError::FenceLost {
            domain: domain.clone(),
            owner: requested.owner().clone(),
        })),
    }
}

#[derive(Debug, thiserror::Error)]
enum DomainPlanningInputConflict {
    #[error("domain '{}' configuration changed", .0.as_str())]
    Domain(DomainName),
    #[error("domain '{}' resource inputs changed", .0.as_str())]
    Resources(DomainName),
    #[error("domain '{}' schedule changed", .0.as_str())]
    Schedule(DomainName),
    #[error("domain '{}' membership changed", .0.as_str())]
    Membership(DomainName),
    #[error("domain '{}' voter set changed", .0.as_str())]
    Voters(DomainName),
    #[error("domain '{}' node eligibility changed", .0.as_str())]
    Eligibility(DomainName),
}

fn validate_domain_planning_inputs(
    state: &StateMachineData,
    expected: &DomainPlanningInputs,
) -> error_stack::Result<(), DomainPlanningInputConflict> {
    let current = state.domain_planning_inputs(expected.domain());
    let domain = expected.domain().clone();
    if current.state != expected.state {
        return Err(Report::new(DomainPlanningInputConflict::Domain(domain)));
    }
    if current.resources != expected.resources {
        return Err(Report::new(DomainPlanningInputConflict::Resources(domain)));
    }
    if current.schedule != expected.schedule {
        return Err(Report::new(DomainPlanningInputConflict::Schedule(domain)));
    }
    if current.topology.members != expected.topology.members {
        return Err(Report::new(DomainPlanningInputConflict::Membership(domain)));
    }
    if current.topology.voters != expected.topology.voters {
        return Err(Report::new(DomainPlanningInputConflict::Voters(domain)));
    }
    if current.topology.cordoned != expected.topology.cordoned {
        return Err(Report::new(DomainPlanningInputConflict::Eligibility(
            domain,
        )));
    }
    Ok(())
}

fn release_domain_mutation(
    state: &mut StateMachineData,
    domain: &DomainName,
    lease: &DomainMutationLease,
) -> error_stack::Result<(), DomainMutationError> {
    validate_domain_mutation(state, domain, Some(lease))?;
    state.domain_mutations.remove(domain);
    Ok(())
}

fn apply_consensus_command_at(
    state: &mut StateMachineData,
    command: &ConsensusCommand,
    context: AppliedEntryContext,
) -> AppliedConsensusCommand {
    let mut changes = StateMachineChanges::default();
    match command {
        ConsensusCommand::AdmitCommandExecution {
            execution,
            mutation_domains,
        } => {
            if let Some(existing) = state.command_executions.get(&execution.reference) {
                if !existing.same_request(execution) {
                    return AppliedConsensusCommand::conflict(format!(
                        "command execution reference '{}' is bound to a different owner, domain, \
                         or request",
                        execution.reference
                    ));
                }
            } else {
                let owner = DomainMutationOwner::command(execution.reference.clone());
                let admitted = match admit_domain_mutations(state, mutation_domains, &owner) {
                    Ok(admitted) => admitted,
                    Err(reason) => return AppliedConsensusCommand::conflict(reason.to_string()),
                };
                let mut execution = execution.as_ref().clone();
                for (domain, lease) in admitted {
                    execution.bind_domain_mutation(domain, lease);
                }
                state
                    .command_executions
                    .insert(execution.reference.clone(), execution);
            }
        }
        ConsensusCommand::AcquireCommandDomainMutation {
            reference,
            owner,
            request_digest,
            domain,
        } => {
            let Some(mut execution) = state.command_executions.get(reference).cloned() else {
                return AppliedConsensusCommand::conflict(format!(
                    "command execution reference '{reference}' is unknown"
                ));
            };
            if &execution.owner != owner || execution.request_digest != *request_digest {
                return AppliedConsensusCommand::conflict(format!(
                    "command execution reference '{reference}' is bound to a different owner or \
                     request"
                ));
            }
            if !matches!(execution.state, CommandExecutionState::Applying) {
                return AppliedConsensusCommand::conflict(format!(
                    "command execution reference '{reference}' is no longer applying"
                ));
            }
            if let Some(lease) = execution.domain_mutation(domain) {
                if let Err(reason) = validate_domain_mutation(state, domain, Some(lease)) {
                    return AppliedConsensusCommand::conflict(reason.to_string());
                }
            } else {
                let mutation_owner = DomainMutationOwner::command(reference.clone());
                let lease = match admit_domain_mutation(state, domain, &mutation_owner) {
                    Ok(lease) => lease,
                    Err(reason) => return AppliedConsensusCommand::conflict(reason.to_string()),
                };
                execution.bind_domain_mutation(domain.clone(), lease);
                state
                    .command_executions
                    .insert(reference.clone(), execution);
            }
        }
        ConsensusCommand::FinishCommandExecution {
            reference,
            owner,
            request_digest,
            at,
            result,
        } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            let Some(mut execution) = state.command_executions.get(reference).cloned() else {
                return AppliedConsensusCommand::conflict(format!(
                    "command execution reference '{reference}' is unknown"
                ));
            };
            if &execution.owner != owner || execution.request_digest != *request_digest {
                return AppliedConsensusCommand::conflict(format!(
                    "command execution reference '{reference}' is bound to a different owner or \
                     request"
                ));
            }
            match &execution.state {
                CommandExecutionState::Applying => {
                    execution.state = CommandExecutionState::Finished {
                        outcome_revision,
                        finished_at: *at,
                        result: result.clone(),
                    };
                    for (domain, lease) in execution.domain_mutations() {
                        if let Err(reason) = validate_domain_mutation(state, domain, Some(lease)) {
                            return AppliedConsensusCommand::conflict(reason.to_string());
                        }
                    }
                    for (domain, lease) in execution.domain_mutations() {
                        release_domain_mutation(state, domain, lease).verified(
                            "every command mutation lease was validated against this same state",
                        );
                    }
                    state
                        .command_executions
                        .insert(reference.clone(), execution);
                }
                CommandExecutionState::Finished {
                    result: existing, ..
                } if existing.as_ref() == result.as_ref() => {}
                CommandExecutionState::Finished { .. } => {
                    return AppliedConsensusCommand::conflict(format!(
                        "command execution reference '{reference}' already has a different \
                         terminal result"
                    ));
                }
                CommandExecutionState::Expired { .. } => {
                    return AppliedConsensusCommand::conflict(format!(
                        "command execution reference '{reference}' has expired"
                    ));
                }
            }
        }
        ConsensusCommand::ExpireCommandExecutions {
            finished_before,
            at,
        } => {
            let references = state.command_executions.keys().cloned().collect::<Vec<_>>();
            for reference in references {
                let execution = state
                    .command_executions
                    .get_mut(&reference)
                    .verified("the reference came from this same record set");
                if let CommandExecutionState::Finished { finished_at, .. } = &execution.state
                    && *finished_at <= *finished_before
                {
                    execution.state = CommandExecutionState::Expired { expired_at: *at };
                }
            }
        }
        ConsensusCommand::ReplaceDomainSchedule {
            inputs,
            schedule,
            mutation,
        } => {
            let domain = inputs.domain();
            if let Err(reason) = validate_domain_mutation(state, domain, mutation.as_deref()) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            if let Err(reason) = validate_domain_planning_inputs(state, inputs) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            state.replace_domain_schedule(domain, schedule.as_deref());
            changes.schedule_changed = true;
        }
        ConsensusCommand::UpdateKafkaPartitionSchedule { inputs, schedule } => {
            if let Err(reason) = validate_domain_planning_inputs(state, inputs) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            state.replace_domain_schedule(inputs.domain(), Some(schedule));
            changes.schedule_changed = true;
        }
        ConsensusCommand::ApplyAutomaticDomainSchedule {
            fence,
            inputs,
            schedule,
        } => {
            let domain = inputs.domain();
            if let Err(reason) = validate_domain_mutation(state, domain, None) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            if context.leader_term != fence.leader_tenure.term {
                return AppliedConsensusCommand::conflict(
                    "automatic schedule decision leader tenure changed".to_string(),
                );
            }
            if let Err(reason) = validate_domain_planning_inputs(state, inputs) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            state.replace_domain_schedule(domain, schedule.as_deref());
            changes.schedule_changed = true;
        }
        ConsensusCommand::ReconcileOwnershipHandoffPreparations { .. } => {}
        ConsensusCommand::PutDomainAndSchedule {
            inputs,
            domain,
            schedule,
            mutation,
        } => {
            if let Err(reason) = validate_domain_mutation(state, &domain.id, mutation.as_deref()) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            if inputs.domain() != &domain.id {
                return AppliedConsensusCommand::conflict(
                    "domain update does not match its captured planning inputs".to_string(),
                );
            }
            if let Err(reason) = validate_domain_planning_inputs(state, inputs) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            state
                .domains
                .insert(domain.id.clone(), domain.as_ref().clone());
            state.replace_domain_schedule(&domain.id, schedule.as_deref());
            changes.domains_changed = true;
            changes.schedule_changed = true;
        }
        ConsensusCommand::PutDomain { domain, mutation } => {
            if let Err(reason) = validate_domain_mutation(state, &domain.id, mutation.as_deref()) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            state.domains.insert(domain.id.clone(), (**domain).clone());
            changes.domains_changed = true;
        }
        ConsensusCommand::StartDomain {
            domain_id,
            start,
            clock,
            authority,
            mutation,
        } => {
            if let Err(reason) = validate_domain_mutation(state, domain_id, mutation.as_deref()) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            changes.domains_changed = state.commit_domain_start(domain_id, start, clock, authority);
        }
        ConsensusCommand::StopDomain {
            domain_id,
            mutation,
        } => {
            if let Err(reason) = validate_domain_mutation(state, domain_id, mutation.as_deref()) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            changes.domains_changed = state.commit_domain_stop(domain_id);
        }
        ConsensusCommand::PauseDomain {
            domain_id,
            mutation,
        } => {
            if let Err(reason) = validate_domain_mutation(state, domain_id, mutation.as_deref()) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            if let Some(domain) = state.domains.get_mut(domain_id)
                && let DomainStatus::Running = domain.status
            {
                domain.status = DomainStatus::Paused;
                changes.domains_changed = true;
            }
        }
        ConsensusCommand::ResumeDomain {
            domain_id,
            mutation,
        } => {
            if let Err(reason) = validate_domain_mutation(state, domain_id, mutation.as_deref()) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
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
            if let Err(reason) = validate_domain_mutation(state, domain_id, None) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
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
        ConsensusCommand::BeginResourceUpload { key, root_checksum } => {
            if let Err(reason) = state.resources.begin_upload(key, root_checksum) {
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
        ConsensusCommand::CompleteResourceUpload { key } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            if let Err(reason) = state.resources.complete_upload(key, outcome_revision) {
                return AppliedConsensusCommand::conflict(reason.to_string());
            }
            changes.resources_changed = true;
        }
        ConsensusCommand::FailResourceUpload { key, reason } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            if let Err(error) = state.resources.fail_upload(key, outcome_revision, reason) {
                return AppliedConsensusCommand::conflict(error.to_string());
            }
            changes.resources_changed = true;
        }
        ConsensusCommand::SetNodeCordoned { node_id, cordoned } => {
            if *cordoned {
                state.cordoned_node_ids.insert(node_id.clone(), ());
            } else {
                state.cordoned_node_ids.remove(node_id);
            }
        }
        ConsensusCommand::FenceNodeAdmission { identity } => {
            let replace = match state.node_admission_fences.get(identity.node_id()) {
                Some(incarnation) => *incarnation < identity.incarnation(),
                None => true,
            };
            if replace {
                state
                    .node_admission_fences
                    .insert(identity.node_id().clone(), identity.incarnation());
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
            activity,
            statement,
            report,
            limits,
        } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            let Some(mut transaction) = state.transactions.get(id).cloned() else {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::Unknown { id: id.clone() }),
                    changes,
                );
            };
            let (result, transaction_changed) = match transaction.queue(
                owner,
                domain,
                *activity,
                outcome_revision,
                statement.as_ref().clone(),
                *limits,
            ) {
                Err(error) => (Err(error), false),
                Ok(transaction::TransactionQueueDecision::Expired) => {
                    if let Some(preview) = transaction.latest_preview() {
                        state.transaction_reports.retain_revision(preview);
                    }
                    state.transactions.insert(id.clone(), transaction.clone());
                    (Ok(transaction), true)
                }
                Ok(transaction::TransactionQueueDecision::Existing) => (Ok(transaction), false),
                Ok(transaction::TransactionQueueDecision::Added) => {
                    let identity = report.identity();
                    if identity.transaction_id != *id
                        || report.domain() != domain
                        || identity.position.accepted_operations() != transaction.statements.len()
                    {
                        (
                            Err(TransactionMutationError::ReportMismatch { id: id.clone() }),
                            false,
                        )
                    } else if state
                        .transaction_reports
                        .insert(report.as_ref().clone())
                        .is_err()
                    {
                        (
                            Err(TransactionMutationError::ReportConflict { id: id.clone() }),
                            false,
                        )
                    } else {
                        transaction.set_latest_preview(identity.clone());
                        state.transactions.insert(id.clone(), transaction.clone());
                        (Ok(transaction), true)
                    }
                }
            };
            changes.transactions_changed = transaction_changed;
            return AppliedConsensusCommand::transaction(result, changes);
        }
        ConsensusCommand::TouchTransaction {
            id,
            owner,
            activity,
        } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            let result = mutate_transaction(state, id, |transaction| {
                transaction.touch(owner, *activity, outcome_revision)
            });
            changes.transactions_changed = result.is_ok();
            return AppliedConsensusCommand::transaction(result, changes);
        }
        ConsensusCommand::StartTransactionCommit {
            id,
            owner,
            activity,
            expected_preview,
            report,
            plan,
        } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            let Some(mut transaction) = state.transactions.get(id).cloned() else {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::Unknown { id: id.clone() }),
                    changes,
                );
            };
            if let Err(error) = transaction.ensure_owner(owner) {
                return AppliedConsensusCommand::transaction(Err(error), changes);
            }
            match transaction.expire(activity.last_activity_at(), outcome_revision) {
                Ok(true) => {
                    if let Some(preview) = transaction.latest_preview() {
                        state.transaction_reports.retain_revision(preview);
                    }
                    state.transactions.insert(id.clone(), transaction.clone());
                    changes.transactions_changed = true;
                    return AppliedConsensusCommand::transaction(Ok(transaction), changes);
                }
                Ok(false) => {}
                Err(error) => {
                    return AppliedConsensusCommand::transaction(Err(error), changes);
                }
            }
            let current_preview = report.identity();
            let decision = plan.decision();
            if current_preview.transaction_id != *id
                || report.domain() != &transaction.domain
                || current_preview.position.accepted_operations() != transaction.statements.len()
                || decision.preview != *current_preview
                || plan.eligibility().domain() != &transaction.domain
                || !report.is_complete()
                || !report.matches_commit_plan(decision)
            {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::InvalidCommitPlan { id: id.clone() }),
                    changes,
                );
            }
            if decision.steps.len() != plan.inputs().len() {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::InvalidCommitPlan { id: id.clone() }),
                    changes,
                );
            }
            if !plan.is_consistent() {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::InvalidCommitPlan { id: id.clone() }),
                    changes,
                );
            }
            let plan_header = TransactionCommitPlanHeader {
                preview: current_preview.clone(),
                step_count: decision.steps.len(),
            };
            if transaction.has_commit_admission(&plan_header) {
                if expected_preview == current_preview
                    && state.transaction_reports.matches_admitted_archive(report)
                    && state.transaction_commit_plans.matches_admission(plan)
                {
                    return AppliedConsensusCommand::transaction(Ok(transaction), changes);
                }
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::InvalidCommitPlan { id: id.clone() }),
                    changes,
                );
            }
            if let Some(inputs) = plan.inputs().first()
                && let Err(reason) = validate_domain_planning_inputs(state, inputs)
            {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::PlanningInputsChanged {
                        id: id.clone(),
                        reason: reason.to_string(),
                    }),
                    changes,
                );
            }
            if expected_preview != current_preview {
                let mut transaction_reports = state.transaction_reports.clone();
                if transaction_reports.insert(report.as_ref().clone()).is_err() {
                    return AppliedConsensusCommand::transaction(
                        Err(TransactionMutationError::ReportConflict { id: id.clone() }),
                        changes,
                    );
                }
                transaction.set_latest_preview(current_preview.clone());
                transaction_reports.retain_revision(current_preview);
                state.transaction_reports = transaction_reports;
                state.transactions.insert(id.clone(), transaction);
                changes.transactions_changed = true;
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::PreviewStale {
                        expected: Box::new(expected_preview.clone()),
                        current: Box::new(current_preview.clone()),
                    }),
                    changes,
                );
            }
            let domain_mutation = if transaction.requires_domain_mutation() {
                let admission = DomainMutationAdmission::decide(
                    state.domain_mutations.get(&transaction.domain),
                    transaction.mutation_owner(),
                    domain_mutation_recovery_fence(state),
                );
                let Some(lease) = admission.clone().into_admitted() else {
                    return AppliedConsensusCommand::transaction(
                        Err(TransactionMutationError::DomainMutationConflict {
                            id: transaction.id.clone(),
                            domain: transaction.domain.clone(),
                            owner: admission.lease().owner().clone(),
                        }),
                        changes,
                    );
                };
                Some(lease)
            } else {
                None
            };
            if let Err(error) = transaction.start_commit(
                owner,
                *activity,
                outcome_revision,
                domain_mutation.clone(),
                plan_header,
            ) {
                return AppliedConsensusCommand::transaction(Err(error), changes);
            }
            let mut transaction_reports = state.transaction_reports.clone();
            let mut transaction_commit_plans = state.transaction_commit_plans.clone();
            if transaction_reports.insert(report.as_ref().clone()).is_err()
                || transaction_commit_plans.insert(plan).is_err()
            {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::ReportConflict { id: id.clone() }),
                    changes,
                );
            }
            transaction_reports.retain_revision(current_preview);
            state.transaction_reports = transaction_reports;
            state.transaction_commit_plans = transaction_commit_plans;
            if let Some(lease) = domain_mutation {
                state
                    .domain_mutations
                    .insert(transaction.domain.clone(), lease);
            }
            state.transactions.insert(id.clone(), transaction.clone());
            changes.transactions_changed = true;
            return AppliedConsensusCommand::transaction(Ok(transaction), changes);
        }
        ConsensusCommand::FailTransactionCommitAdmission { failure } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            let Some(mut transaction) = state.transactions.get(&failure.id).cloned() else {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::Unknown {
                        id: failure.id.clone(),
                    }),
                    changes,
                );
            };
            if let Err(error) = transaction.ensure_owner(&failure.owner) {
                return AppliedConsensusCommand::transaction(Err(error), changes);
            }
            match transaction.expire(failure.activity.last_activity_at(), outcome_revision) {
                Ok(true) => {
                    if let Some(preview) = transaction.latest_preview() {
                        state.transaction_reports.retain_revision(preview);
                    }
                    state
                        .transactions
                        .insert(failure.id.clone(), transaction.clone());
                    changes.transactions_changed = true;
                    return AppliedConsensusCommand::transaction(Ok(transaction), changes);
                }
                Ok(false) => {}
                Err(error) => {
                    return AppliedConsensusCommand::transaction(Err(error), changes);
                }
            }
            let current_preview = failure.report.identity();
            let Some(failing_step) = failure.report.incomplete_failure_step(failure.operation)
            else {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::InvalidCommitFailure {
                        id: failure.id.clone(),
                    }),
                    changes,
                );
            };
            if current_preview.transaction_id != failure.id
                || failure.report.domain() != &transaction.domain
                || failure.inputs.domain() != &transaction.domain
                || current_preview.position.accepted_operations() != transaction.statement_count
            {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::InvalidCommitFailure {
                        id: failure.id.clone(),
                    }),
                    changes,
                );
            }
            if transaction.matches_commit_admission_failure(
                current_preview,
                failing_step,
                &failure.error,
            ) {
                if state
                    .transaction_reports
                    .matches_admitted_archive(&failure.report)
                {
                    return AppliedConsensusCommand::transaction(Ok(transaction), changes);
                }
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::InvalidCommitFailure {
                        id: failure.id.clone(),
                    }),
                    changes,
                );
            }
            if let Err(reason) = validate_domain_planning_inputs(state, &failure.inputs) {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::PlanningInputsChanged {
                        id: failure.id.clone(),
                        reason: reason.to_string(),
                    }),
                    changes,
                );
            }
            if failure.expected_preview != *current_preview {
                let mut transaction_reports = state.transaction_reports.clone();
                if transaction_reports.insert(failure.report.clone()).is_err() {
                    return AppliedConsensusCommand::transaction(
                        Err(TransactionMutationError::ReportConflict {
                            id: failure.id.clone(),
                        }),
                        changes,
                    );
                }
                transaction.set_latest_preview(current_preview.clone());
                transaction_reports.retain_revision(current_preview);
                state.transaction_reports = transaction_reports;
                state
                    .transactions
                    .insert(failure.id.clone(), transaction.clone());
                changes.transactions_changed = true;
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::PreviewStale {
                        expected: Box::new(failure.expected_preview.clone()),
                        current: Box::new(current_preview.clone()),
                    }),
                    changes,
                );
            }
            let failure_decision = match transaction.fail_commit_admission(
                &failure.owner,
                failure.activity,
                outcome_revision,
                current_preview,
                failing_step,
                &failure.error,
            ) {
                Ok(decision) => decision,
                Err(error) => {
                    return AppliedConsensusCommand::transaction(
                        Err(error.current_context().clone()),
                        changes,
                    );
                }
            };
            match failure_decision {
                transaction::TransactionCommitFailureDecision::Existing => {
                    return AppliedConsensusCommand::transaction(Ok(transaction), changes);
                }
                transaction::TransactionCommitFailureDecision::Expired => {
                    if let Some(preview) = transaction.latest_preview() {
                        state.transaction_reports.retain_revision(preview);
                    }
                    state
                        .transactions
                        .insert(failure.id.clone(), transaction.clone());
                    changes.transactions_changed = true;
                    return AppliedConsensusCommand::transaction(Ok(transaction), changes);
                }
                transaction::TransactionCommitFailureDecision::Failed => {}
            }
            let mut transaction_reports = state.transaction_reports.clone();
            if transaction_reports.insert(failure.report.clone()).is_err()
                || transaction_reports
                    .fail_execution_step(current_preview, failure.operation, &failure.error)
                    .is_err()
            {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::ReportConflict {
                        id: failure.id.clone(),
                    }),
                    changes,
                );
            }
            transaction_reports.retain_revision(current_preview);
            state.transaction_reports = transaction_reports;
            state
                .transactions
                .insert(failure.id.clone(), transaction.clone());
            changes.transactions_changed = true;
            return AppliedConsensusCommand::transaction(Ok(transaction), changes);
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
            if let Err(error) = validate_transaction_domain_mutation(state, &transaction) {
                return AppliedConsensusCommand::transaction(
                    Err(error.current_context().clone()),
                    changes,
                );
            }
            let effect_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            let requested_application = TransactionApplyingStep {
                effect_revision,
                next_statement: *next_statement,
                result: result.as_ref().clone(),
                effect: effect.as_deref().cloned(),
                completion: completion.clone(),
            };
            if let TransactionState::Committing(progress) = &transaction.state
                && let Some(current) = &progress.applying
                && current.next_statement == requested_application.next_statement
                && current.result == requested_application.result
                && current.effect == requested_application.effect
                && current.completion == requested_application.completion
            {
                return AppliedConsensusCommand::transaction(Ok(transaction), changes);
            }
            let plan_step_index = match &transaction.state {
                TransactionState::Committing(progress) => progress.results.len(),
                TransactionState::Open(_) | TransactionState::Finished(_) => {
                    return AppliedConsensusCommand::transaction(
                        Err(TransactionMutationError::NotCommitting {
                            id: id.clone(),
                            state: transaction.state.as_str().to_string(),
                        }),
                        changes,
                    );
                }
            };
            let admitted_step = match state.transaction_commit_plans.step(id, plan_step_index) {
                Ok(step) => step,
                Err(_) => {
                    return AppliedConsensusCommand::transaction(
                        Err(TransactionMutationError::InvalidCommitPlan { id: id.clone() }),
                        changes,
                    );
                }
            };
            if admitted_step.decision.impact.operations() != result.impact.operations()
                || admitted_step.decision.impact.planned() != result.impact.planned()
                || !admitted_step.matches_effect(result.result.success, effect.as_deref())
            {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::InvalidStepResult { id: id.clone() }),
                    changes,
                );
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
            if let Err(error) =
                transaction.begin_application(*expected_next_statement, *at, requested_application)
            {
                return AppliedConsensusCommand::transaction(Err(error), changes);
            }
            let Some(preview) = transaction.latest_preview().cloned() else {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::ReportMismatch { id: id.clone() }),
                    changes,
                );
            };
            if state
                .transaction_reports
                .replace_execution_step(&preview, result.impact.clone())
                .is_err()
            {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::ReportConflict { id: id.clone() }),
                    changes,
                );
            }
            if let Some(effect) = effect {
                apply_transaction_step_effect(state, &transaction.domain, effect, &mut changes);
            }
            state.transactions.insert(id.clone(), transaction.clone());
            changes.transactions_changed = true;
            return AppliedConsensusCommand::transaction(Ok(transaction), changes);
        }
        ConsensusCommand::CompleteTransactionApplication {
            id,
            expected_next_statement,
            at,
            application_failure,
        } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            let Some(mut transaction) = state.transactions.get(id).cloned() else {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::Unknown { id: id.clone() }),
                    changes,
                );
            };
            if let Err(error) = validate_transaction_domain_mutation(state, &transaction) {
                return AppliedConsensusCommand::transaction(
                    Err(error.current_context().clone()),
                    changes,
                );
            }
            let domain_mutation = transaction.domain_mutation().cloned();
            if let Err(error) = transaction.complete_application(
                *expected_next_statement,
                *at,
                outcome_revision,
                application_failure.clone(),
            ) {
                return AppliedConsensusCommand::transaction(Err(error), changes);
            }
            let Some(preview) = transaction.latest_preview().cloned() else {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::ReportMismatch { id: id.clone() }),
                    changes,
                );
            };
            let completed_impact = transaction
                .commit_results()
                .last()
                .map(|result| result.impact.clone())
                .verified("application completion appends the applying step to commit results");
            if state
                .transaction_reports
                .replace_execution_step(&preview, completed_impact)
                .is_err()
            {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::ReportConflict { id: id.clone() }),
                    changes,
                );
            }
            if matches!(transaction.state, TransactionState::Finished(_))
                && let Some(lease) = domain_mutation
                && lease.owner().is_transaction()
            {
                release_domain_mutation(state, &transaction.domain, &lease).verified(
                    "the transaction mutation lease was validated against this same state",
                );
            }
            if matches!(transaction.state, TransactionState::Finished(_)) {
                state.transaction_reports.retain_revision(&preview);
            }
            state.transactions.insert(id.clone(), transaction.clone());
            changes.transactions_changed = true;
            return AppliedConsensusCommand::transaction(Ok(transaction), changes);
        }
        ConsensusCommand::FinishEmptyTransactionCommit { id, at } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            let result = mutate_transaction(state, id, |transaction| {
                transaction.finish_empty_commit(*at, outcome_revision)
            });
            if let Ok(transaction) = &result
                && let Some(preview) = transaction.latest_preview()
            {
                state.transaction_reports.retain_revision(preview);
            }
            changes.transactions_changed = result.is_ok();
            return AppliedConsensusCommand::transaction(result, changes);
        }
        ConsensusCommand::RevertTransaction {
            id,
            owner,
            activity,
        } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            let result = mutate_transaction(state, id, |transaction| {
                transaction.revert(owner, *activity, outcome_revision)
            });
            if let Ok(transaction) = &result
                && let Some(preview) = transaction.latest_preview()
            {
                state.transaction_reports.retain_revision(preview);
            }
            changes.transactions_changed = result.is_ok();
            return AppliedConsensusCommand::transaction(result, changes);
        }
        ConsensusCommand::ExpireTransaction { id, at } => {
            let outcome_revision = match &state.last_applied_log_id {
                Some(log_id) => log_id.index,
                None => 0,
            };
            let Some(mut transaction) = state.transactions.get(id).cloned() else {
                return AppliedConsensusCommand::transaction(
                    Err(TransactionMutationError::Unknown { id: id.clone() }),
                    changes,
                );
            };
            match transaction.expire(*at, outcome_revision) {
                Ok(expired) => {
                    if expired {
                        if let Some(preview) = transaction.latest_preview() {
                            state.transaction_reports.retain_revision(preview);
                        }
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
            let removed_ids = state
                .transactions
                .iter()
                .filter_map(|(id, transaction)| {
                    let TransactionState::Finished(finished) = &transaction.state else {
                        return None;
                    };
                    (finished.finished_at <= *finished_before).then(|| id.clone())
                })
                .collect::<Vec<_>>();
            let before = state.transactions.len();
            state.transactions.retain(|_, transaction| {
                let TransactionState::Finished(finished) = &transaction.state else {
                    return true;
                };
                finished.finished_at > *finished_before
            });
            for id in removed_ids {
                state.transaction_commit_plans.remove(&id);
                state.transaction_reports.remove_transaction(&id);
            }
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

fn validate_transaction_domain_mutation(
    state: &StateMachineData,
    transaction: &ReplicatedTransaction,
) -> error_stack::Result<(), TransactionMutationError> {
    let Some(lease) = transaction.domain_mutation() else {
        if transaction.requires_domain_mutation() {
            return Err(Report::new(
                TransactionMutationError::DomainMutationFenceLost {
                    id: transaction.id.clone(),
                    domain: transaction.domain.clone(),
                },
            ));
        }
        return Ok(());
    };
    if state.domain_mutations.get(&transaction.domain) == Some(lease) {
        return Ok(());
    }
    Err(Report::new(
        TransactionMutationError::DomainMutationFenceLost {
            id: transaction.id.clone(),
            domain: transaction.domain.clone(),
        },
    ))
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
    let effect_matches = transaction.domain == *effect.inputs().domain()
        && match effect {
            TransactionStepEffect::ReplaceDomainSchedule { .. } => statements
                .iter()
                .all(|statement| statement.statement.is_model_mutation()),
            TransactionStepEffect::PutDomainAndSchedule { inputs, domain, .. } => {
                statements.len() == 1
                    && transaction.domain == domain.id
                    && inputs
                        .state()
                        .is_some_and(|previous| previous.id == domain.id)
                    && matches!(statements[0].statement, Statement::AlterDomain(_))
            }
            TransactionStepEffect::StartDomain { inputs, .. } => {
                statements.len() == 1
                    && inputs
                        .state()
                        .is_some_and(|domain| matches!(domain.status, DomainStatus::Stopped))
                    && matches!(statements[0].statement, Statement::StartDomain(_))
            }
            TransactionStepEffect::StopDomain { inputs } => {
                statements.len() == 1
                    && inputs
                        .state()
                        .is_some_and(|domain| !matches!(domain.status, DomainStatus::Stopped))
                    && matches!(statements[0].statement, Statement::StopDomain(_))
            }
            TransactionStepEffect::CreateResourceCatalog { identifier, .. } => {
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

    match validate_domain_planning_inputs(state, effect.inputs()) {
        Err(reason) => Err(TransactionMutationError::StepConflict {
            id: transaction.id.clone(),
            reason: reason.to_string(),
        }),
        Ok(()) => Ok(()),
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
            inputs, schedule, ..
        } => {
            state.replace_domain_schedule(inputs.domain(), schedule.as_deref());
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
            inputs,
            start,
            clock,
            authority,
            ..
        } => {
            changes.domains_changed =
                state.commit_domain_start(inputs.domain(), start, clock, authority);
        }
        TransactionStepEffect::StopDomain { inputs } => {
            changes.domains_changed = state.commit_domain_stop(inputs.domain());
        }
        TransactionStepEffect::CreateResourceCatalog { identifier, .. } => {
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

fn read_key<T: durable_batch::StorageDecode>(
    keyspace: &Keyspace,
    key: &[u8],
) -> io::Result<Option<T>> {
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
        time::Duration,
    };

    use fjall::Database;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        ClusterNodeIdentity, ClusterNodeIncarnation, DomainClockAuthority, DomainClockState,
        DomainConfig, DomainName, DomainPace, DomainSchedule, DomainStartPoint, DomainState,
        DomainStatus, DomainTimeRate, NodeEndpoint, ResourceId, ResourceName, ResourceNodeState,
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
        AppliedEntryContext, AutomaticScheduleFence, ClusterSchedule, CommandExecution,
        CommandExecutionEffect, CommandExecutionResult, CommandExecutionResultKind,
        CommandExecutionState, ConsensusCommand, ConsensusResponse, FjallLogReader, FjallStore,
        GossipNode, GossipState, LeaderTenure, MembershipMutation, MembershipSnapshot,
        ProtocolOriginError, ResourceRecords, StateMachineChanges, StateMachineData,
        TransactionCommandResult, TransactionCommitAdmissionFailure, TransactionMutationError,
        TransactionOutcome, TransactionStatement, TransactionStatementRequest,
        TransactionStepEffect, TransactionStepResult, TypeConfig, UserCredentials,
        apply_consensus_command, apply_consensus_command_at, apply_transaction_step_effect,
        io_error, storage_decode, validate_protocol_origin,
    };
    use crate::{
        ClusterNodeName, ConsensusError, LogIdOf, ReplicatedTransaction, TransactionActivity,
        TransactionQueueLimits, TransactionState, UserName, VoteOf,
    };

    fn domain(raw: &str) -> DomainName {
        DomainName::try_from(raw).expect("valid domain")
    }

    fn captured_inputs(state: &StateMachineData, raw: &str) -> Box<super::DomainPlanningInputs> {
        Box::new(state.domain_planning_inputs(&domain(raw)))
    }

    fn transaction_activity(at: i64) -> TransactionActivity {
        TransactionActivity::from_timeout(Timestamp::from_unix_nanos(at), Duration::from_nanos(10))
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

    /// One discovered node whose endpoints have not been published yet.
    fn undiscovered_node(name: &str, incarnation: u64) -> GossipNode {
        GossipNode {
            node_id: ClusterNodeName::parse(name).assured("the test node name is valid"),
            incarnation: ClusterNodeIncarnation::new(incarnation),
            terminating: false,
            client_url: None,
            console_url: None,
            interconnect_endpoint: None,
        }
    }

    fn node_endpoint(advertised: &str) -> NodeEndpoint {
        advertised
            .parse()
            .assured("the test endpoint is a host and port")
    }

    #[test]
    fn dead_gossip_nodes_are_not_membership_admission_candidates() {
        let state = GossipState {
            live_nodes: vec![
                undiscovered_node("node-2", 2),
                undiscovered_node("node-3", 3),
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
        let state = GossipState {
            live_nodes: vec![
                undiscovered_node("node-1", 10),
                undiscovered_node("node-1", 11),
                undiscovered_node("node-2", 20),
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
    fn placement_candidates_exclude_only_the_newest_terminating_incarnation() {
        let gossip_node = |name: &str, incarnation, terminating| GossipNode {
            terminating,
            ..undiscovered_node(name, incarnation)
        };
        let mut state = GossipState {
            live_nodes: vec![
                gossip_node("node-1", 10, false),
                gossip_node("node-1", 11, true),
                gossip_node("node-2", 20, false),
            ],
            dead_node_ids: BTreeSet::new(),
        };

        assert_eq!(
            state.live_node_ids(),
            BTreeSet::from([
                ClusterNodeName::parse("node-1").assured("the test node name is valid"),
                ClusterNodeName::parse("node-2").assured("the test node name is valid"),
            ])
        );
        assert_eq!(
            state.placement_candidate_node_ids(),
            BTreeSet::from([
                ClusterNodeName::parse("node-2").assured("the test node name is valid")
            ])
        );

        state.live_nodes.push(gossip_node("node-1", 12, false));

        assert_eq!(
            state.placement_candidate_node_ids(),
            BTreeSet::from([
                ClusterNodeName::parse("node-1").assured("the test node name is valid"),
                ClusterNodeName::parse("node-2").assured("the test node name is valid"),
            ])
        );
    }

    #[test]
    fn observed_learner_retries_catch_up_before_promotion() {
        let first = ClusterNodeName::parse("node-1").assured("the test node name is valid");
        let joining = ClusterNodeName::parse("node-2").assured("the test node name is valid");
        let gossip = GossipState {
            live_nodes: vec![GossipNode {
                interconnect_endpoint: Some(node_endpoint("node-2.test:7443")),
                ..undiscovered_node("node-2", 2)
            }],
            dead_node_ids: BTreeSet::new(),
        };
        let membership = MembershipSnapshot {
            voters: BTreeSet::from([first.clone()]),
            nodes: BTreeMap::from([
                (first.clone(), "node-1.test:7443".to_string()),
                (joining.clone(), "node-2.test:7443".to_string()),
            ]),
        };
        let admission_fences = BTreeMap::new();

        assert_eq!(
            membership.automatic_mutations(&gossip, &admission_fences),
            vec![
                MembershipMutation::AddLearner {
                    node_id: joining.clone(),
                    endpoint: node_endpoint("node-2.test:7443"),
                    refresh: false,
                },
                MembershipMutation::ChangeVoters {
                    voters: BTreeSet::from([first, joining]),
                },
            ]
        );
    }

    #[test]
    fn an_unavailable_interconnect_endpoint_is_never_an_admission_candidate() {
        let first = ClusterNodeName::parse("node-1").assured("the test node name is valid");
        let gossip = GossipState {
            live_nodes: vec![undiscovered_node("node-2", 2)],
            dead_node_ids: BTreeSet::new(),
        };
        let membership = MembershipSnapshot {
            voters: BTreeSet::from([first.clone()]),
            nodes: BTreeMap::from([(first, "node-1.test:7443".to_string())]),
        };
        let admission_fences = BTreeMap::new();

        assert!(
            membership
                .automatic_mutations(&gossip, &admission_fences)
                .is_empty()
        );
    }

    #[test]
    fn an_unavailable_client_or_console_url_does_not_withhold_admission() {
        let first = ClusterNodeName::parse("node-1").assured("the test node name is valid");
        let joining = ClusterNodeName::parse("node-2").assured("the test node name is valid");
        let gossip = GossipState {
            live_nodes: vec![GossipNode {
                interconnect_endpoint: Some(node_endpoint("node-2.test:7443")),
                ..undiscovered_node("node-2", 2)
            }],
            dead_node_ids: BTreeSet::new(),
        };
        let membership = MembershipSnapshot {
            voters: BTreeSet::from([first.clone()]),
            nodes: BTreeMap::new(),
        };
        let admission_fences = BTreeMap::new();

        assert_eq!(
            membership.automatic_mutations(&gossip, &admission_fences),
            vec![
                MembershipMutation::AddLearner {
                    node_id: joining.clone(),
                    endpoint: node_endpoint("node-2.test:7443"),
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
        let current_address = "node-2.test:7443".to_string();
        let replacement_endpoint = node_endpoint("node-2.test:8443");
        let gossip = GossipState {
            live_nodes: vec![GossipNode {
                interconnect_endpoint: Some(replacement_endpoint.clone()),
                ..undiscovered_node("node-2", 3)
            }],
            dead_node_ids: BTreeSet::new(),
        };
        let membership = MembershipSnapshot {
            voters: BTreeSet::from([first.clone()]),
            nodes: BTreeMap::from([
                (first.clone(), "node-1.test:7443".to_string()),
                (joining.clone(), current_address),
            ]),
        };
        let admission_fences = BTreeMap::new();

        assert_eq!(
            membership.automatic_mutations(&gossip, &admission_fences),
            vec![
                MembershipMutation::AddLearner {
                    node_id: joining.clone(),
                    endpoint: replacement_endpoint,
                    refresh: true,
                },
                MembershipMutation::ChangeVoters {
                    voters: BTreeSet::from([first, joining]),
                },
            ]
        );
    }

    #[test]
    fn removed_node_requires_a_newer_incarnation_before_readmission() {
        let first = ClusterNodeName::parse("node-1").assured("the test node name is valid");
        let joining = ClusterNodeName::parse("node-2").assured("the test node name is valid");
        let gossip = GossipState {
            live_nodes: vec![GossipNode {
                interconnect_endpoint: Some(node_endpoint("node-2.test:7443")),
                ..undiscovered_node("node-2", 2)
            }],
            dead_node_ids: BTreeSet::new(),
        };
        let membership = MembershipSnapshot {
            voters: BTreeSet::from([first.clone()]),
            nodes: BTreeMap::from([(first.clone(), "node-1.test:7443".to_string())]),
        };
        let admission_fences = BTreeMap::from([(joining.clone(), ClusterNodeIncarnation::new(2))]);

        assert!(
            membership
                .automatic_mutations(&gossip, &admission_fences)
                .is_empty()
        );

        let mut restarted = gossip.clone();
        restarted
            .live_nodes
            .first_mut()
            .assured("the test gossip has one observed node")
            .incarnation = ClusterNodeIncarnation::new(3);
        assert_eq!(
            membership.automatic_mutations(&restarted, &admission_fences),
            vec![
                MembershipMutation::AddLearner {
                    node_id: joining.clone(),
                    endpoint: node_endpoint("node-2.test:7443"),
                    refresh: false,
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
        stopped.config.pace = DomainPace::Paced {
            period: "1s"
                .parse()
                .assured("one second is a positive fixture cadence"),
            skew: "0s"
                .parse()
                .assured("zero nanoseconds is a valid fixture skew"),
        };
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
                mutation: None,
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
                mutation: None,
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
        stopped.config.pace = DomainPace::Paced {
            period: "1s"
                .parse()
                .assured("one second is a positive fixture cadence"),
            skew: "0s"
                .parse()
                .assured("zero nanoseconds is a valid fixture skew"),
        };
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
                mutation: None,
            },
        );
        let mut changes = StateMachineChanges::default();
        let inputs = captured_inputs(&transactional, "paced");
        apply_transaction_step_effect(
            &mut transactional,
            &domain_id,
            &TransactionStepEffect::StartDomain {
                inputs,
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
                mutation: None,
            },
        );
        let mut changes = StateMachineChanges::default();
        let inputs = captured_inputs(&transactional, "paced");
        apply_transaction_step_effect(
            &mut transactional,
            &domain_id,
            &TransactionStepEffect::StopDomain { inputs },
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
        let empty = StateMachineData::default();
        let replace = ConsensusCommand::ReplaceDomainSchedule {
            inputs: captured_inputs(&empty, "tenant"),
            schedule: Some(Box::new(domain_schedule("tenant"))),
            mutation: None,
        };
        let scheduled = StateMachineData {
            schedule: ClusterSchedule::from_iter([domain_schedule("tenant")]).into(),
            ..Default::default()
        };
        let clear = ConsensusCommand::ReplaceDomainSchedule {
            inputs: captured_inputs(&scheduled, "tenant"),
            schedule: None,
            mutation: None,
        };

        assert_eq!(replace.to_string(), "replace-domain-schedule:tenant");
        assert_eq!(clear.to_string(), "clear-domain-schedule:tenant");
        assert_eq!(ConsensusResponse::Applied.to_string(), "ok");
    }

    #[test]
    fn encode_decode_roundtrip_and_invalid_bytes_fail() {
        let state = StateMachineData::default();
        let command = ConsensusCommand::ReplaceDomainSchedule {
            inputs: captured_inputs(&state, "tenant"),
            schedule: Some(Box::new(domain_schedule("tenant"))),
            mutation: None,
        };

        let bytes = crate::durable_batch::DurableBatch::encode(&command, 1024)
            .expect("command should encode");
        let decoded: ConsensusCommand = storage_decode(&bytes).expect("command should decode");
        assert_eq!(decoded, command);

        let err = storage_decode::<ConsensusCommand>(b"invalid archive")
            .expect_err("invalid bytes must fail");
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

        let alpha_inputs = captured_inputs(&state, "alpha");
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                inputs: alpha_inputs,
                schedule: Some(Box::new(domain_schedule("alpha"))),
                mutation: None,
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

        let alpha_inputs = captured_inputs(&state, "alpha");
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                inputs: alpha_inputs,
                schedule: Some(Box::new(domain_schedule("alpha"))),
                mutation: None,
            },
        );
        assert_eq!(state.schedule.domains.len(), 2);

        let zeta_inputs = captured_inputs(&state, "zeta");
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                inputs: zeta_inputs,
                schedule: None,
                mutation: None,
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
        let inputs = captured_inputs(&state, "tenant");

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::PutDomainAndSchedule {
                inputs,
                domain: Box::new(domain_state.clone()),
                schedule: Some(Box::new(schedule.clone())),
                mutation: None,
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
        let stale = captured_inputs(&StateMachineData::default(), "tenant");

        let applied = apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                inputs: stale,
                schedule: None,
                mutation: None,
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
    fn schedule_publication_rejects_changed_cordon_inputs() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut state = StateMachineData::default();
        let node = ClusterNodeName::parse("node-1")?;
        let membership = openraft::Membership::new(
            vec![BTreeSet::from([node.clone()])],
            BTreeMap::from([(node.clone(), crate::Node::new("https://node-1.invalid"))]),
        )?;
        state.last_membership =
            triomphe::Arc::new(openraft::StoredMembership::new(None, membership));
        let stale = captured_inputs(&state, "tenant");
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::SetNodeCordoned {
                node_id: node,
                cordoned: true,
            },
        );

        let applied = apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                inputs: stale,
                schedule: Some(Box::new(domain_schedule("tenant"))),
                mutation: None,
            },
        );

        assert_eq!(
            applied.response,
            ConsensusResponse::Conflict("domain 'tenant' node eligibility changed".to_string())
        );
        assert!(state.schedule.domain(&domain("tenant")).is_none());
        assert!(!applied.schedule_changed);
        Ok(())
    }

    #[test]
    fn schedule_publication_rejects_changed_membership_inputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = StateMachineData::default();
        let stale = captured_inputs(&state, "tenant");
        let node = ClusterNodeName::parse("node-1")?;
        let membership = openraft::Membership::new(
            vec![BTreeSet::from([node.clone()])],
            BTreeMap::from([(node, crate::Node::new("https://node-1.invalid"))]),
        )?;
        state.last_membership =
            triomphe::Arc::new(openraft::StoredMembership::new(None, membership));

        let applied = apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                inputs: stale,
                schedule: Some(Box::new(domain_schedule("tenant"))),
                mutation: None,
            },
        );

        assert_eq!(
            applied.response,
            ConsensusResponse::Conflict("domain 'tenant' membership changed".to_string())
        );
        assert!(state.schedule.domain(&domain("tenant")).is_none());
        assert!(!applied.schedule_changed);
        Ok(())
    }

    #[test]
    fn schedule_publication_accepts_the_pause_derived_from_its_captured_state() {
        let tenant = domain("tenant");
        let mut state = StateMachineData::default();
        state
            .domains
            .insert(tenant.clone(), running_domain_state("tenant"));
        let inputs = state.domain_planning_inputs(&tenant).after_domain_pause();
        let pause = apply_consensus_command(
            &mut state,
            &ConsensusCommand::PauseDomain {
                domain_id: tenant.clone(),
                mutation: None,
            },
        );
        assert_eq!(pause.response, ConsensusResponse::Applied);

        let applied = apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                inputs: Box::new(inputs),
                schedule: Some(Box::new(domain_schedule("tenant"))),
                mutation: None,
            },
        );

        assert_eq!(applied.response, ConsensusResponse::Applied);
        assert!(state.schedule.domain(&tenant).is_some());
    }

    #[test]
    fn kafka_metadata_can_advance_under_domain_ownership_and_fences_the_broader_plan() {
        let mut state = StateMachineData::default();
        let stale = captured_inputs(&state, "tenant");
        let owner = super::DomainMutationOwner::transaction("transaction".to_string());
        let lease = super::DomainMutationAdmission::decide(
            None,
            &owner,
            super::DomainMutationRecoveryFence::at_revision(7),
        )
        .into_admitted()
        .assured("an unowned domain admits the transaction");
        state.domain_mutations.insert(domain("tenant"), lease);

        let kafka_update = apply_consensus_command(
            &mut state,
            &ConsensusCommand::UpdateKafkaPartitionSchedule {
                inputs: stale.clone(),
                schedule: Box::new(domain_schedule("tenant")),
            },
        );
        assert_eq!(kafka_update.response, ConsensusResponse::Applied);

        let mutation = state.domain_mutations.get(&domain("tenant")).cloned();
        let broader = apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                inputs: stale,
                schedule: None,
                mutation: mutation.map(Box::new),
            },
        );
        assert_eq!(
            broader.response,
            ConsensusResponse::Conflict("domain 'tenant' schedule changed".to_string())
        );
        assert!(state.schedule.domain(&domain("tenant")).is_some());
    }

    #[test]
    fn automatic_schedule_publication_rejects_a_stale_leader_tenure() {
        let proposed = domain_schedule("tenant");
        let mut state = StateMachineData::default();
        let inputs = captured_inputs(&state, "tenant");
        let applied = apply_consensus_command_at(
            &mut state,
            &ConsensusCommand::ApplyAutomaticDomainSchedule {
                fence: AutomaticScheduleFence {
                    leader_tenure: LeaderTenure {
                        leader_id: ClusterNodeName::parse("node-1")
                            .assured("the test node name is valid"),
                        term: 7,
                    },
                },
                inputs,
                schedule: Some(Box::new(proposed)),
            },
            AppliedEntryContext { leader_term: 8 },
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
    fn automatic_schedule_publication_ignores_an_unrelated_state_change() {
        let proposed = domain_schedule("tenant");
        let mut state = StateMachineData::default();
        let inputs = captured_inputs(&state, "tenant");
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::CreateUser {
                user: Box::new(UserCredentials {
                    name: UserName::parse("unrelated_user").assured("the test user name is valid"),
                    password_hash: "hash".to_string(),
                }),
            },
        );
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::SetNodeCordoned {
                node_id: ClusterNodeName::parse("nonmember").assured("the test node name is valid"),
                cordoned: true,
            },
        );
        let applied = apply_consensus_command_at(
            &mut state,
            &ConsensusCommand::ApplyAutomaticDomainSchedule {
                fence: AutomaticScheduleFence {
                    leader_tenure: LeaderTenure {
                        leader_id: ClusterNodeName::parse("node-1")
                            .assured("the test node name is valid"),
                        term: 7,
                    },
                },
                inputs,
                schedule: Some(Box::new(proposed)),
            },
            AppliedEntryContext { leader_term: 7 },
        );

        assert_eq!(applied.response, ConsensusResponse::Applied);
        assert_eq!(state.schedule.domains.len(), 1);
        assert!(applied.schedule_changed);
    }

    #[test]
    fn automatic_schedule_publication_applies_a_current_fence() {
        let proposed = domain_schedule("tenant");
        let mut state = StateMachineData::default();
        let inputs = captured_inputs(&state, "tenant");
        let applied = apply_consensus_command_at(
            &mut state,
            &ConsensusCommand::ApplyAutomaticDomainSchedule {
                fence: AutomaticScheduleFence {
                    leader_tenure: LeaderTenure {
                        leader_id: ClusterNodeName::parse("node-1")
                            .assured("the test node name is valid"),
                        term: 7,
                    },
                },
                inputs,
                schedule: Some(Box::new(proposed.clone())),
            },
            AppliedEntryContext { leader_term: 7 },
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
                mutation: None,
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

        let schedule_inputs = captured_inputs(&state, "tenant");
        let schedule_change = apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                inputs: schedule_inputs,
                schedule: Some(Box::new(domain_schedule("tenant"))),
                mutation: None,
            },
        );
        state.record_runtime_revision(43, &schedule_change);
        assert_eq!(state.runtime_revision, 43);
    }

    #[test]
    fn command_execution_identity_retains_one_terminal_outcome() {
        let reference = nervix_models::CommandExecutionReference::parse("request-1")
            .assured("the test reference contains only admitted characters");
        let owner =
            UserName::parse("app_user").assured("the test owner is an identifier-shaped literal");
        let execution = CommandExecution::applying(
            reference.clone(),
            owner.clone(),
            Some(domain("tenant")),
            [7; 32],
            Timestamp::from_unix_nanos(1),
            CommandExecutionEffect::CreateUser {
                if_not_exists: false,
                name: UserName::parse("created_user")
                    .assured("the created test user is an identifier-shaped literal"),
                password_hash: "argon2-hash".to_string(),
            },
        );
        let mut state = StateMachineData::default();

        for admitted in [execution.clone(), execution.clone()] {
            let response = apply_consensus_command(
                &mut state,
                &ConsensusCommand::AdmitCommandExecution {
                    execution: Box::new(admitted),
                    mutation_domains: BTreeSet::new(),
                },
            );
            assert!(matches!(response.response, ConsensusResponse::Applied));
        }
        assert_eq!(state.command_executions.len(), 1);

        let mut conflicting = execution.clone();
        conflicting.request_digest = [8; 32];
        let conflict = apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdmitCommandExecution {
                execution: Box::new(conflicting),
                mutation_domains: BTreeSet::new(),
            },
        );
        assert!(matches!(conflict.response, ConsensusResponse::Conflict(_)));

        let result = CommandExecutionResult {
            success: true,
            kind: CommandExecutionResultKind::Ok,
            message: "created".to_string(),
            diagnostics: Vec::new(),
            already_existed: false,
            results: Vec::new(),
            transaction: None,
            transaction_admission: None,
        };
        for terminal in [result.clone(), result.clone()] {
            let response = apply_consensus_command(
                &mut state,
                &ConsensusCommand::FinishCommandExecution {
                    reference: reference.clone(),
                    owner: owner.clone(),
                    request_digest: [7; 32],
                    at: Timestamp::from_unix_nanos(2),
                    result: Box::new(terminal),
                },
            );
            assert!(matches!(response.response, ConsensusResponse::Applied));
        }
        assert!(matches!(
            &state
                .command_executions
                .get(&reference)
                .verified("the execution was admitted into this record set above")
                .state,
            CommandExecutionState::Finished { .. }
        ));

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ExpireCommandExecutions {
                finished_before: Timestamp::from_unix_nanos(2),
                at: Timestamp::from_unix_nanos(3),
            },
        );
        let response = apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdmitCommandExecution {
                execution: Box::new(execution),
                mutation_domains: BTreeSet::new(),
            },
        );
        assert!(matches!(response.response, ConsensusResponse::Applied));
        assert!(matches!(
            &state
                .command_executions
                .get(&reference)
                .verified("expiry retains the admitted execution identity")
                .state,
            CommandExecutionState::Expired { .. }
        ));
    }

    #[test]
    fn domain_mutation_ownership_joins_conflicts_releases_and_fences() {
        fn execution(
            reference: &str,
            owner: &UserName,
            domain: &DomainName,
            request_digest: [u8; 32],
        ) -> CommandExecution {
            CommandExecution::applying(
                nervix_models::CommandExecutionReference::parse(reference)
                    .assured("the test command reference is an accepted literal"),
                owner.clone(),
                Some(domain.clone()),
                request_digest,
                Timestamp::from_unix_nanos(1),
                CommandExecutionEffect::CreateUser {
                    if_not_exists: false,
                    name: UserName::parse("created_user")
                        .assured("the created test user is an accepted literal"),
                    password_hash: "argon2-hash".to_string(),
                },
            )
        }

        let tenant = domain("tenant");
        let independent = domain("independent");
        let owner = UserName::parse("app_user").assured("the test owner is an accepted literal");
        let first = execution("request-1", &owner, &tenant, [1; 32]);
        let second = execution("request-2", &owner, &tenant, [2; 32]);
        let mutation_domains = BTreeSet::from([tenant.clone()]);
        let mut state = StateMachineData {
            last_applied_log_id: Some(LogIdOf::new(committed_leader(1), 11)),
            ..Default::default()
        };

        for admitted in [first.clone(), first.clone()] {
            let response = apply_consensus_command(
                &mut state,
                &ConsensusCommand::AdmitCommandExecution {
                    execution: Box::new(admitted),
                    mutation_domains: mutation_domains.clone(),
                },
            );
            assert_eq!(response.response, ConsensusResponse::Applied);
        }
        let first_lease = state
            .command_executions
            .get(&first.reference)
            .and_then(|execution| execution.domain_mutation(&tenant))
            .cloned()
            .verified("the admitted execution owns its requested domain mutation");
        assert_eq!(first_lease.recovery_fence().revision(), 11);
        assert_eq!(state.domain_mutations.get(&tenant), Some(&first_lease));

        let conflict = apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdmitCommandExecution {
                execution: Box::new(second.clone()),
                mutation_domains: mutation_domains.clone(),
            },
        );
        assert!(matches!(
            conflict.response,
            ConsensusResponse::Conflict(reason)
                if reason.contains("mutation is owned by command 'request-1'")
        ));
        assert!(!state.command_executions.contains_key(&second.reference));

        let blocked = apply_consensus_command(
            &mut state,
            &ConsensusCommand::PutDomain {
                domain: Box::new(running_domain_state("tenant")),
                mutation: None,
            },
        );
        assert!(matches!(blocked.response, ConsensusResponse::Conflict(_)));
        let blocked_lifecycle = apply_consensus_command(
            &mut state,
            &ConsensusCommand::StartDomain {
                domain_id: tenant.clone(),
                start: DomainStartPoint::Resume,
                clock: None,
                authority: None,
                mutation: None,
            },
        );
        assert!(matches!(
            blocked_lifecycle.response,
            ConsensusResponse::Conflict(_)
        ));
        let schedule_inputs = captured_inputs(&state, "tenant");
        let blocked_schedule = apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                inputs: schedule_inputs,
                schedule: Some(Box::new(domain_schedule("tenant"))),
                mutation: None,
            },
        );
        assert!(matches!(
            blocked_schedule.response,
            ConsensusResponse::Conflict(_)
        ));

        let dynamic = execution("request-drain", &owner, &tenant, [3; 32]);
        let admitted_dynamic = apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdmitCommandExecution {
                execution: Box::new(dynamic.clone()),
                mutation_domains: BTreeSet::new(),
            },
        );
        assert_eq!(admitted_dynamic.response, ConsensusResponse::Applied);
        let blocked_dynamic = apply_consensus_command(
            &mut state,
            &ConsensusCommand::AcquireCommandDomainMutation {
                reference: dynamic.reference,
                owner: owner.clone(),
                request_digest: [3; 32],
                domain: tenant.clone(),
            },
        );
        assert!(matches!(
            blocked_dynamic.response,
            ConsensusResponse::Conflict(_)
        ));

        let authorized = apply_consensus_command(
            &mut state,
            &ConsensusCommand::PutDomain {
                domain: Box::new(running_domain_state("tenant")),
                mutation: Some(Box::new(first_lease.clone())),
            },
        );
        assert_eq!(authorized.response, ConsensusResponse::Applied);
        let independent_write = apply_consensus_command(
            &mut state,
            &ConsensusCommand::PutDomain {
                domain: Box::new(running_domain_state("independent")),
                mutation: None,
            },
        );
        assert_eq!(independent_write.response, ConsensusResponse::Applied);
        assert!(state.domains.contains_key(&independent));
        let resource_write = apply_consensus_command(
            &mut state,
            &ConsensusCommand::CreateResourceCatalog {
                domain: tenant.clone(),
                identifier: ResourceName::parse("bundle")
                    .assured("the resource name is an accepted literal"),
            },
        );
        assert_eq!(resource_write.response, ConsensusResponse::Applied);

        let finished = apply_consensus_command(
            &mut state,
            &ConsensusCommand::FinishCommandExecution {
                reference: first.reference.clone(),
                owner: owner.clone(),
                request_digest: [1; 32],
                at: Timestamp::from_unix_nanos(2),
                result: Box::new(CommandExecutionResult {
                    success: true,
                    kind: CommandExecutionResultKind::Ok,
                    message: "finished".to_string(),
                    diagnostics: Vec::new(),
                    already_existed: false,
                    results: Vec::new(),
                    transaction: None,
                    transaction_admission: None,
                }),
            },
        );
        assert_eq!(finished.response, ConsensusResponse::Applied);
        assert!(!state.domain_mutations.contains_key(&tenant));

        state.last_applied_log_id = Some(LogIdOf::new(committed_leader(2), 20));
        let acquired = apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdmitCommandExecution {
                execution: Box::new(second.clone()),
                mutation_domains,
            },
        );
        assert_eq!(acquired.response, ConsensusResponse::Applied);
        let second_lease = state
            .command_executions
            .get(&second.reference)
            .and_then(|execution| execution.domain_mutation(&tenant))
            .verified("the succeeding execution owns the released domain mutation");
        assert_eq!(second_lease.recovery_fence().revision(), 20);
        assert_ne!(second_lease, &first_lease);

        let stale = apply_consensus_command(
            &mut state,
            &ConsensusCommand::PutDomain {
                domain: Box::new(running_domain_state("tenant")),
                mutation: Some(Box::new(first_lease)),
            },
        );
        assert!(matches!(stale.response, ConsensusResponse::Conflict(_)));
    }

    #[test]
    fn transaction_append_identity_and_position_are_exact() {
        let owner =
            UserName::parse("app_user").assured("the test owner is an identifier-shaped literal");
        let domain_id = domain("tenant");
        let mut transaction = ReplicatedTransaction::open(
            "tx-append".to_string(),
            domain_id.clone(),
            owner.clone(),
            transaction_activity(1),
        );
        let limits = TransactionQueueLimits {
            max_statements: 4,
            max_source_bytes: 1024,
        };
        let first = TransactionStatement::test_admitted(TransactionStatementRequest {
            request_reference: nervix_models::CommandExecutionReference::parse("append-1")
                .assured("the test append reference contains only admitted characters"),
            expected_position: 0,
            source: "STOP".to_string(),
            statement: Statement::StopDomain(nervix_models::StopDomain),
        });
        transaction
            .queue(
                &owner,
                &domain_id,
                transaction_activity(2),
                2,
                first.clone(),
                limits,
            )
            .assured("the first append has the transaction owner, domain, and position");
        transaction
            .queue(
                &owner,
                &domain_id,
                transaction_activity(3),
                3,
                first.clone(),
                limits,
            )
            .assured("an exact duplicate joins the append already admitted above");
        assert_eq!(transaction.statements.len(), 1);
        assert_eq!(
            transaction.last_activity_at(),
            Timestamp::from_unix_nanos(2)
        );
        assert_eq!(
            transaction
                .queue_admission(&owner, &domain_id, &first.request, limits)
                .assured("the exact request returns its retained admission"),
            crate::TransactionQueueAdmission::Existing(first.admission.clone())
        );

        let mut changed = first;
        changed.request.source = "STOP;".to_string();
        assert!(matches!(
            transaction.queue(
                &owner,
                &domain_id,
                transaction_activity(4),
                4,
                changed,
                limits,
            ),
            Err(TransactionMutationError::RequestConflict { .. })
        ));
        assert_eq!(
            transaction.last_activity_at(),
            Timestamp::from_unix_nanos(2)
        );

        let mut second = TransactionStatement::test_admitted(TransactionStatementRequest {
            request_reference: nervix_models::CommandExecutionReference::parse("append-2")
                .assured("the test append reference contains only admitted characters"),
            expected_position: 0,
            source: "STOP".to_string(),
            statement: Statement::StopDomain(nervix_models::StopDomain),
        });
        assert!(matches!(
            transaction.queue(
                &owner,
                &domain_id,
                transaction_activity(5),
                5,
                second.clone(),
                limits,
            ),
            Err(TransactionMutationError::PositionConflict {
                expected: 0,
                actual: 1,
                ..
            })
        ));
        assert_eq!(
            transaction.last_activity_at(),
            Timestamp::from_unix_nanos(2)
        );
        second.request.expected_position = 1;
        transaction
            .queue(
                &owner,
                &domain_id,
                transaction_activity(6),
                6,
                second,
                limits,
            )
            .assured("the corrected append uses the current position and a new reference");
        assert_eq!(transaction.statements.len(), 2);
    }

    #[test]
    fn transaction_tombstone_retention_starts_at_the_terminal_decision_inclusively() {
        let owner =
            UserName::parse("app_user").assured("the test owner is an identifier-shaped literal");
        let mut transaction = ReplicatedTransaction::open(
            "retained".to_string(),
            domain("tenant"),
            owner.clone(),
            transaction_activity(1),
        );
        transaction
            .revert(&owner, transaction_activity(2), 7)
            .assured("the open test transaction can be reverted");
        let TransactionState::Finished(finished) = &transaction.state else {
            panic!("revert must make the test transaction terminal");
        };
        assert_eq!(finished.finished_at, Timestamp::from_unix_nanos(2));

        let mut state = StateMachineData::default();
        state
            .transactions
            .insert(transaction.id.clone(), transaction);
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::RemoveFinishedTransactions {
                finished_before: Timestamp::from_unix_nanos(1),
            },
        );
        assert!(state.transactions.contains_key("retained"));
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::RemoveFinishedTransactions {
                finished_before: Timestamp::from_unix_nanos(2),
            },
        );
        assert!(!state.transactions.contains_key("retained"));
    }

    #[test]
    fn replicated_append_retry_and_commit_preview_validation_are_exact() {
        let owner =
            UserName::parse("app_user").assured("the test owner is an identifier-shaped literal");
        let domain_id = domain("tenant");
        let mut state = StateMachineData::default();
        let transaction = ReplicatedTransaction::open(
            "tx-preview".to_string(),
            domain_id.clone(),
            owner.clone(),
            transaction_activity(1),
        );
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::OpenTransaction {
                transaction: Box::new(transaction),
                max_open_transactions: 4,
            },
        );
        let report = crate::transaction_report::test_report_archive("tx-preview", &domain_id, 1);
        let preview = report.identity().clone();
        let admission = nervix_models::TransactionOperationAdmission {
            operation: nervix_models::TransactionOperationNumber::from_index(0)
                .assured("the first test operation is addressable"),
            preview: preview.clone(),
        };
        let command = ConsensusCommand::QueueTransactionStatement {
            id: "tx-preview".to_string(),
            owner: owner.clone(),
            domain: domain_id.clone(),
            activity: transaction_activity(2),
            statement: Box::new(TransactionStatement::admitted(
                TransactionStatementRequest {
                    request_reference: nervix_models::CommandExecutionReference::parse(
                        "append-preview",
                    )
                    .assured("the test request reference is an accepted literal"),
                    expected_position: 0,
                    source: "START".to_string(),
                    statement: Statement::StartDomain(nervix_models::StartDomain {
                        start: DomainStartPoint::Resume,
                    }),
                },
                TransactionCommandResult {
                    success: true,
                    message: "queued".to_string(),
                    diagnostics: Vec::new(),
                    already_existed: false,
                    admission: Some(admission.clone()),
                },
            )),
            report: Box::new(report.clone()),
            limits: TransactionQueueLimits {
                max_statements: 4,
                max_source_bytes: 1024,
            },
        };
        let first = apply_consensus_command(&mut state, &command);
        let ConsensusResponse::Transaction(first) = first.response else {
            panic!("transaction append must return its replicated transaction");
        };
        let first = first
            .result
            .assured("the first append has a matching report revision");
        let retained_after_first = state.clone();
        let retried = apply_consensus_command(&mut state, &command);
        let ConsensusResponse::Transaction(retried) = retried.response else {
            panic!("transaction append retry must return its replicated transaction");
        };
        let retried = retried
            .result
            .assured("the exact append retry joins its retained revision");
        assert_eq!(retried, first);
        assert_eq!(retried.latest_preview(), Some(&preview));
        assert_eq!(retried.statements[0].admission.admission, Some(admission));
        assert_eq!(state, retained_after_first);

        let fresh_report = crate::transaction_report::test_report_archive_with_basis(
            "tx-preview",
            &domain_id,
            1,
            nervix_models::ImpactPlanningBasis::new([9; 32]),
        );
        let fresh_preview = fresh_report.identity().clone();
        let mut plan = crate::transaction::test_commit_plan("tx-preview", 1);
        plan.preview = fresh_preview.clone();
        let stale_admission_plan =
            crate::transaction_plan::test_admission_plan(&state, &domain_id, plan.clone());
        let rejected = apply_consensus_command(
            &mut state,
            &ConsensusCommand::StartTransactionCommit {
                id: "tx-preview".to_string(),
                owner: owner.clone(),
                activity: transaction_activity(3),
                expected_preview: preview.clone(),
                report: Box::new(fresh_report.clone()),
                plan: Box::new(stale_admission_plan),
            },
        );
        let ConsensusResponse::Transaction(rejected) = rejected.response else {
            panic!("stale commit admission must return a transaction error");
        };
        assert!(matches!(
            rejected.result,
            Err(TransactionMutationError::PreviewStale {
                expected,
                current,
            }) if *expected == preview && *current == fresh_preview
        ));
        let refreshed = state
            .transactions
            .get("tx-preview")
            .verified("the rejected transaction remains retained");
        assert!(matches!(refreshed.state, TransactionState::Open(_)));
        assert_eq!(refreshed.latest_preview(), Some(&fresh_preview));
        state
            .transaction_reports
            .report(&fresh_preview)
            .assured("the fresh validated preview is retained for the exact retry");
        assert!(
            state
                .transaction_commit_plans
                .step("tx-preview", 0)
                .is_err()
        );

        let retained_after_refresh = state.clone();
        let delayed_append = apply_consensus_command(&mut state, &command);
        let ConsensusResponse::Transaction(delayed_append) = delayed_append.response else {
            panic!("delayed append retry must return its replicated transaction");
        };
        let delayed_append = delayed_append
            .result
            .assured("the delayed append retry joins its retained operation");
        assert_eq!(delayed_append.latest_preview(), Some(&fresh_preview));
        assert_eq!(state, retained_after_refresh);

        let changed_inputs_plan =
            crate::transaction_plan::test_admission_plan(&state, &domain_id, plan.clone());
        state.resources.ensure_catalog(
            &domain_id,
            &ResourceName::parse("changed_input")
                .assured("the test resource is an identifier-shaped literal"),
        );
        let rejected = apply_consensus_command(
            &mut state,
            &ConsensusCommand::StartTransactionCommit {
                id: "tx-preview".to_string(),
                owner: owner.clone(),
                activity: transaction_activity(4),
                expected_preview: fresh_preview.clone(),
                report: Box::new(fresh_report.clone()),
                plan: Box::new(changed_inputs_plan),
            },
        );
        let ConsensusResponse::Transaction(rejected) = rejected.response else {
            panic!("changed planning inputs must return a transaction error");
        };
        assert!(matches!(
            rejected.result,
            Err(TransactionMutationError::PlanningInputsChanged { .. })
        ));
        assert!(matches!(
            state
                .transactions
                .get("tx-preview")
                .verified("the rejected transaction remains retained")
                .state,
            TransactionState::Open(_)
        ));
        assert!(
            state
                .transaction_commit_plans
                .step("tx-preview", 0)
                .is_err()
        );

        let incomplete =
            crate::transaction_report::test_incomplete_report_archive("tx-preview", &domain_id, 1);
        let incomplete_admission_plan =
            crate::transaction_plan::test_admission_plan(&state, &domain_id, plan.clone());
        let rejected = apply_consensus_command(
            &mut state,
            &ConsensusCommand::StartTransactionCommit {
                id: "tx-preview".to_string(),
                owner: owner.clone(),
                activity: transaction_activity(5),
                expected_preview: incomplete.identity().clone(),
                report: Box::new(incomplete),
                plan: Box::new(incomplete_admission_plan),
            },
        );
        let ConsensusResponse::Transaction(rejected) = rejected.response else {
            panic!("incomplete commit admission must return a transaction error");
        };
        assert!(matches!(
            rejected.result,
            Err(TransactionMutationError::InvalidCommitPlan { .. })
        ));
        assert!(matches!(
            state
                .transactions
                .get("tx-preview")
                .verified("the rejected transaction remains retained")
                .state,
            TransactionState::Open(_)
        ));

        let admitted_plan = crate::transaction_plan::test_admission_plan(&state, &domain_id, plan);
        let start = ConsensusCommand::StartTransactionCommit {
            id: "tx-preview".to_string(),
            owner,
            activity: transaction_activity(6),
            expected_preview: fresh_preview.clone(),
            report: Box::new(fresh_report),
            plan: Box::new(admitted_plan),
        };
        let started = apply_consensus_command(&mut state, &start);
        let ConsensusResponse::Transaction(started) = started.response else {
            panic!("the refreshed preview must admit the frozen commit plan");
        };
        let started = started
            .result
            .assured("the exact refreshed preview admits commit");
        assert!(matches!(started.state, TransactionState::Committing(_)));
        assert_eq!(started.latest_preview(), Some(&fresh_preview));
        state
            .transaction_commit_plans
            .step("tx-preview", 0)
            .assured("commit admission freezes the first execution step");

        state.resources.ensure_catalog(
            &domain_id,
            &ResourceName::parse("after_admission")
                .assured("the test resource is an identifier-shaped literal"),
        );
        let retained_after_start = state.clone();
        let retried = apply_consensus_command(&mut state, &start);
        let ConsensusResponse::Transaction(retried) = retried.response else {
            panic!("exact commit retry must return its replicated transaction");
        };
        assert_eq!(
            retried
                .result
                .assured("the exact commit retry joins the frozen plan"),
            started
        );
        assert_eq!(state, retained_after_start);
    }

    #[test]
    fn incomplete_commit_failure_refreshes_then_replays_its_retained_outcome() {
        let owner =
            UserName::parse("app_user").assured("the test owner is an identifier-shaped literal");
        let domain_id = domain("tenant");
        let mut state = StateMachineData::default();
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::OpenTransaction {
                transaction: Box::new(ReplicatedTransaction::open(
                    "tx-incomplete".to_string(),
                    domain_id.clone(),
                    owner.clone(),
                    transaction_activity(1),
                )),
                max_open_transactions: 4,
            },
        );
        let queued_report =
            crate::transaction_report::test_report_archive("tx-incomplete", &domain_id, 1);
        let queued_preview = queued_report.identity().clone();
        let queued = apply_consensus_command(
            &mut state,
            &ConsensusCommand::QueueTransactionStatement {
                id: "tx-incomplete".to_string(),
                owner: owner.clone(),
                domain: domain_id.clone(),
                activity: transaction_activity(2),
                statement: Box::new(TransactionStatement::test_admitted(
                    TransactionStatementRequest {
                        request_reference: nervix_models::CommandExecutionReference::parse(
                            "append-incomplete",
                        )
                        .assured("the test request reference is an accepted literal"),
                        expected_position: 0,
                        source: "START".to_string(),
                        statement: Statement::StartDomain(nervix_models::StartDomain {
                            start: DomainStartPoint::Resume,
                        }),
                    },
                )),
                report: Box::new(queued_report),
                limits: TransactionQueueLimits {
                    max_statements: 4,
                    max_source_bytes: 1024,
                },
            },
        );
        let ConsensusResponse::Transaction(queued) = queued.response else {
            panic!("the append must return its replicated transaction");
        };
        queued
            .result
            .assured("the incomplete-plan fixture first admits its statement");

        let incomplete = crate::transaction_report::test_incomplete_report_archive_with_basis(
            "tx-incomplete",
            &domain_id,
            1,
            nervix_models::ImpactPlanningBasis::new([2; 32]),
        );
        let incomplete_preview = incomplete.identity().clone();
        let operation = nervix_models::TransactionOperationNumber::from_index(0)
            .assured("the first test operation is addressable");
        let failure_message = "the planned domain transition is invalid".to_string();
        let stale_failure = ConsensusCommand::FailTransactionCommitAdmission {
            failure: Box::new(TransactionCommitAdmissionFailure {
                id: "tx-incomplete".to_string(),
                owner: owner.clone(),
                activity: transaction_activity(3),
                expected_preview: queued_preview.clone(),
                report: incomplete.clone(),
                inputs: state.domain_planning_inputs(&domain_id),
                operation,
                error: failure_message.clone(),
            }),
        };
        let refreshed = apply_consensus_command(&mut state, &stale_failure);
        let ConsensusResponse::Transaction(refreshed) = refreshed.response else {
            panic!("stale incomplete admission must return a transaction error");
        };
        assert!(matches!(
            refreshed.result,
            Err(TransactionMutationError::PreviewStale {
                expected,
                current,
            }) if *expected == queued_preview && *current == incomplete_preview
        ));
        let refreshed = state
            .transactions
            .get("tx-incomplete")
            .verified("the refreshed incomplete preview remains attached to its transaction");
        assert!(matches!(refreshed.state, TransactionState::Open(_)));
        assert_eq!(refreshed.latest_preview(), Some(&incomplete_preview));

        let failure = ConsensusCommand::FailTransactionCommitAdmission {
            failure: Box::new(TransactionCommitAdmissionFailure {
                id: "tx-incomplete".to_string(),
                owner,
                activity: transaction_activity(4),
                expected_preview: incomplete_preview.clone(),
                report: incomplete,
                inputs: state.domain_planning_inputs(&domain_id),
                operation,
                error: failure_message.clone(),
            }),
        };
        let failed = apply_consensus_command(&mut state, &failure);
        let ConsensusResponse::Transaction(failed) = failed.response else {
            panic!("the exact incomplete preview must return its terminal transaction");
        };
        let failed = failed
            .result
            .assured("the exact incomplete preview records its planning failure");
        assert!(matches!(
            &failed.state,
            TransactionState::Finished(finished)
                if matches!(
                    &finished.outcome,
                    TransactionOutcome::Failed {
                        failing_step: 0,
                        error,
                    } if error == &failure_message
                )
        ));
        let retained_report = state
            .transaction_reports
            .report(&incomplete_preview)
            .assured("the terminal planning failure retains its identified report");
        assert!(matches!(
            &retained_report.execution_steps()[0].actual().outcome,
            nervix_models::ExecutionStepOutcome::Failed { diagnostic }
                if diagnostic.message == failure_message
        ));

        state.resources.ensure_catalog(
            &domain_id,
            &ResourceName::parse("after_failure")
                .assured("the test resource is an identifier-shaped literal"),
        );
        let retained_after_failure = state.clone();
        let retried = apply_consensus_command(&mut state, &failure);
        let ConsensusResponse::Transaction(retried) = retried.response else {
            panic!("the exact failure retry must return its retained transaction");
        };
        assert_eq!(
            retried
                .result
                .assured("the exact failure retry joins the retained outcome"),
            failed
        );
        assert_eq!(state, retained_after_failure);
    }

    #[test]
    fn transaction_step_effect_and_progress_are_applied_once() {
        let owner =
            UserName::parse("app_user").assured("the test owner is an identifier-shaped literal");
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
            transaction_activity(1),
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
                    activity: transaction_activity(
                        i64::try_from(at)
                            .unwrap_or_default()
                            .checked_add(2)
                            .assured("the test clock counts from zero"),
                    ),
                    statement: Box::new(TransactionStatement::test_admitted(
                        TransactionStatementRequest {
                            request_reference: nervix_models::CommandExecutionReference::parse(
                                format!("request-{at}"),
                            )
                            .assured("the bounded test index produces an admitted reference"),
                            expected_position: at,
                            source: "statement".to_string(),
                            statement,
                        },
                    )),
                    report: Box::new(crate::transaction_report::test_report_archive(
                        "tx-1",
                        &domain_id,
                        at.checked_add(1)
                            .assured("the test queues two addressable operations"),
                    )),
                    limits: TransactionQueueLimits {
                        max_statements: 4,
                        max_source_bytes: 1024,
                    },
                },
            );
        }
        let mut commit_plan = crate::transaction::test_commit_plan("tx-1", 2);
        commit_plan.steps[0].kind = nervix_models::TransactionCommitStepKind::StartDomain {
            resolved: nervix_models::TransactionResolvedDomainStart {
                start: DomainStartPoint::Resume,
                clock: None,
                authority: None,
            },
        };
        let preview = commit_plan.preview.clone();
        let mut first_impact = commit_plan.steps[0].impact.clone();
        *first_impact.actual_mut() = nervix_models::ActualExecutionStepImpact::applying();
        let mut second_impact = commit_plan.steps[1].impact.clone();
        *second_impact.actual_mut() = nervix_models::ActualExecutionStepImpact::applying();
        let commit_plan =
            crate::transaction_plan::test_admission_plan(&state, &domain_id, commit_plan);
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::StartTransactionCommit {
                id: "tx-1".to_string(),
                owner,
                activity: transaction_activity(4),
                expected_preview: commit_plan.decision().preview.clone(),
                report: Box::new(crate::transaction_report::test_report_archive(
                    "tx-1", &domain_id, 2,
                )),
                plan: Box::new(commit_plan),
            },
        );

        let inputs = captured_inputs(&state, "tenant");
        let first_step = ConsensusCommand::AdvanceTransactionCommit {
            id: "tx-1".to_string(),
            expected_next_statement: 0,
            next_statement: 1,
            at: nervix_models::Timestamp::from_unix_nanos(5),
            result: Box::new(TransactionStepResult {
                impact: first_impact,
                result: TransactionCommandResult {
                    success: true,
                    message: "started".to_string(),
                    diagnostics: Vec::new(),
                    already_existed: false,
                    admission: None,
                },
            }),
            effect: Some(Box::new(TransactionStepEffect::StartDomain {
                inputs,
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
        let transaction = state
            .transactions
            .get("tx-1")
            .verified("the transaction was opened and has not reached terminal cleanup");
        assert_eq!(transaction.completed_statement_count(), 0);
        let TransactionState::Committing(progress) = &transaction.state else {
            panic!("transaction should remain committing during application");
        };
        assert!(progress.applying.is_some());
        let applying_report = state
            .transaction_reports
            .report(&preview)
            .assured("the applying transaction retains its admitted report");
        assert!(matches!(
            applying_report.execution_steps()[0].actual().outcome,
            nervix_models::ExecutionStepOutcome::Applying
        ));
        assert!(matches!(
            applying_report.execution_steps()[1].actual().outcome,
            nervix_models::ExecutionStepOutcome::Unattempted
        ));

        let duplicate = apply_consensus_command(&mut state, &first_step);
        let ConsensusResponse::Transaction(response) = duplicate.response else {
            panic!("duplicate transaction step must return a transaction response");
        };
        assert!(response.result.is_ok());
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
            &ConsensusCommand::CompleteTransactionApplication {
                id: "tx-1".to_string(),
                expected_next_statement: 0,
                at: nervix_models::Timestamp::from_unix_nanos(6),
                application_failure: None,
            },
        );
        let transaction = state
            .transactions
            .get("tx-1")
            .verified("the completed application is still part of the open commit");
        assert_eq!(transaction.completed_statement_count(), 1);
        assert!(matches!(
            transaction.commit_results()[0].impact.actual().outcome,
            nervix_models::ExecutionStepOutcome::Applied
        ));
        let completed_prefix_report = state
            .transaction_reports
            .report(&preview)
            .assured("the completed prefix remains in the retained report");
        assert!(matches!(
            completed_prefix_report.execution_steps()[0]
                .actual()
                .outcome,
            nervix_models::ExecutionStepOutcome::Applied
        ));
        assert!(matches!(
            completed_prefix_report.execution_steps()[1]
                .actual()
                .outcome,
            nervix_models::ExecutionStepOutcome::Unattempted
        ));

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdvanceTransactionCommit {
                id: "tx-1".to_string(),
                expected_next_statement: 1,
                next_statement: 2,
                at: nervix_models::Timestamp::from_unix_nanos(7),
                result: Box::new(TransactionStepResult {
                    impact: second_impact,
                    result: TransactionCommandResult {
                        success: false,
                        message: "validation failed".to_string(),
                        diagnostics: Vec::new(),
                        already_existed: false,
                        admission: None,
                    },
                }),
                effect: None,
                completion: Some(TransactionOutcome::Failed {
                    failing_step: 1,
                    error: "validation failed".to_string(),
                }),
            },
        );
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::CompleteTransactionApplication {
                id: "tx-1".to_string(),
                expected_next_statement: 1,
                at: nervix_models::Timestamp::from_unix_nanos(8),
                application_failure: None,
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
        assert!(matches!(
            transaction.commit_results()[0].impact.actual().outcome,
            nervix_models::ExecutionStepOutcome::Applied
        ));
        assert!(matches!(
            transaction.commit_results()[1].impact.actual().outcome,
            nervix_models::ExecutionStepOutcome::Failed { .. }
        ));
        assert!(transaction.statements.is_empty());
        let terminal_report = state
            .transaction_reports
            .report(&preview)
            .assured("the failed tombstone retains its final report");
        assert_eq!(terminal_report.operations().len(), 2);
        assert!(matches!(
            terminal_report.execution_steps()[0].actual().outcome,
            nervix_models::ExecutionStepOutcome::Applied
        ));
        assert!(matches!(
            terminal_report.execution_steps()[1].actual().outcome,
            nervix_models::ExecutionStepOutcome::Failed { .. }
        ));
        assert_eq!(
            terminal_report.execution_steps()[0]
                .planned()
                .effects
                .topology
                .before
                .nodes
                .len(),
            1
        );
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
                mutation: None,
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
                mutation: None,
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
                root_checksum: "root-1".to_string(),
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
                ClusterNodeIdentity::new(
                    ClusterNodeName::parse("node-2")
                        .assured("the test node name is a valid identifier"),
                    ClusterNodeIncarnation::new(2),
                ),
            ),
            state: ResourceNodeState::Ready,
            root_checksum: Some("root-1".to_string()),
            last_verified_at: Some(nervix_models::Timestamp::from_unix_nanos(77)),
            source_node: Some(ClusterNodeIdentity::new(
                ClusterNodeName::parse("node-1")
                    .assured("the test node name is a valid identifier"),
                ClusterNodeIncarnation::new(1),
            )),
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
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::CompleteResourceUpload {
                key: Box::new(upload_key.clone()),
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
            ResourceUploadState::Completed {
                root_checksum: "root-1".to_string(),
                outcome_revision: 0,
            }
        );

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::BeginResourceUpload {
                key: Box::new(upload_key),
                root_checksum: "root-1".to_string(),
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
    fn apply_consensus_command_advances_node_admission_fences() {
        let mut state = StateMachineData::default();

        for incarnation in [7, 5, 9] {
            apply_consensus_command(
                &mut state,
                &ConsensusCommand::FenceNodeAdmission {
                    identity: node_identity("node-2", incarnation),
                },
            );
        }

        assert_eq!(
            state.node_admission_fences.get("node-2"),
            Some(&ClusterNodeIncarnation::new(9))
        );
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
