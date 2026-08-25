use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Cursor},
    ops::RangeBounds,
    path::Path,
    sync::Arc as StdArc,
    time::Duration,
};

use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use futures_util::StreamExt;
use nervix_models::{
    ClusterSchedule, Domain, DomainClockState, DomainId, DomainSchedule, DomainStartPoint,
    DomainState, DomainStatus, Identifier, ResourceNodeStatus, ResourceVersion,
    ResourceVersionStatus, Statement,
};
pub use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::{
    BasicNode, Config, Entry, LogId, Raft, RaftNetworkFactory, Snapshot, SnapshotMeta,
    StoredMembership, Vote,
    entry::{EntryPayload, RaftPayload},
    error::{RPCError, RaftError, StreamingError},
    network::{RPCOption, RaftNetworkV2},
    storage::{
        IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
    },
    type_config::{
        alias::{CommittedLeaderIdOf, LeaderIdOf},
        async_runtime::watch::WatchReceiver,
    },
};
use parking_lot::Mutex;
use reqwest::{Client as HttpClient, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{
    sync::{RwLock, broadcast, watch},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tracing::info;
use triomphe::Arc;

mod transaction;

pub use transaction::{
    FinishedTransaction, ReplicatedTransaction, TransactionCommandResult,
    TransactionCommitProgress, TransactionDiagnostic, TransactionMutationError,
    TransactionMutationResponse, TransactionOutcome, TransactionState, TransactionStatement,
    TransactionStepEffect, TransactionStepResult,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusCommand {
    ReplaceDomainSchedule {
        domain: Domain,
        schedule: Option<Box<DomainSchedule>>,
    },
    PutDomainAndSchedule {
        domain: Box<DomainState>,
        schedule: Option<Box<DomainSchedule>>,
    },
    PutDomain {
        domain: Box<DomainState>,
    },
    StartDomain {
        domain_id: DomainId,
        start: DomainStartPoint,
        clock: Option<DomainClockState>,
    },
    StopDomain {
        domain_id: DomainId,
    },
    PauseDomain {
        domain_id: DomainId,
    },
    ResumeDomain {
        domain_id: DomainId,
    },
    CreateUser {
        user: Box<UserCredentials>,
    },
    CreateResourceCatalog {
        identifier: String,
    },
    AdvanceResourceVersion {
        identifier: String,
    },
    PutResourceVersion {
        resource: Box<ResourceVersion>,
    },
    PutResourceReplica {
        replica: Box<ResourceNodeStatus>,
    },
    SetNodeCordoned {
        node_id: String,
        cordoned: bool,
    },
    OpenTransaction {
        transaction: Box<ReplicatedTransaction>,
        max_open_transactions: usize,
    },
    QueueTransactionStatement {
        id: String,
        owner: Identifier,
        at: nervix_models::Timestamp,
        statement: Box<TransactionStatement>,
        max_statements: usize,
        max_source_bytes: u64,
    },
    TouchTransaction {
        id: String,
        owner: Identifier,
        at: nervix_models::Timestamp,
    },
    StartTransactionCommit {
        id: String,
        owner: Identifier,
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
        owner: Identifier,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusResponse {
    Applied,
    Transaction(TransactionMutationResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserCredentials {
    pub name: Identifier,
    pub password_hash: String,
}

impl std::fmt::Display for ConsensusCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReplaceDomainSchedule { domain, schedule } => {
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
            Self::CreateUser { user } => write!(f, "create-user:{}", user.name.as_str()),
            Self::CreateResourceCatalog { identifier } => {
                write!(f, "create-resource-catalog:{identifier}")
            }
            Self::AdvanceResourceVersion { identifier } => {
                write!(f, "advance-resource-version:{identifier}")
            }
            Self::PutResourceVersion { resource } => write!(
                f,
                "put-resource-version:{}@{}",
                resource.id.identifier.as_str(),
                resource.id.version
            ),
            Self::PutResourceReplica { replica } => write!(
                f,
                "put-resource-replica:{}@{}:{}",
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
        NodeId = String,
        Node = BasicNode
);

pub type NervixRaft = Raft<TypeConfig, StdArc<FjallStore>>;
pub type NodeId = String;
pub type Node = BasicNode;
pub type LogIdOf = LogId<CommittedLeaderIdOf<TypeConfig>>;
pub type VoteOf = Vote<LeaderIdOf<TypeConfig>>;
pub type StoredMembershipOf = StoredMembership<CommittedLeaderIdOf<TypeConfig>, NodeId, Node>;
pub type SnapshotOf = Snapshot<CommittedLeaderIdOf<TypeConfig>, NodeId, Node, Cursor<Vec<u8>>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRelayHeader {
    pub vote: VoteOf,
    pub meta: SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, NodeId, Node>,
}

pub const RAFT_APPEND_ENTRIES_PATH: &str = "/raft/append-entries";
pub const RAFT_VOTE_PATH: &str = "/raft/vote";
pub const RAFT_INSTALL_SNAPSHOT_PATH: &str = "/raft/install-snapshot";
pub const RAFT_TRANSFER_LEADER_PATH: &str = "/raft/transfer-leader";
pub const RAFT_CONTENT_TYPE_CBOR: &str = "application/cbor";
pub const RAFT_CONTENT_TYPE_RELAY: &str = "application/vnd.nervix.raft-snapshot-stream";

const KEY_VOTE: &[u8] = b"vote";
const KEY_COMMITTED: &[u8] = b"committed";
const KEY_LAST_PURGED: &[u8] = b"last_purged";
const KEY_STATE_MACHINE: &[u8] = b"state_machine";
const KEY_SNAPSHOT: &[u8] = b"snapshot";
const KEY_CLUSTER_SCHEDULE: &[u8] = b"cluster_schedule";
const HEARTBEAT_ERROR_REPORT_MIN_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct ConsensusSettings {
    pub cluster_name: String,
    pub node_id: String,
    pub cluster_api_advertise_url: String,
    pub cluster_api_http_client: HttpClient,
    pub node_unavailability_timeout: Duration,
    pub raft_heartbeat_interval: Duration,
    pub raft_election_timeout_min: Duration,
    pub raft_election_timeout_max: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipNode {
    pub node_id: String,
    pub cluster_api_advertise_addr: String,
    pub grpc_advertise_addr: String,
    pub web_console_advertise_addr: String,
    pub interconnect_advertise_addr: String,
    pub interconnect_mode: String,
    pub interconnect_public_key: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GossipState {
    pub live_nodes: Vec<GossipNode>,
    pub dead_node_ids: BTreeSet<String>,
}

impl GossipState {
    fn admission_candidates(&self) -> impl Iterator<Item = &GossipNode> {
        self.live_nodes
            .iter()
            .filter(|node| !self.dead_node_ids.contains(&node.node_id))
    }
}

#[derive(Debug, Clone)]
pub struct ConsensusRuntimeState {
    pub revision: u64,
    pub schedule: ClusterSchedule,
    pub domains: BTreeMap<DomainId, DomainState>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct StateMachineData {
    last_applied_log_id: Option<LogIdOf>,
    last_membership: StoredMembershipOf,
    schedule: ClusterSchedule,
    domains: BTreeMap<DomainId, DomainState>,
    #[serde(default)]
    users: BTreeMap<Identifier, UserCredentials>,
    resources: ResourceVersionStatus,
    #[serde(default)]
    cordoned_node_ids: BTreeSet<String>,
    transactions: BTreeMap<String, ReplicatedTransaction>,
}

impl StateMachineData {
    fn replace_domain_schedule(&mut self, domain: &Domain, schedule: Option<&DomainSchedule>) {
        let Some(domain_schedule) = schedule else {
            self.schedule.domains.retain(|item| item.domain != *domain);
            return;
        };
        if let Some(existing) = self
            .schedule
            .domains
            .iter_mut()
            .find(|item| item.domain == *domain)
        {
            *existing = domain_schedule.clone();
        } else {
            self.schedule.domains.push(domain_schedule.clone());
            self.schedule
                .domains
                .sort_by(|left, right| left.domain.as_str().cmp(right.domain.as_str()));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredSnapshotData {
    meta: SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, NodeId, Node>,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct PeerHealth {
    unavailable_since: Option<Instant>,
    last_reported_unavailable_at: Option<Instant>,
}

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("failed to open raft database")]
    OpenDatabase,
    #[error("failed to open raft keyspace")]
    OpenKeyspace,
    #[error("failed to serialize raft value")]
    Serialize,
    #[error("failed to deserialize raft value")]
    Deserialize,
    #[error("raft startup failed")]
    Startup,
    #[error("failed to create raft client endpoint")]
    Endpoint,
    #[error("raft transport failed")]
    Transport,
    #[error("{0}")]
    Write(String),
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
pub enum ConsensusTransactionError {
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    #[error(transparent)]
    Mutation(#[from] TransactionMutationError),
    #[error("raft returned an invalid transaction response")]
    InvalidResponse,
}

#[derive(Clone)]
pub struct ConsensusHandle {
    raft: NervixRaft,
    store: StdArc<FjallStore>,
    local_node: GossipNode,
    cluster_api_http_client: HttpClient,
    node_unavailability_timeout: Duration,
    peer_health: Arc<RwLock<BTreeMap<String, PeerHealth>>>,
    events: broadcast::Sender<String>,
    metrics_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl ConsensusHandle {
    fn map_write_error(error: impl std::fmt::Display) -> ConsensusError {
        ConsensusError::Write(format!("raft write failed: {error}"))
    }

    pub async fn open(
        path: impl AsRef<Path>,
        settings: ConsensusSettings,
    ) -> Result<Self, ConsensusError> {
        let db = Database::builder(path)
            .open()
            .map_err(|_| ConsensusError::OpenDatabase)?;
        Self::from_database(db, settings).await
    }

    pub async fn from_database(
        db: Database,
        settings: ConsensusSettings,
    ) -> Result<Self, ConsensusError> {
        let store = StdArc::new(FjallStore::from_database(db)?);
        let config = StdArc::new(
            Config {
                cluster_name: settings.cluster_name,
                heartbeat_interval: u64::try_from(settings.raft_heartbeat_interval.as_millis())
                    .unwrap_or(u64::MAX),
                election_timeout_min: u64::try_from(settings.raft_election_timeout_min.as_millis())
                    .unwrap_or(u64::MAX),
                election_timeout_max: u64::try_from(settings.raft_election_timeout_max.as_millis())
                    .unwrap_or(u64::MAX),
                snapshot_policy: openraft::SnapshotPolicy::Never,
                ..Default::default()
            }
            .validate()
            .map_err(|_| ConsensusError::Startup)?,
        );

        let cluster_api_http_client = settings.cluster_api_http_client.clone();
        let network = NetworkFactory {
            http_client: cluster_api_http_client.clone(),
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
        let (events, _) = broadcast::channel(256);
        let metrics_raft = raft.clone();
        let event_tx = events.clone();
        let metrics_task = tokio::spawn(async move {
            let mut rx = metrics_raft.metrics();
            let mut last_transition = None;
            loop {
                tokio::task::consume_budget().await;
                if rx.changed().await.is_err() {
                    break;
                }
                let metrics = rx.borrow_watched().clone();
                let transition = (
                    format!("{:?}", metrics.state),
                    metrics.current_term,
                    metrics
                        .current_leader
                        .clone()
                        .unwrap_or_else(|| "(none)".to_string()),
                );
                if last_transition.as_ref() != Some(&transition) {
                    let summary = format!(
                        "raft transition: state={} leader={} term={} last_log_index={} \
                         last_applied={}",
                        transition.0,
                        transition.2,
                        transition.1,
                        metrics.last_log_index.unwrap_or_default(),
                        metrics
                            .last_applied
                            .map(|v| v.index.to_string())
                            .unwrap_or_else(|| "(none)".to_string())
                    );
                    info!("{summary}");
                    let _ = event_tx.send(summary.clone());
                    last_transition = Some(transition);
                }
            }
        });

        Ok(Self {
            raft,
            store,
            local_node: GossipNode {
                node_id: settings.node_id,
                cluster_api_advertise_addr: settings.cluster_api_advertise_url,
                grpc_advertise_addr: String::new(),
                web_console_advertise_addr: String::new(),
                interconnect_advertise_addr: String::new(),
                interconnect_mode: "http".to_string(),
                interconnect_public_key: String::new(),
            },
            cluster_api_http_client,
            node_unavailability_timeout: settings.node_unavailability_timeout,
            peer_health: Arc::new(RwLock::new(BTreeMap::new())),
            events,
            metrics_task: Arc::new(Mutex::new(Some(metrics_task))),
        })
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<String> {
        self.events.subscribe()
    }

    pub fn subscribe_schedule(&self) -> watch::Receiver<ClusterSchedule> {
        self.store.inner.schedule_tx.subscribe()
    }

    pub fn subscribe_domains(&self) -> watch::Receiver<BTreeMap<DomainId, DomainState>> {
        self.store.inner.domain_tx.subscribe()
    }

    pub fn subscribe_resources(&self) -> watch::Receiver<ResourceVersionStatus> {
        self.store.inner.resource_tx.subscribe()
    }

    pub fn subscribe_transactions(
        &self,
    ) -> watch::Receiver<BTreeMap<String, ReplicatedTransaction>> {
        self.store.inner.transaction_tx.subscribe()
    }

    pub async fn current_schedule(&self) -> ClusterSchedule {
        self.store.inner.state_machine.read().await.schedule.clone()
    }

    pub async fn current_domains(&self) -> BTreeMap<DomainId, DomainState> {
        self.store.inner.state_machine.read().await.domains.clone()
    }

    pub async fn current_transactions(&self) -> BTreeMap<String, ReplicatedTransaction> {
        self.store
            .inner
            .state_machine
            .read()
            .await
            .transactions
            .clone()
    }

    pub async fn current_transaction(&self, id: &str) -> Option<ReplicatedTransaction> {
        self.store
            .inner
            .state_machine
            .read()
            .await
            .transactions
            .get(id)
            .cloned()
    }

    pub async fn current_runtime_state(&self) -> ConsensusRuntimeState {
        let state = self.store.inner.state_machine.read().await;
        ConsensusRuntimeState {
            revision: state
                .last_applied_log_id
                .as_ref()
                .map_or(0, |log_id| log_id.index),
            schedule: state.schedule.clone(),
            domains: state.domains.clone(),
        }
    }

    pub async fn current_domain(&self, domain_id: &DomainId) -> Option<DomainState> {
        self.store
            .inner
            .state_machine
            .read()
            .await
            .domains
            .get(domain_id)
            .cloned()
    }

    pub async fn current_users(&self) -> BTreeMap<Identifier, UserCredentials> {
        self.store.inner.state_machine.read().await.users.clone()
    }

    pub async fn current_user(&self, user: &Identifier) -> Option<UserCredentials> {
        self.store
            .inner
            .state_machine
            .read()
            .await
            .users
            .get(user)
            .cloned()
    }

    pub async fn current_resources(&self) -> ResourceVersionStatus {
        self.store
            .inner
            .state_machine
            .read()
            .await
            .resources
            .clone()
    }

    pub async fn cordoned_node_ids(&self) -> BTreeSet<String> {
        self.store
            .inner
            .state_machine
            .read()
            .await
            .cordoned_node_ids
            .clone()
    }

    pub async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
        let handle = self.metrics_task.lock().take();
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node.node_id
    }

    pub fn set_local_grpc_advertise_addr(&mut self, grpc_advertise_addr: String) {
        self.local_node.grpc_advertise_addr = grpc_advertise_addr;
    }

    pub fn raft(&self) -> NervixRaft {
        self.raft.clone()
    }

    pub async fn maybe_initialize(&self) -> Result<bool, ConsensusError> {
        if self.store.has_raft_state().await {
            return Ok(false);
        }

        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.local_node.node_id.clone(),
            BasicNode::new(self.local_node.cluster_api_advertise_addr.clone()),
        );
        self.raft
            .initialize(nodes)
            .await
            .map(|_| ())
            .map_err(|_| ConsensusError::Startup)?;
        let message = format!(
            "raft initialized with single-node membership {}",
            self.local_node.node_id
        );
        info!("{message}");
        let _ = self.events.send(message);
        Ok(true)
    }

    pub async fn reconcile_nodes(&self, gossip: GossipState) -> Result<(), ConsensusError> {
        let leader = self.raft.current_leader().await;
        if leader.as_ref() != Some(&self.local_node.node_id) {
            return Ok(());
        }

        let metrics = self.raft.metrics().borrow_watched().clone();
        let current_voters = metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect::<BTreeSet<_>>();
        let mut desired_voters = current_voters.clone();
        let mut added_learner = false;

        for node in gossip.admission_candidates() {
            if node.cluster_api_advertise_addr.is_empty() {
                continue;
            }

            let known_node = metrics
                .membership_config
                .membership()
                .get_node(&node.node_id)
                .cloned();
            if known_node.is_none()
                || known_node.as_ref().map(|known| &known.addr)
                    != Some(&node.cluster_api_advertise_addr)
            {
                let add_message = if known_node.is_some() {
                    format!(
                        "raft refreshing learner {} address to {}",
                        node.node_id, node.cluster_api_advertise_addr
                    )
                } else {
                    format!(
                        "raft adding learner {} at {}",
                        node.node_id, node.cluster_api_advertise_addr
                    )
                };
                info!("{add_message}");
                let _ = self.events.send(add_message);
                self.raft
                    .add_learner(
                        node.node_id.clone(),
                        BasicNode::new(node.cluster_api_advertise_addr.clone()),
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
            if node_id == &self.local_node.node_id {
                continue;
            }

            let Some(addr) = membership_nodes.get(node_id) else {
                continue;
            };

            let chitchat_unavailable = gossip.dead_node_ids.contains(node_id);
            let healthcheck_unavailable = self.ping_peer(addr).await.is_err();
            unavailable.push((
                node_id.clone(),
                chitchat_unavailable || healthcheck_unavailable,
            ));
        }

        {
            let mut peer_health = self.peer_health.write().await;

            for (node_id, is_unavailable) in unavailable {
                let entry = peer_health.entry(node_id.clone()).or_insert(PeerHealth {
                    unavailable_since: None,
                    last_reported_unavailable_at: None,
                });

                if is_unavailable {
                    let unavailable_since =
                        entry.unavailable_since.get_or_insert_with(Instant::now);
                    let should_report = unavailable_since.elapsed()
                        >= self.node_unavailability_timeout
                        && entry.last_reported_unavailable_at.is_none_or(|last| {
                            last.elapsed() >= HEARTBEAT_ERROR_REPORT_MIN_INTERVAL
                        });
                    if should_report {
                        let message = format!(
                            "raft peer {} remains unavailable for {:?} (threshold {:?})",
                            node_id,
                            unavailable_since.elapsed(),
                            self.node_unavailability_timeout
                        );
                        info!("{message}");
                        let _ = self.events.send(message);
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

        self.raft
            .change_membership(desired_voters.clone(), true)
            .await
            .map_err(|_| ConsensusError::Transport)?;
        let after = self
            .raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .voter_ids()
            .collect::<BTreeSet<_>>();
        if current_voters != after {
            let message = format!("raft membership updated: {:?}", after);
            info!("{message}");
            let _ = self.events.send(message);
        }
        Ok(())
    }

    pub async fn current_leader(&self) -> Option<String> {
        self.raft.current_leader().await
    }

    pub async fn replace_domain_schedule(
        &self,
        domain: Domain,
        schedule: Option<DomainSchedule>,
    ) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::ReplaceDomainSchedule {
                domain,
                schedule: schedule.map(Box::new),
            })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn put_domain(&self, domain: DomainState) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::PutDomain {
                domain: Box::new(domain),
            })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn put_domain_and_schedule(
        &self,
        domain: DomainState,
        schedule: Option<DomainSchedule>,
    ) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::PutDomainAndSchedule {
                domain: Box::new(domain),
                schedule: schedule.map(Box::new),
            })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn start_domain(
        &self,
        domain_id: DomainId,
        start: DomainStartPoint,
        clock: Option<DomainClockState>,
    ) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::StartDomain {
                domain_id,
                start,
                clock,
            })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn stop_domain(&self, domain_id: DomainId) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::StopDomain { domain_id })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn pause_domain(&self, domain_id: DomainId) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::PauseDomain { domain_id })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn resume_domain(&self, domain_id: DomainId) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::ResumeDomain { domain_id })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn create_user(&self, user: UserCredentials) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::CreateUser {
                user: Box::new(user),
            })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn allocate_resource_version(&self, identifier: &str) -> Result<u64, ConsensusError> {
        let resources = self.current_resources().await;
        if !resources
            .next_version_by_identifier
            .iter()
            .any(|(stored_identifier, _)| stored_identifier.as_str() == identifier)
        {
            return Err(ConsensusError::Write(format!(
                "resource '{identifier}' does not exist"
            )));
        }
        self.raft
            .client_write(ConsensusCommand::AdvanceResourceVersion {
                identifier: identifier.to_string(),
            })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)?;

        let resources = self.current_resources().await;
        Ok(resources
            .next_version_by_identifier
            .iter()
            .find_map(|(stored_identifier, next_version)| {
                (stored_identifier.as_str() == identifier).then_some(next_version.saturating_sub(1))
            })
            .unwrap_or(1))
    }

    pub async fn create_resource_catalog(&self, identifier: &str) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::CreateResourceCatalog {
                identifier: identifier.to_string(),
            })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn put_resource_version(
        &self,
        resource: ResourceVersion,
    ) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::PutResourceVersion {
                resource: Box::new(resource),
            })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn put_resource_replica(
        &self,
        replica: ResourceNodeStatus,
    ) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::PutResourceReplica {
                replica: Box::new(replica),
            })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    pub async fn set_node_cordoned(
        &self,
        node_id: String,
        cordoned: bool,
    ) -> Result<(), ConsensusError> {
        self.raft
            .client_write(ConsensusCommand::SetNodeCordoned { node_id, cordoned })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    async fn write_transaction(
        &self,
        command: ConsensusCommand,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(Self::map_write_error)?;
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
        owner: Identifier,
        at: nervix_models::Timestamp,
        statement: TransactionStatement,
        max_statements: usize,
        max_source_bytes: u64,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::QueueTransactionStatement {
            id,
            owner,
            at,
            statement: Box::new(statement),
            max_statements,
            max_source_bytes,
        })
        .await
    }

    pub async fn touch_transaction(
        &self,
        id: String,
        owner: Identifier,
        at: nervix_models::Timestamp,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::TouchTransaction { id, owner, at })
            .await
    }

    pub async fn start_transaction_commit(
        &self,
        id: String,
        owner: Identifier,
        at: nervix_models::Timestamp,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
        self.write_transaction(ConsensusCommand::StartTransactionCommit { id, owner, at })
            .await
    }

    pub async fn advance_transaction_commit(
        &self,
        id: String,
        expected_next_statement: usize,
        next_statement: usize,
        at: nervix_models::Timestamp,
        result: TransactionStepResult,
        effect: Option<TransactionStepEffect>,
        completion: Option<TransactionOutcome>,
    ) -> Result<ReplicatedTransaction, ConsensusTransactionError> {
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
        owner: Identifier,
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
        self.raft
            .client_write(ConsensusCommand::RemoveFinishedTransactions { finished_before })
            .await
            .map(|_| ())
            .map_err(Self::map_write_error)
    }

    async fn ping_peer(&self, target_addr: &str) -> Result<(), ConsensusError> {
        self.cluster_api_http_client
            .get(format!("{target_addr}/raft/ping"))
            .send()
            .await
            .map_err(|_| ConsensusError::Transport)
            .and_then(|response| {
                if response.status().is_success() {
                    Ok(())
                } else {
                    Err(ConsensusError::Transport)
                }
            })
    }

    pub async fn status_lines(&self) -> Vec<String> {
        let metrics = self.raft.metrics().borrow_watched().clone();
        let mut lines = Vec::new();
        lines.push(format!("raft.id: {}", self.local_node.node_id));
        lines.push(format!(
            "raft.current_leader: {}",
            metrics
                .current_leader
                .unwrap_or_else(|| "(none)".to_string())
        ));
        lines.push(format!("raft.current_term: {}", metrics.current_term));
        lines.push(format!("raft.state: {:?}", metrics.state));
        lines.push(format!(
            "raft.node_unavailability_timeout: {:?}",
            self.node_unavailability_timeout
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
        lines.push(format!(
            "raft.last_applied: {}",
            metrics
                .last_applied
                .map(|v| v.index.to_string())
                .unwrap_or_else(|| "(none)".to_string())
        ));
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

    pub async fn membership_nodes(&self) -> BTreeMap<String, String> {
        let metrics = self.raft.metrics().borrow_watched().clone();
        metrics
            .membership_config
            .nodes()
            .map(|(node_id, node)| (node_id.clone(), node.addr.clone()))
            .collect()
    }

    pub async fn membership_voter_ids(&self) -> BTreeSet<String> {
        let metrics = self.raft.metrics().borrow_watched().clone();
        metrics.membership_config.membership().voter_ids().collect()
    }

    pub async fn live_voter_ids(
        &self,
        live_node_ids: impl IntoIterator<Item = String>,
    ) -> Vec<String> {
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
        live_node_ids: impl IntoIterator<Item = String>,
    ) -> Vec<String> {
        let cordoned = self.cordoned_node_ids().await;
        self.live_voter_ids(live_node_ids)
            .await
            .into_iter()
            .filter(|node_id| !cordoned.contains(node_id))
            .collect()
    }

    pub async fn drop_node(&self, node_id: &str) -> Result<(), ConsensusError> {
        if node_id == self.local_node.node_id {
            return Err(ConsensusError::RemoveLocalLeader(node_id.to_string()));
        }

        let metrics = self.raft.metrics().borrow_watched().clone();
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

        let membership_change_timeout =
            self.node_unavailability_timeout.max(Duration::from_secs(5)) * 2;
        timeout(
            membership_change_timeout,
            self.raft.change_membership(desired_voters.clone(), false),
        )
        .await
        .map_err(|_| ConsensusError::MembershipChangeTimeout {
            operation: format!("remove node '{node_id}'"),
            timeout: membership_change_timeout,
        })?
        .map_err(|_| ConsensusError::Transport)?;

        let message = format!("raft node removed: {node_id}");
        info!("{message}");
        let _ = self.events.send(message);
        Ok(())
    }

    pub async fn append_entries(
        &self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RaftError<TypeConfig>> {
        self.raft.append_entries(req).await
    }

    pub async fn vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>> {
        self.raft.vote(req).await
    }

    pub async fn transfer_leader(
        &self,
        req: TransferLeaderRequest<TypeConfig>,
    ) -> Result<TransferLeaderResponse<TypeConfig>, openraft::error::Fatal<TypeConfig>> {
        self.raft.handle_transfer_leader(req).await
    }

    pub async fn transfer_leadership_to(
        &self,
        target_node_id: String,
    ) -> Result<(), openraft::error::Fatal<TypeConfig>> {
        self.raft.trigger().transfer_leader(target_node_id).await
    }

    pub async fn install_full_snapshot(
        &self,
        vote: VoteOf,
        meta: openraft::SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, NodeId, Node>,
        snapshot: Vec<u8>,
    ) -> Result<SnapshotResponse<TypeConfig>, openraft::error::Fatal<TypeConfig>> {
        self.raft
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
pub struct NetworkFactory {
    http_client: HttpClient,
}

#[derive(Clone)]
pub struct NetworkClient {
    target: String,
    http_client: HttpClient,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        Self::Network {
            target: node.addr.clone(),
            http_client: self.http_client.clone(),
        }
    }
}

fn io_error(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(err.to_string())
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, io::Error> {
    let mut out = Vec::new();
    ciborium::into_writer(value, &mut out).map_err(io_error)?;
    Ok(out)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, io::Error> {
    ciborium::from_reader(Cursor::new(bytes)).map_err(io_error)
}

fn encode_stream_frame(bytes: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + bytes.len());
    frame.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(bytes);
    frame
}

async fn read_response_bytes(
    response: reqwest::Response,
    context: &str,
) -> Result<Vec<u8>, io::Error> {
    let status = response.status();
    let bytes = response.bytes().await.map_err(io_error)?;
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        return Err(io_error(format!("{context} failed with {status}: {body}")));
    }
    Ok(bytes.to_vec())
}

async fn read_response_bytes_with_timeout(
    response: reqwest::Response,
    context: &str,
    timeout_duration: Duration,
) -> Result<Vec<u8>, io::Error> {
    timeout(timeout_duration, read_response_bytes(response, context))
        .await
        .map_err(|_| io_error(format!("{context} timed out after {timeout_duration:?}")))?
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
        let body = encode(&rpc).map_err(unreachable_err)?;
        let response = self
            .http_client
            .post(format!("{}{}", self.target, RAFT_APPEND_ENTRIES_PATH))
            .timeout(rpc_timeout)
            .header(reqwest::header::CONTENT_TYPE, RAFT_CONTENT_TYPE_CBOR)
            .body(body)
            .send()
            .await
            .map_err(unreachable_err)?;
        let body = read_response_bytes_with_timeout(response, "append_entries", rpc_timeout)
            .await
            .map_err(unreachable_err)?;
        Ok(decode(&body).map_err(unreachable_err)?)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let rpc_timeout = option.hard_ttl();
        let body = encode(&rpc).map_err(unreachable_err)?;
        let response = self
            .http_client
            .post(format!("{}{}", self.target, RAFT_VOTE_PATH))
            .timeout(rpc_timeout)
            .header(reqwest::header::CONTENT_TYPE, RAFT_CONTENT_TYPE_CBOR)
            .body(body)
            .send()
            .await
            .map_err(unreachable_err)?;
        let body = read_response_bytes_with_timeout(response, "vote", rpc_timeout)
            .await
            .map_err(unreachable_err)?;
        Ok(decode(&body).map_err(unreachable_err)?)
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
        std::mem::drop(cancel);
        let bytes = snapshot.snapshot.into_inner();
        let chunk_size = option.snapshot_chunk_size().unwrap_or(256 * 1024).max(1);
        let header = SnapshotRelayHeader {
            vote,
            meta: snapshot.meta,
        };
        let header = encode_stream_frame(
            &encode(&header)
                .map_err(unreachable_err)
                .map_err(StreamingError::from)?,
        );
        let stream = futures_util::stream::unfold(
            (Some(header), bytes, 0usize, chunk_size),
            |(header, bytes, offset, chunk_size)| async move {
                if let Some(header) = header {
                    return Some((
                        Ok::<Vec<u8>, io::Error>(header),
                        (None, bytes, offset, chunk_size),
                    ));
                }
                if offset >= bytes.len() {
                    return None;
                }

                let end = (offset + chunk_size).min(bytes.len());
                Some((
                    Ok::<Vec<u8>, io::Error>(bytes[offset..end].to_vec()),
                    (None, bytes, end, chunk_size),
                ))
            },
        );
        let response = self
            .http_client
            .post(format!("{}{}", self.target, RAFT_INSTALL_SNAPSHOT_PATH))
            .timeout(rpc_timeout)
            .header(reqwest::header::CONTENT_TYPE, RAFT_CONTENT_TYPE_RELAY)
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await
            .map_err(unreachable_err)
            .map_err(StreamingError::from)?;
        let body = read_response_bytes_with_timeout(response, "install_snapshot", rpc_timeout)
            .await
            .map_err(unreachable_err)
            .map_err(StreamingError::from)?;
        decode(&body)
            .map_err(unreachable_err)
            .map_err(StreamingError::from)
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        let rpc_timeout = option.hard_ttl();
        let body = encode(&req).map_err(unreachable_err)?;
        let response = self
            .http_client
            .post(format!("{}{}", self.target, RAFT_TRANSFER_LEADER_PATH))
            .timeout(rpc_timeout)
            .header(reqwest::header::CONTENT_TYPE, RAFT_CONTENT_TYPE_CBOR)
            .body(body)
            .send()
            .await
            .map_err(unreachable_err)?;
        let status = response.status();
        if status != StatusCode::OK {
            let body = timeout(rpc_timeout, response.bytes())
                .await
                .map_err(|_| {
                    io_error(format!(
                        "transfer_leader response timed out after {rpc_timeout:?}"
                    ))
                })
                .and_then(|result| result.map_err(io_error))
                .map_err(unreachable_err)?;
            return Err(RPCError::Unreachable(unreachable_err(io_error(format!(
                "transfer_leader failed with {}: {}",
                status,
                String::from_utf8_lossy(&body)
            )))));
        }
        let body = read_response_bytes_with_timeout(response, "transfer_leader", rpc_timeout)
            .await
            .map_err(unreachable_err)?;
        Ok(decode(&body).map_err(unreachable_err)?)
    }
}

struct StoreInner {
    _db: Database,
    logs: Keyspace,
    meta: Keyspace,
    sm: Keyspace,
    schedule: Keyspace,
    snapshot: Keyspace,
    state_machine: RwLock<StateMachineData>,
    current_snapshot: RwLock<Option<StoredSnapshotData>>,
    schedule_tx: watch::Sender<ClusterSchedule>,
    domain_tx: watch::Sender<BTreeMap<DomainId, DomainState>>,
    resource_tx: watch::Sender<ResourceVersionStatus>,
    transaction_tx: watch::Sender<BTreeMap<String, ReplicatedTransaction>>,
}

pub struct FjallStore {
    inner: Arc<StoreInner>,
}

impl FjallStore {
    fn from_database(db: Database) -> Result<Self, ConsensusError> {
        let logs = db
            .keyspace("raft_logs", KeyspaceCreateOptions::default)
            .map_err(|_| ConsensusError::OpenKeyspace)?;
        let meta = db
            .keyspace("raft_meta", KeyspaceCreateOptions::default)
            .map_err(|_| ConsensusError::OpenKeyspace)?;
        let sm = db
            .keyspace("raft_state_machine", KeyspaceCreateOptions::default)
            .map_err(|_| ConsensusError::OpenKeyspace)?;
        let schedule = db
            .keyspace("raft_schedule", KeyspaceCreateOptions::default)
            .map_err(|_| ConsensusError::OpenKeyspace)?;
        let snapshot = db
            .keyspace("raft_snapshot", KeyspaceCreateOptions::default)
            .map_err(|_| ConsensusError::OpenKeyspace)?;

        let mut state_machine: StateMachineData =
            load_value(&sm, KEY_STATE_MACHINE)?.unwrap_or_default();
        if state_machine.schedule.domains.is_empty()
            && let Some(schedule_state) = load_value(&schedule, KEY_CLUSTER_SCHEDULE)?
        {
            state_machine.schedule = schedule_state;
        }
        let current_snapshot = load_value(&snapshot, KEY_SNAPSHOT)?;
        let (schedule_tx, _) = watch::channel(state_machine.schedule.clone());
        let (domain_tx, _) = watch::channel(state_machine.domains.clone());
        let (resource_tx, _) = watch::channel(state_machine.resources.clone());
        let (transaction_tx, _) = watch::channel(state_machine.transactions.clone());

        Ok(Self {
            inner: Arc::new(StoreInner {
                _db: db,
                logs,
                meta,
                sm,
                schedule,
                snapshot,
                state_machine: RwLock::new(state_machine),
                current_snapshot: RwLock::new(current_snapshot),
                schedule_tx,
                domain_tx,
                resource_tx,
                transaction_tx,
            }),
        })
    }

    async fn has_raft_state(&self) -> bool {
        self.read_vote().await.ok().flatten().is_some()
            || self
                .inner
                .logs
                .iter()
                .next()
                .and_then(|v| v.into_inner().ok())
                .is_some()
    }

    fn log_key(index: u64) -> io::Result<Vec<u8>> {
        storekey::serialize(&index).map_err(io_error)
    }

    async fn read_last_purged(&self) -> io::Result<Option<LogIdOf>> {
        read_key(&self.inner.meta, KEY_LAST_PURGED)
    }

    async fn write_last_purged(&self, value: &Option<LogIdOf>) -> io::Result<()> {
        write_key(&self.inner.meta, KEY_LAST_PURGED, value)
    }

    async fn read_committed_value(&self) -> io::Result<Option<LogIdOf>> {
        read_key(&self.inner.meta, KEY_COMMITTED)
    }

    async fn write_committed_value(&self, value: &Option<LogIdOf>) -> io::Result<()> {
        write_key(&self.inner.meta, KEY_COMMITTED, value)
    }

    async fn read_vote(&self) -> io::Result<Option<VoteOf>> {
        read_key(&self.inner.meta, KEY_VOTE)
    }

    async fn write_vote(&self, vote: &VoteOf) -> io::Result<()> {
        write_key(&self.inner.meta, KEY_VOTE, vote)
    }
}

impl Clone for FjallStore {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl RaftLogReader<TypeConfig> for StdArc<FjallStore> {
    async fn try_get_log_entries<
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<<TypeConfig as openraft::RaftTypeConfig>::Entry>, io::Error> {
        let mut out = Vec::new();
        for item in self.inner.logs.iter() {
            let (key, value) = item.into_inner().map_err(io_error)?;
            let index: u64 = storekey::deserialize(&key).map_err(io_error)?;
            if !range.contains(&index) {
                continue;
            }
            out.push(decode::<
                Entry<CommittedLeaderIdOf<TypeConfig>, ConsensusCommand, NodeId, Node>,
            >(value.as_ref())?);
        }
        out.sort_by_key(|entry| entry.log_id.index);
        Ok(out)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf>, io::Error> {
        FjallStore::read_vote(self).await
    }
}

impl RaftLogStorage<TypeConfig> for StdArc<FjallStore> {
    type LogReader = StdArc<FjallStore>;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let mut last_log_id = self.read_last_purged().await?;
        for item in self.inner.logs.iter() {
            let (_, value) = item.into_inner().map_err(io_error)?;
            let entry: Entry<CommittedLeaderIdOf<TypeConfig>, ConsensusCommand, NodeId, Node> =
                decode(value.as_ref())?;
            last_log_id = Some(entry.log_id);
        }

        Ok(LogState {
            last_purged_log_id: self.read_last_purged().await?,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf) -> Result<(), io::Error> {
        self.write_vote(vote).await
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf>) -> Result<(), io::Error> {
        self.write_committed_value(&committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf>, io::Error> {
        self.read_committed_value().await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = <TypeConfig as openraft::RaftTypeConfig>::Entry>
            + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        for entry in entries {
            let key = FjallStore::log_key(entry.log_id.index)?;
            let bytes = encode(&entry)?;
            self.inner.logs.insert(key, bytes).map_err(io_error)?;
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf>) -> Result<(), io::Error> {
        let cut = last_log_id.clone().map(|v| v.index).unwrap_or(0);
        let mut to_delete = Vec::new();
        for item in self.inner.logs.iter() {
            let (key, _) = item.into_inner().map_err(io_error)?;
            let index: u64 = storekey::deserialize(&key).map_err(io_error)?;
            if last_log_id.is_none() || index > cut {
                to_delete.push(key);
            }
        }
        for key in to_delete {
            self.inner.logs.remove(key).map_err(io_error)?;
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf) -> Result<(), io::Error> {
        let mut to_delete = Vec::new();
        for item in self.inner.logs.iter() {
            let (key, _) = item.into_inner().map_err(io_error)?;
            let index: u64 = storekey::deserialize(&key).map_err(io_error)?;
            if index <= log_id.index {
                to_delete.push(key);
            }
        }
        for key in to_delete {
            self.inner.logs.remove(key).map_err(io_error)?;
        }
        self.write_last_purged(&Some(log_id)).await
    }
}

impl RaftStateMachine<TypeConfig> for StdArc<FjallStore> {
    type SnapshotData = Cursor<Vec<u8>>;

    type SnapshotBuilder = StdArc<FjallStore>;

    async fn applied_state(&mut self) -> Result<(Option<LogIdOf>, StoredMembershipOf), io::Error> {
        let state = self.inner.state_machine.read().await;
        Ok((
            state.last_applied_log_id.clone(),
            state.last_membership.clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures_util::Stream<
                Item = Result<openraft::storage::EntryResponder<TypeConfig>, io::Error>,
            > + Unpin
            + openraft::OptionalSend,
    {
        while let Some(item) = entries.next().await {
            tokio::task::consume_budget().await;
            let (entry, responder) = item?;
            let mut state = self.inner.state_machine.write().await;
            state.last_applied_log_id = Some(entry.log_id.clone());
            if let Some(membership) = entry.get_membership() {
                state.last_membership =
                    StoredMembership::new(Some(entry.log_id.clone()), membership);
            }
            if let EntryPayload::Normal(command) = &entry.payload {
                let applied = apply_consensus_command(&mut state, command);
                if applied.schedule_changed {
                    write_key(&self.inner.schedule, KEY_CLUSTER_SCHEDULE, &state.schedule)?;
                    let _ = self.inner.schedule_tx.send(state.schedule.clone());
                }
                if applied.domains_changed {
                    let _ = self.inner.domain_tx.send(state.domains.clone());
                }
                if applied.resources_changed {
                    let _ = self.inner.resource_tx.send(state.resources.clone());
                }
                if applied.transactions_changed {
                    let _ = self.inner.transaction_tx.send(state.transactions.clone());
                }
                write_key(&self.inner.sm, KEY_STATE_MACHINE, &*state)?;
                drop(state);
                if let Some(responder) = responder {
                    responder.send(applied.response);
                }
                continue;
            }
            write_key(&self.inner.sm, KEY_STATE_MACHINE, &*state)?;
            drop(state);
            if let Some(responder) = responder {
                responder.send(ConsensusResponse::Applied);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<CommittedLeaderIdOf<TypeConfig>, NodeId, Node>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let bytes = snapshot.into_inner();
        let stored: StateMachineData = decode(&bytes)?;
        {
            let mut state = self.inner.state_machine.write().await;
            *state = stored.clone();
        }
        write_key(&self.inner.sm, KEY_STATE_MACHINE, &stored)?;
        write_key(&self.inner.schedule, KEY_CLUSTER_SCHEDULE, &stored.schedule)?;
        let _ = self.inner.schedule_tx.send(stored.schedule.clone());
        let _ = self.inner.domain_tx.send(stored.domains.clone());
        let _ = self.inner.resource_tx.send(stored.resources.clone());
        let _ = self.inner.transaction_tx.send(stored.transactions.clone());
        let stored_snapshot = StoredSnapshotData {
            meta: meta.clone(),
            data: bytes,
        };
        write_key(&self.inner.snapshot, KEY_SNAPSHOT, &stored_snapshot)?;
        let mut current = self.inner.current_snapshot.write().await;
        *current = Some(stored_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf>, io::Error> {
        let snapshot = self.inner.current_snapshot.read().await.clone();
        Ok(snapshot.map(|stored| Snapshot {
            meta: stored.meta,
            snapshot: Cursor::new(stored.data),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StdArc<FjallStore> {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf, io::Error> {
        let state = self.inner.state_machine.read().await.clone();
        let meta = SnapshotMeta {
            last_log_id: state.last_applied_log_id.clone(),
            last_membership: state.last_membership.clone(),
        };
        let data = encode(&state)?;
        let stored_snapshot = StoredSnapshotData {
            meta: meta.clone(),
            data: data.clone(),
        };
        write_key(&self.inner.snapshot, KEY_SNAPSHOT, &stored_snapshot)?;
        let mut current = self.inner.current_snapshot.write().await;
        *current = Some(stored_snapshot);
        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
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

    fn transaction(
        result: Result<ReplicatedTransaction, TransactionMutationError>,
        changes: StateMachineChanges,
    ) -> Self {
        Self {
            response: ConsensusResponse::Transaction(TransactionMutationResponse { result }),
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
        ConsensusCommand::ReplaceDomainSchedule { domain, schedule } => {
            state.replace_domain_schedule(domain, schedule.as_deref());
            changes.schedule_changed = true;
        }
        ConsensusCommand::PutDomainAndSchedule { domain, schedule } => {
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
        } => {
            if let Some(domain) = state.domains.get_mut(domain_id) {
                domain.status = DomainStatus::Running;
                domain.start_version = domain.start_version.saturating_add(1);
                domain.last_start = start.clone();
                domain.clock = clock.clone();
                changes.domains_changed = true;
            }
        }
        ConsensusCommand::StopDomain { domain_id } => {
            if let Some(domain) = state.domains.get_mut(domain_id) {
                domain.status = DomainStatus::Stopped;
                domain.clock = None;
                changes.domains_changed = true;
            }
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
        ConsensusCommand::CreateUser { user } => {
            state
                .users
                .entry(user.name.clone())
                .or_insert_with(|| user.as_ref().clone());
        }
        ConsensusCommand::CreateResourceCatalog { identifier } => {
            ensure_resource_catalog(&mut state.resources, identifier);
            changes.resources_changed = true;
        }
        ConsensusCommand::AdvanceResourceVersion { identifier } => {
            advance_resource_version(&mut state.resources, identifier);
            changes.resources_changed = true;
        }
        ConsensusCommand::PutResourceVersion { resource } => {
            upsert_resource_version(&mut state.resources, resource.as_ref().clone());
            changes.resources_changed = true;
        }
        ConsensusCommand::PutResourceReplica { replica } => {
            upsert_resource_replica(&mut state.resources, replica.as_ref().clone());
            changes.resources_changed = true;
        }
        ConsensusCommand::SetNodeCordoned { node_id, cordoned } => {
            if *cordoned {
                state.cordoned_node_ids.insert(node_id.clone());
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
            at,
            statement,
            max_statements,
            max_source_bytes,
        } => {
            let result = mutate_transaction(state, id, |transaction| {
                transaction.queue(
                    owner,
                    *at,
                    statement.as_ref().clone(),
                    *max_statements,
                    *max_source_bytes,
                )
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
                    state
                        .transactions
                        .get(id)
                        .expect("transaction was read from replicated state"),
                    *expected_next_statement,
                    *next_statement,
                    result,
                    effect,
                    completion.as_ref(),
                ) {
                    return AppliedConsensusCommand::transaction(Err(error), changes);
                }
                apply_transaction_step_effect(state, effect, &mut changes);
            } else if let Err(error) = validate_transaction_step_without_effect(
                state
                    .transactions
                    .get(id)
                    .expect("transaction was read from replicated state"),
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
        Statement::CreateDomain(create) => create.if_not_exists && result.result.already_existed,
        Statement::CreateUser(create) => create.if_not_exists && result.result.already_existed,
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
            statements.iter().all(|statement| {
                statement.domain.as_ref() == Some(domain) && statement.statement.is_model_mutation()
            })
        }
        TransactionStepEffect::PutDomainAndSchedule {
            expected_domain,
            domain,
            ..
        } => {
            statements.len() == 1
                && statements[0].domain.as_ref() == Some(&domain.id)
                && expected_domain.id == domain.id
                && matches!(statements[0].statement, Statement::AlterDomain(_))
        }
        TransactionStepEffect::PutDomain { domain } => {
            statements.len() == 1
                && matches!(
                    &statements[0].statement,
                    Statement::CreateDomain(create) if create.id == domain.id
                )
        }
        TransactionStepEffect::StartDomain { domain_id, .. } => {
            statements.len() == 1
                && statements[0].domain.as_ref() == Some(domain_id)
                && matches!(statements[0].statement, Statement::StartDomain(_))
        }
        TransactionStepEffect::StopDomain { domain_id, .. } => {
            statements.len() == 1
                && statements[0].domain.as_ref() == Some(domain_id)
                && matches!(statements[0].statement, Statement::StopDomain(_))
        }
        TransactionStepEffect::CreateUser { user } => {
            statements.len() == 1
                && matches!(
                    &statements[0].statement,
                    Statement::CreateUser(create) if create.name == user.name
                )
        }
        TransactionStepEffect::CreateResourceCatalog { identifier } => {
            statements.len() == 1
                && matches!(
                    &statements[0].statement,
                    Statement::CreateResource(create)
                        if create.identifier.as_str() == identifier
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
        TransactionStepEffect::PutDomain { domain } => state
            .domains
            .contains_key(&domain.id)
            .then(|| format!("domain '{}' now exists", domain.id.as_str())),
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
        TransactionStepEffect::CreateUser { user } => state
            .users
            .contains_key(&user.name)
            .then(|| format!("user '{}' now exists", user.name.as_str())),
        TransactionStepEffect::CreateResourceCatalog { identifier } => state
            .resources
            .next_version_by_identifier
            .iter()
            .any(|(known, _)| known.as_str() == identifier)
            .then(|| format!("resource '{identifier}' now exists")),
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
        TransactionStepEffect::PutDomain { domain } => {
            state
                .domains
                .insert(domain.id.clone(), domain.as_ref().clone());
            changes.domains_changed = true;
        }
        TransactionStepEffect::StartDomain {
            domain_id,
            start,
            clock,
            ..
        } => {
            if let Some(domain) = state.domains.get_mut(domain_id) {
                domain.status = DomainStatus::Running;
                domain.start_version = domain.start_version.saturating_add(1);
                domain.last_start = start.clone();
                domain.clock = clock.clone();
                changes.domains_changed = true;
            }
        }
        TransactionStepEffect::StopDomain { domain_id, .. } => {
            if let Some(domain) = state.domains.get_mut(domain_id) {
                domain.status = DomainStatus::Stopped;
                domain.clock = None;
                changes.domains_changed = true;
            }
        }
        TransactionStepEffect::CreateUser { user } => {
            state
                .users
                .entry(user.name.clone())
                .or_insert_with(|| user.as_ref().clone());
        }
        TransactionStepEffect::CreateResourceCatalog { identifier } => {
            ensure_resource_catalog(&mut state.resources, identifier);
            changes.resources_changed = true;
        }
    }
}

fn ensure_resource_catalog(resources: &mut ResourceVersionStatus, identifier: &str) {
    let Ok(identifier) = nervix_models::Identifier::parse(identifier) else {
        return;
    };
    if let Err(index) = resources
        .next_version_by_identifier
        .binary_search_by(|(stored_identifier, _)| stored_identifier.cmp(&identifier))
    {
        resources
            .next_version_by_identifier
            .mutate_vec(|entries| entries.insert(index, (identifier, 1)));
    }
}

fn advance_resource_version(resources: &mut ResourceVersionStatus, identifier: &str) {
    let Ok(identifier) = nervix_models::Identifier::parse(identifier) else {
        return;
    };
    match resources
        .next_version_by_identifier
        .binary_search_by(|(stored_identifier, _)| stored_identifier.cmp(&identifier))
    {
        Ok(index) => {
            resources.next_version_by_identifier.mutate_vec(|entries| {
                entries[index].1 = entries[index].1.saturating_add(1);
            });
        }
        Err(index) => {
            resources
                .next_version_by_identifier
                .mutate_vec(|entries| entries.insert(index, (identifier, 2)));
        }
    }
}

fn upsert_resource_version(resources: &mut ResourceVersionStatus, version: ResourceVersion) {
    match resources
        .versions
        .binary_search_by(|existing| existing.id.cmp(&version.id))
    {
        Ok(index) => {
            resources.versions.mutate_vec(|versions| {
                versions[index] = version;
            });
        }
        Err(index) => {
            resources
                .versions
                .mutate_vec(|versions| versions.insert(index, version));
        }
    }
}

fn upsert_resource_replica(resources: &mut ResourceVersionStatus, replica: ResourceNodeStatus) {
    match resources
        .replicas
        .binary_search_by(|existing| existing.key.cmp(&replica.key))
    {
        Ok(index) => {
            resources.replicas.mutate_vec(|replicas| {
                replicas[index] = replica;
            });
        }
        Err(index) => {
            resources
                .replicas
                .mutate_vec(|replicas| replicas.insert(index, replica));
        }
    }
}

fn load_value<T: DeserializeOwned>(
    keyspace: &Keyspace,
    key: &[u8],
) -> Result<Option<T>, ConsensusError> {
    let Some(bytes) = keyspace
        .get(key)
        .map_err(|_| ConsensusError::OpenKeyspace)?
    else {
        return Ok(None);
    };
    decode(bytes.as_ref())
        .map(Some)
        .map_err(|_| ConsensusError::Deserialize)
}

fn read_key<T: DeserializeOwned>(keyspace: &Keyspace, key: &[u8]) -> io::Result<Option<T>> {
    let Some(bytes) = keyspace.get(key).map_err(io_error)? else {
        return Ok(None);
    };
    decode(bytes.as_ref())
}

fn write_key<T: Serialize>(keyspace: &Keyspace, key: &[u8], value: &T) -> io::Result<()> {
    keyspace.insert(key, encode(value)?).map_err(io_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc as StdArc};

    use fjall::Database;
    use nervix_models::{
        Domain, DomainConfig, DomainPace, DomainSchedule, DomainStartPoint, DomainState,
        DomainStatus, Identifier, ResourceId, ResourceNodeState, ResourceNodeStatus,
        ResourceReplicaKey, ResourceVersion, ResourceVersionStatus, Statement,
    };
    use openraft::{
        SnapshotMeta,
        storage::{RaftSnapshotBuilder, RaftStateMachine},
    };
    use tempfile::tempdir;

    use super::{
        ClusterSchedule, ConsensusCommand, ConsensusResponse, FjallStore, GossipNode, GossipState,
        KEY_CLUSTER_SCHEDULE, KEY_SNAPSHOT, SnapshotRelayHeader, StateMachineData,
        StoredMembershipOf, TransactionCommandResult, TransactionMutationError, TransactionOutcome,
        TransactionStatement, TransactionStepEffect, TransactionStepResult, TypeConfig,
        UserCredentials, apply_consensus_command, decode, encode, encode_stream_frame, io_error,
        load_value, read_key, write_key,
    };
    use crate::{ConsensusError, ReplicatedTransaction, VoteOf};

    fn domain(raw: &str) -> Domain {
        Domain::try_from(raw).expect("valid domain")
    }

    #[test]
    fn dead_gossip_nodes_are_not_membership_admission_candidates() {
        let state = GossipState {
            live_nodes: vec![
                GossipNode {
                    node_id: "node-2".to_string(),
                    cluster_api_advertise_addr: "http://node-2".to_string(),
                    grpc_advertise_addr: String::new(),
                    web_console_advertise_addr: String::new(),
                    interconnect_advertise_addr: String::new(),
                    interconnect_mode: String::new(),
                    interconnect_public_key: String::new(),
                },
                GossipNode {
                    node_id: "node-3".to_string(),
                    cluster_api_advertise_addr: "http://node-3".to_string(),
                    grpc_advertise_addr: String::new(),
                    web_console_advertise_addr: String::new(),
                    interconnect_advertise_addr: String::new(),
                    interconnect_mode: String::new(),
                    interconnect_public_key: String::new(),
                },
            ],
            dead_node_ids: ["node-3".to_string()].into_iter().collect(),
        };

        assert_eq!(
            state
                .admission_candidates()
                .map(|node| node.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node-2"]
        );
    }

    fn domain_schedule(raw: &str) -> DomainSchedule {
        DomainSchedule {
            domain: domain(raw),
            nodes: Vec::new(),
            placement_groups: Vec::new(),
        }
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

    fn resource_version(identifier: &str, version: u64) -> ResourceVersion {
        ResourceVersion {
            id: ResourceId::new(
                Identifier::parse(identifier).expect("valid identifier"),
                version,
            ),
            root_checksum: format!("root-{version}"),
            manifest_checksum: format!("manifest-{version}"),
            file_count: 2,
            total_bytes: 128,
            created_at: nervix_models::Timestamp::from_unix_nanos(42),
            created_by_node: "node-1".to_string(),
        }
    }

    fn temp_database() -> Database {
        let dir = tempdir().expect("tempdir");
        Database::builder(dir.keep())
            .open()
            .expect("database should open")
    }

    #[test]
    fn consensus_command_display_distinguishes_replace_and_clear() {
        let replace = ConsensusCommand::ReplaceDomainSchedule {
            domain: domain("tenant"),
            schedule: Some(Box::new(domain_schedule("tenant"))),
        };
        let clear = ConsensusCommand::ReplaceDomainSchedule {
            domain: domain("tenant"),
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
            schedule: Some(Box::new(domain_schedule("tenant"))),
        };

        let bytes = encode(&command).expect("command should encode");
        let decoded: ConsensusCommand = decode(&bytes).expect("command should decode");
        assert_eq!(decoded, command);

        let err = decode::<ConsensusCommand>(b"not-cbor").expect_err("invalid bytes must fail");
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn snapshot_relay_header_roundtrips_in_length_delimited_cbor_frame() {
        let header = SnapshotRelayHeader {
            vote: VoteOf::new(7, "node-1".to_string()),
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembershipOf::default(),
            },
        };

        let payload = encode(&header).expect("snapshot relay header should encode");
        let frame = encode_stream_frame(&payload);
        let length = u32::from_be_bytes(
            frame[..4]
                .try_into()
                .expect("snapshot relay frame has a length prefix"),
        ) as usize;
        assert_eq!(length, payload.len());
        let decoded: SnapshotRelayHeader =
            decode(&frame[4..]).expect("snapshot relay header should decode");
        assert_eq!(decoded, header);
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
            schedule: ClusterSchedule {
                domains: vec![domain_schedule("zeta")],
            },
            ..Default::default()
        };

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                domain: domain("alpha"),
                schedule: Some(Box::new(domain_schedule("alpha"))),
            },
        );
        assert_eq!(
            state
                .schedule
                .domains
                .iter()
                .map(|item| item.domain.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                domain: domain("alpha"),
                schedule: Some(Box::new(domain_schedule("alpha"))),
            },
        );
        assert_eq!(state.schedule.domains.len(), 2);

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ReplaceDomainSchedule {
                domain: domain("zeta"),
                schedule: None,
            },
        );
        assert_eq!(
            state
                .schedule
                .domains
                .iter()
                .map(|item| item.domain.as_str())
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
                domain: Box::new(domain_state.clone()),
                schedule: Some(Box::new(schedule.clone())),
            },
        );

        assert_eq!(state.domains.get(&domain("tenant")), Some(&domain_state));
        assert_eq!(state.schedule.domain(&domain("tenant")), Some(&schedule));
    }

    #[test]
    fn transaction_step_effect_and_progress_are_applied_once() {
        let owner = Identifier::parse("app_user").expect("valid owner");
        let domain_id = domain("tenant");
        let mut stopped = running_domain_state("tenant");
        stopped.status = DomainStatus::Stopped;
        stopped.start_version = 0;
        let mut state = StateMachineData::default();
        state.domains.insert(domain_id.clone(), stopped);

        let transaction = ReplicatedTransaction::open(
            "tx-1".to_string(),
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
                    at: nervix_models::Timestamp::from_unix_nanos(
                        i64::try_from(at).unwrap_or_default().saturating_add(2),
                    ),
                    statement: Box::new(TransactionStatement {
                        source: "statement".to_string(),
                        statement,
                        domain: Some(domain_id.clone()),
                    }),
                    max_statements: 4,
                    max_source_bytes: 1024,
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
                result: TransactionCommandResult {
                    success: true,
                    message: "started".to_string(),
                    diagnostics: Vec::new(),
                    already_existed: false,
                    results: Vec::new(),
                },
            }),
            effect: Some(Box::new(TransactionStepEffect::StartDomain {
                domain_id: domain_id.clone(),
                expected_start_version: 0,
                start: DomainStartPoint::Resume,
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
                    result: TransactionCommandResult {
                        success: false,
                        message: "validation failed".to_string(),
                        diagnostics: Vec::new(),
                        already_existed: false,
                        results: Vec::new(),
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
            resources: ResourceVersionStatus::default(),
            ..Default::default()
        };

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdvanceResourceVersion {
                identifier: "fraud_model".to_string(),
            },
        );
        assert_eq!(
            state
                .resources
                .next_version_by_identifier
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![(
                Identifier::parse("fraud_model").expect("valid identifier"),
                2
            )]
        );

        let version = resource_version("fraud_model", 1);
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::PutResourceVersion {
                resource: Box::new(version.clone()),
            },
        );
        assert_eq!(
            state.resources.versions.iter().cloned().collect::<Vec<_>>(),
            vec![version.clone()]
        );

        let replica = ResourceNodeStatus {
            key: ResourceReplicaKey::new(
                Identifier::parse("fraud_model").expect("valid identifier"),
                1,
                "node-2",
            ),
            state: ResourceNodeState::Ready,
            root_checksum: Some("root-1".to_string()),
            last_verified_at: Some(nervix_models::Timestamp::from_unix_nanos(77)),
            source_node_id: Some("node-1".to_string()),
            error: None,
        };
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::PutResourceReplica {
                replica: Box::new(replica.clone()),
            },
        );
        assert_eq!(
            state.resources.replicas.iter().cloned().collect::<Vec<_>>(),
            vec![replica]
        );
    }

    #[test]
    fn apply_consensus_command_tracks_cordoned_nodes() {
        let mut state = StateMachineData::default();

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::SetNodeCordoned {
                node_id: "node-2".to_string(),
                cordoned: true,
            },
        );
        assert!(state.cordoned_node_ids.contains("node-2"));

        apply_consensus_command(
            &mut state,
            &ConsensusCommand::SetNodeCordoned {
                node_id: "node-2".to_string(),
                cordoned: false,
            },
        );
        assert!(!state.cordoned_node_ids.contains("node-2"));
    }

    #[test]
    fn apply_consensus_command_tracks_users_without_overwriting_existing_password_hash() {
        let mut state = StateMachineData::default();
        let name = Identifier::parse("app_user").expect("valid identifier");

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

    #[tokio::test]
    async fn store_restores_legacy_schedule_and_detects_saved_vote() {
        let db = temp_database();
        let schedule_keyspace = db
            .keyspace("raft_schedule", fjall::KeyspaceCreateOptions::default)
            .expect("schedule keyspace");
        let schedule = ClusterSchedule {
            domains: vec![domain_schedule("tenant")],
        };
        write_key(&schedule_keyspace, KEY_CLUSTER_SCHEDULE, &schedule).expect("write schedule");

        let store = FjallStore::from_database(db).expect("store should open");
        assert_eq!(
            store
                .inner
                .state_machine
                .read()
                .await
                .schedule
                .domains
                .iter()
                .map(|item| item.domain.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant"]
        );
        assert!(!store.has_raft_state().await);

        store
            .write_vote(&VoteOf::new(7, "node-1".to_string()))
            .await
            .expect("vote should persist");
        assert!(store.has_raft_state().await);
    }

    #[tokio::test]
    async fn snapshot_build_and_install_roundtrip_preserves_state() {
        let tenant = domain("tenant");
        let state = StateMachineData {
            schedule: ClusterSchedule {
                domains: vec![domain_schedule("tenant")],
            },
            domains: [(tenant, running_domain_state("tenant"))]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let mut source = StdArc::new(
            FjallStore::from_database(temp_database()).expect("source store should open"),
        );
        *source.inner.state_machine.write().await = state.clone();

        let built = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut source)
            .await
            .expect("snapshot should build");
        let meta = built.meta.clone();
        let bytes = built.snapshot.into_inner();
        let mut target = StdArc::new(
            FjallStore::from_database(temp_database()).expect("target store should open"),
        );

        RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut target,
            &meta,
            Cursor::new(bytes.clone()),
        )
        .await
        .expect("snapshot should install");

        assert_eq!(*target.inner.state_machine.read().await, state);
        let persisted: StateMachineData = read_key(&target.inner.sm, super::KEY_STATE_MACHINE)
            .expect("persisted state should read")
            .expect("persisted state should exist");
        assert_eq!(persisted, state);
        let current = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut target)
            .await
            .expect("current snapshot should read")
            .expect("current snapshot should exist");
        assert_eq!(current.meta, meta);
        assert_eq!(current.snapshot.into_inner(), bytes);
        assert!(
            read_key::<super::StoredSnapshotData>(&target.inner.snapshot, KEY_SNAPSHOT)
                .expect("stored snapshot should read")
                .is_some()
        );
    }

    #[test]
    fn key_helpers_roundtrip_and_missing_values() {
        let db = temp_database();
        let keyspace = db
            .keyspace("test_keys", fjall::KeyspaceCreateOptions::default)
            .expect("keyspace");

        write_key(&keyspace, b"name", &"raft").expect("write should succeed");

        let read_back: Option<String> = read_key(&keyspace, b"name").expect("read should succeed");
        assert_eq!(read_back.as_deref(), Some("raft"));

        let loaded: Option<String> = load_value(&keyspace, b"name").expect("load should succeed");
        assert_eq!(loaded.as_deref(), Some("raft"));

        let missing: Option<String> =
            load_value(&keyspace, b"missing").expect("missing load should succeed");
        assert_eq!(missing, None);

        keyspace
            .insert(b"broken", b"not-cbor")
            .expect("raw insert should succeed");
        let err = load_value::<String>(&keyspace, b"broken").expect_err("invalid value must fail");
        assert!(matches!(err, ConsensusError::Deserialize));
    }
}
