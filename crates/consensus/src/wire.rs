//! The current rkyv representation of Raft messages carried by the interconnect.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Lossless conversion between OpenRaft values and bounded rkyv request records.
//! - **Depends on.** OpenRaft, the replicated command vocabulary, and the interconnect request
//!   primitive.
//! - **Must not know.** HTTP/2 framing, peer addresses, or control-plane policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::Duration,
};

use nervix_interconnect::{InterconnectRequest, PoolClass, RequestSubquota};
use nervix_models::ClusterNodeName;
use openraft::{
    BasicNode, Entry, LogId, Membership, SnapshotMeta, StoredMembership, Vote, entry::EntryPayload,
    raft::TransferLeaderError,
};
use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

use super::{
    AppendEntriesRequest, AppendEntriesResponse, ConsensusCommand, EntryOf, LogIdOf,
    SnapshotResponse, StoredMembershipOf, TransferLeaderRequest, TransferLeaderResponse,
    TypeConfig, VoteOf, VoteRequest, VoteResponse,
};

type SnapshotMetaOf =
    SnapshotMeta<super::CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, BasicNode>;

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq, Error)]
pub(crate) enum ConsensusRequestError {
    #[error("the authenticated request origin is invalid: {0}")]
    InvalidOrigin(String),
    #[error("the rkyv request could not be converted into a Raft request: {0}")]
    InvalidRequest(String),
    #[error("Raft rejected the request: {0}")]
    Raft(String),
    #[error("the snapshot transfer failed: {0}")]
    SnapshotTransfer(String),
}

impl ConsensusRequestError {
    pub(crate) fn invalid_origin(error: impl std::fmt::Display) -> Self {
        Self::InvalidOrigin(error.to_string())
    }

    pub(crate) fn invalid_request(error: impl std::fmt::Display) -> Self {
        Self::InvalidRequest(error.to_string())
    }

    pub(crate) fn raft(error: impl std::fmt::Display) -> Self {
        Self::Raft(error.to_string())
    }

    pub(crate) fn snapshot_transfer(error: impl std::fmt::Display) -> Self {
        Self::SnapshotTransfer(error.to_string())
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct VoteRecord {
    term: u64,
    node_id: ClusterNodeName,
    committed: bool,
}

impl From<VoteOf> for VoteRecord {
    fn from(value: VoteOf) -> Self {
        Self {
            term: value.leader_id.term,
            node_id: value.leader_id.node_id,
            committed: value.committed,
        }
    }
}

impl VoteRecord {
    fn into_vote(self) -> VoteOf {
        if self.committed {
            Vote::new_committed(self.term, self.node_id)
        } else {
            Vote::new(self.term, self.node_id)
        }
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LogIdRecord {
    term: u64,
    node_id: ClusterNodeName,
    index: u64,
}

impl From<LogIdOf> for LogIdRecord {
    fn from(value: LogIdOf) -> Self {
        Self {
            term: value.leader_id.term,
            node_id: value.leader_id.node_id,
            index: value.index,
        }
    }
}

impl LogIdRecord {
    fn into_log_id(self) -> LogIdOf {
        LogId::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: self.term,
                node_id: self.node_id,
            },
            self.index,
        )
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct MembershipNodeRecord {
    node_id: ClusterNodeName,
    address: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct MembershipRecord {
    configurations: Vec<Vec<ClusterNodeName>>,
    nodes: Vec<MembershipNodeRecord>,
}

impl MembershipRecord {
    fn from_membership(value: &Membership<ClusterNodeName, BasicNode>) -> Self {
        Self {
            configurations: value
                .get_joint_config()
                .iter()
                .map(|configuration| configuration.iter().cloned().collect())
                .collect(),
            nodes: value
                .nodes()
                .map(|(node_id, node)| MembershipNodeRecord {
                    node_id: node_id.clone(),
                    address: node.addr.clone(),
                })
                .collect(),
        }
    }

    fn into_membership(self) -> io::Result<Membership<ClusterNodeName, BasicNode>> {
        let configurations = self
            .configurations
            .into_iter()
            .map(|configuration| configuration.into_iter().collect::<BTreeSet<_>>())
            .collect();
        let nodes = self
            .nodes
            .into_iter()
            .map(|node| (node.node_id, BasicNode::new(node.address)))
            .collect::<BTreeMap<_, _>>();
        Membership::new(configurations, nodes).map_err(io::Error::other)
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
enum EntryPayloadRecord {
    Blank,
    Normal(ConsensusCommand),
    Membership(MembershipRecord),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct EntryRecord {
    log_id: LogIdRecord,
    payload: EntryPayloadRecord,
}

impl EntryRecord {
    fn from_entry(value: EntryOf<TypeConfig>) -> Self {
        let payload = match value.payload {
            EntryPayload::Blank => EntryPayloadRecord::Blank,
            EntryPayload::Normal(command) => EntryPayloadRecord::Normal(command),
            EntryPayload::Membership(membership) => {
                EntryPayloadRecord::Membership(MembershipRecord::from_membership(&membership))
            }
        };
        Self {
            log_id: value.log_id.into(),
            payload,
        }
    }

    fn into_entry(self) -> io::Result<EntryOf<TypeConfig>> {
        let payload = match self.payload {
            EntryPayloadRecord::Blank => EntryPayload::Blank,
            EntryPayloadRecord::Normal(command) => EntryPayload::Normal(command),
            EntryPayloadRecord::Membership(membership) => {
                EntryPayload::Membership(membership.into_membership()?)
            }
        };
        Ok(Entry {
            log_id: self.log_id.into_log_id(),
            payload,
        })
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AppendEntriesRecord {
    vote: VoteRecord,
    previous_log_id: Option<LogIdRecord>,
    entries: Vec<EntryRecord>,
    leader_commit: Option<LogIdRecord>,
}

impl AppendEntriesRecord {
    pub(crate) fn from_request(value: AppendEntriesRequest<TypeConfig>) -> Self {
        Self {
            vote: value.vote.into(),
            previous_log_id: value.prev_log_id.map(Into::into),
            entries: value
                .entries
                .into_iter()
                .map(EntryRecord::from_entry)
                .collect(),
            leader_commit: value.leader_commit.map(Into::into),
        }
    }

    pub(crate) fn is_heartbeat(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn origin_node_id(&self) -> &ClusterNodeName {
        &self.vote.node_id
    }

    pub(crate) fn into_request(self) -> io::Result<AppendEntriesRequest<TypeConfig>> {
        Ok(AppendEntriesRequest {
            vote: self.vote.into_vote(),
            prev_log_id: self.previous_log_id.map(LogIdRecord::into_log_id),
            entries: self
                .entries
                .into_iter()
                .map(EntryRecord::into_entry)
                .collect::<io::Result<Vec<_>>>()?,
            leader_commit: self.leader_commit.map(LogIdRecord::into_log_id),
        })
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum AppendEntriesResponseRecord {
    Success,
    PartialSuccess(Option<LogIdRecord>),
    Conflict,
    HigherVote(VoteRecord),
}

impl From<AppendEntriesResponse<TypeConfig>> for AppendEntriesResponseRecord {
    fn from(value: AppendEntriesResponse<TypeConfig>) -> Self {
        match value {
            AppendEntriesResponse::Success => Self::Success,
            AppendEntriesResponse::PartialSuccess(log_id) => {
                Self::PartialSuccess(log_id.map(Into::into))
            }
            AppendEntriesResponse::Conflict => Self::Conflict,
            AppendEntriesResponse::HigherVote(vote) => Self::HigherVote(vote.into()),
        }
    }
}

impl AppendEntriesResponseRecord {
    pub(crate) fn into_response(self) -> AppendEntriesResponse<TypeConfig> {
        match self {
            Self::Success => AppendEntriesResponse::Success,
            Self::PartialSuccess(log_id) => {
                AppendEntriesResponse::PartialSuccess(log_id.map(LogIdRecord::into_log_id))
            }
            Self::Conflict => AppendEntriesResponse::Conflict,
            Self::HigherVote(vote) => AppendEntriesResponse::HigherVote(vote.into_vote()),
        }
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReplicateRequest(pub(crate) AppendEntriesRecord);

impl InterconnectRequest for ReplicateRequest {
    type Response = Result<AppendEntriesResponseRecord, ConsensusRequestError>;
    const NAME: &'static str = "raft_replicate";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HeartbeatRequest(pub(crate) AppendEntriesRecord);

impl InterconnectRequest for HeartbeatRequest {
    type Response = Result<AppendEntriesResponseRecord, ConsensusRequestError>;
    const NAME: &'static str = "raft_heartbeat";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(1);
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct VoteRequestRecord {
    vote: VoteRecord,
    last_log_id: Option<LogIdRecord>,
    leadership_transfer: bool,
}

impl VoteRequestRecord {
    pub(crate) fn from_request(value: VoteRequest<TypeConfig>) -> Self {
        Self {
            vote: value.vote.into(),
            last_log_id: value.last_log_id.map(Into::into),
            leadership_transfer: value.leadership_transfer,
        }
    }

    pub(crate) fn into_request(self) -> VoteRequest<TypeConfig> {
        VoteRequest {
            vote: self.vote.into_vote(),
            last_log_id: self.last_log_id.map(LogIdRecord::into_log_id),
            leadership_transfer: self.leadership_transfer,
        }
    }

    pub(crate) fn origin_node_id(&self) -> &ClusterNodeName {
        &self.vote.node_id
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct VoteResponseRecord {
    vote: VoteRecord,
    vote_granted: bool,
    last_log_id: Option<LogIdRecord>,
}

impl From<VoteResponse<TypeConfig>> for VoteResponseRecord {
    fn from(value: VoteResponse<TypeConfig>) -> Self {
        Self {
            vote: value.vote.into(),
            vote_granted: value.vote_granted,
            last_log_id: value.last_log_id.map(Into::into),
        }
    }
}

impl VoteResponseRecord {
    pub(crate) fn into_response(self) -> VoteResponse<TypeConfig> {
        VoteResponse {
            vote: self.vote.into_vote(),
            vote_granted: self.vote_granted,
            last_log_id: self.last_log_id.map(LogIdRecord::into_log_id),
        }
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RequestVote(pub(crate) VoteRequestRecord);

impl InterconnectRequest for RequestVote {
    type Response = Result<VoteResponseRecord, ConsensusRequestError>;
    const NAME: &'static str = "raft_vote";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct StoredMembershipRecord {
    log_id: Option<LogIdRecord>,
    membership: MembershipRecord,
}

impl StoredMembershipRecord {
    fn from_stored(value: &StoredMembershipOf) -> Self {
        Self {
            log_id: value.log_id().clone().map(Into::into),
            membership: MembershipRecord::from_membership(value.membership()),
        }
    }

    fn into_stored(self) -> io::Result<StoredMembershipOf> {
        Ok(StoredMembership::new(
            self.log_id.map(LogIdRecord::into_log_id),
            self.membership.into_membership()?,
        ))
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct SnapshotMetaRecord {
    last_log_id: Option<LogIdRecord>,
    last_membership: StoredMembershipRecord,
}

impl SnapshotMetaRecord {
    fn from_meta(value: &SnapshotMetaOf) -> Self {
        Self {
            last_log_id: value.last_log_id.clone().map(Into::into),
            last_membership: StoredMembershipRecord::from_stored(&value.last_membership),
        }
    }

    fn into_meta(self) -> io::Result<SnapshotMetaOf> {
        Ok(SnapshotMeta {
            last_log_id: self.last_log_id.map(LogIdRecord::into_log_id),
            last_membership: self.last_membership.into_stored()?,
        })
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct BeginSnapshotTransfer {
    transfer_id: u64,
    vote: VoteRecord,
    meta: SnapshotMetaRecord,
    total_bytes: u64,
}

pub(crate) struct SnapshotTransferStart {
    pub(crate) transfer_id: u64,
    pub(crate) vote: VoteOf,
    pub(crate) meta: SnapshotMetaOf,
    pub(crate) total_bytes: u64,
}

impl BeginSnapshotTransfer {
    pub(crate) fn from_parts(
        transfer_id: u64,
        vote: VoteOf,
        meta: SnapshotMetaOf,
        total_bytes: u64,
    ) -> Self {
        Self {
            transfer_id,
            vote: vote.into(),
            meta: SnapshotMetaRecord::from_meta(&meta),
            total_bytes,
        }
    }

    pub(crate) fn into_start(self) -> io::Result<SnapshotTransferStart> {
        Ok(SnapshotTransferStart {
            transfer_id: self.transfer_id,
            vote: self.vote.into_vote(),
            meta: self.meta.into_meta()?,
            total_bytes: self.total_bytes,
        })
    }

    pub(crate) fn origin_node_id(&self) -> &ClusterNodeName {
        &self.vote.node_id
    }
}

impl InterconnectRequest for BeginSnapshotTransfer {
    type Response = Result<(), ConsensusRequestError>;
    const NAME: &'static str = "raft_begin_snapshot";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_secs(30);
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SnapshotChunk {
    pub(crate) transfer_id: u64,
    pub(crate) offset: u64,
    pub(crate) bytes: Vec<u8>,
}

impl InterconnectRequest for SnapshotChunk {
    type Response = Result<(), ConsensusRequestError>;
    const NAME: &'static str = "raft_snapshot_chunk";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_secs(30);
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct FinishSnapshotTransfer {
    pub(crate) transfer_id: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SnapshotResponseRecord(VoteRecord);

impl From<SnapshotResponse<TypeConfig>> for SnapshotResponseRecord {
    fn from(value: SnapshotResponse<TypeConfig>) -> Self {
        Self(value.vote.into())
    }
}

impl SnapshotResponseRecord {
    pub(crate) fn into_response(self) -> SnapshotResponse<TypeConfig> {
        SnapshotResponse::new(self.0.into_vote())
    }
}

impl InterconnectRequest for FinishSnapshotTransfer {
    type Response = Result<SnapshotResponseRecord, ConsensusRequestError>;
    const NAME: &'static str = "raft_finish_snapshot";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_secs(30);
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TransferLeadership {
    from_leader: VoteRecord,
    to_node_id: ClusterNodeName,
    last_log_id: Option<LogIdRecord>,
}

impl TransferLeadership {
    pub(crate) fn from_request(value: TransferLeaderRequest<TypeConfig>) -> Self {
        Self {
            from_leader: value.from_leader().clone().into(),
            to_node_id: value.to_node_id().clone(),
            last_log_id: value.last_log_id().cloned().map(Into::into),
        }
    }

    pub(crate) fn into_request(self) -> TransferLeaderRequest<TypeConfig> {
        TransferLeaderRequest::new(
            self.from_leader.into_vote(),
            self.to_node_id,
            self.last_log_id.map(LogIdRecord::into_log_id),
        )
    }

    pub(crate) fn origin_node_id(&self) -> &ClusterNodeName {
        &self.from_leader.node_id
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TransferLeadershipErrorRecord {
    VoteChanged {
        expected: VoteRecord,
        actual: VoteRecord,
    },
    LogNotFlushed {
        expected: Option<LogIdRecord>,
        actual: Option<LogIdRecord>,
    },
}

impl From<TransferLeaderError<TypeConfig>> for TransferLeadershipErrorRecord {
    fn from(value: TransferLeaderError<TypeConfig>) -> Self {
        match value {
            TransferLeaderError::VoteChanged { expected, actual } => Self::VoteChanged {
                expected: expected.into(),
                actual: actual.into(),
            },
            TransferLeaderError::LogNotFlushed { expected, actual } => Self::LogNotFlushed {
                expected: expected.map(Into::into),
                actual: actual.map(Into::into),
            },
        }
    }
}

impl TransferLeadershipErrorRecord {
    fn into_error(self) -> TransferLeaderError<TypeConfig> {
        match self {
            Self::VoteChanged { expected, actual } => TransferLeaderError::VoteChanged {
                expected: expected.into_vote(),
                actual: actual.into_vote(),
            },
            Self::LogNotFlushed { expected, actual } => TransferLeaderError::LogNotFlushed {
                expected: expected.map(LogIdRecord::into_log_id),
                actual: actual.map(LogIdRecord::into_log_id),
            },
        }
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TransferLeadershipResponse(Result<(), TransferLeadershipErrorRecord>);

impl From<TransferLeaderResponse<TypeConfig>> for TransferLeadershipResponse {
    fn from(value: TransferLeaderResponse<TypeConfig>) -> Self {
        Self(value.map_err(Into::into))
    }
}

impl TransferLeadershipResponse {
    pub(crate) fn into_response(self) -> TransferLeaderResponse<TypeConfig> {
        self.0.map_err(TransferLeadershipErrorRecord::into_error)
    }
}

impl InterconnectRequest for TransferLeadership {
    type Response = Result<TransferLeadershipResponse, ConsensusRequestError>;
    const NAME: &'static str = "raft_transfer_leadership";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(5);
}
