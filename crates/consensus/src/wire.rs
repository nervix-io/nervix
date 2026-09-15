//! The current rkyv representation of Raft messages carried by the interconnect.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Lossless conversion between OpenRaft requests and Nervix interconnect records.
//! - **Depends on.** OpenRaft, shared Raft records, and the interconnect request primitive.
//! - **Must not know.** HTTP/2 framing, durable storage, peer addresses, or control-plane policy.

use std::{io, time::Duration};

use nervix_interconnect::{
    InterconnectDuplexRequest, InterconnectRequest, PoolClass, RequestSubquota,
};
use nervix_models::ClusterNodeName;
use openraft::{
    BasicNode, SnapshotMeta,
    raft::{StreamAppendError, StreamAppendResult, TransferLeaderError},
};
use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, TypeConfig, VoteOf, VoteRequest, VoteResponse,
    raft_record::{EntryRecord, LogIdRecord, StoredMembershipRecord, VoteRecord},
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

/// Ask the current leader to establish the committed boundary for process runtime admission.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimeAdmissionRead;

impl InterconnectRequest for RuntimeAdmissionRead {
    type Response = Result<LogIdRecord, ConsensusRequestError>;
    const NAME: &'static str = "raft_runtime_admission_read";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Admission;
    const TIMEOUT: Duration = Duration::from_secs(5);
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
            entries: value.entries.into_iter().map(EntryRecord::from).collect(),
            leader_commit: value.leader_commit.map(Into::into),
        }
    }

    pub(crate) fn origin_node_id(&self) -> &ClusterNodeName {
        self.vote.node_id()
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

/// One ordered append stream from a leader to one follower.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct OpenAppendStream {
    pub(crate) leader_node_id: ClusterNodeName,
}

impl InterconnectDuplexRequest for OpenAppendStream {
    type Item = AppendEntriesRecord;
    type Response = Result<StreamAppendResultRecord, ConsensusRequestError>;

    const NAME: &'static str = "raft_append_stream";
    const CLASS: PoolClass = PoolClass::Replication;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Append;
    const SETUP_TIMEOUT: Duration = Duration::from_secs(5);
}

/// What the follower's Raft made of one submitted batch.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum StreamAppendResultRecord {
    Matching(Option<LogIdRecord>),
    Conflict(LogIdRecord),
    HigherVote(VoteRecord),
}

impl From<StreamAppendResult<TypeConfig>> for StreamAppendResultRecord {
    fn from(value: StreamAppendResult<TypeConfig>) -> Self {
        match value {
            Ok(matching) => Self::Matching(matching.map(Into::into)),
            Err(StreamAppendError::Conflict(log_id)) => Self::Conflict(log_id.into()),
            Err(StreamAppendError::HigherVote(vote)) => Self::HigherVote(vote.into()),
        }
    }
}

impl StreamAppendResultRecord {
    pub(crate) fn into_result(self) -> StreamAppendResult<TypeConfig> {
        match self {
            Self::Matching(matching) => Ok(matching.map(LogIdRecord::into_log_id)),
            Self::Conflict(log_id) => Err(StreamAppendError::Conflict(log_id.into_log_id())),
            Self::HigherVote(vote) => Err(StreamAppendError::HigherVote(vote.into_vote())),
        }
    }
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
        self.vote.node_id()
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
    section_count: u32,
    total_bytes: u64,
}

pub(crate) struct SnapshotTransferStart {
    pub(crate) transfer_id: u64,
    pub(crate) vote: VoteOf,
    pub(crate) meta: SnapshotMetaOf,
    pub(crate) section_count: u32,
    pub(crate) total_bytes: u64,
}

impl BeginSnapshotTransfer {
    pub(crate) fn from_parts(
        transfer_id: u64,
        vote: VoteOf,
        meta: SnapshotMetaOf,
        section_count: u32,
        total_bytes: u64,
    ) -> Self {
        Self {
            transfer_id,
            vote: vote.into(),
            meta: SnapshotMetaRecord::from_meta(&meta),
            section_count,
            total_bytes,
        }
    }

    pub(crate) fn into_start(self) -> io::Result<SnapshotTransferStart> {
        Ok(SnapshotTransferStart {
            transfer_id: self.transfer_id,
            vote: self.vote.into_vote(),
            meta: self.meta.into_meta()?,
            section_count: self.section_count,
            total_bytes: self.total_bytes,
        })
    }

    pub(crate) fn origin_node_id(&self) -> &ClusterNodeName {
        self.vote.node_id()
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
    pub(crate) section_index: u32,
    pub(crate) section_bytes: u64,
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
        self.from_leader.node_id()
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
