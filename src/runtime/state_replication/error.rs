//! Typed failures for runtime-state capture, synchronization, persistence, and replica quorum.
//!
//! Layer: data plane.
//! - **Owns.** The semantic failure classes returned by runtime-state replication operations.
//! - **Depends on.** Runtime-state placements and typed interconnect failure envelopes.
//! - **Must not know.** Scheduling policy, NSPL parsing, or connector lifecycle.

use super::*;

#[derive(Debug, Error)]
pub(crate) enum StateReplicationError {
    #[error("failed to capture {placement}")]
    Capture { placement: RuntimeStatePlacement },
    #[error("the remote dispatcher is unavailable while synchronizing {placement} from '{target}'")]
    DispatcherUnavailable {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error("failed to request {placement} from node '{target}'")]
    Request {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error("node '{target}' could not synchronize {placement}: {failure}")]
    RemoteFailure {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
        failure: nervix_interconnect::RemoteOperationFailure,
    },
    #[error(
        "timed out waiting for {required_acks} replica acknowledgements for {placement} at lsm \
         {lsm}"
    )]
    ReplicaQuorum {
        placement: RuntimeStatePlacement,
        lsm: u64,
        required_acks: usize,
    },
    #[error("failed to persist {placement} at lsm {lsm}")]
    Persist {
        placement: RuntimeStatePlacement,
        lsm: u64,
    },
    #[error(
        "failed to commit Kafka offset {next_offset} for topic '{topic}' partition {partition} in \
         {placement}"
    )]
    CommitKafkaOffset {
        placement: RuntimeStatePlacement,
        topic: String,
        partition: i32,
        next_offset: i64,
    },
    #[error("failed to replace the offsets in {placement}")]
    ReplaceKafkaOffsets { placement: RuntimeStatePlacement },
    #[error("this node no longer holds the lifetime or ownership of {placement}")]
    Superseded { placement: RuntimeStatePlacement },
}

impl StateReplicationError {
    /// Whether the node failed to write the state to its own store, rather than to synchronize it.
    pub(crate) const fn is_local_persistence(&self) -> bool {
        matches!(self, Self::Persist { .. })
    }

    /// Whether the state's authority refused the operation, rather than failing it: a peer that is
    /// not the state's authority, or this node after the state's lifetime or ownership moved on.
    pub(crate) const fn is_authority_rejection(&self) -> bool {
        matches!(
            self,
            Self::RemoteFailure {
                failure: nervix_interconnect::RemoteOperationFailure::Rejected { .. },
                ..
            } | Self::Superseded { .. }
        )
    }

    pub(crate) fn as_remote_failure(
        &self,
        subject: nervix_interconnect::RemoteOperationSubject,
    ) -> nervix_interconnect::RemoteOperationFailure {
        nervix_interconnect::RemoteOperationFailure::failed(subject, self.to_string())
    }
}
