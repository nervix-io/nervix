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
}

impl StateReplicationError {
    pub(crate) fn as_remote_failure(
        &self,
        subject: nervix_interconnect::RemoteOperationSubject,
    ) -> nervix_interconnect::RemoteOperationFailure {
        nervix_interconnect::RemoteOperationFailure::failed(subject, self.to_string())
    }
}
