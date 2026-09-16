//! Moving one sealed materialized snapshot between the node that owns it and one that needs it.
//!
//! Layer: control plane.
//!
//! - **Owns.** Describing a sealed generation to a requester, streaming its bytes through the bulk
//!   pool, and staging, verifying and opening one on the receiving side.
//! - **Depends on.** The sealed snapshot form, the node's staging area, the interconnect's bulk
//!   streams, and runtime state placements.
//! - **Must not know.** How a snapshot was built or what installing it means to the state that
//!   receives it.
//!
//! Describing and transferring are separate steps. A requester that is already current learns so
//! from the description and no scan or encoding happens for it. A requester that needs bytes takes
//! them through the bulk pool in bounded chunks, so a snapshot larger than the node's
//! transfer-memory budget moves without either side holding it whole.

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use futures_util::stream;
use meticulous::ResultExt as _;
use nervix_execution::ChargedBytes;
use nervix_interconnect::{
    RemoteOperationFailure, RemoteOperationSubject, StreamHandlerError, StreamingResponse,
};
use nervix_models::ClusterNodeName;
use thiserror::Error;

use super::{
    RestoredMaterializedSnapshot, Runtime, RuntimeStatePlacement, SealedSource,
    materialized_state::MaterializedRelayStateRead,
    state_snapshot_transfer::{
        DescribeStateSnapshot, DescribedStateSnapshot, FetchStateSnapshot, SealedSnapshotEnvelope,
    },
};

/// How long a requester waits for the owner to describe what it would transfer. Sealing runs on
/// the bulk workers behind whatever else is admitted there, so this is longer than an ordinary
/// control request rather than the same.
const DESCRIBE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub(in crate::runtime) enum MaterializedSnapshotExchangeError {
    #[error("the remote dispatcher is unavailable while fetching {placement} from '{target}'")]
    DispatcherUnavailable {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error("failed to describe {placement} on node '{target}'")]
    Describe {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error("node '{target}' could not provide {placement}: {failure}")]
    RemoteFailure {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
        failure: RemoteOperationFailure,
    },
    #[error("node '{target}' offered {placement} with another schema fingerprint")]
    SchemaMismatch {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
        offered: [u8; 32],
    },
    #[error("failed to open the transfer for {placement} on node '{target}'")]
    OpenTransfer {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error(
        "node '{target}' declared {declared} bytes for {placement} but opened a {actual}-byte \
         transfer"
    )]
    TransferLength {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
        declared: u64,
        actual: u64,
    },
    #[error("failed to stage {placement} from node '{target}'")]
    Stage {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error("failed while receiving {placement} from node '{target}'")]
    Receive {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error("failed to write a staged chunk of {placement} from node '{target}'")]
    WriteChunk {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error("failed to verify staged {placement} from node '{target}'")]
    Verify {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error("failed to open transferred {placement} from node '{target}'")]
    OpenSnapshot {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error("failed to install transferred {placement} from node '{target}'")]
    Install {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
}

impl Runtime {
    /// Describe what this node would transfer for a placement, sealing a generation if the
    /// requester needs one.
    pub(crate) async fn describe_sealed_materialized_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
        after_revision: Option<u64>,
    ) -> Result<DescribedStateSnapshot, RemoteOperationFailure> {
        let subject = RemoteOperationSubject::state(&placement.to_remote());
        let state = self
            .inner
            .replicated_materialized_stream_states
            .get(placement)
            .map(|state| super::ReplicatedMaterializedRelayState::read(state.value()));
        let Some(state) = state else {
            return Err(RemoteOperationFailure::unavailable(subject));
        };
        match state.seal_after(&self.inner.executor, after_revision).await {
            Ok(Some(sealed)) => Ok(DescribedStateSnapshot::Sealed(SealedSnapshotEnvelope {
                length: sealed.descriptor.length,
                digest: sealed.descriptor.digest,
                schema_fingerprint: sealed.descriptor.schema_fingerprint,
                revision: sealed.descriptor.revision,
                fence: sealed.descriptor.fence,
                branch_generation: sealed.descriptor.branch_generation,
            })),
            Ok(None) => Ok(DescribedStateSnapshot::Current),
            Err(error) => Err(RemoteOperationFailure::failed(subject, error.to_string())),
        }
    }

    /// Open the bounded stream of one sealed generation's bytes.
    ///
    /// The generation is named by revision, so an owner that has already replaced it refuses
    /// rather than substituting a different one under the description the requester holds.
    pub(crate) async fn stream_sealed_materialized_snapshot(
        &self,
        request: FetchStateSnapshot,
    ) -> Result<StreamingResponse, StreamHandlerError> {
        let placement = RuntimeStatePlacement::from_remote(request.placement)
            .map_err(|error| StreamHandlerError::new(error.to_string()))?;
        let state = self
            .inner
            .replicated_materialized_stream_states
            .get(&placement)
            .map(|state| super::ReplicatedMaterializedRelayState::read(state.value()));
        let Some(state) = state else {
            return Err(StreamHandlerError::new(format!(
                "this node is not currently assigned materialized state for {} '{}'",
                placement.kind.as_str(),
                placement.identifier.as_str()
            )));
        };
        let Some(sealed) = state.sealed_at(request.revision) else {
            return Err(StreamHandlerError::new(format!(
                "materialized relay '{}' no longer holds the sealed generation at revision {}",
                placement.identifier.as_str(),
                request.revision
            )));
        };
        let chunk_bytes = self.inner.executor.limits().bulk_chunk_bytes.as_u64();
        let length = sealed.descriptor.length;
        let chunks = stream::unfold(
            SealedChunks {
                bytes: sealed.bytes,
                offset: 0,
                chunk_bytes,
            },
            |mut chunks| async move { chunks.next().map(|chunk| (chunk, chunks)) },
        );
        Ok(StreamingResponse::new(length, chunks))
    }

    /// Fetch one relay's sealed materialized snapshot from the node that owns it.
    ///
    /// The bytes are staged on disk as they arrive and accepted only when their length and digest
    /// match what the owner declared, so a cancelled, truncated or corrupted transfer leaves the
    /// caller with nothing to install rather than with part of a generation.
    pub(in crate::runtime) async fn fetch_sealed_materialized_snapshot(
        &self,
        target_node_id: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        schema: &std::sync::Arc<arrow_schema::Schema>,
        after_revision: Option<u64>,
    ) -> error_stack::Result<Option<RestoredMaterializedSnapshot>, MaterializedSnapshotExchangeError>
    {
        let Some(dispatcher) = self.inner.remote_dispatcher.load_full() else {
            return Err(Report::new(
                MaterializedSnapshotExchangeError::DispatcherUnavailable {
                    target: target_node_id.clone(),
                    placement: placement.clone(),
                },
            ));
        };
        let described = dispatcher
            .request_with_timeout(
                target_node_id,
                DescribeStateSnapshot {
                    placement: placement.to_remote(),
                    after_revision,
                },
                DESCRIBE_TIMEOUT,
            )
            .await
            .map_err(|reason| {
                Report::new(MaterializedSnapshotExchangeError::Describe {
                    target: target_node_id.clone(),
                    placement: placement.clone(),
                })
                .attach_printable(reason)
            })?
            .map_err(|failure| {
                Report::new(MaterializedSnapshotExchangeError::RemoteFailure {
                    target: target_node_id.clone(),
                    placement: placement.clone(),
                    failure,
                })
            })?;
        let envelope = match described {
            DescribedStateSnapshot::Current => return Ok(None),
            DescribedStateSnapshot::Sealed(envelope) => envelope,
        };
        if envelope.schema_fingerprint != placement.schema_fingerprint {
            return Err(Report::new(
                MaterializedSnapshotExchangeError::SchemaMismatch {
                    target: target_node_id.clone(),
                    placement: placement.clone(),
                    offered: envelope.schema_fingerprint,
                },
            ));
        }
        let mut body = dispatcher
            .request_stream(
                target_node_id,
                FetchStateSnapshot {
                    placement: placement.to_remote(),
                    revision: envelope.revision,
                },
            )
            .await
            .map_err(|reason| {
                Report::new(MaterializedSnapshotExchangeError::OpenTransfer {
                    target: target_node_id.clone(),
                    placement: placement.clone(),
                })
                .attach_printable(reason)
            })?;
        if body.content_length() != envelope.length {
            return Err(Report::new(
                MaterializedSnapshotExchangeError::TransferLength {
                    target: target_node_id.clone(),
                    placement: placement.clone(),
                    declared: envelope.length,
                    actual: body.content_length(),
                },
            ));
        }
        let mut staged = self
            .inner
            .snapshot_staging
            .stage(envelope.length)
            .await
            .change_context(MaterializedSnapshotExchangeError::Stage {
                target: target_node_id.clone(),
                placement: placement.clone(),
            })?;
        while let Some(chunk) =
            body.next_chunk()
                .await
                .change_context(MaterializedSnapshotExchangeError::Receive {
                    target: target_node_id.clone(),
                    placement: placement.clone(),
                })?
        {
            tokio::task::consume_budget().await;
            staged.write_chunk(chunk).await.change_context(
                MaterializedSnapshotExchangeError::WriteChunk {
                    target: target_node_id.clone(),
                    placement: placement.clone(),
                },
            )?;
        }
        let staged = staged.finish(envelope.digest).await.change_context(
            MaterializedSnapshotExchangeError::Verify {
                target: target_node_id.clone(),
                placement: placement.clone(),
            },
        )?;
        RestoredMaterializedSnapshot::open(
            &self.inner.executor,
            schema,
            placement.schema_fingerprint,
            SealedSource::staged(staged),
        )
        .await
        .map(Some)
        .change_context(MaterializedSnapshotExchangeError::OpenSnapshot {
            target: target_node_id.clone(),
            placement: placement.clone(),
        })
    }

    /// Refresh one materialized relay state from the node that owns it, installing whatever it
    /// sealed. Reports whether a generation was installed.
    pub(in crate::runtime) async fn install_materialized_snapshot_from(
        &self,
        target_node_id: &ClusterNodeName,
        state: &MaterializedRelayStateRead,
        installer: &super::MaterializedRelaySnapshotInstaller,
        after_revision: Option<u64>,
    ) -> error_stack::Result<Option<u64>, MaterializedSnapshotExchangeError> {
        let Some(restored) = self
            .fetch_sealed_materialized_snapshot(
                target_node_id,
                state.placement(),
                state.schema(),
                after_revision,
            )
            .await?
        else {
            return Ok(None);
        };
        let revision = restored.revision;
        installer.install(restored).map_err(|error| {
            Report::new(MaterializedSnapshotExchangeError::Install {
                target: target_node_id.clone(),
                placement: state.placement().clone(),
            })
            .attach_printable(error)
        })?;
        Ok(Some(revision))
    }
}

/// One sealed generation being handed to the transport a bounded chunk at a time. Each chunk is a
/// window onto the same charged allocation, so the stream copies nothing.
struct SealedChunks {
    bytes: ChargedBytes,
    offset: usize,
    chunk_bytes: u64,
}

impl SealedChunks {
    fn next(&mut self) -> Option<Result<ChargedBytes, StreamHandlerError>> {
        if self.offset >= self.bytes.len() {
            return None;
        }
        let wanted = usize::try_from(self.chunk_bytes).verified(
            "the configured bulk chunk is validated against a memory budget counted in permits",
        );
        let end = self
            .offset
            .checked_add(wanted)
            .unwrap_or(self.bytes.len())
            .min(self.bytes.len());
        let Some(chunk) = self.bytes.slice(self.offset, end) else {
            return Some(Err(StreamHandlerError::new(
                "the sealed snapshot ended before the chunk it declared",
            )));
        };
        self.offset = end;
        Some(Ok(chunk))
    }
}
