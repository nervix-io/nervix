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

use futures_util::stream;
use nervix_execution::ChargedBytes;
use nervix_interconnect::{StreamHandlerError, StreamingResponse};
use nervix_models::ClusterNodeName;

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

impl Runtime {
    /// Describe what this node would transfer for a placement, sealing a generation if the
    /// requester needs one.
    pub(crate) async fn describe_sealed_materialized_snapshot(
        &self,
        placement: &RuntimeStatePlacement,
        after_revision: Option<u64>,
    ) -> DescribedStateSnapshot {
        let state = self
            .inner
            .replicated_materialized_stream_states
            .get(placement)
            .map(|state| {
                super::ReplicatedMaterializedRelayState::read(state.value())
            });
        let Some(state) = state else {
            return DescribedStateSnapshot::Unavailable(format!(
                "this node is not currently assigned materialized state for {} '{}'",
                placement.kind.as_str(),
                placement.identifier.as_str()
            ));
        };
        match state.seal_after(&self.inner.executor, after_revision).await {
            Ok(Some(sealed)) => DescribedStateSnapshot::Sealed(SealedSnapshotEnvelope {
                length: sealed.descriptor.length,
                digest: sealed.descriptor.digest,
                schema_fingerprint: sealed.descriptor.schema_fingerprint,
                revision: sealed.descriptor.revision,
                fence: sealed.descriptor.fence,
                branch_generation: sealed.descriptor.branch_generation,
            }),
            Ok(None) => DescribedStateSnapshot::Current,
            Err(error) => DescribedStateSnapshot::Unavailable(error.to_string()),
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
            .map_err(StreamHandlerError::new)?;
        let state = self
            .inner
            .replicated_materialized_stream_states
            .get(&placement)
            .map(|state| super::ReplicatedMaterializedRelayState::read(state.value()))
            .ok_or_else(|| {
                StreamHandlerError::new(format!(
                    "this node is not currently assigned materialized state for {} '{}'",
                    placement.kind.as_str(),
                    placement.identifier.as_str()
                ))
            })?;
        let sealed = state
            .sealed_at(request.revision)
            .ok_or_else(|| {
                StreamHandlerError::new(format!(
                    "materialized relay '{}' no longer holds the sealed generation at revision {}",
                    placement.identifier.as_str(),
                    request.revision
                ))
            })?;
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
    pub(crate) async fn fetch_sealed_materialized_snapshot(
        &self,
        target_node_id: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        schema: &std::sync::Arc<arrow_schema::Schema>,
        after_revision: Option<u64>,
    ) -> Result<Option<RestoredMaterializedSnapshot>, String> {
        let Some(dispatcher) = self.inner.remote_dispatcher.read().clone() else {
            return Err("remote dispatcher unavailable".to_string());
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
            .await?;
        let envelope = match described {
            DescribedStateSnapshot::Current => return Ok(None),
            DescribedStateSnapshot::Unavailable(reason) => return Err(reason),
            DescribedStateSnapshot::Sealed(envelope) => envelope,
        };
        if envelope.schema_fingerprint != placement.schema_fingerprint {
            return Err(format!(
                "node '{target_node_id}' offered a materialized relay snapshot of another schema"
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
            .await?;
        if body.content_length() != envelope.length {
            return Err(format!(
                "node '{target_node_id}' offered {} snapshot bytes and is sending {}",
                envelope.length,
                body.content_length()
            ));
        }
        let mut staged = self
            .inner
            .snapshot_staging
            .stage(envelope.length)
            .await
            .map_err(|error| error.to_string())?;
        while let Some(chunk) = body
            .next_chunk()
            .await
            .map_err(|error| format!("snapshot transfer failed: {error}"))?
        {
            tokio::task::consume_budget().await;
            staged
                .write_chunk(chunk)
                .await
                .map_err(|error| error.to_string())?;
        }
        let staged = staged
            .finish(envelope.digest)
            .await
            .map_err(|error| error.to_string())?;
        RestoredMaterializedSnapshot::open(
            &self.inner.executor,
            schema,
            placement.schema_fingerprint,
            SealedSource::staged(staged),
        )
        .await
        .map(Some)
        .map_err(|error| error.to_string())
    }

    /// Refresh one materialized relay state from the node that owns it, installing whatever it
    /// sealed. Reports whether a generation was installed.
    pub(in crate::runtime) async fn install_materialized_snapshot_from(
        &self,
        target_node_id: &ClusterNodeName,
        state: &MaterializedRelayStateRead,
        installer: &super::MaterializedRelaySnapshotInstaller,
        after_revision: Option<u64>,
    ) -> Result<Option<u64>, String> {
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
        installer
            .install(restored)
            .map_err(|error| error.to_string())?;
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
        let wanted = usize::try_from(self.chunk_bytes).unwrap_or(usize::MAX);
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
