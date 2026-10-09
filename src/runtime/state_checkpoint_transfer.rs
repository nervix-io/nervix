//! Transfer one selected runtime checkpoint through the interconnect bulk pool.
//!
//! Layer: data plane.
//! - **Owns.** Describing an exact checkpoint, sending bounded chunks, and verifying all bytes
//!   before a replica or handoff destination may install them.
//! - **Depends on.** Assigned state handles, the execution memory budget and bulk streams.
//! - **Must not know.** Placement policy, NSPL, or how a state kind decodes its checkpoint.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "assigned checkpoint requests retain exact state handles and bounded bulk bytes"
    )
)]

use error_stack::{Report, ResultExt as _};
use futures_util::stream;
use meticulous::ResultExt as _;
use nervix_execution::{CpuClass, MemoryClass};
use nervix_interconnect::{
    FetchStateCheckpoint, StateCheckpointRead, StateSnapshotEnvelope, StreamHandlerError,
    StreamingResponse,
};
use nervix_models::ClusterNodeName;
use thiserror::Error;

use super::{
    PersistedRuntimeStateEntry, Runtime, RuntimeStatePlacement,
    state_replication::{
        StateReplicationError,
        routing::{ReplicatedState, StateReplicationRequest},
    },
};

/// The host accepts at most 64 MiB of guest state. This bound also leaves room for the checkpoint
/// envelope and for the other runtime state kinds that share this transfer mechanism.
const MAX_CHECKPOINT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Error)]
enum CheckpointTransferError {
    #[error("checkpoint has {length} bytes, above the {maximum}-byte transfer bound")]
    TooLarge { length: u64, maximum: u64 },
    #[error("checkpoint length {length} does not fit the receiving address space")]
    AddressSpace { length: u64 },
    #[error(
        "checkpoint revision {lsm} described {described} bytes but opened a {opened}-byte stream"
    )]
    StreamLength {
        lsm: u64,
        described: u64,
        opened: u64,
    },
    #[error("checkpoint revision {lsm} received more than its declared {described} bytes")]
    ExcessBytes { lsm: u64, described: u64 },
    #[error("checkpoint revision {lsm} received {received} of its declared {described} bytes")]
    Truncated {
        lsm: u64,
        described: u64,
        received: u64,
    },
    #[error("checkpoint revision {lsm} failed digest verification")]
    DigestMismatch { lsm: u64 },
}

impl Runtime {
    /// Hash a captured checkpoint on the bounded bulk CPU workers. The capture and the charge
    /// move together so an async request does not hash a whole guest save on its runtime worker.
    pub(crate) async fn describe_state_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
    ) -> error_stack::Result<StateSnapshotEnvelope, StateReplicationError> {
        let capture = || StateReplicationError::Capture {
            placement: placement.clone(),
        };
        let length = u64::try_from(snapshot.payload.len()).assured("checkpoint length fits in u64");
        if length > MAX_CHECKPOINT_BYTES {
            return Err(Report::new(CheckpointTransferError::TooLarge {
                length,
                maximum: MAX_CHECKPOINT_BYTES,
            })
            .change_context(capture()));
        }
        let reservation = self
            .inner
            .executor
            .reserve(MemoryClass::RestoreMetadata, length.max(1))
            .await
            .change_context_lazy(capture)?;
        self.inner
            .executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                let mut hasher = blake3::Hasher::new();
                for chunk in snapshot.payload.chunks(64 * 1024) {
                    cancellation.check()?;
                    hasher.update(chunk);
                }
                Ok::<_, nervix_execution::Cancelled>(StateSnapshotEnvelope {
                    lsm: snapshot.lsm,
                    length,
                    digest: *hasher.finalize().as_bytes(),
                })
            })
            .await
            .change_context_lazy(capture)?
            .change_context_lazy(capture)
    }

    /// The state handle is selected at admission. A handoff may request a committed WASM revision
    /// while a newer published revision awaits its replicas, so either exact retained revision can
    /// satisfy the stream; no request may silently receive a different revision.
    pub(crate) async fn stream_state_checkpoint(
        &self,
        request: FetchStateCheckpoint,
    ) -> Result<StreamingResponse, Report<StreamHandlerError>> {
        let placement = RuntimeStatePlacement::from_remote(request.placement)
            .map_err(StreamHandlerError::with_cause)?;
        let admitted = self
            .resolve_state_replication_request(&placement)
            .ok_or_else(|| {
                Report::new(StreamHandlerError::new(format!(
                    "this node is not assigned {placement}"
                )))
            })?;
        let committed_wasm = admitted
            .state
            .as_ref()
            .and_then(|state| match state.as_ref() {
                ReplicatedState::WasmProcessor(state) => state.committed_snapshot_at(request.lsm),
                _ => None,
            });
        let snapshot = match (request.read, committed_wasm) {
            (_, Some(snapshot)) => Some(snapshot),
            (StateCheckpointRead::Published, None) => self
                .answer_state_sync_request(admitted, request.lsm.checked_sub(1))
                .await
                .map_err(StreamHandlerError::with_cause)?,
            (StateCheckpointRead::HandoffCapture, None) => self
                .answer_state_sync_request(
                    StateReplicationRequest {
                        placement: admitted.placement,
                        state: None,
                    },
                    request.lsm.checked_sub(1),
                )
                .await
                .map_err(StreamHandlerError::with_cause)?,
        };
        let Some(snapshot) = snapshot.filter(|snapshot| snapshot.lsm == request.lsm) else {
            return Err(Report::new(StreamHandlerError::new(format!(
                "{placement} no longer holds checkpoint revision {}",
                request.lsm
            ))));
        };
        let length = u64::try_from(snapshot.payload.len()).assured("checkpoint length fits in u64");
        if length > MAX_CHECKPOINT_BYTES {
            return Err(StreamHandlerError::with_cause(Report::new(
                CheckpointTransferError::TooLarge {
                    length,
                    maximum: MAX_CHECKPOINT_BYTES,
                },
            )));
        }
        let bytes = self
            .inner
            .executor
            .charge_owned(MemoryClass::RestoreMetadata, snapshot.payload)
            .await
            .map_err(StreamHandlerError::with_cause)?;
        tracing::debug!(%placement, lsm = request.lsm, length, "streaming runtime checkpoint");
        let chunk_bytes = usize::try_from(self.inner.executor.limits().bulk_chunk_bytes.as_u64())
            .assured("the configured bulk chunk size fits this host's address space")
            .max(1);
        let chunks = stream::unfold((bytes, 0), move |(bytes, offset)| async move {
            if offset >= bytes.len() {
                return None;
            }
            let end = offset.saturating_add(chunk_bytes).min(bytes.len());
            let chunk = bytes
                .slice(offset, end)
                .expect("bounded chunk is within the checkpoint");
            Some((Ok(chunk), (bytes, end)))
        });
        Ok(StreamingResponse::new(length, chunks))
    }

    /// Receive one descriptor's exact revision. A partial, extra or corrupted body never becomes
    /// an installable checkpoint. The allocation is admitted before any body bytes are consumed.
    pub(super) async fn fetch_state_checkpoint(
        &self,
        target: &ClusterNodeName,
        placement: &RuntimeStatePlacement,
        descriptor: &StateSnapshotEnvelope,
        read: StateCheckpointRead,
    ) -> error_stack::Result<PersistedRuntimeStateEntry, StateReplicationError> {
        let failure = || StateReplicationError::Request {
            target: target.clone(),
            placement: placement.clone(),
        };
        if descriptor.length > MAX_CHECKPOINT_BYTES {
            return Err(Report::new(CheckpointTransferError::TooLarge {
                length: descriptor.length,
                maximum: MAX_CHECKPOINT_BYTES,
            })
            .change_context(failure()));
        }
        let capacity = usize::try_from(descriptor.length).map_err(|_| {
            Report::new(CheckpointTransferError::AddressSpace {
                length: descriptor.length,
            })
            .change_context(failure())
        })?;
        let dispatcher = self.inner.remote_dispatcher.load_full().ok_or_else(|| {
            Report::new(StateReplicationError::DispatcherUnavailable {
                target: target.clone(),
                placement: placement.clone(),
            })
        })?;
        let mut body = dispatcher
            .request_stream(
                target,
                FetchStateCheckpoint {
                    placement: placement.to_remote(),
                    lsm: descriptor.lsm,
                    read,
                },
            )
            .await
            .change_context_lazy(failure)?;
        if body.content_length() != descriptor.length {
            return Err(Report::new(CheckpointTransferError::StreamLength {
                lsm: descriptor.lsm,
                described: descriptor.length,
                opened: body.content_length(),
            })
            .change_context(failure()));
        }
        let _admission = self
            .inner
            .executor
            .reserve(MemoryClass::RestoreMetadata, descriptor.length.max(1))
            .await
            .change_context_lazy(failure)?;
        let mut payload = Vec::with_capacity(capacity);
        let mut hasher = blake3::Hasher::new();
        while let Some(chunk) = body.next_chunk().await.change_context_lazy(failure)? {
            nervix_primitives::task::consume_budget().await;
            if chunk.len() > capacity.saturating_sub(payload.len()) {
                return Err(Report::new(CheckpointTransferError::ExcessBytes {
                    lsm: descriptor.lsm,
                    described: descriptor.length,
                })
                .change_context(failure()));
            }
            hasher.update(chunk.as_ref());
            payload.extend_from_slice(chunk.as_ref());
        }
        drop(body);
        if payload.len() != capacity {
            return Err(Report::new(CheckpointTransferError::Truncated {
                lsm: descriptor.lsm,
                described: descriptor.length,
                received: u64::try_from(payload.len()).assured("received bytes fit u64"),
            })
            .change_context(failure()));
        }
        if hasher.finalize().as_bytes() != &descriptor.digest {
            return Err(Report::new(CheckpointTransferError::DigestMismatch {
                lsm: descriptor.lsm,
            })
            .change_context(failure()));
        }
        tracing::debug!(%placement, target = %target, lsm = descriptor.lsm, length = descriptor.length, "verified runtime checkpoint stream");
        Ok(PersistedRuntimeStateEntry {
            lsm: descriptor.lsm,
            payload,
        })
    }
}
