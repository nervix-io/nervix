//! Bounded exchange of one native Kafka offset checkpoint.
//!
//! Layer: data plane.
//! - **Owns.** Capturing a revision, staging its native encoding, transferring bounded chunks,
//!   verifying a received checkpoint, and installing it through the replica's assignment token.
//! - **Depends on.** Kafka offset state, admitted execution, quota-owned staging and interconnect.
//! - **Must not know.** Brokers, consumer groups, NSPL, or scheduling policy.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "assigned replica polling and bulk checkpoint frames retain exact installed \
                  state handles"
    )
)]

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use futures_util::{StreamExt as _, stream};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{CpuClass, MemoryClass};
use nervix_interconnect::{
    DescribeKafkaOffsets, InterconnectRequest, KafkaOffsetRevision, RemoteOperationFailure,
    RemoteOperationSubject, StreamHandlerError, StreamingResponse, SyncKafkaOffsets,
};
use nervix_models::ClusterNodeName;
use nervix_primitives::sync::Arc;
use thiserror::Error;

use super::{
    KafkaOffsetSnapshotInstaller, ReplicatedKafkaOffsetState, Runtime, RuntimeStatePlacement,
    SnapshotStagingError,
    state_replication::{StateReplicationError, routing::ReplicatedState},
};

// A current replica receives an empty stream. A checkpoint starts with its little-endian revision
// and BLAKE3 digest; the HTTP content length declares the remaining native payload's exact length.
const HEADER_BYTES: usize = 40;
const CHUNK_BYTES: u64 = 64 * 1024;

#[derive(Debug, Error)]
enum KafkaOffsetTransferError {
    #[error(
        "Kafka offset stream declares {declared} bytes; a checkpoint requires at least {minimum}"
    )]
    StreamLength { declared: u64, minimum: u64 },
    #[error("Kafka offset header has {actual} bytes; exactly {expected} are required")]
    HeaderLength { actual: usize, expected: usize },
    #[error(
        "Kafka offset revision {received} does not advance {after} and retain described revision \
         {described}"
    )]
    Revision {
        received: u64,
        after: u64,
        described: u64,
    },
    #[error("Kafka offset checkpoint of {length} bytes exceeds addressable admission")]
    AdmissionSize { length: u64 },
}

fn checkpoint_header(
    header: &[u8],
    after: u64,
    described: u64,
) -> error_stack::Result<(u64, [u8; 32]), KafkaOffsetTransferError> {
    let header: &[u8; HEADER_BYTES] = header.try_into().map_err(|_| {
        Report::new(KafkaOffsetTransferError::HeaderLength {
            actual: header.len(),
            expected: HEADER_BYTES,
        })
    })?;
    let received = u64::from_le_bytes(
        header[..8]
            .try_into()
            .assured("the revision occupies eight header bytes"),
    );
    if received <= after || received < described {
        return Err(Report::new(KafkaOffsetTransferError::Revision {
            received,
            after,
            described,
        }));
    }
    Ok((
        received,
        header[8..]
            .try_into()
            .assured("the digest occupies thirty-two header bytes"),
    ))
}

impl Runtime {
    pub(crate) fn describe_kafka_offsets(
        &self,
        request: DescribeKafkaOffsets,
    ) -> Result<KafkaOffsetRevision, RemoteOperationFailure> {
        let subject = RemoteOperationSubject::state(&request.placement);
        let placement = RuntimeStatePlacement::from_remote(request.placement)
            .map_err(|error| RemoteOperationFailure::failed(subject.clone(), error.to_string()))?;
        let admitted = self
            .resolve_state_replication_request(&placement)
            .ok_or_else(|| RemoteOperationFailure::rejected(subject.clone()))?;
        let state = admitted
            .state
            .ok_or_else(|| RemoteOperationFailure::unavailable(subject.clone()))?;
        let ReplicatedState::KafkaOffset(state) = state.as_ref() else {
            return Err(RemoteOperationFailure::rejected(subject));
        };
        let lsm = ReplicatedKafkaOffsetState::read(state).current_lsm();
        Ok(if lsm <= request.after_lsm {
            KafkaOffsetRevision::Current
        } else {
            KafkaOffsetRevision::Advanced(lsm)
        })
    }

    pub(crate) async fn stream_kafka_offsets(
        &self,
        request: SyncKafkaOffsets,
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
        let Some(state) = admitted.state else {
            return Err(Report::new(StreamHandlerError::new(format!(
                "this node has no {placement}"
            ))));
        };
        let ReplicatedState::KafkaOffset(state) = state.as_ref() else {
            return Err(Report::new(StreamHandlerError::new(format!(
                "{placement} is not Kafka offset state"
            ))));
        };
        let Some(captured) =
            ReplicatedKafkaOffsetState::read(state).capture_after(request.after_lsm)
        else {
            return Ok(StreamingResponse::new(0, stream::empty()));
        };
        let lsm = captured.lsm;
        tracing::debug!(%placement, lsm, "encoding native Kafka offset checkpoint");
        let (maximum, scratch) = captured.bounds().map_err(StreamHandlerError::with_cause)?;
        let metadata = self
            .inner
            .executor
            .try_reserve(MemoryClass::RestoreMetadata, scratch)
            .map_err(StreamHandlerError::with_cause)?;
        let working = self
            .inner
            .executor
            .reserve(MemoryClass::Bulk, CHUNK_BYTES)
            .await
            .map_err(StreamHandlerError::with_cause)?;
        let writer = self
            .inner
            .snapshot_staging
            .stage(maximum)
            .await
            .map_err(StreamHandlerError::with_cause)?;
        let artifact = Arc::new(
            writer
                .encode_artifact(working, move |output, cancellation| {
                    let _metadata = metadata;
                    captured
                        .write(output, cancellation)
                        .change_context(SnapshotStagingError::Encode)
                })
                .await
                .map_err(StreamHandlerError::with_cause)?,
        );
        let length = artifact
            .length()
            .checked_add(HEADER_BYTES.arch_into())
            .ok_or_else(|| {
                Report::new(StreamHandlerError::new(
                    "Kafka offset stream length exceeds address space",
                ))
            })?;
        tracing::debug!(%placement, lsm, length, "encoded native Kafka offset checkpoint");
        let mut header = lsm.to_le_bytes().to_vec();
        header.extend_from_slice(&artifact.digest());
        let header = self
            .inner
            .executor
            .charge_owned(MemoryClass::Bulk, header)
            .await
            .map_err(StreamHandlerError::with_cause)?;
        let reader = artifact
            .open_reader()
            .await
            .map_err(StreamHandlerError::with_cause)?;
        let chunks = stream::once(async move { Ok(header) }).chain(stream::try_unfold(
            (artifact, reader),
            |(artifact, mut reader)| async move {
                let chunk = reader
                    .next_chunk(CHUNK_BYTES)
                    .await
                    .map_err(StreamHandlerError::with_cause)?;
                Ok::<_, Report<StreamHandlerError>>(chunk.map(|chunk| (chunk, (artifact, reader))))
            },
        ));
        Ok(StreamingResponse::new(length, chunks))
    }

    pub(in crate::runtime) async fn sync_kafka_offsets_from(
        &self,
        target: &ClusterNodeName,
        installer: &KafkaOffsetSnapshotInstaller,
        after_lsm: u64,
    ) -> error_stack::Result<u64, StateReplicationError> {
        let placement = installer.read().placement();
        let failure = || StateReplicationError::Request {
            target: target.clone(),
            placement: placement.clone(),
        };
        let dispatcher = self.inner.remote_dispatcher.load_full().ok_or_else(|| {
            Report::new(StateReplicationError::DispatcherUnavailable {
                target: target.clone(),
                placement: placement.clone(),
            })
        })?;
        let described = dispatcher
            .request_with_timeout(
                target,
                DescribeKafkaOffsets {
                    placement: placement.to_remote(),
                    after_lsm,
                },
                DescribeKafkaOffsets::TIMEOUT,
            )
            .await
            .change_context_lazy(failure)?
            .map_err(|failure| {
                Report::new(StateReplicationError::RemoteFailure {
                    target: target.clone(),
                    placement: placement.clone(),
                    failure,
                })
            })?;
        let KafkaOffsetRevision::Advanced(described_lsm) = described else {
            // The owner may still be waiting for an acknowledgement this replica already sent.
            // Report held progress again without encoding or transferring another checkpoint.
            return installer.acknowledged_revision();
        };
        let mut body = dispatcher
            .request_stream(
                target,
                SyncKafkaOffsets {
                    placement: placement.to_remote(),
                    after_lsm: Some(after_lsm),
                },
            )
            .await
            .change_context_lazy(failure)?;
        if body.content_length() == 0 {
            // Read EOF too: an empty declaration does not excuse unexpected response bytes.
            body.next_chunk().await.change_context_lazy(failure)?;
            return installer.acknowledged_revision();
        }
        let length = match body.content_length().checked_sub(HEADER_BYTES.arch_into()) {
            Some(length) if length > 0 => length,
            _ => {
                return Err(Report::new(KafkaOffsetTransferError::StreamLength {
                    declared: body.content_length(),
                    minimum: u64::try_from(HEADER_BYTES)
                        .assured("the fixed header fits a stream length")
                        + 1,
                })
                .change_context(failure()));
            }
        };
        let mut writer = self
            .inner
            .snapshot_staging
            .stage(length)
            .await
            .change_context_lazy(failure)?;
        tracing::debug!(%placement, after_lsm, described_lsm, length, "receiving native Kafka offset checkpoint");
        let mut header = [0_u8; HEADER_BYTES];
        let mut header_read = 0;
        while let Some(chunk) = body.next_chunk().await.change_context_lazy(failure)? {
            nervix_primitives::task::consume_budget().await;
            let copied = (HEADER_BYTES - header_read).min(chunk.len());
            header[header_read..header_read + copied].copy_from_slice(&chunk.as_ref()[..copied]);
            header_read += copied;
            if copied < chunk.len() {
                writer
                    .write_chunk(
                        chunk
                            .slice(copied, chunk.len())
                            .assured("the payload slice stays inside this admitted chunk"),
                    )
                    .await
                    .change_context_lazy(failure)?;
            }
        }
        drop(body);
        let (lsm, digest) = checkpoint_header(&header[..header_read], after_lsm, described_lsm)
            .change_context_lazy(failure)?;
        tracing::debug!(%placement, lsm, length, "received native Kafka offset checkpoint");
        let mut staged = writer.finish(digest).await.change_context_lazy(failure)?;
        // Native conversion retains the encoded checkpoint beside decoded positions and the new
        // table. Its charge is separate from the bounded transport and file I/O working set.
        let admission_size = || {
            Report::new(KafkaOffsetTransferError::AdmissionSize { length })
                .change_context(failure())
        };
        let working = length.checked_mul(12).ok_or_else(admission_size)?;
        let working = working
            .checked_add(CHUNK_BYTES)
            .ok_or_else(admission_size)?;
        let charge = self
            .inner
            .executor
            .try_reserve(MemoryClass::RestoreMetadata, working)
            .change_context_lazy(failure)?;
        let (read_charge, decode_charge) = charge
            .split(length + CHUNK_BYTES)
            .change_context_lazy(failure)?;
        let payload = staged
            .read_admitted(length, read_charge)
            .await
            .change_context_lazy(failure)?;
        let installer = installer.clone();
        self.inner
            .executor
            .run_cpu(
                CpuClass::Bulk,
                decode_charge,
                move |_charge, cancellation| {
                    cancellation
                        .check()
                        .change_context(StateReplicationError::Capture {
                            placement: installer.read().placement().clone(),
                        })?;
                    installer
                        .install_cancellable_snapshot(lsm, payload.as_ref(), cancellation)
                        .change_context(StateReplicationError::Capture {
                            placement: installer.read().placement().clone(),
                        })
                },
            )
            .await
            .change_context_lazy(failure)?
            .change_context_lazy(failure)?;
        tracing::debug!(%placement, lsm, "installed native Kafka offset checkpoint");
        Ok(lsm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_offset_header_requires_a_complete_advancing_described_checkpoint() {
        let mut header = 11_u64.to_le_bytes().to_vec();
        let digest = [71; 32];
        header.extend_from_slice(&digest);
        assert_eq!(
            checkpoint_header(&header, 9, 10).assured("the captured checkpoint advanced"),
            (11, digest)
        );
        let incomplete = checkpoint_header(&header[..39], 9, 10)
            .err()
            .assured("a partial header is refused");
        assert!(matches!(
            incomplete.current_context(),
            KafkaOffsetTransferError::HeaderLength {
                actual: 39,
                expected: 40
            }
        ));
        let unchanged = checkpoint_header(&header, 11, 11)
            .err()
            .assured("an unchanged checkpoint is refused");
        assert!(matches!(
            unchanged.current_context(),
            KafkaOffsetTransferError::Revision {
                received: 11,
                after: 11,
                described: 11
            }
        ));
        let superseded = checkpoint_header(&header, 9, 12)
            .err()
            .assured("a checkpoint preceding its description is refused");
        assert!(matches!(
            superseded.current_context(),
            KafkaOffsetTransferError::Revision {
                received: 11,
                after: 9,
                described: 12
            }
        ));
    }
}
