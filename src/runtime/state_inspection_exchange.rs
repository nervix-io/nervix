//! Native Arrow page carriage between state inspection coordinators and unit owners.
//!
//! Layer: control plane.
//! - **Owns.** Bounded page stream framing and native snapshot decoding.
//! - **Depends on.** Bulk admission, typed page descriptors and the native Arrow snapshot codec.
//! - **Must not know.** Plexus, Row cells, text formats or graph mutation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns the boundary declared above")
)]

use error_stack::{Report, ResultExt as _};
use futures_util::{StreamExt as _, stream};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{BudgetedBuffer, ChargedBytes, Executor, MemoryClass};
use nervix_interconnect::{
    MaterializedStatePageBody, MaterializedStatePageDescriptor, MaterializedStatePageReply,
    MaterializedStatePageTransfer, PoolClass, ReadMaterializedStatePage, RkyvMessage,
    StreamHandlerError, StreamingResponse,
};
use nervix_models::StateInspectionRejection;

use super::{
    MaterializedGenerationRecord, RestoredMaterializedSnapshot, SealedSource,
    state_inspection_read::MaterializedInspectionPage,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum StatePageExchangeError {
    #[error("the state page stream exceeds its declared bound or has an invalid header")]
    Framing,
    #[error("the state page stream could not be received")]
    Receive,
    #[error("the state page descriptor could not be decoded")]
    Descriptor,
    #[error("the state page Arrow container could not be decoded")]
    Columns,
    #[error("state page memory admission was refused")]
    Admission,
}

/// Stream framing is an eight-byte little-endian descriptor length, its bounded rkyv bytes,
/// then the native Arrow container. Its chunks share admitted allocations without copying.
pub(crate) async fn stream_materialized_page(
    executor: &Executor,
    result: Result<
        (MaterializedStatePageDescriptor, MaterializedInspectionPage),
        StateInspectionRejection,
    >,
    max_bytes: u64,
) -> Result<StreamingResponse, Report<StreamHandlerError>> {
    let (reply, body) = match result {
        Ok((descriptor, page)) => {
            if page.generation.records().is_empty() {
                (
                    Ok(MaterializedStatePageTransfer {
                        page: descriptor,
                        body: None,
                    }),
                    None,
                )
            } else {
                let body = page
                    .generation
                    .encode_resident_container(executor)
                    .await
                    .map_err(StreamHandlerError::with_cause)?;
                if u64::try_from(body.len()).assured("resident allocations fit u64") > max_bytes {
                    (Err(StateInspectionRejection::ReplyTooLarge), None)
                } else {
                    let charge = executor
                        .try_reserve(MemoryClass::Bulk, 1)
                        .map_err(StreamHandlerError::with_cause)?;
                    let (identity, body) = executor
                        .run_cpu(
                            nervix_execution::CpuClass::Bulk,
                            charge,
                            move |_charge, cancellation| {
                                cancellation.check()?;
                                let identity = MaterializedStatePageBody {
                                    bytes: u64::try_from(body.len())
                                        .assured("a bounded body fits u64"),
                                    digest: nervix_crypto::digest::sha256(&body),
                                };
                                Ok::<_, Report<nervix_execution::Cancelled>>((identity, body))
                            },
                        )
                        .await
                        .map_err(StreamHandlerError::with_cause)?
                        .map_err(StreamHandlerError::with_cause)?;
                    (
                        Ok(MaterializedStatePageTransfer {
                            page: descriptor,
                            body: Some(identity),
                        }),
                        Some(body),
                    )
                }
            }
        }
        Err(reason) => (Err(reason), None),
    };
    let (header, charge) = MaterializedStatePageReply::encode_rkyv(
        reply,
        executor.clone(),
        PoolClass::Bulk,
        PoolClass::Bulk.payload_limit(executor),
    )
    .await
    .map_err(StreamHandlerError::with_cause)?;
    let prefix_charge = executor
        .try_reserve(MemoryClass::Bulk, 8)
        .map_err(StreamHandlerError::with_cause)?;
    let prefix = ChargedBytes::from_owned(
        u64::try_from(header.len())
            .assured("a resident descriptor fits u64")
            .to_le_bytes()
            .to_vec(),
        prefix_charge,
    );
    let header = ChargedBytes::from_owned(header, charge);
    let total = prefix.len()
        + header.len()
        + match &body {
            Some(body) => body.len(),
            None => 0,
        };
    let chunk_bytes = usize::try_from(executor.limits().bulk_chunk_bytes.as_u64())
        .assured("the executor validates addressable chunks");
    let chunks = stream::iter([Some(prefix), Some(header), body].into_iter().flatten()).flat_map(
        move |bytes| {
            stream::unfold((bytes, 0), move |(bytes, offset)| async move {
                if offset == bytes.len() {
                    return None;
                }
                let end = offset + chunk_bytes.min(bytes.len() - offset);
                let chunk = bytes
                    .slice(offset, end)
                    .verified("the chunk is within its immutable allocation");
                Some((Ok::<_, Report<StreamHandlerError>>(chunk), (bytes, end)))
            })
        },
    );
    Ok(StreamingResponse::new(
        u64::try_from(total).assured("a bounded page fits u64"),
        chunks,
    ))
}

pub(crate) struct ReceivedMaterializedPage {
    pub(crate) descriptor: MaterializedStatePageDescriptor,
    snapshot: Option<RestoredMaterializedSnapshot>,
}

async fn receive_page_bytes(
    executor: &Executor,
    mut stream: nervix_interconnect::IncomingByteStream,
    max_bytes: u64,
) -> Result<ChargedBytes, Report<StatePageExchangeError>> {
    let header_bound = PoolClass::Bulk.payload_limit(executor);
    let with_header = max_bytes
        .checked_add(header_bound)
        .ok_or_else(|| Report::new(StatePageExchangeError::Framing))?;
    let bound = with_header
        .checked_add(8)
        .ok_or_else(|| Report::new(StatePageExchangeError::Framing))?;
    let length = stream.content_length();
    if length > bound || length < 8 {
        return Err(Report::new(StatePageExchangeError::Framing));
    }
    let charge = executor
        .try_reserve(MemoryClass::Bulk, length)
        .change_context(StatePageExchangeError::Admission)?;
    let mut buffer = BudgetedBuffer::with_limit(charge, length);
    while let Some(chunk) = stream
        .next_chunk()
        .await
        .change_context(StatePageExchangeError::Receive)?
    {
        buffer
            .try_extend_from_slice(&chunk)
            .change_context(StatePageExchangeError::Admission)?;
    }
    if u64::try_from(buffer.len()).assured("a bounded buffer fits u64") != length {
        return Err(Report::new(StatePageExchangeError::Framing));
    }
    Ok(ChargedBytes::from_buffer(buffer))
}

pub(crate) async fn receive_materialized_page(
    executor: &Executor,
    stream: nervix_interconnect::IncomingByteStream,
    schema: &nervix_primitives::sync::StdArc<arrow_schema::Schema>,
    request: &ReadMaterializedStatePage,
) -> Result<
    Result<ReceivedMaterializedPage, StateInspectionRejection>,
    Report<StatePageExchangeError>,
> {
    let max_bytes = request.max_bytes;
    let bytes = receive_page_bytes(executor, stream, max_bytes).await?;
    let header_bound = PoolClass::Bulk.payload_limit(executor);
    let prefix: [u8; 8] = bytes[..8]
        .try_into()
        .verified("the declared minimum is checked before receiving");
    let header_length = u64::from_le_bytes(prefix);
    if header_length > header_bound {
        return Err(Report::new(StatePageExchangeError::Framing));
    }
    let header_bytes =
        usize::try_from(header_length).map_err(|_| Report::new(StatePageExchangeError::Framing))?;
    let end = header_bytes
        .checked_add(8)
        .ok_or_else(|| Report::new(StatePageExchangeError::Framing))?;
    if end > bytes.len() {
        return Err(Report::new(StatePageExchangeError::Framing));
    }
    // The descriptor is bounded independently before copying for the shared rkyv decoder.
    let header_charge = executor
        .try_reserve(MemoryClass::Bulk, header_length.max(1))
        .change_context(StatePageExchangeError::Admission)?;
    let (reply, _decoded_charge) = MaterializedStatePageReply::decode_rkyv(
        executor.clone(),
        PoolClass::Bulk,
        bytes[8..end].to_vec(),
    )
    .await
    .change_context(StatePageExchangeError::Descriptor)?;
    drop(header_charge);
    let transfer = match reply {
        Ok(transfer) => transfer,
        Err(reason) => {
            if end != bytes.len() {
                return Err(Report::new(StatePageExchangeError::Framing));
            }
            return Ok(Err(reason));
        }
    };
    let descriptor = transfer.page;
    let start_offset = match request.after {
        Some((_, offset)) => offset,
        None => 0,
    };
    let count = descriptor
        .offset
        .checked_sub(start_offset)
        .ok_or_else(|| Report::new(StatePageExchangeError::Framing))?;
    if count > request.max_rows {
        return Err(Report::new(StatePageExchangeError::Framing));
    }
    if descriptor.more != (descriptor.offset < descriptor.counts.rows)
        || descriptor.offset > descriptor.counts.rows
    {
        return Err(Report::new(StatePageExchangeError::Framing));
    }
    let body = bytes
        .slice(end, bytes.len())
        .verified("the descriptor was bounded within the stream");
    let Some(identity) = transfer.body else {
        if !body.is_empty() || count != 0 {
            return Err(Report::new(StatePageExchangeError::Framing));
        }
        return Ok(Ok(ReceivedMaterializedPage {
            descriptor,
            snapshot: None,
        }));
    };
    if identity.bytes > max_bytes
        || identity.bytes != u64::try_from(body.len()).assured("a resident body fits u64")
        || count == 0
    {
        return Err(Report::new(StatePageExchangeError::Framing));
    }
    let charge = executor
        .try_reserve(MemoryClass::Bulk, 1)
        .change_context(StatePageExchangeError::Admission)?;
    let body = executor
        .run_cpu(
            nervix_execution::CpuClass::Bulk,
            charge,
            move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(StatePageExchangeError::Receive)?;
                if nervix_crypto::digest::sha256(&body) != identity.digest {
                    return Err(Report::new(StatePageExchangeError::Framing));
                }
                Ok(body)
            },
        )
        .await
        .change_context(StatePageExchangeError::Receive)??;
    let snapshot =
        RestoredMaterializedSnapshot::open_relay(executor, schema, SealedSource::memory(body))
            .await
            .change_context(StatePageExchangeError::Columns)?;
    validate_received_snapshot(&snapshot, &descriptor, count)?;
    Ok(Ok(ReceivedMaterializedPage {
        descriptor,
        snapshot: Some(snapshot),
    }))
}

fn validate_received_snapshot(
    snapshot: &RestoredMaterializedSnapshot,
    descriptor: &MaterializedStatePageDescriptor,
    count: u64,
) -> Result<(), Report<StatePageExchangeError>> {
    let last_branch = match snapshot.records.last() {
        Some(record) => record.branch.as_ref().map(|branch| branch.fingerprint()),
        None => None,
    };
    if snapshot.revision != descriptor.origin.revision
        || snapshot.fence != descriptor.fence
        || snapshot.branch_generation != descriptor.origin.generation
        || u64::try_from(snapshot.records.len()).assured("bounded records fit u64") != count
        || last_branch != descriptor.last_branch
    {
        return Err(Report::new(StatePageExchangeError::Framing));
    }
    Ok(())
}

impl ReceivedMaterializedPage {
    pub(crate) fn len(&self) -> usize {
        match &self.snapshot {
            Some(snapshot) => snapshot.records.len(),
            None => 0,
        }
    }
    pub(crate) fn record(&self, index: usize) -> Option<MaterializedGenerationRecord> {
        self.snapshot
            .as_ref()?
            .records
            .get(index)
            .map(|record| MaterializedGenerationRecord {
                branch: record.branch.clone(),
                row: record.row.clone(),
            })
    }
}
