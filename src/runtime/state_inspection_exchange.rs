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

use arrow_array::RecordBatch;
use error_stack::{Report, ResultExt as _};
use futures_util::{StreamExt as _, stream};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{BudgetedBuffer, ChargedBytes, Executor, MemoryClass, Reservation};
use nervix_interconnect::{
    MaterializedStatePageDescriptor, MaterializedStatePageReply, MaterializedStatePageTransfer,
    PoolClass, ProcessorStatePageDescriptor, ProcessorStatePageReply,
    ProcessorStateSectionTransfer, ReadMaterializedStatePage, ReadProcessorStatePage, RkyvMessage,
    StatePageBody, StreamHandlerError, StreamingResponse,
};
use nervix_models::{ProcessorStateSection, SchemaField, StateInspectionRejection};
use nervix_primitives::sync::StdArc;

use super::{
    MaterializedGenerationRecord, PageReply, ProcessorStatePageMemory, ReplyMemory,
    RestoredMaterializedSnapshot, SealedSource, state_inspection_read::MaterializedInspectionPage,
};
use crate::runtime_schema::{ArrowBodyError, RuntimeRecordBatch, batch_payload_bytes};

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

/// The identity of `body`, computed on the bulk executor beside the body it names.
async fn identified_body(
    executor: &Executor,
    body: ChargedBytes,
) -> Result<(StatePageBody, ChargedBytes), Report<StreamHandlerError>> {
    let charge = executor
        .try_reserve(MemoryClass::Bulk, 1)
        .map_err(StreamHandlerError::with_cause)?;
    executor
        .run_cpu(
            nervix_execution::CpuClass::Bulk,
            charge,
            move |_charge, cancellation| {
                cancellation.check()?;
                let identity = StatePageBody {
                    bytes: u64::try_from(body.len()).assured("a bounded body fits u64"),
                    digest: nervix_crypto::digest::sha256(&body),
                };
                Ok::<_, Report<nervix_execution::Cancelled>>((identity, body))
            },
        )
        .await
        .map_err(StreamHandlerError::with_cause)?
        .map_err(StreamHandlerError::with_cause)
}

/// Frames one encoded reply and the bodies it names as a page stream: an eight-byte little-endian
/// reply length, the bounded rkyv reply, then every body in order. Its chunks share the admitted
/// allocations without copying.
fn page_stream(
    executor: &Executor,
    header: Vec<u8>,
    header_charge: Reservation,
    bodies: Vec<ChargedBytes>,
) -> Result<StreamingResponse, Report<StreamHandlerError>> {
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
    let header = ChargedBytes::from_owned(header, header_charge);
    let mut total = prefix
        .len()
        .checked_add(header.len())
        .assured("a bounded descriptor and its prefix fit usize");
    for body in &bodies {
        total = total
            .checked_add(body.len())
            .assured("resident page allocations fit usize");
    }
    let chunk_bytes = usize::try_from(executor.limits().bulk_chunk_bytes.as_u64())
        .assured("the executor validates addressable chunks");
    // The length prefix and the descriptor precede the bodies.
    let part_count = bodies
        .len()
        .checked_add(2)
        .assured("a page carries far fewer bodies than usize::MAX");
    let mut parts = Vec::with_capacity(part_count);
    parts.push(prefix);
    parts.push(header);
    parts.extend(bodies);
    let chunks = stream::iter(parts).flat_map(move |bytes| {
        stream::unfold((bytes, 0), move |(bytes, offset)| async move {
            if offset == bytes.len() {
                return None;
            }
            let remaining = bytes
                .len()
                .checked_sub(offset)
                .verified("the offset never passes the allocation's end");
            let end = offset
                .checked_add(chunk_bytes.min(remaining))
                .verified("a chunk ends within its allocation");
            let chunk = bytes
                .slice(offset, end)
                .verified("the chunk is within its immutable allocation");
            Some((Ok::<_, Report<StreamHandlerError>>(chunk), (bytes, end)))
        })
    });
    Ok(StreamingResponse::new(
        u64::try_from(total).assured("a bounded page fits u64"),
        chunks,
    ))
}

/// Stream framing is an eight-byte little-endian descriptor length, its bounded rkyv bytes,
/// then the native Arrow container.
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
                    let (identity, body) = identified_body(executor, body).await?;
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
    page_stream(executor, header, charge, body.into_iter().collect())
}

/// One section's returned rows as the owner hands them to its page stream, in row order; empty
/// when the page returns none of the section's rows.
pub(crate) struct ProcessorPageSection {
    pub(crate) batches: Vec<RecordBatch>,
}

/// Stream framing is the descriptor framing of every page, then the bodies
/// [`processor_page_reply`] names, in order. `memory` is what the owner's page holds, with what it
/// set aside for this reply; the rest goes back to the bulk pool once the bodies carry the rows.
pub(crate) async fn stream_processor_page(
    executor: &Executor,
    result: Result<
        (ProcessorStatePageDescriptor, Vec<ProcessorPageSection>),
        StateInspectionRejection,
    >,
    max_bytes: u64,
    mut memory: Option<ProcessorStatePageMemory>,
) -> Result<StreamingResponse, Report<StreamHandlerError>> {
    let set_aside = match memory.as_mut() {
        Some(memory) => memory.take_reply(),
        None => None,
    };
    let PageReplyParts {
        reply,
        bodies,
        descriptor,
        split,
    } = processor_page_reply(executor, result, max_bytes, set_aside).await?;
    // The bodies now carry the page's rows, so the memory that held them goes back to the pool
    // before the descriptor is encoded.
    drop(memory);
    let limit = PoolClass::Bulk.payload_limit(executor);
    let descriptor = match (descriptor, split) {
        (Some(charge), _) => Some(charge),
        (None, false) => None,
        // A page whose sections travel in more bodies than one each takes no memory it would wait
        // for while it holds them: without room for its descriptor now, it is not ready.
        (None, true) => match executor.try_reserve(MemoryClass::Bulk, limit) {
            Ok(charge) => Some(charge),
            Err(_refused) => {
                drop(bodies);
                return stream_refusal(executor, StateInspectionRejection::NotReady).await;
            }
        },
    };
    let encoded = match descriptor {
        Some(charge) => {
            reply
                .encode_rkyv_within(
                    executor.clone(),
                    PoolClass::Bulk,
                    limit,
                    nervix_execution::QueueAdmission::RefuseWhenFull,
                    charge,
                )
                .await
        }
        None => {
            reply
                .encode_rkyv(executor.clone(), PoolClass::Bulk, limit)
                .await
        }
    };
    let (header, charge) = encoded.map_err(StreamHandlerError::with_cause)?;
    page_stream(executor, header, charge, bodies)
}

/// The page stream of an owner's typed refusal, which carries no body. It holds nothing else
/// while it waits for the room its descriptor takes.
async fn stream_refusal(
    executor: &Executor,
    reason: StateInspectionRejection,
) -> Result<StreamingResponse, Report<StreamHandlerError>> {
    let reply: ProcessorStatePageReply = Err(reason);
    let (header, charge) = reply
        .encode_rkyv(
            executor.clone(),
            PoolClass::Bulk,
            PoolClass::Bulk.payload_limit(executor),
        )
        .await
        .map_err(StreamHandlerError::with_cause)?;
    page_stream(executor, header, charge, Vec::new())
}

/// A processor page reply ready to frame.
struct PageReplyParts {
    reply: ProcessorStatePageReply,
    /// The bodies the reply names, in order.
    bodies: Vec<ChargedBytes>,
    /// What the page set aside for encoding the reply's descriptor; absent for a page that set
    /// nothing aside, and for a refusal.
    descriptor: Option<Reservation>,
    /// Whether a section travels in more than one body.
    split: bool,
}

impl PageReplyParts {
    /// A refusal: it names no body, and everything the page set aside goes back to the pool.
    fn refused(reason: StateInspectionRejection) -> Self {
        Self {
            reply: Err(reason),
            bodies: Vec::new(),
            descriptor: None,
            split: false,
        }
    }
}

/// The reply a processor page stream frames and the bodies it names: each section that returns
/// rows as one or more native Arrow snapshot sections, in row order and each within the node's
/// section bound, in the descriptor's section order. The bodies take their memory from what the
/// page set aside in `set_aside`, or from the bulk pool for a page that set nothing aside. A page
/// whose bodies exceed `max_bytes`, or that returns a row whose own body exceeds the section
/// bound, is refused as too large, and one whose bodies its set-aside memory and the pool cannot
/// hold at once is not ready. A refusal names no body.
async fn processor_page_reply(
    executor: &Executor,
    result: Result<
        (ProcessorStatePageDescriptor, Vec<ProcessorPageSection>),
        StateInspectionRejection,
    >,
    max_bytes: u64,
    set_aside: Option<ReplyMemory>,
) -> Result<PageReplyParts, Report<StreamHandlerError>> {
    let (mut descriptor, sections) = match result {
        Ok(page) => page,
        Err(reason) => return Ok(PageReplyParts::refused(reason)),
    };
    if descriptor.sections.len() != sections.len() {
        return Err(StreamHandlerError::with_cause(Report::new(
            StatePageExchangeError::Framing,
        )));
    }
    let (descriptor_charge, mut charges) = match set_aside {
        Some(reply) => (
            Some(reply.descriptor),
            BodyCharges::SetAside {
                remaining: Some(reply.bodies),
                framing: reply.framing,
            },
        ),
        None => (
            None,
            BodyCharges::Pool {
                framing: PageReply::body_framing(executor),
            },
        ),
    };
    let mut bodies = Vec::new();
    let mut bytes = 0_u64;
    let mut split = false;
    for (transfer, section) in descriptor.sections.iter_mut().zip(sections) {
        let encoded = match section_bodies(executor, section.batches, &mut charges).await? {
            Ok(encoded) => encoded,
            Err(reason) => return Ok(PageReplyParts::refused(reason)),
        };
        if encoded.len() > 1 {
            split = true;
        }
        for body in encoded {
            bytes = bytes
                .checked_add(u64::try_from(body.len()).assured("resident allocations fit u64"))
                .assured("resident page allocations fit u64");
            if bytes > max_bytes {
                return Ok(PageReplyParts::refused(
                    StateInspectionRejection::ReplyTooLarge,
                ));
            }
            let (identity, body) = identified_body(executor, body).await?;
            transfer.bodies.push(identity);
            bodies.push(body);
        }
    }
    Ok(PageReplyParts {
        reply: Ok(descriptor),
        bodies,
        descriptor: descriptor_charge,
        split,
    })
}

/// Where a page's Arrow bodies take their memory.
enum BodyCharges {
    /// What the page set aside for its bodies when it admitted their rows. Each body takes its
    /// rows' bytes and `framing` from it, and only what it lacks from the bulk pool, at once.
    SetAside {
        remaining: Option<Reservation>,
        framing: u64,
    },
    /// The bulk pool, for a page that set nothing aside. A section's first body waits for room
    /// there, as every section's one body did before sections could travel in more than one; a
    /// further body of the same section takes its rows' bytes and `framing` at once or not at
    /// all, since it would otherwise wait while holding the section's earlier bodies.
    Pool { framing: u64 },
}

/// What encoding one part of a section's rows came to.
enum PartBody {
    /// The part's body, within the section bound.
    Encoded(ChargedBytes),
    /// The part's body would pass the section bound.
    PastLimit,
    /// The memory for the part's body cannot be had at once.
    Declined,
}

impl BodyCharges {
    /// Encodes `part`, whose columns take `payload` bytes, as one body within `limit`. `first`
    /// says whether it is its section's first body.
    async fn encode(
        &mut self,
        executor: &Executor,
        part: RuntimeRecordBatch,
        payload: u64,
        limit: u64,
        first: bool,
    ) -> Result<PartBody, Report<StreamHandlerError>> {
        let encoded = match self {
            Self::Pool { framing } => {
                if first {
                    part.encode_arrow_snapshot_section(
                        executor,
                        nervix_execution::QueueAdmission::RefuseWhenFull,
                    )
                    .await
                } else {
                    let wanted = body_charge(payload, *framing, limit);
                    let charge = match executor.try_reserve(MemoryClass::Bulk, wanted) {
                        Ok(charge) => charge,
                        Err(_refused) => return Ok(PartBody::Declined),
                    };
                    part.encode_arrow_snapshot_section_within(
                        executor,
                        nervix_execution::QueueAdmission::RefuseWhenFull,
                        charge,
                    )
                    .await
                }
            }
            Self::SetAside { remaining, framing } => {
                let wanted = body_charge(payload, *framing, limit);
                let Some(charge) = take_body_charge(executor, remaining, wanted)? else {
                    return Ok(PartBody::Declined);
                };
                part.encode_arrow_snapshot_section_within(
                    executor,
                    nervix_execution::QueueAdmission::RefuseWhenFull,
                    charge,
                )
                .await
            }
        };
        match encoded {
            Ok(body) => Ok(PartBody::Encoded(body)),
            Err(error) => {
                if let ArrowBodyError::EncodedPastLimit { .. } = error.current_context() {
                    return Ok(PartBody::PastLimit);
                }
                Err(StreamHandlerError::with_cause(error))
            }
        }
    }
}

/// The memory one body of rows taking `payload` bytes is encoded into: its rows' bytes and its
/// `framing`, at most the section bound `limit`.
fn body_charge(payload: u64, framing: u64, limit: u64) -> u64 {
    match payload.checked_add(framing) {
        Some(wanted) => wanted.min(limit),
        None => limit,
    }
}

/// Takes `wanted` bytes for one body from what the page set aside, topping them up from the bulk
/// pool at once when the set-aside memory falls short. `None` when the pool has no room for the
/// rest now.
fn take_body_charge(
    executor: &Executor,
    remaining: &mut Option<Reservation>,
    wanted: u64,
) -> Result<Option<Reservation>, Report<StreamHandlerError>> {
    let Some(held) = remaining.take() else {
        // Nothing is left of what the page set aside: all of it from the pool now, or none.
        return match executor.try_reserve(MemoryClass::Bulk, wanted) {
            Ok(charge) => Ok(Some(charge)),
            Err(_refused) => Ok(None),
        };
    };
    let available = held.bytes();
    if available >= wanted {
        let (charge, rest) = held.split(wanted).map_err(StreamHandlerError::with_cause)?;
        *remaining = Some(rest);
        return Ok(Some(charge));
    }
    let lacking = wanted
        .checked_sub(available)
        .verified("the set-aside memory falls short of the body");
    match executor.try_reserve(MemoryClass::Bulk, lacking) {
        Ok(extra) => {
            let charge = held.merge(extra).map_err(StreamHandlerError::with_cause)?;
            Ok(Some(charge))
        }
        Err(_refused) => Ok(None),
    }
}

/// One section's returned rows as the native Arrow snapshot sections a page carries them in, in
/// row order, each within the node's section bound. Rows whose body would pass the bound travel
/// in halves until each part fits. The page goes without them as too large when a single row's
/// body passes the bound, and as not ready when the memory for a body cannot be had at once.
async fn section_bodies(
    executor: &Executor,
    batches: Vec<RecordBatch>,
    charges: &mut BodyCharges,
) -> Result<Result<Vec<ChargedBytes>, StateInspectionRejection>, Report<StreamHandlerError>> {
    let limit = executor.limits().snapshot_section_bytes.as_u64();
    let mut bodies = Vec::with_capacity(batches.len());
    for batch in batches {
        // The row ranges still to carry, the next one last, so the bodies keep the rows' order.
        let mut pending = Vec::<std::ops::Range<usize>>::new();
        pending.push(0..batch.num_rows());
        while let Some(rows) = pending.pop() {
            nervix_primitives::task::consume_budget().await;
            if rows.is_empty() {
                continue;
            }
            let part = batch.slice(rows.start, rows.len());
            let payload = batch_payload_bytes(&part);
            // Rows whose columns alone pass the bound cannot fit one body, so they are halved
            // without being encoded first.
            let encoded = if payload <= limit {
                let part = RuntimeRecordBatch::from_record_batch(part.schema(), part)
                    .map_err(StreamHandlerError::with_cause)?;
                let first = bodies.is_empty();
                charges
                    .encode(executor, part, payload, limit, first)
                    .await?
            } else {
                PartBody::PastLimit
            };
            match encoded {
                PartBody::Encoded(body) => bodies.push(body),
                PartBody::Declined => return Ok(Err(StateInspectionRejection::NotReady)),
                PartBody::PastLimit => {
                    if rows.len() == 1 {
                        return Ok(Err(StateInspectionRejection::ReplyTooLarge));
                    }
                    let middle = rows
                        .start
                        .checked_add(rows.len().div_ceil(2))
                        .assured("half of a range ends inside it");
                    pending.push(middle..rows.end);
                    pending.push(rows.start..middle);
                }
            }
        }
    }
    Ok(Ok(bodies))
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

/// Where a received page stream's descriptor ends. The descriptor is bounded before it is decoded.
fn descriptor_end(
    executor: &Executor,
    bytes: &ChargedBytes,
) -> Result<usize, Report<StatePageExchangeError>> {
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
    Ok(end)
}

/// Decodes the reply a page stream frames before `end`.
async fn decode_reply<R: RkyvMessage>(
    executor: &Executor,
    bytes: &ChargedBytes,
    end: usize,
) -> Result<R, Report<StatePageExchangeError>> {
    let header_length = u64::try_from(
        end.checked_sub(8)
            .verified("a descriptor ends after its eight-byte length"),
    )
    .assured("a bounded descriptor length fits u64");
    // The descriptor is bounded independently before copying for the shared rkyv decoder.
    let header_charge = executor
        .try_reserve(MemoryClass::Bulk, header_length.max(1))
        .change_context(StatePageExchangeError::Admission)?;
    let (reply, _decoded_charge) =
        R::decode_rkyv(executor.clone(), PoolClass::Bulk, bytes[8..end].to_vec())
            .await
            .change_context(StatePageExchangeError::Descriptor)?;
    drop(header_charge);
    Ok(reply)
}

/// Checks `body` against the digest its descriptor names on the bulk executor, before anything
/// decodes it.
async fn verified_body(
    executor: &Executor,
    body: ChargedBytes,
    identity: StatePageBody,
) -> Result<ChargedBytes, Report<StatePageExchangeError>> {
    let charge = executor
        .try_reserve(MemoryClass::Bulk, 1)
        .change_context(StatePageExchangeError::Admission)?;
    executor
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
        .change_context(StatePageExchangeError::Receive)?
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
    let end = descriptor_end(executor, &bytes)?;
    let reply: MaterializedStatePageReply = decode_reply(executor, &bytes, end).await?;
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
    let body = verified_body(executor, body, identity).await?;
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

/// One received section of a processor page: every row of it the unit holds, its complete fields,
/// and its returned rows in the batches its bodies carried, in row order; empty when the page
/// returned none of them.
pub(crate) struct ReceivedProcessorSection {
    pub(crate) section: ProcessorStateSection,
    pub(crate) fields: Vec<SchemaField>,
    pub(crate) rows: u64,
    pub(crate) batches: Vec<RecordBatch>,
}

pub(crate) struct ReceivedProcessorPage {
    pub(crate) descriptor: ProcessorStatePageDescriptor,
    pub(crate) sections: Vec<ReceivedProcessorSection>,
}

/// The rows a processor page returns of each of its sections, in descriptor order, once its
/// counts, offset, sections and runs agree with the read that asked for it.
fn processor_page_rows(
    descriptor: &ProcessorStatePageDescriptor,
    request: &ReadProcessorStatePage,
    payload: &[(ProcessorStateSection, Vec<SchemaField>)],
) -> Result<Vec<u64>, Report<StatePageExchangeError>> {
    let framing = || Report::new(StatePageExchangeError::Framing);
    match (&request.partition_exclusions, &descriptor.partition_catalog) {
        (None, nervix_models::StateInspectionKeyCatalog::Unrequested) => {}
        (Some(excluded), catalog) if catalog.is_requested() => {
            let selected = catalog
                .fingerprints()
                .filter(|key| !excluded.contains(*key))
                .count();
            let selected = u64::try_from(selected).map_err(|_| framing())?;
            if selected != descriptor.counts.branches {
                return Err(framing());
            }
            for key in catalog.fingerprints() {
                match request.selection {
                    nervix_models::StateInspectionBranchSelection::All => {}
                    nervix_models::StateInspectionBranchSelection::Branch(wanted)
                        if key == &wanted => {}
                    _ => return Err(framing()),
                }
            }
        }
        _ => return Err(framing()),
    }
    let start = match request.after {
        Some((_, offset)) => offset,
        None => 0,
    };
    let count = descriptor.offset.checked_sub(start).ok_or_else(framing)?;
    let section_bound = match request.processor.kind {
        nervix_models::ModelKind::Inferencer => payload.len(),
        _ => ProcessorStateSection::FIXED_SECTION_COUNT,
    };
    if count > request.max_rows
        || (request.max_rows == 0 && !descriptor.runs.is_empty())
        || descriptor.more != (descriptor.offset < descriptor.counts.rows)
        || descriptor.offset > descriptor.counts.rows
        || descriptor.sections.len() > section_bound
    {
        return Err(framing());
    }
    if payload.is_empty() {
        if count != 0 {
            return Err(framing());
        }
    } else if descriptor.sections.len() != payload.len() {
        return Err(framing());
    }
    let mut section_rows = vec![0_u64; descriptor.sections.len()];
    let mut returned = 0_u64;
    for run in &descriptor.runs {
        let Some(index) = run
            .section
            .index_in(&descriptor.sections, |section| section.section)
        else {
            return Err(framing());
        };
        if run.rows == 0 {
            return Err(framing());
        }
        section_rows[index] = section_rows[index]
            .checked_add(run.rows)
            .ok_or_else(framing)?;
        if !run.section.is_metadata() {
            returned = returned.checked_add(run.rows).ok_or_else(framing)?;
        }
    }
    if returned != count {
        return Err(framing());
    }
    Ok(section_rows)
}

/// The Arrow bodies a processor page stream carries after its descriptor, taken in section order
/// within the read's byte bound.
struct ProcessorPageBodies<'a> {
    bytes: &'a ChargedBytes,
    /// Where the next body starts in the stream.
    offset: usize,
    /// The body bytes taken so far.
    received: u64,
    max_bytes: u64,
}

impl ProcessorPageBodies<'_> {
    /// The `rows` returned rows of one section, in the batches its bodies carry them in. A
    /// section names no body exactly when it returns no row. Otherwise each body it names is the
    /// next in the stream, within the read's byte bound, verified against its digest and decoded
    /// with exactly the section's declared fields; each holds at least one row, and together they
    /// hold exactly those rows.
    async fn section(
        &mut self,
        executor: &Executor,
        transfer: &ProcessorStateSectionTransfer,
        rows: u64,
    ) -> Result<Vec<RecordBatch>, Report<StatePageExchangeError>> {
        let framing = || Report::new(StatePageExchangeError::Framing);
        if transfer.bodies.is_empty() {
            if rows != 0 {
                return Err(framing());
            }
            return Ok(Vec::new());
        }
        // Every body holds at least one row, so a section names no more bodies than rows.
        let named = u64::try_from(transfer.bodies.len()).map_err(|_| framing())?;
        if named > rows {
            return Err(framing());
        }
        let schema = StdArc::new(SchemaField::arrow_schema(&transfer.fields));
        let mut batches = Vec::with_capacity(transfer.bodies.len());
        let mut carried = 0_u64;
        for identity in &transfer.bodies {
            self.received = self
                .received
                .checked_add(identity.bytes)
                .ok_or_else(framing)?;
            if self.received > self.max_bytes {
                return Err(framing());
            }
            let length = usize::try_from(identity.bytes).map_err(|_| framing())?;
            let end = self.offset.checked_add(length).ok_or_else(framing)?;
            let body = self.bytes.slice(self.offset, end).ok_or_else(framing)?;
            self.offset = end;
            let body = verified_body(executor, body, identity.clone()).await?;
            let decoded = RuntimeRecordBatch::decode_arrow_snapshot_section(
                executor,
                nervix_execution::QueueAdmission::RefuseWhenFull,
                StdArc::clone(&schema),
                body,
            )
            .await
            .change_context(StatePageExchangeError::Columns)?;
            let decoded_rows =
                u64::try_from(decoded.batch().num_rows()).assured("a decoded row count fits u64");
            if decoded_rows == 0 {
                return Err(framing());
            }
            carried = carried.checked_add(decoded_rows).ok_or_else(framing)?;
            batches.push(decoded.batch().clone());
        }
        if carried != rows {
            return Err(framing());
        }
        Ok(batches)
    }
}

/// Receives one processor page and checks it against the read before any row is used, as
/// [`ReceivedProcessorPage::decode`] describes.
pub(crate) async fn receive_processor_page(
    executor: &Executor,
    stream: nervix_interconnect::IncomingByteStream,
    request: &ReadProcessorStatePage,
    payload: &[(ProcessorStateSection, Vec<SchemaField>)],
) -> Result<Result<ReceivedProcessorPage, StateInspectionRejection>, Report<StatePageExchangeError>>
{
    let bytes = receive_page_bytes(executor, stream, request.max_bytes).await?;
    ReceivedProcessorPage::decode(executor, &bytes, request, payload).await
}

impl ReceivedProcessorPage {
    /// Decodes the bytes of one received processor page stream and checks them against the read
    /// before any row is used. `bytes` is the whole stream [`receive_page_bytes`] took, so it
    /// holds at least the descriptor's eight-byte length.
    ///
    /// `payload` names the sections the coordinator expects, in order, each with the payload fields
    /// its own committed schema gives it: a section that returns rows declares exactly those fields
    /// first. A count-only read passes no section and accepts no returned row. Every body is
    /// bounded, verified against its digest and decoded with exactly its declared fields, and the
    /// runs, the returned offset and the row counts must agree with the bodies and the descriptor.
    async fn decode(
        executor: &Executor,
        bytes: &ChargedBytes,
        request: &ReadProcessorStatePage,
        payload: &[(ProcessorStateSection, Vec<SchemaField>)],
    ) -> Result<Result<Self, StateInspectionRejection>, Report<StatePageExchangeError>> {
        let framing = || Report::new(StatePageExchangeError::Framing);
        let end = descriptor_end(executor, bytes)?;
        let reply: ProcessorStatePageReply = decode_reply(executor, bytes, end).await?;
        let descriptor = match reply {
            Ok(descriptor) => descriptor,
            Err(reason) => {
                if end != bytes.len() {
                    return Err(framing());
                }
                return Ok(Err(reason));
            }
        };
        let section_rows = processor_page_rows(&descriptor, request, payload)?;
        let mut bodies = ProcessorPageBodies {
            bytes,
            offset: end,
            received: 0,
            max_bytes: request.max_bytes,
        };
        let mut sections = Vec::with_capacity(descriptor.sections.len());
        for (index, transfer) in descriptor.sections.iter().enumerate() {
            if let Some((expected, fields)) = payload.get(index) {
                let declared = transfer.fields.get(..fields.len());
                if transfer.section != *expected
                    || (!transfer.bodies.is_empty() && declared != Some(fields.as_slice()))
                {
                    return Err(framing());
                }
            }
            let rows = section_rows[index];
            if rows > transfer.rows {
                return Err(framing());
            }
            let batches = bodies.section(executor, transfer, rows).await?;
            sections.push(ReceivedProcessorSection {
                section: transfer.section,
                fields: transfer.fields.clone(),
                rows: transfer.rows,
                batches,
            });
        }
        if bodies.offset != bytes.len() {
            return Err(framing());
        }
        Ok(Ok(Self {
            descriptor,
            sections,
        }))
    }
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

#[cfg(test)]
#[path = "state_inspection_exchange_tests.rs"]
mod tests;
