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
use nervix_execution::{
    AdmissionError, BudgetedBuffer, ChargedBytes, Executor, MemoryClass, Reservation,
};
use nervix_interconnect::{
    MaterializedStatePageBody, MaterializedStatePageDescriptor, MaterializedStatePageGroup,
    MaterializedStatePageReply, MaterializedStatePageTransfer, PoolClass,
    ProcessorStatePageDescriptor, ProcessorStatePageReply, ProcessorStateSectionTransfer,
    ReadMaterializedStatePage, ReadProcessorStatePage, RkyvMessage, StatePageBody,
    StreamHandlerError, StreamingResponse,
};
use nervix_models::{ProcessorStateSection, SchemaField, StateInspectionRejection};
use nervix_primitives::{sync::StdArc, time::Instant};

use super::{
    MaterializedGenerationRecord, PageReply, ProcessorStatePageMemory, ReplyMemory,
    RestoredMaterializedSnapshot, SealedContainerLayout, SealedGroupLayout,
    materialized_snapshot::MaterializedSnapshotError,
    state_inspection_read::{MaterializedInspectionPage, MaterializedReplyMemory},
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

/// A body, the identity a page stream names it by, and the charge of the job that computed it.
struct IdentifiedBody {
    identity: StatePageBody,
    body: ChargedBytes,
    job: Reservation,
}

/// The identity of `body`, computed on the bulk executor beside the body it names, by a job that
/// draws on `job`, memory the owner admitted for it ([`PageReply::DIGEST_JOB_BYTES`]) and gets back.
///
/// The body is a page the owner already read and encoded, so a full bulk queue holds the digest
/// back rather than throwing the page away. The caller's reset of the page stream ends the wait.
async fn identified_body(
    executor: &Executor,
    body: ChargedBytes,
    job: Reservation,
) -> Result<IdentifiedBody, Report<StreamHandlerError>> {
    executor
        .run_cpu(
            nervix_execution::CpuClass::Bulk,
            job,
            move |job, cancellation| {
                cancellation.check()?;
                let identity = StatePageBody {
                    bytes: u64::try_from(body.len()).assured("a bounded body fits u64"),
                    digest: nervix_crypto::digest::sha256(&body),
                };
                Ok::<_, Report<nervix_execution::Cancelled>>(IdentifiedBody {
                    identity,
                    body,
                    job,
                })
            },
        )
        .await
        .map_err(StreamHandlerError::with_cause)?
        .map_err(StreamHandlerError::with_cause)
}

/// Frames one encoded reply and the bodies it names as a page stream: an eight-byte little-endian
/// reply length, held in `prefix_charge`, the bounded rkyv reply, then every body in order. Its
/// chunks share the admitted allocations without copying.
fn page_stream(
    executor: &Executor,
    header: Vec<u8>,
    header_charge: Reservation,
    prefix_charge: Reservation,
    bodies: Vec<ChargedBytes>,
) -> Result<StreamingResponse, Report<StreamHandlerError>> {
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

impl From<&SealedGroupLayout> for MaterializedStatePageGroup {
    fn from(group: &SealedGroupLayout) -> Self {
        Self {
            identities: group.identities,
            columns: group.columns,
        }
    }
}

impl From<&MaterializedStatePageGroup> for SealedGroupLayout {
    fn from(group: &MaterializedStatePageGroup) -> Self {
        Self {
            identities: group.identities,
            columns: group.columns,
        }
    }
}

/// Stream framing is an eight-byte little-endian descriptor length, its bounded rkyv bytes,
/// then the native Arrow container, whose layout the descriptor names.
///
/// The page set aside its reply's memory as it selected its rows, so encoding the container, the
/// digest, the descriptor and its length draw on that memory. The caller runs the page and its
/// stream within the page's allowance (`Executor::within_allowance`), so anything they lack is a
/// charge the reply requires: a share without room for it runs the read again within a larger
/// share, and nothing waits for room the page holds. A typed refusal holds nothing while it waits
/// for room for its descriptor.
pub(crate) async fn stream_materialized_page(
    executor: &Executor,
    result: Result<
        (MaterializedStatePageDescriptor, MaterializedInspectionPage),
        StateInspectionRejection,
    >,
    max_bytes: u64,
) -> Result<StreamingResponse, Report<StreamHandlerError>> {
    let (descriptor, mut page) = match result {
        Ok(page) => page,
        Err(reason) => {
            let reply: MaterializedStatePageReply = Err(reason);
            return stream_refusal(executor, reply).await;
        }
    };
    let MaterializedReplyMemory {
        descriptor: share,
        container,
    } = page.reply.take().ok_or_else(|| {
        StreamHandlerError::with_cause(Report::new(StatePageExchangeError::Admission))
    })?;
    let (job, share) = share
        .split(PageReply::DIGEST_JOB_BYTES)
        .map_err(StreamHandlerError::with_cause)?;
    let (reply, body) = if page.generation.records().is_empty() {
        drop(page);
        (
            Ok(MaterializedStatePageTransfer {
                page: descriptor,
                body: None,
            }),
            None,
        )
    } else {
        let container = page
            .generation
            .encode_resident_container_within(executor, container)
            .await
            .map_err(StreamHandlerError::with_cause)?;
        // The container carries the page's rows now, so the views that held them go back.
        drop(page);
        let length = u64::try_from(container.bytes.len()).assured("resident allocations fit u64");
        if length > max_bytes {
            (Err(StateInspectionRejection::ReplyTooLarge), None)
        } else {
            let IdentifiedBody { identity, body, .. } =
                identified_body(executor, container.bytes, job).await?;
            let groups = container
                .layout
                .groups
                .iter()
                .map(MaterializedStatePageGroup::from)
                .collect();
            (
                Ok(MaterializedStatePageTransfer {
                    page: descriptor,
                    body: Some(MaterializedStatePageBody {
                        identity,
                        records: container.layout.records,
                        groups,
                    }),
                }),
                Some(body),
            )
        }
    };
    let (prefix, descriptor_charge) = share
        .split(PageReply::DESCRIPTOR_LENGTH_BYTES)
        .map_err(StreamHandlerError::with_cause)?;
    // The reply is the owner's answer to a read it already served, so a full bulk queue holds its
    // encoding back; the caller's reset of the page stream ends the wait.
    let (header, charge) = reply
        .encode_rkyv_within(
            executor.clone(),
            PoolClass::Bulk,
            PoolClass::Bulk.payload_limit(executor),
            descriptor_charge,
        )
        .await
        .map_err(StreamHandlerError::with_cause)?;
    page_stream(executor, header, charge, prefix, body.into_iter().collect())
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
    } = processor_page_reply(executor, result, max_bytes, set_aside).await?;
    // The bodies now carry the page's rows, so the memory that held them goes back to the pool
    // before the descriptor is encoded.
    drop(memory);
    let limit = PoolClass::Bulk.payload_limit(executor);
    let framed = limit
        .checked_add(PageReply::DESCRIPTOR_LENGTH_BYTES)
        .assured("the bulk payload bound leaves room for its length");
    let share = match (descriptor, bodies.is_empty()) {
        (Some(charge), _) => charge,
        // A reply that holds no body, a refusal or a page that returns no row, holds nothing
        // while it waits for room for its descriptor and its length.
        (None, true) => executor
            .reserve(MemoryClass::Bulk, framed)
            .await
            .map_err(StreamHandlerError::with_cause)?,
        // A reply that holds bodies draws its descriptor's room as a charge it requires, through
        // the page's allowance: a share without room for it runs the read again within a larger
        // one, so the reply never waits for room while it holds its bodies.
        (None, false) => executor
            .reserve(MemoryClass::Bulk, framed)
            .await
            .map_err(StreamHandlerError::with_cause)?,
    };
    let (prefix, charge) = share
        .split(PageReply::DESCRIPTOR_LENGTH_BYTES)
        .map_err(StreamHandlerError::with_cause)?;
    // The reply is the owner's answer to a read it already served, so a full bulk queue holds its
    // encoding back; the caller's reset of the page stream ends the wait.
    let (header, charge) = reply
        .encode_rkyv_within(executor.clone(), PoolClass::Bulk, limit, charge)
        .await
        .map_err(StreamHandlerError::with_cause)?;
    page_stream(executor, header, charge, prefix, bodies)
}

/// The page stream of an owner's typed refusal `reply`, which carries no body. It holds nothing
/// else while it waits for the room its descriptor and the descriptor's length take. The refusal is
/// the owner's answer to a read it already served, so a full bulk queue holds its encoding back;
/// the caller's reset of the page stream ends the wait.
async fn stream_refusal<R: RkyvMessage>(
    executor: &Executor,
    reply: R,
) -> Result<StreamingResponse, Report<StreamHandlerError>> {
    let limit = PoolClass::Bulk.payload_limit(executor);
    let framed = limit
        .checked_add(PageReply::DESCRIPTOR_LENGTH_BYTES)
        .assured("the bulk payload bound leaves room for its length");
    let share = executor
        .reserve(MemoryClass::Bulk, framed)
        .await
        .map_err(StreamHandlerError::with_cause)?;
    let (prefix, charge) = share
        .split(PageReply::DESCRIPTOR_LENGTH_BYTES)
        .map_err(StreamHandlerError::with_cause)?;
    let (header, charge) = reply
        .encode_rkyv_within(executor.clone(), PoolClass::Bulk, limit, charge)
        .await
        .map_err(StreamHandlerError::with_cause)?;
    page_stream(executor, header, charge, prefix, Vec::new())
}

/// A processor page reply ready to frame.
struct PageReplyParts {
    reply: ProcessorStatePageReply,
    /// The bodies the reply names, in order.
    bodies: Vec<ChargedBytes>,
    /// What the page set aside for encoding the reply's descriptor; absent for a page that set
    /// nothing aside, and for a refusal.
    descriptor: Option<Reservation>,
}

impl PageReplyParts {
    /// A refusal: it names no body, and everything the page set aside goes back to the pool.
    fn refused(reason: StateInspectionRejection) -> Self {
        Self {
            reply: Err(reason),
            bodies: Vec::new(),
            descriptor: None,
        }
    }
}

/// The reply a processor page stream frames and the bodies it names: each section that returns
/// rows as one or more native Arrow snapshot sections, in row order and each within the node's
/// section bound, in the descriptor's section order. The bodies take their memory from what the
/// page set aside in `set_aside`, and what that lacks as a charge the reply requires: the caller
/// runs the page and its reply within the page's allowance, so a share without room for it runs
/// the read again within a larger share. A page whose bodies exceed `max_bytes`, or that returns a
/// row whose own body exceeds the section bound, is refused as too large. A refusal names no
/// body.
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
    let (descriptor_charge, mut job, mut charges) = match set_aside {
        Some(reply) => {
            let (job, descriptor) = reply
                .descriptor
                .split(PageReply::DIGEST_JOB_BYTES)
                .map_err(StreamHandlerError::with_cause)?;
            (
                Some(descriptor),
                Some(job),
                BodyCharges {
                    remaining: Some(reply.bodies),
                    framing: reply.framing,
                },
            )
        }
        None => (
            None,
            None,
            BodyCharges {
                remaining: None,
                framing: PageReply::body_framing(executor),
            },
        ),
    };
    let mut bodies = Vec::new();
    let mut bytes = 0_u64;
    for (transfer, section) in descriptor.sections.iter_mut().zip(sections) {
        let encoded = match section_bodies(executor, section.batches, &mut charges).await? {
            Ok(encoded) => encoded,
            Err(reason) => return Ok(PageReplyParts::refused(reason)),
        };
        for body in encoded {
            bytes = bytes
                .checked_add(u64::try_from(body.len()).assured("resident allocations fit u64"))
                .assured("resident page allocations fit u64");
            if bytes > max_bytes {
                return Ok(PageReplyParts::refused(
                    StateInspectionRejection::ReplyTooLarge,
                ));
            }
            // A page that set nothing aside takes its digest job as a charge the reply requires.
            let charge = match job.take() {
                Some(charge) => charge,
                None => executor
                    .reserve(MemoryClass::Bulk, PageReply::DIGEST_JOB_BYTES)
                    .await
                    .map_err(StreamHandlerError::with_cause)?,
            };
            let identified = identified_body(executor, body, charge).await?;
            job = Some(identified.job);
            transfer.bodies.push(identified.identity);
            bodies.push(identified.body);
        }
    }
    Ok(PageReplyParts {
        reply: Ok(descriptor),
        bodies,
        descriptor: descriptor_charge,
    })
}

/// Where a page's Arrow bodies take their memory: what the page set aside for them when it admitted
/// their rows, and what that lacks as a charge the reply requires. Each body takes its rows' bytes
/// and `framing`. A body never waits for room it holds: the reply draws through the page's
/// allowance, where a share without room for a body runs the read again within a larger share.
struct BodyCharges {
    /// What is left of the memory the page set aside; absent once it is spent, or for a page that
    /// set nothing aside.
    remaining: Option<Reservation>,
    framing: u64,
}

/// What encoding one part of a section's rows came to.
enum PartBody {
    /// The part's body, within the section bound.
    Encoded(ChargedBytes),
    /// The part's body would pass the section bound.
    PastLimit,
}

impl BodyCharges {
    /// Encodes `part`, whose columns take `payload` bytes, as one body within `limit`.
    async fn encode(
        &mut self,
        executor: &Executor,
        part: RuntimeRecordBatch,
        payload: u64,
        limit: u64,
    ) -> Result<PartBody, Report<StreamHandlerError>> {
        let wanted = body_charge(payload, self.framing, limit);
        let charge = take_body_charge(executor, &mut self.remaining, wanted).await?;
        // The part holds rows the owner already read, so a full bulk queue holds its encoding
        // back; the caller's reset of the page stream ends the wait.
        let encoded = part
            .encode_arrow_snapshot_section_within(executor, charge)
            .await;
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

/// Takes `wanted` bytes for one body from what the page set aside, and what the set-aside memory
/// lacks as a charge the reply requires, through the page's allowance.
async fn take_body_charge(
    executor: &Executor,
    remaining: &mut Option<Reservation>,
    wanted: u64,
) -> Result<Reservation, Report<StreamHandlerError>> {
    let Some(held) = remaining.take() else {
        // Nothing is left of what the page set aside: all of the body is required now.
        return executor
            .reserve(MemoryClass::Bulk, wanted)
            .await
            .map_err(StreamHandlerError::with_cause);
    };
    let available = held.bytes();
    if available >= wanted {
        let (charge, rest) = held.split(wanted).map_err(StreamHandlerError::with_cause)?;
        *remaining = Some(rest);
        return Ok(charge);
    }
    let lacking = wanted
        .checked_sub(available)
        .verified("the set-aside memory falls short of the body");
    let extra = executor
        .reserve(MemoryClass::Bulk, lacking)
        .await
        .map_err(StreamHandlerError::with_cause)?;
    held.merge(extra).map_err(StreamHandlerError::with_cause)
}

/// One section's returned rows as the native Arrow snapshot sections a page carries them in, in
/// row order, each within the node's section bound. Rows whose body would pass the bound travel
/// in halves until each part fits. The page goes without them as too large when a single row's
/// body passes the bound.
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
                charges.encode(executor, part, payload, limit).await?
            } else {
                PartBody::PastLimit
            };
            match encoded {
                PartBody::Encoded(body) => bodies.push(body),
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

/// Checks `body` against the digest its descriptor names on the bulk executor, before anything
/// decodes it, on `charge`, memory the caller already holds for the check, and gives that charge
/// back with the body.
///
/// The body is a page the owner already read, so a full bulk queue holds the check back rather
/// than discarding it; the caller's own read deadline bounds the wait.
async fn verified_body_within(
    executor: &Executor,
    body: ChargedBytes,
    identity: StatePageBody,
    charge: Reservation,
) -> Result<(ChargedBytes, Reservation), Report<StatePageExchangeError>> {
    executor
        .run_cpu(
            nervix_execution::CpuClass::Bulk,
            charge,
            move |charge, cancellation| {
                cancellation
                    .check()
                    .change_context(StatePageExchangeError::Receive)?;
                if nervix_crypto::digest::sha256(&body) != identity.digest {
                    return Err(Report::new(StatePageExchangeError::Framing));
                }
                Ok((body, charge))
            },
        )
        .await
        .change_context(StatePageExchangeError::Receive)?
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
    /// The bulk memory the decoded rows hold, kept until they are presented; absent when the page
    /// carries no body.
    pub(crate) memory: Option<ProcessorStatePageMemory>,
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

/// What a page reader reads its stream from: the length the stream declares before any of its
/// bytes, then exact reads straight into buffers whose room the reader already admitted.
pub(crate) trait PageStream: Send {
    /// The exact length the stream declares.
    fn content_length(&self) -> u64;
    /// Copies exactly the next `length` bytes into `buffer`, taking no memory of its own.
    fn read_into(
        &mut self,
        buffer: &mut BudgetedBuffer,
        length: usize,
    ) -> impl Future<Output = Result<(), Report<StatePageExchangeError>>> + Send;
    /// Whether the stream ends where the reader is.
    fn ends(&mut self)
    -> impl Future<Output = Result<bool, Report<StatePageExchangeError>>> + Send;
}

impl PageStream for nervix_interconnect::IncomingByteStream {
    fn content_length(&self) -> u64 {
        nervix_interconnect::IncomingByteStream::content_length(self)
    }

    async fn read_into(
        &mut self,
        buffer: &mut BudgetedBuffer,
        length: usize,
    ) -> Result<(), Report<StatePageExchangeError>> {
        nervix_interconnect::IncomingByteStream::read_into(self, buffer, length)
            .await
            .change_context(StatePageExchangeError::Receive)
    }

    async fn ends(&mut self) -> Result<bool, Report<StatePageExchangeError>> {
        nervix_interconnect::IncomingByteStream::ends(self)
            .await
            .change_context(StatePageExchangeError::Receive)
    }
}

/// Requires `stream` to end where its reader is.
async fn read_to_end(stream: &mut impl PageStream) -> Result<(), Report<StatePageExchangeError>> {
    if stream.ends().await? {
        return Ok(());
    }
    Err(Report::new(StatePageExchangeError::Framing))
}

/// Reads the next `length` bytes of `stream` into memory admitted for them: `charge` holds their
/// room.
async fn read_admitted(
    stream: &mut impl PageStream,
    charge: Reservation,
    length: u64,
) -> Result<ChargedBytes, Report<StatePageExchangeError>> {
    let bytes =
        usize::try_from(length).map_err(|_| Report::new(StatePageExchangeError::Framing))?;
    let mut buffer = BudgetedBuffer::with_limit(charge, length);
    stream.read_into(&mut buffer, bytes).await?;
    Ok(ChargedBytes::from_buffer(buffer))
}

/// What admitting a received page's memory came to.
enum ReadAdmission {
    Admitted(Reservation),
    /// The read goes without its page, with this typed refusal.
    Refused(StateInspectionRejection),
}

/// Takes `bytes` of the bulk class for a read that holds nothing yet, so it may wait for room: the
/// room it waits for can free whatever the read does. It waits until `deadline`, and a size even
/// the whole class could never hold is refused at once rather than waited for.
async fn admit_read(
    executor: &Executor,
    bytes: u64,
    deadline: Instant,
) -> Result<ReadAdmission, Report<StatePageExchangeError>> {
    let waiting = executor.reserve(MemoryClass::Bulk, bytes);
    let Ok(admitted) = nervix_primitives::time::timeout_at(deadline, waiting).await else {
        return Ok(ReadAdmission::Refused(StateInspectionRejection::Deadline));
    };
    match admitted {
        Ok(reservation) => Ok(ReadAdmission::Admitted(reservation)),
        Err(refusal) => {
            if let AdmissionError::ExceedsBudget { .. } = refusal.current_context() {
                return Ok(ReadAdmission::Refused(
                    StateInspectionRejection::ReplyTooLarge,
                ));
            }
            Err(refusal.change_context(StatePageExchangeError::Admission))
        }
    }
}

/// A page stream's decoded reply, and how many bytes follow it: the bodies the reply names.
struct PageDescriptor<R> {
    reply: R,
    bodies: u64,
}

/// Reads the descriptor that opens a page stream whose bodies hold at most `max_bytes`, taking
/// its memory one step ahead of what spends it. It reads the descriptor's length under an
/// admission of its own eight bytes, then admits what the descriptor's bytes and its decoded value
/// take, as many bytes again by the decoder's rule. Each admission holds nothing charged while it
/// waits for room until `deadline`; a descriptor the whole class could never hold is refused at
/// once as `reply_too_large`, and one that finds no room before the deadline as `deadline`. As
/// every decoded reply, the descriptor goes on without its charge: its own contract bounds it, and
/// the read holds nothing charged while it admits the bodies.
async fn read_descriptor<R: RkyvMessage>(
    executor: &Executor,
    stream: &mut impl PageStream,
    max_bytes: u64,
    deadline: Instant,
) -> Result<Result<PageDescriptor<R>, StateInspectionRejection>, Report<StatePageExchangeError>> {
    let framing = || Report::new(StatePageExchangeError::Framing);
    let header_bound = PoolClass::Bulk.payload_limit(executor);
    let length = stream.content_length();
    let with_header = max_bytes.checked_add(header_bound).ok_or_else(framing)?;
    let bound = with_header.checked_add(8).ok_or_else(framing)?;
    if length > bound || length < 8 {
        return Err(framing());
    }
    // The descriptor's length, under an admission of its own eight bytes.
    let prefix_charge = match admit_read(executor, 8, deadline).await? {
        ReadAdmission::Admitted(charge) => charge,
        ReadAdmission::Refused(reason) => return Ok(Err(reason)),
    };
    let prefix = read_admitted(stream, prefix_charge, 8).await?;
    let prefix: [u8; 8] = prefix[..]
        .try_into()
        .verified("the prefix read is exactly eight bytes");
    let header_length = u64::from_le_bytes(prefix);
    let after_header = header_length.checked_add(8).ok_or_else(framing)?;
    if header_length > header_bound || after_header > length {
        return Err(framing());
    }
    // The descriptor's bytes, and its decoded value, which the decoder charges as many bytes again.
    let descriptor_need = header_length
        .checked_add(header_length)
        .ok_or_else(framing)?;
    let descriptor_charge = match admit_read(executor, descriptor_need, deadline).await? {
        ReadAdmission::Admitted(charge) => charge,
        ReadAdmission::Refused(reason) => return Ok(Err(reason)),
    };
    let (bytes_charge, decoded_charge) = descriptor_charge
        .split(header_length)
        .change_context(StatePageExchangeError::Admission)?;
    let header = read_admitted(stream, bytes_charge, header_length).await?;
    // The reply is the owner's answer to a read it already served, so a full bulk queue holds its
    // decoding back rather than discarding that answer; the caller's own read deadline bounds the
    // wait.
    let (reply, decoded_charge) =
        R::decode_rkyv_charged_within(executor.clone(), PoolClass::Bulk, header, decoded_charge)
            .await
            .change_context(StatePageExchangeError::Descriptor)?;
    drop(decoded_charge);
    let bodies = length
        .checked_sub(after_header)
        .verified("the descriptor ends within the stream");
    Ok(Ok(PageDescriptor { reply, bodies }))
}

/// Receives one materialized relay page and checks it against the read before any row is used.
///
/// The reader takes its memory one step ahead of what spends it, as [`receive_processor_page`]
/// does. Once the descriptor is decoded and checked, it admits everything the page's container
/// takes on this node before it reads any of it: the container's exact length, and what opening a
/// relay container of the layout the descriptor names takes, by the opening's own charges. That
/// admission holds nothing charged while it waits for room until `deadline`; a page the whole class
/// could never hold is refused at once as `reply_too_large`, and one that finds no room before the
/// deadline as `deadline`. Receiving, checking and opening the container then draw only on what
/// was admitted, so none of them fails for memory, and the opened rows keep their share until they
/// are presented. The container is verified against its digest before it is opened, must have
/// exactly the declared layout, and must agree with the descriptor's origin, fence, row count and
/// last branch.
pub(crate) async fn receive_materialized_page(
    executor: &Executor,
    mut stream: impl PageStream,
    schema: &StdArc<arrow_schema::Schema>,
    request: &ReadMaterializedStatePage,
    deadline: Instant,
) -> Result<
    Result<ReceivedMaterializedPage, StateInspectionRejection>,
    Report<StatePageExchangeError>,
> {
    let framing = || Report::new(StatePageExchangeError::Framing);
    let read = read_descriptor::<MaterializedStatePageReply>(
        executor,
        &mut stream,
        request.max_bytes,
        deadline,
    )
    .await?;
    let PageDescriptor { reply, bodies } = match read {
        Ok(read) => read,
        Err(reason) => return Ok(Err(reason)),
    };
    let transfer = match reply {
        Ok(transfer) => transfer,
        Err(reason) => {
            // A refusal carries nothing after its descriptor.
            read_to_end(&mut stream).await?;
            return Ok(Err(reason));
        }
    };
    let descriptor = transfer.page;
    let count = materialized_page_rows(&descriptor, request)?;
    let Some(body) = transfer.body else {
        if count != 0 {
            return Err(framing());
        }
        read_to_end(&mut stream).await?;
        return Ok(Ok(ReceivedMaterializedPage {
            descriptor,
            snapshot: None,
        }));
    };
    let layout = SealedContainerLayout {
        records: body.records,
        groups: body.groups.iter().map(SealedGroupLayout::from).collect(),
    };
    let length = body.identity.bytes;
    if length > request.max_bytes || length != bodies || count == 0 || layout.records != count {
        return Err(framing());
    }
    let opening = match layout
        .relay_opening(executor, length)
        .and_then(|opening| opening.need())
    {
        Ok(opening) => opening,
        Err(error) => {
            if let MaterializedSnapshotError::MetadataTooLarge = error.current_context() {
                // This node could never hold the container's row views.
                return Ok(Err(StateInspectionRejection::ReplyTooLarge));
            }
            return Err(error.change_context(StatePageExchangeError::Framing));
        }
    };
    let need = length.checked_add(opening).ok_or_else(framing)?;
    let charge = match admit_read(executor, need, deadline).await? {
        ReadAdmission::Admitted(charge) => charge,
        ReadAdmission::Refused(reason) => return Ok(Err(reason)),
    };
    let (receive_charge, opening_charge) = charge
        .split(length)
        .change_context(StatePageExchangeError::Admission)?;
    let received = read_admitted(&mut stream, receive_charge, length).await?;
    read_to_end(&mut stream).await?;
    let (received, opening_charge) =
        verified_body_within(executor, received, body.identity, opening_charge).await?;
    let snapshot = RestoredMaterializedSnapshot::open_relay_within(
        executor,
        schema,
        received,
        &layout,
        opening_charge,
    )
    .await
    .change_context(StatePageExchangeError::Columns)?;
    validate_received_snapshot(&snapshot, &descriptor, count)?;
    Ok(Ok(ReceivedMaterializedPage {
        descriptor,
        snapshot: Some(snapshot),
    }))
}

/// How many rows a materialized page returns, once its offset, continuation and counts agree with
/// the read that asked for it.
fn materialized_page_rows(
    descriptor: &MaterializedStatePageDescriptor,
    request: &ReadMaterializedStatePage,
) -> Result<u64, Report<StatePageExchangeError>> {
    let framing = || Report::new(StatePageExchangeError::Framing);
    let start = match request.after {
        Some((_, offset)) => offset,
        None => 0,
    };
    let count = descriptor.offset.checked_sub(start).ok_or_else(framing)?;
    if count > request.max_rows {
        return Err(framing());
    }
    if descriptor.more != (descriptor.offset < descriptor.counts.rows)
        || descriptor.offset > descriptor.counts.rows
    {
        return Err(framing());
    }
    Ok(count)
}

/// Receives one processor page and checks it against the read before any row is used.
///
/// The reader takes its memory one step ahead of what spends it. It reads the page's descriptor as
/// [`read_descriptor`] does. Once the descriptor is decoded and checked, it admits everything the
/// bodies take on this node, from the exact length of every body the descriptor names and the
/// decoder's charge for each, before it reads any of them. That admission holds nothing charged
/// while it waits for room until `deadline`; a page the whole class could never hold is refused at
/// once as `reply_too_large`, and one that finds no room before the deadline as `deadline`.
/// Receiving, checking and decoding then draw only on what was admitted, so none of them fails for
/// memory, and the decoded rows keep their charge until they are presented.
///
/// `payload` names the sections the coordinator expects, in order, each with the payload fields its
/// own committed schema gives it: a section that returns rows declares exactly those fields first.
/// A count-only read passes no section and accepts no returned row. Every body is bounded, verified
/// against its digest and decoded with exactly its declared fields, and the runs, the returned
/// offset and the row counts must agree with the bodies and the descriptor.
pub(crate) async fn receive_processor_page(
    executor: &Executor,
    mut stream: impl PageStream,
    request: &ReadProcessorStatePage,
    payload: &[(ProcessorStateSection, Vec<SchemaField>)],
    deadline: Instant,
) -> Result<Result<ReceivedProcessorPage, StateInspectionRejection>, Report<StatePageExchangeError>>
{
    let framing = || Report::new(StatePageExchangeError::Framing);
    let read = read_descriptor::<ProcessorStatePageReply>(
        executor,
        &mut stream,
        request.max_bytes,
        deadline,
    )
    .await?;
    let PageDescriptor { reply, bodies } = match read {
        Ok(read) => read,
        Err(reason) => return Ok(Err(reason)),
    };
    let descriptor = match reply {
        Ok(descriptor) => descriptor,
        Err(reason) => {
            // A refusal carries nothing after its descriptor.
            read_to_end(&mut stream).await?;
            return Ok(Err(reason));
        }
    };
    let section_rows = processor_page_rows(&descriptor, request, payload)?;
    let need = page_body_need(executor, &descriptor, &section_rows, payload)?;
    if need.bytes > request.max_bytes || need.bytes != bodies {
        return Err(framing());
    }
    if need.bytes == 0 {
        read_to_end(&mut stream).await?;
        let mut sections = Vec::with_capacity(descriptor.sections.len());
        for transfer in &descriptor.sections {
            sections.push(ReceivedProcessorSection {
                section: transfer.section,
                fields: transfer.fields.clone(),
                rows: transfer.rows,
                batches: Vec::new(),
            });
        }
        return Ok(Ok(ReceivedProcessorPage {
            descriptor,
            sections,
            memory: None,
        }));
    }
    let charge = match admit_read(executor, need.total, deadline).await? {
        ReadAdmission::Admitted(charge) => charge,
        ReadAdmission::Refused(reason) => return Ok(Err(reason)),
    };
    let (receive_charge, decoding) = charge
        .split(need.bytes)
        .change_context(StatePageExchangeError::Admission)?;
    let received = read_admitted(&mut stream, receive_charge, need.bytes).await?;
    read_to_end(&mut stream).await?;
    let mut bodies = ReceivedBodies {
        bytes: &received,
        offset: 0,
        decoding: Some(decoding),
        held: None,
    };
    let mut sections = Vec::with_capacity(descriptor.sections.len());
    for (index, transfer) in descriptor.sections.iter().enumerate() {
        let batches = bodies
            .section(executor, transfer, section_rows[index])
            .await?;
        sections.push(ReceivedProcessorSection {
            section: transfer.section,
            fields: transfer.fields.clone(),
            rows: transfer.rows,
            batches,
        });
    }
    let held = bodies.held.take();
    drop(bodies);
    // Every body is decoded, so the received bytes go back to the pool; the decoded rows keep the
    // charge their decoding took.
    drop(received);
    let memory = held.map(|held| ProcessorStatePageMemory {
        _reservation: held,
        _windows: Vec::new(),
        reply: None,
    });
    Ok(Ok(ReceivedProcessorPage {
        descriptor,
        sections,
        memory,
    }))
}

/// What a page's bodies take on the reading node.
struct BodyNeed {
    /// The bytes of every body the descriptor names.
    bytes: u64,
    /// Those bytes and what decoding each body takes.
    total: u64,
}

/// Checks every section of `descriptor` against the read and the expected `payload`, and adds up
/// what its bodies take on the reading node: each body's exact length and the decoder's charge for
/// it. A section names no body exactly when it returns no row, and no more bodies than rows.
fn page_body_need(
    executor: &Executor,
    descriptor: &ProcessorStatePageDescriptor,
    section_rows: &[u64],
    payload: &[(ProcessorStateSection, Vec<SchemaField>)],
) -> Result<BodyNeed, Report<StatePageExchangeError>> {
    let framing = || Report::new(StatePageExchangeError::Framing);
    let mut need = BodyNeed { bytes: 0, total: 0 };
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
        if transfer.bodies.is_empty() {
            if rows != 0 {
                return Err(framing());
            }
            continue;
        }
        // Every body holds at least one row, so a section names no more bodies than rows.
        let named = u64::try_from(transfer.bodies.len()).map_err(|_| framing())?;
        if named > rows {
            return Err(framing());
        }
        for identity in &transfer.bodies {
            let decoding = RuntimeRecordBatch::snapshot_section_scratch(executor, identity.bytes);
            need.bytes = need.bytes.checked_add(identity.bytes).ok_or_else(framing)?;
            need.total = need.total.checked_add(identity.bytes).ok_or_else(framing)?;
            need.total = need.total.checked_add(decoding).ok_or_else(framing)?;
        }
    }
    Ok(need)
}

/// The bodies of a received processor page, decoded section by section from the bytes the reader
/// received, each into the share of the admitted memory its decoding takes.
struct ReceivedBodies<'a> {
    bytes: &'a ChargedBytes,
    /// Where the next body starts.
    offset: usize,
    /// What is left of the memory admitted for decoding.
    decoding: Option<Reservation>,
    /// The memory the decoded rows hold so far.
    held: Option<Reservation>,
}

impl ReceivedBodies<'_> {
    /// The `rows` returned rows of one section, in the batches its bodies carry them in. Each body
    /// it names is the next in the stream, verified against its digest and decoded with exactly
    /// the section's declared fields; each holds at least one row, and together they hold exactly
    /// those rows.
    async fn section(
        &mut self,
        executor: &Executor,
        transfer: &ProcessorStateSectionTransfer,
        rows: u64,
    ) -> Result<Vec<RecordBatch>, Report<StatePageExchangeError>> {
        let framing = || Report::new(StatePageExchangeError::Framing);
        let schema = StdArc::new(SchemaField::arrow_schema(&transfer.fields));
        let mut batches = Vec::with_capacity(transfer.bodies.len());
        let mut carried = 0_u64;
        for identity in &transfer.bodies {
            let length = usize::try_from(identity.bytes).map_err(|_| framing())?;
            let end = self.offset.checked_add(length).ok_or_else(framing)?;
            let body = self.bytes.slice(self.offset, end).ok_or_else(framing)?;
            self.offset = end;
            let charge = self.decoding_charge(executor, identity.bytes)?;
            let (body, charge) =
                verified_body_within(executor, body, identity.clone(), charge).await?;
            // The body holds rows the owner already read, so a full bulk queue holds its decoding
            // back; the caller's own read deadline bounds the wait.
            let (decoded, charge) = RuntimeRecordBatch::decode_arrow_snapshot_section_within(
                executor,
                StdArc::clone(&schema),
                body,
                charge,
            )
            .await
            .change_context(StatePageExchangeError::Columns)?;
            self.hold(charge)?;
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

    /// The share of the admitted memory decoding a body of `encoded` bytes takes.
    fn decoding_charge(
        &mut self,
        executor: &Executor,
        encoded: u64,
    ) -> Result<Reservation, Report<StatePageExchangeError>> {
        let wanted = RuntimeRecordBatch::snapshot_section_scratch(executor, encoded);
        let left = self
            .decoding
            .take()
            .ok_or_else(|| Report::new(StatePageExchangeError::Admission))?;
        let (charge, rest) = left
            .split(wanted)
            .change_context(StatePageExchangeError::Admission)?;
        self.decoding = Some(rest);
        Ok(charge)
    }

    /// Keeps `charge` with the decoded rows it covers.
    fn hold(&mut self, charge: Reservation) -> Result<(), Report<StatePageExchangeError>> {
        let held = match self.held.take() {
            Some(held) => held
                .merge(charge)
                .change_context(StatePageExchangeError::Admission)?,
            None => charge,
        };
        self.held = Some(held);
        Ok(())
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
