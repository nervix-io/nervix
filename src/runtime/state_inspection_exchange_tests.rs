//! The checks a coordinator applies to a processor page an owner sent: to its descriptor before it
//! takes any body, and to every body before any row is used.
//!
//! Layer: test harness.
//! - **Owns.** The descriptor contracts of a received processor page: its returned rows start at
//!   the read's resume offset and stay within its row limit, its continuation flag and offset agree
//!   with its complete counts, it names exactly the committed kind's sections a
//!   row-returning read expects, and its runs name sections the page carries, return at least one
//!   row each and add up to the rows its offset advanced. The body contracts: a section returns no
//!   more rows than it holds and names a body exactly when it returns rows, so a count-only read
//!   takes a page with no body; a named body is the next bytes of the stream, within the read's
//!   byte bound and with the digest its descriptor names; it decodes with exactly the section's
//!   declared fields, which begin with the payload fields the read expects, and holds exactly the
//!   rows the runs return; nothing follows the last body; and an owner's typed refusal arrives as
//!   that refusal, with no body. A page that breaks any of them is refused, never presented as
//!   rows.
//! - **Depends on.** The production descriptor and body checks, the interconnect page messages and
//!   their rkyv reply codec, the native Arrow section codec and the body identity owners stream.
//! - **Must not know.** Interconnect transport streams, sessions or Row frames.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use arrow_array::{
    ArrayRef, Int64Array, StringArray, UInt32Array, cast::AsArray as _, types::Int64Type,
};
use futures_util::FutureExt as _;
use nervix_interconnect::ProcessorStateRunTransfer;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, FieldName, ModelKind, ModelName, NodeRef,
    ParseAsType, SchemaFingerprint, StateInspectionBranchSelection, StateInspectionCounts,
    StateInspectionOrigin, StateInspectionStage, Unit,
};

use super::*;
use crate::runtime::{PageReply, named};

fn owner() -> ClusterNodeIdentity {
    ClusterNodeIdentity::new(named("node-1"), ClusterNodeIncarnation::new(1))
}

/// A first read of a reorderer's held rows that returns at most `max_rows` of them.
fn read(max_rows: u64) -> ReadProcessorStatePage {
    ReadProcessorStatePage {
        domain: named("orders"),
        processor: NodeRef::new(
            ModelKind::Reorderer,
            named::<ModelName>("held_notifications"),
        ),
        unit: Unit::SINGLE,
        control_revision: 1,
        schema: SchemaFingerprint::from_digest([7; 32]),
        selection: StateInspectionBranchSelection::All,
        partition_exclusions: None,
        after: None,
        max_rows,
        max_bytes: 1 << 20,
        target: owner(),
    }
}

fn section(section: ProcessorStateSection, rows: u64) -> ProcessorStateSectionTransfer {
    ProcessorStateSectionTransfer {
        section,
        rows,
        fields: Vec::new(),
        bodies: Vec::new(),
    }
}

fn run(section: ProcessorStateSection, rows: u64) -> ProcessorStateRunTransfer {
    ProcessorStateRunTransfer {
        section,
        branch: nervix_models::RemoteBranchKey::Unbranched,
        rows,
    }
}

/// A page of a reorderer holding five rows that returns the first three of them in two runs.
fn pending_page() -> ProcessorStatePageDescriptor {
    ProcessorStatePageDescriptor {
        partition_catalog: nervix_models::StateInspectionKeyCatalog::Unrequested,
        origin: StateInspectionOrigin {
            node: owner(),
            fingerprint: [1; 32],
            generation: 1,
            revision: 1,
            stage: StateInspectionStage::Live,
        },
        counts: StateInspectionCounts {
            branches: 2,
            rows: 5,
            bytes: 50,
        },
        offset: 3,
        last_branch: None,
        more: true,
        runs: vec![
            run(ProcessorStateSection::Pending, 2),
            run(ProcessorStateSection::Pending, 1),
        ],
        sections: vec![section(ProcessorStateSection::Pending, 5)],
    }
}

/// The one section a reorderer read expects, with the payload fields its committed schema gives it.
fn pending_payload() -> Vec<(ProcessorStateSection, Vec<SchemaField>)> {
    vec![(ProcessorStateSection::Pending, Vec::new())]
}

#[test]
fn tensor_sections_use_the_declared_layout_and_bound_their_indices_without_a_scan() {
    let mut request = read(6);
    request.processor.kind = ModelKind::Inferencer;
    let mut descriptor = pending_page();
    descriptor.offset = 6;
    descriptor.counts.rows = 6;
    descriptor.more = false;
    descriptor.sections = vec![section(ProcessorStateSection::Header, 1)];
    descriptor.runs = vec![run(ProcessorStateSection::Header, 1)];
    for index in 0..6 {
        descriptor
            .sections
            .push(section(ProcessorStateSection::Tensor(index), 1));
        descriptor
            .runs
            .push(run(ProcessorStateSection::Tensor(index), 1));
    }
    let payload = descriptor
        .sections
        .iter()
        .map(|section| (section.section, Vec::new()))
        .collect::<Vec<_>>();
    assert_eq!(
        processor_page_rows(&descriptor, &request, &payload)
            .assured("all committed tensor sections are accepted"),
        vec![1; 7]
    );
    descriptor.runs[1].section = ProcessorStateSection::Tensor(u32::MAX);
    assert!(processor_page_rows(&descriptor, &request, &payload).is_err());
}

#[track_caller]
fn assert_refused(
    descriptor: &ProcessorStatePageDescriptor,
    request: &ReadProcessorStatePage,
    payload: &[(ProcessorStateSection, Vec<SchemaField>)],
) {
    let error = processor_page_rows(descriptor, request, payload)
        .err()
        .assured("the descriptor disagrees with the read that asked for it or with itself");
    assert!(matches!(
        error.current_context(),
        StatePageExchangeError::Framing
    ));
}

#[test]
fn a_page_returns_its_rows_per_section_in_runs_that_add_up_to_the_offset_it_advanced() {
    let rows = processor_page_rows(&pending_page(), &read(10), &pending_payload())
        .assured("the page agrees with its read");
    assert_eq!(rows, vec![3]);

    // A correlator page splits its returned rows by side, however its runs interleave the sides.
    let mut correlator = pending_page();
    correlator.counts.rows = 4;
    correlator.offset = 4;
    correlator.more = false;
    correlator.sections = vec![
        section(ProcessorStateSection::Left, 2),
        section(ProcessorStateSection::Right, 2),
    ];
    correlator.runs = vec![
        run(ProcessorStateSection::Left, 1),
        run(ProcessorStateSection::Right, 2),
        run(ProcessorStateSection::Left, 1),
    ];
    let sides: Vec<(ProcessorStateSection, Vec<SchemaField>)> = vec![
        (ProcessorStateSection::Left, Vec::new()),
        (ProcessorStateSection::Right, Vec::new()),
    ];
    let rows = processor_page_rows(&correlator, &read(10), &sides)
        .assured("the correlator page agrees with its read");
    assert_eq!(rows, vec![2, 2]);

    // A continuation returns only the rows after its resume offset.
    let mut continued = read(10);
    continued.after = Some((None, 1));
    let mut rest = pending_page();
    rest.runs = vec![run(ProcessorStateSection::Pending, 2)];
    let rows = processor_page_rows(&rest, &continued, &pending_payload())
        .assured("the continued page agrees with its read");
    assert_eq!(rows, vec![2]);

    // A count-only read expects no section and accepts a page that returns no row.
    let mut counted = pending_page();
    counted.offset = 0;
    counted.runs = Vec::new();
    let rows =
        processor_page_rows(&counted, &read(0), &[]).assured("a count-only page returns no row");
    assert_eq!(rows, vec![0]);
}

#[test]
fn a_partition_catalog_agrees_with_its_request_and_filtered_branch_count() {
    let moved = nervix_models::BranchKeyFingerprint::new([1; 32]);
    let retained = nervix_models::BranchKeyFingerprint::new([2; 32]);
    let mut request = read(10);
    request.processor.kind = ModelKind::Deduplicator;
    request.partition_exclusions = Some(std::collections::BTreeSet::from([moved]));
    let mut page = pending_page();
    page.counts.branches = 1;
    page.partition_catalog =
        nervix_models::StateInspectionKeyCatalog::Direct(std::collections::BTreeSet::from([
            moved, retained,
        ]));
    processor_page_rows(&page, &request, &pending_payload())
        .assured("the complete catalog accounts for the omitted copy");
    page.counts.branches = 2;
    assert!(processor_page_rows(&page, &request, &pending_payload()).is_err());
    page.counts.branches = 1;
    page.partition_catalog = nervix_models::StateInspectionKeyCatalog::Unrequested;
    assert!(processor_page_rows(&page, &request, &pending_payload()).is_err());
    page.partition_catalog =
        nervix_models::StateInspectionKeyCatalog::Direct(std::collections::BTreeSet::from([
            moved, retained,
        ]));
    request.selection = StateInspectionBranchSelection::Branch(retained);
    assert!(
        processor_page_rows(&page, &request, &pending_payload()).is_err(),
        "a single-branch catalog cannot name another branch"
    );
    request.partition_exclusions = None;
    assert!(
        processor_page_rows(&page, &request, &pending_payload()).is_err(),
        "an ordinary page cannot supply unsolicited partition metadata"
    );
}

#[test]
fn window_metadata_counts_as_section_rows_without_advancing_the_retained_row_offset() {
    let mut request = read(1);
    request.processor.kind = ModelKind::WindowProcessor;
    let mut window = pending_page();
    window.offset = 1;
    window.sections = vec![
        section(ProcessorStateSection::Rows, 5),
        section(ProcessorStateSection::Header, 2),
        section(ProcessorStateSection::Accumulators, 6),
    ];
    window.runs = vec![
        run(ProcessorStateSection::Header, 2),
        run(ProcessorStateSection::Accumulators, 6),
        run(ProcessorStateSection::Rows, 1),
    ];
    let payload = vec![
        (ProcessorStateSection::Rows, Vec::new()),
        (
            ProcessorStateSection::Header,
            ProcessorStateSection::Header.metadata_fields(),
        ),
        (
            ProcessorStateSection::Accumulators,
            ProcessorStateSection::Accumulators.metadata_fields(),
        ),
    ];
    let returned = processor_page_rows(&window, &request, &payload)
        .expect("one retained row and bounded metadata");
    assert_eq!(returned, vec![1, 2, 6]);
    let mut counts_only = window.clone();
    counts_only.offset = 0;
    counts_only.runs = vec![run(ProcessorStateSection::Header, 2)];
    assert_refused(&counts_only, &read(0), &[]);
    window.offset = 2;
    assert_refused(&window, &request, &payload);
}

#[test]
fn a_page_that_disagrees_with_its_read_or_with_its_own_counts_is_refused() {
    let payload = pending_payload();

    // Its offset falls before the read's resume offset.
    let mut resumed = read(10);
    resumed.after = Some((None, 4));
    assert_refused(&pending_page(), &resumed, &payload);

    // It returns more rows than the read allows.
    assert_refused(&pending_page(), &read(2), &payload);

    // Its continuation flag, or its offset, contradicts its complete counts.
    let mut finished = pending_page();
    finished.more = false;
    assert_refused(&finished, &read(10), &payload);
    let mut beyond = pending_page();
    beyond.counts.rows = 2;
    beyond.more = false;
    assert_refused(&beyond, &read(10), &payload);

    // It names more sections than a fixed-layout processor holds.
    let mut excessive = pending_page();
    excessive.sections = vec![
        section(ProcessorStateSection::Pending, 5),
        section(ProcessorStateSection::Left, 0),
        section(ProcessorStateSection::Right, 0),
        section(ProcessorStateSection::Keys, 0),
    ];
    let excessive_sections: Vec<(ProcessorStateSection, Vec<SchemaField>)> = vec![
        (ProcessorStateSection::Pending, Vec::new()),
        (ProcessorStateSection::Left, Vec::new()),
        (ProcessorStateSection::Right, Vec::new()),
        (ProcessorStateSection::Keys, Vec::new()),
    ];
    assert_refused(&excessive, &read(10), &excessive_sections);

    // A count-only read accepts no returned row, and a row-returning read expects exactly the
    // sections the page names.
    assert_refused(&pending_page(), &read(10), &[]);
    let sides: Vec<(ProcessorStateSection, Vec<SchemaField>)> = vec![
        (ProcessorStateSection::Left, Vec::new()),
        (ProcessorStateSection::Right, Vec::new()),
    ];
    assert_refused(&pending_page(), &read(10), &sides);

    // A run names a section the page does not carry, returns no row, or the runs do not add up to
    // the rows the offset advanced, including by overflowing.
    let mut foreign = pending_page();
    foreign.runs = vec![
        run(ProcessorStateSection::Pending, 2),
        run(ProcessorStateSection::Left, 1),
    ];
    assert_refused(&foreign, &read(10), &payload);
    let mut empty_run = pending_page();
    empty_run.runs = vec![
        run(ProcessorStateSection::Pending, 3),
        run(ProcessorStateSection::Pending, 0),
    ];
    assert_refused(&empty_run, &read(10), &payload);
    let mut short = pending_page();
    short.runs = vec![run(ProcessorStateSection::Pending, 2)];
    assert_refused(&short, &read(10), &payload);
    let mut overflowing = pending_page();
    overflowing.runs = vec![
        run(ProcessorStateSection::Pending, u64::MAX),
        run(ProcessorStateSection::Pending, 4),
    ];
    assert_refused(&overflowing, &read(10), &payload);
}

fn field(name: &str, ty: ParseAsType) -> SchemaField {
    SchemaField {
        name: named::<FieldName>(name),
        ty,
        optional: false,
        sensitive: false,
    }
}

/// The payload fields a reorderer's committed input schema gives its held rows, which a read
/// expects every row-returning section to declare first.
fn committed_payload() -> Vec<SchemaField> {
    vec![
        field("region", ParseAsType::String),
        field("sequence", ParseAsType::I64),
    ]
}

/// The fields a reorderer owner declares for its held rows: the committed payload fields, then a
/// column it appends.
fn declared_fields() -> Vec<SchemaField> {
    let mut fields = committed_payload();
    fields.push(field("output", ParseAsType::U32));
    fields
}

/// The read's expectation: one held-row section with the committed payload fields.
fn expected_sections() -> Vec<(ProcessorStateSection, Vec<SchemaField>)> {
    vec![(ProcessorStateSection::Pending, committed_payload())]
}

/// One row of `fields` per sequence, each column in the type its field declares.
fn held_rows(fields: &[SchemaField], sequences: &[i64]) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::with_capacity(fields.len());
    for field in fields {
        let column: ArrayRef = match &field.ty {
            ParseAsType::String => StdArc::new(StringArray::from_iter_values(
                sequences.iter().map(|sequence| format!("row-{sequence}")),
            )),
            ParseAsType::I64 => {
                StdArc::new(Int64Array::from_iter_values(sequences.iter().copied()))
            }
            ParseAsType::U32 => StdArc::new(UInt32Array::from_iter_values(
                sequences.iter().map(|_| 0_u32),
            )),
            other => panic!("the fixture builds no {other:?} column"),
        };
        columns.push(column);
    }
    RecordBatch::try_new(StdArc::new(SchemaField::arrow_schema(fields)), columns)
        .assured("every fixture column matches its declared field")
}

/// The native Arrow body an owner streams for `rows`, with the identity it names it by.
async fn owner_body(executor: &Executor, rows: RecordBatch) -> (StatePageBody, ChargedBytes) {
    let rows = RuntimeRecordBatch::from_record_batch(rows.schema(), rows)
        .assured("the fixture rows are a runtime batch");
    let body = rows
        .encode_arrow_snapshot_section(executor, nervix_execution::QueueAdmission::RefuseWhenFull)
        .await
        .assured("a bounded section encodes");
    identified_body(executor, body)
        .await
        .assured("a resident body is identified")
}

/// The first page of a reorderer holding five rows that returns `returned` of them in one run,
/// with its held-row section declaring `fields` and naming `body`.
fn returning_page(
    returned: u64,
    fields: Vec<SchemaField>,
    body: Option<StatePageBody>,
) -> ProcessorStatePageDescriptor {
    let mut page = pending_page();
    page.offset = returned;
    page.more = returned < page.counts.rows;
    page.runs = if returned == 0 {
        Vec::new()
    } else {
        vec![run(ProcessorStateSection::Pending, returned)]
    };
    page.sections = vec![ProcessorStateSectionTransfer {
        section: ProcessorStateSection::Pending,
        rows: 5,
        fields,
        bodies: body.into_iter().collect(),
    }];
    page
}

/// The bytes of the page stream an owner sends for `reply`, as `stream_processor_page` frames it:
/// the reply's eight-byte little-endian length, its bounded rkyv bytes, then `bodies` in order.
async fn sent_page(
    executor: &Executor,
    reply: ProcessorStatePageReply,
    bodies: &[&[u8]],
) -> ChargedBytes {
    let (header, _header_charge) = reply
        .encode_rkyv(
            executor.clone(),
            PoolClass::Bulk,
            PoolClass::Bulk.payload_limit(executor),
        )
        .await
        .assured("a bounded reply encodes");
    let length = u64::try_from(header.len()).assured("a bounded reply length fits u64");
    let mut stream = length.to_le_bytes().to_vec();
    stream.extend_from_slice(&header);
    for body in bodies {
        stream.extend_from_slice(body);
    }
    let charge = executor
        .try_reserve(
            MemoryClass::Bulk,
            u64::try_from(stream.len()).assured("a fixture stream length fits u64"),
        )
        .assured("the fixture executor admits one small page");
    ChargedBytes::from_owned(stream, charge)
}

/// Why the coordinator refused a received page. A page it presents, or reads as an owner's typed
/// refusal, fails the check.
#[track_caller]
fn refusal(
    received: Result<
        Result<ReceivedProcessorPage, StateInspectionRejection>,
        Report<StatePageExchangeError>,
    >,
) -> Report<StatePageExchangeError> {
    match received {
        Err(error) => error,
        Ok(Ok(page)) => panic!(
            "a malformed page was presented with {} returned sections",
            page.sections.len()
        ),
        Ok(Err(reason)) => panic!("a malformed page was read as the owner's refusal {reason:?}"),
    }
}

#[nervix_primitives::test]
async fn a_page_whose_bodies_agree_with_their_descriptor_and_the_read_presents_their_rows() {
    let executor = Executor::default();
    let (identity, body) = owner_body(&executor, held_rows(&declared_fields(), &[1, 2, 3])).await;
    let stream = sent_page(
        &executor,
        Ok(returning_page(3, declared_fields(), Some(identity))),
        &[&body[..]],
    )
    .await;
    let received =
        ReceivedProcessorPage::decode(&executor, &stream, &read(10), &expected_sections())
            .await
            .assured("the page agrees with its descriptor and the read");
    let Ok(page) = received else {
        panic!("an owner that returned rows did not refuse the read");
    };
    let [section] = page.sections.as_slice() else {
        panic!("a reorderer page carries its one held-row section");
    };
    assert_eq!(
        (section.section, section.rows, &section.fields),
        (ProcessorStateSection::Pending, 5, &declared_fields()),
        "the section keeps its complete count and its declared fields"
    );
    let [rows] = section.batches.as_slice() else {
        panic!("a section that returned rows carries them in its one body");
    };
    assert_eq!(
        rows.column(1).as_primitive::<Int64Type>().values().to_vec(),
        vec![1, 2, 3],
        "the rows are the body's rows in the order the owner returned them"
    );

    // A count-only read, as DESCRIBE makes, expects no section and takes a page that returns no row
    // with every section's complete count.
    let counted = sent_page(
        &executor,
        Ok(returning_page(0, declared_fields(), None)),
        &[],
    )
    .await;
    let received = ReceivedProcessorPage::decode(&executor, &counted, &read(0), &[])
        .await
        .assured("a page that returns no row agrees with a count-only read");
    let Ok(page) = received else {
        panic!("an owner that counted its rows did not refuse the read");
    };
    let [section] = page.sections.as_slice() else {
        panic!("a reorderer page carries its one held-row section");
    };
    assert_eq!(
        (section.rows, section.batches.is_empty()),
        (5, true),
        "a count-only page carries the section's complete count and none of its rows"
    );

    // An owner's typed refusal arrives as that refusal, and carries no body.
    let refused = sent_page(&executor, Err(StateInspectionRejection::NotReady), &[]).await;
    let received =
        ReceivedProcessorPage::decode(&executor, &refused, &read(10), &expected_sections())
            .await
            .assured("a refusal is a complete reply");
    assert!(
        matches!(received, Err(StateInspectionRejection::NotReady)),
        "the coordinator reports the owner's refusal as the owner typed it"
    );
    let trailing = sent_page(
        &executor,
        Err(StateInspectionRejection::NotReady),
        &[&body[..]],
    )
    .await;
    let error = refusal(
        ReceivedProcessorPage::decode(&executor, &trailing, &read(10), &expected_sections()).await,
    );
    assert!(matches!(
        error.current_context(),
        StatePageExchangeError::Framing
    ));
}

/// A body whose columns or column types disagree with the fields its section declares is refused as
/// undecodable columns, whatever the descriptor says it returns.
#[nervix_primitives::test]
async fn a_body_whose_columns_or_types_disagree_with_its_declared_fields_is_refused() {
    let executor = Executor::default();
    let missing_column = committed_payload();
    let mut wrong_type = declared_fields();
    wrong_type[1] = field("sequence", ParseAsType::String);
    let mut extra_column = declared_fields();
    extra_column.push(field("note", ParseAsType::String));
    for body_fields in [missing_column, wrong_type, extra_column] {
        let (identity, body) = owner_body(&executor, held_rows(&body_fields, &[1, 2, 3])).await;
        let stream = sent_page(
            &executor,
            Ok(returning_page(3, declared_fields(), Some(identity))),
            &[&body[..]],
        )
        .await;
        let error = refusal(
            ReceivedProcessorPage::decode(&executor, &stream, &read(10), &expected_sections())
                .await,
        );
        assert!(
            matches!(error.current_context(), StatePageExchangeError::Columns),
            "a body of {body_fields:?} under the declared fields is undecodable, found {error:?}"
        );
    }
}

/// A body whose rows, identity or bytes disagree with the descriptor, or with the read, is refused
/// as a framing violation, as is a section that declares fields the read does not expect first.
#[nervix_primitives::test]
async fn a_body_whose_rows_identity_or_bounds_disagree_with_the_descriptor_or_the_read_is_refused()
{
    let executor = Executor::default();
    let expected = expected_sections();
    let (identity, body) = owner_body(&executor, held_rows(&declared_fields(), &[1, 2, 3])).await;
    let (other_identity, other_body) =
        owner_body(&executor, held_rows(&declared_fields(), &[4, 5, 6])).await;
    assert_eq!(
        body.len(),
        other_body.len(),
        "two bodies of the same shape have one length"
    );
    let full = || returning_page(3, declared_fields(), Some(identity.clone()));

    let mut cases = Vec::new();
    // The body holds fewer, or more, rows than the runs return.
    cases.push((
        "fewer rows",
        sent_page(
            &executor,
            Ok(returning_page(4, declared_fields(), Some(identity.clone()))),
            &[&body[..]],
        )
        .await,
        read(10),
    ));
    cases.push((
        "more rows",
        sent_page(
            &executor,
            Ok(returning_page(2, declared_fields(), Some(identity.clone()))),
            &[&body[..]],
        )
        .await,
        read(10),
    ));
    // The runs return more of the section's rows than the section holds.
    let mut overdrawn = returning_page(3, declared_fields(), Some(identity.clone()));
    overdrawn.sections[0].rows = 2;
    cases.push((
        "more returned rows than the section holds",
        sent_page(&executor, Ok(overdrawn), &[&body[..]]).await,
        read(10),
    ));
    // The stream carries other bytes than the digest names.
    cases.push((
        "another body's digest",
        sent_page(
            &executor,
            Ok(returning_page(
                3,
                declared_fields(),
                Some(other_identity.clone()),
            )),
            &[&body[..]],
        )
        .await,
        read(10),
    ));
    // The descriptor names a body longer than the stream holds, or shorter than the body sent.
    let mut overlong = identity.clone();
    overlong.bytes += 1;
    cases.push((
        "a body beyond the stream",
        sent_page(
            &executor,
            Ok(returning_page(3, declared_fields(), Some(overlong))),
            &[&body[..]],
        )
        .await,
        read(10),
    ));
    let mut short = identity.clone();
    short.bytes -= 1;
    cases.push((
        "a body cut short",
        sent_page(
            &executor,
            Ok(returning_page(3, declared_fields(), Some(short))),
            &[&body[..]],
        )
        .await,
        read(10),
    ));
    // Bytes follow the last body.
    cases.push((
        "bytes after the last body",
        sent_page(&executor, Ok(full()), &[&body[..], &other_body[..]]).await,
        read(10),
    ));
    // The body exceeds the bytes the read allows.
    let mut bounded = read(10);
    bounded.max_bytes = identity.bytes - 1;
    cases.push((
        "a body beyond the read's bytes",
        sent_page(&executor, Ok(full()), &[&body[..]]).await,
        bounded,
    ));
    // A section that returns rows names no body, or one that returns none names a body.
    cases.push((
        "returned rows without a body",
        sent_page(
            &executor,
            Ok(returning_page(3, declared_fields(), None)),
            &[],
        )
        .await,
        read(10),
    ));
    cases.push((
        "a body without returned rows",
        sent_page(
            &executor,
            Ok(returning_page(0, declared_fields(), Some(identity.clone()))),
            &[&body[..]],
        )
        .await,
        read(10),
    ));
    // The section's bodies hold more rows than its runs return.
    let mut doubled = full();
    doubled.sections[0].bodies.push(other_identity.clone());
    cases.push((
        "bodies holding more rows than the runs return",
        sent_page(&executor, Ok(doubled), &[&body[..], &other_body[..]]).await,
        read(10),
    ));
    // The section names more bodies than it returns rows, which every body holds one of at least.
    let mut crowded = returning_page(1, declared_fields(), Some(identity.clone()));
    crowded.sections[0].bodies.push(other_identity.clone());
    cases.push((
        "more bodies than returned rows",
        sent_page(&executor, Ok(crowded), &[&body[..], &other_body[..]]).await,
        read(10),
    ));
    // The section's declared fields, with a body that decodes under them, do not begin with the
    // payload fields the read expects.
    let mut reordered = declared_fields();
    reordered.swap(0, 1);
    let (reordered_identity, reordered_body) =
        owner_body(&executor, held_rows(&reordered, &[1, 2, 3])).await;
    cases.push((
        "fields the read does not expect",
        sent_page(
            &executor,
            Ok(returning_page(3, reordered, Some(reordered_identity))),
            &[&reordered_body[..]],
        )
        .await,
        read(10),
    ));

    for (case, stream, request) in cases {
        let error =
            refusal(ReceivedProcessorPage::decode(&executor, &stream, &request, &expected).await);
        assert!(
            matches!(error.current_context(), StatePageExchangeError::Framing),
            "{case} is a framing violation, found {error:?}"
        );
    }
}

/// The node's bound on one snapshot section, narrowed so a fixture page outgrows it.
const NARROW_SECTION_BYTES: u64 = 64 * 1024;

/// An executor whose snapshot sections hold at most [`NARROW_SECTION_BYTES`], as a node with a
/// narrower section bound than the default.
fn narrow_sections() -> Executor {
    Executor::new(nervix_execution::ExecutionConfig {
        limits: nervix_execution::OperationLimits {
            snapshot_section_bytes: ubyte::ByteUnit::Byte(NARROW_SECTION_BYTES),
            ..nervix_execution::OperationLimits::default()
        },
        ..nervix_execution::ExecutionConfig::default()
    })
    .assured("a narrow section bound within the default budgets is valid")
}

/// One row of `fields` per sequence, like [`held_rows`], whose text columns carry `width` bytes.
fn wide_rows(fields: &[SchemaField], sequences: &[i64], width: usize) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::with_capacity(fields.len());
    for field in fields {
        let column: ArrayRef = match &field.ty {
            ParseAsType::String => StdArc::new(StringArray::from_iter_values(
                sequences
                    .iter()
                    .map(|sequence| format!("{sequence:08}{}", "w".repeat(width))),
            )),
            ParseAsType::I64 => {
                StdArc::new(Int64Array::from_iter_values(sequences.iter().copied()))
            }
            ParseAsType::U32 => StdArc::new(UInt32Array::from_iter_values(
                sequences.iter().map(|_| 0_u32),
            )),
            other => panic!("the fixture builds no {other:?} column"),
        };
        columns.push(column);
    }
    RecordBatch::try_new(StdArc::new(SchemaField::arrow_schema(fields)), columns)
        .assured("every fixture column matches its declared field")
}

/// The first and only page of a reorderer holding `held` rows, returning all of them in one run.
fn whole_page(held: u64) -> ProcessorStatePageDescriptor {
    let mut page = pending_page();
    page.counts.rows = held;
    page.offset = held;
    page.more = false;
    page.runs = vec![run(ProcessorStateSection::Pending, held)];
    page.sections = vec![ProcessorStateSectionTransfer {
        section: ProcessorStateSection::Pending,
        rows: held,
        fields: declared_fields(),
        bodies: Vec::new(),
    }];
    page
}

/// A section whose rows outgrow one snapshot section reaches the coordinator whole. The owner
/// carries it in bodies that each stay within the section bound, and the coordinator presents
/// every row, in the order the owner returned them.
#[nervix_primitives::test]
async fn a_section_beyond_one_snapshot_section_reaches_the_coordinator_whole() {
    let executor = narrow_sections();
    let sequences = (1..=48).collect::<Vec<i64>>();
    let held = u64::try_from(sequences.len()).assured("a fixture row count fits u64");
    // 48 rows of 4 KiB text are about three times the section bound.
    let rows = wide_rows(&declared_fields(), &sequences, 4096);
    let request = read(held);
    let parts = processor_page_reply(
        &executor,
        Ok((
            whole_page(held),
            vec![ProcessorPageSection {
                batches: vec![rows],
            }],
        )),
        request.max_bytes,
        None,
    )
    .await
    .assured("an owner carries a section beyond one snapshot section instead of failing");
    let PageReplyParts { reply, bodies, .. } = parts;
    assert!(
        reply.is_ok(),
        "an owner refused a page it can carry: {reply:?}"
    );
    assert!(bodies.len() > 1, "the section outgrows one body");
    for body in &bodies {
        let length = u64::try_from(body.len()).assured("a bounded body fits u64");
        assert!(
            length <= NARROW_SECTION_BYTES,
            "a body of {length} bytes exceeds the {NARROW_SECTION_BYTES}-byte section bound"
        );
    }
    let sent = bodies.iter().map(|body| &body[..]).collect::<Vec<_>>();
    let stream = sent_page(&executor, reply, &sent).await;
    let received =
        ReceivedProcessorPage::decode(&executor, &stream, &request, &expected_sections())
            .await
            .assured("the coordinator accepts the bodies the owner sent");
    let Ok(page) = received else {
        panic!("an owner that returned rows did not refuse the read");
    };
    let [section] = page.sections.as_slice() else {
        panic!("a reorderer page carries its one held-row section");
    };
    assert!(
        section.batches.len() > 1,
        "the section arrives in the batches its bodies carried"
    );
    let mut returned = Vec::new();
    for batch in &section.batches {
        returned.extend_from_slice(batch.column(1).as_primitive::<Int64Type>().values());
    }
    assert_eq!(
        returned, sequences,
        "every row arrives once, in the order the owner returned them"
    );
}

/// A row whose own body would exceed the section bound cannot travel, and the owner refuses the
/// page as too large instead of failing its stream.
#[nervix_primitives::test]
async fn a_row_beyond_one_snapshot_section_is_refused_as_too_large() {
    let executor = narrow_sections();
    // One row of twice the section bound.
    let rows = wide_rows(&declared_fields(), &[1], 128 * 1024);
    let request = read(1);
    let PageReplyParts { reply, bodies, .. } = processor_page_reply(
        &executor,
        Ok((
            whole_page(1),
            vec![ProcessorPageSection {
                batches: vec![rows],
            }],
        )),
        request.max_bytes,
        None,
    )
    .await
    .assured("an owner answers a row it cannot carry with a typed refusal");
    assert!(
        matches!(reply, Err(StateInspectionRejection::ReplyTooLarge)),
        "a row beyond the section bound is refused as too large, found {reply:?}"
    );
    assert!(bodies.is_empty(), "a refused page names no body");
}

/// A page whose own memory leaves the bulk pool less room than its reply takes is still answered.
/// Its owner encodes the bodies and the descriptor from what the page set aside for them while
/// admitting its rows, and never waits for room in the pool, which only that page could free.
#[nervix_primitives::test]
async fn a_page_holding_most_of_the_pool_is_answered_without_waiting_for_room() {
    let executor = Executor::default();
    let capacity = executor.snapshot().bulk_memory.capacity_bytes;
    let left = 1024 * 1024;
    // The page's own memory: all of the bulk pool but one MiB.
    let held_memory = executor
        .try_reserve(
            MemoryClass::Bulk,
            capacity
                .checked_sub(left)
                .assured("the bulk pool holds more than one MiB"),
        )
        .assured("an idle pool admits the page's memory");
    let sequences = (1..=4).collect::<Vec<i64>>();
    let held = u64::try_from(sequences.len()).assured("a fixture row count fits u64");
    // Four rows of 1 MiB text: more than the pool has left.
    let rows = wide_rows(&declared_fields(), &sequences, 1024 * 1024);
    let PageReply::Remote {
        descriptor,
        framing,
    } = PageReply::remote(&executor)
    else {
        panic!("a reply to another node sets memory aside");
    };
    // What the page set aside while admitting its rows: the descriptor, and its one section's
    // rows with their body's framing.
    let bodies = batch_payload_bytes(&rows)
        .checked_add(framing)
        .assured("a fixture page fits u64");
    let (descriptor_memory, rest) = held_memory
        .split(descriptor)
        .assured("the page holds its descriptor's memory");
    let (body_memory, rest) = rest
        .split(bodies)
        .assured("the page holds its bodies' memory");
    let memory = ProcessorStatePageMemory {
        _reservation: rest,
        _windows: Vec::new(),
        reply: Some(ReplyMemory {
            descriptor: descriptor_memory,
            bodies: body_memory,
            framing,
        }),
    };
    let mut request = read(held);
    request.max_bytes = 16 * 1024 * 1024;
    nervix_primitives::time::timeout(
        std::time::Duration::from_secs(30),
        stream_processor_page(
            &executor,
            Ok((
                whole_page(held),
                vec![ProcessorPageSection {
                    batches: vec![rows],
                }],
            )),
            request.max_bytes,
            Some(memory),
        ),
    )
    .await
    .assured("the owner answers without waiting for room in the pool")
    .assured("the page's reply is framed");
}

/// A page that set nothing aside for its reply takes each body's memory at once. A further body of
/// a split section without room refuses the page as not ready, since waiting would hold the
/// section's earlier bodies.
#[nervix_primitives::test]
async fn a_split_section_of_a_page_without_reply_memory_never_waits_for_a_further_body() {
    let executor = narrow_sections();
    let capacity = executor.snapshot().bulk_memory.capacity_bytes;
    // Room for the first body, at most one section bound, but not for a second as well.
    let left = NARROW_SECTION_BYTES + NARROW_SECTION_BYTES / 2;
    let held_memory = executor
        .try_reserve(
            MemoryClass::Bulk,
            capacity
                .checked_sub(left)
                .assured("the bulk pool holds more than a section and a half"),
        )
        .assured("an idle pool admits the held memory");
    let sequences = (1..=48).collect::<Vec<i64>>();
    let held = u64::try_from(sequences.len()).assured("a fixture row count fits u64");
    // 48 rows of 4 KiB text travel in four bodies under a 64 KiB section bound.
    let rows = wide_rows(&declared_fields(), &sequences, 4096);
    let request = read(held);
    let PageReplyParts { reply, bodies, .. } = nervix_primitives::time::timeout(
        std::time::Duration::from_secs(30),
        processor_page_reply(
            &executor,
            Ok((
                whole_page(held),
                vec![ProcessorPageSection {
                    batches: vec![rows],
                }],
            )),
            request.max_bytes,
            None,
        ),
    )
    .await
    .assured("the owner never waits for a further body's room")
    .assured("the owner answers");
    assert!(
        matches!(reply, Err(StateInspectionRejection::NotReady)),
        "a further body without room refuses the page as not ready, found {reply:?}"
    );
    assert!(bodies.is_empty(), "a refused page names no body");
    drop(held_memory);
}

/// A page that set nothing aside for its reply never waits for room while it holds a body it
/// already encoded: the next section's body takes its memory at once, and without room for it the
/// page is refused as not ready, so the owner never waits for memory only it could free.
#[nervix_primitives::test]
async fn a_page_without_reply_memory_never_waits_for_a_body_while_it_holds_another() {
    let executor = Executor::default();
    let capacity = executor.snapshot().bulk_memory.capacity_bytes;
    // Room for the first section's body, its 1 MiB row and its framing, but not for the second's.
    let left = 3 * 1024 * 1024 / 2;
    let held_memory = executor
        .try_reserve(
            MemoryClass::Bulk,
            capacity
                .checked_sub(left)
                .assured("the bulk pool holds more than one and a half MiB"),
        )
        .assured("an idle pool admits the held memory");
    let first = wide_rows(&declared_fields(), &[1], 1024 * 1024);
    let second = wide_rows(&declared_fields(), &[2], 1024 * 1024);
    // A correlator page returning one row on each side, each side its own section.
    let mut page = whole_page(2);
    page.runs = vec![
        run(ProcessorStateSection::Left, 1),
        run(ProcessorStateSection::Right, 1),
    ];
    page.sections = vec![
        ProcessorStateSectionTransfer {
            section: ProcessorStateSection::Left,
            rows: 1,
            fields: declared_fields(),
            bodies: Vec::new(),
        },
        ProcessorStateSectionTransfer {
            section: ProcessorStateSection::Right,
            rows: 1,
            fields: declared_fields(),
            bodies: Vec::new(),
        },
    ];
    let mut request = read(2);
    // A reply bound that holds both bodies, so only the memory decides.
    request.max_bytes = 16 * 1024 * 1024;
    let PageReplyParts { reply, bodies, .. } = nervix_primitives::time::timeout(
        std::time::Duration::from_secs(30),
        processor_page_reply(
            &executor,
            Ok((
                page,
                vec![
                    ProcessorPageSection {
                        batches: vec![first],
                    },
                    ProcessorPageSection {
                        batches: vec![second],
                    },
                ],
            )),
            request.max_bytes,
            None,
        ),
    )
    .await
    .assured("the owner never waits for a body's room while it holds another body")
    .assured("the owner answers");
    assert!(
        matches!(reply, Err(StateInspectionRejection::NotReady)),
        "a body without room refuses the page as not ready, found {reply:?}"
    );
    assert!(bodies.is_empty(), "a refused page names no body");
    drop(held_memory);
}

/// An executor whose bulk class has one worker and one place in its wait queue.
fn one_place_bulk_executor() -> Executor {
    use std::num::NonZeroUsize;

    use nervix_execution::{ExecutionConfig, WorkerCounts};

    let workers = WorkerCounts::default();
    Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            bulk_cpu: NonZeroUsize::MIN,
            pending_jobs: nervix_execution::PendingJobCounts {
                bulk_cpu: NonZeroUsize::MIN,
                ..workers.pending_jobs
            },
            ..workers
        },
        ..ExecutionConfig::default()
    })
    .assured("the test changes only the bulk class's worker and queue counts")
}

/// The bulk class of a [`one_place_bulk_executor`] held full: its worker runs a job that waits for
/// the test, and its one place is taken by a job waiting behind it, so a job that refuses on a full
/// class is refused and one that waits for a place waits until the hold is released.
struct FullBulkClass {
    release: nervix_primitives::sync::oneshot::Sender<()>,
    occupying: nervix_primitives::task::JoinHandle<()>,
    queued: nervix_primitives::task::JoinHandle<()>,
}

impl FullBulkClass {
    async fn fill(executor: &Executor) -> Self {
        let (started, has_started) = nervix_primitives::sync::oneshot::channel();
        let (release, released) = nervix_primitives::sync::oneshot::channel::<()>();
        let charge = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .assured("the untouched bulk class has room for one byte");
        let occupier = executor.clone();
        let occupying = nervix_primitives::task::spawn(async move {
            occupier
                .run_cpu(
                    nervix_execution::CpuClass::Bulk,
                    charge,
                    move |_charge, _cancellation| {
                        started
                            .send(())
                            .assured("the test awaits the occupying job");
                        released
                            .blocking_recv()
                            .assured("the test releases the occupying job before it exits");
                    },
                )
                .await
                .assured("the occupying job runs");
        });
        has_started
            .await
            .assured("the occupying job reports once it holds the worker");
        let charge = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .assured("the bulk class has room for one more byte");
        let queuer = executor.clone();
        let queued = nervix_primitives::task::spawn(async move {
            queuer
                .run_cpu(
                    nervix_execution::CpuClass::Bulk,
                    charge,
                    |_charge, _cancellation| (),
                )
                .await
                .assured("the queued job runs once the worker frees");
        });
        while executor
            .snapshot()
            .cpu_class(nervix_execution::CpuClass::Bulk)
            .pending
            == 0
        {
            nervix_primitives::task::yield_now().await;
        }
        Self {
            release,
            occupying,
            queued,
        }
    }

    async fn release(self) {
        self.release
            .send(())
            .assured("the occupying job waits for the test to release it");
        self.occupying
            .await
            .assured("the occupying job's task is joined");
        self.queued.await.assured("the queued job's task is joined");
    }
}

fn bulk_refusals(executor: &Executor) -> u64 {
    executor
        .snapshot()
        .cpu_class(nervix_execution::CpuClass::Bulk)
        .refused
}

/// A page is the owner's answer to a read it has already served, so an owner whose bulk class is
/// full holds the encoding of the page's rows, their identity and the reply back instead of
/// refusing the read, and frames the page once a place frees.
#[nervix_primitives::test]
async fn an_owner_frames_a_page_once_its_full_bulk_class_frees_instead_of_refusing_it() {
    let executor = one_place_bulk_executor();
    let full = FullBulkClass::fill(&executor).await;
    let refused = bulk_refusals(&executor);
    let mut framing = std::pin::pin!(stream_processor_page(
        &executor,
        Ok((
            returning_page(3, declared_fields(), None),
            vec![ProcessorPageSection {
                batches: vec![held_rows(&declared_fields(), &[1, 2, 3])],
            }],
        )),
        u64::MAX,
        None,
    ));
    assert!(
        framing.as_mut().now_or_never().is_none(),
        "a full bulk class holds the page back"
    );
    full.release().await;
    let framed = framing.await;
    assert!(
        framed.is_ok(),
        "the page is framed once a place frees: {:?}",
        framed.err()
    );
    assert_eq!(
        bulk_refusals(&executor),
        refused,
        "a full bulk class refuses no page"
    );
}

/// A received page carries rows its owner already read, so a coordinator whose bulk class is full
/// holds the decoding of its reply, the check of its body and the decoding of its rows back
/// instead of refusing them, and presents the page once a place frees.
#[nervix_primitives::test]
async fn a_coordinator_presents_a_received_page_once_its_full_bulk_class_frees_instead_of_refusing_it()
 {
    let owner = Executor::default();
    let (identity, body) = owner_body(&owner, held_rows(&declared_fields(), &[1, 2, 3])).await;
    let stream = sent_page(
        &owner,
        Ok(returning_page(3, declared_fields(), Some(identity))),
        &[&body[..]],
    )
    .await;
    let coordinator = one_place_bulk_executor();
    let full = FullBulkClass::fill(&coordinator).await;
    let refused = bulk_refusals(&coordinator);
    let request = read(10);
    let expected = expected_sections();
    let mut decoding = std::pin::pin!(ReceivedProcessorPage::decode(
        &coordinator,
        &stream,
        &request,
        &expected,
    ));
    assert!(
        decoding.as_mut().now_or_never().is_none(),
        "a full bulk class holds the received page back"
    );
    full.release().await;
    let received = decoding
        .await
        .assured("the page is decoded once a place frees");
    let Ok(page) = received else {
        panic!("an owner that returned rows did not refuse the read");
    };
    let [section] = page.sections.as_slice() else {
        panic!("a reorderer page carries its one held-row section");
    };
    let [rows] = section.batches.as_slice() else {
        panic!("a section of three small rows travels in one body");
    };
    assert_eq!(
        rows.column(1).as_primitive::<Int64Type>().values().to_vec(),
        vec![1, 2, 3],
        "the rows are the body's rows in the order the owner returned them"
    );
    assert_eq!(
        bulk_refusals(&coordinator),
        refused,
        "a full bulk class refuses no received page"
    );
}
