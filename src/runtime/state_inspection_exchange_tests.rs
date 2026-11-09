//! The checks a coordinator applies to a processor page an owner sent: to its descriptor before it
//! takes any body, and to every body before any row is used.
//!
//! Layer: test harness.
//! - **Owns.** The descriptor contracts of a received processor page: its returned rows start at
//!   the read's resume offset and stay within its row limit, its continuation flag and offset agree
//!   with its complete counts, it names at most two sections and exactly the sections a
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
use nervix_interconnect::ProcessorStateRunTransfer;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, FieldName, ModelKind, ModelName, NodeRef,
    ParseAsType, SchemaFingerprint, StateInspectionBranchSelection, StateInspectionCounts,
    StateInspectionOrigin, StateInspectionStage, Unit,
};

use super::*;
use crate::runtime::named;

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
        body: None,
    }
}

fn run(section: ProcessorStateSection, rows: u64) -> ProcessorStateRunTransfer {
    ProcessorStateRunTransfer {
        section,
        branch: None,
        rows,
    }
}

/// A page of a reorderer holding five rows that returns the first three of them in two runs.
fn pending_page() -> ProcessorStatePageDescriptor {
    ProcessorStatePageDescriptor {
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

    // It names more sections than any processor holds.
    let mut three = pending_page();
    three.sections = vec![
        section(ProcessorStateSection::Pending, 5),
        section(ProcessorStateSection::Left, 0),
        section(ProcessorStateSection::Right, 0),
    ];
    let three_sections: Vec<(ProcessorStateSection, Vec<SchemaField>)> = vec![
        (ProcessorStateSection::Pending, Vec::new()),
        (ProcessorStateSection::Left, Vec::new()),
        (ProcessorStateSection::Right, Vec::new()),
    ];
    assert_refused(&three, &read(10), &three_sections);

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
        .encode_arrow_snapshot_section(executor)
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
        body,
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
    let rows = section
        .batch
        .as_ref()
        .assured("a section that returned rows carries them");
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
        (section.rows, section.batch.is_none()),
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
