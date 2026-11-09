//! The checks a coordinator applies to the descriptor of a processor page an owner sent, before it
//! takes any body.
//!
//! Layer: test harness.
//! - **Owns.** The descriptor contracts of a received processor page: its returned rows start at
//!   the read's resume offset and stay within its row limit, its continuation flag and offset agree
//!   with its complete counts, it names at most three sections and exactly the sections a
//!   row-returning read expects, and its runs name sections the page carries, return at least one
//!   row each and add up to the rows its offset advanced.
//! - **Depends on.** The production descriptor check and the interconnect page messages.
//! - **Must not know.** Streams, bodies, Arrow decoding, sessions or Row frames.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use nervix_interconnect::ProcessorStateRunTransfer;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ModelKind, ModelName, NodeRef, SchemaFingerprint,
    StateInspectionBranchSelection, StateInspectionCounts, StateInspectionOrigin,
    StateInspectionStage, Unit,
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

    // It names more sections than any processor holds.
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
