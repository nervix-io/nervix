//! Retained inspection ownership through the exported C ABI.

use nervix_client_core::wire::{
    CellView, StateInspection, StateInspectionCounts, StateInspectionOrigin, StateInspectionPage,
    StateInspectionRejection, StateInspectionResult, StateInspectionSection, StateInspectionStage,
    StateInspectionUnit, StateRowsEncoder,
};
use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation, DomainNodeRef, ModelKind, Unit};

use super::*;
use crate::{
    nx_inspection_counts, nx_inspection_free, nx_inspection_json, nx_inspection_row_frame,
    nx_inspection_schema, nx_outcome_inspection,
};

fn inspected() -> CommandOutcome {
    let limits = SessionLimits::DEFAULT;
    let mut encoder = StateRowsEncoder::unbranched(&limits);
    encoder
        .push_row(|cells| cells.push_u64(u64::MAX))
        .assured("one exact cell fits");
    let frame = encoder.finish(&limits).assured("accepted row finishes");
    let counts = StateInspectionCounts {
        branches: 1,
        rows: 1,
        bytes: 8,
    };
    let mut outcome = command_outcome(
        CommandDisposition::Completed {
            already_existed: false,
        },
        false,
    );
    outcome.state = Some(Box::new(StateInspection {
        entity: DomainNodeRef::node_in(
            name("tenant"),
            ModelKind::Relay,
            name::<nervix_models::RelayName>("events"),
        ),
        result: StateInspectionResult::Page(StateInspectionPage {
            counts,
            unit_count: 1,
            continuation: None,
            units: vec![StateInspectionUnit {
                unit: Unit::SINGLE,
                owner: name("node"),
                replicas: vec![],
                origin: StateInspectionOrigin {
                    node: ClusterNodeIdentity::new(name("node"), ClusterNodeIncarnation::new(4)),
                    fingerprint: [7; 32],
                    generation: 9,
                    revision: 13,
                    stage: StateInspectionStage::Live,
                },
                counts,
                sections: vec![StateInspectionSection {
                    name: "records".to_string(),
                    schema: RowSchema {
                        fields: vec![field("value", ParseAsType::U64)],
                        branch: None,
                    },
                    frames: vec![frame],
                    omitted_rows: 0,
                    watermarks: None,
                }],
                wasm: None,
            }],
            wasm_binding: None,
        }),
    }));
    outcome
}

#[test]
fn inspection_keeps_exact_rows_after_the_original_outcome_is_freed() {
    let outcome = Box::into_raw(Box::new(Outcome::new(inspected())));
    let mut inspection = ptr::null_mut();
    // SAFETY: handles are live, outputs writable and each returned handle is freed once below.
    unsafe {
        succeeded(nx_outcome_inspection(outcome, &mut inspection));
        nx_outcome_free(outcome);
        let (mut branches, mut rows, mut bytes) = (0, 0, 0);
        assert!(nx_inspection_counts(
            inspection,
            &mut branches,
            &mut rows,
            &mut bytes
        ));
        assert_eq!((branches, rows, bytes), (1, 1, 8));
        let (mut data, mut len) = (ptr::null(), 0);
        nx_inspection_json(inspection, &mut data, &mut len);
        assert!(
            std::str::from_utf8(slice::from_raw_parts(data, len))
                .assured("JSON UTF8")
                .contains("18446744073709551615")
        );
        let mut schema = ptr::null_mut();
        succeeded(nx_inspection_schema(inspection, 0, 0, &mut schema));
        succeeded(nx_inspection_row_frame(
            inspection, 0, 0, 0, &mut data, &mut len,
        ));
        let borrowed = slice::from_raw_parts(data, len);
        let retained = nervix_client_core::wire::StateRowFrame::from_bytes(
            bytes::Bytes::copy_from_slice(borrowed),
            &SessionLimits::DEFAULT,
        )
        .assured("borrowed NXRB frame verifies");
        assert_eq!(
            retained.batch().rows().next().assured("one row").get(0),
            Some(CellView::U64(u64::MAX))
        );
        assert_eq!(
            failure_kind(nx_inspection_row_frame(
                inspection, 0, 0, 1, &mut data, &mut len
            )),
            FailureKind::InvalidArgument
        );
        nx_inspection_free(inspection);
        let mut count = 0;
        succeeded(nx_schema_field_count(schema, ROWS, &mut count));
        assert_eq!(count, 1);
        nx_schema_free(schema);
    }
}

#[test]
fn inspection_refusal_retains_its_reason_without_fabricated_counts_or_sections() {
    let mut outcome = inspected();
    outcome.state.as_mut().assured("typed inspection").result =
        StateInspectionResult::Rejected(StateInspectionRejection::Deadline);
    let outcome = Box::into_raw(Box::new(Outcome::new(outcome)));
    let mut inspection = ptr::null_mut();
    // SAFETY: handles are live and each owned output is released once.
    unsafe {
        succeeded(nx_outcome_inspection(outcome, &mut inspection));
        nx_outcome_free(outcome);
        assert!(!nx_inspection_counts(
            inspection,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut()
        ));
        let mut schema = ptr::null_mut();
        assert_eq!(
            failure_kind(nx_inspection_schema(inspection, 0, 0, &mut schema)),
            FailureKind::InvalidArgument
        );
        nx_inspection_free(inspection);
    }
}
