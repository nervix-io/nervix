//! Typed inspection projections and Python-owned view lifetimes.

#[path = "../../../tests/common/native_inspection.rs"]
mod fixture;

use meticulous::ResultExt as _;
use nervix_client_core::wire::{StateInspectionResult, StateInspectionStage};
use nervix_client_host as host;
use nervix_models::Unit;
use pyo3::{prelude::*, types::PyBytes};

fn check_enum_projection(input: &[u8]) {
    let choice = usize::from(input.first().copied().unwrap_or(0));
    let reason = fixture::REJECTIONS[choice % fixture::REJECTIONS.len()];
    let projected = crate::enums::InspectionRejection::from(reason);
    assert_eq!(host::InspectionRejection::from(projected), reason);
    let stages = [host::InspectionStage::Live, host::InspectionStage::Stored];
    let stage = stages[choice % stages.len()];
    assert_eq!(
        host::InspectionStage::from(crate::enums::InspectionStage::from(stage)),
        stage
    );
    let kinds = [
        host::InspectionUnitKind::Single,
        host::InspectionUnitKind::Partition,
        host::InspectionUnitKind::Instance,
    ];
    let kind = kinds[choice % kinds.len()];
    assert_eq!(
        host::InspectionUnitKind::from(crate::enums::InspectionUnitKind::from(kind)),
        kind
    );
}

#[test]
fn every_inspection_enum_round_trips() {
    for index in 0..18 {
        check_enum_projection(&[index]);
    }
}

#[test]
fn bolero_inspection_enum_round_trips() {
    bolero::check!()
        .with_iterations(32)
        .with_max_len(8)
        .for_each(|input: &[u8]| check_enum_projection(input));
}

#[test]
fn inspection_metadata_and_frames_outlive_their_python_parents() {
    let state = fixture::page(
        Unit::try_instance(Unit::MAX_INSTANCE_INDEX).assured("the maximum instance index is valid"),
        StateInspectionStage::Stored,
        u64::MAX,
    );
    let expected_json = state.to_json();
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("the fixture constructs a page");
    };
    let expected_frame = page.units[0].sections[0].frames[0].bytes().to_vec();
    super::run_with(
        c"
import gc, weakref

assert inspection.domain == 'sales'
assert inspection.entity_kind == 'RELAY'
assert inspection.entity_name == 'events'
assert inspection.json == expected_json
assert inspection.rejection is None
assert inspection.unit_count == 1
assert inspection.continuation == 'opaque' + chr(0) + 'position'
assert (inspection.counts.branches, inspection.counts.rows, inspection.counts.bytes) == (2, 2, 80)
assert len(inspection.units) == 1
unit = inspection.units[0]
assert unit.kind == nx.InspectionUnitKind.INSTANCE
assert unit.index == expected_index
assert unit.owner == 'node_a'
assert unit.replicas == ['node_b', 'node_c']
assert unit.origin_node == 'node_a'
assert unit.origin_incarnation == 2**64 - 1
assert unit.origin_fingerprint == bytes([37] * 32)
assert unit.origin_generation == 2**64 - 1
assert unit.origin_revision == 2**64 - 1
assert unit.origin_stage == nx.InspectionStage.STORED
assert (unit.counts.branches, unit.counts.rows, unit.counts.bytes) == (2, 2, 80)
assert len(unit.sections) == 1
section = unit.sections[0]
assert section.name == 'records'
assert section.omitted_rows == 1
assert (section.watermark_low, section.watermark_high) == (3, 4)
assert len(section.frames) == 1
schema = section.schema
assert schema.branch == 'by_tenant'
assert [(field.name, field.sensitive) for field in schema.key_fields] == [('tenant', True)]
assert [field.name for field in schema.fields] == ['sequence', 'secret', 'sample', 'low', 'high', 'note', 'raw', 'text']
assert schema.fields[0].type == nx.FieldType.U64
assert schema.fields[1].sensitive
assert schema.fields[5].nullable
frame = section.frames[0]
assert frame.row_count == 1
assert [field.name for field in frame.schema.fields] == [field.name for field in schema.fields]
view = frame.frame
assert view.readonly and bytes(view) == expected_frame
assert bytes(memoryview(frame)) == expected_frame
try:
    view[0] = 0
except TypeError:
    pass
else:
    raise AssertionError('an inspection frame must be read only')
page_reference = weakref.ref(inspection)
frame_reference = weakref.ref(frame)
slice_view = view[3:]
del inspection, unit, section, frame
gc.collect()
assert page_reference() is None
assert frame_reference() is not None
assert bytes(view) == expected_frame
assert schema.fields[0].name == 'sequence'
view.release()
del view
gc.collect()
assert bytes(slice_view) == expected_frame[3:]
slice_view.release()
del slice_view
gc.collect()
assert frame_reference() is None
",
        |py, globals| {
            globals.set_item(
                "inspection",
                Bound::new(py, crate::inspection::Inspection::new(host::Inspection::new(state)))?,
            )?;
            globals.set_item("expected_index", Unit::MAX_INSTANCE_INDEX)?;
            globals.set_item("expected_json", expected_json)?;
            globals.set_item("expected_frame", PyBytes::new(py, &expected_frame))
        },
    );
}

#[test]
fn every_inspection_refusal_is_distinct_from_an_empty_page() {
    for reason in fixture::REJECTIONS {
        let state = fixture::refused(reason);
        let expected_json = state.to_json();
        super::run_with(
            c"
assert inspection.domain == 'sales'
assert inspection.entity_name == 'plain'
assert inspection.rejection == getattr(nx.InspectionRejection, expected_reason)
assert inspection.counts is None
assert inspection.unit_count is None
assert inspection.continuation is None
assert inspection.units == []
assert inspection.json == expected_json
",
            |py, globals| {
                globals.set_item(
                    "inspection",
                    Bound::new(
                        py,
                        crate::inspection::Inspection::new(host::Inspection::new(state)),
                    )?,
                )?;
                globals.set_item("expected_reason", reason.as_ref().to_ascii_uppercase())?;
                globals.set_item("expected_json", expected_json)
            },
        );
    }
}
