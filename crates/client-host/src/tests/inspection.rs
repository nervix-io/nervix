//! Complete inspection projections, retained verified frames and checked indices.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current envelopes and exact metadata, schema and byte oracles.
//! - **Depends on.** The production inspection owner and current wire fixtures.
//! - **Must not know.** Host-language registration or server execution.

#[path = "../../../../tests/common/native_inspection.rs"]
mod fixture;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::wire::{StateInspectionResult, StateInspectionStage};
use nervix_models::{StateInspectionEntity, Unit, UnitKind};

use crate::{FailureKind, Inspection, InspectionUnitKind, Part};

fn check(input: &[u8]) {
    let mut bytes = [0_u8; 8];
    let count = input.len().min(bytes.len());
    bytes[..count].copy_from_slice(&input[..count]);
    let value = u64::from_le_bytes(bytes);
    let unit = match input.get(8).copied().unwrap_or(0) % 3 {
        0 => Unit::SINGLE,
        1 => Unit::partition(u32::from_le_bytes(
            bytes[..4].try_into().assured("four bytes"),
        )),
        _ => Unit::try_instance(value % (Unit::MAX_INSTANCE_INDEX + 1))
            .assured("the bounded instance index is valid"),
    };
    let stage = match input.get(9).copied().unwrap_or(0) % 2 {
        0 => StateInspectionStage::Live,
        _ => StateInspectionStage::Stored,
    };
    let mut state = fixture::page(unit, stage, value);
    if input.get(10).copied().unwrap_or(0) % 2 == 1 {
        state.entity = StateInspectionEntity::ClusterLicense;
    }
    let inspection = Inspection::new(state.clone());
    assert_eq!(inspection.state(), &state);
    assert_eq!(inspection.json(), state.to_json());
    match &state.entity {
        StateInspectionEntity::DomainNode(entity) => {
            assert_eq!(inspection.domain(), Some(entity.domain.as_str()));
            assert_eq!(inspection.entity_kind(), entity.node.kind.keyword_phrase());
            assert_eq!(
                inspection.entity_name(),
                Some(entity.node.identifier.as_str())
            );
        }
        StateInspectionEntity::ClusterLicense => {
            assert_eq!(inspection.domain(), None);
            assert_eq!(inspection.entity_kind(), "LICENSE");
            assert_eq!(inspection.entity_name(), None);
        }
    }
    assert_eq!(inspection.rejection(), None);
    let StateInspectionResult::Page(page) = state.result else {
        panic!("the fixture is a page");
    };
    assert_eq!(inspection.counts(), Some(page.counts));
    assert_eq!(inspection.unit_count(), Some(page.unit_count));
    assert_eq!(inspection.continuation(), page.continuation.as_deref());
    assert_eq!(inspection.units(), page.units);
    let expected_unit = &page.units[0];
    assert_eq!(inspection.unit(0).assured("the unit exists"), expected_unit);
    let (kind, index) = match unit.kind() {
        UnitKind::Single => (InspectionUnitKind::Single, None),
        UnitKind::Partition(index) => (InspectionUnitKind::Partition, Some(u64::from(index))),
        UnitKind::Instance(index) => (InspectionUnitKind::Instance, Some(index)),
    };
    assert_eq!(inspection.unit_kind(0).assured("the unit exists"), kind);
    assert_eq!(inspection.unit_index(0).assured("the unit exists"), index);
    let section = &expected_unit.sections[0];
    assert_eq!(
        inspection.section(0, 0).assured("the section exists"),
        section
    );
    let schema = inspection.schema(0, 0).assured("the section exists");
    assert_eq!(schema.fields(Part::Rows), section.schema.fields);
    let branch = section
        .schema
        .branch
        .as_ref()
        .assured("the fixture is branched");
    assert_eq!(schema.branch(), Some(branch.branch().as_str()));
    assert_eq!(schema.fields(Part::BranchKey), branch.fields());
    let frame = inspection
        .row_frame(0, 0, 0)
        .assured("the verified frame conforms");
    assert_eq!(frame.bytes(), section.frames[0].bytes().as_ref());
    assert_eq!(frame.bytes().as_ptr(), section.frames[0].bytes().as_ptr());
    assert_eq!(frame.row_count(), 1);
    assert_eq!(frame.schema().fields(Part::Rows), schema.fields(Part::Rows));
    assert_eq!(
        inspection.unit(1).unwrap_err().kind(),
        FailureKind::InvalidArgument
    );
    assert_eq!(
        inspection.row_frame(0, 1, 0).unwrap_err().kind(),
        FailureKind::InvalidArgument
    );
    assert_eq!(
        inspection.row_frame(0, 0, 1).unwrap_err().kind(),
        FailureKind::InvalidArgument
    );
    drop(inspection);
    assert_eq!(frame.bytes(), section.frames[0].bytes().as_ref());
    assert_eq!(frame.schema().fields(Part::Rows), section.schema.fields);
    for reason in fixture::REJECTIONS {
        let state = fixture::refused(reason);
        let inspection = Inspection::new(state.clone());
        assert_eq!(inspection.state(), &state);
        assert_eq!(inspection.json(), state.to_json());
        assert_eq!(inspection.rejection(), Some(reason));
        assert_eq!(inspection.counts(), None);
        assert_eq!(inspection.unit_count(), None);
        assert_eq!(inspection.continuation(), None);
        assert!(inspection.units().is_empty());
        assert_eq!(
            inspection.section(0, 0).unwrap_err().kind(),
            FailureKind::InvalidArgument
        );
    }
}

#[test]
fn inspection_views_preserve_boundary_values() {
    for input in [&[][..], &[0; 10], &[255; 10], &[1; 10], &[2; 10]] {
        check(input);
    }
    check(&[255; 11]);
    let mut state = fixture::page(Unit::SINGLE, StateInspectionStage::Live, 0);
    let StateInspectionResult::Page(page) = &mut state.result else {
        panic!("the fixture is a page");
    };
    page.units[0].sections[0].schema.fields[0].ty = nervix_models::ParseAsType::String;
    let inspection = Inspection::new(state);
    assert_eq!(
        inspection.row_frame(0, 0, 0).unwrap_err().kind(),
        FailureKind::Protocol,
        "a frame that disagrees with its declared schema cannot become a native view"
    );
}

#[test]
fn bolero_inspection_views_preserve_complete_envelopes() {
    bolero::check!()
        .with_iterations(64)
        .with_max_len(32)
        .for_each(|input: &[u8]| check(input));
}
