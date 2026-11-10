//! Current typed inspection fixtures shared by native binding boundary tests.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** Exact page, refusal and verified Row fixtures with independent field oracles.
//! - **Depends on.** The public wire encoder and vocabulary.
//! - **Must not know.** A host language, runtime state storage or a binding's implementation.

use std::{fmt::Debug, str::FromStr};

use meticulous::ResultExt as _;
use nervix_client_core::wire::{
    RowBranch, RowSchema, SessionLimits, StateInspection, StateInspectionCounts,
    StateInspectionOrigin, StateInspectionPage, StateInspectionRejection, StateInspectionResult,
    StateInspectionSection, StateInspectionStage, StateInspectionUnit, StateRowsEncoder,
    StateWatermarkColumns,
};
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, DomainNodeRef, ModelKind, ParseAsType, RelayName,
    SchemaField, Timestamp, Unit,
};

fn name<T: FromStr>(text: &str) -> T
where
    T::Err: Debug,
{
    text.parse()
        .assured("the fixture supplies a valid typed name")
}

fn field(text: &str, ty: ParseAsType, sensitive: bool, optional: bool) -> SchemaField {
    SchemaField {
        name: name(text),
        ty,
        sensitive,
        optional,
    }
}

pub fn page(unit: Unit, stage: StateInspectionStage, value: u64) -> StateInspection {
    let limits = SessionLimits::DEFAULT;
    let schema = RowSchema {
        fields: vec![
            field("sequence", ParseAsType::U64, false, false),
            field("secret", ParseAsType::String, true, false),
            field("sample", ParseAsType::F32, false, false),
            field("low", ParseAsType::Datetime, false, false),
            field("high", ParseAsType::Datetime, false, false),
            field("note", ParseAsType::String, false, true),
            field("raw", ParseAsType::Bytes, false, false),
            field("text", ParseAsType::String, false, false),
        ],
        branch: Some(
            RowBranch::new(
                name("by_tenant"),
                vec![field("tenant", ParseAsType::String, true, false)],
            )
            .assured("the fixture declares one branch key field"),
        ),
    };
    let mut rows = StateRowsEncoder::branched(&limits, |cells| cells.push_redacted())
        .assured("the fixture branch key fits the Row limit");
    rows.push_row(|cells| {
        cells.push_u64(value)?;
        cells.push_redacted()?;
        cells.push_f32(-0.0)?;
        cells.push_datetime(Timestamp::from_unix_nanos(i64::MIN))?;
        cells.push_datetime(Timestamp::from_unix_nanos(i64::MAX))?;
        cells.push_null()?;
        cells.push_bytes(&[0, 255, 128])?;
        cells.push_string("h\u{e9}\0")
    })
    .assured("the fixture's eight exact typed cells fit the Row limit");
    let frame = rows
        .finish(&limits)
        .assured("the fixture's encoded Row frame verifies");
    frame
        .conform(&schema)
        .assured("every encoded cell has the exact declared type and sensitivity");
    let counts = StateInspectionCounts {
        branches: 2,
        rows: 2,
        bytes: 80,
    };
    StateInspection {
        entity: DomainNodeRef::node_in(
            name("sales"),
            ModelKind::Relay,
            name::<RelayName>("events"),
        )
        .into(),
        result: StateInspectionResult::Page(StateInspectionPage {
            counts,
            unit_count: 1,
            units: vec![StateInspectionUnit {
                unit,
                owner: name("node_a"),
                replicas: vec![name("node_b"), name("node_c")],
                origin: StateInspectionOrigin {
                    node: ClusterNodeIdentity::new(
                        name("node_a"),
                        ClusterNodeIncarnation::new(u64::MAX),
                    ),
                    fingerprint: [37; 32],
                    generation: u64::MAX,
                    revision: u64::MAX,
                    stage,
                },
                counts,
                sections: vec![StateInspectionSection {
                    name: "records".to_owned(),
                    schema,
                    frames: vec![frame],
                    omitted_rows: 1,
                    watermarks: Some(StateWatermarkColumns { low: 3, high: 4 }),
                }],
                wasm: None,
            }],
            continuation: Some("opaque\0position".to_owned()),
            wasm_binding: None,
        }),
    }
}

pub fn refused(reason: StateInspectionRejection) -> StateInspection {
    StateInspection {
        entity: DomainNodeRef::node_in(name("sales"), ModelKind::Relay, name::<RelayName>("plain"))
            .into(),
        result: StateInspectionResult::Rejected(reason),
    }
}

pub const REJECTIONS: [StateInspectionRejection; 9] = [
    StateInspectionRejection::UnknownEntity,
    StateInspectionRejection::NotMaterialized,
    StateInspectionRejection::InvalidSelection,
    StateInspectionRejection::InvalidLimit,
    StateInspectionRejection::StaleContinuation,
    StateInspectionRejection::OwnerUnavailable,
    StateInspectionRejection::NotReady,
    StateInspectionRejection::Deadline,
    StateInspectionRejection::ReplyTooLarge,
];
