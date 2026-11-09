//! Current inspection envelope round trips, exact cells and retained backing storage.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, DomainNodeRef, ModelKind, ParseAsType,
    SchemaField, Timestamp, Unit,
};

use super::*;
use crate::{
    CommandDisposition, Reply, ReplyBody, ReplyDelivery, RowBranch, ServerFrame, ServerMessage,
    SessionLimits, StateRowsEncoder, VerifiedFrame,
    tests::{
        fixtures::{name, request, round_trip_reply},
        samples::command_outcome,
    },
};

fn field(raw: &str, ty: ParseAsType, sensitive: bool) -> SchemaField {
    SchemaField {
        name: name(raw),
        ty,
        sensitive,
        optional: false,
    }
}

pub(crate) fn inspection(value: u64) -> StateInspection {
    let limits = SessionLimits::DEFAULT;
    let schema = RowSchema {
        fields: vec![
            field("sequence", ParseAsType::U64, false),
            field("secret", ParseAsType::String, true),
            field("sample", ParseAsType::F32, false),
            field("low", ParseAsType::Datetime, false),
            field("high", ParseAsType::Datetime, false),
        ],
        branch: Some(
            RowBranch::new(
                name("by_tenant"),
                vec![field("tenant", ParseAsType::String, true)],
            )
            .assured("the branch has one field"),
        ),
    };
    let mut rows = StateRowsEncoder::branched(&limits, |writer| writer.push_redacted())
        .assured("a redacted key fits");
    rows.push_row(|writer| {
        writer.push_u64(value)?;
        writer.push_redacted()?;
        writer.push_f32(-0.0)?;
        writer.push_datetime(Timestamp::from_unix_nanos(i64::MIN))?;
        writer.push_datetime(Timestamp::from_unix_nanos(i64::MAX))
    })
    .assured("the typed fixture fits");
    let frame = rows
        .finish(&limits)
        .assured("accepted cells finish within their bound");
    let counts = StateInspectionCounts {
        branches: 2,
        rows: 2,
        bytes: 80,
    };
    StateInspection {
        entity: DomainNodeRef::node_in(
            name("sales"),
            ModelKind::Relay,
            name::<nervix_models::RelayName>("events"),
        )
        .into(),
        result: StateInspectionResult::Page(StateInspectionPage {
            counts,
            unit_count: 1,
            units: vec![StateInspectionUnit {
                unit: Unit::partition(3),
                owner: name("node_a"),
                replicas: vec![name("node_b")],
                origin: StateInspectionOrigin {
                    node: ClusterNodeIdentity::new(name("node_a"), ClusterNodeIncarnation::new(17)),
                    fingerprint: [37; 32],
                    generation: 4,
                    revision: 19,
                    stage: StateInspectionStage::Live,
                },
                counts,
                sections: vec![StateInspectionSection {
                    name: "records".to_string(),
                    schema,
                    frames: vec![frame],
                    omitted_rows: 1,
                    watermarks: Some(StateWatermarkColumns { low: 3, high: 4 }),
                }],
            }],
            continuation: Some("one-opaque-position".to_string()),
        }),
    }
}

fn outcome(state: StateInspection) -> crate::CommandOutcome {
    let mut outcome = command_outcome(CommandDisposition::Completed {
        already_existed: false,
    });
    outcome.message = state.to_json();
    outcome.state = Some(Box::new(state));
    outcome
}

#[test]
fn exact_typed_rows_origins_and_continuation_round_trip_beside_their_json() {
    let original = outcome(inspection(u64::MAX));
    let decoded = round_trip_reply(&Reply {
        request_id: request(1),
        body: ReplyBody::Command(Box::new(original.clone())),
    });
    assert_eq!(decoded.body, ReplyBody::Command(Box::new(original)));
    let ReplyBody::Command(command) = decoded.body else {
        panic!("the reply is a command");
    };
    let state = command.state.assured("the command inspected state");
    let json: serde_json::Value =
        serde_json::from_str(&command.message).assured("one JSON document");
    assert_eq!(json["counts"]["rows"], 2);
    assert_eq!(
        json["units"][0]["sections"][0]["frames"][0]["rows"][0][0]["value"],
        u64::MAX
    );
    assert_eq!(
        json["units"][0]["sections"][0]["frames"][0]["rows"][0][2]["bits"],
        (-0.0_f32).to_bits()
    );
    let text = state.display_text().assured("rows conform");
    assert!(text.contains("owner: node_a"));
    assert!(text.contains("node_a#17"));
    assert!(text.contains("key={\"tenant\":\"<masked>\"}"));
    assert!(text.contains("\"secret\":\"<masked>\""));
}

#[test]
fn embedded_frames_share_the_verified_reply_and_survive_its_release() {
    let limits = SessionLimits::DEFAULT;
    let reply = Reply {
        request_id: request(1),
        body: ReplyBody::Command(Box::new(outcome(inspection(9)))),
    };
    let ReplyDelivery::Frame(encoded) = reply.encode(&limits).assured("the fixture fits one frame")
    else {
        panic!("one frame");
    };
    let outer = encoded.into_bytes();
    let frame = VerifiedFrame::<ServerFrame>::verify(outer.clone(), &limits)
        .assured("the encoder writes a verified frame");
    let ServerMessage::Reply(reply) = ServerMessage::decode(&frame).assured("the reply decodes")
    else {
        panic!("reply");
    };
    let ReplyBody::Command(command) = reply.body else {
        panic!("command");
    };
    let state = command.state.assured("state envelope");
    let StateInspectionResult::Page(page) = state.result else {
        panic!("page");
    };
    let rows = page.units[0].sections[0].frames[0].clone();
    let original = outer
        .windows(rows.bytes().len())
        .find(|window| *window == rows.bytes().as_ref())
        .assured("embedded frame bytes occur in their parent");
    assert_eq!(original.as_ptr(), rows.bytes().as_ptr());
    drop(frame);
    drop(outer);
    drop(page);
    assert_eq!(rows.batch().len(), 1);
}

#[test]
fn every_current_stage_and_refusal_has_a_lossless_wire_variant() {
    for &reason in codec::ALL_STATE_INSPECTION_REJECTIONS {
        let mut state = inspection(1);
        state.result = StateInspectionResult::Rejected(reason);
        let original = outcome(state);
        assert_eq!(
            round_trip_reply(&Reply {
                request_id: request(1),
                body: ReplyBody::Command(Box::new(original.clone()))
            })
            .body,
            ReplyBody::Command(Box::new(original))
        );
    }
    for &stage in codec::ALL_STATE_INSPECTION_STAGES {
        let mut state = inspection(1);
        let StateInspectionResult::Page(page) = &mut state.result else {
            panic!("page");
        };
        page.units[0].origin.stage = stage;
        let original = outcome(state);
        assert_eq!(
            round_trip_reply(&Reply {
                request_id: request(1),
                body: ReplyBody::Command(Box::new(original.clone()))
            })
            .body,
            ReplyBody::Command(Box::new(original))
        );
    }
}

#[test]
fn bolero_state_inspection_envelope_round_trips() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary =
                nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Vocabulary);
            let mut state = inspection(arbitrary.positive_u64().get());
            state.entity = if arbitrary.entropy().flag() {
                StateInspectionEntity::ClusterLicense
            } else {
                StateInspectionEntity::DomainNode(DomainNodeRef::node_in(
                    arbitrary.name(),
                    ModelKind::Relay,
                    arbitrary.name::<nervix_models::RelayName>(),
                ))
            };
            if arbitrary.entropy().flag() {
                let reason = codec::ALL_STATE_INSPECTION_REJECTIONS[arbitrary.entropy().index(
                    std::num::NonZeroUsize::new(codec::ALL_STATE_INSPECTION_REJECTIONS.len())
                        .assured("the rejection catalog is nonempty"),
                )];
                state.result = StateInspectionResult::Rejected(reason);
            } else {
                let StateInspectionResult::Page(page) = &mut state.result else {
                    panic!("the fixture is a page");
                };
                page.continuation = arbitrary
                    .entropy()
                    .flag()
                    .then(|| arbitrary.non_empty_string());
                let unit = &mut page.units[0];
                unit.owner = arbitrary.name();
                unit.origin.node = ClusterNodeIdentity::new(
                    unit.owner.clone(),
                    ClusterNodeIncarnation::new(arbitrary.positive_u64().get()),
                );
                unit.origin.fingerprint = std::array::from_fn(|_| arbitrary.entropy().byte());
                unit.origin.generation = arbitrary.positive_u64().get();
                unit.origin.revision = arbitrary.positive_u64().get();
                unit.origin.stage = codec::ALL_STATE_INSPECTION_STAGES[arbitrary.entropy().index(
                    std::num::NonZeroUsize::new(codec::ALL_STATE_INSPECTION_STAGES.len())
                        .assured("the stage catalog is nonempty"),
                )];
            }
            let original = outcome(state);
            let decoded = round_trip_reply(&Reply {
                request_id: request(1),
                body: ReplyBody::Command(Box::new(original.clone())),
            });
            assert_eq!(decoded.body, ReplyBody::Command(Box::new(original)));
        });
}

#[test]
fn an_embedded_row_root_is_verified_independently_of_its_outer_byte_vector() {
    let limits = SessionLimits::DEFAULT;
    let state = inspection(9);
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("page")
    };
    let nested = page.units[0].sections[0].frames[0].bytes();
    let reply = Reply {
        request_id: request(1),
        body: ReplyBody::Command(Box::new(outcome(state.clone()))),
    };
    let ReplyDelivery::Frame(encoded) = reply.encode(&limits).assured("one bounded reply") else {
        panic!("frame")
    };
    let mut bytes = encoded.into_bytes().to_vec();
    let offset = bytes
        .windows(nested.len())
        .position(|window| window == nested.as_ref())
        .assured("the frame is embedded intact");
    bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let outer = VerifiedFrame::<ServerFrame>::verify(bytes.into(), &limits)
        .assured("the outer byte vector remains structurally valid");
    assert!(
        ServerMessage::decode(&outer).is_err(),
        "a malformed embedded root never becomes a row view"
    );
}

#[test]
fn watermark_positions_must_name_distinct_required_public_datetime_columns() {
    for columns in [
        StateWatermarkColumns { low: 3, high: 3 },
        StateWatermarkColumns { low: 0, high: 4 },
        StateWatermarkColumns { low: 3, high: 99 },
    ] {
        let mut state = inspection(7);
        let StateInspectionResult::Page(page) = &mut state.result else {
            panic!("page")
        };
        page.units[0].sections[0].watermarks = Some(columns);
        let reply = Reply {
            request_id: request(1),
            body: ReplyBody::Command(Box::new(outcome(state))),
        };
        assert!(reply.encode(&SessionLimits::DEFAULT).is_err());
    }
}
