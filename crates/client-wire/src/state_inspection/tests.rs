//! Current inspection envelope round trips, exact cells and retained backing storage.

#[path = "published_tests.rs"]
pub(crate) mod published;

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BranchKeyFingerprint, ClusterNodeIdentity, ClusterNodeIncarnation, CommandExecutionReference,
    DomainNodeRef, ModelKind, ParseAsType, SchemaField, Timestamp, Unit, WasmBranchStateInspection,
    WasmCheckpointCounts, WasmCheckpointStage, WasmCheckpointState, WasmRecoveryCounts,
    WasmRecoveryInspection, WasmSavedStateRejection, WasmStateGeneration, WasmStateRecoveryOutcome,
    WasmStateReset, WasmStateResetInspection, WasmStateResetReadiness, WasmStateResetReason,
    WasmStateResetScope,
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
                wasm: None,
            }],
            continuation: Some("one-opaque-position".to_string()),
            wasm_binding: None,
        }),
    }
}

fn reference(text: &str) -> CommandExecutionReference {
    CommandExecutionReference::parse(text).assured("the fixture reference follows the rule")
}

fn generation(value: u64) -> WasmStateGeneration {
    WasmStateGeneration::try_from(value).assured("the fixture generation is positive")
}

/// A WASM processor's state read: one partition unit whose latest reset of one branch is ready,
/// with a confirmed branch and a durable branch that also recorded a recovery.
pub(crate) fn wasm_inspection() -> StateInspection {
    let mut reset = WasmStateReset::publishing(
        reference("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44"),
        WasmStateResetScope::Branch(BranchKeyFingerprint::new([1; 32])),
        WasmStateResetReason::Transaction,
    );
    reset.mark_ready();
    let counts = StateInspectionCounts {
        branches: 3,
        rows: 0,
        bytes: 0,
    };
    StateInspection {
        entity: DomainNodeRef::node_in(
            name("sales"),
            ModelKind::WasmProcessor,
            name::<nervix_models::WasmProcessorName>("counting_guest"),
        )
        .into(),
        result: StateInspectionResult::Page(StateInspectionPage {
            counts,
            unit_count: 1,
            units: vec![StateInspectionUnit {
                unit: Unit::partition(1),
                owner: name("node_a"),
                replicas: vec![name("node_b")],
                origin: StateInspectionOrigin {
                    node: ClusterNodeIdentity::new(name("node_a"), ClusterNodeIncarnation::new(17)),
                    fingerprint: [41; 32],
                    generation: 2,
                    revision: 4,
                    stage: StateInspectionStage::Live,
                },
                counts,
                sections: Vec::new(),
                wasm: Some(WasmUnitStateInspection {
                    default_generation: generation(1),
                    reset: Some(WasmStateResetInspection {
                        reset,
                        generation: generation(2),
                        readiness: WasmStateResetReadiness::Ready,
                    }),
                    entries: 3,
                    checkpoints: WasmCheckpointCounts {
                        total: 2,
                        locally_durable: 1,
                        awaiting_replicas: 1,
                        replica_confirmed: 1,
                        ..WasmCheckpointCounts::default()
                    },
                    recoveries: WasmRecoveryCounts {
                        recovered: 1,
                        ..WasmRecoveryCounts::default()
                    },
                    branches: vec![
                        WasmBranchStateInspection {
                            branch: Some(BranchKeyFingerprint::new([1; 32])),
                            checkpoint: Some(WasmCheckpointState {
                                generation: generation(2),
                                committed_revision: NonZeroU64::new(4),
                                latest_revision: NonZeroU64::new(4),
                                stage: WasmCheckpointStage::ReplicaConfirmed,
                                required_replicas: Some(1),
                                confirmed_replicas: Some(1),
                            }),
                            recovery: None,
                        },
                        WasmBranchStateInspection {
                            branch: Some(BranchKeyFingerprint::new([2; 32])),
                            checkpoint: Some(WasmCheckpointState {
                                generation: generation(1),
                                committed_revision: NonZeroU64::new(3),
                                latest_revision: NonZeroU64::new(4),
                                stage: WasmCheckpointStage::LocallyDurable,
                                required_replicas: Some(2),
                                confirmed_replicas: Some(1),
                            }),
                            recovery: Some(WasmRecoveryInspection {
                                generation: generation(1),
                                rejection: WasmSavedStateRejection::ApplicationState,
                                request: reference("recovery-0001"),
                                outcome: WasmStateRecoveryOutcome::Recovered,
                            }),
                        },
                    ],
                }),
            }],
            continuation: Some("next-wasm-position".to_string()),
            wasm_binding: Some(WasmStateBinding {
                resource: name("wasm_reset_guest"),
                version: 1,
                file: "processors/filter_even.wasm".to_string(),
            }),
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

#[test]
fn a_wasm_state_page_round_trips_every_typed_fact_beside_its_json_and_text() {
    let original = outcome(wasm_inspection());
    let decoded = round_trip_reply(&Reply {
        request_id: request(1),
        body: ReplyBody::Command(Box::new(original.clone())),
    });
    assert_eq!(decoded.body, ReplyBody::Command(Box::new(original.clone())));
    let json: serde_json::Value =
        serde_json::from_str(&original.message).assured("one JSON document");
    assert_eq!(json["wasm_binding"]["resource"], "wasm_reset_guest");
    assert_eq!(json["wasm_binding"]["version"], 1);
    let wasm = &json["units"][0]["wasm"];
    assert_eq!(wasm["default_generation"], 1);
    assert_eq!(wasm["entries"], 3);
    assert_eq!(wasm["reset"]["scope"]["kind"], "branch");
    assert_eq!(wasm["reset"]["scope"]["branch"], "01".repeat(32));
    assert_eq!(wasm["reset"]["phase"], "ready");
    assert_eq!(wasm["reset"]["reason"], "transaction");
    assert_eq!(wasm["reset"]["readiness"], "ready");
    assert_eq!(wasm["checkpoints"]["awaiting_replicas"], 1);
    assert_eq!(wasm["recoveries"]["recovered"], 1);
    assert_eq!(
        wasm["branches"][0]["checkpoint"]["stage"],
        "replica_confirmed"
    );
    assert_eq!(wasm["branches"][0]["recovery"], serde_json::Value::Null);
    assert_eq!(wasm["branches"][1]["checkpoint"]["committed_revision"], 3);
    assert_eq!(
        wasm["branches"][1]["recovery"]["rejection"],
        "application_state"
    );
    assert_eq!(wasm["branches"][1]["recovery"]["outcome"], "recovered");
    let state = original.state.assured("the command inspected state");
    let text = state.display_text().assured("WASM facts render");
    for line in [
        "binding: resource=wasm_reset_guest version=1 file=processors/filter_even.wasm",
        "state default generation: 1\nstate reset: READY, branch, generation 2\nstate reset \
         reason: TRANSACTION\nstate reset readiness: READY",
        "rejected state recoveries: attempted=0 recovered=1 failed=0\nstate structures: 2",
        "checkpoints awaiting replicas: 1\nfailed checkpoints: 0",
        "generation=2 committed_revision=4 latest_revision=4 stage=REPLICA_CONFIRMED \
         required_replicas=1 confirmed_replicas=1",
        "generation=1, application state rejected, recovered",
        "continuation: next-wasm-position",
    ] {
        assert!(text.contains(line), "{line} in {text}");
    }
}

#[test]
fn every_wasm_stage_reset_and_recovery_variant_has_a_lossless_wire_variant() {
    let mut cases = Vec::new();
    for &stage in wasm::ALL_WASM_CHECKPOINT_STAGES {
        for &readiness in wasm::ALL_WASM_RESET_READINESS {
            let mut state = wasm_inspection();
            let StateInspectionResult::Page(page) = &mut state.result else {
                panic!("page");
            };
            let unit = page.units[0].wasm.as_mut().assured("WASM facts");
            unit.branches[0]
                .checkpoint
                .as_mut()
                .assured("a checkpoint")
                .stage = stage;
            unit.reset.as_mut().assured("a reset").readiness = readiness;
            cases.push(state);
        }
    }
    for &reason in wasm::ALL_WASM_RESET_REASONS {
        for scope in [
            WasmStateResetScope::Unbranched,
            WasmStateResetScope::Branch(BranchKeyFingerprint::new([9; 32])),
            WasmStateResetScope::AllBranches,
        ] {
            for &phase in wasm::ALL_WASM_RESET_PHASES {
                let mut state = wasm_inspection();
                let StateInspectionResult::Page(page) = &mut state.result else {
                    panic!("page");
                };
                let unit = page.units[0].wasm.as_mut().assured("WASM facts");
                let mut reset = WasmStateReset::publishing(reference("reset-0002"), scope, reason);
                if phase == nervix_models::WasmStateResetPhase::Ready {
                    reset.mark_ready();
                }
                unit.reset.as_mut().assured("a reset").reset = reset;
                cases.push(state);
            }
        }
    }
    for &outcome_kind in wasm::ALL_WASM_RECOVERY_OUTCOMES {
        for &rejection in wasm::ALL_WASM_SAVED_STATE_REJECTIONS {
            let mut state = wasm_inspection();
            let StateInspectionResult::Page(page) = &mut state.result else {
                panic!("page");
            };
            let unit = page.units[0].wasm.as_mut().assured("WASM facts");
            let recovery = unit.branches[1].recovery.as_mut().assured("a recovery");
            recovery.outcome = outcome_kind;
            recovery.rejection = rejection;
            cases.push(state);
        }
    }
    let mut unbranched = wasm_inspection();
    let StateInspectionResult::Page(page) = &mut unbranched.result else {
        panic!("page");
    };
    let unit = page.units[0].wasm.as_mut().assured("WASM facts");
    unit.reset = None;
    unit.branches.truncate(1);
    unit.branches[0].branch = None;
    unit.branches[0].checkpoint = Some(WasmCheckpointState {
        generation: generation(u64::MAX),
        committed_revision: None,
        latest_revision: None,
        stage: WasmCheckpointStage::Empty,
        required_replicas: None,
        confirmed_replicas: None,
    });
    cases.push(unbranched);
    for state in cases {
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
fn wasm_branch_entries_are_strictly_ordered_bounded_and_carry_state() {
    let mut reordered = wasm_inspection();
    let StateInspectionResult::Page(page) = &mut reordered.result else {
        panic!("page");
    };
    page.units[0]
        .wasm
        .as_mut()
        .assured("WASM facts")
        .branches
        .reverse();
    let mut empty = wasm_inspection();
    let StateInspectionResult::Page(page) = &mut empty.result else {
        panic!("page");
    };
    let unit = page.units[0].wasm.as_mut().assured("WASM facts");
    unit.branches[0].checkpoint = None;
    let mut overcounted = wasm_inspection();
    let StateInspectionResult::Page(page) = &mut overcounted.result else {
        panic!("page");
    };
    page.units[0].wasm.as_mut().assured("WASM facts").entries = 1;
    for state in [reordered, empty, overcounted] {
        let reply = Reply {
            request_id: request(1),
            body: ReplyBody::Command(Box::new(outcome(state))),
        };
        assert!(reply.encode(&SessionLimits::DEFAULT).is_err());
    }
}

#[test]
fn every_largest_wasm_branch_entry_fits_its_encoded_bound() {
    let limits = SessionLimits::DEFAULT;
    let longest = reference(&"r".repeat(128));
    let mut base = wasm_inspection();
    let StateInspectionResult::Page(page) = &mut base.result else {
        panic!("page");
    };
    let unit = page.units[0].wasm.as_mut().assured("WASM facts");
    unit.branches.clear();
    unit.entries = 0;
    // The rendered message is left as the sample's, so only the tables differ between replies.
    let encoded_len = |state: StateInspection| {
        let mut command = command_outcome(CommandDisposition::Completed {
            already_existed: false,
        });
        command.state = Some(Box::new(state));
        let reply = Reply {
            request_id: request(1),
            body: ReplyBody::Command(Box::new(command)),
        };
        let ReplyDelivery::Frame(encoded) = reply.encode(&limits).assured("a bounded reply") else {
            panic!("one frame");
        };
        encoded.into_bytes().len()
    };
    let mut largest = base.clone();
    let StateInspectionResult::Page(page) = &mut largest.result else {
        panic!("page");
    };
    let unit = page.units[0].wasm.as_mut().assured("WASM facts");
    let count = 64_usize;
    for index in 0..count {
        let mut fingerprint = [0xff; 32];
        fingerprint[..8].copy_from_slice(
            &u64::try_from(index)
                .assured("an index fits u64")
                .to_be_bytes(),
        );
        unit.branches.push(WasmBranchStateInspection {
            branch: Some(BranchKeyFingerprint::new(fingerprint)),
            checkpoint: Some(WasmCheckpointState {
                generation: generation(u64::MAX),
                committed_revision: Some(NonZeroU64::MAX),
                latest_revision: Some(NonZeroU64::MAX),
                stage: WasmCheckpointStage::LocallyDurable,
                required_replicas: Some(u32::MAX),
                confirmed_replicas: Some(u32::MAX),
            }),
            recovery: Some(WasmRecoveryInspection {
                generation: generation(u64::MAX),
                rejection: WasmSavedStateRejection::SnapshotEnvelope,
                request: longest.clone(),
                outcome: WasmStateRecoveryOutcome::Attempted,
            }),
        });
    }
    unit.entries = count;
    let empty = encoded_len(base);
    let full = encoded_len(largest);
    let table_bytes = full - empty;
    assert!(
        table_bytes <= count * MAX_ENCODED_WASM_BRANCH_ENTRY_BYTES,
        "{count} largest entries took {table_bytes} bytes"
    );
}

/// One hand-built `WasmUnitStateFacts` table. A peer can send what the encoder never writes, such
/// as a zero generation or entries out of branch order, so the decoder refuses it on its own.
struct CraftedUnit {
    default_generation: u64,
    reset: Option<CraftedReset>,
    entries: u64,
    branches: Vec<CraftedEntry>,
}

struct CraftedReset {
    generation: u64,
    scope: crate::wire::WasmStateScope,
}

struct CraftedEntry {
    branch: u8,
    checkpoint: Option<CraftedCheckpoint>,
    recovery_generation: Option<u64>,
}

struct CraftedCheckpoint {
    generation: u64,
    committed_revision: Option<u64>,
}

impl CraftedUnit {
    /// Two ordered branch entries with a checkpoint and a recovery, and a ready unbranched reset.
    fn valid() -> Self {
        Self {
            default_generation: 1,
            reset: Some(CraftedReset {
                generation: 2,
                scope: crate::wire::WasmStateScope::WasmUnbranchedScope,
            }),
            entries: 2,
            branches: vec![
                CraftedEntry {
                    branch: 1,
                    checkpoint: Some(CraftedCheckpoint {
                        generation: 2,
                        committed_revision: Some(3),
                    }),
                    recovery_generation: None,
                },
                CraftedEntry {
                    branch: 2,
                    checkpoint: None,
                    recovery_generation: Some(1),
                },
            ],
        }
    }

    fn decode(
        &self,
    ) -> Result<WasmUnitStateInspection, error_stack::Report<crate::WireDecodeError>> {
        use crate::wire;
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let reset = match &self.reset {
            Some(crafted) => {
                let request = fbb.create_string("reset-0003");
                let scope =
                    wire::WasmUnbranchedScope::create(&mut fbb, &wire::WasmUnbranchedScopeArgs {});
                Some(wire::WasmResetFacts::create(
                    &mut fbb,
                    &wire::WasmResetFactsArgs {
                        request: Some(request),
                        scope_type: crafted.scope,
                        scope: Some(scope.as_union_value()),
                        phase: Some(wire::WasmResetPhase::Ready),
                        reason: Some(wire::WasmResetReason::Operator),
                        generation: crafted.generation,
                        readiness: Some(wire::WasmResetReadiness::Ready),
                    },
                ))
            }
            None => None,
        };
        let mut entries = Vec::new();
        for crafted in &self.branches {
            let bytes = fbb.create_vector(&[crafted.branch; 32]);
            let branch =
                wire::Fingerprint::create(&mut fbb, &wire::FingerprintArgs { bytes: Some(bytes) });
            let checkpoint = crafted.checkpoint.as_ref().map(|checkpoint| {
                wire::WasmCheckpointFacts::create(
                    &mut fbb,
                    &wire::WasmCheckpointFactsArgs {
                        generation: checkpoint.generation,
                        committed_revision: checkpoint.committed_revision,
                        latest_revision: checkpoint.committed_revision,
                        stage: Some(wire::WasmCheckpointStage::ReplicaConfirmed),
                        required_replicas: Some(1),
                        confirmed_replicas: Some(1),
                    },
                )
            });
            let recovery = match crafted.recovery_generation {
                Some(generation) => {
                    let request = fbb.create_string("recovery-0003");
                    Some(wire::WasmRecoveryFacts::create(
                        &mut fbb,
                        &wire::WasmRecoveryFactsArgs {
                            generation,
                            rejection: Some(wire::WasmSavedStateRejection::ApplicationState),
                            request: Some(request),
                            outcome: Some(wire::WasmRecoveryOutcome::Recovered),
                        },
                    ))
                }
                None => None,
            };
            entries.push(wire::WasmBranchStateFacts::create(
                &mut fbb,
                &wire::WasmBranchStateFactsArgs {
                    branch: Some(branch),
                    checkpoint,
                    recovery,
                },
            ));
        }
        let branches = fbb.create_vector(&entries);
        let checkpoints = wire::WasmCheckpointCounts::create(
            &mut fbb,
            &wire::WasmCheckpointCountsArgs {
                total: 1,
                empty: 0,
                captured: 0,
                locally_durable: 0,
                awaiting_replicas: 0,
                replica_confirmed: 1,
                failed: 0,
            },
        );
        let recoveries = wire::WasmRecoveryCounts::create(
            &mut fbb,
            &wire::WasmRecoveryCountsArgs {
                attempted: 0,
                recovered: 1,
                failed: 0,
            },
        );
        let unit = wire::WasmUnitStateFacts::create(
            &mut fbb,
            &wire::WasmUnitStateFactsArgs {
                default_generation: self.default_generation,
                reset,
                entries: self.entries,
                checkpoints: Some(checkpoints),
                recoveries: Some(recoveries),
                branches: Some(branches),
            },
        );
        fbb.finish_minimal(unit);
        let table = flatbuffers::root::<wire::WasmUnitStateFacts>(fbb.finished_data())
            .assured("every table above is complete, so the buffer verifies");
        super::wasm::decode_unit(crate::codec::Decoder::new(&SessionLimits::DEFAULT), table)
    }

    fn refusal(&self) -> crate::WireDecodeError {
        match self.decode() {
            Ok(decoded) => panic!("a malformed unit decoded: {decoded:?}"),
            Err(error) => error.current_context().clone(),
        }
    }
}

#[test]
fn a_peer_wasm_unit_table_is_refused_for_what_the_encoder_never_writes() {
    let decoded = CraftedUnit::valid()
        .decode()
        .assured("the well-formed hand-built unit decodes");
    assert_eq!(decoded.entries, 2);
    assert_eq!(decoded.branches.len(), 2);
    assert!(decoded.reset.is_some());

    let generation = |field| crate::WireDecodeError::InvalidValue {
        field,
        kind: "nonzero WASM state generation",
    };
    let zero_default = CraftedUnit {
        default_generation: 0,
        ..CraftedUnit::valid()
    };
    assert_eq!(
        zero_default.refusal(),
        generation("WasmUnitStateFacts.default_generation")
    );
    let mut zero_reset = CraftedUnit::valid();
    if let Some(reset) = &mut zero_reset.reset {
        reset.generation = 0;
    }
    assert_eq!(
        zero_reset.refusal(),
        generation("WasmResetFacts.generation")
    );
    let mut undeclared_scope = CraftedUnit::valid();
    if let Some(reset) = &mut undeclared_scope.reset {
        reset.scope = crate::wire::WasmStateScope(9);
    }
    assert_eq!(
        undeclared_scope.refusal(),
        crate::WireDecodeError::UnknownUnionVariant {
            field: "WasmResetFacts.scope",
            discriminant: 9,
        }
    );
    let mut zero_checkpoint = CraftedUnit::valid();
    zero_checkpoint.branches[0].checkpoint = Some(CraftedCheckpoint {
        generation: 0,
        committed_revision: Some(3),
    });
    assert_eq!(
        zero_checkpoint.refusal(),
        generation("WasmCheckpointFacts.generation")
    );
    let mut zero_revision = CraftedUnit::valid();
    zero_revision.branches[0].checkpoint = Some(CraftedCheckpoint {
        generation: 2,
        committed_revision: Some(0),
    });
    assert_eq!(
        zero_revision.refusal(),
        crate::WireDecodeError::ZeroValue {
            field: "WasmCheckpointFacts.committed_revision",
        }
    );
    let mut zero_recovery = CraftedUnit::valid();
    zero_recovery.branches[1].recovery_generation = Some(0);
    assert_eq!(
        zero_recovery.refusal(),
        generation("WasmRecoveryFacts.generation")
    );

    let mut unordered = CraftedUnit::valid();
    unordered.branches.reverse();
    let mut stateless = CraftedUnit::valid();
    stateless.branches[1].recovery_generation = None;
    let overcounted = CraftedUnit {
        entries: 1,
        ..CraftedUnit::valid()
    };
    for (case, unit) in [
        ("unordered", unordered),
        ("stateless", stateless),
        ("overcounted", overcounted),
    ] {
        let refusal = unit.refusal();
        assert!(
            matches!(
                refusal,
                crate::WireDecodeError::InvalidValue {
                    field: "WasmUnitStateFacts.branches",
                    ..
                }
            ),
            "{case}: {refusal:?}"
        );
    }
}

#[test]
fn a_checkpoint_without_a_durability_boundary_renders_its_replica_counts_as_unknown() {
    let state = WasmUnitStateInspection {
        default_generation: generation(1),
        reset: None,
        entries: 1,
        checkpoints: WasmCheckpointCounts {
            total: 1,
            empty: 1,
            captured: 0,
            locally_durable: 0,
            awaiting_replicas: 0,
            replica_confirmed: 0,
            failed: 0,
        },
        recoveries: WasmRecoveryCounts {
            attempted: 0,
            recovered: 0,
            failed: 0,
        },
        branches: vec![WasmBranchStateInspection {
            branch: None,
            checkpoint: Some(WasmCheckpointState {
                generation: generation(1),
                committed_revision: None,
                latest_revision: None,
                stage: WasmCheckpointStage::Empty,
                required_replicas: None,
                confirmed_replicas: None,
            }),
            recovery: None,
        }],
    };
    assert_eq!(
        super::wasm::entry_lines(&state),
        vec![
            "checkpoint branch=unbranched generation=1 committed_revision=none \
             latest_revision=none stage=EMPTY required_replicas=unknown confirmed_replicas=unknown"
                .to_string()
        ]
    );
}
