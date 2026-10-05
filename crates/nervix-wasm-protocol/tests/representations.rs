//! Bounded checks of the current host/guest message boundary.
//!
//! Layer: test harness.
//! - **Owns.** Current message cases and complete conversion or typed rejection assertions.
//! - **Depends on.** The production protocol and its verified decoders.
//! - **Must not know.** Guest execution, live acknowledgements or checkpoint publication.

use flatbuffers::FlatBufferBuilder;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::WasmValues;
use nervix_wasm_protocol::*;

#[test]
fn bolero_branch_metadata_preserves_every_type_and_field() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut values = WasmValues::new(bytes);
            let mut init = values.branch_init();
            // Every run reaches the complete scalar vocabulary and both nested collection kinds.
            for kind in 0..14 {
                let scalar = WasmValues::scalar(kind);
                for ty in [
                    scalar.clone(),
                    ProcessorType::Vec {
                        element: Box::new(ProcessorType::Array {
                            element: Box::new(scalar),
                            len: u32::MAX,
                        }),
                    },
                ] {
                    init.input_schema.fields.push(ProcessorField {
                        name: format!("covered_{}", init.input_schema.fields.len()),
                        ty,
                        optional: values.entropy().flag(),
                    });
                }
            }
            for branch_key in [
                None,
                values.branch_key(),
                Some(b"{\"tenant\":\"alpha\"}".to_vec()),
            ] {
                init.branch_key = branch_key;
                let encoded = init.encode();
                assert_eq!(
                    BranchInit::decode(&encoded).assured("valid init metadata decodes"),
                    init
                );
                let mut builder = FlatBufferBuilder::with_capacity(1);
                init.encode_in(&mut builder);
                assert_eq!(builder.finished_data(), encoded);
            }
        });
}

fn assert_envelope(envelope: &Envelope) {
    let encoded = envelope.encode();
    let view = EnvelopeRef::decode(&encoded).assured("a valid envelope verifies");
    let borrowed = match (envelope, view) {
        (
            Envelope::Input {
                arrow_ipc_batch,
                acks,
            },
            EnvelopeRef::Input(input),
        ) => {
            assert_eq!(input.arrow_ipc_batch(), arrow_ipc_batch);
            assert_eq!(input.acks(), *acks);
            input.arrow_ipc_batch()
        }
        (
            Envelope::Output {
                generated_arrow_ipc_batch,
                outputs,
            },
            EnvelopeRef::Output(output),
        ) => {
            assert_eq!(
                output.generated_arrow_ipc_batch(),
                generated_arrow_ipc_batch
            );
            assert_eq!(
                output.outputs().assured("valid column references decode"),
                *outputs
            );
            output.generated_arrow_ipc_batch()
        }
        _ => panic!("an encoded envelope preserves its input/output variant"),
    };
    let start = encoded.as_ptr().addr();
    let end = start
        .checked_add(encoded.len())
        .assured("the allocation fits its address range");
    let borrowed_start = borrowed.as_ptr().addr();
    let borrowed_end = borrowed_start
        .checked_add(borrowed.len())
        .assured("a subslice fits its allocation");
    assert!(start <= borrowed_start && borrowed_end <= end);
    let owned = view
        .to_owned()
        .assured("a valid borrowed view becomes owned");
    assert_eq!(owned, *envelope);
    assert_eq!(
        Envelope::decode(&encoded).assured("the direct owned decoder succeeds"),
        *envelope
    );
    let mut builder = FlatBufferBuilder::with_capacity(1);
    envelope.encode_in(&mut builder);
    assert_eq!(builder.finished_data(), encoded);
    drop(encoded);
    assert_eq!(owned, *envelope);
}

#[test]
fn bolero_envelopes_preserve_borrowed_bytes_sidecars_and_order() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut values = WasmValues::new(bytes);
            for output in [false, true] {
                assert_envelope(&values.envelope(output));
            }
            let acks = AckSidecar {
                rows: vec![
                    OutputRow {
                        tokens: vec![],
                        source_token: None,
                    },
                    OutputRow {
                        tokens: vec![AckToken(0), AckToken(u64::MAX), AckToken(0)],
                        source_token: Some(AckToken(0)),
                    },
                ],
                acked: vec![AckTokenSet {
                    tokens: vec![AckToken(u64::MAX), AckToken(0)],
                }],
                nacked: vec![NackSet {
                    tokens: vec![AckToken(1)],
                    reason: "retry\0世界".into(),
                }],
                message_errors: vec![MessageErrorSet {
                    tokens: vec![],
                    reason: String::new(),
                }],
            };
            assert_envelope(&Envelope::Input {
                arrow_ipc_batch: vec![],
                acks: acks.clone(),
            });
            assert_envelope(&Envelope::Output {
                generated_arrow_ipc_batch: vec![0, 255],
                outputs: vec![
                    RoutedOutput {
                        output_relay: "first".into(),
                        columns: vec![
                            OutputColumnRef::Input {
                                column_index: u32::MAX,
                            },
                            OutputColumnRef::Generated { column_index: 0 },
                            OutputColumnRef::Uninitialized,
                        ],
                        acks: acks.clone(),
                    },
                    RoutedOutput {
                        output_relay: "second".into(),
                        columns: vec![],
                        acks,
                    },
                ],
            });
        });
}

#[test]
fn bolero_snapshots_preserve_opaque_state_and_init_bytes() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut values = WasmValues::new(bytes);
            let snapshot = values.snapshot();
            for original in [
                snapshot,
                GuestSnapshot {
                    init_metadata: values.bytes(),
                    application_state: vec![],
                },
            ] {
                let encoded = original.encode();
                assert_eq!(
                    GuestSnapshot::decode(&encoded).assured("a valid snapshot decodes"),
                    original
                );
                let mut builder = FlatBufferBuilder::with_capacity(1);
                original.encode_in(&mut builder);
                assert_eq!(builder.finished_data(), encoded);
            }
        });
}

#[test]
fn bolero_reset_and_rejection_codes_preserve_typed_distinctions() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(16)
        .for_each(|bytes| {
            let mut values = WasmValues::new(bytes);
            let code = u32::try_from(values.entropy().up_to(u64::from(u32::MAX)))
                .verified("the range ends at u32::MAX")
                .cast_signed();
            for code in [code, i32::MIN, i32::MAX, -9, -8, -7, -6, 0, 1] {
                let rejection = match code {
                    -7 => Some(SavedStateRejection::SnapshotEnvelope),
                    -8 => Some(SavedStateRejection::ApplicationState),
                    _ => None,
                };
                let answer = match code {
                    0 => Some(StateResetRequestAnswer::Accepted),
                    -9 => Some(StateResetRequestAnswer::Refused),
                    _ => None,
                };
                assert_eq!(SavedStateRejection::from_code(code), rejection);
                assert_eq!(StateResetRequestAnswer::from_code(code), answer);
                if let Some(rejection) = rejection {
                    assert_eq!(rejection.code(), code);
                }
                if let Some(answer) = answer {
                    assert_eq!(answer.code(), code);
                }
            }
        });
}
