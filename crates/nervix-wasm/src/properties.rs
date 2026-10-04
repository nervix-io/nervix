//! The native host's complete guest-message conversions.
//!
//! Layer: test harness.
//! - **Owns.** Exact envelope equality and the explicitly projected ABI schema contract.
//! - **Depends on.** Production host conversions, vocabulary and guest message generators.
//! - **Must not know.** Guest callbacks, runtime scheduling or durable checkpoint publication.

use nervix_arbitrary::{Arbitrary, Domain, WasmValues};

use super::*;

fn assert_type(model: &ParseAsType, actual: &protocol::ProcessorType) {
    use protocol::ProcessorType as P;
    match (model, actual) {
        (ParseAsType::U8, P::U8)
        | (ParseAsType::I8, P::I8)
        | (ParseAsType::U16, P::U16)
        | (ParseAsType::I16, P::I16)
        | (ParseAsType::U32, P::U32)
        | (ParseAsType::I32, P::I32)
        | (ParseAsType::U64, P::U64)
        | (ParseAsType::I64, P::I64)
        | (ParseAsType::Bool, P::Bool)
        | (ParseAsType::String, P::String)
        | (ParseAsType::Bytes, P::Bytes)
        | (ParseAsType::Datetime, P::Datetime)
        | (ParseAsType::F32, P::F32)
        | (ParseAsType::F64, P::F64) => {}
        (
            ParseAsType::Array { element, len },
            P::Array {
                element: decoded,
                len: actual,
            },
        ) => {
            assert_eq!(len.get(), *actual);
            assert_type(element, decoded);
        }
        (ParseAsType::Vec { element }, P::Vec { element: decoded }) => {
            assert_type(element, decoded)
        }
        _ => panic!("the ABI schema preserves every vocabulary type exactly"),
    }
}

#[test]
fn bolero_host_schema_preserves_the_complete_abi_projection() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let schema = arbitrary.create_schema();
            let host = WasmProcessorSchema::from(&schema);
            let mut values = WasmValues::new(bytes);
            let host_init = WasmBranchInit {
                domain_name: arbitrary.name_text(),
                domain_type: "runtime".into(),
                branch_key: values.branch_key(),
                input_schema: host.clone(),
                output_schemas: vec![
                    host.clone(),
                    WasmProcessorSchema::from(&arbitrary.create_schema()),
                ],
            };
            let protocol = host_init.to_protocol();
            let decoded = protocol::BranchInit::decode(&protocol.encode())
                .assured("valid host init verifies");
            assert_eq!(decoded, protocol);
            assert_eq!(decoded.input_schema.name, schema.name.as_str());
            assert_eq!(decoded.input_schema.fields.len(), schema.fields.len());
            for (original, decoded) in schema.fields.iter().zip(&decoded.input_schema.fields) {
                assert_eq!(decoded.name, original.name.as_str());
                assert_eq!(decoded.optional, original.optional);
                assert_type(&original.ty, &decoded.ty);
            }
            // Sensitivity remains a host execution rule. The ABI projects name/type/nullability.
            let mut promoted = schema.clone();
            for field in &mut promoted.fields {
                field.sensitive = true;
            }
            assert_eq!(WasmProcessorSchema::from(&promoted), host);
            let mut builder = FlatBufferBuilder::with_capacity(1);
            protocol.encode_in(&mut builder);
            assert_eq!(builder.finished_data(), protocol.encode());
        });
}

#[test]
fn bolero_host_envelopes_preserve_views_owned_bytes_and_sidecars() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut values = WasmValues::new(bytes);
            for output in [false, true] {
                let original = values.envelope(output);
                let host = match &original {
                    protocol::Envelope::Input {
                        arrow_ipc_batch,
                        acks,
                    } => WasmEnvelope::input(
                        arrow_ipc_batch.clone(),
                        WasmAckSidecar::from_protocol(acks.clone()),
                    ),
                    protocol::Envelope::Output {
                        generated_arrow_ipc_batch,
                        outputs,
                    } => WasmEnvelope::output(
                        generated_arrow_ipc_batch.clone(),
                        outputs
                            .iter()
                            .cloned()
                            .map(WasmRoutedOutput::from_protocol)
                            .collect(),
                    ),
                };
                let encoded = host.encode().assured("valid host envelopes encode");
                assert_eq!(
                    protocol::Envelope::decode(&encoded).assured("the protocol decoder succeeds"),
                    original
                );
                let view =
                    WasmEnvelope::decode_borrowed(&encoded).assured("the host view verifies");
                assert_eq!(
                    view.to_owned().assured("valid sidecars become owned"),
                    original
                );
                let retained = WasmEnvelope::decode_owned(encoded)
                    .assured("the host retains its output bytes");
                assert_eq!(retained, host);
                let clone = retained.clone();
                drop(retained);
                assert_eq!(clone, host);
                assert_eq!(
                    protocol::Envelope::decode(&clone.encode().assured("retained bytes encode"))
                        .assured("retained bytes verify"),
                    original
                );
            }
        });
}
