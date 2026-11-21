//! The native host's complete guest-message conversions.
//!
//! Layer: test harness.
//! - **Owns.** Exact envelope equality, the explicitly projected ABI schema contract and the
//!   state descriptions a guest sends back.
//! - **Depends on.** Production host conversions, vocabulary and guest message generators.
//! - **Must not know.** Guest callbacks, runtime scheduling or durable checkpoint publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

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
            // A schema a guest sends back, as a state description section does, is the host schema
            // it was projected from, and every declared type names the exact Nervix type again.
            assert_eq!(
                WasmProcessorSchema::from_protocol(decoded.input_schema.clone()),
                host
            );
            for (original, projected) in schema.fields.iter().zip(&host.fields) {
                assert_eq!(
                    ParseAsType::try_from(&projected.ty)
                        .assured("a projected type names a Nervix type"),
                    original.ty
                );
            }
            let mut builder = FlatBufferBuilder::with_capacity(1);
            protocol.encode_in(&mut builder);
            assert_eq!(builder.finished_data(), protocol.encode());
        });
}

/// Whether `ty` declares, at any depth, an ARRAY of zero elements, which no Nervix type is.
fn declares_an_empty_array(ty: &WasmProcessorType) -> bool {
    match ty {
        WasmProcessorType::Array { element, len } => *len == 0 || declares_an_empty_array(element),
        WasmProcessorType::Vec { element } => declares_an_empty_array(element),
        WasmProcessorType::U8
        | WasmProcessorType::I8
        | WasmProcessorType::U16
        | WasmProcessorType::I16
        | WasmProcessorType::U32
        | WasmProcessorType::I32
        | WasmProcessorType::U64
        | WasmProcessorType::I64
        | WasmProcessorType::Bool
        | WasmProcessorType::String
        | WasmProcessorType::Bytes
        | WasmProcessorType::Datetime
        | WasmProcessorType::F32
        | WasmProcessorType::F64 => false,
    }
}

#[test]
fn bolero_host_state_descriptions_preserve_sections_and_refuse_empty_arrays() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut values = WasmValues::new(bytes);
            let original = values.state_description();
            let decoded = protocol::StateDescription::decode(&original.encode())
                .assured("a valid state description verifies");
            let host = WasmStateDescription::from_protocol(decoded);
            assert_eq!(host.sections.len(), original.sections.len());
            for (section, expected) in host.sections.iter().zip(&original.sections) {
                assert_eq!(section.schema.to_protocol(), expected.schema);
                assert_eq!(section.arrow_ipc_batch, expected.arrow_ipc_batch);
                for field in &section.schema.fields {
                    match ParseAsType::try_from(&field.ty) {
                        Ok(parsed) => {
                            assert!(!declares_an_empty_array(&field.ty));
                            assert_eq!(WasmProcessorType::from(&parsed), field.ty);
                        }
                        Err(refusal) => {
                            assert!(matches!(
                                refusal.current_context(),
                                WasmProcessorTypeError::EmptyArray
                            ));
                            assert!(declares_an_empty_array(&field.ty));
                        }
                    }
                }
            }
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
