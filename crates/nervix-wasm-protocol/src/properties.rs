//! Bounded checks of the current host/guest message boundary.
//!
//! Layer: test harness.
//! - **Owns.** Current message cases and complete conversion or typed rejection assertions.
//! - **Depends on.** The production protocol and its verified decoders.
//! - **Must not know.** Guest execution, live acknowledgements or checkpoint publication.

use nervix_arbitrary::WasmValues;

use super::*;

fn assert_protocol_failure(bytes: &[u8]) {
    assert!(BranchInit::decode(bytes).is_err());
    assert!(Envelope::decode(bytes).is_err());
    assert!(GuestSnapshot::decode(bytes).is_err());
}

#[test]
fn bolero_corrupt_messages_validate_sizes_offsets_and_counts() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut values = WasmValues::new(bytes);
            for original in [
                values.branch_init().encode(),
                values.envelope(false).encode(),
                values.envelope(true).encode(),
                values.snapshot().encode(),
            ] {
                let length = original.len();
                let cut = values.entropy().count(length - 1);
                assert_protocol_failure(&original[..cut]);
                let mut wrong_size = original.clone();
                wrong_size[..4].copy_from_slice(&u32::MAX.to_le_bytes());
                assert_protocol_failure(&wrong_size);
                let mut trailing = original.clone();
                trailing.push(0);
                assert_protocol_failure(&trailing);
                let mut identifier = original.clone();
                identifier[8] ^= 255;
                assert_protocol_failure(&identifier);
                let mut offset = original.clone();
                offset[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
                assert_protocol_failure(&offset);
                let message =
                    verified_message(&original).assured("the unmodified message verifies");
                let vector = if let Some(init) = message.payload_as_branch_init() {
                    init.input_schema().fields().bytes()
                } else if let Some(input) = message.payload_as_input_envelope() {
                    input.arrow_ipc_batch().bytes()
                } else if let Some(output) = message.payload_as_output_envelope() {
                    output.generated_arrow_ipc_batch().bytes()
                } else {
                    message
                        .payload_as_guest_snapshot()
                        .assured("the fourth family is a snapshot")
                        .application_state()
                        .bytes()
                };
                let count_offset = vector
                    .as_ptr()
                    .addr()
                    .checked_sub(original.as_ptr().addr())
                    .assured("a verified vector points into the original frame")
                    .checked_sub(4)
                    .assured("a FlatBuffers vector has a four-byte length prefix");
                let mut count = original.clone();
                count[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
                assert_protocol_failure(&count);
            }
        });
}

fn encoded_type(kind: u8) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let ty = wire::ProcessorType::create(
        &mut builder,
        &wire::ProcessorTypeArgs {
            kind: wire::ProcessorTypeKind(kind),
            element: None,
            array_len: 0,
        },
    );
    let name = builder.create_string("field");
    let field = wire::ProcessorField::create(
        &mut builder,
        &wire::ProcessorFieldArgs {
            name: Some(name),
            type_: Some(ty),
            optional: true,
        },
    );
    let fields = builder.create_vector(&[field]);
    let name = builder.create_string("schema");
    let schema = wire::ProcessorSchema::create(
        &mut builder,
        &wire::ProcessorSchemaArgs {
            name: Some(name),
            fields: Some(fields),
        },
    );
    let domain = builder.create_string("domain");
    let outputs = builder.create_vector::<WIPOffset<wire::ProcessorSchema<'_>>>(&[]);
    let init = wire::BranchInit::create(
        &mut builder,
        &wire::BranchInitArgs {
            domain_name: Some(domain),
            domain_type: Some(domain),
            branch_key: None,
            input_schema: Some(schema),
            output_schemas: Some(outputs),
        },
    );
    finish_message(
        &mut builder,
        wire::MessagePayload::BranchInit,
        init.as_union_value(),
    );
    builder.finished_data().to_vec()
}

fn encoded_column(source: u8, index: u32) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let column = wire::OutputColumnRef::create(
        &mut builder,
        &wire::OutputColumnRefArgs {
            source: wire::ColumnSource(source),
            column_index: index,
        },
    );
    let columns = builder.create_vector(&[column]);
    let name = builder.create_string("output");
    let acks = build_ack_sidecar(&mut builder, &AckSidecar::default());
    let route = wire::RoutedOutput::create(
        &mut builder,
        &wire::RoutedOutputArgs {
            output_relay: Some(name),
            columns: Some(columns),
            acks: Some(acks),
        },
    );
    let outputs = builder.create_vector(&[route]);
    let ipc = builder.create_vector::<u8>(&[]);
    let output = wire::OutputEnvelope::create(
        &mut builder,
        &wire::OutputEnvelopeArgs {
            generated_arrow_ipc_batch: Some(ipc),
            outputs: Some(outputs),
        },
    );
    finish_message(
        &mut builder,
        wire::MessagePayload::OutputEnvelope,
        output.as_union_value(),
    );
    builder.finished_data().to_vec()
}

#[test]
fn bolero_message_tags_and_column_descriptors_fail_typed() {
    bolero::check!().with_iterations(256).with_max_len(64).for_each(|bytes| {
        let mut values = WasmValues::new(bytes);
        let tag = values.entropy().byte();
        for kind in [tag,13,14,16,255] {
            let encoded = encoded_type(kind);
            match BranchInit::decode(&encoded) {
                Ok(init) => {
                    assert!(kind<=15 && kind!=13 && kind!=14);
                    assert_eq!(BranchInit::decode(&init.encode()).assured("an accepted scalar reencodes"),init);
                }
                Err(error) => match kind {
                    13|14 => assert!(matches!(error.current_context(),ProtocolError::MissingElementType { .. })),
                    _ => assert!(matches!(error.current_context(),ProtocolError::UnknownEnum { kind:"processor type",value } if *value==kind)),
                },
            }
        }
        for source in [tag,0,1,2,3,255] {
            let index = u32::try_from(values.entropy().boundary_biased(1..=u64::from(u32::MAX)))
                .verified("a positive column index fits u32");
            let encoded = encoded_column(source,index);
            let view = EnvelopeRef::decode(&encoded).assured("column tags are checked at the view-to-owned boundary");
            match view.to_owned() {
                Ok(envelope) => {
                    assert!(source<2);
                    assert_eq!(Envelope::decode(&envelope.encode()).assured("accepted references reencode"),envelope);
                }
                Err(error) => {
                    if source==2 {
                        assert!(matches!(error.current_context(),ProtocolError::InvalidUninitializedColumnIndex { column_index } if *column_index==index));
                    } else {
                        assert!(matches!(error.current_context(),ProtocolError::UnknownEnum { kind:"output column source",value } if *value==source));
                    }
                }
            }
        }
        for tag in [0,5,255,tag] {
            let tag = match tag { 1..=4 => tag+4,other => other };
            let mut builder = FlatBufferBuilder::new();
            let init = build_branch_init(&mut builder,&BranchInit {
                domain_name:"domain".into(),domain_type:"runtime".into(),branch_key:None,
                input_schema:ProcessorSchema { name:"input".into(),fields:vec![] },output_schemas:vec![],
            });
            finish_message(&mut builder,wire::MessagePayload(tag),init.as_union_value());
            assert_protocol_failure(builder.finished_data());
        }
    });
}

#[test]
fn bolero_malformed_messages_fail_typed_within_bounds() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes| {
            // A complete size prefix is insufficient to hold a root and its identifier.
            // Sweep every short length rather than relying on random bytes matching the prefix.
            for length in 4..12 {
                let mut short = vec![0; length];
                short[..4].copy_from_slice(
                    &u32::try_from(length - 4)
                        .assured("a short frame length fits u32")
                        .to_le_bytes(),
                );
                assert_protocol_failure(&short);
            }
            for input in [bytes, &[]] {
                match BranchInit::decode(input) {
                    Ok(value) => assert_eq!(
                        BranchInit::decode(&value.encode()).assured("accepted metadata reencodes"),
                        value
                    ),
                    Err(error) => assert!(matches!(
                        error.current_context(),
                        ProtocolError::LengthMismatch { .. }
                            | ProtocolError::InvalidIdentifier
                            | ProtocolError::InvalidFlatbuffer(_)
                            | ProtocolError::UnexpectedPayload { .. }
                            | ProtocolError::UnknownEnum { .. }
                            | ProtocolError::MissingElementType { .. }
                    )),
                }
                match GuestSnapshot::decode(input) {
                    Ok(value) => assert_eq!(
                        GuestSnapshot::decode(&value.encode())
                            .assured("accepted snapshot reencodes"),
                        value
                    ),
                    Err(error) => assert!(matches!(
                        error.current_context(),
                        ProtocolError::LengthMismatch { .. }
                            | ProtocolError::InvalidIdentifier
                            | ProtocolError::InvalidFlatbuffer(_)
                            | ProtocolError::UnexpectedPayload { .. }
                    )),
                }
                match EnvelopeRef::decode(input) {
                    Ok(view) => match view.to_owned() {
                        Ok(value) => assert_eq!(
                            Envelope::decode(input).assured("the verified owned value decoded"),
                            value
                        ),
                        Err(error) => assert!(matches!(
                            error.current_context(),
                            ProtocolError::UnknownEnum { .. }
                                | ProtocolError::InvalidUninitializedColumnIndex { .. }
                        )),
                    },
                    Err(error) => assert!(matches!(
                        error.current_context(),
                        ProtocolError::LengthMismatch { .. }
                            | ProtocolError::InvalidIdentifier
                            | ProtocolError::InvalidFlatbuffer(_)
                            | ProtocolError::UnexpectedPayload { .. }
                    )),
                }
            }
        });
}
