//! Frame verification: identifiers, size, truncation, corruption, UTF-8, required fields and the
//! traversal limits that bound what a frame can make a receiver do.

use bytes::Bytes;
use error_stack::Report;
use flatbuffers::FlatBufferBuilder;
use meticulous::ResultExt as _;
use nervix_models::{ParseAsType, SchemaField, TransactionInspection, TransactionLifecycle};

use super::{
    fixtures::{checked, frame_error, limits, name, position_of, request, settings, size},
    samples::{
        client_messages, command_outcome, impact_report, rows_frame, subscription, transaction,
    },
};
use crate::{
    CellView, CellWriter, CellsView, ClientFrame, ClientMessage, ClientRequest, CommandDisposition,
    CommandRequest, FrameError, FrameViolation, InspectionOutcome, NoticeLevel, Reply, ReplyBody,
    ReplyDelivery, RowBranch, RowSchema, ServerEvent, ServerFrame, ServerMessage, ServerNotice,
    SessionLimitSettings, SessionLimits, SubscribeDisposition, SubscribeOutcome,
    SubscriptionOpened, SubscriptionRowsEncoder, SubscriptionType, UploadChunk, UploadDisposition,
    UploadFailure, UploadFrame, UploadMessage, UploadReply, UploadReplyFrame, UploadStart,
    VerifiedFrame, WireEncodeError,
    limits::{MAX_NESTING_DEPTH, MIN_NESTING_DEPTH},
    wire,
};

const MARKER: &str = "UTF8MARKER";

fn marked_command() -> ClientMessage {
    ClientMessage {
        request_id: request(7),
        request: ClientRequest::Command(CommandRequest {
            query: format!("SHOW {MARKER};"),
            domain: Some(name("tenant")),
            execution_reference: super::fixtures::reference("marked"),
            expected_transaction_position: None,
            expected_preview: None,
        }),
    }
}

fn client_bytes(message: &ClientMessage) -> Bytes {
    message
        .encode(&limits())
        .assured("the test message fits the default limits")
        .into_bytes()
}

fn command_reply_bytes() -> Bytes {
    let reply = Reply {
        request_id: request(3),
        body: ReplyBody::Command(Box::new(command_outcome(
            CommandDisposition::PreviewStale {
                expected: super::samples::preview(1),
                current: super::samples::preview(2),
            },
        ))),
    };
    reply_bytes(&reply)
}

fn inspection_reply_bytes() -> Bytes {
    let reply = Reply {
        request_id: request(4),
        body: ReplyBody::Inspection(InspectionOutcome::Inspected(Box::new(
            TransactionInspection {
                transaction: transaction(TransactionLifecycle::Committing),
                operation: None,
                report: impact_report(),
            },
        ))),
    };
    reply_bytes(&reply)
}

fn reply_bytes(reply: &Reply) -> Bytes {
    match reply
        .encode(&limits())
        .assured("the test reply fits the default limits")
    {
        ReplyDelivery::Frame(frame) => frame.into_bytes(),
        ReplyDelivery::Transfer(_) => panic!("a test reply below the frame limit fits one frame"),
    }
}

fn upload_reply_bytes() -> Bytes {
    UploadReply {
        request_id: Some(request(9)),
        disposition: UploadDisposition::Failed {
            upload_identity: Some(
                nervix_models::ResourceUploadIdentity::parse("upload-1")
                    .assured("a valid upload identity"),
            ),
            failure: UploadFailure::SizeMismatch,
            assigned_version: Some(super::fixtures::non_zero(4)),
        },
        message: "upload size mismatch".to_string(),
        diagnostics: super::samples::diagnostics(),
    }
    .encode(&limits())
    .assured("the test reply fits the default limits")
    .into_bytes()
}

fn upload_chunk_bytes() -> Bytes {
    UploadChunk::encode(b"archive bytes", &limits())
        .assured("the test chunk fits the default limits")
        .into_bytes()
}

#[test]
fn each_root_carries_its_own_identifier() {
    let client = client_bytes(&marked_command());
    assert_eq!(&client[4..8], b"NXCM");
    assert_eq!(&command_reply_bytes()[4..8], b"NXSM");
    let start = UploadStart {
        request_id: request(1),
        domain: name("tenant"),
        resource: name("model"),
        upload_identity: nervix_models::ResourceUploadIdentity::parse("upload-1")
            .assured("a valid upload identity"),
        total_bytes: super::fixtures::non_zero(1),
    }
    .encode(&limits())
    .assured("the test start fits the default limits");
    assert_eq!(&start.bytes()[4..8], b"NXUM");
    assert_eq!(&upload_reply_bytes()[4..8], b"NXUR");
}

#[test]
fn a_frame_of_another_root_is_refused() {
    let server = command_reply_bytes();
    let error = frame_error(VerifiedFrame::<ClientFrame>::verify(server, &limits()));
    assert_eq!(
        error,
        FrameError::WrongIdentifier {
            root: "ClientMessage",
            expected: "NXCM",
        }
    );
    let client = client_bytes(&marked_command());
    let error = frame_error(VerifiedFrame::<UploadFrame>::verify(client, &limits()));
    assert_eq!(
        error,
        FrameError::WrongIdentifier {
            root: "UploadMessage",
            expected: "NXUM",
        }
    );
}

#[test]
fn bytes_too_short_for_a_header_are_truncated() {
    for length in 0..8 {
        let error = frame_error(VerifiedFrame::<ServerFrame>::verify(
            Bytes::from(vec![0; length]),
            &limits(),
        ));
        assert_eq!(
            error,
            FrameError::Truncated {
                root: "ServerMessage",
                actual: length,
            }
        );
    }
}

#[test]
fn truncation_never_yields_a_different_message() {
    let message = marked_command();
    let bytes = client_bytes(&message);
    for length in 0..bytes.len() {
        let Ok(frame) = VerifiedFrame::<ClientFrame>::verify(bytes.slice(..length), &limits())
        else {
            continue;
        };
        // Only trailing alignment padding is unreferenced, so a truncation that verifies can only
        // have dropped padding, and must read back as the same message.
        assert!(
            length + 8 > bytes.len(),
            "a truncation to {length} of {} bytes verified",
            bytes.len()
        );
        let decoded = ClientMessage::decode(&frame).assured("a verified truncation decodes");
        assert_eq!(decoded, message);
    }
}

/// Reads every part of a decoded server message that is read lazily from its frame.
fn read_every_view(message: &ServerMessage) {
    match message {
        ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) => {
            let batch = rows.batch();
            assert!(!format!("{batch:?}").is_empty());
            if let Some(key) = batch.branch_key() {
                read_cells(key);
            }
            for row in batch.rows() {
                read_cells(row);
            }
        }
        ServerMessage::Event(ServerEvent::DomainSnapshot(snapshot)) => {
            assert!(snapshot.graph_json().len() <= snapshot.frame().len());
        }
        ServerMessage::TransferPart(part) => {
            assert!(!part.chunk().is_empty());
        }
        _ => {}
    }
}

fn read_cells(cells: CellsView<'_>) {
    for cell in cells.iter() {
        if let CellView::List(elements) = cell {
            read_cells(elements);
        }
    }
}

/// Flips bits of every byte of a server frame, and verifies, decodes and reads whatever still
/// verifies. Nothing may panic: a corrupted frame is refused or read as whatever it now says.
fn corrupt_server_frame(bytes: &Bytes, flips: &[u8]) {
    for index in 0..bytes.len() {
        for flip in flips {
            let mut corrupted = bytes.to_vec();
            corrupted[index] ^= flip;
            let Ok(frame) = VerifiedFrame::<ServerFrame>::verify(Bytes::from(corrupted), &limits())
            else {
                continue;
            };
            if let Ok(message) = ServerMessage::decode(&frame) {
                read_every_view(&message);
            }
        }
    }
}

#[test]
fn corrupting_a_command_reply_never_panics() {
    corrupt_server_frame(&command_reply_bytes(), &[0x01, 0x80, 0xFF]);
}

#[test]
fn corrupting_an_inspection_reply_never_panics() {
    corrupt_server_frame(&inspection_reply_bytes(), &[0xFF]);
}

#[test]
fn corrupting_a_row_batch_never_panics() {
    corrupt_server_frame(&rows_frame(&limits()).into_bytes(), &[0x01, 0x80, 0xFF]);
}

#[test]
fn corrupting_client_and_upload_frames_never_panics() {
    let client_frames = client_messages()
        .iter()
        .map(client_bytes)
        .collect::<Vec<_>>();
    for bytes in client_frames {
        for index in 0..bytes.len() {
            let mut corrupted = bytes.to_vec();
            corrupted[index] ^= 0xFF;
            if let Ok(frame) =
                VerifiedFrame::<ClientFrame>::verify(Bytes::from(corrupted), &limits())
            {
                drop(ClientMessage::decode(&frame));
            }
        }
    }
    for bytes in [upload_reply_bytes(), upload_chunk_bytes()] {
        for index in 0..bytes.len() {
            let mut corrupted = bytes.to_vec();
            corrupted[index] ^= 0xFF;
            let corrupted = Bytes::from(corrupted);
            if let Ok(frame) =
                VerifiedFrame::<UploadReplyFrame>::verify(corrupted.clone(), &limits())
            {
                drop(UploadReply::decode(&frame));
            }
            if let Ok(frame) = VerifiedFrame::<UploadFrame>::verify(corrupted, &limits())
                && let Ok(UploadMessage::Chunk(chunk)) = UploadMessage::decode(&frame)
            {
                assert!(!chunk.bytes().is_empty());
            }
        }
    }
}

#[test]
fn the_frame_limit_is_inclusive() {
    let mut message = marked_command();
    if let ClientRequest::Command(command) = &mut message.request {
        command.query = "x".repeat(2048);
    }
    let bytes = client_bytes(&message);
    let length = bytes.len();
    let exact = checked(SessionLimitSettings {
        frame_bytes: size(length),
        ..settings()
    });
    assert!(VerifiedFrame::<ClientFrame>::verify(bytes.clone(), &exact).is_ok());
    let below = checked(SessionLimitSettings {
        frame_bytes: size(length - 1),
        ..settings()
    });
    assert_eq!(
        frame_error(VerifiedFrame::<ClientFrame>::verify(bytes, &below)),
        FrameError::TooLarge {
            root: "ClientMessage",
            actual: length,
            limit: length - 1,
        }
    );
    let error = message
        .encode(&below)
        .expect_err("an encoder refuses a frame its receiver would refuse");
    assert!(matches!(
        error.current_context(),
        WireEncodeError::FrameTooLarge { .. }
    ));
}

fn invalid(violation: FrameViolation, root: &'static str) -> FrameError {
    FrameError::Invalid { root, violation }
}

#[test]
fn invalid_utf8_is_refused() {
    let replacements: [&[u8]; 3] = [&[0xFF], &[0xC0, 0x80], &[0xE2, 0x28, 0xA1]];
    for replacement in replacements {
        let mut bytes = client_bytes(&marked_command()).to_vec();
        let position = position_of(&bytes, MARKER.as_bytes());
        bytes[position..position + replacement.len()].copy_from_slice(replacement);
        let error = frame_error(VerifiedFrame::<ClientFrame>::verify(
            Bytes::from(bytes),
            &limits(),
        ));
        assert_eq!(error, invalid(FrameViolation::InvalidUtf8, "ClientMessage"));
    }
}

#[test]
fn a_string_without_its_null_terminator_is_refused() {
    let mut bytes = client_bytes(&marked_command()).to_vec();
    let marker = format!("SHOW {MARKER};");
    let position = position_of(&bytes, marker.as_bytes());
    bytes[position + marker.len()] = b'!';
    let error = frame_error(VerifiedFrame::<ClientFrame>::verify(
        Bytes::from(bytes),
        &limits(),
    ));
    assert_eq!(
        error,
        invalid(FrameViolation::MissingNullTerminator, "ClientMessage")
    );
}

#[test]
fn an_absent_required_field_is_refused() {
    let mut builder = FlatBufferBuilder::new();
    let table = builder.start_table();
    builder.push_slot_always::<u64>(wire::ClientMessage::VT_REQUEST_ID, 1);
    let root = builder.end_table(table);
    let bytes = super::fixtures::finish_raw(builder, root, "NXCM");
    let error = frame_error(VerifiedFrame::<ClientFrame>::verify(bytes, &limits()));
    assert_eq!(
        error,
        invalid(
            FrameViolation::MissingRequiredField {
                field: "request".into(),
            },
            "ClientMessage",
        )
    );
}

#[test]
fn a_union_discriminant_without_its_member_is_refused() {
    let mut builder = FlatBufferBuilder::new();
    let table = builder.start_table();
    builder.push_slot_always::<u64>(wire::ClientMessage::VT_REQUEST_ID, 1);
    builder.push_slot_always(
        wire::ClientMessage::VT_REQUEST_TYPE,
        wire::ClientRequest::ListDomainsRequest,
    );
    let root = builder.end_table(table);
    let bytes = super::fixtures::finish_raw(builder, root, "NXCM");
    let error = frame_error(VerifiedFrame::<ClientFrame>::verify(bytes, &limits()));
    assert_eq!(
        error,
        invalid(
            FrameViolation::InconsistentUnion {
                field: "request_type".into(),
            },
            "ClientMessage",
        )
    );
}

#[test]
fn a_root_offset_outside_the_frame_is_refused() {
    let mut bytes = client_bytes(&marked_command()).to_vec();
    bytes[0..4].copy_from_slice(&0xFFFF_FFF0_u32.to_le_bytes());
    let error = frame_error(VerifiedFrame::<ClientFrame>::verify(
        Bytes::from(bytes),
        &limits(),
    ));
    assert_eq!(error, invalid(FrameViolation::OutOfBounds, "ClientMessage"));
}

#[test]
fn a_misaligned_root_offset_is_refused() {
    let mut bytes = client_bytes(&marked_command()).to_vec();
    let offset = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    bytes[0..4].copy_from_slice(&(offset + 1).to_le_bytes());
    let error = frame_error(VerifiedFrame::<ClientFrame>::verify(
        Bytes::from(bytes),
        &limits(),
    ));
    assert_eq!(error, invalid(FrameViolation::Misaligned, "ClientMessage"));
}

fn nest(cells: &mut CellWriter<'_, 'static>, depth: usize) -> Result<(), Report<WireEncodeError>> {
    if depth == 0 {
        cells.push_u8(1)?;
        return Ok(());
    }
    cells.push_list(|elements| nest(elements, depth - 1))
}

/// Rows holding one list value nested `levels` deep, encoded under `limits`.
fn nested_rows(levels: usize, limits: &SessionLimits) -> Result<Bytes, Report<WireEncodeError>> {
    let mut batch = SubscriptionRowsEncoder::unbranched(subscription(), limits)?;
    batch.push_row(|cells| nest(cells, levels))?;
    Ok(batch.finish()?.into_bytes())
}

/// A list type nested `levels` deep around `U8`.
fn nested_type(levels: usize) -> ParseAsType {
    let mut ty = ParseAsType::U8;
    for _ in 0..levels {
        ty = ParseAsType::Vec {
            element: Box::new(ty),
        };
    }
    ty
}

/// A subscribe reply whose schema holds one field of a list type nested `levels` deep, in the
/// row fields or, when `in_branch`, in the branch key fields.
fn nested_schema_reply(
    levels: usize,
    in_branch: bool,
    limits: &SessionLimits,
) -> Result<Bytes, Report<WireEncodeError>> {
    let field = SchemaField {
        name: name("value"),
        ty: nested_type(levels),
        optional: false,
        sensitive: false,
    };
    let schema = if in_branch {
        let branch =
            RowBranch::new(name("tenants"), vec![field]).assured("the branch key has a field");
        RowSchema {
            fields: Vec::new(),
            branch: Some(branch),
        }
    } else {
        RowSchema {
            fields: vec![field],
            branch: None,
        }
    };
    let reply = Reply {
        request_id: request(1),
        body: ReplyBody::Subscribe(SubscribeOutcome {
            disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
                subscription: subscription(),
                domain: name("tenant"),
                relay: name("orders"),
                subscription_type: SubscriptionType::Row,
                schema,
            })),
            message: String::new(),
            diagnostics: Vec::new(),
        }),
    };
    let ReplyDelivery::Frame(frame) = reply.encode(limits)? else {
        panic!("a one-field schema fits one frame");
    };
    Ok(frame.into_bytes())
}

fn nesting_limits(depth: usize) -> SessionLimits {
    checked(SessionLimitSettings {
        nesting_depth: size(depth),
        ..settings()
    })
}

#[test]
fn nesting_is_bounded_by_the_limit() {
    // A server message, its rows, the batch, a row, and a cell holding a list are five tables;
    // each list level adds a cell and a list, and the innermost value adds two more.
    let deepest = nested_rows(29, &limits()).assured("64 nested tables are within the limit of 64");
    let deepest = VerifiedFrame::<ServerFrame>::verify(deepest, &limits())
        .assured("the encoder admits exactly what a receiver verifies");
    assert!(ServerMessage::decode(&deepest).is_ok());
    let error = nested_rows(30, &limits()).expect_err("66 nested tables exceed the limit of 64");
    assert_eq!(
        error.current_context(),
        &WireEncodeError::NestingTooDeep {
            field: "ListCell.elements",
            limit: 64,
        }
    );

    // A receiver with a lower limit refuses what a sender with a higher one encodes.
    let deeper = nested_rows(30, &nesting_limits(66)).assured("66 nested tables fit a limit of 66");
    let error = frame_error(VerifiedFrame::<ServerFrame>::verify(deeper, &limits()));
    assert_eq!(
        error,
        invalid(
            FrameViolation::NestingTooDeep { limit: 64 },
            "ServerMessage"
        )
    );
    let within_default = nested_rows(28, &limits()).assured("62 nested tables fit the default");
    let error = frame_error(VerifiedFrame::<ServerFrame>::verify(
        within_default,
        &nesting_limits(16),
    ));
    assert_eq!(
        error,
        invalid(
            FrameViolation::NestingTooDeep { limit: 16 },
            "ServerMessage"
        )
    );
}

#[test]
fn schema_nesting_is_bounded_by_the_limit() {
    // A server message, its reply, the subscribe outcome, the opened subscription, the schema, a
    // field and its type are seven tables; each list level adds a list shape and a field type,
    // and the innermost scalar shape one more. A branch key field is one level deeper.
    for (levels, in_branch) in [(28, false), (27, true)] {
        let deepest = nested_schema_reply(levels, in_branch, &limits())
            .assured("64 nested tables are within the limit of 64");
        let deepest = VerifiedFrame::<ServerFrame>::verify(deepest, &limits())
            .assured("the encoder admits exactly what a receiver verifies");
        assert!(ServerMessage::decode(&deepest).is_ok());
        let error = nested_schema_reply(levels + 1, in_branch, &limits())
            .expect_err("one more level exceeds the limit of 64");
        assert_eq!(
            error.current_context(),
            &WireEncodeError::NestingTooDeep {
                field: "FieldType.shape",
                limit: 64,
            }
        );
    }
}

#[test]
fn the_deepest_admitted_nesting_verifies_decodes_and_conforms() {
    // The verifier, the decoders and conformance recurse once per level, so the deepest nesting
    // any limit admits must fit a test thread's stack in a debug build.
    let limits = nesting_limits(MAX_NESTING_DEPTH);
    let levels = (MAX_NESTING_DEPTH - 6) / 2;
    let rows = nested_rows(levels, &limits).assured("the deepest admitted rows encode");
    let rows = VerifiedFrame::<ServerFrame>::verify(rows, &limits)
        .assured("the deepest admitted rows verify");
    let ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) =
        ServerMessage::decode(&rows).assured("the deepest admitted rows decode")
    else {
        panic!("a rows frame decodes as rows");
    };
    let schema = RowSchema {
        fields: vec![SchemaField {
            name: name("value"),
            ty: nested_type(levels),
            optional: false,
            sensitive: false,
        }],
        branch: None,
    };
    rows.batch()
        .conform(&schema)
        .assured("the rows hold the nested list type");
    assert!(!format!("{:?}", rows.batch()).is_empty());

    let reply = nested_schema_reply((MAX_NESTING_DEPTH - 8) / 2, false, &limits)
        .assured("the deepest admitted schema encodes");
    let reply = VerifiedFrame::<ServerFrame>::verify(reply, &limits)
        .assured("the deepest admitted schema verifies");
    assert!(ServerMessage::decode(&reply).is_ok());
}

#[test]
fn aliased_tables_that_multiply_traversal_are_refused() {
    let mut builder = FlatBufferBuilder::new();
    let null = wire::NullCell::create(&mut builder, &wire::NullCellArgs {});
    let cell = wire::Cell::create(
        &mut builder,
        &wire::CellArgs {
            value_type: wire::CellValue::NullCell,
            value: Some(null.as_union_value()),
        },
    );
    let cells = builder.create_vector(&[cell; 1000]);
    let row = wire::Row::create(&mut builder, &wire::RowArgs { cells: Some(cells) });
    let rows = builder.create_vector(&[row; 1000]);
    let error = frame_error(VerifiedFrame::<ServerFrame>::verify(
        finish_rows(builder, rows),
        &limits(),
    ));
    assert!(
        matches!(
            error,
            FrameError::Invalid {
                violation: FrameViolation::TooManyTables { .. },
                ..
            }
        ),
        "{error:?}"
    );

    let mut builder = FlatBufferBuilder::new();
    let text = builder.create_string(&"x".repeat(1024));
    let string =
        wire::StringCell::create(&mut builder, &wire::StringCellArgs { value: Some(text) });
    let cell = wire::Cell::create(
        &mut builder,
        &wire::CellArgs {
            value_type: wire::CellValue::StringCell,
            value: Some(string.as_union_value()),
        },
    );
    let cells = builder.create_vector(&[cell; 1000]);
    let row = wire::Row::create(&mut builder, &wire::RowArgs { cells: Some(cells) });
    let rows = builder.create_vector(&[row]);
    let error = frame_error(VerifiedFrame::<ServerFrame>::verify(
        finish_rows(builder, rows),
        &limits(),
    ));
    assert!(
        matches!(
            error,
            FrameError::Invalid {
                violation: FrameViolation::ApparentSizeTooLarge { .. },
                ..
            }
        ),
        "{error:?}"
    );
}

fn finish_rows<'fbb>(
    mut builder: FlatBufferBuilder<'fbb>,
    rows: flatbuffers::WIPOffset<
        flatbuffers::Vector<'fbb, flatbuffers::ForwardsUOffset<wire::Row<'fbb>>>,
    >,
) -> Bytes {
    let batch = wire::RowBatch::create(
        &mut builder,
        &wire::RowBatchArgs {
            branch_key: None,
            rows: Some(rows),
        },
    );
    let name = builder.create_string("live");
    let handle = wire::SubscriptionHandle::create(
        &mut builder,
        &wire::SubscriptionHandleArgs {
            name: Some(name),
            generation: 1,
        },
    );
    let message = wire::SubscriptionRows::create(
        &mut builder,
        &wire::SubscriptionRowsArgs {
            subscription: Some(handle),
            batch: Some(batch),
        },
    );
    let root = wire::ServerMessage::create(
        &mut builder,
        &wire::ServerMessageArgs {
            body_type: wire::ServerBody::SubscriptionRows,
            body: Some(message.as_union_value()),
        },
    );
    super::fixtures::finish_raw(builder, root, "NXSM")
}

#[test]
fn a_detached_frame_owns_an_exact_copy() {
    let frame = VerifiedFrame::<ServerFrame>::verify(command_reply_bytes(), &limits())
        .assured("an encoded frame verifies");
    let detached = frame.detached();
    assert_eq!(detached.bytes(), frame.bytes());
    assert_ne!(detached.bytes().as_ptr(), frame.bytes().as_ptr());
    assert_eq!(detached.len(), frame.len());
    assert!(!detached.is_empty());
    let original = ServerMessage::decode(&frame).assured("the frame decodes");
    let copied = ServerMessage::decode(&detached).assured("the copy decodes");
    let (ServerMessage::Reply(original), ServerMessage::Reply(copied)) = (original, copied) else {
        panic!("a reply frame decodes as a reply");
    };
    assert_eq!(original, copied);
}

#[test]
fn request_identity_is_read_before_decoding() {
    let client = VerifiedFrame::<ClientFrame>::verify(client_bytes(&marked_command()), &limits())
        .assured("an encoded frame verifies");
    assert_eq!(client.request_id(), Some(request(7)));

    let reply = VerifiedFrame::<ServerFrame>::verify(command_reply_bytes(), &limits())
        .assured("an encoded frame verifies");
    assert_eq!(reply.request_id(), Some(request(3)));

    let notice = ServerNotice {
        level: NoticeLevel::Warning,
        message: "no request".to_string(),
    }
    .encode(&limits())
    .assured("a notice fits the default limits");
    let notice = VerifiedFrame::<ServerFrame>::verify(notice.into_bytes(), &limits())
        .assured("an encoded frame verifies");
    assert_eq!(notice.request_id(), None);
    assert!(matches!(
        ServerMessage::decode(&notice),
        Ok(ServerMessage::Event(ServerEvent::Notice(_)))
    ));
}

#[test]
fn every_fixed_structure_fits_the_smallest_nesting_limit() {
    // The inspection reply holds the deepest structure of the schema that does not recurse: 13
    // tables down to a node coverage in an execution step's affected topology.
    let shallow = checked(SessionLimitSettings {
        frame_bytes: size(64 * 1024 * 1024),
        transfer_bytes: size(64 * 1024 * 1024),
        nesting_depth: size(MIN_NESTING_DEPTH),
        ..settings()
    });
    let reply = Reply {
        request_id: request(1),
        body: ReplyBody::Inspection(InspectionOutcome::Inspected(Box::new(
            TransactionInspection {
                transaction: transaction(TransactionLifecycle::Open),
                operation: None,
                report: impact_report(),
            },
        ))),
    };
    let ReplyDelivery::Frame(frame) = reply.encode(&shallow).assured("the reply fits the limits")
    else {
        panic!("a 64 MiB frame holds the sample inspection");
    };
    let frame = frame
        .verify(&shallow)
        .assured("the deepest fixed structure verifies under the smallest nesting limit");
    assert!(ServerMessage::decode(&frame).is_ok());
    for message in client_messages() {
        let frame = message
            .encode(&shallow)
            .assured("a request fits the limits")
            .verify(&shallow)
            .assured("every request verifies under the smallest nesting limit");
        assert!(ClientMessage::decode(&frame).is_ok());
    }
}
