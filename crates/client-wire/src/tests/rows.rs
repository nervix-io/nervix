//! Typed rows: every cell kind at the edges of its range, null, redacted and absent branch
//! identities, conformance to the announced schema, and the limits a batch is held to.

use std::num::NonZeroU32;

use bytes::Bytes;
use error_stack::Report;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{FieldName, ParseAsType, SchemaField, Timestamp};

use super::{
    fixtures::{
        checked, decode_error, decode_event, finish_raw, limits, name, raw_server, settings, size,
    },
    samples::{row_schema, rows_frame, subscription},
};
use crate::{
    CellView, CellWriter, CellsView, EmptyBranchKey, RowBranch, RowConformanceError, RowLocation,
    RowSchema, ServerEvent, ServerMessage, SessionLimitSettings, SessionLimits, SubscriptionRows,
    SubscriptionRowsEncoder, WireDecodeError, WireEncodeError, row::CELL_TABLES_BYTES, wire,
};

fn decode_rows(bytes: Bytes) -> SubscriptionRows {
    let frame = raw_server(bytes);
    match ServerMessage::decode(&frame).assured("the rows frame decodes") {
        ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) => rows,
        other => panic!("a rows frame decoded as {other:?}"),
    }
}

fn cells(view: CellsView<'_>) -> Vec<CellView<'_>> {
    view.iter().collect()
}

#[test]
fn a_batch_round_trips_every_cell_kind_at_its_bounds() {
    let ServerEvent::SubscriptionRows(rows) = decode_event(rows_frame(&limits())) else {
        panic!("a rows frame decodes as rows");
    };
    assert_eq!(rows.subscription(), &subscription());
    let batch = rows.batch();
    assert_eq!(batch.len(), 2);
    assert!(!batch.is_empty());

    let key = batch.branch_key().assured("the sample batch is branched");
    assert_eq!(
        cells(key),
        [CellView::String("acme"), CellView::Null, CellView::Redacted]
    );

    let minimum = batch.row(0).assured("the batch holds two rows");
    let minimum_cells = cells(minimum);
    assert_eq!(minimum_cells.len(), row_schema().fields.len());
    assert_eq!(
        minimum_cells[..16],
        [
            CellView::U8(u8::MIN),
            CellView::I8(i8::MIN),
            CellView::U16(u16::MIN),
            CellView::I16(i16::MIN),
            CellView::U32(u32::MIN),
            CellView::I32(i32::MIN),
            CellView::U64(u64::MIN),
            CellView::I64(i64::MIN),
            CellView::F32(-0.0),
            CellView::F64(f64::NEG_INFINITY),
            CellView::Bool(false),
            CellView::String(""),
            CellView::Datetime(Timestamp::from_unix_nanos(i64::MIN)),
            CellView::Null,
            CellView::Redacted,
            CellView::Redacted,
        ]
    );
    let CellView::F32(negative_zero) = minimum_cells[8] else {
        panic!("the ninth cell is a 32-bit float");
    };
    assert!(
        negative_zero.is_sign_negative(),
        "the sign of zero survives"
    );
    let CellView::List(triple) = minimum_cells[16] else {
        panic!("the triple is a list");
    };
    assert_eq!(
        cells(triple),
        [
            CellView::I32(i32::MIN),
            CellView::I32(0),
            CellView::I32(i32::MAX)
        ]
    );
    let CellView::List(pairs) = minimum_cells[17] else {
        panic!("the pairs are a list");
    };
    assert!(pairs.is_empty());

    let maximum = cells(batch.row(1).assured("the batch holds two rows"));
    assert_eq!(
        maximum[..16],
        [
            CellView::U8(u8::MAX),
            CellView::I8(i8::MAX),
            CellView::U16(u16::MAX),
            CellView::I16(i16::MAX),
            CellView::U32(u32::MAX),
            CellView::I32(i32::MAX),
            CellView::U64(u64::MAX),
            CellView::I64(i64::MAX),
            CellView::F32(f32::from_bits(0x7FC0_0001)),
            CellView::F64(f64::from_bits(1)),
            CellView::Bool(true),
            CellView::String("ünïcødé 🦀 \u{0}"),
            CellView::Datetime(Timestamp::from_unix_nanos(i64::MAX)),
            CellView::String("present"),
            CellView::Redacted,
            CellView::Redacted,
        ]
    );
    let CellView::F32(nan) = maximum[8] else {
        panic!("the ninth cell is a 32-bit float");
    };
    assert_eq!(nan.to_bits(), 0x7FC0_0001, "a NaN keeps its payload");
    let CellView::List(pairs) = maximum[17] else {
        panic!("the pairs are a list");
    };
    let pairs = cells(pairs);
    assert_eq!(pairs.len(), 2);
    let CellView::List(first) = pairs[0] else {
        panic!("a pair is a list");
    };
    assert_eq!(cells(first), [CellView::U8(0), CellView::U8(255)]);
    assert_eq!(batch.row(2), None);
    assert_eq!(key.get(3), None);

    batch
        .conform(&row_schema())
        .assured("the sample batch follows the sample schema");
    assert_eq!(batch.rows().count(), 2);
    assert!(!format!("{batch:?}").is_empty());
}

#[test]
fn row_views_borrow_from_the_frame() {
    let ServerEvent::SubscriptionRows(rows) = decode_event(rows_frame(&limits())) else {
        panic!("a rows frame decodes as rows");
    };
    let batch = rows.batch();
    let row = batch.row(1).assured("the batch holds two rows");
    let CellView::String(text) = row.get(11).assured("the row holds its text cell") else {
        panic!("the twelfth cell is a string");
    };
    let frame = rows.frame().bytes();
    let start = frame.as_ptr().addr();
    assert!((start..start + frame.len()).contains(&text.as_ptr().addr()));
}

fn unbranched_schema(fields: Vec<SchemaField>) -> RowSchema {
    RowSchema {
        fields,
        branch: None,
    }
}

fn field(raw: &str, ty: ParseAsType) -> SchemaField {
    SchemaField {
        name: name(raw),
        ty,
        optional: false,
        sensitive: false,
    }
}

type WriteCells = fn(&mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>>;

fn unbranched_rows(rows: &[WriteCells]) -> SubscriptionRows {
    let mut batch = SubscriptionRowsEncoder::unbranched(subscription(), &limits())
        .assured("a subscription handle fits the limits");
    for row in rows {
        batch.push_row(row).assured("the test row fits the limits");
    }
    decode_rows(batch.finish().assured("the test batch fits").into_bytes())
}

fn conformance_error(rows: &SubscriptionRows, schema: &RowSchema) -> RowConformanceError {
    match rows.batch().conform(schema) {
        Ok(()) => panic!("the batch unexpectedly conforms"),
        Err(error) => error.current_context().clone(),
    }
}

#[test]
fn bytes_cells_round_trip_as_borrowed_binary_values() {
    let rows = unbranched_rows(&[
        |cells| cells.push_bytes(&[0, 255]),
        |cells| cells.push_bytes(&[]),
    ]);
    let schema = unbranched_schema(vec![field("raw", ParseAsType::Bytes)]);
    rows.batch()
        .conform(&schema)
        .assured("the binary cells match their schema");
    let first = rows.batch().row(0).assured("the first row exists");
    assert_eq!(first.get(0), Some(CellView::Bytes(&[0, 255])));
    let second = rows.batch().row(1).assured("the second row exists");
    assert_eq!(second.get(0), Some(CellView::Bytes(&[])));
    assert_eq!(
        rows.batch()
            .display_lines(&schema)
            .assured("the binary cells match their schema"),
        ["{\"raw\":\"AP8=\"}", "{\"raw\":\"\"}"]
    );
}

#[test]
fn branch_identity_must_match_the_schema() {
    let rows = unbranched_rows(&[|cells| {
        cells.push_u8(1)?;
        Ok(())
    }]);
    assert_eq!(rows.batch().branch_key(), None);
    let unbranched = unbranched_schema(vec![field("id", ParseAsType::U8)]);
    rows.batch()
        .conform(&unbranched)
        .assured("an unbranched batch follows an unbranched schema");

    let branched = RowSchema {
        branch: Some(
            RowBranch::new(name("tenants"), vec![field("tenant", ParseAsType::String)])
                .assured("a branch with a key field"),
        ),
        ..unbranched.clone()
    };
    assert_eq!(
        conformance_error(&rows, &branched),
        RowConformanceError::MissingBranchKey
    );

    let mut keyed =
        SubscriptionRowsEncoder::branched(subscription(), &limits(), |key| key.push_string("acme"))
            .assured("a one-field key fits the limits");
    keyed
        .push_row(|cells| {
            cells.push_u8(1)?;
            Ok(())
        })
        .assured("the row fits");
    let keyed = decode_rows(keyed.finish().assured("the batch fits").into_bytes());
    keyed
        .batch()
        .conform(&branched)
        .assured("a keyed batch follows a branched schema");
    assert_eq!(
        conformance_error(&keyed, &unbranched),
        RowConformanceError::UnexpectedBranchKey
    );

    let redacted_key = RowSchema {
        branch: Some(
            RowBranch::new(
                name("tenants"),
                vec![SchemaField {
                    sensitive: true,
                    ..field("tenant", ParseAsType::String)
                }],
            )
            .assured("a branch with a key field"),
        ),
        ..unbranched
    };
    assert_eq!(
        conformance_error(&keyed, &redacted_key),
        RowConformanceError::SensitiveFieldNotRedacted {
            location: RowLocation::BranchKey,
            field: name("tenant"),
        }
    );
}

#[test]
fn cells_must_follow_their_fields() {
    let id: FieldName = name("id");
    let rows = unbranched_rows(&[|cells| {
        cells.push_i8(1)?;
        Ok(())
    }]);
    let schema = unbranched_schema(vec![field("id", ParseAsType::U8)]);
    assert_eq!(
        conformance_error(&rows, &schema),
        RowConformanceError::TypeMismatch {
            location: RowLocation::Row { index: 0 },
            field: id.clone(),
            expected: ParseAsType::U8,
            actual: "I8",
        }
    );

    let rows = unbranched_rows(&[
        |cells| {
            cells.push_u8(1)?;
            Ok(())
        },
        |_| Ok(()),
    ]);
    assert_eq!(
        conformance_error(&rows, &schema),
        RowConformanceError::CellCount {
            location: RowLocation::Row { index: 1 },
            expected: 1,
            actual: 0,
        }
    );

    let rows = unbranched_rows(&[|cells| {
        cells.push_null()?;
        Ok(())
    }]);
    assert_eq!(
        conformance_error(&rows, &schema),
        RowConformanceError::UnexpectedNull {
            location: RowLocation::Row { index: 0 },
            field: id.clone(),
        }
    );
    let nullable = unbranched_schema(vec![SchemaField {
        optional: true,
        ..field("id", ParseAsType::U8)
    }]);
    rows.batch()
        .conform(&nullable)
        .assured("a null follows a nullable field");

    let rows = unbranched_rows(&[|cells| {
        cells.push_redacted()?;
        Ok(())
    }]);
    assert_eq!(
        conformance_error(&rows, &nullable),
        RowConformanceError::UnexpectedRedaction {
            location: RowLocation::Row { index: 0 },
            field: id.clone(),
        }
    );

    let sensitive_nullable = unbranched_schema(vec![SchemaField {
        optional: true,
        sensitive: true,
        ..field("id", ParseAsType::U8)
    }]);
    rows.batch()
        .conform(&sensitive_nullable)
        .assured("a redaction follows a sensitive field");
    let revealing: [WriteCells; 2] = [
        |cells| {
            cells.push_null()?;
            Ok(())
        },
        |cells| {
            cells.push_u8(1)?;
            Ok(())
        },
    ];
    for write in revealing {
        let rows = unbranched_rows(&[write]);
        assert_eq!(
            conformance_error(&rows, &sensitive_nullable),
            RowConformanceError::SensitiveFieldNotRedacted {
                location: RowLocation::Row { index: 0 },
                field: id.clone(),
            },
            "a sensitive field never reveals whether it holds a value"
        );
    }
}

#[test]
fn lists_must_follow_their_element_type_and_length() {
    let triple = unbranched_schema(vec![field(
        "triple",
        ParseAsType::Array {
            element: Box::new(ParseAsType::I32),
            len: NonZeroU32::new(3).assured("a non-zero length"),
        },
    )]);
    let rows = unbranched_rows(&[|cells| {
        cells.push_list(|elements| {
            elements.push_i32(1)?;
            elements.push_i32(2)?;
            Ok(())
        })
    }]);
    assert_eq!(
        conformance_error(&rows, &triple),
        RowConformanceError::FixedListLength {
            location: RowLocation::Row { index: 0 },
            field: name("triple"),
            expected: 3,
            actual: 2,
        }
    );

    let rows = unbranched_rows(&[|cells| {
        cells.push_list(|elements| {
            elements.push_i32(1)?;
            elements.push_null()?;
            elements.push_i32(3)?;
            Ok(())
        })
    }]);
    assert_eq!(
        conformance_error(&rows, &triple),
        RowConformanceError::TypeMismatch {
            location: RowLocation::Row { index: 0 },
            field: name("triple"),
            expected: ParseAsType::I32,
            actual: "NULL",
        }
    );

    let rows = unbranched_rows(&[|cells| {
        cells.push_i32(1)?;
        Ok(())
    }]);
    let list = unbranched_schema(vec![field(
        "values",
        ParseAsType::Vec {
            element: Box::new(ParseAsType::I32),
        },
    )]);
    assert!(matches!(
        conformance_error(&rows, &list),
        RowConformanceError::TypeMismatch { actual: "I32", .. }
    ));
}

#[test]
fn an_empty_batch_is_refused() {
    let batch = SubscriptionRowsEncoder::unbranched(subscription(), &limits())
        .assured("a subscription handle fits the limits");
    let error = batch.finish().expect_err("a batch needs a row");
    assert_eq!(
        error.current_context(),
        &WireEncodeError::EmptyCollection {
            field: "RowBatch.rows",
        }
    );

    let bytes = raw_rows(|builder| builder.create_vector::<WIPOffset<wire::Row>>(&[]));
    assert_eq!(
        decode_error(ServerMessage::decode(&raw_server(bytes))),
        WireDecodeError::EmptyCollection {
            field: "RowBatch.rows",
        }
    );
}

fn raw_rows(
    rows: impl FnOnce(
        &mut FlatBufferBuilder<'static>,
    ) -> WIPOffset<Vector<'static, ForwardsUOffset<wire::Row<'static>>>>,
) -> Bytes {
    let mut builder = FlatBufferBuilder::new();
    let rows = rows(&mut builder);
    let batch = wire::RowBatch::create(
        &mut builder,
        &wire::RowBatchArgs {
            branch_key: None,
            rows: Some(rows),
        },
    );
    let handle_name = builder.create_string("live");
    let handle = wire::SubscriptionHandle::create(
        &mut builder,
        &wire::SubscriptionHandleArgs {
            name: Some(handle_name),
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
    finish_raw(builder, root, "NXSM")
}

fn one_cell_rows(
    cell: impl FnOnce(&mut FlatBufferBuilder<'static>) -> WIPOffset<wire::Cell<'static>>,
) -> Bytes {
    raw_rows(|builder| {
        let cell = cell(builder);
        let cells = builder.create_vector(&[cell]);
        let row = wire::Row::create(builder, &wire::RowArgs { cells: Some(cells) });
        builder.create_vector(&[row])
    })
}

#[test]
fn malformed_cells_are_refused() {
    let bytes = one_cell_rows(|builder| {
        let value = wire::F32Cell::create(builder, &wire::F32CellArgs { value: None });
        wire::Cell::create(
            builder,
            &wire::CellArgs {
                value_type: wire::CellValue::F32Cell,
                value: Some(value.as_union_value()),
            },
        )
    });
    assert_eq!(
        decode_error(ServerMessage::decode(&raw_server(bytes))),
        WireDecodeError::MissingField {
            field: "F32Cell.value",
        }
    );

    let bytes = one_cell_rows(|builder| {
        let value = wire::F64Cell::create(builder, &wire::F64CellArgs { value: None });
        wire::Cell::create(
            builder,
            &wire::CellArgs {
                value_type: wire::CellValue::F64Cell,
                value: Some(value.as_union_value()),
            },
        )
    });
    assert_eq!(
        decode_error(ServerMessage::decode(&raw_server(bytes))),
        WireDecodeError::MissingField {
            field: "F64Cell.value",
        }
    );

    let bytes = one_cell_rows(|builder| {
        let value = wire::NullCell::create(builder, &wire::NullCellArgs {});
        wire::Cell::create(
            builder,
            &wire::CellArgs {
                value_type: wire::CellValue(99),
                value: Some(value.as_union_value()),
            },
        )
    });
    assert_eq!(
        decode_error(ServerMessage::decode(&raw_server(bytes))),
        WireDecodeError::UnknownUnionVariant {
            field: "Cell.value",
            discriminant: 99,
        }
    );

    let bytes = one_cell_rows(|builder| {
        let inner = wire::NullCell::create(builder, &wire::NullCellArgs {});
        let inner = wire::Cell::create(
            builder,
            &wire::CellArgs {
                value_type: wire::CellValue(18),
                value: Some(inner.as_union_value()),
            },
        );
        let elements = builder.create_vector(&[inner]);
        let list = wire::ListCell::create(
            builder,
            &wire::ListCellArgs {
                elements: Some(elements),
            },
        );
        wire::Cell::create(
            builder,
            &wire::CellArgs {
                value_type: wire::CellValue::ListCell,
                value: Some(list.as_union_value()),
            },
        )
    });
    assert_eq!(
        decode_error(ServerMessage::decode(&raw_server(bytes))),
        WireDecodeError::UnknownUnionVariant {
            field: "Cell.value",
            discriminant: 18,
        }
    );
}

#[test]
fn rows_and_cells_are_held_to_the_collection_limit() {
    let limited = checked(SessionLimitSettings {
        collection_entries: size(3),
        ..settings()
    });
    let mut batch = SubscriptionRowsEncoder::unbranched(subscription(), &limited)
        .assured("a subscription handle fits the limits");
    for _ in 0..3 {
        batch
            .push_row(|cells| {
                cells.push_u8(1)?;
                Ok(())
            })
            .assured("three rows fit a limit of three");
    }
    let error = batch
        .push_row(|cells| {
            cells.push_u8(1)?;
            Ok(())
        })
        .expect_err("a fourth row exceeds a limit of three");
    assert_eq!(
        error.current_context(),
        &WireEncodeError::TooManyEntries {
            field: "RowBatch.rows",
            actual: 4,
            limit: 3,
        }
    );
    assert_eq!(batch.rows(), 3);
    let exact = decode_rows_under(
        batch.finish().assured("three rows fit").into_bytes(),
        &limited,
    );
    assert_eq!(exact.batch().len(), 3);

    let mut wide = SubscriptionRowsEncoder::unbranched(subscription(), &limited)
        .assured("a subscription handle fits the limits");
    let error = wide
        .push_row(|cells| {
            for value in 0..4 {
                cells.push_u8(value)?;
            }
            Ok(())
        })
        .expect_err("four cells exceed a limit of three");
    assert_eq!(
        error.current_context(),
        &WireEncodeError::TooManyEntries {
            field: "Row.cells",
            actual: 4,
            limit: 3,
        }
    );

    let mut many = SubscriptionRowsEncoder::unbranched(subscription(), &limits())
        .assured("a subscription handle fits the limits");
    for _ in 0..4 {
        many.push_row(|cells| {
            cells.push_u8(1)?;
            Ok(())
        })
        .assured("four rows fit the default limits");
    }
    let frame = crate::VerifiedFrame::<crate::ServerFrame>::verify(
        many.finish().assured("four rows fit").into_bytes(),
        &limited,
    )
    .assured("collection limits apply when decoding");
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::TooManyEntries {
            field: "RowBatch.rows",
            actual: 4,
            limit: 3,
        }
    );
}

fn decode_rows_under(bytes: Bytes, limits: &crate::SessionLimits) -> SubscriptionRows {
    let frame = crate::VerifiedFrame::<crate::ServerFrame>::verify(bytes, limits)
        .assured("the frame fits the limits it was encoded for");
    match ServerMessage::decode(&frame).assured("the frame decodes") {
        ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) => rows,
        other => panic!("a rows frame decoded as {other:?}"),
    }
}

/// Pushes rows until one is refused, then finishes the batch, which must hold every accepted row.
fn fill_and_finish(
    limits: &SessionLimits,
    mut write_row: impl FnMut(&mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>>,
) -> Report<WireEncodeError> {
    let mut batch = SubscriptionRowsEncoder::unbranched(subscription(), limits)
        .assured("a subscription handle fits the limits");
    let mut pushed = 0;
    let error = loop {
        match batch.push_row(&mut write_row) {
            Ok(()) => pushed += 1,
            Err(error) => break error,
        }
        assert!(
            pushed < 10_000,
            "a small frame holds a bounded number of rows"
        );
    };
    assert!(pushed >= 1, "the first row fits: {error:?}");
    assert!(batch.encoded_bytes() > 0);
    assert!(!format!("{batch:?}").is_empty());
    let frame = batch
        .finish()
        .assured("a batch whose rows were accepted always finishes");
    assert!(frame.len() <= limits.frame_bytes());
    let rows = decode_rows_under(frame.into_bytes(), limits);
    assert_eq!(rows.batch().len(), pushed);
    error
}

#[test]
fn a_batch_stops_at_the_frame_limit_and_still_finishes() {
    let small = checked(SessionLimitSettings {
        frame_bytes: size(1024),
        ..settings()
    });
    let text = "v".repeat(100);
    let error = fill_and_finish(&small, |cells| cells.push_string(&text));
    assert!(matches!(
        error.current_context(),
        WireEncodeError::FrameTooLarge { limit: 1024, .. }
    ));

    // A row refused part way through a cell leaves its accepted cells behind in the frame.
    for shape in 0..3 {
        let error = fill_and_finish(&small, |cells| {
            cells.push_u64(u64::MAX)?;
            match shape {
                0 => cells.push_string(&text),
                1 => cells.push_list(|elements| {
                    for _ in 0..8 {
                        elements.push_f64(f64::MIN)?;
                    }
                    elements.push_string(&text)
                }),
                _ => cells.push_list(|elements| {
                    elements.push_list(|inner| {
                        inner.push_datetime(Timestamp::from_unix_nanos(i64::MAX))?;
                        inner.push_string(&text)
                    })
                }),
            }
        });
        assert!(matches!(
            error.current_context(),
            WireEncodeError::FrameTooLarge { limit: 1024, .. }
        ));
    }
}

#[test]
fn every_cell_kind_stays_within_its_table_bound() {
    let limits = limits();
    let mut batch = SubscriptionRowsEncoder::unbranched(subscription(), &limits)
        .assured("a subscription handle fits the limits");
    let cells: [WriteCells; 16] = [
        |cells| cells.push_null(),
        |cells| cells.push_redacted(),
        |cells| cells.push_u8(u8::MAX),
        |cells| cells.push_i8(i8::MIN),
        |cells| cells.push_u16(u16::MAX),
        |cells| cells.push_i16(i16::MIN),
        |cells| cells.push_u32(u32::MAX),
        |cells| cells.push_i32(i32::MIN),
        |cells| cells.push_u64(u64::MAX),
        |cells| cells.push_i64(i64::MIN),
        |cells| cells.push_f32(f32::MIN),
        |cells| cells.push_f64(f64::MIN),
        |cells| cells.push_bool(true),
        |cells| cells.push_string(""),
        |cells| cells.push_datetime(Timestamp::from_unix_nanos(i64::MIN)),
        |cells| cells.push_list(|_| Ok(())),
    ];
    for write in cells {
        batch
            .push_row(|cells| {
                for _ in 0..4 {
                    let before = cells.encoded_bytes();
                    write(cells)?;
                    let grown = cells.encoded_bytes() - before;
                    assert!(
                        grown <= CELL_TABLES_BYTES,
                        "a cell grew the frame by {grown} bytes"
                    );
                }
                Ok(())
            })
            .assured("a row of small cells fits the default limits");
    }
}

fn schema_reply(
    field_type: impl FnOnce(&mut FlatBufferBuilder<'static>) -> WIPOffset<wire::FieldType<'static>>,
    branch_fields: Option<usize>,
) -> Bytes {
    let mut builder = FlatBufferBuilder::new();
    let field_type = field_type(&mut builder);
    let field_name = builder.create_string("value");
    let field = wire::RowField::create(
        &mut builder,
        &wire::RowFieldArgs {
            name: Some(field_name),
            field_type: Some(field_type),
            nullable: false,
            sensitive: false,
        },
    );
    let fields = builder.create_vector(&[field]);
    let branch = branch_fields.map(|count| {
        let branch_name = builder.create_string("tenants");
        let key_fields = builder.create_vector(&vec![field; count]);
        wire::RowBranch::create(
            &mut builder,
            &wire::RowBranchArgs {
                branch: Some(branch_name),
                fields: Some(key_fields),
            },
        )
    });
    let schema = wire::RowSchema::create(
        &mut builder,
        &wire::RowSchemaArgs {
            fields: Some(fields),
            branch,
        },
    );
    let handle_name = builder.create_string("live");
    let handle = wire::SubscriptionHandle::create(
        &mut builder,
        &wire::SubscriptionHandleArgs {
            name: Some(handle_name),
            generation: 1,
        },
    );
    let domain = builder.create_string("tenant");
    let relay = builder.create_string("orders");
    let opened = wire::SubscriptionOpened::create(
        &mut builder,
        &wire::SubscriptionOpenedArgs {
            subscription: Some(handle),
            domain: Some(domain),
            relay: Some(relay),
            subscription_type: Some(wire::SubscriptionType::Row),
            schema: Some(schema),
        },
    );
    let message = builder.create_string("");
    let diagnostics = builder.create_vector::<WIPOffset<wire::Diagnostic>>(&[]);
    let outcome = wire::SubscribeOutcome::create(
        &mut builder,
        &wire::SubscribeOutcomeArgs {
            disposition_type: wire::SubscribeDisposition::SubscriptionOpened,
            disposition: Some(opened.as_union_value()),
            message: Some(message),
            diagnostics: Some(diagnostics),
        },
    );
    let reply = wire::Reply::create(
        &mut builder,
        &wire::ReplyArgs {
            request_id: 1,
            body_type: wire::ReplyBody::SubscribeOutcome,
            body: Some(outcome.as_union_value()),
        },
    );
    let root = wire::ServerMessage::create(
        &mut builder,
        &wire::ServerMessageArgs {
            body_type: wire::ServerBody::Reply,
            body: Some(reply.as_union_value()),
        },
    );
    finish_raw(builder, root, "NXSM")
}

fn scalar_type(
    builder: &mut FlatBufferBuilder<'static>,
    scalar: Option<wire::ScalarType>,
) -> WIPOffset<wire::FieldType<'static>> {
    let scalar = wire::ScalarFieldType::create(builder, &wire::ScalarFieldTypeArgs { scalar });
    wire::FieldType::create(
        builder,
        &wire::FieldTypeArgs {
            shape_type: wire::FieldTypeShape::ScalarFieldType,
            shape: Some(scalar.as_union_value()),
        },
    )
}

#[test]
fn malformed_schemas_are_refused() {
    let bytes = schema_reply(
        |builder| {
            let element = scalar_type(builder, Some(wire::ScalarType::U8));
            let list = wire::FixedListFieldType::create(
                builder,
                &wire::FixedListFieldTypeArgs {
                    element: Some(element),
                    length: 0,
                },
            );
            wire::FieldType::create(
                builder,
                &wire::FieldTypeArgs {
                    shape_type: wire::FieldTypeShape::FixedListFieldType,
                    shape: Some(list.as_union_value()),
                },
            )
        },
        None,
    );
    assert_eq!(
        decode_error(ServerMessage::decode(&raw_server(bytes))),
        WireDecodeError::ZeroValue {
            field: "FixedListFieldType.length",
        }
    );

    let bytes = schema_reply(|builder| scalar_type(builder, None), None);
    assert_eq!(
        decode_error(ServerMessage::decode(&raw_server(bytes))),
        WireDecodeError::MissingField {
            field: "ScalarFieldType.scalar",
        }
    );

    let bytes = schema_reply(
        |builder| scalar_type(builder, Some(wire::ScalarType(14))),
        None,
    );
    assert_eq!(
        decode_error(ServerMessage::decode(&raw_server(bytes))),
        WireDecodeError::UnknownEnumValue {
            field: "ScalarFieldType.scalar",
            value: 14,
        }
    );

    let bytes = schema_reply(
        |builder| scalar_type(builder, Some(wire::ScalarType::U8)),
        Some(0),
    );
    assert_eq!(
        decode_error(ServerMessage::decode(&raw_server(bytes))),
        WireDecodeError::EmptyCollection {
            field: "RowBranch.fields",
        }
    );
    let error = RowBranch::new(name("tenants"), Vec::new()).expect_err("a key needs fields");
    assert_eq!(error.current_context(), &EmptyBranchKey);

    let bytes = schema_reply(
        |builder| scalar_type(builder, Some(wire::ScalarType::U8)),
        Some(2),
    );
    assert!(ServerMessage::decode(&raw_server(bytes)).is_ok());
}

/// A small schema whose display text is known exactly: fields out of name order, an optional, a
/// sensitive, a datetime and a list field, on a two-field branch key.
fn display_schema(branched: bool) -> RowSchema {
    let field = |raw: &str, ty: ParseAsType| SchemaField {
        name: name(raw),
        ty,
        optional: false,
        sensitive: false,
    };
    let branch = RowBranch::new(
        name("tenants"),
        vec![
            field("tenant", ParseAsType::String),
            field("region", ParseAsType::U16),
        ],
    )
    .assured("the display branch has key fields");
    RowSchema {
        fields: vec![
            field("user_id", ParseAsType::U32),
            field("amount", ParseAsType::F32),
            SchemaField {
                optional: true,
                ..field("note", ParseAsType::String)
            },
            SchemaField {
                sensitive: true,
                ..field("secret", ParseAsType::String)
            },
            field("seen_at", ParseAsType::Datetime),
            field(
                "tags",
                ParseAsType::Vec {
                    element: Box::new(ParseAsType::String),
                },
            ),
        ],
        branch: branched.then_some(branch),
    }
}

fn write_display_row(
    cells: &mut CellWriter<'_, 'static>,
    user_id: u32,
    amount: f32,
    note: Option<&str>,
) -> Result<(), Report<WireEncodeError>> {
    cells.push_u32(user_id)?;
    cells.push_f32(amount)?;
    match note {
        Some(note) => cells.push_string(note)?,
        None => cells.push_null()?,
    }
    cells.push_redacted()?;
    cells.push_datetime(Timestamp::from_unix_nanos(1_500_000_000))?;
    cells.push_list(|tags| {
        tags.push_string("a")?;
        tags.push_string("b")?;
        Ok(())
    })
}

#[test]
fn rows_display_as_json_objects_in_field_name_order_after_their_branch_key() {
    let mut batch = SubscriptionRowsEncoder::branched(subscription(), &limits(), |key| {
        key.push_string("acme")?;
        key.push_u16(7)
    })
    .assured("the display key fits the limits");
    batch
        .push_row(|cells| write_display_row(cells, 1, 0.5, None))
        .assured("the display row fits the limits");
    batch
        .push_row(|cells| write_display_row(cells, 2, 0.1, Some("say \"hi\"\n")))
        .assured("the display row fits the limits");
    let rows = decode_rows(
        batch
            .finish()
            .assured("the display batch fits")
            .into_bytes(),
    );

    let lines = rows
        .batch()
        .display_lines(&display_schema(true))
        .assured("the batch follows the display schema");
    assert_eq!(
        lines,
        [
            "key={\"tenant\":\"acme\",\"region\":7} \
             payload={\"amount\":0.5,\"secret\":\"<masked>\",\"seen_at\":\"1970-01-01T00:00:01.\
             500+00:00\",\"tags\":[\"a\",\"b\"],\"user_id\":1}",
            "key={\"tenant\":\"acme\",\"region\":7} \
             payload={\"amount\":0.10000000149011612,\"note\":\"say \
             \\\"hi\\\"\\n\",\"secret\":\"<masked>\",\"seen_at\":\"1970-01-01T00:00:01.500+00:00\"\
             ,\"tags\":[\"a\",\"b\"],\"user_id\":2}",
        ]
    );

    let error = rows
        .batch()
        .display_lines(&display_schema(false))
        .expect_err("a branched batch does not follow an unbranched schema");
    assert_eq!(
        error.current_context(),
        &RowConformanceError::UnexpectedBranchKey
    );
}

#[test]
fn unbranched_rows_display_without_a_key() {
    let mut batch = SubscriptionRowsEncoder::unbranched(subscription(), &limits())
        .assured("an unbranched batch starts within the limits");
    batch
        .push_row(|cells| write_display_row(cells, 3, -1.25, Some("")))
        .assured("the display row fits the limits");
    let rows = decode_rows(
        batch
            .finish()
            .assured("the display batch fits")
            .into_bytes(),
    );

    let lines = rows
        .batch()
        .display_lines(&display_schema(false))
        .assured("the batch follows the display schema");
    assert_eq!(
        lines,
        [
            "{\"amount\":-1.25,\"note\":\"\",\"secret\":\"<masked>\",\"seen_at\":\"1970-01-01T00:\
             00:01.500+00:00\",\"tags\":[\"a\",\"b\"],\"user_id\":3}"
        ]
    );
}
