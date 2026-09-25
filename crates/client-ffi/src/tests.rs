//! Tests of the C ABI through its exported functions, as a host calls them.
//!
//! Rows events are built from frames the wire contract encodes, so every accessor reads a real
//! verified frame. The conformance scenarios drive the same functions against a running cluster.

use std::{num::NonZeroU64, ptr, slice, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{
    ClientError, CommandDisposition, CommandExecutionReference, CommandOutcome, Diagnostic,
    LeaderRedirect, OutcomeOrigin, RowSchema, SourceSpan, SubscriptionEvent, SubscriptionHandle,
    SubscriptionInterruption, SubscriptionOpened, SubscriptionRowsEvent, UnknownOutcomeCause,
    wire::{
        CellWriter, RequestRejection, RowBranch, RowsSkippedCause, ServerEvent, ServerMessage,
        SessionLimits, SubscriptionDeliveryLost, SubscriptionEndReason, SubscriptionEnded,
        SubscriptionRowsEncoder, SubscriptionRowsSkipped, SubscriptionType, WireEncodeError,
    },
};
use nervix_models::{ParseAsType, SchemaField, Timestamp};
use triomphe::Arc;

use crate::{
    Cancel, CellState, Disposition, Event, EventKind, FailureKind, FieldType, Outcome, Schema,
    nx_cancel_free, nx_cancel_new, nx_cancel_trigger, nx_cancel_with_deadline,
    nx_error_execution_reference, nx_error_free, nx_error_kind_of, nx_error_message,
    nx_event_cell_varlen, nx_event_column_fixed, nx_event_column_states, nx_event_column_varlen,
    nx_event_frame, nx_event_kind_of, nx_event_release, nx_event_retain, nx_event_row_count,
    nx_event_schema, nx_event_subscription, nx_execution_free, nx_outcome_diagnostic,
    nx_outcome_diagnostic_count, nx_outcome_disposition, nx_outcome_execution_reference,
    nx_outcome_free, nx_outcome_message, nx_outcome_schema, nx_outcome_subscription,
    nx_schema_branch, nx_schema_field, nx_schema_field_count, nx_schema_free, nx_session_connect,
    nx_session_free,
};

const ROWS: i32 = 1;
const BRANCH_KEY: i32 = 2;

/// Reads the kind of a returned failure and releases it, failing the test on success.
fn failure_kind(failure: *mut crate::Failure) -> FailureKind {
    assert!(
        !failure.is_null(),
        "the call succeeded where it had to fail"
    );
    // SAFETY: a non-null failure is a live error the binding returned, released once here.
    unsafe {
        let kind = nx_error_kind_of(failure);
        nx_error_free(failure);
        kind
    }
}

fn succeeded(failure: *mut crate::Failure) {
    if failure.is_null() {
        return;
    }
    let mut message = ptr::null();
    let mut message_len = 0;
    // SAFETY: a non-null failure is a live error the binding returned.
    let text = unsafe {
        nx_error_message(failure, &mut message, &mut message_len);
        String::from_utf8_lossy(slice::from_raw_parts(message, message_len)).into_owned()
    };
    panic!("the call failed: {text}");
}

fn name<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str, Error = nervix_models::NameError>,
{
    N::try_from(raw).assured("the test passes a valid name")
}

fn field(raw: &str, ty: ParseAsType) -> SchemaField {
    SchemaField {
        name: name(raw),
        ty,
        optional: false,
        sensitive: false,
    }
}

/// A branched schema with one field of every fixed-width and variable-length type, a nullable
/// field, a sensitive field and a list field.
fn schema() -> RowSchema {
    RowSchema {
        fields: vec![
            field("u8", ParseAsType::U8),
            field("i8", ParseAsType::I8),
            field("u16", ParseAsType::U16),
            field("i16", ParseAsType::I16),
            field("u32", ParseAsType::U32),
            field("i32", ParseAsType::I32),
            field("u64", ParseAsType::U64),
            field("i64", ParseAsType::I64),
            field("f32", ParseAsType::F32),
            field("f64", ParseAsType::F64),
            field("flag", ParseAsType::Bool),
            field("at", ParseAsType::Datetime),
            SchemaField {
                optional: true,
                ..field("text", ParseAsType::String)
            },
            field("raw", ParseAsType::Bytes),
            SchemaField {
                sensitive: true,
                ..field("secret", ParseAsType::String)
            },
            field(
                "items",
                ParseAsType::Vec {
                    element: Box::new(ParseAsType::I64),
                },
            ),
        ],
        branch: Some(
            RowBranch::new(name("tenants"), vec![field("tenant", ParseAsType::String)])
                .assured("the branch has a key field"),
        ),
    }
}

fn handle() -> SubscriptionHandle {
    SubscriptionHandle {
        name: name("watch"),
        generation: NonZeroU64::new(7).assured("seven is non-zero"),
    }
}

fn write_row(
    cells: &mut CellWriter<'_, 'static>,
    text: Option<&str>,
    extreme: bool,
) -> Result<(), error_stack::Report<WireEncodeError>> {
    if extreme {
        cells.push_u8(u8::MAX)?;
        cells.push_i8(i8::MIN)?;
        cells.push_u16(u16::MAX)?;
        cells.push_i16(i16::MIN)?;
        cells.push_u32(u32::MAX)?;
        cells.push_i32(i32::MIN)?;
        cells.push_u64(u64::MAX)?;
        cells.push_i64(i64::MIN)?;
        cells.push_f32(-0.0)?;
        cells.push_f64(f64::from_bits(1))?;
        cells.push_bool(true)?;
        cells.push_datetime(Timestamp::from_unix_nanos(i64::MAX))?;
    } else {
        cells.push_u8(1)?;
        cells.push_i8(2)?;
        cells.push_u16(3)?;
        cells.push_i16(4)?;
        cells.push_u32(5)?;
        cells.push_i32(6)?;
        cells.push_u64(7)?;
        cells.push_i64(8)?;
        cells.push_f32(1.5)?;
        cells.push_f64(0.25)?;
        cells.push_bool(false)?;
        cells.push_datetime(Timestamp::from_unix_nanos(1))?;
    }
    match text {
        Some(text) => cells.push_string(text)?,
        None => cells.push_null()?,
    }
    cells.push_bytes(&[0, 255, 0x80])?;
    cells.push_redacted()?;
    cells.push_list(|elements| elements.push_i64(9))?;
    Ok(())
}

fn rows_event(schema: RowSchema) -> SubscriptionEvent {
    let limits = SessionLimits::DEFAULT;
    let mut batch =
        SubscriptionRowsEncoder::branched(handle(), &limits, |key| key.push_string("acme"))
            .assured("the key fits the limits");
    batch
        .push_row(|cells| write_row(cells, Some("h\u{e9}\u{0}"), true))
        .assured("the row fits the limits");
    batch
        .push_row(|cells| write_row(cells, None, false))
        .assured("the row fits the limits");
    let frame = batch
        .finish()
        .assured("the batch fits the limits")
        .verify(&limits)
        .assured("an encoded frame verifies");
    let ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) =
        ServerMessage::decode(&frame).assured("an encoded frame decodes")
    else {
        panic!("a rows frame decodes as rows");
    };
    SubscriptionEvent::Rows(SubscriptionRowsEvent {
        relay: name("typed"),
        schema: Arc::new(schema),
        rows,
    })
}

/// An event handed out as a host's first reference, released when dropped.
struct Shared(*mut Event);

impl Shared {
    fn new(event: SubscriptionEvent) -> Self {
        Self(
            Event::new(event)
                .assured("the event conforms")
                .into_shared(),
        )
    }
}

// SAFETY: an event is immutable and its references are counted atomically, so a reference may be
// released on any thread, which is what the header promises.
unsafe impl Send for Shared {}

impl Drop for Shared {
    fn drop(&mut self) {
        // SAFETY: the reference is live and released once, here.
        unsafe { nx_event_release(self.0) };
    }
}

fn fixed<const WIDTH: usize>(
    event: &Shared,
    part: i32,
    column: usize,
    cells: usize,
) -> Vec<[u8; WIDTH]> {
    let mut values = vec![0_u8; cells * WIDTH];
    // SAFETY: the event is live and `values` holds its length in writable bytes.
    succeeded(unsafe {
        nx_event_column_fixed(
            event.0,
            part,
            column,
            values.as_mut_ptr().cast(),
            values.len(),
        )
    });
    values.as_chunks::<WIDTH>().0.to_vec()
}

fn states(event: &Shared, part: i32, column: usize, cells: usize) -> Vec<u8> {
    let mut states = vec![0_u8; cells];
    // SAFETY: the event is live and `states` holds `cells` writable bytes.
    succeeded(unsafe { nx_event_column_states(event.0, part, column, states.as_mut_ptr(), cells) });
    states
}

#[test]
fn a_rows_event_copies_every_fixed_width_column_in_one_call_each() {
    let event = Shared::new(rows_event(schema()));
    // SAFETY: the event is live.
    unsafe {
        assert_eq!(nx_event_kind_of(event.0), EventKind::Rows);
        assert_eq!(nx_event_row_count(event.0), 2);
    }
    assert_eq!(fixed::<1>(&event, ROWS, 0, 2), [[255], [1]]);
    assert_eq!(fixed::<1>(&event, ROWS, 1, 2), [[0x80], [2]]);
    assert_eq!(
        fixed::<2>(&event, ROWS, 2, 2),
        [u16::MAX.to_ne_bytes(), 3_u16.to_ne_bytes()]
    );
    assert_eq!(
        fixed::<2>(&event, ROWS, 3, 2),
        [i16::MIN.to_ne_bytes(), 4_i16.to_ne_bytes()]
    );
    assert_eq!(
        fixed::<4>(&event, ROWS, 4, 2),
        [u32::MAX.to_ne_bytes(), 5_u32.to_ne_bytes()]
    );
    assert_eq!(
        fixed::<4>(&event, ROWS, 5, 2),
        [i32::MIN.to_ne_bytes(), 6_i32.to_ne_bytes()]
    );
    assert_eq!(
        fixed::<8>(&event, ROWS, 6, 2),
        [u64::MAX.to_ne_bytes(), 7_u64.to_ne_bytes()]
    );
    assert_eq!(
        fixed::<8>(&event, ROWS, 7, 2),
        [i64::MIN.to_ne_bytes(), 8_i64.to_ne_bytes()]
    );
    assert_eq!(
        fixed::<4>(&event, ROWS, 8, 2),
        [(-0.0_f32).to_ne_bytes(), 1.5_f32.to_ne_bytes()]
    );
    assert_eq!(
        fixed::<8>(&event, ROWS, 9, 2),
        [1_u64.to_ne_bytes(), 0.25_f64.to_ne_bytes()]
    );
    assert_eq!(fixed::<1>(&event, ROWS, 10, 2), [[1], [0]]);
    assert_eq!(
        fixed::<8>(&event, ROWS, 11, 2),
        [i64::MAX.to_ne_bytes(), 1_i64.to_ne_bytes()]
    );
    let value = u8::from(CellState::Value);
    let null = u8::from(CellState::Null);
    let redacted = u8::from(CellState::Redacted);
    assert_eq!(states(&event, ROWS, 12, 2), [value, null]);
    assert_eq!(states(&event, ROWS, 14, 2), [redacted, redacted]);
    assert_eq!(states(&event, BRANCH_KEY, 0, 1), [value]);
}

#[test]
fn a_redacted_fixed_width_cell_is_written_as_zero_bytes() {
    let mut schema = schema();
    schema.fields[0].sensitive = true;
    let limits = SessionLimits::DEFAULT;
    let mut batch =
        SubscriptionRowsEncoder::branched(handle(), &limits, |key| key.push_string("acme"))
            .assured("the key fits the limits");
    batch
        .push_row(|cells| {
            cells.push_redacted()?;
            cells.push_i8(0)?;
            cells.push_u16(0)?;
            cells.push_i16(0)?;
            cells.push_u32(0)?;
            cells.push_i32(0)?;
            cells.push_u64(0)?;
            cells.push_i64(0)?;
            cells.push_f32(0.0)?;
            cells.push_f64(0.0)?;
            cells.push_bool(false)?;
            cells.push_datetime(Timestamp::from_unix_nanos(0))?;
            cells.push_null()?;
            cells.push_bytes(&[])?;
            cells.push_redacted()?;
            cells.push_list(|_| Ok(()))
        })
        .assured("the row fits the limits");
    let frame = batch
        .finish()
        .assured("the batch fits the limits")
        .verify(&limits)
        .assured("an encoded frame verifies");
    let ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) =
        ServerMessage::decode(&frame).assured("an encoded frame decodes")
    else {
        panic!("a rows frame decodes as rows");
    };
    let event = Shared::new(SubscriptionEvent::Rows(SubscriptionRowsEvent {
        relay: name("typed"),
        schema: Arc::new(schema),
        rows,
    }));
    assert_eq!(fixed::<1>(&event, ROWS, 0, 1), [[0]]);
    assert_eq!(states(&event, ROWS, 0, 1), [u8::from(CellState::Redacted)]);
}

#[test]
fn string_and_bytes_columns_are_copied_with_offsets_and_borrowed_per_cell() {
    let event = Shared::new(rows_event(schema()));
    let mut data_len = 0;
    // SAFETY: the event is live; a null `data` asks only for the length.
    succeeded(unsafe {
        nx_event_column_varlen(
            event.0,
            ROWS,
            12,
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            0,
            &mut data_len,
        )
    });
    assert_eq!(data_len, "h\u{e9}\u{0}".len());
    let mut offsets = [0_u64; 3];
    let mut data = vec![0_u8; data_len];
    // SAFETY: the event is live and both buffers hold their lengths.
    succeeded(unsafe {
        nx_event_column_varlen(
            event.0,
            ROWS,
            12,
            offsets.as_mut_ptr(),
            offsets.len(),
            data.as_mut_ptr(),
            data.len(),
            &mut data_len,
        )
    });
    assert_eq!(offsets, [0, 4, 4]);
    assert_eq!(data, "h\u{e9}\u{0}".as_bytes());

    let mut value = ptr::null();
    let mut value_len = 0;
    // SAFETY: the event is live and the out-parameters are writable; the borrowed value lives
    // in the event's frame for as long as the event.
    let borrowed = unsafe {
        succeeded(nx_event_cell_varlen(
            event.0,
            ROWS,
            0,
            13,
            &mut value,
            &mut value_len,
        ));
        slice::from_raw_parts(value, value_len)
    };
    assert_eq!(borrowed, [0, 255, 0x80]);
    // SAFETY: as above, for the branch key's only cell.
    let key = unsafe {
        succeeded(nx_event_cell_varlen(
            event.0,
            BRANCH_KEY,
            0,
            0,
            &mut value,
            &mut value_len,
        ));
        slice::from_raw_parts(value, value_len)
    };
    assert_eq!(key, b"acme");

    let mut small = [0_u8; 1];
    // SAFETY: the event is live and every buffer holds its stated length.
    let kind = failure_kind(unsafe {
        nx_event_column_varlen(
            event.0,
            ROWS,
            12,
            offsets.as_mut_ptr(),
            offsets.len(),
            small.as_mut_ptr(),
            small.len(),
            &mut data_len,
        )
    });
    assert_eq!(kind, FailureKind::InvalidArgument);
    assert_eq!(
        data_len, 4,
        "a refused copy still reports the length it needs"
    );
    let mut short_offsets = [0_u64; 2];
    // SAFETY: as above.
    let kind = failure_kind(unsafe {
        nx_event_column_varlen(
            event.0,
            ROWS,
            12,
            short_offsets.as_mut_ptr(),
            short_offsets.len(),
            data.as_mut_ptr(),
            data.len(),
            &mut data_len,
        )
    });
    assert_eq!(kind, FailureKind::InvalidArgument);
}

#[test]
fn reading_a_column_as_the_wrong_type_or_shape_is_refused() {
    let event = Shared::new(rows_event(schema()));
    let mut values = [0_u8; 16];
    let mut value = ptr::null();
    let mut value_len = 0;
    let mut data_len = 0;
    // SAFETY: the event is live and every buffer holds its stated length.
    unsafe {
        let string_as_fixed =
            nx_event_column_fixed(event.0, ROWS, 12, values.as_mut_ptr().cast(), values.len());
        assert_eq!(failure_kind(string_as_fixed), FailureKind::Type);
        let list_as_fixed =
            nx_event_column_fixed(event.0, ROWS, 15, values.as_mut_ptr().cast(), values.len());
        assert_eq!(failure_kind(list_as_fixed), FailureKind::Type);
        let wrong_length =
            nx_event_column_fixed(event.0, ROWS, 0, values.as_mut_ptr().cast(), values.len());
        assert_eq!(failure_kind(wrong_length), FailureKind::InvalidArgument);
        let fixed_as_varlen = nx_event_column_varlen(
            event.0,
            ROWS,
            0,
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            0,
            &mut data_len,
        );
        assert_eq!(failure_kind(fixed_as_varlen), FailureKind::Type);
        let null_cell = nx_event_cell_varlen(event.0, ROWS, 1, 12, &mut value, &mut value_len);
        assert_eq!(failure_kind(null_cell), FailureKind::Type);
        let past_rows = nx_event_cell_varlen(event.0, ROWS, 2, 12, &mut value, &mut value_len);
        assert_eq!(failure_kind(past_rows), FailureKind::InvalidArgument);
        let past_key = nx_event_cell_varlen(event.0, BRANCH_KEY, 1, 0, &mut value, &mut value_len);
        assert_eq!(failure_kind(past_key), FailureKind::InvalidArgument);
        let past_columns = nx_event_column_states(event.0, ROWS, 16, values.as_mut_ptr(), 2);
        assert_eq!(failure_kind(past_columns), FailureKind::InvalidArgument);
        let unknown_part = nx_event_column_states(event.0, 3, 0, values.as_mut_ptr(), 2);
        assert_eq!(failure_kind(unknown_part), FailureKind::InvalidArgument);
        let wrong_states = nx_event_column_states(event.0, ROWS, 0, values.as_mut_ptr(), 3);
        assert_eq!(failure_kind(wrong_states), FailureKind::InvalidArgument);
        let missing_states = nx_event_column_states(event.0, ROWS, 0, ptr::null_mut(), 2);
        assert_eq!(failure_kind(missing_states), FailureKind::InvalidArgument);
        let missing_out =
            nx_event_cell_varlen(event.0, ROWS, 0, 12, ptr::null_mut(), &mut value_len);
        assert_eq!(failure_kind(missing_out), FailureKind::InvalidArgument);
    }
}

#[test]
fn a_batch_that_does_not_conform_to_its_schema_is_a_protocol_failure() {
    let mut schema = schema();
    schema.fields.pop();
    let SubscriptionEvent::Rows(rows) = rows_event(schema) else {
        panic!("the test builds a rows event");
    };
    let failure =
        Event::new(SubscriptionEvent::Rows(rows)).expect_err("the batch does not conform");
    assert_eq!(failure.kind(), FailureKind::Protocol);
}

#[test]
fn retained_references_keep_the_frame_until_the_last_one_is_released() {
    let event = Shared::new(rows_event(schema()));
    let mut frame = ptr::null();
    let mut frame_len = 0;
    // SAFETY: the event is live and the out-parameters are writable.
    succeeded(unsafe { nx_event_frame(event.0, &mut frame, &mut frame_len) });
    // SAFETY: the frame is borrowed from the live event.
    let copied = unsafe { slice::from_raw_parts(frame, frame_len) }.to_vec();
    assert_eq!(&copied[4..8], b"NXSM");
    // SAFETY: the event is live; the second reference is released on another thread while the
    // first is still read here.
    let second = unsafe { nx_event_retain(event.0) };
    assert_eq!(
        second, event.0,
        "a retained reference addresses the same event"
    );
    let second = Shared(second);
    std::thread::spawn(move || drop(second))
        .join()
        .assured("releasing on another thread does not panic");
    let mut again = ptr::null();
    let mut again_len = 0;
    // SAFETY: the first reference is still live.
    succeeded(unsafe { nx_event_frame(event.0, &mut again, &mut again_len) });
    // SAFETY: as above.
    assert_eq!(
        unsafe { slice::from_raw_parts(again, again_len) },
        copied.as_slice()
    );

    let mut schema = ptr::null_mut();
    // SAFETY: the event is live and `schema` is writable.
    succeeded(unsafe { nx_event_schema(event.0, &mut schema) });
    let mut count = 0;
    // SAFETY: the schema is live and `count` is writable; the schema is released once.
    unsafe {
        succeeded(nx_schema_field_count(schema, ROWS, &mut count));
        nx_schema_free(schema);
    }
    assert_eq!(count, 16);
    // SAFETY: releasing null is allowed and does nothing.
    unsafe { nx_event_release(ptr::null_mut()) };
}

#[test]
fn every_event_kind_reports_its_subscription_and_count() {
    let events = [
        (
            SubscriptionEvent::DeliveryLost(SubscriptionDeliveryLost {
                subscription: handle(),
                dropped_rows: NonZeroU64::new(3).assured("three is non-zero"),
            }),
            EventKind::DeliveryLost,
            3,
        ),
        (
            SubscriptionEvent::RowsSkipped(SubscriptionRowsSkipped {
                subscription: handle(),
                cause: RowsSkippedCause::FilterFailed,
                skipped_rows: NonZeroU64::new(4).assured("four is non-zero"),
                message: "the filter failed".to_string(),
            }),
            EventKind::RowsSkipped,
            4,
        ),
        (
            SubscriptionEvent::Ended(SubscriptionEnded {
                subscription: handle(),
                reason: SubscriptionEndReason::RelayRemoved,
                message: "the relay was removed".to_string(),
            }),
            EventKind::Ended,
            0,
        ),
        (
            SubscriptionEvent::Interrupted(SubscriptionInterruption {
                subscription: handle(),
            }),
            EventKind::Interrupted,
            0,
        ),
        (
            SubscriptionEvent::ConsumerOverflow(handle()),
            EventKind::ConsumerOverflow,
            0,
        ),
    ];
    for (event, kind, count) in events {
        let event = Shared::new(event);
        let mut name = ptr::null();
        let mut name_len = 0;
        let mut generation = 0;
        let mut frame = ptr::null();
        let mut frame_len = 0;
        let mut schema = ptr::null_mut();
        // SAFETY: the event is live and every out-parameter is writable.
        unsafe {
            assert_eq!(nx_event_kind_of(event.0), kind);
            assert_eq!(nx_event_row_count(event.0), count);
            nx_event_subscription(event.0, &mut name, &mut name_len, &mut generation);
            assert_eq!(slice::from_raw_parts(name, name_len), b"watch");
            assert_eq!(generation, 7);
            assert_eq!(
                failure_kind(nx_event_frame(event.0, &mut frame, &mut frame_len)),
                FailureKind::Type
            );
            assert_eq!(
                failure_kind(nx_event_schema(event.0, &mut schema)),
                FailureKind::Type
            );
        }
    }
}

fn outcome(disposition: CommandDisposition, subscription: bool) -> Outcome {
    let opened = SubscriptionOpened {
        subscription: handle(),
        domain: name("tenant"),
        relay: name("typed"),
        subscription_type: SubscriptionType::Row,
        schema: schema(),
    };
    Outcome::new(CommandOutcome {
        execution_reference: Some(
            CommandExecutionReference::parse("reference-1").assured("a valid reference"),
        ),
        origin: Some(OutcomeOrigin::Executed),
        disposition,
        message: "done".to_string(),
        diagnostics: vec![
            Diagnostic {
                message: "here".to_string(),
                span: Some(SourceSpan::new(3, 9).assured("an ordered span")),
            },
            Diagnostic {
                message: "nowhere".to_string(),
                span: None,
            },
        ],
        statements: Vec::new(),
        transaction: None,
        transaction_admission: None,
        inspection: None,
        subscription: subscription.then(|| Box::new(opened)),
        resource_upload: None,
    })
}

#[test]
fn an_outcome_reports_its_disposition_message_diagnostics_and_subscription() {
    let outcome = Box::into_raw(Box::new(outcome(
        CommandDisposition::Completed {
            already_existed: false,
        },
        true,
    )));
    let mut text = ptr::null();
    let mut text_len = 0;
    let mut has_span = false;
    let mut start = 0;
    let mut end = 0;
    let mut generation = 0;
    let mut schema = ptr::null_mut();
    // SAFETY: the outcome is live until it is freed at the end, and every out-parameter is
    // writable.
    unsafe {
        assert_eq!(nx_outcome_disposition(outcome), Disposition::Completed);
        nx_outcome_message(outcome, &mut text, &mut text_len);
        assert_eq!(slice::from_raw_parts(text, text_len), b"done");
        assert!(nx_outcome_execution_reference(
            outcome,
            &mut text,
            &mut text_len
        ));
        assert_eq!(slice::from_raw_parts(text, text_len), b"reference-1");
        assert_eq!(nx_outcome_diagnostic_count(outcome), 2);
        succeeded(nx_outcome_diagnostic(
            outcome,
            0,
            &mut text,
            &mut text_len,
            &mut has_span,
            &mut start,
            &mut end,
        ));
        assert_eq!((has_span, start, end), (true, 3, 9));
        succeeded(nx_outcome_diagnostic(
            outcome,
            1,
            &mut text,
            &mut text_len,
            &mut has_span,
            &mut start,
            &mut end,
        ));
        assert!(!has_span);
        assert_eq!(
            failure_kind(nx_outcome_diagnostic(
                outcome,
                2,
                &mut text,
                &mut text_len,
                &mut has_span,
                &mut start,
                &mut end,
            )),
            FailureKind::InvalidArgument
        );
        assert!(nx_outcome_subscription(
            outcome,
            &mut text,
            &mut text_len,
            &mut generation
        ));
        assert_eq!(
            (slice::from_raw_parts(text, text_len), generation),
            (&b"watch"[..], 7)
        );
        succeeded(nx_outcome_schema(outcome, &mut schema));
        read_schema(schema);
        nx_schema_free(schema);
        nx_outcome_free(outcome);
    }
}

/// Reads every field of the test schema through the ABI.
///
/// # Safety
///
/// `schema` is a live schema.
unsafe fn read_schema(schema: *mut Schema) {
    let mut name = ptr::null();
    let mut name_len = 0;
    let mut field_type = FieldType::U8;
    let mut nullable = false;
    let mut sensitive = false;
    let mut count = 0;
    // SAFETY: the caller guarantees a live schema, and every out-parameter is writable.
    unsafe {
        assert!(nx_schema_branch(schema, &mut name, &mut name_len));
        assert_eq!(slice::from_raw_parts(name, name_len), b"tenants");
        succeeded(nx_schema_field_count(schema, BRANCH_KEY, &mut count));
        assert_eq!(count, 1);
        succeeded(nx_schema_field(
            schema,
            ROWS,
            12,
            &mut name,
            &mut name_len,
            &mut field_type,
            &mut nullable,
            &mut sensitive,
        ));
        assert_eq!(
            (
                slice::from_raw_parts(name, name_len),
                field_type,
                nullable,
                sensitive
            ),
            (&b"text"[..], FieldType::String, true, false)
        );
        succeeded(nx_schema_field(
            schema,
            ROWS,
            15,
            &mut name,
            &mut name_len,
            &mut field_type,
            &mut nullable,
            &mut sensitive,
        ));
        assert_eq!(field_type, FieldType::List);
        assert_eq!(
            failure_kind(nx_schema_field(
                schema,
                ROWS,
                16,
                &mut name,
                &mut name_len,
                &mut field_type,
                &mut nullable,
                &mut sensitive,
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_schema_field_count(schema, 0, &mut count)),
            FailureKind::InvalidArgument
        );
    }
}

#[test]
fn an_outcome_without_a_subscription_has_no_schema() {
    let outcome = Box::into_raw(Box::new(outcome(CommandDisposition::Failed, false)));
    let mut name = ptr::null();
    let mut name_len = 0;
    let mut generation = 0;
    let mut schema = ptr::null_mut();
    // SAFETY: the outcome is live until it is freed, and every out-parameter is writable.
    unsafe {
        assert_eq!(nx_outcome_disposition(outcome), Disposition::Failed);
        assert!(!nx_outcome_subscription(
            outcome,
            &mut name,
            &mut name_len,
            &mut generation
        ));
        assert_eq!(
            failure_kind(nx_outcome_schema(outcome, &mut schema)),
            FailureKind::InvalidArgument
        );
        nx_outcome_free(outcome);
        nx_outcome_free(ptr::null_mut());
    }
}

#[test]
fn every_command_disposition_has_the_header_value() {
    let cases = [
        (CommandDisposition::Failed, Disposition::Failed),
        (
            CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
            Disposition::NotLeader,
        ),
        (
            CommandDisposition::TransactionDetached {
                transaction_id: "t".to_string(),
            },
            Disposition::TransactionDetached,
        ),
        (
            CommandDisposition::TransactionTakenOver {
                transaction_id: "t".to_string(),
            },
            Disposition::TransactionTakenOver,
        ),
        (
            CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::LeadershipLost),
            Disposition::OutcomeUnknown,
        ),
        (
            CommandDisposition::ExecutionReferenceConflict(
                nervix_client_core::ExecutionReferenceConflict::Content,
            ),
            Disposition::ExecutionReferenceConflict,
        ),
        (
            CommandDisposition::ExecutionReferenceExpired,
            Disposition::ExecutionReferenceExpired,
        ),
    ];
    for (disposition, expected) in cases {
        assert_eq!(Disposition::from(&disposition), expected);
    }
    let without_reference = Outcome::new(CommandOutcome {
        execution_reference: None,
        ..outcome(CommandDisposition::Failed, false).command().clone()
    });
    let mut text = ptr::null();
    let mut text_len = 0;
    // SAFETY: the outcome is live and the out-parameters are writable.
    assert!(!unsafe {
        nx_outcome_execution_reference(&without_reference, &mut text, &mut text_len)
    });
}

#[test]
fn client_errors_are_classified_and_keep_their_causes() {
    let reference = CommandExecutionReference::parse("reference-2").assured("a valid reference");
    let uncertain = crate::Failure::from(ClientError::UncertainCommand {
        reference: reference.clone(),
        source: Box::new(ClientError::RetryDeadline),
    });
    assert_eq!(uncertain.kind(), FailureKind::Uncertain);
    assert_eq!(uncertain.execution_reference(), Some(&reference));
    assert!(
        uncertain
            .message()
            .contains("session retry deadline expired")
    );
    let cases = [
        (ClientError::SessionClosed, FailureKind::Closed),
        (
            ClientError::InvalidServerEndpoint,
            FailureKind::InvalidArgument,
        ),
        (ClientError::NoActiveDomain, FailureKind::InvalidArgument),
        (ClientError::TlsRequired, FailureKind::Connect),
        (ClientError::SessionOpenDeadline, FailureKind::Connect),
        (
            ClientError::RequestDeadline {
                request: nervix_client_core::RequestKind::Command,
            },
            FailureKind::Deadline,
        ),
        (
            ClientError::EventOverflow {
                stream: nervix_client_core::EventStreamKind::Subscription,
            },
            FailureKind::Overflow,
        ),
        (
            ClientError::RequestRejected {
                request: nervix_client_core::RequestKind::Command,
                rejection: RequestRejection::ServerBusy,
                field: None,
                message: "busy".to_string(),
            },
            FailureKind::Rejected,
        ),
        (
            ClientError::UnexpectedReply {
                request: nervix_client_core::RequestKind::Command,
            },
            FailureKind::Protocol,
        ),
    ];
    for (error, kind) in cases {
        let failure = crate::Failure::from(error);
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.execution_reference(), None);
    }
    let handed_out = Box::into_raw(Box::new(uncertain));
    let mut text = ptr::null();
    let mut text_len = 0;
    // SAFETY: the failure is live until it is freed, and the out-parameters are writable.
    unsafe {
        assert!(nx_error_execution_reference(
            handed_out,
            &mut text,
            &mut text_len
        ));
        assert_eq!(slice::from_raw_parts(text, text_len), b"reference-2");
        nx_error_free(handed_out);
        nx_error_free(ptr::null_mut());
    }
    let unnamed = Box::into_raw(Box::new(crate::Failure::from(ClientError::SessionClosed)));
    // SAFETY: as above.
    unsafe {
        assert!(!nx_error_execution_reference(
            unnamed,
            &mut text,
            &mut text_len
        ));
        nx_error_free(unnamed);
    }
}

#[test]
fn a_token_bounds_a_call_by_cancellation_and_by_deadline() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .assured("a test runtime starts");
    let forever = || std::future::pending::<Result<(), crate::Failure>>();

    let cancelled = Cancel::default();
    cancelled.trigger();
    let result = runtime.block_on(cancelled.bound(async { Ok(()) }));
    assert_eq!(
        result
            .expect_err("a triggered token wins over a ready result")
            .kind(),
        FailureKind::Cancelled
    );

    let expiring = Cancel::with_deadline(Duration::from_millis(10)).assured("a short deadline");
    let result = runtime.block_on(expiring.bound(forever()));
    assert_eq!(
        result.expect_err("the deadline ends the wait").kind(),
        FailureKind::Deadline
    );

    let open = Cancel::default();
    assert!(runtime.block_on(open.bound(async { Ok(()) })).is_ok());

    let too_long = Cancel::with_deadline(Duration::from_secs(2 * 24 * 60 * 60));
    assert_eq!(
        too_long
            .expect_err("a deadline beyond a day is refused")
            .kind(),
        FailureKind::InvalidArgument
    );

    let token = nx_cancel_new();
    let mut expiring = ptr::null_mut();
    // SAFETY: the tokens are live until they are freed, and `expiring` is writable.
    unsafe {
        nx_cancel_trigger(token);
        nx_cancel_free(token);
        succeeded(nx_cancel_with_deadline(1_000, &mut expiring));
        nx_cancel_free(expiring);
        assert_eq!(
            failure_kind(nx_cancel_with_deadline(1_000, ptr::null_mut())),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_cancel_with_deadline(u64::MAX, &mut expiring)),
            FailureKind::InvalidArgument
        );
        nx_cancel_free(ptr::null_mut());
    }
}

#[test]
fn connecting_refuses_invalid_arguments_and_reports_an_unreachable_server() {
    let server = "http://127.0.0.1:1";
    let not_utf8 = [0xFF_u8];
    let user = "user";
    let mut session = ptr::null_mut();
    // SAFETY: every text argument addresses its length in readable bytes, and `session` is
    // writable; no call succeeds, so there is no session to free.
    unsafe {
        let missing_server = nx_session_connect(
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            &mut session,
        );
        assert_eq!(failure_kind(missing_server), FailureKind::InvalidArgument);
        let invalid_text = nx_session_connect(
            not_utf8.as_ptr(),
            not_utf8.len(),
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            &mut session,
        );
        assert_eq!(failure_kind(invalid_text), FailureKind::InvalidArgument);
        let half_credentials = nx_session_connect(
            server.as_ptr(),
            server.len(),
            ptr::null(),
            0,
            user.as_ptr(),
            user.len(),
            ptr::null(),
            0,
            ptr::null(),
            &mut session,
        );
        assert_eq!(failure_kind(half_credentials), FailureKind::InvalidArgument);
        let domain = "not a domain";
        let invalid_domain = nx_session_connect(
            server.as_ptr(),
            server.len(),
            domain.as_ptr(),
            domain.len(),
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            &mut session,
        );
        assert_eq!(failure_kind(invalid_domain), FailureKind::InvalidArgument);
        let missing_out = nx_session_connect(
            server.as_ptr(),
            server.len(),
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            ptr::null_mut(),
        );
        assert_eq!(failure_kind(missing_out), FailureKind::InvalidArgument);
        let unreachable = nx_session_connect(
            server.as_ptr(),
            server.len(),
            ptr::null(),
            0,
            user.as_ptr(),
            user.len(),
            user.as_ptr(),
            user.len(),
            ptr::null(),
            &mut session,
        );
        assert_eq!(failure_kind(unreachable), FailureKind::Connect);
        nx_session_free(ptr::null_mut());
        nx_execution_free(ptr::null_mut());
    }
}
