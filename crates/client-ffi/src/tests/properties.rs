//! Generated shared-binding access agrees with every production wire cell and retains its bytes.
//!
//! Layer: test harness.
//! - **Owns.** Shared binding fidelity and reference retention over generated Row events.
//! - **Depends on.** The production binding, wire views and vocabulary test generators.
//! - **Must not know.** Runtime Arrow arrays or session transport scheduling.

use std::{ptr, slice};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{
    SubscriptionEvent, SubscriptionRowsEvent,
    wire::{CellView, ServerEvent, ServerMessage, SessionLimits, SubscriptionRowsEncoder},
};
use nervix_models::Timestamp;
use triomphe::Arc;

use super::{Shared, handle, name, schema, succeeded};
use crate::{Event, nx_event_frame, nx_event_release, nx_event_retain, schema::Part};

fn check(bytes: &[u8]) {
    let mut arbitrary =
        nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Vocabulary);
    let integer = arbitrary.entropy().any_u64();
    let signed = arbitrary.entropy().any_i64();
    let low8 = integer.to_le_bytes()[0];
    let low16 = u16::from_le_bytes(
        integer.to_le_bytes()[..2]
            .try_into()
            .assured("two-byte prefix"),
    );
    let low32 = u32::from_le_bytes(
        integer.to_le_bytes()[..4]
            .try_into()
            .assured("four-byte prefix"),
    );
    let f32_bits = u32::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()));
    let f64_bits = u64::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()));
    let f32_bits = arbitrary.entropy().pick([
        f32_bits,
        0x8000_0000,
        0x7f80_0000,
        0xff80_0000,
        0x7fc0_0001,
        0x7fa0_0001,
        1,
        0x7f7f_ffff,
    ]);
    let f64_bits = arbitrary.entropy().pick([
        f64_bits,
        0x8000_0000_0000_0000,
        0x7ff0_0000_0000_0000,
        0xfff0_0000_0000_0000,
        0x7ff8_0000_0000_0001,
        0x7ff4_0000_0000_0001,
        1,
        0x7fef_ffff_ffff_ffff,
    ]);
    let text = arbitrary.string();
    let raw: Vec<_> = (0..arbitrary.entropy().count(64))
        .map(|_| arbitrary.entropy().byte())
        .collect();
    let count = arbitrary.entropy().count(4) + 1;
    let row_schema = schema();
    let mut batch =
        SubscriptionRowsEncoder::branched(handle(), &SessionLimits::DEFAULT, |writer| {
            writer.push_string(&text)
        })
        .assured("bounded key fits");
    for row in 0..count {
        batch
            .push_row(|writer| {
                writer.push_u8(low8)?;
                writer.push_i8(low8.cast_signed())?;
                writer.push_u16(low16)?;
                writer.push_i16(low16.cast_signed())?;
                writer.push_u32(low32)?;
                writer.push_i32(low32.cast_signed())?;
                writer.push_u64(integer)?;
                writer.push_i64(signed)?;
                writer.push_f32(f32::from_bits(f32_bits))?;
                writer.push_f64(f64::from_bits(f64_bits))?;
                writer.push_bool(row.is_multiple_of(2))?;
                writer.push_datetime(Timestamp::from_unix_nanos(signed))?;
                if row.is_multiple_of(2) {
                    writer.push_string(&text)?;
                } else {
                    writer.push_null()?;
                }
                writer.push_bytes(&raw)?;
                writer.push_redacted()?;
                writer.push_list(|items| items.push_i64(signed))
            })
            .assured("bounded row fits");
    }
    let frame = batch
        .finish()
        .assured("bounded frame fits")
        .verify(&SessionLimits::DEFAULT)
        .assured("frame verifies");
    let ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) =
        ServerMessage::decode(&frame).assured("valid rows decode")
    else {
        panic!("rows retain their variant");
    };
    let expected_frame = frame.bytes().to_vec();
    drop(frame);
    let expected = rows.clone();
    let event = Shared::new(SubscriptionEvent::Rows(SubscriptionRowsEvent {
        relay: name("typed"),
        schema: Arc::new(row_schema.clone()),
        rows,
    }));
    // SAFETY: Shared owns a live Event for this entire borrow.
    let view: &Event = unsafe { &*event.0 };
    assert_eq!(
        view.schema()
            .assured("rows have a schema")
            .fields(Part::Rows),
        row_schema.fields
    );
    assert_eq!(
        view.schema().assured("rows have a schema").branch(),
        Some("tenants")
    );
    assert_eq!(
        view.schema()
            .assured("rows have a schema")
            .fields(Part::BranchKey),
        row_schema
            .branch
            .as_ref()
            .assured("the generated schema has a branch")
            .fields()
    );
    for part in [Part::Rows, Part::BranchKey] {
        let fields = view.schema().assured("rows have a schema");
        for (column, _) in fields.fields(part).iter().enumerate() {
            let cells: Vec<_> = match part {
                Part::Rows => expected
                    .batch()
                    .rows()
                    .map(|row| row.get(column).assured("declared cell exists"))
                    .collect(),
                Part::BranchKey => vec![
                    expected
                        .batch()
                        .branch_key()
                        .assured("concrete branch exists")
                        .get(column)
                        .assured("key cell exists"),
                ],
            };
            let mut states = vec![0; cells.len()];
            view.column_states(part, column, &mut states)
                .assured("states fit their exact buffer");
            // The public header declares Value=1, Null=2 and Redacted=3. Use that contract
            // directly rather than the binding's conversion under test.
            let expected_states: Vec<_> = cells
                .iter()
                .map(|cell| match cell {
                    CellView::Null => 2,
                    CellView::Redacted => 3,
                    _ => 1,
                })
                .collect();
            assert_eq!(states, expected_states);
            if (12..=14).contains(&column) || matches!(part, Part::BranchKey) {
                let mut content = Vec::new();
                let mut offsets = vec![0];
                for cell in &cells {
                    match cell {
                        CellView::String(text) => content.extend_from_slice(text.as_bytes()),
                        CellView::Bytes(bytes) => content.extend_from_slice(bytes),
                        CellView::Null | CellView::Redacted => {}
                        _ => panic!("variable column has its declared type"),
                    }
                    offsets.push(u64::try_from(content.len()).assured("bounded content"));
                }
                assert_eq!(
                    view.column_varlen_len(part, column)
                        .assured("variable column is readable"),
                    content.len()
                );
                let mut copied = vec![0; content.len()];
                let mut actual_offsets = vec![0; offsets.len()];
                view.column_varlen(part, column, &mut actual_offsets, &mut copied)
                    .assured("exact buffers fit");
                assert_eq!(copied, content);
                assert_eq!(actual_offsets, offsets);
            } else if column < 12 {
                let mut expected_bytes = Vec::new();
                for cell in cells {
                    match cell {
                        CellView::U8(value) => expected_bytes.push(value),
                        CellView::I8(value) => {
                            expected_bytes.extend_from_slice(&value.to_ne_bytes())
                        }
                        CellView::U16(value) => {
                            expected_bytes.extend_from_slice(&value.to_ne_bytes())
                        }
                        CellView::I16(value) => {
                            expected_bytes.extend_from_slice(&value.to_ne_bytes())
                        }
                        CellView::U32(value) => {
                            expected_bytes.extend_from_slice(&value.to_ne_bytes())
                        }
                        CellView::I32(value) => {
                            expected_bytes.extend_from_slice(&value.to_ne_bytes())
                        }
                        CellView::U64(value) => {
                            expected_bytes.extend_from_slice(&value.to_ne_bytes())
                        }
                        CellView::I64(value) => {
                            expected_bytes.extend_from_slice(&value.to_ne_bytes())
                        }
                        CellView::F32(value) => {
                            expected_bytes.extend_from_slice(&value.to_bits().to_ne_bytes())
                        }
                        CellView::F64(value) => {
                            expected_bytes.extend_from_slice(&value.to_bits().to_ne_bytes())
                        }
                        CellView::Bool(value) => expected_bytes.push(u8::from(value)),
                        CellView::Datetime(value) => {
                            expected_bytes.extend_from_slice(&value.unix_nanos().to_ne_bytes())
                        }
                        _ => panic!("a fixed column has its declared exact type"),
                    }
                }
                let mut copied = vec![0; expected_bytes.len()];
                view.column_fixed(part, column, &mut copied)
                    .assured("fixed buffer fits");
                assert_eq!(copied, expected_bytes);
            } else {
                for cell in cells {
                    let CellView::List(items) = cell else {
                        panic!("the declared vector retains its cell type");
                    };
                    assert_eq!(items.iter().collect::<Vec<_>>(), [CellView::I64(signed)]);
                }
                let error = view
                    .column_varlen_len(part, column)
                    .expect_err("vectors have no scalar variable-width column view");
                assert_eq!(error.kind(), crate::FailureKind::Type);
            }
        }
    }
    drop(expected);
    // SAFETY: the original reference is live. Retain creates one additional reference, which
    // survives dropping Shared and is released exactly once after the last borrowed byte read.
    let retained = unsafe { nx_event_retain(event.0) };
    drop(event);
    let mut pointer = ptr::null();
    let mut length = 0;
    succeeded(unsafe { nx_event_frame(retained, &mut pointer, &mut length) });
    assert_eq!(
        unsafe { slice::from_raw_parts(pointer, length) },
        expected_frame
    );
    unsafe { nx_event_release(retained) };
}

#[test]
fn bolero_shared_binding_keeps_complete_cells_and_retained_frames() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            check(bytes);
        });
}

#[test]
fn binding_boundaries_replay_every_column_and_retained_frames() {
    for seed in [0, 1, 2, 3, 4, 5, 0x80, 0xff] {
        check(&[seed; 512]);
    }
}
