//! Wire Row fidelity uses production views, including NaN payloads, nulls and redacted cells.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current frame cases and their complete representation assertions.
//! - **Depends on.** Production wire codecs and vocabulary test generators.
//! - **Must not know.** Runtime Arrow arrays, service dispatch or live external systems.

use std::num::NonZeroU32;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{ParseAsType, SchemaField};

use super::WireValues;
use crate::{tests::fixtures::name, *};

fn field(index: usize, ty: ParseAsType) -> SchemaField {
    SchemaField {
        name: name(&format!("field_{index}")),
        ty,
        optional: false,
        sensitive: false,
    }
}

fn check(bytes: &[u8]) {
    let mut values = WireValues::new(bytes);
    let limits = SessionLimits::DEFAULT;
    let handle = values.subscription();
    let text = values.arbitrary.string();
    let raw = values.bytes(0);
    let integer = values.arbitrary.entropy().any_u64();
    let signed = values.arbitrary.entropy().any_i64();
    let bits32 = u32::from_le_bytes(values.digest());
    let bits32 = values.arbitrary.entropy().pick([
        bits32,
        0x8000_0000,
        0x7f80_0000,
        0xff80_0000,
        0x7fc0_0001,
        0x7fa0_0001,
        1,
        0x7f7f_ffff,
    ]);
    let bits64 = u64::from_le_bytes(values.digest());
    let bits64 = values.arbitrary.entropy().pick([
        bits64,
        0x8000_0000_0000_0000,
        0x7ff0_0000_0000_0000,
        0xfff0_0000_0000_0000,
        0x7ff8_0000_0000_0001,
        0x7ff4_0000_0000_0001,
        1,
        0x7fef_ffff_ffff_ffff,
    ]);
    let timestamp = values.timestamp();
    let flag = values.arbitrary.entropy().flag();
    let low8 = integer.to_le_bytes()[0];
    let low16 = u16::from_le_bytes([integer.to_le_bytes()[0], integer.to_le_bytes()[1]]);
    let low32 = u32::from_le_bytes(
        integer.to_le_bytes()[..4]
            .try_into()
            .assured("four-byte prefix"),
    );
    let expected = [
        CellView::U8(low8),
        CellView::I8(low8.cast_signed()),
        CellView::U16(low16),
        CellView::I16(low16.cast_signed()),
        CellView::U32(low32),
        CellView::I32(low32.cast_signed()),
        CellView::U64(integer),
        CellView::I64(signed),
        CellView::F32(f32::from_bits(bits32)),
        CellView::F64(f64::from_bits(bits64)),
        CellView::Bool(flag),
        CellView::String(&text),
        CellView::Bytes(&raw),
        CellView::Datetime(timestamp),
        CellView::Null,
        CellView::Redacted,
    ];
    let types = [
        ParseAsType::U8,
        ParseAsType::I8,
        ParseAsType::U16,
        ParseAsType::I16,
        ParseAsType::U32,
        ParseAsType::I32,
        ParseAsType::U64,
        ParseAsType::I64,
        ParseAsType::F32,
        ParseAsType::F64,
        ParseAsType::Bool,
        ParseAsType::String,
        ParseAsType::Bytes,
        ParseAsType::Datetime,
        ParseAsType::String,
        ParseAsType::String,
    ];
    let mut fields: Vec<_> = types
        .into_iter()
        .enumerate()
        .map(|(index, ty)| field(index, ty))
        .collect();
    fields[14].optional = true;
    fields[15].sensitive = true;
    fields.push(field(
        16,
        ParseAsType::Array {
            element: Box::new(ParseAsType::I64),
            len: NonZeroU32::new(2).assured("a two-element array"),
        },
    ));
    fields.push(field(
        17,
        ParseAsType::Vec {
            element: Box::new(ParseAsType::Vec {
                element: Box::new(ParseAsType::U64),
            }),
        },
    ));
    let branched = values.arbitrary.entropy().flag();
    let schema = RowSchema {
        fields,
        branch: if branched {
            Some(
                RowBranch::new(name("by_tenant"), vec![field(0, ParseAsType::String)])
                    .assured("one branch field"),
            )
        } else {
            None
        },
    };
    let mut encoder = if branched {
        SubscriptionRowsEncoder::branched(handle.clone(), &limits, |writer| {
            writer.push_string(&text)
        })
        .assured("bounded branch fits")
    } else {
        SubscriptionRowsEncoder::unbranched(handle.clone(), &limits)
            .assured("unbranched frame fits")
    };
    let rows = values.arbitrary.entropy().count(3) + 1;
    for _ in 0..rows {
        encoder
            .push_row(|writer| {
                writer.push_u8(low8)?;
                writer.push_i8(low8.cast_signed())?;
                writer.push_u16(low16)?;
                writer.push_i16(low16.cast_signed())?;
                writer.push_u32(low32)?;
                writer.push_i32(low32.cast_signed())?;
                writer.push_u64(integer)?;
                writer.push_i64(signed)?;
                writer.push_f32(f32::from_bits(bits32))?;
                writer.push_f64(f64::from_bits(bits64))?;
                writer.push_bool(flag)?;
                writer.push_string(&text)?;
                writer.push_bytes(&raw)?;
                writer.push_datetime(timestamp)?;
                writer.push_null()?;
                writer.push_redacted()?;
                writer.push_list(|items| {
                    items.push_i64(signed)?;
                    items.push_i64(i64::MAX)
                })?;
                writer.push_list(|items| {
                    items.push_list(|inner| inner.push_u64(integer))?;
                    items.push_list(|_| Ok(()))
                })
            })
            .assured("bounded typed row fits");
    }
    let frame = encoder
        .finish()
        .assured("bounded rows fit")
        .verify(&limits)
        .assured("typed rows verify");
    let ServerMessage::Event(ServerEvent::SubscriptionRows(decoded)) =
        ServerMessage::decode(&frame).assured("valid Row frames never skip failures")
    else {
        panic!("rows keep their variant");
    };
    let retained = decoded.clone();
    drop(frame);
    drop(decoded);
    assert_eq!(retained.subscription(), &handle);
    let batch = retained.batch();
    batch
        .conform(&schema)
        .assured("each exact cell agrees with the opening schema");
    assert_eq!(batch.len(), rows);
    assert_eq!(batch.is_empty(), rows == 0);
    if branched {
        assert_eq!(
            batch
                .branch_key()
                .assured("branch remains present")
                .iter()
                .collect::<Vec<_>>(),
            [CellView::String(&text)]
        );
    } else {
        assert!(batch.branch_key().is_none());
    }
    for row in batch.rows() {
        assert_eq!(row.len(), 18);
        for (index, original) in expected.iter().enumerate() {
            assert_eq!(
                row.get(index).assured("the complete row has this cell"),
                *original
            );
        }
        let CellView::List(array) = row.get(16).assured("the array cell exists") else {
            panic!("an array remains a list view");
        };
        assert_eq!(
            array.iter().collect::<Vec<_>>(),
            [CellView::I64(signed), CellView::I64(i64::MAX)]
        );
        let CellView::List(vector) = row.get(17).assured("the vector cell exists") else {
            panic!("a vector remains a list view");
        };
        assert_eq!(vector.len(), 2);
        let CellView::List(inner) = vector.get(0).assured("the first nested list exists") else {
            panic!("a nested vector remains a list");
        };
        assert_eq!(inner.iter().collect::<Vec<_>>(), [CellView::U64(integer)]);
        let CellView::List(empty) = vector.get(1).assured("the empty nested list exists") else {
            panic!("empty and null lists differ");
        };
        assert!(empty.is_empty());
    }
    assert!(batch.row(rows).is_none());
}

#[test]
fn bolero_row_views_keep_complete_cells_and_buffer_ownership() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            check(bytes);
        });
}

#[test]
fn row_boundaries_include_every_cell_variant_and_float_bits() {
    for seed in [0, 1, 2, 3, 4, 5, 0x80, 0xff] {
        check(&[seed; 512]);
    }
}
