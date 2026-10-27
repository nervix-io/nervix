//! Evidence for the current Arrow-to-BSON boundary.
//!
//! Layer: test harness.
//! - **Owns.** Exact scalar representation and complete mapped document checks.
//! - **Depends on.** The production BSON column owner, Arrow arrays and BSON values.
//! - **Must not know.** Graph execution, client pools or live servers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the BSON boundary's bounded evidence")
)]

use nervix_primitives::sync::StdArc;

use super::*;

#[test]
fn mapped_bson_uses_exact_integer_widths_and_native_dates() {
    let cases: Vec<(ArrayRef, Bson)> = vec![
        (
            StdArc::new(BooleanArray::from(vec![true])),
            Bson::Boolean(true),
        ),
        (StdArc::new(Int8Array::from(vec![-8])), Bson::Int32(-8)),
        (StdArc::new(Int16Array::from(vec![-16])), Bson::Int32(-16)),
        (
            StdArc::new(Int32Array::from(vec![i32::MIN])),
            Bson::Int32(i32::MIN),
        ),
        (
            StdArc::new(UInt8Array::from(vec![u8::MAX])),
            Bson::Int32(i32::from(u8::MAX)),
        ),
        (
            StdArc::new(UInt16Array::from(vec![u16::MAX])),
            Bson::Int32(i32::from(u16::MAX)),
        ),
        (
            StdArc::new(Int64Array::from(vec![i64::MIN])),
            Bson::Int64(i64::MIN),
        ),
        (
            StdArc::new(UInt32Array::from(vec![u32::MAX])),
            Bson::Int64(i64::from(u32::MAX)),
        ),
        (StdArc::new(UInt64Array::from(vec![42])), Bson::Int64(42)),
        (
            StdArc::new(TimestampNanosecondArray::from(vec![1_234_567_890])),
            Bson::DateTime(mongodb::bson::DateTime::from_millis(1234)),
        ),
        (
            StdArc::new(TimestampNanosecondArray::from(vec![-1])),
            Bson::DateTime(mongodb::bson::DateTime::from_millis(-1)),
        ),
    ];
    for (array, expected) in cases {
        let column = MappedBsonColumn::new(&array).assured("a declared scalar column maps");
        let actual = column
            .value(0, "value")
            .assured("the scalar is representable");
        assert_eq!(actual, expected);
    }
}

#[derive(Debug, Clone, bolero::TypeGenerator)]
struct BsonCase {
    boolean: bool,
    mapped_identifier: bool,
    tiny_signed: i8,
    short_signed: i16,
    tiny_unsigned: u8,
    small_signed: i32,
    small_unsigned: u16,
    large_signed: i64,
    large_unsigned: u32,
    unsigned: u64,
    float_bits: u64,
    small_float_bits: u32,
    instant: i64,
    octets: [u8; 8],
}

#[test]
fn bolero_mapped_documents_preserve_bson_types_values_nulls_and_sizes() {
    use arrow_array::types::Int32Type;
    use arrow_schema::{Field, Schema};

    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .with_type::<BsonCase>()
        .for_each(|case| {
            let unsigned = case.unsigned >> 1;
            let number = f64::from_bits(case.float_bits);
            let small_number = f32::from_bits(case.small_float_bits);
            let text = format!("value-{:x}", case.unsigned);
            let list = ListArray::from_iter_primitive::<Int32Type, _, _>([
                Some(vec![Some(case.small_signed), None]),
                None,
            ]);
            let values: Vec<ArrayRef> = vec![
                StdArc::new(BooleanArray::from(vec![Some(case.boolean), None])),
                StdArc::new(Int8Array::from(vec![Some(case.tiny_signed), None])),
                StdArc::new(Int16Array::from(vec![Some(case.short_signed), None])),
                StdArc::new(UInt8Array::from(vec![Some(case.tiny_unsigned), None])),
                StdArc::new(Int32Array::from(vec![Some(case.small_signed), None])),
                StdArc::new(UInt16Array::from(vec![Some(case.small_unsigned), None])),
                StdArc::new(Int64Array::from(vec![Some(case.large_signed), None])),
                StdArc::new(UInt32Array::from(vec![Some(case.large_unsigned), None])),
                StdArc::new(UInt64Array::from(vec![Some(unsigned), None])),
                StdArc::new(Float64Array::from(vec![Some(number), None])),
                StdArc::new(Float32Array::from(vec![Some(small_number), None])),
                StdArc::new(TimestampNanosecondArray::from(vec![
                    Some(case.instant),
                    None,
                ])),
                StdArc::new(BinaryArray::from(vec![Some(case.octets.as_slice()), None])),
                StdArc::new(StringArray::from(vec![Some(text.as_str()), None])),
                StdArc::new(list),
            ];
            let expected = vec![
                Bson::Boolean(case.boolean),
                Bson::Int32(i32::from(case.tiny_signed)),
                Bson::Int32(i32::from(case.short_signed)),
                Bson::Int32(i32::from(case.tiny_unsigned)),
                Bson::Int32(case.small_signed),
                Bson::Int32(i32::from(case.small_unsigned)),
                Bson::Int64(case.large_signed),
                Bson::Int64(i64::from(case.large_unsigned)),
                Bson::Int64(
                    i64::try_from(unsigned).assured("the generated unsigned value has 63 bits"),
                ),
                Bson::Double(number),
                Bson::Double(f64::from(small_number)),
                Bson::DateTime(mongodb::bson::DateTime::from_millis(
                    case.instant.div_euclid(1_000_000),
                )),
                Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: case.octets.to_vec(),
                }),
                Bson::String(text),
                Bson::Array(vec![Bson::Int32(case.small_signed), Bson::Null]),
            ];
            let mut names: Vec<String> = (0..values.len())
                .map(|index| format!("field_{index}"))
                .collect();
            if case.mapped_identifier {
                names[0] = "_id".to_string();
            }
            let fields: Vec<Field> = names
                .iter()
                .zip(&values)
                .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
                .collect();
            let batch = RecordBatch::try_new(StdArc::new(Schema::new(fields)), values)
                .assured("the generated columns have the same row count");
            let columns =
                MappedBsonColumns::new(&batch, &names).assured("every generated type is declared");
            for row in 0..2 {
                let mut expected_document = Document::new();
                for (name, value) in names.iter().zip(&expected) {
                    expected_document
                        .insert(name, if row == 0 { value.clone() } else { Bson::Null });
                }
                let actual = columns
                    .document(row)
                    .assured("every generated value is representable");
                let encoded = RawDocumentBuf::from_document(&expected_document)
                    .assured("the expected document encodes");
                let stored = StorableDocument::new(
                    WrittenRow {
                        position: EmitterRecordPosition {
                            batch_index: 0,
                            row_index: row,
                        },
                        occurred_at: Timestamp::from_unix_nanos(0),
                    },
                    actual,
                )
                .assured("a bounded generated document is storable");
                // Exact bytes preserve every BSON tag, field order, floating-point bit and null.
                assert_eq!(stored.raw.as_bytes(), encoded.as_bytes());
                if !case.mapped_identifier {
                    expected_document
                        .insert("_id", mongodb::bson::oid::ObjectId::from_bytes([1; 12]));
                }
                let with_identifier = RawDocumentBuf::from_document(&expected_document)
                    .assured("the bounded reference document with its identifier encodes");
                let stored_bytes = u64::try_from(with_identifier.as_bytes().len())
                    .assured("the bounded length fits u64");
                assert_eq!(stored.stored_bytes, stored_bytes);
            }
        });
}
