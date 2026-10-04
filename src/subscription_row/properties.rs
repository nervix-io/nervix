//! Arrow subscription fidelity and whole-input rejection through the production Row adapter.
//!
//! Layer: test harness.
//! - **Owns.** Bounded Arrow cases and a logical-value oracle independent of Row encoding.
//! - **Depends on.** Arrow, the production subscription adapter, wire views and vocabulary.
//! - **Must not know.** Session scheduling, live services or an alternative payload carrier.

use std::num::{NonZeroU32, NonZeroUsize};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{Field, Schema};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_client_wire::{
    CellView, Reply, ReplyBody, ReplyDelivery, ServerEvent, ServerMessage, SessionLimits,
    SubscribeDisposition, SubscribeOutcome, SubscriptionHandle,
};
use nervix_models::{ParseAsType, SchemaField, Timestamp};
use nervix_primitives::sync::StdArc;

use super::{SubscriptionRowEncodingError, SubscriptionRowOpening, SubscriptionRowSelection};
use crate::{runtime::BranchKey, runtime_schema::RuntimeValue};

struct ArrowCase {
    schema: nervix_client_wire::RowSchema,
    batch: RecordBatch,
    branches: Vec<Option<BranchKey>>,
    tenants: Vec<String>,
    selected: Vec<usize>,
    rows_per_frame: NonZeroUsize,
    handle: SubscriptionHandle,
}

impl ArrowCase {
    fn new(bytes: &[u8]) -> Self {
        let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
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
        let binary: Vec<_> = (0..arbitrary.entropy().count(64))
            .map(|_| arbitrary.entropy().byte())
            .collect();
        let mut columns: Vec<ArrayRef> = Vec::new();
        let mut fields = Vec::new();
        macro_rules! column {
            ($array:ident, $ty:ident, $value:expr, $zero:expr) => {{
                let optional = arbitrary.entropy().flag();
                let sensitive = arbitrary.entropy().flag();
                let value = $value;
                let middle = if optional { None } else { Some(value) };
                let array: ArrayRef =
                    StdArc::new($array::from(vec![Some(value), middle, Some($zero)]));
                fields.push(SchemaField {
                    name: format!("field_{}", fields.len())
                        .parse()
                        .assured("a generated field name"),
                    ty: ParseAsType::$ty,
                    optional,
                    sensitive,
                });
                columns.push(array);
            }};
        }
        column!(UInt8Array, U8, low8, 0);
        column!(Int8Array, I8, low8.cast_signed(), 0);
        column!(UInt16Array, U16, low16, 0);
        column!(Int16Array, I16, low16.cast_signed(), 0);
        column!(UInt32Array, U32, low32, 0);
        column!(Int32Array, I32, low32.cast_signed(), 0);
        column!(UInt64Array, U64, integer, 0);
        column!(Int64Array, I64, signed, 0);
        column!(Float32Array, F32, f32::from_bits(f32_bits), -0.0);
        column!(Float64Array, F64, f64::from_bits(f64_bits), -0.0);
        column!(BooleanArray, Bool, arbitrary.entropy().flag(), false);
        column!(StringArray, String, text.as_str(), "");
        column!(BinaryArray, Bytes, binary.as_slice(), &[][..]);
        column!(TimestampNanosecondArray, Datetime, signed, 0);
        let list_item = StdArc::new(Field::new("item", arrow_schema::DataType::Int64, false));
        let list_values: ArrayRef =
            StdArc::new(Int64Array::from(vec![signed, 0, i64::MAX, i64::MIN]));
        let list = ListArray::new(
            list_item.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 4])),
            list_values.clone(),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let fixed_values: ArrayRef =
            StdArc::new(Int64Array::from(vec![signed, 0, 0, 0, i64::MAX, i64::MIN]));
        let fixed = FixedSizeListArray::new(
            list_item,
            2,
            fixed_values,
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let list: ArrayRef = StdArc::new(list);
        let fixed: ArrayRef = StdArc::new(fixed);
        for (ty, array) in [
            (
                ParseAsType::Vec {
                    element: Box::new(ParseAsType::I64),
                },
                list,
            ),
            (
                ParseAsType::Array {
                    element: Box::new(ParseAsType::I64),
                    len: NonZeroU32::new(2).assured("two elements"),
                },
                fixed,
            ),
        ] {
            fields.push(SchemaField {
                name: format!("field_{}", fields.len())
                    .parse()
                    .assured("a generated field name"),
                ty,
                optional: true,
                sensitive: arbitrary.entropy().flag(),
            });
            columns.push(array);
        }
        // A nested vector also covers empty lists distinctly from nullable parent lists.
        let children = ListArray::new(
            StdArc::new(Field::new("item", arrow_schema::DataType::Int64, false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 4])),
            list_values,
            None,
        );
        let parent = ListArray::new(
            StdArc::new(Field::new("item", children.data_type().clone(), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
            StdArc::new(children),
            None,
        );
        fields.push(SchemaField {
            name: "nested".parse().assured("a literal field name"),
            ty: ParseAsType::Vec {
                element: Box::new(ParseAsType::Vec {
                    element: Box::new(ParseAsType::I64),
                }),
            },
            optional: false,
            sensitive: false,
        });
        columns.push(StdArc::new(parent));
        let arrow_fields: Vec<_> = fields
            .iter()
            .zip(&columns)
            .map(|(field, array)| {
                Field::new(
                    field.name.as_str(),
                    array.data_type().clone(),
                    field.optional,
                )
            })
            .collect();
        let batch = RecordBatch::try_new(StdArc::new(Schema::new(arrow_fields)), columns)
            .assured("generated columns have exactly three rows and declared types");
        let branched = arbitrary.entropy().flag();
        let branch = if branched {
            Some(
                nervix_client_wire::RowBranch::new(
                    "by_tenant".parse().assured("a branch name"),
                    vec![SchemaField {
                        name: "tenant".parse().assured("a field name"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    }],
                )
                .assured("a nonempty branch"),
            )
        } else {
            None
        };
        let tenants = vec![text.clone(), format!("other_{text}"), text];
        let branches = tenants
            .iter()
            .map(|tenant| {
                if branched {
                    Some(
                        BranchKey::from_fields([(
                            "tenant".parse().assured("a field name"),
                            RuntimeValue::String(tenant.clone()),
                        )])
                        .assured("one typed branch field"),
                    )
                } else {
                    None
                }
            })
            .collect();
        let mut selected = Vec::new();
        for row in 0..3 {
            if arbitrary.entropy().flag() {
                selected.push(row);
            }
        }
        let rows_per_frame = NonZeroUsize::new(arbitrary.entropy().count(2) + 1)
            .assured("one to three rows per frame");
        Self {
            schema: nervix_client_wire::RowSchema { fields, branch },
            batch,
            branches,
            tenants,
            selected,
            rows_per_frame,
            handle: SubscriptionHandle {
                name: arbitrary.name(),
                generation: arbitrary.positive_u64(),
            },
        }
    }

    fn check(&self, selected: &[usize]) {
        let limits = SessionLimits::DEFAULT;
        let opening = SubscriptionRowOpening::new(
            self.handle.clone(),
            self.schema.clone(),
            limits,
            self.rows_per_frame,
        )
        .assured("the row limit is within the collection budget");
        let (metadata, encoder) = opening.open(
            "tenant".parse().assured("a domain name"),
            "rows".parse().assured("a relay name"),
        );
        assert_eq!(metadata.schema, self.schema);
        let original = Reply {
            request_id: nervix_client_wire::RequestId::new(std::num::NonZeroU64::MIN),
            body: ReplyBody::Subscribe(SubscribeOutcome {
                disposition: SubscribeDisposition::Opened(Box::new(metadata)),
                message: String::new(),
                diagnostics: Vec::new(),
            }),
        };
        let ReplyDelivery::Frame(frame) = original.encode(&limits).assured("opening metadata fits")
        else {
            panic!("bounded metadata fits one frame");
        };
        let frame = frame.verify(&limits).assured("opening verifies");
        let ServerMessage::Reply(decoded) =
            ServerMessage::decode(&frame).assured("opening decodes")
        else {
            panic!("opening is a reply");
        };
        assert_eq!(decoded, original);
        let frames = encoder
            .encode(
                &self.batch,
                &self.branches,
                SubscriptionRowSelection::Rows(selected),
            )
            .assured("the complete valid Arrow input encodes");
        let mut position = 0;
        for encoded in frames {
            assert!(encoded.rows.get() <= self.rows_per_frame.get());
            let frame = encoded
                .frame
                .verify(&limits)
                .assured("Arrow Row frame verifies");
            let ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) =
                ServerMessage::decode(&frame).assured("valid Arrow Row frames never skip failures")
            else {
                panic!("Row frame keeps its variant");
            };
            drop(frame);
            assert_eq!(rows.subscription(), &self.handle);
            let batch = rows.batch();
            batch
                .conform(&self.schema)
                .assured("decoded rows agree with exact types and sensitivity");
            assert_eq!(batch.len(), encoded.rows.get());
            for cells in batch.rows() {
                let row = selected[position];
                if self.schema.branch.is_some() {
                    assert_eq!(
                        batch
                            .branch_key()
                            .assured("a concrete branch is present")
                            .get(0),
                        Some(CellView::String(&self.tenants[row]))
                    );
                } else {
                    assert!(batch.branch_key().is_none());
                }
                assert_eq!(cells.len(), self.schema.fields.len());
                for (column, field) in self.schema.fields.iter().enumerate() {
                    let cell = cells.get(column).assured("each declared column has a cell");
                    if field.sensitive {
                        assert_eq!(cell, CellView::Redacted);
                    } else {
                        assert_arrow_cell(self.batch.column(column).as_ref(), row, cell);
                    }
                }
                position += 1;
            }
        }
        assert_eq!(position, selected.len());
    }
}

fn assert_arrow_cell(array: &dyn Array, row: usize, cell: CellView<'_>) {
    if array.is_null(row) {
        assert_eq!(cell, CellView::Null);
        return;
    }
    macro_rules! scalar {
        ($array:ident, $variant:ident) => {
            if let Some(array) = array.as_any().downcast_ref::<$array>() {
                assert_eq!(cell, CellView::$variant(array.value(row)));
                return;
            }
        };
    }
    scalar!(UInt8Array, U8);
    scalar!(Int8Array, I8);
    scalar!(UInt16Array, U16);
    scalar!(Int16Array, I16);
    scalar!(UInt32Array, U32);
    scalar!(Int32Array, I32);
    scalar!(UInt64Array, U64);
    scalar!(Int64Array, I64);
    scalar!(Float32Array, F32);
    scalar!(Float64Array, F64);
    scalar!(BooleanArray, Bool);
    scalar!(StringArray, String);
    scalar!(BinaryArray, Bytes);
    if let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        assert_eq!(
            cell,
            CellView::Datetime(Timestamp::from_unix_nanos(array.value(row)))
        );
        return;
    }
    let children = if let Some(array) = array.as_any().downcast_ref::<ListArray>() {
        array.value(row)
    } else if let Some(array) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        array.value(row)
    } else {
        panic!("the complete oracle must cover every generated Arrow type");
    };
    let CellView::List(cells) = cell else {
        panic!("a present Arrow list keeps a list cell");
    };
    assert_eq!(cells.len(), children.len());
    for (index, cell) in cells.iter().enumerate() {
        assert_arrow_cell(children.as_ref(), index, cell);
    }
}

#[test]
fn bolero_arrow_rows_preserve_exact_values_or_explicit_redaction() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let case = ArrowCase::new(bytes);
            case.check(&[0, 1, 2]);
            case.check(&case.selected);
        });
}

#[test]
fn arrow_boundaries_are_replayable_without_row_materialization() {
    for seed in [0, 1, 2, 3, 4, 5, 0x80, 0xff] {
        let case = ArrowCase::new(&[seed; 512]);
        case.check(&[0, 1, 2]);
        case.check(&case.selected);
    }
}

#[test]
fn bolero_invalid_arrow_selection_has_no_partial_emission() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let case = ArrowCase::new(bytes);
            let opening = SubscriptionRowOpening::new(
                case.handle,
                case.schema,
                SessionLimits::DEFAULT,
                case.rows_per_frame,
            )
            .assured("bounded opening");
            let (_, encoder) = opening.open(
                "tenant".parse().assured("a domain name"),
                "rows".parse().assured("a relay name"),
            );
            for selected in [[0, 3], [1, 0], [0, 0]] {
                let error = encoder
                    .encode(
                        &case.batch,
                        &case.branches,
                        SubscriptionRowSelection::Rows(&selected),
                    )
                    .expect_err("invalid selection is refused as a whole");
                assert!(matches!(
                    error.current_context(),
                    SubscriptionRowEncodingError::SelectedRowOutOfBounds { .. }
                        | SubscriptionRowEncodingError::SelectedRowsNotIncreasing { .. }
                ));
            }
            assert!(
                encoder
                    .encode(
                        &case.batch,
                        &case.branches[..2],
                        SubscriptionRowSelection::All
                    )
                    .is_err()
            );
            assert!(
                encoder
                    .encode(&case.batch, &case.branches, SubscriptionRowSelection::All)
                    .is_ok()
            );
        });
}
