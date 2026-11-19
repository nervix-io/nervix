//! Direct evidence for dense tensor columns at the Arrow boundary of inference.
//!
//! Layer: test harness.
//! Owns: message tensor reads without copies, native input ownership, and the layout, offsets and
//! float bits of output columns of several messages.
//! Depends on: the production tensor column owner.
//! Must not know: sessions, routes or carried state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "checks tensor columns and native input ownership")
)]

use std::num::NonZeroU32;

use arrow_array::{FixedSizeListArray, Float32Array, ListArray, RecordBatch};
use nervix_models::{
    InferencerTensorDimension, InferencerTensorElementType, InferencerTensorRepresentation,
    InferencerTensorSchema,
};
use nervix_primitives::sync::StdArc;

use super::*;
use crate::runtime_schema::RuntimeRecordBatch;

fn schema(dimensions: Vec<InferencerTensorDimension>) -> InferencerTensorSchema {
    InferencerTensorSchema {
        representation: InferencerTensorRepresentation::Dense,
        element_type: InferencerTensorElementType::F32,
        dimensions,
    }
}

fn fixed(size: u32) -> InferencerTensorDimension {
    InferencerTensorDimension::Fixed(NonZeroU32::new(size).expect("test axes are positive"))
}

/// The message a failed operation reports to the processor that called it.
fn failure<T, C: error_stack::Context>(result: error_stack::Result<T, C>) -> String {
    let Err(error) = result else {
        panic!("the operation must fail");
    };
    error.current_context().to_string()
}

fn bits(values: &[f32]) -> Vec<u32> {
    values.iter().map(|value| value.to_bits()).collect()
}

#[test]
fn a_message_tensor_borrows_its_column_values_and_its_native_copy_owns_them() {
    let pairs = schema(vec![fixed(2)]);
    let data_type = pairs.message_type().arrow_data_type();
    let arrow_schema::DataType::FixedSizeList(field, length) = &data_type else {
        unreachable!("a fixed axis is a fixed-size list")
    };
    let values = Float32Array::from(vec![100.0, 1000.0, 1.0, 10.0]);
    let column: ArrayRef = StdArc::new(
        FixedSizeListArray::try_new(field.clone(), *length, StdArc::new(values.clone()), None)
            .expect("the fixture consists of pairs"),
    );
    let tensor = MessageTensor::read(&column, 1, "features", &pairs).expect("the second pair");
    assert_eq!(tensor.shape, [2]);
    assert_eq!(tensor.values.values().as_ref(), &[1.0, 10.0]);
    assert_eq!(
        tensor.values.values().as_ptr(),
        values.values().as_ptr().wrapping_add(2),
        "reading a message borrows the column's values"
    );
    let native = tensor
        .to_native()
        .expect("the model input owns its native allocation");
    drop(tensor);
    drop(column);
    drop(values);
    assert_eq!(
        native
            .try_extract_tensor::<f32>()
            .expect("native input outlives every Arrow owner")
            .1,
        &[1.0, 10.0]
    );
    let scalars: ArrayRef = StdArc::new(Float32Array::from(vec![1.0]));
    assert_eq!(
        failure(MessageTensor::read(&scalars, 0, "features", &pairs)),
        "input tensor 'features' has an invalid Arrow type Float32"
    );
}

#[test]
fn a_batch_axis_is_not_part_of_one_messages_value() {
    let batched = schema(vec![InferencerTensorDimension::Batch, fixed(2)]);
    let data_type = batched.message_type().arrow_data_type();
    let arrow_schema::DataType::FixedSizeList(field, length) = &data_type else {
        unreachable!("a fixed axis is a fixed-size list")
    };
    let column: ArrayRef = StdArc::new(
        FixedSizeListArray::try_new(
            field.clone(),
            *length,
            StdArc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            None,
        )
        .expect("two pairs"),
    );
    let tensor = MessageTensor::read(&column, 1, "features", &batched).expect("the second pair");
    assert_eq!(tensor.shape, [2]);
    assert_eq!(tensor.values.values().as_ref(), &[3.0, 4.0]);
}

#[test]
fn output_columns_preserve_float_bits_and_nested_offsets() {
    let tensor = schema(vec![fixed(2), InferencerTensorDimension::Dynamic]);
    let values = [f32::from_bits(0x7fc0_1234), -0.0, 1.0, 2.0, 3.0, 4.0];
    assert_eq!(
        TensorColumn::message_bytes(&tensor, &[2, 3], values.len()),
        Some(36)
    );
    let data_type = tensor.message_type().arrow_data_type();
    let mut column = TensorColumn::new(&tensor, data_type.clone());
    column
        .push(&[2, 3], &values)
        .expect("the message matches its declared axes");
    let array = column
        .finish()
        .expect("the public tensor has bounded offsets");
    assert_eq!(array.data_type(), &data_type);
    let rows = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("fixed outer axis")
        .value(0);
    let lists = rows
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("dynamic inner axis");
    assert_eq!(lists.value_offsets(), &[0, 3, 6]);
    let actual = lists
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("F32 leaves");
    assert_eq!(bits(actual.values()), bits(&values));
}

#[test]
fn each_message_keeps_its_own_dynamic_length() {
    let tensor = schema(vec![InferencerTensorDimension::Dynamic, fixed(2)]);
    let data_type = tensor.message_type().arrow_data_type();
    let mut column = TensorColumn::new(&tensor, data_type);
    column.push(&[1, 2], &[1.0, 2.0]).expect("one pair");
    column
        .push(&[2, 2], &[3.0, 4.0, 5.0, 6.0])
        .expect("two pairs");
    column.push(&[0, 2], &[]).expect("no pairs");
    let array = column.finish().expect("three messages");
    let lists = array
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("dynamic outer axis");
    assert_eq!(lists.len(), 3);
    assert_eq!(lists.value_offsets(), &[0, 1, 3, 3]);
}

#[test]
fn output_columns_refuse_messages_their_schema_does_not_describe() {
    let pairs = schema(vec![fixed(2)]);
    let mut column = TensorColumn::new(&pairs, pairs.message_type().arrow_data_type());
    assert_eq!(
        failure(column.push(&[2, 1], &[1.0, 2.0])),
        "tensor shape [2, 1] does not match declared dimensions [Fixed(2)]"
    );
    assert_eq!(
        failure(column.push(&[3], &[1.0, 2.0, 3.0])),
        "tensor shape [3] has dimension 3, expected 2"
    );
    assert_eq!(
        failure(column.push(&[2], &[1.0])),
        "tensor contains 1 values, expected 2 for shape [2]"
    );
    let empty = column.finish().expect("a refused message adds nothing");
    assert_eq!(empty.len(), 0);
}

/// Encodes every generated message into one column through the production output path, then
/// reads each message back through the production input path.
#[test]
fn bolero_tensor_columns_preserve_shape_and_float_bits() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            for rank in 0..=4 {
                let mut entropy = nervix_arbitrary::Entropy::new(bytes);
                let mut dimensions = Vec::with_capacity(rank);
                for _ in 0..rank {
                    if entropy.flag() {
                        dimensions.push(InferencerTensorDimension::Dynamic);
                    } else {
                        dimensions.push(fixed(1 + u32::from(entropy.byte() % 4)));
                    }
                }
                let tensor = schema(dimensions.clone());
                let data_type = tensor.message_type().arrow_data_type();
                let messages = 1 + usize::from(entropy.byte() % 3);
                let mut expected = Vec::with_capacity(messages);
                let mut column = TensorColumn::new(&tensor, data_type.clone());
                for _ in 0..messages {
                    let mut shape = Vec::with_capacity(rank);
                    for (axis, dimension) in dimensions.iter().enumerate() {
                        let size = match dimension {
                            InferencerTensorDimension::Fixed(size) => {
                                usize::try_from(size.get()).expect("bounded axis")
                            }
                            // Only the innermost axis may be empty: an empty outer vector leaves
                            // the length of an inner DYNAMIC axis undefined.
                            InferencerTensorDimension::Dynamic if axis + 1 == rank => {
                                usize::from(entropy.byte() % 5)
                            }
                            InferencerTensorDimension::Dynamic => {
                                1 + usize::from(entropy.byte() % 4)
                            }
                            InferencerTensorDimension::Batch => unreachable!("no BATCH axis"),
                        };
                        shape.push(size);
                    }
                    let count = shape.iter().product::<usize>();
                    let values = (0..count)
                        .map(|_| {
                            f32::from_bits(u32::from_le_bytes([
                                entropy.byte(),
                                entropy.byte(),
                                entropy.byte(),
                                entropy.byte(),
                            ]))
                        })
                        .collect::<Vec<_>>();
                    column
                        .push(&shape, &values)
                        .expect("bounded current dense tensors encode");
                    expected.push((shape, values));
                }
                let array = column
                    .finish()
                    .expect("bounded current dense tensors encode");
                assert_eq!(array.data_type(), &data_type);
                assert_eq!(array.len(), messages);
                assert_eq!(array.null_count(), 0);
                let arrow_schema =
                    StdArc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                        "features", data_type, false,
                    )]));
                let record_batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
                    .expect("one ordered tensor column");
                let batch =
                    RuntimeRecordBatch::from_record_batch(arrow_schema.clone(), record_batch)
                        .expect("the exact current tensor schema");
                assert_eq!(batch.schema(), arrow_schema);
                for (row, (shape, values)) in expected.iter().enumerate() {
                    let decoded =
                        MessageTensor::read(batch.batch().column(0), row, "features", &tensor)
                            .expect("the complete tensor decodes");
                    assert_eq!(&decoded.shape, shape);
                    assert_eq!(bits(decoded.values.values()), bits(values));
                }
            }
        });
}

/// An axis no Arrow list can hold is refused with a typed error before any Arrow array is built,
/// on the way in and on the way out.
#[test]
fn axes_beyond_arrow_list_bounds_are_refused_before_any_arrow_conversion() {
    let huge = schema(vec![fixed(u32::MAX)]);
    let item = StdArc::new(arrow_schema::Field::new(
        "item",
        arrow_schema::DataType::Float32,
        false,
    ));
    let column = TensorColumn::new(
        &huge,
        arrow_schema::DataType::FixedSizeList(item.clone(), i32::MAX),
    );
    assert_eq!(
        failure(column.finish()),
        "output tensor size exceeds the supported size"
    );

    let pairs: ArrayRef = StdArc::new(
        FixedSizeListArray::try_new(
            item,
            2,
            StdArc::new(Float32Array::from(vec![1.0, 2.0])),
            None,
        )
        .expect("one pair"),
    );
    assert_eq!(
        failure(MessageTensor::read(&pairs, 0, "features", &huge)),
        "input tensor 'features' has an invalid shape"
    );
    assert_eq!(
        TensorColumn::message_bytes(&huge, &[usize::MAX], usize::MAX),
        None
    );
}
