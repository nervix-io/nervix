//! Dense `F32` tensor columns at the Arrow boundary of inference.
//!
//! Layer: data plane.
//! Owns: reading one message's tensor from a mapped Arrow column, and writing the public output
//! tensors of many messages into one Arrow column, with every axis length and offset checked
//! before an Arrow array is built.
//! Depends on: validated tensor schemas, Arrow and ONNX Runtime tensors.
//! Must not know: Models, sessions, routes, branches or carried state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        data_plane,
        reason = "converts admitted tensor columns for a branch's model run"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "reads only validated tensor schemas")
)]

use arrow_array::{Array as _, ArrayRef, FixedSizeListArray, Float32Array, ListArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef};
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_models::{InferencerTensorDimension, InferencerTensorSchema};
use nervix_primitives::sync::StdArc;
use ort::value::{DynValue, Tensor};

use super::InferencerError;

/// The tensor one message carries in one mapped input column: its concrete shape, without any
/// `BATCH` axis, and its values in row-major order, borrowed from the column without a copy.
pub(super) struct MessageTensor {
    pub(super) shape: Vec<usize>,
    pub(super) values: Float32Array,
}

impl MessageTensor {
    /// The tensor `row` of `column` holds, read through the axes `schema` declares for one
    /// message. A `BATCH` axis is not part of one message's value and is skipped.
    pub(super) fn read(
        column: &ArrayRef,
        row: usize,
        port: &str,
        schema: &InferencerTensorSchema,
    ) -> error_stack::Result<Self, InferencerError> {
        let input_shape = || InferencerError::InputShape {
            port: port.to_owned(),
        };
        if row >= column.len() {
            return Err(Report::new(InferencerError::InputPort {
                port: port.to_owned(),
            }));
        }
        let mut current = column.slice(row, 1);
        let mut shape = Vec::with_capacity(schema.dimensions.len());
        for axis in &schema.dimensions {
            if current.null_count() != 0 {
                return Err(Report::new(InferencerError::NullInput {
                    port: port.to_owned(),
                }));
            }
            match axis {
                InferencerTensorDimension::Batch => {}
                InferencerTensorDimension::Fixed(size) => {
                    let array = current
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| {
                            Report::new(InferencerError::InputType {
                                port: port.to_owned(),
                                actual: current.data_type().clone(),
                            })
                        })?;
                    let size: usize = size
                        .get()
                        .try_into()
                        .assured("supported usize widths hold u32");
                    if usize::try_from(array.value_length()).ok() != Some(size) {
                        return Err(Report::new(input_shape()));
                    }
                    let start = usize::try_from(array.value_offset(0))
                        .map_err(|error| Report::new(error).change_context(input_shape()))?;
                    let count = current
                        .len()
                        .checked_mul(size)
                        .ok_or_else(|| Report::new(input_shape()))?;
                    shape.push(size);
                    current = array.values().slice(start, count);
                }
                InferencerTensorDimension::Dynamic => {
                    let array = current
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| {
                            Report::new(InferencerError::InputType {
                                port: port.to_owned(),
                                actual: current.data_type().clone(),
                            })
                        })?;
                    let offsets = array.value_offsets();
                    let Some(first) = offsets.windows(2).next() else {
                        return Err(Report::new(InferencerError::InnerDynamicFromEmpty)
                            .change_context(input_shape()));
                    };
                    let length = first[1] - first[0];
                    if offsets.windows(2).any(|pair| pair[1] - pair[0] != length) {
                        return Err(Report::new(input_shape()));
                    }
                    let start =
                        usize::try_from(offsets[0]).assured("Arrow list offsets are nonnegative");
                    let length = usize::try_from(length).assured("Arrow list offsets are ordered");
                    let count = current
                        .len()
                        .checked_mul(length)
                        .ok_or_else(|| Report::new(input_shape()))?;
                    shape.push(length);
                    current = array.values().slice(start, count);
                }
            }
        }
        if current.null_count() != 0 {
            return Err(Report::new(InferencerError::NullInput {
                port: port.to_owned(),
            }));
        }
        let values = current
            .as_any()
            .downcast_ref::<Float32Array>()
            .cloned()
            .ok_or_else(|| {
                Report::new(InferencerError::InputType {
                    port: port.to_owned(),
                    actual: current.data_type().clone(),
                })
            })?;
        Ok(Self { shape, values })
    }

    /// The message's values as a native tensor ONNX Runtime owns. Native outputs can alias an
    /// input, so retaining an output never retains a pointer borrowed from the admitted batch.
    pub(super) fn to_native(&self) -> error_stack::Result<DynValue, InferencerError> {
        let mut tensor =
            Tensor::<f32>::new(&ort::memory::Allocator::default(), self.shape.as_slice())
                .change_context(InferencerError::BuildInput)?;
        tensor
            .try_extract_tensor_mut::<f32>()
            .change_context(InferencerError::BuildInput)?
            .1
            .copy_from_slice(self.values.values().as_ref());
        Ok(tensor.into_dyn())
    }
}

/// The public output tensors of many messages, gathered in message order into one Arrow column of
/// the tensor's message type. Each message's tensor has its own concrete shape, so `DYNAMIC` axes
/// may differ from message to message while every fixed axis keeps its declared length.
pub(super) struct TensorColumn {
    dimensions: Vec<InferencerTensorDimension>,
    data_type: DataType,
    values: Vec<f32>,
    shapes: Vec<Vec<usize>>,
}

impl TensorColumn {
    /// An empty column of `schema`'s message type, without its `BATCH` axis.
    pub(super) fn new(schema: &InferencerTensorSchema, data_type: DataType) -> Self {
        let mut dimensions = Vec::with_capacity(schema.dimensions.len());
        for axis in &schema.dimensions {
            if !axis.is_batch() {
                dimensions.push(*axis);
            }
        }
        Self {
            dimensions,
            data_type,
            values: Vec::new(),
            shapes: Vec::new(),
        }
    }

    /// The Arrow bytes one message of `shape` holding `elements` values adds to a column of
    /// `schema`: four bytes for every value and one four-byte offset for every list it opens.
    pub(super) fn message_bytes(
        schema: &InferencerTensorSchema,
        shape: &[usize],
        elements: usize,
    ) -> Option<u64> {
        let mut bytes = u64::try_from(elements).ok()?.checked_mul(4)?;
        let mut parents = 1u64;
        let mut axes = schema.dimensions.iter().filter(|axis| !axis.is_batch());
        for size in shape {
            let axis = axes.next()?;
            if let InferencerTensorDimension::Dynamic = axis {
                bytes = bytes.checked_add(parents.checked_add(1)?.checked_mul(4)?)?;
            }
            parents = parents.checked_mul(u64::try_from(*size).ok()?)?;
        }
        Some(bytes)
    }

    /// Append one message's tensor of `shape`, its `BATCH` axis already removed, holding `values`
    /// in row-major order.
    pub(super) fn push(
        &mut self,
        shape: &[usize],
        values: &[f32],
    ) -> error_stack::Result<(), InferencerError> {
        if shape.len() != self.dimensions.len() {
            return Err(Report::new(InferencerError::ShapeDimensionCountMismatch {
                shape: shape.to_vec(),
                dimensions: self.dimensions.clone(),
            }));
        }
        let mut elements = 1usize;
        for (actual, declared) in shape.iter().zip(&self.dimensions) {
            if let InferencerTensorDimension::Fixed(expected) = declared
                && usize::try_from(expected.get()).ok() != Some(*actual)
            {
                return Err(Report::new(InferencerError::ShapeDimensionMismatch {
                    shape: shape.to_vec(),
                    actual: *actual,
                    expected: *expected,
                }));
            }
            elements = elements
                .checked_mul(*actual)
                .ok_or_else(|| Report::new(InferencerError::OutputSize))?;
        }
        if values.len() != elements {
            return Err(Report::new(InferencerError::ValueCountForShape {
                actual: values.len(),
                expected: elements,
                shape: shape.to_vec(),
            }));
        }
        self.values.extend_from_slice(values);
        self.shapes.push(shape.to_vec());
        Ok(())
    }

    /// The column of every pushed message, in push order. Every list level's length, offset and
    /// fixed size is checked against Arrow's 32-bit bounds before its array is built.
    pub(super) fn finish(self) -> error_stack::Result<ArrayRef, InferencerError> {
        let Self {
            dimensions,
            data_type,
            values,
            shapes,
        } = self;
        let mut fields = Vec::<FieldRef>::with_capacity(dimensions.len());
        let mut level_type = data_type;
        for _ in &dimensions {
            let field = match level_type {
                DataType::FixedSizeList(field, _) | DataType::List(field) => field,
                actual => return Err(Report::new(InferencerError::OutputType { actual })),
            };
            level_type = field.data_type().clone();
            fields.push(field);
        }
        if level_type != DataType::Float32 {
            return Err(Report::new(InferencerError::OutputType {
                actual: level_type,
            }));
        }
        let mut array: ArrayRef = StdArc::new(Float32Array::from(values));
        for (level, (field, axis)) in fields.into_iter().zip(&dimensions).enumerate().rev() {
            array = match axis {
                InferencerTensorDimension::Fixed(size) => {
                    let size =
                        i32::try_from(size.get()).change_context(InferencerError::OutputSize)?;
                    StdArc::new(
                        FixedSizeListArray::try_new(field, size, array, None)
                            .change_context(InferencerError::OutputArray)?,
                    )
                }
                InferencerTensorDimension::Dynamic => {
                    let offsets = Self::list_offsets(&shapes, level)?;
                    StdArc::new(
                        ListArray::try_new(
                            field,
                            OffsetBuffer::new(ScalarBuffer::from(offsets)),
                            array,
                            None,
                        )
                        .change_context(InferencerError::OutputArray)?,
                    )
                }
                InferencerTensorDimension::Batch => {
                    return Err(Report::new(InferencerError::OutputSize));
                }
            };
        }
        if array.len() != shapes.len() {
            return Err(Report::new(InferencerError::OutputArray));
        }
        Ok(array)
    }

    /// The offsets of list level `level`: every message opens one list for each element of the
    /// levels above it, and each of those lists holds the message's length at `level`.
    fn list_offsets(
        shapes: &[Vec<usize>],
        level: usize,
    ) -> error_stack::Result<Vec<i32>, InferencerError> {
        let mut offsets = Vec::with_capacity(
            shapes
                .len()
                .checked_add(LIST_TERMINAL_OFFSET_COUNT)
                .ok_or_else(|| Report::new(InferencerError::OutputSize))?,
        );
        let mut offset = 0i32;
        offsets.push(offset);
        for shape in shapes {
            let mut parents = 1usize;
            for size in &shape[..level] {
                parents = parents
                    .checked_mul(*size)
                    .ok_or_else(|| Report::new(InferencerError::OutputSize))?;
            }
            let length = i32::try_from(shape[level]).change_context(InferencerError::OutputSize)?;
            for _ in 0..parents {
                offset = offset
                    .checked_add(length)
                    .ok_or_else(|| Report::new(InferencerError::OutputSize))?;
                offsets.push(offset);
            }
        }
        Ok(offsets)
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "Arrow List offset buffers contain one terminal boundary after their parent rows"
    )
)]
const LIST_TERMINAL_OFFSET_COUNT: usize = 1;

#[cfg(test)]
mod tests;
