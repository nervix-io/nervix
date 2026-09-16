use std::path::{Path, PathBuf};

use ahash::{HashMap, HashMapExt};
use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{
    InferencerExecutionMode, InferencerTensorDeclaration, InferencerTensorDimension,
    InferencerTensorMapping, InferencerTensorSchema,
};
use ordered_float::OrderedFloat;
use ort::{
    session::{Session, SessionInputValue},
    value::Tensor,
};
use parking_lot::Mutex;
use triomphe::Arc;

use crate::runtime_schema::{RuntimeRecordBatch, RuntimeValue};

/// Every way ONNX inference fails, from loading a model to reshaping one tensor axis.
#[derive(Debug, thiserror::Error)]
pub(super) enum InferencerError {
    #[error("failed to initialize ONNX session")]
    InitializeSession,
    #[error("failed to load ONNX model '{}'", .path.display())]
    LoadModel { path: PathBuf },
    #[error("failed to join ONNX model loading task")]
    JoinModelLoad,
    #[error("failed to join ONNX execution task")]
    JoinExecution,
    #[error("cannot execute ONNX inference for an empty message batch")]
    EmptyMessageBatch,
    #[error("per-message ONNX invocation has invalid routing")]
    InvalidPerMessageRouting,
    #[error("ONNX output tensor '{tensor}' omitted message row {row}")]
    OmittedMessageRow { tensor: String, row: usize },
    #[error("ONNX input tensor '{tensor}' mapped value is missing")]
    MissingInputValue { tensor: String },
    #[error("failed to read ONNX input tensor '{tensor}'")]
    ReadInputValue { tensor: String },
    #[error("failed to build ONNX input '{tensor}'")]
    BuildInput { tensor: String },
    #[error("ONNX model invocation failed")]
    Invocation,
    #[error("ONNX invocation omitted output tensor '{tensor}'")]
    OmittedOutputTensor { tensor: String },
    #[error("failed to extract ONNX output tensor '{tensor}' as F32")]
    ExtractOutput { tensor: String },
    #[error("ONNX output tensor '{tensor}' returned a negative shape {shape:?}")]
    NegativeOutputShape { tensor: String, shape: Vec<i64> },
    #[error("batched tensor schema has no dimensions")]
    BatchedSchemaWithoutDimensions,
    #[error("batched tensor schema has no BATCH axis")]
    MissingBatchAxis,
    #[error("tensor slice rank {actual} does not match declared rank {expected}")]
    SliceRankMismatch { actual: usize, expected: usize },
    #[error("batched tensor rank {actual} does not match declared rank {expected}")]
    BatchedRankMismatch { actual: usize, expected: usize },
    #[error("tensor shape {shape:?} does not match declared dimensions {dimensions:?}")]
    ShapeDimensionCountMismatch {
        shape: Vec<usize>,
        dimensions: Vec<InferencerTensorDimension>,
    },
    #[error("tensor shape {shape:?} has dimension {actual}, expected {expected}")]
    ShapeDimensionMismatch {
        shape: Vec<usize>,
        actual: usize,
        expected: std::num::NonZeroU32,
    },
    #[error("tensor shape {shape:?} has batch dimension {actual}, expected {batch_size}")]
    ShapeBatchMismatch {
        shape: Vec<usize>,
        actual: usize,
        batch_size: usize,
    },
    #[error("cannot join an empty tensor batch")]
    EmptyTensorBatch,
    #[error(
        "batched DYNAMIC tensor slices must have one concrete shape; got {first:?} and {other:?}"
    )]
    RaggedBatchSlices {
        first: Vec<usize>,
        other: Vec<usize>,
    },
    #[error("batched tensor outer element count overflowed")]
    OuterCountOverflow,
    #[error("batched tensor inner element count overflowed")]
    InnerCountOverflow,
    #[error("batched tensor slice element count overflowed")]
    SliceCountOverflow,
    #[error("batched tensor slice contains {actual} values, expected {expected}")]
    SliceValueCount { actual: usize, expected: usize },
    #[error("batched tensor joined element count overflowed")]
    JoinedCountOverflow,
    #[error("batched tensor output element count overflowed")]
    OutputCountOverflow,
    #[error("batched output contains {actual} values, expected {expected}")]
    BatchedValueCount { actual: usize, expected: usize },
    #[error("tensor element requires F32, got {value:?}")]
    ElementRequiresF32 { value: RuntimeValue },
    #[error("tensor ARRAY axis contains {actual} values, expected {expected}")]
    FixedAxisLength {
        actual: usize,
        expected: std::num::NonZeroU32,
    },
    #[error("fixed tensor axis requires ARRAY, got {value:?}")]
    FixedAxisRequiresArray { value: RuntimeValue },
    #[error("dynamic tensor axis requires VEC, got {value:?}")]
    DynamicAxisRequiresVec { value: RuntimeValue },
    #[error("dense tensor is ragged: child shapes {expected:?} and {actual:?} differ")]
    RaggedChildShapes {
        expected: Vec<usize>,
        actual: Vec<usize>,
    },
    #[error("cannot infer an inner DYNAMIC axis from an empty outer vector")]
    InnerDynamicFromEmpty,
    #[error("scalar tensor has unexpected remaining shape {shape:?}")]
    ScalarRemainingShape { shape: Vec<usize> },
    #[error("scalar tensor contains {actual} values, expected 1")]
    ScalarValueCount { actual: usize },
    #[error("tensor value has fewer axes than its schema")]
    FewerAxesThanSchema,
    #[error("tensor axis has length {size}, expected {expected}")]
    AxisLength {
        size: usize,
        expected: std::num::NonZeroU32,
    },
    #[error("tensor child element count overflowed")]
    ChildCountOverflow,
    #[error("tensor element count overflowed")]
    ElementCountOverflow,
    #[error("tensor contains {actual} values, expected {expected} for shape {shape:?}")]
    ValueCountForShape {
        actual: usize,
        expected: usize,
        shape: Vec<usize>,
    },
}

#[derive(Clone)]
pub(super) struct OnnxInferencerSession {
    version: u64,
    session: Arc<Mutex<Session>>,
}

impl std::fmt::Debug for OnnxInferencerSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OnnxInferencerSession")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl OnnxInferencerSession {
    pub(super) async fn load(
        version: u64,
        path: &Path,
    ) -> error_stack::Result<Self, InferencerError> {
        let path = path.to_path_buf();
        let session = tokio::task::spawn_blocking(move || {
            let mut builder =
                Session::builder().change_context(InferencerError::InitializeSession)?;
            builder
                .commit_from_file(&path)
                .change_context_lazy(|| InferencerError::LoadModel { path: path.clone() })
        })
        .await
        .change_context(InferencerError::JoinModelLoad)??;
        Ok(Self {
            version,
            session: Arc::new(Mutex::new(session)),
        })
    }

    pub(super) fn version(&self) -> u64 {
        self.version
    }

    pub(super) async fn execute(
        &self,
        batch: &RuntimeRecordBatch,
        inputs: &[InferencerTensorMapping],
        output_schema: &[InferencerTensorDeclaration],
        mode: InferencerExecutionMode,
    ) -> error_stack::Result<Vec<Vec<RuntimeValue>>, InferencerError> {
        let prepared = PreparedExecution::from_batch(batch, inputs, output_schema, mode)?;
        let session = Arc::clone(&self.session);
        tokio::task::spawn_blocking(move || prepared.run(&mut session.lock()))
            .await
            .change_context(InferencerError::JoinExecution)?
    }
}

struct PreparedExecution {
    invocations: Vec<PreparedInvocation>,
    output_schema: Vec<InferencerTensorDeclaration>,
    message_count: usize,
    mode: InferencerExecutionMode,
}

impl PreparedExecution {
    fn from_batch(
        batch: &RuntimeRecordBatch,
        inputs: &[InferencerTensorMapping],
        output_schema: &[InferencerTensorDeclaration],
        mode: InferencerExecutionMode,
    ) -> error_stack::Result<Self, InferencerError> {
        let message_count = batch.batch().num_rows();
        if message_count == 0 {
            return Err(Report::new(InferencerError::EmptyMessageBatch));
        }
        let invocations = match mode {
            InferencerExecutionMode::PerMessage => (0..message_count)
                .map(|message_index| PreparedInvocation::for_row(message_index, batch, inputs))
                .collect::<Result<Vec<_>, _>>()?,
            InferencerExecutionMode::Batched => {
                PreparedInvocation::for_shape_batches(batch, inputs)?
            }
        };
        Ok(Self {
            invocations,
            output_schema: output_schema.to_vec(),
            message_count,
            mode,
        })
    }

    fn run(
        self,
        session: &mut Session,
    ) -> error_stack::Result<Vec<Vec<RuntimeValue>>, InferencerError> {
        let mut columns = self
            .output_schema
            .iter()
            .map(|_| vec![None; self.message_count])
            .collect::<Vec<Vec<Option<RuntimeValue>>>>();
        match self.mode {
            InferencerExecutionMode::PerMessage => {
                for invocation in self.invocations {
                    let [message_index] = invocation.message_indices.as_slice() else {
                        return Err(Report::new(InferencerError::InvalidPerMessageRouting));
                    };
                    let message_index = *message_index;
                    let output_tensors = invocation.run(session, &self.output_schema, 1)?;
                    for (column, (declaration, tensor)) in columns
                        .iter_mut()
                        .zip(self.output_schema.iter().zip(output_tensors))
                    {
                        column[message_index] = Some(
                            declaration
                                .schema
                                .runtime_value_from_tensor(&tensor.values, &tensor.shape)?,
                        );
                    }
                }
            }
            InferencerExecutionMode::Batched => {
                for invocation in self.invocations {
                    let message_indices = invocation.message_indices.clone();
                    let batch_size = message_indices.len();
                    let output_tensors =
                        invocation.run(session, &self.output_schema, batch_size)?;
                    for (column, (declaration, tensor)) in columns
                        .iter_mut()
                        .zip(self.output_schema.iter().zip(output_tensors))
                    {
                        let slices = declaration.schema.split_batch_values(
                            &tensor.values,
                            batch_size,
                            &tensor.shape,
                        )?;
                        let slice_shape = declaration.schema.shape_without_batch(&tensor.shape)?;
                        for (message_index, values) in message_indices.iter().zip(slices) {
                            column[*message_index] = Some(
                                declaration
                                    .schema
                                    .runtime_value_from_tensor(&values, &slice_shape)?,
                            );
                        }
                    }
                }
            }
        }
        let mut outputs = Vec::with_capacity(columns.len());
        for (column_index, column) in columns.into_iter().enumerate() {
            let mut values = Vec::with_capacity(column.len());
            for (row, value) in column.into_iter().enumerate() {
                let value = value.ok_or_else(|| {
                    Report::new(InferencerError::OmittedMessageRow {
                        tensor: self.output_schema[column_index].tensor.clone(),
                        row,
                    })
                })?;
                values.push(value);
            }
            outputs.push(values);
        }
        Ok(outputs)
    }
}

struct PreparedInvocation {
    inputs: Vec<PreparedTensor>,
    message_indices: Vec<usize>,
}

impl PreparedInvocation {
    fn for_row(
        message_index: usize,
        batch: &RuntimeRecordBatch,
        mappings: &[InferencerTensorMapping],
    ) -> error_stack::Result<Self, InferencerError> {
        let mut inputs = Vec::with_capacity(mappings.len());
        for mapping in mappings {
            let value = mapped_input_value(batch, message_index, mapping)?;
            let tensor = mapping.schema.tensor_from_runtime_value(&value)?;
            inputs.push(PreparedTensor {
                name: mapping.tensor.clone(),
                shape: tensor.shape,
                values: tensor.values,
            });
        }
        Ok(Self {
            inputs,
            message_indices: vec![message_index],
        })
    }

    fn for_shape_batches(
        batch: &RuntimeRecordBatch,
        mappings: &[InferencerTensorMapping],
    ) -> error_stack::Result<Vec<Self>, InferencerError> {
        let mut groups = HashMap::<Vec<Vec<usize>>, Vec<usize>>::new();
        for message_index in 0..batch.batch().num_rows() {
            let mut shapes = Vec::with_capacity(mappings.len());
            for mapping in mappings {
                let value = mapped_input_value(batch, message_index, mapping)?;
                shapes.push(mapping.schema.tensor_from_runtime_value(&value)?.shape);
            }
            groups.entry(shapes).or_default().push(message_index);
        }
        groups
            .into_values()
            .map(|message_indices| Self::for_batch(batch, mappings, message_indices))
            .collect()
    }

    fn for_batch(
        batch: &RuntimeRecordBatch,
        mappings: &[InferencerTensorMapping],
        message_indices: Vec<usize>,
    ) -> error_stack::Result<Self, InferencerError> {
        let mut inputs = Vec::with_capacity(mappings.len());
        for mapping in mappings {
            let mut slices = Vec::with_capacity(message_indices.len());
            for message_index in &message_indices {
                let value = mapped_input_value(batch, *message_index, mapping)?;
                slices.push(mapping.schema.tensor_from_runtime_value(&value)?);
            }
            let shape = mapping
                .schema
                .batch_shape(&slices[0].shape, message_indices.len())?;
            inputs.push(PreparedTensor {
                name: mapping.tensor.clone(),
                shape,
                values: mapping.schema.join_batch_values(&slices)?,
            });
        }
        Ok(Self {
            inputs,
            message_indices,
        })
    }

    fn run(
        self,
        session: &mut Session,
        output_schema: &[InferencerTensorDeclaration],
        batch_size: usize,
    ) -> error_stack::Result<Vec<ExecutedTensor>, InferencerError> {
        let mut inputs = Vec::with_capacity(self.inputs.len());
        for tensor in self.inputs {
            let value =
                Tensor::from_array((tensor.shape, tensor.values)).change_context_lazy(|| {
                    InferencerError::BuildInput {
                        tensor: tensor.name.clone(),
                    }
                })?;
            inputs.push((tensor.name, SessionInputValue::from(value)));
        }
        let session_outputs = session
            .run(inputs)
            .change_context(InferencerError::Invocation)?;
        let mut executed = Vec::with_capacity(output_schema.len());
        for declaration in output_schema {
            let output = session_outputs.get(&declaration.tensor).ok_or_else(|| {
                Report::new(InferencerError::OmittedOutputTensor {
                    tensor: declaration.tensor.clone(),
                })
            })?;
            let (shape, values) = output.try_extract_tensor::<f32>().change_context_lazy(|| {
                InferencerError::ExtractOutput {
                    tensor: declaration.tensor.clone(),
                }
            })?;
            let shape = shape
                .iter()
                .map(|dimension| usize::try_from(*dimension))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| {
                    Report::new(InferencerError::NegativeOutputShape {
                        tensor: declaration.tensor.clone(),
                        shape: shape.as_ref().to_vec(),
                    })
                })?;
            declaration.schema.validate_concrete_shape(
                &shape,
                batch_size,
                declaration.schema.batch_axis().is_some(),
            )?;
            executed.push(ExecutedTensor {
                shape,
                values: values.to_vec(),
            });
        }
        Ok(executed)
    }
}

/// Read the value one tensor mapping projects for a message, which every invocation shape needs
/// before it can build a tensor from it.
fn mapped_input_value(
    batch: &RuntimeRecordBatch,
    message_index: usize,
    mapping: &InferencerTensorMapping,
) -> error_stack::Result<RuntimeValue, InferencerError> {
    let value = batch
        .value(message_index, &mapping.tensor)
        .change_context_lazy(|| InferencerError::ReadInputValue {
            tensor: mapping.tensor.clone(),
        })?;
    value.ok_or_else(|| {
        Report::new(InferencerError::MissingInputValue {
            tensor: mapping.tensor.clone(),
        })
    })
}

struct PreparedTensor {
    name: String,
    shape: Vec<usize>,
    values: Vec<f32>,
}

struct ExecutedTensor {
    shape: Vec<usize>,
    values: Vec<f32>,
}

struct RuntimeTensorSlice {
    shape: Vec<usize>,
    values: Vec<f32>,
}

trait RuntimeTensorSchema {
    fn tensor_from_runtime_value(
        &self,
        value: &RuntimeValue,
    ) -> error_stack::Result<RuntimeTensorSlice, InferencerError>;
    fn runtime_value_from_tensor(
        &self,
        values: &[f32],
        shape: &[usize],
    ) -> error_stack::Result<RuntimeValue, InferencerError>;
    fn batch_shape(
        &self,
        slice_shape: &[usize],
        batch_size: usize,
    ) -> error_stack::Result<Vec<usize>, InferencerError>;
    fn shape_without_batch(
        &self,
        shape: &[usize],
    ) -> error_stack::Result<Vec<usize>, InferencerError>;
    fn validate_concrete_shape(
        &self,
        shape: &[usize],
        batch_size: usize,
        include_batch: bool,
    ) -> error_stack::Result<(), InferencerError>;
    fn join_batch_values(
        &self,
        slices: &[RuntimeTensorSlice],
    ) -> error_stack::Result<Vec<f32>, InferencerError>;
    fn split_batch_values(
        &self,
        values: &[f32],
        batch_size: usize,
        shape: &[usize],
    ) -> error_stack::Result<Vec<Vec<f32>>, InferencerError>;
    fn tensor_from_dimensions(
        &self,
        dimensions: &[InferencerTensorDimension],
        value: &RuntimeValue,
    ) -> error_stack::Result<RuntimeTensorSlice, InferencerError>;
    fn shape_from_dimensions_without_values(
        &self,
        dimensions: &[InferencerTensorDimension],
    ) -> error_stack::Result<Vec<usize>, InferencerError>;
    fn runtime_value_from_dimensions(
        &self,
        dimensions: &[InferencerTensorDimension],
        values: &[f32],
        shape: &[usize],
    ) -> error_stack::Result<RuntimeValue, InferencerError>;
}

impl RuntimeTensorSchema for InferencerTensorSchema {
    fn tensor_from_runtime_value(
        &self,
        value: &RuntimeValue,
    ) -> error_stack::Result<RuntimeTensorSlice, InferencerError> {
        self.tensor_from_dimensions(&self.dimensions, value)
    }

    fn runtime_value_from_tensor(
        &self,
        values: &[f32],
        shape: &[usize],
    ) -> error_stack::Result<RuntimeValue, InferencerError> {
        self.runtime_value_from_dimensions(&self.dimensions, values, shape)
    }

    fn batch_shape(
        &self,
        slice_shape: &[usize],
        batch_size: usize,
    ) -> error_stack::Result<Vec<usize>, InferencerError> {
        let expected_slice_rank = self
            .dimensions
            .len()
            .checked_sub(1)
            .ok_or_else(|| Report::new(InferencerError::BatchedSchemaWithoutDimensions))?;
        if slice_shape.len() != expected_slice_rank {
            return Err(Report::new(InferencerError::SliceRankMismatch {
                actual: slice_shape.len(),
                expected: expected_slice_rank,
            }));
        }
        let mut shape = Vec::with_capacity(self.dimensions.len());
        let mut slice_dimensions = slice_shape.iter();
        for dimension in &self.dimensions {
            if let InferencerTensorDimension::Batch = dimension {
                shape.push(batch_size);
            } else {
                shape.push(*slice_dimensions.next().verified(
                    "the rank check above rejected a slice shape with too few dimensions",
                ));
            }
        }
        self.validate_concrete_shape(&shape, batch_size, true)?;
        Ok(shape)
    }

    fn shape_without_batch(
        &self,
        shape: &[usize],
    ) -> error_stack::Result<Vec<usize>, InferencerError> {
        let batch_axis = self
            .batch_axis()
            .ok_or_else(|| Report::new(InferencerError::MissingBatchAxis))?;
        if shape.len() != self.dimensions.len() {
            return Err(Report::new(InferencerError::BatchedRankMismatch {
                actual: shape.len(),
                expected: self.dimensions.len(),
            }));
        }
        let mut result = shape.to_vec();
        result.remove(batch_axis);
        Ok(result)
    }

    fn validate_concrete_shape(
        &self,
        shape: &[usize],
        batch_size: usize,
        include_batch: bool,
    ) -> error_stack::Result<(), InferencerError> {
        let dimensions = self
            .dimensions
            .iter()
            .filter(|dimension| include_batch || !dimension.is_batch());
        if shape.len() != dimensions.clone().count() {
            return Err(Report::new(InferencerError::ShapeDimensionCountMismatch {
                shape: shape.to_vec(),
                dimensions: self.dimensions.clone(),
            }));
        }
        for (actual, declared) in shape.iter().zip(dimensions) {
            match declared {
                InferencerTensorDimension::Fixed(expected)
                    if *actual != expected.get().arch_into() =>
                {
                    return Err(Report::new(InferencerError::ShapeDimensionMismatch {
                        shape: shape.to_vec(),
                        actual: *actual,
                        expected: *expected,
                    }));
                }
                InferencerTensorDimension::Batch if *actual != batch_size => {
                    return Err(Report::new(InferencerError::ShapeBatchMismatch {
                        shape: shape.to_vec(),
                        actual: *actual,
                        batch_size,
                    }));
                }
                InferencerTensorDimension::Fixed(_)
                | InferencerTensorDimension::Dynamic
                | InferencerTensorDimension::Batch => {}
            }
        }
        Ok(())
    }

    fn join_batch_values(
        &self,
        slices: &[RuntimeTensorSlice],
    ) -> error_stack::Result<Vec<f32>, InferencerError> {
        let first = slices
            .first()
            .ok_or_else(|| Report::new(InferencerError::EmptyTensorBatch))?;
        if let Some(slice) = slices.iter().find(|slice| slice.shape != first.shape) {
            return Err(Report::new(InferencerError::RaggedBatchSlices {
                first: first.shape.clone(),
                other: slice.shape.clone(),
            }));
        }
        let shape = self.batch_shape(&first.shape, slices.len())?;
        let batch_axis = self
            .batch_axis()
            .ok_or_else(|| Report::new(InferencerError::MissingBatchAxis))?;
        let outer = shape[..batch_axis]
            .iter()
            .try_fold(1_usize, |count, size| count.checked_mul(*size))
            .ok_or_else(|| Report::new(InferencerError::OuterCountOverflow))?;
        let inner = shape[batch_axis + 1..]
            .iter()
            .try_fold(1_usize, |count, size| count.checked_mul(*size))
            .ok_or_else(|| Report::new(InferencerError::InnerCountOverflow))?;
        let expected_slice_len = outer
            .checked_mul(inner)
            .ok_or_else(|| Report::new(InferencerError::SliceCountOverflow))?;
        if let Some(actual) = slices
            .iter()
            .map(|slice| slice.values.len())
            .find(|actual| *actual != expected_slice_len)
        {
            return Err(Report::new(InferencerError::SliceValueCount {
                actual,
                expected: expected_slice_len,
            }));
        }
        let joined_len = expected_slice_len
            .checked_mul(slices.len())
            .ok_or_else(|| Report::new(InferencerError::JoinedCountOverflow))?;
        let mut joined = Vec::with_capacity(joined_len);
        for outer_index in 0..outer {
            for slice in slices {
                let start = outer_index * inner;
                joined.extend_from_slice(&slice.values[start..start + inner]);
            }
        }
        Ok(joined)
    }

    fn split_batch_values(
        &self,
        values: &[f32],
        batch_size: usize,
        shape: &[usize],
    ) -> error_stack::Result<Vec<Vec<f32>>, InferencerError> {
        self.validate_concrete_shape(shape, batch_size, true)?;
        let batch_axis = self
            .batch_axis()
            .ok_or_else(|| Report::new(InferencerError::MissingBatchAxis))?;
        let outer = shape[..batch_axis].iter().product::<usize>();
        let inner = shape[batch_axis + 1..].iter().product::<usize>();
        let expected = outer
            .checked_mul(batch_size)
            .and_then(|count| count.checked_mul(inner));
        let Some(expected) = expected else {
            return Err(Report::new(InferencerError::OutputCountOverflow));
        };
        if values.len() != expected {
            return Err(Report::new(InferencerError::BatchedValueCount {
                actual: values.len(),
                expected,
            }));
        }
        let slice_len = outer
            .checked_mul(inner)
            .ok_or_else(|| Report::new(InferencerError::SliceCountOverflow))?;
        let mut slices = vec![Vec::with_capacity(slice_len); batch_size];
        for outer_index in 0..outer {
            for (batch_index, slice) in slices.iter_mut().enumerate() {
                let start = (outer_index * batch_size + batch_index) * inner;
                slice.extend_from_slice(&values[start..start + inner]);
            }
        }
        Ok(slices)
    }

    fn tensor_from_dimensions(
        &self,
        dimensions: &[InferencerTensorDimension],
        value: &RuntimeValue,
    ) -> error_stack::Result<RuntimeTensorSlice, InferencerError> {
        let Some((dimension, remaining)) = dimensions.split_first() else {
            let RuntimeValue::F32(value) = value else {
                return Err(Report::new(InferencerError::ElementRequiresF32 {
                    value: value.clone(),
                }));
            };
            return Ok(RuntimeTensorSlice {
                shape: Vec::new(),
                values: vec![value.into_inner()],
            });
        };
        if let InferencerTensorDimension::Batch = dimension {
            return self.tensor_from_dimensions(remaining, value);
        }
        let (values, size) = match (dimension, value) {
            (InferencerTensorDimension::Fixed(expected), RuntimeValue::Array(values))
                if values.len() == expected.get().arch_into() =>
            {
                (values, expected.get().arch_into())
            }
            (InferencerTensorDimension::Fixed(expected), RuntimeValue::Array(values)) => {
                return Err(Report::new(InferencerError::FixedAxisLength {
                    actual: values.len(),
                    expected: *expected,
                }));
            }
            (InferencerTensorDimension::Dynamic, RuntimeValue::Vec(values)) => {
                (values, values.len())
            }
            (InferencerTensorDimension::Fixed(_), value) => {
                return Err(Report::new(InferencerError::FixedAxisRequiresArray {
                    value: value.clone(),
                }));
            }
            (InferencerTensorDimension::Dynamic, value) => {
                return Err(Report::new(InferencerError::DynamicAxisRequiresVec {
                    value: value.clone(),
                }));
            }
            (InferencerTensorDimension::Batch, _) => unreachable!(),
        };
        let mut child_shape: Option<Vec<usize>> = None;
        let mut flattened = Vec::new();
        for child in values {
            let child = self.tensor_from_dimensions(remaining, child)?;
            if let Some(expected) = &child_shape
                && expected != &child.shape
            {
                return Err(Report::new(InferencerError::RaggedChildShapes {
                    expected: expected.clone(),
                    actual: child.shape.clone(),
                }));
            }
            child_shape.get_or_insert_with(|| child.shape.clone());
            flattened.extend(child.values);
        }
        let child_shape = match child_shape {
            Some(shape) => shape,
            None => self.shape_from_dimensions_without_values(remaining)?,
        };
        let mut shape = Vec::with_capacity(child_shape.len() + 1);
        shape.push(size);
        shape.extend(child_shape);
        Ok(RuntimeTensorSlice {
            shape,
            values: flattened,
        })
    }

    fn shape_from_dimensions_without_values(
        &self,
        dimensions: &[InferencerTensorDimension],
    ) -> error_stack::Result<Vec<usize>, InferencerError> {
        let mut shape = Vec::new();
        for dimension in dimensions {
            match dimension {
                InferencerTensorDimension::Fixed(size) => shape.push(size.get().arch_into()),
                InferencerTensorDimension::Dynamic => {
                    return Err(Report::new(InferencerError::InnerDynamicFromEmpty));
                }
                InferencerTensorDimension::Batch => {}
            }
        }
        Ok(shape)
    }

    fn runtime_value_from_dimensions(
        &self,
        dimensions: &[InferencerTensorDimension],
        values: &[f32],
        shape: &[usize],
    ) -> error_stack::Result<RuntimeValue, InferencerError> {
        let Some((dimension, remaining)) = dimensions.split_first() else {
            let [] = shape else {
                return Err(Report::new(InferencerError::ScalarRemainingShape {
                    shape: shape.to_vec(),
                }));
            };
            let [value] = values else {
                return Err(Report::new(InferencerError::ScalarValueCount {
                    actual: values.len(),
                }));
            };
            return Ok(RuntimeValue::F32(OrderedFloat(*value)));
        };
        if let InferencerTensorDimension::Batch = dimension {
            return self.runtime_value_from_dimensions(remaining, values, shape);
        }
        let Some((&size, child_shape)) = shape.split_first() else {
            return Err(Report::new(InferencerError::FewerAxesThanSchema));
        };
        if let InferencerTensorDimension::Fixed(expected) = dimension
            && size != expected.get().arch_into()
        {
            return Err(Report::new(InferencerError::AxisLength {
                size,
                expected: *expected,
            }));
        }
        let child_len = child_shape
            .iter()
            .try_fold(1_usize, |count, size| count.checked_mul(*size))
            .ok_or_else(|| Report::new(InferencerError::ChildCountOverflow))?;
        let expected_len = size
            .checked_mul(child_len)
            .ok_or_else(|| Report::new(InferencerError::ElementCountOverflow))?;
        if values.len() != expected_len {
            return Err(Report::new(InferencerError::ValueCountForShape {
                actual: values.len(),
                expected: expected_len,
                shape: shape.to_vec(),
            }));
        }
        let mut children = Vec::with_capacity(size);
        for index in 0..size {
            let start = index * child_len;
            children.push(self.runtime_value_from_dimensions(
                remaining,
                &values[start..start + child_len],
                child_shape,
            )?);
        }
        match dimension {
            InferencerTensorDimension::Fixed(_) => Ok(RuntimeValue::Array(children)),
            InferencerTensorDimension::Dynamic => Ok(RuntimeValue::Vec(children)),
            InferencerTensorDimension::Batch => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        InferencerTensorDimension, InferencerTensorElementType, InferencerTensorRepresentation,
        InferencerTensorSchema,
    };
    use nonzero_ext::nonzero;
    use ordered_float::OrderedFloat;

    use super::{RuntimeTensorSchema, RuntimeTensorSlice};
    use crate::runtime_schema::RuntimeValue;

    #[test]
    fn multidimensional_tensor_conversion_preserves_nested_array_shape() {
        let schema = InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![
                InferencerTensorDimension::Fixed(nonzero!(2u32)),
                InferencerTensorDimension::Fixed(nonzero!(3u32)),
            ],
        };
        let value = RuntimeValue::Array(vec![
            RuntimeValue::Array(vec![
                RuntimeValue::F32(OrderedFloat(1.0)),
                RuntimeValue::F32(OrderedFloat(2.0)),
                RuntimeValue::F32(OrderedFloat(3.0)),
            ]),
            RuntimeValue::Array(vec![
                RuntimeValue::F32(OrderedFloat(4.0)),
                RuntimeValue::F32(OrderedFloat(5.0)),
                RuntimeValue::F32(OrderedFloat(6.0)),
            ]),
        ]);

        let tensor = schema.tensor_from_runtime_value(&value).unwrap();

        assert_eq!(tensor.shape, vec![2, 3]);
        assert_eq!(tensor.values, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        assert_eq!(
            schema
                .runtime_value_from_tensor(&tensor.values, &tensor.shape)
                .unwrap(),
            value
        );
    }

    #[test]
    fn dynamic_dense_tensor_rejects_ragged_vectors() {
        let schema = InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![
                InferencerTensorDimension::Dynamic,
                InferencerTensorDimension::Dynamic,
            ],
        };
        let value = RuntimeValue::Vec(vec![
            RuntimeValue::Vec(vec![RuntimeValue::F32(OrderedFloat(1.0))]),
            RuntimeValue::Vec(vec![
                RuntimeValue::F32(OrderedFloat(2.0)),
                RuntimeValue::F32(OrderedFloat(3.0)),
            ]),
        ]);

        assert!(schema.tensor_from_runtime_value(&value).is_err());
    }

    #[test]
    fn non_leading_batch_axis_preserves_message_slices() {
        let schema = InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![
                InferencerTensorDimension::Fixed(nonzero!(2u32)),
                InferencerTensorDimension::Batch,
                InferencerTensorDimension::Fixed(nonzero!(3u32)),
            ],
        };
        let slices = vec![
            RuntimeTensorSlice {
                shape: vec![2, 3],
                values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            },
            RuntimeTensorSlice {
                shape: vec![2, 3],
                values: vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            },
        ];

        let joined = schema.join_batch_values(&slices).unwrap();

        assert_eq!(
            joined,
            vec![
                1.0, 2.0, 3.0, 10.0, 20.0, 30.0, 4.0, 5.0, 6.0, 40.0, 50.0, 60.0
            ]
        );
        assert_eq!(
            schema.split_batch_values(&joined, 2, &[2, 2, 3]).unwrap(),
            slices
                .into_iter()
                .map(|slice| slice.values)
                .collect::<Vec<_>>()
        );
    }
}
