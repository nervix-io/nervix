use std::path::Path;

use ahash::{HashMap, HashMapExt};
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

use crate::runtime_schema::{RuntimeRecord, RuntimeValue};

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
    pub(super) async fn load(version: u64, path: &Path) -> Result<Self, String> {
        let path = path.to_path_buf();
        let session = tokio::task::spawn_blocking(move || {
            let mut builder = Session::builder()
                .map_err(|error| format!("failed to initialize ONNX session: {error}"))?;
            builder
                .commit_from_file(&path)
                .map_err(|error| format!("failed to load ONNX model '{}': {error}", path.display()))
        })
        .await
        .map_err(|error| format!("failed to join ONNX model loading task: {error}"))??;
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
        records: &[RuntimeRecord],
        inputs: &[InferencerTensorMapping],
        output_schema: &[InferencerTensorDeclaration],
        mode: InferencerExecutionMode,
    ) -> Result<Vec<HashMap<String, RuntimeValue>>, String> {
        let prepared = PreparedExecution::from_records(records, inputs, output_schema, mode)?;
        let session = Arc::clone(&self.session);
        tokio::task::spawn_blocking(move || prepared.run(&mut session.lock()))
            .await
            .map_err(|error| format!("failed to join ONNX execution task: {error}"))?
    }
}

struct PreparedExecution {
    invocations: Vec<PreparedInvocation>,
    output_schema: Vec<InferencerTensorDeclaration>,
    message_count: usize,
    mode: InferencerExecutionMode,
}

impl PreparedExecution {
    fn from_records(
        records: &[RuntimeRecord],
        inputs: &[InferencerTensorMapping],
        output_schema: &[InferencerTensorDeclaration],
        mode: InferencerExecutionMode,
    ) -> Result<Self, String> {
        if records.is_empty() {
            return Err("cannot execute ONNX inference for an empty message batch".to_string());
        }
        let invocations = match mode {
            InferencerExecutionMode::PerMessage => records
                .iter()
                .enumerate()
                .map(|(message_index, record)| {
                    PreparedInvocation::for_record(message_index, record, inputs)
                })
                .collect::<Result<Vec<_>, _>>()?,
            InferencerExecutionMode::Batched => {
                PreparedInvocation::for_shape_batches(records, inputs)?
            }
        };
        Ok(Self {
            invocations,
            output_schema: output_schema.to_vec(),
            message_count: records.len(),
            mode,
        })
    }

    fn run(self, session: &mut Session) -> Result<Vec<HashMap<String, RuntimeValue>>, String> {
        let mut records = vec![HashMap::new(); self.message_count];
        match self.mode {
            InferencerExecutionMode::PerMessage => {
                for invocation in self.invocations {
                    let [message_index] = invocation.message_indices.as_slice() else {
                        return Err("per-message ONNX invocation has invalid routing".to_string());
                    };
                    let message_index = *message_index;
                    let output_tensors = invocation.run(session, &self.output_schema, 1)?;
                    for (declaration, tensor) in self.output_schema.iter().zip(output_tensors) {
                        records[message_index].insert(
                            declaration.tensor.clone(),
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
                    for (declaration, tensor) in self.output_schema.iter().zip(output_tensors) {
                        let slices = declaration.schema.split_batch_values(
                            &tensor.values,
                            batch_size,
                            &tensor.shape,
                        )?;
                        let slice_shape = declaration.schema.shape_without_batch(&tensor.shape)?;
                        for (message_index, values) in message_indices.iter().zip(slices) {
                            records[*message_index].insert(
                                declaration.tensor.clone(),
                                declaration
                                    .schema
                                    .runtime_value_from_tensor(&values, &slice_shape)?,
                            );
                        }
                    }
                }
            }
        }
        Ok(records)
    }
}

struct PreparedInvocation {
    inputs: Vec<PreparedTensor>,
    message_indices: Vec<usize>,
}

impl PreparedInvocation {
    fn for_record(
        message_index: usize,
        record: &RuntimeRecord,
        mappings: &[InferencerTensorMapping],
    ) -> Result<Self, String> {
        let inputs = mappings
            .iter()
            .map(|mapping| {
                let value = record.value(&mapping.tensor).ok_or_else(|| {
                    format!(
                        "ONNX input tensor '{}' mapped value is missing",
                        mapping.tensor
                    )
                })?;
                let tensor = mapping.schema.tensor_from_runtime_value(value)?;
                Ok(PreparedTensor {
                    name: mapping.tensor.clone(),
                    shape: tensor.shape,
                    values: tensor.values,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            inputs,
            message_indices: vec![message_index],
        })
    }

    fn for_shape_batches(
        records: &[RuntimeRecord],
        mappings: &[InferencerTensorMapping],
    ) -> Result<Vec<Self>, String> {
        let mut groups = HashMap::<Vec<Vec<usize>>, Vec<usize>>::new();
        for (message_index, record) in records.iter().enumerate() {
            let shapes = mappings
                .iter()
                .map(|mapping| {
                    let value = record.value(&mapping.tensor).ok_or_else(|| {
                        format!(
                            "ONNX input tensor '{}' mapped value is missing",
                            mapping.tensor
                        )
                    })?;
                    Ok(mapping.schema.tensor_from_runtime_value(value)?.shape)
                })
                .collect::<Result<Vec<_>, String>>()?;
            groups.entry(shapes).or_default().push(message_index);
        }
        groups
            .into_values()
            .map(|message_indices| Self::for_batch(records, mappings, message_indices))
            .collect()
    }

    fn for_batch(
        records: &[RuntimeRecord],
        mappings: &[InferencerTensorMapping],
        message_indices: Vec<usize>,
    ) -> Result<Self, String> {
        let inputs = mappings
            .iter()
            .map(|mapping| {
                let slices = message_indices
                    .iter()
                    .map(|message_index| {
                        let record = &records[*message_index];
                        let value = record.value(&mapping.tensor).ok_or_else(|| {
                            format!(
                                "ONNX input tensor '{}' mapped value is missing",
                                mapping.tensor
                            )
                        })?;
                        mapping.schema.tensor_from_runtime_value(value)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let shape = mapping
                    .schema
                    .batch_shape(&slices[0].shape, message_indices.len())?;
                Ok(PreparedTensor {
                    name: mapping.tensor.clone(),
                    shape,
                    values: mapping.schema.join_batch_values(&slices)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
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
    ) -> Result<Vec<ExecutedTensor>, String> {
        let inputs = self
            .inputs
            .into_iter()
            .map(|tensor| {
                let value = Tensor::from_array((tensor.shape, tensor.values)).map_err(|error| {
                    format!("failed to build ONNX input '{}': {error}", tensor.name)
                })?;
                Ok((tensor.name, SessionInputValue::from(value)))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let session_outputs = session
            .run(inputs)
            .map_err(|error| format!("ONNX model invocation failed: {error}"))?;
        output_schema
            .iter()
            .map(|declaration| {
                let output = session_outputs.get(&declaration.tensor).ok_or_else(|| {
                    format!(
                        "ONNX invocation omitted output tensor '{}'",
                        declaration.tensor
                    )
                })?;
                let (shape, values) = output.try_extract_tensor::<f32>().map_err(|error| {
                    format!(
                        "failed to extract ONNX output tensor '{}' as F32: {error}",
                        declaration.tensor
                    )
                })?;
                let shape = shape
                    .iter()
                    .map(|dimension| usize::try_from(*dimension))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| {
                        format!(
                            "ONNX output tensor '{}' returned a negative shape {:?}",
                            declaration.tensor,
                            shape.as_ref()
                        )
                    })?;
                declaration.schema.validate_concrete_shape(
                    &shape,
                    batch_size,
                    declaration.schema.batch_axis().is_some(),
                )?;
                Ok(ExecutedTensor {
                    shape,
                    values: values.to_vec(),
                })
            })
            .collect()
    }
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
    fn tensor_from_runtime_value(&self, value: &RuntimeValue)
    -> Result<RuntimeTensorSlice, String>;
    fn runtime_value_from_tensor(
        &self,
        values: &[f32],
        shape: &[usize],
    ) -> Result<RuntimeValue, String>;
    fn batch_shape(&self, slice_shape: &[usize], batch_size: usize) -> Result<Vec<usize>, String>;
    fn shape_without_batch(&self, shape: &[usize]) -> Result<Vec<usize>, String>;
    fn validate_concrete_shape(
        &self,
        shape: &[usize],
        batch_size: usize,
        include_batch: bool,
    ) -> Result<(), String>;
    fn join_batch_values(&self, slices: &[RuntimeTensorSlice]) -> Result<Vec<f32>, String>;
    fn split_batch_values(
        &self,
        values: &[f32],
        batch_size: usize,
        shape: &[usize],
    ) -> Result<Vec<Vec<f32>>, String>;
    fn tensor_from_dimensions(
        &self,
        dimensions: &[InferencerTensorDimension],
        value: &RuntimeValue,
    ) -> Result<RuntimeTensorSlice, String>;
    fn shape_from_dimensions_without_values(
        &self,
        dimensions: &[InferencerTensorDimension],
    ) -> Result<Vec<usize>, String>;
    fn runtime_value_from_dimensions(
        &self,
        dimensions: &[InferencerTensorDimension],
        values: &[f32],
        shape: &[usize],
    ) -> Result<RuntimeValue, String>;
}

impl RuntimeTensorSchema for InferencerTensorSchema {
    fn tensor_from_runtime_value(
        &self,
        value: &RuntimeValue,
    ) -> Result<RuntimeTensorSlice, String> {
        self.tensor_from_dimensions(&self.dimensions, value)
    }

    fn runtime_value_from_tensor(
        &self,
        values: &[f32],
        shape: &[usize],
    ) -> Result<RuntimeValue, String> {
        self.runtime_value_from_dimensions(&self.dimensions, values, shape)
    }

    fn batch_shape(&self, slice_shape: &[usize], batch_size: usize) -> Result<Vec<usize>, String> {
        let expected_slice_rank = self.dimensions.len().saturating_sub(1);
        if slice_shape.len() != expected_slice_rank {
            return Err(format!(
                "tensor slice rank {} does not match declared rank {}",
                slice_shape.len(),
                expected_slice_rank
            ));
        }
        let mut shape = Vec::with_capacity(self.dimensions.len());
        let mut slice_dimensions = slice_shape.iter();
        for dimension in &self.dimensions {
            if let InferencerTensorDimension::Batch = dimension {
                shape.push(batch_size);
            } else {
                shape.push(*slice_dimensions.next().expect("slice rank was validated"));
            }
        }
        self.validate_concrete_shape(&shape, batch_size, true)?;
        Ok(shape)
    }

    fn shape_without_batch(&self, shape: &[usize]) -> Result<Vec<usize>, String> {
        let batch_axis = self
            .batch_axis()
            .ok_or_else(|| "batched tensor schema has no BATCH axis".to_string())?;
        if shape.len() != self.dimensions.len() {
            return Err(format!(
                "batched tensor rank {} does not match declared rank {}",
                shape.len(),
                self.dimensions.len()
            ));
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
    ) -> Result<(), String> {
        let dimensions = self
            .dimensions
            .iter()
            .filter(|dimension| include_batch || !dimension.is_batch());
        if shape.len() != dimensions.clone().count() {
            return Err(format!(
                "tensor shape {shape:?} does not match declared dimensions {:?}",
                self.dimensions
            ));
        }
        for (actual, declared) in shape.iter().zip(dimensions) {
            match declared {
                InferencerTensorDimension::Fixed(expected) if *actual != *expected as usize => {
                    return Err(format!(
                        "tensor shape {shape:?} has dimension {actual}, expected {expected}"
                    ));
                }
                InferencerTensorDimension::Batch if *actual != batch_size => {
                    return Err(format!(
                        "tensor shape {shape:?} has batch dimension {actual}, expected \
                         {batch_size}"
                    ));
                }
                InferencerTensorDimension::Fixed(_)
                | InferencerTensorDimension::Dynamic
                | InferencerTensorDimension::Batch => {}
            }
        }
        Ok(())
    }

    fn join_batch_values(&self, slices: &[RuntimeTensorSlice]) -> Result<Vec<f32>, String> {
        let first = slices
            .first()
            .ok_or_else(|| "cannot join an empty tensor batch".to_string())?;
        if let Some(slice) = slices.iter().find(|slice| slice.shape != first.shape) {
            return Err(format!(
                "batched DYNAMIC tensor slices must have one concrete shape; got {:?} and {:?}",
                first.shape, slice.shape
            ));
        }
        let shape = self.batch_shape(&first.shape, slices.len())?;
        let batch_axis = self
            .batch_axis()
            .ok_or_else(|| "batched tensor schema has no BATCH axis".to_string())?;
        let outer = shape[..batch_axis]
            .iter()
            .try_fold(1_usize, |count, size| count.checked_mul(*size))
            .ok_or_else(|| "batched tensor outer element count overflowed".to_string())?;
        let inner = shape[batch_axis + 1..]
            .iter()
            .try_fold(1_usize, |count, size| count.checked_mul(*size))
            .ok_or_else(|| "batched tensor inner element count overflowed".to_string())?;
        let expected_slice_len = outer
            .checked_mul(inner)
            .ok_or_else(|| "batched tensor slice element count overflowed".to_string())?;
        if let Some(actual) = slices
            .iter()
            .map(|slice| slice.values.len())
            .find(|actual| *actual != expected_slice_len)
        {
            return Err(format!(
                "batched tensor slice contains {actual} values, expected {expected_slice_len}"
            ));
        }
        let mut joined = Vec::with_capacity(expected_slice_len.saturating_mul(slices.len()));
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
    ) -> Result<Vec<Vec<f32>>, String> {
        self.validate_concrete_shape(shape, batch_size, true)?;
        let batch_axis = self
            .batch_axis()
            .ok_or_else(|| "batched tensor schema has no BATCH axis".to_string())?;
        let outer = shape[..batch_axis].iter().product::<usize>();
        let inner = shape[batch_axis + 1..].iter().product::<usize>();
        let expected = outer.saturating_mul(batch_size).saturating_mul(inner);
        if values.len() != expected {
            return Err(format!(
                "batched output contains {} values, expected {}",
                values.len(),
                expected
            ));
        }
        let mut slices = vec![Vec::with_capacity(outer.saturating_mul(inner)); batch_size];
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
    ) -> Result<RuntimeTensorSlice, String> {
        let Some((dimension, remaining)) = dimensions.split_first() else {
            let RuntimeValue::F32(value) = value else {
                return Err(format!("tensor element requires F32, got {value:?}"));
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
                if values.len() == *expected as usize =>
            {
                (values, *expected as usize)
            }
            (InferencerTensorDimension::Fixed(expected), RuntimeValue::Array(values)) => {
                return Err(format!(
                    "tensor ARRAY axis contains {} values, expected {expected}",
                    values.len()
                ));
            }
            (InferencerTensorDimension::Dynamic, RuntimeValue::Vec(values)) => {
                (values, values.len())
            }
            (InferencerTensorDimension::Fixed(_), value) => {
                return Err(format!("fixed tensor axis requires ARRAY, got {value:?}"));
            }
            (InferencerTensorDimension::Dynamic, value) => {
                return Err(format!("dynamic tensor axis requires VEC, got {value:?}"));
            }
            (InferencerTensorDimension::Batch, _) => unreachable!(),
        };
        let mut child_shape = None;
        let mut flattened = Vec::new();
        for child in values {
            let child = self.tensor_from_dimensions(remaining, child)?;
            if let Some(expected) = &child_shape
                && expected != &child.shape
            {
                return Err(format!(
                    "dense tensor is ragged: child shapes {expected:?} and {:?} differ",
                    child.shape
                ));
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
    ) -> Result<Vec<usize>, String> {
        let mut shape = Vec::new();
        for dimension in dimensions {
            match dimension {
                InferencerTensorDimension::Fixed(size) => shape.push(*size as usize),
                InferencerTensorDimension::Dynamic => {
                    return Err(
                        "cannot infer an inner DYNAMIC axis from an empty outer vector".to_string(),
                    );
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
    ) -> Result<RuntimeValue, String> {
        let Some((dimension, remaining)) = dimensions.split_first() else {
            let [] = shape else {
                return Err(format!(
                    "scalar tensor has unexpected remaining shape {shape:?}"
                ));
            };
            let [value] = values else {
                return Err(format!(
                    "scalar tensor contains {} values, expected 1",
                    values.len()
                ));
            };
            return Ok(RuntimeValue::F32(OrderedFloat(*value)));
        };
        if let InferencerTensorDimension::Batch = dimension {
            return self.runtime_value_from_dimensions(remaining, values, shape);
        }
        let Some((&size, child_shape)) = shape.split_first() else {
            return Err("tensor value has fewer axes than its schema".to_string());
        };
        if let InferencerTensorDimension::Fixed(expected) = dimension
            && size != *expected as usize
        {
            return Err(format!(
                "tensor axis has length {size}, expected {expected}"
            ));
        }
        let child_len = child_shape
            .iter()
            .try_fold(1_usize, |count, size| count.checked_mul(*size))
            .ok_or_else(|| "tensor child element count overflowed".to_string())?;
        let expected_len = size
            .checked_mul(child_len)
            .ok_or_else(|| "tensor element count overflowed".to_string())?;
        if values.len() != expected_len {
            return Err(format!(
                "tensor contains {} values, expected {expected_len} for shape {shape:?}",
                values.len()
            ));
        }
        let children = (0..size)
            .map(|index| {
                let start = index * child_len;
                self.runtime_value_from_dimensions(
                    remaining,
                    &values[start..start + child_len],
                    child_shape,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
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
    use ordered_float::OrderedFloat;

    use super::{RuntimeTensorSchema, RuntimeTensorSlice};
    use crate::runtime_schema::RuntimeValue;

    #[test]
    fn multidimensional_tensor_conversion_preserves_nested_array_shape() {
        let schema = InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![
                InferencerTensorDimension::Fixed(2),
                InferencerTensorDimension::Fixed(3),
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
                InferencerTensorDimension::Fixed(2),
                InferencerTensorDimension::Batch,
                InferencerTensorDimension::Fixed(3),
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
