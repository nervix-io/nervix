use std::{collections::HashMap, path::Path, sync::Arc};

use nervix_models::{
    InferencerExecutionMode, InferencerTensorDimension, InferencerTensorMapping,
    InferencerTensorSchema,
};
use ordered_float::OrderedFloat;
use ort::{
    session::{Session, SessionInputValue},
    value::Tensor,
};
use parking_lot::Mutex;

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
        outputs: &[InferencerTensorMapping],
        mode: InferencerExecutionMode,
    ) -> Result<Vec<HashMap<String, RuntimeValue>>, String> {
        let prepared = PreparedExecution::from_records(records, inputs, outputs, mode)?;
        let session = Arc::clone(&self.session);
        tokio::task::spawn_blocking(move || prepared.run(&mut session.lock()))
            .await
            .map_err(|error| format!("failed to join ONNX execution task: {error}"))?
    }
}

struct PreparedExecution {
    invocations: Vec<PreparedInvocation>,
    outputs: Vec<InferencerTensorMapping>,
    message_count: usize,
    mode: InferencerExecutionMode,
}

impl PreparedExecution {
    fn from_records(
        records: &[RuntimeRecord],
        inputs: &[InferencerTensorMapping],
        outputs: &[InferencerTensorMapping],
        mode: InferencerExecutionMode,
    ) -> Result<Self, String> {
        if records.is_empty() {
            return Err("cannot execute ONNX inference for an empty message batch".to_string());
        }
        let invocations = match mode {
            InferencerExecutionMode::PerMessage => records
                .iter()
                .map(|record| PreparedInvocation::for_record(record, inputs))
                .collect::<Result<Vec<_>, _>>()?,
            InferencerExecutionMode::Batched => {
                vec![PreparedInvocation::for_batch(records, inputs)?]
            }
        };
        Ok(Self {
            invocations,
            outputs: outputs.to_vec(),
            message_count: records.len(),
            mode,
        })
    }

    fn run(self, session: &mut Session) -> Result<Vec<HashMap<String, RuntimeValue>>, String> {
        let mut records = vec![HashMap::new(); self.message_count];
        match self.mode {
            InferencerExecutionMode::PerMessage => {
                for (message_index, invocation) in self.invocations.into_iter().enumerate() {
                    let output_tensors = invocation.run(session, &self.outputs, 1)?;
                    for (mapping, tensor) in self.outputs.iter().zip(output_tensors) {
                        records[message_index].insert(
                            mapping.field.as_str().to_string(),
                            mapping.schema.runtime_value_from_slice(&tensor.values)?,
                        );
                    }
                }
            }
            InferencerExecutionMode::Batched => {
                let invocation = self
                    .invocations
                    .into_iter()
                    .next()
                    .expect("batched execution must contain one invocation");
                let output_tensors = invocation.run(session, &self.outputs, self.message_count)?;
                for (mapping, tensor) in self.outputs.iter().zip(output_tensors) {
                    let slices = mapping
                        .schema
                        .split_batch_values(&tensor.values, self.message_count)?;
                    for (message_index, values) in slices.into_iter().enumerate() {
                        records[message_index].insert(
                            mapping.field.as_str().to_string(),
                            mapping.schema.runtime_value_from_slice(&values)?,
                        );
                    }
                }
            }
        }
        Ok(records)
    }
}

struct PreparedInvocation {
    inputs: Vec<PreparedTensor>,
}

impl PreparedInvocation {
    fn for_record(
        record: &RuntimeRecord,
        mappings: &[InferencerTensorMapping],
    ) -> Result<Self, String> {
        let inputs = mappings
            .iter()
            .map(|mapping| {
                let value = record.value(mapping.field.as_str()).ok_or_else(|| {
                    format!(
                        "ONNX input tensor '{}' field '{}.{}' is missing",
                        mapping.tensor,
                        mapping.relay.as_str(),
                        mapping.field.as_str()
                    )
                })?;
                Ok(PreparedTensor {
                    name: mapping.tensor.clone(),
                    shape: mapping.schema.concrete_shape(1, false),
                    values: mapping.schema.values_from_runtime_value(value)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self { inputs })
    }

    fn for_batch(
        records: &[RuntimeRecord],
        mappings: &[InferencerTensorMapping],
    ) -> Result<Self, String> {
        let inputs = mappings
            .iter()
            .map(|mapping| {
                let slices = records
                    .iter()
                    .map(|record| {
                        let value = record.value(mapping.field.as_str()).ok_or_else(|| {
                            format!(
                                "ONNX input tensor '{}' field '{}.{}' is missing",
                                mapping.tensor,
                                mapping.relay.as_str(),
                                mapping.field.as_str()
                            )
                        })?;
                        mapping.schema.values_from_runtime_value(value)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PreparedTensor {
                    name: mapping.tensor.clone(),
                    shape: mapping.schema.concrete_shape(records.len(), true),
                    values: mapping.schema.join_batch_values(&slices)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self { inputs })
    }

    fn run(
        self,
        session: &mut Session,
        outputs: &[InferencerTensorMapping],
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
        outputs
            .iter()
            .map(|mapping| {
                let output = session_outputs.get(&mapping.tensor).ok_or_else(|| {
                    format!("ONNX invocation omitted output tensor '{}'", mapping.tensor)
                })?;
                let (shape, values) = output.try_extract_tensor::<f32>().map_err(|error| {
                    format!(
                        "failed to extract ONNX output tensor '{}' as F32: {error}",
                        mapping.tensor
                    )
                })?;
                let expected_shape = mapping
                    .schema
                    .concrete_shape(batch_size, mapping.schema.batch_axis().is_some());
                let expected_shape = expected_shape
                    .into_iter()
                    .map(|dimension| dimension as i64)
                    .collect::<Vec<_>>();
                if shape.as_ref() != expected_shape.as_slice() {
                    return Err(format!(
                        "ONNX output tensor '{}' returned shape {:?}, expected {:?}",
                        mapping.tensor,
                        shape.as_ref(),
                        expected_shape
                    ));
                }
                Ok(ExecutedTensor {
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
    values: Vec<f32>,
}

trait RuntimeTensorSchema {
    fn concrete_shape(&self, batch_size: usize, include_batch: bool) -> Vec<usize>;
    fn values_from_runtime_value(&self, value: &RuntimeValue) -> Result<Vec<f32>, String>;
    fn runtime_value_from_slice(&self, values: &[f32]) -> Result<RuntimeValue, String>;
    fn join_batch_values(&self, slices: &[Vec<f32>]) -> Result<Vec<f32>, String>;
    fn split_batch_values(
        &self,
        values: &[f32],
        batch_size: usize,
    ) -> Result<Vec<Vec<f32>>, String>;
}

impl RuntimeTensorSchema for InferencerTensorSchema {
    fn concrete_shape(&self, batch_size: usize, include_batch: bool) -> Vec<usize> {
        self.dimensions
            .iter()
            .filter_map(|dimension| match dimension {
                InferencerTensorDimension::Fixed(size) => Some(*size as usize),
                InferencerTensorDimension::Batch if include_batch => Some(batch_size),
                InferencerTensorDimension::Batch => None,
            })
            .collect()
    }

    fn values_from_runtime_value(&self, value: &RuntimeValue) -> Result<Vec<f32>, String> {
        let expected = self
            .fixed_element_count()
            .ok_or_else(|| "tensor slice element count overflowed".to_string())?;
        let values = match value {
            RuntimeValue::F32(value) => vec![value.into_inner()],
            RuntimeValue::Array(values) => values
                .iter()
                .map(|value| match value {
                    RuntimeValue::F32(value) => Ok(value.into_inner()),
                    other => Err(format!("tensor slice contains non-F32 value {other:?}")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            other => return Err(format!("tensor slice requires F32 values, got {other:?}")),
        };
        if values.len() != expected {
            return Err(format!(
                "tensor slice contains {} F32 values, expected {}",
                values.len(),
                expected
            ));
        }
        Ok(values)
    }

    fn runtime_value_from_slice(&self, values: &[f32]) -> Result<RuntimeValue, String> {
        let has_fixed_dimensions = self.dimensions.iter().any(|dimension| {
            if let InferencerTensorDimension::Fixed(_) = dimension {
                true
            } else {
                false
            }
        });
        if !has_fixed_dimensions {
            let [value] = values else {
                return Err(format!(
                    "scalar tensor slice contains {} values, expected 1",
                    values.len()
                ));
            };
            return Ok(RuntimeValue::F32(OrderedFloat(*value)));
        }
        Ok(RuntimeValue::Array(
            values
                .iter()
                .map(|value| RuntimeValue::F32(OrderedFloat(*value)))
                .collect(),
        ))
    }

    fn join_batch_values(&self, slices: &[Vec<f32>]) -> Result<Vec<f32>, String> {
        let batch_axis = self
            .batch_axis()
            .ok_or_else(|| "batched tensor schema has no BATCH axis".to_string())?;
        let outer = self.dimensions[..batch_axis]
            .iter()
            .map(|dimension| match dimension {
                InferencerTensorDimension::Fixed(size) => *size as usize,
                InferencerTensorDimension::Batch => 1,
            })
            .product::<usize>();
        let inner = self.dimensions[batch_axis + 1..]
            .iter()
            .map(|dimension| match dimension {
                InferencerTensorDimension::Fixed(size) => *size as usize,
                InferencerTensorDimension::Batch => 1,
            })
            .product::<usize>();
        let expected_slice_len = outer.saturating_mul(inner);
        if let Some(actual) = slices
            .iter()
            .map(Vec::len)
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
                joined.extend_from_slice(&slice[start..start + inner]);
            }
        }
        Ok(joined)
    }

    fn split_batch_values(
        &self,
        values: &[f32],
        batch_size: usize,
    ) -> Result<Vec<Vec<f32>>, String> {
        let batch_axis = self
            .batch_axis()
            .ok_or_else(|| "batched tensor schema has no BATCH axis".to_string())?;
        let outer = self.dimensions[..batch_axis]
            .iter()
            .map(|dimension| match dimension {
                InferencerTensorDimension::Fixed(size) => *size as usize,
                InferencerTensorDimension::Batch => 1,
            })
            .product::<usize>();
        let inner = self.dimensions[batch_axis + 1..]
            .iter()
            .map(|dimension| match dimension {
                InferencerTensorDimension::Fixed(size) => *size as usize,
                InferencerTensorDimension::Batch => 1,
            })
            .product::<usize>();
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
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        InferencerTensorDimension, InferencerTensorElementType, InferencerTensorRepresentation,
        InferencerTensorSchema,
    };

    use super::RuntimeTensorSchema;

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
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        ];

        let joined = schema.join_batch_values(&slices).unwrap();

        assert_eq!(
            joined,
            vec![
                1.0, 2.0, 3.0, 10.0, 20.0, 30.0, 4.0, 5.0, 6.0, 40.0, 50.0, 60.0
            ]
        );
        assert_eq!(schema.split_batch_values(&joined, 2).unwrap(), slices);
    }
}
