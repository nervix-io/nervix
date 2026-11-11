//! Explicit native tensor generations owned by one concrete branch.
//!
//! Layer: data plane.
//! Owns: prepared INITIAL tensors, resolved native ports and atomic successor installation.
//! Depends on: typed state plans, Arrow, configuration constant evaluation and bounded execution.
//! Must not know: Models, route flush cadence, snapshots or placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "executes a branch's explicit ONNX state")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(
        plans,
        reason = "executes only validated tensor plans and prepared native generations"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "runs one native invocation per admitted branch row"
    )
)]

use std::path::Path;

use arch_into::ArchInto as _;
use arrow_array::RecordBatch;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Cancellation, CpuClass, Executor, MemoryClass};
use nervix_models::{
    InferencerStateInitial, InferencerTensorDeclaration, InferencerTensorDimension,
    InferencerTensorMapping, InferencerTensorSchema, Timestamp,
};
use nervix_primitives::sync::{Arc, StdArc};
use nervix_roto::UdfExecutor;
use ort::{
    session::{Session, SessionInputValue},
    value::{DynValue, Tensor},
};

use super::{InferencerError, RuntimeTensorSchema, onnx_environment};
use crate::{registry::InferencerStatePlan, runtime_schema::RuntimeRecordBatch};

#[derive(Debug, thiserror::Error)]
pub(in crate::runtime) enum StatefulInferenceError {
    #[error("failed to prepare INITIAL tensors")]
    Initial,
    #[error("the stateful inference working set exceeds the configured relay operation limit")]
    WorkingSet,
    #[error(
        "the declared state holds {bytes} bytes, exceeding one configured snapshot section \
         ({limit} bytes)"
    )]
    StateSize { bytes: u64, limit: u64 },
    #[error("bounded execution refused stateful inference")]
    Execution,
    #[error("stateful inference was cancelled")]
    Cancelled,
    #[error("failed to load the stateful ONNX session")]
    Load,
    #[error("model port '{port}' is unavailable")]
    Port { port: String },
    #[error("stateful inference requires exactly one row")]
    RowCount,
    #[error("input tensor '{port}' is unavailable")]
    InputPort { port: String },
    #[error("input tensor '{port}' contains null values")]
    NullInput { port: String },
    #[error("input tensor '{port}' has an invalid Arrow type {actual:?}")]
    InputType {
        port: String,
        actual: arrow_schema::DataType,
    },
    #[error("input tensor '{port}' has an invalid shape")]
    InputShape { port: String },
    #[error("failed to build a native input tensor")]
    BuildInput,
    #[error("the ONNX model refused this row")]
    Invocation,
    #[error("model output '{port}' has an invalid tensor type or shape")]
    OutputTensor { port: String },
    #[error("output tensor has an invalid Arrow type {actual:?}")]
    OutputType { actual: arrow_schema::DataType },
    #[error("output tensor size exceeds the supported size")]
    OutputSize,
    #[error("failed to build the output Arrow arrays")]
    OutputArray,
    #[error("failed to build the output Arrow batch")]
    OutputBatch,
}

/// Immutable initial generation shared by newly created branches; successors belong to one branch.
pub(in crate::runtime) struct NativeGeneration {
    tensors: Vec<DynValue>,
}

impl std::fmt::Debug for NativeGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeGeneration")
            .field("tensors", &self.tensors.len())
            .finish_non_exhaustive()
    }
}
impl NativeGeneration {
    pub(in crate::runtime) fn snapshot(
        &self,
        cancellation: &Cancellation,
    ) -> error_stack::Result<
        Vec<crate::inferencer_state_snapshot::TensorSnapshot<'_>>,
        StatefulInferenceError,
    > {
        self.tensors
            .iter()
            .map(|tensor| {
                cancellation
                    .check()
                    .change_context(StatefulInferenceError::Cancelled)?;
                let (shape, values) = tensor
                    .try_extract_tensor::<f32>()
                    .change_context(StatefulInferenceError::Initial)?;
                let shape = shape
                    .iter()
                    .map(|axis| {
                        u64::try_from(*axis).change_context(StatefulInferenceError::Initial)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(crate::inferencer_state_snapshot::TensorSnapshot {
                    shape,
                    values: std::borrow::Cow::Borrowed(values),
                })
            })
            .collect()
    }
    pub(in crate::runtime) fn restore(
        tensors: Vec<crate::inferencer_state_snapshot::TensorSnapshot<'static>>,
        plan: &InferencerStatePlan,
        cancellation: &Cancellation,
    ) -> error_stack::Result<Self, StatefulInferenceError> {
        if tensors.len() != plan.bindings.len() {
            return Err(Report::new(StatefulInferenceError::Initial));
        }
        let tensors = tensors
            .into_iter()
            .zip(&plan.bindings)
            .map(|(tensor, binding)| {
                cancellation
                    .check()
                    .change_context(StatefulInferenceError::Cancelled)?;
                let expected: Vec<u64> = binding
                    .schema
                    .dimensions
                    .iter()
                    .filter_map(|axis| match axis {
                        InferencerTensorDimension::Fixed(size) => Some(u64::from(size.get())),
                        _ => None,
                    })
                    .collect();
                if tensor.shape != expected || tensor.values.len() != binding.elements {
                    return Err(Report::new(StatefulInferenceError::Initial));
                }
                let shape = tensor
                    .shape
                    .into_iter()
                    .map(usize::try_from)
                    .collect::<Result<Vec<_>, _>>()
                    .change_context(StatefulInferenceError::Initial)?;
                Tensor::<f32>::from_array((shape, tensor.values.into_owned()))
                    .map(Tensor::into_dyn)
                    .change_context(StatefulInferenceError::Initial)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { tensors })
    }
}

#[derive(Clone)]
pub(in crate::runtime) struct PreparedStatefulInference {
    pub(in crate::runtime) inputs: Vec<InferencerTensorMapping>,
    pub(in crate::runtime) outputs: Vec<InferencerTensorDeclaration>,
    pub(in crate::runtime) state: InferencerStatePlan,
    initial: Arc<NativeGeneration>,
    output_arrow_schema: StdArc<arrow_schema::Schema>,
}
impl std::fmt::Debug for PreparedStatefulInference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStatefulInference")
            .field("bindings", &self.state.bindings.len())
            .field("bytes_per_branch", &self.state.bytes_per_branch)
            .finish_non_exhaustive()
    }
}
impl PreparedStatefulInference {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "materializes initial tensors before publishing a processor revision"
        )
    )]
    pub(in crate::runtime) async fn prepare(
        executor: &Executor,
        inputs: &[InferencerTensorMapping],
        outputs: &[InferencerTensorDeclaration],
        state: &InferencerStatePlan,
        udfs: Option<&UdfExecutor>,
        vm_settings: nervix_vm::VmSettings,
    ) -> error_stack::Result<Self, StatefulInferenceError> {
        let limit = executor.limits().snapshot_section_bytes.as_u64();
        let snapshot_bytes = state
            .snapshot_bytes()
            .change_context(StatefulInferenceError::Initial)?;
        if snapshot_bytes > limit {
            return Err(Report::new(StatefulInferenceError::StateSize {
                bytes: snapshot_bytes,
                limit,
            }));
        }
        // RuntimeValue is used only for configuration constants; message and carried tensors stay native/Arrow.
        let mut initial = Vec::with_capacity(state.bindings.len());
        for binding in &state.bindings {
            nervix_primitives::task::consume_budget().await;
            initial.push(match &binding.initial {
                InferencerStateInitial::Zeros => None,
                InferencerStateInitial::Constant(expression) => Some(
                    crate::runtime::evaluate_constant_expression_vm(
                        executor,
                        expression,
                        udfs,
                        Timestamp::from_unix_nanos(0),
                        vm_settings,
                    )
                    .await
                    .change_context(StatefulInferenceError::Initial)?,
                ),
            });
        }
        let reservation = executor
            .reserve(MemoryClass::Bulk, state.bytes_per_branch.max(1))
            .await
            .change_context(StatefulInferenceError::Execution)?;
        let state_for_job = state.clone();
        let generation = executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                let mut tensors = Vec::with_capacity(state_for_job.bindings.len());
                for (binding, initial) in state_for_job.bindings.iter().zip(initial) {
                    cancellation
                        .check()
                        .change_context(StatefulInferenceError::Cancelled)?;
                    let shape = binding
                        .schema
                        .dimensions
                        .iter()
                        .map(|axis| match axis {
                            InferencerTensorDimension::Fixed(size) => size.get().arch_into(),
                            _ => unreachable!("the typed state plan contains fixed axes only"),
                        })
                        .collect::<Vec<usize>>();
                    let mut tensor = Tensor::<f32>::new(&ort::memory::Allocator::default(), shape)
                        .change_context(StatefulInferenceError::Initial)?;
                    let (_, target) = tensor
                        .try_extract_tensor_mut::<f32>()
                        .change_context(StatefulInferenceError::Initial)?;
                    match initial {
                        None => target.fill(0.0),
                        Some(value) => {
                            let values = binding
                                .schema
                                .tensor_from_runtime_value(&value)
                                .change_context(StatefulInferenceError::Initial)?
                                .values;
                            target.copy_from_slice(&values);
                        }
                    }
                    tensors.push(tensor.into_dyn());
                }
                Ok::<_, Report<StatefulInferenceError>>(NativeGeneration { tensors })
            })
            .await
            .change_context(StatefulInferenceError::Execution)??;
        Ok(Self {
            inputs: inputs.to_vec(),
            outputs: outputs.to_vec(),
            state: state.clone(),
            initial: Arc::new(generation),
            output_arrow_schema: StdArc::new(arrow_schema::Schema::new(
                outputs
                    .iter()
                    .map(|port| {
                        arrow_schema::Field::new(
                            &port.tensor,
                            port.schema.message_type().arrow_data_type(),
                            false,
                        )
                    })
                    .collect::<Vec<_>>(),
            )),
        })
    }

    pub(in crate::runtime) fn same_binding(&self, other: &Self) -> bool {
        self.inputs == other.inputs && self.outputs == other.outputs && self.state == other.state
    }
    pub(in crate::runtime) fn instantiate(
        &self,
        state_bytes: crate::metrics::InferencerBranchBytes,
    ) -> StatefulInferenceSession {
        StatefulInferenceSession {
            session: None,
            generation: self.initial.clone(),
            _state_bytes: state_bytes,
        }
    }
}

impl StatefulInferenceSession {
    pub(in crate::runtime) fn state_metrics(&self) -> crate::metrics::InferencerStateMetrics {
        self._state_bytes.recorder()
    }

    pub(in crate::runtime) fn generation(&self) -> Arc<NativeGeneration> {
        self.generation.clone()
    }
    pub(in crate::runtime) fn restore_generation(&mut self, generation: Arc<NativeGeneration>) {
        self.generation = generation;
    }
}

#[derive(Debug)]
enum InputSlot {
    Message(usize),
    State(usize),
}
struct BoundSession {
    session: Session,
    inputs: Vec<InputSlot>,
    public_outputs: Vec<usize>,
    state_outputs: Vec<usize>,
}
pub(in crate::runtime) struct StatefulInferenceSession {
    session: Option<BoundSession>,
    generation: Arc<NativeGeneration>,
    _state_bytes: crate::metrics::InferencerBranchBytes,
}
impl std::fmt::Debug for StatefulInferenceSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatefulInferenceSession")
            .field("loaded", &self.session.is_some())
            .finish_non_exhaustive()
    }
}
impl BoundSession {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "populates one branch's session cache before admission; later rows reuse the \
                      retained native binding"
        )
    )]
    async fn load(
        executor: &Executor,
        path: &Path,
        definition: &Arc<PreparedStatefulInference>,
    ) -> error_stack::Result<Self, StatefulInferenceError> {
        let path = path.to_path_buf();
        let definition = definition.clone();
        let reservation = executor
            .reserve(MemoryClass::Bulk, super::SESSION_LOAD_RESERVATION_BYTES)
            .await
            .change_context(StatefulInferenceError::Execution)?;
        executor
            .accepted_work()
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(StatefulInferenceError::Cancelled)?;
                let environment =
                    onnx_environment().change_context(StatefulInferenceError::Load)?;
                let session = Session::builder(environment)
                    .change_context(StatefulInferenceError::Load)?
                    .commit_from_file(&path)
                    .change_context(StatefulInferenceError::Load)?;
                let mut inputs = Vec::with_capacity(session.inputs().len());
                for port in session.inputs() {
                    let name = port.name();
                    if let Some(index) = definition
                        .inputs
                        .iter()
                        .position(|input| input.tensor == name)
                    {
                        inputs.push(InputSlot::Message(index));
                    } else if let Some(index) = definition
                        .state
                        .bindings
                        .iter()
                        .position(|state| state.input == name)
                    {
                        inputs.push(InputSlot::State(index));
                    } else {
                        return Err(Report::new(StatefulInferenceError::Port {
                            port: name.to_owned(),
                        }));
                    }
                }
                let output_index = |name: &str| {
                    session
                        .outputs()
                        .iter()
                        .position(|output| output.name() == name)
                        .ok_or_else(|| {
                            Report::new(StatefulInferenceError::Port {
                                port: name.to_owned(),
                            })
                        })
                };
                let public_outputs = definition
                    .outputs
                    .iter()
                    .map(|port| output_index(&port.tensor))
                    .collect::<Result<Vec<_>, _>>()?;
                let state_outputs = definition
                    .state
                    .bindings
                    .iter()
                    .map(|state| output_index(&state.output))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self {
                    session,
                    inputs,
                    public_outputs,
                    state_outputs,
                })
            })
            .await
            .change_context(StatefulInferenceError::Execution)?
    }

    fn run(
        &mut self,
        definition: &PreparedStatefulInference,
        generation: &NativeGeneration,
        row: &RuntimeRecordBatch,
        limit: u64,
        cancellation: &Cancellation,
    ) -> error_stack::Result<(RuntimeRecordBatch, NativeGeneration), StatefulInferenceError> {
        cancellation
            .check()
            .change_context(StatefulInferenceError::Cancelled)?;
        if row.batch().num_rows() != 1 {
            return Err(Report::new(StatefulInferenceError::RowCount));
        }
        let message_inputs = definition
            .inputs
            .iter()
            .enumerate()
            .map(|(index, mapping)| {
                ArrowTensorInput::from_row(row, index, &mapping.tensor, &mapping.schema)?
                    .to_native()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let values = self
            .inputs
            .iter()
            .map(|slot| match *slot {
                InputSlot::Message(index) => SessionInputValue::from(&message_inputs[index]),
                InputSlot::State(index) => SessionInputValue::from(&generation.tensors[index]),
            })
            .collect::<Vec<_>>();
        let mut outputs = self
            .session
            .run(values.as_slice())
            .change_context(StatefulInferenceError::Invocation)?
            .into_iter()
            .map(|(_, value)| Some(value))
            .collect::<Vec<_>>();
        let mut columns = Vec::with_capacity(definition.outputs.len());
        let mut bytes = definition.state.bytes_per_branch;
        for (column_index, (port, index)) in definition
            .outputs
            .iter()
            .zip(&self.public_outputs)
            .enumerate()
        {
            cancellation
                .check()
                .change_context(StatefulInferenceError::Cancelled)?;
            let value = outputs[*index].as_ref().ok_or_else(|| {
                Report::new(StatefulInferenceError::Port {
                    port: port.tensor.clone(),
                })
            })?;
            let (shape, values) = value.try_extract_tensor::<f32>().change_context_lazy(|| {
                StatefulInferenceError::OutputTensor {
                    port: port.tensor.clone(),
                }
            })?;
            let shape = shape
                .iter()
                .map(|axis| usize::try_from(*axis))
                .collect::<Result<Vec<_>, _>>()
                .change_context_lazy(|| StatefulInferenceError::OutputTensor {
                    port: port.tensor.clone(),
                })?;
            port.schema
                .validate_concrete_shape(&shape, 1, false)
                .change_context_lazy(|| StatefulInferenceError::OutputTensor {
                    port: port.tensor.clone(),
                })?;
            let output_bytes = output_arrow_bytes(&port.schema, &shape, values.len())
                .ok_or_else(|| Report::new(StatefulInferenceError::OutputSize))?;
            bytes = bytes
                .checked_add(output_bytes)
                .ok_or_else(|| Report::new(StatefulInferenceError::WorkingSet))?;
            if bytes > limit {
                return Err(Report::new(StatefulInferenceError::WorkingSet));
            }
            columns.push(output_tensor_array(
                &port.schema,
                &shape,
                values,
                definition
                    .output_arrow_schema
                    .field(column_index)
                    .data_type(),
            )?);
        }
        let mut tensors = Vec::with_capacity(definition.state.bindings.len());
        for (port, index) in definition.state.bindings.iter().zip(&self.state_outputs) {
            cancellation
                .check()
                .change_context(StatefulInferenceError::Cancelled)?;
            let value = outputs[*index].take().ok_or_else(|| {
                Report::new(StatefulInferenceError::Port {
                    port: port.output.clone(),
                })
            })?;
            let (shape, values) = value.try_extract_tensor::<f32>().change_context_lazy(|| {
                StatefulInferenceError::OutputTensor {
                    port: port.output.clone(),
                }
            })?;
            let shape = shape
                .iter()
                .map(|axis| usize::try_from(*axis))
                .collect::<Result<Vec<_>, _>>()
                .change_context_lazy(|| StatefulInferenceError::OutputTensor {
                    port: port.output.clone(),
                })?;
            port.schema
                .validate_concrete_shape(&shape, 1, false)
                .change_context_lazy(|| StatefulInferenceError::OutputTensor {
                    port: port.output.clone(),
                })?;
            if values.len() != port.elements {
                return Err(Report::new(StatefulInferenceError::OutputTensor {
                    port: port.output.clone(),
                }));
            }
            tensors.push(value);
        }
        let batch = RecordBatch::try_new(definition.output_arrow_schema.clone(), columns)
            .change_context(StatefulInferenceError::OutputBatch)?;
        let batch =
            RuntimeRecordBatch::from_record_batch(definition.output_arrow_schema.clone(), batch)
                .change_context(StatefulInferenceError::OutputBatch)?;
        cancellation
            .check()
            .change_context(StatefulInferenceError::Cancelled)?;
        Ok((batch, NativeGeneration { tensors }))
    }
}
impl StatefulInferenceSession {
    /// A full worker class waits while this branch retains its session. A native row refusal is the inner result.
    /// The previous native generation stays installed until the whole row and every output validate.
    pub(in crate::runtime) async fn execute(
        &mut self,
        executor: &Executor,
        path: &Path,
        definition: &Arc<PreparedStatefulInference>,
        row: RuntimeRecordBatch,
    ) -> error_stack::Result<
        error_stack::Result<RuntimeRecordBatch, StatefulInferenceError>,
        StatefulInferenceError,
    > {
        if self.session.is_none() {
            self.session = Some(BoundSession::load(executor, path, definition).await?);
        }
        let input_bytes = row
            .estimated_bytes()
            .checked_mul(2)
            .ok_or_else(|| Report::new(StatefulInferenceError::WorkingSet))?;
        let bytes = input_bytes
            .checked_add(definition.state.bytes_per_branch)
            .ok_or_else(|| Report::new(StatefulInferenceError::WorkingSet))?;
        if bytes > executor.limits().relay_decoded_bytes.as_u64() {
            return Err(Report::new(StatefulInferenceError::WorkingSet));
        }
        let charge = executor
            .reserve(MemoryClass::Relay, bytes.max(1))
            .await
            .change_context(StatefulInferenceError::Execution)?;
        // Keep the installed native session while the full class has not granted a place.
        // Cancelling this wait drops only its charge and FIFO registration.
        let admitted = executor
            .accepted_work()
            .prepare_cpu(CpuClass::Extension, charge)
            .await
            .change_context(StatefulInferenceError::Execution)?;
        let mut session = self
            .session
            .take()
            .verified("the branch loaded its session before admission");
        let generation = self.generation.clone();
        let definition = definition.clone();
        let limit = executor.limits().relay_decoded_bytes.as_u64();
        let (session, result) = admitted
            .run(move |_charge, cancellation| {
                let result = session.run(&definition, &generation, &row, limit, cancellation);
                (session, result)
            })
            .await
            .change_context(StatefulInferenceError::Execution)?;
        self.session = Some(session);
        match result {
            Ok((batch, generation)) => {
                self.generation = Arc::new(generation);
                Ok(Ok(batch))
            }
            Err(error) => Ok(Err(error)),
        }
    }
}

struct ArrowTensorInput {
    shape: Vec<usize>,
    values: arrow_array::Float32Array,
}

impl ArrowTensorInput {
    fn from_row(
        batch: &RuntimeRecordBatch,
        column_index: usize,
        port: &str,
        schema: &InferencerTensorSchema,
    ) -> error_stack::Result<Self, StatefulInferenceError> {
        use arrow_array::{Array as _, FixedSizeListArray, Float32Array, ListArray};
        let column = batch.batch().columns().get(column_index).ok_or_else(|| {
            Report::new(StatefulInferenceError::InputPort {
                port: port.to_owned(),
            })
        })?;
        if batch.batch().num_rows() != 1 {
            return Err(Report::new(StatefulInferenceError::RowCount));
        }
        let mut current = column.slice(0, 1);
        let mut shape = Vec::with_capacity(schema.dimensions.len());
        for axis in &schema.dimensions {
            if current.null_count() != 0 {
                return Err(Report::new(StatefulInferenceError::NullInput {
                    port: port.to_owned(),
                }));
            }
            match axis {
                InferencerTensorDimension::Fixed(size) => {
                    let array = current
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| {
                            Report::new(StatefulInferenceError::InputType {
                                port: port.to_owned(),
                                actual: current.data_type().clone(),
                            })
                        })?;
                    let size =
                        usize::try_from(size.get()).assured("supported usize widths hold u32");
                    if usize::try_from(array.value_length()).ok() != Some(size) {
                        return Err(Report::new(StatefulInferenceError::InputShape {
                            port: port.to_owned(),
                        }));
                    }
                    let start = usize::try_from(array.value_offset(0)).map_err(|error| {
                        Report::new(error).change_context(StatefulInferenceError::InputShape {
                            port: port.to_owned(),
                        })
                    })?;
                    let count = current.len().checked_mul(size).ok_or_else(|| {
                        Report::new(StatefulInferenceError::InputShape {
                            port: port.to_owned(),
                        })
                    })?;
                    shape.push(size);
                    current = array.values().slice(start, count);
                }
                InferencerTensorDimension::Dynamic => {
                    let array = current
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| {
                            Report::new(StatefulInferenceError::InputType {
                                port: port.to_owned(),
                                actual: current.data_type().clone(),
                            })
                        })?;
                    let offsets = array.value_offsets();
                    let Some(first) = offsets.windows(2).next() else {
                        return Err(Report::new(InferencerError::InnerDynamicFromEmpty)
                            .change_context(StatefulInferenceError::InputShape {
                                port: port.to_owned(),
                            }));
                    };
                    let length = first[1] - first[0];
                    if offsets.windows(2).any(|pair| pair[1] - pair[0] != length) {
                        return Err(Report::new(StatefulInferenceError::InputShape {
                            port: port.to_owned(),
                        }));
                    }
                    let start =
                        usize::try_from(offsets[0]).assured("Arrow list offsets are nonnegative");
                    let length = usize::try_from(length).assured("Arrow list offsets are ordered");
                    let count = current.len().checked_mul(length).ok_or_else(|| {
                        Report::new(StatefulInferenceError::InputShape {
                            port: port.to_owned(),
                        })
                    })?;
                    shape.push(length);
                    current = array.values().slice(start, count);
                }
                InferencerTensorDimension::Batch => {
                    return Err(Report::new(StatefulInferenceError::InputShape {
                        port: port.to_owned(),
                    }));
                }
            }
        }
        if current.null_count() != 0 {
            return Err(Report::new(StatefulInferenceError::NullInput {
                port: port.to_owned(),
            }));
        }
        let values = current
            .as_any()
            .downcast_ref::<Float32Array>()
            .cloned()
            .ok_or_else(|| {
                Report::new(StatefulInferenceError::InputType {
                    port: port.to_owned(),
                    actual: current.data_type().clone(),
                })
            })?;
        schema
            .validate_concrete_shape(&shape, 1, false)
            .change_context_lazy(|| StatefulInferenceError::InputShape {
                port: port.to_owned(),
            })?;
        Ok(Self { shape, values })
    }

    fn to_native(&self) -> error_stack::Result<DynValue, StatefulInferenceError> {
        // Native outputs can alias an input. ORT owns this allocation, so retaining an output
        // never retains a pointer borrowed from the admitted Arrow row.
        let mut tensor =
            Tensor::<f32>::new(&ort::memory::Allocator::default(), self.shape.as_slice())
                .change_context(StatefulInferenceError::BuildInput)?;
        tensor
            .try_extract_tensor_mut::<f32>()
            .change_context(StatefulInferenceError::BuildInput)?
            .1
            .copy_from_slice(self.values.values().as_ref());
        Ok(tensor.into_dyn())
    }
}

/// Counts the complete one-row Arrow payload before allocating its values or list offsets.
fn output_arrow_bytes(
    schema: &InferencerTensorSchema,
    shape: &[usize],
    elements: usize,
) -> Option<u64> {
    let mut bytes = u64::try_from(elements).ok()?.checked_mul(4)?;
    let mut parents = 1u64;
    for (axis, size) in schema.dimensions.iter().zip(shape) {
        if matches!(axis, InferencerTensorDimension::Dynamic) {
            bytes = bytes.checked_add(parents.checked_add(1)?.checked_mul(4)?)?;
        }
        parents = parents.checked_mul(u64::try_from(*size).ok()?)?;
    }
    Some(bytes)
}

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "Arrow List offset buffers contain one terminal boundary after their parent rows"
    )
)]
const LIST_TERMINAL_OFFSET_COUNT: usize = 1;

/// Copies only public outputs into Arrow. Native state tensors never enter this conversion.
fn output_tensor_array(
    schema: &InferencerTensorSchema,
    shape: &[usize],
    values: &[f32],
    arrow_type: &arrow_schema::DataType,
) -> error_stack::Result<arrow_array::ArrayRef, StatefulInferenceError> {
    use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, ListArray};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::DataType;
    let mut levels = Vec::with_capacity(shape.len());
    let mut data_type = arrow_type.clone();
    let mut parents = 1usize;
    for size in shape {
        let field = match data_type {
            DataType::FixedSizeList(field, _) | DataType::List(field) => field,
            actual => return Err(Report::new(StatefulInferenceError::OutputType { actual })),
        };
        levels.push((field.clone(), parents, *size));
        data_type = field.data_type().clone();
        parents = parents
            .checked_mul(*size)
            .ok_or_else(|| Report::new(StatefulInferenceError::OutputSize))?;
    }
    let mut array: ArrayRef = StdArc::new(Float32Array::from(values.to_vec()));
    for ((field, parents, size), axis) in levels.into_iter().zip(&schema.dimensions).rev() {
        array = match axis {
            InferencerTensorDimension::Fixed(_) => {
                let size =
                    i32::try_from(size).change_context(StatefulInferenceError::OutputSize)?;
                StdArc::new(
                    FixedSizeListArray::try_new(field, size, array, None)
                        .change_context(StatefulInferenceError::OutputArray)?,
                )
            }
            InferencerTensorDimension::Dynamic => {
                let mut offsets = Vec::with_capacity(
                    parents
                        .checked_add(LIST_TERMINAL_OFFSET_COUNT)
                        .ok_or_else(|| Report::new(StatefulInferenceError::OutputSize))?,
                );
                for parent in 0..=parents {
                    let offset = parent
                        .checked_mul(size)
                        .ok_or_else(|| Report::new(StatefulInferenceError::OutputSize))?;
                    offsets.push(
                        i32::try_from(offset).change_context(StatefulInferenceError::OutputSize)?,
                    );
                }
                StdArc::new(
                    ListArray::try_new(
                        field,
                        OffsetBuffer::new(ScalarBuffer::from(offsets)),
                        array,
                        None,
                    )
                    .change_context(StatefulInferenceError::OutputArray)?,
                )
            }
            InferencerTensorDimension::Batch => {
                return Err(Report::new(StatefulInferenceError::OutputSize));
            }
        };
    }
    Ok(array)
}

#[cfg(test)]
mod tests;
