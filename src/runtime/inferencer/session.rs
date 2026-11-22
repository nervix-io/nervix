//! The one native ONNX session of a concrete branch's inferencer, and the model run every
//! inferencer makes for each admitted batch.
//!
//! Layer: data plane.
//! Owns: the prepared definition every branch of one inferencer shares; the branch's native
//! session, loaded once and resolved to its ports; and the model run over an admitted batch, one
//! message at a time in arrival order, or one call for each group of equally shaped messages when
//! the tensors declare `BATCH`. An inferencer that declares `STATE` runs one message at a time and
//! carries its tensors from message to message.
//! Depends on: typed tensor and state plans, Arrow, the bounded executor and ONNX Runtime.
//! Must not know: Models, routes, flush cadence, acknowledgements, snapshots or placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "runs a branch's model over its admitted batches")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(
        plans,
        reason = "runs only validated tensor plans and prepared native generations"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "runs the model once for each admitted batch of a branch task"
    )
)]

use std::path::Path;

use ahash::{HashMap, HashMapExt};
use arrow_array::RecordBatch;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Cancellation, CpuClass, Executor, MemoryClass};
use nervix_models::{
    InferencerExecutionMode, InferencerTensorDeclaration, InferencerTensorMapping,
};
use nervix_primitives::sync::{Arc, StdArc};
use nervix_roto::UdfExecutor;
use ort::{
    session::{Session, SessionInputValue},
    value::{DynValue, Tensor},
};

use super::{
    InferencerError, OnnxSessionOptions, RuntimeTensorSchema, SESSION_LOAD_RESERVATION_BYTES,
    columns::{MessageTensor, TensorColumn},
    onnx_environment,
    stateful::{CarriedState, NativeGeneration},
};
use crate::{registry::InferencerStatePlan, runtime_schema::RuntimeRecordBatch};

/// The definition every concrete branch of one inferencer shares: its tensor ports, its execution
/// mode, the Arrow schema of its public outputs, and the initial generation of its `STATE`.
#[derive(Clone)]
pub(in crate::runtime) struct PreparedInference {
    pub(in crate::runtime) inputs: Vec<InferencerTensorMapping>,
    pub(in crate::runtime) outputs: Vec<InferencerTensorDeclaration>,
    pub(in crate::runtime) mode: InferencerExecutionMode,
    pub(in crate::runtime) state: Option<PreparedState>,
    output_arrow_schema: StdArc<arrow_schema::Schema>,
}

/// The validated `STATE` of an inferencer and the initial generation a new branch starts from.
#[derive(Clone)]
pub(in crate::runtime) struct PreparedState {
    pub(in crate::runtime) plan: InferencerStatePlan,
    pub(in crate::runtime) initial: Arc<NativeGeneration>,
}

impl std::fmt::Debug for PreparedInference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedInference")
            .field("inputs", &self.inputs.len())
            .field("outputs", &self.outputs.len())
            .field("mode", &self.mode)
            .field(
                "state_bytes_per_branch",
                &self.state.as_ref().map(|state| state.plan.bytes_per_branch),
            )
            .finish_non_exhaustive()
    }
}

impl PreparedInference {
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
        mode: InferencerExecutionMode,
        state: Option<&InferencerStatePlan>,
        udfs: Option<&UdfExecutor>,
        vm_settings: nervix_vm::VmSettings,
    ) -> error_stack::Result<Self, InferencerError> {
        let state = match state {
            Some(plan) => Some(PreparedState {
                plan: plan.clone(),
                initial: Arc::new(
                    NativeGeneration::initial(executor, plan, udfs, vm_settings).await?,
                ),
            }),
            None => None,
        };
        let fields = outputs
            .iter()
            .map(|port| {
                arrow_schema::Field::new(
                    &port.tensor,
                    port.schema.message_type().arrow_data_type(),
                    false,
                )
            })
            .collect::<Vec<_>>();
        Ok(Self {
            inputs: inputs.to_vec(),
            outputs: outputs.to_vec(),
            mode,
            state,
            output_arrow_schema: StdArc::new(arrow_schema::Schema::new(fields)),
        })
    }

    /// Whether a branch built from `other` runs the same model binding and carries the same state.
    pub(in crate::runtime) fn same_binding(&self, other: &Self) -> bool {
        self.inputs == other.inputs
            && self.outputs == other.outputs
            && self.mode == other.mode
            && self.state.as_ref().map(|state| &state.plan)
                == other.state.as_ref().map(|state| &state.plan)
    }

    /// The bytes one message's mapped tensors hold in their columns' layout. An evaluated column
    /// can share its values with a larger batch, so a row slice of it can report that whole batch's
    /// bytes; the message's own tensors cannot. A tensor this cannot read adds nothing, because the
    /// run refuses that message anyway.
    fn message_bytes(&self, mapped: &RuntimeRecordBatch, row: usize) -> Option<u64> {
        let mut bytes = 0_u64;
        for (index, mapping) in self.inputs.iter().enumerate() {
            let column = mapped.batch().column(index);
            let Ok(tensor) = MessageTensor::read(column, row, &mapping.tensor, &mapping.schema)
            else {
                continue;
            };
            let tensor_bytes =
                TensorColumn::message_bytes(&mapping.schema, &tensor.shape, tensor.values.len())?;
            bytes = bytes.checked_add(tensor_bytes)?;
        }
        Some(bytes)
    }

    /// The bytes one message's run holds at once: its mapped input row, the native copy of it the
    /// model reads, and the state it carries.
    fn message_working_bytes(&self, row_bytes: u64) -> Option<u64> {
        let carried = match &self.state {
            Some(state) => state.plan.bytes_per_branch,
            None => 0,
        };
        row_bytes.checked_mul(2)?.checked_add(carried)
    }
}

#[derive(Debug, Clone, Copy)]
enum InputSlot {
    Message(usize),
    State(usize),
}

/// The branch's native session, with each model port resolved to the declaration it serves.
struct BoundSession {
    session: Session,
    inputs: Vec<InputSlot>,
    public_outputs: Vec<usize>,
    state_outputs: Vec<usize>,
}

/// One concrete branch's model: its native session, loaded on the first admitted batch, and the
/// tensors it carries when the inferencer declares `STATE`.
///
/// The branch task owns both. A run moves the session into its admitted job and the job hands it
/// back with the run's outcome, so the branch holds one session and no lock guards it.
pub(in crate::runtime) struct InferenceSession {
    bound: Option<BoundSession>,
    carried: Option<CarriedState>,
}

impl std::fmt::Debug for InferenceSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InferenceSession")
            .field("loaded", &self.bound.is_some())
            .field("carries_state", &self.carried.is_some())
            .finish_non_exhaustive()
    }
}

/// Why the model run did not produce a message's outputs. Every refusal is value-free: no native
/// diagnostic and no tensor value is retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "a message the model run refuses alone is a row outcome its routes' \
                  message-error policies receive; the refusal is a fixed value-free projection, \
                  because native diagnostics and the failures beneath them can carry tensor values"
    )
)]
pub(in crate::runtime) enum MessageRefusal {
    /// The message's mapped `INPUTS` do not form the declared input tensors.
    Input,
    /// The model refused the message, with ONNX Runtime's status.
    Model { status: ort::ErrorCode },
    /// The model answered with an output tensor its declaration does not describe.
    Output,
    /// The message's public outputs would exceed the configured relay operation limit.
    WorkingSet,
}

/// One message the model refused alone.
#[derive(Debug)]
pub(in crate::runtime) struct RefusedMessage {
    pub(in crate::runtime) row: usize,
    pub(in crate::runtime) refusal: MessageRefusal,
}

/// Messages that failed together: a batched call the model refused, messages the run never
/// reached, or a message too large to run.
#[derive(Debug)]
pub(in crate::runtime) struct FailedMessages {
    pub(in crate::runtime) rows: Vec<usize>,
    pub(in crate::runtime) error: Report<InferencerError>,
}

/// What one run over an admitted batch did with each of its messages. Rows are positions in the
/// mapped batch; every row appears exactly once among `accepted`, `refused` and `failed`.
#[derive(Debug, Default)]
pub(in crate::runtime) struct InferenceRun {
    /// The public outputs of the accepted messages: row `i` belongs to `accepted[i]`.
    pub(in crate::runtime) outputs: Option<RuntimeRecordBatch>,
    /// The messages the model accepted, in arrival order.
    pub(in crate::runtime) accepted: Vec<usize>,
    pub(in crate::runtime) refused: Vec<RefusedMessage>,
    pub(in crate::runtime) failed: Vec<FailedMessages>,
    /// Whether the run installed the state its accepted messages left, which it does only once
    /// their outputs are joined.
    pub(in crate::runtime) advanced: bool,
}

/// The messages one admitted job runs, and what it is charged.
struct RunChunk {
    rows: Vec<usize>,
    charge: u64,
}

/// What the admitted jobs of one run left before their public outputs are joined: the run's
/// outcome so far, each job's outputs in arrival order, and the carried state after the last
/// accepted message, which the join installs only once every job's outputs are joined.
struct RunJobs {
    run: InferenceRun,
    outputs: Vec<RecordBatch>,
    successor: Option<Arc<NativeGeneration>>,
}

/// What one admitted job returns: the outcome of its rows and the successor of the carried state.
#[derive(Default)]
struct ChunkOutcome {
    outputs: Option<RecordBatch>,
    accepted: Vec<usize>,
    refused: Vec<RefusedMessage>,
    failed: Vec<FailedMessages>,
    successor: Option<NativeGeneration>,
}

impl InferenceSession {
    pub(in crate::runtime) fn new(carried: Option<CarriedState>) -> Self {
        Self {
            bound: None,
            carried,
        }
    }

    /// Whether the branch has not loaded its native session yet.
    pub(in crate::runtime) fn needs_load(&self) -> bool {
        self.bound.is_none()
    }

    pub(in crate::runtime) fn carried(&self) -> Option<&CarriedState> {
        self.carried.as_ref()
    }

    /// Load the model at `path` on the node's bulk workers and resolve each of its ports to the
    /// declaration it serves. A full bulk class waits, because the branch already holds the batch
    /// that needs the session.
    pub(in crate::runtime) async fn load(
        &mut self,
        executor: &Executor,
        path: &Path,
        prepared: &Arc<PreparedInference>,
    ) -> error_stack::Result<(), InferencerError> {
        let path = path.to_path_buf();
        let definition = prepared.clone();
        let reservation = executor
            .reserve(MemoryClass::Bulk, SESSION_LOAD_RESERVATION_BYTES)
            .await
            .change_context(InferencerError::LoadExecution)?;
        let bound = executor
            .accepted_work()
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                BoundSession::load(&path, &definition, cancellation)
            })
            .await
            .change_context(InferencerError::LoadExecution)??;
        self.bound = Some(bound);
        Ok(())
    }

    /// Run the model over every row of `mapped`, the admitted batch's `INPUTS` in arrival order,
    /// on the node's extension workers. Rows run one at a time unless the tensors declare `BATCH`,
    /// and a batch too large for one charge runs as several admitted jobs, each starting from the
    /// state the previous jobs' accepted messages left. The carried state takes that state only
    /// once every job's outputs are joined, so a message that fails, alone or because the outputs
    /// cannot be joined, leaves it where the preceding accepted message left it.
    pub(in crate::runtime) async fn run(
        &mut self,
        executor: &Executor,
        prepared: &Arc<PreparedInference>,
        mapped: &RuntimeRecordBatch,
    ) -> InferenceRun {
        let jobs = self.run_jobs(executor, prepared, mapped).await;
        self.join_outputs(prepared, jobs)
    }

    /// Runs every admitted job of `mapped`, in arrival order, and keeps each job's public outputs
    /// for the join.
    async fn run_jobs(
        &mut self,
        executor: &Executor,
        prepared: &Arc<PreparedInference>,
        mapped: &RuntimeRecordBatch,
    ) -> RunJobs {
        let limit = executor.limits().relay_decoded_bytes.as_u64();
        let mut run = InferenceRun::default();
        let chunks = Self::plan_chunks(prepared, mapped, limit, &mut run);
        let mut outputs = Vec::new();
        let mut successor = None::<Arc<NativeGeneration>>;
        let mut chunks = chunks.into_iter();
        while let Some(chunk) = chunks.next() {
            nervix_primitives::task::consume_budget().await;
            let start = match &successor {
                Some(successor) => Some(successor.clone()),
                None => self.carried.as_ref().map(CarriedState::generation),
            };
            match self
                .run_chunk(executor, prepared, mapped, &chunk, start, limit)
                .await
            {
                Ok(outcome) => {
                    if let Some(next) = outcome.successor {
                        successor = Some(Arc::new(next));
                    }
                    if let Some(batch) = outcome.outputs {
                        outputs.push(batch);
                    }
                    run.accepted.extend(outcome.accepted);
                    run.refused.extend(outcome.refused);
                    run.failed.extend(outcome.failed);
                }
                Err(error) => {
                    let mut rows = chunk.rows;
                    for later in chunks.by_ref() {
                        rows.extend(later.rows);
                    }
                    run.failed.push(FailedMessages { rows, error });
                    break;
                }
            }
        }
        RunJobs {
            run,
            outputs,
            successor,
        }
    }

    /// Joins the jobs' public outputs into the run's one output batch and only then installs the
    /// state the accepted messages left. A join that fails fails every accepted message, and the
    /// state they left is dropped with them.
    fn join_outputs(&mut self, prepared: &PreparedInference, jobs: RunJobs) -> InferenceRun {
        let RunJobs {
            mut run,
            outputs,
            successor,
        } = jobs;
        match self.output_batch(prepared, outputs) {
            Ok(batch) => {
                run.outputs = batch;
                if let Some(successor) = successor {
                    let carried = self
                        .carried
                        .as_mut()
                        .verified("only a run that carries state returns a successor");
                    carried.install(successor);
                    run.advanced = true;
                }
            }
            Err(error) => {
                run.failed.push(FailedMessages {
                    rows: std::mem::take(&mut run.accepted),
                    error,
                });
            }
        }
        run
    }

    /// Divides `mapped` into the admitted jobs that run it. A batched model's call covers a whole
    /// group of rows, so a batched batch is one job charged at most the relay operation limit. One
    /// message at a time, a batch whose working set fits that limit is one job; otherwise a message
    /// whose own working set exceeds it fails alone, and the rest are packed into jobs whose working
    /// sets fit it.
    fn plan_chunks(
        prepared: &PreparedInference,
        mapped: &RuntimeRecordBatch,
        limit: u64,
        run: &mut InferenceRun,
    ) -> Vec<RunChunk> {
        let rows = mapped.batch().num_rows();
        if rows == 0 {
            return Vec::new();
        }
        if let InferencerExecutionMode::Batched = prepared.mode {
            let charge = prepared
                .message_working_bytes(mapped.estimated_bytes())
                .unwrap_or(limit)
                .min(limit);
            return vec![RunChunk {
                rows: (0..rows).collect(),
                charge,
            }];
        }
        let whole = prepared.message_working_bytes(mapped.estimated_bytes());
        if let Some(whole) = whole
            && whole <= limit
        {
            // No message of a batch that fits one job can exceed the limit on its own.
            return vec![RunChunk {
                rows: (0..rows).collect(),
                charge: whole,
            }];
        }
        let mut chunks = Vec::new();
        let mut current = RunChunk {
            rows: Vec::new(),
            charge: 0,
        };
        for row in 0..rows {
            let working = match prepared.message_bytes(mapped, row) {
                Some(row_bytes) => prepared.message_working_bytes(row_bytes),
                None => None,
            };
            let working = match working {
                Some(working) if working <= limit => working,
                Some(_) | None => {
                    run.failed.push(FailedMessages {
                        rows: vec![row],
                        error: Report::new(InferencerError::WorkingSet),
                    });
                    continue;
                }
            };
            let combined = current.charge.checked_add(working);
            match combined {
                Some(combined) if combined <= limit => {
                    current.charge = combined;
                    current.rows.push(row);
                }
                Some(_) | None => {
                    let full = std::mem::replace(
                        &mut current,
                        RunChunk {
                            rows: vec![row],
                            charge: working,
                        },
                    );
                    if !full.rows.is_empty() {
                        chunks.push(full);
                    }
                }
            }
        }
        if !current.rows.is_empty() {
            chunks.push(current);
        }
        chunks
    }

    /// One admitted job over `chunk`, starting from the carried state `carried`. The branch keeps
    /// its session while the job waits for a place; once admitted, the session moves into the job
    /// and returns with its outcome. A job that panicked loses the session, and the next batch
    /// loads it again.
    async fn run_chunk(
        &mut self,
        executor: &Executor,
        prepared: &Arc<PreparedInference>,
        mapped: &RuntimeRecordBatch,
        chunk: &RunChunk,
        carried: Option<Arc<NativeGeneration>>,
        limit: u64,
    ) -> error_stack::Result<ChunkOutcome, InferencerError> {
        let charge = executor
            .reserve(MemoryClass::Relay, chunk.charge.max(1))
            .await
            .change_context(InferencerError::RunExecution)?;
        let admitted = executor
            .accepted_work()
            .prepare_cpu(CpuClass::Extension, charge)
            .await
            .change_context(InferencerError::RunExecution)?;
        let mut bound = self
            .bound
            .take()
            .ok_or_else(|| Report::new(InferencerError::SessionUnavailable))?;
        let definition = prepared.clone();
        let mapped = mapped.clone();
        let rows = chunk.rows.clone();
        let (bound, outcome) = admitted
            .run(move |_charge, cancellation| {
                let outcome = match definition.mode {
                    InferencerExecutionMode::PerMessage => bound.run_messages(
                        &definition,
                        carried.as_deref(),
                        &mapped,
                        &rows,
                        limit,
                        cancellation,
                    ),
                    InferencerExecutionMode::Batched => {
                        bound.run_groups(&definition, &mapped, &rows, cancellation)
                    }
                };
                (bound, outcome)
            })
            .await
            .change_context(InferencerError::RunExecution)?;
        self.bound = Some(bound);
        Ok(outcome)
    }

    /// The public outputs of every accepted message, in arrival order, as one batch.
    fn output_batch(
        &self,
        prepared: &PreparedInference,
        mut outputs: Vec<RecordBatch>,
    ) -> error_stack::Result<Option<RuntimeRecordBatch>, InferencerError> {
        let batch = match outputs.len() {
            0 => return Ok(None),
            1 => outputs
                .pop()
                .verified("the match arm holds exactly one batch"),
            _ => arrow_select::concat::concat_batches(&prepared.output_arrow_schema, &outputs)
                .change_context(InferencerError::OutputBatch)?,
        };
        RuntimeRecordBatch::from_record_batch(prepared.output_arrow_schema.clone(), batch)
            .change_context(InferencerError::OutputBatch)
            .map(Some)
    }
}

impl BoundSession {
    /// Load the model and resolve every one of its ports, which validation has already matched to
    /// the declaration. The session carries the options [`OnnxSessionOptions`] sets on every
    /// session.
    fn load(
        path: &Path,
        definition: &PreparedInference,
        cancellation: &Cancellation,
    ) -> error_stack::Result<Self, InferencerError> {
        cancellation
            .check()
            .change_context(InferencerError::Cancelled)?;
        let environment = onnx_environment().change_context(InferencerError::InitializeSession)?;
        let session = OnnxSessionOptions::builder(environment)
            .change_context(InferencerError::InitializeSession)?
            .commit_from_file(path)
            .change_context_lazy(|| InferencerError::LoadModel {
                path: path.to_path_buf(),
            })?;
        let mut message_ports = HashMap::with_capacity(definition.inputs.len());
        for (index, mapping) in definition.inputs.iter().enumerate() {
            message_ports.insert(mapping.tensor.as_str(), index);
        }
        let mut state_ports = HashMap::new();
        if let Some(state) = &definition.state {
            for (index, binding) in state.plan.bindings.iter().enumerate() {
                state_ports.insert(binding.input.as_str(), index);
            }
        }
        let mut inputs = Vec::with_capacity(session.inputs().len());
        for port in session.inputs() {
            let name = port.name();
            let slot = match (message_ports.get(name), state_ports.get(name)) {
                (Some(index), _) => InputSlot::Message(*index),
                (None, Some(index)) => InputSlot::State(*index),
                (None, None) => {
                    return Err(Report::new(InferencerError::Port {
                        port: name.to_owned(),
                    }));
                }
            };
            inputs.push(slot);
        }
        let mut output_ports = HashMap::with_capacity(session.outputs().len());
        for (index, port) in session.outputs().iter().enumerate() {
            output_ports.insert(port.name(), index);
        }
        let output_index = |name: &str| {
            output_ports.get(name).copied().ok_or_else(|| {
                Report::new(InferencerError::Port {
                    port: name.to_owned(),
                })
            })
        };
        let mut public_outputs = Vec::with_capacity(definition.outputs.len());
        for port in &definition.outputs {
            public_outputs.push(output_index(&port.tensor)?);
        }
        let mut state_outputs = Vec::new();
        if let Some(state) = &definition.state {
            for binding in &state.plan.bindings {
                state_outputs.push(output_index(&binding.output)?);
            }
        }
        Ok(Self {
            session,
            inputs,
            public_outputs,
            state_outputs,
        })
    }
}

/// The public output port `index` of one model answer, validated against its declaration.
struct CheckedOutput {
    index: usize,
    shape: Vec<usize>,
}

impl BoundSession {
    /// Run `rows` of `mapped` one message at a time, in arrival order. A message the model refuses,
    /// or whose outputs its declaration does not describe, fails alone and leaves the carried state
    /// as it was; all state successors advance together once a message is accepted.
    fn run_messages(
        &mut self,
        definition: &PreparedInference,
        carried: Option<&NativeGeneration>,
        mapped: &RuntimeRecordBatch,
        rows: &[usize],
        limit: u64,
        cancellation: &Cancellation,
    ) -> ChunkOutcome {
        let mut columns = definition.output_columns();
        let mut outcome = ChunkOutcome::default();
        let mut successor = None::<NativeGeneration>;
        for (position, row) in rows.iter().copied().enumerate() {
            if cancellation.check().is_err() {
                outcome.failed.push(FailedMessages {
                    rows: rows[position..].to_vec(),
                    error: Report::new(InferencerError::Cancelled),
                });
                break;
            }
            let state = match &successor {
                Some(successor) => Some(successor),
                None => carried,
            };
            match self.run_message(definition, state, mapped, row, limit, &mut columns) {
                Ok(next) => {
                    outcome.accepted.push(row);
                    if let Some(next) = next {
                        successor = Some(next);
                    }
                }
                Err(refusal) => outcome.refused.push(RefusedMessage { row, refusal }),
            }
        }
        outcome.successor = successor;
        outcome.finish(definition, columns);
        outcome
    }

    /// One message's model call. Nothing is added to `columns` and no successor is returned unless
    /// every public output and every state successor is valid.
    fn run_message(
        &mut self,
        definition: &PreparedInference,
        state: Option<&NativeGeneration>,
        mapped: &RuntimeRecordBatch,
        row: usize,
        limit: u64,
        columns: &mut [TensorColumn],
    ) -> Result<Option<NativeGeneration>, MessageRefusal> {
        let mut message_inputs = Vec::with_capacity(definition.inputs.len());
        for (index, mapping) in definition.inputs.iter().enumerate() {
            let column = mapped.batch().column(index);
            let Ok(tensor) = MessageTensor::read(column, row, &mapping.tensor, &mapping.schema)
            else {
                return Err(MessageRefusal::Input);
            };
            if mapping
                .schema
                .validate_concrete_shape(&tensor.shape, 1, false)
                .is_err()
            {
                return Err(MessageRefusal::Input);
            }
            let Ok(native) = tensor.to_native() else {
                return Err(MessageRefusal::Input);
            };
            message_inputs.push(native);
        }
        let mut inputs = Vec::with_capacity(self.inputs.len());
        for slot in &self.inputs {
            let value = match *slot {
                InputSlot::Message(index) => SessionInputValue::from(&message_inputs[index]),
                InputSlot::State(index) => SessionInputValue::from(
                    state
                        .verified(
                            "a session resolves state ports only for a branch that carries state",
                        )
                        .input(index),
                ),
            };
            inputs.push(value);
        }
        let answer = match self.session.run(inputs.as_slice()) {
            Ok(answer) => answer,
            // The native refusal text can quote tensor values, so only its status is kept.
            Err(refusal) => {
                return Err(MessageRefusal::Model {
                    status: refusal.code(),
                });
            }
        };
        let mut outputs = Vec::with_capacity(answer.len());
        for (_, value) in answer {
            outputs.push(Some(value));
        }
        let successor = match &definition.state {
            Some(state) => {
                let mut tensors = Vec::with_capacity(state.plan.bindings.len());
                for (binding, index) in state.plan.bindings.iter().zip(&self.state_outputs) {
                    let Some(slot) = outputs.get_mut(*index) else {
                        return Err(MessageRefusal::Output);
                    };
                    let Some(value) = slot.take() else {
                        return Err(MessageRefusal::Output);
                    };
                    let Ok((shape, values)) = value.try_extract_tensor::<f32>() else {
                        return Err(MessageRefusal::Output);
                    };
                    let Ok(shape) = native_shape(shape) else {
                        return Err(MessageRefusal::Output);
                    };
                    if binding
                        .schema
                        .validate_concrete_shape(&shape, 1, false)
                        .is_err()
                        || values.len() != binding.elements
                    {
                        return Err(MessageRefusal::Output);
                    }
                    tensors.push(value);
                }
                Some(NativeGeneration::from_successors(tensors))
            }
            None => None,
        };
        let mut bytes = match &definition.state {
            Some(state) => state.plan.bytes_per_branch,
            None => 0,
        };
        let mut checked = Vec::with_capacity(definition.outputs.len());
        for (port, index) in definition.outputs.iter().zip(&self.public_outputs) {
            let Some(Some(value)) = outputs.get(*index) else {
                return Err(MessageRefusal::Output);
            };
            let Ok((shape, values)) = value.try_extract_tensor::<f32>() else {
                return Err(MessageRefusal::Output);
            };
            let Ok(shape) = native_shape(shape) else {
                return Err(MessageRefusal::Output);
            };
            if port
                .schema
                .validate_concrete_shape(&shape, 1, false)
                .is_err()
                || element_count(&shape) != Some(values.len())
            {
                return Err(MessageRefusal::Output);
            }
            let Some(output_bytes) =
                TensorColumn::message_bytes(&port.schema, &shape, values.len())
            else {
                return Err(MessageRefusal::WorkingSet);
            };
            bytes = bytes
                .checked_add(output_bytes)
                .ok_or(MessageRefusal::WorkingSet)?;
            if bytes > limit {
                return Err(MessageRefusal::WorkingSet);
            }
            checked.push(CheckedOutput {
                index: *index,
                shape,
            });
        }
        for (column, output) in columns.iter_mut().zip(checked) {
            let value = outputs[output.index]
                .as_ref()
                .verified("the loop above read this output");
            let (_, values) = value
                .try_extract_tensor::<f32>()
                .verified("the loop above extracted this output as F32");
            column
                .push(&output.shape, values)
                .verified("the loop above checked this output's shape and value count");
        }
        Ok(successor)
    }
}

/// Messages of one batched run whose mapped tensors have one concrete shape for every port, and so
/// share one model call.
struct ShapeGroup {
    rows: Vec<usize>,
    tensors: Vec<Vec<MessageTensor>>,
}

/// The validated public outputs of one batched model call, with the call's batch size.
struct GroupAnswer {
    outputs: Vec<DynValue>,
    shapes: Vec<Vec<usize>>,
}

impl BoundSession {
    /// Run `rows` of `mapped` as one model call for each group of messages whose mapped tensors
    /// share one concrete shape, in the order the groups first appear. Each message keeps its
    /// arrival position in the outputs. A call the model refuses fails its messages together.
    fn run_groups(
        &mut self,
        definition: &PreparedInference,
        mapped: &RuntimeRecordBatch,
        rows: &[usize],
        cancellation: &Cancellation,
    ) -> ChunkOutcome {
        let mut outcome = ChunkOutcome::default();
        let mut groups = Vec::<ShapeGroup>::new();
        let mut group_of_shapes = HashMap::<Vec<Vec<usize>>, usize>::new();
        let mut placements = Vec::with_capacity(rows.len());
        for row in rows.iter().copied() {
            let mut tensors = Vec::with_capacity(definition.inputs.len());
            let mut shapes = Vec::with_capacity(definition.inputs.len());
            let mut readable = true;
            for (index, mapping) in definition.inputs.iter().enumerate() {
                let column = mapped.batch().column(index);
                let Ok(tensor) = MessageTensor::read(column, row, &mapping.tensor, &mapping.schema)
                else {
                    readable = false;
                    break;
                };
                shapes.push(tensor.shape.clone());
                tensors.push(tensor);
            }
            if !readable {
                outcome.refused.push(RefusedMessage {
                    row,
                    refusal: MessageRefusal::Input,
                });
                placements.push(None);
                continue;
            }
            let group = match group_of_shapes.get(&shapes) {
                Some(group) => *group,
                None => {
                    let group = groups.len();
                    groups.push(ShapeGroup {
                        rows: Vec::new(),
                        tensors: Vec::new(),
                    });
                    group_of_shapes.insert(shapes, group);
                    group
                }
            };
            let position = groups[group].rows.len();
            groups[group].rows.push(row);
            groups[group].tensors.push(tensors);
            placements.push(Some((group, position)));
        }
        let mut answers = Vec::with_capacity(groups.len());
        let mut groups = groups.into_iter();
        while let Some(group) = groups.next() {
            if cancellation.check().is_err() {
                let mut remaining = group.rows;
                for later in groups.by_ref() {
                    remaining.extend(later.rows);
                }
                outcome.failed.push(FailedMessages {
                    rows: remaining,
                    error: Report::new(InferencerError::Cancelled),
                });
                break;
            }
            match self.run_group(definition, &group) {
                Ok(answer) => answers.push(Some(answer)),
                Err(error) => {
                    outcome.failed.push(FailedMessages {
                        rows: group.rows,
                        error,
                    });
                    answers.push(None);
                }
            }
        }
        let mut columns = definition.output_columns();
        let mut gathered = Vec::new();
        for (row, placement) in rows.iter().copied().zip(placements) {
            let Some((group, position)) = placement else {
                continue;
            };
            let Some(Some(answer)) = answers.get(group) else {
                continue;
            };
            for ((column, port), (output, shape)) in columns
                .iter_mut()
                .zip(&definition.outputs)
                .zip(answer.outputs.iter().zip(&answer.shapes))
            {
                let (_, values) = output
                    .try_extract_tensor::<f32>()
                    .verified("run_group extracted every output as F32");
                let batch_axis = port
                    .schema
                    .batch_axis()
                    .verified("every tensor of a batched inferencer declares one BATCH axis");
                let message_shape = batch_slice(values, shape, batch_axis, position, &mut gathered);
                column
                    .push(&message_shape, &gathered)
                    .verified("run_group checked the shape of every output");
            }
            outcome.accepted.push(row);
        }
        outcome.finish(definition, columns);
        outcome
    }

    /// One batched model call over `group`, its message tensors joined along each port's `BATCH`
    /// axis. Only the native status of a refusal is kept.
    fn run_group(
        &mut self,
        definition: &PreparedInference,
        group: &ShapeGroup,
    ) -> error_stack::Result<GroupAnswer, InferencerError> {
        let batch_size = group.rows.len();
        let mut joined = Vec::with_capacity(definition.inputs.len());
        for (port, mapping) in definition.inputs.iter().enumerate() {
            let slice_shape = &group.tensors[0][port].shape;
            let shape = mapping.schema.batch_shape(slice_shape, batch_size)?;
            let batch_axis = mapping
                .schema
                .batch_axis()
                .ok_or_else(|| Report::new(InferencerError::MissingBatchAxis))?;
            let mut tensor = Tensor::<f32>::new(&ort::memory::Allocator::default(), shape)
                .change_context(InferencerError::BuildInput)?;
            let (_, target) = tensor
                .try_extract_tensor_mut::<f32>()
                .change_context(InferencerError::BuildInput)?;
            let mut slices = Vec::with_capacity(batch_size);
            for tensors in &group.tensors {
                slices.push(tensors[port].values.values().as_ref());
            }
            join_batch(target, &slices, slice_shape, batch_axis)?;
            joined.push(tensor.into_dyn());
        }
        let mut inputs = Vec::with_capacity(self.inputs.len());
        for slot in &self.inputs {
            let InputSlot::Message(index) = *slot else {
                return Err(Report::new(InferencerError::BatchWithState));
            };
            inputs.push(SessionInputValue::from(&joined[index]));
        }
        let answer = match self.session.run(inputs.as_slice()) {
            Ok(answer) => answer,
            // The native refusal text can quote tensor values, so only its status is kept.
            Err(refusal) => {
                return Err(Report::new(InferencerError::Refused {
                    status: refusal.code(),
                }));
            }
        };
        let mut values_by_index = Vec::with_capacity(answer.len());
        for (_, value) in answer {
            values_by_index.push(Some(value));
        }
        let mut outputs = Vec::with_capacity(definition.outputs.len());
        let mut shapes = Vec::with_capacity(definition.outputs.len());
        for (port, index) in definition.outputs.iter().zip(&self.public_outputs) {
            let invalid = || InferencerError::OutputTensor {
                port: port.tensor.clone(),
            };
            let Some(slot) = values_by_index.get_mut(*index) else {
                return Err(Report::new(invalid()));
            };
            let Some(value) = slot.take() else {
                return Err(Report::new(invalid()));
            };
            let (shape, values) = value
                .try_extract_tensor::<f32>()
                .change_context_lazy(invalid)?;
            let shape = native_shape(shape).change_context_lazy(invalid)?;
            port.schema
                .validate_concrete_shape(&shape, batch_size, true)
                .change_context_lazy(invalid)?;
            if element_count(&shape) != Some(values.len()) {
                return Err(Report::new(invalid()));
            }
            outputs.push(value);
            shapes.push(shape);
        }
        Ok(GroupAnswer { outputs, shapes })
    }
}

impl ChunkOutcome {
    /// Turns the gathered public outputs into the batch of the accepted messages. Should Arrow
    /// refuse the columns, every accepted message fails with the cause, and the carried state
    /// keeps none of their successors.
    fn finish(&mut self, definition: &PreparedInference, columns: Vec<TensorColumn>) {
        if self.accepted.is_empty() {
            return;
        }
        match definition.output_record_batch(columns) {
            Ok(batch) => self.outputs = Some(batch),
            Err(error) => {
                self.successor = None;
                self.failed.push(FailedMessages {
                    rows: std::mem::take(&mut self.accepted),
                    error,
                });
            }
        }
    }
}

impl PreparedInference {
    /// One empty column for each public output, in declaration order.
    fn output_columns(&self) -> Vec<TensorColumn> {
        let mut columns = Vec::with_capacity(self.outputs.len());
        for (port, field) in self.outputs.iter().zip(self.output_arrow_schema.fields()) {
            columns.push(TensorColumn::new(&port.schema, field.data_type().clone()));
        }
        columns
    }

    fn output_record_batch(
        &self,
        columns: Vec<TensorColumn>,
    ) -> error_stack::Result<RecordBatch, InferencerError> {
        let mut arrays = Vec::with_capacity(columns.len());
        for column in columns {
            arrays.push(column.finish()?);
        }
        RecordBatch::try_new(self.output_arrow_schema.clone(), arrays)
            .change_context(InferencerError::OutputBatch)
    }
}

/// A native tensor shape as axis lengths. ONNX Runtime reports a dimension as a signed integer.
fn native_shape(shape: &[i64]) -> error_stack::Result<Vec<usize>, InferencerError> {
    let mut axes = Vec::with_capacity(shape.len());
    for axis in shape {
        let axis = usize::try_from(*axis).map_err(|_| {
            Report::new(InferencerError::NegativeOutputShape {
                shape: shape.to_vec(),
            })
        })?;
        axes.push(axis);
    }
    Ok(axes)
}

/// How many values a tensor of `shape` holds, or nothing when the count overflows.
fn element_count(shape: &[usize]) -> Option<usize> {
    let mut count = 1usize;
    for axis in shape {
        count = count.checked_mul(*axis)?;
    }
    Some(count)
}

/// Writes the message tensors `slices`, each of `slice_shape`, into `target`, the batched tensor
/// whose `BATCH` axis sits at `batch_axis`: for every position of the axes before it, the messages
/// follow one another in batch order, each contributing its block of the axes after it.
fn join_batch(
    target: &mut [f32],
    slices: &[&[f32]],
    slice_shape: &[usize],
    batch_axis: usize,
) -> error_stack::Result<(), InferencerError> {
    let outer = element_count(&slice_shape[..batch_axis])
        .ok_or_else(|| Report::new(InferencerError::OuterCountOverflow))?;
    let inner = element_count(&slice_shape[batch_axis..])
        .ok_or_else(|| Report::new(InferencerError::InnerCountOverflow))?;
    let expected = outer
        .checked_mul(inner)
        .ok_or_else(|| Report::new(InferencerError::SliceCountOverflow))?;
    for slice in slices {
        if slice.len() != expected {
            return Err(Report::new(InferencerError::SliceValueCount {
                actual: slice.len(),
                expected,
            }));
        }
    }
    let joined = expected
        .checked_mul(slices.len())
        .ok_or_else(|| Report::new(InferencerError::JoinedCountOverflow))?;
    if target.len() != joined {
        return Err(Report::new(InferencerError::BatchedValueCount {
            actual: target.len(),
            expected: joined,
        }));
    }
    let mut destination = target.chunks_exact_mut(inner.max(1));
    for outer_index in 0..outer {
        let start = outer_index * inner;
        for slice in slices {
            if inner == 0 {
                continue;
            }
            let block = destination
                .next()
                .verified("the target holds one block per message for every outer position");
            block.copy_from_slice(&slice[start..start + inner]);
        }
    }
    Ok(())
}

/// The message at `position` of a batched output of `shape` whose `BATCH` axis sits at
/// `batch_axis`: its values are gathered into `gathered`, and its shape, without the `BATCH`
/// axis, is returned.
fn batch_slice(
    values: &[f32],
    shape: &[usize],
    batch_axis: usize,
    position: usize,
    gathered: &mut Vec<f32>,
) -> Vec<usize> {
    let batch_size = shape[batch_axis];
    let outer = element_count(&shape[..batch_axis])
        .verified("run_group checked that the output's element count fits usize");
    let inner = element_count(&shape[batch_axis + 1..])
        .verified("run_group checked that the output's element count fits usize");
    gathered.clear();
    for outer_index in 0..outer {
        let block = outer_index
            .checked_mul(batch_size)
            .verified("every position of a checked output lies within its element count");
        let block = block
            .checked_add(position)
            .verified("every position of a checked output lies within its element count");
        let start = block
            .checked_mul(inner)
            .verified("every position of a checked output lies within its element count");
        gathered.extend_from_slice(&values[start..start + inner]);
    }
    let mut message_shape = shape.to_vec();
    message_shape.remove(batch_axis);
    message_shape
}

#[cfg(test)]
mod tests;
