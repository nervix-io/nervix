//! The native tensors an inferencer that declares `STATE` carries for one concrete branch.
//!
//! Layer: data plane.
//! Owns: prepared INITIAL generations, the branch's carried generation and its publication,
//! successor installation, and the native snapshot and restore projections of a generation.
//! Depends on: typed state plans, configuration constant evaluation and bounded execution.
//! Must not know: Models, model runs, route flush cadence, snapshot encodings or placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "carries a branch's explicit ONNX state")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(
        plans,
        reason = "carries only validated state plans and native generations"
    )
)]

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use nervix_execution::{Cancellation, CpuClass, Executor, MemoryClass};
use nervix_models::{InferencerStateInitial, InferencerTensorDimension, Timestamp};
use nervix_primitives::sync::Arc;
use nervix_roto::UdfExecutor;
use ort::value::{DynValue, Tensor};

use super::{InferencerError, RuntimeTensorSchema, session::PreparedState};
use crate::{
    registry::InferencerStatePlan,
    runtime::{ReplicatedInferencerState, inferencer_state::InferencerGeneration},
};

/// One immutable generation of a branch's carried tensors, one per `STATE` entry in declaration
/// order. The initial generation is shared by every new branch; a successor belongs to one branch.
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
    /// The generation a branch starts from: zeros, or each entry's constant `INITIAL` value,
    /// checked to fit one snapshot section before any tensor is allocated.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "materializes initial tensors before publishing a processor revision"
        )
    )]
    pub(in crate::runtime) async fn initial(
        executor: &Executor,
        state: &InferencerStatePlan,
        udfs: Option<&UdfExecutor>,
        vm_settings: nervix_vm::VmSettings,
    ) -> error_stack::Result<Self, InferencerError> {
        let limit = executor.limits().snapshot_section_bytes.as_u64();
        let snapshot_bytes = state
            .snapshot_bytes()
            .change_context(InferencerError::Initial)?;
        if snapshot_bytes > limit {
            return Err(Report::new(InferencerError::StateSize {
                bytes: snapshot_bytes,
                limit,
            }));
        }
        // RuntimeValue is used only for configuration constants; message and carried tensors stay
        // native or Arrow.
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
                    .change_context(InferencerError::Initial)?,
                ),
            });
        }
        let reservation = executor
            .reserve(MemoryClass::Bulk, state.bytes_per_branch.max(1))
            .await
            .change_context(InferencerError::InitialExecution)?;
        let state = state.clone();
        executor
            .run_cpu_with(
                CpuClass::Bulk,
                nervix_execution::QueueAdmission::WaitForPlace,
                reservation,
                move |_charge, cancellation| {
                    let mut tensors = Vec::with_capacity(state.bindings.len());
                    for (binding, initial) in state.bindings.iter().zip(initial) {
                        cancellation
                            .check()
                            .change_context(InferencerError::Cancelled)?;
                        let mut shape = Vec::with_capacity(binding.schema.dimensions.len());
                        for axis in &binding.schema.dimensions {
                            let InferencerTensorDimension::Fixed(size) = axis else {
                                return Err(Report::new(InferencerError::Initial));
                            };
                            let size: usize = size.get().arch_into();
                            shape.push(size);
                        }
                        let mut tensor =
                            Tensor::<f32>::new(&ort::memory::Allocator::default(), shape)
                                .change_context(InferencerError::Initial)?;
                        let (_, target) = tensor
                            .try_extract_tensor_mut::<f32>()
                            .change_context(InferencerError::Initial)?;
                        match initial {
                            None => target.fill(0.0),
                            Some(value) => {
                                let values = binding
                                    .schema
                                    .tensor_from_runtime_value(&value)
                                    .change_context(InferencerError::Initial)?
                                    .values;
                                if values.len() != target.len() {
                                    return Err(Report::new(InferencerError::Initial));
                                }
                                target.copy_from_slice(&values);
                            }
                        }
                        tensors.push(tensor.into_dyn());
                    }
                    Ok(Self { tensors })
                },
            )
            .await
            .change_context(InferencerError::InitialExecution)?
    }

    /// The successor a model run produced, one validated state output per `STATE` entry.
    pub(super) fn from_successors(tensors: Vec<DynValue>) -> Self {
        Self { tensors }
    }

    /// The tensor a model reads for `STATE` entry `index`.
    pub(super) fn input(&self, index: usize) -> &DynValue {
        &self.tensors[index]
    }

    /// Borrow one immutable tensor for a bounded observation without materializing other entries.
    pub(in crate::runtime) fn tensor(
        &self,
        index: usize,
    ) -> error_stack::Result<(&[i64], &[f32]), InferencerError> {
        let tensor = self
            .tensors
            .get(index)
            .ok_or_else(|| Report::new(InferencerError::StateTensor { index }))?;
        let (shape, values) = tensor
            .try_extract_tensor::<f32>()
            .change_context(InferencerError::StateTensor { index })?;
        Ok((shape, values))
    }

    pub(in crate::runtime) fn snapshot(
        &self,
        cancellation: &Cancellation,
    ) -> error_stack::Result<
        Vec<crate::inferencer_state_snapshot::TensorSnapshot<'_>>,
        InferencerError,
    > {
        let mut snapshot = Vec::with_capacity(self.tensors.len());
        for tensor in &self.tensors {
            cancellation
                .check()
                .change_context(InferencerError::Cancelled)?;
            let (shape, values) = tensor
                .try_extract_tensor::<f32>()
                .change_context(InferencerError::Initial)?;
            let mut axes = Vec::with_capacity(shape.len());
            for axis in shape.iter() {
                axes.push(u64::try_from(*axis).change_context(InferencerError::Initial)?);
            }
            snapshot.push(crate::inferencer_state_snapshot::TensorSnapshot {
                shape: axes,
                values: std::borrow::Cow::Borrowed(values),
            });
        }
        Ok(snapshot)
    }

    pub(in crate::runtime) fn restore(
        tensors: Vec<crate::inferencer_state_snapshot::TensorSnapshot<'static>>,
        plan: &InferencerStatePlan,
        cancellation: &Cancellation,
    ) -> error_stack::Result<Self, InferencerError> {
        if tensors.len() != plan.bindings.len() {
            return Err(Report::new(InferencerError::Initial));
        }
        let mut restored = Vec::with_capacity(tensors.len());
        for (tensor, binding) in tensors.into_iter().zip(&plan.bindings) {
            cancellation
                .check()
                .change_context(InferencerError::Cancelled)?;
            let mut expected = Vec::with_capacity(binding.schema.dimensions.len());
            for axis in &binding.schema.dimensions {
                if let InferencerTensorDimension::Fixed(size) = axis {
                    expected.push(u64::from(size.get()));
                }
            }
            if tensor.shape != expected || tensor.values.len() != binding.elements {
                return Err(Report::new(InferencerError::Initial));
            }
            let mut shape = Vec::with_capacity(tensor.shape.len());
            for axis in tensor.shape {
                shape.push(usize::try_from(axis).change_context(InferencerError::Initial)?);
            }
            let restored_tensor = Tensor::<f32>::from_array((shape, tensor.values.into_owned()))
                .change_context(InferencerError::Initial)?;
            restored.push(restored_tensor.into_dyn());
        }
        Ok(Self { tensors: restored })
    }
}

/// The tensors one concrete branch carries from message to message, and the publication the
/// branch's snapshots, replicas, handoffs and inspection read them from.
pub(in crate::runtime) struct CarriedState {
    generation: Arc<NativeGeneration>,
    publication: Arc<ReplicatedInferencerState>,
    incarnation: u64,
    snapshot_bytes: u64,
    bytes: crate::metrics::InferencerBranchBytes,
}

impl std::fmt::Debug for CarriedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CarriedState")
            .field("incarnation", &self.incarnation)
            .finish_non_exhaustive()
    }
}

impl CarriedState {
    /// The state a branch of `incarnation` starts from: the generation its publication holds for
    /// that same incarnation, or the initial generation. A publication left by an earlier
    /// incarnation is withdrawn, so a returning key never reads a predecessor's tensors.
    pub(in crate::runtime) fn restore(
        publication: Arc<ReplicatedInferencerState>,
        prepared: &PreparedState,
        incarnation: u64,
        snapshot_bytes: u64,
        metrics: crate::metrics::InferencerStateMetrics,
    ) -> Self {
        let published = publication.generations.load();
        let mut generation = prepared.initial.clone();
        let mut restored = false;
        if let Some(value) = &published.value {
            if value.incarnation == incarnation {
                generation = value.generation.clone();
                restored = true;
            } else {
                publication.publish(None);
            }
        }
        metrics.initialized(restored);
        Self {
            generation,
            publication,
            incarnation,
            snapshot_bytes,
            bytes: metrics.track_branch(prepared.plan.bytes_per_branch),
        }
    }

    pub(in crate::runtime) fn generation(&self) -> Arc<NativeGeneration> {
        self.generation.clone()
    }

    pub(in crate::runtime) fn publication(&self) -> &Arc<ReplicatedInferencerState> {
        &self.publication
    }

    pub(in crate::runtime) fn metrics(&self) -> crate::metrics::InferencerStateMetrics {
        self.bytes.recorder()
    }

    /// Install the successor of an accepted message as the branch's carried generation.
    pub(super) fn install(&mut self, successor: NativeGeneration) {
        self.generation = Arc::new(successor);
    }

    /// Publish the carried generation as the branch's newest immutable generation.
    pub(in crate::runtime) fn publish(&self) {
        self.publication.publish(Some(InferencerGeneration::new(
            self.incarnation,
            self.generation.clone(),
            self.snapshot_bytes,
        )));
    }

    /// Publish the empty generation an evicted branch leaves, so a returning key starts from
    /// `INITIAL`.
    pub(in crate::runtime) fn evict(&self) {
        self.publication.publish(None);
    }
}

#[cfg(test)]
mod tests;
