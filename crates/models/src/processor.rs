//! The Models of the nodes that process records between relays.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The routes a producing node writes and the inputs a consuming node reads, the
//!   reingestor, junction, deduplicator, correlator, reorderer, window processor, inferencer and
//!   WASM processor Models, and the `ALTER` operations those processors share.
//! - **Depends on.** Expressions, route construction, validated names and resource names.
//! - **Must not know.** How a processor is validated, planned, scheduled or executed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{
    marker::PhantomData,
    num::{NonZeroU32, NonZeroU64},
};

use error_stack::Report;
use meticulous::ResultExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumString, IntoStaticStr};
use thiserror::Error;

use crate::{
    AckMode, BranchSelection, CorrelatorName, DeduplicatorName, GeneralErrorPolicy, InferencerName,
    JunctionName, MessageErrorPolicy, ParseAsType, ReingestorName, RelayName, ReordererName,
    ResourceName, WasmProcessorName, WindowProcessorName,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProcessorOutput {
    pub relay: RelayName,
    #[serde(default)]
    pub construction: crate::RouteConstruction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_policy: Option<FlushPolicy>,
    pub message_error_policy: MessageErrorPolicy,
    pub branch: Option<crate::OutputBranch>,
}

/// How a route or an emitter releases what it has buffered.
///
/// `FLUSH IMMEDIATE` releases each message as it arrives, so there is no batch left to bound.
/// `FLUSH EACH` releases on a cadence and always bounds the batch it releases. One variant per
/// form is what keeps a cadence without a bound, and a bound without a cadence, out of the model.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum FlushPolicy {
    Immediate,
    Each {
        interval: String,
        max_batch_size: String,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct InputCollectPolicy {
    pub collect_for: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<String>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProcessorInputWhere {
    pub relay: RelayName,
    pub where_clause: crate::Expression,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProcessorInputs {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<RelayName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub r#where: Vec<ProcessorInputWhere>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collect_policy: Option<InputCollectPolicy>,
}

impl ProcessorInputs {
    pub fn new(from: Vec<RelayName>, r#where: Vec<ProcessorInputWhere>) -> Self {
        Self {
            from,
            r#where,
            collect_policy: None,
        }
    }

    pub fn single(relay: RelayName) -> Self {
        Self {
            from: vec![relay],
            r#where: Vec::new(),
            collect_policy: None,
        }
    }

    pub fn with_collect_policy(
        mut self,
        collect_for: String,
        max_batch_size: Option<String>,
    ) -> Self {
        self.collect_policy = Some(InputCollectPolicy {
            collect_for,
            max_batch_size,
        });
        self
    }

    pub fn first(&self) -> Option<&RelayName> {
        self.from.first()
    }

    pub fn relays(&self) -> &[RelayName] {
        &self.from
    }

    pub fn input_where(&self) -> &[ProcessorInputWhere] {
        &self.r#where
    }

    pub fn where_clauses(&self) -> &[ProcessorInputWhere] {
        &self.r#where
    }
}

impl ProcessorOutput {
    pub fn new(relay: RelayName) -> Self {
        Self {
            relay,
            construction: crate::RouteConstruction::default(),
            flush_policy: None,
            message_error_policy: MessageErrorPolicy::Log,
            branch: None,
        }
    }

    pub fn with_flush_policy(relay: RelayName, flush_policy: FlushPolicy) -> Self {
        Self {
            relay,
            construction: crate::RouteConstruction::default(),
            flush_policy: Some(flush_policy),
            message_error_policy: MessageErrorPolicy::Log,
            branch: None,
        }
    }

    pub fn with_branch(mut self, branch: crate::OutputBranch) -> Self {
        self.branch = Some(branch);
        self
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProcessorOutputs {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<ProcessorOutput>,
}

impl ProcessorOutputs {
    pub fn new(routes: Vec<ProcessorOutput>) -> Self {
        Self { routes }
    }

    pub fn single(relay: RelayName) -> Self {
        Self {
            routes: vec![ProcessorOutput::new(relay)],
        }
    }

    pub fn with_flush_policy(mut self, flush_policy: FlushPolicy) -> Self {
        for output in &mut self.routes {
            output.flush_policy = Some(flush_policy.clone());
        }
        self
    }

    pub fn with_branch(mut self, branch: crate::OutputBranch) -> Self {
        for output in &mut self.routes {
            output.branch = Some(branch.clone());
        }
        self
    }

    pub fn relays(&self) -> impl Iterator<Item = &RelayName> {
        self.outputs().map(|output| &output.relay)
    }

    pub fn outputs(&self) -> impl Iterator<Item = &ProcessorOutput> {
        self.routes.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateReingestor {
    pub name: ReingestorName,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub mode: AckMode,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterReingestor {
    pub reingestor: ReingestorName,
    pub operations: Vec<AlterProcessorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterReingestorError {
    #[error("ALTER targets reingestor `{requested}`, but the stored reingestor is `{stored}`")]
    ReingestorNameMismatch {
        stored: ReingestorName,
        requested: ReingestorName,
    },
    #[error(transparent)]
    Processor(#[from] AlterProcessorError),
}

impl CreateReingestor {
    pub fn apply_alter(
        &mut self,
        alter: &AlterReingestor,
    ) -> error_stack::Result<(), AlterReingestorError> {
        if self.name != alter.reingestor {
            return Err(Report::new(AlterReingestorError::ReingestorNameMismatch {
                stored: self.name.clone(),
                requested: alter.reingestor.clone(),
            }));
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.processor_alter_target().apply(operation)?;
        }
        *self = candidate;
        Ok(())
    }

    fn processor_alter_target(&mut self) -> ProcessorAlterTarget<'_, AlterReingestorError> {
        ProcessorAlterTarget {
            from: &mut self.from,
            output_routes: &mut self.output_routes,
            branched_by: None,
            mode: &mut self.mode,
            filter_where: &mut self.filter_where,
            materialized_state: &mut self.materialized_state,
            rejection: PhantomData,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateInferencer<Version = u64> {
    pub name: InferencerName,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub resource: ResourceName,
    pub resource_version: Version,
    pub file: String,
    pub inputs: Vec<InferencerTensorMapping>,
    pub output_schema: Vec<InferencerTensorDeclaration>,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

impl CreateInferencer {
    pub fn execution_mode(
        &self,
    ) -> error_stack::Result<InferencerExecutionMode, InferencerTensorSchemaError> {
        let mut execution_mode = None;
        for (tensor, schema) in self
            .inputs
            .iter()
            .map(|mapping| (mapping.tensor.as_str(), &mapping.schema))
            .chain(
                self.output_schema
                    .iter()
                    .map(|declaration| (declaration.tensor.as_str(), &declaration.schema)),
            )
        {
            let batch_axis_count = schema.batch_axis_count();
            if batch_axis_count > 1 {
                return Err(Report::new(
                    InferencerTensorSchemaError::MultipleBatchAxes {
                        tensor: tensor.to_string(),
                    },
                ));
            }
            if schema.fixed_element_count().is_none() {
                return Err(Report::new(
                    InferencerTensorSchemaError::ElementCountOverflow {
                        tensor: tensor.to_string(),
                    },
                ));
            }
            let mapping_mode = if batch_axis_count == 1 {
                InferencerExecutionMode::Batched
            } else {
                InferencerExecutionMode::PerMessage
            };
            if let Some(execution_mode) = execution_mode
                && execution_mode != mapping_mode
            {
                return Err(Report::new(
                    InferencerTensorSchemaError::MixedExecutionModes,
                ));
            }
            execution_mode = Some(mapping_mode);
        }
        Ok(execution_mode.unwrap_or(InferencerExecutionMode::PerMessage))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InferencerExecutionMode {
    PerMessage,
    Batched,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InferencerTensorSchemaError {
    #[error("tensor '{tensor}' contains more than one BATCH axis")]
    MultipleBatchAxes { tensor: String },
    #[error("inferencer mixes batched and per-message tensor bindings")]
    MixedExecutionModes,
    #[error("tensor '{tensor}' fixed element count exceeds the supported size")]
    ElementCountOverflow { tensor: String },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateWasmProcessor<Version = u64> {
    pub name: WasmProcessorName,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub resource: ResourceName,
    pub resource_version: Version,
    pub file: String,
    pub limits: WasmProcessorLimits,
    pub global_error_policy: GeneralErrorPolicy,
    pub rejected_state_policy: WasmRejectedStatePolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct WasmProcessorLimits {
    pub max_fuel: NonZeroU64,
    pub max_memory_bytes: NonZeroU64,
}

/// What a WASM processor does with a saved snapshot its guest refused to restore.
///
/// A guest's verdict on the bytes it was handed is the only failure that classifies saved state as
/// unusable, so it is the only failure this policy answers. Every other way instantiating a branch
/// can fail — compiling the module, initializing the guest, an exhausted limit, storage,
/// replication, or the state's authority — leaves the saved state exactly as usable as it was and
/// never reaches this decision.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::AsRefStr,
)]
#[strum(serialize_all = "UPPERCASE")]
pub enum WasmRejectedStatePolicy {
    /// Keep the refused snapshot and report the refusal. Every later instance of the branch is
    /// handed the same bytes and reports the same failure until an operator acts.
    #[default]
    Preserve,
    /// Replace the refused lifetime once, through the coordinated reset, and resume the branch on
    /// the fresh lifetime it publishes. One refused lifetime is worth exactly one attempt.
    Reset,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct InferencerTensorMapping {
    pub tensor: String,
    pub schema: InferencerTensorSchema,
    pub expression: crate::Expression,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct InferencerTensorDeclaration {
    pub tensor: String,
    pub schema: InferencerTensorSchema,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct InferencerTensorSchema {
    pub representation: InferencerTensorRepresentation,
    pub element_type: InferencerTensorElementType,
    pub dimensions: Vec<InferencerTensorDimension>,
}

impl InferencerTensorSchema {
    pub fn batch_axis(&self) -> Option<usize> {
        self.dimensions
            .iter()
            .position(InferencerTensorDimension::is_batch)
    }

    pub fn batch_axis_count(&self) -> usize {
        self.dimensions
            .iter()
            .filter(|dimension| dimension.is_batch())
            .count()
    }

    pub fn fixed_element_count(&self) -> Option<usize> {
        self.dimensions
            .iter()
            .filter_map(|dimension| match dimension {
                InferencerTensorDimension::Fixed(size) => Some(
                    usize::try_from(size.get())
                        .assured("u32 fits usize on every architecture supported by the models"),
                ),
                InferencerTensorDimension::Dynamic | InferencerTensorDimension::Batch => None,
            })
            .try_fold(1_usize, usize::checked_mul)
    }

    pub fn is_compatible_with_field_type(&self, field_type: &ParseAsType) -> bool {
        let mut field_type = field_type;
        for dimension in &self.dimensions {
            match dimension {
                InferencerTensorDimension::Fixed(expected) => {
                    let ParseAsType::Array { element, len } = field_type else {
                        return false;
                    };
                    if len != expected {
                        return false;
                    }
                    field_type = element;
                }
                InferencerTensorDimension::Dynamic => {
                    let ParseAsType::Vec { element } = field_type else {
                        return false;
                    };
                    field_type = element;
                }
                InferencerTensorDimension::Batch => {}
            }
        }
        field_type == &ParseAsType::F32
    }

    pub fn message_type(&self) -> ParseAsType {
        self.dimensions
            .iter()
            .rev()
            .fold(ParseAsType::F32, |element, dimension| match dimension {
                InferencerTensorDimension::Fixed(len) => ParseAsType::Array {
                    element: Box::new(element),
                    len: *len,
                },
                InferencerTensorDimension::Dynamic => ParseAsType::Vec {
                    element: Box::new(element),
                },
                InferencerTensorDimension::Batch => element,
            })
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum InferencerTensorRepresentation {
    Dense,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum InferencerTensorElementType {
    F32,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum InferencerTensorDimension {
    Fixed(NonZeroU32),
    Dynamic,
    Batch,
}

impl InferencerTensorDimension {
    pub fn is_batch(&self) -> bool {
        matches!(self, Self::Batch)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateJunction {
    pub name: JunctionName,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterJunction {
    pub junction: JunctionName,
    pub operations: Vec<AlterProcessorOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterJunctionError {
    #[error("ALTER targets junction `{requested}`, but the stored junction is `{stored}`")]
    JunctionNameMismatch {
        stored: JunctionName,
        requested: JunctionName,
    },
    #[error(transparent)]
    Processor(#[from] AlterProcessorError),
}

impl CreateJunction {
    pub fn apply_alter(
        &mut self,
        alter: &AlterJunction,
    ) -> error_stack::Result<(), AlterJunctionError> {
        if self.name != alter.junction {
            return Err(Report::new(AlterJunctionError::JunctionNameMismatch {
                stored: self.name.clone(),
                requested: alter.junction.clone(),
            }));
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.processor_alter_target().apply(operation)?;
        }
        *self = candidate;
        Ok(())
    }

    fn processor_alter_target(&mut self) -> ProcessorAlterTarget<'_, AlterJunctionError> {
        ProcessorAlterTarget {
            from: &mut self.from,
            output_routes: &mut self.output_routes,
            branched_by: Some(&mut self.branched_by),
            mode: &mut self.mode,
            filter_where: &mut self.filter_where,
            materialized_state: &mut self.materialized_state,
            rejection: PhantomData,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateDeduplicator {
    pub name: DeduplicatorName,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub deduplicate_on: Vec<crate::Expression>,
    pub max_time: String,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterDeduplicator {
    pub deduplicator: DeduplicatorName,
    pub operations: Vec<AlterDeduplicatorOperation>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterDeduplicatorOperation {
    Processor(Box<AlterProcessorOperation>),
    SetDeduplicateOn { expressions: Vec<crate::Expression> },
    SetMaxTime { max_time: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterDeduplicatorError {
    #[error("ALTER targets deduplicator `{requested}`, but the stored deduplicator is `{stored}`")]
    DeduplicatorNameMismatch {
        stored: DeduplicatorName,
        requested: DeduplicatorName,
    },
    #[error(transparent)]
    Processor(#[from] AlterProcessorError),
}

impl CreateDeduplicator {
    pub fn apply_alter(
        &mut self,
        alter: &AlterDeduplicator,
    ) -> error_stack::Result<(), AlterDeduplicatorError> {
        if self.name != alter.deduplicator {
            return Err(Report::new(
                AlterDeduplicatorError::DeduplicatorNameMismatch {
                    stored: self.name.clone(),
                    requested: alter.deduplicator.clone(),
                },
            ));
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            match operation {
                AlterDeduplicatorOperation::Processor(operation) => {
                    candidate.processor_alter_target().apply(operation)?;
                }
                AlterDeduplicatorOperation::SetDeduplicateOn { expressions } => {
                    candidate.deduplicate_on = expressions.clone();
                }
                AlterDeduplicatorOperation::SetMaxTime { max_time } => {
                    candidate.max_time = max_time.clone();
                }
            }
        }
        *self = candidate;
        Ok(())
    }

    fn processor_alter_target(&mut self) -> ProcessorAlterTarget<'_, AlterDeduplicatorError> {
        ProcessorAlterTarget {
            from: &mut self.from,
            output_routes: &mut self.output_routes,
            branched_by: Some(&mut self.branched_by),
            mode: &mut self.mode,
            filter_where: &mut self.filter_where,
            materialized_state: &mut self.materialized_state,
            rejection: PhantomData,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateCorrelator {
    pub name: CorrelatorName,
    pub left: ProcessorInputs,
    pub right: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub correlate_where: crate::Expression,
    pub match_policy: CorrelatorMatchPolicy,
    pub max_time: String,
    pub timeout_policy: CorrelationTimeoutPolicy,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    AsRefStr,
    EnumString,
    IntoStaticStr,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE", ascii_case_insensitive)]
pub enum CorrelatorMatchPolicy {
    Earliest,
    Latest,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CorrelationTimeoutPolicy {
    pub left: CorrelationTimeoutAction,
    pub right: CorrelationTimeoutAction,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum CorrelationTimeoutAction {
    Drop,
    SendTo { relay: RelayName },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateReorderer {
    pub name: ReordererName,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub order_by: Vec<crate::Expression>,
    pub max_time: String,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterReorderer {
    pub reorderer: ReordererName,
    pub operations: Vec<AlterReordererOperation>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterReordererOperation {
    Processor(Box<AlterProcessorOperation>),
    SetOrderBy { expressions: Vec<crate::Expression> },
    SetMaxTime { max_time: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterReordererError {
    #[error("ALTER targets reorderer `{requested}`, but the stored reorderer is `{stored}`")]
    ReordererNameMismatch {
        stored: ReordererName,
        requested: ReordererName,
    },
    #[error(transparent)]
    Processor(#[from] AlterProcessorError),
}

impl CreateReorderer {
    pub fn apply_alter(
        &mut self,
        alter: &AlterReorderer,
    ) -> error_stack::Result<(), AlterReordererError> {
        if self.name != alter.reorderer {
            return Err(Report::new(AlterReordererError::ReordererNameMismatch {
                stored: self.name.clone(),
                requested: alter.reorderer.clone(),
            }));
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            match operation {
                AlterReordererOperation::Processor(operation) => {
                    candidate.processor_alter_target().apply(operation)?;
                }
                AlterReordererOperation::SetOrderBy { expressions } => {
                    candidate.order_by = expressions.clone();
                }
                AlterReordererOperation::SetMaxTime { max_time } => {
                    candidate.max_time = max_time.clone();
                }
            }
        }
        *self = candidate;
        Ok(())
    }

    fn processor_alter_target(&mut self) -> ProcessorAlterTarget<'_, AlterReordererError> {
        ProcessorAlterTarget {
            from: &mut self.from,
            output_routes: &mut self.output_routes,
            branched_by: Some(&mut self.branched_by),
            mode: &mut self.mode,
            filter_where: &mut self.filter_where,
            materialized_state: &mut self.materialized_state,
            rejection: PhantomData,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterProcessorOperation {
    AddFrom {
        relay: RelayName,
        where_clause: Option<crate::Expression>,
    },
    DropFrom {
        relay: RelayName,
    },
    AlterFromSetWhere {
        relay: RelayName,
        where_clause: crate::Expression,
    },
    AlterFromDropWhere {
        relay: RelayName,
    },
    SetCollect {
        policy: InputCollectPolicy,
    },
    DropCollect,
    SetFilterWhere {
        where_clause: crate::Expression,
    },
    DropFilterWhere,
    SetMode {
        mode: AckMode,
    },
    SetBranching {
        branching: BranchSelection,
    },
    AddMaterializedState {
        dependency: crate::MaterializedStateDependency,
    },
    DropMaterializedState {
        relay: RelayName,
    },
    AlterMaterializedState {
        relay: RelayName,
        policy: crate::MaterializedStatePolicy,
    },
    AddRoute {
        route: ProcessorOutput,
    },
    DropRoute {
        relay: RelayName,
    },
    ReplaceRoute {
        route: ProcessorOutput,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterProcessorError {
    #[error("input relay `{relay}` is already configured")]
    InputAlreadyExists { relay: RelayName },
    #[error("input relay `{relay}` is not configured")]
    InputNotFound { relay: RelayName },
    #[error("input relay `{relay}` has no WHERE clause")]
    InputWhereNotConfigured { relay: RelayName },
    #[error("a processor must retain at least one input")]
    CannotDropLastInput,
    #[error("materialized-state dependency `{relay}` is already configured")]
    MaterializedStateAlreadyConfigured { relay: RelayName },
    #[error("materialized-state dependency `{relay}` is not configured")]
    MaterializedStateNotConfigured { relay: RelayName },
    #[error("route target `{relay}` is not configured")]
    RouteTargetNotFound { relay: RelayName },
    #[error("route target `{relay}` is ambiguous because it is configured more than once")]
    RouteTargetAmbiguous { relay: RelayName },
    #[error("a processor must retain at least one route")]
    CannotDropLastRoute,
    #[error("this processor configures branching per route")]
    BranchingUnsupported,
}

/// The input, route and dependency fields every processor alters the same way, borrowed from the
/// processor being altered.
///
/// `E` is that processor's alteration error. A rejection is reported in it where it is detected, so
/// the report starts at the failure rather than at a conversion in the processor's `apply_alter`.
struct ProcessorAlterTarget<'a, E> {
    from: &'a mut ProcessorInputs,
    output_routes: &'a mut ProcessorOutputs,
    branched_by: Option<&'a mut BranchSelection>,
    mode: &'a mut AckMode,
    filter_where: &'a mut Option<crate::Expression>,
    materialized_state: &'a mut Vec<crate::MaterializedStateDependency>,
    rejection: PhantomData<E>,
}

impl<E> ProcessorAlterTarget<'_, E>
where
    E: From<AlterProcessorError> + error_stack::Context,
{
    /// Reports `rejection` as the altered processor's own error.
    #[track_caller]
    fn reject(rejection: AlterProcessorError) -> Report<E> {
        Report::new(E::from(rejection))
    }

    fn apply(&mut self, operation: &AlterProcessorOperation) -> error_stack::Result<(), E> {
        match operation {
            AlterProcessorOperation::AddFrom {
                relay,
                where_clause,
            } => {
                self.ensure_input_absent(relay)?;
                self.from.from.push(relay.clone());
                if let Some(where_clause) = where_clause {
                    self.from.r#where.push(ProcessorInputWhere {
                        relay: relay.clone(),
                        where_clause: where_clause.clone(),
                    });
                }
            }
            AlterProcessorOperation::DropFrom { relay } => {
                let index = self.input_index(relay)?;
                if self.from.from.len() == 1 {
                    return Err(Self::reject(AlterProcessorError::CannotDropLastInput));
                }
                self.from.from.remove(index);
                self.from
                    .r#where
                    .retain(|input_where| input_where.relay != *relay);
            }
            AlterProcessorOperation::AlterFromSetWhere {
                relay,
                where_clause,
            } => {
                self.input_index(relay)?;
                if let Some(input_where) = self
                    .from
                    .r#where
                    .iter_mut()
                    .find(|input_where| input_where.relay == *relay)
                {
                    input_where.where_clause = where_clause.clone();
                } else {
                    self.from.r#where.push(ProcessorInputWhere {
                        relay: relay.clone(),
                        where_clause: where_clause.clone(),
                    });
                }
            }
            AlterProcessorOperation::AlterFromDropWhere { relay } => {
                self.input_index(relay)?;
                let Some(index) = self
                    .from
                    .r#where
                    .iter()
                    .position(|input_where| input_where.relay == *relay)
                else {
                    return Err(Self::reject(AlterProcessorError::InputWhereNotConfigured {
                        relay: relay.clone(),
                    }));
                };
                self.from.r#where.remove(index);
            }
            AlterProcessorOperation::SetCollect { policy } => {
                self.from.collect_policy = Some(policy.clone());
            }
            AlterProcessorOperation::DropCollect => {
                self.from.collect_policy = None;
            }
            AlterProcessorOperation::SetFilterWhere { where_clause } => {
                *self.filter_where = Some(where_clause.clone());
            }
            AlterProcessorOperation::DropFilterWhere => {
                *self.filter_where = None;
            }
            AlterProcessorOperation::SetMode { mode } => {
                *self.mode = *mode;
            }
            AlterProcessorOperation::SetBranching { branching } => {
                let Some(branched_by) = self.branched_by.as_deref_mut() else {
                    return Err(Self::reject(AlterProcessorError::BranchingUnsupported));
                };
                *branched_by = branching.clone();
            }
            AlterProcessorOperation::AddMaterializedState { dependency } => {
                if self
                    .materialized_state
                    .iter()
                    .any(|existing| existing.relay == dependency.relay)
                {
                    return Err(Self::reject(
                        AlterProcessorError::MaterializedStateAlreadyConfigured {
                            relay: dependency.relay.clone(),
                        },
                    ));
                }
                self.materialized_state.push(dependency.clone());
            }
            AlterProcessorOperation::DropMaterializedState { relay } => {
                let index = self.materialized_state_index(relay)?;
                self.materialized_state.remove(index);
            }
            AlterProcessorOperation::AlterMaterializedState { relay, policy } => {
                let index = self.materialized_state_index(relay)?;
                self.materialized_state[index].policy = policy.clone();
            }
            AlterProcessorOperation::AddRoute { route } => {
                self.output_routes.routes.push(route.clone());
            }
            AlterProcessorOperation::DropRoute { relay } => {
                let index = self.unique_route_index(relay)?;
                if self.output_routes.routes.len() == 1 {
                    return Err(Self::reject(AlterProcessorError::CannotDropLastRoute));
                }
                self.output_routes.routes.remove(index);
            }
            AlterProcessorOperation::ReplaceRoute { route } => {
                let index = self.unique_route_index(&route.relay)?;
                self.output_routes.routes[index] = route.clone();
            }
        }
        Ok(())
    }

    fn input_index(&self, relay: &RelayName) -> error_stack::Result<usize, E> {
        self.from
            .from
            .iter()
            .position(|candidate| candidate == relay)
            .ok_or_else(|| {
                Self::reject(AlterProcessorError::InputNotFound {
                    relay: relay.clone(),
                })
            })
    }

    fn ensure_input_absent(&self, relay: &RelayName) -> error_stack::Result<(), E> {
        if self.from.from.iter().any(|candidate| candidate == relay) {
            Err(Self::reject(AlterProcessorError::InputAlreadyExists {
                relay: relay.clone(),
            }))
        } else {
            Ok(())
        }
    }

    fn materialized_state_index(&self, relay: &RelayName) -> error_stack::Result<usize, E> {
        self.materialized_state
            .iter()
            .position(|dependency| dependency.relay == *relay)
            .ok_or_else(|| {
                Self::reject(AlterProcessorError::MaterializedStateNotConfigured {
                    relay: relay.clone(),
                })
            })
    }

    fn unique_route_index(&self, relay: &RelayName) -> error_stack::Result<usize, E> {
        let mut indexes = self
            .output_routes
            .routes
            .iter()
            .enumerate()
            .filter_map(|(index, route)| (route.relay == *relay).then_some(index));
        let Some(index) = indexes.next() else {
            return Err(Self::reject(AlterProcessorError::RouteTargetNotFound {
                relay: relay.clone(),
            }));
        };
        if indexes.next().is_some() {
            return Err(Self::reject(AlterProcessorError::RouteTargetAmbiguous {
                relay: relay.clone(),
            }));
        }
        Ok(index)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateWindowProcessor {
    pub name: WindowProcessorName,
    pub from: ProcessorInputs,
    pub output_routes: ProcessorOutputs,
    pub branched_by: BranchSelection,
    pub width: WindowBound,
    pub step: WindowBound,
    pub state_limit: WindowStateLimit,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_where: Option<crate::Expression>,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

/// The state allowance of one concrete window branch. Sketch windows require a byte limit.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum WindowStateLimit {
    Unbounded,
    MaxBytes(NonZeroU64),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WindowBound {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,
}

impl WindowBound {
    /// A bound counted in messages alone.
    pub fn of_messages(messages: u64) -> Self {
        Self {
            messages: Some(messages),
            duration: None,
        }
    }

    /// A bound measured in time alone.
    pub fn of_duration(duration: String) -> Self {
        Self {
            messages: None,
            duration: Some(duration),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_none() && self.duration.is_none()
    }

    pub fn to_describe_string(&self) -> String {
        let mut parts = Vec::new();
        if let Some(messages) = self.messages {
            parts.push(format!("{messages} MESSAGES"));
        }
        if let Some(duration) = &self.duration {
            parts.push(format!("{duration} DURATION"));
        }
        parts.join(" ")
    }
}

#[cfg(test)]
#[path = "processor_tests.rs"]
mod tests;
