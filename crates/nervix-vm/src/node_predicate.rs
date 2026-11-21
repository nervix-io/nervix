//! Columnar input for a predicate over committed cluster node records.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Lowering a node expression, binding its literal tag reads and supplying the fixed
//!   property columns to the shared VM.
//! - **Depends on.** Typed node record vocabulary and the VM compiler and evaluator.
//! - **Must not know.** Which records the caller captures, placement or command results.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "evaluates node predicate columns")
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

use arch_into::ArchInto as _;
use arrow_array::{
    BooleanArray, StringArray, UInt64Array,
    builder::{ListBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema};
use error_stack::{Report, ResultExt as _};
use nervix_models::{
    ClusterNodeName, CommittedNodeRecord, Detected, Expression, Literal, NODE_SCOPE,
    NODE_TAG_FUNCTION, NodeSelectorField, OnnxRuntime, OnnxRuntimeDetection, PropertyText,
    TagValue,
};
use nervix_primitives::sync::StdArc;
use strum::IntoEnumIterator as _;
use thiserror::Error;

use crate::{
    CompileBinding, DeterministicPredicate, DeterministicPredicateResult, RuntimeError,
    SemanticScopePolicy, TypedArray, TypedBatch, compile_deterministic_predicate,
    execute_deterministic_predicate, lower_expression, program::InternalFieldNamespace,
};

#[cfg(test)]
mod tests;

#[derive(Debug, Error)]
#[error("invalid node selector")]
pub struct NodePredicateError;

#[derive(Debug, Clone)]
pub struct NodePredicate {
    predicate: DeterministicPredicate,
    tag_keys: BTreeSet<String>,
}

impl NodePredicate {
    pub fn compile(expression: &Expression) -> Result<Self, Report<NodePredicateError>> {
        let mut tag_keys = BTreeSet::new();
        expression.visit_calls(&mut |name, arguments| {
            if name.as_str() == NODE_TAG_FUNCTION
                && let [Expression::Literal(Literal::String(key))] = arguments
            {
                tag_keys.insert(key.clone());
            }
        });
        let properties = Schema::new(
            NodeSelectorField::iter()
                .map(NodeSelectorField::arrow_field)
                .collect::<Vec<_>>(),
        );
        let tags = Schema::new(
            tag_keys
                .iter()
                .map(|key| Field::new(key, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        );
        let bindings = [
            CompileBinding::readonly(NODE_SCOPE, StdArc::new(properties)),
            CompileBinding::internal_readonly(InternalFieldNamespace::NodeTags, StdArc::new(tags)),
        ];
        let expression = lower_expression(expression, SemanticScopePolicy::NodeSelector)
            .change_context(NodePredicateError)?;
        let predicate = compile_deterministic_predicate(&expression, bindings)
            .change_context(NodePredicateError)?;
        Ok(Self {
            predicate,
            tag_keys,
        })
    }

    /// Rows follow the order of the node names. The VM returns the original indices and errors.
    pub fn evaluate(
        &self,
        records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>,
    ) -> Result<DeterministicPredicateResult, Report<RuntimeError>> {
        let mut columns = Vec::new();
        for field in NodeSelectorField::iter() {
            columns.push(Self::column(records, field));
        }
        for key in &self.tag_keys {
            let values = records
                .values()
                .map(|row| row.record.tags.get(key).map(TagValue::as_str));
            columns.push(StringArray::from_iter(values).into());
        }
        let batch = TypedBatch::try_new(self.predicate.input_schema().clone(), columns)?;
        execute_deterministic_predicate(&self.predicate, &batch)
    }

    fn column(
        records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>,
        field: NodeSelectorField,
    ) -> TypedArray {
        let values = records.values().map(|row| &row.record);
        match field {
            NodeSelectorField::Name => {
                StringArray::from_iter_values(records.keys().map(ClusterNodeName::as_str)).into()
            }
            NodeSelectorField::Architecture => StringArray::from_iter_values(
                values.map(|record| record.properties.architecture.as_str()),
            )
            .into(),
            NodeSelectorField::CpuCount => UInt64Array::from_iter(
                values.map(|record| record.capabilities().cpu_count.map(NonZeroU64::get)),
            )
            .into(),
            NodeSelectorField::CpuModel => StringArray::from_iter(values.map(|record| {
                record
                    .properties
                    .cpu_model
                    .value()
                    .map(PropertyText::as_str)
            }))
            .into(),
            NodeSelectorField::MemoryLimit => UInt64Array::from_iter(
                values.map(|record| record.capabilities().memory_bytes.map(NonZeroU64::get)),
            )
            .into(),
            NodeSelectorField::GpuCount => UInt64Array::from_iter_values(
                values.map(|record| record.properties.gpus.count().arch_into()),
            )
            .into(),
            NodeSelectorField::GpuMemory => UInt64Array::from_iter(values.map(|record| {
                record
                    .properties
                    .gpus
                    .smallest_memory_bytes()
                    .map(NonZeroU64::get)
            }))
            .into(),
            NodeSelectorField::GpuDriver => StringArray::from_iter(values.map(|record| {
                let Some(Detected::Value(driver)) = record.properties.gpus.driver() else {
                    return None;
                };
                Some(driver.as_str())
            }))
            .into(),
            NodeSelectorField::OnnxVersion => StringArray::from_iter(values.map(|record| {
                Self::onnx(&record.properties.onnx)?
                    .version
                    .value()
                    .map(PropertyText::as_str)
            }))
            .into(),
            NodeSelectorField::OnnxCuda => {
                BooleanArray::from_iter(values.map(|record| Some(record.properties.onnx.cuda())))
                    .into()
            }
            NodeSelectorField::CudaAvailable => BooleanArray::from_iter(
                values.map(|record| Some(record.properties.cuda_available())),
            )
            .into(),
            NodeSelectorField::GpuModels => Self::gpu_models(records),
            NodeSelectorField::OnnxProviders => Self::onnx_providers(records),
        }
    }

    fn onnx(detection: &OnnxRuntimeDetection) -> Option<&OnnxRuntime> {
        match detection {
            OnnxRuntimeDetection::Loaded(runtime) => Some(runtime),
            OnnxRuntimeDetection::Undetected(_) => None,
        }
    }

    fn gpu_models(records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>) -> TypedArray {
        let mut array = ListBuilder::new(StringBuilder::new());
        for row in records.values() {
            for device in row.record.properties.gpus.devices() {
                array
                    .values()
                    .append_option(device.model.value().map(PropertyText::as_str));
            }
            array.append(true);
        }
        TypedArray::Generic(StdArc::new(array.finish()))
    }

    fn onnx_providers(records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>) -> TypedArray {
        let field = Field::new("item", DataType::Utf8, false);
        let mut array = ListBuilder::new(StringBuilder::new()).with_field(StdArc::new(field));
        for row in records.values() {
            let Some(runtime) = Self::onnx(&row.record.properties.onnx) else {
                array.append(false);
                continue;
            };
            let Some(providers) = runtime.providers.value() else {
                array.append(false);
                continue;
            };
            for provider in providers.as_slice() {
                array.values().append_value(provider.as_str());
            }
            array.append(true);
        }
        TypedArray::Generic(StdArc::new(array.finish()))
    }
}
