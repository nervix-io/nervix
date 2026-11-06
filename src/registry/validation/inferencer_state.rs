//! The fixed tensor and initialization contracts of model-carried inferencer state.
//!
//! Layer: decisions.
//! - **Owns.** Distinct stateful model ports, fixed state shapes and deterministic exact INITIAL types.
//! - **Depends on.** The vocabulary, the VM type checker and validated UDF declarations.
//! - **Must not know.** ONNX sessions, branch task lifetimes or snapshot and recovery mechanisms.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "validates inferencer state before planning")
)]

use ahash::HashSet;
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_models::{
    Assignment, AssignmentTarget, CreateInferencer, DomainName, FieldName, InferencerStateInitial,
    InferencerTensorDimension, InferencerTensorSchema, ModelIndex, ModelName, RouteConstruction,
};
use nervix_primitives::sync::StdArc;
use nervix_vm::{
    CompileBinding, CompileOptions, OutputMode, SchemaSensitivity, SemanticScopePolicy,
    compile_program_with_options_for_bindings_with_sensitivity,
    infer_set_expr_types_for_bindings_with_udfs, lower_route_construction,
};

use super::{
    materialized_state::expression_contains_nondeterministic_or_side_effect_call,
    vm::udf_compile_options,
};
use crate::registry::error::RegistryError;

/// The individual refusal stays beneath the typed owning inferencer and port context.
#[derive(Debug, thiserror::Error)]
pub(in crate::registry) enum InferencerStateValidationError {
    #[error("the model port is declared twice")]
    DuplicatePort,
    #[error("STATE tensors require fixed axes")]
    NonFixedAxes,
    #[error("the fixed tensor element count exceeds the supported size")]
    ElementCount,
    #[error("the combined branch state byte count exceeds the supported size")]
    ByteCount,
    #[error("a message tensor fixed axis exceeds Arrow's supported list size")]
    ArrowAxis,
    #[error("INITIAL must be constant and read no fields")]
    FieldRead,
    #[error("INITIAL must be deterministic and side-effect-free")]
    Nondeterministic,
    #[error("failed to lower INITIAL")]
    LowerInitial,
    #[error("failed to type-check INITIAL")]
    CompileInitial,
    #[error("INITIAL produced no tensor value")]
    NoInitialValue,
    #[error("INITIAL requires {expected:?} non-null, found {actual:?} (nullable: {nullable})")]
    InitialType {
        expected: InferencerTensorSchema,
        actual: arrow_schema::DataType,
        nullable: bool,
    },
}

pub(in crate::registry) fn ensure_inferencer_state(
    domain: &DomainName,
    node: &ModelName,
    models: &ModelIndex,
    processor: &CreateInferencer,
) -> error_stack::Result<(), RegistryError> {
    if processor.state.is_empty() {
        return Ok(());
    }
    let context = |port: &str| RegistryError::InferencerState {
        domain: domain.clone(),
        node: node.clone(),
        port: port.to_owned(),
    };
    let mut ports = HashSet::default();
    for (port, schema) in processor
        .inputs
        .iter()
        .map(|input| (&input.tensor, &input.schema))
        .chain(
            processor
                .output_schema
                .iter()
                .map(|output| (&output.tensor, &output.schema)),
        )
    {
        if schema.dimensions.iter().any(|axis| matches!(axis, InferencerTensorDimension::Fixed(size) if i32::try_from(size.get()).is_err())) {
            return Err(Report::new(InferencerStateValidationError::ArrowAxis).change_context(context(port)));
        }
    }
    for port in processor
        .inputs
        .iter()
        .map(|mapping| mapping.tensor.as_str())
        .chain(
            processor
                .state
                .iter()
                .flat_map(|state| [state.input.as_str(), state.output.as_str()]),
        )
        .chain(
            processor
                .output_schema
                .iter()
                .map(|output| output.tensor.as_str()),
        )
    {
        if !ports.insert(port) {
            return Err(Report::new(InferencerStateValidationError::DuplicatePort)
                .change_context(context(port)));
        }
    }
    let mut bytes = 0u64;
    for binding in &processor.state {
        let context = || context(&binding.input);
        if binding
            .schema
            .dimensions
            .iter()
            .any(|axis| !matches!(axis, InferencerTensorDimension::Fixed(_)))
        {
            return Err(
                Report::new(InferencerStateValidationError::NonFixedAxes).change_context(context())
            );
        }
        let elements = binding.schema.fixed_element_count().ok_or_else(|| {
            Report::new(InferencerStateValidationError::ElementCount).change_context(context())
        })?;
        let elements = u64::try_from(elements)
            .change_context(InferencerStateValidationError::ByteCount)
            .change_context_lazy(context)?;
        let tensor_bytes = elements.checked_mul(4).ok_or_else(|| {
            Report::new(InferencerStateValidationError::ByteCount).change_context(context())
        })?;
        bytes = bytes.checked_add(tensor_bytes).ok_or_else(|| {
            Report::new(InferencerStateValidationError::ByteCount).change_context(context())
        })?;
        let InferencerStateInitial::Constant(expression) = &binding.initial else {
            continue;
        };
        let mut field_read = false;
        expression.visit_fields(&mut |_| field_read = true);
        if field_read {
            return Err(
                Report::new(InferencerStateValidationError::FieldRead).change_context(context())
            );
        }
        if expression_contains_nondeterministic_or_side_effect_call(expression, models) {
            return Err(
                Report::new(InferencerStateValidationError::Nondeterministic)
                    .change_context(context()),
            );
        }
        let parsed = lower_route_construction(
            &RouteConstruction {
                assignments: vec![Assignment {
                    target: AssignmentTarget::bare(
                        FieldName::parse("initial_tensor")
                            .assured("initial_tensor is a language-defined field name"),
                    ),
                    value: expression.clone(),
                }],
                ..RouteConstruction::default()
            },
            SemanticScopePolicy::read_write("input", "initial"),
        )
        .change_context(InferencerStateValidationError::LowerInitial)
        .change_context_lazy(context)?;
        let inferred = infer_set_expr_types_for_bindings_with_udfs(
            &parsed,
            [
                CompileBinding::readonly("input", StdArc::new(arrow_schema::Schema::empty())),
                CompileBinding::writeonly("initial", StdArc::new(arrow_schema::Schema::empty())),
            ],
            udf_compile_options(models, CompileOptions::default()).udf_signatures,
        )
        .change_context(InferencerStateValidationError::CompileInitial)
        .change_context_lazy(context)?;
        let inferred = inferred.first().ok_or_else(|| {
            Report::new(InferencerStateValidationError::NoInitialValue).change_context(context())
        })?;
        if !binding.schema.matches_initial_type(&inferred.data_type) || inferred.nullable {
            return Err(Report::new(InferencerStateValidationError::InitialType {
                expected: binding.schema.clone(),
                actual: inferred.data_type.clone(),
                nullable: inferred.nullable,
            })
            .change_context(context()));
        }
        let output_schema = StdArc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "initial_tensor",
            inferred.data_type.clone(),
            false,
        )]));
        compile_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            output_schema.clone(),
            SchemaSensitivity::default(),
            [
                CompileBinding::readonly("input", StdArc::new(arrow_schema::Schema::empty())),
                CompileBinding::writeonly("initial", output_schema),
            ],
            udf_compile_options(
                models,
                CompileOptions {
                    output_mode: OutputMode::ExplicitOnly,
                    ..CompileOptions::default()
                },
            ),
        )
        .change_context(InferencerStateValidationError::CompileInitial)
        .change_context_lazy(context)?;
    }
    Ok(())
}

trait StateInitialType {
    fn matches_initial_type(&self, actual: &arrow_schema::DataType) -> bool;
}

impl StateInitialType for InferencerTensorSchema {
    fn matches_initial_type(&self, actual: &arrow_schema::DataType) -> bool {
        use arrow_schema::DataType;
        let mut actual = actual;
        for axis in &self.dimensions {
            let InferencerTensorDimension::Fixed(size) = axis else {
                return false;
            };
            let DataType::FixedSizeList(field, length) = actual else {
                return false;
            };
            if u32::try_from(*length).ok() != Some(size.get()) || field.is_nullable() {
                return false;
            }
            actual = field.data_type();
        }
        matches!(actual, DataType::Float32)
    }
}
