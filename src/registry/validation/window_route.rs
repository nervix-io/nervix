//! Whether a window route's aggregates hold their type contracts when the processor is applied.
//!
//! Layer: decisions.
//!
//! - **Owns.** Compiling a window route's aggregate program with the compiler the runtime uses,
//!   against the declared input, output, branch and materialized-state schemas.
//! - **Depends on.** The VM's window route compiler and the schema and binding rules.
//! - **Must not know.** How a window executes, emits or keeps its state.

use ahash::HashSet;
use arrow_schema::DataType;
use error_stack::Report;
use nervix_models::{CreateSchema, ProcessorOutput};
use nervix_vm::{
    CompileOptions,
    program::{FieldRef, Program, SpannedNode},
    window::{
        CompiledWindowRoute, WindowAggregateExpr, WindowAggregateProgram, WindowRouteSchemas,
    },
};

use crate::registry::{
    error::RegistryError,
    validation::{
        materialized_state::referenced_materialized_stream_bindings,
        processor::ModelValidationContext,
        schema::{
            arrow_schema_for_internal_schema, readonly_binding_for_internal_schema,
            schema_sensitivity_for_internal_schema,
        },
        vm::{BRANCH_NAMESPACE, udf_compile_options},
    },
};

/// Compile the route's aggregate program with the compiler the runtime uses, so every argument
/// type, result type, nullability and sensitivity contract of its aggregates holds when the
/// processor is applied rather than surfacing when a branch first runs it.
pub(in crate::registry) fn validate_window_route_types(
    context: ModelValidationContext<'_, '_>,
    output: &ProcessorOutput,
    aggregate: &WindowAggregateProgram,
    output_schema: &CreateSchema,
    input_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
) -> Result<(), Report<RegistryError>> {
    let ModelValidationContext {
        domain,
        identifier,
        models,
    } = context;
    let mut readable = Vec::new();
    if let Some(branch_schema) = branch_schema {
        readable.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    readable.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &window_value_program(aggregate),
        &local_namespaces,
        "window route SET",
    )?);
    let input_arrow_schema = arrow_schema_for_internal_schema(input_schema);
    let input_sensitivity = schema_sensitivity_for_internal_schema(input_schema);
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let output_sensitivity = schema_sensitivity_for_internal_schema(output_schema);
    let compiled = CompiledWindowRoute::compile(
        aggregate,
        WindowRouteSchemas {
            input: &input_arrow_schema,
            input_sensitivity: &input_sensitivity,
            readable: &readable,
            output: &output_arrow_schema,
            output_sensitivity: &output_sensitivity,
        },
        &udf_compile_options(models, CompileOptions::default()),
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("window output '{}' compile failed: {error:#}", output.relay),
        })
    })?;
    for (demand_index, demand) in compiled.demands.iter().enumerate() {
        for (argument_index, argument) in demand.arguments.iter().enumerate() {
            if contains_bytes(&argument.data_type) {
                return Err(Report::new(RegistryError::WindowArgumentContainsBytes {
                    domain: domain.clone(),
                    processor: identifier.clone(),
                    route: output.relay.clone(),
                    demand: demand_index,
                    argument: argument_index,
                }));
            }
        }
    }
    Ok(())
}

fn contains_bytes(data_type: &DataType) -> bool {
    match data_type {
        DataType::Binary => true,
        DataType::List(field) | DataType::FixedSizeList(field, _) => {
            contains_bytes(field.data_type())
        }
        _ => false,
    }
}

/// Every scalar value a window route assigns, as one program whose references can be walked.
fn window_value_program(aggregate: &WindowAggregateProgram) -> SpannedNode<Program> {
    let mut set = Vec::new();
    for assignment in &aggregate.assignments {
        let mut pending = vec![&assignment.value.inner];
        while let Some(value) = pending.pop() {
            match value {
                WindowAggregateExpr::Scalar(expr) => {
                    set.push((
                        FieldRef {
                            relay: assignment.target.relay.clone(),
                            field: assignment.target.field.clone(),
                        },
                        expr.clone(),
                    ));
                }
                WindowAggregateExpr::Array(items) => {
                    pending.extend(items.iter().map(|item| &item.inner));
                }
            }
        }
    }
    SpannedNode {
        inner: Program {
            filter: None,
            set,
            invoke: Vec::new(),
        },
        span: (0..0).into(),
    }
}
