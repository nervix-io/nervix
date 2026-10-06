//! The exact key types a deduplicator's `DEDUPLICATE ON` expressions produce.
//!
//! Layer: decisions.
//!
//! - **Owns.** Inferring, with the compiler the runtime keys its keyspace with, the type of every
//!   key expression over the deduplicator's input schema, in written order.
//! - **Depends on.** Deduplicator and schema models, the VM's set-expression type inference and the
//!   registry's schema bindings.
//! - **Must not know.** Runtime keyspaces, archive encodings or transport.

use error_stack::Report;
use nervix_models::{
    Assignment, AssignmentTarget, CreateDeduplicator, CreateSchema, DomainName, FieldName,
    ModelIndex, ModelName, RouteConstruction,
};
use nervix_vm::{
    CompileOptions, InferredSetField, SemanticScopePolicy,
    infer_set_expr_types_for_bindings_with_udfs, lower_route_construction,
};

use crate::registry::{
    error::RegistryError,
    validation::{schema::writable_binding_for_internal_schema, vm::udf_compile_options},
};

/// The exact type each `DEDUPLICATE ON` expression produces over `input_schema`, in written order,
/// as the compiler the runtime keys its keyspace with infers it.
pub(in crate::registry) fn deduplicator_key_types(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    deduplicator: &CreateDeduplicator,
    input_schema: &CreateSchema,
) -> Result<Vec<InferredSetField>, Report<RegistryError>> {
    if deduplicator.deduplicate_on.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "DEDUPLICATE ON requires at least one expression".to_string(),
        }));
    }
    let assignments = deduplicator
        .deduplicate_on
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            Ok(Assignment {
                target: AssignmentTarget::bare(
                    FieldName::parse(&format!("deduplicate_key_{index}")).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: format!("invalid deduplicate key target: {error}"),
                        })
                    })?,
                ),
                value: expression.clone(),
            })
        })
        .collect::<Result<Vec<_>, Report<RegistryError>>>()?;
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments,
            ..RouteConstruction::default()
        },
        SemanticScopePolicy::read_write("input", "input"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("DEDUPLICATE ON is invalid: {reason}"),
        })
    })?;
    let bindings = vec![writable_binding_for_internal_schema("input", input_schema)];
    let key_types = infer_set_expr_types_for_bindings_with_udfs(
        &parsed,
        bindings,
        udf_compile_options(models, CompileOptions::default()).udf_signatures,
    )
    .map_err(|error| {
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("DEDUPLICATE ON compile failed: {}", message),
        })
    })?;
    if key_types.len() != deduplicator.deduplicate_on.len() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "DEDUPLICATE ON inferred a different number of key fields".to_string(),
        }));
    }
    Ok(key_types)
}
