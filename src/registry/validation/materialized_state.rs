//! What a node may read from materialized state, and what it must declare to read it.
//!
//! Layer: decisions.
//!
//! - **Owns.** The dependencies a model declares on materialized relays, the typed constant a
//!   default binds, and the rule that a read names a relay the domain materializes.
//! - **Depends on.** The relay Models and the expressions that reference them.
//! - **Must not know.** How state is stored, snapshotted or evicted.

use std::sync::Arc as StdArc;

use ahash::{HashMap, HashSet};
use arrow_schema::Schema as ArrowSchema;
use error_stack::Report;
use nervix_models::{
    DomainName, Expression, MaterializedStateDependency, MaterializedStatePolicy, Model,
    ModelIndex, ModelKind, ModelName, NodeRef, RelayName, RouteConstruction,
};
use nervix_vm::{
    CompileBinding, CompileOptions, OutputMode, SchemaSensitivity,
    compile_program_with_options_for_bindings_with_sensitivity, lower_set_only_route,
};
use petgraph::{graph::DiGraph, prelude::NodeIndex};

use crate::registry::{
    error::RegistryError,
    graph::{ActiveNode, EdgeKind, expect_kind},
    validation::{
        expression::{collect_program_field_refs, visit_model_expressions},
        schema::{
            arrow_field_for_schema_field, arrow_schema_for_internal_schema,
            schema_sensitivity_for_internal_schema, writable_binding_for_internal_schema,
        },
        vm::{BRANCH_NAMESPACE, udf_compile_options},
        wire::schema_for_ack_model,
    },
};
pub(in crate::registry) fn model_materialized_state_dependencies(
    model: &Model,
) -> &[MaterializedStateDependency] {
    match model {
        Model::Reingestor(model) => &model.materialized_state,
        Model::Inferencer(model) => &model.materialized_state,
        Model::WasmProcessor(model) => &model.materialized_state,
        Model::Junction(model) => &model.materialized_state,
        Model::Deduplicator(model) => &model.materialized_state,
        Model::Correlator(model) => &model.materialized_state,
        Model::Reorderer(model) => &model.materialized_state,
        Model::WindowProcessor(model) => &model.materialized_state,
        Model::Emitter(model) => &model.materialized_state,
        _ => &[],
    }
}

pub(in crate::registry) fn add_materialized_state_dependency_edges(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    dependencies: &[MaterializedStateDependency],
) -> Result<(), Report<RegistryError>> {
    let mut declared = HashSet::default();
    for dependency in dependencies {
        if !declared.insert(dependency.relay.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "materialized-state relay '{}' is declared more than once",
                    dependency.relay
                ),
            }));
        }
        let relay = expect_kind(
            domain,
            identifier,
            models,
            indices,
            &dependency.relay,
            ModelKind::Relay,
        )?;
        ensure_stream_is_materialized(domain, identifier, models, &dependency.relay)?;
        validate_materialized_state_default(domain, identifier, models, dependency)?;
        graph.add_edge(relay, source, EdgeKind::RequiredBy);
    }
    Ok(())
}

fn validate_materialized_state_default(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    dependency: &MaterializedStateDependency,
) -> Result<(), Report<RegistryError>> {
    let MaterializedStatePolicy::Default(assignments) = &dependency.policy else {
        return Ok(());
    };
    let mut targets = HashSet::default();
    for assignment in assignments {
        if !targets.insert(assignment.target.field.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "materialized-state DEFAULT for '{}' assigns field '{}' more than once",
                    dependency.relay, assignment.target.field
                ),
            }));
        }
        let mut field_reference = None;
        assignment
            .value
            .visit_fields(&mut |field| field_reference = Some(field.clone()));
        if let Some(field) = field_reference {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "materialized-state DEFAULT for '{}' must be constant; field reference \
                     '{field:?}' is not allowed",
                    dependency.relay
                ),
            }));
        }
        if expression_contains_nondeterministic_or_side_effect_call(&assignment.value, models) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "materialized-state DEFAULT for '{}' must use deterministic side-effect-free \
                     expressions",
                    dependency.relay
                ),
            }));
        }
    }

    let schema = schema_for_ack_model(domain, identifier, models, &dependency.relay)?;
    let output_schema = arrow_schema_for_internal_schema(schema);
    let construction = RouteConstruction {
        assignments: assignments.clone(),
        ..RouteConstruction::default()
    };
    let parsed = lower_set_only_route(&construction, output_schema.as_ref()).map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "materialized-state DEFAULT for '{}' is invalid: {reason}",
                dependency.relay
            ),
        })
    })?;
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        schema_sensitivity_for_internal_schema(schema),
        vec![writable_binding_for_internal_schema("output", schema)],
        udf_compile_options(
            models,
            CompileOptions {
                output_mode: OutputMode::ExplicitOnly,
                ..CompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "materialized-state DEFAULT for '{}' is invalid: {}",
                dependency.relay, error.message
            ),
        })
    })?;
    Ok(())
}

fn expression_contains_nondeterministic_or_side_effect_call(
    expression: &Expression,
    models: &ModelIndex,
) -> bool {
    match expression {
        Expression::Literal(_) | Expression::Field(_) => false,
        Expression::Unary { expression, .. } | Expression::Cast { expression, .. } => {
            expression_contains_nondeterministic_or_side_effect_call(expression, models)
        }
        Expression::Binary { left, right, .. } => {
            expression_contains_nondeterministic_or_side_effect_call(left, models)
                || expression_contains_nondeterministic_or_side_effect_call(right, models)
        }
        Expression::Call {
            function,
            arguments,
        } => {
            matches!(
                function.as_str().to_ascii_lowercase().as_str(),
                "now" | "uuid_v4" | "uuid_v7" | "write_header"
            ) || arguments.iter().any(|argument| {
                expression_contains_nondeterministic_or_side_effect_call(argument, models)
            })
        }
        Expression::UdfCall {
            function,
            arguments,
        } => {
            models
                .get(&NodeRef::new(ModelKind::Udf, function.clone()))
                .is_none_or(|model| !matches!(model, Model::Udf(udf) if !udf.volatile))
                || arguments.iter().any(|argument| {
                    expression_contains_nondeterministic_or_side_effect_call(argument, models)
                })
        }
        Expression::Array(items) => items
            .iter()
            .any(|item| expression_contains_nondeterministic_or_side_effect_call(item, models)),
        Expression::If {
            condition,
            then_result,
            else_result,
        } => {
            expression_contains_nondeterministic_or_side_effect_call(condition, models)
                || expression_contains_nondeterministic_or_side_effect_call(then_result, models)
                || expression_contains_nondeterministic_or_side_effect_call(else_result, models)
        }
        Expression::Case {
            operand,
            branches,
            else_result,
        } => {
            operand.as_ref().is_some_and(|operand| {
                expression_contains_nondeterministic_or_side_effect_call(operand, models)
            }) || branches.iter().any(|branch| {
                expression_contains_nondeterministic_or_side_effect_call(&branch.when, models)
                    || expression_contains_nondeterministic_or_side_effect_call(
                        &branch.result,
                        models,
                    )
            }) || else_result.as_ref().is_some_and(|result| {
                expression_contains_nondeterministic_or_side_effect_call(result, models)
            })
        }
    }
}

pub(in crate::registry) fn validate_declared_materialized_state_references(
    domain: &DomainName,
    identifier: &ModelName,
    model: &Model,
    dependencies: &[MaterializedStateDependency],
) -> Result<(), Report<RegistryError>> {
    let declared = if let Model::Generator(generator) = model {
        HashSet::from_iter([generator.materialized_relay.clone()])
    } else {
        dependencies
            .iter()
            .map(|dependency| dependency.relay.clone())
            .collect()
    };
    let mut referenced = HashSet::default();
    visit_model_expressions(model, &mut |expression| {
        expression.visit_fields(&mut |field| {
            if let nervix_models::FieldScope::RelayState { relay } = &field.scope {
                referenced.insert(relay.clone());
            }
        });
    });
    for relay in referenced {
        if !declared.contains(&relay) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "materialized-state reference 'relay_state.{relay}' has no matching USING \
                     MATERIALIZED STATE declaration"
                ),
            }));
        }
    }
    Ok(())
}

pub(in crate::registry) fn ensure_stream_is_materialized(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    relay: &RelayName,
) -> Result<(), Report<RegistryError>> {
    let Some(Model::Relay(ack_model)) = models.get(&NodeRef::new(ModelKind::Relay, relay.clone()))
    else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("missing relay '{}'", relay.as_str()),
        }));
    };
    if ack_model.materialized_state.is_none() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "generator source relay '{}' must declare materialized state",
                relay.as_str()
            ),
        }));
    }
    Ok(())
}

pub(in crate::registry) fn referenced_materialized_stream_bindings(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    parsed: &nervix_vm::program::SpannedNode<nervix_vm::program::Program>,
    excluded_namespaces: &HashSet<String>,
    program_label: &str,
) -> Result<Vec<CompileBinding>, Report<RegistryError>> {
    let mut fields_by_stream = HashMap::<RelayName, HashSet<String>>::default();
    for (relay, field) in collect_program_field_refs(&parsed.inner) {
        if excluded_namespaces.contains(&relay) || relay == "metadata" || relay == BRANCH_NAMESPACE
        {
            continue;
        }
        let Some(relay_name) = relay.strip_prefix("relay_state.") else {
            continue;
        };
        let relay = RelayName::parse(relay_name).map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("invalid materialized-state relay '{relay_name}': {error}"),
            })
        })?;
        let Some(Model::Relay(ack_model)) =
            models.get(&NodeRef::new(ModelKind::Relay, relay.clone()))
        else {
            return Err(Report::new(RegistryError::MissingReference {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                expected_kind: ModelKind::Relay.as_str(),
                reference: relay.as_str().to_string(),
            }));
        };
        if ack_model.materialized_state.is_none() {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{} source relay '{}' must declare materialized state",
                    program_label,
                    relay.as_str()
                ),
            }));
        }
        fields_by_stream.entry(relay).or_default().insert(field);
    }

    let mut bindings = Vec::with_capacity(fields_by_stream.len());
    for (relay, fields) in fields_by_stream {
        let schema = schema_for_ack_model(domain, identifier, models, &relay)?;
        let projected_fields = schema
            .fields
            .iter()
            .filter(|field| fields.contains(field.name.as_str()))
            .map(arrow_field_for_schema_field)
            .collect::<Vec<_>>();
        let projected_sensitivity = SchemaSensitivity::from_sensitive_fields(
            schema
                .fields
                .iter()
                .filter(|field| field.sensitive && fields.contains(field.name.as_str()))
                .map(|field| field.name.as_str().to_string()),
        );
        bindings.push(
            CompileBinding::readonly(
                format!("relay_state.{}", relay.as_str()),
                StdArc::new(ArrowSchema::new(projected_fields)),
            )
            .with_sensitivity(projected_sensitivity),
        );
    }

    Ok(bindings)
}
