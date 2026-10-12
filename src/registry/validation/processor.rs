//! What each processor may read, what it must produce, and against which schema.
//!
//! Layer: decisions.
//!
//! - **Owns.** Processor input and output contracts: the declared schemas of every input, the
//!   collection policy, the filters and routes each output carries, and the rules specific to
//!   deduplicators, correlators, windows, inferencers, generators and WASM guests.
//! - **Depends on.** The schema rules, the expression walk, and the VM's type inference.
//! - **Must not know.** How a processor executes.
#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use ahash::{HashMap, HashSet, HashSetExt};
use error_stack::Report;
use meticulous::ResultExt;
use nervix_models::{
    Assignment, AssignmentTarget, CorrelationTimeoutAction, CreateCorrelator, CreateDeduplicator,
    CreateGenerator, CreateInferencer, CreateLookup, CreateSchema, CreateWindowProcessor,
    DomainName, Expression, FieldName, FlushPolicy, ModelIndex, ModelKind, ModelName, NodeRef,
    ProcessorOutput, ProcessorOutputs, RelayName, RouteConstruction, SchemaField, SchemaName,
    parse_duration_text,
};
use nervix_vm::{
    CompileBinding, CompileOptions, OutputMode, SemanticScopePolicy,
    compile_program_with_options_for_bindings_with_sensitivity,
    infer_set_expr_types_for_bindings_with_udfs, lower_finalized_output_filter,
    lower_generated_route, lower_route_construction, lower_set_only_route,
    lower_transforming_route,
    window::{WindowSketchConfig, lower_window_assignments, referenced_field_refs},
};
use petgraph::{graph::DiGraph, prelude::NodeIndex};

mod deduplicator_key;
mod sketch;

pub(in crate::registry) use deduplicator_key::deduplicator_key_types;

use crate::registry::{
    error::RegistryError,
    graph::{ActiveNode, EdgeKind, expect_kind},
    validation::{
        branching::relay_declared_branch_schema,
        expression::{
            LookupHashMapRewriteResult, collect_program_field_refs, lookup_hash_map_bindings,
            rewrite_lookup_hash_map_program,
        },
        materialized_state::referenced_materialized_stream_bindings,
        schema::{
            arrow_schema_for_internal_schema, ensure_equal_internal_schema,
            ensure_internal_schema_compatibility, readonly_binding_for_internal_schema,
            schema_sensitivity_for_internal_schema, writable_binding_for_internal_schema,
        },
        vm::{BRANCH_NAMESPACE, INNER_OUTPUT_NAMESPACE, udf_compile_options},
        window_route::validate_window_route_types,
        wire::schema_for_ack_model,
    },
};
#[derive(Clone, Copy)]
pub(in crate::registry) struct ModelValidationContext<'location, 'models> {
    pub(in crate::registry) domain: &'location DomainName,
    pub(in crate::registry) identifier: &'location ModelName,
    pub(in crate::registry) models: &'models ModelIndex,
}

pub(in crate::registry) fn processor_input_schemas<'inputs, 'models>(
    context: ModelValidationContext<'_, 'models>,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    inputs: &'inputs nervix_models::ProcessorInputs,
    relation: &str,
) -> Result<Vec<(&'inputs RelayName, &'models CreateSchema)>, Report<RegistryError>> {
    let ModelValidationContext {
        domain,
        identifier,
        models,
    } = context;
    ensure_input_collect_policy(domain, identifier, inputs.collect_policy.as_ref(), relation)?;
    if inputs.from.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{relation} requires at least one input relay"),
        }));
    }

    let mut seen = HashSet::new();
    let mut input_schemas = Vec::new();
    let mut reference_schema = None;
    for from_relay in inputs.relays() {
        if !seen.insert(from_relay.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{relation} input relay '{}' is declared more than once",
                    from_relay.as_str()
                ),
            }));
        }
        let input = expect_kind(
            domain,
            identifier,
            models,
            indices,
            from_relay,
            ModelKind::Relay,
        )?;
        graph.add_edge(input, source, EdgeKind::RequiredBy);
        graph.add_edge(input, source, EdgeKind::SendsTo);

        let input_schema = schema_for_ack_model(domain, identifier, models, from_relay)?;
        if let Some(reference_schema) = reference_schema {
            ensure_equal_internal_schema(
                domain,
                identifier,
                input_schema,
                reference_schema,
                relation,
            )?;
        } else {
            reference_schema = Some(input_schema);
        }
        input_schemas.push((from_relay, input_schema));
    }
    Ok(input_schemas)
}

fn ensure_input_collect_policy(
    domain: &DomainName,
    identifier: &ModelName,
    policy: Option<&nervix_models::InputCollectPolicy>,
    relation: &str,
) -> Result<(), Report<RegistryError>> {
    let Some(policy) = policy else {
        return Ok(());
    };
    let duration = parse_duration_text(&policy.collect_for).map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "invalid {relation} COLLECT FOR duration '{}': {error}",
                policy.collect_for
            ),
        })
    })?;
    if duration.is_zero() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{relation} COLLECT FOR duration must be greater than zero"),
        }));
    }
    if let Some(max_batch_size) = policy.max_batch_size.as_deref() {
        let parsed = max_batch_size.parse::<ubyte::ByteUnit>().map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "invalid {relation} COLLECT MAX BATCH SIZE '{max_batch_size}': {error}"
                ),
            })
        })?;
        if parsed.as_u64() == 0 {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("{relation} COLLECT MAX BATCH SIZE must be greater than zero"),
            }));
        }
    }
    Ok(())
}

pub(in crate::registry) fn validate_correlator_input_sides_do_not_overlap(
    domain: &DomainName,
    identifier: &ModelName,
    correlator: &CreateCorrelator,
) -> Result<(), Report<RegistryError>> {
    let mut left = HashSet::new();
    for relay in correlator.left.relays() {
        left.insert(relay.clone());
    }
    for relay in correlator.right.relays() {
        if left.contains(relay) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "correlator input relay '{}' is declared on both LEFT and RIGHT",
                    relay.as_str()
                ),
            }));
        }
    }
    Ok(())
}

pub(in crate::registry) fn processor_first_input_relay<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    inputs: &'a nervix_models::ProcessorInputs,
    relation: &str,
) -> Result<&'a RelayName, Report<RegistryError>> {
    inputs.from.first().ok_or_else(|| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{relation} requires at least one input relay"),
        })
    })
}

pub(in crate::registry) fn ensure_window_processor_output_schemas(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    window_processor: &CreateWindowProcessor,
    input_schemas: &[(&RelayName, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
) -> Result<(), Report<RegistryError>> {
    ensure_processor_outputs_declared(domain, identifier, &window_processor.output_routes)?;
    let mut sketch_demands = Vec::new();
    let mut sketch_route = None;
    for output in window_processor.output_routes.outputs() {
        let output_schema = schema_for_ack_model(domain, identifier, models, &output.relay)?;
        let sketches = validate_window_processor_output(
            domain,
            identifier,
            models,
            output,
            output_schema,
            input_schemas,
            branch_schema,
        )?;
        if !sketches.is_empty() && sketch_route.is_none() {
            sketch_route = Some(output.relay.clone());
        }
        sketch_demands.extend(sketches);
    }
    if let Some(route) = sketch_route {
        sketch::validate(
            domain,
            identifier,
            models,
            window_processor,
            &route,
            &sketch_demands,
        )?;
    }
    Ok(())
}

pub(in crate::registry) fn ensure_wasm_processor_output_schemas(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    processor: &nervix_models::CreateWasmProcessor,
    input_schemas: &[(&RelayName, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
) -> Result<(), Report<RegistryError>> {
    ensure_processor_outputs_declared(domain, identifier, &processor.output_routes)?;
    let mut output_relays = HashSet::new();
    for output in processor.output_routes.outputs() {
        if !output_relays.insert(output.relay.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "WASM processor output relay '{}' is declared more than once",
                    output.relay.as_str()
                ),
            }));
        }
        let output_schema = schema_for_ack_model(domain, identifier, models, &output.relay)?;
        let effective_schema = effective_wasm_output_filter_map_schema(
            domain,
            identifier,
            models,
            input_schemas,
            output,
            output_schema,
            branch_schema,
        )?;
        ProcessorOutputSchemaCompatibility::Compatible.ensure(
            domain,
            identifier,
            &effective_schema,
            output_schema,
            "wasm processor flow",
        )?;
    }

    Ok(())
}

fn effective_wasm_output_filter_map_schema(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    input_schemas: &[(&RelayName, &CreateSchema)],
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
) -> Result<CreateSchema, Report<RegistryError>> {
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = lower_generated_route(
        &output.construction,
        output_arrow_schema.as_ref(),
        output_arrow_schema.as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("WASM output route is invalid: {reason}"),
        })
    })?;
    if !parsed.inner.invoke.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "WASM processor TO clauses may use SET and WHERE, but not INVOKE".to_string(),
        }));
    }

    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let Some((_first_input_relay, _first_input_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "wasm processor input requires at least one input relay".to_string(),
        }));
    };
    let mut bindings = vec![
        readonly_binding_for_internal_schema("generated", output_schema),
        writable_binding_for_internal_schema("output", output_schema),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let mut local_namespaces = HashSet::new();
    local_namespaces.insert("generated".to_string());
    local_namespaces.insert("output".to_string());
    local_namespaces.insert(BRANCH_NAMESPACE.to_string());
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "FILTER-MAP",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        udf_compile_options(
            models,
            CompileOptions {
                output_mode: OutputMode::ExplicitOnly,
                ..CompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", message),
        })
    })?;
    Ok(output_schema.clone())
}

fn validate_window_processor_output(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    input_schemas: &[(&RelayName, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
) -> Result<Vec<WindowSketchConfig>, Report<RegistryError>> {
    let aggregate = lower_window_assignments(&output.construction).map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("window output '{}' is invalid: {reason}", output.relay),
        })
    })?;
    if aggregate.inner.demands().is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "window output '{}' must contain at least one aggregate function",
                output.relay
            ),
        }));
    }
    let Some((_input_relay, input_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "window processor requires at least one input relay".to_string(),
        }));
    };
    for assignment in &aggregate.inner.assignments {
        for field_ref in referenced_field_refs(&assignment.value.inner) {
            if field_ref.relay == "input"
                && !input_schema
                    .fields
                    .iter()
                    .any(|field| field.name.as_str() == field_ref.field)
            {
                return Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "window aggregate references unknown input field '{}.{}'",
                        field_ref.relay, field_ref.field
                    ),
                }));
            }
        }
    }
    let assigned_fields = aggregate
        .inner
        .assignments
        .iter()
        .map(|assignment| assignment.target.field.as_str())
        .collect::<HashSet<_>>();
    for assignment in &aggregate.inner.assignments {
        if output_schema
            .fields
            .iter()
            .any(|field| field.name.as_str() == assignment.target.field)
        {
            continue;
        }
        return Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "window aggregate target field '{}.{}' is not declared in output schema '{}'",
                output.relay, assignment.target.field, output_schema.name
            ),
        }));
    }
    for field in &output_schema.fields {
        if field.optional || assigned_fields.contains(field.name.as_str()) {
            continue;
        }
        return Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "window aggregate must assign required output field '{}.{}'",
                output.relay, field.name
            ),
        }));
    }
    validate_window_route_types(
        ModelValidationContext {
            domain,
            identifier,
            models,
        },
        output,
        &aggregate.inner,
        output_schema,
        input_schema,
        branch_schema,
    )?;
    validate_window_route_where(
        domain,
        identifier,
        models,
        output,
        output_schema,
        branch_schema,
    )?;
    Ok(aggregate
        .inner
        .demands()
        .iter()
        .filter_map(|demand| demand.sketch)
        .collect())
}

fn validate_window_route_where(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
) -> Result<(), Report<RegistryError>> {
    let Some(where_clause) = output.construction.where_clause.as_ref() else {
        return Ok(());
    };
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = lower_finalized_output_filter(where_clause, output_arrow_schema.as_ref())
        .map_err(|reason| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "window output '{}' WHERE is invalid: {reason}",
                    output.relay
                ),
            })
        })?;
    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut bindings = vec![writable_binding_for_internal_schema(
        "output",
        output_schema,
    )];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let local_namespaces = HashSet::from_iter(["output".to_string(), BRANCH_NAMESPACE.to_string()]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "window route WHERE",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        udf_compile_options(models, CompileOptions::default()),
    )
    .map_err(|error| {
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "window output '{}' WHERE compile failed: {}",
                output.relay, message
            ),
        })
    })?;
    Ok(())
}

pub(in crate::registry) fn parse_window_bound_duration(
    domain: &DomainName,
    identifier: &ModelName,
    bound_name: &str,
    duration: Option<&str>,
) -> Result<(), Report<RegistryError>> {
    let Some(duration) = duration else {
        return Ok(());
    };
    parse_duration_text(duration).map(|_| ()).map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("invalid window {bound_name} duration '{duration}': {error}"),
        })
    })
}

#[derive(Debug, Clone, Copy)]
pub(in crate::registry) enum ProcessorOutputSchemaCompatibility {
    Compatible,
    Equal,
}

impl ProcessorOutputSchemaCompatibility {
    fn ensure(
        self,
        domain: &DomainName,
        identifier: &ModelName,
        effective_schema: &CreateSchema,
        output_schema: &CreateSchema,
        relation: &str,
    ) -> Result<(), Report<RegistryError>> {
        match self {
            Self::Compatible => ensure_internal_schema_compatibility(
                domain,
                identifier,
                effective_schema,
                output_schema,
                relation,
            ),
            Self::Equal => ensure_equal_internal_schema(
                domain,
                identifier,
                effective_schema,
                output_schema,
                relation,
            ),
        }
    }
}

fn ensure_processor_outputs_declared(
    domain: &DomainName,
    identifier: &ModelName,
    outputs: &ProcessorOutputs,
) -> Result<(), Report<RegistryError>> {
    if outputs.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "processor must declare at least one TO destination".to_string(),
        }));
    }

    Ok(())
}

pub(in crate::registry) fn ensure_processor_output_flush_policies(
    domain: &DomainName,
    identifier: &ModelName,
    outputs: &ProcessorOutputs,
) -> Result<(), Report<RegistryError>> {
    for output in outputs.outputs() {
        let Some(policy) = output.flush_policy.as_ref() else {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "TO output '{}' must declare FLUSH EACH or FLUSH IMMEDIATE",
                    output.relay.as_str()
                ),
            }));
        };
        let FlushPolicy::Each {
            interval,
            max_batch_size,
        } = policy
        else {
            continue;
        };
        parse_duration_text(interval).map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "invalid TO output '{}' FLUSH EACH duration '{interval}': {error}",
                    output.relay.as_str()
                ),
            })
        })?;
        max_batch_size.parse::<ubyte::ByteUnit>().map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "invalid TO output '{}' MAX BATCH SIZE '{max_batch_size}': {error}",
                    output.relay.as_str()
                ),
            })
        })?;
    }
    Ok(())
}

pub(in crate::registry) fn add_processor_output_edges(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    outputs: &ProcessorOutputs,
) -> Result<(), Report<RegistryError>> {
    ensure_processor_outputs_declared(domain, identifier, outputs)?;
    for output in outputs.outputs() {
        let output_node = expect_kind(
            domain,
            identifier,
            models,
            indices,
            &output.relay,
            ModelKind::Relay,
        )?;
        graph.add_edge(output_node, source, EdgeKind::RequiredBy);
        graph.add_edge(source, output_node, EdgeKind::SendsTo);
    }
    Ok(())
}

pub(in crate::registry) fn validate_filter_where_for_internal_schemas(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    input_schemas: &[(&RelayName, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    filter_where: Option<&Expression>,
) -> Result<(), Report<RegistryError>> {
    let Some(filter_where) = filter_where else {
        return Ok(());
    };
    validate_where_program_for_internal_schemas(
        ModelValidationContext {
            domain,
            identifier,
            models,
        },
        input_schemas,
        branch_schema,
        filter_where,
        "FILTER WHERE",
        CompileOptions::default(),
    )
}

pub(in crate::registry) fn validate_from_where_for_internal_schemas(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    input_schemas: &[(&RelayName, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    from_where: &[nervix_models::ProcessorInputWhere],
) -> Result<(), Report<RegistryError>> {
    validate_scoped_from_where_for_internal_schemas(
        domain,
        identifier,
        models,
        input_schemas,
        branch_schema,
        from_where,
        "input",
    )
}

pub(in crate::registry) fn validate_scoped_from_where_for_internal_schemas(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    input_schemas: &[(&RelayName, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    from_where: &[nervix_models::ProcessorInputWhere],
    input_namespace: &'static str,
) -> Result<(), Report<RegistryError>> {
    let mut seen_relays = HashSet::new();
    for source_filter in from_where {
        if !seen_relays.insert(source_filter.relay.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "FROM WHERE declared more than once for input relay '{}'",
                    source_filter.relay.as_str()
                ),
            }));
        }
        let Some((relay, schema)) = input_schemas
            .iter()
            .find(|(relay, _schema)| **relay == source_filter.relay)
            .copied()
        else {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "FROM WHERE references unknown input relay '{}'",
                    source_filter.relay.as_str()
                ),
            }));
        };
        validate_where_program_for_scoped_internal_schemas(
            ModelValidationContext {
                domain,
                identifier,
                models,
            },
            &[(relay, schema)],
            branch_schema,
            &source_filter.where_clause,
            "FROM WHERE",
            CompileOptions::default(),
            input_namespace,
        )?;
    }
    Ok(())
}

pub(in crate::registry) fn validate_where_program_for_internal_schemas(
    context: ModelValidationContext<'_, '_>,
    input_schemas: &[(&RelayName, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    where_program: &Expression,
    clause_name: &str,
    compile_options: CompileOptions,
) -> Result<(), Report<RegistryError>> {
    validate_where_program_for_scoped_internal_schemas(
        context,
        input_schemas,
        branch_schema,
        where_program,
        clause_name,
        compile_options,
        "input",
    )
}

fn validate_where_program_for_scoped_internal_schemas(
    context: ModelValidationContext<'_, '_>,
    input_schemas: &[(&RelayName, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    where_program: &Expression,
    clause_name: &str,
    compile_options: CompileOptions,
    input_namespace: &'static str,
) -> Result<(), Report<RegistryError>> {
    let ModelValidationContext {
        domain,
        identifier,
        models,
    } = context;
    let parsed = lower_route_construction(
        &RouteConstruction {
            where_clause: Some(where_program.clone()),
            ..RouteConstruction::default()
        },
        SemanticScopePolicy::read_only(input_namespace),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{clause_name} is invalid: {reason}"),
        })
    })?;

    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let Some((_first_relay, first_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{clause_name} requires at least one input relay"),
        }));
    };
    let mut bindings = vec![
        CompileBinding::writable(
            input_namespace,
            arrow_schema_for_internal_schema(first_schema),
        )
        .with_sensitivity(schema_sensitivity_for_internal_schema(first_schema)),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let mut input_relay_names = input_schemas
        .iter()
        .map(|(relay, _schema)| relay.as_str().to_string())
        .collect::<HashSet<_>>();
    input_relay_names.insert(input_namespace.to_string());
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &input_relay_names,
        clause_name,
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(first_schema),
        schema_sensitivity_for_internal_schema(first_schema),
        bindings,
        udf_compile_options(models, compile_options),
    )
    .map_err(|error| {
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{clause_name} compile failed: {}", message),
        })
    })?;
    Ok(())
}

pub(in crate::registry) fn effective_processor_output_filter_map_schema(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    input_schemas: &[(&RelayName, &CreateSchema)],
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
) -> Result<CreateSchema, Report<RegistryError>> {
    let Some((_first_relay, first_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "processor output requires at least one input relay".to_string(),
        }));
    };
    let input_arrow_schema = arrow_schema_for_internal_schema(first_schema);
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = lower_transforming_route(
        &output.construction,
        input_arrow_schema.as_ref(),
        output_arrow_schema.as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("output route is invalid: {reason}"),
        })
    })?;
    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;

    let mut bindings = vec![
        readonly_binding_for_internal_schema("input", first_schema),
        writable_binding_for_internal_schema("output", output_schema),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let input_relay_names = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &input_relay_names,
        "FILTER-MAP",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(output_schema),
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        udf_compile_options(
            models,
            CompileOptions {
                output_mode: OutputMode::ExplicitOnly,
                ..CompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", message),
        })
    })?;

    Ok(output_schema.clone())
}

pub(in crate::registry) fn ensure_processor_output_schemas(
    context: ModelValidationContext<'_, '_>,
    outputs: &ProcessorOutputs,
    input_schemas: &[(&RelayName, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    relation: &str,
    compatibility: ProcessorOutputSchemaCompatibility,
) -> Result<(), Report<RegistryError>> {
    let ModelValidationContext {
        domain,
        identifier,
        models,
    } = context;
    ensure_processor_outputs_declared(domain, identifier, outputs)?;
    for output in outputs.outputs() {
        let output_schema = schema_for_ack_model(domain, identifier, models, &output.relay)?;
        let effective_schema = effective_processor_output_filter_map_schema(
            domain,
            identifier,
            models,
            input_schemas,
            output,
            output_schema,
            branch_schema,
        )?;
        compatibility.ensure(
            domain,
            identifier,
            &effective_schema,
            output_schema,
            relation,
        )?;
    }
    Ok(())
}

pub(in crate::registry) fn ensure_deduplicator_key_compiles(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    deduplicator: &CreateDeduplicator,
    input_schemas: &[(&RelayName, &CreateSchema)],
) -> Result<(), Report<RegistryError>> {
    let Some((_primary_relay, primary_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "deduplicator input requires at least one input relay".to_string(),
        }));
    };
    deduplicator_key_types(domain, identifier, models, deduplicator, primary_schema)?;
    Ok(())
}

pub(in crate::registry) fn validate_correlator(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    correlator: &CreateCorrelator,
    left_schemas: &[(&RelayName, &CreateSchema)],
    right_schemas: &[(&RelayName, &CreateSchema)],
) -> Result<(), Report<RegistryError>> {
    parse_duration_text(&correlator.max_time).map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "invalid correlator MAX TIME '{}': {error}",
                correlator.max_time
            ),
        })
    })?;
    ensure_processor_output_flush_policies(domain, identifier, &correlator.output_routes)?;

    validate_correlate_where_for_internal_schemas(
        domain,
        identifier,
        models,
        correlator,
        left_schemas,
        right_schemas,
    )?;

    let Some((_left_relay, left_schema)) = left_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator left timeout requires at least one input relay".to_string(),
        }));
    };
    let Some((_right_relay, right_schema)) = right_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator right timeout requires at least one input relay".to_string(),
        }));
    };
    validate_correlator_timeout_action(
        domain,
        identifier,
        models,
        left_schema,
        &correlator.timeout_policy.left,
        "correlator left timeout",
    )?;
    validate_correlator_timeout_action(
        domain,
        identifier,
        models,
        right_schema,
        &correlator.timeout_policy.right,
        "correlator right timeout",
    )
}

fn validate_correlate_where_for_internal_schemas(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    correlator: &CreateCorrelator,
    left_schemas: &[(&RelayName, &CreateSchema)],
    right_schemas: &[(&RelayName, &CreateSchema)],
) -> Result<(), Report<RegistryError>> {
    let parsed = lower_route_construction(
        &RouteConstruction {
            where_clause: Some(correlator.correlate_where.clone()),
            ..RouteConstruction::default()
        },
        SemanticScopePolicy::unavailable(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("CORRELATE WHERE is invalid: {reason}"),
        })
    })?;
    let Some((_first_relay, first_schema)) = left_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator left input requires at least one input relay".to_string(),
        }));
    };
    let Some((_right_relay, right_schema)) = right_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator right input requires at least one input relay".to_string(),
        }));
    };
    let bindings = vec![
        writable_binding_for_internal_schema("left", first_schema),
        readonly_binding_for_internal_schema("right", right_schema),
    ];

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(first_schema),
        schema_sensitivity_for_internal_schema(first_schema),
        bindings,
        udf_compile_options(models, CompileOptions::default()),
    )
    .map_err(|error| {
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("CORRELATE WHERE compile failed: {}", message),
        })
    })?;

    Ok(())
}

pub(in crate::registry) fn validate_correlator_output(
    context: ModelValidationContext<'_, '_>,
    left_schemas: &[(&RelayName, &CreateSchema)],
    right_schemas: &[(&RelayName, &CreateSchema)],
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
) -> Result<(), Report<RegistryError>> {
    let ModelValidationContext {
        domain,
        identifier,
        models,
    } = context;
    if output.construction.assignments.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "correlator TO output '{}' must declare SET assignments",
                output.relay.as_str()
            ),
        }));
    }
    let parsed = lower_route_construction(
        &output.construction,
        SemanticScopePolicy::write_only("output"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "correlator TO output '{}' is invalid: {}",
                output.relay.as_str(),
                reason
            ),
        })
    })?;
    if !parsed.inner.invoke.is_empty() || parsed.inner.set.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "correlator TO output '{}' must contain SET assignments and may contain WHERE",
                output.relay.as_str()
            ),
        }));
    }

    let Some((_left_relay, left_schema)) = left_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator left input requires at least one input relay".to_string(),
        }));
    };
    let Some((_right_relay, right_schema)) = right_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator right input requires at least one input relay".to_string(),
        }));
    };
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let mut bindings = vec![
        readonly_binding_for_internal_schema("left", left_schema),
        readonly_binding_for_internal_schema("right", right_schema),
        writable_binding_for_internal_schema("output", output_schema),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let local_namespaces = HashSet::from_iter([
        "left".to_string(),
        "right".to_string(),
        "output".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &parsed,
        &local_namespaces,
        "correlator output",
    )?);
    let compiled = compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema.clone(),
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        udf_compile_options(
            models,
            CompileOptions {
                output_mode: OutputMode::ExplicitOnly,
                ..CompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "correlator TO output '{}' compile failed: {}",
                output.relay, message
            ),
        })
    })?;

    for field in compiled.output_schema.fields() {
        let Some(target) = output_arrow_schema
            .fields()
            .iter()
            .find(|target| target.name() == field.name())
        else {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "correlator TO output '{}' assigns unknown field '{}.{}'",
                    output.relay.as_str(),
                    output.relay.as_str(),
                    field.name()
                ),
            }));
        };
        if target.data_type() != field.data_type() {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "correlator TO output '{}' field '{}' type mismatch: expression {:?}, schema \
                     {:?}",
                    output.relay.as_str(),
                    field.name(),
                    field.data_type(),
                    target.data_type()
                ),
            }));
        }
    }

    for target in output_arrow_schema.fields() {
        if !target.is_nullable()
            && !compiled
                .output_schema
                .fields()
                .iter()
                .any(|field| field.name() == target.name())
        {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "correlator TO output '{}' does not assign required field '{}.{}'",
                    output.relay.as_str(),
                    output.relay.as_str(),
                    target.name()
                ),
            }));
        }
    }

    Ok(())
}

fn validate_correlator_timeout_action(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    input_schema: &CreateSchema,
    action: &CorrelationTimeoutAction,
    relation: &str,
) -> Result<(), Report<RegistryError>> {
    let CorrelationTimeoutAction::SendTo { relay } = action else {
        return Ok(());
    };
    let target_schema = schema_for_ack_model(domain, identifier, models, relay)?;
    ensure_internal_schema_compatibility(domain, identifier, input_schema, target_schema, relation)
}

pub(in crate::registry) fn add_correlation_timeout_action_edges(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    action: &CorrelationTimeoutAction,
) -> Result<(), Report<RegistryError>> {
    let CorrelationTimeoutAction::SendTo { relay } = action else {
        return Ok(());
    };
    let relay = expect_kind(domain, identifier, models, indices, relay, ModelKind::Relay)?;
    graph.add_edge(relay, source, EdgeKind::RequiredBy);
    graph.add_edge(source, relay, EdgeKind::CorrelationTimeout);
    Ok(())
}

pub(in crate::registry) fn ensure_inferencer_input_mappings(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    processor: &CreateInferencer,
    input_schemas: &[(&RelayName, &CreateSchema)],
) -> Result<(), Report<RegistryError>> {
    let Some((_relay, input_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "inferencer requires at least one input relay".to_string(),
        }));
    };
    for mapping in &processor.inputs {
        let target = FieldName::parse("mapped_tensor").map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("invalid inferencer mapping target: {error}"),
            })
        })?;
        let parsed = lower_route_construction(
            &RouteConstruction {
                assignments: vec![Assignment {
                    target: AssignmentTarget::bare(target.clone()),
                    value: mapping.expression.clone(),
                }],
                ..RouteConstruction::default()
            },
            SemanticScopePolicy::read_write("input", "input"),
        )
        .map_err(|reason| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("inference input '{}' is invalid: {reason}", mapping.tensor),
            })
        })?;
        let inferred = infer_set_expr_types_for_bindings_with_udfs(
            &parsed,
            [writable_binding_for_internal_schema("input", input_schema)],
            udf_compile_options(models, CompileOptions::default()).udf_signatures,
        )
        .map_err(|error| {
            let message = error.current_context().message.clone();
            error.change_context(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "inference input '{}' compile failed: {}",
                    mapping.tensor, message
                ),
            })
        })?;
        let Some(inferred) = inferred.first() else {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("inference input '{}' produced no value", mapping.tensor),
            }));
        };
        let expected_type = mapping.schema.message_type().arrow_data_type();
        if inferred.data_type != expected_type || inferred.nullable {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "inference input '{}' requires {:?} non-null, found {:?}{}",
                    mapping.tensor,
                    expected_type,
                    inferred.data_type,
                    if inferred.nullable {
                        " nullable"
                    } else {
                        " non-null"
                    }
                ),
            }));
        }
    }

    Ok(())
}

pub(in crate::registry) fn validate_inferencer_output_filter_map(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
    processor: &CreateInferencer,
) -> Result<(), Report<RegistryError>> {
    let inner_output_schema = processor.inner_output_schema(domain, identifier)?;
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let generated_arrow_schema = arrow_schema_for_internal_schema(&inner_output_schema);
    let parsed = lower_generated_route(
        &output.construction,
        output_arrow_schema.as_ref(),
        generated_arrow_schema.as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("inferencer output route is invalid: {reason}"),
        })
    })?;
    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut bindings = vec![
        readonly_binding_for_internal_schema("generated", &inner_output_schema),
        writable_binding_for_internal_schema("output", output_schema),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let mut local_namespaces = HashSet::new();
    local_namespaces.insert("generated".to_string());
    local_namespaces.insert("output".to_string());
    local_namespaces.insert(BRANCH_NAMESPACE.to_string());
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "FILTER-MAP",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        udf_compile_options(
            models,
            CompileOptions {
                output_mode: OutputMode::ExplicitOnly,
                ..CompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", message),
        })
    })?;

    Ok(())
}

trait InferencerRegistrySchema {
    fn inner_output_schema(
        &self,
        domain: &DomainName,
        identifier: &ModelName,
    ) -> Result<CreateSchema, Report<RegistryError>>;
}

impl InferencerRegistrySchema for CreateInferencer {
    fn inner_output_schema(
        &self,
        domain: &DomainName,
        identifier: &ModelName,
    ) -> Result<CreateSchema, Report<RegistryError>> {
        let fields = self
            .output_schema
            .iter()
            .map(|declaration| {
                let name = FieldName::parse(&declaration.tensor).map_err(|error| {
                    Report::new(RegistryError::InvalidModel {
                        domain: domain.as_str().to_string(),
                        identifier: identifier.as_str().to_string(),
                        reason: format!(
                            "ONNX output tensor '{}' cannot be referenced as '{}.{}': {}",
                            declaration.tensor, INNER_OUTPUT_NAMESPACE, declaration.tensor, error
                        ),
                    })
                })?;
                Ok(SchemaField {
                    name,
                    ty: declaration.schema.message_type(),
                    optional: false,
                    sensitive: false,
                })
            })
            .collect::<Result<Vec<_>, Report<RegistryError>>>()?;
        Ok(CreateSchema {
            name: SchemaName::parse(INNER_OUTPUT_NAMESPACE)
                .assured("this is a constant literal that satisfies the identifier grammar"),
            fields,
        })
    }
}

pub(in crate::registry) fn validate_lookup_schema(
    domain: &DomainName,
    identifier: &ModelName,
    lookup: &CreateLookup,
    schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    for field in &schema.fields {
        if field.ty.contains_bytes() {
            return Err(Report::new(RegistryError::LookupFieldContainsBytes {
                domain: domain.clone(),
                lookup: identifier.clone(),
                field: field.name.clone(),
            }));
        }
    }
    if schema
        .fields
        .iter()
        .any(|field| field.name == lookup.key_field)
    {
        return Ok(());
    }

    Err(Report::new(RegistryError::IncompatibleSchema {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "LOOKUP KEY field '{}' is missing from schema '{}'",
            lookup.key_field.as_str(),
            schema.name.as_str()
        ),
    }))
}

pub(in crate::registry) fn validate_generator_output(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    generator: &CreateGenerator,
    output: &ProcessorOutput,
) -> Result<(), Report<RegistryError>> {
    let output_schema = schema_for_ack_model(domain, identifier, models, &output.relay)?;
    let source_schema =
        schema_for_ack_model(domain, identifier, models, &generator.materialized_relay)?;
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = lower_set_only_route(&output.construction, output_arrow_schema.as_ref()).map_err(
        |reason| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("generator output '{}' is invalid: {reason}", output.relay),
            })
        },
    )?;
    let allowed_state_namespace = format!("relay_state.{}", generator.materialized_relay);
    for (namespace, _field) in collect_program_field_refs(&parsed.inner) {
        if namespace.starts_with("relay_state.") && namespace != allowed_state_namespace {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "generator output '{}' references materialized state namespace '{namespace}', \
                     but only '{}' is declared",
                    output.relay, allowed_state_namespace
                ),
            }));
        }
    }

    let mut bindings = vec![
        writable_binding_for_internal_schema("output", output_schema),
        readonly_binding_for_internal_schema(&allowed_state_namespace, source_schema),
    ];
    if let Some(branch_schema) =
        relay_declared_branch_schema(domain, identifier, models, &generator.materialized_relay)?
    {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        udf_compile_options(
            models,
            CompileOptions {
                output_mode: OutputMode::ExplicitOnly,
                ..CompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        let message = error.current_context().message.clone();
        error.change_context(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "generator output '{}' compile failed: {}",
                output.relay, message
            ),
        })
    })?;
    Ok(())
}

#[cfg(test)]
#[path = "processor/bytes_tests.rs"]
mod bytes_tests;
#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod duration_tests;
#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests;
#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod vm_report_tests;
