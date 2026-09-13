//! What each processor may read, what it must produce, and against which schema.
//!
//! Layer: decisions.
//!
//! - **Owns.** Processor input and output contracts: the declared schemas of every input, the
//!   collection policy, the filters and routes each output carries, and the rules specific to
//!   deduplicators, correlators, windows, inferencers, generators and WASM guests.
//! - **Depends on.** The schema rules, the expression walk, and the VM's type inference.
//! - **Must not know.** How a processor executes.
use ahash::{HashMap, HashSet, HashSetExt};
use error_stack::Report;
use meticulous::ResultExt;
use nervix_models::{
    Assignment, AssignmentTarget, CorrelationTimeoutAction, CreateCorrelator, CreateDeduplicator,
    CreateGenerator, CreateInferencer, CreateLookup, CreateSchema, CreateWindowProcessor,
    DomainName, Expression, FieldName, FlushPolicy, ModelIndex, ModelKind, ModelName, NodeRef,
    ProcessorOutput, ProcessorOutputs, RelayName, RouteConstruction, SchemaField, SchemaName,
};
use nervix_vm::{
    CompileBinding, CompileOptions, OutputMode, SemanticNamespaces,
    compile_program_with_options_for_bindings_with_sensitivity,
    infer_set_expr_types_for_bindings_with_udfs, lower_finalized_output_filter,
    lower_generated_route, lower_route_construction, lower_set_only_route,
    lower_transforming_route,
    window::{lower_window_assignments, referenced_field_refs},
};
use petgraph::{graph::DiGraph, prelude::NodeIndex};

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
            arrow_data_type_for_parse_as, arrow_schema_for_internal_schema,
            ensure_equal_internal_schema, ensure_internal_schema_compatibility,
            readonly_binding_for_internal_schema, schema_sensitivity_for_internal_schema,
            writable_binding_for_internal_schema,
        },
        vm::{BRANCH_NAMESPACE, INNER_OUTPUT_NAMESPACE, udf_compile_options},
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
    let duration = humantime::parse_duration(&policy.collect_for).map_err(|error| {
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
    for output in window_processor.output_routes.outputs() {
        let output_schema = schema_for_ack_model(domain, identifier, models, &output.relay)?;
        validate_window_processor_output(
            domain,
            identifier,
            models,
            output,
            output_schema,
            input_schemas,
            branch_schema,
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
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", error.message),
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
) -> Result<(), Report<RegistryError>> {
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
    validate_window_route_where(
        domain,
        identifier,
        models,
        output,
        output_schema,
        branch_schema,
    )?;
    Ok(())
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
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "window output '{}' WHERE compile failed: {}",
                output.relay, error.message
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
    humantime::parse_duration(duration)
        .map(|_| ())
        .map_err(|error| {
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
        humantime::parse_duration(interval).map_err(|error| {
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
        SemanticNamespaces::new(input_namespace, "__invalid_filter_target"),
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
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{clause_name} compile failed: {}", error.message),
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
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", error.message),
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
        SemanticNamespaces::new("input", "input"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("DEDUPLICATE ON is invalid: {reason}"),
        })
    })?;
    let bindings = vec![writable_binding_for_internal_schema(
        "input",
        primary_schema,
    )];
    let key_types = infer_set_expr_types_for_bindings_with_udfs(
        &parsed,
        bindings,
        udf_compile_options(models, CompileOptions::default()).udf_signatures,
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("DEDUPLICATE ON compile failed: {}", error.message),
        })
    })?;
    if key_types.len() != deduplicator.deduplicate_on.len() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "DEDUPLICATE ON inferred a different number of key fields".to_string(),
        }));
    }
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
    humantime::parse_duration(&correlator.max_time).map_err(|error| {
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
        SemanticNamespaces::new(
            "__invalid_correlator_bare_read",
            "__invalid_correlator_target",
        ),
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
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("CORRELATE WHERE compile failed: {}", error.message),
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
        SemanticNamespaces::new("__invalid_correlator_bare_read", "output"),
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
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "correlator TO output '{}' compile failed: {}",
                output.relay.as_str(),
                error.message
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
            SemanticNamespaces::new("input", "input"),
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
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "inference input '{}' compile failed: {}",
                    mapping.tensor, error.message
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
        let expected_type = arrow_data_type_for_parse_as(&mapping.schema.message_type());
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
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", error.message),
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

pub(in crate::registry) fn ensure_lookup_key_field_exists(
    domain: &DomainName,
    identifier: &ModelName,
    lookup: &CreateLookup,
    schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
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
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "generator output '{}' compile failed: {}",
                output.relay, error.message
            ),
        })
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nervix_models::{
        AckMode, BranchSelection, CreateJunction, CreateReingestor, CreateWireSchema,
        InputCollectPolicy, JsonType, MessageErrorPolicy, Model, OutputBranch, ParseAsType,
        ProcessorInputs, WireSchemaField, WireSchemaName,
    };

    use super::*;
    use crate::registry::{
        storage::Registry,
        test_fixtures::{
            branch_for_relay, branch_name_for_relay, branch_schema, client_model, codec,
            deduplicator, example_graph_models, explicitly_unbranched_relay, ingestor_with_params,
            junction, named, processor, relay, relay_branched_by_relay_branch, relay_branched_like,
            schema, temp_db_path, unbranched_transforming_outputs, window_processor,
            with_inherit_all, with_output_branch,
        },
    };

    #[test]
    fn apply_batch_accepts_inferencer_generated_output() {
        let (domain, models) = example_graph_models(
            "inferencer generated output schema",
            r#"
            CREATE SCHEMA features (
              tenant STRING,
              vector ARRAY<F32, 2>
            );

            CREATE SCHEMA scored (
              score ARRAY<F32, 1>
            );

            CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
            CREATE RELAY features SCHEMA features BRANCHED BY by_tenant_branch;
            CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant_branch;
            CREATE BRANCH by_tenant_branch
              SCHEMA tenant_branch TTL 5m;

            CREATE INFERENCER score_model
              FROM features
              USING RESOURCE fraud_model VERSION 1
              FILE 'models/simple_score.onnx'
              INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector }
              OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] }
              BRANCHED BY by_tenant_branch
              TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("inferencer should construct output from immutable generated state");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_mixed_inferencer_execution_modes() {
        let (domain, models) = example_graph_models(
            "mixed inferencer execution modes",
            r#"
            CREATE SCHEMA features ( vector ARRAY<F32, 2> );
            CREATE SCHEMA scored ( score ARRAY<F32, 1> );
            CREATE RELAY features SCHEMA features UNBRANCHED;
            CREATE RELAY scored SCHEMA scored UNBRANCHED;
            CREATE INFERENCER score_model
              FROM features
              USING RESOURCE fraud_model FILE 'models/simple_score.onnx'
              INPUTS { "features" DENSE TENSOR<F32>[BATCH, 2] = input.vector }
              OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] }
              UNBRANCHED
              TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let error = registry
            .apply_batch(&domain, models)
            .expect_err("mixed inferencer execution modes must fail");
        assert!(error.to_string().contains("mixes batched and per-message"));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_multiple_batch_axes_in_one_tensor() {
        let (domain, models) = example_graph_models(
            "multiple inferencer batch axes",
            r#"
            CREATE SCHEMA features ( vector ARRAY<F32, 2> );
            CREATE SCHEMA scored ( score ARRAY<F32, 1> );
            CREATE RELAY features SCHEMA features UNBRANCHED;
            CREATE RELAY scored SCHEMA scored UNBRANCHED;
            CREATE INFERENCER score_model
              FROM features
              USING RESOURCE fraud_model FILE 'models/simple_score.onnx'
              INPUTS { "features" DENSE TENSOR<F32>[BATCH, BATCH, 2] = input.vector }
              OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[BATCH, 1] }
              UNBRANCHED
              TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let error = registry
            .apply_batch(&domain, models)
            .expect_err("multiple BATCH axes must fail");
        assert!(
            error
                .to_string()
                .contains("contains more than one BATCH axis")
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_window_processor_generated_output_schema() {
        let (domain, models) = example_graph_models(
            "window processor generated output schema",
            r#"
            CREATE SCHEMA metric (
              tenant STRING,
              latency I64
            );

            CREATE SCHEMA metric_summary (
              tenant STRING,
              sample_count I64
            );

            CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
            CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant_branch;
            CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
            CREATE BRANCH by_tenant_branch
              SCHEMA tenant_branch TTL 5m;

            CREATE WINDOW PROCESSOR latency_window
              FROM metrics
              WIDTH 2 MESSAGES
              STEP 2 MESSAGES
              BRANCHED BY by_tenant_branch
              TO metric_summaries
                SET tenant = FIRST(input.tenant),
                    sample_count = COUNT(input.latency)
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("window aggregate outputs should define non-input output fields");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_window_processor_unassigned_output_field() {
        let (domain, models) = example_graph_models(
            "window processor unassigned output field",
            r#"
            CREATE SCHEMA metric (
              tenant STRING,
              latency U64
            );

            CREATE SCHEMA metric_summary (
              tenant STRING,
              total_latency U64
            );

            CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
            CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant_branch;
            CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
            CREATE BRANCH by_tenant_branch
              SCHEMA tenant_branch TTL 5m;

            CREATE WINDOW PROCESSOR latency_window
              FROM metrics
              WIDTH 10s DURATION
              STEP 5s DURATION
              BRANCHED BY by_tenant_branch
              TO metric_summaries
                SET total_latency = SUM(input.latency)
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("window aggregate should reject unassigned output fields");
        assert!(
            format!("{err}").contains(
                "window aggregate must assign required output field 'metric_summaries.tenant'"
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_window_output_route_filter_on_generated_output() {
        let (domain, models) = example_graph_models(
            "window processor output route filter",
            r#"
            CREATE SCHEMA metric (
              tenant STRING,
              latency I64
            );

            CREATE SCHEMA metric_summary (
              tenant STRING,
              sample_count I64,
              total_latency I64
            );

            CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
            CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant_branch;
            CREATE RELAY high_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
            CREATE RELAY low_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
            CREATE BRANCH by_tenant_branch
              SCHEMA tenant_branch TTL 5m;

            CREATE WINDOW PROCESSOR first_window
              FROM metrics
              WIDTH 2 MESSAGES
              STEP 2 MESSAGES
              BRANCHED BY by_tenant_branch
              TO high_summaries
                SET tenant = FIRST(input.tenant),
                    sample_count = COUNT(input.latency),
                    total_latency = SUM(input.latency)
                WHERE total_latency >= 100
                ON MESSAGE ERROR LOG
              TO low_summaries
                SET tenant = FIRST(input.tenant),
                    sample_count = COUNT(input.latency),
                    total_latency = SUM(input.latency)
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("window output route predicates should read generated output fields");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_window_output_route_filter_on_live_input() {
        let (domain, models) = example_graph_models(
            "window processor output input filter",
            r#"
            CREATE SCHEMA metric (
              tenant STRING,
              latency I64
            );

            CREATE SCHEMA metric_summary (
              tenant STRING,
              total_latency I64
            );

            CREATE RELAY metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY metric_summaries SCHEMA metric_summary UNBRANCHED;

            CREATE WINDOW PROCESSOR latency_window
              FROM metrics
              WIDTH 2 MESSAGES
              STEP 2 MESSAGES
              UNBRANCHED
              TO metric_summaries
                SET tenant = FIRST(input.tenant),
                    total_latency = SUM(input.latency)
                WHERE input.latency > 0
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let error = registry
            .apply_batch(&domain, models)
            .expect_err("window route WHERE must not expose live input");
        assert!(
            format!("{error}").contains("input is unavailable after set-only output finalization"),
            "unexpected error: {error}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_wasm_output_routes_on_generated_output() {
        let (domain, models) = example_graph_models(
            "wasm processor output routes",
            r#"
            CREATE SCHEMA metric (
              value I64,
              source STRING
            );

            CREATE SCHEMA projected_metric (
              value I64,
              source STRING OPTIONAL,
              bucket STRING
            );

            CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY even_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY projected_metrics SCHEMA projected_metric UNBRANCHED;

            CREATE WASM PROCESSOR route_guest_output
              FROM raw_metrics
              FILTER WHERE input.value >= 0
              USING RESOURCE wasm_filter VERSION 1
              FILE 'processors/filter_even.wasm'
              MAX FUEL 1000000000 MAX MEMORY 64MiB
              UNBRANCHED
              TO even_metrics
                SET value = value, source = source
                WHERE value >= 10
                ON MESSAGE ERROR LOG
              TO projected_metrics
                SET value = value,
                    source = source,
                    bucket = lower(bucket)
                ON MESSAGE ERROR LOG
              ON GLOBAL ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("wasm output routes should read guest output fields");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_processor_from_where() {
        let (domain, models) = example_graph_models(
            "processor source where",
            r#"
            CREATE SCHEMA metric (
              value I64,
              source STRING
            );

            CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY deduped_metrics SCHEMA metric UNBRANCHED;

            CREATE DEDUPLICATOR dedup_metrics
              FROM raw_metrics WHERE input.value >= 0
              DEDUPLICATE ON input.source
              MAX TIME 10m
              UNBRANCHED
              TO deduped_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("source WHERE should validate against the input relay");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_non_boolean_processor_from_where() {
        let (domain, models) = example_graph_models(
            "processor non-boolean source where",
            r#"
            CREATE SCHEMA metric (
              value I64,
              source STRING
            );

            CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY deduped_metrics SCHEMA metric UNBRANCHED;

            CREATE DEDUPLICATOR dedup_metrics
              FROM raw_metrics WHERE input.value
              DEDUPLICATE ON input.source
              MAX TIME 10m
              UNBRANCHED
              TO deduped_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("non-boolean source WHERE must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err:#}").contains("FROM WHERE compile failed"),
            "unexpected error: {err:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_processor_from_where_unavailable_scope() {
        let (domain, models) = example_graph_models(
            "processor source where other relay",
            r#"
            CREATE SCHEMA metric (
              value I64,
              source STRING
            );

            CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY deduped_metrics SCHEMA metric UNBRANCHED;

            CREATE DEDUPLICATOR dedup_metrics
              FROM raw_metrics WHERE branch.value >= 0
              DEDUPLICATE ON input.source
              MAX TIME 10m
              UNBRANCHED
              TO deduped_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("source WHERE cannot reference a branch during unbranched execution");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("FROM WHERE") && rendered.contains("branch"),
            "unexpected error: {rendered}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_duplicate_wasm_output_route() {
        let (domain, models) = example_graph_models(
            "wasm processor duplicate output route",
            r#"
            CREATE SCHEMA metric (
              value I64
            );

            CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY projected_metrics SCHEMA metric UNBRANCHED;

            CREATE WASM PROCESSOR route_guest_output
              FROM raw_metrics
              USING RESOURCE wasm_filter VERSION 1
              FILE 'processors/filter_even.wasm'
              MAX FUEL 1000000000 MAX MEMORY 64MiB
              UNBRANCHED
              TO projected_metrics SET value = value ON MESSAGE ERROR LOG
              TO projected_metrics SET value = value WHERE value >= 0 ON MESSAGE ERROR LOG
              ON GLOBAL ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("duplicate WASM output routes must be rejected");
        assert!(
            format!("{err}").contains(
                "WASM processor output relay 'projected_metrics' is declared more than once"
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_unconditional_processor_output_route() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain =
            DomainName::parse("unconditional_processor_output_route").expect("domain should parse");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    explicitly_unbranched_relay("raw_events", "event_schema"),
                    explicitly_unbranched_relay("projected_events", "event_schema"),
                    Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_events"),
                        from: ProcessorInputs::single(named("raw_events")),
                        output_routes: with_inherit_all(ProcessorOutputs::single(named(
                            "projected_events",
                        )))
                        .with_flush_policy(FlushPolicy::Immediate),
                        branched_by: BranchSelection::unbranched(),
                        deduplicate_on: vec![
                            nervix_nspl::parse_expression("input.value")
                                .expect("deduplicate expression must parse"),
                        ],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect("unconditional output route should be accepted");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_zero_input_collection_boundaries() {
        let cases = [
            (
                InputCollectPolicy {
                    collect_for: "0s".to_string(),
                    max_batch_size: None,
                },
                "COLLECT FOR duration must be greater than zero",
            ),
            (
                InputCollectPolicy {
                    collect_for: "1s".to_string(),
                    max_batch_size: Some("0B".to_string()),
                },
                "COLLECT MAX BATCH SIZE must be greater than zero",
            ),
        ];

        for (index, (collect_policy, expected)) in cases.into_iter().enumerate() {
            let path = temp_db_path();
            let registry = Registry::open(&path).expect("registry should open");
            let domain = DomainName::parse(&format!("invalid_input_collection_{index}"))
                .expect("domain should parse");
            let mut inputs = ProcessorInputs::single(named("raw_events"));
            inputs.collect_policy = Some(collect_policy);
            let junction = Model::Junction(CreateJunction {
                name: named("collect_events"),
                from: inputs,
                output_routes: unbranched_transforming_outputs("collected_events"),
                branched_by: BranchSelection::unbranched(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            });

            let error = registry
                .apply_batch(
                    &domain,
                    vec![
                        schema("event_schema"),
                        explicitly_unbranched_relay("raw_events", "event_schema"),
                        explicitly_unbranched_relay("collected_events", "event_schema"),
                        junction,
                    ],
                )
                .expect_err("zero input collection boundaries must be rejected");
            assert!(
                format!("{error:#}").contains(expected),
                "unexpected validation error: {error:#}"
            );

            let _ = fs::remove_dir_all(path);
        }
    }

    #[test]
    fn apply_batch_rejects_incompatible_deduplicator_stream_schemas() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    Model::Schema(CreateSchema {
                        name: SchemaName::parse("wide_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: FieldName::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: FieldName::parse("extra").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay_branched_like("wide", "wide_schema", "notifications"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    processor("project", "notifications", "wide"),
                ],
            )
            .expect_err("deduplicator schema mismatch should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_multiple_deduplicator_inputs_with_different_schemas() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    Model::Schema(CreateSchema {
                        name: named("wide_schema"),
                        fields: vec![
                            SchemaField {
                                name: named("value"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: named("extra"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    explicitly_unbranched_relay("notifications_a", "event_schema"),
                    explicitly_unbranched_relay("notifications_b", "wide_schema"),
                    explicitly_unbranched_relay("deduped", "event_schema"),
                    Model::Deduplicator(CreateDeduplicator {
                        name: named("dedup_notifications"),
                        from: ProcessorInputs::new(
                            vec![named("notifications_a"), named("notifications_b")],
                            Vec::new(),
                        ),
                        output_routes: (ProcessorOutputs::single(named("deduped")))
                            .with_flush_policy(FlushPolicy::Immediate),
                        branched_by: BranchSelection::unbranched(),
                        deduplicate_on: vec![
                            nervix_nspl::parse_expression("input.value")
                                .expect("deduplicate expression must parse"),
                        ],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect_err("deduplicator input schema mismatch should fail");

        let message = format!("{err:#}");
        assert!(
            message.contains("deduplicator input"),
            "unexpected error: {message}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incompatible_junction_stream_schemas() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    Model::Schema(CreateSchema {
                        name: SchemaName::parse("wide_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: FieldName::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: FieldName::parse("extra").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    relay("notifications_a", "event_schema"),
                    relay("notifications_b", "wide_schema"),
                    relay("merged", "event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications_a", "value_branch"),
                    junction(
                        "join_streams",
                        &["notifications_a", "notifications_b"],
                        "merged",
                    ),
                ],
            )
            .expect_err("junction schema mismatch should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_deduplicator_field_missing_from_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay("deduped", "event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    deduplicator(
                        "dedup",
                        "notifications",
                        "deduped",
                        "notifications.transaction_id",
                        "10m",
                    ),
                ],
            )
            .expect_err("missing dedup field should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(format!("{err}").contains("DEDUPLICATE ON compile failed"));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_correlate_where_non_boolean_predicate() {
        let (domain, models) = example_graph_models(
            "correlator non-boolean predicate",
            r#"
            CREATE SCHEMA event (
              value STRING
            );

            CREATE SCHEMA correlated_event (
              value STRING
            );

            CREATE RELAY left_events SCHEMA event UNBRANCHED;
            CREATE RELAY right_events SCHEMA event UNBRANCHED;
            CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;

            CREATE CORRELATOR correlate_events
              LEFT FROM left_events
              RIGHT FROM right_events
              CORRELATE WHERE lower(left.value)
              MATCH EARLIEST
              MAX TIME 5s
              ON CORRELATION TIMEOUT DROP, DROP
              UNBRANCHED
              TO correlated_events
                SET value = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("non-boolean CORRELATE WHERE must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err:#}").contains("CORRELATE WHERE compile failed"),
            "unexpected error: {err:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_correlator_side_scoped_from_where() {
        let (domain, models) = example_graph_models(
            "correlator side source predicates",
            r#"
            CREATE SCHEMA left_event (
              value STRING,
              marker I64
            );

            CREATE SCHEMA right_event (
              value STRING,
              active BOOL
            );

            CREATE SCHEMA correlated_event (
              value STRING
            );

            CREATE RELAY left_events SCHEMA left_event UNBRANCHED;
            CREATE RELAY right_events SCHEMA right_event UNBRANCHED;
            CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;

            CREATE CORRELATOR correlate_events
              LEFT FROM left_events WHERE left.marker > 0
              RIGHT FROM right_events WHERE right.active
              CORRELATE WHERE left.value = right.value
              MATCH EARLIEST
              MAX TIME 5s
              ON CORRELATION TIMEOUT DROP, DROP
              UNBRANCHED
              TO correlated_events
                SET value = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("side source predicates should use their correlator side scope");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_correlate_where_non_input_namespace() {
        let (domain, models) = example_graph_models(
            "correlator non-input namespace",
            r#"
            CREATE SCHEMA tenant_branch (
              tenant STRING
            );

            CREATE SCHEMA event (
              tenant STRING,
              value STRING
            );

            CREATE SCHEMA correlated_event (
              value STRING
            );

            CREATE RELAY left_events SCHEMA event BRANCHED BY by_tenant_branch;
            CREATE RELAY right_events SCHEMA event BRANCHED BY by_tenant_branch;
            CREATE RELAY correlated_events SCHEMA correlated_event BRANCHED BY by_tenant_branch;
            CREATE BRANCH by_tenant_branch
              SCHEMA tenant_branch TTL 5m;

            CREATE CORRELATOR correlate_events
              LEFT FROM left_events
              RIGHT FROM right_events
              CORRELATE WHERE input.tenant = left.tenant
              MATCH EARLIEST
              MAX TIME 5s
              ON CORRELATION TIMEOUT DROP, DROP
              BRANCHED BY by_tenant_branch
              TO correlated_events
                SET value = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("non-input CORRELATE WHERE namespace must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("CORRELATE WHERE compile failed") && rendered.contains("input"),
            "unexpected error: {rendered}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_validates_each_correlator_output_against_its_destination_schema() {
        let (domain, models) = example_graph_models(
            "correlator destination schemas",
            r#"
            CREATE SCHEMA event (
              value STRING
            );

            CREATE SCHEMA correlated_event (
              value STRING
            );

            CREATE SCHEMA correlation_count (
              count I64
            );

            CREATE RELAY left_events SCHEMA event UNBRANCHED;
            CREATE RELAY right_events SCHEMA event UNBRANCHED;
            CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;
            CREATE RELAY correlation_counts SCHEMA correlation_count UNBRANCHED;

            CREATE CORRELATOR correlate_events
              LEFT FROM left_events
              RIGHT FROM right_events
              CORRELATE WHERE left.value = right.value
              MATCH EARLIEST
              MAX TIME 5s
              ON CORRELATION TIMEOUT DROP, DROP
              UNBRANCHED
              TO correlated_events
                SET value = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG
              TO correlation_counts
                SET count = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("each correlator route must use its own destination schema");

        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("correlator TO output 'correlation_counts' compile failed"),
            "unexpected error: {rendered}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_correlator_left_side_schema_mismatch() {
        let (domain, models) = example_graph_models(
            "correlator left schema mismatch",
            r#"
            CREATE SCHEMA left_event (
              value STRING
            );

            CREATE SCHEMA other_left_event (
              value I64
            );

            CREATE SCHEMA right_event (
              value STRING
            );

            CREATE SCHEMA correlated_event (
              value STRING
            );

            CREATE RELAY left_events SCHEMA left_event UNBRANCHED;
            CREATE RELAY other_left_events SCHEMA other_left_event UNBRANCHED;
            CREATE RELAY right_events SCHEMA right_event UNBRANCHED;
            CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;

            CREATE CORRELATOR correlate_events
              LEFT FROM left_events, other_left_events
              RIGHT FROM right_events
              CORRELATE WHERE left.value = right.value
              MATCH EARLIEST
              MAX TIME 5s
              ON CORRELATION TIMEOUT DROP, DROP
              UNBRANCHED
              TO correlated_events
                SET value = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("same-side correlator schema mismatch must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err:#}").contains("correlator left input requires equal internal schemas"),
            "unexpected error: {err:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_window_message_target() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay_branched_like("summaries", "event_schema", "notifications"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    window_processor(
                        "window",
                        "notifications",
                        "summaries",
                        "SET message.value = COUNT(input.value)",
                    ),
                ],
            )
            .expect_err("message is not a window output target");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("window SET targets must be bare or output.<field>"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_window_aggregate_argument_outside_input() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay_branched_like("summaries", "event_schema", "notifications"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    window_processor(
                        "window",
                        "notifications",
                        "summaries",
                        "SET value = COUNT(output.value)",
                    ),
                ],
            )
            .expect_err("aggregate arguments must read the original input");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("window aggregate arguments may read only input fields"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_output_predicate_missing_from_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: SchemaName::parse("event_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: FieldName::parse("tenant").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: FieldName::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::WireJsonSchema(CreateWireSchema {
                        name: WireSchemaName::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![
                            WireSchemaField {
                                name: FieldName::parse("tenant").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                            WireSchemaField {
                                name: FieldName::parse("value").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    }),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay_branched_by_relay_branch("errors", "event_schema"),
                    relay_branched_like("info", "event_schema", "errors"),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("notifications", "tenant_branch"),
                    branch_for_relay("errors", "tenant_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant"],
                    ),
                    Model::Reingestor(CreateReingestor {
                        name: named("route_logs"),
                        from: ProcessorInputs::single(named("notifications")),
                        output_routes: with_output_branch(
                            with_inherit_all(ProcessorOutputs::new(vec![
                                ProcessorOutput {
                                    relay: named("errors"),
                                    construction: nervix_nspl::parse_route_construction(
                                        r#"WHERE input.missing = "error""#,
                                    )
                                    .expect("route construction must parse"),
                                    flush_policy: None,
                                    message_error_policy: MessageErrorPolicy::Log,
                                    branch: None,
                                },
                                ProcessorOutput::new(named("info")),
                            ]))
                            .with_flush_policy(FlushPolicy::Each {
                                interval: "100ms".to_string(),
                                max_batch_size: "1MiB".to_string(),
                            }),
                            OutputBranch::BranchedBy {
                                branch: branch_name_for_relay("notifications"),
                                assignments: Vec::new(),
                            },
                        ),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect_err("reingestor output predicate on missing field should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("unknown input field 'missing'"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }
}
