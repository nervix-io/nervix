//! Where a failed operation's structured error goes.
//!
//! Layer: decisions.
//!
//! - **Owns.** Validating every message-error policy a model declares: the relay it targets, the
//!   branch that relay must carry, and the schema the error record is constructed against.
//! - **Depends on.** The schema rules and the graph's edges.
//! - **Must not know.** How an error record is emitted at runtime.

use ahash::{HashMap, HashSet};
use error_stack::Report;
use nervix_models::{
    BranchName, BranchSelection, CreateSchema, DomainName, MessageErrorPolicy, Model, ModelIndex,
    ModelKind, ModelName, NodeRef, ProcessorOutputs, RouteConstruction,
};
use nervix_vm::{
    CompileBinding, CompileOptions, OutputMode, SemanticNamespaces,
    compile_program_with_options_for_bindings_with_sensitivity, lower_route_construction,
};
use petgraph::{graph::DiGraph, prelude::NodeIndex};

use crate::registry::{
    error::RegistryError,
    graph::{ActiveNode, EdgeKind, expect_kind},
    validation::{
        branching::{format_branch_name, relay_declared_branch},
        connector::ingest_source_supports_headers,
        materialized_state::referenced_materialized_stream_bindings,
        processor::processor_first_input_relay,
        schema::{
            all_optional_binding_for_internal_schema, arrow_schema_for_internal_schema,
            compile_binding_with_internal_schema, readonly_binding_for_internal_schema,
            schema_sensitivity_for_internal_schema, structured_message_error_arrow_schema,
        },
        vm::udf_compile_options,
        wire::{schema_for_ack_model, schema_for_codec_model},
    },
};
pub(in crate::registry) fn add_message_error_policy_edges(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    policy: &MessageErrorPolicy,
) -> Result<(), Report<RegistryError>> {
    let MessageErrorPolicy::Dlq { relay, .. } = policy else {
        return Ok(());
    };
    let dlq = expect_kind(domain, identifier, models, indices, relay, ModelKind::Relay)?;
    graph.add_edge(dlq, source, EdgeKind::RequiredBy);
    graph.add_edge(source, dlq, EdgeKind::MessageError);
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct MessageErrorSchemas<'a> {
    input: Option<&'a CreateSchema>,
    left: Option<&'a CreateSchema>,
    right: Option<&'a CreateSchema>,
    partial_output: Option<&'a CreateSchema>,
    allow_header_reads: bool,
}

pub(in crate::registry) fn validate_model_message_error_policies(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    model: &Model,
) -> Result<(), Report<RegistryError>> {
    let validate_outputs = |outputs: &ProcessorOutputs,
                            schemas: MessageErrorSchemas<'_>,
                            expected_branch: Option<&BranchName>| {
        for output in outputs.outputs() {
            let partial_output = schema_for_ack_model(domain, identifier, models, &output.relay)?;
            validate_message_error_policy(
                domain,
                identifier,
                models,
                &output.message_error_policy,
                MessageErrorSchemas {
                    partial_output: Some(partial_output),
                    ..schemas
                },
                expected_branch,
            )?;
        }
        Ok::<(), Report<RegistryError>>(())
    };

    match model {
        Model::Ingestor(node) => {
            let input =
                schema_for_codec_model(domain, identifier, models, &node.decode_using_codec)?;
            validate_outputs(
                &node.output_routes,
                MessageErrorSchemas {
                    input: Some(input),
                    allow_header_reads: ingest_source_supports_headers(&node.source),
                    ..MessageErrorSchemas::default()
                },
                None,
            )
        }
        Model::Reingestor(node) => {
            let relay = processor_first_input_relay(
                domain,
                identifier,
                &node.from,
                "reingestor error input",
            )?;
            let input = schema_for_ack_model(domain, identifier, models, relay)?;
            let branch = relay_declared_branch(domain, identifier, models, relay)?;
            validate_outputs(
                &node.output_routes,
                MessageErrorSchemas {
                    input: Some(input),
                    ..MessageErrorSchemas::default()
                },
                branch,
            )
        }
        Model::Generator(node) => validate_outputs(
            &node.output_routes,
            MessageErrorSchemas::default(),
            node.branched_by.branch(),
        ),
        Model::Inferencer(node) => {
            let relay = processor_first_input_relay(
                domain,
                identifier,
                &node.from,
                "inferencer error input",
            )?;
            let input = schema_for_ack_model(domain, identifier, models, relay)?;
            validate_outputs(
                &node.output_routes,
                MessageErrorSchemas {
                    input: Some(input),
                    ..MessageErrorSchemas::default()
                },
                node.branched_by.branch(),
            )
        }
        Model::WasmProcessor(node) => {
            let relay = processor_first_input_relay(
                domain,
                identifier,
                &node.from,
                "WASM processor error input",
            )?;
            let input = schema_for_ack_model(domain, identifier, models, relay)?;
            validate_outputs(
                &node.output_routes,
                MessageErrorSchemas {
                    input: Some(input),
                    ..MessageErrorSchemas::default()
                },
                node.branched_by.branch(),
            )
        }
        Model::Junction(node) => validate_transforming_processor_message_errors(
            domain,
            identifier,
            models,
            &node.from,
            &node.output_routes,
            &node.branched_by,
            "junction error input",
        ),
        Model::Deduplicator(node) => validate_transforming_processor_message_errors(
            domain,
            identifier,
            models,
            &node.from,
            &node.output_routes,
            &node.branched_by,
            "deduplicator error input",
        ),
        Model::Reorderer(node) => validate_transforming_processor_message_errors(
            domain,
            identifier,
            models,
            &node.from,
            &node.output_routes,
            &node.branched_by,
            "reorderer error input",
        ),
        Model::WindowProcessor(node) => validate_outputs(
            &node.output_routes,
            MessageErrorSchemas::default(),
            node.branched_by.branch(),
        ),
        Model::Correlator(node) => {
            let left_relay = processor_first_input_relay(
                domain,
                identifier,
                &node.left,
                "correlator left error input",
            )?;
            let right_relay = processor_first_input_relay(
                domain,
                identifier,
                &node.right,
                "correlator right error input",
            )?;
            validate_outputs(
                &node.output_routes,
                MessageErrorSchemas {
                    left: Some(schema_for_ack_model(
                        domain, identifier, models, left_relay,
                    )?),
                    right: Some(schema_for_ack_model(
                        domain,
                        identifier,
                        models,
                        right_relay,
                    )?),
                    ..MessageErrorSchemas::default()
                },
                node.branched_by.branch(),
            )
        }
        Model::Emitter(node) => {
            let partial_output = node
                .encode_using_codec
                .as_ref()
                .map(|codec| schema_for_codec_model(domain, identifier, models, codec))
                .transpose()?;
            for input_relay in node.from.relays() {
                let input = schema_for_ack_model(domain, identifier, models, input_relay)?;
                let branch = relay_declared_branch(domain, identifier, models, input_relay)?;
                validate_message_error_policy(
                    domain,
                    identifier,
                    models,
                    &node.error_policies.message,
                    MessageErrorSchemas {
                        input: Some(input),
                        partial_output,
                        ..MessageErrorSchemas::default()
                    },
                    branch,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_transforming_processor_message_errors(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    inputs: &nervix_models::ProcessorInputs,
    outputs: &ProcessorOutputs,
    branch: &BranchSelection,
    input_label: &str,
) -> Result<(), Report<RegistryError>> {
    let relay = processor_first_input_relay(domain, identifier, inputs, input_label)?;
    let input = schema_for_ack_model(domain, identifier, models, relay)?;
    for output in outputs.outputs() {
        validate_message_error_policy(
            domain,
            identifier,
            models,
            &output.message_error_policy,
            MessageErrorSchemas {
                input: Some(input),
                partial_output: Some(schema_for_ack_model(
                    domain,
                    identifier,
                    models,
                    &output.relay,
                )?),
                ..MessageErrorSchemas::default()
            },
            branch.branch(),
        )?;
    }
    Ok(())
}

fn validate_message_error_policy(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    policy: &MessageErrorPolicy,
    schemas: MessageErrorSchemas<'_>,
    expected_branch: Option<&BranchName>,
) -> Result<(), Report<RegistryError>> {
    let MessageErrorPolicy::Dlq { relay, assignments } = policy else {
        return Ok(());
    };
    let actual_branch = relay_declared_branch(domain, identifier, models, relay)?;
    if actual_branch != expected_branch {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "message-error relay '{}' uses branch {}, expected {}",
                relay,
                format_branch_name(actual_branch),
                format_branch_name(expected_branch),
            ),
        }));
    }

    let error_output = schema_for_ack_model(domain, identifier, models, relay)?;
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments: assignments.clone(),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("error_output", "error_output"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("message-error SET is invalid: {reason}"),
        })
    })?;
    let mut bindings = vec![compile_binding_with_internal_schema(
        CompileBinding::writable(
            "error_output",
            arrow_schema_for_internal_schema(error_output),
        ),
        error_output,
    )];
    if let Some(input) = schemas.input {
        bindings.push(readonly_binding_for_internal_schema("input", input));
    }
    if let Some(left) = schemas.left {
        bindings.push(readonly_binding_for_internal_schema("left", left));
    }
    if let Some(right) = schemas.right {
        bindings.push(readonly_binding_for_internal_schema("right", right));
    }
    if let Some(partial_output) = schemas.partial_output {
        bindings.push(all_optional_binding_for_internal_schema(
            "partial_output",
            partial_output,
        ));
    }
    bindings.push(CompileBinding::readonly(
        "error",
        structured_message_error_arrow_schema(),
    ));
    let local_namespaces = HashSet::from_iter([
        "error_output".to_string(),
        "input".to_string(),
        "left".to_string(),
        "right".to_string(),
        "partial_output".to_string(),
        "error".to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &parsed,
        &local_namespaces,
        "message-error SET",
    )?);
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(error_output),
        schema_sensitivity_for_internal_schema(error_output),
        bindings,
        udf_compile_options(
            models,
            CompileOptions {
                output_mode: OutputMode::ExplicitOnly,
                allow_header_reads: schemas.allow_header_reads,
                ..CompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("message-error SET compile failed: {}", error.message),
        })
    })?;
    Ok(())
}

pub(in crate::registry) fn add_output_message_error_policy_edges(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    outputs: &ProcessorOutputs,
) -> Result<(), Report<RegistryError>> {
    for output in outputs.outputs() {
        add_message_error_policy_edges(
            domain,
            identifier,
            models,
            indices,
            graph,
            source,
            &output.message_error_policy,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nervix_models::{Assignment, AssignmentTarget, ParseAsType, SchemaField};

    use super::*;
    use crate::registry::{
        storage::Registry,
        test_fixtures::{
            branch, branch_schema, explicitly_unbranched_relay, named, relay_branched_by, schema,
            temp_db_path, unbranched_correlator,
        },
    };

    #[test]
    fn correlator_message_error_set_uses_structured_scopes_and_all_optional_partial_output() {
        fn error_schema() -> Model {
            Model::Schema(CreateSchema {
                name: named("error_schema"),
                fields: vec![
                    SchemaField {
                        name: named("reference"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                    SchemaField {
                        name: named("fields"),
                        ty: ParseAsType::Vec {
                            element: Box::new(ParseAsType::String),
                        },
                        optional: false,
                        sensitive: false,
                    },
                    SchemaField {
                        name: named("attempted"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                ],
            })
        }

        let models_with_policy = |assignment_source: &str| {
            let Model::Correlator(mut correlator) = unbranched_correlator(
                "match_events",
                "left_events",
                "right_events",
                "matched_events",
            ) else {
                unreachable!("helper must return correlator")
            };
            correlator.output_routes.routes[0].message_error_policy = MessageErrorPolicy::Dlq {
                relay: named("correlator_errors"),
                assignments: vec![
                    Assignment {
                        target: AssignmentTarget::bare(named("reference")),
                        value: nervix_nspl::parse_expression("error.reference")
                            .expect("error reference must parse"),
                    },
                    Assignment {
                        target: AssignmentTarget::bare(named("fields")),
                        value: nervix_nspl::parse_expression("error.fields")
                            .expect("error fields must parse"),
                    },
                    Assignment {
                        target: AssignmentTarget::bare(named("attempted")),
                        value: nervix_nspl::parse_expression(assignment_source)
                            .expect("attempted value must parse"),
                    },
                ],
            };
            vec![
                schema("event_schema"),
                error_schema(),
                explicitly_unbranched_relay("left_events", "event_schema"),
                explicitly_unbranched_relay("right_events", "event_schema"),
                explicitly_unbranched_relay("matched_events", "event_schema"),
                explicitly_unbranched_relay("correlator_errors", "error_schema"),
                Model::Correlator(correlator),
            ]
        };

        let valid_path = temp_db_path();
        Registry::open(&valid_path)
            .expect("registry should open")
            .apply_batch(
                &DomainName::parse("default").expect("valid domain"),
                models_with_policy("coalesce(partial_output.value, 'missing')"),
            )
            .expect("structured correlator error construction should validate");
        let _ = fs::remove_dir_all(valid_path);

        let invalid_path = temp_db_path();
        let error = Registry::open(&invalid_path)
            .expect("registry should open")
            .apply_batch(
                &DomainName::parse("default").expect("valid domain"),
                models_with_policy("input.value"),
            )
            .expect_err("correlator error construction must not expose input");
        assert!(format!("{error:?}").contains("input"));
        let _ = fs::remove_dir_all(invalid_path);
    }

    #[test]
    fn message_error_relay_requires_the_exact_named_branch() {
        let Model::Correlator(mut correlator) = unbranched_correlator(
            "match_events",
            "left_events",
            "right_events",
            "matched_events",
        ) else {
            unreachable!("helper must return correlator")
        };
        correlator.branched_by = BranchSelection::branched_by(named("event_branch"));
        correlator.output_routes.routes[0].message_error_policy = MessageErrorPolicy::Dlq {
            relay: named("correlator_errors"),
            assignments: vec![Assignment {
                target: AssignmentTarget::bare(named("value")),
                value: nervix_nspl::parse_expression("left.value")
                    .expect("correlator error input must parse"),
            }],
        };
        let path = temp_db_path();
        let error = Registry::open(&path)
            .expect("registry should open")
            .apply_batch(
                &DomainName::parse("default").expect("valid domain"),
                vec![
                    schema("event_schema"),
                    schema("error_schema"),
                    branch_schema("branch_key", &["tenant"]),
                    branch("event_branch", "branch_key"),
                    branch("error_branch", "branch_key"),
                    relay_branched_by("left_events", "event_schema", "event_branch"),
                    relay_branched_by("right_events", "event_schema", "event_branch"),
                    relay_branched_by("matched_events", "event_schema", "event_branch"),
                    relay_branched_by("correlator_errors", "error_schema", "error_branch"),
                    Model::Correlator(correlator),
                ],
            )
            .expect_err("structurally equal but differently named branches must be rejected");

        let rendered = format!("{error:?}");
        assert!(rendered.contains("message-error relay 'correlator_errors'"));
        assert!(rendered.contains("error_branch"));
        assert!(rendered.contains("event_branch"));
        let _ = fs::remove_dir_all(path);
    }
}
