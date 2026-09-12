//! Which branch each relay carries, and which branch each node may read and write.
//!
//! Layer: decisions.
//!
//! - **Owns.** Inferring a relay's branching from what writes to it, the branch a processor or
//!   generator is declared in, and the rule that every ordinary input shares one exact branch.
//! - **Depends on.** The branch and schema Models and the graph's edges.
//! - **Must not know.** How a branch is materialized at runtime.

use std::time::Duration;

use ahash::{HashMap, HashSet};
use error_stack::Report;
use meticulous::OptionExt;
use nervix_models::{
    BranchName, BranchSelection, CorrelationTimeoutAction, CreateBranch, CreateSchema, DomainName,
    FieldName, Model, ModelIndex, ModelKind, ModelName, NodeRef, OutputBranch, ProcessorOutput,
    ProcessorOutputs, RelayName, SchemaName,
};
use nervix_vm::{
    CompileOptions, OutputMode, compile_program_with_options_for_bindings_with_sensitivity,
    lower_branch_construction,
};
use petgraph::{graph::DiGraph, prelude::NodeIndex};
use sorted_vec::SortedSet;

use crate::registry::{
    error::RegistryError,
    graph::{ActiveNode, EdgeKind, expect_kind},
    validation::{
        expression::{
            LookupHashMapRewriteResult, lookup_hash_map_bindings, rewrite_lookup_hash_map_program,
        },
        materialized_state::referenced_materialized_stream_bindings,
        schema::{
            arrow_schema_for_internal_schema, readonly_binding_for_internal_schema,
            schema_sensitivity_for_internal_schema, writable_binding_for_internal_schema,
        },
        vm::{BRANCH_NAMESPACE, udf_compile_options},
    },
};
pub(in crate::registry) fn validate_branch_model(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    branch: &CreateBranch,
) -> Result<(), Report<RegistryError>> {
    parse_branch_ttl(domain, identifier, &branch.ttl)?;
    ensure_branch_schema_exists(domain, identifier, models, branch)
}

fn parse_branch_ttl(
    domain: &DomainName,
    identifier: &ModelName,
    ttl: &str,
) -> Result<Duration, Report<RegistryError>> {
    humantime::parse_duration(ttl).map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("invalid branch ttl '{ttl}': {error}"),
        })
    })
}

fn ensure_branch_schema_exists(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    branch: &CreateBranch,
) -> Result<(), Report<RegistryError>> {
    let Some(Model::Schema(_)) =
        models.get(&NodeRef::new(ModelKind::Schema, branch.schema.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Schema.as_str(),
            reference: branch.schema.as_str().to_string(),
        }));
    };

    Ok(())
}

pub(in crate::registry) fn relay_declared_branch<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    models: &'a ModelIndex,
    relay: &RelayName,
) -> Result<Option<&'a BranchName>, Report<RegistryError>> {
    let Some(Model::Relay(relay_model)) =
        models.get(&NodeRef::new(ModelKind::Relay, relay.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Relay.as_str(),
            reference: relay.as_str().to_string(),
        }));
    };
    Ok(relay_model.branching.branch())
}

pub(in crate::registry) fn relay_declared_branch_schema<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    models: &'a ModelIndex,
    relay: &RelayName,
) -> Result<Option<&'a CreateSchema>, Report<RegistryError>> {
    let Some(Model::Relay(relay_model)) =
        models.get(&NodeRef::new(ModelKind::Relay, relay.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Relay.as_str(),
            reference: relay.as_str().to_string(),
        }));
    };
    let Some(branch_ref) = relay_model.branching.branch() else {
        return Ok(None);
    };
    let branch = branch_model(domain, identifier, models, branch_ref)?;
    let Some(Model::Schema(schema)) =
        models.get(&NodeRef::new(ModelKind::Schema, branch.schema.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Schema.as_str(),
            reference: branch.schema.as_str().to_string(),
        }));
    };
    Ok(Some(schema))
}

pub(in crate::registry) fn ensure_output_branch(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    output: &ProcessorOutput,
    input_schema: &CreateSchema,
    output_schema: &CreateSchema,
    incoming_branch: Option<&BranchName>,
) -> Result<(), Report<RegistryError>> {
    let target_branch = relay_declared_branch(domain, identifier, models, &output.relay)?;
    let Some(branch_action) = output.branch.as_ref() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "TO output '{}' must declare BRANCHED BY or UNBRANCHED",
                output.relay.as_str()
            ),
        }));
    };

    let (branch_ref, assignments) = match branch_action {
        OutputBranch::Unbranched => {
            if let Some(target_branch) = target_branch {
                return Err(Report::new(RegistryError::IncompatibleSchema {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "TO output '{}' is BRANCHED BY '{}', but the route declares UNBRANCHED",
                        output.relay.as_str(),
                        target_branch.as_str()
                    ),
                }));
            }
            return Ok(());
        }
        OutputBranch::BranchedBy {
            branch,
            assignments,
        } => (branch, assignments),
    };

    if target_branch != Some(branch_ref) {
        return Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "TO output '{}' must use its exact declared branch '{}'",
                output.relay.as_str(),
                format_branch_name(target_branch)
            ),
        }));
    }

    if incoming_branch == Some(branch_ref) {
        if assignments.is_empty() {
            return Ok(());
        }
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "TO output '{}' preserves branch '{}' and cannot construct a new key",
                output.relay.as_str(),
                branch_ref.as_str()
            ),
        }));
    }

    let branch = branch_model(domain, identifier, models, branch_ref)?;
    let branch_schema = schema_model(domain, identifier, models, &branch.schema)?;
    let parsed = lower_branch_construction(
        assignments,
        arrow_schema_for_internal_schema(branch_schema).as_ref(),
        arrow_schema_for_internal_schema(output_schema).as_ref(),
        arrow_schema_for_internal_schema(input_schema).as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("branch construction is invalid: {reason}"),
        })
    })?;
    let original_parsed = parsed.clone();
    let LookupHashMapRewriteResult {
        program: parsed,
        fields: lookup_fields,
    } = rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut bindings = vec![
        readonly_binding_for_internal_schema("input", input_schema),
        readonly_binding_for_internal_schema("output", output_schema),
        readonly_binding_for_internal_schema("message", output_schema),
        writable_binding_for_internal_schema(BRANCH_NAMESPACE, branch_schema),
    ];
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        "message".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "branch SET",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(branch_schema),
        schema_sensitivity_for_internal_schema(branch_schema),
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
            reason: format!("branch SET compile failed: {}", error.message),
        })
    })?;
    Ok(())
}

pub(in crate::registry) fn infer_stream_branchings(
    domain: &DomainName,
    models: &ModelIndex,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
) -> Result<(), Report<RegistryError>> {
    let producer_ids = SortedSet::from_unsorted(
        models
            .iter()
            .filter_map(|(key, model)| {
                matches!(
                    model,
                    Model::Generator(_)
                        | Model::Inferencer(_)
                        | Model::Ingestor(_)
                        | Model::Reingestor(_)
                        | Model::Deduplicator(_)
                        | Model::Correlator(_)
                        | Model::Junction(_)
                        | Model::WindowProcessor(_)
                )
                .then_some(key.identifier.clone())
            })
            .collect::<Vec<_>>(),
    )
    .into_vec();

    let mut changed = true;
    while changed {
        changed = false;

        for producer_id in &producer_ids {
            let mut model = None;
            for kind in [
                ModelKind::Generator,
                ModelKind::Inferencer,
                ModelKind::WasmProcessor,
                ModelKind::Ingestor,
                ModelKind::Reingestor,
                ModelKind::Deduplicator,
                ModelKind::Junction,
                ModelKind::WindowProcessor,
            ] {
                if let Some(candidate) = models.get(&NodeRef::new(kind, producer_id.clone())) {
                    model = Some(candidate);
                    break;
                }
            }
            let Some(model) = model else {
                continue;
            };

            let proposed = match model {
                Model::Generator(generator) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &generator.branched_by,
                    )?;
                    Some(
                        generator
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect::<Vec<_>>(),
                    )
                }
                Model::Inferencer(processor) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &processor.branched_by,
                    )?;
                    Some(
                        processor
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                Model::WasmProcessor(processor) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &processor.branched_by,
                    )?;
                    Some(
                        processor
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                Model::Ingestor(ingestor) => Some(resolved_output_branches(
                    domain,
                    producer_id,
                    models,
                    &ingestor.output_routes,
                )?),
                Model::Reingestor(reingestor) => Some(resolved_output_branches(
                    domain,
                    producer_id,
                    models,
                    &reingestor.output_routes,
                )?),
                Model::Deduplicator(deduplicator) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &deduplicator.branched_by,
                    )?;
                    Some(
                        deduplicator
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                Model::Correlator(correlator) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &correlator.branched_by,
                    )?;
                    Some(
                        correlator
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                Model::Junction(junction) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &junction.branched_by,
                    )?;
                    Some(
                        junction
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                Model::WindowProcessor(window_processor) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &window_processor.branched_by,
                    )?;
                    Some(
                        window_processor
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                _ => None,
            };

            let Some(proposed_targets) = proposed else {
                continue;
            };

            for (target_relay, branching) in proposed_targets {
                changed |= assign_stream_branching(
                    domain,
                    producer_id,
                    &target_relay,
                    branching,
                    indices,
                    graph,
                )?;
            }
        }
    }

    Ok(())
}

pub(in crate::registry) fn validate_processing_branch_selections(
    domain: &DomainName,
    models: &ModelIndex,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &DiGraph<ActiveNode, EdgeKind>,
) -> Result<(), Report<RegistryError>> {
    // Normal processors are branch-preserving: they must run under an explicit
    // concrete relay branch. Only REINGESTOR may change branching and
    // only EMITTER may fan in across branches, so every processor source checked
    // here must already have an inferred branch shape.
    for (key, model) in models {
        match model {
            Model::Generator(generator) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "generator",
                    models,
                    indices,
                    graph,
                };
                check.matches_relay(&generator.branched_by, &generator.materialized_relay)?;
                check.matches_outputs(&generator.branched_by, &generator.output_routes)?;
            }
            Model::Inferencer(processor) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "inferencer",
                    models,
                    indices,
                    graph,
                };
                for from_relay in processor.from.relays() {
                    check.matches_relay(&processor.branched_by, from_relay)?;
                }
                for dependency in &processor.materialized_state {
                    check.matches_relay(&processor.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&processor.branched_by, &processor.output_routes)?;
            }
            Model::WasmProcessor(processor) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "wasm processor",
                    models,
                    indices,
                    graph,
                };
                for from_relay in processor.from.relays() {
                    check.matches_relay(&processor.branched_by, from_relay)?;
                }
                for dependency in &processor.materialized_state {
                    check.matches_relay(&processor.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&processor.branched_by, &processor.output_routes)?;
            }
            Model::Deduplicator(deduplicator) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "deduplicator",
                    models,
                    indices,
                    graph,
                };
                for from_relay in deduplicator.from.relays() {
                    check.matches_relay(&deduplicator.branched_by, from_relay)?;
                }
                for dependency in &deduplicator.materialized_state {
                    check.matches_relay(&deduplicator.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&deduplicator.branched_by, &deduplicator.output_routes)?;
            }
            Model::Correlator(correlator) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "correlator",
                    models,
                    indices,
                    graph,
                };
                for relay in correlator.left.relays() {
                    check.matches_relay(&correlator.branched_by, relay)?;
                }
                for relay in correlator.right.relays() {
                    check.matches_relay(&correlator.branched_by, relay)?;
                }
                if let CorrelationTimeoutAction::SendTo { relay } = &correlator.timeout_policy.left
                {
                    check.matches_relay(&correlator.branched_by, relay)?;
                }
                if let CorrelationTimeoutAction::SendTo { relay } = &correlator.timeout_policy.right
                {
                    check.matches_relay(&correlator.branched_by, relay)?;
                }
                for dependency in &correlator.materialized_state {
                    check.matches_relay(&correlator.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&correlator.branched_by, &correlator.output_routes)?;
            }
            Model::Reorderer(reorderer) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "reorderer",
                    models,
                    indices,
                    graph,
                };
                for from_relay in reorderer.from.relays() {
                    check.matches_relay(&reorderer.branched_by, from_relay)?;
                }
                for dependency in &reorderer.materialized_state {
                    check.matches_relay(&reorderer.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&reorderer.branched_by, &reorderer.output_routes)?;
            }
            Model::Reingestor(reingestor) => {
                for from_relay in reingestor.from.relays() {
                    ensure_processing_source_branching(
                        domain,
                        &key.identifier,
                        "reingestor",
                        from_relay,
                        indices,
                        graph,
                    )?;
                }
                if let Some(from_relay) = reingestor.from.first() {
                    for dependency in &reingestor.materialized_state {
                        ensure_relays_have_same_branch(
                            domain,
                            &key.identifier,
                            "reingestor materialized state",
                            from_relay,
                            &dependency.relay,
                            indices,
                            graph,
                        )?;
                    }
                }
            }
            Model::WindowProcessor(window_processor) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "window processor",
                    models,
                    indices,
                    graph,
                };
                for from_relay in window_processor.from.relays() {
                    check.matches_relay(&window_processor.branched_by, from_relay)?;
                }
                for dependency in &window_processor.materialized_state {
                    check.matches_relay(&window_processor.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(
                    &window_processor.branched_by,
                    &window_processor.output_routes,
                )?;
            }
            Model::Junction(junction) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "junction",
                    models,
                    indices,
                    graph,
                };
                for from_relay in junction.from.relays() {
                    check.matches_relay(&junction.branched_by, from_relay)?;
                }
                for dependency in &junction.materialized_state {
                    check.matches_relay(&junction.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&junction.branched_by, &junction.output_routes)?;
            }
            Model::Emitter(emitter) => {
                for input_relay in emitter.from.relays() {
                    for dependency in &emitter.materialized_state {
                        ensure_relays_have_same_branch(
                            domain,
                            &key.identifier,
                            "emitter materialized state",
                            input_relay,
                            &dependency.relay,
                            indices,
                            graph,
                        )?;
                    }
                }
            }
            _ => {}
        }
    }

    Ok(())
}

struct ProcessorBranchingCheck<'a> {
    domain: &'a DomainName,
    identifier: &'a ModelName,
    model_kind: &'a str,
    models: &'a ModelIndex,
    indices: &'a HashMap<NodeRef, NodeIndex>,
    graph: &'a DiGraph<ActiveNode, EdgeKind>,
}

impl ProcessorBranchingCheck<'_> {
    fn matches_outputs(
        &self,
        branched_by: &BranchSelection,
        outputs: &ProcessorOutputs,
    ) -> Result<(), Report<RegistryError>> {
        for output in outputs.outputs() {
            self.matches_relay(branched_by, &output.relay)?;
        }
        Ok(())
    }

    fn matches_relay(
        &self,
        branched_by: &BranchSelection,
        relay: &RelayName,
    ) -> Result<(), Report<RegistryError>> {
        let declared =
            resolved_branch_selection(self.domain, self.identifier, self.models, branched_by)?;
        let relay_branching =
            if let Some(relay_branching) = relay_branching(self.indices, self.graph, relay) {
                relay_branching
            } else if declared.is_empty() {
                return Ok(());
            } else {
                return Err(Report::new(RegistryError::IncompatibleSchema {
                    domain: self.domain.as_str().to_string(),
                    identifier: self.identifier.as_str().to_string(),
                    reason: format!(
                        "{} '{}' requires relay '{}' to have branch fields ({})",
                        self.model_kind,
                        self.identifier.as_str(),
                        relay.as_str(),
                        format_branched_by(&declared.fields),
                    ),
                }));
            };

        if relay_branching.fields.is_empty() && !declared.fields.is_empty() {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: self.domain.as_str().to_string(),
                identifier: self.identifier.as_str().to_string(),
                reason: format!(
                    "{} '{}' requires relay '{}' to have branch fields ({})",
                    self.model_kind,
                    self.identifier.as_str(),
                    relay.as_str(),
                    format_branched_by(&declared.fields),
                ),
            }));
        }

        if relay_branching.fields != declared.fields {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: self.domain.as_str().to_string(),
                identifier: self.identifier.as_str().to_string(),
                reason: format!(
                    "{} '{}' branch fields ({}) do not match relay '{}' branch fields ({})",
                    self.model_kind,
                    self.identifier.as_str(),
                    format_branched_by(&declared.fields),
                    relay.as_str(),
                    format_branched_by(&relay_branching.fields),
                ),
            }));
        }

        if relay_branching.branch == declared.branch {
            return Ok(());
        }

        Err(Report::new(RegistryError::IncompatibleSchema {
            domain: self.domain.as_str().to_string(),
            identifier: self.identifier.as_str().to_string(),
            reason: format!(
                "{} '{}' branch name '{}' does not match relay '{}' branch name '{}'",
                self.model_kind,
                self.identifier.as_str(),
                format_branch_name(declared.branch.as_ref()),
                relay.as_str(),
                format_branch_name(relay_branching.branch.as_ref()),
            ),
        }))
    }
}

fn ensure_processing_source_branching(
    domain: &DomainName,
    identifier: &ModelName,
    model_kind: &str,
    relay: &RelayName,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &DiGraph<ActiveNode, EdgeKind>,
) -> Result<(), Report<RegistryError>> {
    let Some(index) = indices.get(&NodeRef::new(ModelKind::Relay, relay.clone())) else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: "relay",
            reference: relay.as_str().to_string(),
        }));
    };
    let Some(node) = graph.node_weight(*index) else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: "relay",
            reference: relay.as_str().to_string(),
        }));
    };
    if node.effective_branching.is_some() {
        return Ok(());
    }

    Err(Report::new(RegistryError::IncompatibleSchema {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "{} '{}' requires relay '{}' to declare BRANCHED BY or UNBRANCHED",
            model_kind,
            identifier.as_str(),
            relay.as_str(),
        ),
    }))
}

fn ensure_relays_have_same_branch(
    domain: &DomainName,
    identifier: &ModelName,
    context: &str,
    left: &RelayName,
    right: &RelayName,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &DiGraph<ActiveNode, EdgeKind>,
) -> Result<(), Report<RegistryError>> {
    let left_branching = relay_branching(indices, graph, left);
    let right_branching = relay_branching(indices, graph, right);
    let compatible = match (&left_branching, &right_branching) {
        (None, None) => true,
        (Some(left), Some(right)) => left.branch == right.branch && left.fields == right.fields,
        _ => false,
    };
    if compatible {
        return Ok(());
    }
    Err(Report::new(RegistryError::IncompatibleSchema {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "{context} requires relay '{}' and materialized relay '{}' to use the same exact \
             branch",
            left, right
        ),
    }))
}

fn relay_branching(
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &DiGraph<ActiveNode, EdgeKind>,
    relay: &RelayName,
) -> Option<ResolvedBranching> {
    let index = indices.get(&NodeRef::new(ModelKind::Relay, relay.clone()))?;
    let node = graph.node_weight(*index)?;
    let Model::Relay(relay) = node.config.as_ref() else {
        return None;
    };
    Some(ResolvedBranching {
        branch: relay.branching.branch().cloned(),
        schema: node.effective_branching_schema.clone(),
        fields: node.effective_branching.clone()?,
    })
}

#[derive(Clone)]
pub(in crate::registry) struct ResolvedBranching {
    branch: Option<BranchName>,
    pub(in crate::registry) schema: Option<SchemaName>,
    pub(in crate::registry) fields: Vec<FieldName>,
}

impl ResolvedBranching {
    fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

pub(in crate::registry) trait BranchReference {
    fn branch_ref(&self) -> Option<&BranchName>;
}

impl BranchReference for BranchSelection {
    fn branch_ref(&self) -> Option<&BranchName> {
        self.branch()
    }
}

impl BranchReference for OutputBranch {
    fn branch_ref(&self) -> Option<&BranchName> {
        self.branch()
    }
}

fn resolved_output_branches(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    outputs: &ProcessorOutputs,
) -> Result<Vec<(RelayName, ResolvedBranching)>, Report<RegistryError>> {
    outputs
        .outputs()
        .map(|output| {
            let Some(branch) = output.branch.as_ref() else {
                return Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "TO output '{}' must declare BRANCHED BY or UNBRANCHED",
                        output.relay.as_str()
                    ),
                }));
            };
            Ok((
                output.relay.clone(),
                resolved_branch_selection(domain, identifier, models, branch)?,
            ))
        })
        .collect()
}

pub(in crate::registry) fn resolved_branch_selection(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    branched_by: &dyn BranchReference,
) -> Result<ResolvedBranching, Report<RegistryError>> {
    let Some(branch_ref) = branched_by.branch_ref() else {
        return Ok(ResolvedBranching {
            branch: None,
            schema: None,
            fields: Vec::new(),
        });
    };
    let branch = branch_model(domain, identifier, models, branch_ref)?;
    Ok(ResolvedBranching {
        branch: Some(branch_ref.clone()),
        schema: Some(branch.schema.clone()),
        fields: branching_schema_fields(domain, identifier, models, &branch.schema)?,
    })
}

pub(in crate::registry) fn branch_model<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    models: &'a ModelIndex,
    branch_ref: &BranchName,
) -> Result<&'a CreateBranch, Report<RegistryError>> {
    let Some(Model::Branch(branch)) =
        models.get(&NodeRef::new(ModelKind::Branch, branch_ref.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Branch.as_str(),
            reference: branch_ref.as_str().to_string(),
        }));
    };
    Ok(branch)
}

fn schema_model<'a>(
    domain: &DomainName,
    identifier: &ModelName,
    models: &'a ModelIndex,
    schema_ref: &SchemaName,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    let Some(Model::Schema(schema)) =
        models.get(&NodeRef::new(ModelKind::Schema, schema_ref.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Schema.as_str(),
            reference: schema_ref.as_str().to_string(),
        }));
    };
    Ok(schema)
}

pub(in crate::registry) fn model_branch_selection(model: &Model) -> Option<&dyn BranchReference> {
    match model {
        Model::Generator(generator) => Some(&generator.branched_by),
        Model::Inferencer(processor) => Some(&processor.branched_by),
        Model::WasmProcessor(processor) => Some(&processor.branched_by),
        Model::Deduplicator(deduplicator) => Some(&deduplicator.branched_by),
        Model::Correlator(correlator) => Some(&correlator.branched_by),
        Model::Junction(junction) => Some(&junction.branched_by),
        Model::Reorderer(reorderer) => Some(&reorderer.branched_by),
        Model::WindowProcessor(window_processor) => Some(&window_processor.branched_by),
        _ => None,
    }
}

pub(in crate::registry) fn branching_schema_fields(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    branch_schema: &SchemaName,
) -> Result<Vec<FieldName>, Report<RegistryError>> {
    let Some(Model::Schema(schema)) =
        models.get(&NodeRef::new(ModelKind::Schema, branch_schema.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Schema.as_str(),
            reference: branch_schema.as_str().to_string(),
        }));
    };
    Ok(schema
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect())
}

fn assign_stream_branching(
    domain: &DomainName,
    producer: &ModelName,
    relay: &RelayName,
    branching: ResolvedBranching,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
) -> Result<bool, Report<RegistryError>> {
    let index = *indices
        .get(&NodeRef::new(ModelKind::Relay, relay.clone()))
        .verified(
            "the relay reference was validated above, and every validated relay has a graph node",
        );
    let node = graph.node_weight_mut(index).verified(
        "the relay reference was validated above, and every validated relay has a graph node",
    );

    match &node.effective_branching {
        None => {
            node.effective_branching = Some(branching.fields);
            node.effective_branching_schema = branching.schema;
            Ok(true)
        }
        Some(existing) if *existing == branching.fields => {
            let Model::Relay(relay_model) = node.config.as_ref() else {
                unreachable!("stream branching may only be assigned to a relay")
            };
            if relay_model.branching.branch() != branching.branch.as_ref() {
                return Err(Report::new(RegistryError::IncompatibleSchema {
                    domain: domain.as_str().to_string(),
                    identifier: producer.as_str().to_string(),
                    reason: format!(
                        "stream '{}' receives conflicting branch names: existing '{}' vs producer \
                         '{}' with '{}'",
                        relay.as_str(),
                        format_branch_name(relay_model.branching.branch()),
                        producer.as_str(),
                        format_branch_name(branching.branch.as_ref()),
                    ),
                }));
            }
            if node.effective_branching_schema.is_none() && branching.schema.is_some() {
                node.effective_branching_schema = branching.schema;
                return Ok(true);
            }
            Ok(false)
        }
        Some(existing) => Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: producer.as_str().to_string(),
            reason: format!(
                "stream '{}' receives conflicting branch fields: existing ({}) vs producer '{}' \
                 with ({})",
                relay.as_str(),
                format_branched_by(existing),
                producer.as_str(),
                format_branched_by(&branching.fields),
            ),
        })),
    }
}

pub(in crate::registry) fn format_branch_name(branch: Option<&BranchName>) -> &str {
    match branch {
        Some(name) => name.as_str(),
        None => "UNBRANCHED",
    }
}

fn format_branched_by(branched_by: &[FieldName]) -> String {
    if branched_by.is_empty() {
        "(none)".to_string()
    } else {
        branched_by
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

pub(in crate::registry) fn add_output_branch_dependency_edges(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    outputs: &ProcessorOutputs,
) -> Result<(), Report<RegistryError>> {
    for output in outputs.outputs() {
        let Some(branch_ref) = output.branch.as_ref().and_then(OutputBranch::branch) else {
            continue;
        };
        let branch = expect_kind(
            domain,
            identifier,
            models,
            indices,
            branch_ref,
            ModelKind::Branch,
        )?;
        graph.add_edge(branch, source, EdgeKind::RequiredBy);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nervix_models::{
        AckMode, Assignment, AssignmentTarget, AssignmentTargetScope, ClusterNodeName,
        CreateGenerator, CreateIngestor, CreateReingestor, CreateWireSchema, Expression,
        FieldReference, FieldScope, FlushPolicy, GeneralErrorPolicy, IngestSource, JsonType,
        KafkaIngestMode, KafkaOffsetMode, MaterializedRelayState, MessageErrorPolicy, ParseAsType,
        PlacementPolicy, ProcessorInputs, SchemaField, WireSchemaField, WireSchemaName,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::registry::{
        storage::Registry,
        test_fixtures::{
            branch, branch_for_relay, branch_name_for_relay, branch_schema,
            branch_schema_with_types, branched_by, client_model, codec, deduplicator,
            ingestor_with_params, junction, named, processor, reingestor, relay, relay_branched_by,
            relay_branched_by_relay_branch, relay_branched_like, scheduled_node, schema,
            temp_db_path, unbranched_ingestor, wire_schema, with_inherit_all, with_output_branch,
            with_processor_branching,
        },
    };

    #[test]
    fn apply_batch_accepts_unbranched_ingestor_without_branch_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("kafka_main"),
                    relay("notifications", "event_schema"),
                    unbranched_ingestor("ing", "notifications", "event_codec", "kafka_main"),
                ],
            )
            .expect("unbranched ingestor should not require a branch schema");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let relay = graph
            .node(ModelKind::Relay, &named("notifications"))
            .expect("relay should exist");
        assert_eq!(relay.effective_branching, Some(Vec::new()));
        assert_eq!(relay.effective_branching_schema, None);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_branching_value_type_mismatch() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: named("event_schema"),
                        fields: vec![SchemaField {
                            name: named("value"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    relay_branched_by_relay_branch("events", "event_schema"),
                    Model::Schema(CreateSchema {
                        name: named("value_branch"),
                        fields: vec![SchemaField {
                            name: named("value"),
                            ty: ParseAsType::U32,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    branch_for_relay("events", "value_branch"),
                    client_model("kafka_main"),
                    ingestor_with_params(
                        "events_in",
                        "events",
                        "event_codec",
                        "kafka_main",
                        &["value"],
                    ),
                ],
            )
            .expect_err("branch value type mismatch must fail");

        let message = format!("{err}");
        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            message.contains(
                "branch SET compile failed: SET field 'value' has expression type Utf8, expected \
                 declared output type UInt32"
            ),
            "unexpected error: {message}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_branched_by_fields_missing_from_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    branch_schema("missing_key_branch", &["missing_key"]),
                    branch_for_relay("notifications", "missing_key_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["missing_key"],
                    ),
                ],
            )
            .expect_err("missing branch field should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("unknown finalized output field 'missing_key'"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incomplete_ingestor_branch_construction() {
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::I64,
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: JsonType::Integer,
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
                    client_model("broker_in_2"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    branch_schema_with_types(
                        "tenant_user_id_branch",
                        &[
                            ("tenant", ParseAsType::String),
                            ("user_id", ParseAsType::I64),
                        ],
                    ),
                    branch_for_relay("notifications", "tenant_user_id_branch"),
                    ingestor_with_params(
                        "ing_a",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant", "user_id"],
                    ),
                    Model::Ingestor(CreateIngestor {
                        name: named("ing_b"),
                        output_routes: with_output_branch(
                            with_inherit_all(ProcessorOutputs::single(named("notifications")))
                                .with_flush_policy(FlushPolicy::Each {
                                    interval: "100ms".to_string(),
                                    max_batch_size: "1MiB".to_string(),
                                }),
                            OutputBranch::BranchedBy {
                                branch: branch_name_for_relay("notifications"),
                                assignments: vec![Assignment {
                                    target: AssignmentTarget {
                                        scope: AssignmentTargetScope::Bare,
                                        field: named("user_id"),
                                    },
                                    value: Expression::Field(FieldReference::scoped(
                                        FieldScope::Message,
                                        named("user_id"),
                                    )),
                                }],
                            },
                        ),
                        decode_using_codec: named("event_codec"),
                        timestamp_source: None,
                        source: IngestSource::Kafka {
                            client: named("broker_in_2"),
                            topic: named("notifications"),
                            offset_mode: KafkaOffsetMode::ConsumerGroup(named("cg")),
                            instances: nonzero!(1u64),
                            mode: KafkaIngestMode::AckSequential {
                                timeout: "30s".to_string(),
                                retry_policy: nervix_models::RetryPolicy {
                                    backoff: "200ms".to_string(),
                                    max_backoff: "5s".to_string(),
                                },
                            },
                            quiesce: nervix_models::IngestQuiesceMode::Suspend,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    }),
                ],
            )
            .expect_err("every required branch field must be initialized");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("required branch field 'tenant' remains uninitialized"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_ingestor_branch_name_mismatch_with_same_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let Model::Ingestor(mut ingestor) = ingestor_with_params(
            "ing",
            "notifications",
            "event_codec",
            "broker_in",
            &["value"],
        ) else {
            unreachable!("ingestor helper must build an ingestor model")
        };
        let Some(OutputBranch::BranchedBy {
            branch: ingestor_branch,
            ..
        }) = &mut ingestor.output_routes.routes[0].branch
        else {
            unreachable!("ingestor helper must build a branched ingestor")
        };
        *ingestor_branch = named("branch_b");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by("notifications", "event_schema", "branch_a"),
                    branch_schema("value_branch", &["value"]),
                    branch("branch_a", "value_branch"),
                    branch("branch_b", "value_branch"),
                    Model::Ingestor(ingestor),
                ],
            )
            .expect_err("differently named ingestor and relay branches must be incompatible");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains("must use its exact declared branch 'branch_a'"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_processor_crossing_same_schema_branch_names() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let Model::Deduplicator(mut processor) = processor("project", "input", "output") else {
            unreachable!("processor helper must build a deduplicator model")
        };
        processor.branched_by = BranchSelection::branched_by(named("branch_b"));

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay_branched_by("input", "event_schema", "branch_a"),
                    relay_branched_by("output", "event_schema", "branch_b"),
                    branch_schema("value_branch", &["value"]),
                    branch("branch_a", "value_branch"),
                    branch("branch_b", "value_branch"),
                    Model::Deduplicator(processor),
                ],
            )
            .expect_err("normal processors must not cross differently named branches");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains(
                "branch name 'branch_b' does not match relay 'input' branch name 'branch_a'"
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_generator_crossing_same_schema_branch_names() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let Model::Relay(mut input) = relay_branched_by("input", "event_schema", "branch_a") else {
            unreachable!("relay helper must build a relay model")
        };
        input.materialized_state = Some(MaterializedRelayState::LastByTimestamp);

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    Model::Relay(input),
                    relay_branched_by("output", "event_schema", "branch_b"),
                    branch_schema("value_branch", &["value"]),
                    branch("branch_a", "value_branch"),
                    branch("branch_b", "value_branch"),
                    Model::Generator(CreateGenerator {
                        name: named("generate"),
                        materialized_relay: named("input"),
                        branched_by: BranchSelection::branched_by(named("branch_b")),
                        each: "100ms".to_string(),
                        output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                            relay: named("output"),
                            construction: nervix_nspl::parse_route_construction(
                                "SET value = relay_state.input.value",
                            )
                            .expect("generator route must parse"),
                            flush_policy: Some(FlushPolicy::Immediate),
                            message_error_policy: MessageErrorPolicy::Log,
                            branch: None,
                        }]),
                    }),
                ],
            )
            .expect_err("generators must not cross differently named branches");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains(
                "generator 'generate' branch name 'branch_b' does not match relay 'input' branch \
                 name 'branch_a'"
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_infers_stream_branching_through_deduplicator_chain() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::I64,
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: JsonType::Integer,
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
                    relay_branched_like("projected", "event_schema", "notifications"),
                    branch_schema_with_types(
                        "tenant_user_id_branch",
                        &[
                            ("tenant", ParseAsType::String),
                            ("user_id", ParseAsType::I64),
                        ],
                    ),
                    branch_for_relay("notifications", "tenant_user_id_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant", "user_id"],
                    ),
                    with_processor_branching(processor("project", "notifications", "projected")),
                ],
            )
            .expect("graph with inherited branch fields should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let projected = graph
            .node(
                ModelKind::Relay,
                &ModelName::from(&RelayName::parse("projected").expect("valid relay name")),
            )
            .expect("projected relay should exist");

        assert_eq!(
            projected
                .effective_branching
                .as_ref()
                .expect("projected relay should be branched")
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant", "user_id"]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_infers_stream_branching_through_reingestor_outputs() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::I64,
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: JsonType::Integer,
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
                    relay_branched_by(
                        "notifications",
                        "event_schema",
                        branch_name_for_relay("notifications").as_str(),
                    ),
                    relay_branched_by("errors", "event_schema", "by_route_logs"),
                    relay_branched_by("warnings", "event_schema", "by_route_logs"),
                    relay_branched_by("info", "event_schema", "by_route_logs"),
                    branch_schema_with_types(
                        "tenant_user_id_branch",
                        &[
                            ("tenant", ParseAsType::String),
                            ("user_id", ParseAsType::I64),
                        ],
                    ),
                    branch_for_relay("notifications", "tenant_user_id_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant", "user_id"],
                    ),
                    branch("by_route_logs", "tenant_user_id_branch"),
                    Model::Reingestor(CreateReingestor {
                        name: named("route_logs"),
                        from: ProcessorInputs::single(named("notifications")),
                        output_routes: with_output_branch(
                            with_inherit_all(ProcessorOutputs::new(vec![
                                ProcessorOutput {
                                    relay: named("errors"),
                                    construction: nervix_nspl::parse_route_construction(
                                        r#"WHERE input.value = "error""#,
                                    )
                                    .expect("route construction must parse"),
                                    flush_policy: None,
                                    message_error_policy: MessageErrorPolicy::Log,
                                    branch: None,
                                },
                                ProcessorOutput {
                                    relay: named("warnings"),
                                    construction: nervix_nspl::parse_route_construction(
                                        r#"WHERE input.value = "warn""#,
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
                            branched_by("route_logs", &["tenant", "user_id"]),
                        ),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect("reingestor graph should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");

        for relay_name in ["errors", "warnings", "info"] {
            let relay = graph
                .node(
                    ModelKind::Relay,
                    &ModelName::from(&RelayName::parse(relay_name).expect("valid identifier")),
                )
                .expect("routed relay should exist");

            assert_eq!(
                relay
                    .effective_branching
                    .as_ref()
                    .expect("routed relay should be branched")
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>(),
                vec!["tenant", "user_id"]
            );
        }

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_deduplicator_without_explicit_upstream_branching_alias() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    relay("notifications", "event_schema"),
                    relay_branched_like("projected", "event_schema", "notifications"),
                    processor("project", "notifications", "projected"),
                ],
            )
            .expect_err("deduplicator without upstream branch fields should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains(
                "deduplicator 'project' requires relay 'notifications' to have branch fields",
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_infers_stream_branching_through_deduplicators() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let changes = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: SchemaName::parse("notification").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: FieldName::parse("tenant").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: FieldName::parse("transaction_id").expect("valid identifier"),
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
                                name: FieldName::parse("transaction_id").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    }),
                    codec("event_codec", "notification"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("notifications", "notification"),
                    relay_branched_like("deduped", "notification", "notifications"),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("notifications", "tenant_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant"],
                    ),
                    with_processor_branching(deduplicator(
                        "dedup",
                        "notifications",
                        "deduped",
                        "notifications.transaction_id",
                        "10m",
                    )),
                ],
            )
            .expect("graph with deduplicator branch fields should succeed");

        let schedule = changes
            .graph
            .expect("graph should be present")
            .schedule_for_domain(
                &domain,
                &[ClusterNodeName::parse("node-1").expect("valid name")],
                0,
                PlacementPolicy::Neutral,
            );
        let deduped = scheduled_node(&schedule, ModelKind::Relay, "deduped");
        assert_eq!(
            deduped.effective_branching,
            Some(vec![FieldName::parse("tenant").expect("valid field name")])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_deduplicator_without_explicit_upstream_branching() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    relay("notifications", "event_schema"),
                    relay_branched_like("deduped", "event_schema", "notifications"),
                    deduplicator(
                        "dedup",
                        "notifications",
                        "deduped",
                        "notifications.value",
                        "10m",
                    ),
                ],
            )
            .expect_err("deduplicator without upstream branch fields should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains(
                "deduplicator 'dedup' requires relay 'notifications' to have branch fields",
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_constructs_reingestor_target_branching() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::I64,
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: JsonType::Integer,
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
                    relay_branched_by_relay_branch("tenant_notifications", "event_schema"),
                    branch_schema_with_types(
                        "tenant_user_id_branch",
                        &[
                            ("tenant", ParseAsType::String),
                            ("user_id", ParseAsType::I64),
                        ],
                    ),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("notifications", "tenant_user_id_branch"),
                    branch_for_relay("tenant_notifications", "tenant_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant", "user_id"],
                    ),
                    reingestor(
                        "tenant_partition",
                        "notifications",
                        "tenant_notifications",
                        &["tenant"],
                    ),
                ],
            )
            .expect("graph with reingestor branch fields should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let target = graph
            .node(
                ModelKind::Relay,
                &ModelName::from(
                    &RelayName::parse("tenant_notifications").expect("valid relay name"),
                ),
            )
            .expect("target relay should exist");

        assert_eq!(
            target
                .effective_branching
                .as_ref()
                .expect("target relay should be branched")
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant"]
        );
        assert_eq!(
            target
                .effective_branching_schema
                .as_ref()
                .map(|name| name.as_str()),
            Some("tenant_branch")
        );

        let dataflow_graph = graph.to_dataflow_graph(domain.as_str());
        let branches = dataflow_graph
            .nodes
            .iter()
            .map(|node| {
                (
                    node.id.as_str(),
                    node.branch
                        .as_ref()
                        .map(|branch| (branch.name.as_str(), branch.key_schema.as_str())),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(branches.get("ingestor:ing"), Some(&None));
        assert_eq!(branches.get("reingestor:tenant_partition"), Some(&None));
        assert_eq!(
            branches.get("relay:tenant_notifications"),
            Some(&Some(("by_tenant_notifications", "tenant_branch")))
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_reingestor_from_unbranched_source_to_branched_target() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::U32,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("tenant_notifications", "tenant_branch"),
                    relay("notifications", "event_schema"),
                    relay_branched_by_relay_branch("tenant_notifications", "event_schema"),
                    reingestor(
                        "tenant_partition",
                        "notifications",
                        "tenant_notifications",
                        &["tenant"],
                    ),
                ],
            )
            .expect("reingestor may repartition an explicitly unbranched source");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let source = graph
            .node(ModelKind::Relay, &named("notifications"))
            .expect("source relay should exist");
        assert_eq!(source.effective_branching, Some(Vec::new()));
        assert_eq!(source.effective_branching_schema, None);

        let target = graph
            .node(ModelKind::Relay, &named("tenant_notifications"))
            .expect("target relay should exist");
        assert_eq!(
            target
                .effective_branching
                .as_ref()
                .expect("target relay should be branched")
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant"]
        );
        assert_eq!(
            target
                .effective_branching_schema
                .as_ref()
                .map(|name| name.as_str()),
            Some("tenant_branch")
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_junction_without_explicit_upstream_branching() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("left", "value_branch"),
                    relay("left", "event_schema"),
                    relay("right", "event_schema"),
                    relay_branched_like("merged", "event_schema", "left"),
                    junction("join_streams", &["left", "right"], "merged"),
                ],
            )
            .expect_err("junction without upstream branch fields should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}")
                .contains("junction 'join_streams' requires relay 'left' to have branch fields"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incompatible_branches_for_one_relay() {
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::I64,
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
                                name: FieldName::parse("user_id").expect("valid identifier"),
                                ty: JsonType::Integer,
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
                    client_model("broker_in_2"),
                    relay_branched_by_relay_branch("left", "event_schema"),
                    relay_branched_by_relay_branch("right", "event_schema"),
                    relay_branched_like("merged", "event_schema", "left"),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("left", "tenant_branch"),
                    ingestor_with_params(
                        "ing_left",
                        "left",
                        "event_codec",
                        "broker_in",
                        &["tenant"],
                    ),
                    branch_schema_with_types("user_id_branch", &[("user_id", ParseAsType::I64)]),
                    branch_for_relay("right", "user_id_branch"),
                    ingestor_with_params(
                        "ing_right",
                        "right",
                        "event_codec",
                        "broker_in_2",
                        &["user_id"],
                    ),
                    with_processor_branching(processor("left_proc", "left", "merged")),
                    with_processor_branching(processor("right_proc", "right", "merged")),
                ],
            )
            .expect_err("one relay cannot receive incompatible branches");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains("conflicting branch fields"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }
}
