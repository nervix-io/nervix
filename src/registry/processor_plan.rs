//! Pure processor planning from validated Models into typed execution specifications.
//!
//! Layer: decisions.
//!
//! - **Owns.** Processor topology, branch policy and ordered route decisions derived from a
//!   validated schedule.
//! - **Depends on.** Vocabulary Models and the guarantees established by registry validation.
//! - **Must not know.** Runtime tasks, Arrow batches, relay services or node-local resources.

use std::num::NonZeroU64;

use ahash::{HashMap, HashSet};
use meticulous::OptionExt as _;
use nervix_models::{
    AckMode, BranchName, CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy,
    CreateBranch, ErrorPolicies, FlushPolicy, GeneralErrorPolicy, InferencerTensorDeclaration,
    InferencerTensorMapping, MessageErrorPolicy, Model, ModelKind, ModelName, NodeRef,
    ProcessorInputWhere, ProcessorInputs, ProcessorOutput as ModelProcessorOutput,
    ProcessorOutputs as ModelProcessorOutputs, RelayName, ResolvedBranching, ResourceName,
    ScheduledNodes, SchemaFingerprint, WasmStateGenerations, WindowBound,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BranchedProcessorNodeSpec {
    pub(crate) spec: BranchedProcessorSpec,
    pub(crate) branch: Option<BranchName>,
    pub(crate) branch_ttl: Option<String>,
    pub(crate) branch_max_instances: Option<NonZeroU64>,
    pub(crate) wasm_state_reset: Option<nervix_models::WasmStateReset>,
    pub(crate) binding: ProcessorPlanBinding,
}

/// Schedule residue that changes lowering even when the semantic processor Model is equal.
/// Model-only test planning carries no residue; every installed schedule supplies both values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ProcessorPlanBinding {
    schema_fingerprint: Option<SchemaFingerprint>,
    resolved_branching: Option<ResolvedBranching>,
    wasm_state_generations: Option<WasmStateGenerations>,
}

impl BranchedProcessorNodeSpec {
    /// Whether an already prepared plan represents this exact installed node revision.
    pub(crate) fn reuses_prepared_revision(
        &self,
        previous: Option<&BranchedProcessorNodeSpec>,
    ) -> bool {
        previous == Some(self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BranchedNodeSpecs {
    pub(crate) processors: Vec<BranchedProcessorNodeSpec>,
}

impl BranchedNodeSpecs {
    pub(crate) fn processor(
        &self,
        kind: ModelKind,
        identifier: &ModelName,
    ) -> Option<&BranchedProcessorNodeSpec> {
        self.processors
            .iter()
            .find(|node| node.spec.kind == kind && &node.spec.processor == identifier)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BranchedProcessorSpec {
    pub(crate) kind: ModelKind,
    pub(crate) processor: ModelName,
    pub(crate) input_relays: Vec<RelayName>,
    pub(crate) input_collect_policies: HashMap<RelayName, nervix_models::InputCollectPolicy>,
    pub(crate) mode: AckMode,
    pub(crate) error_policies: ErrorPolicies,
    pub(crate) from_where: HashMap<RelayName, nervix_models::Expression>,
    pub(crate) filter_where: Option<nervix_models::Expression>,
    pub(crate) materialized_state: Vec<nervix_models::MaterializedStateDependency>,
    pub(crate) operation: BranchedProcessorOperationSpec,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BranchedProcessorOperationSpec {
    Deduplicator {
        output_routes: BranchedProcessorOutputsSpec,
        deduplicate_on: Vec<nervix_models::Expression>,
        max_time: String,
    },
    WindowProcessor {
        output_routes: BranchedProcessorOutputsSpec,
        width: WindowBound,
        step: WindowBound,
        state_limit: nervix_models::WindowStateLimit,
    },
    Reorderer {
        output_routes: BranchedProcessorOutputsSpec,
        order_by: Vec<nervix_models::Expression>,
        max_time: String,
    },
    Correlator {
        output_routes: BranchedProcessorOutputsSpec,
        left_relays: Vec<RelayName>,
        right_relays: Vec<RelayName>,
        correlate_where: nervix_models::Expression,
        match_policy: CorrelatorMatchPolicy,
        max_time: String,
        timeout_policy: CorrelationTimeoutPolicy,
    },
    Junction {
        output_routes: BranchedProcessorOutputsSpec,
    },
    Inferencer {
        output_routes: BranchedProcessorOutputsSpec,
        resource: ResourceName,
        resource_version: u64,
        file: String,
        inputs: Vec<InferencerTensorMapping>,
        output_schema: Vec<InferencerTensorDeclaration>,
    },
    WasmProcessor {
        output_routes: BranchedProcessorOutputsSpec,
        resource: ResourceName,
        resource_version: u64,
        file: String,
        limits: nervix_models::WasmProcessorLimits,
        rejected_state_policy: nervix_models::WasmRejectedStatePolicy,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BranchedProcessorOutputsSpec {
    pub(crate) routes: Vec<BranchedProcessorOutputSpec>,
}

impl BranchedProcessorOutputsSpec {
    pub(crate) fn outputs(&self) -> impl Iterator<Item = &BranchedProcessorOutputSpec> {
        self.routes.iter()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BranchedProcessorOutputSpec {
    pub(crate) relay: RelayName,
    pub(crate) construction: nervix_models::RouteConstruction,
    pub(crate) flush_policy: Option<FlushPolicy>,
    pub(crate) message_error_policy: MessageErrorPolicy,
}

impl BranchedProcessorSpec {
    pub(crate) fn output_relays(&self) -> HashSet<RelayName> {
        let mut relays = HashSet::default();
        match &self.operation {
            BranchedProcessorOperationSpec::Deduplicator { output_routes, .. }
            | BranchedProcessorOperationSpec::Reorderer { output_routes, .. }
            | BranchedProcessorOperationSpec::WindowProcessor { output_routes, .. }
            | BranchedProcessorOperationSpec::Junction { output_routes, .. }
            | BranchedProcessorOperationSpec::Inferencer { output_routes, .. }
            | BranchedProcessorOperationSpec::WasmProcessor { output_routes, .. } => {
                relays.extend(output_routes.outputs().map(|output| output.relay.clone()));
            }
            BranchedProcessorOperationSpec::Correlator {
                output_routes,
                timeout_policy,
                ..
            } => {
                relays.extend(output_routes.outputs().map(|output| output.relay.clone()));
                if let CorrelationTimeoutAction::SendTo { relay } = &timeout_policy.left {
                    relays.insert(relay.clone());
                }
                if let CorrelationTimeoutAction::SendTo { relay } = &timeout_policy.right {
                    relays.insert(relay.clone());
                }
            }
        }
        relays
    }

    pub(crate) fn relay_ids(&self) -> HashSet<RelayName> {
        let mut relays = self.output_relays();
        relays.extend(self.input_relays.iter().cloned());
        relays
    }
}

/// Every branch one set of validated Models declares, keyed by its name.
fn branch_declarations<'a>(
    models: impl Iterator<Item = &'a Model>,
) -> HashMap<BranchName, CreateBranch> {
    let mut branches = HashMap::default();
    for model in models {
        if let Model::Branch(branch) = model {
            branches.insert(branch.name.clone(), branch.clone());
        }
    }
    branches
}

fn internal_processor_error_policies(general: GeneralErrorPolicy) -> ErrorPolicies {
    ErrorPolicies {
        message: MessageErrorPolicy::Log,
        general,
    }
}

fn branched_output(output: &ModelProcessorOutput) -> BranchedProcessorOutputSpec {
    BranchedProcessorOutputSpec {
        relay: output.relay.clone(),
        construction: output.construction.clone(),
        flush_policy: output.flush_policy.clone(),
        message_error_policy: output.message_error_policy.clone(),
    }
}

fn branched_outputs(outputs: &ModelProcessorOutputs) -> BranchedProcessorOutputsSpec {
    BranchedProcessorOutputsSpec {
        routes: outputs.routes.iter().map(branched_output).collect(),
    }
}

pub(crate) fn processor_input_where_by_relay(
    from_where: &[ProcessorInputWhere],
) -> HashMap<RelayName, nervix_models::Expression> {
    from_where
        .iter()
        .map(|source_filter| {
            (
                source_filter.relay.clone(),
                source_filter.where_clause.clone(),
            )
        })
        .collect()
}

fn processor_input_where_by_inputs(
    inputs: &ProcessorInputs,
) -> HashMap<RelayName, nervix_models::Expression> {
    processor_input_where_by_relay(inputs.where_clauses())
}

fn processor_input_collect_policies(
    inputs: &ProcessorInputs,
) -> HashMap<RelayName, nervix_models::InputCollectPolicy> {
    let Some(policy) = inputs.collect_policy.as_ref() else {
        return HashMap::default();
    };
    inputs
        .relays()
        .iter()
        .cloned()
        .map(|relay| (relay, policy.clone()))
        .collect()
}

struct BranchPolicy {
    branch: Option<BranchName>,
    ttl: Option<String>,
    max_instances: Option<NonZeroU64>,
}

fn branch_policy(
    branch_ref: Option<&BranchName>,
    branches: &HashMap<BranchName, CreateBranch>,
) -> BranchPolicy {
    let Some(branch_ref) = branch_ref else {
        return BranchPolicy {
            branch: None,
            ttl: None,
            max_instances: None,
        };
    };
    let branch = branches.get(branch_ref).verified(
        "the registry resolved every branch reference before the schedule reached planning",
    );
    BranchPolicy {
        branch: Some(branch_ref.clone()),
        ttl: Some(branch.ttl.clone()),
        max_instances: branch
            .eviction
            .as_ref()
            .map(|eviction| eviction.max_instances()),
    }
}

fn processor_node_spec(
    spec: BranchedProcessorSpec,
    branched_by: &nervix_models::BranchSelection,
    branches: &HashMap<BranchName, CreateBranch>,
) -> BranchedProcessorNodeSpec {
    let policy = branch_policy(branched_by.branch(), branches);
    BranchedProcessorNodeSpec {
        spec,
        branch: policy.branch,
        branch_ttl: policy.ttl,
        branch_max_instances: policy.max_instances,
        wasm_state_reset: None,
        binding: ProcessorPlanBinding::default(),
    }
}

/// One validated model the planner turns into node specs, named as it was registered.
pub(crate) struct PlannedModel {
    pub(crate) kind: ModelKind,
    pub(crate) identifier: ModelName,
    pub(crate) model: Model,
}

pub(crate) fn branched_node_specs_from_scheduled_nodes(
    nodes: &ScheduledNodes,
) -> BranchedNodeSpecs {
    let resets = nodes
        .values()
        .filter_map(|node| {
            node.wasm_state_reset()
                .cloned()
                .map(|reset| (node.identifier.clone(), reset))
        })
        .collect::<HashMap<_, _>>();
    let mut specs = branched_node_specs_from_models(nodes.values().map(|node| PlannedModel {
        kind: node.kind(),
        identifier: node.identifier.clone(),
        model: (*node.config).clone(),
    }));
    for processor in &mut specs.processors {
        let identity = NodeRef::new(processor.spec.kind, processor.spec.processor.clone());
        let scheduled = nodes.get(&identity).verified(
            "the processor specification came from this same validated scheduled-node map",
        );
        processor.binding = ProcessorPlanBinding {
            schema_fingerprint: Some(scheduled.schema_fingerprint),
            resolved_branching: scheduled.resolved_branching.clone(),
            wasm_state_generations: scheduled.wasm_state_generations().cloned(),
        };
        processor.wasm_state_reset = resets.get(&processor.spec.processor).cloned();
    }
    specs
}

pub(crate) fn branched_node_specs_from_models(
    nodes: impl Iterator<Item = PlannedModel>,
) -> BranchedNodeSpecs {
    let nodes = nodes.collect::<Vec<_>>();
    let branches = branch_declarations(nodes.iter().map(|planned| &planned.model));
    let mut processors = Vec::new();

    for PlannedModel {
        kind,
        identifier,
        model,
    } in nodes
    {
        match &model {
            Model::Deduplicator(deduplicator) => {
                if deduplicator.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: deduplicator.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(&deduplicator.from),
                    mode: deduplicator.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&deduplicator.from),
                    filter_where: deduplicator.filter_where.clone(),
                    materialized_state: deduplicator.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Deduplicator {
                        output_routes: branched_outputs(&deduplicator.output_routes),
                        deduplicate_on: deduplicator.deduplicate_on.clone(),
                        max_time: deduplicator.max_time.clone(),
                    },
                };
                processors.push(processor_node_spec(
                    spec,
                    &deduplicator.branched_by,
                    &branches,
                ));
            }
            Model::Reorderer(reorderer) => {
                if reorderer.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: reorderer.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(&reorderer.from),
                    mode: reorderer.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&reorderer.from),
                    filter_where: reorderer.filter_where.clone(),
                    materialized_state: reorderer.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Reorderer {
                        output_routes: branched_outputs(&reorderer.output_routes),
                        order_by: reorderer.order_by.clone(),
                        max_time: reorderer.max_time.clone(),
                    },
                };
                processors.push(processor_node_spec(spec, &reorderer.branched_by, &branches));
            }
            Model::Correlator(correlator) => {
                let mut input_relays = Vec::with_capacity(
                    correlator.left.relays().len() + correlator.right.relays().len(),
                );
                input_relays.extend(correlator.left.relays().iter().cloned());
                input_relays.extend(correlator.right.relays().iter().cloned());
                let mut from_where = processor_input_where_by_inputs(&correlator.left);
                from_where.extend(processor_input_where_by_inputs(&correlator.right));
                let mut input_collect_policies = processor_input_collect_policies(&correlator.left);
                input_collect_policies.extend(processor_input_collect_policies(&correlator.right));
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays,
                    input_collect_policies,
                    mode: correlator.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where,
                    filter_where: correlator.filter_where.clone(),
                    materialized_state: correlator.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Correlator {
                        output_routes: branched_outputs(&correlator.output_routes),
                        left_relays: correlator.left.relays().to_vec(),
                        right_relays: correlator.right.relays().to_vec(),
                        correlate_where: correlator.correlate_where.clone(),
                        match_policy: correlator.match_policy,
                        max_time: correlator.max_time.clone(),
                        timeout_policy: correlator.timeout_policy.clone(),
                    },
                };
                processors.push(processor_node_spec(
                    spec,
                    &correlator.branched_by,
                    &branches,
                ));
            }
            Model::WindowProcessor(window_processor) => {
                if window_processor.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: window_processor.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(
                        &window_processor.from,
                    ),
                    mode: window_processor.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&window_processor.from),
                    filter_where: window_processor.filter_where.clone(),
                    materialized_state: window_processor.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::WindowProcessor {
                        output_routes: branched_outputs(&window_processor.output_routes),
                        width: window_processor.width.clone(),
                        step: window_processor.step.clone(),
                        state_limit: window_processor.state_limit,
                    },
                };
                processors.push(processor_node_spec(
                    spec,
                    &window_processor.branched_by,
                    &branches,
                ));
            }
            Model::Junction(junction) => {
                if junction.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: junction.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(&junction.from),
                    mode: junction.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&junction.from),
                    filter_where: junction.filter_where.clone(),
                    materialized_state: junction.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Junction {
                        output_routes: branched_outputs(&junction.output_routes),
                    },
                };
                processors.push(processor_node_spec(spec, &junction.branched_by, &branches));
            }
            Model::Inferencer(inferencer) => {
                if inferencer.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: inferencer.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(&inferencer.from),
                    mode: inferencer.mode,
                    error_policies: internal_processor_error_policies(GeneralErrorPolicy::Log),
                    from_where: processor_input_where_by_inputs(&inferencer.from),
                    filter_where: inferencer.filter_where.clone(),
                    materialized_state: inferencer.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::Inferencer {
                        output_routes: branched_outputs(&inferencer.output_routes),
                        resource: inferencer.resource.clone(),
                        resource_version: inferencer.resource_version,
                        file: inferencer.file.clone(),
                        inputs: inferencer.inputs.clone(),
                        output_schema: inferencer.output_schema.clone(),
                    },
                };
                processors.push(processor_node_spec(
                    spec,
                    &inferencer.branched_by,
                    &branches,
                ));
            }
            Model::WasmProcessor(processor) => {
                if processor.from.first().is_none() {
                    continue;
                }
                let spec = BranchedProcessorSpec {
                    kind,
                    processor: identifier,
                    input_relays: processor.from.relays().to_vec(),
                    input_collect_policies: processor_input_collect_policies(&processor.from),
                    mode: processor.mode,
                    error_policies: internal_processor_error_policies(
                        processor.global_error_policy.clone(),
                    ),
                    from_where: processor_input_where_by_inputs(&processor.from),
                    filter_where: processor.filter_where.clone(),
                    materialized_state: processor.materialized_state.clone(),
                    operation: BranchedProcessorOperationSpec::WasmProcessor {
                        output_routes: branched_outputs(&processor.output_routes),
                        resource: processor.resource.clone(),
                        resource_version: processor.resource_version,
                        file: processor.file.clone(),
                        limits: processor.limits,
                        rejected_state_policy: processor.rejected_state_policy,
                    },
                };
                processors.push(processor_node_spec(spec, &processor.branched_by, &branches));
            }
            _ => {}
        }
    }

    processors.sort_by(|left, right| left.spec.processor.cmp(&right.spec.processor));

    BranchedNodeSpecs { processors }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        BranchEviction, BranchSelection, CreateJunction, CreateSchema, ParseAsType,
        ProcessorOutput, ProcessorOutputs, ScheduledNode, SchemaField,
    };

    use super::*;

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).assured("the test uses valid model names")
    }

    fn scheduled_junction(schema_byte: u8, input: &str, branch_ttl: &str) -> ScheduledNodes {
        let branch_name = named::<BranchName>("by_tenant");
        let branch_schema = CreateSchema {
            name: named("tenant_key"),
            fields: vec![SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        };
        let branch = ScheduledNode::new(
            Model::Branch(CreateBranch {
                name: branch_name.clone(),
                schema: branch_schema.name.clone(),
                ttl: branch_ttl.to_string(),
                eviction: Some(BranchEviction::Lru {
                    max_instances: NonZeroU64::new(8)
                        .assured("the fixture's branch capacity is a nonzero literal"),
                }),
            }),
            SchemaFingerprint::from_digest([schema_byte; 32]),
        );
        let junction = ScheduledNode::new(
            Model::Junction(CreateJunction {
                name: named("route_events"),
                from: ProcessorInputs::single(named(input)),
                output_routes: ProcessorOutputs::new(vec![
                    ProcessorOutput::with_flush_policy(
                        named("primary_events"),
                        FlushPolicy::Immediate,
                    ),
                    ProcessorOutput::with_flush_policy(
                        named("audit_events"),
                        FlushPolicy::Immediate,
                    ),
                ]),
                branched_by: BranchSelection::branched_by(branch_name.clone()),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            }),
            SchemaFingerprint::from_digest([schema_byte; 32]),
        )
        .with_resolved_branching(Some(ResolvedBranching::branched(
            branch_name,
            branch_schema,
        )));
        [branch, junction]
            .into_iter()
            .map(|node| (node.identity(), node))
            .collect()
    }

    #[test]
    fn scheduled_planner_preserves_topology_schema_and_branch_policy() {
        let nodes = scheduled_junction(7, "incoming", "5m");
        let specs = branched_node_specs_from_scheduled_nodes(&nodes);
        let plan = specs
            .processor(ModelKind::Junction, &named("route_events"))
            .assured("the scheduled junction produces a processor plan by construction");

        assert_eq!(plan.spec.input_relays, vec![named("incoming")]);
        let BranchedProcessorOperationSpec::Junction { output_routes } = &plan.spec.operation
        else {
            panic!("the planned node should remain a junction");
        };
        assert_eq!(
            output_routes
                .routes
                .iter()
                .map(|route| route.relay.clone())
                .collect::<Vec<_>>(),
            vec![named("primary_events"), named("audit_events")]
        );
        assert_eq!(plan.branch.as_ref(), Some(&named("by_tenant")));
        assert_eq!(plan.branch_ttl.as_deref(), Some("5m"));
        assert_eq!(plan.branch_max_instances, NonZeroU64::new(8));
        assert_eq!(
            plan.binding.schema_fingerprint,
            Some(SchemaFingerprint::from_digest([7; 32]))
        );
        assert!(matches!(
            plan.binding.resolved_branching,
            Some(ResolvedBranching::Branched { .. })
        ));
    }

    #[test]
    fn prepared_revision_reuse_requires_equal_topology_schema_and_branch_decisions() {
        let current =
            branched_node_specs_from_scheduled_nodes(&scheduled_junction(1, "incoming", "5m"));
        let current = current
            .processor(ModelKind::Junction, &named("route_events"))
            .assured("the fixture contains the current junction plan");

        let unchanged =
            branched_node_specs_from_scheduled_nodes(&scheduled_junction(1, "incoming", "5m"));
        let changed_topology =
            branched_node_specs_from_scheduled_nodes(&scheduled_junction(1, "alternate", "5m"));
        let changed_schema =
            branched_node_specs_from_scheduled_nodes(&scheduled_junction(2, "incoming", "5m"));
        let changed_branch =
            branched_node_specs_from_scheduled_nodes(&scheduled_junction(1, "incoming", "10m"));

        assert!(
            unchanged
                .processor(ModelKind::Junction, &named("route_events"))
                .assured("the fixture contains the unchanged junction plan")
                .reuses_prepared_revision(Some(current))
        );
        for changed in [&changed_topology, &changed_schema, &changed_branch] {
            assert!(
                !changed
                    .processor(ModelKind::Junction, &named("route_events"))
                    .assured("each fixture contains a changed junction plan")
                    .reuses_prepared_revision(Some(current))
            );
        }
        assert!(!current.reuses_prepared_revision(None));
    }

    #[test]
    fn wasm_guest_state_generation_changes_the_prepared_revision() {
        let nodes = crate::registry::test_fixtures::unplaced_schedule(vec![
            crate::registry::test_fixtures::wasm_processor("filter", "incoming", "outgoing"),
        ]);
        let mut changed = nodes.clone();
        changed
            .values_mut()
            .find(|node| node.kind() == ModelKind::WasmProcessor)
            .assured("the fixture contains a WASM processor")
            .begin_wasm_state_generation();

        let current = branched_node_specs_from_scheduled_nodes(&nodes);
        let current = current
            .processor(ModelKind::WasmProcessor, &named("filter"))
            .assured("the scheduled WASM processor has a plan");
        let changed = branched_node_specs_from_scheduled_nodes(&changed);
        let changed = changed
            .processor(ModelKind::WasmProcessor, &named("filter"))
            .assured("the changed WASM processor has a plan");

        assert_ne!(
            current.binding.wasm_state_generations,
            changed.binding.wasm_state_generations
        );
        assert!(!changed.reuses_prepared_revision(Some(current)));
    }
}
