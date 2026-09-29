//! Pinned resource and generator decisions for one scheduled domain revision.
//!
//! Layer: decisions.
//! - **Owns.** Selecting lookup files, UDF programs, generator routes and WASM modules from a
//!   validated schedule, with exact relay, branch and resource identities.
//! - **Depends on.** Vocabulary Models, the activation plan and the expression VM frontend.
//! - **Must not know.** Resource stores, compiled runtime modules, clocks or spawned tasks.

use std::collections::{BTreeMap, BTreeSet};

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    ClusterNodeName, CodecName, CreateUdf, CreateWasmProcessor, DomainClockPeriod, DomainName,
    FieldName, FlushPolicy, GeneratorName, LookupName, MessageErrorPolicy, Model, ModelKind,
    ModelName, RelayName, ResolvedBranching, ResourceId, ScheduledNode, ScheduledNodes,
};
use nervix_roto::UdfProgram;
use nervix_vm::{
    lower_set_only_route,
    program::{Program, SpannedNode},
};
use thiserror::Error;
use triomphe::Arc;

use super::DomainActivationPlan;
use crate::runtime_schema::CompiledSchema;

#[derive(Debug, Error)]
pub(crate) enum ResourcePlanError {
    #[error("scheduled {kind:?} '{node}' has a different configured identity")]
    NodeIdentity { kind: ModelKind, node: ModelName },
    #[error("lookup '{lookup}' references missing codec '{codec}'")]
    MissingLookupCodec {
        lookup: LookupName,
        codec: CodecName,
    },
    #[error("lookup '{lookup}' codec '{codec}' has no key field '{field}'")]
    MissingLookupKey {
        lookup: LookupName,
        codec: CodecName,
        field: FieldName,
    },
    #[error("generator '{generator}' references missing materialized relay '{relay}'")]
    MissingGeneratorSource {
        generator: GeneratorName,
        relay: RelayName,
    },
    #[error("generator '{generator}' references relay '{relay}' without materialized state")]
    UnmaterializedGeneratorSource {
        generator: GeneratorName,
        relay: RelayName,
    },
    #[error("generator '{generator}' route references missing relay '{relay}'")]
    MissingGeneratorOutput {
        generator: GeneratorName,
        relay: RelayName,
    },
    #[error("generator '{generator}' route to '{relay}' has a different branch declaration")]
    GeneratorBranch {
        generator: GeneratorName,
        relay: RelayName,
    },
    #[error("generator '{generator}' route to '{relay}' has no flush policy")]
    GeneratorFlush {
        generator: GeneratorName,
        relay: RelayName,
    },
    #[error("generator '{generator}' route to '{relay}' has an invalid set-only construction")]
    GeneratorConstruction {
        generator: GeneratorName,
        relay: RelayName,
    },
    #[error("scheduled WASM processor '{processor}' has no guest-state generations")]
    MissingWasmGenerations { processor: ModelName },
}

/// Assignment from the same scheduled entry whose declaration produced a resource plan.
#[derive(Debug, Clone)]
pub(crate) struct PlannedNodeAssignment {
    primary: Option<ClusterNodeName>,
    assigned: BTreeSet<ClusterNodeName>,
}

impl PlannedNodeAssignment {
    fn from_scheduled(node: &ScheduledNode) -> Self {
        Self {
            primary: node.primary_node.clone(),
            assigned: node.assigned_nodes.iter().cloned().collect(),
        }
    }

    pub(crate) fn executes_on(&self, local: Option<&ClusterNodeName>) -> bool {
        match local {
            Some(local) => match &self.primary {
                Some(primary) => primary == local,
                None => self.assigned.contains(local),
            },
            None => self.primary.is_none() && self.assigned.is_empty(),
        }
    }

    pub(crate) fn is_assigned_to(&self, local: &ClusterNodeName) -> bool {
        self.assigned.contains(local)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LookupResourcePlan {
    pub(crate) name: LookupName,
    pub(crate) key_field: FieldName,
    pub(crate) resource: ResourceId,
    pub(crate) path: String,
    pub(crate) codec: CodecName,
}

/// A generator route after set-only lowering, in the order its declaration writes it.
#[derive(Debug, Clone)]
pub(crate) struct GeneratorRoutePlan {
    pub(crate) relay: RelayName,
    pub(crate) program: SpannedNode<Program>,
    pub(crate) flush_policy: FlushPolicy,
    pub(crate) message_error_policy: MessageErrorPolicy,
    pub(crate) output_schema: Arc<CompiledSchema>,
}

#[derive(Debug, Clone)]
pub(crate) struct GeneratorExecutionPlan {
    pub(crate) name: GeneratorName,
    pub(crate) each: DomainClockPeriod,
    pub(crate) source_relay: RelayName,
    pub(crate) source_branching: ResolvedBranching,
    pub(crate) source_schema: Arc<CompiledSchema>,
    pub(crate) routes: Vec<GeneratorRoutePlan>,
    pub(crate) assignment: PlannedNodeAssignment,
}

/// The exact file of one pinned WASM module. Candidate validation and installed execution both
/// prepare this same engine input, while only an installed schedule supplies guest-state identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WasmModulePlan {
    pub(crate) processor: ModelName,
    pub(crate) resource: ResourceId,
    pub(crate) file: String,
}

impl WasmModulePlan {
    pub(crate) fn from_model(domain: &DomainName, model: &CreateWasmProcessor) -> Self {
        Self {
            processor: ModelName::from(&model.name),
            resource: ResourceId::new(
                domain.clone(),
                model.resource.clone(),
                model.resource_version,
            ),
            file: model.file.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedWasmProcessor {
    pub(crate) module: WasmModulePlan,
    pub(crate) assignment: PlannedNodeAssignment,
}

/// The resource-bearing node specifications of one committed schedule. They are not persisted;
/// the schedule is authoritative and each build derives the same pinned values from it.
#[derive(Debug, Default)]
pub(crate) struct ResourceExecutionPlans {
    pub(crate) lookups: BTreeMap<LookupName, LookupResourcePlan>,
    pub(crate) generators: BTreeMap<GeneratorName, GeneratorExecutionPlan>,
    pub(crate) udfs: Vec<UdfProgram>,
    pub(crate) wasm: BTreeMap<ModelName, PlannedWasmProcessor>,
}

pub(crate) fn udf_program(model: &CreateUdf) -> UdfProgram {
    UdfProgram {
        name: model.name.clone(),
        language: model.language,
        arguments: model.arguments.clone(),
        returns: model.returns.clone(),
        volatile: model.volatile,
        code: model.code.clone(),
        code_hash: model.code_hash.clone(),
    }
}

impl ResourceExecutionPlans {
    pub(crate) fn from_scheduled_nodes(
        domain: &DomainName,
        nodes: &ScheduledNodes,
        activation: &DomainActivationPlan,
    ) -> error_stack::Result<Self, ResourcePlanError> {
        let mut plans = Self::default();
        for node in nodes.values() {
            match node.config.as_ref() {
                Model::Lookup(lookup) => {
                    if node.identifier != ModelName::from(&lookup.name) {
                        return Err(Report::new(ResourcePlanError::NodeIdentity {
                            kind: ModelKind::Lookup,
                            node: node.identifier.clone(),
                        }));
                    }
                    let Some(codec) = activation.codecs.get(&lookup.decode_using_codec) else {
                        return Err(Report::new(ResourcePlanError::MissingLookupCodec {
                            lookup: lookup.name.clone(),
                            codec: lookup.decode_using_codec.clone(),
                        }));
                    };
                    if codec
                        .schema
                        .arrow_schema()
                        .field_with_name(lookup.key_field.as_str())
                        .is_err()
                    {
                        return Err(Report::new(ResourcePlanError::MissingLookupKey {
                            lookup: lookup.name.clone(),
                            codec: lookup.decode_using_codec.clone(),
                            field: lookup.key_field.clone(),
                        }));
                    }
                    plans.lookups.insert(
                        lookup.name.clone(),
                        LookupResourcePlan {
                            name: lookup.name.clone(),
                            key_field: lookup.key_field.clone(),
                            resource: ResourceId::new(
                                domain.clone(),
                                lookup.resource.clone(),
                                lookup.resource_version,
                            ),
                            path: lookup.path.clone(),
                            codec: lookup.decode_using_codec.clone(),
                        },
                    );
                }
                Model::Generator(generator) => {
                    if node.identifier != ModelName::from(&generator.name) {
                        return Err(Report::new(ResourcePlanError::NodeIdentity {
                            kind: ModelKind::Generator,
                            node: node.identifier.clone(),
                        }));
                    }
                    let Some(source) = activation.relays.get(&generator.materialized_relay) else {
                        return Err(Report::new(ResourcePlanError::MissingGeneratorSource {
                            generator: generator.name.clone(),
                            relay: generator.materialized_relay.clone(),
                        }));
                    };
                    if !source.materialized {
                        return Err(Report::new(
                            ResourcePlanError::UnmaterializedGeneratorSource {
                                generator: generator.name.clone(),
                                relay: source.name.clone(),
                            },
                        ));
                    }
                    let mut routes = Vec::with_capacity(generator.output_routes.routes.len());
                    for output in generator.output_routes.outputs() {
                        let Some(relay) = activation.relays.get(&output.relay) else {
                            return Err(Report::new(ResourcePlanError::MissingGeneratorOutput {
                                generator: generator.name.clone(),
                                relay: output.relay.clone(),
                            }));
                        };
                        if relay.branching != source.branching {
                            return Err(Report::new(ResourcePlanError::GeneratorBranch {
                                generator: generator.name.clone(),
                                relay: relay.name.clone(),
                            }));
                        }
                        let Some(flush_policy) = output.flush_policy.clone() else {
                            return Err(Report::new(ResourcePlanError::GeneratorFlush {
                                generator: generator.name.clone(),
                                relay: relay.name.clone(),
                            }));
                        };
                        let program = lower_set_only_route(
                            &output.construction,
                            relay.schema.arrow_schema().as_ref(),
                        )
                        .change_context(
                            ResourcePlanError::GeneratorConstruction {
                                generator: generator.name.clone(),
                                relay: relay.name.clone(),
                            },
                        )?;
                        routes.push(GeneratorRoutePlan {
                            relay: relay.name.clone(),
                            program,
                            flush_policy,
                            message_error_policy: output.message_error_policy.clone(),
                            output_schema: relay.schema.clone(),
                        });
                    }
                    plans.generators.insert(
                        generator.name.clone(),
                        GeneratorExecutionPlan {
                            name: generator.name.clone(),
                            each: generator.each,
                            source_relay: source.name.clone(),
                            source_branching: source.branching.clone(),
                            source_schema: source.schema.clone(),
                            routes,
                            assignment: PlannedNodeAssignment::from_scheduled(node),
                        },
                    );
                }
                Model::Udf(udf) => {
                    if node.identifier != ModelName::from(&udf.name) {
                        return Err(Report::new(ResourcePlanError::NodeIdentity {
                            kind: ModelKind::Udf,
                            node: node.identifier.clone(),
                        }));
                    }
                    plans.udfs.push(udf_program(udf));
                }
                Model::WasmProcessor(processor) => {
                    if node.identifier != ModelName::from(&processor.name) {
                        return Err(Report::new(ResourcePlanError::NodeIdentity {
                            kind: ModelKind::WasmProcessor,
                            node: node.identifier.clone(),
                        }));
                    }
                    if node.wasm_state_generations().is_none() {
                        return Err(Report::new(ResourcePlanError::MissingWasmGenerations {
                            processor: node.identifier.clone(),
                        }));
                    }
                    plans.wasm.insert(
                        node.identifier.clone(),
                        PlannedWasmProcessor {
                            module: WasmModulePlan::from_model(domain, processor),
                            assignment: PlannedNodeAssignment::from_scheduled(node),
                        },
                    );
                }
                _ => {}
            }
        }
        plans.udfs.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(plans)
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        BranchSelection, CreateGenerator, CreateLookup, CreateUdf, MaterializedRelayState,
        ParseAsType, ProcessorOutput, ProcessorOutputs, RelayBranching, UdfArgument, UdfLanguage,
        UdfReturn,
    };

    use super::*;
    use crate::registry::test_fixtures::{
        branch, branch_schema, named, relay, schema, syslog_codec, unplaced_schedule,
        wasm_processor,
    };

    fn plan_result(
        models: Vec<Model>,
    ) -> error_stack::Result<ResourceExecutionPlans, ResourcePlanError> {
        let domain = named("orders");
        let nodes = unplaced_schedule(models);
        let activation = DomainActivationPlan::from_scheduled_nodes(&domain, &nodes)
            .assured("the fixture declares all referenced schemas and relays");
        ResourceExecutionPlans::from_scheduled_nodes(&domain, &nodes, &activation)
    }

    fn planned(models: Vec<Model>) -> ResourceExecutionPlans {
        plan_result(models).assured("the fixture declares valid resource-bearing nodes")
    }

    fn generator_models() -> Vec<Model> {
        let Model::Relay(mut source) = relay("latest", "payload") else {
            unreachable!("the relay fixture builds a relay");
        };
        source.materialized_state = Some(MaterializedRelayState::LastByTimestamp);
        vec![
            schema("payload"),
            Model::Relay(source),
            relay("generated", "payload"),
            Model::Generator(CreateGenerator {
                name: named("refresh"),
                materialized_relay: named("latest"),
                branched_by: BranchSelection::unbranched(),
                each: "100ms".parse().assured("the cadence is positive"),
                output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                    relay: named("generated"),
                    construction: nervix_nspl::parse_route_construction(
                        "SET value = relay_state.latest.value",
                    )
                    .assured("the route is valid NSPL"),
                    flush_policy: Some(FlushPolicy::Immediate),
                    message_error_policy: MessageErrorPolicy::Log,
                    branch: None,
                }]),
            }),
        ]
    }

    #[test]
    fn lookup_and_wasm_plans_pin_the_installed_resource_version() {
        let plans = planned(vec![
            schema("payload"),
            relay("incoming", "payload"),
            relay("outgoing", "payload"),
            syslog_codec("records", "payload"),
            Model::Lookup(CreateLookup {
                name: named("by_value"),
                key_field: named("value"),
                resource: named("lookup_bundle"),
                resource_version: 7,
                path: "data/lookup.json".to_string(),
                decode_using_codec: named("records"),
            }),
            wasm_processor("filter", "incoming", "outgoing"),
        ]);

        let lookup = plans
            .lookups
            .get("by_value")
            .assured("the installed lookup has a plan");
        assert_eq!(lookup.resource.domain, named("orders"));
        assert_eq!(lookup.resource.identifier, named("lookup_bundle"));
        assert_eq!(lookup.resource.version, 7);
        assert_eq!(lookup.path, "data/lookup.json");
        assert_eq!(lookup.codec, named("records"));

        let wasm = plans
            .wasm
            .get("filter")
            .assured("the installed WASM processor has a plan");
        assert_eq!(wasm.module.resource.identifier, named("wasm_filter"));
        assert_eq!(wasm.module.resource.version, 1);
        assert_eq!(wasm.module.file, "processors/filter_even.wasm");
    }

    #[test]
    fn generator_plan_lowers_routes_against_the_exact_materialized_source() {
        let plans = planned(generator_models());

        let generator = plans
            .generators
            .get("refresh")
            .assured("the installed generator has a plan");
        assert_eq!(generator.source_relay, named("latest"));
        assert_eq!(generator.source_branching, ResolvedBranching::unbranched());
        assert_eq!(generator.routes.len(), 1);
        assert_eq!(generator.routes[0].relay, named("generated"));
        assert_eq!(generator.routes[0].program.inner.set.len(), 1);
    }

    #[test]
    fn generator_plan_rejects_unavailable_or_unmaterialized_sources() {
        let mut missing_source = generator_models();
        missing_source.remove(1);
        let error = plan_result(missing_source).expect_err("the source relay must exist");
        assert!(matches!(
            error.current_context(),
            ResourcePlanError::MissingGeneratorSource { generator, relay }
                if generator == &named("refresh") && relay == &named("latest")
        ));

        let mut unmaterialized_source = generator_models();
        let Model::Relay(source) = &mut unmaterialized_source[1] else {
            unreachable!("the second fixture model is the source relay");
        };
        source.materialized_state = None;
        let error = plan_result(unmaterialized_source)
            .expect_err("a generator can only read materialized state");
        assert!(matches!(
            error.current_context(),
            ResourcePlanError::UnmaterializedGeneratorSource { generator, relay }
                if generator == &named("refresh") && relay == &named("latest")
        ));
    }

    #[test]
    fn generator_plan_rejects_missing_or_mismatched_output_relays() {
        let mut missing_output = generator_models();
        missing_output.remove(2);
        let error = plan_result(missing_output).expect_err("the route output relay must exist");
        assert!(matches!(
            error.current_context(),
            ResourcePlanError::MissingGeneratorOutput { generator, relay }
                if generator == &named("refresh") && relay == &named("generated")
        ));

        let mut mismatched_branch = generator_models();
        mismatched_branch.push(branch_schema("tenant_key", &["value"]));
        mismatched_branch.push(branch("by_tenant", "tenant_key"));
        let Model::Relay(output) = &mut mismatched_branch[2] else {
            unreachable!("the third fixture model is the output relay");
        };
        output.branching = RelayBranching::branched_by(named("by_tenant"));
        let error = plan_result(mismatched_branch)
            .expect_err("a generator route must preserve the source branch");
        assert!(matches!(
            error.current_context(),
            ResourcePlanError::GeneratorBranch { generator, relay }
                if generator == &named("refresh") && relay == &named("generated")
        ));
    }

    #[test]
    fn generator_plan_rejects_a_route_without_its_flush_or_set_only_contract() {
        let mut no_flush = generator_models();
        let Model::Generator(generator) = &mut no_flush[3] else {
            unreachable!("the fourth fixture model is the generator");
        };
        generator.output_routes.routes[0].flush_policy = None;
        let error =
            plan_result(no_flush).expect_err("each generator route needs an explicit flush");
        assert!(matches!(
            error.current_context(),
            ResourcePlanError::GeneratorFlush { generator, relay }
                if generator == &named("refresh") && relay == &named("generated")
        ));

        let mut transforming = generator_models();
        let Model::Generator(generator) = &mut transforming[3] else {
            unreachable!("the fourth fixture model is the generator");
        };
        generator.output_routes.routes[0].construction =
            nervix_nspl::parse_route_construction("INHERIT ALL")
                .assured("inheritance is valid syntax for a transforming route");
        let error = plan_result(transforming)
            .expect_err("a generator route cannot inherit an input record");
        assert!(matches!(
            error.current_context(),
            ResourcePlanError::GeneratorConstruction { generator, relay }
                if generator == &named("refresh") && relay == &named("generated")
        ));
    }

    #[test]
    fn scheduled_udf_plan_carries_the_exact_body_signature_and_hash() {
        let udf = CreateUdf::new(
            named("add_one"),
            UdfLanguage::Roto0_13,
            vec![UdfArgument {
                name: named("value"),
                ty: ParseAsType::I64,
                optional: false,
            }],
            UdfReturn {
                ty: ParseAsType::I64,
                optional: false,
            },
            false,
            "fn add_one(value: I64Column) -> I64Column { value.add_s(1) }".to_string(),
        );
        let plans = planned(vec![Model::Udf(udf.clone())]);
        assert_eq!(plans.udfs.len(), 1);
        assert_eq!(plans.udfs[0].name, udf.name);
        assert_eq!(plans.udfs[0].arguments, udf.arguments);
        assert_eq!(plans.udfs[0].returns, udf.returns);
        assert_eq!(plans.udfs[0].code, udf.code);
        assert_eq!(plans.udfs[0].code_hash, udf.code_hash);
    }

    #[test]
    fn lookup_plan_reports_a_missing_scheduled_codec() {
        let error = plan_result(vec![
            schema("payload"),
            Model::Lookup(CreateLookup {
                name: named("by_value"),
                key_field: named("value"),
                resource: named("lookup_bundle"),
                resource_version: 7,
                path: "data/lookup.json".to_string(),
                decode_using_codec: named("missing_codec"),
            }),
        ])
        .expect_err("the lookup codec must be part of the committed schedule");
        assert!(matches!(
            error.current_context(),
            ResourcePlanError::MissingLookupCodec { lookup, codec }
                if lookup == &named("by_value") && codec == &named("missing_codec")
        ));
    }

    #[test]
    fn lookup_plan_rejects_an_unresolved_key_before_runtime_loading() {
        let domain = named("orders");
        let nodes = unplaced_schedule(vec![
            schema("payload"),
            syslog_codec("records", "payload"),
            Model::Lookup(CreateLookup {
                name: named("by_value"),
                key_field: named("missing"),
                resource: named("lookup_bundle"),
                resource_version: 7,
                path: "data/lookup.json".to_string(),
                decode_using_codec: named("records"),
            }),
        ]);
        let activation = DomainActivationPlan::from_scheduled_nodes(&domain, &nodes)
            .assured("the codec and schema are present");

        let error = ResourceExecutionPlans::from_scheduled_nodes(&domain, &nodes, &activation)
            .expect_err("the codec has no field for the lookup key");
        assert!(matches!(
            error.current_context(),
            ResourcePlanError::MissingLookupKey { lookup, field, .. }
                if lookup == &named("by_value") && field == &named("missing")
        ));
    }
}
