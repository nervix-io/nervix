//! The ingestor and reingestor decisions of one scheduled domain revision.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving every ingestor's source and every reingestor's inputs, validating each
//!   node's source, codec and route identities together, deciding how each route branches and
//!   where its ACK ends, and lowering its filters, route constructions and branch constructions
//!   into expression VM programs.
//! - **Depends on.** Validated schedule Models, the relays of the domain activation plan and the
//!   VM frontend.
//! - **Must not know.** Runtime tasks, node-local lookups, UDF executors, materialized-state
//!   bindings, connectors, or how the scheduler places a node.

use std::{collections::BTreeMap, num::NonZeroUsize, time::Duration};

use arrow_schema::Schema;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{
    AckMode, Assignment, BranchName, CodecName, CreateIngestor, CreateReingestor, ErrorPolicies,
    Expression, FlushPolicy, GeneralErrorPolicy, IngestorName, MessageErrorOperation,
    MessageErrorPolicy, Model, ModelKind, ModelName, NodeRef, OutputBranch, ProcessorOutput,
    ReingestorName, RelayName, ResolvedBranching, RouteConstruction, ScheduledNodes, SchemaName,
};
use nervix_primitives::sync::Arc;
use nervix_vm::{
    FrontendResult, SemanticScopePolicy, lower_branch_construction, lower_route_construction,
    lower_transforming_route,
    program::{Program, SpannedNode},
};
use thiserror::Error;

use super::{
    DomainActivationPlan, domain_activation_plan::PlannedRelay, ingestor_plan::IngestorStartPlan,
    reingestor_plan::ReingestorPlan,
};
use crate::runtime_schema::compile_schema;

/// Why the ingestors and reingestors of a schedule cannot be planned.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum EntrypointPlanError {
    #[error("scheduled {kind:?} '{node}' is configured as another node")]
    NodeIdentityMismatch { kind: ModelKind, node: ModelName },
    #[error("ingestor '{ingestor}' reads missing {kind:?} '{reference}'")]
    MissingSource {
        ingestor: IngestorName,
        kind: ModelKind,
        reference: ModelName,
    },
    #[error("ingestor '{ingestor}' reads its source through a {resolved:?} of another kind")]
    SourceKindMismatch {
        ingestor: IngestorName,
        resolved: ModelKind,
    },
    #[error("ingestor '{ingestor}' reads '{expected}', but its source resolved to '{resolved}'")]
    SourceIdentityMismatch {
        ingestor: IngestorName,
        expected: ModelName,
        resolved: ModelName,
    },
    #[error("ingestor '{ingestor}' decodes with missing codec '{codec}'")]
    MissingCodec {
        ingestor: IngestorName,
        codec: CodecName,
    },
    #[error("ingestor '{ingestor}' reads client batches of missing schema '{schema}'")]
    MissingClientSchema {
        ingestor: IngestorName,
        schema: SchemaName,
    },
    #[error("ingestor '{ingestor}' declares {clause} '{value}', which is not a positive duration")]
    InvalidClientDuration {
        ingestor: IngestorName,
        clause: &'static str,
        value: String,
    },
    #[error("ingestor '{ingestor}' has an endpoint contract canonical NSPL cannot render")]
    UnrenderableClientContract { ingestor: IngestorName },
    #[error("{kind:?} '{node}' declares no input relay")]
    MissingInput { kind: ModelKind, node: ModelName },
    #[error("{kind:?} '{node}' declares no output route")]
    MissingOutputRoute { kind: ModelKind, node: ModelName },
    #[error("{kind:?} '{node}' routes to missing relay '{relay}'")]
    MissingRouteRelay {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("{kind:?} '{node}' reads missing relay '{relay}'")]
    MissingInputRelay {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("{kind:?} '{node}' reads relay '{relay}', whose schema differs from its first input")]
    InputSchemaMismatch {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("{kind:?} '{node}' has an invalid {operation:?} predicate")]
    InvalidFilter {
        kind: ModelKind,
        node: ModelName,
        operation: MessageErrorOperation,
    },
    #[error("{kind:?} '{node}' route to '{relay}' has an invalid construction")]
    InvalidRouteConstruction {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("{kind:?} '{node}' route to '{relay}' declares a branch its relay is not branched by")]
    RouteBranchMismatch {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("{kind:?} '{node}' route to '{relay}' has an invalid branch construction")]
    InvalidBranchConstruction {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
}

/// A predicate that reads one input message, lowered into a VM program: an ingestor's
/// `FILTER WHERE`, or a reingestor's `FROM ... WHERE` or `FILTER WHERE`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredFilter {
    program: SpannedNode<Program>,
}

impl LoweredFilter {
    /// Lowers a predicate over the `input` scope of the message it filters.
    pub(crate) fn input(filter: &Expression) -> FrontendResult<Self> {
        let construction = RouteConstruction {
            where_clause: Some(filter.clone()),
            ..RouteConstruction::default()
        };
        let program =
            lower_route_construction(&construction, SemanticScopePolicy::read_only("input"))?;
        Ok(Self { program })
    }

    /// Lowers the predicate `owner` applies to each input message as `operation`.
    pub(in crate::registry) fn planned(
        owner: &EntrypointOwner<'_>,
        filter: &Expression,
        operation: MessageErrorOperation,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        Self::input(filter).change_context(EntrypointPlanError::InvalidFilter {
            kind: owner.kind(),
            node: owner.identifier().clone(),
            operation,
        })
    }

    pub(crate) fn program(&self) -> &SpannedNode<Program> {
        &self.program
    }
}

/// A transforming route construction lowered into a VM program, with inheritance already expanded
/// into the leading SET steps of that program.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredConstruction {
    program: SpannedNode<Program>,
    inherited: usize,
}

impl LoweredConstruction {
    /// Lowers a route that reads `input` rows and writes `output` rows.
    pub(crate) fn transforming(
        construction: &RouteConstruction,
        input: &Schema,
        output: &Schema,
    ) -> FrontendResult<Self> {
        let program = lower_transforming_route(construction, input, output)?;
        let inherited = program
            .inner
            .set
            .len()
            .checked_sub(construction.assignments.len())
            .assured(
                "a lowered transforming route lists one SET per inherited field before its \
                 assignments",
            );
        Ok(Self { program, inherited })
    }

    pub(crate) fn program(&self) -> &SpannedNode<Program> {
        &self.program
    }

    /// The operation each SET step of the program reports its failures as: the inherited fields
    /// first, then the route's own assignments.
    pub(crate) fn set_operations(&self) -> Vec<MessageErrorOperation> {
        let mut operations = Vec::with_capacity(self.program.inner.set.len());
        for index in 0..self.program.inner.set.len() {
            if index < self.inherited {
                operations.push(MessageErrorOperation::Inherit);
            } else {
                operations.push(MessageErrorOperation::Set);
            }
        }
        operations
    }
}

/// The ordered branch-key assignments of one route, lowered into a VM program that writes the
/// route's outgoing branch key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredBranchConstruction {
    program: SpannedNode<Program>,
}

impl LoweredBranchConstruction {
    pub(crate) fn new(
        assignments: &[Assignment],
        branch: &Schema,
        output: &Schema,
        input: &Schema,
    ) -> FrontendResult<Self> {
        let program = lower_branch_construction(assignments, branch, output, input)?;
        Ok(Self { program })
    }

    pub(crate) fn program(&self) -> &SpannedNode<Program> {
        &self.program
    }
}

/// Where the ACK of a record an entrypoint route writes ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BranchInstanceAckBoundary {
    /// The record keeps the ACK of the source message an ingestor received.
    Preserve,
    /// The record ends the ACK of the relay message a reingestor read, as its attachment declares.
    Reingestor(AckMode),
}

/// The ingestor or reingestor an entrypoint route belongs to, and the contracts all of its routes
/// share.
pub(in crate::registry) struct EntrypointOwner<'a> {
    kind: ModelKind,
    identifier: &'a ModelName,
    ack_boundary: BranchInstanceAckBoundary,
    general_error_policy: GeneralErrorPolicy,
}

impl<'a> EntrypointOwner<'a> {
    pub(in crate::registry) fn kind(&self) -> ModelKind {
        self.kind
    }

    pub(in crate::registry) fn identifier(&self) -> &ModelName {
        self.identifier
    }

    /// An ingestor's routes keep the ACK of the source message and report general failures through
    /// the ingestor's own policy.
    pub(in crate::registry) fn ingestor(
        identifier: &'a ModelName,
        ingestor: &CreateIngestor,
    ) -> Self {
        Self {
            kind: ModelKind::Ingestor,
            identifier,
            ack_boundary: BranchInstanceAckBoundary::Preserve,
            general_error_policy: ingestor.general_error_policy.clone(),
        }
    }

    /// A reingestor's routes end at the ACK boundary its attachment declares, and it has no general
    /// error policy of its own.
    pub(in crate::registry) fn reingestor(
        identifier: &'a ModelName,
        reingestor: &CreateReingestor,
    ) -> Self {
        Self {
            kind: ModelKind::Reingestor,
            identifier,
            ack_boundary: BranchInstanceAckBoundary::Reingestor(reingestor.mode),
            general_error_policy: GeneralErrorPolicy::Log,
        }
    }
}

/// The branch the records of one route belong to on the relay they are written to, and how that
/// relay retains its branches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RouteBranchRetention {
    pub(crate) branch: BranchName,
    /// How long a branch stays without traffic before it expires.
    pub(crate) ttl: Duration,
    /// How many branches the relay keeps before it evicts one, when its branch declaration bounds
    /// them.
    pub(crate) max_instances: Option<NonZeroUsize>,
}

impl RouteBranchRetention {
    /// How `relay`, which is branched by `branch`, retains the branches of that declaration.
    fn of(relay: &PlannedRelay, branch: &BranchName) -> Self {
        let ttl = relay
            .retention
            .branch_ttl
            .assured("the activation plan retains the branches of every branched relay for a TTL");
        Self {
            branch: branch.clone(),
            ttl,
            max_instances: relay.retention.branch_capacity,
        }
    }
}

/// How the records of one route receive their outgoing branch key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PlannedRouteBranch {
    /// The records leave unbranched for an unbranched relay.
    Unbranched,
    /// The records keep the branch key their input arrived with. An ingestor's input arrives
    /// unbranched.
    Preserved(RouteBranchRetention),
    /// The records receive the branch key this program constructs.
    Constructed {
        retention: RouteBranchRetention,
        program: LoweredBranchConstruction,
    },
}

impl PlannedRouteBranch {
    /// The branch the route's records belong to and how their relay retains it, unless they leave
    /// unbranched.
    pub(crate) fn retention(&self) -> Option<&RouteBranchRetention> {
        match self {
            Self::Unbranched => None,
            Self::Preserved(retention) | Self::Constructed { retention, .. } => Some(retention),
        }
    }
}

/// One output route of an ingestor or reingestor, lowered for the host to bind.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlannedEntryRoute {
    pub(crate) relay: RelayName,
    pub(crate) construction: LoweredConstruction,
    pub(crate) branch: PlannedRouteBranch,
    pub(crate) ack_boundary: BranchInstanceAckBoundary,
    pub(crate) flush_policy: FlushPolicy,
    pub(crate) error_policies: ErrorPolicies,
}

impl PlannedEntryRoute {
    pub(crate) fn message_error_policy(&self) -> &MessageErrorPolicy {
        &self.error_policies.message
    }
}

/// The relays the routes of one domain revision lower against.
pub(in crate::registry) struct EntrypointRouteContext<'a> {
    activation: &'a DomainActivationPlan,
}

impl EntrypointRouteContext<'_> {
    pub(in crate::registry) fn activation(&self) -> &DomainActivationPlan {
        self.activation
    }

    /// Plans every route of `owner`, whose input rows have the `input` schema.
    pub(in crate::registry) fn plan_routes<'b>(
        &self,
        owner: &EntrypointOwner<'_>,
        outputs: impl Iterator<Item = &'b ProcessorOutput>,
        input: &Schema,
    ) -> Result<Vec<PlannedEntryRoute>, Report<EntrypointPlanError>> {
        let mut routes = Vec::new();
        for output in outputs {
            routes.push(self.plan_route(owner, output, input)?);
        }
        if routes.is_empty() {
            return Err(Report::new(EntrypointPlanError::MissingOutputRoute {
                kind: owner.kind(),
                node: owner.identifier().clone(),
            }));
        }
        Ok(routes)
    }

    fn plan_route(
        &self,
        owner: &EntrypointOwner<'_>,
        output: &ProcessorOutput,
        input: &Schema,
    ) -> Result<PlannedEntryRoute, Report<EntrypointPlanError>> {
        let Some(relay) = self.activation.relays.get(&output.relay) else {
            return Err(Report::new(EntrypointPlanError::MissingRouteRelay {
                kind: owner.kind(),
                node: owner.identifier().clone(),
                relay: output.relay.clone(),
            }));
        };
        let output_schema = relay.schema.arrow_schema();
        let construction =
            LoweredConstruction::transforming(&output.construction, input, output_schema.as_ref())
                .change_context(EntrypointPlanError::InvalidRouteConstruction {
                    kind: owner.kind(),
                    node: owner.identifier().clone(),
                    relay: output.relay.clone(),
                })?;
        let declared_branch = output.branch.as_ref().assured(
            "the registry requires every route of these nodes to declare its branch behavior",
        );
        let branch = match (declared_branch, &relay.branching) {
            (OutputBranch::Unbranched, ResolvedBranching::Unbranched) => {
                PlannedRouteBranch::Unbranched
            }
            (
                OutputBranch::BranchedBy {
                    branch,
                    assignments,
                },
                ResolvedBranching::Branched {
                    branch: relay_branch,
                    schema: branch_schema,
                },
            ) if branch == relay_branch => {
                let retention = RouteBranchRetention::of(relay, branch);
                if assignments.is_empty() {
                    PlannedRouteBranch::Preserved(retention)
                } else {
                    let branch_schema = compile_schema(branch_schema).arrow_schema();
                    let program = LoweredBranchConstruction::new(
                        assignments,
                        branch_schema.as_ref(),
                        output_schema.as_ref(),
                        input,
                    )
                    .change_context(
                        EntrypointPlanError::InvalidBranchConstruction {
                            kind: owner.kind(),
                            node: owner.identifier().clone(),
                            relay: output.relay.clone(),
                        },
                    )?;
                    PlannedRouteBranch::Constructed { retention, program }
                }
            }
            _ => {
                return Err(Report::new(EntrypointPlanError::RouteBranchMismatch {
                    kind: owner.kind(),
                    node: owner.identifier().clone(),
                    relay: output.relay.clone(),
                }));
            }
        };
        let flush_policy = output
            .flush_policy
            .clone()
            .assured("the registry requires a flush policy on every ingestor and reingestor route");
        Ok(PlannedEntryRoute {
            relay: output.relay.clone(),
            construction,
            branch,
            ack_boundary: owner.ack_boundary,
            flush_policy,
            error_policies: ErrorPolicies {
                message: output.message_error_policy.clone(),
                general: owner.general_error_policy.clone(),
            },
        })
    }
}

/// The ingestor and reingestor decisions of one scheduled domain revision.
///
/// Every node of the revision is planned, wherever it executes, so the same decision serves the
/// node that starts an ingestor, the nodes that replicate its offsets, and the relays whose
/// consumers a reingestor subscribes. Plans are in memory only and are never persisted.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct EntrypointPlans {
    ingestors: BTreeMap<IngestorName, Arc<IngestorStartPlan>>,
    reingestors: BTreeMap<ReingestorName, Arc<ReingestorPlan>>,
}

impl EntrypointPlans {
    pub(crate) fn from_scheduled_nodes(
        domain: &nervix_models::DomainName,
        nodes: &ScheduledNodes,
        activation: &DomainActivationPlan,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        let routes = EntrypointRouteContext { activation };
        let mut plans = Self::default();
        for node in nodes.values() {
            match node.config.as_ref() {
                Model::Ingestor(ingestor) => {
                    if node.identifier != ModelName::from(&ingestor.name) {
                        return Err(Report::new(EntrypointPlanError::NodeIdentityMismatch {
                            kind: ModelKind::Ingestor,
                            node: node.identifier.clone(),
                        }));
                    }
                    let source_kind = ingestor.input.source_model_kind();
                    let source_ref = ingestor.input.source_ref();
                    let Some(source) = nodes.get(&NodeRef::new(source_kind, source_ref.clone()))
                    else {
                        return Err(Report::new(EntrypointPlanError::MissingSource {
                            ingestor: ingestor.name.clone(),
                            kind: source_kind,
                            reference: source_ref,
                        }));
                    };
                    let plan = IngestorStartPlan::decide(
                        domain,
                        node,
                        ingestor,
                        source.config.as_ref(),
                        &routes,
                    )?;
                    plans
                        .ingestors
                        .insert(ingestor.name.clone(), Arc::new(plan));
                }
                Model::Reingestor(reingestor) => {
                    if node.identifier != ModelName::from(&reingestor.name) {
                        return Err(Report::new(EntrypointPlanError::NodeIdentityMismatch {
                            kind: ModelKind::Reingestor,
                            node: node.identifier.clone(),
                        }));
                    }
                    let plan = ReingestorPlan::decide(reingestor, &routes)?;
                    plans
                        .reingestors
                        .insert(reingestor.name.clone(), Arc::new(plan));
                }
                _ => {}
            }
        }
        Ok(plans)
    }

    pub(crate) fn ingestor(&self, name: &IngestorName) -> Option<&Arc<IngestorStartPlan>> {
        self.ingestors.get(name)
    }

    pub(crate) fn ingestors(&self) -> impl Iterator<Item = &Arc<IngestorStartPlan>> {
        self.ingestors.values()
    }

    pub(crate) fn reingestor(&self, name: &ReingestorName) -> Option<&Arc<ReingestorPlan>> {
        self.reingestors.get(name)
    }

    pub(crate) fn reingestors(&self) -> impl Iterator<Item = &Arc<ReingestorPlan>> {
        self.reingestors.values()
    }

    /// Every route of every planned ingestor and reingestor.
    pub(crate) fn routes(&self) -> impl Iterator<Item = &PlannedEntryRoute> {
        let ingestor_routes = self
            .ingestors()
            .flat_map(|plan| plan.ingestor.routes.iter());
        let reingestor_routes = self.reingestors().flat_map(|plan| plan.routes.iter());
        ingestor_routes.chain(reingestor_routes)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use meticulous::ResultExt as _;
    use nervix_models::{
        AssignmentTarget, AssignmentTargetScope, BranchEviction, CreateBranch, FieldReference,
        FieldScope, ProcessorInputs, ProcessorOutputs,
    };

    use super::*;
    use crate::registry::test_fixtures::{
        branch_for_relay, branch_name_for_relay, branch_schema, client_model, codec, ingestor,
        ingestor_statement, ingestor_with_params, named, planned_entrypoints, reingestor,
        reingestor_statement, relay, relay_branched_by_relay_branch, schema, unplaced_schedule,
        wire_schema, with_inherit_all,
    };

    /// A payload relay the ingestor fills and a relay branched by its `value` field.
    fn domain_models() -> Vec<Model> {
        vec![
            schema("payload"),
            wire_schema("event_wire"),
            codec("json", "payload"),
            client_model("broker"),
            relay("events", "payload"),
            branch_schema("value_key", &["value"]),
            branch_for_relay("by_value", "value_key"),
            relay_branched_by_relay_branch("by_value", "payload"),
        ]
    }

    fn only_route(routes: &[PlannedEntryRoute]) -> &PlannedEntryRoute {
        assert_eq!(
            routes.len(),
            1,
            "the fixture node declares exactly one route"
        );
        &routes[0]
    }

    fn by_value_retention() -> RouteBranchRetention {
        RouteBranchRetention {
            branch: branch_name_for_relay("by_value"),
            ttl: Duration::from_secs(300),
            max_instances: None,
        }
    }

    fn assign_value_to(field: &str) -> Assignment {
        Assignment {
            target: AssignmentTarget {
                scope: AssignmentTargetScope::Output,
                field: named(field),
            },
            value: Expression::Field(FieldReference::scoped(FieldScope::Input, named("value"))),
        }
    }

    #[test]
    fn lowers_inherited_routes_and_decides_how_each_route_branches() {
        let mut models = domain_models();
        models.push(ingestor("unbranched_source", "events", "json", "broker"));
        models.push(ingestor_with_params(
            "branching_source",
            "by_value",
            "json",
            "broker",
            &["value"],
        ));
        models.push(reingestor("rebranch", "events", "by_value", &["value"]));
        let mut preserving = reingestor_statement("keep", "by_value", "by_value", &[]);
        // Preserving the incoming branch declares the same branch without a SET.
        preserving.output_routes.routes[0].branch = Some(OutputBranch::BranchedBy {
            branch: branch_name_for_relay("by_value"),
            assignments: Vec::new(),
        });
        models.push(Model::Reingestor(preserving));

        let plans = planned_entrypoints(models).assured("the fixture schedule plans");

        assert_eq!(plans.routes().count(), 4);
        let unbranched = plans
            .ingestor(&named("unbranched_source"))
            .assured("the unbranched ingestor is planned");
        let route = only_route(&unbranched.ingestor.routes);
        assert_eq!(route.relay, named("events"));
        assert_eq!(route.branch, PlannedRouteBranch::Unbranched);
        assert_eq!(route.ack_boundary, BranchInstanceAckBoundary::Preserve);
        assert_eq!(
            route.construction.set_operations(),
            vec![MessageErrorOperation::Inherit]
        );

        let branching = plans
            .ingestor(&named("branching_source"))
            .assured("the branching ingestor is planned");
        let route = only_route(&branching.ingestor.routes);
        assert!(matches!(
            route.branch,
            PlannedRouteBranch::Constructed { .. }
        ));
        if let PlannedRouteBranch::Constructed { program, .. } = &route.branch {
            assert_eq!(program.program().inner.set.len(), 1);
        }
        assert_eq!(route.branch.retention(), Some(&by_value_retention()));

        let rebranch = plans
            .reingestor(&named("rebranch"))
            .assured("the rebranching reingestor is planned");
        let route = only_route(&rebranch.routes);
        assert!(matches!(
            route.branch,
            PlannedRouteBranch::Constructed { .. }
        ));
        assert_eq!(
            route.ack_boundary,
            BranchInstanceAckBoundary::Reingestor(nervix_models::AckMode::Attached)
        );

        let keep = plans
            .reingestor(&named("keep"))
            .assured("the preserving reingestor is planned");
        assert_eq!(
            only_route(&keep.routes).branch,
            PlannedRouteBranch::Preserved(by_value_retention())
        );
    }

    #[test]
    fn a_branched_route_keeps_the_eviction_bound_of_its_relay_branch() {
        let mut models = domain_models();
        models.retain(|model| !matches!(model, Model::Branch(_)));
        models.push(Model::Branch(CreateBranch {
            name: branch_name_for_relay("by_value"),
            schema: named("value_key"),
            ttl: "90s".to_string(),
            eviction: Some(BranchEviction::Lru {
                max_instances: NonZeroU64::new(8)
                    .assured("the fixture's branch capacity is a nonzero literal"),
            }),
        }));
        models.push(ingestor_with_params(
            "source",
            "by_value",
            "json",
            "broker",
            &["value"],
        ));

        let plans = planned_entrypoints(models).assured("the fixture schedule plans");
        let route = only_route(
            &plans
                .ingestor(&named("source"))
                .assured("the ingestor is planned")
                .ingestor
                .routes,
        );

        assert_eq!(
            route.branch.retention(),
            Some(&RouteBranchRetention {
                branch: branch_name_for_relay("by_value"),
                ttl: Duration::from_secs(90),
                max_instances: NonZeroUsize::new(8),
            })
        );
    }

    #[test]
    fn a_route_with_assignments_reports_them_after_its_inherited_fields() {
        let mut models = domain_models();
        let mut source = ingestor_statement("source", "events", "json", "broker", &[]);
        source.output_routes.routes[0]
            .construction
            .assignments
            .push(assign_value_to("value"));
        models.push(Model::Ingestor(source));

        let plans = planned_entrypoints(models).assured("the fixture schedule plans");
        let route = only_route(
            &plans
                .ingestor(&named("source"))
                .assured("the ingestor is planned")
                .ingestor
                .routes,
        );

        assert_eq!(
            route.construction.set_operations(),
            vec![MessageErrorOperation::Inherit, MessageErrorOperation::Set]
        );
    }

    #[test]
    fn rejects_a_route_to_a_relay_that_is_not_scheduled() {
        let mut models = domain_models();
        models.push(ingestor("source", "missing", "json", "broker"));

        let error = planned_entrypoints(models).expect_err("the route relay is required");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::MissingRouteRelay {
                kind: ModelKind::Ingestor,
                node: named("source"),
                relay: named("missing"),
            }
        );
    }

    #[test]
    fn rejects_a_route_that_sets_a_field_its_relay_does_not_have() {
        let mut models = domain_models();
        let mut source = ingestor_statement("source", "events", "json", "broker", &[]);
        source.output_routes.routes[0]
            .construction
            .assignments
            .push(assign_value_to("unknown"));
        models.push(Model::Ingestor(source));

        let error = planned_entrypoints(models).expect_err("the SET target is unknown");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::InvalidRouteConstruction {
                kind: ModelKind::Ingestor,
                node: named("source"),
                relay: named("events"),
            }
        );
    }

    #[test]
    fn rejects_a_branch_construction_toward_an_unbranched_relay() {
        let mut models = domain_models();
        let mut source = ingestor_statement("source", "by_value", "json", "broker", &["value"]);
        source.output_routes.routes[0].relay = named("events");
        models.push(Model::Ingestor(source));

        let error = planned_entrypoints(models).expect_err("the relay has no branch key");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::RouteBranchMismatch {
                kind: ModelKind::Ingestor,
                node: named("source"),
                relay: named("events"),
            }
        );
    }

    #[test]
    fn rejects_an_unbranched_route_toward_a_branched_relay() {
        let mut models = domain_models();
        models.push(reingestor("flatten", "events", "by_value", &[]));

        let error = planned_entrypoints(models).expect_err("the relay is branched");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::RouteBranchMismatch {
                kind: ModelKind::Reingestor,
                node: named("flatten"),
                relay: named("by_value"),
            }
        );
    }

    #[test]
    fn rejects_a_node_without_output_routes() {
        let mut models = domain_models();
        models.push(Model::Reingestor(CreateReingestor {
            name: named("silent"),
            from: ProcessorInputs::single(named("events")),
            output_routes: with_inherit_all(ProcessorOutputs::new(Vec::new()))
                .with_flush_policy(FlushPolicy::Immediate),
            mode: nervix_models::AckMode::Attached,
            materialized_state: Vec::new(),
            filter_where: None,
        }));

        let error = planned_entrypoints(models).expect_err("a route is required");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::MissingOutputRoute {
                kind: ModelKind::Reingestor,
                node: named("silent"),
            }
        );
    }

    #[rstest::rstest]
    #[case::ingestor(
        ingestor("source", "events", "json", "broker"),
        ModelKind::Ingestor,
        "source"
    )]
    #[case::reingestor(
        reingestor("repartition", "events", "events", &[]),
        ModelKind::Reingestor,
        "repartition"
    )]
    fn rejects_a_scheduled_node_configured_as_another_node(
        #[case] entrypoint: Model,
        #[case] kind: ModelKind,
        #[case] name: &str,
    ) {
        let mut models = domain_models();
        models.push(entrypoint);
        let mut nodes = unplaced_schedule(models);
        let mut node = nodes
            .shift_remove(&NodeRef::new(kind, named::<ModelName>(name)))
            .assured("the fixture schedules the node under its own name");
        node.identifier = named("renamed");
        nodes.insert(node.identity(), node);
        let domain = named::<nervix_models::DomainName>("sales");
        let activation = DomainActivationPlan::from_scheduled_nodes(&domain, &nodes)
            .assured("the fixture surfaces resolve");

        let error = EntrypointPlans::from_scheduled_nodes(&domain, &nodes, &activation)
            .expect_err("the node is configured under another name");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::NodeIdentityMismatch {
                kind,
                node: named("renamed"),
            }
        );
    }
}
