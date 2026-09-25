//! Transaction inspection rendering tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that TEXT and JSON render the same inspected report and that selecting
//!   an operation reorders the report without narrowing it.
//! - **Depends on.** The rendering under test and hand-built impact reports.
//! - **Must not know.** Production ownership beyond the parent module under test.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ActivationAction, ActivationImpact, ActualExecutionStepImpact, ActualQuiescence,
    AffectedTopology, AttributedGateBoundary, AttributedImpactNode, BranchKeyFingerprint,
    BranchName, CanonicalImpactSet, ClusterNodeName, ConcreteBranchCoverage, ConfigurationImpact,
    ConfigurationTransition, DomainLifecycleAction, DomainLifecycleImpact, DomainName,
    ExecutionStepImpactReport, ExecutionStepOutcome, ForceFlushImpact, ImpactAttribution,
    ImpactDiagnostic, ImpactDiagnosticKind, ImpactEffects, ImpactGateBoundary, ImpactNodeCoverage,
    ImpactPlanningBasis, ImpactReportCompleteness, ImpactTopology, InspectionFormat,
    ModelChangeAspect, ModelKind, NodeRef, OperationImpactReason, OperationImpactReport,
    OwnershipMoveImpact, PauseRequirement, PlannedExecutionStepImpact, QuiesceSubgraph,
    QuiescenceOutcome, RebuildImpact, RebuildReason, RelayName, RequestedResourceVersion,
    ResourceBindingImpact, ResourceCatalogAction, ResourceCatalogImpact, ResourceName, StatePurge,
    StateResetImpact, TransactionImpactReport, TransactionInspection, TransactionLifecycle,
    TransactionOperation, TransactionOperationNumber, TransactionOperationRange,
    TransactionPosition, TransactionStatus,
};

use super::InspectionRendering;

fn operation(number: usize) -> TransactionOperationNumber {
    TransactionOperationNumber::from_index(
        number
            .checked_sub(1)
            .assured("a test operation number is one-based"),
    )
    .assured("a test operation number is addressable")
}

fn domain() -> DomainName {
    DomainName::parse("payments").assured("the fixture domain is an accepted literal")
}

fn node(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(
        kind,
        nervix_models::ModelName::parse(name).assured("the fixture names are accepted literals"),
    )
}

fn step_range(first: usize, last: usize) -> TransactionOperationRange {
    TransactionOperationRange::new(operation(first), operation(last))
        .assured("the fixture ranges are ordered")
}

/// Two operations applied as one entity-paused step, and a third that starts the domain alone.
fn three_operation_report(completeness: ImpactReportCompleteness) -> TransactionImpactReport {
    let schema = node(ModelKind::Schema, "orders");
    let junction = node(ModelKind::Junction, "enrich");
    let paused_step = step_range(1, 2);
    let lifecycle_step = step_range(3, 3);
    let created = ConfigurationImpact {
        transition: ConfigurationTransition::Created {
            node: schema.clone(),
        },
        attribution: ImpactAttribution::single(operation(1)),
    };
    let changed = ConfigurationImpact {
        transition: ConfigurationTransition::Changed {
            node: junction.clone(),
        },
        attribution: ImpactAttribution::single(operation(2)),
    };
    let rebuild = RebuildImpact {
        node: ImpactNodeCoverage::all_executions(junction.clone()),
        reason: RebuildReason::Configuration,
        attribution: ImpactAttribution::single(operation(2)),
    };
    let scope = QuiesceSubgraph::new(
        domain(),
        [AttributedImpactNode {
            coverage: ImpactNodeCoverage::all_executions(junction.clone()),
            attribution: ImpactAttribution::single(operation(2)),
        }],
        [AttributedGateBoundary {
            boundary: ImpactGateBoundary {
                relay: RelayName::parse("orders_in").assured("the fixture relay is accepted"),
                branches: ConcreteBranchCoverage::All,
            },
            attribution: ImpactAttribution::single(operation(2)),
        }],
    );
    let operations = vec![
        OperationImpactReport {
            number: operation(1),
            operation: TransactionOperation::CreateConfiguration {
                domain: domain(),
                node: schema,
            },
            execution_step: paused_step,
            completeness: ImpactReportCompleteness::Complete,
            reasons: Vec::new(),
            contribution: ImpactEffects {
                changed_configuration: CanonicalImpactSet::new([created.clone()]),
                ..ImpactEffects::default()
            },
        },
        OperationImpactReport {
            number: operation(2),
            operation: TransactionOperation::AlterConfiguration {
                domain: domain(),
                node: junction.clone(),
            },
            execution_step: paused_step,
            completeness: ImpactReportCompleteness::Complete,
            reasons: vec![OperationImpactReason::Configuration {
                node: junction,
                aspect: ModelChangeAspect::ProcessorRoutes,
            }],
            contribution: ImpactEffects {
                changed_configuration: CanonicalImpactSet::new([changed.clone()]),
                rebuilds: CanonicalImpactSet::new([rebuild.clone()]),
                ..ImpactEffects::default()
            },
        },
        OperationImpactReport {
            number: operation(3),
            operation: TransactionOperation::StartDomain { domain: domain() },
            execution_step: lifecycle_step,
            completeness: ImpactReportCompleteness::Complete,
            reasons: vec![OperationImpactReason::DomainStart],
            contribution: ImpactEffects::default(),
        },
    ];
    let steps = vec![
        ExecutionStepImpactReport::new(
            paused_step,
            PlannedExecutionStepImpact {
                completeness: ImpactReportCompleteness::Complete,
                pause: PauseRequirement::Subgraph { scope },
                effects: ImpactEffects {
                    changed_configuration: CanonicalImpactSet::new([created, changed]),
                    rebuilds: CanonicalImpactSet::new([rebuild]),
                    ..ImpactEffects::default()
                },
            },
            ActualExecutionStepImpact::unattempted(),
        ),
        ExecutionStepImpactReport::new(
            lifecycle_step,
            PlannedExecutionStepImpact {
                completeness: ImpactReportCompleteness::Complete,
                pause: PauseRequirement::NoPause,
                effects: ImpactEffects::default(),
            },
            ActualExecutionStepImpact::unattempted(),
        ),
    ];
    TransactionImpactReport::new(
        domain(),
        TransactionPosition::new(3),
        ImpactPlanningBasis::new([0xab; 32]),
        completeness,
        operations,
        steps,
    )
    .assured("the fixture numbers its operations in order and covers each with one step")
}

fn inspection(
    lifecycle: TransactionLifecycle,
    applied_operations: usize,
    selected: Option<TransactionOperationNumber>,
    report: TransactionImpactReport,
) -> TransactionInspection {
    let transaction = TransactionStatus::new(
        "0199c1a0-7c1e".to_string(),
        domain(),
        lifecycle,
        report.position(),
        applied_operations,
    )
    .assured("the fixture never applies more operations than it accepted");
    TransactionInspection {
        transaction,
        operation: selected,
        report,
    }
}

fn planning_basis_line() -> String {
    format!("planning basis: {}", "ab".repeat(32))
}

#[test]
fn text_renders_identity_scope_operations_and_steps_in_order() {
    let inspection = inspection(
        TransactionLifecycle::Open,
        0,
        None,
        three_operation_report(ImpactReportCompleteness::Complete),
    );

    let text = InspectionRendering::new(&inspection).render(InspectionFormat::Text);

    let expected = [
        "transaction: 0199c1a0-7c1e",
        "domain: payments",
        "state: OPEN",
        "operations: 3 accepted, 0 applied, 3 pending",
        "report: COMPLETE",
        &planning_basis_line(),
        "quiesce level: ENTITY_PAUSE",
        "pause: SUBGRAPH nodes=1 gates=1",
        "  node kind=junction name=enrich branches=ALL operations=2",
        "  gate relay=orders_in branches=ALL operations=2",
        "operation 1: CREATE_CONFIGURATION kind=schema name=orders",
        "  execution step: 1-2",
        "operation 2: ALTER_CONFIGURATION kind=junction name=enrich",
        "  execution step: 1-2",
        "  reason: CONFIGURATION kind=junction name=enrich aspect=PROCESSOR_ROUTES",
        "operation 3: START_DOMAIN domain=payments",
        "  execution step: 3",
        "  reason: DOMAIN_START",
        "execution step 1-2: planned ENTITY_PAUSE, actual DYNAMIC, outcome UNATTEMPTED",
        "  pause: SUBGRAPH nodes=1 gates=1",
        "    node kind=junction name=enrich branches=ALL operations=2",
        "    gate relay=orders_in branches=ALL operations=2",
        "  effect: configuration CREATED kind=schema name=orders operations=1",
        "  effect: configuration CHANGED kind=junction name=enrich operations=2",
        "  effect: rebuild CONFIGURATION kind=junction name=enrich branches=ALL operations=2",
        "execution step 3: planned DYNAMIC, actual DYNAMIC, outcome UNATTEMPTED",
        "  pause: NO_PAUSE",
    ]
    .join("\n");
    assert_eq!(text, expected);
}

#[test]
fn a_selected_operation_leads_with_its_contribution_and_step_without_narrowing_the_report() {
    let inspection = inspection(
        TransactionLifecycle::Open,
        0,
        Some(operation(2)),
        three_operation_report(ImpactReportCompleteness::Complete),
    );

    let text = InspectionRendering::new(&inspection).render(InspectionFormat::Text);
    let lines = text.lines().collect::<Vec<_>>();

    let selected = lines
        .iter()
        .position(|line| *line == "inspected operation: 2")
        .verified("the selected operation is named");
    assert_eq!(
        &lines[selected + 1..selected + 6],
        [
            "operation 2: ALTER_CONFIGURATION kind=junction name=enrich",
            "  execution step: 1-2",
            "  reason: CONFIGURATION kind=junction name=enrich aspect=PROCESSOR_ROUTES",
            "  contribution: configuration CHANGED kind=junction name=enrich operations=2",
            "  contribution: rebuild CONFIGURATION kind=junction name=enrich branches=ALL \
             operations=2",
        ]
    );
    assert_eq!(
        lines[selected + 6],
        "execution step 1-2: planned ENTITY_PAUSE, actual DYNAMIC, outcome UNATTEMPTED"
    );
    let summary = lines
        .iter()
        .position(|line| *line == "quiesce level: ENTITY_PAUSE")
        .verified("the whole transaction's requirement is still reported");
    assert!(summary > selected, "the selected operation comes first");
    for rest in [
        "operation 1: CREATE_CONFIGURATION kind=schema name=orders",
        "operation 3: START_DOMAIN domain=payments",
        "execution step 3: planned DYNAMIC, actual DYNAMIC, outcome UNATTEMPTED",
    ] {
        assert_eq!(
            lines.iter().filter(|line| **line == rest).count(),
            1,
            "{rest:?} is reported exactly once"
        );
    }
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.starts_with("operation 2:"))
            .count(),
        1,
        "the selected operation is not repeated"
    );
}

#[test]
fn a_failed_transaction_and_an_incomplete_report_say_so() {
    let diagnostic = ImpactDiagnostic {
        kind: ImpactDiagnosticKind::Topology,
        operation: Some(operation(2)),
        message: "relay 'orders_in' is not yet resolved".to_string(),
    };
    let completeness = ImpactReportCompleteness::incomplete(vec![diagnostic])
        .assured("the fixture names what remains unresolved");
    let mut failed = inspection(
        TransactionLifecycle::Failed {
            failing_operation: operation(3),
            error: "domain start refused".to_string(),
        },
        2,
        None,
        three_operation_report(completeness),
    );
    let steps = failed.report.execution_steps().to_vec();
    let mut applied = steps[0].actual().clone();
    applied.outcome = ExecutionStepOutcome::Applied;
    let mut refused = steps[1].actual().clone();
    refused.outcome = ExecutionStepOutcome::Failed {
        diagnostic: ImpactDiagnostic {
            kind: ImpactDiagnosticKind::Application,
            operation: Some(operation(3)),
            message: "domain start refused".to_string(),
        },
    };
    let mut recorded = Vec::with_capacity(steps.len());
    for (mut step, actual) in steps.into_iter().zip([applied, refused]) {
        *step.actual_mut() = actual;
        recorded.push(step);
    }
    failed.report = TransactionImpactReport::new(
        failed.report.domain().clone(),
        failed.report.position(),
        failed.report.planning_basis(),
        failed.report.completeness().clone(),
        failed.report.operations().to_vec(),
        recorded,
    )
    .assured("recording outcomes keeps the report's numbering");

    let text = InspectionRendering::new(&failed).render(InspectionFormat::Text);

    for expected in [
        "state: FAILED",
        "failing operation: 3",
        "error: domain start refused",
        "operations: 3 accepted, 2 applied, 0 pending",
        "report: INCOMPLETE",
        "- kind=TOPOLOGY operation=2 relay 'orders_in' is not yet resolved",
        "execution step 1-2: planned ENTITY_PAUSE, actual DYNAMIC, outcome APPLIED",
        "execution step 3: planned DYNAMIC, actual DYNAMIC, outcome FAILED",
        "  failure: kind=APPLICATION operation=3 domain start refused",
    ] {
        assert!(
            text.lines().any(|line| line == expected),
            "{expected:?} missing from:\n{text}"
        );
    }
}

#[test]
fn json_renders_the_same_typed_inspection() {
    let inspection = inspection(
        TransactionLifecycle::Open,
        0,
        Some(operation(2)),
        three_operation_report(ImpactReportCompleteness::Complete),
    );

    let json = InspectionRendering::new(&inspection).render(InspectionFormat::Json);
    let document: serde_json::Value =
        serde_json::from_str(&json).assured("FORMAT JSON prints one JSON document");

    assert_eq!(document["transaction"]["transaction_id"], "0199c1a0-7c1e");
    assert_eq!(document["transaction"]["domain"], "payments");
    assert_eq!(document["transaction"]["state"], "OPEN");
    assert_eq!(document["transaction"]["accepted_operations"], 3);
    assert_eq!(document["transaction"]["applied_operations"], 0);
    assert_eq!(document["operation"], 2);
    assert_eq!(
        document["report"]["planning_basis"],
        "ab".repeat(32),
        "JSON spells the planning basis the way TEXT does"
    );
    let report: TransactionImpactReport = serde_json::from_value(document["report"].clone())
        .assured("the JSON report is the typed report's own representation");
    assert_eq!(report, inspection.report);
}

#[test]
fn json_names_a_failure_inline_and_an_unselected_operation_as_null() {
    let inspection = inspection(
        TransactionLifecycle::Failed {
            failing_operation: operation(3),
            error: "domain start refused".to_string(),
        },
        2,
        None,
        three_operation_report(ImpactReportCompleteness::Complete),
    );

    let json = InspectionRendering::new(&inspection).render(InspectionFormat::Json);
    let document: serde_json::Value =
        serde_json::from_str(&json).assured("FORMAT JSON prints one JSON document");

    assert_eq!(document["transaction"]["state"], "FAILED");
    assert_eq!(document["transaction"]["failing_operation"], 3);
    assert_eq!(document["transaction"]["error"], "domain start refused");
    assert!(document["operation"].is_null());
}

fn relay_node(name: &str) -> NodeRef {
    node(ModelKind::Relay, name)
}

fn resource(name: &str) -> ResourceName {
    ResourceName::parse(name).assured("the fixture resource name is an accepted literal")
}

fn branch(name: &str) -> BranchName {
    BranchName::parse(name).assured("the fixture branch name is an accepted literal")
}

fn cluster_node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the fixture node name is an accepted literal")
}

fn unresolved(operation: Option<TransactionOperationNumber>) -> ImpactReportCompleteness {
    ImpactReportCompleteness::incomplete(vec![ImpactDiagnostic {
        kind: ImpactDiagnosticKind::Planning,
        operation,
        message: "the prefix is still being planned".to_string(),
    }])
    .assured("the fixture names what remains unresolved")
}

/// One operation for each operation kind the text names, each executed as its own step, with every
/// effect, coverage and actual outcome a step can carry.
fn every_kind_report() -> TransactionImpactReport {
    let dropped = relay_node("retired_events");
    let rebound = node(ModelKind::Inferencer, "score");
    let enrich = node(ModelKind::Junction, "enrich");
    let bundle = resource("bundle");
    let selected = ConcreteBranchCoverage::selected(
        branch("tenant"),
        [
            BranchKeyFingerprint::new([1; 32]),
            BranchKeyFingerprint::new([2; 32]),
        ],
    )
    .assured("the fixture selects two concrete branch keys");
    let operations = vec![
        OperationImpactReport {
            number: operation(1),
            operation: TransactionOperation::DropConfiguration {
                domain: domain(),
                node: dropped.clone(),
            },
            execution_step: step_range(1, 1),
            completeness: unresolved(Some(operation(1))),
            reasons: vec![OperationImpactReason::Configuration {
                node: dropped.clone(),
                aspect: ModelChangeAspect::RelaySchema,
            }],
            contribution: ImpactEffects::default(),
        },
        OperationImpactReport {
            number: operation(2),
            operation: TransactionOperation::AlterDomain { domain: domain() },
            execution_step: step_range(2, 2),
            completeness: ImpactReportCompleteness::Complete,
            reasons: vec![OperationImpactReason::DomainPlacement],
            contribution: ImpactEffects::default(),
        },
        OperationImpactReport {
            number: operation(3),
            operation: TransactionOperation::StopDomain { domain: domain() },
            execution_step: step_range(3, 3),
            completeness: ImpactReportCompleteness::Complete,
            reasons: vec![OperationImpactReason::DomainStop],
            contribution: ImpactEffects::default(),
        },
        OperationImpactReport {
            number: operation(4),
            operation: TransactionOperation::CreateResource {
                domain: domain(),
                resource: bundle.clone(),
            },
            execution_step: step_range(4, 4),
            completeness: ImpactReportCompleteness::Complete,
            reasons: vec![OperationImpactReason::ResourceCatalog {
                resource: bundle.clone(),
            }],
            contribution: ImpactEffects::default(),
        },
        OperationImpactReport {
            number: operation(5),
            operation: TransactionOperation::RebindResource {
                domain: domain(),
                resource: bundle.clone(),
                requested: RequestedResourceVersion::Latest,
                version: 3,
            },
            execution_step: step_range(5, 5),
            completeness: ImpactReportCompleteness::Complete,
            reasons: vec![OperationImpactReason::ResourceRebinding {
                node: rebound.clone(),
                resource: bundle.clone(),
                from_version: 2,
                to_version: 3,
            }],
            contribution: ImpactEffects::default(),
        },
    ];
    let first_effects = ImpactEffects {
        changed_configuration: CanonicalImpactSet::new([ConfigurationImpact {
            transition: ConfigurationTransition::Dropped {
                node: dropped.clone(),
            },
            attribution: ImpactAttribution::single(operation(1)),
        }]),
        topology: AffectedTopology {
            before: ImpactTopology {
                nodes: CanonicalImpactSet::new([AttributedImpactNode {
                    coverage: ImpactNodeCoverage::configuration(dropped.clone()),
                    attribution: ImpactAttribution::single(operation(1)),
                }]),
                edges: CanonicalImpactSet::default(),
            },
            after: ImpactTopology::default(),
        },
        ownership_moves: CanonicalImpactSet::new([OwnershipMoveImpact {
            node: ImpactNodeCoverage::execution(
                enrich.clone(),
                ConcreteBranchCoverage::AllOfBranch {
                    branch: branch("tenant"),
                },
            ),
            source: cluster_node("node-1"),
            destination: cluster_node("node-2"),
            attribution: ImpactAttribution::single(operation(1)),
        }]),
        activations: CanonicalImpactSet::new([ActivationImpact {
            node: ImpactNodeCoverage::execution(
                dropped.clone(),
                ConcreteBranchCoverage::Unbranched,
            ),
            action: ActivationAction::Deactivate,
            attribution: ImpactAttribution::single(operation(1)),
        }]),
        rebuilds: CanonicalImpactSet::new([RebuildImpact {
            node: ImpactNodeCoverage::execution(enrich.clone(), selected),
            reason: RebuildReason::Ownership,
            attribution: ImpactAttribution::single(operation(1)),
        }]),
        state_resets: CanonicalImpactSet::new([StateResetImpact {
            node: ImpactNodeCoverage::all_executions(enrich.clone()),
            state: StatePurge::WindowAccumulator,
            attribution: ImpactAttribution::single(operation(1)),
        }]),
        force_flushes: CanonicalImpactSet::new([ForceFlushImpact {
            node: ImpactNodeCoverage::configuration(enrich),
            attribution: ImpactAttribution::single(operation(1)),
        }]),
        ..ImpactEffects::default()
    };
    let lifecycle_effects = ImpactEffects {
        lifecycle: CanonicalImpactSet::new([DomainLifecycleImpact {
            domain: domain(),
            action: DomainLifecycleAction::Stop,
            attribution: ImpactAttribution::single(operation(3)),
        }]),
        ..ImpactEffects::default()
    };
    let catalog_effects = ImpactEffects {
        resource_catalog: CanonicalImpactSet::new([ResourceCatalogImpact {
            resource: bundle.clone(),
            action: ResourceCatalogAction::Create,
            attribution: ImpactAttribution::single(operation(4)),
        }]),
        ..ImpactEffects::default()
    };
    let binding_effects = ImpactEffects {
        resource_bindings: CanonicalImpactSet::new([ResourceBindingImpact {
            node: rebound,
            resource: bundle,
            requested: RequestedResourceVersion::Latest,
            version: 3,
            attribution: ImpactAttribution::single(operation(5)),
        }]),
        ..ImpactEffects::default()
    };
    let domain_pause = PauseRequirement::Domain { domain: domain() };
    let applied = ActualExecutionStepImpact {
        outcome: ExecutionStepOutcome::Applied,
        quiescence: vec![ActualQuiescence {
            requirement: domain_pause.clone(),
            outcomes: vec![
                QuiescenceOutcome::Requested,
                QuiescenceOutcome::Confirmed,
                QuiescenceOutcome::Released,
            ],
        }],
        effects: ImpactEffects::default(),
    };
    let refused = ActualExecutionStepImpact {
        outcome: ExecutionStepOutcome::Failed {
            diagnostic: ImpactDiagnostic {
                kind: ImpactDiagnosticKind::Quiescence,
                operation: None,
                message: "the domain did not pause".to_string(),
            },
        },
        quiescence: vec![ActualQuiescence {
            requirement: domain_pause.clone(),
            outcomes: vec![
                QuiescenceOutcome::Requested,
                QuiescenceOutcome::Failed {
                    diagnostic: ImpactDiagnostic {
                        kind: ImpactDiagnosticKind::Quiescence,
                        operation: Some(operation(2)),
                        message: "pause refused".to_string(),
                    },
                },
            ],
        }],
        effects: ImpactEffects::default(),
    };
    let uncertain = ActualExecutionStepImpact {
        outcome: ExecutionStepOutcome::Applying,
        quiescence: vec![ActualQuiescence {
            requirement: domain_pause.clone(),
            outcomes: vec![QuiescenceOutcome::Uncertain {
                diagnostic: ImpactDiagnostic {
                    kind: ImpactDiagnosticKind::Recovery,
                    operation: None,
                    message: "leadership changed".to_string(),
                },
            }],
        }],
        effects: ImpactEffects::default(),
    };
    let planned = |pause: PauseRequirement, effects: ImpactEffects| PlannedExecutionStepImpact {
        completeness: ImpactReportCompleteness::Complete,
        pause,
        effects,
    };
    let steps = vec![
        ExecutionStepImpactReport::new(
            step_range(1, 1),
            PlannedExecutionStepImpact {
                completeness: unresolved(Some(operation(1))),
                pause: domain_pause.clone(),
                effects: first_effects,
            },
            applied,
        ),
        ExecutionStepImpactReport::new(
            step_range(2, 2),
            planned(domain_pause.clone(), ImpactEffects::default()),
            refused,
        ),
        ExecutionStepImpactReport::new(
            step_range(3, 3),
            planned(domain_pause, lifecycle_effects),
            uncertain,
        ),
        ExecutionStepImpactReport::new(
            step_range(4, 4),
            planned(PauseRequirement::NoPause, catalog_effects),
            ActualExecutionStepImpact::unattempted(),
        ),
        ExecutionStepImpactReport::new(
            step_range(5, 5),
            planned(PauseRequirement::NoPause, binding_effects),
            ActualExecutionStepImpact::unattempted(),
        ),
    ];
    TransactionImpactReport::new(
        domain(),
        TransactionPosition::new(5),
        ImpactPlanningBasis::new([0xcd; 32]),
        unresolved(None),
        operations,
        steps,
    )
    .assured("the fixture numbers its operations in order and covers each with one step")
}

#[test]
fn text_names_every_operation_reason_effect_and_outcome() {
    let inspection = inspection(
        TransactionLifecycle::Committing,
        1,
        None,
        every_kind_report(),
    );

    let text = InspectionRendering::new(&inspection).render(InspectionFormat::Text);

    for expected in [
        "state: COMMITTING",
        "operations: 5 accepted, 1 applied, 4 pending",
        "report: INCOMPLETE",
        "- kind=PLANNING the prefix is still being planned",
        "quiesce level: DOMAIN_PAUSE",
        "pause: DOMAIN domain=payments",
        "operation 1: DROP_CONFIGURATION kind=relay name=retired_events",
        "  incomplete: kind=PLANNING operation=1 the prefix is still being planned",
        "  reason: CONFIGURATION kind=relay name=retired_events aspect=RELAY_SCHEMA",
        "operation 2: ALTER_DOMAIN domain=payments",
        "  reason: DOMAIN_PLACEMENT",
        "operation 3: STOP_DOMAIN domain=payments",
        "  reason: DOMAIN_STOP",
        "operation 4: CREATE_RESOURCE resource=bundle",
        "  reason: RESOURCE_CATALOG resource=bundle",
        "operation 5: REBIND_RESOURCE resource=bundle version=3 requested=LATEST",
        "  reason: RESOURCE_REBINDING kind=inferencer name=score resource=bundle from=2 to=3",
        "execution step 1: planned DOMAIN_PAUSE, actual DOMAIN_PAUSE, outcome APPLIED",
        "  pause: DOMAIN domain=payments",
        "  effect: configuration DROPPED kind=relay name=retired_events operations=1",
        "  effect: topology before nodes=1 edges=0 after nodes=0 edges=0",
        "  effect: ownership move kind=junction name=enrich branches=ALL_OF_BRANCH branch=tenant \
         from=node-1 to=node-2 operations=1",
        "  effect: activation DEACTIVATE kind=relay name=retired_events branches=UNBRANCHED \
         operations=1",
        "  effect: rebuild OWNERSHIP kind=junction name=enrich branches=SELECTED branch=tenant \
         keys=2 operations=1",
        "  effect: state reset WINDOW_ACCUMULATOR kind=junction name=enrich branches=ALL \
         operations=1",
        "  effect: force flush kind=junction name=enrich operations=1",
        "  quiescence: DOMAIN domain=payments REQUESTED, CONFIRMED, RELEASED",
        "execution step 2: planned DOMAIN_PAUSE, actual DYNAMIC, outcome FAILED",
        "  quiescence: DOMAIN domain=payments REQUESTED, FAILED (pause refused)",
        "  failure: kind=QUIESCENCE the domain did not pause",
        "execution step 3: planned DOMAIN_PAUSE, actual DOMAIN_PAUSE, outcome APPLYING",
        "  effect: lifecycle STOP domain=payments operations=3",
        "  quiescence: DOMAIN domain=payments UNCERTAIN (leadership changed)",
        "execution step 4: planned DYNAMIC, actual DYNAMIC, outcome UNATTEMPTED",
        "  effect: resource CREATE resource=bundle operations=4",
        "execution step 5: planned DYNAMIC, actual DYNAMIC, outcome UNATTEMPTED",
        "  effect: resource binding kind=inferencer name=score resource=bundle version=3 \
         requested=LATEST operations=5",
    ] {
        assert!(
            text.lines().any(|line| line == expected),
            "{expected:?} missing from:\n{text}"
        );
    }
    let step_one = text
        .lines()
        .position(|line| line.starts_with("execution step 1:"))
        .verified("the first step is rendered");
    assert_eq!(
        text.lines().nth(step_one + 2),
        Some("  incomplete: kind=PLANNING operation=1 the prefix is still being planned"),
        "an incomplete step says what it could not resolve"
    );
}

#[test]
fn json_keeps_every_kind_of_the_same_report() {
    let inspection = inspection(
        TransactionLifecycle::Committing,
        1,
        Some(operation(5)),
        every_kind_report(),
    );

    let json = InspectionRendering::new(&inspection).render(InspectionFormat::Json);
    let document: serde_json::Value =
        serde_json::from_str(&json).assured("FORMAT JSON prints one JSON document");
    let report: TransactionImpactReport = serde_json::from_value(document["report"].clone())
        .assured("the JSON report is the typed report's own representation");

    assert_eq!(report, inspection.report);
    assert_eq!(document["transaction"]["state"], "COMMITTING");
    assert_eq!(document["operation"], 5);
    let rebuild_keys = &document["report"]["execution_steps"][0]["planned"]["effects"]["rebuilds"]
        [0]["node"]["branches"]["keys"];
    assert_eq!(
        rebuild_keys[0],
        "01".repeat(32),
        "branch-key fingerprints are hexadecimal like the planning basis"
    );
}
