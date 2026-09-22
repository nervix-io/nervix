use nervix_models::{
    ActualExecutionStepImpact, ActualQuiescence, AttributedGateBoundary, AttributedImpactNode,
    BranchKeyFingerprint, CanonicalImpactSet, ConfigurationImpact, ForceFlushImpact,
    ImpactGateBoundary, ImpactNodeCoverage, ImpactPlanningBasis, ImpactReportCompleteness,
    ImpactTopologyEdge, ModelName, PlannedExecutionStepImpact, QuiesceSubgraph, RelayName,
    ResourceBindingImpact, ResourceCatalogImpact, TransactionOperation, TransactionPosition,
};

use super::*;
use crate::graph::layout::EdgeTravel;

fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).expect("the test passes a valid Nervix name")
}

fn domain() -> DomainName {
    named("tenant")
}

fn op(number: usize) -> TransactionOperationNumber {
    TransactionOperationNumber::from_index(number - 1).expect("test operations are one-based")
}

fn ops(numbers: &[usize]) -> ImpactAttribution {
    ImpactAttribution::new(numbers.iter().copied().map(op)).expect("tests attribute every item")
}

fn contributors(numbers: &[usize]) -> Vec<TransactionOperationNumber> {
    numbers.iter().copied().map(op).collect()
}

fn range(first: usize, last: usize) -> TransactionOperationRange {
    TransactionOperationRange::new(op(first), op(last)).expect("test ranges run forwards")
}

fn node(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(kind, named::<ModelName>(name))
}

fn item(kind: ModelKind, name: &str) -> ImpactItemId {
    ImpactItemId::Node(node(kind, name))
}

fn unbranched(kind: ModelKind, name: &str) -> ImpactNodeCoverage {
    ImpactNodeCoverage::execution(node(kind, name), ConcreteBranchCoverage::Unbranched)
}

fn branched(kind: ModelKind, name: &str, branch: &str) -> ImpactNodeCoverage {
    ImpactNodeCoverage::execution(
        node(kind, name),
        ConcreteBranchCoverage::AllOfBranch {
            branch: named(branch),
        },
    )
}

fn configuration(kind: ModelKind, name: &str) -> ImpactNodeCoverage {
    ImpactNodeCoverage::configuration(node(kind, name))
}

fn held(coverage: &ImpactNodeCoverage, numbers: &[usize]) -> AttributedImpactNode {
    AttributedImpactNode {
        coverage: coverage.clone(),
        attribution: ops(numbers),
    }
}

fn relation(
    source: &ImpactNodeCoverage,
    target: &ImpactNodeCoverage,
    kind: ImpactEdgeKind,
    numbers: &[usize],
) -> ImpactTopologyEdge {
    ImpactTopologyEdge {
        source: source.clone(),
        target: target.clone(),
        kind,
        attribution: ops(numbers),
    }
}

/// A configuration dependency: `target`'s configuration requires `source`.
fn requires(
    source: &ImpactNodeCoverage,
    target: &ImpactNodeCoverage,
    numbers: &[usize],
) -> ImpactTopologyEdge {
    relation(
        source,
        target,
        ImpactEdgeKind::ConfigurationDependency,
        numbers,
    )
}

/// Record flow from `source` to `target`.
fn flows(
    source: &ImpactNodeCoverage,
    target: &ImpactNodeCoverage,
    numbers: &[usize],
) -> ImpactTopologyEdge {
    relation(source, target, ImpactEdgeKind::Dataflow, numbers)
}

fn topology(
    nodes: impl IntoIterator<Item = AttributedImpactNode>,
    edges: impl IntoIterator<Item = ImpactTopologyEdge>,
) -> ImpactTopology {
    ImpactTopology {
        nodes: CanonicalImpactSet::new(nodes),
        edges: CanonicalImpactSet::new(edges),
    }
}

fn unchanged(side: ImpactTopology) -> AffectedTopology {
    AffectedTopology {
        before: side.clone(),
        after: side,
    }
}

fn planned_step(
    operations: TransactionOperationRange,
    effects: ImpactEffects,
    pause: PauseRequirement,
) -> ExecutionStepImpactReport {
    ExecutionStepImpactReport::new(
        operations,
        PlannedExecutionStepImpact {
            completeness: ImpactReportCompleteness::Complete,
            pause,
            effects,
        },
        ActualExecutionStepImpact::unattempted(),
    )
}

/// A whole report around the given steps, with one accepted operation per number they cover.
fn report(steps: Vec<ExecutionStepImpactReport>) -> TransactionImpactReport {
    let mut operations = Vec::new();
    for step in &steps {
        for number in step.operations().operations() {
            operations.push(OperationImpactReport {
                number,
                operation: TransactionOperation::AlterConfiguration {
                    domain: domain(),
                    node: node(ModelKind::Relay, "events"),
                },
                execution_step: step.operations(),
                completeness: ImpactReportCompleteness::Complete,
                reasons: Vec::new(),
                contribution: ImpactEffects::default(),
            });
        }
    }
    TransactionImpactReport::new(
        domain(),
        TransactionPosition::new(operations.len()),
        ImpactPlanningBasis::new([3; 32]),
        ImpactReportCompleteness::Complete,
        operations,
        steps,
    )
    .expect("the test report has a consecutive operation and step sequence")
}

fn subgraph(
    nodes: impl IntoIterator<Item = AttributedImpactNode>,
    gates: impl IntoIterator<Item = AttributedGateBoundary>,
) -> PauseRequirement {
    PauseRequirement::Subgraph {
        scope: QuiesceSubgraph::new(domain(), nodes, gates),
    }
}

fn gate(relay: &str, numbers: &[usize]) -> AttributedGateBoundary {
    AttributedGateBoundary {
        boundary: ImpactGateBoundary {
            relay: named::<RelayName>(relay),
            branches: ConcreteBranchCoverage::Unbranched,
        },
        attribution: ops(numbers),
    }
}

fn roles(graph: &ImpactGraph, id: &ImpactItemId) -> Vec<ImpactRole> {
    graph.items[id].roles.keys().cloned().collect()
}

fn role_contributors(
    graph: &ImpactGraph,
    id: &ImpactItemId,
    role: &ImpactRole,
) -> Vec<TransactionOperationNumber> {
    graph.items[id].roles[role].operations().collect()
}

fn edge_id(source: ImpactItemId, target: ImpactItemId, kind: ImpactEdgeKind) -> ImpactEdgeId {
    ImpactEdgeId {
        source,
        target,
        relation: ImpactRelation::Topology(kind),
    }
}

fn segment_rect(start: (i32, i32), end: (i32, i32)) -> Rect {
    Rect {
        x: start.0.min(end.0),
        y: start.1.min(end.1),
        width: (start.0 - end.0).abs().max(1),
        height: (start.1 - end.1).abs().max(1),
    }
}

/// The drawing invariants every impact graph keeps: items never overlap, and no relation passes
/// through an item other than the two it joins.
fn assert_drawable(graph: &ImpactGraph) {
    let items = graph.items.values().collect::<Vec<_>>();
    for (index, item) in items.iter().enumerate() {
        assert!(item.rect.width > 0 && item.rect.height > 0, "{:?}", item.id);
        for other in items.iter().skip(index + 1) {
            assert!(
                !item.rect.intersects(&other.rect),
                "{:?} overlaps {:?}",
                item.id,
                other.id
            );
        }
    }
    for edge in graph.edges.values() {
        assert!(edge.route.points.len() >= 2, "{:?} is routed", edge.id);
        for window in edge.route.points.windows(2) {
            let run = segment_rect(window[0], window[1]);
            for item in &items {
                if item.id == edge.id.source || item.id == edge.id.target {
                    continue;
                }
                assert!(
                    !run.intersects(&item.rect),
                    "{:?} crosses {:?}",
                    edge.id,
                    item.id
                );
            }
        }
    }
}

/// Relays, a schema, a junction and a resource that all share one name, joined by record flow,
/// configuration dependencies in both directions and a resource binding.
fn colliding_names() -> ImpactGraph {
    let schema = configuration(ModelKind::Schema, "orders");
    let relay = unbranched(ModelKind::Relay, "orders");
    let junction = unbranched(ModelKind::Junction, "orders");
    let routed = unbranched(ModelKind::Relay, "routed");
    let side = topology(
        [
            held(&schema, &[1]),
            held(&relay, &[1]),
            held(&junction, &[1]),
            held(&routed, &[1]),
        ],
        [
            requires(&schema, &relay, &[1]),
            flows(&relay, &junction, &[1]),
            requires(&relay, &junction, &[1]),
            flows(&junction, &routed, &[1]),
            requires(&routed, &junction, &[1]),
        ],
    );
    let effects = ImpactEffects {
        topology: unchanged(side),
        resource_bindings: CanonicalImpactSet::new([ResourceBindingImpact {
            node: node(ModelKind::Junction, "orders"),
            resource: named("orders"),
            requested: RequestedResourceVersion::Latest,
            version: 3,
            attribution: ops(&[1]),
        }]),
        ..ImpactEffects::default()
    };
    let step = planned_step(range(1, 1), effects, PauseRequirement::NoPause);
    ImpactGraph::execution_step(&step, ImpactOutcome::Planned)
}

#[test]
fn items_sharing_a_name_across_kinds_stay_distinct() {
    let graph = colliding_names();

    let expected = [
        item(ModelKind::Schema, "orders"),
        item(ModelKind::Relay, "orders"),
        item(ModelKind::Junction, "orders"),
        item(ModelKind::Relay, "routed"),
        ImpactItemId::Resource(named("orders")),
    ];
    assert_eq!(graph.items.len(), expected.len());
    for id in &expected {
        assert!(graph.items.contains_key(id), "{id:?} must be drawn");
    }
    assert_eq!(
        graph.items[&item(ModelKind::Schema, "orders")].id.caption(),
        "SCHEMA"
    );
    assert_eq!(
        graph.items[&ImpactItemId::Resource(named("orders"))]
            .id
            .caption(),
        "RESOURCE"
    );

    let schema_link = &graph.edges[&edge_id(
        item(ModelKind::Schema, "orders"),
        item(ModelKind::Relay, "orders"),
        ImpactEdgeKind::ConfigurationDependency,
    )];
    let schema = graph.items[&item(ModelKind::Schema, "orders")].rect;
    let relay = graph.items[&item(ModelKind::Relay, "orders")].rect;
    assert_eq!(schema_link.route.points[0].0, schema.right());
    assert_eq!(
        schema_link.route.points.last().expect("the link arrives").0,
        relay.x
    );

    let search = GraphSearch::parse("orders").expect("a long enough search");
    assert_eq!(graph.matches(&search).count(), 4);
    let by_kind = GraphSearch::parse("schema").expect("a long enough search");
    assert_eq!(
        graph
            .matches(&by_kind)
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
        [item(ModelKind::Schema, "orders")]
    );
    assert_drawable(&graph);
}

#[test]
fn parallel_relations_keep_their_own_edges_and_routes() {
    let graph = colliding_names();
    let relay = item(ModelKind::Relay, "orders");
    let junction = item(ModelKind::Junction, "orders");
    let routed = item(ModelKind::Relay, "routed");

    let records = &graph.edges[&edge_id(relay.clone(), junction.clone(), ImpactEdgeKind::Dataflow)];
    let requirement = &graph.edges[&edge_id(
        relay.clone(),
        junction.clone(),
        ImpactEdgeKind::ConfigurationDependency,
    )];
    assert_eq!(records.route.kind, LayoutEdgeKind::Flow);
    assert_eq!(requirement.route.kind, LayoutEdgeKind::Dependency);
    assert_ne!(records.route.points, requirement.route.points);
    assert_ne!(
        records.route.points[0].1, requirement.route.points[0].1,
        "each relation leaves through its own port"
    );

    let output = &graph.edges[&edge_id(junction.clone(), routed.clone(), ImpactEdgeKind::Dataflow)];
    let required_output = &graph.edges[&edge_id(
        routed.clone(),
        junction.clone(),
        ImpactEdgeKind::ConfigurationDependency,
    )];
    assert_eq!(output.route.travel, EdgeTravel::Forward);
    assert_eq!(
        required_output.route.travel,
        EdgeTravel::Backward,
        "the junction's requirement on the relay it writes points against the flow"
    );
    assert!(graph.items[&junction].rect.right() <= graph.items[&routed].rect.x);
    assert_drawable(&graph);
}

#[test]
fn configuration_relations_never_travel_as_records_or_state() {
    assert_eq!(
        ImpactRelation::Topology(ImpactEdgeKind::ConfigurationDependency).layout_kind(),
        LayoutEdgeKind::Dependency
    );
    assert_eq!(
        ImpactRelation::ResourceBinding.layout_kind(),
        LayoutEdgeKind::Dependency
    );
    assert_eq!(
        ImpactRelation::Topology(ImpactEdgeKind::MaterializedState).layout_kind(),
        LayoutEdgeKind::State
    );
    for kind in [
        ImpactEdgeKind::Dataflow,
        ImpactEdgeKind::MessageError,
        ImpactEdgeKind::CorrelationTimeout,
    ] {
        assert_eq!(
            ImpactRelation::Topology(kind).layout_kind(),
            LayoutEdgeKind::Flow
        );
    }

    let graph = colliding_names();
    let binding = &graph.edges[&ImpactEdgeId {
        source: ImpactItemId::Resource(named("orders")),
        target: item(ModelKind::Junction, "orders"),
        relation: ImpactRelation::ResourceBinding,
    }];
    assert_eq!(binding.route.kind, LayoutEdgeKind::Dependency);
    assert!(binding.route.badge.is_none());
    assert_eq!(binding.presence, TopologyPresence::Both);
    assert!(
        roles(&graph, &item(ModelKind::Junction, "orders")).contains(&ImpactRole::Binding {
            resource: named("orders"),
            requested: RequestedResourceVersion::Latest,
            version: 3,
        })
    );
}

#[test]
fn configuration_nodes_stand_beside_the_runtime_nodes_that_require_them() {
    let wire = configuration(ModelKind::WireJsonSchema, "order_wire");
    let codec = configuration(ModelKind::Codec, "order_codec");
    let client = configuration(ModelKind::Client, "broker");
    let schema = configuration(ModelKind::Schema, "order");
    let branch = configuration(ModelKind::Branch, "by_tenant");
    let ingestor = unbranched(ModelKind::Ingestor, "orders_in");
    let relay = branched(ModelKind::Relay, "orders", "by_tenant");
    let placement = configuration(ModelKind::Placement, "pinned");
    let side = topology(
        [
            &wire, &codec, &client, &schema, &branch, &ingestor, &relay, &placement,
        ]
        .map(|coverage| held(coverage, &[1])),
        [
            requires(&wire, &codec, &[1]),
            requires(&schema, &codec, &[1]),
            requires(&codec, &ingestor, &[1]),
            requires(&client, &ingestor, &[1]),
            requires(&schema, &relay, &[1]),
            requires(&branch, &relay, &[1]),
            flows(&ingestor, &relay, &[1]),
            requires(&ingestor, &placement, &[1]),
        ],
    );
    let step = planned_step(
        range(1, 1),
        ImpactEffects {
            topology: unchanged(side),
            ..ImpactEffects::default()
        },
        PauseRequirement::NoPause,
    );
    let graph = ImpactGraph::execution_step(&step, ImpactOutcome::Planned);

    let place = |kind: ModelKind, name: &str| graph.items[&item(kind, name)].rect;
    assert!(
        place(ModelKind::WireJsonSchema, "order_wire").right()
            <= place(ModelKind::Codec, "order_codec").x
    );
    assert!(
        place(ModelKind::Codec, "order_codec").right() <= place(ModelKind::Ingestor, "orders_in").x
    );
    assert!(
        place(ModelKind::Client, "broker").right() <= place(ModelKind::Ingestor, "orders_in").x
    );
    assert!(place(ModelKind::Branch, "by_tenant").right() <= place(ModelKind::Relay, "orders").x);
    assert!(
        place(ModelKind::Ingestor, "orders_in").right() <= place(ModelKind::Placement, "pinned").x,
        "a placement requires the node it pins, so it stands after it"
    );
    for edge in graph.edges.values() {
        assert_eq!(edge.route.travel, EdgeTravel::Forward, "{:?}", edge.id);
    }
    for id in [
        item(ModelKind::Codec, "order_codec"),
        item(ModelKind::Branch, "by_tenant"),
    ] {
        assert_eq!(graph.items[&id].branches_before, None);
        assert_eq!(graph.items[&id].branches_after, None);
    }
    assert_eq!(
        graph.items[&item(ModelKind::Relay, "orders")].branches_after,
        Some(ConcreteBranchCoverage::AllOfBranch {
            branch: named("by_tenant")
        })
    );
    assert_drawable(&graph);
}

#[test]
fn shared_gates_and_pauses_appear_once_with_every_contributing_operation() {
    let input = unbranched(ModelKind::Relay, "input");
    let junction = unbranched(ModelKind::Junction, "route");
    let output = unbranched(ModelKind::Relay, "output");
    let side = topology(
        [
            held(&input, &[1, 2]),
            held(&junction, &[1, 2]),
            held(&output, &[1, 2]),
        ],
        [
            flows(&input, &junction, &[1, 2]),
            flows(&junction, &output, &[1, 2]),
        ],
    );
    let effects = || ImpactEffects {
        topology: unchanged(side.clone()),
        ..ImpactEffects::default()
    };
    // The first step's two operations share a gate and a pause, and the second step gates the
    // same relay again for its own operation.
    let first = planned_step(
        range(1, 2),
        effects(),
        subgraph(
            [held(&junction, &[1]), held(&junction, &[2])],
            [gate("input", &[1]), gate("input", &[2])],
        ),
    );
    let second = planned_step(
        range(3, 3),
        effects(),
        subgraph([held(&junction, &[3])], [gate("input", &[3])]),
    );
    let graph = ImpactGraph::transaction(&report(vec![first, second]), ImpactOutcome::Planned);

    let relay = item(ModelKind::Relay, "input");
    let gates = roles(&graph, &relay)
        .into_iter()
        .filter(|role| matches!(role, ImpactRole::Gate { .. }))
        .collect::<Vec<_>>();
    assert_eq!(
        gates.len(),
        1,
        "one gate at the relay, however many share it"
    );
    assert_eq!(
        role_contributors(&graph, &relay, &gates[0]),
        contributors(&[1, 2, 3])
    );

    let route = item(ModelKind::Junction, "route");
    let pause = ImpactRole::Pause {
        branches: Some(ConcreteBranchCoverage::Unbranched),
        engagement: Engagement::Planned,
    };
    assert_eq!(roles(&graph, &route), std::slice::from_ref(&pause));
    assert_eq!(
        role_contributors(&graph, &route, &pause),
        contributors(&[1, 2, 3])
    );
    assert!(graph.items[&route].contributors.includes(op(3)));
    assert!(graph.domain.pauses.is_empty());
    assert!(graph.domain.outline.is_none());
}

#[test]
fn removed_topology_stays_in_the_drawing_and_out_of_the_after_view() {
    let feed = unbranched(ModelKind::Relay, "feed");
    let backup = unbranched(ModelKind::Relay, "backup");
    let dropped = unbranched(ModelKind::Junction, "legacy_route");
    let kept = unbranched(ModelKind::Junction, "route");
    let sink = unbranched(ModelKind::Relay, "sink");
    let before = topology(
        [&feed, &backup, &dropped, &kept, &sink].map(|coverage| held(coverage, &[1])),
        [
            flows(&feed, &dropped, &[1]),
            flows(&dropped, &sink, &[1]),
            flows(&feed, &kept, &[1]),
            flows(&kept, &sink, &[1]),
        ],
    );
    // The junction is dropped, and the one that remains reads the backup relay instead.
    let after = topology(
        [&feed, &backup, &kept, &sink].map(|coverage| held(coverage, &[1])),
        [flows(&backup, &kept, &[1]), flows(&kept, &sink, &[1])],
    );
    let effects = ImpactEffects {
        changed_configuration: CanonicalImpactSet::new([
            ConfigurationImpact {
                transition: ConfigurationTransition::Dropped {
                    node: node(ModelKind::Junction, "legacy_route"),
                },
                attribution: ops(&[1]),
            },
            ConfigurationImpact {
                transition: ConfigurationTransition::Changed {
                    node: node(ModelKind::Junction, "route"),
                },
                attribution: ops(&[1]),
            },
        ]),
        topology: AffectedTopology { before, after },
        ..ImpactEffects::default()
    };
    let step = planned_step(range(1, 1), effects, PauseRequirement::NoPause);
    let graph = ImpactGraph::execution_step(&step, ImpactOutcome::Planned);

    let legacy = item(ModelKind::Junction, "legacy_route");
    assert_eq!(graph.items[&legacy].presence, TopologyPresence::Before);
    assert_eq!(
        roles(&graph, &legacy),
        [ImpactRole::Configuration(ConfigurationChange::Dropped)]
    );
    assert_eq!(
        roles(&graph, &item(ModelKind::Junction, "route")),
        [ImpactRole::Configuration(ConfigurationChange::Changed)]
    );
    let disconnected = edge_id(
        item(ModelKind::Relay, "feed"),
        item(ModelKind::Junction, "route"),
        ImpactEdgeKind::Dataflow,
    );
    let rewired = edge_id(
        item(ModelKind::Relay, "backup"),
        item(ModelKind::Junction, "route"),
        ImpactEdgeKind::Dataflow,
    );
    assert_eq!(
        graph.edges[&disconnected].presence,
        TopologyPresence::Before
    );
    assert_eq!(graph.edges[&rewired].presence, TopologyPresence::After);
    assert_eq!(
        graph.items[&item(ModelKind::Relay, "feed")].presence,
        TopologyPresence::Both
    );

    let shown = |view: ImpactView| {
        graph
            .items_in(view)
            .map(|item| item.id.clone())
            .collect::<BTreeSet<_>>()
    };
    assert!(shown(ImpactView::Before).contains(&legacy));
    assert!(shown(ImpactView::Changes).contains(&legacy));
    assert!(!shown(ImpactView::After).contains(&legacy));
    let relations = |view: ImpactView| {
        graph
            .edges_in(view)
            .map(|edge| edge.id.clone())
            .collect::<BTreeSet<_>>()
    };
    assert!(relations(ImpactView::Changes).contains(&disconnected));
    assert!(!relations(ImpactView::After).contains(&disconnected));
    assert!(!relations(ImpactView::Before).contains(&rewired));
    assert!(
        graph.edges[&disconnected].route.points.len() >= 2,
        "a disconnected relation keeps a route to inspect"
    );
    assert_drawable(&graph);
}

#[test]
fn a_domain_pause_frames_the_whole_drawing_apart_from_branch_groups() {
    let ingestor = unbranched(ModelKind::Ingestor, "orders_in");
    let relay = branched(ModelKind::Relay, "orders", "by_tenant");
    let junction = branched(ModelKind::Junction, "route", "by_tenant");
    let side = topology(
        [&ingestor, &relay, &junction].map(|coverage| held(coverage, &[1])),
        [
            flows(&ingestor, &relay, &[1]),
            flows(&relay, &junction, &[1]),
        ],
    );
    let step = planned_step(
        range(1, 1),
        ImpactEffects {
            topology: unchanged(side),
            ..ImpactEffects::default()
        },
        PauseRequirement::Domain { domain: domain() },
    );
    let graph = ImpactGraph::execution_step(&step, ImpactOutcome::Planned);

    assert_eq!(
        graph.domain.pauses,
        [DomainPause {
            domain: domain(),
            step: range(1, 1),
            engagement: Engagement::Planned,
        }]
    );
    let outline = graph
        .domain
        .outline
        .expect("a domain pause frames the drawing");
    let inside = |rect: &Rect| {
        outline.frame.x < rect.x
            && outline.frame.y < rect.y
            && rect.right() < outline.frame.right()
            && rect.bottom() < outline.frame.bottom()
    };
    for item in graph.items.values() {
        assert!(
            inside(&item.rect),
            "{:?} lies inside the domain frame",
            item.id
        );
        assert!(!outline.label.intersects(&item.rect));
    }
    assert_eq!(
        graph.groups.len(),
        1,
        "the domain frame is not a branch group"
    );
    assert_eq!(graph.groups[0].branch, named::<BranchName>("by_tenant"));
    for band in &graph.groups[0].bands {
        assert!(inside(band));
        assert!(!outline.label.intersects(band));
    }
    for item in graph.items.values() {
        assert!(
            !item
                .roles
                .keys()
                .any(|role| matches!(role, ImpactRole::Pause { .. })),
            "a whole-domain pause is not spelled out per item"
        );
    }
}

#[test]
fn interleaved_branch_groups_hold_exactly_their_members() {
    let ingestor = unbranched(ModelKind::Ingestor, "source");
    let mut nodes = vec![held(&ingestor, &[1])];
    let mut edges = Vec::new();
    let mut members = BTreeMap::<BranchName, Vec<ImpactItemId>>::new();
    // Name order alternates the two branches in every column they share.
    for (index, branch) in [
        (1, "by_tenant"),
        (2, "by_user"),
        (3, "by_tenant"),
        (4, "by_user"),
    ] {
        let input = branched(ModelKind::Relay, &format!("r{index}_input"), branch);
        let junction = branched(ModelKind::Junction, &format!("j{index}_route"), branch);
        let output = branched(ModelKind::Relay, &format!("r{index}_output"), branch);
        for coverage in [&input, &junction, &output] {
            nodes.push(held(coverage, &[1]));
            members
                .entry(named(branch))
                .or_default()
                .push(ImpactItemId::Node(coverage.node.clone()));
        }
        edges.push(flows(&ingestor, &input, &[1]));
        edges.push(flows(&input, &junction, &[1]));
        edges.push(flows(&junction, &output, &[1]));
    }
    let step = planned_step(
        range(1, 1),
        ImpactEffects {
            topology: unchanged(topology(nodes, edges)),
            ..ImpactEffects::default()
        },
        PauseRequirement::NoPause,
    );
    let graph = ImpactGraph::execution_step(&step, ImpactOutcome::Planned);

    assert_eq!(graph.groups.len(), 2);
    for group in &graph.groups {
        let expected = &members[&group.branch];
        for item in graph.items.values() {
            let inside = group.bands.iter().any(|band| band.intersects(&item.rect));
            assert_eq!(
                inside,
                expected.contains(&item.id),
                "{:?} containment in {:?}",
                item.id,
                group.branch
            );
        }
    }
    for band in &graph.groups[0].bands {
        for other in &graph.groups[1].bands {
            assert!(!band.intersects(other), "{band:?} overlaps {other:?}");
        }
    }
    assert_drawable(&graph);
}

fn transition(transition: ConfigurationTransition, numbers: &[usize]) -> ConfigurationImpact {
    ConfigurationImpact {
        transition,
        attribution: ops(numbers),
    }
}

#[test]
fn a_transaction_composes_its_steps_in_order() {
    let stable = unbranched(ModelKind::Relay, "stable");
    let created = unbranched(ModelKind::Relay, "created");
    let transient = unbranched(ModelKind::Relay, "transient");

    let first = planned_step(
        range(1, 1),
        ImpactEffects {
            changed_configuration: CanonicalImpactSet::new([
                transition(
                    ConfigurationTransition::Created {
                        node: created.node.clone(),
                    },
                    &[1],
                ),
                transition(
                    ConfigurationTransition::Created {
                        node: transient.node.clone(),
                    },
                    &[1],
                ),
            ]),
            topology: AffectedTopology {
                before: topology([held(&stable, &[1])], []),
                after: topology(
                    [
                        held(&stable, &[1]),
                        held(&created, &[1]),
                        held(&transient, &[1]),
                    ],
                    [],
                ),
            },
            ..ImpactEffects::default()
        },
        PauseRequirement::NoPause,
    );
    let second = planned_step(
        range(2, 2),
        ImpactEffects {
            changed_configuration: CanonicalImpactSet::new([
                transition(
                    ConfigurationTransition::Changed {
                        node: created.node.clone(),
                    },
                    &[2],
                ),
                transition(
                    ConfigurationTransition::Dropped {
                        node: transient.node.clone(),
                    },
                    &[2],
                ),
            ]),
            topology: AffectedTopology {
                before: topology([held(&created, &[2]), held(&transient, &[2])], []),
                after: topology([held(&created, &[2])], []),
            },
            ..ImpactEffects::default()
        },
        PauseRequirement::NoPause,
    );
    let step_only = ImpactGraph::execution_step(&second, ImpactOutcome::Planned);
    let graph = ImpactGraph::transaction(&report(vec![first, second]), ImpactOutcome::Planned);

    let created_item = item(ModelKind::Relay, "created");
    assert_eq!(graph.items[&created_item].presence, TopologyPresence::After);
    let created_role = ImpactRole::Configuration(ConfigurationChange::Created);
    assert_eq!(
        roles(&graph, &created_item),
        std::slice::from_ref(&created_role)
    );
    assert_eq!(
        role_contributors(&graph, &created_item, &created_role),
        contributors(&[1, 2])
    );
    assert_eq!(
        step_only.items[&created_item].presence,
        TopologyPresence::Both,
        "within its own step the relay already existed"
    );
    assert_eq!(
        roles(&step_only, &created_item),
        [ImpactRole::Configuration(ConfigurationChange::Changed)]
    );

    let transient_item = item(ModelKind::Relay, "transient");
    assert_eq!(
        graph.items[&transient_item].presence,
        TopologyPresence::Transient
    );
    assert_eq!(
        roles(&graph, &transient_item),
        [ImpactRole::Configuration(
            ConfigurationChange::CreatedAndDropped
        )]
    );
    assert!(!ImpactView::Before.shows(TopologyPresence::Transient));
    assert!(!ImpactView::After.shows(TopologyPresence::Transient));
    assert!(ImpactView::Changes.shows(TopologyPresence::Transient));

    assert_eq!(
        graph.items[&item(ModelKind::Relay, "stable")].presence,
        TopologyPresence::Both,
        "a node the second step leaves alone keeps the sides the first step gave it"
    );
}

#[test]
fn recorded_outcomes_are_drawn_apart_from_the_plan() {
    let junction = unbranched(ModelKind::Junction, "route");
    let side = topology([held(&junction, &[1])], []);
    let requirement = subgraph([held(&junction, &[1])], [gate("input", &[1])]);
    let outcomes = vec![
        QuiescenceOutcome::Requested,
        QuiescenceOutcome::Confirmed,
        QuiescenceOutcome::Released,
    ];
    let actual = ActualExecutionStepImpact {
        outcome: nervix_models::ExecutionStepOutcome::Applied,
        quiescence: vec![ActualQuiescence {
            requirement: requirement.clone(),
            outcomes: outcomes.clone(),
        }],
        effects: ImpactEffects {
            topology: unchanged(side.clone()),
            ..ImpactEffects::default()
        },
    };
    let step = ExecutionStepImpactReport::new(
        range(1, 1),
        PlannedExecutionStepImpact {
            completeness: ImpactReportCompleteness::Complete,
            pause: requirement,
            effects: ImpactEffects {
                topology: unchanged(side),
                ..ImpactEffects::default()
            },
        },
        actual,
    );

    let planned = ImpactGraph::execution_step(&step, ImpactOutcome::Planned);
    let recorded = ImpactGraph::execution_step(&step, ImpactOutcome::Actual);
    let route = item(ModelKind::Junction, "route");
    assert_eq!(
        roles(&planned, &route),
        [ImpactRole::Pause {
            branches: Some(ConcreteBranchCoverage::Unbranched),
            engagement: Engagement::Planned,
        }]
    );
    assert_eq!(
        roles(&recorded, &route),
        [ImpactRole::Pause {
            branches: Some(ConcreteBranchCoverage::Unbranched),
            engagement: Engagement::Recorded(outcomes.clone()),
        }]
    );
    assert_eq!(
        roles(&recorded, &item(ModelKind::Relay, "input")),
        [ImpactRole::Gate {
            branches: ConcreteBranchCoverage::Unbranched,
            engagement: Engagement::Recorded(outcomes),
        }]
    );
    let unattempted = planned_step(
        range(1, 1),
        ImpactEffects::default(),
        PauseRequirement::Domain { domain: domain() },
    );
    let nothing_yet = ImpactGraph::execution_step(&unattempted, ImpactOutcome::Actual);
    assert!(nothing_yet.items.is_empty());
    assert!(
        nothing_yet.domain.pauses.is_empty(),
        "an unattempted step engaged nothing"
    );
}

#[test]
fn every_role_is_attached_to_the_item_it_names() {
    let junction = unbranched(ModelKind::Junction, "route");
    let relay = unbranched(ModelKind::Relay, "orders");
    let side = topology(
        [held(&junction, &[1]), held(&relay, &[1])],
        [flows(&relay, &junction, &[1])],
    );
    let effects = ImpactEffects {
        topology: unchanged(side),
        ownership_moves: CanonicalImpactSet::new([nervix_models::OwnershipMoveImpact {
            node: junction.clone(),
            source: named("node-1"),
            destination: named("node-2"),
            attribution: ops(&[1]),
        }]),
        activations: CanonicalImpactSet::new([nervix_models::ActivationImpact {
            node: junction.clone(),
            action: ActivationAction::Activate,
            attribution: ops(&[1]),
        }]),
        rebuilds: CanonicalImpactSet::new([nervix_models::RebuildImpact {
            node: junction.clone(),
            reason: RebuildReason::Ownership,
            attribution: ops(&[1]),
        }]),
        state_resets: CanonicalImpactSet::new([nervix_models::StateResetImpact {
            node: junction.clone(),
            state: StatePurge::DeduplicatorKeyspace,
            attribution: ops(&[1]),
        }]),
        force_flushes: CanonicalImpactSet::new([
            ForceFlushImpact {
                node: relay.clone(),
                attribution: ops(&[1]),
            },
            ForceFlushImpact {
                node: unbranched(ModelKind::Emitter, "elsewhere"),
                attribution: ops(&[1]),
            },
        ]),
        lifecycle: CanonicalImpactSet::new([nervix_models::DomainLifecycleImpact {
            domain: domain(),
            action: DomainLifecycleAction::Stop,
            attribution: ops(&[1]),
        }]),
        resource_catalog: CanonicalImpactSet::new([ResourceCatalogImpact {
            resource: named("model_bundle"),
            action: ResourceCatalogAction::Create,
            attribution: ops(&[1]),
        }]),
        ..ImpactEffects::default()
    };
    let step = planned_step(range(1, 1), effects, PauseRequirement::NoPause);
    let graph = ImpactGraph::execution_step(&step, ImpactOutcome::Planned);

    let unbranched_coverage = Some(ConcreteBranchCoverage::Unbranched);
    assert_eq!(
        roles(&graph, &item(ModelKind::Junction, "route")),
        [
            ImpactRole::Move {
                branches: unbranched_coverage.clone(),
                source: named("node-1"),
                destination: named("node-2"),
            },
            ImpactRole::Rebuild {
                branches: unbranched_coverage.clone(),
                reason: RebuildReason::Ownership,
            },
            ImpactRole::StateReset {
                branches: unbranched_coverage.clone(),
                state: StatePurge::DeduplicatorKeyspace,
            },
            ImpactRole::Activation {
                branches: unbranched_coverage.clone(),
                action: ActivationAction::Activate,
            },
        ]
    );
    assert_eq!(
        roles(&graph, &item(ModelKind::Relay, "orders")),
        [ImpactRole::ForceFlush {
            branches: unbranched_coverage,
        }]
    );
    assert!(
        !graph
            .items
            .contains_key(&item(ModelKind::Emitter, "elsewhere")),
        "a flush alone does not pull a node into the affected graph"
    );
    assert!(
        graph
            .domain
            .flushed_elsewhere
            .contains_key(&node(ModelKind::Emitter, "elsewhere"))
    );
    assert!(
        graph
            .domain
            .lifecycle
            .contains_key(&DomainLifecycleAction::Stop)
    );
    let bundle = ImpactItemId::Resource(named("model_bundle"));
    assert_eq!(graph.items[&bundle].presence, TopologyPresence::After);
    assert_eq!(
        roles(&graph, &bundle),
        [ImpactRole::Catalog(ResourceCatalogAction::Create)]
    );
    assert_drawable(&graph);
}

#[test]
fn explicit_branch_coverage_is_kept_on_every_role() {
    let branch: BranchName = named("by_tenant");
    let keys = ConcreteBranchCoverage::selected(
        branch.clone(),
        [
            BranchKeyFingerprint::new([1; 32]),
            BranchKeyFingerprint::new([2; 32]),
        ],
    )
    .expect("two keys select concrete branches");
    let relay = ImpactNodeCoverage::execution(node(ModelKind::Relay, "orders"), keys.clone());
    let step = planned_step(
        range(1, 1),
        ImpactEffects {
            topology: unchanged(topology([held(&relay, &[1])], [])),
            rebuilds: CanonicalImpactSet::new([nervix_models::RebuildImpact {
                node: relay.clone(),
                reason: RebuildReason::Configuration,
                attribution: ops(&[1]),
            }]),
            ..ImpactEffects::default()
        },
        PauseRequirement::NoPause,
    );
    let graph = ImpactGraph::execution_step(&step, ImpactOutcome::Planned);
    let orders = &graph.items[&item(ModelKind::Relay, "orders")];
    assert_eq!(orders.branches_after, Some(keys.clone()));
    assert_eq!(
        roles(&graph, &orders.id),
        [ImpactRole::Rebuild {
            branches: Some(keys),
            reason: RebuildReason::Configuration,
        }]
    );
    assert_eq!(graph.groups.len(), 1);
    assert_eq!(graph.groups[0].branch, branch);
}

#[test]
fn an_operation_draws_its_contribution_without_the_steps_pause() {
    let junction = unbranched(ModelKind::Junction, "route");
    let contribution = ImpactEffects {
        changed_configuration: CanonicalImpactSet::new([ConfigurationImpact {
            transition: ConfigurationTransition::Changed {
                node: junction.node.clone(),
            },
            attribution: ops(&[1]),
        }]),
        topology: unchanged(topology([held(&junction, &[1])], [])),
        ..ImpactEffects::default()
    };
    let operation = OperationImpactReport {
        number: op(1),
        operation: TransactionOperation::AlterConfiguration {
            domain: domain(),
            node: junction.node.clone(),
        },
        execution_step: range(1, 1),
        completeness: ImpactReportCompleteness::Complete,
        reasons: Vec::new(),
        contribution,
    };
    let graph = ImpactGraph::operation(&operation);
    assert_eq!(
        roles(&graph, &item(ModelKind::Junction, "route")),
        [ImpactRole::Configuration(ConfigurationChange::Changed)]
    );
    assert!(graph.domain.pauses.is_empty());
}

#[test]
fn identical_reports_draw_identical_geometry() {
    let first = colliding_names();
    let second = colliding_names();
    assert_eq!(format!("{first:?}"), format!("{second:?}"));

    // Every view selects among the same placed items, so a switch moves nothing.
    for view in [ImpactView::Before, ImpactView::Changes, ImpactView::After] {
        for item in first.items_in(view) {
            assert_eq!(item.rect, second.items[&item.id].rect);
        }
    }
}

#[test]
fn the_drawing_frames_with_the_shared_viewport_rules() {
    let graph = colliding_names();
    let canvas = graph.canvas_bounds();
    for item in graph.items.values() {
        let mut held = canvas;
        held.include_bounds(GraphBounds::from_rect(item.rect));
        assert_eq!(held, canvas, "{:?} lies on the canvas", item.id);
    }

    let schema = GraphSearch::parse("schema").expect("a long enough search");
    assert_eq!(
        graph.search_bounds(&schema),
        Some(GraphBounds::from_rect(
            graph.items[&item(ModelKind::Schema, "orders")].rect
        ))
    );
    let nothing = GraphSearch::parse("absent").expect("a long enough search");
    assert_eq!(graph.search_bounds(&nothing), None);

    let records = edge_id(
        item(ModelKind::Relay, "orders"),
        item(ModelKind::Junction, "orders"),
        ImpactEdgeKind::Dataflow,
    );
    let focus = graph
        .edge_bounds(&records)
        .expect("a routed relation frames");
    for endpoint in [&records.source, &records.target] {
        let mut held = focus;
        held.include_bounds(GraphBounds::from_rect(graph.items[endpoint].rect));
        assert_eq!(held, focus, "the focus holds {endpoint:?}");
    }
}

#[test]
fn an_empty_report_draws_nothing() {
    let graph = ImpactGraph::transaction(&report(Vec::new()), ImpactOutcome::Planned);
    assert!(graph.items.is_empty());
    assert!(graph.edges.is_empty());
    assert!(graph.domain.outline.is_none());
    assert_eq!(graph.width, 0);
}
