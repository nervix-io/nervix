//! A transaction's impact drawn as a graph.
//!
//! Layer: edges.
//!
//! - **Owns.** How a typed impact report becomes drawing items: the configuration nodes, runtime
//!   nodes and resources the report names, the relations between them, which side of the change
//!   each belongs to, the roles each plays with the operations that contribute them, and where
//!   everything is placed.
//! - **Depends on.** The report vocabulary and the console's graph layout.
//! - **Must not know.** How a report is planned, persisted, transported or inspected, or what the
//!   live graph last showed. Nothing here is inferred from a live snapshot; every item, relation
//!   and role is read from the report.
//!
//! The topology before the change and the topology after it are laid out together, once. Showing
//! the graph as it was, as it will be, or with its changes marked only selects among items that
//! are already placed, so switching between those views never moves anything.

use std::collections::{BTreeMap, BTreeSet};

use nervix_models::{
    ActivationAction, AffectedTopology, BranchName, ClusterNodeName, ConcreteBranchCoverage,
    ConfigurationTransition, DomainLifecycleAction, DomainName, ExecutionStepImpactReport,
    ImpactAttribution, ImpactEdgeKind, ImpactEffects, ImpactTopology, ModelKind, NodeRef,
    OperationImpactReport, PauseRequirement, QuiescenceOutcome, RebuildReason,
    RequestedResourceVersion, ResourceCatalogAction, ResourceName, StatePurge,
    TransactionImpactReport, TransactionOperationNumber, TransactionOperationRange,
};

use crate::graph::{
    GraphSearch,
    layout::{GroupRegion, Layout, LayoutEdge, LayoutEdgeKind, LayoutItem, Rect, RoutedEdge},
    viewport::GraphBounds,
};

/// Every item of an impact graph is drawn at one size, so its shape says nothing about how much
/// happens to it. The card is taller than a live-graph card to hold a strip of role marks.
pub const IMPACT_ITEM_WIDTH: i32 = 192;
pub const IMPACT_ITEM_HEIGHT: i32 = 76;
/// How far the whole-domain outline stands off the drawing. The band between the two holds the
/// outline's label, above everything the outline encloses.
const DOMAIN_OUTLINE_INSET: i32 = 24;
const DOMAIN_LABEL_HEIGHT: i32 = 18;

/// Whether a drawing shows what a report planned or what actually happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpactOutcome {
    /// Each execution step's planned effects and the pause it requires.
    Planned,
    /// Each execution step's recorded effects and the quiescence it actually engaged.
    Actual,
}

/// What an item of the impact graph stands for.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ImpactItemId {
    /// A configuration or runtime node of the domain.
    Node(NodeRef),
    /// An uploaded resource that a node binds or the change creates.
    Resource(ResourceName),
}

impl ImpactItemId {
    /// The name the item carries in NSPL.
    pub fn name(&self) -> &str {
        match self {
            Self::Node(node) => node.identifier.as_str(),
            Self::Resource(resource) => resource.as_str(),
        }
    }

    /// The kind the item is, as NSPL spells it.
    pub fn caption(&self) -> &'static str {
        match self {
            Self::Node(node) => node.kind.keyword_phrase(),
            Self::Resource(_) => "RESOURCE",
        }
    }

    /// Whether the item is a relay, which the drawing keeps in columns of its own.
    fn is_relay(&self) -> bool {
        match self {
            Self::Node(node) => node.kind == ModelKind::Relay,
            Self::Resource(_) => false,
        }
    }
}

/// What relates two items of the impact graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ImpactRelation {
    /// A relation of the report's affected topology.
    Topology(ImpactEdgeKind),
    /// The target node binds a version of the source resource.
    ResourceBinding,
}

impl ImpactRelation {
    /// How the relation takes part in placing items. Records and state place the flow; a
    /// configuration dependency and a resource binding carry nothing, so they only place what the
    /// flow does not.
    const fn layout_kind(self) -> LayoutEdgeKind {
        match self {
            Self::Topology(
                ImpactEdgeKind::Dataflow
                | ImpactEdgeKind::MessageError
                | ImpactEdgeKind::CorrelationTimeout,
            ) => LayoutEdgeKind::Flow,
            Self::Topology(ImpactEdgeKind::MaterializedState) => LayoutEdgeKind::State,
            Self::Topology(ImpactEdgeKind::ConfigurationDependency) | Self::ResourceBinding => {
                LayoutEdgeKind::Dependency
            }
        }
    }
}

/// One edge of the impact graph, named by the items it joins and what relates them. Parallel
/// relations between one pair of items are different edges.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ImpactEdgeId {
    pub source: ImpactItemId,
    pub target: ImpactItemId,
    pub relation: ImpactRelation,
}

/// Which side of the change an item or a relation belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyPresence {
    /// Only before: the change drops it or disconnects it from the affected graph.
    Before,
    /// Only after: the change creates it or connects it.
    After,
    /// Before and after.
    Both,
    /// Neither: one step of the transaction adds it and a later step removes it again.
    Transient,
}

impl TopologyPresence {
    const fn from_sides(before: bool, after: bool) -> Self {
        match (before, after) {
            (true, true) => Self::Both,
            (true, false) => Self::Before,
            (false, true) => Self::After,
            (false, false) => Self::Transient,
        }
    }

    pub const fn before(self) -> bool {
        match self {
            Self::Before | Self::Both => true,
            Self::After | Self::Transient => false,
        }
    }

    pub const fn after(self) -> bool {
        match self {
            Self::After | Self::Both => true,
            Self::Before | Self::Transient => false,
        }
    }
}

/// Which side of the change a drawing shows. Every view draws the same geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpactView {
    /// The affected graph as it stands before the change.
    Before,
    /// Both sides together, with what the change adds, removes and alters marked.
    Changes,
    /// The affected graph as the change leaves it.
    After,
}

impl ImpactView {
    /// Whether something present on the given sides is part of this view. The changes view holds
    /// everything, so a dropped node or a disconnected relation can always be inspected there.
    pub const fn shows(self, presence: TopologyPresence) -> bool {
        match self {
            Self::Before => presence.before(),
            Self::After => presence.after(),
            Self::Changes => true,
        }
    }
}

/// The accepted operations something in the impact is attributed to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Contributors(BTreeSet<TransactionOperationNumber>);

impl Contributors {
    fn of(attribution: &ImpactAttribution) -> Self {
        Self(attribution.operations().iter().copied().collect())
    }

    fn add(&mut self, other: &Self) {
        self.0.extend(other.0.iter().copied());
    }

    /// The contributing operations, in written order.
    pub fn operations(&self) -> impl Iterator<Item = TransactionOperationNumber> + '_ {
        self.0.iter().copied()
    }

    pub fn includes(&self, operation: TransactionOperationNumber) -> bool {
        self.0.contains(&operation)
    }
}

/// How the change affects a node's own configuration, across every step a drawing composes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConfigurationChange {
    Created,
    Changed,
    Dropped,
    /// One step creates the node and a later step drops it, so neither side holds it.
    CreatedAndDropped,
}

/// Whether a pause or a gate was planned, or how its engagement actually went.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Engagement {
    Planned,
    /// The outcomes the engagement recorded, in order.
    Recorded(Vec<QuiescenceOutcome>),
}

/// One part an item plays in the impact.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ImpactRole {
    /// The change creates, changes or drops the node's configuration.
    Configuration(ConfigurationChange),
    /// These executions of the node pause with the affected subgraph.
    Pause {
        branches: Option<ConcreteBranchCoverage>,
        engagement: Engagement,
    },
    /// New admission into the paused subgraph is held at this relay for these executions.
    Gate {
        branches: ConcreteBranchCoverage,
        engagement: Engagement,
    },
    /// These executions move from one cluster node to another.
    Move {
        branches: Option<ConcreteBranchCoverage>,
        source: ClusterNodeName,
        destination: ClusterNodeName,
    },
    /// These executions are rebuilt.
    Rebuild {
        branches: Option<ConcreteBranchCoverage>,
        reason: RebuildReason,
    },
    /// Part of these executions' state is discarded.
    StateReset {
        branches: Option<ConcreteBranchCoverage>,
        state: StatePurge,
    },
    /// These executions flush their buffered work before the change applies.
    ForceFlush {
        branches: Option<ConcreteBranchCoverage>,
    },
    /// These executions are activated, deactivated, or have their listener refreshed.
    Activation {
        branches: Option<ConcreteBranchCoverage>,
        action: ActivationAction,
    },
    /// The node binds a version of a resource.
    Binding {
        resource: ResourceName,
        requested: RequestedResourceVersion,
        version: u64,
    },
    /// The change acts on the resource itself.
    Catalog(ResourceCatalogAction),
}

/// One item of the impact graph, placed.
#[derive(Debug, Clone)]
pub struct ImpactItem {
    pub id: ImpactItemId,
    pub presence: TopologyPresence,
    /// The concrete executions the node covers before the change. A configuration node, a
    /// resource, or a node absent before covers none.
    pub branches_before: Option<ConcreteBranchCoverage>,
    /// The concrete executions the node covers after the change.
    pub branches_after: Option<ConcreteBranchCoverage>,
    /// Every operation the item is attributed to.
    pub contributors: Contributors,
    /// Every role the item plays, each once, with every operation that contributes it. A gate
    /// shared by several operations is therefore one role naming all of them.
    pub roles: BTreeMap<ImpactRole, Contributors>,
    pub rect: Rect,
}

impl ImpactItem {
    /// The branch group the item is drawn inside: the declared branch of every execution it
    /// covers, preferring the side the change leaves it on.
    fn group(&self) -> Option<BranchName> {
        let coverage = if self.presence.after() {
            self.branches_after.as_ref()
        } else {
            self.branches_before.as_ref()
        };
        match coverage {
            Some(
                ConcreteBranchCoverage::AllOfBranch { branch }
                | ConcreteBranchCoverage::Selected { branch, .. },
            ) => Some(branch.clone()),
            Some(ConcreteBranchCoverage::All | ConcreteBranchCoverage::Unbranched) | None => None,
        }
    }

    /// Whether a search matches the item's name or its kind.
    pub fn matches(&self, search: &GraphSearch) -> bool {
        search.matches(self.id.name()) || search.matches(self.id.caption())
    }
}

/// One relation of the impact graph, routed.
#[derive(Debug, Clone)]
pub struct ImpactEdge {
    pub id: ImpactEdgeId,
    pub presence: TopologyPresence,
    pub contributors: Contributors,
    pub route: RoutedEdge<ImpactItemId>,
}

/// The whole domain pausing, as one execution step requires or engaged it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainPause {
    pub domain: DomainName,
    /// The execution step that pauses the domain.
    pub step: TransactionOperationRange,
    pub engagement: Engagement,
}

/// The frame drawn around everything when the whole domain pauses. It is not a branch group: it
/// holds every item rather than the items running per branch, and its label sits outside the
/// drawing it frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DomainOutline {
    pub frame: Rect,
    pub label: Rect,
}

/// What the change does to the domain as a whole rather than to any one item.
#[derive(Debug, Clone, Default)]
pub struct DomainImpact {
    pub pauses: Vec<DomainPause>,
    pub lifecycle: BTreeMap<DomainLifecycleAction, Contributors>,
    /// Execution nodes whose buffered work is flushed although the affected graph does not hold
    /// them: a flush that precedes a pause reaches every execution node of the domain.
    pub flushed_elsewhere: BTreeMap<NodeRef, Contributors>,
    /// Present when the whole domain pauses and there is a drawing to frame.
    pub outline: Option<DomainOutline>,
}

/// A transaction's impact, ready to draw.
#[derive(Debug, Clone)]
pub struct ImpactGraph {
    pub items: BTreeMap<ImpactItemId, ImpactItem>,
    pub edges: BTreeMap<ImpactEdgeId, ImpactEdge>,
    pub groups: Vec<GroupRegion<BranchName>>,
    pub domain: DomainImpact,
    pub width: i32,
    pub height: i32,
}

impl ImpactGraph {
    /// The whole transaction: every execution step, composed in the order they apply.
    pub fn transaction(report: &TransactionImpactReport, outcome: ImpactOutcome) -> Self {
        let mut projection = Projection::default();
        for step in report.execution_steps() {
            projection.add_step(step, outcome);
        }
        projection.place()
    }

    /// One execution step's effective impact.
    pub fn execution_step(step: &ExecutionStepImpactReport, outcome: ImpactOutcome) -> Self {
        let mut projection = Projection::default();
        projection.add_step(step, outcome);
        projection.place()
    }

    /// What one operation contributed. A contribution carries no pause of its own: pausing is a
    /// fact about the execution step that contains the operation.
    pub fn operation(operation: &OperationImpactReport) -> Self {
        let mut projection = Projection::default();
        projection.add_effects(&operation.contribution);
        projection.place()
    }

    /// The items a view shows.
    pub fn items_in(&self, view: ImpactView) -> impl Iterator<Item = &ImpactItem> + '_ {
        self.items
            .values()
            .filter(move |item| view.shows(item.presence))
    }

    /// The relations a view shows.
    pub fn edges_in(&self, view: ImpactView) -> impl Iterator<Item = &ImpactEdge> + '_ {
        self.edges
            .values()
            .filter(move |edge| view.shows(edge.presence))
    }

    /// The items a search matches.
    pub fn matches<'a>(
        &'a self,
        search: &'a GraphSearch,
    ) -> impl Iterator<Item = &'a ImpactItem> + 'a {
        self.items.values().filter(move |item| item.matches(search))
    }

    /// The whole drawing, which the fit control frames.
    pub fn canvas_bounds(&self) -> GraphBounds {
        GraphBounds::canvas(self.width, self.height)
    }

    /// The region holding every item a search matches, which the search frames.
    pub fn search_bounds(&self, search: &GraphSearch) -> Option<GraphBounds> {
        let mut bounds = None;
        for item in self.matches(search) {
            GraphBounds::include(&mut bounds, GraphBounds::from_rect(item.rect));
        }
        bounds
    }

    /// The region a relation and the two items it joins occupy, which focusing on it frames.
    pub fn edge_bounds(&self, id: &ImpactEdgeId) -> Option<GraphBounds> {
        let edge = self.edges.get(id)?;
        let mut bounds = None;
        for endpoint in [&id.source, &id.target] {
            if let Some(item) = self.items.get(endpoint) {
                GraphBounds::include(&mut bounds, GraphBounds::from_rect(item.rect));
            }
        }
        for point in &edge.route.points {
            GraphBounds::include(&mut bounds, GraphBounds::from_point(point.0, point.1));
        }
        bounds
    }
}

/// A node as one side of a topology holds it: the executions it covers there, none for a
/// configuration node.
#[derive(Debug, Clone)]
struct Holding {
    branches: Option<ConcreteBranchCoverage>,
}

/// Where a node stands across the steps a drawing composes. The first step holding it says what
/// it was before the composed change, and the last step holding it says what it is afterwards.
#[derive(Debug, Clone)]
struct NodeSides {
    before: Option<Holding>,
    after: Option<Holding>,
}

impl NodeSides {
    fn presence(&self) -> TopologyPresence {
        TopologyPresence::from_sides(self.before.is_some(), self.after.is_some())
    }
}

/// Whether a topology relation is held before the composed change and after it, read the same
/// way as [`NodeSides`].
#[derive(Debug, Clone, Copy)]
struct RelationSides {
    before: bool,
    after: bool,
}

/// An item being assembled from every step that names it.
#[derive(Debug, Default)]
struct ItemDraft {
    /// Present once any composed topology holds the item. An item only a role names has none.
    sides: Option<NodeSides>,
    contributors: Contributors,
    roles: BTreeMap<ImpactRole, Contributors>,
}

impl ItemDraft {
    fn add_role(&mut self, role: ImpactRole, contributors: &Contributors) {
        self.contributors.add(contributors);
        self.roles.entry(role).or_default().add(contributors);
    }
}

/// Where a relation's sides come from.
#[derive(Debug)]
enum EdgeSides {
    /// A relation of the affected topology, on the sides the composed steps put it.
    Topology(RelationSides),
    /// A resource binding, which exists whenever the node binding the resource does.
    FollowsTarget,
}

#[derive(Debug)]
struct EdgeDraft {
    sides: EdgeSides,
    contributors: Contributors,
}

/// How a node's own configuration changes, from its first transition to its last.
#[derive(Debug)]
struct TransitionDraft {
    existed_before: bool,
    exists_after: bool,
    contributors: Contributors,
}

impl TransitionDraft {
    const fn change(&self) -> ConfigurationChange {
        match (self.existed_before, self.exists_after) {
            (false, true) => ConfigurationChange::Created,
            (true, true) => ConfigurationChange::Changed,
            (true, false) => ConfigurationChange::Dropped,
            (false, false) => ConfigurationChange::CreatedAndDropped,
        }
    }
}

/// A flushed execution, kept aside until the drawn items are known.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FlushedExecution {
    node: NodeRef,
    branches: Option<ConcreteBranchCoverage>,
}

/// The report content one drawing composes, assembled step by step and then placed once.
#[derive(Debug, Default)]
struct Projection {
    items: BTreeMap<ImpactItemId, ItemDraft>,
    edges: BTreeMap<ImpactEdgeId, EdgeDraft>,
    transitions: BTreeMap<NodeRef, TransitionDraft>,
    flushes: BTreeMap<FlushedExecution, Contributors>,
    domain: DomainImpact,
}

impl Projection {
    fn add_step(&mut self, step: &ExecutionStepImpactReport, outcome: ImpactOutcome) {
        match outcome {
            ImpactOutcome::Planned => {
                self.add_effects(&step.planned().effects);
                self.add_pause(
                    &step.planned().pause,
                    step.operations(),
                    &Engagement::Planned,
                );
            }
            ImpactOutcome::Actual => {
                self.add_effects(&step.actual().effects);
                for engagement in &step.actual().quiescence {
                    self.add_pause(
                        &engagement.requirement,
                        step.operations(),
                        &Engagement::Recorded(engagement.outcomes.clone()),
                    );
                }
            }
        }
    }

    fn add_effects(&mut self, effects: &ImpactEffects) {
        self.add_topology(&effects.topology);
        for change in &effects.changed_configuration {
            self.add_transition(&change.transition, Contributors::of(&change.attribution));
        }
        for moved in &effects.ownership_moves {
            let role = ImpactRole::Move {
                branches: moved.node.branches.clone(),
                source: moved.source.clone(),
                destination: moved.destination.clone(),
            };
            self.add_node_role(
                &moved.node.node,
                role,
                &Contributors::of(&moved.attribution),
            );
        }
        for lifecycle in &effects.lifecycle {
            self.domain
                .lifecycle
                .entry(lifecycle.action)
                .or_default()
                .add(&Contributors::of(&lifecycle.attribution));
        }
        for activation in &effects.activations {
            let role = ImpactRole::Activation {
                branches: activation.node.branches.clone(),
                action: activation.action,
            };
            let contributors = Contributors::of(&activation.attribution);
            self.add_node_role(&activation.node.node, role, &contributors);
        }
        for rebuild in &effects.rebuilds {
            let role = ImpactRole::Rebuild {
                branches: rebuild.node.branches.clone(),
                reason: rebuild.reason,
            };
            self.add_node_role(
                &rebuild.node.node,
                role,
                &Contributors::of(&rebuild.attribution),
            );
        }
        for reset in &effects.state_resets {
            let role = ImpactRole::StateReset {
                branches: reset.node.branches.clone(),
                state: reset.state,
            };
            self.add_node_role(
                &reset.node.node,
                role,
                &Contributors::of(&reset.attribution),
            );
        }
        for flush in &effects.force_flushes {
            let execution = FlushedExecution {
                node: flush.node.node.clone(),
                branches: flush.node.branches.clone(),
            };
            self.flushes
                .entry(execution)
                .or_default()
                .add(&Contributors::of(&flush.attribution));
        }
        for catalog in &effects.resource_catalog {
            let item = self
                .items
                .entry(ImpactItemId::Resource(catalog.resource.clone()))
                .or_default();
            item.add_role(
                ImpactRole::Catalog(catalog.action),
                &Contributors::of(&catalog.attribution),
            );
        }
        for binding in &effects.resource_bindings {
            let contributors = Contributors::of(&binding.attribution);
            let resource = ImpactItemId::Resource(binding.resource.clone());
            let node = ImpactItemId::Node(binding.node.clone());
            self.items.entry(node.clone()).or_default().add_role(
                ImpactRole::Binding {
                    resource: binding.resource.clone(),
                    requested: binding.requested,
                    version: binding.version,
                },
                &contributors,
            );
            self.items
                .entry(resource.clone())
                .or_default()
                .contributors
                .add(&contributors);
            let edge = self
                .edges
                .entry(ImpactEdgeId {
                    source: resource,
                    target: node,
                    relation: ImpactRelation::ResourceBinding,
                })
                .or_insert(EdgeDraft {
                    sides: EdgeSides::FollowsTarget,
                    contributors: Contributors::default(),
                });
            edge.contributors.add(&contributors);
        }
    }

    /// Compose one step's affected topology. Every node and relation either side holds becomes an
    /// item or an edge, including what only the side before holds, so dropped nodes and
    /// disconnected relations stay in the drawing.
    fn add_topology(&mut self, topology: &AffectedTopology) {
        let before = TopologySide::of(&topology.before);
        let after = TopologySide::of(&topology.after);

        let nodes = before
            .nodes
            .keys()
            .chain(after.nodes.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        for node in nodes {
            let item = self
                .items
                .entry(ImpactItemId::Node(node.clone()))
                .or_default();
            let mut held_before = None;
            if let Some(held) = before.nodes.get(node) {
                item.contributors.add(&held.contributors);
                held_before = Some(held.holding.clone());
            }
            let mut held_after = None;
            if let Some(held) = after.nodes.get(node) {
                item.contributors.add(&held.contributors);
                held_after = Some(held.holding.clone());
            }
            match &mut item.sides {
                // A later step that holds the node says what the composed change leaves it as.
                Some(sides) => sides.after = held_after,
                None => {
                    item.sides = Some(NodeSides {
                        before: held_before,
                        after: held_after,
                    });
                }
            }
        }

        let edges = before
            .edges
            .keys()
            .chain(after.edges.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for id in edges {
            let held_before = before.edges.get(&id);
            let held_after = after.edges.get(&id);
            match self.edges.get_mut(&id) {
                Some(edge) => {
                    if let EdgeSides::Topology(sides) = &mut edge.sides {
                        sides.after = held_after.is_some();
                    }
                    for contributors in [held_before, held_after].into_iter().flatten() {
                        edge.contributors.add(contributors);
                    }
                }
                None => {
                    let mut contributors = Contributors::default();
                    for held in [held_before, held_after].into_iter().flatten() {
                        contributors.add(held);
                    }
                    let sides = RelationSides {
                        before: held_before.is_some(),
                        after: held_after.is_some(),
                    };
                    self.edges.insert(
                        id,
                        EdgeDraft {
                            sides: EdgeSides::Topology(sides),
                            contributors,
                        },
                    );
                }
            }
        }
    }

    fn add_transition(&mut self, transition: &ConfigurationTransition, contributors: Contributors) {
        let (node, existed_before, exists_after) = match transition {
            ConfigurationTransition::Created { node } => (node, false, true),
            ConfigurationTransition::Changed { node } => (node, true, true),
            ConfigurationTransition::Dropped { node } => (node, true, false),
        };
        match self.transitions.get_mut(node) {
            Some(draft) => {
                draft.exists_after = exists_after;
                draft.contributors.add(&contributors);
            }
            None => {
                self.transitions.insert(
                    node.clone(),
                    TransitionDraft {
                        existed_before,
                        exists_after,
                        contributors,
                    },
                );
            }
        }
    }

    fn add_node_role(&mut self, node: &NodeRef, role: ImpactRole, contributors: &Contributors) {
        let item = self
            .items
            .entry(ImpactItemId::Node(node.clone()))
            .or_default();
        item.add_role(role, contributors);
    }

    fn add_pause(
        &mut self,
        requirement: &PauseRequirement,
        step: TransactionOperationRange,
        engagement: &Engagement,
    ) {
        match requirement {
            PauseRequirement::NoPause => {}
            PauseRequirement::Domain { domain } => self.domain.pauses.push(DomainPause {
                domain: domain.clone(),
                step,
                engagement: engagement.clone(),
            }),
            PauseRequirement::Subgraph { scope } => {
                for paused in scope.nodes() {
                    let role = ImpactRole::Pause {
                        branches: paused.coverage.branches.clone(),
                        engagement: engagement.clone(),
                    };
                    let contributors = Contributors::of(&paused.attribution);
                    self.add_node_role(&paused.coverage.node, role, &contributors);
                }
                for gate in scope.gate_boundaries() {
                    let relay = NodeRef::new(ModelKind::Relay, &gate.boundary.relay);
                    let item = self.items.entry(ImpactItemId::Node(relay)).or_default();
                    item.add_role(
                        ImpactRole::Gate {
                            branches: gate.boundary.branches.clone(),
                            engagement: engagement.clone(),
                        },
                        &Contributors::of(&gate.attribution),
                    );
                }
            }
        }
    }

    /// Settle what only the whole composition decides, then place everything once.
    fn place(mut self) -> ImpactGraph {
        for (node, draft) in std::mem::take(&mut self.transitions) {
            let item = self.items.entry(ImpactItemId::Node(node)).or_default();
            item.add_role(
                ImpactRole::Configuration(draft.change()),
                &draft.contributors,
            );
        }
        for (execution, contributors) in std::mem::take(&mut self.flushes) {
            match self
                .items
                .get_mut(&ImpactItemId::Node(execution.node.clone()))
            {
                Some(item) => item.add_role(
                    ImpactRole::ForceFlush {
                        branches: execution.branches,
                    },
                    &contributors,
                ),
                None => self
                    .domain
                    .flushed_elsewhere
                    .entry(execution.node)
                    .or_default()
                    .add(&contributors),
            }
        }

        let items = self.settled_items();
        let edges = self.settled_edges(&items);
        let layout_items = items
            .values()
            .map(|item| LayoutItem {
                id: item.id.clone(),
                width: IMPACT_ITEM_WIDTH,
                height: IMPACT_ITEM_HEIGHT,
                relay: item.id.is_relay(),
                branch: item.group(),
            })
            .collect::<Vec<_>>();
        let layout_edges = edges
            .iter()
            .map(|edge| LayoutEdge {
                id: edge.id.clone(),
                source: edge.id.source.clone(),
                target: edge.id.target.clone(),
                kind: edge.id.relation.layout_kind(),
                badge: false,
            })
            .collect::<Vec<_>>();
        let mut layout = Layout::build(&layout_items, &layout_edges);

        let mut placed_items = BTreeMap::new();
        for (id, mut item) in items {
            let Some(rect) = layout.items.get(&id).copied() else {
                continue;
            };
            item.rect = rect;
            placed_items.insert(id, item);
        }
        let mut placed_edges = BTreeMap::new();
        for edge in edges {
            let Some(route) = layout.edges.remove(&edge.id) else {
                continue;
            };
            placed_edges.insert(
                edge.id.clone(),
                ImpactEdge {
                    id: edge.id,
                    presence: edge.presence,
                    contributors: edge.contributors,
                    route,
                },
            );
        }

        let mut domain = self.domain;
        if !domain.pauses.is_empty() && !placed_items.is_empty() {
            domain.outline = Some(DomainOutline::around(layout.width, layout.height));
        }
        ImpactGraph {
            items: placed_items,
            edges: placed_edges,
            groups: layout.groups,
            domain,
            width: layout.width,
            height: layout.height,
        }
    }

    fn settled_items(&mut self) -> BTreeMap<ImpactItemId, ImpactItem> {
        let mut items = BTreeMap::new();
        for (id, draft) in std::mem::take(&mut self.items) {
            let presence = match &draft.sides {
                Some(sides) => sides.presence(),
                // A resource the change creates exists only afterwards; anything else a role
                // names stands on both sides of the change.
                None if draft.roles.keys().any(ImpactRole::creates_resource) => {
                    TopologyPresence::After
                }
                None => TopologyPresence::Both,
            };
            let mut branches_before = None;
            let mut branches_after = None;
            if let Some(sides) = draft.sides {
                if let Some(holding) = sides.before {
                    branches_before = holding.branches;
                }
                if let Some(holding) = sides.after {
                    branches_after = holding.branches;
                }
            }
            items.insert(
                id.clone(),
                ImpactItem {
                    id,
                    presence,
                    branches_before,
                    branches_after,
                    contributors: draft.contributors,
                    roles: draft.roles,
                    rect: Rect::default(),
                },
            );
        }
        items
    }

    fn settled_edges(&mut self, items: &BTreeMap<ImpactItemId, ImpactItem>) -> Vec<SettledEdge> {
        let mut edges = Vec::new();
        for (id, draft) in std::mem::take(&mut self.edges) {
            let presence = match &draft.sides {
                EdgeSides::Topology(sides) => {
                    TopologyPresence::from_sides(sides.before, sides.after)
                }
                EdgeSides::FollowsTarget => match items.get(&id.target) {
                    Some(target) => target.presence,
                    None => TopologyPresence::Both,
                },
            };
            edges.push(SettledEdge {
                id,
                presence,
                contributors: draft.contributors,
            });
        }
        edges
    }
}

/// A relation whose sides are settled, waiting for its route.
struct SettledEdge {
    id: ImpactEdgeId,
    presence: TopologyPresence,
    contributors: Contributors,
}

impl ImpactRole {
    fn creates_resource(&self) -> bool {
        match self {
            Self::Catalog(ResourceCatalogAction::Create) => true,
            Self::Configuration(_)
            | Self::Pause { .. }
            | Self::Gate { .. }
            | Self::Move { .. }
            | Self::Rebuild { .. }
            | Self::StateReset { .. }
            | Self::ForceFlush { .. }
            | Self::Activation { .. }
            | Self::Binding { .. } => false,
        }
    }
}

/// One node on one side of a step's topology, with the operations that put it there.
struct HeldNode {
    holding: Holding,
    contributors: Contributors,
}

/// One side of a step's topology, keyed the way the drawing names things. The report keys its
/// relations by the executions they join; the drawing joins items, so a relation whose endpoints
/// change their branch coverage is still one relation.
struct TopologySide<'a> {
    nodes: BTreeMap<&'a NodeRef, HeldNode>,
    edges: BTreeMap<ImpactEdgeId, Contributors>,
}

impl<'a> TopologySide<'a> {
    fn of(topology: &'a ImpactTopology) -> Self {
        let mut nodes = BTreeMap::new();
        for held in &topology.nodes {
            nodes.insert(
                &held.coverage.node,
                HeldNode {
                    holding: Holding {
                        branches: held.coverage.branches.clone(),
                    },
                    contributors: Contributors::of(&held.attribution),
                },
            );
        }
        let mut edges = BTreeMap::<ImpactEdgeId, Contributors>::new();
        for edge in &topology.edges {
            let id = ImpactEdgeId {
                source: ImpactItemId::Node(edge.source.node.clone()),
                target: ImpactItemId::Node(edge.target.node.clone()),
                relation: ImpactRelation::Topology(edge.kind),
            };
            edges
                .entry(id)
                .or_default()
                .add(&Contributors::of(&edge.attribution));
        }
        Self { nodes, edges }
    }
}

impl DomainOutline {
    /// The frame around a whole drawing of the given size, with its label in the band between the
    /// frame and the drawing.
    const fn around(width: i32, height: i32) -> Self {
        let frame = Rect {
            x: DOMAIN_OUTLINE_INSET,
            y: DOMAIN_OUTLINE_INSET,
            width: width - DOMAIN_OUTLINE_INSET * 2,
            height: height - DOMAIN_OUTLINE_INSET * 2,
        };
        let label = Rect {
            x: frame.x + DOMAIN_OUTLINE_INSET / 2,
            y: frame.y + (DOMAIN_OUTLINE_INSET - DOMAIN_LABEL_HEIGHT) / 2,
            width: frame.width - DOMAIN_OUTLINE_INSET,
            height: DOMAIN_LABEL_HEIGHT,
        };
        Self { frame, label }
    }
}

#[cfg(test)]
mod tests;
