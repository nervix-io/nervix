//! The validated execution graph of one domain.
//!
//! Layer: decisions.
//!
//! - **Owns.** The nodes and edges a domain's Models form, the fingerprints that detect a changed
//!   shape, the dataflow description a client renders, and the drop and cycle checks the graph
//!   answers.
//! - **Depends on.** The vocabulary and the dataflow-graph description.
//! - **Must not know.** How a node is placed or executed.
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use error_stack::Report;
use meticulous::{OptionExt, ResultExt};
use nervix_dataflow_graph::{
    DataflowBranch, DataflowEdge, DataflowEdgeKind, DataflowGraph, DataflowInputSide,
    DataflowMetricRef, DataflowNode, DataflowNodeRole, DataflowProcessorKind, DataflowSchemaField,
};
use nervix_models::{
    CreateSchema, DomainName, DomainSchedule, FieldName, IngestSource, Model, ModelIndex,
    ModelKind, ModelName, NodeRef, ParseAsType, PlacementPolicy, RelayName, SchemaField,
    SchemaName,
};
use petgraph::{
    Direction, algo::is_cyclic_directed, graph::DiGraph, prelude::NodeIndex, visit::EdgeRef,
};
use triomphe::Arc;

use crate::registry::{
    domain_state::DomainState,
    error::RegistryError,
    placement::{PlacementAnalysis, PlacementPlan},
    validation::branching::model_branch_selection,
};
#[derive(Debug, Clone)]
pub(crate) struct ActiveGraph {
    pub(in crate::registry) graph: DiGraph<ActiveNode, EdgeKind>,
    pub(in crate::registry) indices: HashMap<NodeRef, NodeIndex>,
    pub(in crate::registry) placement: PlacementAnalysis,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DataflowGraphCounts {
    pub(crate) nodes: usize,
    pub(crate) relays: usize,
}

impl ActiveGraph {
    pub(crate) fn from_scheduled_models(
        schedule: &DomainSchedule,
    ) -> Result<Self, Report<RegistryError>> {
        let models = schedule
            .nodes
            .values()
            .map(|node| node.config.as_ref().clone())
            .collect::<ModelIndex>();
        DomainState::build(&schedule.domain, &models).map(|state| state.graph)
    }

    pub(in crate::registry) fn placement_plan(
        &self,
        default_policy: PlacementPolicy,
    ) -> PlacementPlan {
        self.placement.plan(default_policy)
    }

    pub(crate) fn node(&self, kind: ModelKind, identifier: &ModelName) -> Option<&ActiveNode> {
        self.indices
            .get(&NodeRef::new(kind, identifier.clone()))
            .and_then(|index| self.graph.node_weight(*index))
    }

    pub(in crate::registry) fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    pub(in crate::registry) fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    pub(crate) fn dataflow_graph_counts(&self) -> DataflowGraphCounts {
        let mut nodes = HashSet::<String>::default();
        let mut relays = HashSet::<String>::default();
        for node in self
            .graph
            .node_weights()
            .filter(|node| node.is_dataflow_node())
        {
            if let ModelKind::Relay = node.kind {
                relays.insert(node.dataflow_id());
            } else {
                nodes.insert(node.dataflow_id());
            }
            if let Some(client) = node.dataflow_source_client() {
                nodes.insert(client.node.id);
            }
            if let Some(client) = node.dataflow_sink_client() {
                nodes.insert(client.node.id);
            }
        }
        DataflowGraphCounts {
            nodes: nodes.len(),
            relays: relays.len(),
        }
    }

    pub(crate) fn edges(&self) -> Vec<ActiveEdge> {
        self.graph
            .edge_references()
            .map(|edge| {
                let from = self
                    .graph
                    .node_weight(edge.source())
                    .verified("this endpoint comes from an edge of the same graph")
                    .identifier
                    .clone();
                let to = self
                    .graph
                    .node_weight(edge.target())
                    .verified("this endpoint comes from an edge of the same graph")
                    .identifier
                    .clone();
                ActiveEdge {
                    from,
                    to,
                    kind: *edge.weight(),
                }
            })
            .collect()
    }

    pub(crate) fn nodes(&self) -> Vec<ActiveNode> {
        self.graph.node_weights().cloned().collect()
    }

    pub(in crate::registry) fn dependent_dataflow_entities(
        &self,
        seeds: &HashSet<NodeRef>,
    ) -> HashSet<NodeRef> {
        let mut pending = seeds
            .iter()
            .filter_map(|key| self.indices.get(key).copied())
            .collect::<Vec<_>>();
        let mut visited = HashSet::default();
        let mut affected = HashSet::default();

        while let Some(index) = pending.pop() {
            if !visited.insert(index) {
                continue;
            }
            let node = self
                .graph
                .node_weight(index)
                .verified("this index came from the same graph, which is not modified here");
            if node.is_dataflow_node() {
                affected.insert(NodeRef {
                    kind: node.kind,
                    identifier: node.identifier.clone(),
                });
            }
            pending.extend(
                self.graph
                    .edges_directed(index, Direction::Outgoing)
                    .filter_map(|edge| {
                        (*edge.weight() == EdgeKind::RequiredBy).then_some(edge.target())
                    }),
            );
        }

        affected
    }

    pub(in crate::registry) fn schema_fingerprint_for_index(&self, index: NodeIndex) -> [u8; 32] {
        /// One schema model that a node's fingerprint covers, encoded so the hash reflects the
        /// exact stored shape rather than the order the graph walk reached it in.
        struct FingerprintedSchema {
            kind: ModelKind,
            identifier: ModelName,
            encoded: Vec<u8>,
        }

        let mut pending = vec![index];
        let mut visited = HashSet::default();
        let mut schemas = Vec::<FingerprintedSchema>::new();

        while let Some(index) = pending.pop() {
            if !visited.insert(index) {
                continue;
            }
            let node = self
                .graph
                .node_weight(index)
                .verified("this index came from the same graph, which is not modified here");
            if let Model::Schema(_)
            | Model::WireJsonSchema(_)
            | Model::WireCborSchema(_)
            | Model::WireAvroSchema(_) = node.config.as_ref()
            {
                schemas.push(FingerprintedSchema {
                    kind: node.kind,
                    identifier: node.identifier.clone(),
                    encoded: serde_json::to_vec(node.config.as_ref()).assured(
                        "registry models are plain serde structures with string keys, which \
                         serde_json always encodes",
                    ),
                });
            }
            pending.extend(
                self.graph
                    .edges_directed(index, Direction::Incoming)
                    .filter_map(|edge| {
                        (*edge.weight() == EdgeKind::RequiredBy).then_some(edge.source())
                    }),
            );
        }
        schemas.sort_by(|left, right| {
            left.kind
                .as_str()
                .cmp(right.kind.as_str())
                .then_with(|| left.identifier.as_str().cmp(right.identifier.as_str()))
        });

        let mut hasher = blake3::Hasher::new();
        for schema in schemas {
            hasher.update(schema.kind.as_str().as_bytes());
            hasher.update(&[0]);
            hasher.update(schema.identifier.as_str().as_bytes());
            hasher.update(&[0]);
            hasher.update(&schema.encoded);
            hasher.update(&[0]);
        }
        *hasher.finalize().as_bytes()
    }

    pub(crate) fn schema_fingerprint(
        &self,
        kind: ModelKind,
        identifier: &ModelName,
    ) -> Option<[u8; 32]> {
        self.indices
            .get(&NodeRef::new(kind, identifier.clone()))
            .map(|index| self.schema_fingerprint_for_index(*index))
    }

    pub(in crate::registry) fn describe(&self) -> String {
        self.to_dataflow_graph("").render_ascii()
    }

    /// The graph the console draws: every dataflow node, the record flow between them, the
    /// external clients at either end, and the materialized state they read.
    pub(crate) fn to_dataflow_graph(&self, domain: impl Into<String>) -> DataflowGraph {
        let mut schemas = HashMap::default();
        for index in self.graph.node_indices() {
            let node = self
                .graph
                .node_weight(index)
                .verified("this index came from the same graph, which is not modified here");
            let Model::Schema(schema) = node.config.as_ref() else {
                continue;
            };
            schemas.insert(node.identifier.clone(), schema.clone());
        }

        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        // Every dataflow node is walked, whether or not an edge reaches it, so a node nothing
        // sends to or reads from is still drawn. A flow target is itself a dataflow node and so
        // is walked in its own turn, which is why the traversal below never has to remember one.
        for source_index in self.graph.node_indices() {
            let source = self
                .graph
                .node_weight(source_index)
                .verified("this index came from the same graph, which is not modified here");
            if !source.is_dataflow_node() {
                continue;
            }
            nodes.push(source.to_dataflow_node(&schemas));

            for visible_target in visible_dataflow_targets(&self.graph, source_index) {
                let target = self
                    .graph
                    .node_weight(visible_target.index)
                    .verified("this index came from the same graph, which is not modified here");
                let flow_edge =
                    source.dataflow_edge_to(target, dataflow_edge_kind(visible_target.edge_kind));
                edges.push(flow_edge);
            }

            if let Some(client) = source.dataflow_source_client() {
                let ingest_edge = DataflowEdge::data(
                    client.node.id.clone(),
                    source.dataflow_id(),
                    DataflowEdgeKind::Data,
                )
                .with_metric(client.metric);
                edges.push(ingest_edge);
                nodes.push(client.node);
            }
            if let Some(client) = source.dataflow_sink_client() {
                let emit_edge = DataflowEdge::data(
                    source.dataflow_id(),
                    client.node.id.clone(),
                    DataflowEdgeKind::Data,
                )
                .with_metric(client.metric);
                edges.push(emit_edge);
                nodes.push(client.node);
            }

            edges.extend(source.dataflow_state_link_edges());
        }

        // One drawn node and one drawn edge per identity. A client several nodes name is drawn
        // once, and a generator's source relay, which arrives both as converted record flow and
        // as a state-link declaration, keeps a single state link.
        nodes.sort_by(|left, right| left.id.cmp(&right.id));
        nodes.dedup_by(|left, right| left.id == right.id);
        edges.sort_by(|left, right| {
            left.source
                .cmp(&right.source)
                .then_with(|| left.target.cmp(&right.target))
                .then_with(|| left.kind.cmp(&right.kind))
        });
        edges.dedup_by(|left, right| {
            left.source == right.source && left.target == right.target && left.kind == right.kind
        });

        DataflowGraph {
            domain: domain.into(),
            statistics: Default::default(),
            nodes,
            edges,
        }
    }
}

/// One node reached by following visible edges, named by the edge kind that leads to it. A node
/// the console does not draw is walked through rather than drawn, so what arrives at the far side
/// keeps the kind the first visible edge carried.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct VisibleDataflowTarget {
    index: NodeIndex,
    edge_kind: EdgeKind,
}

/// The drawn nodes `source_index` reaches, walking through everything the console does not draw.
/// Every returned index is a dataflow node, so a caller already visiting every dataflow node
/// learns of no further node here.
fn visible_dataflow_targets(
    graph: &DiGraph<ActiveNode, EdgeKind>,
    source_index: NodeIndex,
) -> Vec<VisibleDataflowTarget> {
    let mut pending = Vec::new();
    for edge in graph.edges_directed(source_index, Direction::Outgoing) {
        let edge_kind = *edge.weight();
        if edge_kind.is_visible_dataflow_edge() {
            pending.push(VisibleDataflowTarget {
                index: edge.target(),
                edge_kind,
            });
        }
    }

    let mut targets = Vec::new();
    let mut visited = HashSet::new();
    while let Some(step) = pending.pop() {
        if !visited.insert(step) {
            continue;
        }
        let node = graph
            .node_weight(step.index)
            .verified("this index came from the same graph, which is not modified here");
        if node.is_dataflow_node() {
            targets.push(step);
            continue;
        }
        for edge in graph.edges_directed(step.index, Direction::Outgoing) {
            if edge.weight().is_visible_dataflow_edge() {
                pending.push(VisibleDataflowTarget {
                    index: edge.target(),
                    edge_kind: step.edge_kind,
                });
            }
        }
    }

    targets
}

const fn dataflow_edge_kind(kind: EdgeKind) -> DataflowEdgeKind {
    match kind {
        EdgeKind::RequiredBy => DataflowEdgeKind::Data,
        EdgeKind::SendsTo => DataflowEdgeKind::Data,
        EdgeKind::CorrelationTimeout => DataflowEdgeKind::CorrelationTimeout,
        EdgeKind::MessageError => DataflowEdgeKind::MessageError,
    }
}

/// One edge of an active graph, named by the models it joins and what the dependency means.
#[derive(Debug, Clone)]
pub(crate) struct ActiveEdge {
    pub(crate) from: ModelName,
    pub(crate) to: ModelName,
    pub(crate) kind: EdgeKind,
}

/// An external system drawn beside the node that talks to it: the client's own drawn node,
/// together with the metric counting what crosses that boundary.
struct DataflowClient {
    node: DataflowNode,
    metric: DataflowMetricRef,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveNode {
    pub(crate) identifier: ModelName,
    pub(crate) kind: ModelKind,
    pub(crate) config: Arc<Model>,
    pub(crate) effective_branching: Option<Vec<FieldName>>,
    pub(crate) effective_branching_schema: Option<SchemaName>,
}

impl ActiveNode {
    /// How this node is addressed: the kind it is and the name it carries, together.
    pub(in crate::registry) fn node_ref(&self) -> NodeRef {
        NodeRef::new(self.kind, self.identifier.clone())
    }

    fn dataflow_id(&self) -> String {
        format!("{}:{}", self.kind.as_str(), self.identifier.as_str())
    }

    /// The external system an ingestor reads from. The ingest and emit sides of one named client
    /// are separate identities, so a client both ingested from and emitted to is drawn twice.
    fn dataflow_source_client(&self) -> Option<DataflowClient> {
        let Model::Ingestor(ingestor) = self.config.as_ref() else {
            return None;
        };
        let source = ingestor.source.source_ref();
        let source_kind = ingestor.source.source_kind().as_str();
        let node = DataflowNode::new(
            format!("{}_source:{}", source_kind, source.as_str()),
            source.as_str(),
            DataflowNodeRole::Client {
                transport: ingestor.source.transport_label().to_string(),
            },
        );
        let metric = DataflowMetricRef::new(
            self.kind.as_str().to_ascii_uppercase(),
            self.identifier.as_str(),
            "received",
            None::<String>,
        );
        Some(DataflowClient { node, metric })
    }

    /// The external system an emitter writes to. The metric names the input relay only when the
    /// emitter has exactly one, since that is what makes the count attributable to a relay.
    fn dataflow_sink_client(&self) -> Option<DataflowClient> {
        let Model::Emitter(emitter) = self.config.as_ref() else {
            return None;
        };
        let client = emitter.sink.client();
        let node = DataflowNode::new(
            format!("client_sink:{}", client.as_str()),
            client.as_str(),
            DataflowNodeRole::Client {
                transport: emitter.sink.transport_label().to_string(),
            },
        );
        let sole_input_relay = if emitter.from.relays().len() == 1 {
            emitter.from.first().map(|relay| relay.as_str().to_string())
        } else {
            None
        };
        let metric = DataflowMetricRef::new(
            self.kind.as_str().to_ascii_uppercase(),
            self.identifier.as_str(),
            "sent",
            sole_input_relay,
        );
        Some(DataflowClient { node, metric })
    }

    /// The drawn edge from this node to `target`. A generator reads its source relay as
    /// materialized state rather than receiving its records, so that one edge becomes a state
    /// link instead of record flow.
    fn dataflow_edge_to(&self, target: &Self, kind: DataflowEdgeKind) -> DataflowEdge {
        if kind == DataflowEdgeKind::Data
            && target.kind == ModelKind::Generator
            && target.reads_materialized_state_from(&RelayName::from(&self.identifier))
        {
            return DataflowEdge::data(
                self.dataflow_id(),
                target.dataflow_id(),
                DataflowEdgeKind::StateLink,
            );
        }
        DataflowEdge::data(self.dataflow_id(), target.dataflow_id(), kind)
            .with_metric(self.dataflow_metric_for_target(target))
            .with_input_side(target.correlator_input_side(&RelayName::from(&self.identifier)))
            .with_routes(self.dataflow_routes_to(target, kind))
    }

    /// Materialized-state dependencies drawn as state links. Every declaration is included; the
    /// generator's own source relay arrives here as well as through its converted flow edge, and
    /// the two are identical so the graph's edge deduplication keeps exactly one.
    fn dataflow_state_link_edges(&self) -> Vec<DataflowEdge> {
        self.config
            .materialized_state_relays()
            .into_iter()
            .map(|relay| {
                DataflowEdge::data(
                    format!("{}:{}", ModelKind::Relay.as_str(), relay.as_str()),
                    self.dataflow_id(),
                    DataflowEdgeKind::StateLink,
                )
            })
            .collect()
    }

    fn reads_materialized_state_from(&self, relay: &RelayName) -> bool {
        self.config
            .materialized_state_relays()
            .into_iter()
            .any(|declared| declared == relay)
    }

    /// Which side of a correlator an input relay enters. Correlators are the only nodes whose
    /// inputs are distinguishable, and the console labels the two sides.
    fn correlator_input_side(&self, source: &RelayName) -> Option<DataflowInputSide> {
        let Model::Correlator(correlator) = self.config.as_ref() else {
            return None;
        };
        if correlator.left.from.iter().any(|relay| relay == source) {
            return Some(DataflowInputSide::Left);
        }
        correlator
            .right
            .from
            .iter()
            .any(|relay| relay == source)
            .then_some(DataflowInputSide::Right)
    }

    /// How many declared routes this node sends to `target`. Several routes to one relay are
    /// drawn as a single edge, so the count is what tells the reader they were collapsed.
    fn dataflow_routes_to(&self, target: &Self, kind: DataflowEdgeKind) -> u32 {
        if kind != DataflowEdgeKind::Data || target.kind != ModelKind::Relay {
            return 1;
        }
        let Some(outputs) = self.config.output_routes() else {
            return 1;
        };
        let routes = outputs
            .routes
            .iter()
            .filter(|route| route.relay == RelayName::from(&target.identifier))
            .count();
        u32::try_from(routes).unwrap_or(u32::MAX).max(1)
    }

    fn dataflow_metric_for_target(&self, target: &ActiveNode) -> DataflowMetricRef {
        if let ModelKind::Relay = target.kind {
            return DataflowMetricRef::new(
                self.kind.as_str().to_ascii_uppercase(),
                self.identifier.as_str(),
                "sent",
                Some(target.identifier.as_str().to_string()),
            );
        }
        DataflowMetricRef::new(
            target.kind.as_str().to_ascii_uppercase(),
            target.identifier.as_str(),
            "received",
            Some(self.identifier.as_str().to_string()),
        )
    }

    fn to_dataflow_node(&self, schemas: &HashMap<ModelName, CreateSchema>) -> DataflowNode {
        let node = DataflowNode::new(
            self.dataflow_id(),
            self.identifier.as_str(),
            self.dataflow_role(),
        )
        .with_branch(self.dataflow_branch());
        match self.config.as_ref() {
            Model::Relay(relay) => {
                let Some(schema) = schemas.get(&ModelName::from(&relay.schema)) else {
                    return node;
                };
                node.with_schema(
                    schema.name.as_str(),
                    schema
                        .fields
                        .iter()
                        .map(dataflow_schema_field)
                        .collect::<Vec<_>>(),
                )
            }
            _ => node,
        }
    }

    fn dataflow_role(&self) -> DataflowNodeRole {
        match self.kind {
            ModelKind::Ingestor => DataflowNodeRole::Ingestor {
                transport: ingestor_subtype(self.config.as_ref()).to_string(),
            },
            ModelKind::Emitter => DataflowNodeRole::Emitter {
                transport: emitter_subtype(self.config.as_ref()).to_string(),
            },
            ModelKind::Relay => DataflowNodeRole::Relay,
            kind => DataflowNodeRole::Processor {
                processor: dataflow_processor_kind(kind).verified(
                    "only nodes accepted by is_dataflow_node reach here, and the kinds left after \
                     ingestor, emitter and relay all map to a processor",
                ),
            },
        }
    }

    /// The branch this node runs under, named as declared. Nodes that run once, outside any
    /// branch, resolve to no branch at all.
    fn dataflow_branch(&self) -> Option<DataflowBranch> {
        let name = match self.config.as_ref() {
            Model::Relay(relay) => relay.branching.branch()?,
            model => model_branch_selection(model)?.branch_ref()?,
        };
        Some(DataflowBranch {
            name: name.as_str().to_string(),
            key_schema: self
                .effective_branching_schema
                .as_ref()?
                .as_str()
                .to_string(),
            key_fields: self
                .effective_branching
                .iter()
                .flatten()
                .map(|field| field.as_str().to_string())
                .collect(),
        })
    }

    pub(in crate::registry) fn is_dataflow_node(&self) -> bool {
        matches!(
            self.kind,
            ModelKind::Ingestor
                | ModelKind::Relay
                | ModelKind::Generator
                | ModelKind::Inferencer
                | ModelKind::WasmProcessor
                | ModelKind::Reingestor
                | ModelKind::Correlator
                | ModelKind::Junction
                | ModelKind::Deduplicator
                | ModelKind::Reorderer
                | ModelKind::WindowProcessor
                | ModelKind::Emitter
        )
    }
}

fn dataflow_schema_field(field: &SchemaField) -> DataflowSchemaField {
    DataflowSchemaField {
        name: field.name.as_str().to_string(),
        ty: parse_as_to_dataflow_label(&field.ty),
        optional: field.optional,
        sensitive: field.sensitive,
    }
}

fn parse_as_to_dataflow_label(ty: &ParseAsType) -> String {
    match ty {
        ParseAsType::U8 => "U8".to_string(),
        ParseAsType::I8 => "I8".to_string(),
        ParseAsType::U16 => "U16".to_string(),
        ParseAsType::I16 => "I16".to_string(),
        ParseAsType::U32 => "U32".to_string(),
        ParseAsType::I32 => "I32".to_string(),
        ParseAsType::U64 => "U64".to_string(),
        ParseAsType::I64 => "I64".to_string(),
        ParseAsType::Bool => "BOOL".to_string(),
        ParseAsType::String => "STRING".to_string(),
        ParseAsType::Datetime => "DATETIME".to_string(),
        ParseAsType::F32 => "F32".to_string(),
        ParseAsType::F64 => "F64".to_string(),
        ParseAsType::Array { element, len } => {
            format!("ARRAY<{}, {}>", parse_as_to_dataflow_label(element), len)
        }
        ParseAsType::Vec { element } => format!("VEC<{}>", parse_as_to_dataflow_label(element)),
    }
}

fn ingestor_subtype(model: &Model) -> &str {
    let Model::Ingestor(ingestor) = model else {
        return "INGESTOR";
    };
    if let IngestSource::Endpoint { .. } = ingestor.source {
        return "INGESTOR";
    }
    ingestor.source.transport_label()
}

fn emitter_subtype(model: &Model) -> &str {
    let Model::Emitter(emitter) = model else {
        return "EMITTER";
    };
    emitter.sink.transport_label()
}

const fn dataflow_processor_kind(kind: ModelKind) -> Option<DataflowProcessorKind> {
    match kind {
        ModelKind::Junction => Some(DataflowProcessorKind::Junction),
        ModelKind::Deduplicator => Some(DataflowProcessorKind::Deduplicator),
        ModelKind::Correlator => Some(DataflowProcessorKind::Correlator),
        ModelKind::Reorderer => Some(DataflowProcessorKind::Reorderer),
        ModelKind::WindowProcessor => Some(DataflowProcessorKind::WindowProcessor),
        ModelKind::WasmProcessor => Some(DataflowProcessorKind::WasmProcessor),
        ModelKind::Inferencer => Some(DataflowProcessorKind::Inferencer),
        ModelKind::Generator => Some(DataflowProcessorKind::Generator),
        ModelKind::Reingestor => Some(DataflowProcessorKind::Reingestor),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum EdgeKind {
    RequiredBy,
    SendsTo,
    CorrelationTimeout,
    MessageError,
}

impl EdgeKind {
    const fn is_visible_dataflow_edge(self) -> bool {
        self.is_runtime_flow_edge()
    }

    pub(in crate::registry) const fn is_runtime_flow_edge(self) -> bool {
        match self {
            Self::RequiredBy => false,
            Self::SendsTo | Self::CorrelationTimeout | Self::MessageError => true,
        }
    }
}

pub(in crate::registry) fn schedulable_depth(
    graph: &DiGraph<ActiveNode, EdgeKind>,
    index: NodeIndex,
    cache: &mut HashMap<NodeIndex, usize>,
) -> usize {
    schedulable_depth_inner(graph, index, cache, &mut HashSet::new())
}

fn schedulable_depth_inner(
    graph: &DiGraph<ActiveNode, EdgeKind>,
    index: NodeIndex,
    cache: &mut HashMap<NodeIndex, usize>,
    visiting: &mut HashSet<NodeIndex>,
) -> usize {
    if let Some(depth) = cache.get(&index) {
        return *depth;
    }
    if !visiting.insert(index) {
        return 0;
    }

    let mut max_depth = 0usize;
    for edge in graph.edges_directed(index, Direction::Incoming) {
        if !edge.weight().is_runtime_flow_edge() {
            continue;
        }
        let source = edge.source();
        let source_node = graph
            .node_weight(source)
            .verified("this endpoint comes from an edge of the same graph");
        let candidate_depth = if is_schedulable_model(source_node.config.as_ref()) {
            schedulable_depth_inner(graph, source, cache, visiting) + 1
        } else {
            schedulable_depth_inner(graph, source, cache, visiting)
        };
        max_depth = max_depth.max(candidate_depth);
    }

    visiting.remove(&index);
    cache.insert(index, max_depth);
    max_depth
}

pub(in crate::registry) fn is_schedulable_model(model: &Model) -> bool {
    matches!(
        model,
        Model::Generator(_)
            | Model::Inferencer(_)
            | Model::Ingestor(_)
            | Model::Reingestor(_)
            | Model::Relay(_)
            | Model::Lookup(_)
            | Model::Deduplicator(_)
            | Model::Correlator(_)
            | Model::Reorderer(_)
            | Model::Junction(_)
            | Model::WindowProcessor(_)
            | Model::WasmProcessor(_)
            | Model::Emitter(_)
    )
}

pub(in crate::registry) fn expect_kind(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    indices: &HashMap<NodeRef, NodeIndex>,
    referenced: impl Into<ModelName>,
    expected_kind: ModelKind,
) -> Result<NodeIndex, Report<RegistryError>> {
    expect_node(
        domain,
        identifier,
        models,
        indices,
        &NodeRef::new(expected_kind, referenced),
    )
}

pub(in crate::registry) fn expect_node(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    indices: &HashMap<NodeRef, NodeIndex>,
    referenced: &NodeRef,
) -> Result<NodeIndex, Report<RegistryError>> {
    models.get(referenced).ok_or_else(|| {
        Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: referenced.kind.as_str(),
            reference: referenced.identifier.as_str().to_string(),
        })
    })?;

    Ok(*indices
        .get(referenced)
        .verified("the reference was resolved above, and every resolved model has an index"))
}

pub(in crate::registry) fn has_required_by_cycle(graph: &DiGraph<ActiveNode, EdgeKind>) -> bool {
    let mut required_by_graph = DiGraph::<(), ()>::new();
    let mut node_map = HashMap::new();

    for index in graph.node_indices() {
        node_map.insert(index, required_by_graph.add_node(()));
    }

    for edge in graph.edge_references() {
        if *edge.weight() != EdgeKind::RequiredBy {
            continue;
        }
        let source = *node_map
            .get(&edge.source())
            .verified("this endpoint comes from an edge of the same graph");
        let target = *node_map
            .get(&edge.target())
            .verified("this endpoint comes from an edge of the same graph");
        required_by_graph.add_edge(source, target, ());
    }

    is_cyclic_directed(&required_by_graph)
}

pub(in crate::registry) fn ensure_drop_targets_are_not_in_use(
    domain: &DomainName,
    graph: &ActiveGraph,
    drops_in_batch: &HashSet<NodeRef>,
) -> Result<(), Report<RegistryError>> {
    for key in drops_in_batch {
        let Some(index) = graph.indices.get(key).copied() else {
            continue;
        };

        let mut blockers = Vec::new();
        for blocker_index in graph.graph.edges_directed(index, Direction::Outgoing) {
            if *blocker_index.weight() != EdgeKind::RequiredBy {
                continue;
            }
            let blocker = graph
                .graph
                .node_weight(blocker_index.target())
                .verified("this endpoint comes from an edge of the same graph")
                .clone();
            if !drops_in_batch.contains(&blocker.node_ref()) {
                blockers.push(blocker.identifier);
            }
        }
        blockers.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        blockers.dedup_by(|a, b| a.as_str() == b.as_str());

        if !blockers.is_empty() {
            return Err(Report::new(RegistryError::DeleteInUse {
                domain: domain.as_str().to_string(),
                identifier: key.identifier.as_str().to_string(),
                blockers: blockers
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            }));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nervix_models::{
        Assignment, AssignmentTarget, CorrelationTimeoutAction, CorrelationTimeoutPolicy,
        MessageErrorPolicy,
    };

    use super::*;
    use crate::registry::{
        storage::Registry,
        test_fixtures::{
            branch_for_relay, branch_schema, client_model, codec, emitter,
            explicitly_unbranched_relay, full_graph_batch, ingestor_with_params,
            materialized_relay, named, processor, relay_branched_by_relay_branch,
            relay_branched_like, schema, temp_db_path, unbranched_correlator, unbranched_ingestor,
            wasm_processor, wire_schema,
        },
    };

    #[test]
    fn apply_batch_builds_full_graph_in_single_batch() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("full graph batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        assert_eq!(graph.node_count(), 12);
        assert_eq!(graph.edge_count(), 21);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_includes_deduplicator_between_two_relays() {
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
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("raw_events", "event_schema"),
                    relay_branched_like("deduped_events", "event_schema", "raw_events"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("raw_events", "value_branch"),
                    ingestor_with_params(
                        "ingest_events",
                        "raw_events",
                        "event_codec",
                        "broker_in",
                        &["value"],
                    ),
                    processor("dedup_events", "raw_events", "deduped_events"),
                ],
            )
            .expect("deduplicator graph should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        assert_eq!(
            graph.dataflow_graph_counts(),
            DataflowGraphCounts {
                nodes: 3,
                relays: 2,
            }
        );
        let dataflow_graph = graph.to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert!(
            node_ids.contains(&"relay:raw_events"),
            "raw relay missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains(&"deduplicator:dedup_events"),
            "deduplicator missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains(&"relay:deduped_events"),
            "deduped relay missing from {node_ids:?}"
        );
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
        assert_eq!(branches.get("ingestor:ingest_events"), Some(&None));
        assert_eq!(
            branches.get("relay:raw_events"),
            Some(&Some(("by_raw_events", "value_branch")))
        );
        assert_eq!(
            branches.get("relay:deduped_events"),
            Some(&Some(("by_raw_events", "value_branch")))
        );
        let edges = dataflow_graph
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str()))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            edges,
            std::collections::BTreeSet::from([
                ("client_source:broker_in", "ingestor:ingest_events"),
                ("ingestor:ingest_events", "relay:raw_events"),
                ("relay:raw_events", "deduplicator:dedup_events"),
                ("deduplicator:dedup_events", "relay:deduped_events"),
            ])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_includes_wasm_processor_between_two_relays() {
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
                    client_model("broker_in"),
                    explicitly_unbranched_relay("raw_events", "event_schema"),
                    explicitly_unbranched_relay("filtered_events", "event_schema"),
                    unbranched_ingestor("ingest_events", "raw_events", "event_codec", "broker_in"),
                    wasm_processor("filter_events", "raw_events", "filtered_events"),
                ],
            )
            .expect("wasm processor graph should succeed");

        let dataflow_graph = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert!(
            node_ids.contains(&"relay:raw_events"),
            "raw relay missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains(&"wasm_processor:filter_events"),
            "wasm processor missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains(&"relay:filtered_events"),
            "filtered relay missing from {node_ids:?}"
        );
        let edges = dataflow_graph
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str()))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            edges,
            std::collections::BTreeSet::from([
                ("client_source:broker_in", "ingestor:ingest_events"),
                ("ingestor:ingest_events", "relay:raw_events"),
                ("relay:raw_events", "wasm_processor:filter_events"),
                ("wasm_processor:filter_events", "relay:filtered_events"),
            ])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_keeps_reused_ingest_and_emit_client_nodes_separate() {
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
                    client_model("broker"),
                    explicitly_unbranched_relay("raw_events", "event_schema"),
                    unbranched_ingestor("ingest_events", "raw_events", "event_codec", "broker"),
                    emitter("emit_events", "raw_events", "event_codec", "broker"),
                ],
            )
            .expect("client reuse graph should succeed");

        let dataflow_graph = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            node_ids.contains("client_source:broker"),
            "source client missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains("client_sink:broker"),
            "sink client missing from {node_ids:?}"
        );
        let edges = dataflow_graph
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str()))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            edges,
            std::collections::BTreeSet::from([
                ("client_source:broker", "ingestor:ingest_events"),
                ("ingestor:ingest_events", "relay:raw_events"),
                ("relay:raw_events", "emitter:emit_events"),
                ("emitter:emit_events", "client_sink:broker"),
            ])
        );
        let sink_metric = dataflow_graph
            .edges
            .iter()
            .find(|edge| edge.target == "client_sink:broker")
            .and_then(|edge| edge.metric.as_ref())
            .expect("single-input emitter sink edge must carry a metric");
        assert_eq!(sink_metric.relay.as_deref(), Some("raw_events"));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_includes_correlator_between_input_and_output_relays() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    explicitly_unbranched_relay("left_events", "event_schema"),
                    explicitly_unbranched_relay("right_events", "event_schema"),
                    explicitly_unbranched_relay("matched_events", "event_schema"),
                    explicitly_unbranched_relay("uncorrelated_left_events", "event_schema"),
                    explicitly_unbranched_relay("uncorrelated_right_events", "event_schema"),
                    explicitly_unbranched_relay("correlator_errors", "event_schema"),
                    {
                        let Model::Correlator(mut correlator) = unbranched_correlator(
                            "match_events",
                            "left_events",
                            "right_events",
                            "matched_events",
                        ) else {
                            unreachable!("helper must return correlator")
                        };
                        correlator.timeout_policy = CorrelationTimeoutPolicy {
                            left: CorrelationTimeoutAction::SendTo {
                                relay: named("uncorrelated_left_events"),
                            },
                            right: CorrelationTimeoutAction::SendTo {
                                relay: named("uncorrelated_right_events"),
                            },
                        };
                        correlator.output_routes.routes[0].message_error_policy =
                            MessageErrorPolicy::Dlq {
                                relay: named("correlator_errors"),
                                assignments: vec![Assignment {
                                    target: AssignmentTarget::bare(named("value")),
                                    value: nervix_nspl::parse_expression("left.value")
                                        .expect("error assignment must parse"),
                                }],
                            };
                        Model::Correlator(correlator)
                    },
                ],
            )
            .expect("correlator graph should succeed");

        let dataflow_graph = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert!(
            node_ids.contains(&"correlator:match_events"),
            "correlator missing from {node_ids:?}"
        );
        let edges = dataflow_graph
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str(), edge.kind))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            edges,
            std::collections::BTreeSet::from([
                (
                    "relay:left_events",
                    "correlator:match_events",
                    DataflowEdgeKind::Data,
                ),
                (
                    "relay:right_events",
                    "correlator:match_events",
                    DataflowEdgeKind::Data,
                ),
                (
                    "correlator:match_events",
                    "relay:matched_events",
                    DataflowEdgeKind::Data,
                ),
                (
                    "correlator:match_events",
                    "relay:uncorrelated_left_events",
                    DataflowEdgeKind::CorrelationTimeout,
                ),
                (
                    "correlator:match_events",
                    "relay:uncorrelated_right_events",
                    DataflowEdgeKind::CorrelationTimeout,
                ),
                (
                    "correlator:match_events",
                    "relay:correlator_errors",
                    DataflowEdgeKind::MessageError,
                ),
            ])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_represents_materialized_state_with_the_relay_node() {
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
                    client_model("broker_in"),
                    materialized_relay("state_txns", "event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("state_txns", "value_branch"),
                    ingestor_with_params(
                        "state_txns_ingestor",
                        "state_txns",
                        "event_codec",
                        "broker_in",
                        &["value"],
                    ),
                ],
            )
            .expect("materialized relay graph should succeed");

        let dataflow_graph = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert!(
            node_ids.contains(&"ingestor:state_txns_ingestor"),
            "ingestor missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains(&"relay:state_txns"),
            "relay missing from {node_ids:?}"
        );
        let edges = dataflow_graph
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str()))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            edges,
            std::collections::BTreeSet::from([
                ("client_source:broker_in", "ingestor:state_txns_ingestor"),
                ("ingestor:state_txns_ingestor", "relay:state_txns")
            ])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_draws_a_relay_nothing_reads_or_writes() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    explicitly_unbranched_relay("raw_events", "event_schema"),
                ],
            )
            .expect("isolated relay graph should succeed");

        let dataflow_graph = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(node_ids, vec!["relay:raw_events"]);
        let relay_node = dataflow_graph
            .nodes
            .first()
            .expect("the relay is the one drawn node");
        assert_eq!(relay_node.schema.as_deref(), Some("event_schema"));
        assert!(
            dataflow_graph.edges.is_empty(),
            "a relay nothing reads or writes has no edges, found {:?}",
            dataflow_graph.edges
        );

        let _ = fs::remove_dir_all(path);
    }
}
