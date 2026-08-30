use std::collections::BTreeMap;

use ascii_dag::{Graph, LayoutConfig, RenderMode};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataflowGraph {
    pub domain: String,
    pub statistics: DataflowStatistics,
    pub nodes: Vec<DataflowNode>,
    pub edges: Vec<DataflowEdge>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataflowNode {
    pub id: String,
    pub label: String,
    pub role: DataflowNodeRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema_fields: Vec<DataflowSchemaField>,
    #[serde(default)]
    pub status: DataflowNodeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_wait_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<DataflowBranch>,
    #[serde(default, skip_serializing_if = "DataflowStatistics::is_empty")]
    pub statistics: DataflowStatistics,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branches: Vec<DataflowBranchStatistics>,
}

/// What a graph node is, carrying the detail that distinguishes nodes of the same kind. The
/// console renders directly from this; it never re-derives a node's nature from a label.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataflowNodeRole {
    Client { transport: String },
    Ingestor { transport: String },
    Processor { processor: DataflowProcessorKind },
    Emitter { transport: String },
    Relay,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DataflowProcessorKind {
    Junction,
    Deduplicator,
    Correlator,
    Reorderer,
    WindowProcessor,
    WasmProcessor,
    Inferencer,
    Generator,
    Reingestor,
}

/// The branch a node runs under: the declared branch name, the schema of its key, and the key's
/// field names. Nodes that run once, outside any branch, carry no branch at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataflowBranch {
    pub name: String,
    pub key_schema: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataflowSchemaField {
    pub name: String,
    pub ty: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub optional: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub sensitive: bool,
}

const fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct DataflowStatistics {
    pub messages_per_second: f64,
    pub bytes_per_second: f64,
    pub batches_per_second: f64,
    pub messages_total: u64,
    pub bytes_total: u64,
    pub batches_total: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_buffer_capacity: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_buffer_len_p50: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_buffer_len_p90: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_buffer_len_p99: Option<f64>,
}

impl DataflowStatistics {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataflowBranchStatistics {
    pub branch: String,
    pub statistics: DataflowStatistics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DataflowNodeKind {
    Client,
    Ingestor,
    Processor,
    Emitter,
    Relay,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, AsRefStr)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DataflowNodeStatus {
    #[default]
    Ok,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataflowEdge {
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub kind: DataflowEdgeKind,
    /// Which side of a correlator this edge enters. Present only on correlator inputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_side: Option<DataflowInputSide>,
    /// How many declared output routes this edge stands for. Several routes to one relay are
    /// drawn as one edge whose traffic is their sum.
    #[serde(
        default = "DataflowEdge::single_route",
        skip_serializing_if = "DataflowEdge::is_single_route"
    )]
    pub routes: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<DataflowMetricRef>,
    #[serde(default, skip_serializing_if = "DataflowStatistics::is_empty")]
    pub statistics: DataflowStatistics,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branches: Vec<DataflowBranchStatistics>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DataflowInputSide {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataflowMetricRef {
    pub target_kind: String,
    pub target: String,
    pub direction: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay: Option<String>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DataflowEdgeKind {
    #[default]
    Data,
    CorrelationTimeout,
    MessageError,
    /// A materialized-state dependency: the target reads the source relay's state rather than
    /// receiving its records.
    StateLink,
}

impl DataflowEdgeKind {
    /// Whether records travel along this edge, which is what makes it carry traffic and take
    /// part in the drawn flow direction.
    pub const fn carries_records(self) -> bool {
        match self {
            Self::Data | Self::CorrelationTimeout | Self::MessageError => true,
            Self::StateLink => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DataflowGraphError {
    #[error("failed to serialize dataflow graph")]
    Serialize,
    #[error("failed to deserialize dataflow graph")]
    Deserialize,
}

impl DataflowGraph {
    pub fn new(domain: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
            statistics: DataflowStatistics::default(),
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, DataflowGraphError> {
        serde_json::to_vec(self).map_err(|_| DataflowGraphError::Serialize)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, DataflowGraphError> {
        serde_json::from_slice(bytes).map_err(|_| DataflowGraphError::Deserialize)
    }

    pub fn render_ascii(&self) -> String {
        if self.nodes.is_empty() {
            return "(empty)".to_string();
        }

        let mut ids = BTreeMap::new();
        let labels = self
            .nodes
            .iter()
            .map(DataflowNode::ascii_label)
            .collect::<Vec<_>>();
        let mut dag = Graph::new();
        for (index, (node, label)) in self.nodes.iter().zip(labels.iter()).enumerate() {
            ids.insert(node.id.as_str(), index);
            dag.add_node(index, label.as_str());
        }

        for edge in &self.edges {
            if let (Some(source), Some(target)) =
                (ids.get(edge.source.as_str()), ids.get(edge.target.as_str()))
            {
                dag.add_edge(*source, *target, None);
            }
        }

        let mut config = LayoutConfig::quality();
        config.node_spacing = 6;
        config.level_spacing = 3;
        config.render_mode = RenderMode::Vertical;

        dag.compute_layout_with_config(&config).render_scanline()
    }
}

impl DataflowNode {
    pub fn new(id: impl Into<String>, label: impl Into<String>, role: DataflowNodeRole) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            role,
            schema: None,
            schema_fields: Vec::new(),
            status: DataflowNodeStatus::Ok,
            status_detail: None,
            reconnect_wait_millis: None,
            branch: None,
            statistics: DataflowStatistics::default(),
            branches: Vec::new(),
        }
    }

    pub fn with_statistics(mut self, statistics: DataflowStatistics) -> Self {
        self.statistics = statistics;
        self
    }

    pub fn with_branch(mut self, branch: Option<DataflowBranch>) -> Self {
        self.branch = branch;
        self
    }

    pub fn with_schema(
        mut self,
        schema: impl Into<String>,
        fields: Vec<DataflowSchemaField>,
    ) -> Self {
        self.schema = Some(schema.into());
        self.schema_fields = fields;
        self
    }

    pub fn with_branches(mut self, branches: Vec<DataflowBranchStatistics>) -> Self {
        self.branches = branches;
        self
    }

    pub fn with_status(
        mut self,
        status: DataflowNodeStatus,
        detail: Option<impl Into<String>>,
    ) -> Self {
        self.status = status;
        self.status_detail = detail.map(Into::into);
        self
    }

    pub const fn kind(&self) -> DataflowNodeKind {
        self.role.kind()
    }

    fn ascii_label(&self) -> String {
        format!(
            "{}:{}:{}",
            self.role.kind().as_ref(),
            self.role.detail_label(),
            self.label
        )
    }
}

impl DataflowNodeRole {
    pub const fn kind(&self) -> DataflowNodeKind {
        match self {
            Self::Client { .. } => DataflowNodeKind::Client,
            Self::Ingestor { .. } => DataflowNodeKind::Ingestor,
            Self::Processor { .. } => DataflowNodeKind::Processor,
            Self::Emitter { .. } => DataflowNodeKind::Emitter,
            Self::Relay => DataflowNodeKind::Relay,
        }
    }

    pub fn transport(&self) -> Option<&str> {
        match self {
            Self::Client { transport } | Self::Ingestor { transport } => Some(transport.as_str()),
            Self::Emitter { transport } => Some(transport.as_str()),
            Self::Processor { .. } | Self::Relay => None,
        }
    }

    pub const fn processor(&self) -> Option<DataflowProcessorKind> {
        match self {
            Self::Processor { processor } => Some(*processor),
            _ => None,
        }
    }

    pub const fn is_relay(&self) -> bool {
        matches!(self, Self::Relay)
    }

    /// Whether this node constructs outgoing branches, and therefore bounds the upstream side of
    /// a branch group.
    pub const fn constructs_branches(&self) -> bool {
        match self {
            Self::Ingestor { .. } => true,
            Self::Processor { processor } => processor.is_reingestor(),
            _ => false,
        }
    }

    /// Whether this node collapses incoming branches, and therefore bounds the downstream side
    /// of a branch group.
    pub const fn collapses_branches(&self) -> bool {
        match self {
            Self::Emitter { .. } => true,
            Self::Processor { processor } => processor.is_reingestor(),
            _ => false,
        }
    }

    /// The short caption drawn under a node's name, and the middle segment of its ASCII label.
    pub fn detail_label(&self) -> &str {
        match self {
            Self::Client { transport } | Self::Ingestor { transport } => transport.as_str(),
            Self::Emitter { transport } => transport.as_str(),
            Self::Processor { processor } => processor.as_ref(),
            Self::Relay => "RELAY",
        }
    }
}

impl DataflowProcessorKind {
    pub const fn is_reingestor(self) -> bool {
        matches!(self, Self::Reingestor)
    }

    pub const fn is_correlator(self) -> bool {
        matches!(self, Self::Correlator)
    }

    pub const fn is_generator(self) -> bool {
        matches!(self, Self::Generator)
    }
}

impl DataflowEdge {
    pub fn data(
        source: impl Into<String>,
        target: impl Into<String>,
        kind: DataflowEdgeKind,
    ) -> Self {
        Self {
            source: source.into(),
            target: target.into(),
            kind,
            input_side: None,
            routes: 1,
            metric: None,
            statistics: DataflowStatistics::default(),
            branches: Vec::new(),
        }
    }

    pub fn with_metric(mut self, metric: DataflowMetricRef) -> Self {
        self.metric = Some(metric);
        self
    }

    pub fn with_statistics(mut self, statistics: DataflowStatistics) -> Self {
        self.statistics = statistics;
        self
    }

    pub fn with_branches(mut self, branches: Vec<DataflowBranchStatistics>) -> Self {
        self.branches = branches;
        self
    }

    pub const fn with_input_side(mut self, side: Option<DataflowInputSide>) -> Self {
        self.input_side = side;
        self
    }

    pub const fn with_routes(mut self, routes: u32) -> Self {
        self.routes = routes;
        self
    }

    const fn single_route() -> u32 {
        1
    }

    fn is_single_route(routes: &u32) -> bool {
        *routes <= 1
    }
}

impl DataflowMetricRef {
    pub fn new(
        target_kind: impl Into<String>,
        target: impl Into<String>,
        direction: impl Into<String>,
        relay: Option<impl Into<String>>,
    ) -> Self {
        Self {
            target_kind: target_kind.into(),
            target: target.into(),
            direction: direction.into(),
            relay: relay.map(Into::into),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_and_deserializes_graph() {
        let graph = sample_graph();
        let encoded = graph.serialize().expect("graph should serialize");
        let decoded = DataflowGraph::deserialize(&encoded).expect("graph should deserialize");
        assert_eq!(decoded, graph);
        assert_eq!(decoded.statistics.messages_total, 42);
        assert_eq!(decoded.nodes[0].statistics.messages_total, 3);
        assert_eq!(decoded.nodes[0].branches[0].branch, r#"{"tenant":"alpha"}"#);
    }

    #[test]
    fn round_trips_typed_roles_and_branch_identity() {
        let graph = DataflowGraph {
            domain: "prod".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                DataflowNode::new(
                    "correlator:match",
                    "match",
                    DataflowNodeRole::Processor {
                        processor: DataflowProcessorKind::Correlator,
                    },
                )
                .with_branch(Some(DataflowBranch {
                    name: "by_tenant".to_string(),
                    key_schema: "tenant_key".to_string(),
                    key_fields: vec!["tenant".to_string(), "region".to_string()],
                })),
            ],
            edges: vec![
                DataflowEdge::data("relay:left", "correlator:match", DataflowEdgeKind::Data)
                    .with_input_side(Some(DataflowInputSide::Left))
                    .with_routes(3),
            ],
        };

        let decoded =
            DataflowGraph::deserialize(&graph.serialize().expect("graph should serialize"))
                .expect("graph should deserialize");

        assert_eq!(decoded, graph);
        let node = &decoded.nodes[0];
        assert_eq!(node.kind(), DataflowNodeKind::Processor);
        assert_eq!(
            node.role.processor(),
            Some(DataflowProcessorKind::Correlator)
        );
        let branch = node.branch.as_ref().expect("branch identity must survive");
        assert_eq!(branch.name, "by_tenant");
        assert_eq!(branch.key_fields, vec!["tenant", "region"]);
        assert_eq!(decoded.edges[0].input_side, Some(DataflowInputSide::Left));
        assert_eq!(decoded.edges[0].routes, 3);
    }

    #[test]
    fn state_links_do_not_carry_records() {
        assert!(!DataflowEdgeKind::StateLink.carries_records());
        assert!(DataflowEdgeKind::Data.carries_records());
        assert!(DataflowEdgeKind::MessageError.carries_records());
        assert!(DataflowEdgeKind::CorrelationTimeout.carries_records());
    }

    #[test]
    fn branch_boundaries_follow_node_role() {
        let ingestor = DataflowNodeRole::Ingestor {
            transport: "KAFKA".to_string(),
        };
        let reingestor = DataflowNodeRole::Processor {
            processor: DataflowProcessorKind::Reingestor,
        };
        let emitter = DataflowNodeRole::Emitter {
            transport: "REDIS".to_string(),
        };
        let junction = DataflowNodeRole::Processor {
            processor: DataflowProcessorKind::Junction,
        };

        assert!(ingestor.constructs_branches() && !ingestor.collapses_branches());
        assert!(reingestor.constructs_branches() && reingestor.collapses_branches());
        assert!(emitter.collapses_branches() && !emitter.constructs_branches());
        assert!(!junction.constructs_branches() && !junction.collapses_branches());
    }

    #[test]
    fn ascii_render_uses_dataflow_edges() {
        let rendered = sample_graph().render_ascii();
        assert!(rendered.contains("KAFKA:a"), "{rendered}");
        assert!(rendered.contains("RELAY:RELAY:raw"), "{rendered}");
        assert!(rendered.contains("EMITTER:SINK:sink"), "{rendered}");
    }

    fn sample_graph() -> DataflowGraph {
        DataflowGraph {
            domain: "prod".to_string(),
            statistics: DataflowStatistics {
                messages_total: 42,
                ..DataflowStatistics::default()
            },
            nodes: vec![
                DataflowNode::new(
                    "ingestor:a",
                    "a",
                    DataflowNodeRole::Ingestor {
                        transport: "KAFKA".to_string(),
                    },
                )
                .with_statistics(DataflowStatistics {
                    messages_total: 3,
                    ..DataflowStatistics::default()
                })
                .with_branches(vec![DataflowBranchStatistics {
                    branch: r#"{"tenant":"alpha"}"#.to_string(),
                    statistics: DataflowStatistics {
                        messages_total: 2,
                        ..DataflowStatistics::default()
                    },
                }]),
                DataflowNode::new("relay:raw", "raw", DataflowNodeRole::Relay),
                DataflowNode::new(
                    "emitter:sink",
                    "sink",
                    DataflowNodeRole::Emitter {
                        transport: "SINK".to_string(),
                    },
                ),
            ],
            edges: vec![
                DataflowEdge::data("ingestor:a", "relay:raw", DataflowEdgeKind::Data),
                DataflowEdge::data("relay:raw", "emitter:sink", DataflowEdgeKind::Data),
            ],
        }
    }
}
