use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
};

use ascii_dag::{Graph, LayoutConfig, RenderMode};
use petgraph::{Direction, graph::DiGraph, prelude::NodeIndex, visit::EdgeRef};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use thiserror::Error;

const LAYOUT_RANK_SPACING: i32 = 320;
const LAYOUT_ROW_SPACING: i32 = 118;
const LAYOUT_VERTICAL_CENTER: i32 = 210;

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
    pub kind: DataflowNodeKind,
    pub subtype: String,
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
    pub parameterization_schema: Option<String>,
    #[serde(default, skip_serializing_if = "DataflowStatistics::is_empty")]
    pub statistics: DataflowStatistics,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branches: Vec<DataflowBranchStatistics>,
    pub x: i32,
    pub y: i32,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<DataflowMetricRef>,
    #[serde(default, skip_serializing_if = "DataflowStatistics::is_empty")]
    pub statistics: DataflowStatistics,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branches: Vec<DataflowBranchStatistics>,
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

    pub fn laid_out(mut self) -> Self {
        let ranks = self.ranks();
        let rank_order = self.crossing_reduced_rank_order(&ranks);
        let mut rank_counts = BTreeMap::<usize, usize>::new();
        for rank in ranks.values() {
            *rank_counts.entry(*rank).or_insert(0) += 1;
        }

        let mut rank_offsets = BTreeMap::<usize, usize>::new();
        self.nodes.sort_by(|left, right| {
            let left_rank = *ranks.get(&left.id).unwrap_or(&0);
            let right_rank = *ranks.get(&right.id).unwrap_or(&0);
            left_rank
                .cmp(&right_rank)
                .then_with(|| {
                    rank_order
                        .get(&left.id)
                        .unwrap_or(&usize::MAX)
                        .cmp(rank_order.get(&right.id).unwrap_or(&usize::MAX))
                })
                .then_with(|| left.layout_order_cmp(right))
        });

        for node in &mut self.nodes {
            let rank = *ranks.get(&node.id).unwrap_or(&0);
            let offset = rank_offsets.entry(rank).or_insert(0);
            let count = *rank_counts.get(&rank).unwrap_or(&1);
            node.x = 24 + (rank as i32 * LAYOUT_RANK_SPACING);
            node.y = centered_row_y(*offset, count);
            *offset += 1;
        }

        self
    }

    fn crossing_reduced_rank_order(
        &self,
        ranks: &BTreeMap<String, usize>,
    ) -> BTreeMap<String, usize> {
        let node_by_id = self
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), node))
            .collect::<BTreeMap<_, _>>();
        let mut rank_nodes = BTreeMap::<usize, Vec<String>>::new();
        for node in &self.nodes {
            rank_nodes
                .entry(*ranks.get(&node.id).unwrap_or(&0))
                .or_default()
                .push(node.id.clone());
        }
        for ids in rank_nodes.values_mut() {
            ids.sort_by(|left, right| {
                node_by_id
                    .get(left.as_str())
                    .expect("layout node id must be indexed")
                    .layout_order_cmp(
                        node_by_id
                            .get(right.as_str())
                            .expect("layout node id must be indexed"),
                    )
            });
        }

        let max_rank = rank_nodes.keys().next_back().copied().unwrap_or(0);
        let mut order = Self::rank_order_map(&rank_nodes);
        for rank in (0..=max_rank).rev() {
            self.sort_rank_by_neighbor_order(
                rank,
                &node_by_id,
                ranks,
                &mut rank_nodes,
                &mut order,
                Direction::Outgoing,
            );
        }
        for rank in 0..=max_rank {
            self.sort_rank_by_neighbor_order(
                rank,
                &node_by_id,
                ranks,
                &mut rank_nodes,
                &mut order,
                Direction::Incoming,
            );
        }
        order
    }

    fn sort_rank_by_neighbor_order(
        &self,
        rank: usize,
        node_by_id: &BTreeMap<&str, &DataflowNode>,
        ranks: &BTreeMap<String, usize>,
        rank_nodes: &mut BTreeMap<usize, Vec<String>>,
        order: &mut BTreeMap<String, usize>,
        direction: Direction,
    ) {
        let Some(ids) = rank_nodes.get_mut(&rank) else {
            return;
        };
        let previous_order = ids
            .iter()
            .enumerate()
            .map(|(index, id)| (id.clone(), index))
            .collect::<BTreeMap<_, _>>();
        ids.sort_by(|left, right| {
            let left_score = self.neighbor_order_score(left, ranks, order, direction);
            let right_score = self.neighbor_order_score(right, ranks, order, direction);
            LayoutNeighborScore::compare_options(left_score, right_score)
                .then_with(|| {
                    previous_order
                        .get(left)
                        .unwrap_or(&usize::MAX)
                        .cmp(previous_order.get(right).unwrap_or(&usize::MAX))
                })
                .then_with(|| {
                    node_by_id
                        .get(left.as_str())
                        .expect("layout node id must be indexed")
                        .layout_order_cmp(
                            node_by_id
                                .get(right.as_str())
                                .expect("layout node id must be indexed"),
                        )
                })
        });
        for (index, id) in ids.iter().enumerate() {
            order.insert(id.clone(), index);
        }
    }

    fn neighbor_order_score(
        &self,
        id: &str,
        ranks: &BTreeMap<String, usize>,
        order: &BTreeMap<String, usize>,
        direction: Direction,
    ) -> Option<LayoutNeighborScore> {
        let rank = *ranks.get(id)?;
        let mut score = LayoutNeighborScore::default();
        for edge in &self.edges {
            let neighbor = match direction {
                Direction::Outgoing if edge.source == id => edge.target.as_str(),
                Direction::Incoming if edge.target == id => edge.source.as_str(),
                _ => continue,
            };
            let Some(neighbor_rank) = ranks.get(neighbor).copied() else {
                continue;
            };
            match direction {
                Direction::Outgoing if neighbor_rank <= rank => continue,
                Direction::Incoming if neighbor_rank >= rank => continue,
                _ => {}
            }
            let Some(position) = order.get(neighbor) else {
                continue;
            };
            score.add(*position);
        }
        score.has_neighbors().then_some(score)
    }

    fn rank_order_map(rank_nodes: &BTreeMap<usize, Vec<String>>) -> BTreeMap<String, usize> {
        rank_nodes
            .values()
            .flat_map(|ids| {
                ids.iter()
                    .enumerate()
                    .map(|(index, id)| (id.clone(), index))
            })
            .collect()
    }

    pub fn to_petgraph(&self) -> DiGraph<DataflowNode, ()> {
        let mut graph = DiGraph::<DataflowNode, ()>::new();
        let mut indices = BTreeMap::new();
        for node in &self.nodes {
            indices.insert(node.id.clone(), graph.add_node(node.clone()));
        }
        for edge in &self.edges {
            if let (Some(source), Some(target)) =
                (indices.get(&edge.source), indices.get(&edge.target))
            {
                graph.add_edge(*source, *target, ());
            }
        }
        graph
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

    fn ranks(&self) -> BTreeMap<String, usize> {
        let graph = self.to_petgraph();
        let mut ids = BTreeMap::new();
        for index in graph.node_indices() {
            ids.insert(graph[index].id.clone(), index);
        }

        let mut indegree = BTreeMap::<NodeIndex, usize>::new();
        let mut ranks = BTreeMap::<NodeIndex, usize>::new();
        let mut queue = VecDeque::new();
        for index in graph.node_indices() {
            let incoming = graph.edges_directed(index, Direction::Incoming).count();
            indegree.insert(index, incoming);
            ranks.insert(index, 0);
            if incoming == 0 {
                queue.push_back(index);
            }
        }

        while let Some(source) = queue.pop_front() {
            let source_rank = *ranks.get(&source).unwrap_or(&0);
            for edge in graph.edges_directed(source, Direction::Outgoing) {
                let target = edge.target();
                let target_rank = ranks.entry(target).or_insert(0);
                *target_rank = (*target_rank).max(source_rank + 1);
                let incoming = indegree
                    .get_mut(&target)
                    .expect("target indegree must be initialized");
                *incoming = incoming.saturating_sub(1);
                if *incoming == 0 {
                    queue.push_back(target);
                }
            }
        }

        ids.into_iter()
            .map(|(id, index)| (id, *ranks.get(&index).unwrap_or(&0)))
            .collect()
    }
}

impl DataflowNode {
    pub fn new(
        id: impl Into<String>,
        label: impl Into<String>,
        kind: DataflowNodeKind,
        subtype: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            kind,
            subtype: subtype.into(),
            schema: None,
            schema_fields: Vec::new(),
            status: DataflowNodeStatus::Ok,
            status_detail: None,
            reconnect_wait_millis: None,
            parameterization_schema: None,
            statistics: DataflowStatistics::default(),
            branches: Vec::new(),
            x: 0,
            y: 0,
        }
    }

    pub fn with_statistics(mut self, statistics: DataflowStatistics) -> Self {
        self.statistics = statistics;
        self
    }

    pub fn with_parameterization_schema(mut self, schema: impl Into<String>) -> Self {
        self.parameterization_schema = Some(schema.into());
        self
    }

    pub fn with_optional_parameterization_schema(
        mut self,
        schema: Option<impl Into<String>>,
    ) -> Self {
        self.parameterization_schema = schema.map(Into::into);
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

    fn ascii_label(&self) -> String {
        format!("{}:{}:{}", self.kind.as_ref(), self.subtype, self.label)
    }

    fn layout_order_cmp(&self, other: &Self) -> Ordering {
        self.kind
            .cmp(&other.kind)
            .then_with(|| self.label.cmp(&other.label))
            .then_with(|| self.id.cmp(&other.id))
    }
}

#[derive(Clone, Copy, Default)]
struct LayoutNeighborScore {
    sum: u128,
    count: u128,
}

impl LayoutNeighborScore {
    fn add(&mut self, position: usize) {
        self.sum += position as u128;
        self.count += 1;
    }

    const fn has_neighbors(self) -> bool {
        self.count > 0
    }

    fn compare_options(left: Option<Self>, right: Option<Self>) -> Ordering {
        match (left, right) {
            (Some(left), Some(right)) => left.compare(right),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }

    fn compare(self, other: Self) -> Ordering {
        (self.sum * other.count).cmp(&(other.sum * self.count))
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

fn centered_row_y(offset: usize, count: usize) -> i32 {
    let total = count.saturating_sub(1) as i32 * LAYOUT_ROW_SPACING;
    let top = (LAYOUT_VERTICAL_CENTER - total / 2).max(24);
    top + offset as i32 * LAYOUT_ROW_SPACING
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
    fn layout_places_downstream_nodes_to_the_right() {
        let graph = sample_graph().laid_out();
        let source = graph
            .nodes
            .iter()
            .find(|node| node.id == "ingestor:a")
            .unwrap();
        let relay = graph
            .nodes
            .iter()
            .find(|node| node.id == "relay:raw")
            .unwrap();
        let sink = graph
            .nodes
            .iter()
            .find(|node| node.id == "emitter:sink")
            .unwrap();

        assert!(source.x < relay.x);
        assert!(relay.x < sink.x);
    }

    #[test]
    fn layout_leaves_route_corridors_between_wide_console_nodes() {
        let graph = sample_graph().laid_out();
        let source = graph
            .nodes
            .iter()
            .find(|node| node.id == "ingestor:a")
            .unwrap();
        let relay = graph
            .nodes
            .iter()
            .find(|node| node.id == "relay:raw")
            .unwrap();

        assert!(
            relay.x - source.x >= 300,
            "adjacent ranks need room for wide console node cards and routed edges"
        );
    }

    #[test]
    fn layout_orders_source_clients_by_their_target_order() {
        let graph = DataflowGraph {
            domain: "datalake_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                DataflowNode::new(
                    "client_source:kafka_auth",
                    "kafka_auth",
                    DataflowNodeKind::Client,
                    "KAFKA",
                ),
                DataflowNode::new(
                    "client_source:mqtt_devices",
                    "mqtt_devices",
                    DataflowNodeKind::Client,
                    "MQTT",
                ),
                DataflowNode::new(
                    "client_source:nats_edge",
                    "nats_edge",
                    DataflowNodeKind::Client,
                    "NATS",
                ),
                DataflowNode::new(
                    "ingestor:auth_server_activity",
                    "auth_server_activity",
                    DataflowNodeKind::Ingestor,
                    "KAFKA",
                ),
                DataflowNode::new(
                    "ingestor:edge_server_activity",
                    "edge_server_activity",
                    DataflowNodeKind::Ingestor,
                    "NATS",
                ),
                DataflowNode::new(
                    "ingestor:iot_device_activity",
                    "iot_device_activity",
                    DataflowNodeKind::Ingestor,
                    "MQTT",
                ),
            ],
            edges: vec![
                DataflowEdge::data(
                    "client_source:kafka_auth",
                    "ingestor:auth_server_activity",
                    DataflowEdgeKind::Data,
                ),
                DataflowEdge::data(
                    "client_source:mqtt_devices",
                    "ingestor:iot_device_activity",
                    DataflowEdgeKind::Data,
                ),
                DataflowEdge::data(
                    "client_source:nats_edge",
                    "ingestor:edge_server_activity",
                    DataflowEdgeKind::Data,
                ),
            ],
        }
        .laid_out();
        let y_by_id = graph
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), node.y))
            .collect::<BTreeMap<_, _>>();

        assert!(y_by_id["client_source:kafka_auth"] < y_by_id["client_source:nats_edge"]);
        assert!(y_by_id["client_source:nats_edge"] < y_by_id["client_source:mqtt_devices"]);
        assert!(
            y_by_id["ingestor:auth_server_activity"] < y_by_id["ingestor:edge_server_activity"]
        );
        assert!(y_by_id["ingestor:edge_server_activity"] < y_by_id["ingestor:iot_device_activity"]);
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
                DataflowNode::new("ingestor:a", "a", DataflowNodeKind::Ingestor, "KAFKA")
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
                DataflowNode::new("relay:raw", "raw", DataflowNodeKind::Relay, "RELAY"),
                DataflowNode::new("emitter:sink", "sink", DataflowNodeKind::Emitter, "SINK"),
            ],
            edges: vec![
                DataflowEdge {
                    source: "ingestor:a".to_string(),
                    target: "relay:raw".to_string(),
                    kind: DataflowEdgeKind::Data,
                    metric: None,
                    statistics: DataflowStatistics::default(),
                    branches: Vec::new(),
                },
                DataflowEdge {
                    source: "relay:raw".to_string(),
                    target: "emitter:sink".to_string(),
                    kind: DataflowEdgeKind::Data,
                    metric: None,
                    statistics: DataflowStatistics::default(),
                    branches: Vec::new(),
                },
            ],
        }
    }
}
