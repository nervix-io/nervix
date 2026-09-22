//! The graphs the console draws: how the live execution graph and a transaction's impact become
//! items, edges and branch groups, and where those are placed on the canvas.

pub mod impact;
pub mod layout;
pub mod viewport;

use nervix_dataflow_graph::{DataflowEdge, DataflowEdgeKind, DataflowNode};

use crate::graph::layout::{Layout, LayoutEdge, LayoutEdgeKind, LayoutItem};

/// Every processing node is drawn at one size, so a card's shape says nothing about its traffic
/// or its state.
pub const NODE_WIDTH: i32 = 176;
pub const NODE_HEIGHT: i32 = 64;
/// Relays are capsules sized to their name, within bounds that keep a relay column narrow.
pub const RELAY_HEIGHT: i32 = 26;
pub const RELAY_MIN_WIDTH: i32 = 72;
pub const RELAY_MAX_WIDTH: i32 = 220;

/// How the live graph names one of its edges: the items it joins and what travels along it. Two
/// edges joining the same items that carry different things are two edges, each with its own
/// route.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GraphEdgeId {
    pub source: String,
    pub target: String,
    pub kind: DataflowEdgeKind,
}

impl From<&DataflowEdge> for GraphEdgeId {
    fn from(edge: &DataflowEdge) -> Self {
        Self {
            source: edge.source.clone(),
            target: edge.target.clone(),
            kind: edge.kind,
        }
    }
}

/// The geometry of the live execution graph. Items and branch groups keep the identities the
/// graph snapshot gives them.
pub type LiveGraphLayout = Layout<String, GraphEdgeId, String>;

/// What a reader typed into a graph's search box, ready to match item names against. A search
/// needs at least two characters, so a single keystroke never lights up the whole graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GraphSearch(String);

impl GraphSearch {
    const MIN_CHARACTERS: usize = 2;

    /// The search a reader typed, or none while the input is too short to search with.
    pub fn parse(input: &str) -> Option<Self> {
        let query = input.trim().to_ascii_lowercase();
        if query.chars().count() < Self::MIN_CHARACTERS {
            return None;
        }
        Some(Self(query))
    }

    /// Whether `text` contains the search, ignoring ASCII case.
    pub fn matches(&self, text: &str) -> bool {
        text.to_ascii_lowercase().contains(&self.0)
    }
}

/// The drawn width of a relay capsule. The console has no text metrics before paint, so this
/// estimates from the label and the renderer truncates anything that overflows.
pub fn relay_width(label: &str) -> i32 {
    let character_count = match i32::try_from(label.chars().count()) {
        Ok(count) => count.min(i32::MAX / 8),
        Err(_) => i32::MAX / 8,
    };
    let estimated = character_count * 7 + 32;
    estimated.clamp(RELAY_MIN_WIDTH, RELAY_MAX_WIDTH)
}

pub fn graph_layout_item(node: &DataflowNode) -> LayoutItem<String, String> {
    let relay = node.role.is_relay();
    LayoutItem {
        id: node.id.clone(),
        width: if relay {
            relay_width(&node.label)
        } else {
            NODE_WIDTH
        },
        height: if relay { RELAY_HEIGHT } else { NODE_HEIGHT },
        relay,
        // Only items that run per branch belong to a group. The ingestors, reingestors and
        // emitters that construct or collapse a branch sit outside it, on its border.
        branch: node
            .branch
            .as_ref()
            .filter(|_| !node.role.constructs_branches() && !node.role.collapses_branches())
            .map(|branch| branch.name.clone()),
    }
}

pub fn graph_layout_edge(edge: &DataflowEdge) -> LayoutEdge<String, GraphEdgeId> {
    LayoutEdge {
        id: GraphEdgeId::from(edge),
        source: edge.source.clone(),
        target: edge.target.clone(),
        kind: if edge.kind.carries_records() {
            LayoutEdgeKind::Flow
        } else {
            LayoutEdgeKind::State
        },
        // State links carry no traffic, so they carry no rate badge either.
        badge: edge.kind.carries_records(),
    }
}

#[cfg(test)]
mod tests {
    use nervix_dataflow_graph::{
        DataflowBranch, DataflowEdgeKind, DataflowNodeRole, DataflowProcessorKind,
    };

    use super::*;

    fn branched(role: DataflowNodeRole) -> DataflowNode {
        DataflowNode::new("id", "label", role).with_branch(Some(DataflowBranch {
            name: "by_tenant".to_string(),
            key_schema: "tenant_key".to_string(),
            key_fields: vec!["tenant".to_string()],
        }))
    }

    #[test]
    fn branch_members_exclude_the_nodes_that_bound_the_branch() {
        let junction = branched(DataflowNodeRole::Processor {
            processor: DataflowProcessorKind::Junction,
        });
        assert_eq!(
            graph_layout_item(&junction).branch.as_deref(),
            Some("by_tenant")
        );

        let reingestor = branched(DataflowNodeRole::Processor {
            processor: DataflowProcessorKind::Reingestor,
        });
        assert_eq!(graph_layout_item(&reingestor).branch, None);

        let ingestor = branched(DataflowNodeRole::Ingestor {
            transport: "KAFKA".to_string(),
        });
        assert_eq!(graph_layout_item(&ingestor).branch, None);
    }

    #[test]
    fn relays_are_capsules_and_processors_are_cards() {
        let relay = DataflowNode::new("relay:orders", "orders", DataflowNodeRole::Relay);
        let item = graph_layout_item(&relay);
        assert!(item.relay);
        assert_eq!(item.height, RELAY_HEIGHT);
        assert!(item.width >= RELAY_MIN_WIDTH && item.width <= RELAY_MAX_WIDTH);

        let emitter = DataflowNode::new(
            "emitter:sink",
            "sink",
            DataflowNodeRole::Emitter {
                transport: "REDIS".to_string(),
            },
        );
        let item = graph_layout_item(&emitter);
        assert!(!item.relay);
        assert_eq!((item.width, item.height), (NODE_WIDTH, NODE_HEIGHT));
    }

    #[test]
    fn a_very_long_relay_name_stays_within_the_column_bound() {
        let relay = DataflowNode::new("relay:x", "a".repeat(200), DataflowNodeRole::Relay);
        assert_eq!(graph_layout_item(&relay).width, RELAY_MAX_WIDTH);
    }

    #[test]
    fn state_links_carry_no_badge_and_do_not_flow() {
        let link = DataflowEdge::data("relay:state", "generator:g", DataflowEdgeKind::StateLink);
        let converted = graph_layout_edge(&link);
        assert!(!converted.badge);
        assert_eq!(converted.kind, LayoutEdgeKind::State);

        let data = DataflowEdge::data("relay:a", "junction:b", DataflowEdgeKind::Data);
        let converted = graph_layout_edge(&data);
        assert!(converted.badge);
        assert_eq!(converted.kind, LayoutEdgeKind::Flow);
    }

    #[test]
    fn a_search_needs_two_characters_and_ignores_case() {
        assert_eq!(GraphSearch::parse(" t "), None, "one letter is too broad");
        let search = GraphSearch::parse(" TeLe ").expect("two or more characters search");
        assert!(search.matches("mqtt_telemetry"));
        assert!(search.matches("TELEMETRY"));
        assert!(!search.matches("orders"));
    }

    #[test]
    fn a_relay_read_both_as_input_and_as_state_keeps_two_routes() {
        let nodes = [
            DataflowNode::new("relay:events", "events", DataflowNodeRole::Relay),
            DataflowNode::new(
                "junction:enrich",
                "enrich",
                DataflowNodeRole::Processor {
                    processor: DataflowProcessorKind::Junction,
                },
            ),
        ];
        let edges = [
            DataflowEdge::data("relay:events", "junction:enrich", DataflowEdgeKind::Data),
            DataflowEdge::data(
                "relay:events",
                "junction:enrich",
                DataflowEdgeKind::StateLink,
            ),
        ];
        let layout = LiveGraphLayout::build(
            &nodes.iter().map(graph_layout_item).collect::<Vec<_>>(),
            &edges.iter().map(graph_layout_edge).collect::<Vec<_>>(),
        );

        let records = &layout.edges[&GraphEdgeId::from(&edges[0])];
        let state = &layout.edges[&GraphEdgeId::from(&edges[1])];
        assert_eq!(records.kind, LayoutEdgeKind::Flow);
        assert_eq!(state.kind, LayoutEdgeKind::State);
        assert_ne!(
            records.points, state.points,
            "the input and the state read are drawn apart"
        );
    }
}
