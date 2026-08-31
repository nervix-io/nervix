//! The execution graph the console draws: how a snapshot becomes items, edges and branch
//! groups, and where those are placed on the canvas.

pub mod layout;

use nervix_dataflow_graph::{DataflowEdge, DataflowNode};

use crate::graph::layout::{LayoutEdge, LayoutEdgeKind, LayoutItem};

/// Every processing node is drawn at one size, so a card's shape says nothing about its traffic
/// or its state.
pub const NODE_WIDTH: i32 = 176;
pub const NODE_HEIGHT: i32 = 64;
/// Relays are capsules sized to their name, within bounds that keep a relay column narrow.
pub const RELAY_HEIGHT: i32 = 26;
pub const RELAY_MIN_WIDTH: i32 = 72;
pub const RELAY_MAX_WIDTH: i32 = 220;

/// The drawn width of a relay capsule. The console has no text metrics before paint, so this
/// estimates from the label and the renderer truncates anything that overflows.
pub fn relay_width(label: &str) -> i32 {
    let estimated = i32::try_from(label.chars().count()).unwrap_or(i32::MAX / 8) * 7 + 32;
    estimated.clamp(RELAY_MIN_WIDTH, RELAY_MAX_WIDTH)
}

pub fn graph_layout_item(node: &DataflowNode) -> LayoutItem {
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

pub fn graph_layout_edge(edge: &DataflowEdge) -> LayoutEdge {
    LayoutEdge {
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
}
