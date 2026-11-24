//! Loops in a domain's graph: the search that finds one and the path a refusal names.
//!
//! Layer: decisions.
//!
//! - **Owns.** The hops data takes between the nodes of a domain's graph, the search for a loop
//!   through them, and the loop's path as a refusal names it: its nodes in the order data would
//!   travel them, starting at the loop's first relay, with how data moves from each to the next.
//! - **Depends on.** The registry graph's nodes and edges, which say which hop each edge is, and
//!   the vocabulary's node references.
//! - **Must not know.** How the graph is built, placed or executed, or who reports the refusal.
//!
//! A domain's graph has no loops: data never comes back to a relay it already passed, whether it
//! travels as rows along a route, an error route or a correlation timeout, or as the state a relay
//! materializes for the node that reads it. The registry refuses a graph with a loop before any of
//! it applies, so every order that follows the data, such as the order in which a change stops the
//! parts it replaces, can rely on one existing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::fmt;

use meticulous::OptionExt as _;
use nervix_models::{ModelKind, NodeRef};
use petgraph::{graph::DiGraph, prelude::NodeIndex, visit::EdgeRef as _};

use crate::registry::graph::{ActiveNode, EdgeKind};

/// How data moves along one edge of a domain's graph, from a node to the next.
///
/// The variants are declared in the order a loop's path prefers them when two nodes are joined by
/// more than one kind of edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum DataHop {
    /// Rows along a route: from a relay to the node that reads it, or from a node to the relay it
    /// writes.
    Route,
    /// Failed rows along an `ON MESSAGE ERROR SEND TO` route.
    ErrorRoute,
    /// Unmatched rows along an `ON CORRELATION TIMEOUT SEND TO` action.
    CorrelationTimeout,
    /// A relay's materialized state, which the next node reads.
    StateRead,
}

impl DataHop {
    /// The arrow a loop's path draws for this hop. A route is the plain arrow; every other hop
    /// names itself, because nothing else in the path shows that data takes it.
    fn arrow(self) -> &'static str {
        match self {
            Self::Route => "→",
            Self::ErrorRoute => "→[error route]",
            Self::CorrelationTimeout => "→[correlation timeout]",
            Self::StateRead => "→[state read]",
        }
    }
}

/// One node of a loop and how data leaves it for the next node of the loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoopStep {
    pub(crate) node: NodeRef,
    pub(crate) hop: DataHop,
}

/// A circle data would travel in a domain's graph. It starts at the loop's first relay in node
/// order, and the last step's hop leads back to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GraphLoop {
    steps: Vec<LoopStep>,
}

impl GraphLoop {
    /// The nodes of the loop in the order data would travel them, each with the hop it leaves by.
    /// The refusal reports the loop through its rendering; the tests read the steps themselves.
    #[cfg(test)]
    pub(crate) fn steps(&self) -> &[LoopStep] {
        &self.steps
    }

    /// The loop the search closed when the node at the end of `path` led back to `target`, a node
    /// earlier on the same path.
    fn closing(
        graph: &DiGraph<ActiveNode, EdgeKind>,
        successors: &DataSuccessors,
        path: &[PathEntry],
        target: NodeIndex,
    ) -> Self {
        let first = path
            .iter()
            .position(|entry| entry.node == target)
            .verified("the search closes a loop only on a node of its current path");
        let mut steps = Vec::new();
        for entry in path.iter().skip(first) {
            let taken = entry
                .followed
                .checked_sub(1)
                .verified("every node on the search's path has followed the hop to the next one");
            let hop = successors
                .of(entry.node)
                .get(taken)
                .verified("a node follows only the successors it has")
                .hop;
            steps.push(LoopStep {
                node: graph[entry.node].node_ref(),
                hop,
            });
        }
        let start = Self::first_relay(&steps);
        steps.rotate_left(start);
        Self { steps }
    }

    /// Where the loop's first relay in node order stands among `steps`, so the same loop always
    /// reads from the same place, whichever node the search reached it from.
    fn first_relay(steps: &[LoopStep]) -> usize {
        let mut first: Option<usize> = None;
        for (position, step) in steps.iter().enumerate() {
            if step.node.kind != ModelKind::Relay {
                continue;
            }
            if let Some(current) = first
                && let Some(current) = steps.get(current)
                && current.node <= step.node
            {
                continue;
            }
            first = Some(position);
        }
        first.assured(
            "every edge data travels joins a relay and a node that reads or writes it, so every \
             loop passes a relay",
        )
    }
}

impl fmt::Display for GraphLoop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for step in &self.steps {
            write!(
                formatter,
                "{} '{}' {} ",
                step.node.kind.keyword_phrase(),
                step.node.identifier,
                step.hop.arrow()
            )?;
        }
        let first = self
            .steps
            .first()
            .assured("a loop is only built by closing one, which passes at least one node");
        write!(
            formatter,
            "{} '{}'",
            first.node.kind.keyword_phrase(),
            first.node.identifier
        )
    }
}

/// Where data goes next from a node: another node, and the hop that takes it there.
#[derive(Debug, Clone, Copy)]
struct Successor {
    node: NodeIndex,
    hop: DataHop,
}

/// For each node of a graph, the nodes data reaches from it in one hop, each once and in node
/// order, so the search visits a graph the same way on every cluster node.
struct DataSuccessors {
    lists: Vec<Vec<Successor>>,
}

impl DataSuccessors {
    fn new(graph: &DiGraph<ActiveNode, EdgeKind>) -> Self {
        let mut lists = vec![Vec::new(); graph.node_count()];
        for edge in graph.edge_references() {
            let source = &graph[edge.source()];
            let target = &graph[edge.target()];
            let Some(hop) = source.data_hop_to(target, *edge.weight()) else {
                continue;
            };
            let list = lists
                .get_mut(edge.source().index())
                .verified("the lists were sized by this graph's node count");
            list.push(Successor {
                node: edge.target(),
                hop,
            });
        }
        for list in &mut lists {
            list.sort_by(|left, right| {
                graph[left.node]
                    .node_ref()
                    .cmp(&graph[right.node].node_ref())
                    .then(left.hop.cmp(&right.hop))
            });
            list.dedup_by_key(|successor| successor.node);
        }
        Self { lists }
    }

    fn of(&self, node: NodeIndex) -> &[Successor] {
        self.lists
            .get(node.index())
            .verified("the lists were sized by the node count of the graph the node belongs to")
    }
}

/// Where the search stands with one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Visit {
    /// The search has not reached the node.
    Unvisited,
    /// The node is on the search's current path, so a hop back to it closes a loop.
    OnPath,
    /// The search has followed every hop out of the node and found no loop through it.
    Finished,
}

/// A node on the search's current path, and how many of its successors the search has followed.
struct PathEntry {
    node: NodeIndex,
    followed: usize,
}

/// A loop data could travel in `graph`, or `None` when data never comes back to a node it passed.
///
/// Only the edges data travels count: routes, error routes, correlation timeouts and reads of a
/// relay's materialized state. A configuration dependency carries nothing, and configuration
/// dependencies are checked for cycles on their own. The search starts from every node in node
/// order and follows successors in node order, so a graph with several loops reports the same one
/// on every cluster node.
pub(in crate::registry) fn find_graph_loop(
    graph: &DiGraph<ActiveNode, EdgeKind>,
) -> Option<GraphLoop> {
    let successors = DataSuccessors::new(graph);
    let mut starts = graph.node_indices().collect::<Vec<_>>();
    starts.sort_by_key(|index| graph[*index].node_ref());
    let mut visits = vec![Visit::Unvisited; graph.node_count()];
    for start in starts {
        let visit = *visits
            .get(start.index())
            .verified("the visits were sized by this graph's node count");
        if visit != Visit::Unvisited {
            continue;
        }
        if let Some(found) = search_from(graph, &successors, &mut visits, start) {
            return Some(found);
        }
    }
    None
}

/// Walks depth first from `start` through the nodes no earlier walk finished, and returns the
/// first loop it closes.
fn search_from(
    graph: &DiGraph<ActiveNode, EdgeKind>,
    successors: &DataSuccessors,
    visits: &mut [Visit],
    start: NodeIndex,
) -> Option<GraphLoop> {
    set_visit(visits, start, Visit::OnPath);
    let mut path = vec![PathEntry {
        node: start,
        followed: 0,
    }];
    while let Some(entry) = path.last_mut() {
        let node = entry.node;
        let Some(next) = successors.of(node).get(entry.followed).copied() else {
            set_visit(visits, node, Visit::Finished);
            path.pop();
            continue;
        };
        entry.followed = entry
            .followed
            .checked_add(1)
            .assured("a node has fewer successors than a usize counts");
        let reached = *visits
            .get(next.node.index())
            .verified("the visits were sized by this graph's node count");
        match reached {
            Visit::OnPath => {
                return Some(GraphLoop::closing(graph, successors, &path, next.node));
            }
            Visit::Unvisited => {
                set_visit(visits, next.node, Visit::OnPath);
                path.push(PathEntry {
                    node: next.node,
                    followed: 0,
                });
            }
            Visit::Finished => {}
        }
    }
    None
}

fn set_visit(visits: &mut [Visit], node: NodeIndex, visit: Visit) {
    let slot = visits
        .get_mut(node.index())
        .verified("the visits were sized by this graph's node count");
    *slot = visit;
}

#[cfg(test)]
#[path = "graph_loop_tests.rs"]
mod tests;
