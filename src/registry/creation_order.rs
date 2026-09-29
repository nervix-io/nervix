//! The order a domain's Models can be created in, one statement at a time.
//!
//! Layer: decisions.
//!
//! - **Owns.** Ordering a validated domain's Models so that each follows every Model its
//!   configuration names, with a deterministic order among Models that do not depend on each
//!   other.
//! - **Depends on.** The active graph the registry builds from a domain's schedule, and the
//!   vocabulary.
//! - **Must not know.** How the order is rendered, stored or applied.
//!
//! The order follows the graph's configuration dependencies, which every reference a Model
//! declares contributes: a relay's schema and branch, a processor's inputs, outputs, error routes
//! and materialized state, a codec's schemas, a placement's members, a UDF's callers. The graph
//! refuses a cycle among them, so the order always exists. A hash map named inside an expression is
//! the one reference without an edge, so every kind that can hold an expression is ranked after the
//! hash maps, and the rank breaks every remaining tie before the name does.

use std::collections::BTreeSet;

use ahash::{HashMap, HashMapExt as _};
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_models::{DomainSchedule, ModelKind, ModelName, NodeRef};
use petgraph::{Direction, graph::NodeIndex, visit::EdgeRef as _};

use crate::registry::{error::RegistryError, graph::ActiveGraph};

/// Where a kind of Model sits among Models that do not depend on each other: the vocabulary a
/// flow is built from first, then the relays, then everything that reads or writes them.
const fn creation_rank(kind: ModelKind) -> u8 {
    match kind {
        ModelKind::Schema => 0,
        ModelKind::WireJsonSchema => 1,
        ModelKind::WireCborSchema => 2,
        ModelKind::WireAvroSchema => 3,
        ModelKind::Codec => 4,
        ModelKind::SignalingProtocol => 5,
        ModelKind::Client => 6,
        ModelKind::Vhost => 7,
        ModelKind::Endpoint => 8,
        ModelKind::Branch => 9,
        ModelKind::Udf => 10,
        ModelKind::Lookup => 11,
        ModelKind::Relay => 12,
        ModelKind::Ingestor => 13,
        ModelKind::Reingestor => 14,
        ModelKind::Generator => 15,
        ModelKind::Junction => 16,
        ModelKind::Deduplicator => 17,
        ModelKind::Correlator => 18,
        ModelKind::Reorderer => 19,
        ModelKind::WindowProcessor => 20,
        ModelKind::Inferencer => 21,
        ModelKind::WasmProcessor => 22,
        ModelKind::Emitter => 23,
        ModelKind::Placement => 24,
    }
}

/// A Model ready to be created, ordered by rank and then by name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ReadyModel {
    rank: u8,
    name: ModelName,
    index: NodeIndex,
}

impl ActiveGraph {
    /// Every Model of the graph, each after every Model its configuration depends on. Absent when
    /// the configuration dependencies form a cycle, which leaves no Model of it creatable first.
    pub(crate) fn creation_order(&self) -> Option<Vec<NodeRef>> {
        let mut waiting_on = HashMap::with_capacity(self.graph.node_count());
        for index in self.graph.node_indices() {
            let mut dependencies = 0_usize;
            for edge in self.graph.edges_directed(index, Direction::Incoming) {
                if edge.weight().is_configuration_dependency() {
                    dependencies = dependencies
                        .checked_add(1)
                        .assured("a node has fewer edges than the address space can count");
                }
            }
            waiting_on.insert(index, dependencies);
        }

        let mut ready = BTreeSet::new();
        for (index, dependencies) in &waiting_on {
            if *dependencies == 0 {
                ready.insert(self.ready_model(*index));
            }
        }

        let mut order = Vec::with_capacity(self.graph.node_count());
        while let Some(next) = ready.pop_first() {
            order.push(self.node_ref_at(next.index));
            for edge in self.graph.edges_directed(next.index, Direction::Outgoing) {
                if !edge.weight().is_configuration_dependency() {
                    continue;
                }
                let dependent = edge.target();
                let remaining = waiting_on
                    .get_mut(&dependent)
                    .assured("every node of the graph was counted above");
                *remaining = remaining
                    .checked_sub(1)
                    .assured("a dependency is released once for each edge counted into it");
                if *remaining == 0 {
                    ready.insert(self.ready_model(dependent));
                }
            }
        }
        if order.len() != self.graph.node_count() {
            return None;
        }
        Some(order)
    }

    fn ready_model(&self, index: NodeIndex) -> ReadyModel {
        let node = self
            .graph
            .node_weight(index)
            .assured("the index was taken from this graph");
        ReadyModel {
            rank: creation_rank(node.kind),
            name: node.identifier.clone(),
            index,
        }
    }

    fn node_ref_at(&self, index: NodeIndex) -> NodeRef {
        self.graph
            .node_weight(index)
            .assured("the index was taken from this graph")
            .node_ref()
    }
}

/// The order `schedule`'s Models can be created in, one statement at a time.
pub(crate) fn creation_order(
    schedule: &DomainSchedule,
) -> Result<Vec<NodeRef>, Report<RegistryError>> {
    let graph = ActiveGraph::from_scheduled_models(schedule)?;
    match graph.creation_order() {
        Some(order) => Ok(order),
        None => Err(Report::new(RegistryError::ConfigurationCycle {
            domain: schedule.domain.as_str().to_string(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use meticulous::ResultExt as _;
    use nervix_models::{DomainName, ScheduledNode, SchemaFingerprint};

    use super::*;
    use crate::registry::test_fixtures::{example_graph_models, full_graph_batch};

    fn schedule_of(domain: &DomainName, models: Vec<nervix_models::Model>) -> DomainSchedule {
        DomainSchedule::new(
            domain.clone(),
            models
                .into_iter()
                .map(|model| ScheduledNode::new(model, SchemaFingerprint::from_digest([0; 32]))),
            Vec::new(),
        )
    }

    /// Asserts that every configuration dependency of `graph` precedes the Model that depends
    /// on it in `order`.
    fn assert_dependencies_first(graph: &ActiveGraph, order: &[NodeRef]) {
        let positions = order
            .iter()
            .enumerate()
            .map(|(position, node)| (node.clone(), position))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(positions.len(), graph.graph.node_count());
        for edge in graph.graph.edge_references() {
            if !edge.weight().is_configuration_dependency() {
                continue;
            }
            let dependency = graph.node_ref_at(edge.source());
            let dependent = graph.node_ref_at(edge.target());
            assert!(
                positions[&dependency] < positions[&dependent],
                "{dependency:?} must be created before {dependent:?}"
            );
        }
    }

    #[test]
    fn every_model_follows_what_its_configuration_names() {
        let domain = DomainName::parse("default").assured("the test domain is valid");
        let schedule = schedule_of(&domain, full_graph_batch());
        let graph = ActiveGraph::from_scheduled_models(&schedule).assured("the batch validates");
        let order = graph
            .creation_order()
            .assured("a validated graph has no dependency cycle");
        assert_dependencies_first(&graph, &order);
    }

    #[test]
    fn the_order_of_a_domain_does_not_depend_on_the_order_its_models_arrive_in() {
        let domain = DomainName::parse("default").assured("the test domain is valid");
        let mut models = full_graph_batch();
        let forward =
            creation_order(&schedule_of(&domain, models.clone())).assured("the batch validates");
        models.reverse();
        let reversed = creation_order(&schedule_of(&domain, models)).assured("the batch validates");
        assert_eq!(forward, reversed);
    }

    #[test]
    fn runnable_examples_order_their_models_by_dependency_then_rank() {
        for (name, source) in [
            ("iot", include_str!("../../examples/iot/iot.nspl")),
            (
                "nats_factory_windows",
                include_str!("../../examples/nats-factory-windows/nats_factory_windows.nspl"),
            ),
            (
                "datalake",
                include_str!("../../examples/datalake/datalake.nspl"),
            ),
        ] {
            let (domain, models) = example_graph_models(name, source);
            let schedule = schedule_of(&domain, models);
            let graph = ActiveGraph::from_scheduled_models(&schedule)
                .unwrap_or_else(|error| panic!("{name} validates: {error:?}"));
            let order = graph
                .creation_order()
                .assured("a validated graph has no dependency cycle");
            assert_dependencies_first(&graph, &order);
            let first = order.first().assured("every example declares a model");
            assert_eq!(first.kind, ModelKind::Schema, "{name} starts with a schema");
            let relays = order
                .iter()
                .position(|node| node.kind == ModelKind::Relay)
                .assured("every example declares a relay");
            let lookups = order
                .iter()
                .rposition(|node| node.kind == ModelKind::Lookup);
            if let Some(last_lookup) = lookups {
                let first_processor = order
                    .iter()
                    .position(|node| node.kind.is_processor())
                    .unwrap_or(order.len());
                assert!(
                    last_lookup < first_processor,
                    "{name} creates every hash map before the processors that may read them"
                );
            }
            assert!(relays > 0, "{name} declares what a relay needs before it");
        }
    }
}
