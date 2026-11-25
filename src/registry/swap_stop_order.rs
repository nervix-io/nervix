//! The order in which an entity swap stops the parts it replaces.
//!
//! Layer: decisions.
//!
//! - **Owns.** Which ingestors a swap stops before it holds any relay, and the order in which it
//!   stops the reingestors it replaces or moves, decided from the schedule whose parts it stops.
//! - **Depends on.** The vocabulary's schedules, the record inputs and outputs of their Models,
//!   and the relays the swap holds.
//! - **Must not know.** How a part stops, how a relay is held, or which cluster node runs what.
//!
//! A swap holds the input relays of every part it replaces until it ends, and a part that stops
//! delivers what it holds before it ends. A stopping part whose rows reach a relay the swap
//! already holds would wait for the swap while the swap waits for it, so the swap stops parts
//! upstream first: the ingestors whose rows reach a relay it holds before it holds any, and the
//! reingestors in the order rows flow through them, each after holding its own input relays and
//! before the holds downstream of it. A domain's graph has no loops, so that order always exists.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::collections::{BTreeMap, BTreeSet};

use meticulous::OptionExt as _;
use nervix_models::{
    CorrelationTimeoutAction, DomainSchedule, IngestorName, MessageErrorPolicy, Model, ModelKind,
    NodeRef, RelayName,
};

use crate::registry::entity_gate::entity_input_relays;

/// The order in which one entity swap stops the parts it replaces, so that no stopping part
/// delivers into a relay the same swap already holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SwapStopOrder {
    /// The ingestors whose rows reach a relay the swap holds. The swap stops their units before
    /// it holds any relay; every other ingestor unit it stops keeps its place under the swap's
    /// hold, so its intake ends no sooner than it has to.
    ingestors_before_holds: BTreeSet<IngestorName>,
    /// The reingestors the swap replaces or moves, in the order rows flow through them.
    reingestors: Vec<NodeRef>,
}

impl SwapStopOrder {
    /// Decides the order for a swap that stops parts of `existing`, replaces or moves the nodes
    /// `gated` names, and holds the relays `held` names.
    pub(crate) fn plan(existing: &DomainSchedule, gated: &[NodeRef], held: &[RelayName]) -> Self {
        let flow = RecordFlow::of(existing);
        let mut ingestors_before_holds = BTreeSet::new();
        for node in flow.nodes_reaching(held) {
            if node.kind == ModelKind::Ingestor {
                ingestors_before_holds.insert(IngestorName::from(&node.identifier));
            }
        }
        let gated = gated.iter().collect::<BTreeSet<_>>();
        let mut reingestors = Vec::new();
        for node in flow.upstream_first() {
            if node.kind == ModelKind::Reingestor && gated.contains(&node) {
                reingestors.push(node);
            }
        }
        Self {
            ingestors_before_holds,
            reingestors,
        }
    }

    /// Whether the swap stops the units of `ingestor` before it holds any relay, because rows the
    /// ingestor delivers reach a relay the swap holds.
    pub(crate) fn stops_before_holds(&self, ingestor: &IngestorName) -> bool {
        self.ingestors_before_holds.contains(ingestor)
    }

    /// The reingestors the swap replaces or moves, upstream first.
    pub(crate) fn reingestors(&self) -> &[NodeRef] {
        &self.reingestors
    }
}

/// How rows travel through one schedule: the relays each node delivers rows into and the relays
/// each node reads rows from. A read of a relay's materialized state carries no rows, so it is
/// not a record input: the node that reads state never holds back the relay it reads.
struct RecordFlow {
    outputs: BTreeMap<NodeRef, Vec<RelayName>>,
    inputs: BTreeMap<NodeRef, Vec<RelayName>>,
}

impl RecordFlow {
    fn of(schedule: &DomainSchedule) -> Self {
        let mut outputs = BTreeMap::new();
        let mut inputs = BTreeMap::new();
        for node in schedule.nodes.values() {
            let model = node.config.as_ref();
            outputs.insert(node.identity(), delivered_relays(model));
            inputs.insert(node.identity(), record_inputs(model));
        }
        Self { outputs, inputs }
    }

    /// Every node whose rows reach one of `held`: a node that delivers into one of them, and every
    /// node that delivers rows to such a node, through any number of relays and nodes.
    fn nodes_reaching(&self, held: &[RelayName]) -> BTreeSet<NodeRef> {
        let mut writers = BTreeMap::<&RelayName, Vec<&NodeRef>>::new();
        for (node, relays) in &self.outputs {
            for relay in relays {
                writers.entry(relay).or_default().push(node);
            }
        }
        let mut reached_relays = held.iter().collect::<BTreeSet<_>>();
        let mut pending = held.iter().collect::<Vec<_>>();
        let mut reaching = BTreeSet::new();
        while let Some(relay) = pending.pop() {
            let Some(relay_writers) = writers.get(relay) else {
                continue;
            };
            for writer in relay_writers {
                if !reaching.insert((*writer).clone()) {
                    continue;
                }
                let Some(writer_inputs) = self.inputs.get(*writer) else {
                    continue;
                };
                for input in writer_inputs {
                    if reached_relays.insert(input) {
                        pending.push(input);
                    }
                }
            }
        }
        reaching
    }

    /// Every node of the schedule, each after every node whose rows reach it, and in node order
    /// where rows decide nothing.
    fn upstream_first(&self) -> Vec<NodeRef> {
        let mut readers = BTreeMap::<&RelayName, Vec<&NodeRef>>::new();
        for (node, relays) in &self.inputs {
            for relay in relays {
                readers.entry(relay).or_default().push(node);
            }
        }
        let mut downstream = BTreeMap::<&NodeRef, Vec<&NodeRef>>::new();
        let mut waiting_on = self
            .outputs
            .keys()
            .map(|node| (node, 0_usize))
            .collect::<BTreeMap<_, _>>();
        for (node, relays) in &self.outputs {
            for relay in relays {
                let Some(relay_readers) = readers.get(relay) else {
                    continue;
                };
                for reader in relay_readers {
                    downstream.entry(node).or_default().push(*reader);
                    let count = waiting_on
                        .get_mut(*reader)
                        .verified("every node that reads rows is a node of the same schedule");
                    *count = count
                        .checked_add(1)
                        .assured("a node waits on fewer edges than a usize counts");
                }
            }
        }
        let mut ready = BTreeSet::new();
        for (node, count) in &waiting_on {
            if *count == 0 {
                ready.insert(*node);
            }
        }
        let mut order = Vec::with_capacity(waiting_on.len());
        while order.len() < waiting_on.len() {
            let node = ready.pop_first().verified(
                "the registry refuses every loop in a domain's graph before a schedule holds it, \
                 so a node whose upstream nodes all have their place is always left",
            );
            order.push(node.clone());
            let Some(next) = downstream.get(node) else {
                continue;
            };
            for reader in next {
                let count = waiting_on
                    .get_mut(*reader)
                    .verified("every node that reads rows is a node of the same schedule");
                *count = count
                    .checked_sub(1)
                    .verified("each edge into a reader was counted once above");
                if *count == 0 {
                    ready.insert(*reader);
                }
            }
        }
        order
    }
}

/// The relays `model` delivers rows into: its routes, the error routes of its routes or, for an
/// emitter, its own error route, and the relays a correlator's timeout actions send to.
fn delivered_relays(model: &Model) -> Vec<RelayName> {
    let mut relays = Vec::new();
    if let Some(outputs) = model.output_routes() {
        for output in outputs.outputs() {
            relays.push(output.relay.clone());
            if let MessageErrorPolicy::Dlq { relay, .. } = &output.message_error_policy {
                relays.push(relay.clone());
            }
        }
    }
    match model {
        Model::Emitter(emitter) => {
            if let MessageErrorPolicy::Dlq { relay, .. } = &emitter.error_policies.message {
                relays.push(relay.clone());
            }
        }
        Model::Correlator(correlator) => {
            for action in [
                &correlator.timeout_policy.left,
                &correlator.timeout_policy.right,
            ] {
                if let CorrelationTimeoutAction::SendTo { relay } = action {
                    relays.push(relay.clone());
                }
            }
        }
        _ => {}
    }
    relays.sort();
    relays.dedup();
    relays
}

/// The relays whose rows `model` reads. A generator reads only its source relay's materialized
/// state, so it reads no rows.
fn record_inputs(model: &Model) -> Vec<RelayName> {
    if let Model::Generator(_) = model {
        return Vec::new();
    }
    entity_input_relays(model)
}

#[cfg(test)]
#[path = "swap_stop_order_tests.rs"]
mod tests;
