//! The remote consumer edges a relay owner keeps for emitter units placed on other nodes.
//!
//! Layer: data plane.
//! - **Owns.** Which relay units of the relays this node owns each remote emitter unit reads: every
//!   unit for a single-unit emitter, and exactly the units an instance serves.
//! - **Depends on.** The installed revision's emitter plans and unit placements, and the remote
//!   consumer registrations of relay dispatch.
//! - **Must not know.** How remote batches are framed, admitted or acknowledged.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::{remote_dispatch::push_remote_scoped_runtime_consumer, *};
use crate::emitter_execution_plan::EmitterInputUnits;

impl Runtime {
    /// Adds to `consumers` the remote consumer of every relay unit this node owns (`owned_relays`)
    /// that an emitter unit placed on another node reads.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the typed revision supplies bounded iteration over its \
                                   installed emitter plans and their units")
    )]
    pub(super) fn push_emitter_remote_consumers(
        revision: &ExecutionRevision,
        local_node_id: &ClusterNodeName,
        owned_relays: &HashSet<RelayName>,
        consumers: &mut HashMap<RelayName, Vec<RemoteRuntimeConsumer>>,
    ) {
        // A single-unit emitter reads every unit of its inputs; an instance reads only the relay
        // units it serves, on the node that runs it.
        for emitter in revision.emitters.emitters() {
            let node = revision
                .nodes
                .get(&NodeRef::new(
                    ModelKind::Emitter,
                    ModelName::from(&emitter.name),
                ))
                .assured("the emitter plans were decided from this same schedule");
            for unit in node.units.keys().copied() {
                let Some(target_node) = node.unit_execution_node(unit) else {
                    continue;
                };
                if target_node == local_node_id {
                    continue;
                }
                for input in &emitter.inputs {
                    if !owned_relays.contains(&input.relay) {
                        continue;
                    }
                    match emitter.input_units(unit, input) {
                        EmitterInputUnits::WholeRelay => push_remote_scoped_runtime_consumer(
                            consumers.entry(input.relay.clone()).or_default(),
                            target_node,
                            &input.relay,
                            emitter.mode,
                            None,
                        ),
                        EmitterInputUnits::Units(relay_units) => {
                            for relay_unit in relay_units {
                                // An unpartitioned relay's one unit is the whole relay.
                                let scope = if relay_unit == Unit::SINGLE {
                                    None
                                } else {
                                    Some(relay_unit)
                                };
                                push_remote_scoped_runtime_consumer(
                                    consumers.entry(input.relay.clone()).or_default(),
                                    target_node,
                                    &input.relay,
                                    emitter.mode,
                                    scope,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
