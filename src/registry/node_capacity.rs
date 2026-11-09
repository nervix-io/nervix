//! Exact CPU and memory normalization at the scheduler's load-ranking tier.
//!
//! Layer: decisions.
//! - **Owns.** Proportional load scores from the limiting measured capacity dimension.
//! - **Depends on.** Resolved node capabilities and integer ratios.
//! - **Must not know.** Membership, locks, tasks or how assignments execute.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns capacity-weighted load scores")
)]

use std::collections::BTreeMap;

use nervix_models::{ClusterNodeName, NodeCapabilities};
use num_rational::Ratio;

/// Relative capacity uses only dimensions measured for every eligible node. Unknown measurements
/// do not invent hardware. With no common measurement the existing uniform ranking applies.
pub(super) struct CapacityWeights {
    weights: BTreeMap<ClusterNodeName, Ratio<u128>>,
}

impl CapacityWeights {
    pub(super) fn new(
        nodes: &[ClusterNodeName],
        capabilities: &BTreeMap<ClusterNodeName, NodeCapabilities>,
    ) -> Self {
        let cpu = nodes
            .iter()
            .map(|node| capabilities.get(node)?.cpu_count)
            .collect::<Option<Vec<_>>>();
        let memory = nodes
            .iter()
            .map(|node| capabilities.get(node)?.memory_bytes)
            .collect::<Option<Vec<_>>>();
        let min_cpu = cpu.as_ref().and_then(|values| values.iter().min()).copied();
        let min_memory = memory
            .as_ref()
            .and_then(|values| values.iter().min())
            .copied();
        let mut weights = BTreeMap::new();
        for node in nodes {
            let Some(capacity) = capabilities.get(node) else {
                continue;
            };
            let cpu = match (capacity.cpu_count, min_cpu) {
                (Some(count), Some(minimum)) => Some(Ratio::new(
                    u128::from(count.get()),
                    u128::from(minimum.get()),
                )),
                _ => None,
            };
            let memory = match (capacity.memory_bytes, min_memory) {
                (Some(bytes), Some(minimum)) => Some(Ratio::new(
                    u128::from(bytes.get()),
                    u128::from(minimum.get()),
                )),
                _ => None,
            };
            let weight = match (cpu, memory) {
                (Some(cpu), Some(memory)) => cpu.min(memory),
                (Some(weight), None) | (None, Some(weight)) => weight,
                (None, None) => Ratio::from_integer(1),
            };
            weights.insert(node.clone(), weight);
        }
        Self { weights }
    }

    pub(super) fn load(&self, node: &ClusterNodeName, load: usize) -> Ratio<u128> {
        use meticulous::{OptionExt as _, ResultExt as _};
        let weight = self
            .weights
            .get(node)
            .assured("only nodes with committed capability inputs enter assignment");
        let load = u128::try_from(load).assured("an in-memory node count fits u128");
        let numerator = load
            .checked_mul(*weight.denom())
            .assured("a platform-sized load and a u64 capacity fit u128");
        Ratio::new(numerator, *weight.numer())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    #[test]
    fn the_limiting_dimension_weights_load_without_multiplying_capacity() {
        let nodes = ["small", "large", "cpu_only"]
            .map(|node| ClusterNodeName::parse(node).expect("node name"));
        let receive = nervix_interconnect::PeerReceiveSettings::default().bounds();
        let values = [(2, 8), (4, 16), (16, 8)];
        let capabilities = nodes
            .iter()
            .zip(values)
            .map(|(node, (cpu, memory))| {
                (
                    node.clone(),
                    NodeCapabilities {
                        interconnect: nervix_interconnect::TransportSettings::default()
                            .capabilities(),
                        dns: nervix_arbitrary::Arbitrary::new(
                            &[],
                            nervix_arbitrary::Domain::Vocabulary,
                        )
                        .node_capability_record()
                        .capabilities
                        .dns,
                        node_database: nervix_arbitrary::Arbitrary::new(
                            &[],
                            nervix_arbitrary::Domain::Vocabulary,
                        )
                        .node_capability_record()
                        .capabilities
                        .node_database,
                        consensus_database: nervix_arbitrary::Arbitrary::new(
                            &[],
                            nervix_arbitrary::Domain::Vocabulary,
                        )
                        .node_capability_record()
                        .capabilities
                        .consensus_database,
                        consensus: nervix_arbitrary::Arbitrary::new(
                            &[],
                            nervix_arbitrary::Domain::Vocabulary,
                        )
                        .node_capability_record()
                        .capabilities
                        .consensus,
                        cpu_count: NonZeroU64::new(cpu),
                        memory_bytes: NonZeroU64::new(memory),
                        receive,
                    },
                )
            })
            .collect();
        let weights = CapacityWeights::new(&nodes, &capabilities);
        assert_eq!(weights.load(&nodes[0], 8), weights.load(&nodes[1], 16));
        assert_eq!(weights.load(&nodes[0], 8), weights.load(&nodes[2], 8));
    }

    #[test]
    fn extreme_capacity_ratios_and_loads_compare_without_overflow() {
        let nodes = ["small", "large"].map(|node| ClusterNodeName::parse(node).expect("node name"));
        let receive = nervix_interconnect::PeerReceiveSettings::default().bounds();
        let capabilities = [
            (
                nodes[0].clone(),
                NodeCapabilities {
                    interconnect: nervix_interconnect::TransportSettings::default().capabilities(),
                    dns: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .dns,
                    node_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .node_database,
                    consensus_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus_database,
                    consensus: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus,
                    cpu_count: NonZeroU64::new(1),
                    memory_bytes: NonZeroU64::new(1),
                    receive,
                },
            ),
            (
                nodes[1].clone(),
                NodeCapabilities {
                    interconnect: nervix_interconnect::TransportSettings::default().capabilities(),
                    dns: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .dns,
                    node_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .node_database,
                    consensus_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus_database,
                    consensus: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus,
                    cpu_count: NonZeroU64::new(u64::MAX),
                    memory_bytes: NonZeroU64::new(u64::MAX - 1),
                    receive,
                },
            ),
        ]
        .into();
        let weights = CapacityWeights::new(&nodes, &capabilities);
        assert!(weights.load(&nodes[1], usize::MAX) < weights.load(&nodes[0], usize::MAX));
    }

    #[test]
    fn unknown_dimensions_do_not_invent_capacity_or_suppress_known_common_dimensions() {
        let nodes = ["a", "b"].map(|node| ClusterNodeName::parse(node).expect("node name"));
        let receive = nervix_interconnect::PeerReceiveSettings::default().bounds();
        let mut capabilities = BTreeMap::from([
            (
                nodes[0].clone(),
                NodeCapabilities {
                    interconnect: nervix_interconnect::TransportSettings::default().capabilities(),
                    dns: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .dns,
                    node_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .node_database,
                    consensus_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus_database,
                    consensus: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus,
                    cpu_count: NonZeroU64::new(2),
                    memory_bytes: None,
                    receive,
                },
            ),
            (
                nodes[1].clone(),
                NodeCapabilities {
                    interconnect: nervix_interconnect::TransportSettings::default().capabilities(),
                    dns: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .dns,
                    node_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .node_database,
                    consensus_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus_database,
                    consensus: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus,
                    cpu_count: NonZeroU64::new(4),
                    memory_bytes: NonZeroU64::new(16),
                    receive,
                },
            ),
        ]);
        let weights = CapacityWeights::new(&nodes, &capabilities);
        assert_eq!(weights.load(&nodes[0], 1), weights.load(&nodes[1], 2));
        capabilities
            .get_mut(&nodes[1])
            .expect("second node")
            .cpu_count = None;
        let weights = CapacityWeights::new(&nodes, &capabilities);
        assert_eq!(weights.load(&nodes[0], 2), weights.load(&nodes[1], 2));
    }
}
