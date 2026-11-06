//! Exact units that share an indivisible placement.
//!
//! Layer: decisions.
//! - **Owns.** Expanding logical colocation and partition-bound intake into unit components.
//! - **Depends on.** The validated graph and vocabulary identities.
//! - **Must not know.** Runtime state, transport, tasks or locks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::collections::{BTreeMap, BTreeSet};

use nervix_models::{NodeRef, NodeUnitRef};

use super::graph::ActiveGraph;

pub(in crate::registry) struct UnitPlacementGroups {
    pub groups: Vec<Vec<NodeUnitRef>>,
    pub by_member: BTreeMap<NodeUnitRef, usize>,
}

impl UnitPlacementGroups {
    pub fn decide(graph: &ActiveGraph, logical_groups: &[Vec<NodeRef>]) -> Self {
        let mut edges = BTreeMap::<NodeUnitRef, BTreeSet<NodeUnitRef>>::new();
        let mut connect = |left: NodeUnitRef, right: NodeUnitRef| {
            edges.entry(left.clone()).or_default().insert(right.clone());
            edges.entry(right).or_default().insert(left);
        };
        for group in logical_groups {
            let members = group
                .iter()
                .filter_map(|member| graph.node(member.kind, &member.identifier))
                .flat_map(|node| {
                    node.placement_units()
                        .into_iter()
                        .map(|unit| NodeUnitRef::new(node.node_ref(), unit))
                })
                .collect::<Vec<_>>();
            if let Some(first) = members.first() {
                for member in &members[1..] {
                    connect(first.clone(), member.clone());
                }
            }
        }
        for node in graph.graph.node_weights() {
            let Some(instancing) = &node.ingestor_units else {
                continue;
            };
            for unit in instancing.units() {
                for relay in instancing.colocated_relays(unit) {
                    connect(NodeUnitRef::new(node.node_ref(), unit), relay);
                }
            }
        }
        let mut groups = Vec::new();
        let mut by_member = BTreeMap::new();
        for member in edges.keys() {
            if by_member.contains_key(member) {
                continue;
            }
            let mut pending = vec![member.clone()];
            let mut component = BTreeSet::new();
            while let Some(next) = pending.pop() {
                if !component.insert(next.clone()) {
                    continue;
                }
                pending.extend(edges.get(&next).into_iter().flatten().cloned());
            }
            for member in &component {
                by_member.insert(member.clone(), groups.len());
            }
            groups.push(component.into_iter().collect());
        }
        Self { groups, by_member }
    }
}
