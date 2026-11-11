//! The partition epoch every branch of a domain's next schedule takes.
//!
//! Layer: decisions.
//!
//! - **Owns.** Deciding each branch's partition epoch from the schedule in force and the branch
//!   declarations of the next one, the units a transition keeps scheduled for keys placed under
//!   the previous count, and whether a branch is still in such a transition.
//! - **Depends on.** Scheduled vocabulary, branch declarations and the active graph's nodes.
//! - **Must not know.** Runtime tasks, how a key moves between units, how a transition is observed
//!   or when it retires.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::collections::{BTreeMap, BTreeSet};

use nervix_models::{
    BranchName, BranchPartitionEpoch, DomainSchedule, Model, ModelKind, ModelName, NodeRef,
    PartitionEpochNumber, PartitionTopology, ResolvedBranchPartitions, Unit,
};

use crate::registry::graph::ActiveNode;

/// The partition epoch each branch of a domain's next schedule takes, decided from the schedule in
/// force: a branch whose count is unchanged keeps its epoch, and one whose count changed begins the
/// next epoch with a transition from the count its keys were placed by.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PartitionEpochs {
    epochs: BTreeMap<BranchName, BranchPartitionEpoch>,
    retained: BTreeMap<BranchName, BTreeSet<Unit>>,
}

impl PartitionEpochs {
    /// Reinstates captured epochs only for the layouts the restored graph actually declares.
    pub(crate) fn restored(
        snapshot: &nervix_models::PartitionTopologySnapshot,
        declared: &BTreeMap<BranchName, ResolvedBranchPartitions>,
    ) -> Option<Self> {
        if snapshot
            .epochs
            .iter()
            .any(|(branch, epoch)| declared.get(branch) != Some(&epoch.partitions))
        {
            return None;
        }
        Some(Self {
            epochs: snapshot.epochs.clone(),
            retained: snapshot.holding_units.clone(),
        })
    }

    /// Decides the epoch of every branch `declared` names against `existing`, the schedule in
    /// force, or none for a domain that has no schedule yet. A changed branch in `keyed`, whose
    /// entities keep state per key, begins a transition from its previous count; any other changed
    /// branch begins its next epoch settled, since none of its keys has state to move.
    pub(crate) fn decide(
        existing: Option<&DomainSchedule>,
        declared: &BTreeMap<BranchName, ResolvedBranchPartitions>,
        topology: &PartitionTopology,
        changed_sources: &BTreeSet<BranchName>,
        keyed: &BTreeSet<BranchName>,
    ) -> Self {
        let mut epochs = BTreeMap::new();
        for (branch, partitions) in declared {
            let recorded = match existing {
                Some(schedule) => schedule.partition_epochs.get(branch),
                None => None,
            };
            let in_force = match (recorded, existing) {
                (Some(epoch), _) => Some(epoch.partitions),
                (None, Some(schedule)) => scheduled_branch_partitions(schedule, branch),
                (None, None) => None,
            };
            let decided = match in_force {
                // A branch the schedule in force does not declare begins with its first count.
                None if partitions.is_inherited() => Some(BranchPartitionEpoch {
                    number: PartitionEpochNumber::FIRST,
                    partitions: *partitions,
                    transition: None,
                }),
                None => None,
                Some(count)
                    if count == *partitions
                        && !changed_sources.contains(branch)
                        && existing.is_none_or(|schedule| {
                            schedule.partition_topology.branches.get(branch)
                                == topology.branches.get(branch)
                        }) =>
                {
                    recorded.cloned()
                }
                Some(count) if keyed.contains(branch) => {
                    Some(BranchPartitionEpoch::changed(recorded, count, *partitions))
                }
                Some(_) => Some(BranchPartitionEpoch::settled_change(recorded, *partitions)),
            };
            if let Some(decided) = decided {
                epochs.insert(branch.clone(), decided);
            }
        }
        let mut retained = BTreeMap::new();
        for (branch, epoch) in &epochs {
            if epoch.transition.is_none() {
                continue;
            }
            let mut units = epoch.holding_units().into_iter().collect::<BTreeSet<_>>();
            if let Some(existing) = existing {
                for node in existing.nodes.values() {
                    if node.keeps_state_per_key() && node.partition_branch() == Some(branch) {
                        units.extend(
                            node.units
                                .keys()
                                .copied()
                                .filter(|unit| !epoch.partitions.schedules(*unit)),
                        );
                    }
                }
            }
            if !units.is_empty() {
                retained.insert(branch.clone(), units);
            }
        }
        Self { epochs, retained }
    }

    /// The units every node `branch` branches keeps scheduled beyond its current count's units.
    pub(crate) fn holding_units(&self, branch: &BranchName) -> Vec<Unit> {
        if let Some(units) = self.retained.get(branch) {
            return units.iter().copied().collect();
        }
        match self.epochs.get(branch) {
            Some(epoch) => epoch.holding_units(),
            None => Vec::new(),
        }
    }

    /// Every unit `node` is scheduled as: the units of its current layout, then, for a node that
    /// keeps state per key, the units its branch's transition keeps for keys placed under the
    /// previous count.
    pub(crate) fn scheduled_units(&self, node: &ActiveNode) -> Vec<Unit> {
        let mut units = node.placement_units();
        if node.config.keeps_state_per_key()
            && let Some(branch) = node.partition_branch()
        {
            units.extend(self.holding_units(branch));
        }
        units
    }

    pub(crate) fn into_map(self) -> BTreeMap<BranchName, BranchPartitionEpoch> {
        self.epochs
    }
}

/// The count `branch` was placed by when `schedule` was decided, read from its declaration, or
/// none when `schedule` does not declare the branch.
fn scheduled_branch_partitions(
    schedule: &DomainSchedule,
    branch: &BranchName,
) -> Option<ResolvedBranchPartitions> {
    let node = schedule
        .nodes
        .get(&NodeRef::new(ModelKind::Branch, ModelName::from(branch)))?;
    match node.config.as_ref() {
        Model::Branch(declaration) => declaration.partitions.direct_layout().or_else(|| {
            schedule
                .partition_topology
                .branches
                .get(branch)
                .map(|layout| layout.partitions())
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        BranchPartitionEpoch, CreateBranch, DomainName, PartitionEpochNumber, PartitionTransition,
        ScheduledNode, SchemaFingerprint,
    };

    use super::*;

    fn partitions(count: u16) -> ResolvedBranchPartitions {
        ResolvedBranchPartitions::Direct(NonZeroU16::new(count).assured("test counts are positive"))
    }

    fn branch(name: &str) -> BranchName {
        BranchName::parse(name).assured("test branch names are valid")
    }

    fn schedule_declaring(name: &str, count: u16) -> DomainSchedule {
        let declaration = CreateBranch {
            name: branch(name),
            schema: nervix_models::SchemaName::parse("tenant_key")
                .assured("test schema names are valid"),
            ttl: "5m".to_string(),
            partitions: nervix_models::BranchPartitions::Direct(
                NonZeroU16::new(count).assured("positive test count"),
            ),
            eviction: None,
        };
        let node = ScheduledNode::new(
            Model::Branch(declaration),
            SchemaFingerprint::from_digest([0; 32]),
        );
        DomainSchedule::new(
            DomainName::parse("payments").assured("test domain names are valid"),
            [node],
            Vec::new(),
        )
    }

    fn declared(name: &str, count: u16) -> BTreeMap<BranchName, ResolvedBranchPartitions> {
        BTreeMap::from([(branch(name), partitions(count))])
    }

    fn keyed() -> BTreeSet<BranchName> {
        BTreeSet::from([branch("by_tenant")])
    }

    #[test]
    fn a_branch_without_keyed_state_begins_its_next_epoch_settled() {
        let existing = schedule_declaring("by_tenant", 8);
        let decided = PartitionEpochs::decide(
            Some(&existing),
            &declared("by_tenant", 4),
            &PartitionTopology::default(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        );
        let epoch = decided
            .epochs
            .get(&branch("by_tenant"))
            .assured("a changed count records its epoch");
        assert_eq!(epoch.number.get(), 2);
        assert_eq!(epoch.partitions, partitions(4));
        assert_eq!(epoch.transition, None);
        assert!(decided.holding_units(&branch("by_tenant")).is_empty());
    }

    #[test]
    fn a_branch_without_a_schedule_in_force_records_no_epoch() {
        let decided = PartitionEpochs::decide(
            None,
            &declared("by_tenant", 4),
            &PartitionTopology::default(),
            &BTreeSet::new(),
            &keyed(),
        );
        assert_eq!(decided.epochs.get(&branch("by_tenant")), None);
        assert!(decided.holding_units(&branch("by_tenant")).is_empty());
    }

    #[test]
    fn an_unchanged_count_keeps_the_branch_in_its_first_epoch() {
        let existing = schedule_declaring("by_tenant", 4);
        let decided = PartitionEpochs::decide(
            Some(&existing),
            &declared("by_tenant", 4),
            &PartitionTopology::default(),
            &BTreeSet::new(),
            &keyed(),
        );
        assert_eq!(decided.epochs.get(&branch("by_tenant")), None);
    }

    #[test]
    fn a_growing_count_begins_epoch_two_without_holding_units() {
        let existing = schedule_declaring("by_tenant", 4);
        let decided = PartitionEpochs::decide(
            Some(&existing),
            &declared("by_tenant", 8),
            &PartitionTopology::default(),
            &BTreeSet::new(),
            &keyed(),
        );
        let epoch = decided
            .epochs
            .get(&branch("by_tenant"))
            .assured("a changed count records its epoch");
        assert_eq!(epoch.number.get(), 2);
        assert_eq!(epoch.partitions, partitions(8));
        assert_eq!(
            epoch.transition,
            Some(PartitionTransition {
                previous: partitions(4)
            })
        );
        assert!(decided.holding_units(&branch("by_tenant")).is_empty());
    }

    #[test]
    fn a_shrinking_count_holds_the_units_the_new_count_drops() {
        let existing = schedule_declaring("by_tenant", 8);
        let decided = PartitionEpochs::decide(
            Some(&existing),
            &declared("by_tenant", 4),
            &PartitionTopology::default(),
            &BTreeSet::new(),
            &keyed(),
        );
        assert_eq!(
            decided.holding_units(&branch("by_tenant")),
            vec![
                Unit::partition(4),
                Unit::partition(5),
                Unit::partition(6),
                Unit::partition(7)
            ]
        );
    }

    #[test]
    fn growing_from_one_partition_holds_the_single_unit_and_shrinking_to_one_holds_every_partition()
    {
        let existing = schedule_declaring("by_tenant", 1);
        let grown = PartitionEpochs::decide(
            Some(&existing),
            &declared("by_tenant", 3),
            &PartitionTopology::default(),
            &BTreeSet::new(),
            &keyed(),
        );
        assert_eq!(
            grown.holding_units(&branch("by_tenant")),
            vec![Unit::SINGLE]
        );

        let existing = schedule_declaring("by_tenant", 3);
        let shrunk = PartitionEpochs::decide(
            Some(&existing),
            &declared("by_tenant", 1),
            &PartitionTopology::default(),
            &BTreeSet::new(),
            &keyed(),
        );
        assert_eq!(
            shrunk.holding_units(&branch("by_tenant")),
            vec![Unit::partition(0), Unit::partition(1), Unit::partition(2)]
        );
    }

    #[test]
    fn a_recorded_epoch_advances_from_its_number_and_keeps_a_settled_epoch_unchanged() {
        let mut existing = schedule_declaring("by_tenant", 8);
        existing.partition_epochs.insert(
            branch("by_tenant"),
            BranchPartitionEpoch {
                number: PartitionEpochNumber::FIRST.next(),
                partitions: partitions(8),
                transition: None,
            },
        );
        let kept = PartitionEpochs::decide(
            Some(&existing),
            &declared("by_tenant", 8),
            &PartitionTopology::default(),
            &BTreeSet::new(),
            &keyed(),
        );
        assert_eq!(
            kept.epochs.get(&branch("by_tenant")),
            existing.partition_epochs.get(&branch("by_tenant"))
        );
        let advanced = PartitionEpochs::decide(
            Some(&existing),
            &declared("by_tenant", 16),
            &PartitionTopology::default(),
            &BTreeSet::new(),
            &keyed(),
        );
        let epoch = advanced
            .epochs
            .get(&branch("by_tenant"))
            .assured("a changed count records its epoch");
        assert_eq!(epoch.number.get(), 3);
        assert_eq!(
            epoch.transition,
            Some(PartitionTransition {
                previous: partitions(8)
            })
        );
    }

    #[test]
    fn an_unchanged_count_keeps_a_transition_in_force_until_it_retires() {
        let mut existing = schedule_declaring("by_tenant", 8);
        existing.partition_epochs.insert(
            branch("by_tenant"),
            BranchPartitionEpoch::changed(None, partitions(4), partitions(8)),
        );
        let kept = PartitionEpochs::decide(
            Some(&existing),
            &declared("by_tenant", 8),
            &PartitionTopology::default(),
            &BTreeSet::new(),
            &keyed(),
        );
        assert_eq!(
            kept.epochs.get(&branch("by_tenant")),
            existing.partition_epochs.get(&branch("by_tenant"))
        );
    }
}
