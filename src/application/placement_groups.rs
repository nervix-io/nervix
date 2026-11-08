//! Lookup of committed hard groups by their exact execution units.
//!
//! Layer: control plane.
//! - **Owns.** A borrowed index of the disjoint groups in one schedule.
//! - **Depends on.** Scheduled groups and shared unit identities.
//! - **Must not know.** Placement policy, runtime tasks, transport or locks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns the boundary declared above")
)]

use ahash::{HashMap, HashSet};
use nervix_models::{NodeUnitRef, PlacementGroupSchedule};

pub(in crate::application) struct PlacementGroupIndex<'a> {
    groups: &'a [PlacementGroupSchedule],
    by_member: HashMap<&'a NodeUnitRef, usize>,
    empty: Option<usize>,
}

impl<'a> PlacementGroupIndex<'a> {
    pub fn new(groups: &'a [PlacementGroupSchedule]) -> Self {
        Self {
            groups,
            by_member: groups
                .iter()
                .enumerate()
                .flat_map(|(index, group)| group.members.iter().map(move |member| (member, index)))
                .collect(),
            empty: groups.iter().position(|group| group.members.is_empty()),
        }
    }

    pub fn index_of(&self, members: &[NodeUnitRef]) -> Option<usize> {
        let Some(first) = members.first() else {
            return self.empty;
        };
        let index = *self.by_member.get(first)?;
        let group = &self.groups[index];
        let members = members.iter().collect::<HashSet<_>>();
        (group.members.len() == members.len()
            && group.members.iter().all(|member| members.contains(member)))
        .then_some(index)
    }

    pub fn find(&self, members: &[NodeUnitRef]) -> Option<&'a PlacementGroupSchedule> {
        self.index_of(members).map(|index| &self.groups[index])
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{ModelKind, ModelName, NodeRef, Unit};

    use super::*;

    fn member(name: &str, partition: u32) -> NodeUnitRef {
        NodeUnitRef::new(
            NodeRef::new(
                ModelKind::Relay,
                ModelName::try_from(name).expect("valid relay name"),
            ),
            Unit::partition(partition),
        )
    }

    #[test]
    fn finds_the_exact_group_in_any_member_order_without_selecting_siblings() {
        let groups = [0, 1].map(|partition| PlacementGroupSchedule {
            members: vec![member("incoming", partition), member("outgoing", partition)],
            primary_node: None,
        });
        let index = PlacementGroupIndex::new(&groups);
        assert_eq!(index.index_of(&groups[0].members), Some(0));
        assert_eq!(
            index.index_of(&[member("outgoing", 1), member("incoming", 1)]),
            Some(1)
        );
        assert!(index.find(&[member("incoming", 0)]).is_none());
        assert!(
            index
                .find(&[member("incoming", 0), member("outgoing", 1)])
                .is_none()
        );
        assert!(
            index
                .find(&[member("incoming", 0), member("incoming", 0)])
                .is_none()
        );
    }
}
