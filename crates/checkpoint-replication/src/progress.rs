//! What the replicas of one placement reported holding.

use std::collections::{BTreeMap, BTreeSet};

use meticulous::OptionExt as _;
use nervix_models::ClusterNodeName;

/// The highest revision each replica of one placement reported holding on its stable storage.
///
/// A replica's progress only rises. Acknowledgements travel independently of each other, and a
/// replica acknowledges what it already holds again whenever it is offered a checkpoint, so an
/// older report can arrive after a newer one; it leaves the newer one in place.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicaProgress {
    held: BTreeMap<ClusterNodeName, u64>,
}

impl ReplicaProgress {
    /// Record that `replica` reported holding `revision`, and whether that raised what it holds.
    pub fn record(&mut self, replica: &ClusterNodeName, revision: u64) -> bool {
        let Some(held) = self.held.get_mut(replica) else {
            self.held.insert(replica.clone(), revision);
            return true;
        };
        if *held >= revision {
            return false;
        }
        *held = revision;
        true
    }

    /// The highest revision `replica` reported holding, or nothing when it never reported.
    pub fn held(&self, replica: &ClusterNodeName) -> Option<u64> {
        self.held.get(replica).copied()
    }

    /// Whether `replica` reported holding `revision` or a newer one.
    pub fn holds(&self, replica: &ClusterNodeName, revision: u64) -> bool {
        match self.held(replica) {
            Some(held) => held >= revision,
            None => false,
        }
    }

    /// The replicas among `replicas` that have not reported holding `revision`.
    pub fn awaiting(
        &self,
        replicas: &BTreeSet<ClusterNodeName>,
        revision: u64,
    ) -> BTreeSet<ClusterNodeName> {
        let mut awaiting = BTreeSet::new();
        for replica in replicas {
            if !self.holds(replica, revision) {
                awaiting.insert(replica.clone());
            }
        }
        awaiting
    }

    /// How many of `replicas` reported holding `revision`.
    pub fn holding(&self, replicas: &BTreeSet<ClusterNodeName>, revision: u64) -> usize {
        let mut holding = 0_usize;
        for replica in replicas {
            if self.holds(replica, revision) {
                holding = holding
                    .checked_add(1)
                    .assured("the count rises once per member of a set whose length is a usize");
            }
        }
        holding
    }
}

#[cfg(test)]
#[path = "progress_tests.rs"]
mod tests;
