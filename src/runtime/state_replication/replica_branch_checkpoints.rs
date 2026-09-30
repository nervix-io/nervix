//! Layer: data plane.
//! Owns: what the replica task of one branch-keyed entity knows of the branch checkpoints it keeps
//! current: the owner's catalog as far as the task read it, what this node holds of each branch,
//! and the branches the task still has to fetch or acknowledge.
//! May depend on: catalog pages, the owner's announcements, the branches a lifecycle names, and
//! typed branch keys.
//! Must not know: how a page, an announcement or a checkpoint travels, how a checkpoint is
//! installed or stored, schedules, or NSPL.

use ahash::HashMap;
use meticulous::OptionExt as _;
use nervix_interconnect::{BranchCheckpointCursor, RuntimeState};

use super::super::{
    BranchKey,
    branch_checkpoint_catalog::{CatalogedCheckpoint, CheckpointListing, CheckpointPage},
    branch_lifecycle_state::{AnnouncedCheckpoint, NamedBranches},
};

/// What the replica task of one branch-keyed entity knows of the entity's branch checkpoints.
///
/// The task owns it alone, so nothing it records is shared or locked. A round of the task reads
/// the owner's catalog from the cursor kept here, takes the owner's announcements, plans a step for
/// each branch that may lag, and settles each step with what happened. A branch nothing changed
/// for is never looked at, so a round in which no branch changed plans nothing.
#[derive(Debug, Default)]
pub(super) struct ReplicaBranchCheckpoints {
    /// Where the task stands in the owner's catalog, absent before it read the catalog.
    cursor: Option<BranchCheckpointCursor>,
    /// The owner's catalog as the pages the task read describe it.
    owner: HashMap<Option<BranchKey>, OwnerCheckpoint>,
    /// What this node holds of each branch the task looked at. A branch the task has not looked
    /// at since it started is absent: its step reads what this node holds first.
    held: HashMap<Option<BranchKey>, Held>,
    /// The branches to look at in the next round, and why.
    pending: HashMap<Option<BranchKey>, PendingBranch>,
}

/// The newest checkpoint the owner catalogued for one branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OwnerCheckpoint {
    state: RuntimeState,
    lsm: u64,
}

/// What this node holds of one branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Held {
    /// This node holds no checkpoint of the branch.
    Nothing,
    /// This node holds the checkpoint at this revision.
    Revision(u64),
}

/// Why a branch waits to be looked at.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PendingBranch {
    /// The newest checkpoint the owner announced for the branch and waits for this node to
    /// acknowledge.
    announced: Option<OwnerCheckpoint>,
}

/// What the task does for one branch in a round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BranchStep {
    pub(super) branch: Option<BranchKey>,
    /// The state the newest known checkpoint of the branch belongs to.
    pub(super) state: RuntimeState,
    /// What this node holds of the branch, when the task already knows. An unknown branch is read
    /// from this node's own copy first.
    pub(super) held: Option<Held>,
    /// The newest revision the owner catalogued or announced for the branch.
    pub(super) target: u64,
    /// Whether the owner announced `target` and waits for this node to acknowledge it, which this
    /// node does even when it already holds it.
    pub(super) acknowledge: bool,
}

/// What happened to one step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StepOutcome {
    /// The step completed, and this node holds this of the branch afterwards.
    Settled(Held),
    /// The state the step names is not the one this node's schedule names for the branch, such as
    /// guest state of a replaced generation. The step is dropped: a checkpoint of the state the
    /// schedule names brings the branch back when the owner catalogues or announces it.
    Stale,
    /// The step failed before it completed. The branch is looked at again in the next round, with
    /// what this node was found to hold, when the step got that far.
    Failed(Option<Held>),
}

impl Held {
    /// The revision a request for a newer checkpoint names, nothing when this node holds none.
    pub(super) fn after_lsm(self) -> Option<u64> {
        match self {
            Self::Nothing => None,
            Self::Revision(lsm) => Some(lsm),
        }
    }

    /// Whether this is revision `lsm` or a newer one.
    pub(super) fn covers(self, lsm: u64) -> bool {
        match self {
            Self::Nothing => false,
            Self::Revision(held) => held >= lsm,
        }
    }
}

impl ReplicaBranchCheckpoints {
    /// Where the task stands in the owner's catalog.
    pub(super) fn cursor(&self) -> Option<BranchCheckpointCursor> {
        self.cursor
    }

    /// Forget what the task read of the owner's catalog, because the owner holds no branch state
    /// of the entity any more.
    pub(super) fn forget_owner(&mut self) {
        self.cursor = None;
        self.owner.clear();
    }

    /// Apply one page of the owner's catalog, and look at every branch it revised. Returns whether
    /// further changes follow the page.
    pub(super) fn apply(&mut self, listing: CheckpointListing) -> bool {
        let page = match listing {
            CheckpointListing::Restarted(page) => {
                self.owner.clear();
                page
            }
            CheckpointListing::Continued(page) => page,
        };
        let CheckpointPage {
            cursor,
            removed,
            revised,
            more,
        } = page;
        for branch in removed {
            self.owner.remove(&branch);
        }
        for CatalogedCheckpoint { branch, state, lsm } in revised {
            self.owner
                .insert(branch.clone(), OwnerCheckpoint { state, lsm });
            self.waiting(branch);
        }
        self.cursor = Some(cursor);
        more
    }

    /// Take the owner's announcements: each announced branch is looked at in the next round, and
    /// acknowledged even when this node already holds the announced revision.
    pub(super) fn take_announced(
        &mut self,
        announced: impl IntoIterator<Item = (Option<BranchKey>, AnnouncedCheckpoint)>,
    ) {
        for (branch, checkpoint) in announced {
            let announcement = OwnerCheckpoint {
                state: checkpoint.state,
                lsm: checkpoint.lsm,
            };
            let pending = self.waiting(branch);
            let newer = match pending.announced {
                Some(earlier) => earlier.lsm < announcement.lsm,
                None => true,
            };
            if newer {
                pending.announced = Some(announcement);
            }
        }
    }

    /// Follow the lifecycle this node holds from `previous` to `current`: forget what this node
    /// held of the branches `current` no longer names, whose copies it dropped, and look at the
    /// branches the owner catalogued that `current` names for the first time.
    pub(super) fn follow_lifecycle(
        &mut self,
        previous: Option<&NamedBranches>,
        current: &NamedBranches,
    ) {
        self.held
            .retain(|branch, _| Self::may_install(current, branch.as_ref()));
        let mut newly_named = Vec::new();
        for branch in self.owner.keys() {
            let named_before = match previous {
                Some(previous) => Self::may_install(previous, branch.as_ref()),
                None => false,
            };
            if !named_before && Self::may_install(current, branch.as_ref()) {
                newly_named.push(branch.clone());
            }
        }
        for branch in newly_named {
            self.waiting(branch);
        }
    }

    /// The steps of the next round: one for every waiting branch this node may install a
    /// checkpoint of while `named` is the lifecycle it holds, and that it may not hold at its newest
    /// known revision yet, or whose announcement it has to acknowledge. A waiting concrete branch
    /// the lifecycle does not name is dropped: a checkpoint of it would be refused, and the
    /// lifecycle that names it brings it back.
    pub(super) fn plan(&mut self, named: &NamedBranches) -> Vec<BranchStep> {
        let mut steps = Vec::new();
        for (branch, pending) in self.pending.drain() {
            if !Self::may_install(named, branch.as_ref()) {
                continue;
            }
            let catalogued = self.owner.get(&branch).copied();
            // Revisions order the checkpoints of one state only. An announcement of another state,
            // such as guest state of a generation the catalog has since replaced, never hides the
            // catalogued checkpoint: the catalog lists a checkpoint once, while the owner repeats
            // an announcement until it is acknowledged.
            let newest = match (catalogued, pending.announced) {
                (Some(catalogued), Some(announced))
                    if announced.state == catalogued.state && announced.lsm > catalogued.lsm =>
                {
                    announced
                }
                (Some(catalogued), _) => catalogued,
                (None, Some(announced)) => announced,
                (None, None) => continue,
            };
            let held = self.held.get(&branch).copied();
            let acknowledge = pending.announced.is_some();
            if let Some(held) = held
                && held.covers(newest.lsm)
                && !acknowledge
            {
                continue;
            }
            steps.push(BranchStep {
                branch,
                state: newest.state,
                held,
                target: newest.lsm,
                acknowledge,
            });
        }
        steps
    }

    /// Record what happened to `step`. A failed step is looked at again in the next round.
    pub(super) fn settle(&mut self, step: BranchStep, outcome: StepOutcome) {
        let (held, failed) = match outcome {
            StepOutcome::Settled(held) => (Some(held), false),
            StepOutcome::Stale => (None, false),
            StepOutcome::Failed(held) => (held, true),
        };
        if let Some(held) = held {
            self.held.insert(step.branch.clone(), held);
        }
        if !failed {
            return;
        }
        let acknowledge = step.acknowledge;
        let announced = OwnerCheckpoint {
            state: step.state,
            lsm: step.target,
        };
        let pending = self.waiting(step.branch);
        if acknowledge && pending.announced.is_none() {
            pending.announced = Some(announced);
        }
    }

    /// Whether this node may install a checkpoint of `branch` while `named` is the lifecycle it
    /// holds. A concrete branch has to be named; unbranched work is never evicted, so its state is
    /// installed whatever the lifecycle names.
    fn may_install(named: &NamedBranches, branch: Option<&BranchKey>) -> bool {
        match branch {
            Some(branch) => named.names(Some(branch)),
            None => true,
        }
    }

    /// The waiting record of `branch`, which waits from now on if it did not already.
    fn waiting(&mut self, branch: Option<BranchKey>) -> &mut PendingBranch {
        if !self.pending.contains_key(&branch) {
            self.pending
                .insert(branch.clone(), PendingBranch::default());
        }
        self.pending
            .get_mut(&branch)
            .verified("the branch was inserted just above when it did not wait yet")
    }
}

#[cfg(test)]
#[path = "replica_branch_checkpoints_tests.rs"]
mod tests;
