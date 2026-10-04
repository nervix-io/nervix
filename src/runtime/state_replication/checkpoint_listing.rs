//! Layer: data plane.
//! Owns: the owner's answer to a replica's catalog read, and its conversion to and from the
//! interconnect envelope that carries it.
//! May depend on: catalog listings, typed branch keys and the interconnect envelopes.
//! Must not know: how the catalog is kept, how a replica acts on a listing, or NSPL.

use nervix_interconnect::{
    BranchCheckpointListing, BranchCheckpointPage, BranchCheckpointRevision,
};

use super::*;
use crate::runtime::branch_checkpoint_catalog::{
    CatalogedCheckpoint, CheckpointListing, CheckpointPage,
};

/// What the owner of a branch-keyed entity answers a replica's catalog read with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) enum OwnerCheckpointListing {
    /// The owner holds no branch state of the entity.
    Absent,
    Listed(CheckpointListing),
}

/// Why a listing an owner sent cannot be read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(in crate::runtime) enum CheckpointListingError {
    #[error("a branch checkpoint listing names a branch key that does not decode")]
    BranchKey,
}

impl OwnerCheckpointListing {
    pub(in crate::runtime) fn to_remote(&self) -> BranchCheckpointListing {
        match self {
            Self::Absent => BranchCheckpointListing::Absent,
            Self::Listed(CheckpointListing::Restarted(page)) => {
                BranchCheckpointListing::Restarted(page.to_remote())
            }
            Self::Listed(CheckpointListing::Continued(page)) => {
                BranchCheckpointListing::Continued(page.to_remote())
            }
        }
    }

    pub(in crate::runtime) fn from_remote(
        listing: BranchCheckpointListing,
    ) -> error_stack::Result<Self, CheckpointListingError> {
        let listing = match listing {
            BranchCheckpointListing::Absent => return Ok(Self::Absent),
            BranchCheckpointListing::Restarted(page) => {
                CheckpointListing::Restarted(CheckpointPage::from_remote(page)?)
            }
            BranchCheckpointListing::Continued(page) => {
                CheckpointListing::Continued(CheckpointPage::from_remote(page)?)
            }
        };
        Ok(Self::Listed(listing))
    }
}

impl CheckpointPage {
    fn to_remote(&self) -> BranchCheckpointPage {
        let mut removed = Vec::with_capacity(self.removed.len());
        for branch in &self.removed {
            removed.push(BranchKey::to_remote_key(branch));
        }
        let mut revised = Vec::with_capacity(self.revised.len());
        for checkpoint in &self.revised {
            revised.push(BranchCheckpointRevision {
                branch_key: BranchKey::to_remote_key(&checkpoint.branch),
                state: checkpoint.state,
                lsm: checkpoint.lsm,
            });
        }
        BranchCheckpointPage {
            cursor: self.cursor,
            removed,
            revised,
            more: self.more,
        }
    }

    fn from_remote(
        page: BranchCheckpointPage,
    ) -> error_stack::Result<Self, CheckpointListingError> {
        let mut removed = Vec::with_capacity(page.removed.len());
        for branch in page.removed {
            let branch = BranchKey::from_remote_key(branch)
                .change_context(CheckpointListingError::BranchKey)?;
            removed.push(branch);
        }
        let mut revised = Vec::with_capacity(page.revised.len());
        for checkpoint in page.revised {
            let branch = BranchKey::from_remote_key(checkpoint.branch_key)
                .change_context(CheckpointListingError::BranchKey)?;
            revised.push(CatalogedCheckpoint {
                branch,
                state: checkpoint.state,
                lsm: checkpoint.lsm,
            });
        }
        Ok(Self {
            cursor: page.cursor,
            removed,
            revised,
            more: page.more,
        })
    }
}
