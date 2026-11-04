//! Coherent gossip observations and the catch-up check for a construction policy.
//!
//! Layer: vocabulary.
//! - **Owns.** Revision ordering and the validity of committed/running publication pairs.
//! - **Depends on.** Current parameter records and serialization.
//! - **Must not know.** Gossip transport, Raft or runtime installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns parameter publication invariants")
)]

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize, de::Error as _};
use thiserror::Error;

use super::ClusterParameterRecord;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "publication validation returns typed revision contradictions"
    )
)]
pub enum ClusterParameterPublicationError {
    #[error("parameter revision {revision} names different values")]
    RevisionMismatch { revision: NonZeroU64 },
    #[error("running parameter revision {running} is ahead of committed revision {committed}")]
    RunningAhead {
        committed: NonZeroU64,
        running: NonZeroU64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterParameterCatchUp {
    Pending,
    Ready,
}

impl ClusterParameterRecord {
    /// Compare a caught-up log with the live publication used at construction.
    pub fn catch_up(
        &self,
        applied: Self,
    ) -> Result<ClusterParameterCatchUp, ClusterParameterPublicationError> {
        if applied.revision < self.revision {
            return Ok(ClusterParameterCatchUp::Pending);
        }
        if applied.revision == self.revision && applied.values != self.values {
            return Err(ClusterParameterPublicationError::RevisionMismatch {
                revision: self.revision,
            });
        }
        Ok(ClusterParameterCatchUp::Ready)
    }
}

/// One node's coherent publication. Running state is absent until its first installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ClusterParameterPublication {
    committed: ClusterParameterRecord,
    running: Option<ClusterParameterRecord>,
}

impl ClusterParameterPublication {
    pub fn new(committed: ClusterParameterRecord) -> Self {
        Self {
            committed,
            running: None,
        }
    }
    pub fn committed(self) -> ClusterParameterRecord {
        self.committed
    }
    pub fn running(self) -> Option<ClusterParameterRecord> {
        self.running
    }

    pub fn observe(
        mut self,
        committed: ClusterParameterRecord,
    ) -> Result<Self, ClusterParameterPublicationError> {
        if committed.revision < self.committed.revision {
            return Ok(self);
        }
        self.committed.catch_up(committed)?;
        self.committed = committed;
        Ok(self)
    }

    pub fn install(
        mut self,
        running: ClusterParameterRecord,
    ) -> Result<Self, ClusterParameterPublicationError> {
        self = self.observe(running)?;
        if let Some(previous) = self.running {
            if running.revision < previous.revision {
                return Ok(self);
            }
            previous.catch_up(running)?;
        }
        self.running = Some(running);
        Ok(self)
    }
}

impl<'de> Deserialize<'de> for ClusterParameterPublication {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            committed: ClusterParameterRecord,
            running: Option<ClusterParameterRecord>,
        }
        let Fields { committed, running } = Fields::deserialize(deserializer)?;
        if let Some(running) = running {
            if running.revision > committed.revision {
                return Err(D::Error::custom(
                    ClusterParameterPublicationError::RunningAhead {
                        committed: committed.revision,
                        running: running.revision,
                    },
                ));
            }
            if running.revision == committed.revision && running.values != committed.values {
                return Err(D::Error::custom(
                    ClusterParameterPublicationError::RevisionMismatch {
                        revision: committed.revision,
                    },
                ));
            }
        }
        Ok(Self { committed, running })
    }
}
