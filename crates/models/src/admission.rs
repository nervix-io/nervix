//! The leader's judgment of each live node's current run against the voters it must run beside.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The verdict one run receives, the verdicts one leader holds under the term it
//!   leads, and their gossip text.
//! - **Depends on.** Node identities, the prerequisite mismatch and serialization crates.
//! - **Must not know.** How the leader judges a run, how gossip carries the verdicts, or what a
//!   joining node does with its own.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::collections::{BTreeMap, btree_map::Entry};

use error_stack::Report;
use meticulous::ResultExt as _;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ClusterNodeIdentity, ClusterNodeName, PrerequisiteMismatch};

/// What the leader judged one live run to be beside the live voters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionVerdict {
    /// The run can run beside every live voter the leader accepted before it.
    Accepted,
    /// The run cannot run beside `voter`, a live voter the leader accepted before it. The mismatch
    /// is seen from that voter: `local` is the voter's value and `peer` the run's.
    Refused {
        voter: ClusterNodeName,
        mismatch: PrerequisiteMismatch,
    },
}

/// The verdicts one leader holds for the live runs it judged, with the Raft term it leads, so a
/// reader that sees the verdicts of two leaders follows the newer one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionVerdicts {
    leader_term: u64,
    verdicts: BTreeMap<ClusterNodeIdentity, AdmissionVerdict>,
}

impl AdmissionVerdicts {
    pub fn new(
        leader_term: u64,
        verdicts: BTreeMap<ClusterNodeIdentity, AdmissionVerdict>,
    ) -> Self {
        Self {
            leader_term,
            verdicts,
        }
    }

    /// The Raft term of the leader that judged these runs.
    pub const fn leader_term(&self) -> u64 {
        self.leader_term
    }

    /// The leader's verdict on `run`, or `None` while it has not judged that run.
    pub fn verdict_of(&self, run: &ClusterNodeIdentity) -> Option<&AdmissionVerdict> {
        self.verdicts.get(run)
    }

    /// Every run the leader judged, with its verdict.
    pub fn verdicts(&self) -> &BTreeMap<ClusterNodeIdentity, AdmissionVerdict> {
        &self.verdicts
    }

    /// The verdicts as their gossip value carries them.
    pub fn to_gossip_text(&self) -> String {
        let mut runs = Vec::with_capacity(self.verdicts.len());
        for (run, verdict) in &self.verdicts {
            runs.push(RunVerdictText {
                run: run.clone(),
                verdict: verdict.clone(),
            });
        }
        let text = VerdictsText {
            leader_term: self.leader_term,
            runs,
        };
        serde_json::to_string(&text).assured(
            "the verdicts hold only names, counts, strings, digests and durations JSON can write",
        )
    }

    /// Reads the verdicts from their gossip value.
    pub fn from_gossip_text(text: &str) -> Result<Self, Report<AdmissionVerdictsTextError>> {
        let read: VerdictsText = serde_json::from_str(text).map_err(|error| {
            Report::new(error).change_context(AdmissionVerdictsTextError::Malformed)
        })?;
        let mut verdicts = BTreeMap::new();
        for entry in read.runs {
            match verdicts.entry(entry.run) {
                Entry::Vacant(vacant) => {
                    vacant.insert(entry.verdict);
                }
                Entry::Occupied(occupied) => {
                    return Err(Report::new(AdmissionVerdictsTextError::RepeatedRun {
                        run: occupied.key().clone(),
                    }));
                }
            }
        }
        Ok(Self {
            leader_term: read.leader_term,
            verdicts,
        })
    }
}

/// Why a gossip value holds no admission verdicts.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AdmissionVerdictsTextError {
    #[error("a gossip value holds no admission verdicts")]
    Malformed,
    #[error("a gossip value judges run '{run}' more than once")]
    RepeatedRun { run: ClusterNodeIdentity },
}

/// The gossip value of [`AdmissionVerdicts`]: JSON holds a map only under text keys, so each run
/// and its verdict are one entry of a list, in run order.
#[derive(Serialize, Deserialize)]
struct VerdictsText {
    leader_term: u64,
    runs: Vec<RunVerdictText>,
}

#[derive(Serialize, Deserialize)]
struct RunVerdictText {
    run: ClusterNodeIdentity,
    verdict: AdmissionVerdict,
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
