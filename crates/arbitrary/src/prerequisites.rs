//! The prerequisite record a node publishes in gossip and carries in its connection handshake, the
//! mismatch that names why two records cannot run together, and the verdicts a leader publishes on
//! the runs it judged.
//!
//! A record is compared for equality and for the bounded distance of its timings, never read for
//! meaning, so any text, digest, count and duration is as valid as a production one. A verdict
//! names any run, any voter and any mismatch, under any term.

use std::{collections::BTreeMap, time::Duration};

use meticulous::ResultExt as _;
use nervix_models::{
    AdmissionVerdict, AdmissionVerdicts, ConsensusFloors, NodePrerequisites, PrerequisiteMismatch,
    PrerequisiteTimings, RaftLogDurability, TimingPrerequisite, TrustRootFingerprint,
    WireContractFingerprint,
};

use crate::Arbitrary;

/// The most runs one leader's verdicts hold here, a few more than a cluster's live nodes in the
/// generated cases.
const MAX_JUDGED_RUNS: u64 = 6;

impl Arbitrary<'_> {
    /// A prerequisite record with any cluster identity, digests, consensus floors and timings.
    pub fn node_prerequisites(&mut self) -> NodePrerequisites {
        let cluster_identity = self.string();
        let trust_root = TrustRootFingerprint::from_digest(self.digest());
        let wire_contract = WireContractFingerprint::from_digest(self.digest());
        let consensus_floors = ConsensusFloors {
            largest_command_bytes: self.entropy.boundary_biased(0..=u64::MAX),
            largest_management_event_bytes: self.entropy.boundary_biased(0..=u64::MAX),
        };
        let timings = PrerequisiteTimings {
            raft_heartbeat_interval: self.any_duration(),
            gossip_interval: self.any_duration(),
            clock_skew_tolerance: self.any_duration(),
        };
        NodePrerequisites {
            cluster_identity,
            trust_root,
            wire_contract,
            consensus_floors,
            durability: RaftLogDurability::FsyncEveryAppend,
            timings,
        }
    }

    /// The prerequisite two records disagree on, of any kind, with any pair of its values.
    pub fn prerequisite_mismatch(&mut self) -> PrerequisiteMismatch {
        match self.entropy.byte() % 7 {
            0 => PrerequisiteMismatch::ClusterIdentity {
                local: self.string(),
                peer: self.string(),
            },
            1 => PrerequisiteMismatch::TrustRoot {
                local: TrustRootFingerprint::from_digest(self.digest()),
                peer: TrustRootFingerprint::from_digest(self.digest()),
            },
            2 => PrerequisiteMismatch::WireContract {
                local: WireContractFingerprint::from_digest(self.digest()),
                peer: WireContractFingerprint::from_digest(self.digest()),
            },
            3 => PrerequisiteMismatch::LargestCommand {
                local: self.entropy.boundary_biased(0..=u64::MAX),
                peer: self.entropy.boundary_biased(0..=u64::MAX),
            },
            4 => PrerequisiteMismatch::LargestManagementEvent {
                local: self.entropy.boundary_biased(0..=u64::MAX),
                peer: self.entropy.boundary_biased(0..=u64::MAX),
            },
            5 => PrerequisiteMismatch::Durability {
                local: RaftLogDurability::FsyncEveryAppend,
                peer: RaftLogDurability::FsyncEveryAppend,
            },
            _ => PrerequisiteMismatch::Timing {
                prerequisite: self.entropy.pick([
                    TimingPrerequisite::RaftHeartbeatInterval,
                    TimingPrerequisite::GossipInterval,
                    TimingPrerequisite::ClockSkewTolerance,
                ]),
                local: self.any_duration(),
                peer: self.any_duration(),
            },
        }
    }

    /// The verdicts one leader holds under any term: none to a handful of runs, each accepted or
    /// refused naming any voter and any mismatch.
    pub fn admission_verdicts(&mut self) -> AdmissionVerdicts {
        let leader_term = self.entropy.boundary_biased(0..=u64::MAX);
        let runs = self.entropy.boundary_biased(0..=MAX_JUDGED_RUNS);
        let mut verdicts = BTreeMap::new();
        for _ in 0..runs {
            let run = self.node_identity();
            let verdict = if self.entropy.flag() {
                AdmissionVerdict::Accepted
            } else {
                AdmissionVerdict::Refused {
                    voter: self.rule_name(),
                    mismatch: self.prerequisite_mismatch(),
                }
            };
            verdicts.insert(run, verdict);
        }
        AdmissionVerdicts::new(leader_term, verdicts)
    }

    /// Any duration: whole seconds across the full range, landing on its ends as often as
    /// elsewhere, and any nanosecond fraction below one second.
    fn any_duration(&mut self) -> Duration {
        let seconds = self.entropy.boundary_biased(0..=u64::MAX);
        let nanoseconds = self.entropy.between(0..=999_999_999);
        let nanoseconds = u32::try_from(nanoseconds)
            .assured("the range above stops below one second, which fits 32 bits");
        Duration::new(seconds, nanoseconds)
    }
}
