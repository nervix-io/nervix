//! The convergence a real-process cluster's members report before a scenario sends it commands.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Reading the leader, the voters and the application-health target of every peer that
//!   each member's public cluster status reports, the targets the running members observed an
//!   exited member's process through before it is relaunched, deciding whether the members have
//!   converged, and waiting for that within one budget after a start or after one member is
//!   relaunched.
//! - **Depends on.** Bounded status requests and the phase deadline.
//! - **Must not know.** How a member is launched, signalled or restarted, or how a server decides
//!   leadership and availability.
//!
//! The members have converged when every member reports the same leader, lists every member as a
//! voter, and reports that its latest application-health probe of every other member reached that
//! member's application. Agreement on Raft alone does not make a relaunched member usable: its
//! killed process never left the voter configuration, so the voters agree as soon as the new
//! process follows the leader, while every peer still judges the member by what it learned of the
//! killed process. Placement, failover and `RELOCATE` read that availability. A peer therefore
//! converges on a relaunched member only through a target newer than the one it observed the
//! killed process through: a peer begins a target only for a process it has discovered, and it
//! never discovers an exited process again.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    io,
    str::FromStr as _,
    time::Duration,
};

use error_stack::Report;
use thiserror::Error;

use super::{
    phase_deadline::PhaseDeadline,
    status_request::{STATUS_REQUEST_TIMEOUT, StatusEndpoint, StatusRequestError},
};

/// How long a wait pauses before it asks every member again.
const CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CURRENT_LEADER_PREFIX: &str = "raft.current_leader: ";
const NO_LEADER: &str = "(none)";
const MEMBERSHIP_HEADER: &str = "raft.membership:";
const INTERCONNECT_HEADER: &str = "[interconnect]";
const NO_TARGETS: &str = "(none)";
const LIST_ITEM_PREFIX: &str = "- ";
const VOTER_ROLE: &str = "voter";
const GENERATION_FIELD: &str = "generation";
const STATUS_FIELD: &str = "status";

/// What a member's latest application-health probe of a peer concluded, as the member's
/// `[interconnect]` section names it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::Display, strum::EnumString)]
#[strum(serialize_all = "kebab-case")]
pub(crate) enum PeerReachability {
    /// The latest probe reached the target's application and is still fresh.
    Connected,
    /// Probes have failed for less than the node-unavailability interval.
    ProbeFailed,
    /// Probes have failed for the whole node-unavailability interval.
    Unavailable,
    /// No fresh probe of the target has concluded.
    Unknown,
}

/// The application-health target through which one member observes a peer's process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PeerTarget {
    /// The generation the observing member issued when it began this target. Every target it
    /// begins later receives a higher one, and a target observes one process for as long as it
    /// exists.
    generation: u64,
    reachability: PeerReachability,
}

/// What one member's cluster status reports that convergence reads.
#[derive(Debug)]
struct MemberStatus {
    /// The member the status names as Raft leader, absent while it knows none.
    leader: Option<String>,
    voters: BTreeSet<String>,
    /// The target through which this member observes each peer, by the peer's node name.
    targets: BTreeMap<String, PeerTarget>,
}

/// The part of a member's cluster status a line belongs to.
#[derive(Clone, Copy)]
enum StatusSection {
    Membership,
    Interconnect,
    Other,
}

impl MemberStatus {
    fn read(member: &str, status: &str) -> Result<Self, Report<ConvergenceError>> {
        let mut read = Self {
            leader: None,
            voters: BTreeSet::new(),
            targets: BTreeMap::new(),
        };
        let mut section = StatusSection::Other;
        for line in status.lines() {
            if let Some(leader) = line.strip_prefix(CURRENT_LEADER_PREFIX)
                && leader != NO_LEADER
            {
                read.leader = Some(leader.to_string());
            }
            if line == MEMBERSHIP_HEADER {
                section = StatusSection::Membership;
                continue;
            }
            if line == INTERCONNECT_HEADER {
                section = StatusSection::Interconnect;
                continue;
            }
            let Some(item) = line.strip_prefix(LIST_ITEM_PREFIX) else {
                section = StatusSection::Other;
                continue;
            };
            match section {
                StatusSection::Membership => read.read_membership(member, line, item)?,
                StatusSection::Interconnect => read.read_target(member, line, item)?,
                StatusSection::Other => {}
            }
        }
        Ok(read)
    }

    /// Reads one `raft.membership` item: the node, its role in brackets, and its address.
    fn read_membership(
        &mut self,
        member: &str,
        line: &str,
        item: &str,
    ) -> Result<(), Report<ConvergenceError>> {
        let Some((node, rest)) = item.split_once(" [") else {
            return Err(unreadable(member, line));
        };
        let Some((role, _address)) = rest.split_once(']') else {
            return Err(unreadable(member, line));
        };
        if role == VOTER_ROLE {
            self.voters.insert(node.to_string());
        }
        Ok(())
    }

    /// Reads one `[interconnect]` item: the peer, then the fields of the target that observes it.
    fn read_target(
        &mut self,
        member: &str,
        line: &str,
        item: &str,
    ) -> Result<(), Report<ConvergenceError>> {
        if item == NO_TARGETS {
            return Ok(());
        }
        let Some((peer, fields)) = item.split_once(": ") else {
            return Err(unreadable(member, line));
        };
        let mut generation = None;
        let mut reachability = None;
        for field in fields.split(' ') {
            let Some((name, value)) = field.split_once('=') else {
                return Err(unreadable(member, line));
            };
            if name == GENERATION_FIELD {
                let Ok(read) = value.parse::<u64>() else {
                    return Err(unreadable(member, line));
                };
                generation = Some(read);
            }
            if name == STATUS_FIELD {
                let Ok(read) = PeerReachability::from_str(value) else {
                    return Err(unreadable(member, line));
                };
                reachability = Some(read);
            }
        }
        let (Some(generation), Some(reachability)) = (generation, reachability) else {
            return Err(unreadable(member, line));
        };
        self.targets.insert(
            peer.to_string(),
            PeerTarget {
                generation,
                reachability,
            },
        );
        Ok(())
    }

    /// The leader this member names once its status meets every condition of convergence but
    /// agreement with the other members, or the first condition it does not meet.
    fn converged_leader(
        &self,
        member: &str,
        members: &BTreeSet<String>,
        relaunched: Option<&RelaunchedMember>,
    ) -> Result<&str, Divergence> {
        let Some(leader) = &self.leader else {
            return Err(Divergence::NoLeader {
                member: member.to_string(),
            });
        };
        if !members.contains(leader) {
            return Err(Divergence::LeaderOutsideCluster {
                member: member.to_string(),
                leader: leader.clone(),
            });
        }
        for voter in members {
            if !self.voters.contains(voter) {
                return Err(Divergence::VoterMissing {
                    member: member.to_string(),
                    voter: voter.clone(),
                });
            }
        }
        for peer in members {
            if peer == member {
                continue;
            }
            let Some(target) = self.targets.get(peer) else {
                return Err(Divergence::PeerUnobserved {
                    member: member.to_string(),
                    peer: peer.clone(),
                });
            };
            if let Some(relaunched) = relaunched
                && relaunched.observes_exited_process(member, peer, target)
            {
                return Err(Divergence::ExitedProcessObserved {
                    member: member.to_string(),
                    peer: peer.clone(),
                    generation: target.generation,
                });
            }
            if target.reachability != PeerReachability::Connected {
                return Err(Divergence::PeerUnreached {
                    member: member.to_string(),
                    peer: peer.clone(),
                    reachability: target.reachability,
                });
            }
        }
        Ok(leader)
    }
}

fn unreadable(member: &str, line: &str) -> Report<ConvergenceError> {
    Report::new(ConvergenceError::UnreadableStatus {
        member: member.to_string(),
        line: line.to_string(),
    })
}

/// A member relaunched while the others kept running, with the target each of them observed its
/// exited process through.
#[derive(Debug)]
struct RelaunchedMember {
    member: String,
    /// The generation of the target each running member observed the exited process through. A
    /// member that had stopped observing it holds none, so any target it reports later observes
    /// the new process.
    exited_targets: BTreeMap<String, u64>,
}

impl RelaunchedMember {
    /// Whether `observer` still observes `peer`, when it is the relaunched member, through the
    /// target it observed the exited process through, or an earlier one.
    fn observes_exited_process(&self, observer: &str, peer: &str, target: &PeerTarget) -> bool {
        if peer != self.member {
            return false;
        }
        let Some(exited) = self.exited_targets.get(observer) else {
            return false;
        };
        target.generation <= *exited
    }
}

/// The first condition the members do not meet yet, in the order they are asked.
#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum Divergence {
    #[error("member '{member}' did not answer its status request")]
    StatusUnanswered { member: String },
    #[error("member '{member}' names no Raft leader")]
    NoLeader { member: String },
    #[error("member '{member}' names '{leader}', which is not a member, as Raft leader")]
    LeaderOutsideCluster { member: String, leader: String },
    #[error(
        "member '{member}' names '{leader}' as Raft leader, member '{other}' names \
         '{other_leader}'"
    )]
    LeadersDiffer {
        member: String,
        leader: String,
        other: String,
        other_leader: String,
    },
    #[error("member '{member}' does not list '{voter}' as a voter")]
    VoterMissing { member: String, voter: String },
    #[error("member '{member}' observes no application-health target for '{peer}'")]
    PeerUnobserved { member: String, peer: String },
    #[error(
        "member '{member}' still observes relaunched '{peer}' through generation {generation}, \
         the target of its exited process"
    )]
    ExitedProcessObserved {
        member: String,
        peer: String,
        generation: u64,
    },
    #[error("member '{member}' reports '{peer}' {reachability}, not connected")]
    PeerUnreached {
        member: String,
        peer: String,
        reachability: PeerReachability,
    },
}

/// What a wait last heard from one member.
#[derive(Debug, Default)]
struct MemberObservations {
    /// The latest status the member answered with.
    status: Option<String>,
    /// How the latest request to the member failed, when it failed after that status.
    failure: Option<Report<StatusRequestError>>,
}

/// What a wait last heard from each member, for its failure.
#[derive(Debug, Default)]
pub(crate) struct LastObservations(BTreeMap<String, MemberObservations>);

impl LastObservations {
    fn answered(&mut self, member: &str, status: String) {
        let observations = self.0.entry(member.to_string()).or_default();
        observations.status = Some(status);
        observations.failure = None;
    }

    /// Records a failed request beside the member's latest status rather than in its place, so the
    /// last request of a wait, which its own deadline may cut short, hides nothing the member said.
    fn failed(&mut self, member: &str, failure: Report<StatusRequestError>) {
        self.0.entry(member.to_string()).or_default().failure = Some(failure);
    }
}

impl fmt::Display for LastObservations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (member, observations) in &self.0 {
            if let Some(status) = &observations.status {
                write!(formatter, "\n- {member} last reported {status:?}")?;
            }
            if let Some(failure) = &observations.failure {
                write!(
                    formatter,
                    "\n- {member} did not answer its latest request: {failure:#}"
                )?;
            }
        }
        Ok(())
    }
}

/// Why a real-process cluster did not converge.
#[derive(Debug, Error)]
pub(crate) enum ConvergenceError {
    #[error("member '{member}' reported a status line convergence cannot read: {line:?}")]
    UnreadableStatus { member: String, line: String },
    #[error(
        "the running members did not report the targets they observe '{relaunched}' through \
         within {}; last observations:{last}",
        humantime::format_duration(*.budget)
    )]
    ExitedTargetsUnread {
        relaunched: String,
        budget: Duration,
        last: LastObservations,
    },
    #[error("relaunching member '{member}' failed")]
    Relaunch { member: String },
    #[error(
        "the process cluster did not converge within {}: {divergence}; last observations:{last}",
        humantime::format_duration(*.budget)
    )]
    NotConverged {
        budget: Duration,
        divergence: Divergence,
        last: LastObservations,
    },
}

/// The members of one real-process cluster, by node name, and how long each phase of their
/// convergence may take.
pub(crate) struct ClusterConvergence {
    members: BTreeMap<String, StatusEndpoint>,
    budget: Duration,
}

impl ClusterConvergence {
    pub(crate) fn new(members: BTreeMap<String, StatusEndpoint>, budget: Duration) -> Self {
        Self { members, budget }
    }

    /// Relaunches `member`, whose previous process has exited, through `relaunch`, then waits
    /// until every member has converged on the new process. First every other member reports
    /// the target it observes the exited process through, so that afterwards it converges only
    /// through a newer one.
    pub(crate) async fn relaunch<F>(
        &self,
        member: &str,
        relaunch: F,
    ) -> Result<(), Report<ConvergenceError>>
    where
        F: Future<Output = io::Result<()>>,
    {
        let relaunched = self.exited_targets(member).await?;
        relaunch.await.map_err(|failure| {
            Report::new(failure).change_context(ConvergenceError::Relaunch {
                member: member.to_string(),
            })
        })?;
        self.converge(Some(&relaunched)).await
    }

    /// Waits until every member of a cluster whose processes all started together has converged.
    pub(crate) async fn wait(&self) -> Result<(), Report<ConvergenceError>> {
        self.converge(None).await
    }

    /// Asks every member but `relaunched` for its status, again for a member that has not
    /// answered, until each reported the target it observes `relaunched` through, if any.
    async fn exited_targets(
        &self,
        relaunched: &str,
    ) -> Result<RelaunchedMember, Report<ConvergenceError>> {
        let deadline = PhaseDeadline::after(self.budget);
        let mut exited_targets = BTreeMap::new();
        let mut unread = BTreeSet::new();
        for member in self.members.keys() {
            if member != relaunched {
                unread.insert(member.clone());
            }
        }
        let mut last = LastObservations::default();
        loop {
            nervix_primitives::task::consume_budget().await;
            for (member, endpoint) in &self.members {
                nervix_primitives::task::consume_budget().await;
                if !unread.contains(member) {
                    continue;
                }
                let answered = endpoint
                    .cluster_status(deadline.nested(STATUS_REQUEST_TIMEOUT))
                    .await;
                let status = match answered {
                    Ok(status) => status,
                    Err(failure) => {
                        last.failed(member, failure);
                        continue;
                    }
                };
                let read = MemberStatus::read(member, &status)?;
                if let Some(target) = read.targets.get(relaunched) {
                    exited_targets.insert(member.clone(), target.generation);
                }
                unread.remove(member);
                last.answered(member, status);
            }
            if unread.is_empty() {
                return Ok(RelaunchedMember {
                    member: relaunched.to_string(),
                    exited_targets,
                });
            }
            if deadline.has_passed() {
                return Err(Report::new(ConvergenceError::ExitedTargetsUnread {
                    relaunched: relaunched.to_string(),
                    budget: self.budget,
                    last,
                }));
            }
            deadline.pause(CONVERGENCE_POLL_INTERVAL).await;
        }
    }

    /// Asks every member for its status until all of them have converged or the budget passes.
    /// Every member must answer the round that converges. When the budget passes, the wait names
    /// the first condition the members' latest answers did not meet in the last round that ended
    /// before it: a round that runs into the budget can have its last requests cut short by the
    /// budget itself, which says nothing about the members.
    async fn converge(
        &self,
        relaunched: Option<&RelaunchedMember>,
    ) -> Result<(), Report<ConvergenceError>> {
        let deadline = PhaseDeadline::after(self.budget);
        let members = self.members.keys().cloned().collect::<BTreeSet<_>>();
        let mut latest = BTreeMap::new();
        let mut last = LastObservations::default();
        let mut ended_in_budget = None;
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut unanswered = None;
            for (member, endpoint) in &self.members {
                nervix_primitives::task::consume_budget().await;
                let answered = endpoint
                    .cluster_status(deadline.nested(STATUS_REQUEST_TIMEOUT))
                    .await;
                match answered {
                    Ok(status) => {
                        latest.insert(member.clone(), MemberStatus::read(member, &status)?);
                        last.answered(member, status);
                    }
                    Err(failure) => {
                        last.failed(member, failure);
                        if unanswered.is_none() {
                            unanswered = Some(member.clone());
                        }
                    }
                }
            }
            let divergence = Self::divergence(&members, &latest, relaunched);
            let divergence = match (divergence, unanswered) {
                (None, None) => return Ok(()),
                (Some(divergence), _) => divergence,
                (None, Some(member)) => Divergence::StatusUnanswered { member },
            };
            if deadline.has_passed() {
                let divergence = match ended_in_budget {
                    Some(earlier) => earlier,
                    None => divergence,
                };
                return Err(Report::new(ConvergenceError::NotConverged {
                    budget: self.budget,
                    divergence,
                    last,
                }));
            }
            ended_in_budget = Some(divergence);
            deadline.pause(CONVERGENCE_POLL_INTERVAL).await;
        }
    }

    /// The first condition the latest status of each member, in node order, does not meet, or
    /// none once every member has converged on the same leader.
    fn divergence(
        members: &BTreeSet<String>,
        latest: &BTreeMap<String, MemberStatus>,
        relaunched: Option<&RelaunchedMember>,
    ) -> Option<Divergence> {
        /// The leader the first converged member named.
        struct NamedLeader<'status> {
            member: &'status str,
            leader: &'status str,
        }

        let mut first_leader: Option<NamedLeader<'_>> = None;
        for member in members {
            let Some(status) = latest.get(member) else {
                return Some(Divergence::StatusUnanswered {
                    member: member.clone(),
                });
            };
            let leader = match status.converged_leader(member, members, relaunched) {
                Ok(leader) => leader,
                Err(divergence) => return Some(divergence),
            };
            match &first_leader {
                Some(first) if first.leader != leader => {
                    return Some(Divergence::LeadersDiffer {
                        member: member.clone(),
                        leader: leader.to_string(),
                        other: first.member.to_string(),
                        other_leader: first.leader.to_string(),
                    });
                }
                Some(_) => {}
                None => first_leader = Some(NamedLeader { member, leader }),
            }
        }
        None
    }
}
