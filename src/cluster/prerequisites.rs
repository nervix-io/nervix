//! The prerequisite records nodes publish in gossip, the leader's verdicts on the runs it judged,
//! and the clock probe a joining node bounds its skew to a peer with.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading a node's prerequisite record from its gossip state, the view of every live
//!   node's record that readers share and the membership task replaces, republishing this node's
//!   own record, publishing the verdicts this node holds while it leads and reading the newest
//!   leader's verdict on one run, and the management request that reads a peer's physical clock
//!   with the bound on skew one answer proves.
//! - **Depends on.** The parent module's gossip state and wall-clock read, the vocabulary's
//!   prerequisite record and admission verdicts, and authenticated interconnect requests.
//! - **Must not know.** How two records or a run are judged, how a node is admitted, or what a
//!   joining node does with a skew or a verdict.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{
    collections::BTreeMap,
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chitchat::{ChitchatId, NodeState};
use error_stack::Report;
use nervix_interconnect::{
    HandlerRegistrationError, InterconnectRequest, PoolClass, RequestSubquota,
    Transport as InterconnectTransport,
};
use nervix_models::{
    AdmissionVerdict, AdmissionVerdicts, ClusterNodeIdentity, ClusterNodeIncarnation,
    ClusterNodeName, NodePrerequisites, TimingPrerequisite,
};
use nervix_primitives::{
    publication::{ArcSwap, Guard},
    sync::{Arc, StdArc},
};
use rkyv::{Archive, Deserialize, Serialize};
use tracing::{debug, warn};

use super::{ClusterHandle, GossipExchange, cluster_node_identity};

/// Everything the node brings to the cluster, as its prerequisite record's gossip text.
const KEY_PREREQUISITES: &str = "prerequisites";
/// The verdicts a leader holds on the live runs it judged, as their gossip text.
const KEY_ADMISSION_VERDICTS: &str = "admission_verdicts";

/// The gossip key and value that publish `prerequisites` from a node's first gossip round.
pub(super) fn initial_key_value(prerequisites: &NodePrerequisites) -> (String, String) {
    (
        KEY_PREREQUISITES.to_string(),
        prerequisites.to_gossip_text(),
    )
}

/// Every live node's published prerequisite record, as this node's gossip last saw it.
///
/// The membership task replaces it whenever the live nodes or their records change, so a reader on
/// a scrape or a statement takes one consistent view without waiting on gossip.
#[derive(Clone)]
pub struct PrerequisitesView {
    records: Arc<ArcSwap<BTreeMap<ClusterNodeName, NodePrerequisites>>>,
}

impl PrerequisitesView {
    pub(super) fn new() -> Self {
        Self {
            records: Arc::new(ArcSwap::from_pointee(BTreeMap::new())),
        }
    }

    /// Publishes the record of each live node's newest run, leaving out a node that has not
    /// published one this build reads.
    pub(super) fn publish(&self, nodes: &BTreeMap<ChitchatId, NodeState>) {
        let mut newest = BTreeMap::<ClusterNodeName, NewestRecord>::new();
        for (node, state) in nodes {
            let Some(identity) = cluster_node_identity(node) else {
                continue;
            };
            let Some(record) = advertised_prerequisites(&identity, state) else {
                continue;
            };
            let replaces = match newest.get(identity.node_id()) {
                Some(known) => known.incarnation < identity.incarnation(),
                None => true,
            };
            if replaces {
                newest.insert(
                    identity.node_id().clone(),
                    NewestRecord {
                        incarnation: identity.incarnation(),
                        record,
                    },
                );
            }
        }
        let mut records = BTreeMap::new();
        for (node, newest) in newest {
            records.insert(node, newest.record);
        }
        self.records.store(StdArc::new(records));
    }

    /// The records as last published.
    pub fn load(&self) -> Guard<StdArc<BTreeMap<ClusterNodeName, NodePrerequisites>>> {
        self.records.load()
    }

    /// The nodes whose timing prerequisites differ from `leader`'s, each with the timings that
    /// differ, or `None` while the leader's record is unknown and nothing can be compared to it.
    pub fn timing_drift(
        &self,
        leader: &ClusterNodeName,
    ) -> Option<BTreeMap<ClusterNodeName, Vec<TimingPrerequisite>>> {
        let records = self.load();
        let leader_record = records.get(leader)?;
        let mut drift = BTreeMap::new();
        for (node, record) in records.iter() {
            if node == leader {
                continue;
            }
            let differing = record.timings.differing(&leader_record.timings);
            if !differing.is_empty() {
                drift.insert(node.clone(), differing);
            }
        }
        Some(drift)
    }
}

/// The record of a node's newest run seen so far, while one publication picks it.
struct NewestRecord {
    incarnation: ClusterNodeIncarnation,
    record: NodePrerequisites,
}

/// The prerequisite record `state` advertises, or `None` while it is unavailable.
///
/// A record this build cannot read stays unavailable, so its node is never admitted, until a later
/// gossip round replaces it.
pub(super) fn advertised_prerequisites(
    identity: &ClusterNodeIdentity,
    state: &NodeState,
) -> Option<NodePrerequisites> {
    let published = state.get(KEY_PREREQUISITES)?;
    match NodePrerequisites::from_gossip_text(published) {
        Ok(record) => Some(record),
        Err(error) => {
            debug!(
                node = %identity,
                error = %format!("{error:#}"),
                "a node's gossip record holds no prerequisite record this build reads"
            );
            None
        }
    }
}

/// Asks a peer for its physical clock, so a joining node can bound how far apart the two are.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct PeerClockProbe;

/// The answering node's physical clock when it answered.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
enum PeerClock {
    /// Its clock read this long after the Unix epoch.
    SinceUnixEpoch(Duration),
    /// Its clock read before the Unix epoch.
    BeforeUnixEpoch,
}

impl InterconnectRequest for PeerClockProbe {
    type Response = PeerClock;

    const NAME: &'static str = "peer_clock";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Discovery;
    // A probe is answered from the node's clock alone, as a gossip exchange is from its memory, so
    // it takes the same discovery deadline.
    const TIMEOUT: Duration = <GossipExchange as InterconnectRequest>::TIMEOUT;
    const REQUIRES_LIVE_TARGET: bool = false;
}

/// How far a peer's physical clock is from this node's, as one probe can tell.
///
/// The peer read its clock somewhere between this node sending the probe and receiving the answer,
/// so only a reading outside that window shows skew, and the bound is the distance to its nearer
/// end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockSkew {
    /// The peer's reading fell inside the probe's window: no skew this probe can tell.
    WithinProbe,
    /// The peer's clock is ahead of this node's by at least this much.
    PeerAhead { at_least: Duration },
    /// The peer's clock is behind this node's by at least this much.
    PeerBehind { at_least: Duration },
}

impl ClockSkew {
    /// The skew one probe proves, whichever way it points.
    pub fn at_least(self) -> Duration {
        match self {
            Self::WithinProbe => Duration::ZERO,
            Self::PeerAhead { at_least } | Self::PeerBehind { at_least } => at_least,
        }
    }

    /// Bounds the skew from this node's clock before sending a probe and after receiving its
    /// answer, both as time since the Unix epoch, and the peer's answer.
    fn bound(sent: Duration, peer: PeerClock, received: Duration) -> Self {
        let peer = match peer {
            PeerClock::SinceUnixEpoch(peer) => peer,
            // A clock before the epoch is behind this one by more than all the time since it.
            PeerClock::BeforeUnixEpoch => return Self::PeerBehind { at_least: sent },
        };
        if let Some(at_least) = sent.checked_sub(peer)
            && !at_least.is_zero()
        {
            return Self::PeerBehind { at_least };
        }
        if let Some(at_least) = peer.checked_sub(received)
            && !at_least.is_zero()
        {
            return Self::PeerAhead { at_least };
        }
        Self::WithinProbe
    }
}

impl fmt::Display for ClockSkew {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithinProbe => formatter.write_str("no skew the probe can tell"),
            Self::PeerAhead { at_least } => {
                write!(formatter, "the peer is at least {at_least:?} ahead")
            }
            Self::PeerBehind { at_least } => {
                write!(formatter, "the peer is at least {at_least:?} behind")
            }
        }
    }
}

/// Why a peer's clock could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClockProbeError {
    #[error("this node's physical clock reads before the Unix epoch")]
    LocalClockBeforeEpoch,
    #[error("the peer did not answer the clock probe")]
    Unanswered,
}

/// This node's physical clock as time since the Unix epoch.
fn time_since_unix_epoch() -> Result<Duration, Report<ClockProbeError>> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Report::new(error).change_context(ClockProbeError::LocalClockBeforeEpoch))
}

/// Answers every peer's clock probe with this node's physical clock.
pub fn register_clock_probe_handler(
    interconnect: &InterconnectTransport,
) -> Result<(), Report<HandlerRegistrationError>> {
    interconnect.register_handler::<PeerClockProbe, _, _>(|_context, _probe| async {
        match time_since_unix_epoch() {
            Ok(since_unix_epoch) => PeerClock::SinceUnixEpoch(since_unix_epoch),
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "answered a clock probe with a clock that reads before the Unix epoch"
                );
                PeerClock::BeforeUnixEpoch
            }
        }
    })
}

impl ClusterHandle {
    /// Every live node's published prerequisite record, as this node's gossip last saw it.
    pub fn prerequisites_view(&self) -> PrerequisitesView {
        self.prerequisites.clone()
    }

    /// Publishes the record of what this node brings to the cluster, replacing the one its peers
    /// hold, such as after a credential reload changed its trust root.
    pub async fn publish_local_prerequisites(&self, prerequisites: &NodePrerequisites) {
        let mut chitchat = self.chitchat.lock().await;
        chitchat
            .self_node_state()
            .set(KEY_PREREQUISITES, prerequisites.to_gossip_text());
    }

    /// Publishes the verdicts this node holds while it leads, or withdraws the ones it published
    /// once it no longer leads.
    pub async fn publish_admission_verdicts(&self, verdicts: Option<&AdmissionVerdicts>) {
        let mut chitchat = self.chitchat.lock().await;
        let state = chitchat.self_node_state();
        match verdicts {
            Some(verdicts) => state.set(KEY_ADMISSION_VERDICTS, verdicts.to_gossip_text()),
            None => {
                if state.get(KEY_ADMISSION_VERDICTS).is_some() {
                    state.delete(KEY_ADMISSION_VERDICTS);
                }
            }
        }
    }

    /// The verdict on `run` of the newest leader whose verdicts this node's gossip holds, or
    /// `None` while that leader has not judged the run. A node that stopped leading withdraws its
    /// verdicts; until gossip carries that, the newer leader's win by their term.
    pub async fn admission_verdict_of(
        &self,
        run: &ClusterNodeIdentity,
    ) -> Option<AdmissionVerdict> {
        let chitchat = self.chitchat.lock().await;
        let mut newest: Option<AdmissionVerdicts> = None;
        for node in chitchat.live_nodes() {
            let Some(state) = chitchat.node_state(node) else {
                continue;
            };
            let Some(text) = state.get(KEY_ADMISSION_VERDICTS) else {
                continue;
            };
            let verdicts = match AdmissionVerdicts::from_gossip_text(text) {
                Ok(verdicts) => verdicts,
                Err(error) => {
                    debug!(
                        node = %node.node_id,
                        error = %format!("{error:#}"),
                        "a node's gossip state holds admission verdicts this build does not read"
                    );
                    continue;
                }
            };
            let newer = match &newest {
                Some(current) => current.leader_term() < verdicts.leader_term(),
                None => true,
            };
            if newer {
                newest = Some(verdicts);
            }
        }
        let newest = newest?;
        newest.verdict_of(run).cloned()
    }

    /// Bounds how far `peer`'s physical clock is from this node's with one probe over the
    /// management pool.
    pub async fn probe_clock_skew(
        &self,
        peer: &ClusterNodeName,
    ) -> Result<ClockSkew, Report<ClockProbeError>> {
        let sent = time_since_unix_epoch()?;
        let answer = self
            .gossip_transport
            .inner
            .interconnect
            .request(peer, PeerClockProbe)
            .await
            .map_err(|error| error.change_context(ClockProbeError::Unanswered))?;
        let received = time_since_unix_epoch()?;
        Ok(ClockSkew::bound(sent, answer, received))
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_arbitrary::{Arbitrary, Domain};

    use super::*;

    const SENT: Duration = Duration::from_secs(1_000);
    const RECEIVED: Duration = Duration::from_millis(1_000_200);

    #[test]
    fn a_reading_inside_the_probe_window_proves_no_skew() {
        for peer in [SENT, Duration::from_millis(1_000_100), RECEIVED] {
            let skew = ClockSkew::bound(SENT, PeerClock::SinceUnixEpoch(peer), RECEIVED);
            assert_eq!(skew, ClockSkew::WithinProbe);
            assert_eq!(skew.at_least(), Duration::ZERO);
        }
    }

    #[test]
    fn a_reading_outside_the_window_proves_the_distance_to_its_nearer_end() {
        let behind = ClockSkew::bound(
            SENT,
            PeerClock::SinceUnixEpoch(Duration::from_secs(997)),
            RECEIVED,
        );
        assert_eq!(
            behind,
            ClockSkew::PeerBehind {
                at_least: Duration::from_secs(3),
            }
        );
        let ahead = ClockSkew::bound(
            SENT,
            PeerClock::SinceUnixEpoch(Duration::from_millis(1_002_200)),
            RECEIVED,
        );
        assert_eq!(
            ahead,
            ClockSkew::PeerAhead {
                at_least: Duration::from_secs(2),
            }
        );
        assert_eq!(ahead.at_least(), Duration::from_secs(2));
        assert_eq!(ahead.to_string(), "the peer is at least 2s ahead");
    }

    #[test]
    fn a_clock_before_the_epoch_is_behind_by_all_the_time_since_it() {
        let skew = ClockSkew::bound(SENT, PeerClock::BeforeUnixEpoch, RECEIVED);
        assert_eq!(skew, ClockSkew::PeerBehind { at_least: SENT });
        assert_eq!(skew.to_string(), "the peer is at least 1000s behind");
    }

    #[test]
    fn bolero_peer_clock_probes_and_answers_round_trip_through_the_archive() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(64)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
                let entropy = arbitrary.entropy();
                let answer = if entropy.flag() {
                    PeerClock::BeforeUnixEpoch
                } else {
                    let seconds = entropy.boundary_biased(0..=u64::MAX);
                    let nanoseconds = u32::try_from(entropy.between(0..=999_999_999))
                        .assured("the range stops below one second, which fits 32 bits");
                    PeerClock::SinceUnixEpoch(Duration::new(seconds, nanoseconds))
                };
                let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&answer)
                    .assured("a clock answer archives");
                let decoded = rkyv::from_bytes::<PeerClock, rkyv::rancor::Error>(&encoded)
                    .assured("an archived clock answer reads back");
                assert_eq!(decoded, answer);

                let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&PeerClockProbe)
                    .assured("a clock probe archives");
                let decoded = rkyv::from_bytes::<PeerClockProbe, rkyv::rancor::Error>(&encoded)
                    .assured("an archived clock probe reads back");
                assert_eq!(decoded, PeerClockProbe);
            });
    }
}
