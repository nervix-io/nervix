//! The application progress command completion reads: this process incarnation's own record, the
//! progress other incarnations report to it while it leads, and the report of its own record to
//! the leader it observes.
//!
//! Layer: control plane.
//!
//! - **Owns.** The incarnation-bound progress record and the advances of its writers, the reports
//!   this node holds while it leads, and the delivery of its own record to the leader it observes.
//! - **Depends on.** Consensus for the applied revision and the leader tenure, cluster availability
//!   for which reports stay held, and typed interconnect progress requests.
//! - **Must not know.** What a revision changed, the graph or the runtime that applied it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::{BTreeMap, BTreeSet};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_consensus::{LeaderTenure, Observer};
use nervix_interconnect::{
    ApplicationProgress, ApplicationProgressPhase, ApplicationProgressReceipt,
    ApplicationProgressReport, HandlerRegistrationError, Transport,
};
use nervix_models::{ClusterNodeIdentity, ClusterNodeName};
use nervix_primitives::{
    sync::{Arc, CancellationToken, watch},
    task::JoinHandle,
};
use tracing::debug;

use crate::cluster::ClusterHandle;

/// This process incarnation's application progress, and the progress other incarnations reported
/// to it while it led. The writers, the revision-query handler, the progress reporter and every
/// revision barrier on the node share one value.
#[derive(Clone)]
pub(in crate::application) struct ApplicationProgressState {
    inner: Arc<ApplicationProgressStateInner>,
}

struct ApplicationProgressStateInner {
    /// The one record of this incarnation's progress. Its writers only advance a phase.
    local: watch::Sender<ApplicationProgress>,
    /// The latest progress each other incarnation reported to this node while it led.
    reported: watch::Sender<ReportedProgress>,
}

/// The latest progress other incarnations reported, keyed by incarnation.
#[derive(Debug, Default, PartialEq, Eq)]
pub(in crate::application) struct ReportedProgress {
    by_incarnation: BTreeMap<ClusterNodeIdentity, ApplicationProgress>,
}

impl ReportedProgress {
    /// Takes `report` into what its incarnation reported before, and keeps only the incarnations
    /// `live` holds besides the reporter. Answers whether anything changed.
    fn record(
        &mut self,
        report: ApplicationProgress,
        live: &BTreeSet<ClusterNodeIdentity>,
    ) -> bool {
        let reporter = report.identity.clone();
        let entries_before = self.by_incarnation.len();
        self.by_incarnation
            .retain(|identity, _| identity == &reporter || live.contains(identity));
        let mut changed = self.by_incarnation.len() != entries_before;
        match self.by_incarnation.get_mut(&reporter) {
            Some(held) => {
                if held.absorb(&report) {
                    changed = true;
                }
            }
            None => {
                self.by_incarnation.insert(reporter, report);
                changed = true;
            }
        }
        changed
    }

    /// The latest progress `identity` reported, absent when it reported nothing.
    pub(in crate::application) fn of(
        &self,
        identity: &ClusterNodeIdentity,
    ) -> Option<&ApplicationProgress> {
        self.by_incarnation.get(identity)
    }
}

/// One barrier's view of its node's progress state. The barrier registers for a change before it
/// reads, so a record or report that arrives after an observation always wakes it.
pub(in crate::application) struct ApplicationProgressObserver {
    /// Keeps both senders alive for as long as the observer waits on them.
    _state: ApplicationProgressState,
    local: watch::Receiver<ApplicationProgress>,
    reported: watch::Receiver<ReportedProgress>,
}

impl ApplicationProgressObserver {
    /// Marks this node's record and the reports it holds as seen, and returns its record. An
    /// advance or a report after this observation wakes [`Self::changed`], so a barrier observes
    /// before it reads the reports it holds.
    pub(in crate::application) fn observe(&mut self) -> ApplicationProgress {
        drop(self.reported.borrow_and_update());
        self.local.borrow_and_update().clone()
    }

    /// Resolves once this node's record advanced, or another incarnation's report changed what
    /// it holds, after the last observation.
    pub(in crate::application) async fn changed(&mut self) {
        let changed = nervix_primitives::select! {
            changed = self.local.changed() => changed,
            changed = self.reported.changed() => changed,
        };
        changed.assured("the observer retains the progress state, which owns both senders");
    }
}

impl ApplicationProgressState {
    /// The state of an incarnation that has reached no phase yet.
    pub(in crate::application) fn new(identity: ClusterNodeIdentity) -> Self {
        let (local, _) = watch::channel(ApplicationProgress::new(identity));
        let (reported, _) = watch::channel(ReportedProgress::default());
        Self {
            inner: Arc::new(ApplicationProgressStateInner { local, reported }),
        }
    }

    /// The incarnation this state records the progress of.
    pub(in crate::application) fn identity(&self) -> ClusterNodeIdentity {
        self.inner.local.borrow().identity.clone()
    }

    /// Records that this incarnation reached `revision` in `phase`. A revision at or before the one
    /// already recorded changes nothing and wakes no one.
    pub(in crate::application) fn advance(&self, phase: ApplicationProgressPhase, revision: u64) {
        self.inner
            .local
            .send_if_modified(|progress| progress.advance(phase, revision));
    }

    /// This incarnation's current progress.
    pub(in crate::application) fn local(&self) -> ApplicationProgress {
        self.inner.local.borrow().clone()
    }

    /// Takes a report from another incarnation, keeping only reports from the incarnations `live`
    /// holds besides the reporter, so the reports stay bounded by the live cluster.
    pub(in crate::application) fn record_report(
        &self,
        report: ApplicationProgress,
        live: &BTreeSet<ClusterNodeIdentity>,
    ) {
        self.inner
            .reported
            .send_if_modified(|reported| reported.record(report, live));
    }

    /// The receipt this node answers for `report`, which arrived over a connection authenticated
    /// as `sender`, while this node observes `tenure`. Only an accepted report is held: its sender
    /// is the node it names, and this node leads the term the sender addressed.
    fn receipt_for(
        &self,
        sender: &ClusterNodeName,
        report: &ApplicationProgressReport,
        tenure: Option<&LeaderTenure>,
    ) -> ApplicationProgressReceipt {
        if sender != report.progress.identity.node_id() {
            return ApplicationProgressReceipt::ForeignReporter;
        }
        let Some(tenure) = tenure else {
            return ApplicationProgressReceipt::NotLeader;
        };
        let leads_addressed_term = tenure.leader_id() == &report.leader
            && tenure.term() == report.term
            && tenure.leader_id() == self.identity().node_id();
        if !leads_addressed_term {
            return ApplicationProgressReceipt::NotLeader;
        }
        ApplicationProgressReceipt::Accepted
    }

    /// The progress this node holds for `peers`: its own record, and the latest report of each
    /// other peer that reported.
    pub(in crate::application) fn progress_of(
        &self,
        peers: &BTreeSet<ClusterNodeIdentity>,
    ) -> Vec<ApplicationProgress> {
        let local = self.local();
        let reported = self.inner.reported.borrow();
        let mut progress = Vec::new();
        for peer in peers {
            if peer == &local.identity {
                progress.push(local.clone());
                continue;
            }
            if let Some(report) = reported.of(peer) {
                progress.push(report.clone());
            }
        }
        progress
    }

    /// A view of this state for one barrier.
    pub(in crate::application) fn observer(&self) -> ApplicationProgressObserver {
        ApplicationProgressObserver {
            _state: self.clone(),
            local: self.inner.local.subscribe(),
            reported: self.inner.reported.subscribe(),
        }
    }
}

/// Registers the handler that holds the progress reports other incarnations send this node while
/// it leads. A report is held only when the connection's authenticated node is the node the report
/// names and this node leads the term the sender addressed. The transport authenticates nodes, not
/// incarnations, and a barrier reads a report only for an incarnation its leader view names.
pub(in crate::application) fn register_application_progress_report_handler(
    progress: ApplicationProgressState,
    cluster: Arc<ClusterHandle>,
    consensus: Observer,
    interconnect: &Transport,
) -> Result<(), Report<HandlerRegistrationError>> {
    interconnect.register_handler::<ApplicationProgressReport, _, _>(move |context, report| {
        let progress = progress.clone();
        let cluster = cluster.clone();
        let consensus = consensus.clone();
        async move {
            let tenure = consensus.current_leader_tenure();
            let receipt = progress.receipt_for(context.peer_node_id(), &report, tenure.as_ref());
            if receipt == ApplicationProgressReceipt::Accepted {
                let live = cluster.availability_state().await.live_identities();
                progress.record_report(report.progress, &live);
            }
            receipt
        }
    })
}

/// What this incarnation last reported, to the leader of which term.
#[derive(PartialEq, Eq)]
struct DeliveredReport {
    tenure: LeaderTenure,
    progress: ApplicationProgress,
}

impl DeliveredReport {
    fn covers(&self, tenure: &LeaderTenure, progress: &ApplicationProgress) -> bool {
        &self.tenure == tenure && &self.progress == progress
    }
}

/// Reports this incarnation's record to the leader it observes.
struct ProgressReporter {
    progress: ApplicationProgressState,
    consensus: Observer,
    interconnect: Transport,
    fault_injection: crate::ConfiguredFaultInjection,
    identity: ClusterNodeIdentity,
    /// The report the leader last received or refused, which this reporter does not send again.
    delivered: Option<DeliveredReport>,
}

impl ProgressReporter {
    /// Reports whenever a phase advances or the observed leadership changes, until `shutdown` or
    /// until consensus stops.
    async fn run(mut self, shutdown: CancellationToken) {
        let mut records = self.progress.inner.local.subscribe();
        let mut topology = self.consensus.subscribe_topology();
        loop {
            nervix_primitives::task::consume_budget().await;
            let current = records.borrow_and_update().clone();
            if let Some(tenure) = self.undelivered_tenure(&current) {
                nervix_primitives::select! {
                    _ = shutdown.cancelled() => break,
                    () = self.report(&mut records, tenure) => {}
                }
            }
            nervix_primitives::select! {
                _ = shutdown.cancelled() => break,
                changed = records.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                changed = topology.changed() => {
                    if !changed {
                        break;
                    }
                }
            }
        }
    }

    /// The tenure of the leader `current` still has to reach: absent while this node leads, while
    /// no leader is known, and once that leader received or refused this record.
    fn undelivered_tenure(&self, current: &ApplicationProgress) -> Option<LeaderTenure> {
        let tenure = self.consensus.current_leader_tenure()?;
        if tenure.leader_id() == self.identity.node_id() {
            return None;
        }
        if let Some(delivered) = &self.delivered
            && delivered.covers(&tenure, current)
        {
            return None;
        }
        Some(tenure)
    }

    /// Sends the newest record to the leader of `tenure`. Whatever the leader answers, this record
    /// counts as delivered there, so a report that failed is not repeated.
    async fn report(
        &mut self,
        records: &mut watch::Receiver<ApplicationProgress>,
        tenure: LeaderTenure,
    ) {
        self.fault_injection
            .pause_application_progress_report_if_armed(self.identity.node_id())
            .await;
        // A held report carries what this incarnation reached by the time it is released.
        let current = records.borrow_and_update().clone();
        let report = ApplicationProgressReport {
            progress: current.clone(),
            leader: tenure.leader_id().clone(),
            term: tenure.term(),
        };
        match self.interconnect.request(tenure.leader_id(), report).await {
            Ok(ApplicationProgressReceipt::Accepted) => {}
            Ok(receipt) => {
                debug!(
                    leader = %tenure.leader_id(),
                    term = tenure.term(),
                    ?receipt,
                    "the leader held no application progress report"
                );
            }
            Err(error) => {
                debug!(
                    leader = %tenure.leader_id(),
                    term = tenure.term(),
                    error = %format_args!("{error:#}"),
                    "failed to report application progress"
                );
            }
        }
        self.delivered = Some(DeliveredReport {
            tenure,
            progress: current,
        });
    }
}

#[cfg(not(feature = "testing"))]
impl crate::ConfiguredFaultInjection {
    /// A build without the test harness holds no progress report.
    async fn pause_application_progress_report_if_armed(&self, _node: &ClusterNodeName) {}
}

/// Reports this incarnation's record to the leader it observes whenever a phase advances or the
/// leadership it observes changes. One report is in flight at a time, carrying the newest record,
/// so a burst of advances costs one report per round trip. A report the leader did not hold is
/// logged and not repeated: its barriers query a node whose report they lack.
pub(in crate::application) fn spawn_application_progress_reporting(
    progress: ApplicationProgressState,
    consensus: Observer,
    interconnect: Transport,
    fault_injection: crate::ConfiguredFaultInjection,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let identity = progress.identity();
    let reporter = ProgressReporter {
        progress,
        consensus,
        interconnect,
        fault_injection,
        identity,
        delivered: None,
    };
    nervix_primitives::task::spawn(reporter.run(shutdown))
}

/// Records each authoritative revision this incarnation applies.
pub(in crate::application) fn spawn_authoritative_progress_recording(
    progress: ApplicationProgressState,
    consensus: Observer,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    nervix_primitives::task::spawn(record_authoritative_progress(progress, consensus, shutdown))
}

/// Advances this incarnation's authoritative phase to every revision it applies, until `shutdown`
/// or until consensus stops.
pub(in crate::application) async fn record_authoritative_progress(
    progress: ApplicationProgressState,
    consensus: Observer,
    shutdown: CancellationToken,
) {
    let mut applied = consensus.subscribe_applied();
    loop {
        nervix_primitives::task::consume_budget().await;
        // Marks the applied revision seen before reading it, so an apply after the read wakes
        // the wait below.
        drop(applied.borrow_and_update());
        if let Some(revision) = consensus.applied_revision() {
            progress.advance(ApplicationProgressPhase::Authoritative, revision);
        }
        nervix_primitives::select! {
            _ = shutdown.cancelled() => break,
            changed = applied.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_interconnect::{
        ApplicationProgress, ApplicationProgressPhase, ApplicationProgressReceipt,
        ApplicationProgressReport,
    };
    use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName};

    use super::{ApplicationProgressState, ReportedProgress};
    use crate::application::test_fixtures::{TestService, build_test_service};

    fn identity(name: &str, incarnation: u64) -> ClusterNodeIdentity {
        ClusterNodeIdentity::new(
            ClusterNodeName::parse(name).assured("the test node name is valid"),
            ClusterNodeIncarnation::new(incarnation),
        )
    }

    fn reached(
        identity: ClusterNodeIdentity,
        phase: ApplicationProgressPhase,
        revision: u64,
    ) -> ApplicationProgress {
        let mut progress = ApplicationProgress::new(identity);
        progress.advance(phase, revision);
        progress
    }

    #[test]
    fn an_advance_moves_one_phase_forward_and_publishes_only_a_change() {
        let local = identity("node-1", 7);
        let state = ApplicationProgressState::new(local.clone());
        let mut observer = state.observer();
        assert_eq!(observer.observe(), ApplicationProgress::new(local.clone()));

        state.advance(ApplicationProgressPhase::RuntimePrepared, 4);
        state.advance(ApplicationProgressPhase::RuntimePrepared, 3);
        assert_eq!(
            state.local(),
            reached(local.clone(), ApplicationProgressPhase::RuntimePrepared, 4),
            "an earlier revision moves no phase back"
        );
        assert_eq!(state.identity(), local);

        let published = observer.observe();
        assert_eq!(published.runtime_prepared, Some(4));
        state.advance(ApplicationProgressPhase::RuntimePrepared, 4);
        assert!(
            !observer
                .local
                .has_changed()
                .assured("the state owns the record's sender"),
            "an advance that moves nothing publishes nothing"
        );
    }

    #[test]
    fn a_held_report_merges_per_incarnation_and_stays_bounded_by_the_live_view() {
        let first = identity("node-2", 2);
        let restarted = identity("node-2", 5);
        let other = identity("node-3", 3);
        let mut reported = ReportedProgress::default();
        let live = BTreeSet::from([first.clone(), other.clone()]);

        assert!(reported.record(
            reached(first.clone(), ApplicationProgressPhase::RuntimeReady, 9),
            &live
        ));
        assert!(reported.record(
            reached(other.clone(), ApplicationProgressPhase::Authoritative, 4),
            &live
        ));
        assert!(
            !reported.record(
                reached(first.clone(), ApplicationProgressPhase::RuntimeReady, 8),
                &live
            ),
            "a late report of the same incarnation moves no phase back"
        );
        assert_eq!(
            reported.of(&first),
            Some(&reached(
                first.clone(),
                ApplicationProgressPhase::RuntimeReady,
                9
            ))
        );

        let after_restart = BTreeSet::from([restarted.clone(), other.clone()]);
        assert!(reported.record(
            reached(restarted.clone(), ApplicationProgressPhase::RuntimeReady, 2),
            &after_restart
        ));
        assert_eq!(
            reported.of(&first),
            None,
            "an incarnation the live view no longer holds is dropped"
        );
        assert_eq!(
            reported.of(&restarted),
            Some(&reached(
                restarted.clone(),
                ApplicationProgressPhase::RuntimeReady,
                2
            )),
            "a restarted incarnation starts from its own report, never its predecessor's"
        );
        assert!(reported.of(&other).is_some());

        let joining = identity("node-4", 1);
        assert!(reported.record(ApplicationProgress::new(joining.clone()), &after_restart));
        assert!(
            reported.of(&joining).is_some(),
            "the reporter is held even before the live view lists it"
        );
    }

    #[test]
    fn progress_of_answers_the_local_record_and_each_peer_that_reported() {
        let local = identity("node-1", 1);
        let reporting = identity("node-2", 2);
        let silent = identity("node-3", 3);
        let state = ApplicationProgressState::new(local.clone());
        state.advance(ApplicationProgressPhase::Authoritative, 6);
        let live = BTreeSet::from([reporting.clone(), silent.clone()]);
        state.record_report(
            reached(
                reporting.clone(),
                ApplicationProgressPhase::RuntimePrepared,
                5,
            ),
            &live,
        );

        let peers = BTreeSet::from([local.clone(), reporting.clone(), silent]);
        assert_eq!(
            state.progress_of(&peers),
            vec![
                reached(local, ApplicationProgressPhase::Authoritative, 6),
                reached(reporting, ApplicationProgressPhase::RuntimePrepared, 5),
            ],
            "a peer that reported nothing has no entry"
        );
    }

    #[nervix_primitives::test]
    async fn an_observer_wakes_for_a_report_that_arrives_after_its_observation() {
        let local = identity("node-1", 1);
        let reporting = identity("node-2", 2);
        let state = ApplicationProgressState::new(local);
        let mut observer = state.observer();
        drop(observer.observe());
        let live = BTreeSet::from([reporting.clone()]);
        state.record_report(
            reached(reporting, ApplicationProgressPhase::RuntimeReady, 3),
            &live,
        );
        nervix_primitives::time::timeout(Duration::from_secs(10), observer.changed())
            .await
            .assured("a report recorded after the observation wakes the observer at once");
    }

    #[nervix_primitives::test]
    async fn a_report_is_held_only_from_the_node_it_names_while_this_node_leads_the_addressed_term()
    {
        let bound = Duration::from_secs(30);
        let TestService { service, path, .. } =
            nervix_primitives::time::timeout(bound, build_test_service(false))
                .await
                .assured("the progress fixture starts within its test deadline");
        let Some(tenure) = service.inner.consensus.current_leader_tenure() else {
            panic!("the fixture returns once its node leads its single-node consensus");
        };
        let leader = &service.inner.application_progress;
        let reporter = identity("node-2", 2);
        let report = ApplicationProgressReport {
            progress: reached(reporter.clone(), ApplicationProgressPhase::RuntimeReady, 3),
            leader: tenure.leader_id().clone(),
            term: tenure.term(),
        };
        let later_term = tenure
            .term()
            .checked_add(1)
            .assured("a fresh single-node consensus is far below the last Raft term");

        assert_eq!(
            leader.receipt_for(reporter.node_id(), &report, Some(&tenure)),
            ApplicationProgressReceipt::Accepted,
            "the reporter's own connection reaches the leader of the term it addressed"
        );
        assert_eq!(
            leader.receipt_for(identity("node-3", 3).node_id(), &report, Some(&tenure)),
            ApplicationProgressReceipt::ForeignReporter,
            "a connection authenticated as another node cannot report for the reporter"
        );
        assert_eq!(
            leader.receipt_for(reporter.node_id(), &report, None),
            ApplicationProgressReceipt::NotLeader,
            "a node that observes no leader holds no report"
        );
        let other_term = ApplicationProgressReport {
            term: later_term,
            ..report.clone()
        };
        assert_eq!(
            leader.receipt_for(reporter.node_id(), &other_term, Some(&tenure)),
            ApplicationProgressReceipt::NotLeader,
            "a report addressed to another term is not held"
        );
        let other_leader = ApplicationProgressReport {
            leader: reporter.node_id().clone(),
            ..report.clone()
        };
        assert_eq!(
            leader.receipt_for(reporter.node_id(), &other_leader, Some(&tenure)),
            ApplicationProgressReceipt::NotLeader,
            "a report addressed to another leader is not held"
        );
        let follower = ApplicationProgressState::new(identity("node-4", 4));
        assert_eq!(
            follower.receipt_for(reporter.node_id(), &report, Some(&tenure)),
            ApplicationProgressReceipt::NotLeader,
            "a node that does not lead the observed term holds no report addressed to its leader"
        );

        drop(service);
        std::fs::remove_dir_all(&path).expect("the test database directory is removed");
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use std::collections::{BTreeMap, BTreeSet};

    use meticulous::ResultExt as _;
    use nervix_interconnect::{ApplicationProgress, ApplicationProgressPhase};
    use nervix_model_harness::shuttle::{check_pct, check_random};
    use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName};
    use nervix_primitives::thread;
    use shuttle::future::block_on;

    use super::ApplicationProgressState;

    const MODEL_THREAD_JOINS: &str =
        "Shuttle fails the whole execution when a model thread panics, so no join observes one";
    const PCT_DEPTH: usize = 3;
    const PCT_ITERATIONS: usize = 1_000;
    const RANDOM_ITERATIONS: usize = 1_000;
    const REVISION: u64 = 7;

    fn identity(name: &str, incarnation: u64) -> ClusterNodeIdentity {
        ClusterNodeIdentity::new(
            ClusterNodeName::parse(name).assured("the model node name is valid"),
            ClusterNodeIncarnation::new(incarnation),
        )
    }

    fn ready(identity: ClusterNodeIdentity, revision: u64) -> ApplicationProgress {
        let mut progress = ApplicationProgress::new(identity);
        progress.advance(ApplicationProgressPhase::RuntimeReady, revision);
        progress
    }

    /// Whether the state holds every peer at `REVISION` or later in runtime readiness: the local
    /// record for the local incarnation, a held report for every other.
    fn every_peer_ready(
        state: &ApplicationProgressState,
        peers: &BTreeSet<ClusterNodeIdentity>,
    ) -> bool {
        let mut held = BTreeMap::new();
        for progress in state.progress_of(peers) {
            held.insert(progress.identity.clone(), progress);
        }
        for peer in peers {
            let Some(progress) = held.get(peer) else {
                return false;
            };
            let Some(revision) = progress.reached(ApplicationProgressPhase::RuntimeReady) else {
                return false;
            };
            if revision < REVISION {
                return false;
            }
        }
        true
    }

    /// Two followers' reports and the leader's own advance race a barrier on the leader that
    /// observes, reads, and waits for a change while a peer is pending. A wake lost between its
    /// read and its wait would leave the barrier waiting with no change to come, which the model
    /// reports as a deadlock.
    fn reports_racing_a_waiting_barrier() {
        let local = identity("leader", 1);
        let first = identity("follower-a", 2);
        let second = identity("follower-b", 3);
        let state = ApplicationProgressState::new(local.clone());
        let peers = BTreeSet::from([local, first.clone(), second.clone()]);
        let live = peers.clone();

        let waiting = state.clone();
        let waiting_peers = peers.clone();
        let barrier = thread::spawn(move || {
            block_on(async move {
                let mut observer = waiting.observer();
                loop {
                    drop(observer.observe());
                    if every_peer_ready(&waiting, &waiting_peers) {
                        return;
                    }
                    observer.changed().await;
                }
            });
        });
        let first_reporting = state.clone();
        let first_live = live.clone();
        let first_reporter = thread::spawn(move || {
            first_reporting.record_report(ready(first, REVISION), &first_live)
        });
        let second_reporting = state.clone();
        let second_reporter = thread::spawn(move || {
            second_reporting.record_report(ready(second, REVISION), &live);
        });
        let advancing = state.clone();
        let local_writer = thread::spawn(move || {
            advancing.advance(ApplicationProgressPhase::RuntimeReady, REVISION);
        });

        first_reporter.join().assured(MODEL_THREAD_JOINS);
        second_reporter.join().assured(MODEL_THREAD_JOINS);
        local_writer.join().assured(MODEL_THREAD_JOINS);
        barrier.join().assured(MODEL_THREAD_JOINS);
        assert!(every_peer_ready(&state, &peers));
    }

    /// A newer and a late report of one incarnation race each other and an observer; whatever the
    /// order, the held progress is the newer one and never moves back.
    fn racing_reports_of_one_incarnation() {
        let local = identity("leader", 1);
        let reporter = identity("follower-a", 2);
        let state = ApplicationProgressState::new(local);
        let live = BTreeSet::from([reporter.clone()]);

        let newer_state = state.clone();
        let newer_live = live.clone();
        let newer_reporter = reporter.clone();
        let newer = thread::spawn(move || {
            newer_state.record_report(ready(newer_reporter, REVISION), &newer_live);
        });
        let late_state = state.clone();
        let late_reporter = reporter.clone();
        let late = thread::spawn(move || {
            late_state.record_report(ready(late_reporter, REVISION - 1), &live);
        });
        let observing = state.clone();
        let observed_peers = BTreeSet::from([reporter.clone()]);
        let observer = thread::spawn(move || {
            let mut previous = None;
            for _ in 0..3 {
                thread::yield_now();
                let held = observing.progress_of(&observed_peers);
                let current = match held.first() {
                    Some(progress) => progress.reached(ApplicationProgressPhase::RuntimeReady),
                    None => None,
                };
                assert!(
                    current >= previous,
                    "the held readiness moved back from {previous:?} to {current:?}"
                );
                previous = current;
            }
        });

        newer.join().assured(MODEL_THREAD_JOINS);
        late.join().assured(MODEL_THREAD_JOINS);
        observer.join().assured(MODEL_THREAD_JOINS);
        assert_eq!(
            state.progress_of(&BTreeSet::from([reporter.clone()])),
            vec![ready(reporter, REVISION)]
        );
    }

    #[test]
    fn shuttle_a_barrier_that_observes_before_it_reads_sees_every_racing_report_and_advance() {
        check_random(reports_racing_a_waiting_barrier, RANDOM_ITERATIONS);
        check_pct(reports_racing_a_waiting_barrier, PCT_ITERATIONS, PCT_DEPTH);
    }

    #[test]
    fn shuttle_racing_reports_of_one_incarnation_keep_its_newest_progress() {
        check_random(racing_reports_of_one_incarnation, RANDOM_ITERATIONS);
        check_pct(racing_reports_of_one_incarnation, PCT_ITERATIONS, PCT_DEPTH);
    }
}
