//! Lower bounds on when a row a node holds is delivered, and the runtime revisions that release
//! such a row early.
//!
//! Layer: test harness.
//! - **Owns.** The runtime revisions a scenario records before it publishes what a node then
//!   holds, and the judgement of a row that arrives before its lower bound.
//! - **Depends on.** The application progress each node of the scenario's cluster answers for
//!   itself, and the cucumber log.
//! - **Must not know.** What holds the row, or how a node decides to send it.
//!
//! A route's flush, an input collection, an ingestor's batch timeout, a WASM guest's buffering and
//! an emitter's retry backoff bound how long a node holds a row from above only. Anything that
//! forces a flush sends the row sooner, which is correct: a failover, a forced recovery, a move or
//! an alteration anywhere in the domain. Each of these changes the cluster's schedule or domains,
//! which every node then applies as a newer runtime revision, and a node that applies one that
//! moves or replaces units of the domain force-flushes the whole domain. On a loaded host a node
//! can miss its health probes during a hold, and the failover that follows releases the row early
//! through no fault of what held it.
//!
//! A step that bounds a delivery from below therefore states whether such a revision may release
//! the row. When it may, the scenario records the revision each node applied before it publishes,
//! and a row that arrives before its bound passes only once a node shows that it applied a newer
//! revision since, or restarted. Not every newer revision forces a flush, but none of the forced
//! flushes these steps admit comes without one. A node shows a revision once it prepared it, or
//! failed to, which comes after the flush it forced, so the step waits a bounded time for it
//! rather than deciding on the instant the row arrived.

use super::*;
use crate::common::runtime_applications::{NodeRuntimeApplication, RuntimeApplications};

/// How long a row that arrived before its lower bound waits for a node to show the newer runtime
/// revision that released it: one status wait, the budget for the cluster's state to converge. A
/// node shows the revision after the flush it forced, once it prepared or failed to prepare it, and
/// the wait ends as soon as one node shows one.
const RELEASING_REVISION_BUDGET: Duration = crate::common::status_request::STATUS_WAIT_BUDGET;

/// How long one node may take to say which runtime revision it applied before an observation goes
/// on without its answer: one status request, which reaches the same node by a longer path.
const NODE_ANSWER_BUDGET: Duration = crate::common::status_request::STATUS_REQUEST_TIMEOUT;

/// How long a row that arrived early waits between two observations of what the nodes applied.
/// Each observation asks every node once, so a short interval costs the cluster little.
const RELEASING_REVISION_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// The runtime revision each node had applied when the scenario recorded them, before it published
/// what a node then holds.
#[derive(Debug)]
pub(crate) struct RecordedRuntimeRevisions {
    /// When the recording began. A bound may only count from a publication no earlier than this,
    /// or a revision applied in between would release the row unseen.
    recorded_at: Instant,
    applications: RuntimeApplications,
}

#[given("the runtime revision each node applied is recorded")]
#[when("the runtime revision each node applied is recorded")]
async fn record_runtime_revisions(world: &mut ScenarioWorld) {
    let recorded_at = Instant::now();
    let observation = observe_runtime_applications(world).await;
    append_cucumber_log_line(&format!(
        "recorded the runtime revision each node applied: {observation}"
    ));
    world.recorded_runtime_revisions = Some(RecordedRuntimeRevisions {
        recorded_at,
        applications: observation.answered,
    });
}

/// What the nodes of the scenario's cluster answered at one moment, and why each other node did
/// not.
struct RuntimeApplicationObservation {
    answered: RuntimeApplications,
    unanswered: Vec<String>,
}

impl fmt::Display for RuntimeApplicationObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.answered)?;
        for reason in &self.unanswered {
            write!(formatter, "; {reason}")?;
        }
        Ok(())
    }
}

/// Asks every node of the scenario's cluster, at once, which runtime revision it applied.
async fn observe_runtime_applications(world: &ScenarioWorld) -> RuntimeApplicationObservation {
    let nodes = world.cluster().node_ids();
    let answers = join_all(
        nodes
            .iter()
            .map(|node| answer_runtime_application(&world.fault_injection, node)),
    )
    .await;
    let mut observation = RuntimeApplicationObservation {
        answered: RuntimeApplications::default(),
        unanswered: Vec::new(),
    };
    for (node, answer) in nodes.into_iter().zip(answers) {
        match answer {
            Ok(application) => observation.answered.record(node, application),
            Err(reason) => observation.unanswered.push(reason),
        }
    }
    observation
}

/// What `node` answers for the newest runtime revision it applied, or why it did not answer: it
/// is stopped, its answer failed, or it took longer than [`NODE_ANSWER_BUDGET`].
async fn answer_runtime_application(
    fault_injection: &FaultInjection,
    node: &str,
) -> Result<NodeRuntimeApplication, String> {
    let node = crate::common::cluster::node_name(node);
    let query = fault_injection.query_application_progress(&node, node.clone());
    match nervix_primitives::time::timeout(NODE_ANSWER_BUDGET, query).await {
        Ok(Ok(progress)) => Ok(NodeRuntimeApplication::of(&progress)),
        Ok(Err(error)) => Err(format!("node {node} did not answer: {error:#}")),
        Err(_elapsed) => Err(format!(
            "node {node} did not answer within {NODE_ANSWER_BUDGET:?}"
        )),
    }
}

/// What a lower bound does with a row that arrives before it.
#[derive(Debug, Clone, Copy)]
pub(crate) enum EarlyArrival {
    /// Nothing may release the row early, so an early row fails the step.
    Refused,
    /// A runtime revision a node applied after the scenario recorded them may release the row
    /// early. An early row passes once a node shows one, and fails the step otherwise.
    ReleasedByNewerRuntimeRevision,
}

impl EarlyArrival {
    /// Judges what arrived before a lower bound that counts from `published_at`. `early` says what
    /// arrived and when, and is the failure when nothing released it.
    pub(crate) async fn judge(self, world: &ScenarioWorld, published_at: Instant, early: &str) {
        match self {
            Self::Refused => panic!("{early}"),
            Self::ReleasedByNewerRuntimeRevision => {
                require_newer_runtime_revision(world, published_at, early).await;
            }
        }
    }
}

/// A lower bound on when the payloads a wait expects may be delivered, counted from their
/// publication.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DeliveryLowerBound {
    pub(crate) published_at: Instant,
    pub(crate) delay: Duration,
    pub(crate) early_arrival: EarlyArrival,
}

/// The payloads a wait read sooner than its lower bound allows, judged together once it has read
/// every payload it expects, so that waiting for a releasing revision takes nothing from the time
/// the wait gives the payloads still to come.
pub(crate) struct EarlyDeliveries {
    bound: DeliveryLowerBound,
    payloads: Vec<String>,
}

impl EarlyDeliveries {
    pub(crate) fn new(bound: DeliveryLowerBound) -> Self {
        Self {
            bound,
            payloads: Vec::new(),
        }
    }

    /// Checks `payload`, read now, against the bound. A payload read too soon fails the step at
    /// once when nothing may release it, and is kept for [`Self::judge`] otherwise.
    pub(crate) fn check(&mut self, payload: &str) {
        let arrived_after = self.bound.published_at.elapsed();
        if arrived_after >= self.bound.delay {
            return;
        }
        let early = format!("{payload:?} arrived {arrived_after:?} after it was published");
        match self.bound.early_arrival {
            EarlyArrival::Refused => panic!(
                "subscription payload {early}, sooner than {:?}",
                self.bound.delay
            ),
            EarlyArrival::ReleasedByNewerRuntimeRevision => self.payloads.push(early),
        }
    }

    /// Judges every payload [`Self::check`] kept.
    pub(crate) async fn judge(self, world: &ScenarioWorld) {
        if self.payloads.is_empty() {
            return;
        }
        let early = format!(
            "subscription payloads {} sooner than {:?}",
            self.payloads.join(" and "),
            self.bound.delay
        );
        self.bound
            .early_arrival
            .judge(world, self.bound.published_at, &early)
            .await;
    }
}

/// Requires that a node shows, within [`RELEASING_REVISION_BUDGET`], that it applied a runtime
/// revision newer than the ones the scenario recorded before `published_at`, or restarted: only
/// that explains `unmet`, which fails the step when no node does.
pub(crate) async fn require_newer_runtime_revision(
    world: &ScenarioWorld,
    published_at: Instant,
    unmet: &str,
) {
    let Some(recorded) = world.recorded_runtime_revisions.as_ref() else {
        panic!(
            "{unmet}, and the scenario recorded no runtime revisions before it published, so \
             nothing tells a forced flush from a row released too soon"
        );
    };
    assert!(
        recorded.recorded_at <= published_at,
        "{unmet}, but the runtime revisions were recorded after the publication the bound counts \
         from, so a revision applied in between would have gone unseen: record them before \
         publishing"
    );
    let deadline = Instant::now() + RELEASING_REVISION_BUDGET;
    loop {
        nervix_primitives::task::consume_budget().await;
        let current = observe_runtime_applications(world).await;
        if let Some(newer) = current.answered.moved_on_from(&recorded.applications) {
            append_cucumber_log_line(&format!("{unmet}, which a forced flush explains: {newer}"));
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{unmet}, and within {RELEASING_REVISION_BUDGET:?} no node showed a runtime revision \
             newer than the ones recorded before it was published: recorded {}, last observed \
             {current}",
            recorded.applications
        );
        nervix_primitives::time::sleep(RELEASING_REVISION_POLL_INTERVAL).await;
    }
}
