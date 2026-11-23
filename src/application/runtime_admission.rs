//! Admission and serialized installation of ownership-sensitive runtime execution.
//!
//! Layer: control plane.
//!
//! - **Owns.** The process-local linearizable catch-up proof and ordering of coherent runtime-state
//!   installation and activation after that proof and this run's committed capacity claim: the
//!   one installation turn, and the task each installation or activation runs to its end in.
//! - **Depends on.** Consensus observation, execution primitive synchronization and shutdown.
//! - **Must not know.** Runtime graph internals, connector implementations, or scheduling policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::time::Duration;

use error_stack::ResultExt as _;
use meticulous::ResultExt as _;
use nervix_consensus::{ConsensusRuntimeState, Observer};
use nervix_models::{ClusterNodeName, DomainName};
use nervix_primitives::sync::{
    CancellationToken, Mutex, OwnedMutexGuard, StdArc, blocking::OnceLock,
};
use tracing::{info, warn};

use crate::{
    registry::InstalledSchedule,
    runtime::{Runtime, RuntimeError},
};

/// What this node's runtime applied of the committed schedule.
#[derive(Debug, Default)]
struct AppliedSchedule {
    /// The last revision this node applied to every domain; none before its first.
    revision: Option<u64>,
    /// The committed schedule each domain's execution runs, which that domain's next change is
    /// planned against.
    installed: InstalledSchedule,
}

/// The process-start barrier and runtime-installation sequencer shared by every installation path.
pub(in crate::application) struct RuntimeAdmission {
    retry_interval: Duration,
    identity: nervix_models::ClusterNodeIdentity,
    committed_log_index: OnceLock<u64>,
    attempt: Mutex<()>,
    /// The turn of every local installation and activation. A section keeps it in the task it runs
    /// in until it ends, so the turn is shared with that task through Tokio's owned guard, which
    /// takes the standard `Arc`.
    installation: StdArc<Mutex<()>>,
    /// The installation turn terminal teardown took for good, so that no installation begins once
    /// the runtime stops.
    finished: OnceLock<OwnedMutexGuard<()>>,
    /// What this node applied of the committed schedule. A failed revision is retried, and a later
    /// one applied, against the schedule each domain's execution runs, including after a local WASM
    /// reset application.
    applied_schedule: Mutex<AppliedSchedule>,
}

impl RuntimeAdmission {
    pub(in crate::application) fn new(
        identity: nervix_models::ClusterNodeIdentity,
        retry_interval: Duration,
    ) -> Self {
        Self {
            retry_interval,
            identity,
            committed_log_index: OnceLock::new(),
            attempt: Mutex::new(()),
            installation: StdArc::new(Mutex::new(())),
            finished: OnceLock::new(),
            applied_schedule: Mutex::new(AppliedSchedule::default()),
        }
    }

    pub(in crate::application) fn retry_interval(&self) -> Duration {
        self.retry_interval
    }

    /// A session may classify an absent or stopped endpoint as final only after this node has
    /// caught up through a quorum-confirmed log boundary. Before that, its local domain snapshot
    /// may still describe the state from before a committed START.
    pub(in crate::application) fn is_admitted(&self) -> bool {
        self.committed_log_index.get().is_some()
    }

    pub(in crate::application) async fn apply_planned_cluster_state(
        &self,
        runtime: &Runtime,
        local_node_id: &ClusterNodeName,
        state: &ConsensusRuntimeState,
    ) -> error_stack::Result<(), RuntimeError> {
        let mut applied = self.applied_schedule.lock().await;
        if applied
            .revision
            .is_some_and(|revision| state.revision <= revision)
        {
            return Ok(());
        }
        let planned = applied
            .installed
            .plan(&state.schedule)
            .change_context(RuntimeError::PlanScheduleRevision)?;
        let application = runtime
            .apply_planned_cluster_state(
                local_node_id,
                state.revision,
                &state.domains,
                &state.domain_clock_authorities,
                &planned,
            )
            .await;
        // An attempt that fails part way has still brought the domains before the failing one to
        // this revision, so each domain's next change is planned from what its execution runs now.
        let attempt = applied.installed.record_attempt(
            &planned,
            &state.schedule,
            &runtime.installed_revision_digests(),
        );
        if application.is_err() && !attempt.advanced.is_empty() {
            let domains = attempt
                .advanced
                .iter()
                .map(DomainName::as_str)
                .collect::<Vec<_>>();
            info!(
                %local_node_id,
                revision = state.revision,
                ?domains,
                "a failed application brought these domains to its revision; the next change of \
                 each is planned from the revision it runs"
            );
        }
        if !attempt.unplanned.is_empty() {
            let domains = attempt
                .unplanned
                .iter()
                .map(DomainName::as_str)
                .collect::<Vec<_>>();
            warn!(
                %local_node_id,
                revision = state.revision,
                ?domains,
                "these domains run a revision schedule application did not install; their next \
                 change replaces their execution"
            );
        }
        application?;
        applied.revision = Some(state.revision);
        Ok(())
    }

    /// Waits for this node's installation turn, which serializes each local runtime installation
    /// or activation. `shutdown` ends the wait; once terminal teardown took the turn for good, only
    /// `shutdown` ends it.
    pub(in crate::application) async fn begin_installation(
        &self,
        shutdown: &CancellationToken,
    ) -> Option<InstallationTurn> {
        let installation = StdArc::clone(&self.installation).lock_owned();
        tokio::pin!(installation);
        nervix_primitives::select! {
            biased;
            _ = shutdown.cancelled() => None,
            turn = &mut installation => Some(InstallationTurn { turn }),
        }
    }

    /// Waits for the installation or activation still under way, and keeps the turn, so that none
    /// begins any more.
    ///
    /// Terminal teardown calls this once drain support has ended, before it stops the runtime. A
    /// section keeps its turn until it ends, so taking the turn waits for the last one under way.
    /// The turn is kept for good: an application that checked the shutdown before drain support
    /// ended, and asks for the turn only now, waits for it until that shutdown ends its wait,
    /// instead of beginning a section while the runtime stops.
    pub(in crate::application) async fn finish_installations(&self) {
        let turn = StdArc::clone(&self.installation).lock_owned().await;
        self.finished.set(turn).assured(
            "the turn is taken above, and once `finished` holds it the turn is never free again, \
             so no second call reaches this point",
        );
    }

    /// Return state captured after admission, retrying independently of replicated value changes.
    pub(in crate::application) async fn runtime_state(
        &self,
        consensus: &Observer,
        shutdown: &CancellationToken,
    ) -> Option<ConsensusRuntimeState> {
        if shutdown.is_cancelled() {
            return None;
        }
        while self.committed_log_index.get().is_none() {
            if consensus
                .node_record(self.identity.node_id())
                .is_some_and(|committed| *committed.record.identity() == self.identity)
            {
                break;
            }
            nervix_primitives::select! {
                biased;
                _ = shutdown.cancelled() => return None,
                _ = nervix_primitives::time::sleep(self.retry_interval) => {},
            }
        }
        if self.committed_log_index.get().is_some() {
            return Some(consensus.current_runtime_state().await);
        }

        let attempt = self.attempt.lock();
        tokio::pin!(attempt);
        let _attempt = nervix_primitives::select! {
            biased;
            _ = shutdown.cancelled() => return None,
            attempt = &mut attempt => attempt,
        };
        if shutdown.is_cancelled() {
            return None;
        }
        if self.committed_log_index.get().is_some() {
            return Some(consensus.current_runtime_state().await);
        }

        let mut reported_wait = false;
        loop {
            nervix_primitives::task::consume_budget().await;
            let admission = nervix_primitives::select! {
                biased;
                _ = shutdown.cancelled() => return None,
                admission = consensus.admitted_runtime_state() => admission,
            };
            match admission {
                Ok(admission) => {
                    let committed_log_index = admission.committed_log_index();
                    self.committed_log_index.set(committed_log_index).assured(
                        "the serialized admission attempt rechecked the unset proof after locking",
                    );
                    info!(
                        committed_log_index,
                        "runtime execution admitted after linearizable consensus catch-up"
                    );
                    return Some(admission.into_runtime_state());
                }
                Err(error) => {
                    if !reported_wait {
                        warn!(
                            error = %format_args!("{error:#}"),
                            "runtime execution is waiting for linearizable consensus catch-up"
                        );
                        reported_wait = true;
                    }
                    nervix_primitives::select! {
                        biased;
                        _ = shutdown.cancelled() => return None,
                        _ = nervix_primitives::time::sleep(self.retry_interval) => {}
                    }
                }
            }
        }
    }
}

/// This node's turn to install or activate the committed runtime state. One section runs at a
/// time, in the order the turns were taken.
pub(in crate::application) struct InstallationTurn {
    turn: OwnedMutexGuard<()>,
}

impl InstallationTurn {
    /// Runs `section` to its end in a task that holds this turn, and returns what it returned.
    ///
    /// The section is the installation or activation this turn began. A caller that goes away only
    /// stops waiting: the section still ends, and both the next turn and
    /// [`RuntimeAdmission::finish_installations`] wait for it. `None` means the section's task
    /// ended without an outcome, which only the async runtime's own shutdown does. A section that
    /// panics panics the caller that waits for it, as a section run inline would.
    pub(in crate::application) async fn run<Section>(
        self,
        section: Section,
    ) -> Option<Section::Output>
    where
        Section: Future + Send + 'static,
        Section::Output: Send + 'static,
    {
        let Self { turn } = self;
        let task = nervix_primitives::task::spawn(async move {
            let outcome = section.await;
            drop(turn);
            outcome
        });
        match task.await {
            Ok(outcome) => Some(outcome),
            Err(ended) => match ended.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(_cancelled) => None,
            },
        }
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "runtime_admission_shuttle_tests.rs"]
mod shuttle_tests;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::{
        ClusterSchedule, CreateRelay, DomainSchedule, Model, RelayBranching, ScheduledNode,
        SchemaFingerprint,
    };
    use nonzero_ext::nonzero;

    use super::*;

    #[nervix_primitives::test]
    async fn stale_state_keeps_the_last_applied_schedule_as_the_next_predecessor() {
        let admission = RuntimeAdmission::new(
            nervix_models::ClusterNodeIdentity::new(
                ClusterNodeName::parse("node-1").expect("fixture node"),
                nervix_models::ClusterNodeIncarnation::new(1),
            ),
            nervix_models::NodeOperationalCapabilities::default()
                .control_plane
                .runtime_state_retry_interval(),
        );
        let runtime = Runtime::new();
        let local = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
        let current = ConsensusRuntimeState {
            parameters: None,
            revision: 2,
            schedule: ClusterSchedule::default(),
            domains: BTreeMap::new(),
            domain_clock_authorities: BTreeMap::new(),
        };
        admission
            .apply_planned_cluster_state(&runtime, &local, &current)
            .await
            .assured("the empty current revision applies");

        let stale = ConsensusRuntimeState {
            parameters: None,
            revision: 1,
            schedule: ClusterSchedule::from_iter([DomainSchedule::new(
                nervix_models::DomainName::parse("testing")
                    .assured("the fixture domain name is valid"),
                vec![ScheduledNode::new(
                    Model::Relay(CreateRelay {
                        labels: Default::default(),
                        name: nervix_models::RelayName::parse("events")
                            .assured("the fixture relay name is valid"),
                        schema: nervix_models::SchemaName::parse("missing_schema")
                            .assured("the fixture schema name is valid"),
                        buffer: nonzero!(2usize),
                        branching: RelayBranching::unbranched(),
                        materialized_state: None,
                    }),
                    SchemaFingerprint::from_digest([1; 32]),
                )],
                Vec::new(),
            )]),
            domains: BTreeMap::new(),
            domain_clock_authorities: BTreeMap::new(),
        };
        admission
            .apply_planned_cluster_state(&runtime, &local, &stale)
            .await
            .assured("a stale state does not plan or replace its predecessor");
        let applied = admission.applied_schedule.lock().await;
        assert_eq!(applied.revision, Some(2));
    }

    /// A section runs in a task of its own, where a panic would end only that task. The caller
    /// that waits for it panics with the section's own payload, as it did when the section ran
    /// inline, and the section gives the turn back as it unwinds, so the next one can begin.
    #[nervix_primitives::test]
    async fn a_section_that_panics_panics_its_caller_and_gives_the_turn_back() {
        let admission = RuntimeAdmission::new(
            nervix_models::ClusterNodeIdentity::new(
                ClusterNodeName::parse("node-1").assured("the fixture node name is valid"),
                nervix_models::ClusterNodeIncarnation::new(1),
            ),
            Duration::from_millis(500),
        );
        let shutdown = CancellationToken::new();
        let Some(turn) = admission.begin_installation(&shutdown).await else {
            panic!("the turn is free and the shutdown is not cancelled, so the turn begins");
        };

        let caller = nervix_primitives::task::spawn(turn.run(async {
            std::panic::panic_any("the section's own panic");
        }));
        let Err(ended) = caller.await else {
            panic!("the caller returned although the section it waited for panicked");
        };
        let payload = ended.try_into_panic().assured(
            "nothing aborts the caller's task and the runtime outlives it, so it ended by \
             panicking",
        );
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"the section's own panic"),
            "the caller panics with the section's payload"
        );
        assert!(
            admission.installation.try_lock().is_ok(),
            "the section gave the turn back as it unwound"
        );
    }
}
