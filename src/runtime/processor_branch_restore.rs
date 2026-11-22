//! Restoring the concrete branches a processor task owns before the task accepts input.
//!
//! Layer: data plane.
//!
//! - **Owns.** Installing every branch a processor's branch lifecycle checkpoint or its previous
//!   task's handoff names, all of them or none, releasing that lifecycle only once they are
//!   installed; building them on executor work that waits for capacity instead of being refused,
//!   while telling the task when it waits so that the task answers its commands meanwhile; and the
//!   backoff between attempts that fail.
//! - **Depends on.** Planned processor templates, processor branch startup, the runtime's
//!   restorable branch lifecycle, the bounded executor's capacity waits and the runtime's retry
//!   backoff.
//! - **Must not know.** Plexus text, control-plane transactions, consensus, or connector protocols.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "restoring a processor's branches installs their retained execution state when \
                  its task starts"
    )
)]

use std::{future::Future, pin::Pin};

use error_stack::ResultExt as _;
use nervix_execution::CapacityWaits;

use super::*;

/// Where the branches a processor task restores come from.
enum ProcessorBranchRestoreSource {
    /// The branches the processor's branch lifecycle checkpoint names, whether a transfer left it
    /// on this node, this node holds it, or its storage keeps it.
    Lifecycle,
    /// The branches the processor's previous task on this node handed over, with the input each
    /// of them still had pending.
    Handoffs(Vec<ProcessorBranchHandoff>),
}

/// The branches a processor task installs before it accepts input, kept until every one of them
/// runs so that a failed attempt loses none of them.
pub(super) struct PendingProcessorBranchRestore {
    source: ProcessorBranchRestoreSource,
    backoff: RuntimeReconnectBackoff,
    retry_at: Instant,
}

/// One attempt of a pending restore. It builds every branch the restore names, opening their
/// retained state on executor work that waits for a place in a full worker class, or for room in
/// a full memory class, rather than being refused, and it tells its task whenever that work waits.
pub(super) struct ProcessorBranchRestoreAttempt {
    waits: CapacityWaits,
    build: RestoreBuild,
}

/// Where one attempt's build stands.
enum RestoreBuild {
    /// Still building, on a future that owns everything it reads, so that the task can answer its
    /// commands between polls of it.
    Building(Pin<Box<dyn Future<Output = RestoreBuilt> + Send>>),
    /// Finished, with what it built or why it failed, until the task installs it.
    Built(RestoreBuilt),
}

/// What one attempt built: every branch its restore names with the lifecycle that names them,
/// nothing when the processor never formed a lifecycle, or why it could not build them.
type RestoreBuilt = error_stack::Result<Option<BuiltBranches>, ProcessorBranchTaskError>;

/// What a restore attempt does after its task has run it.
pub(super) enum RestoreProgress {
    /// Its build finished, and the task installs what it built.
    Built,
    /// Its build waits for capacity, and the task answers its commands beside it.
    Waiting,
}

/// The lifecycle a restore's branches belong to and the lifetime each of them resumes.
struct RestorePlan {
    lifecycle: RestorableBranchLifecycle,
    lifetimes: Vec<BranchInstanceSnapshotEntry<Option<BranchKey>>>,
}

/// Every branch one attempt built and has not started yet, with the lifecycle that names them.
struct BuiltBranches {
    lifecycle: RestorableBranchLifecycle,
    branches: Vec<BuiltBranch>,
}

/// One branch an attempt built, with the lifetime it resumes.
struct BuiltBranch {
    branch: PreparedProcessorBranch,
    lifetime: BranchInstanceSnapshotEntry<Option<BranchKey>>,
}

impl PendingProcessorBranchRestore {
    /// The restore a new processor task performs first: the branches its previous task handed
    /// over, or the branches its lifecycle checkpoint names when there are none.
    pub(super) fn new(
        handoffs: Vec<ProcessorBranchHandoff>,
        settings: nervix_models::DataPlaneSettings,
        retry: nervix_connector::RetryBackoffSettings,
    ) -> Self {
        let source = if handoffs.is_empty() {
            ProcessorBranchRestoreSource::Lifecycle
        } else {
            ProcessorBranchRestoreSource::Handoffs(handoffs)
        };
        Self {
            source,
            backoff: RuntimeReconnectBackoff::from_settings(settings, retry),
            retry_at: Instant::now(),
        }
    }

    /// When the next attempt is due.
    pub(super) fn retry_at(&self) -> Instant {
        self.retry_at
    }

    /// The branches a processor whose restore is still pending hands to its successor, which
    /// restores them in its place.
    pub(super) fn into_handoffs(self) -> Vec<ProcessorBranchHandoff> {
        match self.source {
            ProcessorBranchRestoreSource::Lifecycle => Vec::new(),
            ProcessorBranchRestoreSource::Handoffs(handoffs) => handoffs,
        }
    }

    /// Begin one attempt to restore every branch this restore names.
    ///
    /// The attempt reads the lifecycle those branches belong to at once. It then builds each
    /// branch, opening its retained state through an executor handle that waits for capacity
    /// rather than being refused and that tells the attempt while it waits. Nothing the attempt
    /// builds runs, and nothing is taken from this restore, until [`Self::finish`] installs it.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one restore attempt reads the processor's retained branch lifecycle and \
                      begins building its branch lifetimes"
        )
    )]
    pub(super) fn begin(
        &self,
        context: &ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
    ) -> ProcessorBranchRestoreAttempt {
        let waits = CapacityWaits::new();
        let build = match self.plan(context, template) {
            Err(error) => RestoreBuild::Built(Err(error)),
            // The processor has never formed a branch lifecycle, so it has no branch to restore.
            Ok(None) => RestoreBuild::Built(Ok(None)),
            Ok(Some(plan)) if plan.lifetimes.is_empty() => {
                RestoreBuild::Built(Ok(Some(BuiltBranches {
                    lifecycle: plan.lifecycle,
                    branches: Vec::new(),
                })))
            }
            Ok(Some(plan)) => {
                let executor = context.runtime_handle.executor().reporting_waits_to(&waits);
                let built = plan.build(context.clone(), Arc::new(template.clone()), executor);
                RestoreBuild::Building(Box::pin(built))
            }
        };
        ProcessorBranchRestoreAttempt { waits, build }
    }

    /// The lifecycle this restore's branches belong to, with the lifetime each resumes, or nothing
    /// when the processor never formed a lifecycle and the restore names no handed-over branch.
    fn plan(
        &self,
        context: &ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
    ) -> error_stack::Result<Option<RestorePlan>, ProcessorBranchTaskError> {
        let runtime = &context.runtime_handle;
        let placement = branch_lru_placement(runtime, &context.domain, template)
            .change_context(ProcessorBranchTaskError::ReadLruSnapshot)?;
        let lifecycle = runtime
            .restorable_branch_lru_snapshot(&placement)
            .change_context(ProcessorBranchTaskError::ReadLruSnapshot)?;
        match &self.source {
            ProcessorBranchRestoreSource::Lifecycle => {
                let Some(lifecycle) = lifecycle else {
                    return Ok(None);
                };
                let lifetimes = lifecycle
                    .branches()
                    .change_context(ProcessorBranchTaskError::DecodeLruSnapshot)?;
                Ok(Some(RestorePlan {
                    lifecycle,
                    lifetimes,
                }))
            }
            ProcessorBranchRestoreSource::Handoffs(handoffs) => {
                let Some(lifecycle) = lifecycle else {
                    return Err(Report::new(
                        ProcessorBranchTaskError::HandedOffLifecycleUnavailable,
                    ));
                };
                let mut lifetimes = Vec::with_capacity(handoffs.len());
                for handoff in handoffs {
                    if handoff.incarnation > lifecycle.lsm() {
                        return Err(Report::new(
                            ProcessorBranchTaskError::HandedOffLifetimeAfterLifecycle {
                                branch: BranchScope::from(&handoff.key),
                                incarnation: handoff.incarnation,
                                lsm: lifecycle.lsm(),
                            },
                        ));
                    }
                    lifetimes.push(BranchInstanceSnapshotEntry {
                        key: handoff.key.clone(),
                        last_ingestion: handoff.restored_at,
                        incarnation: handoff.incarnation,
                    });
                }
                Ok(Some(RestorePlan {
                    lifecycle,
                    lifetimes,
                }))
            }
        }
    }

    /// Install every branch `attempt` built into `instances` and release the lifecycle that names
    /// them, all of them or none.
    ///
    /// A failed attempt leaves `instances`, the lifecycle and the handed-over input as they were,
    /// and schedules the next attempt after the backoff.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one finished restore attempt installs the processor's retained branch \
                      lifetimes"
        )
    )]
    pub(super) fn finish(
        &mut self,
        attempt: ProcessorBranchRestoreAttempt,
        context: &ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
        instances: &mut ProcessorBranches,
        last_persisted_lru_lsm: &mut u64,
    ) -> error_stack::Result<(), ProcessorBranchTaskError> {
        let built = attempt
            .into_built()
            .verified("the task finishes an attempt only once its build has ended");
        let built = match built {
            Ok(built) => built,
            Err(error) => {
                self.retry_at = Instant::now() + self.backoff.take_next_delay();
                return Err(error);
            }
        };
        if let Some(built) = built {
            // The restored lifecycle is the revision this node last persisted or had transferred
            // to it, so the task writes it again only once it changes.
            *last_persisted_lru_lsm = built.start(context, template, instances, &mut self.source);
        }
        Ok(())
    }
}

impl ProcessorBranchRestoreAttempt {
    /// Build until the attempt has finished or waits for capacity.
    ///
    /// Its task answers no command meanwhile: an attempt that makes progress keeps every command
    /// behind it, as a restore its task awaited inline did.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one restore attempt builds the processor's retained branch lifetimes"
        )
    )]
    pub(super) async fn run_until_waiting(&mut self) -> RestoreProgress {
        let Self { waits, build } = self;
        let RestoreBuild::Building(building) = build else {
            return RestoreProgress::Built;
        };
        let built = nervix_primitives::select! {
            biased;
            built = building.as_mut() => Some(built),
            () = waits.began() => None,
        };
        match built {
            Some(built) => {
                *build = RestoreBuild::Built(built);
                RestoreProgress::Built
            }
            None => RestoreProgress::Waiting,
        }
    }

    /// Build while the attempt waits for capacity, and return once it no longer does: it took its
    /// place or its room and goes on, or it finished.
    ///
    /// The task answers its commands beside this future and may drop it between them: the build
    /// it drives stays in the attempt, and so does its place in line.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one restore attempt waits for the capacity its retained branch lifetimes \
                      need"
        )
    )]
    pub(super) async fn until_no_longer_waiting(&mut self) {
        let Self { waits, build } = self;
        let RestoreBuild::Building(building) = build else {
            return;
        };
        let built = nervix_primitives::select! {
            biased;
            built = building.as_mut() => Some(built),
            () = waits.ended() => None,
        };
        if let Some(built) = built {
            *build = RestoreBuild::Built(built);
        }
    }

    /// What the attempt built, once its build has ended.
    fn into_built(self) -> Option<RestoreBuilt> {
        match self.build {
            RestoreBuild::Built(built) => Some(built),
            RestoreBuild::Building(_) => None,
        }
    }
}

impl RestorePlan {
    /// Build every branch the plan names under the lifetime it resumes, opening its retained state
    /// on `executor` and waiting for a place in a full class rather than being refused.
    async fn build(
        self,
        context: ProcessorRuntimeContext,
        template: Arc<BranchInstanceTemplate>,
        executor: Executor,
    ) -> RestoreBuilt {
        let Self {
            lifecycle,
            lifetimes,
        } = self;
        context
            .runtime_handle
            .inner
            .fault_injection
            .pause_processor_branch_restore_if_armed(&context.domain, &template.owner.node)
            .await;
        let opening = RetainedStateOpening::ReportingWaits(executor);
        let mut branches = Vec::with_capacity(lifetimes.len());
        for lifetime in lifetimes {
            nervix_primitives::task::consume_budget().await;
            let branch = PreparedProcessorBranch::prepare(
                context.clone(),
                &template,
                lifetime.key.clone(),
                ProcessorBranchLifetime::Restored,
                lifetime.incarnation,
                &opening,
            )
            .await?;
            branches.push(BuiltBranch { branch, lifetime });
        }
        Ok(Some(BuiltBranches {
            lifecycle,
            branches,
        }))
    }
}

impl BuiltBranches {
    /// Start every built branch under its lifetime, with the input a handed-over branch still had
    /// pending, then release the lifecycle they resume and return its revision.
    fn start(
        self,
        context: &ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
        instances: &mut ProcessorBranches,
        source: &mut ProcessorBranchRestoreSource,
    ) -> u64 {
        let Self {
            lifecycle,
            branches,
        } = self;
        let runtime = &context.runtime_handle;
        let mut handed_over = match source {
            ProcessorBranchRestoreSource::Lifecycle => None,
            ProcessorBranchRestoreSource::Handoffs(handoffs) => {
                Some(std::mem::take(handoffs).into_iter())
            }
        };
        for built in branches {
            let pending_materialized = match handed_over.as_mut() {
                Some(handoffs) => {
                    let handoff = handoffs.next().verified(
                        "an attempt builds one branch for each handoff it began with, in their \
                         order, and only this install takes them",
                    );
                    handoff.pending_materialized
                }
                None => VecDeque::new(),
            };
            let task = built.branch.start(pending_materialized);
            runtime.observe_branch_instance_created(
                &context.domain,
                template.parametrizer.as_ref(),
                &built.lifetime.key,
            );
            instances.insert_restored(
                built.lifetime.key,
                built.lifetime.last_ingestion,
                built.lifetime.incarnation,
                ProcessorBranchSlot::Live(task),
            );
        }
        let lsm = lifecycle.lsm();
        instances.set_version(lsm);
        runtime.release_restored_branch_lru_snapshot(lifecycle);
        lsm
    }
}

#[cfg(test)]
#[path = "processor_branch_restore_tests.rs"]
mod tests;
