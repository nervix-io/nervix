//! Restoring the concrete branches a processor task owns before the task accepts input.
//!
//! Layer: data plane.
//!
//! - **Owns.** Installing every branch a processor's branch lifecycle checkpoint or its previous
//!   task's handoff names, all of them or none, releasing that lifecycle only once they are
//!   installed, and the backoff between attempts while a restore stays pending.
//! - **Depends on.** Planned processor templates, processor branch startup, the runtime's
//!   restorable branch lifecycle and the runtime's retry backoff.
//! - **Must not know.** NSPL text, control-plane transactions, consensus, or connector protocols.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "restoring a processor's branches installs their retained execution state when \
                  its task starts"
    )
)]

use error_stack::ResultExt as _;

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

/// One branch a restore has built and not started yet, with the lifetime it resumes.
struct RestoredProcessorBranch {
    branch: PreparedProcessorBranch,
    key: Option<BranchKey>,
    last_ingestion: Timestamp,
    incarnation: u64,
    pending_materialized: VecDeque<PendingMaterializedBatch>,
}

impl PendingProcessorBranchRestore {
    /// The restore a new processor task performs first: the branches its previous task handed
    /// over, or the branches its lifecycle checkpoint names when there are none.
    pub(super) fn new(handoffs: Vec<ProcessorBranchHandoff>) -> Self {
        let source = if handoffs.is_empty() {
            ProcessorBranchRestoreSource::Lifecycle
        } else {
            ProcessorBranchRestoreSource::Handoffs(handoffs)
        };
        Self {
            source,
            backoff: RuntimeReconnectBackoff::default(),
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

    /// Install every branch this restore names into `instances`, or none of them.
    ///
    /// Every branch is built before any starts, and the lifecycle is released only once all of
    /// them run. A failure leaves `instances`, the lifecycle and the handed-over input as they
    /// were, and schedules the next attempt.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one restore attempt installs the processor's retained branch lifetimes"
        )
    )]
    pub(super) async fn attempt(
        &mut self,
        context: &ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
        instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
        last_persisted_lru_lsm: &mut u64,
    ) -> error_stack::Result<(), ProcessorBranchTaskError> {
        let attempt = self
            .install(context, template, instances, last_persisted_lru_lsm)
            .await;
        if attempt.is_err() {
            self.retry_at = Instant::now() + self.backoff.take_next_delay();
        }
        attempt
    }

    async fn install(
        &mut self,
        context: &ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
        instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
        last_persisted_lru_lsm: &mut u64,
    ) -> error_stack::Result<(), ProcessorBranchTaskError> {
        let runtime = &context.runtime_handle;
        let placement = branch_lru_placement(runtime, &context.domain, template)
            .change_context(ProcessorBranchTaskError::ReadLruSnapshot)?;
        let lifecycle = runtime
            .restorable_branch_lru_snapshot(&placement)
            .change_context(ProcessorBranchTaskError::ReadLruSnapshot)?;
        let restored = match &mut self.source {
            ProcessorBranchRestoreSource::Lifecycle => {
                let Some(lifecycle) = lifecycle else {
                    // The processor has never formed a branch lifecycle, so it has no branch to
                    // restore.
                    return Ok(());
                };
                RestoredBranches::from_lifecycle(context, template, lifecycle).await?
            }
            ProcessorBranchRestoreSource::Handoffs(handoffs) => {
                let Some(lifecycle) = lifecycle else {
                    return Err(Report::new(
                        ProcessorBranchTaskError::HandedOffLifecycleUnavailable,
                    ));
                };
                RestoredBranches::from_handoffs(context, template, lifecycle, handoffs).await?
            }
        };
        // The restored lifecycle is the revision this node last persisted or had transferred to
        // it, so the task writes it again only once it changes.
        *last_persisted_lru_lsm = restored.start(context, template, instances);
        Ok(())
    }
}

/// Every branch one restore attempt built, and the lifecycle that names them.
struct RestoredBranches {
    lifecycle: RestorableBranchLifecycle,
    branches: Vec<RestoredProcessorBranch>,
}

impl RestoredBranches {
    /// Build every branch `lifecycle` names under the lifetime it records.
    async fn from_lifecycle(
        context: &ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
        lifecycle: RestorableBranchLifecycle,
    ) -> error_stack::Result<Self, ProcessorBranchTaskError> {
        let entries = lifecycle
            .branches()
            .change_context(ProcessorBranchTaskError::DecodeLruSnapshot)?;
        let mut branches = Vec::with_capacity(entries.len());
        for entry in entries {
            nervix_primitives::task::consume_budget().await;
            let branch = PreparedProcessorBranch::prepare(
                context.clone(),
                template,
                entry.key.clone(),
                ProcessorBranchLifetime::Restored,
                entry.incarnation,
            )
            .await?;
            branches.push(RestoredProcessorBranch {
                branch,
                key: entry.key,
                last_ingestion: entry.last_ingestion,
                incarnation: entry.incarnation,
                pending_materialized: VecDeque::new(),
            });
        }
        Ok(Self {
            lifecycle,
            branches,
        })
    }

    /// Build every branch in `handoffs` under the lifetime it was handed over with, then take the
    /// input each holds. A failure leaves `handoffs` as they were.
    async fn from_handoffs(
        context: &ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
        lifecycle: RestorableBranchLifecycle,
        handoffs: &mut Vec<ProcessorBranchHandoff>,
    ) -> error_stack::Result<Self, ProcessorBranchTaskError> {
        for handoff in handoffs.iter() {
            if handoff.incarnation > lifecycle.lsm() {
                return Err(Report::new(
                    ProcessorBranchTaskError::HandedOffLifetimeAfterLifecycle {
                        branch: BranchScope::from(&handoff.key),
                        incarnation: handoff.incarnation,
                        lsm: lifecycle.lsm(),
                    },
                ));
            }
        }
        let mut prepared = Vec::with_capacity(handoffs.len());
        for handoff in handoffs.iter() {
            nervix_primitives::task::consume_budget().await;
            let branch = PreparedProcessorBranch::prepare(
                context.clone(),
                template,
                handoff.key.clone(),
                ProcessorBranchLifetime::Restored,
                handoff.incarnation,
            )
            .await?;
            prepared.push(branch);
        }
        let mut branches = Vec::with_capacity(prepared.len());
        for (handoff, branch) in std::mem::take(handoffs).into_iter().zip(prepared) {
            branches.push(RestoredProcessorBranch {
                branch,
                key: handoff.key,
                last_ingestion: handoff.restored_at,
                incarnation: handoff.incarnation,
                pending_materialized: handoff.pending_materialized,
            });
        }
        Ok(Self {
            lifecycle,
            branches,
        })
    }

    /// Start every built branch under its lifetime, then release the lifecycle they resume and
    /// return its revision.
    fn start(
        self,
        context: &ProcessorRuntimeContext,
        template: &BranchInstanceTemplate,
        instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
    ) -> u64 {
        let Self {
            lifecycle,
            branches,
        } = self;
        let runtime = &context.runtime_handle;
        for restored in branches {
            let task = restored.branch.start(restored.pending_materialized);
            runtime.observe_branch_instance_created(
                &context.domain,
                template.branch.as_ref(),
                &restored.key,
            );
            instances.insert_restored(
                restored.key,
                restored.last_ingestion,
                restored.incarnation,
                task,
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
