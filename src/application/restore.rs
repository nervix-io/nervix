//! Restoring configuration, users and resources from a backup archive.
//!
//! Layer: control plane.
//!
//! - **Owns.** Serving one restore stream: staging and verifying its archive, planning the restore
//!   before anything changes, admitting it as a persistent command under its execution reference,
//!   applying its steps in their fixed order with each step recorded in consensus, and the report
//!   the restore answers with. A dry run stops after planning and records nothing.
//! - **Depends on.** The archive format's reader, the language layer to parse each domain's
//!   models, the decision layer's restore and transaction planners, consensus for admission and
//!   step progress, resource imports, the direct model batch, and the staging area.
//! - **Must not know.** Which transport carried the stream, or how a client reads or stores its
//!   archive.
//!
//! Nothing is written before the whole archive has verified and the whole restore has planned. A
//! restore is then a persistent command whose effect records every step it applies, so a new
//! leader, or a retry that sends the archive again, resumes it from the first step not recorded
//! and never applies a recorded step twice. Only the node a client streamed the archive to holds
//! it; a leader without it waits for the client to send it again.

mod archives;
mod prepare;
mod runner;
#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;
mod steps;

use std::collections::BTreeSet;

use arch_into::ArchInto as _;
use error_stack::Report;
use futures_util::Stream;
use meticulous::OptionExt as _;
use nervix_backup::DescribedRuntimeState;
use nervix_consensus::{CommandExecution, RestoreExecution};
use nervix_models::{
    ClusterSchedule, CommandExecutionReference, DomainName, ModelKind, NodeRef, Restore,
    RestoreArchive, RestoreMode, RestoreReport, RestoreScope, RestoreState, RestoreStep,
    RestoreStepOutcome, RestoreStepReport, RestoredDomain, RestoredUsers, SchemaFingerprint,
    Timestamp, UserName,
};
use tracing::info;

use self::{
    archives::StagingOutcome,
    prepare::RestoreRefusal,
    runner::{RestoreRunEnd, run_restore_steps},
    steps::ServerRestoreSteps,
};
pub(in crate::application) use self::{
    archives::{RestoreArchives, RestoreStreamPart, StagingRefusal},
    prepare::VerifiedArchive,
};
use super::{
    command_execution::{CommandAdmission, CommandExecutionOwnerGuard, PersistentCommandRequest},
    command_result::{
        CommandDiagnostic, CommandDisposition, CommandOrigin, CommandResponse, CommandResult,
        OutcomeUnknownCause,
    },
    domain_clock::current_timestamp,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
    subscription::SessionSubscriptions,
};
use crate::{registry::RestorePlan, runtime::StagedArtifact};

/// The archives this node's restores read, as the server stages them.
pub(in crate::application) type ServerRestoreArchives = RestoreArchives<VerifiedArchive>;

/// The restore a stream's start asks for.
pub(in crate::application) struct RestoreRequest {
    pub(in crate::application) reference: CommandExecutionReference,
    pub(in crate::application) restore: Restore,
    pub(in crate::application) archive: RestoreArchive,
}

/// How a restore stream is answered.
pub(in crate::application) enum RestoreAnswer {
    /// The outcome of the restore, as the persistent command it is.
    Outcome(Box<CommandResponse>),
    /// The stream did not carry the archive its start declared, and no restore ran.
    Refused(StagingRefusal),
}

impl RestoreAnswer {
    fn outcome(response: CommandResponse) -> Self {
        Self::Outcome(Box::new(response))
    }
}

impl SessionServiceImpl {
    /// Serves the restore `request` names from the archive `parts` carry. A transport failure while
    /// the stream is read is returned as the transport's own error.
    pub(in crate::application) async fn serve_restore_stream<P, E>(
        &self,
        user: UserName,
        request: RestoreRequest,
        parts: P,
    ) -> Result<RestoreAnswer, E>
    where
        P: Stream<Item = Result<RestoreStreamPart, E>> + Unpin + Send,
    {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            let redirect = self.not_leader_response("", leader).await;
            return Ok(RestoreAnswer::outcome(CommandResponse::executed(redirect)));
        }
        match request.restore.mode {
            RestoreMode::DryRun => self.dry_run_restore(user, request, parts).await,
            RestoreMode::Apply => self.apply_restore_stream(user, request, parts).await,
        }
    }

    /// Stages and verifies the archive, plans the restore, and answers with the plan. Nothing is
    /// recorded, and the archive is released when the answer is built.
    async fn dry_run_restore<P, E>(
        &self,
        user: UserName,
        request: RestoreRequest,
        parts: P,
    ) -> Result<RestoreAnswer, E>
    where
        P: Stream<Item = Result<RestoreStreamPart, E>> + Unpin + Send,
    {
        let staged = match self.stage_restore_stream(request.archive, parts).await {
            StagingOutcome::Staged(artifact) => artifact,
            StagingOutcome::Refused(refusal) => return Ok(RestoreAnswer::Refused(refusal)),
            StagingOutcome::Transport(error) => return Err(error),
        };
        let verified = match self.verify_restore_archive(staged).await {
            Ok(verified) => verified,
            Err(refusal) => {
                return Ok(RestoreAnswer::outcome(CommandResponse::executed(
                    restore_refused(&refusal),
                )));
            }
        };
        let result = match self.plan_dry_run(&user, &request, &verified).await {
            Ok(report) => {
                let message = dry_run_message(&request.restore, &report);
                CommandResult {
                    restore: Some(Box::new(report)),
                    ..command_ok(message)
                }
            }
            Err(refusal) => restore_refused(&refusal),
        };
        Ok(RestoreAnswer::outcome(CommandResponse::executed(result)))
    }

    /// Plans a dry run: the restore, and the transaction planner's report on each domain's model
    /// run.
    async fn plan_dry_run(
        &self,
        user: &UserName,
        request: &RestoreRequest,
        verified: &VerifiedArchive,
    ) -> Result<RestoreReport, Report<RestoreRefusal>> {
        let plan = self
            .plan_restore(&request.restore, verified, &BTreeSet::new())
            .await?;
        let impacts = self.plan_model_runs(&plan, user).await?;
        let mut domains = Vec::with_capacity(plan.domains.len());
        for (domain, planned_models) in plan.domains.values().zip(impacts) {
            domains.push(RestoredDomain {
                source: domain.source.clone(),
                domain: domain.target.clone(),
                resource_versions: count(domain.versions.len()),
                models: count(domain.models.len()),
                planned_models,
            });
        }
        let steps = plan
            .steps()
            .into_iter()
            .map(|step| RestoreStepReport {
                step,
                outcome: RestoreStepOutcome::Planned,
            })
            .collect();
        let users = plan.users.as_ref().map(|users| users.outcome);
        Ok(RestoreReport {
            mode: RestoreMode::DryRun,
            archive: request.archive,
            captured_at: verified.captured_at(),
            users,
            domains,
            steps,
        })
    }

    /// Serves a restore that applies: stages the archive unless this node already holds it or
    /// the restore already ended, and then verifies it, plans a new restore before admitting it,
    /// and runs the command in a task of its own, so all of it goes on once the archive arrived,
    /// whether or not the caller waits.
    async fn apply_restore_stream<P, E>(
        &self,
        user: UserName,
        request: RestoreRequest,
        parts: P,
    ) -> Result<RestoreAnswer, E>
    where
        P: Stream<Item = Result<RestoreStreamPart, E>> + Unpin + Send,
    {
        let existing = self
            .inner
            .consensus
            .current_command_execution(&request.reference)
            .await;
        if let Some(execution) = &existing
            && execution.is_applying()
            && self.inner.restore_archives.retains(&request.reference)
        {
            // This node holds the archive, so the restore is applying here, under a caller that
            // waits for it or under reconciliation. The stream is answered at once rather than
            // held open while it applies; a later repetition returns its outcome.
            return Ok(RestoreAnswer::outcome(
                self.restore_still_applying(&user, &request).await,
            ));
        }
        let needs_archive = match &existing {
            None => true,
            Some(execution) => execution.is_applying(),
        };
        let mut staged = None;
        if needs_archive {
            match self.stage_restore_stream(request.archive, parts).await {
                StagingOutcome::Staged(artifact) => staged = Some(artifact),
                StagingOutcome::Refused(refusal) => return Ok(RestoreAnswer::Refused(refusal)),
                StagingOutcome::Transport(error) => return Err(error),
            }
        }
        // The whole archive arrived, so the restore no longer depends on the caller: it is
        // verified, planned and applied in a task of its own, which goes on when the caller
        // disconnects.
        let new_restore = existing.is_none();
        let service = self.clone();
        let command = self.inner.service_tasks.spawn(async move {
            service
                .admit_and_run_restore(user, request, new_restore, staged)
                .await
        });
        let response = match command.await {
            Ok(Some(response)) => response,
            Ok(None) => CommandResponse::executed(command_error(
                "the node shut down before the restore finished; send it again with the same \
                 execution reference to learn its outcome"
                    .to_string(),
            )),
            Err(error) => CommandResponse::executed(command_error(format!(
                "the restore task failed: {error}"
            ))),
        };
        Ok(RestoreAnswer::outcome(response))
    }

    /// Verifies the archive a stream staged, plans a restore not admitted yet, and runs the
    /// restore command. Every refusal happens before the restore writes anything.
    async fn admit_and_run_restore(
        &self,
        user: UserName,
        request: RestoreRequest,
        new_restore: bool,
        staged: Option<StagedArtifact>,
    ) -> CommandResponse {
        let mut verified = None;
        if let Some(artifact) = staged {
            match self.verify_restore_archive(artifact).await {
                Ok(archive) => verified = Some(archive),
                Err(refusal) => return CommandResponse::executed(restore_refused(&refusal)),
            }
        }
        let mut targets = BTreeSet::new();
        if new_restore && let Some(archive) = &verified {
            match self
                .plan_new_restore(&user, &request.restore, archive)
                .await
            {
                Ok(plan) => targets = plan.target_domains(),
                Err(refusal) => return CommandResponse::executed(restore_refused(&refusal)),
            }
        }
        self.run_restore_command(user, request, targets, verified)
            .await
    }

    /// The answer to a repetition of a restore still applying on this node: its identity is
    /// checked against the admitted restore, and its outcome is not known yet.
    async fn restore_still_applying(
        &self,
        user: &UserName,
        request: &RestoreRequest,
    ) -> CommandResponse {
        let persistent = match PersistentCommandRequest::restore(
            request.restore.clone(),
            request.archive,
            BTreeSet::new(),
        ) {
            Ok(persistent) => persistent,
            Err(error) => return CommandResponse::executed(command_error(format!("{error:#}"))),
        };
        match self
            .admit_persistent_command(request.reference.clone(), user.clone(), &persistent)
            .await
        {
            Ok(CommandAdmission::Admitted(_) | CommandAdmission::Existing(_)) => {
                let message = format!(
                    "restore '{}' is still applying; send it again with the same execution \
                     reference to learn its outcome",
                    request.reference
                );
                CommandResponse {
                    result: CommandResult {
                        diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
                        ..CommandResult::new(
                            CommandDisposition::OutcomeUnknown(OutcomeUnknownCause::StillApplying),
                            message,
                        )
                    },
                    origin: CommandOrigin::Recovered,
                }
            }
            Err(result) => CommandResponse::executed(*result),
        }
    }

    /// Plans a restore not yet admitted, including each domain's model run, so every refusal
    /// happens before the restore writes anything.
    async fn plan_new_restore(
        &self,
        user: &UserName,
        restore: &Restore,
        verified: &VerifiedArchive,
    ) -> Result<RestorePlan, Report<RestoreRefusal>> {
        let plan = self
            .plan_restore(restore, verified, &BTreeSet::new())
            .await?;
        self.plan_model_runs(&plan, user).await?;
        Ok(plan)
    }

    /// Admits the restore, or joins its earlier admission, retains the archive this stream staged
    /// for it, and runs it to its outcome.
    async fn run_restore_command(
        &self,
        user: UserName,
        request: RestoreRequest,
        targets: BTreeSet<DomainName>,
        verified: Option<VerifiedArchive>,
    ) -> CommandResponse {
        let persistent = match PersistentCommandRequest::restore(
            request.restore.clone(),
            request.archive,
            targets,
        ) {
            Ok(persistent) => persistent,
            Err(error) => return CommandResponse::executed(command_error(format!("{error:#}"))),
        };
        let _owner = self
            .inner
            .command_executions
            .lock(request.reference.clone())
            .await;
        let admission = self
            .admit_persistent_command(request.reference.clone(), user.clone(), &persistent)
            .await;
        let (execution, origin) = match admission {
            Ok(CommandAdmission::Admitted(execution)) => (execution, CommandOrigin::Executed),
            Ok(CommandAdmission::Existing(execution)) => (execution, CommandOrigin::Recovered),
            Err(result) => return CommandResponse::executed(*result),
        };
        if execution.is_applying()
            && let Some(verified) = verified
        {
            let Some(retained_until) = self.restore_retained_until(&request.reference) else {
                return CommandResponse::executed(command_error(format!(
                    "execution reference '{}' names no time its retry validity starts from",
                    request.reference
                )));
            };
            let retained = self.inner.restore_archives.retain(
                request.reference.clone(),
                user.clone(),
                request.archive,
                retained_until,
                verified,
            );
            if let Err(refusal) = retained {
                return CommandResponse::executed(command_error(refusal.to_string()));
            }
        }
        let mut subscriptions = SessionSubscriptions::for_user(user);
        let result = self
            .complete_persistent_command_request(execution, &mut subscriptions)
            .await;
        CommandResponse { result, origin }
    }

    /// The instant a restore's archive stops being retained: when the retry validity of its
    /// execution reference ends, after which no retry can resume it.
    fn restore_retained_until(&self, reference: &CommandExecutionReference) -> Option<Timestamp> {
        let issued_at = reference.retry_issued_at().ok()?;
        issued_at
            .checked_add(self.inner.command_execution_policy.retry_validity())
            .ok()
    }

    /// Whether no retry can send the archive of the restore `reference` names any more.
    pub(in crate::application) fn restore_archive_retry_ended(
        &self,
        reference: &CommandExecutionReference,
    ) -> bool {
        match self.restore_retained_until(reference) {
            Some(retained_until) => current_timestamp() >= retained_until,
            None => true,
        }
    }

    /// Ends an applying restore whose archive no retry can send any more where it stopped: the
    /// steps it recorded stay applied, and its outcome names the first step it did not reach.
    pub(in crate::application) fn finish_restore_without_archive(
        &self,
        execution: CommandExecution,
        owner_guard: CommandExecutionOwnerGuard,
    ) {
        let service = self.clone();
        self.inner.service_tasks.spawn(async move {
            let _owner_guard = owner_guard;
            let recorded = match execution.restore_execution() {
                Some(restore) => restore.completed_steps().len(),
                None => 0,
            };
            let message = format!(
                "restore stopped after the steps it recorded: its archive was not sent to this \
                 leader again before the retry validity of execution reference '{}' ended; the \
                 {recorded} steps it applied stay applied",
                execution.reference
            );
            let result = CommandResult {
                diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
                ..command_error(message)
            };
            let owner = execution
                .owner()
                .verified("the reconciliation index contains only applying executions")
                .clone();
            let digest = execution
                .request_digest()
                .verified("the reconciliation index contains only applying executions");
            if let Err(error) = service
                .finish_persistent_command(execution.reference.clone(), owner, digest, &result)
                .await
            {
                service.broadcast_error(format!(
                    "failed to record the end of restore '{}': {}",
                    execution.reference, error.message
                ));
            }
        });
    }

    /// Applies every step of the admitted restore `execution` records that is not recorded yet, in
    /// order, from the archive this node retains for it.
    pub(in crate::application) async fn execute_restore(
        &self,
        execution: &CommandExecution,
        restore: RestoreExecution,
    ) -> CommandResult {
        let owner = execution
            .owner()
            .verified("an applying execution retains its owner")
            .clone();
        let Some(archive) =
            self.inner
                .restore_archives
                .get(&execution.reference, &owner, &restore.archive)
        else {
            return restore_failed_before_steps(format!(
                "this leader no longer holds the archive of execution reference '{}'",
                execution.reference
            ));
        };
        let plan = match self
            .plan_restore(&restore.restore, &archive, restore.completed_steps())
            .await
        {
            Ok(plan) => plan,
            Err(refusal) => return restore_failed_before_steps(format!("{refusal:#}")),
        };
        let steps = plan.steps();
        let effects = ServerRestoreSteps {
            service: self,
            execution,
            plan: &plan,
            archive: &archive,
            state: restore.restore.state,
        };
        let run = run_restore_steps(&steps, restore.completed_steps(), &effects).await;
        let outcomes = run.steps;
        match run.end {
            RestoreRunEnd::Completed => {}
            RestoreRunEnd::LeadershipLost(redirect) => return *redirect,
            RestoreRunEnd::Failed { step, reason } => {
                let restored = self.restored_users(&execution.reference).await;
                let report = restore_report(&restore, &plan, &archive, restored, outcomes);
                return restore_failed_at(&step, &reason, report);
            }
        }
        let restored = self.restored_users(&execution.reference).await;
        let report = restore_report(&restore, &plan, &archive, restored, outcomes);
        let schedule = self.inner.consensus.current_schedule().await;
        let warnings = restore_state_warnings(&plan, &archive, &schedule, restore.restore.state);
        let message = restore_message(&restore.restore, &report);
        info!(
            execution_reference = %execution.reference,
            domains = report.domains.len(),
            "restored backup archive"
        );
        CommandResult {
            restore: Some(Box::new(report)),
            diagnostics: warnings,
            ..command_ok(message)
        }
    }

    /// What the users step of the restore `reference` names did, as consensus recorded it.
    async fn restored_users(&self, reference: &CommandExecutionReference) -> Option<RestoredUsers> {
        let execution = self
            .inner
            .consensus
            .current_command_execution(reference)
            .await?;
        execution.restore_execution()?.restored_users()
    }

    /// Releases every restore archive whose retry validity has ended.
    pub(in crate::application) fn sweep_restore_archives(&self) {
        self.inner.restore_archives.sweep(current_timestamp());
    }
}

/// A count of planned items, as the report carries it.
fn count(items: usize) -> u64 {
    items.arch_into()
}

/// The report of a restore that applied `outcomes`.
fn restore_report(
    restore: &RestoreExecution,
    plan: &RestorePlan,
    archive: &VerifiedArchive,
    users: Option<RestoredUsers>,
    outcomes: Vec<RestoreStepReport>,
) -> RestoreReport {
    let domains = plan
        .domains
        .values()
        .map(|domain| RestoredDomain {
            source: domain.source.clone(),
            domain: domain.target.clone(),
            resource_versions: count(domain.versions.len()),
            models: count(domain.models.len()),
            planned_models: None,
        })
        .collect();
    RestoreReport {
        mode: RestoreMode::Apply,
        archive: restore.archive,
        captured_at: archive.captured_at(),
        users,
        domains,
        steps: outcomes,
    }
}

/// Skips are a successful restore outcome, but the operator must see which state was not
/// installed. Derive the diagnostics from the verified archive and published schedule so a
/// resumed command reports the same warnings even when its state step was already recorded.
fn restore_state_warnings(
    plan: &RestorePlan,
    archive: &VerifiedArchive,
    schedule: &ClusterSchedule,
    state: RestoreState,
) -> Vec<CommandDiagnostic> {
    if state == RestoreState::ConfigurationOnly {
        return Vec::new();
    }
    let mut warnings = Vec::new();
    for domain in plan.domains.values() {
        for skipped in archive.skipped_state_for(&domain.source) {
            if state == RestoreState::WithoutSourceOffsets
                && skipped.path.as_str().contains("/state/kafka_offset/")
            {
                continue;
            }
            warnings.push(CommandDiagnostic::unlocated(format!(
                "warning: skipped state section '{}' while restoring domain '{}': {}",
                skipped.path, domain.target, skipped.reason,
            )));
        }
        for archived in archive.states_for(&domain.source) {
            let (kind, entity, schema) = match archived {
                DescribedRuntimeState::Wasm { descriptor, .. } => (
                    ModelKind::WasmProcessor,
                    &descriptor.entity,
                    Some(descriptor.schema),
                ),
                DescribedRuntimeState::KafkaOffsets { offsets, .. } => {
                    if state == RestoreState::WithoutSourceOffsets {
                        continue;
                    }
                    (ModelKind::Ingestor, &offsets.entity, Some(offsets.schema))
                }
                DescribedRuntimeState::BranchLifecycle { lifecycle, .. } => (
                    lifecycle.owner_kind,
                    &lifecycle.entity,
                    Some(lifecycle.schema),
                ),
            };
            let node = schedule
                .domain(&domain.target)
                .and_then(|scheduled| scheduled.nodes.get(&NodeRef::new(kind, entity.clone())));
            let reason = state_skip_reason(schema, node.map(|node| node.schema_fingerprint));
            if let Some(reason) = reason {
                warnings.push(CommandDiagnostic::unlocated(format!(
                    "warning: skipped {} state '{}' in domain '{}': {reason}",
                    kind.as_str(),
                    entity,
                    domain.target,
                )));
            }
        }
    }
    warnings
}

fn state_skip_reason(
    archived: Option<SchemaFingerprint>,
    published: Option<SchemaFingerprint>,
) -> Option<&'static str> {
    match published {
        None => Some("the entity is absent from the restored schedule"),
        Some(published) if archived.is_some_and(|archived| archived != published) => {
            Some("the archived schema fingerprint does not match the restored schedule")
        }
        Some(_) => None,
    }
}

/// A restore refused before it wrote anything.
fn restore_refused(refusal: &Report<RestoreRefusal>) -> CommandResult {
    let message = format!("restore refused: {refusal:#}");
    CommandResult {
        diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
        ..command_error(message)
    }
}

/// An admitted restore that cannot reach its first step not yet recorded.
fn restore_failed_before_steps(reason: String) -> CommandResult {
    let message = format!("restore failed: {reason}; the steps it recorded stay applied");
    CommandResult {
        diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
        ..command_error(message)
    }
}

/// A restore that failed at `step`, with the report of what it applied before it.
fn restore_failed_at(step: &RestoreStep, reason: &str, report: RestoreReport) -> CommandResult {
    let message =
        format!("restore failed at step '{step}': {reason}; the steps before it stay applied");
    CommandResult {
        diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
        restore: Some(Box::new(report)),
        ..CommandResult::new(CommandDisposition::Failed, message)
    }
}

/// What a restore's scope names, for its messages.
fn scope_label(restore: &Restore) -> String {
    match &restore.scope {
        RestoreScope::Cluster { .. } => "the cluster".to_string(),
        RestoreScope::Domain {
            domain,
            target: Some(target),
        } => format!("domain '{domain}' as '{target}'"),
        RestoreScope::Domain {
            domain,
            target: None,
        } => format!("domain '{domain}'"),
    }
}

/// The counts a report sums over its domains, and its users, as a message lists them.
fn report_counts(report: &RestoreReport) -> String {
    let mut versions = 0_u64;
    let mut models = 0_u64;
    for domain in &report.domains {
        versions = versions.checked_add(domain.resource_versions).assured(
            "the versions of every domain are counted from lists this node holds in memory",
        );
        models = models
            .checked_add(domain.models)
            .assured("the models of every domain are counted from lists this node holds in memory");
    }
    let users = match &report.users {
        Some(users) => format!(
            ", users {} created, {} skipped, {} replaced",
            users.created, users.skipped, users.replaced
        ),
        None => String::new(),
    };
    format!(
        "{} domains, {versions} resource versions, {models} models{users}",
        report.domains.len()
    )
}

fn restore_message(restore: &Restore, report: &RestoreReport) -> String {
    format!(
        "restored {} from '{}': {}; restored domains are stopped",
        scope_label(restore),
        restore.source,
        report_counts(report)
    )
}

fn dry_run_message(restore: &Restore, report: &RestoreReport) -> String {
    format!(
        "dry run: restoring {} from '{}' would create {}; nothing was changed",
        scope_label(restore),
        restore.source,
        report_counts(report)
    )
}

#[cfg(test)]
mod backup_state_warning_tests {
    use super::*;

    #[test]
    fn a_restore_reports_only_state_the_schedule_will_skip() {
        let archived = SchemaFingerprint::from_digest([1; 32]);
        let published = SchemaFingerprint::from_digest([2; 32]);
        assert_eq!(
            state_skip_reason(Some(archived), None),
            Some("the entity is absent from the restored schedule")
        );
        assert_eq!(
            state_skip_reason(Some(archived), Some(published)),
            Some("the archived schema fingerprint does not match the restored schedule")
        );
        assert_eq!(state_skip_reason(Some(archived), Some(archived)), None);
        assert_eq!(state_skip_reason(None, Some(published)), None);
    }
}
