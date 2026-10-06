//! Bounded generators of current consensus records: state-machine revisions, the Raft positions
//! and memberships they carry, and the replicated commands a log entry holds.
//!
//! Layer: test harness.
//! - **Owns.** Generated current consensus values, each built through the constructors and
//!   mutations the consensus crate itself uses, with every record keyed by the identity its value
//!   carries, and the checks that every variant of a command or record is generated.
//! - **Depends on.** The consensus record types and the vocabulary generators.
//! - **Must not know.** Storage, Raft scheduling or transport.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    time::Duration,
};

use nervix_arbitrary::{Arbitrary, Domain, StatementVariant};
use nervix_models::{
    ClusterNodeName, CommandExecutionReference, DomainName, DomainSchedule, DomainStartPoint,
    DomainState, DomainStatus, ExistingUserPolicy, ResetWasmState, ResourceId, ResourceName,
    ResourceReplicaKey, ResourceUploadKey, RestoreMode, RestoreStateAuthority, RestoreStep,
    Statement, Timestamp, TransactionCommitPlan, TransactionCommitStepKind,
    TransactionImpactReport, TransactionLifecycle, TransactionOperationAdmission,
    TransactionOperationNumber, TransactionOperationRange, TransactionPosition,
    TransactionPreviewIdentity, UserName,
};
use openraft::{BasicNode, Membership, StoredMembership, vote::RaftLeaderId as _};
use strum::IntoEnumIterator as _;

use super::*;
use crate::{
    AutomaticScheduleFence, CommandExecution, CommandExecutionAdmissionPolicy,
    CommandExecutionDiagnostic, CommandExecutionDisposition, CommandExecutionEffect,
    CommandExecutionPreviewStale, CommandExecutionResult, CommandExecutionState,
    CommandExecutionStatementDisposition, CommandExecutionStatementResult,
    CommandExecutionTransactionOperation, CommandExecutionTransactionRequest,
    CommandExecutionTransactionStatus, CommandExecutionTransactionTarget, DiagnosticSpan,
    DomainMutationOwner, DomainPlanningInputs, DomainResourcePlanningInputs, FinishedTransaction,
    LeaderTenure, ReplicatedTransaction, RestoreExecution, RestoreStepEffect, RestoredResource,
    ScheduleTopologyInputs, TransactionActivity, TransactionApplicationOutcome,
    TransactionApplyingStep, TransactionCommandResult, TransactionCommitAdmissionFailure,
    TransactionCommitAdmissionPlan, TransactionCommitPlanHeader, TransactionCommitProgress,
    TransactionDiagnostic, TransactionOutcome, TransactionQueueLimits, TransactionReportArchive,
    TransactionScheduleEligibility, TransactionState, TransactionStatement,
    TransactionStatementRequest, TransactionStepEffect, TransactionStepResult, UserCredentials,
    command_execution::CommandExecutionRecords, domain_mutation::DomainMutationLease,
    restore::DomainRestoreInstallation, transaction_plan::TransactionCommitPlanRecords,
    transaction_report::TransactionReportRecords,
};

/// The most records of one family a generated revision holds, and the most items of one list a
/// generated record holds.
const RECORDS: usize = 3;

/// The most statements a generated transaction queues.
const QUEUED: usize = 3;

/// The most consecutive statements one generated commit step applies.
const STEP_STATEMENTS: usize = 3;

/// A Raft log position under any term and leader.
pub(super) fn log_id(arbitrary: &mut Arbitrary<'_>) -> LogIdOf {
    let term = arbitrary.entropy().any_u64();
    let leader = arbitrary.rule_name::<ClusterNodeName>();
    let index = arbitrary.entropy().any_u64();
    LogIdOf::new(
        openraft::type_config::alias::CommittedLeaderIdOf::<TypeConfig>::new(term, leader),
        index,
    )
}

/// A cluster membership: up to four known nodes, any of which may be a learner, and no, one, or a
/// joint pair of non-empty voter configurations drawn from them.
pub(super) fn membership(arbitrary: &mut Arbitrary<'_>) -> Membership<ClusterNodeName, BasicNode> {
    let mut nodes = BTreeMap::new();
    for _ in 0..arbitrary.entropy().count(4) {
        let node = arbitrary.rule_name::<ClusterNodeName>();
        let address = arbitrary.string();
        nodes.insert(node, BasicNode::new(address));
    }
    let known = nodes.keys().cloned().collect::<Vec<_>>();
    let mut configurations = Vec::new();
    if !known.is_empty() {
        for _ in 0..arbitrary.entropy().count(2) {
            let mut voters = BTreeSet::new();
            for node in &known {
                if arbitrary.entropy().flag() {
                    voters.insert(node.clone());
                }
            }
            if voters.is_empty() {
                let first = known
                    .first()
                    .verified("the membership knows at least one node");
                voters.insert(first.clone());
            }
            configurations.push(voters);
        }
    }
    Membership::new(configurations, nodes)
        .assured("every configuration is non-empty and names only nodes the membership knows")
}

/// The membership a log position last committed, or none for a fresh cluster.
pub(super) fn stored_membership(arbitrary: &mut Arbitrary<'_>) -> StoredMembershipOf {
    let log_id = if arbitrary.entropy().flag() {
        Some(log_id(arbitrary))
    } else {
        None
    };
    StoredMembership::new(log_id, membership(arbitrary))
}

/// A value `build` builds, or none, each as likely as the other.
fn optional<'bytes, T>(
    arbitrary: &mut Arbitrary<'bytes>,
    build: impl FnOnce(&mut Arbitrary<'bytes>) -> T,
) -> Option<T> {
    if arbitrary.entropy().flag() {
        Some(build(arbitrary))
    } else {
        None
    }
}

/// Any count a `usize` holds, landing on zero and on the largest as often as elsewhere.
fn any_count(arbitrary: &mut Arbitrary<'_>) -> usize {
    let widest = u64::try_from(usize::MAX).assured("supported targets address at most 64 bits");
    let count = arbitrary.entropy().boundary_biased(0..=widest);
    usize::try_from(count).verified("the draw is at most usize::MAX")
}

/// An instant at or after `earliest`, landing on it and on the last instant a timestamp holds as
/// often as between them.
fn instant_from(arbitrary: &mut Arbitrary<'_>, earliest: Timestamp) -> Timestamp {
    let earliest = earliest.unix_nanos();
    let room = earliest.abs_diff(i64::MAX);
    let offset = arbitrary.entropy().boundary_biased(0..=room);
    let nanos = earliest
        .checked_add_unsigned(offset)
        .verified("the offset is at most the room left below i64::MAX");
    Timestamp::from_unix_nanos(nanos)
}

/// A leader's tenure: any node, in any term.
fn leader_tenure(arbitrary: &mut Arbitrary<'_>) -> LeaderTenure {
    let leader_id = arbitrary.rule_name();
    let term = arbitrary.entropy().any_u64();
    LeaderTenure { leader_id, term }
}

/// A user and the password hash it signs in with.
fn user_credentials(arbitrary: &mut Arbitrary<'_>) -> UserCredentials {
    let name = arbitrary.rule_name::<UserName>();
    let password_hash = arbitrary.string();
    UserCredentials {
        name,
        password_hash,
    }
}

/// The authority a command or a transaction holds to publish one domain's mutations.
fn domain_mutation_lease(arbitrary: &mut Arbitrary<'_>) -> DomainMutationLease {
    let owner = if arbitrary.entropy().flag() {
        DomainMutationOwner::command(arbitrary.execution_reference())
    } else {
        DomainMutationOwner::transaction(arbitrary.string())
    };
    let revision = arbitrary.entropy().any_u64();
    DomainMutationLease::recorded(owner, revision)
}

/// A restored domain's installation gate: pending its state, or installing under an authority.
fn restore_installation(arbitrary: &mut Arbitrary<'_>) -> DomainRestoreInstallation {
    if !arbitrary.entropy().flag() {
        return DomainRestoreInstallation::Pending {
            execution: arbitrary.execution_reference(),
        };
    }
    DomainRestoreInstallation::Installing(arbitrary.restore_state_authority())
}

/// The resource catalog: declared sequences, published versions, replica records and uploads,
/// each keyed by the identity its value carries, with every upload assigned a version of its own.
fn resource_records(arbitrary: &mut Arbitrary<'_>) -> ResourceRecords {
    let mut resources = ResourceRecords::default();
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let domain = arbitrary.rule_name::<DomainName>();
        let identifier = arbitrary.rule_name::<ResourceName>();
        let next_version = arbitrary.positive_u64();
        resources.restore_catalog(&domain, &identifier, next_version);
    }
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let id = ResourceId::new(
            arbitrary.rule_name(),
            arbitrary.rule_name(),
            arbitrary.entropy().any_u64(),
        );
        let version = arbitrary.resource_version_of(id.clone());
        resources.versions.insert(id, version);
    }
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let key = ResourceReplicaKey::new(
            arbitrary.rule_name(),
            arbitrary.rule_name(),
            arbitrary.entropy().any_u64(),
            arbitrary.node_identity(),
        );
        let replica = arbitrary.resource_node_status_of(key.clone());
        resources.replicas.insert(key, replica);
    }
    let mut assigned = BTreeSet::new();
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let key = ResourceUploadKey::new(
            arbitrary.rule_name::<UserName>(),
            arbitrary.rule_name(),
            arbitrary.rule_name(),
            arbitrary.upload_identity(),
        );
        let version = arbitrary.entropy().any_u64();
        let id = ResourceId::new(key.domain.clone(), key.identifier.clone(), version);
        // One version belongs to exactly one upload; a repeated draw adds nothing.
        if !assigned.insert(id) {
            continue;
        }
        let upload = arbitrary.resource_upload_of(key.clone(), version);
        resources.uploads.insert(key, upload);
    }
    resources
}

/// The membership schedule decisions read: up to three member nodes, any of them voters, and any
/// of the voters cordoned, since only a voter is ever read as cordoned.
fn schedule_topology_inputs(arbitrary: &mut Arbitrary<'_>) -> ScheduleTopologyInputs {
    let members = arbitrary.records(Arbitrary::rule_name::<ClusterNodeName>);
    let mut voters = Vec::new();
    for member in &members {
        if arbitrary.entropy().flag() {
            voters.push(member.clone());
        }
    }
    let mut cordoned = Vec::new();
    for voter in &voters {
        if arbitrary.entropy().flag() {
            cordoned.push(voter.clone());
        }
    }
    ScheduleTopologyInputs {
        members: members.into_iter().collect(),
        voters: voters.into_iter().collect(),
        cordoned: cordoned.into_iter().collect(),
    }
}

/// The committed schedule of `domain`, or none.
fn optional_schedule(
    arbitrary: &mut Arbitrary<'_>,
    domain: &DomainName,
) -> Option<Box<DomainSchedule>> {
    optional(arbitrary, |arbitrary| {
        Box::new(arbitrary.domain_schedule_of(domain.clone()))
    })
}

/// The inputs a plan for `domain` captured beside the domain's `state`: up to three resources the
/// domain declares and completed versions of its resources, its committed schedule or none, and
/// the cluster's membership.
fn planning_inputs_with(
    arbitrary: &mut Arbitrary<'_>,
    domain: DomainName,
    state: Option<DomainState>,
) -> DomainPlanningInputs {
    let resources = arbitrary.records(Arbitrary::rule_name::<ResourceName>);
    let completed_versions = arbitrary.records(|arbitrary| {
        let identifier = arbitrary.rule_name();
        let version = arbitrary.entropy().any_u64();
        ResourceId::new(domain.clone(), identifier, version)
    });
    let schedule = optional_schedule(arbitrary, &domain);
    let topology = schedule_topology_inputs(arbitrary);
    DomainPlanningInputs {
        domain,
        state: state.map(Box::new),
        resources: DomainResourcePlanningInputs {
            resources: resources.into_iter().collect(),
            completed_versions: completed_versions.into_iter().collect(),
        },
        schedule,
        topology,
    }
}

/// The inputs a plan for `domain` captured: the domain's state, or none for a domain that does not
/// exist, beside everything [`planning_inputs_with`] captures.
fn domain_planning_inputs(
    arbitrary: &mut Arbitrary<'_>,
    domain: DomainName,
) -> DomainPlanningInputs {
    let state = optional(arbitrary, |arbitrary| {
        arbitrary.domain_state_of(domain.clone())
    });
    planning_inputs_with(arbitrary, domain, state)
}

/// The inactivity boundary a transaction's latest activity fixed.
fn transaction_activity(arbitrary: &mut Arbitrary<'_>) -> TransactionActivity {
    let last_activity_at = arbitrary.timestamp();
    let timeout = Duration::from_nanos(arbitrary.entropy().any_u64());
    TransactionActivity::from_timeout(last_activity_at, timeout)
}

/// Any byte offset into a statement's source a `u32` holds, landing on the extremes as often as
/// elsewhere.
fn source_offset(arbitrary: &mut Arbitrary<'_>) -> u32 {
    let offset = arbitrary.entropy().boundary_biased(0..=u64::from(u32::MAX));
    u32::try_from(offset).verified("the draw is at most u32::MAX")
}

/// The byte range of a statement's source a diagnostic points at: two offsets, the first not after
/// the second.
fn diagnostic_span(arbitrary: &mut Arbitrary<'_>) -> DiagnosticSpan {
    let first = source_offset(arbitrary);
    let second = source_offset(arbitrary);
    DiagnosticSpan {
        start: first.min(second),
        end: first.max(second),
    }
}

/// A diagnostic with any message, located in its statement's source or not.
fn transaction_diagnostic(arbitrary: &mut Arbitrary<'_>) -> TransactionDiagnostic {
    let message = arbitrary.string();
    let span = optional(arbitrary, diagnostic_span);
    TransactionDiagnostic { message, span }
}

/// What admitting or applying statements reported: `success`, with any message and up to three
/// diagnostics, whether the entity already existed, and `admission` when the result answers an
/// accepted append.
fn transaction_command_result(
    arbitrary: &mut Arbitrary<'_>,
    success: bool,
    admission: Option<TransactionOperationAdmission>,
) -> TransactionCommandResult {
    let message = arbitrary.string();
    let diagnostics = arbitrary.records(transaction_diagnostic);
    let already_existed = arbitrary.entropy().flag();
    TransactionCommandResult {
        success,
        message,
        diagnostics,
        already_existed,
        admission,
    }
}

/// A request to queue any statement with any source at `expected_position`, under
/// `request_reference`.
fn transaction_statement_request(
    arbitrary: &mut Arbitrary<'_>,
    request_reference: CommandExecutionReference,
    expected_position: usize,
) -> TransactionStatementRequest {
    let source = arbitrary.string();
    let statement = arbitrary.statement();
    TransactionStatementRequest {
        request_reference,
        expected_position,
        source,
        statement,
    }
}

/// A statement the transaction `transaction_id` queued at `expected_position` under
/// `request_reference`, with the successful admission its append recorded.
fn queued_statement(
    arbitrary: &mut Arbitrary<'_>,
    transaction_id: &str,
    request_reference: CommandExecutionReference,
    expected_position: usize,
) -> TransactionStatement {
    let request = transaction_statement_request(arbitrary, request_reference, expected_position);
    let operation = TransactionOperationNumber::from_index(expected_position)
        .assured("a generated transaction queues at most three statements");
    let admission =
        arbitrary.transaction_operation_admission_of(transaction_id.to_string(), operation);
    let result = transaction_command_result(arbitrary, true, Some(admission));
    TransactionStatement::admitted(request, result)
}

/// The statements the transaction `transaction_id` queued while it was open: up to three, each at
/// the position it was appended at, under a request reference of its own. A reference names one
/// statement of its transaction, so a repeated reference adds nothing.
fn queued_statements(
    arbitrary: &mut Arbitrary<'_>,
    transaction_id: &str,
) -> Vec<TransactionStatement> {
    let mut statements = Vec::new();
    let mut references = BTreeSet::new();
    for _ in 0..arbitrary.entropy().count(QUEUED) {
        let request_reference = arbitrary.execution_reference();
        if !references.insert(request_reference.clone()) {
            continue;
        }
        let expected_position = statements.len();
        let statement = queued_statement(
            arbitrary,
            transaction_id,
            request_reference,
            expected_position,
        );
        statements.push(statement);
    }
    statements
}

/// The source bytes `statements` queue together.
fn queued_source_bytes(statements: &[TransactionStatement]) -> u64 {
    let mut bytes = 0_u64;
    for statement in statements {
        bytes = bytes
            .checked_add(statement.source_bytes())
            .assured("a generated queue holds at most three bounded sources");
    }
    bytes
}

/// How commit admission divides `statements` queued statements into execution steps, in order:
/// each step applies one through three consecutive statements, and together they apply every
/// statement once.
fn commit_steps(
    arbitrary: &mut Arbitrary<'_>,
    statements: usize,
) -> Vec<TransactionOperationRange> {
    let mut steps = Vec::new();
    let mut first = 0_usize;
    while first < statements {
        let remaining = statements
            .checked_sub(first)
            .verified("the loop runs only while statements remain");
        let bound = NonZeroUsize::new(remaining.min(STEP_STATEMENTS))
            .verified("the loop runs only while statements remain");
        let count = arbitrary.entropy().positive_count(bound);
        let step = TransactionOperationRange::from_index_and_count(first, count)
            .assured("a generated transaction queues at most three statements");
        steps.push(step);
        first = step.end_index();
    }
    steps
}

/// The result of the step that applied `operations` of a transaction of `domain`: anything it
/// planned and did, and a result reporting `success`.
fn transaction_step_result(
    arbitrary: &mut Arbitrary<'_>,
    domain: &DomainName,
    operations: TransactionOperationRange,
    success: bool,
) -> TransactionStepResult {
    let impact = arbitrary.execution_step_impact_report(domain, operations);
    let result = transaction_command_result(arbitrary, success, None);
    TransactionStepResult { impact, result }
}

/// The results of `steps` of a transaction of `domain`, every one of which applied successfully.
fn applied_results(
    arbitrary: &mut Arbitrary<'_>,
    domain: &DomainName,
    steps: &[TransactionOperationRange],
) -> Vec<TransactionStepResult> {
    let mut results = Vec::with_capacity(steps.len());
    for step in steps {
        results.push(transaction_step_result(arbitrary, domain, *step, true));
    }
    results
}

/// A reset of any WASM processor of `domain`, as a `RESET WASM PROCESSOR ... STATE` requests it.
fn wasm_state_reset(arbitrary: &mut Arbitrary<'_>, domain: &DomainName) -> ResetWasmState {
    let statement = arbitrary.statement_of(StatementVariant::ResetWasmState);
    let reset = match statement {
        Statement::ResetWasmState(reset) => Some(reset),
        _ => None,
    };
    let reset = reset.assured("statement_of builds the statement form it is asked for");
    ResetWasmState {
        domain: domain.clone(),
        ..reset
    }
}

/// The authoritative change a successful step of a transaction of `domain` made, with the inputs
/// it was planned from: a schedule replaced, the domain and its schedule put, the domain started
/// at the start, clock and authority admission resolved, the domain stopped, a resource catalog
/// created, or a WASM processor's guest state reset.
fn transaction_step_effect(
    arbitrary: &mut Arbitrary<'_>,
    domain: &DomainName,
) -> TransactionStepEffect {
    let inputs = Box::new(domain_planning_inputs(arbitrary, domain.clone()));
    match arbitrary.entropy().byte() % 6 {
        0 => TransactionStepEffect::ReplaceDomainSchedule {
            inputs,
            schedule: optional_schedule(arbitrary, domain),
        },
        1 => TransactionStepEffect::PutDomainAndSchedule {
            inputs,
            domain: Box::new(arbitrary.domain_state_of(domain.clone())),
            schedule: optional_schedule(arbitrary, domain),
        },
        2 => {
            let resolved = arbitrary.transaction_resolved_domain_start();
            TransactionStepEffect::StartDomain {
                inputs,
                start: resolved.start,
                clock: resolved.clock,
                authority: resolved.authority,
            }
        }
        3 => TransactionStepEffect::StopDomain { inputs },
        4 => TransactionStepEffect::CreateResourceCatalog {
            inputs,
            identifier: arbitrary.rule_name(),
        },
        _ => TransactionStepEffect::ResetWasmState {
            inputs,
            reset: Box::new(wasm_state_reset(arbitrary, domain)),
            request: arbitrary.retry_reference(),
            schedule: Box::new(arbitrary.domain_schedule_of(domain.clone())),
        },
    }
}

/// A failure of the step beginning at statement `failing_step`: of the step itself, or of the
/// inputs it was planned from, with any error.
fn failed_outcome(arbitrary: &mut Arbitrary<'_>, failing_step: usize) -> TransactionOutcome {
    let error = arbitrary.string();
    if arbitrary.entropy().flag() {
        TransactionOutcome::Failed {
            failing_step,
            error,
        }
    } else {
        TransactionOutcome::PlanningInputsChanged {
            failing_step,
            error,
        }
    }
}

/// `step` of a transaction of `domain` while it applies: the revision that recorded its effect, its
/// result, the effect it made when it succeeded, and the outcome its completion finishes the
/// transaction with. A failed step fails the transaction, and the `last` step commits it.
fn applying_step(
    arbitrary: &mut Arbitrary<'_>,
    domain: &DomainName,
    step: TransactionOperationRange,
    last: bool,
) -> TransactionApplyingStep {
    let effect_revision = arbitrary.entropy().any_u64();
    let success = arbitrary.entropy().flag();
    let result = transaction_step_result(arbitrary, domain, step, success);
    let effect = if success {
        optional(arbitrary, |arbitrary| {
            transaction_step_effect(arbitrary, domain)
        })
    } else {
        None
    };
    let completion = if !success {
        Some(failed_outcome(arbitrary, step.first_index()))
    } else if last {
        Some(TransactionOutcome::Committed)
    } else {
        None
    };
    TransactionApplyingStep {
        effect_revision,
        next_statement: step.end_index(),
        result,
        effect,
        completion,
    }
}

/// A preview of the transaction `transaction_id` at `accepted` operations, with any planning basis.
fn transaction_preview(
    arbitrary: &mut Arbitrary<'_>,
    transaction_id: &str,
    accepted: usize,
) -> TransactionPreviewIdentity {
    let planning_basis = arbitrary.impact_planning_basis();
    TransactionPreviewIdentity {
        transaction_id: transaction_id.to_string(),
        position: TransactionPosition::new(accepted),
        planning_basis,
    }
}

/// `opened` while it is still open: queuing up to three statements, with the preview its latest
/// append recorded.
fn open_transaction(
    arbitrary: &mut Arbitrary<'_>,
    opened: ReplicatedTransaction,
) -> ReplicatedTransaction {
    let statements = queued_statements(arbitrary, &opened.id);
    let mut latest_preview = None;
    if let Some(latest) = statements.last()
        && let Some(admission) = &latest.admission.admission
    {
        latest_preview = Some(admission.preview.clone());
    }
    let state = opened.state.clone();
    let statement_count = statements.len();
    let source_bytes = queued_source_bytes(&statements);
    ReplicatedTransaction::generated(
        opened,
        state,
        statement_count,
        source_bytes,
        statements,
        latest_preview,
        None,
    )
}

/// `opened` committing its queued statements: the plan its commit admission recorded divides them
/// into steps, every step before the one it applies succeeded, since a failed or a last step
/// finishes the transaction, and admission granted it a mutation lease when a statement needs one.
fn committing_transaction(
    arbitrary: &mut Arbitrary<'_>,
    opened: ReplicatedTransaction,
) -> ReplicatedTransaction {
    let statements = queued_statements(arbitrary, &opened.id);
    let steps = commit_steps(arbitrary, statements.len());
    let preview = transaction_preview(arbitrary, &opened.id, statements.len());
    let commit_plan = TransactionCommitPlanHeader {
        preview: preview.clone(),
        step_count: steps.len(),
    };
    let completed = match steps.len().checked_sub(1) {
        Some(before_last) => arbitrary.entropy().count(before_last),
        None => 0,
    };
    let applied = steps
        .get(..completed)
        .verified("fewer steps completed than the plan holds");
    let results = applied_results(arbitrary, &opened.domain, applied);
    let next_statement = match applied.last() {
        Some(step) => step.end_index(),
        None => 0,
    };
    let mut applying = None;
    if let Some(step) = steps.get(completed)
        && arbitrary.entropy().flag()
    {
        let following = completed
            .checked_add(1)
            .assured("a generated plan holds at most three steps");
        let last = following == steps.len();
        applying = Some(applying_step(arbitrary, &opened.domain, *step, last));
    }
    let mut needs_lease = false;
    for statement in &statements {
        if statement.statement.requires_domain_mutation_ownership() {
            needs_lease = true;
        }
    }
    let domain_mutation = if needs_lease {
        let revision = arbitrary.entropy().any_u64();
        Some(DomainMutationLease::recorded(
            opened.mutation_owner().clone(),
            revision,
        ))
    } else {
        None
    };
    let last_activity_at = instant_from(arbitrary, opened.created_at);
    let state = TransactionState::Committing(Box::new(TransactionCommitProgress {
        last_activity_at,
        next_statement,
        results,
        applying,
        domain_mutation,
    }));
    let statement_count = statements.len();
    let source_bytes = queued_source_bytes(&statements);
    ReplicatedTransaction::generated(
        opened,
        state,
        statement_count,
        source_bytes,
        statements,
        Some(preview),
        Some(commit_plan),
    )
}

/// How a generated transaction finished, and what it recorded beside its outcome.
struct Ending {
    outcome: TransactionOutcome,
    results: Vec<TransactionStepResult>,
    latest_preview: Option<TransactionPreviewIdentity>,
    commit_plan: Option<TransactionCommitPlanHeader>,
}

/// A commit that applied every one of `steps` of a transaction of `domain`, under the plan its
/// admission recorded for `preview`.
fn committed(
    arbitrary: &mut Arbitrary<'_>,
    domain: &DomainName,
    steps: &[TransactionOperationRange],
    preview: TransactionPreviewIdentity,
) -> Ending {
    let results = applied_results(arbitrary, domain, steps);
    let commit_plan = TransactionCommitPlanHeader {
        preview: preview.clone(),
        step_count: steps.len(),
    };
    Ending {
        outcome: TransactionOutcome::Committed,
        results,
        latest_preview: Some(preview),
        commit_plan: Some(commit_plan),
    }
}

/// A commit that failed at step `failing` of `steps` of a transaction of `domain`, after every step
/// before it applied, under the plan its admission recorded for `preview`.
fn failed_commit(
    arbitrary: &mut Arbitrary<'_>,
    domain: &DomainName,
    steps: &[TransactionOperationRange],
    failing: usize,
    preview: TransactionPreviewIdentity,
) -> Ending {
    let applied = steps
        .get(..failing)
        .assured("the failing step is drawn from the plan's steps");
    let failed = *steps
        .get(failing)
        .assured("the failing step is drawn from the plan's steps");
    let mut results = applied_results(arbitrary, domain, applied);
    results.push(transaction_step_result(arbitrary, domain, failed, false));
    let outcome = failed_outcome(arbitrary, failed.first_index());
    let commit_plan = TransactionCommitPlanHeader {
        preview: preview.clone(),
        step_count: steps.len(),
    };
    Ending {
        outcome,
        results,
        latest_preview: Some(preview),
        commit_plan: Some(commit_plan),
    }
}

/// A commit whose admission was refused at statement `failing_step`, against the preview its
/// failure named. Nothing applied, and no plan was recorded.
fn refused_commit(
    arbitrary: &mut Arbitrary<'_>,
    failing_step: usize,
    preview: TransactionPreviewIdentity,
) -> Ending {
    let error = arbitrary.string();
    Ending {
        outcome: TransactionOutcome::Failed {
            failing_step,
            error,
        },
        results: Vec::new(),
        latest_preview: Some(preview),
        commit_plan: None,
    }
}

/// A transaction that ended with `outcome`, reverted or expired, while it was open, holding the
/// preview `preview` its latest append recorded when it queued any of its `statement_count`
/// statements.
fn withdrawn(
    outcome: TransactionOutcome,
    statement_count: usize,
    preview: TransactionPreviewIdentity,
) -> Ending {
    let latest_preview = if statement_count == 0 {
        None
    } else {
        Some(preview)
    };
    Ending {
        outcome,
        results: Vec::new(),
        latest_preview,
        commit_plan: None,
    }
}

/// `opened` finished after queuing up to three statements: committed with every step applied,
/// failed at a step it applied or by a refused commit admission, reverted, or expired. Finishing
/// cleared its queue, and only a commit admission recorded a plan.
fn finished_transaction(
    arbitrary: &mut Arbitrary<'_>,
    opened: ReplicatedTransaction,
) -> ReplicatedTransaction {
    let statement_count = arbitrary.entropy().count(QUEUED);
    let steps = commit_steps(arbitrary, statement_count);
    let preview = transaction_preview(arbitrary, &opened.id, statement_count);
    let ending = match NonZeroUsize::new(steps.len()) {
        None => match arbitrary.entropy().byte() % 3 {
            0 => committed(arbitrary, &opened.domain, &steps, preview),
            1 => withdrawn(TransactionOutcome::Reverted, statement_count, preview),
            _ => withdrawn(TransactionOutcome::Expired, statement_count, preview),
        },
        Some(step_count) => match arbitrary.entropy().byte() % 5 {
            0 => committed(arbitrary, &opened.domain, &steps, preview),
            1 => {
                let failing = arbitrary.entropy().index(step_count);
                failed_commit(arbitrary, &opened.domain, &steps, failing, preview)
            }
            2 => {
                let queued = NonZeroUsize::new(statement_count)
                    .verified("a plan holds steps only for queued statements");
                let failing_step = arbitrary.entropy().index(queued);
                refused_commit(arbitrary, failing_step, preview)
            }
            3 => withdrawn(TransactionOutcome::Reverted, statement_count, preview),
            _ => withdrawn(TransactionOutcome::Expired, statement_count, preview),
        },
    };
    let outcome_revision = arbitrary.entropy().any_u64();
    let finished_at = instant_from(arbitrary, opened.created_at);
    let state = TransactionState::Finished(FinishedTransaction {
        outcome: ending.outcome,
        outcome_revision,
        finished_at,
        results: ending.results,
    });
    ReplicatedTransaction::generated(
        opened,
        state,
        statement_count,
        0,
        Vec::new(),
        ending.latest_preview,
        ending.commit_plan,
    )
}

/// A transaction of any domain and owner, opened by a session or for one command, at any point of
/// its life: open, committing, or finished.
fn replicated_transaction(arbitrary: &mut Arbitrary<'_>) -> ReplicatedTransaction {
    let domain = arbitrary.rule_name::<DomainName>();
    let owner = arbitrary.rule_name::<UserName>();
    let activity = transaction_activity(arbitrary);
    let opened = if arbitrary.entropy().flag() {
        let command = arbitrary.retry_reference();
        let id = format!("command.{}", command.as_str());
        ReplicatedTransaction::open_for_command(id, domain, owner, activity, command)
    } else {
        let id = arbitrary.transaction_id();
        ReplicatedTransaction::open(id, domain, owner, activity)
    };
    match arbitrary.entropy().byte() % 3 {
        0 => open_transaction(arbitrary, opened),
        1 => committing_transaction(arbitrary, opened),
        _ => finished_transaction(arbitrary, opened),
    }
}

/// Up to three transactions, each keyed by its identity.
fn transaction_records(arbitrary: &mut Arbitrary<'_>) -> Records<String, ReplicatedTransaction> {
    let mut transactions = Records::default();
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let transaction = replicated_transaction(arbitrary);
        transactions.insert(transaction.id.clone(), transaction);
    }
    transactions
}

/// The schedule eligibility commit admission checked a plan of `domain` against: up to three
/// voters, live node processes and placement candidates.
fn schedule_eligibility(
    arbitrary: &mut Arbitrary<'_>,
    domain: DomainName,
) -> TransactionScheduleEligibility {
    let voters = arbitrary.records(Arbitrary::rule_name::<ClusterNodeName>);
    let live_identities = arbitrary.records(Arbitrary::node_identity);
    let placement_candidate_identities = arbitrary.records(Arbitrary::node_identity);
    TransactionScheduleEligibility::new(
        domain,
        voters,
        live_identities,
        placement_candidate_identities,
    )
}

/// Leaves room in the start generation of every state `decision` installs for each `START DOMAIN`
/// that advances it afterwards, as the planner does: it never plans a start past the last
/// generation. Returns how many starts advance the state captured before the first step, which
/// no `ALTER DOMAIN` has replaced yet.
fn leave_room_for_starts(decision: &mut TransactionCommitPlan) -> u64 {
    let mut later_starts = 0_u64;
    for step in decision.steps.iter_mut().rev() {
        match &mut step.kind {
            TransactionCommitStepKind::StartDomain { .. } => {
                later_starts = later_starts
                    .checked_add(1)
                    .assured("a generated plan holds at most three steps");
            }
            TransactionCommitStepKind::AlterDomain { next, .. } => {
                let latest = u64::MAX
                    .checked_sub(later_starts)
                    .assured("a generated plan holds at most three steps");
                next.start_version = next.start_version.min(latest);
                later_starts = 0;
            }
            TransactionCommitStepKind::Models { .. }
            | TransactionCommitStepKind::StopDomain
            | TransactionCommitStepKind::CreateResource { .. }
            | TransactionCommitStepKind::ResetWasmState { .. } => {}
        }
    }
    later_starts
}

/// The plan commit admission froze for `report`, a report of `domain` as admission freezes it: the
/// decision for each of its steps, the inputs captured before its first step, whose domain state
/// leaves room for every start the plan applies, and the schedule eligibility it was checked
/// against. Every later step's inputs follow from the step before it.
fn admission_plan_for(
    arbitrary: &mut Arbitrary<'_>,
    domain: DomainName,
    report: &TransactionImpactReport,
) -> TransactionCommitAdmissionPlan {
    let mut decision = arbitrary.transaction_commit_plan_for(report);
    let starts = leave_room_for_starts(&mut decision);
    let latest = u64::MAX
        .checked_sub(starts)
        .assured("a generated plan holds at most three steps");
    let mut state = arbitrary.domain_state_of(domain.clone());
    state.start_version = arbitrary.entropy().boundary_biased(0..=latest);
    let first_inputs = planning_inputs_with(arbitrary, domain.clone(), Some(state));
    let eligibility = schedule_eligibility(arbitrary, domain);
    TransactionCommitAdmissionPlan::capture(decision, first_inputs, eligibility).assured(
        "the captured domain state exists and leaves room for every start the plan applies, and \
         the eligibility names the same domain",
    )
}

/// Up to three plans admitted for new transactions, each stored through the production insert
/// under the transaction it admits. A transaction holds one admitted plan, so a plan for a
/// transaction already admitted adds nothing.
fn commit_plan_records(arbitrary: &mut Arbitrary<'_>) -> TransactionCommitPlanRecords {
    let mut plans = TransactionCommitPlanRecords::default();
    let mut admitted = BTreeSet::new();
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let domain = arbitrary.rule_name::<DomainName>();
        let report = arbitrary.admitted_impact_report(domain.clone());
        let plan = admission_plan_for(arbitrary, domain, &report);
        if !admitted.insert(plan.decision().preview.transaction_id.clone()) {
            continue;
        }
        plans.insert(&plan).assured(
            "admission captured the plan's inputs from its own decisions, and no other plan \
             admits its transaction",
        );
    }
    plans
}

/// Up to three report revisions, each stored through the production insert: a revision of a new
/// transaction or of one reported already, as inspection reads it at any point of the
/// transaction's life or as commit admission froze it. A transaction keeps its domain across its
/// revisions, and a preview identity names one revision, so a repeated identity adds nothing.
fn report_records(arbitrary: &mut Arbitrary<'_>) -> TransactionReportRecords {
    /// A transaction a revision was stored for.
    #[derive(Clone)]
    struct Reported {
        transaction_id: String,
        domain: DomainName,
    }

    let mut reports = TransactionReportRecords::default();
    let mut reported = Vec::new();
    let mut identities = BTreeSet::new();
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let earlier = NonZeroUsize::new(reported.len());
        let revisits = arbitrary.entropy().flag();
        let transaction = match earlier {
            Some(count) if revisits => {
                let index = arbitrary.entropy().index(count);
                let revisited: &Reported = &reported[index];
                revisited.clone()
            }
            Some(_) | None => {
                let transaction = Reported {
                    transaction_id: arbitrary.transaction_id(),
                    domain: arbitrary.rule_name(),
                };
                reported.push(transaction.clone());
                transaction
            }
        };
        let report = if arbitrary.entropy().flag() {
            arbitrary.transaction_impact_report(transaction.domain)
        } else {
            arbitrary.admitted_impact_report(transaction.domain)
        };
        let archive = TransactionReportArchive::new(transaction.transaction_id, report)
            .assured("distinct topologies of a valid report archive under distinct identities");
        if !identities.insert(archive.identity().clone()) {
            continue;
        }
        reports.insert(archive).assured(
            "an archive of a valid report is complete, and no other revision holds its identity",
        );
    }
    reports
}

/// The transaction a command opens or continues, with the activity it records.
fn transaction_target(arbitrary: &mut Arbitrary<'_>) -> CommandExecutionTransactionTarget {
    let id = arbitrary.transaction_id();
    let activity = transaction_activity(arbitrary);
    if arbitrary.entropy().flag() {
        CommandExecutionTransactionTarget::New { id, activity }
    } else {
        CommandExecutionTransactionTarget::Existing { id, activity }
    }
}

/// One operation of a transaction request: a statement to queue at any position, a commit that
/// expects a preview or none, or a revert.
fn transaction_request_operation(
    arbitrary: &mut Arbitrary<'_>,
) -> CommandExecutionTransactionOperation {
    match arbitrary.entropy().byte() % 3 {
        0 => {
            let request_reference = arbitrary.execution_reference();
            let expected_position = any_count(arbitrary);
            let request =
                transaction_statement_request(arbitrary, request_reference, expected_position);
            CommandExecutionTransactionOperation::Queue(Box::new(request))
        }
        1 => CommandExecutionTransactionOperation::Commit {
            expected_preview: optional(arbitrary, Arbitrary::transaction_preview_identity),
        },
        _ => CommandExecutionTransactionOperation::Revert,
    }
}

/// A transaction request a command carries: its target and up to three operations.
fn transaction_request(arbitrary: &mut Arbitrary<'_>) -> CommandExecutionTransactionRequest {
    let target = transaction_target(arbitrary);
    let operations = arbitrary.records(transaction_request_operation);
    CommandExecutionTransactionRequest { target, operations }
}

/// A restore while it applies: a `RESTORE` of any scope that applies, and the steps it applied so
/// far, the first of its fixed order, with what its users step did once that step applied.
fn restore_execution(arbitrary: &mut Arbitrary<'_>) -> RestoreExecution {
    let restore = arbitrary.restore_of(RestoreMode::Apply);
    let steps = arbitrary.restore_steps(&restore.scope);
    let archive = arbitrary.restore_archive();
    let mut execution = RestoreExecution::new(restore, archive);
    let applied = arbitrary.entropy().count(steps.len());
    for step in steps.into_iter().take(applied) {
        let users = if step == RestoreStep::Users {
            Some(arbitrary.restored_users())
        } else {
            None
        };
        execution.record(step, users);
    }
    execution
}

/// A `CREATE DOMAIN` as admission captured it: the domain as admission found it when it already
/// existed, and otherwise the new domain, stopped and never started.
fn create_domain_effect(arbitrary: &mut Arbitrary<'_>) -> CommandExecutionEffect {
    let if_not_exists = arbitrary.entropy().flag();
    let existed_at_admission = arbitrary.entropy().flag();
    let id = arbitrary.rule_name::<DomainName>();
    let found = arbitrary.domain_state_of(id);
    let state = if existed_at_admission {
        found
    } else {
        DomainState {
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: DomainStartPoint::Resume,
            clock: None,
            ..found
        }
    };
    CommandExecutionEffect::CreateDomain {
        if_not_exists,
        existed_at_admission,
        state: Box::new(state),
    }
}

/// What a command admitted under `reference` applies: a domain to create, a statement applied
/// through a transaction of its own, a transaction request, any other statement, a user to create,
/// a node to drop, or a restore. A statement of its own is a `BACKUP` as often as any other
/// statement.
fn command_execution_effect(
    arbitrary: &mut Arbitrary<'_>,
    reference: &CommandExecutionReference,
) -> CommandExecutionEffect {
    match arbitrary.entropy().byte() % 7 {
        0 => create_domain_effect(arbitrary),
        1 => CommandExecutionEffect::Transaction {
            transaction_id: format!("command.{}", reference.as_str()),
            source: arbitrary.string(),
            statement: Box::new(arbitrary.statement()),
        },
        2 => CommandExecutionEffect::TransactionRequest(Box::new(transaction_request(arbitrary))),
        3 => {
            let source = arbitrary.string();
            let statement = if arbitrary.entropy().flag() {
                arbitrary.statement_of(StatementVariant::Backup)
            } else {
                arbitrary.statement()
            };
            CommandExecutionEffect::Statement {
                source,
                statement: Box::new(statement),
            }
        }
        4 => CommandExecutionEffect::CreateUser {
            if_not_exists: arbitrary.entropy().flag(),
            name: arbitrary.rule_name(),
            password_hash: arbitrary.string(),
        },
        5 => CommandExecutionEffect::DropNode {
            identity: arbitrary.node_identity(),
            member_at_admission: arbitrary.entropy().flag(),
        },
        _ => CommandExecutionEffect::Restore(Box::new(restore_execution(arbitrary))),
    }
}

/// A command as admission records it, under the retry identity every admitted command holds: its
/// owner, the domain and transaction position it was admitted against or none, its request digest
/// and admission instant, and the effect it applies.
fn admitted_execution(arbitrary: &mut Arbitrary<'_>) -> CommandExecution {
    let reference = arbitrary.retry_reference();
    let owner = arbitrary.rule_name::<UserName>();
    let domain = optional(arbitrary, Arbitrary::rule_name::<DomainName>);
    let expected_transaction_position = optional(arbitrary, Arbitrary::transaction_position);
    let request_digest = arbitrary.digest();
    let admitted_at = arbitrary.timestamp();
    let effect = command_execution_effect(arbitrary, &reference);
    CommandExecution::applying_at_position(
        reference,
        owner,
        domain,
        expected_transaction_position,
        request_digest,
        admitted_at,
        effect,
    )
}

/// An admitted command still applying, holding the mutation leases of up to three domains, each
/// owned by the command itself.
fn applying_execution(arbitrary: &mut Arbitrary<'_>) -> CommandExecution {
    let mut execution = admitted_execution(arbitrary);
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let domain = arbitrary.rule_name::<DomainName>();
        let owner = DomainMutationOwner::command(execution.reference.clone());
        let revision = arbitrary.entropy().any_u64();
        execution.bind_domain_mutation(domain, DomainMutationLease::recorded(owner, revision));
    }
    execution
}

/// A diagnostic with any message, located in its command's source or not.
fn command_execution_diagnostic(arbitrary: &mut Arbitrary<'_>) -> CommandExecutionDiagnostic {
    let message = arbitrary.string();
    let span = optional(arbitrary, diagnostic_span);
    CommandExecutionDiagnostic { message, span }
}

/// How one statement of a multi-statement command ended, with any message and up to three
/// diagnostics.
fn statement_result(arbitrary: &mut Arbitrary<'_>) -> CommandExecutionStatementResult {
    let disposition = if arbitrary.entropy().flag() {
        let already_existed = arbitrary.entropy().flag();
        CommandExecutionStatementDisposition::Completed { already_existed }
    } else {
        CommandExecutionStatementDisposition::Failed
    };
    let message = arbitrary.string();
    let diagnostics = arbitrary.records(command_execution_diagnostic);
    CommandExecutionStatementResult {
        disposition,
        message,
        diagnostics,
    }
}

/// The transaction a finished command was bound to, as the command reported it: any transaction of
/// any domain, at a lifecycle the operations it accepted allow, having applied none of them while
/// open or once withdrawn, every one once committed, and any number of them otherwise.
fn transaction_status(arbitrary: &mut Arbitrary<'_>) -> CommandExecutionTransactionStatus {
    let transaction_id = arbitrary.transaction_id();
    let domain = arbitrary.rule_name();
    let accepted_operations = arbitrary.transaction_position();
    let lifecycle = arbitrary.transaction_lifecycle_of(accepted_operations);
    let accepted = accepted_operations.accepted_operations();
    let applied_operations = match &lifecycle {
        TransactionLifecycle::Open
        | TransactionLifecycle::Reverted
        | TransactionLifecycle::Expired => 0,
        TransactionLifecycle::Committed => accepted,
        TransactionLifecycle::Committing | TransactionLifecycle::Failed { .. } => {
            let accepted =
                u64::try_from(accepted).assured("supported targets address at most 64 bits");
            let applied = arbitrary.entropy().boundary_biased(0..=accepted);
            usize::try_from(applied).verified("the draw is at most the accepted count")
        }
    };
    CommandExecutionTransactionStatus {
        transaction_id,
        domain,
        lifecycle,
        accepted_operations,
        applied_operations,
    }
}

/// The preview a refused commit of the transaction `transaction_id` expected, beside the one that
/// describes the transaction now: any two positions and planning bases.
fn stale_preview(
    arbitrary: &mut Arbitrary<'_>,
    transaction_id: &str,
) -> CommandExecutionPreviewStale {
    let expected_position = arbitrary.transaction_position().accepted_operations();
    let expected = transaction_preview(arbitrary, transaction_id, expected_position);
    let current_position = arbitrary.transaction_position().accepted_operations();
    let current = transaction_preview(arbitrary, transaction_id, current_position);
    CommandExecutionPreviewStale { expected, current }
}

/// How a command whose effect was `effect` ended: completed, failed, or, for a transaction request
/// whose commit expected a preview that no longer described its transaction, refused as stale.
fn command_execution_disposition(
    arbitrary: &mut Arbitrary<'_>,
    effect: &CommandExecutionEffect,
) -> CommandExecutionDisposition {
    if let CommandExecutionEffect::TransactionRequest(request) = effect
        && arbitrary.entropy().flag()
    {
        let stale = stale_preview(arbitrary, request.target.id());
        return CommandExecutionDisposition::PreviewStale(stale);
    }
    if arbitrary.entropy().flag() {
        let already_existed = arbitrary.entropy().flag();
        CommandExecutionDisposition::Completed { already_existed }
    } else {
        CommandExecutionDisposition::Failed
    }
}

/// The result a command whose effect was `effect` finished with: a disposition its kind reports,
/// any message and diagnostics, and the parts only its kind reports: the statement results,
/// transaction status and admission of a transaction, the archive summary of a backup, and the
/// report of a restore, each absent when the command ended before producing it.
fn command_execution_result(
    arbitrary: &mut Arbitrary<'_>,
    effect: &CommandExecutionEffect,
) -> CommandExecutionResult {
    let disposition = command_execution_disposition(arbitrary, effect);
    let message = arbitrary.string();
    let diagnostics = arbitrary.records(command_execution_diagnostic);
    let mut result = CommandExecutionResult {
        disposition,
        message,
        diagnostics,
        statements: Vec::new(),
        transaction: None,
        transaction_admission: None,
        backup: None,
        restore: None,
    };
    match effect {
        CommandExecutionEffect::Transaction { .. }
        | CommandExecutionEffect::TransactionRequest(_) => {
            result.statements = arbitrary.records(statement_result);
            result.transaction = optional(arbitrary, transaction_status);
            result.transaction_admission =
                optional(arbitrary, Arbitrary::transaction_operation_admission);
        }
        CommandExecutionEffect::Statement { statement, .. } => {
            if let Statement::Backup(_) = statement.as_ref() {
                result.backup = optional(arbitrary, Arbitrary::backup_archive_summary);
            }
        }
        CommandExecutionEffect::Restore(restore) => {
            result.restore = optional(arbitrary, |arbitrary| {
                arbitrary.restore_report_of(&restore.restore, restore.archive)
            });
        }
        CommandExecutionEffect::CreateDomain { .. }
        | CommandExecutionEffect::CreateUser { .. }
        | CommandExecutionEffect::DropNode { .. } => {}
    }
    result
}

/// An admitted command that finished through the production transition, with the result its
/// effect finished with. Finishing released whatever mutation leases the command held.
fn finished_execution(arbitrary: &mut Arbitrary<'_>) -> CommandExecution {
    let applying = admitted_execution(arbitrary);
    let effect = applying
        .effect()
        .verified("admission records a command applying its effect");
    let result = command_execution_result(arbitrary, effect);
    let outcome_revision = arbitrary.entropy().any_u64();
    let finished_at = arbitrary.timestamp();
    applying
        .into_finished(outcome_revision, finished_at, Box::new(result))
        .verified("admission records a command applying its effect")
}

/// An admitted command at any point of its life: applying, finished, or expired, which keeps only
/// its retry identity until the retry fence passes the identity's issue time.
fn command_execution(arbitrary: &mut Arbitrary<'_>) -> CommandExecution {
    match arbitrary.entropy().byte() % 3 {
        0 => applying_execution(arbitrary),
        1 => finished_execution(arbitrary),
        _ => CommandExecution {
            reference: arbitrary.retry_reference(),
            state: CommandExecutionState::Expired,
        },
    }
}

/// Up to three command executions, each keyed by its reference, under the retry fence admission or
/// reclamation left: present once a command was admitted, and otherwise any fence or none.
fn command_execution_records(arbitrary: &mut Arbitrary<'_>) -> CommandExecutionRecords {
    let executions = arbitrary.records(command_execution);
    let retry_fence = if executions.is_empty() {
        optional(arbitrary, Arbitrary::timestamp)
    } else {
        Some(arbitrary.timestamp())
    };
    CommandExecutionRecords::generated(executions, retry_fence).assured(
        "every expired execution generated above holds a retry identity, whose issue time reads \
         back",
    )
}

/// A complete current state-machine revision. Every family holds up to three records, each keyed
/// by the identity its value carries, so the revision is one a cluster could have applied.
pub(super) fn state_machine(arbitrary: &mut Arbitrary<'_>) -> StateMachineData {
    let last_applied_log_id = if arbitrary.entropy().flag() {
        Some(log_id(arbitrary))
    } else {
        None
    };
    let last_membership = Arc::new(stored_membership(arbitrary));
    let runtime_revision = arbitrary.entropy().any_u64();

    let mut state = StateMachineData {
        last_applied_log_id,
        last_membership,
        runtime_revision,
        ..StateMachineData::default()
    };
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let domain = arbitrary.rule_name::<DomainName>();
        let schedule = arbitrary.domain_schedule_of(domain.clone());
        state.schedule.domains.insert(domain, schedule);
    }
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let domain = arbitrary.rule_name::<DomainName>();
        let domain_state = arbitrary.domain_state_of(domain.clone());
        state.domains.insert(domain, domain_state);
    }
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let domain = arbitrary.rule_name::<DomainName>();
        let authority = arbitrary.domain_clock_authority();
        state.domain_clock_authorities.insert(domain, authority);
    }
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let user = user_credentials(arbitrary);
        state.users.insert(user.name.clone(), user);
    }
    state.resources = resource_records(arbitrary);
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let node = arbitrary.rule_name::<ClusterNodeName>();
        state.cordoned_node_ids.insert(node, ());
    }
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let node = arbitrary.rule_name::<ClusterNodeName>();
        let incarnation = arbitrary.incarnation();
        state.node_admission_fences.insert(node, incarnation);
    }
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let domain = arbitrary.rule_name::<DomainName>();
        let lease = domain_mutation_lease(arbitrary);
        state.domain_mutations.insert(domain, lease);
    }
    for _ in 0..arbitrary.entropy().count(RECORDS) {
        let domain = arbitrary.rule_name::<DomainName>();
        let installation = restore_installation(arbitrary);
        state
            .domain_restore_installations
            .insert(domain, installation);
    }
    state.transactions = transaction_records(arbitrary);
    state.transaction_commit_plans = commit_plan_records(arbitrary);
    state.transaction_reports = report_records(arbitrary);
    state.command_executions = command_execution_records(arbitrary);
    state
}

/// A vote for any term and candidate, committed or not.
pub(super) fn vote(arbitrary: &mut Arbitrary<'_>) -> VoteOf {
    let term = arbitrary.entropy().any_u64();
    let candidate = arbitrary.rule_name::<ClusterNodeName>();
    if arbitrary.entropy().flag() {
        openraft::Vote::new_committed(term, candidate)
    } else {
        openraft::Vote::new(term, candidate)
    }
}

/// A Raft log position, or none.
pub(super) fn optional_log_id(arbitrary: &mut Arbitrary<'_>) -> Option<LogIdOf> {
    if arbitrary.entropy().flag() {
        Some(log_id(arbitrary))
    } else {
        None
    }
}

/// A domain mutation lease a command may carry, or none.
fn optional_lease(arbitrary: &mut Arbitrary<'_>) -> Option<Box<DomainMutationLease>> {
    if arbitrary.entropy().flag() {
        Some(Box::new(domain_mutation_lease(arbitrary)))
    } else {
        None
    }
}

/// The administrative identity of one resource upload attempt.
fn upload_key(arbitrary: &mut Arbitrary<'_>) -> ResourceUploadKey {
    ResourceUploadKey::new(
        arbitrary.rule_name::<UserName>(),
        arbitrary.rule_name(),
        arbitrary.rule_name(),
        arbitrary.upload_identity(),
    )
}

/// One node process's record of a resource version.
fn replica(arbitrary: &mut Arbitrary<'_>) -> nervix_models::ResourceNodeStatus {
    let key = ResourceReplicaKey::new(
        arbitrary.rule_name(),
        arbitrary.rule_name(),
        arbitrary.entropy().any_u64(),
        arbitrary.node_identity(),
    );
    arbitrary.resource_node_status_of(key)
}

/// A published resource version's metadata.
fn resource_version(arbitrary: &mut Arbitrary<'_>) -> nervix_models::ResourceVersion {
    let id = ResourceId::new(
        arbitrary.rule_name(),
        arbitrary.rule_name(),
        arbitrary.entropy().any_u64(),
    );
    arbitrary.resource_version_of(id)
}

/// A resource a restored domain declares, with any positive version its next upload receives.
fn restored_resource(arbitrary: &mut Arbitrary<'_>) -> RestoredResource {
    let resource = arbitrary.rule_name();
    let next_version = arbitrary.positive_u64();
    RestoredResource {
        resource,
        next_version,
    }
}

/// The effect of `step` of the restore `reference` applies: the archived users under any policy
/// for its users, the domain it creates, stopped, with up to three resources it declares, the
/// completion of resource versions other commands imported, or the release of a domain whose
/// restored state reached every node, with the clock a resumed paced domain runs on.
fn restore_step_effect(
    arbitrary: &mut Arbitrary<'_>,
    step: &RestoreStep,
    reference: &CommandExecutionReference,
) -> RestoreStepEffect {
    match step {
        RestoreStep::Users => {
            let users = arbitrary.records(user_credentials);
            let policy = arbitrary.entropy().pick([
                ExistingUserPolicy::Fail,
                ExistingUserPolicy::Skip,
                ExistingUserPolicy::Replace,
            ]);
            RestoreStepEffect::Users { users, policy }
        }
        RestoreStep::CreateDomain(domain) => {
            let archived = arbitrary.domain_state_of(domain.clone());
            let state = DomainState {
                status: DomainStatus::Stopped,
                clock: None,
                ..archived
            };
            let resources = arbitrary.records(restored_resource);
            RestoreStepEffect::Domain {
                state: Box::new(state),
                resources,
            }
        }
        RestoreStep::ImportResources(_) => RestoreStepEffect::Completion,
        RestoreStep::ApplyModels(_) => {
            let authority = RestoreStateAuthority {
                execution: reference.clone(),
                ..arbitrary.restore_state_authority()
            };
            let clock = optional(arbitrary, Arbitrary::domain_clock_state);
            RestoreStepEffect::InstalledState { authority, clock }
        }
    }
}

/// The admission policy a leader proposes a command under: its clock reading, any retry validity
/// and any capacity.
fn admission_policy(arbitrary: &mut Arbitrary<'_>) -> CommandExecutionAdmissionPolicy {
    let now = arbitrary.timestamp();
    let retry_validity = Duration::from_nanos(arbitrary.entropy().any_u64());
    let capacity = any_count(arbitrary);
    CommandExecutionAdmissionPolicy::at(now, retry_validity, capacity)
}

/// How the application of a transaction step of `domain` ended: applied, failed with its effect
/// committed, or rolled back to the domain as the failed step left it.
fn application_outcome(
    arbitrary: &mut Arbitrary<'_>,
    domain: DomainName,
) -> TransactionApplicationOutcome {
    match arbitrary.entropy().byte() % 3 {
        0 => TransactionApplicationOutcome::Applied,
        1 => TransactionApplicationOutcome::Failed {
            error: arbitrary.string(),
        },
        _ => {
            let error = arbitrary.string();
            let inputs = Box::new(domain_planning_inputs(arbitrary, domain));
            TransactionApplicationOutcome::RolledBack { error, inputs }
        }
    }
}

/// The proposal admitting a command: the command as admission records it, the domains whose
/// mutation leases it asks for, and the policy it is admitted under.
fn admit_command(arbitrary: &mut Arbitrary<'_>) -> ConsensusCommand {
    let execution = admitted_execution(arbitrary);
    let mutation_domains = arbitrary
        .records(Arbitrary::rule_name::<DomainName>)
        .into_iter()
        .collect();
    let policy = admission_policy(arbitrary);
    ConsensusCommand::AdmitCommandExecution {
        execution: Box::new(execution),
        mutation_domains,
        policy,
    }
}

/// The proposal finishing an admitted command, with the result its effect finished with.
fn finish_command(arbitrary: &mut Arbitrary<'_>) -> ConsensusCommand {
    let execution = admitted_execution(arbitrary);
    let effect = execution
        .effect()
        .verified("admission records a command applying its effect");
    let result = command_execution_result(arbitrary, effect);
    let owner = execution
        .owner()
        .cloned()
        .verified("admission records a command with its owner");
    let request_digest = execution
        .request_digest()
        .verified("admission records a command with its request digest");
    let at = arbitrary.timestamp();
    ConsensusCommand::FinishCommandExecution {
        reference: execution.reference,
        owner,
        request_digest,
        at,
        result: Box::new(result),
    }
}

/// The proposal applying one step of the restore an admitted command applies, with the effect that
/// step makes.
fn restore_step_command(arbitrary: &mut Arbitrary<'_>) -> ConsensusCommand {
    let reference = arbitrary.retry_reference();
    let step = arbitrary.restore_step();
    let effect = restore_step_effect(arbitrary, &step, &reference);
    ConsensusCommand::ApplyRestoreStep {
        reference,
        step,
        effect: Box::new(effect),
    }
}

/// The proposal opening a new transaction, under any limit on the transactions open at once.
fn open_transaction_command(arbitrary: &mut Arbitrary<'_>) -> ConsensusCommand {
    let id = arbitrary.transaction_id();
    let domain = arbitrary.rule_name::<DomainName>();
    let owner = arbitrary.rule_name::<UserName>();
    let activity = transaction_activity(arbitrary);
    let transaction = ReplicatedTransaction::open(id, domain, owner, activity);
    ConsensusCommand::OpenTransaction {
        transaction: Box::new(transaction),
        max_open_transactions: any_count(arbitrary),
    }
}

/// The proposal queuing a statement into a transaction, with the report revision the append
/// planned and any queue limits.
fn queue_statement_command(arbitrary: &mut Arbitrary<'_>) -> ConsensusCommand {
    let id = arbitrary.transaction_id();
    let owner = arbitrary.rule_name::<UserName>();
    let domain = arbitrary.rule_name::<DomainName>();
    let activity = transaction_activity(arbitrary);
    let request_reference = arbitrary.execution_reference();
    let expected_position = arbitrary.entropy().count(QUEUED);
    let statement = queued_statement(arbitrary, &id, request_reference, expected_position);
    let report = arbitrary.transaction_impact_report(domain.clone());
    let report = TransactionReportArchive::new(id.clone(), report)
        .assured("distinct topologies of a valid report archive under distinct identities");
    let limits = TransactionQueueLimits {
        max_statements: any_count(arbitrary),
        max_source_bytes: arbitrary.entropy().any_u64(),
    };
    ConsensusCommand::QueueTransactionStatement {
        id,
        owner,
        domain,
        activity,
        statement: Box::new(statement),
        report: Box::new(report),
        limits,
    }
}

/// The proposal admitting a transaction's commit: the report admission froze, the plan admitted for
/// it, and the preview the committing client expected, which is that report's or a stale one.
fn start_commit_command(arbitrary: &mut Arbitrary<'_>) -> ConsensusCommand {
    let domain = arbitrary.rule_name::<DomainName>();
    let report = arbitrary.admitted_impact_report(domain.clone());
    let plan = admission_plan_for(arbitrary, domain, &report);
    let id = plan.decision().preview.transaction_id.clone();
    let report = TransactionReportArchive::new(id.clone(), report)
        .assured("distinct topologies of a valid report archive under distinct identities");
    let expected_preview = if arbitrary.entropy().flag() {
        report.identity().clone()
    } else {
        arbitrary.transaction_preview_identity()
    };
    let owner = arbitrary.rule_name::<UserName>();
    let activity = transaction_activity(arbitrary);
    ConsensusCommand::StartTransactionCommit {
        id,
        owner,
        activity,
        expected_preview,
        report: Box::new(report),
        plan: Box::new(plan),
    }
}

/// The proposal refusing a transaction's commit admission at any operation: the report it planned
/// from, the preview the committing client expected, and the inputs its planning read.
fn fail_commit_admission_command(arbitrary: &mut Arbitrary<'_>) -> ConsensusCommand {
    let id = arbitrary.transaction_id();
    let owner = arbitrary.rule_name::<UserName>();
    let activity = transaction_activity(arbitrary);
    let domain = arbitrary.rule_name::<DomainName>();
    let report = arbitrary.transaction_impact_report(domain.clone());
    let report = TransactionReportArchive::new(id.clone(), report)
        .assured("distinct topologies of a valid report archive under distinct identities");
    let expected_preview = if arbitrary.entropy().flag() {
        report.identity().clone()
    } else {
        arbitrary.transaction_preview_identity()
    };
    let inputs = domain_planning_inputs(arbitrary, domain);
    let operation = arbitrary.transaction_operation_number();
    let error = arbitrary.string();
    ConsensusCommand::FailTransactionCommitAdmission {
        failure: Box::new(TransactionCommitAdmissionFailure {
            id,
            owner,
            activity,
            expected_preview,
            report,
            inputs,
            operation,
            error,
        }),
    }
}

/// The proposal beginning the application of a transaction step over any operations: its result,
/// the effect it made when it succeeded, and the outcome its completion finishes the transaction
/// with.
fn advance_commit_command(arbitrary: &mut Arbitrary<'_>) -> ConsensusCommand {
    let id = arbitrary.transaction_id();
    let domain = arbitrary.rule_name::<DomainName>();
    let operations = arbitrary.transaction_operation_range();
    let last = arbitrary.entropy().flag();
    let step = applying_step(arbitrary, &domain, operations, last);
    let at = arbitrary.timestamp();
    ConsensusCommand::AdvanceTransactionCommit {
        id,
        expected_next_statement: operations.first_index(),
        next_statement: step.next_statement,
        at,
        result: Box::new(step.result),
        effect: step.effect.map(Box::new),
        completion: step.completion,
    }
}

/// The proposal completing the application of a transaction step over any operations, with what it
/// actually did and how its application ended.
fn complete_application_command(arbitrary: &mut Arbitrary<'_>) -> ConsensusCommand {
    let id = arbitrary.transaction_id();
    let domain = arbitrary.rule_name::<DomainName>();
    let operations = arbitrary.transaction_operation_range();
    let at = arbitrary.timestamp();
    let actual = arbitrary.actual_execution_step_impact_of(&domain, operations);
    let outcome = application_outcome(arbitrary, domain);
    ConsensusCommand::CompleteTransactionApplication {
        id,
        expected_next_statement: operations.first_index(),
        at,
        actual: Box::new(actual),
        outcome,
    }
}

/// Every variant of [`ConsensusCommand`], in declaration order, so [`command`] builds each as often
/// as any other and a coverage check can name each one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, strum::EnumIter)]
enum CommandVariant {
    AdmitCommandExecution,
    AcquireCommandDomainMutation,
    ReleaseCommandDomainMutation,
    FinishCommandExecution,
    ReclaimCommandExecutions,
    ReplaceDomainSchedule,
    UpdateKafkaPartitionSchedule,
    ApplyAutomaticDomainSchedule,
    ReconcileOwnershipHandoffPreparations,
    PutDomainAndSchedule,
    PutDomain,
    StartDomain,
    StopDomain,
    PauseDomain,
    ResumeDomain,
    ReconcileDomainClockAuthority,
    CreateUser,
    CreateResourceCatalog,
    BeginResourceUpload,
    PublishResourceUpload,
    PutResourceReplica,
    CompleteResourceUpload,
    FailResourceUpload,
    ApplyRestoreStep,
    BeginRestoreStateInstallation,
    ImportResourceVersion,
    SetNodeCordoned,
    FenceNodeAdmission,
    OpenTransaction,
    QueueTransactionStatement,
    TouchTransaction,
    StartTransactionCommit,
    FailTransactionCommitAdmission,
    AdvanceTransactionCommit,
    CompleteTransactionApplication,
    FinishEmptyTransactionCommit,
    RevertTransaction,
    ExpireTransaction,
    RemoveFinishedTransactions,
}

impl CommandVariant {
    /// The variant `command` is. The match is exhaustive, so a new command does not compile until
    /// [`command`] is taught to build it.
    fn of(command: &ConsensusCommand) -> Self {
        match command {
            ConsensusCommand::AdmitCommandExecution { .. } => Self::AdmitCommandExecution,
            ConsensusCommand::AcquireCommandDomainMutation { .. } => {
                Self::AcquireCommandDomainMutation
            }
            ConsensusCommand::ReleaseCommandDomainMutation { .. } => {
                Self::ReleaseCommandDomainMutation
            }
            ConsensusCommand::FinishCommandExecution { .. } => Self::FinishCommandExecution,
            ConsensusCommand::ReclaimCommandExecutions { .. } => Self::ReclaimCommandExecutions,
            ConsensusCommand::ReplaceDomainSchedule { .. } => Self::ReplaceDomainSchedule,
            ConsensusCommand::UpdateKafkaPartitionSchedule { .. } => {
                Self::UpdateKafkaPartitionSchedule
            }
            ConsensusCommand::ApplyAutomaticDomainSchedule { .. } => {
                Self::ApplyAutomaticDomainSchedule
            }
            ConsensusCommand::ReconcileOwnershipHandoffPreparations { .. } => {
                Self::ReconcileOwnershipHandoffPreparations
            }
            ConsensusCommand::PutDomainAndSchedule { .. } => Self::PutDomainAndSchedule,
            ConsensusCommand::PutDomain { .. } => Self::PutDomain,
            ConsensusCommand::StartDomain { .. } => Self::StartDomain,
            ConsensusCommand::StopDomain { .. } => Self::StopDomain,
            ConsensusCommand::PauseDomain { .. } => Self::PauseDomain,
            ConsensusCommand::ResumeDomain { .. } => Self::ResumeDomain,
            ConsensusCommand::ReconcileDomainClockAuthority { .. } => {
                Self::ReconcileDomainClockAuthority
            }
            ConsensusCommand::CreateUser { .. } => Self::CreateUser,
            ConsensusCommand::CreateResourceCatalog { .. } => Self::CreateResourceCatalog,
            ConsensusCommand::BeginResourceUpload { .. } => Self::BeginResourceUpload,
            ConsensusCommand::PublishResourceUpload { .. } => Self::PublishResourceUpload,
            ConsensusCommand::PutResourceReplica { .. } => Self::PutResourceReplica,
            ConsensusCommand::CompleteResourceUpload { .. } => Self::CompleteResourceUpload,
            ConsensusCommand::FailResourceUpload { .. } => Self::FailResourceUpload,
            ConsensusCommand::ApplyRestoreStep { .. } => Self::ApplyRestoreStep,
            ConsensusCommand::BeginRestoreStateInstallation { .. } => {
                Self::BeginRestoreStateInstallation
            }
            ConsensusCommand::ImportResourceVersion { .. } => Self::ImportResourceVersion,
            ConsensusCommand::SetNodeCordoned { .. } => Self::SetNodeCordoned,
            ConsensusCommand::FenceNodeAdmission { .. } => Self::FenceNodeAdmission,
            ConsensusCommand::OpenTransaction { .. } => Self::OpenTransaction,
            ConsensusCommand::QueueTransactionStatement { .. } => Self::QueueTransactionStatement,
            ConsensusCommand::TouchTransaction { .. } => Self::TouchTransaction,
            ConsensusCommand::StartTransactionCommit { .. } => Self::StartTransactionCommit,
            ConsensusCommand::FailTransactionCommitAdmission { .. } => {
                Self::FailTransactionCommitAdmission
            }
            ConsensusCommand::AdvanceTransactionCommit { .. } => Self::AdvanceTransactionCommit,
            ConsensusCommand::CompleteTransactionApplication { .. } => {
                Self::CompleteTransactionApplication
            }
            ConsensusCommand::FinishEmptyTransactionCommit { .. } => {
                Self::FinishEmptyTransactionCommit
            }
            ConsensusCommand::RevertTransaction { .. } => Self::RevertTransaction,
            ConsensusCommand::ExpireTransaction { .. } => Self::ExpireTransaction,
            ConsensusCommand::RemoveFinishedTransactions { .. } => Self::RemoveFinishedTransactions,
        }
    }
}

/// A replicated command of any kind, each kind as likely as any other.
pub(super) fn command(arbitrary: &mut Arbitrary<'_>) -> ConsensusCommand {
    let variants = CommandVariant::iter().collect::<Vec<_>>();
    let count = NonZeroUsize::new(variants.len()).assured("ConsensusCommand declares variants");
    let variant = variants[arbitrary.entropy().index(count)];
    command_of(arbitrary, variant)
}

/// A replicated command of the `variant` kind, its payload built from the records above: node
/// cordons and admission fences, command admission, domain mutations, completion and retry
/// reclamation, schedules and domain lifecycle and clock authority, users, the resource catalog,
/// restore steps and installation authority, handoff reconciliation, and every transaction
/// lifecycle step with the reports and plans it carries.
fn command_of(arbitrary: &mut Arbitrary<'_>, variant: CommandVariant) -> ConsensusCommand {
    match variant {
        CommandVariant::AdmitCommandExecution => admit_command(arbitrary),
        CommandVariant::AcquireCommandDomainMutation => {
            ConsensusCommand::AcquireCommandDomainMutation {
                reference: arbitrary.execution_reference(),
                owner: arbitrary.rule_name(),
                request_digest: arbitrary.digest(),
                domain: arbitrary.rule_name(),
            }
        }
        CommandVariant::ReleaseCommandDomainMutation => {
            ConsensusCommand::ReleaseCommandDomainMutation {
                reference: arbitrary.execution_reference(),
                owner: arbitrary.rule_name(),
                request_digest: arbitrary.digest(),
                domain: arbitrary.rule_name(),
            }
        }
        CommandVariant::FinishCommandExecution => finish_command(arbitrary),
        CommandVariant::ReclaimCommandExecutions => ConsensusCommand::ReclaimCommandExecutions {
            finished_before: arbitrary.timestamp(),
            retry_fence: arbitrary.timestamp(),
        },
        CommandVariant::ReplaceDomainSchedule => {
            let domain = arbitrary.rule_name::<DomainName>();
            let inputs = domain_planning_inputs(arbitrary, domain.clone());
            let schedule = optional_schedule(arbitrary, &domain);
            ConsensusCommand::ReplaceDomainSchedule {
                inputs: Box::new(inputs),
                schedule,
                mutation: optional_lease(arbitrary),
            }
        }
        CommandVariant::UpdateKafkaPartitionSchedule => {
            let domain = arbitrary.rule_name::<DomainName>();
            let inputs = domain_planning_inputs(arbitrary, domain.clone());
            let schedule = arbitrary.domain_schedule_of(domain);
            ConsensusCommand::UpdateKafkaPartitionSchedule {
                inputs: Box::new(inputs),
                schedule: Box::new(schedule),
            }
        }
        CommandVariant::ApplyAutomaticDomainSchedule => {
            let leader_tenure = leader_tenure(arbitrary);
            let domain = arbitrary.rule_name::<DomainName>();
            let inputs = domain_planning_inputs(arbitrary, domain.clone());
            let schedule = optional_schedule(arbitrary, &domain);
            ConsensusCommand::ApplyAutomaticDomainSchedule {
                fence: AutomaticScheduleFence { leader_tenure },
                inputs: Box::new(inputs),
                schedule,
            }
        }
        CommandVariant::ReconcileOwnershipHandoffPreparations => {
            ConsensusCommand::ReconcileOwnershipHandoffPreparations {
                authority: nervix_models::CoordinationIdentity::new(
                    arbitrary.rule_name(),
                    arbitrary.entropy().any_u64(),
                    arbitrary.entropy().any_u64(),
                ),
            }
        }
        CommandVariant::PutDomainAndSchedule => {
            let domain = arbitrary.rule_name::<DomainName>();
            let inputs = domain_planning_inputs(arbitrary, domain.clone());
            let state = arbitrary.domain_state_of(domain.clone());
            let schedule = optional_schedule(arbitrary, &domain);
            ConsensusCommand::PutDomainAndSchedule {
                inputs: Box::new(inputs),
                domain: Box::new(state),
                schedule,
                mutation: optional_lease(arbitrary),
            }
        }
        CommandVariant::PutDomain => {
            let domain = arbitrary.rule_name::<DomainName>();
            ConsensusCommand::PutDomain {
                domain: Box::new(arbitrary.domain_state_of(domain)),
                mutation: optional_lease(arbitrary),
            }
        }
        CommandVariant::StartDomain => {
            let domain_id = arbitrary.rule_name();
            let start = arbitrary.start_point();
            let clock = optional(arbitrary, Arbitrary::domain_clock_state);
            let authority = optional(arbitrary, Arbitrary::node_identity);
            ConsensusCommand::StartDomain {
                domain_id,
                start,
                clock,
                authority,
                mutation: optional_lease(arbitrary),
            }
        }
        CommandVariant::StopDomain => ConsensusCommand::StopDomain {
            domain_id: arbitrary.rule_name(),
            mutation: optional_lease(arbitrary),
        },
        CommandVariant::PauseDomain => ConsensusCommand::PauseDomain {
            domain_id: arbitrary.rule_name(),
            mutation: optional_lease(arbitrary),
        },
        CommandVariant::ResumeDomain => ConsensusCommand::ResumeDomain {
            domain_id: arbitrary.rule_name(),
            mutation: optional_lease(arbitrary),
        },
        CommandVariant::ReconcileDomainClockAuthority => {
            let domain_id = arbitrary.rule_name();
            let expected_start_version = arbitrary.entropy().any_u64();
            let expected_authority = arbitrary.domain_clock_authority();
            let owner = optional(arbitrary, Arbitrary::node_identity);
            ConsensusCommand::ReconcileDomainClockAuthority {
                domain_id,
                expected_start_version,
                expected_authority,
                owner,
            }
        }
        CommandVariant::CreateUser => ConsensusCommand::CreateUser {
            user: Box::new(user_credentials(arbitrary)),
        },
        CommandVariant::CreateResourceCatalog => ConsensusCommand::CreateResourceCatalog {
            domain: arbitrary.rule_name(),
            identifier: arbitrary.rule_name(),
        },
        CommandVariant::BeginResourceUpload => ConsensusCommand::BeginResourceUpload {
            key: Box::new(upload_key(arbitrary)),
            root_checksum: arbitrary.string(),
        },
        CommandVariant::PublishResourceUpload => ConsensusCommand::PublishResourceUpload {
            key: Box::new(upload_key(arbitrary)),
            resource: Box::new(resource_version(arbitrary)),
            replica: Box::new(replica(arbitrary)),
        },
        CommandVariant::PutResourceReplica => ConsensusCommand::PutResourceReplica {
            replica: Box::new(replica(arbitrary)),
        },
        CommandVariant::CompleteResourceUpload => ConsensusCommand::CompleteResourceUpload {
            key: Box::new(upload_key(arbitrary)),
        },
        CommandVariant::FailResourceUpload => ConsensusCommand::FailResourceUpload {
            key: Box::new(upload_key(arbitrary)),
            reason: arbitrary.string(),
        },
        CommandVariant::ApplyRestoreStep => restore_step_command(arbitrary),
        CommandVariant::BeginRestoreStateInstallation => {
            ConsensusCommand::BeginRestoreStateInstallation {
                reference: arbitrary.execution_reference(),
                domain: arbitrary.rule_name(),
                tenure: leader_tenure(arbitrary),
            }
        }
        CommandVariant::ImportResourceVersion => ConsensusCommand::ImportResourceVersion {
            reference: arbitrary.execution_reference(),
            key: Box::new(upload_key(arbitrary)),
            resource: Box::new(resource_version(arbitrary)),
            replica: Box::new(replica(arbitrary)),
        },
        CommandVariant::SetNodeCordoned => ConsensusCommand::SetNodeCordoned {
            node_id: arbitrary.rule_name(),
            cordoned: arbitrary.entropy().flag(),
        },
        CommandVariant::FenceNodeAdmission => ConsensusCommand::FenceNodeAdmission {
            identity: arbitrary.node_identity(),
        },
        CommandVariant::OpenTransaction => open_transaction_command(arbitrary),
        CommandVariant::QueueTransactionStatement => queue_statement_command(arbitrary),
        CommandVariant::TouchTransaction => ConsensusCommand::TouchTransaction {
            id: arbitrary.transaction_id(),
            owner: arbitrary.rule_name(),
            activity: transaction_activity(arbitrary),
        },
        CommandVariant::StartTransactionCommit => start_commit_command(arbitrary),
        CommandVariant::FailTransactionCommitAdmission => fail_commit_admission_command(arbitrary),
        CommandVariant::AdvanceTransactionCommit => advance_commit_command(arbitrary),
        CommandVariant::CompleteTransactionApplication => complete_application_command(arbitrary),
        CommandVariant::FinishEmptyTransactionCommit => {
            ConsensusCommand::FinishEmptyTransactionCommit {
                id: arbitrary.transaction_id(),
                at: arbitrary.timestamp(),
            }
        }
        CommandVariant::RevertTransaction => ConsensusCommand::RevertTransaction {
            id: arbitrary.transaction_id(),
            owner: arbitrary.rule_name(),
            activity: transaction_activity(arbitrary),
        },
        CommandVariant::ExpireTransaction => ConsensusCommand::ExpireTransaction {
            id: arbitrary.transaction_id(),
            at: arbitrary.timestamp(),
        },
        CommandVariant::RemoveFinishedTransactions => {
            ConsensusCommand::RemoveFinishedTransactions {
                finished_before: arbitrary.timestamp(),
            }
        }
    }
}

/// A Raft log entry at any position: blank, carrying a command, or changing the membership.
pub(super) fn entry(arbitrary: &mut Arbitrary<'_>) -> EntryOf<TypeConfig> {
    let log_id = log_id(arbitrary);
    let payload = match arbitrary.entropy().byte() % 3 {
        0 => openraft::entry::EntryPayload::Blank,
        1 => openraft::entry::EntryPayload::Normal(command(arbitrary)),
        _ => openraft::entry::EntryPayload::Membership(membership(arbitrary)),
    };
    openraft::Entry { log_id, payload }
}

/// How many seeded inputs a coverage check draws its values from.
const SEEDS: u64 = 256;

/// How many bytes one seeded input of the command check holds: as many as the longest input the
/// properties read.
const COMMAND_BYTES: usize = 4096;

/// How many commands the command check builds from one seeded input.
const COMMANDS_PER_SEED: usize = 4;

/// How many bytes one seeded input of the record check holds. A complete revision, with every
/// family drawn before the bytes run out, reads about 38 KiB on average, so this leaves room for
/// the families drawn last.
const REVISION_BYTES: usize = 64 * 1024;

/// `length` bytes that differ from seed to seed: the BLAKE3 output stream of the seed, which
/// spreads the late choices of a value between seeds as evenly as the first.
fn seeded_bytes(seed: u64, length: usize) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&seed.to_le_bytes());
    let mut bytes = vec![0; length];
    hasher.finalize_xof().fill(&mut bytes);
    bytes
}

/// Every variant and shape of the transaction, commit plan, report and command execution records
/// [`state_machine`] promises to reach, as [`RecordWalk`] names them.
const EXPECTED_RECORDS: [&str; 45] = [
    "ReplicatedTransaction::session",
    "ReplicatedTransaction::command",
    "ReplicatedTransaction::latest_preview",
    "ReplicatedTransaction::commit_plan",
    "TransactionStatement::queued",
    "TransactionDiagnostic::located",
    "TransactionDiagnostic::unlocated",
    "TransactionState::Open",
    "TransactionState::Committing",
    "TransactionCommitProgress::results",
    "TransactionCommitProgress::applying",
    "TransactionCommitProgress::domain_mutation",
    "TransactionState::Finished",
    "FinishedTransaction::results",
    "TransactionOutcome::Committed",
    "TransactionOutcome::Failed",
    "TransactionOutcome::PlanningInputsChanged",
    "TransactionOutcome::Reverted",
    "TransactionOutcome::Expired",
    "TransactionCommitPlanRecords::stored",
    "TransactionReportRecords::stored",
    "CommandExecutionRecords::retry_fence",
    "CommandExecutionState::Applying",
    "CommandExecutionState::domain_mutations",
    "CommandExecutionState::Finished",
    "CommandExecutionState::Expired",
    "CommandExecutionEffect::CreateDomain",
    "CommandExecutionEffect::Transaction",
    "CommandExecutionEffect::TransactionRequest",
    "CommandExecutionEffect::Statement",
    "CommandExecutionEffect::CreateUser",
    "CommandExecutionEffect::DropNode",
    "CommandExecutionEffect::Restore",
    "RestoreExecution::completed_steps",
    "RestoreExecution::restored_users",
    "CommandExecutionDisposition::Completed",
    "CommandExecutionDisposition::Failed",
    "CommandExecutionDisposition::PreviewStale",
    "CommandExecutionDiagnostic::located",
    "CommandExecutionDiagnostic::unlocated",
    "CommandExecutionResult::statements",
    "CommandExecutionResult::transaction",
    "CommandExecutionResult::transaction_admission",
    "CommandExecutionResult::backup",
    "CommandExecutionResult::restore",
];

/// Walks generated revisions, asserting the rules the generators promise and recording every
/// variant and shape of the records they hold.
#[derive(Debug, Default)]
struct RecordWalk {
    seen: BTreeSet<&'static str>,
}

impl RecordWalk {
    fn see(&mut self, shape: &'static str) {
        self.seen.insert(shape);
    }

    /// Every transaction is stored under its identity, and every family shows what it holds.
    fn state(&mut self, state: &StateMachineData) {
        for (id, transaction) in state.transactions.iter() {
            assert_eq!(id, &transaction.id);
            self.transaction(transaction);
        }
        if let Some(stored) = state.transaction_commit_plans.stored_records()
            && stored > 0
        {
            self.see("TransactionCommitPlanRecords::stored");
        }
        if let Some(stored) = state.transaction_reports.stored_records()
            && stored > 0
        {
            self.see("TransactionReportRecords::stored");
        }
        if state.command_executions.retry_fence().is_some() {
            self.see("CommandExecutionRecords::retry_fence");
        }
        for execution in state.command_executions.executions() {
            self.execution(execution);
        }
    }

    /// Statements sit at the positions they were appended at, commit progress never runs past
    /// them, and a finished transaction queues nothing.
    fn transaction(&mut self, transaction: &ReplicatedTransaction) {
        if transaction.mutation_owner().is_transaction() {
            self.see("ReplicatedTransaction::session");
        } else {
            self.see("ReplicatedTransaction::command");
        }
        if transaction.latest_preview().is_some() {
            self.see("ReplicatedTransaction::latest_preview");
        }
        if transaction.commit_plan().is_some() {
            self.see("ReplicatedTransaction::commit_plan");
        }
        for (position, statement) in transaction.statements.iter().enumerate() {
            self.see("TransactionStatement::queued");
            assert_eq!(statement.expected_position, position);
            for diagnostic in &statement.admission.diagnostics {
                self.transaction_diagnostic(diagnostic);
            }
        }
        assert!(transaction.pending_statement_count() <= transaction.statements.len());
        match &transaction.state {
            TransactionState::Open(_) => self.see("TransactionState::Open"),
            TransactionState::Committing(progress) => {
                self.see("TransactionState::Committing");
                if !progress.results.is_empty() {
                    self.see("TransactionCommitProgress::results");
                }
                if progress.applying.is_some() {
                    self.see("TransactionCommitProgress::applying");
                }
                if progress.domain_mutation.is_some() {
                    self.see("TransactionCommitProgress::domain_mutation");
                }
            }
            TransactionState::Finished(finished) => {
                self.see("TransactionState::Finished");
                assert!(transaction.statements.is_empty());
                assert_eq!(transaction.queued_source_bytes, 0);
                if !finished.results.is_empty() {
                    self.see("FinishedTransaction::results");
                }
                self.outcome(&finished.outcome);
            }
        }
    }

    /// A located diagnostic's span starts at or before its end.
    fn transaction_diagnostic(&mut self, diagnostic: &TransactionDiagnostic) {
        match diagnostic.span {
            Some(span) => {
                self.see("TransactionDiagnostic::located");
                assert!(span.start <= span.end);
            }
            None => self.see("TransactionDiagnostic::unlocated"),
        }
    }

    fn outcome(&mut self, outcome: &TransactionOutcome) {
        match outcome {
            TransactionOutcome::Committed => self.see("TransactionOutcome::Committed"),
            TransactionOutcome::Failed { .. } => self.see("TransactionOutcome::Failed"),
            TransactionOutcome::PlanningInputsChanged { .. } => {
                self.see("TransactionOutcome::PlanningInputsChanged");
            }
            TransactionOutcome::Reverted => self.see("TransactionOutcome::Reverted"),
            TransactionOutcome::Expired => self.see("TransactionOutcome::Expired"),
        }
    }

    /// An expired execution holds a retry identity whose issue time reads back.
    fn execution(&mut self, execution: &CommandExecution) {
        match &execution.state {
            CommandExecutionState::Applying {
                effect,
                domain_mutations,
                ..
            } => {
                self.see("CommandExecutionState::Applying");
                if !domain_mutations.is_empty() {
                    self.see("CommandExecutionState::domain_mutations");
                }
                self.effect(effect);
            }
            CommandExecutionState::Finished { result, .. } => {
                self.see("CommandExecutionState::Finished");
                self.result(result);
            }
            CommandExecutionState::Expired => {
                self.see("CommandExecutionState::Expired");
                assert!(
                    execution.reference.retry_issued_at().is_ok(),
                    "{}",
                    execution.reference
                );
            }
        }
    }

    fn effect(&mut self, effect: &CommandExecutionEffect) {
        match effect {
            CommandExecutionEffect::CreateDomain { .. } => {
                self.see("CommandExecutionEffect::CreateDomain");
            }
            CommandExecutionEffect::Transaction { .. } => {
                self.see("CommandExecutionEffect::Transaction");
            }
            CommandExecutionEffect::TransactionRequest(_) => {
                self.see("CommandExecutionEffect::TransactionRequest");
            }
            CommandExecutionEffect::Statement { .. } => {
                self.see("CommandExecutionEffect::Statement");
            }
            CommandExecutionEffect::CreateUser { .. } => {
                self.see("CommandExecutionEffect::CreateUser");
            }
            CommandExecutionEffect::DropNode { .. } => self.see("CommandExecutionEffect::DropNode"),
            CommandExecutionEffect::Restore(restore) => {
                self.see("CommandExecutionEffect::Restore");
                if !restore.completed_steps().is_empty() {
                    self.see("RestoreExecution::completed_steps");
                }
                if restore.restored_users().is_some() {
                    self.see("RestoreExecution::restored_users");
                }
            }
        }
    }

    /// A reported transaction never applied more operations than it accepted.
    fn result(&mut self, result: &CommandExecutionResult) {
        match &result.disposition {
            CommandExecutionDisposition::Completed { .. } => {
                self.see("CommandExecutionDisposition::Completed");
            }
            CommandExecutionDisposition::Failed => self.see("CommandExecutionDisposition::Failed"),
            CommandExecutionDisposition::PreviewStale(_) => {
                self.see("CommandExecutionDisposition::PreviewStale");
            }
        }
        for diagnostic in &result.diagnostics {
            match diagnostic.span {
                Some(span) => {
                    self.see("CommandExecutionDiagnostic::located");
                    assert!(span.start <= span.end);
                }
                None => self.see("CommandExecutionDiagnostic::unlocated"),
            }
        }
        if !result.statements.is_empty() {
            self.see("CommandExecutionResult::statements");
        }
        if let Some(transaction) = &result.transaction {
            self.see("CommandExecutionResult::transaction");
            let accepted = transaction.accepted_operations.accepted_operations();
            assert!(transaction.applied_operations <= accepted);
        }
        if result.transaction_admission.is_some() {
            self.see("CommandExecutionResult::transaction_admission");
        }
        if result.backup.is_some() {
            self.see("CommandExecutionResult::backup");
        }
        if result.restore.is_some() {
            self.see("CommandExecutionResult::restore");
        }
    }
}

#[test]
fn every_command_variant_is_built_across_seeds() {
    let mut built = BTreeSet::new();
    for seed in 0..SEEDS {
        let bytes = seeded_bytes(seed, COMMAND_BYTES);
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
        for _ in 0..COMMANDS_PER_SEED {
            let command = command(&mut arbitrary);
            built.insert(CommandVariant::of(&command));
        }
    }
    let declared = CommandVariant::iter().collect::<BTreeSet<_>>();
    let missing = declared.difference(&built).collect::<Vec<_>>();
    assert!(missing.is_empty(), "never built: {missing:?}");
}

#[test]
fn every_record_variant_is_generated_across_seeds() {
    let mut walk = RecordWalk::default();
    for seed in 0..SEEDS {
        let bytes = seeded_bytes(seed, REVISION_BYTES);
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
        let state = state_machine(&mut arbitrary);
        walk.state(&state);
    }
    let expected = BTreeSet::from(EXPECTED_RECORDS);
    let missing = expected.difference(&walk.seen).collect::<Vec<_>>();
    assert!(missing.is_empty(), "never generated: {missing:?}");
    let unexpected = walk.seen.difference(&expected).collect::<Vec<_>>();
    assert!(
        unexpected.is_empty(),
        "generated but not expected: {unexpected:?}"
    );
}
