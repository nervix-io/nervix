//! Applying a batch of parsed statements to the cluster's models.
//!
//! Layer: edges.
//!
//! - **Owns.** The mutation batch, the statement dispatch behind it, and the command results a
//!   session reads back.
//! - **Depends on.** Pure statement routing, the registry to validate, consensus to replicate, and the runtime to
//!   reconcile what a committed model changes.
//! - **Must not know.** How a listener delivered the statement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        edge,
        reason = "session adapter dispatches parsed client statements to mutation owners"
    )
)]

use std::collections::BTreeSet;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{
    ConsensusError, DomainMutationLease, DomainPlanningInputs, ReplicatedTransaction,
    TransactionStepEffect,
};
use nervix_execution::QueueAdmission;
use nervix_interconnect::EntityGatePurpose;
use nervix_models::{
    DomainName, DomainSchedule, DomainStatus, ExecutionStepImpactReport, ImpactDiagnosticKind,
    ModelKind, ModelName, OperationImpactReason, OperationImpactReport, PauseRequirement,
    QuiesceLevel, RequestedResourceVersion, ResourceBindingImpact, Statement, TransactionOperation,
    TransactionOperationNumber,
};
use nervix_plexus::client_statement::ClientStatement;
use tracing::{error, info, warn};

use super::{
    cluster_description::describe_cluster,
    cluster_status::render_cluster_status,
    command_result::{CommandDiagnostic, CommandDisposition, CommandResult},
    completion::CompletionError,
    domain_lifecycle::DomainAlterError,
    entity_gate::{ClusterEntityGateScope, EntityGateParticipants},
    ownership_handoff::{mark_complete_ownership_transitions, planned_ownership_moves},
    schedule_planning::DomainSchedulePlanningSnapshot,
    scheduling::ScheduleTransition,
    session_service::{SessionServiceImpl, create_registry_error_response, find_identifier_span},
    statement_routing::{requires_existing_domain, requires_leader, requires_request_domain},
    transaction::{
        InspectingSession, TransactionCommitError, TransactionModelStepContext,
        transaction_planning_error_message,
    },
};
use crate::{
    registry::{EntityGatePlan, RegistryError, RegistryMutation, ScheduleDelta},
    runtime::RuntimeError,
};

struct TransactionModelDecision {
    planned: crate::registry::PlannedMutations,
    expected_schedule: Option<DomainSchedule>,
    schedule: Option<DomainSchedule>,
    classified_level: QuiesceLevel,
    planned_relocations: usize,
    model_gate: EntityGatePlan,
    ownership_gate: EntityGatePlan,
}

struct DirectModelPlan {
    step: crate::registry::PlannedTransactionStep,
    operations: Vec<OperationImpactReport>,
    inputs: DomainPlanningInputs,
    schedule_inputs: DomainSchedulePlanningSnapshot,
}

/// One statement of a model-mutation batch that reached the registry: which statement it was,
/// the model it changed, and the message its own result reports.
struct AppliedModelMutation {
    index: usize,
    model: ModelName,
    message: AppliedModelMutationMessage,
}

enum AppliedModelMutationMessage {
    Standard(String),
    ResourceRebind(String),
}

impl From<String> for AppliedModelMutationMessage {
    fn from(message: String) -> Self {
        Self::Standard(message)
    }
}

struct CollectedModelMutations {
    results: Vec<Option<CommandResult>>,
    /// The mutations as their statements wrote them. A transaction step already carries the plan
    /// that pinned their resource versions; a direct batch pins them before planning.
    mutations: Vec<RegistryMutation<RequestedResourceVersion>>,
    applied: Vec<AppliedModelMutation>,
}

pub(in crate::application) fn command_ok(message: String) -> CommandResult {
    CommandResult::new(
        CommandDisposition::Completed {
            already_existed: false,
        },
        message,
    )
}

pub(in crate::application) fn command_ok_already_existed(message: String) -> CommandResult {
    CommandResult::new(
        CommandDisposition::Completed {
            already_existed: true,
        },
        message,
    )
}

pub(in crate::application) fn append_command_result(
    results: &mut Vec<CommandResult>,
    result: CommandResult,
) {
    if result.statements.is_empty() {
        results.push(result);
    } else {
        results.extend(result.statements);
    }
}

pub(in crate::application) fn command_batch_result(
    mut previous_results: Vec<CommandResult>,
    result: CommandResult,
    is_batch: bool,
) -> CommandResult {
    if !is_batch {
        return result;
    }

    let transaction = result.transaction.clone();
    append_command_result(&mut previous_results, result);
    let success = previous_results.iter().all(CommandResult::succeeded);
    let diagnostics = match previous_results.last() {
        Some(result) => result.diagnostics.clone(),
        None => Vec::new(),
    };
    // A failed batch reports the disposition of the statement that ended it, so a redirect or a
    // stale preview keeps its meaning when it arrives inside a batch.
    let disposition = if success {
        CommandDisposition::Completed {
            already_existed: false,
        }
    } else {
        match previous_results.last() {
            Some(result) => result.disposition.clone(),
            None => CommandDisposition::Failed,
        }
    };
    let message = command_results_message(&previous_results);
    CommandResult {
        diagnostics,
        statements: previous_results,
        transaction,
        ..CommandResult::new(disposition, message)
    }
}

pub(in crate::application) fn command_results_message(results: &[CommandResult]) -> String {
    results
        .iter()
        .map(|result| result.message.as_str())
        .filter(|message| !message.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

pub(in crate::application) fn append_command_output(message: &mut String, output: &str) {
    if !message.is_empty() {
        message.push_str("; ");
    }
    message.push_str(output);
}

pub(in crate::application) fn quiesce_level_message(level: QuiesceLevel) -> String {
    format!("quiesce level: {}", level.as_str())
}

/// When a command reports the number a `VERSION LATEST` resolved to. An explicit version needs no
/// report, because the statement already names it.
#[derive(Debug, Clone, Copy)]
pub(in crate::application) enum LatestResolutionReport {
    /// A queued statement was admitted. `COMMIT` resolves `LATEST` again against the versions
    /// completed by then.
    Provisional,
    /// The statement was applied and the model stores the reported number.
    Applied,
}

impl LatestResolutionReport {
    /// One message per binding among `bindings` that a statement wrote as `VERSION LATEST`.
    pub(in crate::application) fn messages<'a>(
        self,
        bindings: impl IntoIterator<Item = &'a ResourceBindingImpact>,
    ) -> Vec<String> {
        let qualifier = match self {
            Self::Provisional => "provisionally ",
            Self::Applied => "",
        };
        let mut messages = Vec::new();
        for binding in bindings {
            if let RequestedResourceVersion::Latest = binding.requested {
                messages.push(format!(
                    "{qualifier}resolved VERSION LATEST of resource '{}' to version {} for {} '{}'",
                    binding.resource.as_str(),
                    binding.version,
                    binding.node.kind.as_str(),
                    binding.node.identifier.as_str()
                ));
            }
        }
        messages
    }
}

/// What a model mutation batch reports once it applied, beside the per-statement results.
struct ModelMutationOutcome<'a> {
    classified_level: QuiesceLevel,
    planned_relocations: usize,
    /// The resolved `VERSION LATEST` messages of the applied batch.
    latest_resolutions: &'a [String],
}

fn model_mutation_success_result(
    existing_results: &[Option<CommandResult>],
    applied: &[AppliedModelMutation],
    outcome: ModelMutationOutcome<'_>,
) -> CommandResult {
    let ModelMutationOutcome {
        classified_level,
        planned_relocations,
        latest_resolutions,
    } = outcome;
    let mut results = existing_results.to_vec();
    let mut first_applied = true;
    for mutation in applied {
        let mut message = match &mutation.message {
            AppliedModelMutationMessage::Standard(message)
            | AppliedModelMutationMessage::ResourceRebind(message) => message.clone(),
        };
        if first_applied {
            match &mutation.message {
                AppliedModelMutationMessage::ResourceRebind(_) => {
                    let details = message.find('\n').map(|index| message.split_off(index));
                    message.push('\n');
                    message.push_str(&quiesce_level_message(classified_level));
                    if planned_relocations > 0 {
                        message.push('\n');
                        message.push_str(&format!("planned relocations: {planned_relocations}"));
                    }
                    for resolution in latest_resolutions {
                        message.push('\n');
                        message.push_str(resolution);
                    }
                    if let Some(details) = details {
                        message.push_str(&details);
                    }
                }
                AppliedModelMutationMessage::Standard(_) => {
                    append_command_output(&mut message, &quiesce_level_message(classified_level));
                    append_command_output(
                        &mut message,
                        &format!("planned relocations: {planned_relocations}"),
                    );
                    for resolution in latest_resolutions {
                        append_command_output(&mut message, resolution);
                    }
                }
            }
            first_applied = false;
        }
        results[mutation.index] = Some(command_ok(message));
    }
    let results = results
        .into_iter()
        .map(|result| {
            result.verified(
                "each statement either filled its own slot or was recorded in applied, which this \
                 function fills",
            )
        })
        .collect::<Vec<_>>();
    if results.len() == 1 {
        return results
            .into_iter()
            .next()
            .verified("the length check observed the only model mutation result");
    }
    let message = command_results_message(&results);
    CommandResult {
        statements: results,
        ..command_ok(message)
    }
}

fn resource_usage_kind(kind: ModelKind) -> &'static str {
    match kind {
        ModelKind::Lookup => "hash_map",
        _ => kind.as_str(),
    }
}

pub(in crate::application) fn rebind_resource_message(
    impact: &OperationImpactReport,
) -> Option<String> {
    struct Usage<'a> {
        node: &'a nervix_models::NodeRef,
        from_version: u64,
        to_version: u64,
    }

    let TransactionOperation::RebindResource {
        resource,
        requested,
        version,
        ..
    } = &impact.operation
    else {
        return None;
    };
    let mut usages = impact
        .reasons
        .iter()
        .filter_map(|reason| match reason {
            OperationImpactReason::ResourceRebinding {
                node,
                from_version,
                to_version,
                ..
            } => Some(Usage {
                node,
                from_version: *from_version,
                to_version: *to_version,
            }),
            _ => None,
        })
        .collect::<Vec<_>>();
    usages.sort_by(|left, right| left.node.cmp(right.node));
    let changed = usages
        .iter()
        .filter(|usage| usage.from_version != usage.to_version)
        .count();
    let latest = match requested {
        RequestedResourceVersion::Latest => " (latest)",
        RequestedResourceVersion::Number(_) => "",
    };
    let mut message = format!(
        "rebound {changed} of {} usage(s) of resource '{}' to version {version}{latest}",
        usages.len(),
        resource.as_str()
    );
    for usage in usages {
        let unchanged = if usage.from_version == usage.to_version {
            " unchanged"
        } else {
            ""
        };
        message.push_str(&format!(
            "\n- kind={} name={} from={} to={}{unchanged}",
            resource_usage_kind(usage.node.kind),
            usage.node.identifier.as_str(),
            usage.from_version,
            usage.to_version,
        ));
    }
    Some(message)
}

pub(in crate::application) fn command_error(message: String) -> CommandResult {
    command_failure(message.clone(), message)
}

/// A definitive failure whose one diagnostic, pointing at no part of the source, says
/// `diagnostic`.
pub(in crate::application) fn command_failure(
    message: String,
    diagnostic: String,
) -> CommandResult {
    CommandResult {
        diagnostics: vec![CommandDiagnostic::unlocated(diagnostic)],
        ..CommandResult::new(CommandDisposition::Failed, message)
    }
}

/// A schedule publication that failed, underlining the whole command that asked for it.
fn failed_schedule_publication(domain: &DomainName, error: String, query: &str) -> CommandResult {
    let diagnostic = CommandDiagnostic {
        message: error,
        span: Some(0..query.len()),
    };
    let message = format!(
        "failed to publish schedule for domain '{}'",
        domain.as_str()
    );
    CommandResult {
        diagnostics: vec![diagnostic],
        ..CommandResult::new(CommandDisposition::Failed, message)
    }
}

/// A committed standalone batch whose HTTPS listener TLS configuration did not install on every
/// live node, with what restoring its previous models needs.
struct HttpsListenerRollback<'a> {
    domain: &'a DomainName,
    plan: crate::registry::PlannedMutations,
    classified_level: QuiesceLevel,
    mutation: Option<&'a DomainMutationLease>,
    failure: Report<CompletionError>,
}

impl SessionServiceImpl {
    /// Restores the models a committed standalone batch replaced after an HTTPS listener could not
    /// install them. The restored schedule is applied on every node through its own revision, so
    /// every listener installs the previous certificates again before the batch reports failure.
    async fn roll_back_uninstalled_https_listener(
        &self,
        rollback: HttpsListenerRollback<'_>,
    ) -> CommandResult {
        let HttpsListenerRollback {
            domain,
            plan,
            classified_level,
            mutation,
            failure,
        } = rollback;
        let restored =
            Box::pin(self.rollback_model_alteration(domain, plan, classified_level, mutation))
                .await;
        let message = match restored {
            Ok(()) => {
                let restored_revision =
                    Box::pin(self.inner.consensus.current_runtime_revision()).await;
                match Box::pin(self.wait_for_https_listener_installation(restored_revision)).await {
                    Ok(()) => format!("{failure}; the model batch was rolled back"),
                    Err(restore_failure) => format!(
                        "{failure}; the model batch was rolled back, but the restored HTTPS \
                         listener TLS configuration did not install: {restore_failure}"
                    ),
                }
            }
            Err(rollback_failure) => {
                format!("{failure}; rolling the model batch back also failed: {rollback_failure:#}")
            }
        };
        warn!(
            domain = domain.as_str(),
            error = message.as_str(),
            "an HTTPS listener did not install a committed model batch"
        );
        command_error(message)
    }

    fn collect_model_mutations(
        &self,
        statements: Vec<Statement>,
        domain: &DomainName,
        no_op_operations: Option<&BTreeSet<TransactionOperationNumber>>,
        operation_impacts: Option<&[OperationImpactReport]>,
        first_operation_index: usize,
    ) -> CollectedModelMutations {
        let mut results = vec![None; statements.len()];
        let mut mutations = Vec::new();
        let mut applied = Vec::<AppliedModelMutation>::new();

        for (index, statement) in statements.into_iter().enumerate() {
            match statement {
                Statement::Create(create) => {
                    let if_not_exists = create.if_not_exists;
                    let model = create.body;
                    let model_id = model.name();
                    let model_kind = model.kind();
                    let absolute_index = first_operation_index
                        .checked_add(index)
                        .assured("a collected model mutation is in its transaction step range");
                    let operation = TransactionOperationNumber::from_index(absolute_index).assured(
                        "a configured transaction statement limit keeps indices addressable",
                    );
                    let planned_noop = match no_op_operations {
                        Some(no_op_operations) => no_op_operations.contains(&operation),
                        None => false,
                    };
                    let existing_noop = if no_op_operations.is_none() {
                        match self.inner.registry.contains(domain, model_kind, &model_id) {
                            Ok(exists) => exists && if_not_exists,
                            Err(error) => {
                                results[index] = Some(command_error(format!("{error:#}")));
                                continue;
                            }
                        }
                    } else {
                        false
                    };
                    if planned_noop || existing_noop {
                        results[index] = Some(command_ok_already_existed(format!(
                            "model '{}' already exists in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )));
                        continue;
                    }

                    applied.push(AppliedModelMutation {
                        index,
                        model: model_id.clone(),
                        message: String::new().into(),
                    });
                    mutations.push(RegistryMutation::Create(model));
                }
                Statement::AlterSchema(alter) => {
                    let model_id = alter.schema.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered schema '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterSchema(alter));
                }
                Statement::AlterWireJsonSchema(alter) => {
                    let model_id = alter.schema.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered JSON wire schema '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterWireJsonSchema(alter));
                }
                Statement::AlterWireCborSchema(alter) => {
                    let model_id = alter.schema.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered CBOR wire schema '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterWireCborSchema(alter));
                }
                Statement::AlterWireAvroSchema(alter) => {
                    let model_id = alter.schema.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered AVRO wire schema '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterWireAvroSchema(alter));
                }
                Statement::AlterRelay(alter) => {
                    let model_id = alter.relay.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered relay '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterRelay(alter));
                }
                Statement::AlterJunction(alter) => {
                    let model_id = alter.junction.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered junction '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterJunction(alter));
                }
                Statement::AlterDeduplicator(alter) => {
                    let model_id = alter.deduplicator.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered deduplicator '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterDeduplicator(alter));
                }
                Statement::AlterReorderer(alter) => {
                    let model_id = alter.reorderer.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered reorderer '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterReorderer(alter));
                }
                Statement::AlterEmitter(alter) => {
                    let model_id = alter.emitter.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered emitter '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterEmitter(alter));
                }
                Statement::AlterIngestor(alter) => {
                    let model_id = alter.ingestor.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered ingestor '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterIngestor(alter));
                }
                Statement::AlterReingestor(alter) => {
                    let model_id = alter.reingestor.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered reingestor '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterReingestor(alter));
                }
                Statement::AlterGenerator(alter) => {
                    let model_id = alter.generator.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered generator '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterGenerator(alter));
                }
                Statement::AlterPlacement(alter) => {
                    let model_id = alter.placement.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered placement '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str(),
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::AlterPlacement(alter));
                }
                Statement::Drop(drop) => {
                    let model_id = drop.name.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: model_id.clone(),
                        message: format!(
                            "dropped model '{}' from domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )
                        .into(),
                    });
                    mutations.push(RegistryMutation::Drop(drop));
                }
                Statement::RebindResource(rebind) => {
                    let absolute_index = first_operation_index
                        .checked_add(index)
                        .assured("a collected resource rebind is in its transaction step range");
                    let operation = TransactionOperationNumber::from_index(absolute_index).assured(
                        "a configured transaction statement limit keeps indices addressable",
                    );
                    let impact = operation_impacts.and_then(|impacts| {
                        impacts.iter().find(|impact| impact.number == operation)
                    });
                    let Some(impact) = impact else {
                        results[index] = Some(command_error(
                            "REBIND RESOURCE requires a unified transaction plan".to_string(),
                        ));
                        continue;
                    };
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&rebind.resource),
                        message: AppliedModelMutationMessage::ResourceRebind(
                            rebind_resource_message(impact).verified(
                                "a REBIND statement's unified plan carries its rebind operation",
                            ),
                        ),
                    });
                }
                _ => unreachable!("model mutation batch contains a non-mutation statement"),
            }
        }

        CollectedModelMutations {
            results,
            mutations,
            applied,
        }
    }

    pub(in crate::application) async fn process_model_mutation_batch(
        &self,
        statements: Vec<Statement>,
        query: &str,
        request_domain: Option<&DomainName>,
    ) -> CommandResult {
        Box::pin(self.process_model_mutation_batch_with_transaction(
            statements,
            query,
            request_domain,
            None,
            None,
        ))
        .await
    }

    /// Applies a restore's models to `domain` as one batch, publishing under `lease`, the mutation
    /// lease the restore holds on the domain.
    pub(in crate::application) async fn process_restored_model_batch(
        &self,
        statements: Vec<Statement>,
        query: &str,
        domain: &DomainName,
        lease: &DomainMutationLease,
    ) -> CommandResult {
        Box::pin(self.process_model_mutation_batch_with_transaction(
            statements,
            query,
            Some(domain),
            None,
            Some(lease),
        ))
        .await
    }

    /// Records the effect of a transaction model step in `domain`.
    ///
    /// A test build can make the leader refuse it before appending it, the way a leader whose lease
    /// lapsed or whose retained log is saturated refuses a write.
    async fn publish_transaction_model_step(
        &self,
        _domain: &DomainName,
        transaction: &ReplicatedTransaction,
        impact: ExecutionStepImpactReport,
        result: CommandResult,
        effect: TransactionStepEffect,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        #[cfg(feature = "testing")]
        if let Some(refusal) = self.inner.runtime.take_model_publication_refusal(_domain) {
            return Err(Report::new(self.refused_write(refusal))
                .change_context(nervix_consensus::ConsensusTransactionError::Consensus)
                .change_context(TransactionCommitError::Proposal));
        }
        self.record_transaction_step(transaction, impact, result, Some(effect))
            .await
    }

    /// Applies one batch of model mutations. A transaction step publishes under its transaction's
    /// lease, and a batch a command owns under `command_lease`, the lease that command holds on the
    /// domain; a batch with neither presents none.
    pub(in crate::application) async fn process_model_mutation_batch_with_transaction(
        &self,
        statements: Vec<Statement>,
        query: &str,
        request_domain: Option<&DomainName>,
        transaction_step: Option<TransactionModelStepContext<'_>>,
        command_lease: Option<&DomainMutationLease>,
    ) -> CommandResult {
        let Some(domain) = request_domain.cloned() else {
            return command_error("no active domain selected".to_string());
        };
        let domain_mutation = match (&transaction_step, command_lease) {
            (Some(step), _) => step.transaction.domain_mutation().cloned(),
            (None, Some(lease)) => Some(lease.clone()),
            (None, None) => None,
        };

        let leader = Box::pin(self.inner.consensus.current_leader()).await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            return Box::pin(self.not_leader_response(query, leader.into())).await;
        }

        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_command_admission_if_armed(self.inner.consensus.local_node_id())
            .await;

        let direct_plan = if transaction_step.is_none()
            && statements
                .iter()
                .any(|statement| matches!(statement, Statement::RebindResource(_)))
        {
            let captured = match Box::pin(self.plan_transaction_statements(
                &domain,
                &statements,
                &[],
                0,
                false,
            ))
            .await
            {
                Ok(captured) => captured,
                Err(error) => return command_error(transaction_planning_error_message(&error)),
            };
            let Some(step) = captured.plan.steps().first().cloned() else {
                return command_error(
                    "resource rebind planning produced no model step".to_string(),
                );
            };
            Some(DirectModelPlan {
                step,
                operations: captured.plan.operations().to_vec(),
                inputs: captured.inputs,
                schedule_inputs: captured.schedule_inputs,
            })
        } else {
            None
        };

        let _alter_guard = match self.inner.runtime.try_begin_domain_alter(&domain) {
            Some(guard) => guard,
            None => {
                return command_error(
                    DomainAlterError::ConcurrentAlter {
                        domain: domain.clone(),
                    }
                    .to_string(),
                );
            }
        };
        let Some(domain_state) = Box::pin(self.inner.consensus.current_domain(&domain)).await
        else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        let adopted_domain_pause =
            matches!(domain_state.status, DomainStatus::Paused) && transaction_step.is_some();
        if let DomainStatus::Paused = domain_state.status
            && !adopted_domain_pause
        {
            return command_error(format!(
                "domain '{}' is paused by a model alteration",
                domain.as_str()
            ));
        }
        // A coordinator before this one engaged the pause for this step and stopped before it could
        // release it, so the pause is this transaction's own. Recording it before anything can end
        // the step lets every outcome release it under the transaction's lease, including a
        // refusal this step records instead of applying.
        let adopted_pause_attempt = match transaction_step.as_ref() {
            Some(step) if adopted_domain_pause => {
                let attempt = step.actual.request(PauseRequirement::Domain {
                    domain: domain.clone(),
                });
                step.actual.confirm(attempt);
                Some(attempt)
            }
            Some(_) | None => None,
        };
        let CollectedModelMutations {
            results,
            mutations,
            applied,
        } = {
            let planned_step = match (&transaction_step, &direct_plan) {
                (Some(step), _) => Some(&step.planned_step),
                (None, Some(plan)) => Some(&plan.step),
                (None, None) => None,
            };
            let no_op_operations = match planned_step {
                Some(step) => match &step.kind {
                    crate::registry::PlannedTransactionStepKind::Models { plan } => {
                        Some(&plan.no_op_operations)
                    }
                    _ => None,
                },
                None => None,
            };
            let first_operation_index = match transaction_step.as_ref() {
                Some(step) => step.first_statement,
                None => 0,
            };
            let operation_impacts = match (&transaction_step, &direct_plan) {
                (Some(step), _) => Some(step.operations.as_slice()),
                (None, Some(plan)) => Some(plan.operations.as_slice()),
                (None, None) => None,
            };
            self.collect_model_mutations(
                statements,
                &domain,
                no_op_operations,
                operation_impacts,
                first_operation_index,
            )
        };
        // Validation, quiescence, persistence, and activation are separate control-plane phases.
        // Keep their futures indirect so this coordinator does not reserve all of their state in
        // one debug poll frame.
        let mut completed_result = None;
        let mut recorded_transaction = None;
        let mut transaction_application_failure = None;
        let mut transaction_application_deferred = false;
        if !applied.is_empty() {
            let error_target = applied
                .first()
                .map(|mutation| mutation.model.clone())
                .verified(
                    "every arm that records a mutation records an applied model in the same step",
                );
            let planned_step = match (&transaction_step, &direct_plan) {
                (Some(step), _) => Some(&step.planned_step),
                (None, Some(plan)) => Some(&plan.step),
                (None, None) => None,
            };
            let transaction_decision = match planned_step {
                Some(step) => {
                    let crate::registry::PlannedTransactionStepKind::Models { plan } = &step.kind
                    else {
                        return command_error(
                            "transaction model execution received a non-model plan".to_string(),
                        );
                    };
                    let Some(planned) = plan.planned.clone() else {
                        return command_error(
                            "transaction model execution received an incomplete plan".to_string(),
                        );
                    };
                    Some(TransactionModelDecision {
                        planned,
                        expected_schedule: plan.expected_schedule.clone(),
                        schedule: plan.schedule.clone(),
                        classified_level: step.impact.planned().pause.level(),
                        planned_relocations: step.impact.planned().effects.ownership_moves.len(),
                        model_gate: plan.model_gate.clone(),
                        ownership_gate: plan.ownership_gate.clone(),
                    })
                }
                None => None,
            };
            let operation_impacts = match (&transaction_step, &direct_plan) {
                (Some(step), _) => Some(step.operations.as_slice()),
                (None, Some(plan)) => Some(plan.operations.as_slice()),
                (None, None) => None,
            };
            let latest_resolutions = match (planned_step, operation_impacts) {
                (Some(step), Some(operation_impacts)) => {
                    let rebind_operations = operation_impacts
                        .iter()
                        .filter_map(|impact| match impact.operation {
                            TransactionOperation::RebindResource { .. } => Some(impact.number),
                            _ => None,
                        })
                        .collect::<BTreeSet<_>>();
                    LatestResolutionReport::Applied.messages(
                        step.impact
                            .planned()
                            .effects
                            .resource_bindings
                            .as_slice()
                            .iter()
                            .filter(|binding| {
                                !binding
                                    .attribution
                                    .operations()
                                    .iter()
                                    .any(|operation| rebind_operations.contains(operation))
                            }),
                    )
                }
                (None, None) => Vec::new(),
                _ => Vec::new(),
            };
            let planned = match &transaction_decision {
                Some(decision) => decision.planned.clone(),
                None => {
                    // A batch outside a transaction resolves the resource versions it wrote
                    // against the versions completed now, when it is applied.
                    let resources = Box::pin(self.inner.consensus.current_resources()).await;
                    let mut pinned_mutations = Vec::with_capacity(mutations.len());
                    for mutation in mutations.iter().cloned() {
                        let pinned = mutation.pin_resource_versions(|resource, requested| {
                            resources
                                .uploads
                                .resolve_completed_version(&domain, resource, requested)
                                .map(|id| id.version)
                        });
                        match pinned {
                            Ok(mutation) => pinned_mutations.push(mutation),
                            Err(error) => return command_error(error.to_string()),
                        }
                    }
                    match self
                        .inner
                        .registry
                        .plan_mutations(&domain, &pinned_mutations)
                    {
                        Ok(planned) => planned,
                        Err(err) => {
                            warn!(
                                domain = domain.as_str(),
                                error = %err,
                                "failed to plan model mutation batch"
                            );
                            return create_registry_error_response(
                                query,
                                &domain,
                                &error_target,
                                &err,
                            );
                        }
                    }
                }
            };
            let client_admission = if domain_mutation.is_some() {
                QueueAdmission::WaitForPlace
            } else {
                QueueAdmission::RefuseWhenFull
            };
            if let Err(error) = Box::pin(self.validate_changed_model_bindings(
                &domain,
                domain_state.config.pace,
                &planned,
                client_admission,
            ))
            .await
            {
                return command_error(
                    super::model_validation::client_validation_message(&error)
                        .unwrap_or_else(|| format!("{error:#}")),
                );
            }
            let prepared_udfs = match Box::pin(self.prepare_planned_domain_udfs(&planned)).await {
                Ok(prepared) => prepared,
                Err(error) => return command_error(format!("{error:#}")),
            };
            let base_classified_level = if let DomainStatus::Running = domain_state.status {
                planned.quiesce().level()
            } else {
                QuiesceLevel::Dynamic
            };
            let affected_entities = planned.quiesce().affected_entities().to_vec();
            let entity_pause_roots = planned
                .quiesce()
                .changed()
                .iter()
                .filter(|change| change.level.requires_entity_pause())
                .map(|change| change.node.clone())
                .collect::<Vec<_>>();
            let is_noop = planned.is_noop();
            let changes_vhosts = planned.changes_vhosts();
            let mut cluster_entity_gate = None;
            let mut ownership_handoff = None;
            let mut domain_pause_attempt = adopted_pause_attempt;
            // Ordered plans and direct model commits share this schedule publication boundary.
            #[cfg(feature = "testing")]
            if !is_noop
                && self
                    .inner
                    .runtime
                    .take_armed_schedule_publication_fault(&domain)
            {
                let error = format!(
                    "injected schedule publication fault for domain '{}'",
                    domain.as_str()
                );
                return failed_schedule_publication(&domain, error, query);
            }
            let ScheduleTransition {
                expected_schedule,
                mut prepared_schedule,
                planned_relocations,
                mut inputs,
                planning,
                transaction_eligibility,
            } = if let Some(decision) = &transaction_decision {
                if is_noop {
                    ScheduleTransition::default()
                } else {
                    let captured_inputs = match (&transaction_step, &direct_plan) {
                        (Some(step), _) => Some(step.inputs.clone()),
                        (None, Some(plan)) => Some(plan.inputs.clone()),
                        (None, None) => None,
                    };
                    let captured_schedule_inputs = match (&transaction_step, &direct_plan) {
                        (Some(_), _) => None,
                        (None, Some(plan)) => Some(plan.schedule_inputs.clone()),
                        (None, None) => None,
                    };
                    ScheduleTransition {
                        expected_schedule: decision.expected_schedule.clone(),
                        prepared_schedule: decision.schedule.clone(),
                        planned_relocations: decision.planned_relocations,
                        inputs: captured_inputs,
                        planning: captured_schedule_inputs,
                        transaction_eligibility: transaction_step
                            .as_ref()
                            .map(|step| step.eligibility.clone()),
                    }
                }
            } else if !is_noop {
                let prepared = Box::pin(self.prepare_domain_schedule(
                    &domain,
                    planned.candidate_graph(),
                    domain_state.config.placement,
                ))
                .await;
                ScheduleTransition {
                    expected_schedule: prepared.inputs.schedule().cloned(),
                    prepared_schedule: prepared.schedule,
                    planned_relocations: prepared.relocations,
                    inputs: Some(prepared.inputs),
                    planning: Some(prepared.planning),
                    transaction_eligibility: None,
                }
            } else {
                ScheduleTransition::default()
            };
            // The plan captured its inputs before this transaction paused the domain. An adopted
            // pause is this transaction's own transition, so this step expects the inputs as that
            // pause left them, and publishes against them.
            if adopted_domain_pause && let Some(captured) = inputs.take() {
                inputs = Some(captured.after_domain_pause());
            }
            if !is_noop
                && let Some(inputs) = inputs.as_ref()
                && let Err(error) = self.validate_domain_planning_inputs(inputs).await
            {
                let message = error.to_string();
                if let Some(step) = transaction_step.as_ref() {
                    step.retain_planning_input_conflict(message.clone());
                }
                return command_error(message);
            }
            if !is_noop
                && let Some(planning) = planning.as_ref()
                && let Err(error) = planning.validate_eligibility(self).await
            {
                let message = error.to_string();
                if let Some(step) = transaction_step.as_ref() {
                    step.retain_planning_input_conflict(message.clone());
                }
                return command_error(message);
            }
            if !is_noop
                && let Some(eligibility) = transaction_eligibility.as_ref()
                && let Err(error) = self
                    .validate_transaction_schedule_eligibility(eligibility)
                    .await
            {
                let message = error.to_string();
                if let Some(step) = transaction_step.as_ref() {
                    step.retain_planning_input_conflict(message.clone());
                }
                return command_error(message);
            }
            let schedule_delta =
                ScheduleDelta::between(expected_schedule.as_ref(), prepared_schedule.as_ref());
            let classified_level = match &transaction_decision {
                Some(decision) => decision.classified_level,
                None => {
                    if !matches!(domain_state.status, DomainStatus::Running) {
                        QuiesceLevel::Dynamic
                    } else {
                        let ownership_level = if planned_relocations > 0 {
                            QuiesceLevel::EntityPause
                        } else {
                            QuiesceLevel::Dynamic
                        };
                        base_classified_level
                            .max(ownership_level)
                            .max(schedule_delta.quiesce_level())
                    }
                }
            };
            let requires_domain_pause = classified_level.requires_domain_pause();
            if transaction_step.is_none()
                && let Some(prepared_schedule) = prepared_schedule.as_mut()
            {
                mark_complete_ownership_transitions(expected_schedule.as_ref(), prepared_schedule);
            }
            let model_gate = match &transaction_decision {
                Some(decision) => decision.model_gate.clone(),
                None if matches!(domain_state.status, DomainStatus::Running)
                    && classified_level.requires_entity_pause() =>
                {
                    let mut affected_entities = if base_classified_level.requires_entity_pause() {
                        affected_entities.clone()
                    } else {
                        Vec::new()
                    };
                    let mut changed_entities = if base_classified_level.requires_entity_pause() {
                        entity_pause_roots.clone()
                    } else {
                        Vec::new()
                    };
                    let schedule_gate_entities = schedule_delta.entity_gate_entities();
                    affected_entities.extend(schedule_gate_entities.iter().cloned());
                    changed_entities.extend(schedule_gate_entities);
                    EntityGatePlan::for_model_change(
                        expected_schedule.as_ref(),
                        affected_entities,
                        changed_entities,
                    )
                }
                None => EntityGatePlan::default(),
            };
            let ownership_gate = match &transaction_decision {
                Some(decision) => decision.ownership_gate.clone(),
                None => {
                    let moved_entities = planned_ownership_moves(
                        expected_schedule.as_ref(),
                        prepared_schedule.as_ref(),
                    )
                    .into_iter()
                    .map(|moved| moved.entity);
                    EntityGatePlan::for_ownership_handoff(
                        expected_schedule.as_ref(),
                        prepared_schedule.as_ref(),
                        moved_entities,
                    )
                }
            };
            if is_noop {
                info!(
                    domain = domain.as_str(),
                    "model mutation batch has no model diff; skipping persistence and schedule \
                     publication"
                );
                if let Err(error) = Box::pin(self.apply_current_cluster_state()).await {
                    return command_error(format!(
                        "the existing models in domain '{}' are not usable everywhere: {error:#}",
                        domain.as_str()
                    ));
                }
            }
            if !is_noop && requires_domain_pause && !adopted_domain_pause {
                let pause = Box::pin(self.pause_and_drain_domain_for_alter(
                    &domain,
                    domain_mutation.as_ref(),
                    transaction_step.as_ref().map(|step| step.actual),
                ))
                .await;
                let attempt = match pause {
                    Ok(attempt) => attempt,
                    Err(error) => {
                        let message = format!("{error:#}");
                        let response = match error.downcast_ref::<ConsensusError>() {
                            Some(cause) => {
                                Box::pin(self.consensus_error_response(cause, message)).await
                            }
                            None => command_error(message),
                        };
                        if response.is_not_leader()
                            && let Some(step) = transaction_step.as_ref()
                        {
                            *step.outcome.lock() = Some(Err(error.change_context(
                                TransactionCommitError::RecoverQuiescence {
                                    id: step.transaction.id.clone(),
                                },
                            )));
                        }
                        return response;
                    }
                };
                domain_pause_attempt = attempt;
            }
            if !is_noop
                && requires_domain_pause
                && adopted_domain_pause
                && let Err(error) = Box::pin(self.drain_adopted_domain_pause(&domain)).await
            {
                // The step records this failure, and its application completion releases the
                // adopted pause under the transaction's lease before the outcome is recorded.
                if let Some(step) = transaction_step.as_ref()
                    && let Some(attempt) = domain_pause_attempt
                {
                    step.actual.fail(
                        attempt,
                        ImpactDiagnosticKind::Quiescence,
                        format!("{error:#}"),
                    );
                }
                return command_error(format!("{error:#}"));
            }
            if !is_noop && requires_domain_pause && !adopted_domain_pause {
                let paused_inputs = inputs
                    .take()
                    .verified("a model plan that pauses its domain carries publication inputs")
                    .after_domain_pause();
                inputs = Some(paused_inputs);
            }
            if !is_noop && !model_gate.affected_entities().is_empty() {
                let relays = model_gate.relays();
                let affected_entities = model_gate.affected_entities();
                let deadline = nervix_primitives::time::Instant::now()
                    + self.inner.runtime.entity_gate_deadline();
                let gate = match Box::pin(self.engage_cluster_entity_gates(
                    ClusterEntityGateScope {
                        domain: &domain,
                        relays,
                        affected_entities,
                        purpose: EntityGatePurpose::ModelAlteration,
                        participants: EntityGateParticipants::EveryAvailableNode,
                    },
                    deadline,
                    transaction_step.as_ref().map(|step| {
                        (
                            step.actual,
                            step.planned_step.impact.planned().pause.clone(),
                        )
                    }),
                ))
                .await
                {
                    Ok(gate) => gate,
                    Err(error) => return command_error(error.to_string()),
                };
                #[cfg(feature = "testing")]
                self.inner.runtime.pause_entity_gate_if_armed(&domain).await;
                if let Err(error) = Box::pin(self.wait_for_cluster_entity_drain(
                    &gate,
                    relays,
                    affected_entities,
                    EntityGatePurpose::ModelAlteration,
                    &[],
                    deadline,
                ))
                .await
                {
                    if let Some(step) = transaction_step.as_ref()
                        && let Some(attempt) = gate.impact_attempt()
                    {
                        step.actual.fail(
                            attempt,
                            ImpactDiagnosticKind::Quiescence,
                            error.to_string(),
                        );
                    }
                    Box::pin(self.release_cluster_entity_gates_recording(
                        gate,
                        transaction_step.as_ref().map(|step| step.actual),
                    ))
                    .await;
                    return command_error(error.to_string());
                }
                cluster_entity_gate = Some(gate);
            }
            if !is_noop && planned_relocations > 0 {
                ownership_handoff =
                    match Box::pin(self.begin_planned_ownership_handoff_with_exact_gate(
                        &domain,
                        expected_schedule.as_ref(),
                        prepared_schedule.as_ref(),
                        &ownership_gate,
                        transaction_step.as_ref().map(|step| {
                            (
                                step.actual,
                                step.planned_step.impact.planned().pause.clone(),
                            )
                        }),
                    ))
                    .await
                    {
                        Ok(handoff) => handoff,
                        Err(error) => {
                            if let Some(gate) = cluster_entity_gate.take() {
                                Box::pin(self.release_cluster_entity_gates_recording(
                                    gate,
                                    transaction_step.as_ref().map(|step| step.actual),
                                ))
                                .await;
                            }
                            return command_error(error.to_string());
                        }
                    };
            }
            let domain_pause_impact = || {
                transaction_step
                    .as_ref()
                    .and_then(|step| domain_pause_attempt.map(|attempt| (step.actual, attempt)))
            };

            let mut rollback_plan = None;
            // The runtime revision whose HTTPS listener installation a direct batch confirms, once
            // its models are committed.
            let mut listener_revision = None;
            if !is_noop {
                rollback_plan = Some(planned.clone());
                let _runtime_changes = match self.inner.registry.commit_planned(planned) {
                    Ok(changes) => changes,
                    Err(err) => {
                        if let Some(step) = transaction_step.as_ref()
                            && let Some(attempt) = cluster_entity_gate
                                .as_ref()
                                .and_then(|gate| gate.impact_attempt())
                        {
                            step.actual.fail(
                                attempt,
                                ImpactDiagnosticKind::Application,
                                err.to_string(),
                            );
                        }
                        if let Some(handoff) = ownership_handoff.take() {
                            Box::pin(self.abort_planned_ownership_handoff(
                                &domain,
                                handoff,
                                transaction_step.as_ref().map(|step| step.actual),
                            ))
                            .await;
                        }
                        if let Some(gate) = cluster_entity_gate.take() {
                            Box::pin(self.release_cluster_entity_gates_recording(
                                gate,
                                transaction_step.as_ref().map(|step| step.actual),
                            ))
                            .await;
                        }
                        if let RegistryError::ConcurrentMutation { .. } = err.current_context() {
                            error!(
                                domain = domain.as_str(),
                                error = %err,
                                "registry base-model CAS fired while the exclusive domain ALTER \
                                 lock was held"
                            );
                        }
                        let resume_error = if requires_domain_pause {
                            Box::pin(self.resume_domain_after_alter_with_impact(
                                &domain,
                                domain_mutation.as_ref(),
                                domain_pause_impact(),
                            ))
                            .await
                            .err()
                        } else {
                            None
                        };
                        warn!(
                            domain = domain.as_str(),
                            error = %err,
                            "failed to apply model mutation batch"
                        );
                        if let Some(resume_error) = resume_error {
                            return command_error(format!(
                                "failed to apply model mutation batch: {err}; {resume_error:#}"
                            ));
                        }
                        return create_registry_error_response(query, &domain, &error_target, &err);
                    }
                };
                if let Some(prepared_udfs) = prepared_udfs {
                    self.inner
                        .runtime
                        .install_prepared_domain_udfs(&domain, prepared_udfs);
                }

                if let Some(transaction_step) = transaction_step.as_ref() {
                    let step_result = model_mutation_success_result(
                        &results,
                        &applied,
                        ModelMutationOutcome {
                            classified_level,
                            planned_relocations,
                            latest_resolutions: &latest_resolutions,
                        },
                    );
                    let effect = TransactionStepEffect::ReplaceDomainSchedule {
                        inputs: Box::new(inputs.clone().verified(
                            "a transaction model plan carries its captured publication inputs",
                        )),
                        schedule: prepared_schedule.clone().map(Box::new),
                    };
                    transaction_step.actual.begin_application(
                        transaction_step
                            .planned_step
                            .impact
                            .planned()
                            .effects
                            .clone(),
                    );
                    match Box::pin(
                        self.publish_transaction_model_step(
                            &domain,
                            transaction_step.transaction,
                            transaction_step
                                .actual
                                .apply_to(transaction_step.planned_step.impact.clone()),
                            step_result,
                            effect,
                        ),
                    )
                    .await
                    {
                        Ok(transaction) => {
                            self.pause_transaction_commit_if_armed(&transaction).await;
                            let activation_error =
                                Box::pin(self.apply_current_cluster_state_recording_recovery(
                                    transaction_step.actual,
                                    &transaction,
                                ))
                                .await;
                            recorded_transaction = Some(transaction);
                            if let Some(handoff) = ownership_handoff.take() {
                                if let Some(error) = &activation_error {
                                    transaction_application_failure = Some(format!(
                                        "committed transaction model step in domain '{}' failed \
                                         runtime activation before ownership handoff completion: \
                                         {error:#}",
                                        domain.as_str()
                                    ));
                                    self.defer_planned_ownership_handoff_release(
                                        &domain,
                                        handoff,
                                        error,
                                        Some(transaction_step.actual),
                                    );
                                } else {
                                    if let Err(error) =
                                        Box::pin(self.finish_planned_ownership_handoff(
                                            &domain,
                                            handoff,
                                            Some(transaction_step.actual),
                                        ))
                                        .await
                                    {
                                        transaction_application_failure = Some(format!(
                                            "committed transaction model step in domain '{}' \
                                             failed ownership activation: {error:#}",
                                            domain.as_str()
                                        ));
                                        self.broadcast_error(format!(
                                            "failed to confirm ownership state activation for \
                                             committed transaction model step in domain '{}': \
                                             {error:#}",
                                            domain.as_str()
                                        ));
                                    }
                                }
                            }
                            if let Some(error) = activation_error {
                                if transaction_application_failure.is_none()
                                    && matches!(
                                        error.current_context(),
                                        RuntimeError::RuntimeRevisionPreparation { .. }
                                            | RuntimeError::RuntimeRevisionReadiness { .. }
                                    )
                                {
                                    transaction_application_deferred = true;
                                } else if transaction_application_failure.is_none() {
                                    transaction_application_failure = Some(format!(
                                        "committed transaction model step in domain '{}' failed \
                                         runtime activation: {error:#}",
                                        domain.as_str()
                                    ));
                                }
                                self.broadcast_error(format!(
                                    "failed to reconcile committed transaction model step in \
                                     domain '{}': {error:#}",
                                    domain.as_str()
                                ));
                            }
                        }
                        Err(error) => {
                            if let Some(handoff) = ownership_handoff.take() {
                                Box::pin(self.abort_planned_ownership_handoff(
                                    &domain,
                                    handoff,
                                    Some(transaction_step.actual),
                                ))
                                .await;
                            }
                            if let Some(gate) = cluster_entity_gate.take() {
                                Box::pin(self.release_cluster_entity_gates_recording(
                                    gate,
                                    Some(transaction_step.actual),
                                ))
                                .await;
                            }
                            let rollback_error = if let Some(plan) = rollback_plan.take() {
                                match self.inner.registry.rollback_committed(plan) {
                                    Ok(_) => None,
                                    Err(rollback) => Some(rollback.to_string()),
                                }
                            } else {
                                None
                            };
                            let resume_error = if requires_domain_pause {
                                Box::pin(self.resume_domain_after_alter_with_impact(
                                    &domain,
                                    domain_mutation.as_ref(),
                                    domain_pause_impact(),
                                ))
                                .await
                                .err()
                            } else {
                                None
                            };
                            let error = match rollback_error {
                                Some(rollback) => error.attach(format!(
                                    "local registry rollback also failed: {rollback}"
                                )),
                                None => error,
                            };
                            let error = match resume_error {
                                Some(resume) => error
                                    .attach(format!("the domain also remains paused: {resume:#}")),
                                None => error,
                            };
                            let message = format!(
                                "failed to atomically publish transaction model step for domain \
                                 '{}': {error}",
                                domain.as_str()
                            );
                            *transaction_step.outcome.lock() = Some(Err(error));
                            return command_error(message);
                        }
                    }
                } else {
                    let inputs = inputs.clone().verified(
                        "a non-noop model plan captures authoritative publication inputs",
                    );
                    if let Err(error) = Box::pin(self.inner.consensus.replace_domain_schedule(
                        inputs,
                        prepared_schedule.clone(),
                        domain_mutation.as_ref(),
                    ))
                    .await
                    {
                        let err = ConsensusError::report_message(&error);
                        if let Some(handoff) = ownership_handoff.take() {
                            Box::pin(self.abort_planned_ownership_handoff(
                                &domain,
                                handoff,
                                transaction_step.as_ref().map(|step| step.actual),
                            ))
                            .await;
                        }
                        if let Some(gate) = cluster_entity_gate.take() {
                            Box::pin(self.release_cluster_entity_gates_recording(
                                gate,
                                transaction_step.as_ref().map(|step| step.actual),
                            ))
                            .await;
                        }
                        if let Some(rollback_plan) = rollback_plan.take()
                            && let Err(rollback_error) = Box::pin(self.rollback_model_alteration(
                                &domain,
                                rollback_plan,
                                classified_level,
                                domain_mutation.as_ref(),
                            ))
                            .await
                        {
                            return Box::pin(self.consensus_error_response(
                                error.current_context(),
                                format!(
                                    "failed to publish model alteration schedule for domain '{}': \
                                     {err}; {rollback_error:#}",
                                    domain.as_str()
                                ),
                            ))
                            .await;
                        }
                        self.broadcast_error(format!(
                            "schedule publish failed in domain '{}': {}",
                            domain.as_str(),
                            err
                        ));
                        warn!(
                            domain = domain.as_str(),
                            error = %err,
                            "failed to publish schedule for model mutation batch"
                        );
                        if let ConsensusError::LeadershipLost { observed } = error.current_context()
                        {
                            return Box::pin(self.not_leader_response(query, observed.clone()))
                                .await;
                        }
                        return failed_schedule_publication(&domain, err, query);
                    }
                    listener_revision =
                        Some(Box::pin(self.inner.consensus.current_runtime_revision()).await);
                    if let Err(error) = Box::pin(self.apply_current_cluster_state()).await {
                        if let Some(handoff) = ownership_handoff.take() {
                            self.defer_planned_ownership_handoff_release(
                                &domain,
                                handoff,
                                &error,
                                transaction_step.as_ref().map(|step| step.actual),
                            );
                        }
                        if let Some(gate) = cluster_entity_gate.take() {
                            Box::pin(self.release_cluster_entity_gates_recording(
                                gate,
                                transaction_step.as_ref().map(|step| step.actual),
                            ))
                            .await;
                        }
                        let paused = if requires_domain_pause {
                            match Box::pin(self.resume_domain_after_alter_with_impact(
                                &domain,
                                domain_mutation.as_ref(),
                                domain_pause_impact(),
                            ))
                            .await
                            {
                                Ok(()) => String::new(),
                                Err(resume) => {
                                    format!("; the domain also remains paused: {resume:#}")
                                }
                            }
                        } else {
                            String::new()
                        };
                        return command_error(format!(
                            "committed models and schedule for domain '{}', but the destination \
                             failed to activate: {error}{paused}",
                            domain.as_str()
                        ));
                    }
                    if let Some(handoff) = ownership_handoff.take()
                        && let Err(error) = Box::pin(self.finish_planned_ownership_handoff(
                            &domain,
                            handoff,
                            transaction_step.as_ref().map(|step| step.actual),
                        ))
                        .await
                    {
                        if let Some(gate) = cluster_entity_gate.take() {
                            Box::pin(self.release_cluster_entity_gates_recording(
                                gate,
                                transaction_step.as_ref().map(|step| step.actual),
                            ))
                            .await;
                        }
                        let paused = if requires_domain_pause {
                            match Box::pin(self.resume_domain_after_alter_with_impact(
                                &domain,
                                domain_mutation.as_ref(),
                                domain_pause_impact(),
                            ))
                            .await
                            {
                                Ok(()) => String::new(),
                                Err(resume) => {
                                    format!("; the domain also remains paused: {resume:#}")
                                }
                            }
                        } else {
                            String::new()
                        };
                        return command_error(format!(
                            "committed models and schedule for domain '{}', but ownership state \
                             activation did not complete: {error:#}{paused}",
                            domain.as_str(),
                        ));
                    }
                }

                if requires_domain_pause {
                    if let Err(error) = Box::pin(self.wait_for_paused_domain_drain(&domain)).await {
                        if let Some((impact, attempt)) = domain_pause_impact() {
                            impact.fail(
                                attempt,
                                ImpactDiagnosticKind::Quiescence,
                                format!("{error:#}"),
                            );
                        }
                        if transaction_step.is_some() {
                            transaction_application_failure = Some(format!(
                                "committed transaction model step in domain '{}' failed to drain \
                                 its pause: {error:#}",
                                domain.as_str()
                            ));
                            self.broadcast_error(format!(
                                "committed transaction model step in domain '{}' is waiting for \
                                 quiescence recovery: {error:#}",
                                domain.as_str()
                            ));
                        } else {
                            if let Some(rollback_plan) = rollback_plan.take()
                                && let Err(rollback_error) =
                                    Box::pin(self.rollback_model_alteration(
                                        &domain,
                                        rollback_plan,
                                        classified_level,
                                        domain_mutation.as_ref(),
                                    ))
                                    .await
                            {
                                return command_error(format!("{error:#}; {rollback_error:#}"));
                            }
                            return command_error(format!("{error:#}"));
                        }
                    }
                    if let Err(error) = Box::pin(self.resume_domain_after_alter_with_impact(
                        &domain,
                        domain_mutation.as_ref(),
                        domain_pause_impact(),
                    ))
                    .await
                    {
                        if transaction_step.is_some() {
                            // Application completion recovers this hold under the same mutation
                            // lease before it can record a terminal outcome.
                            if let DomainAlterError::RestoreIngestion { .. } =
                                error.current_context()
                            {
                                transaction_application_failure = Some(format!(
                                    "committed transaction model step in domain '{}' failed to \
                                     restore ingestion after releasing its pause: {error:#}",
                                    domain.as_str()
                                ));
                            }
                            self.broadcast_error(format!(
                                "failed to release transaction-owned pause in domain '{}': \
                                 {error:#}",
                                domain.as_str()
                            ));
                        } else {
                            if let Some(rollback_plan) = rollback_plan.take()
                                && let Err(rollback_error) =
                                    Box::pin(self.rollback_model_alteration(
                                        &domain,
                                        rollback_plan,
                                        classified_level,
                                        domain_mutation.as_ref(),
                                    ))
                                    .await
                            {
                                return command_error(format!("{error:#}; {rollback_error:#}"));
                            }
                            return command_error(format!("{error:#}"));
                        }
                    }
                }
            }
            if let Some(gate) = cluster_entity_gate
                && let Err(error) = Box::pin(self.release_cluster_entity_gates_and_wait_recording(
                    gate,
                    transaction_step.as_ref().map(|step| step.actual),
                ))
                .await
            {
                if transaction_step.is_some() {
                    transaction_application_failure = Some(format!(
                        "committed transaction model step in domain '{}' failed to release its \
                         entity gate: {error}",
                        domain.as_str()
                    ));
                } else {
                    return command_error(format!(
                        "committed models in domain '{}', but their entity gate did not release: \
                         {error}",
                        domain.as_str()
                    ));
                }
            }

            // Every live HTTPS listener must present the certificates of the committed VHOSTs. A
            // direct batch that did not pause is rolled back with its failure; a paused batch has
            // already resumed, so it keeps its committed models like any other activation failure.
            // A transaction step confirms its listeners when it records its application outcome.
            if changes_vhosts
                && let Some(revision) = listener_revision
                && let Err(failure) =
                    Box::pin(self.wait_for_https_listener_installation(revision)).await
            {
                if classified_level == QuiesceLevel::Dynamic {
                    return Box::pin(self.roll_back_uninstalled_https_listener(
                        HttpsListenerRollback {
                            domain: &domain,
                            plan: rollback_plan.take().verified(
                                "only failure paths that return take the plan after its commit",
                            ),
                            classified_level,
                            mutation: domain_mutation.as_ref(),
                            failure,
                        },
                    ))
                    .await;
                }
                return command_error(format!(
                    "committed models in domain '{}', but {failure}",
                    domain.as_str()
                ));
            }
            if let Some(operation_impacts) = operation_impacts {
                for impact in operation_impacts {
                    let TransactionOperation::RebindResource {
                        resource, version, ..
                    } = &impact.operation
                    else {
                        continue;
                    };
                    let usage_count = impact
                        .reasons
                        .iter()
                        .filter(|reason| {
                            matches!(reason, OperationImpactReason::ResourceRebinding { .. })
                        })
                        .count();
                    let rebound_count = impact
                        .reasons
                        .iter()
                        .filter(|reason| {
                            matches!(
                                reason,
                                OperationImpactReason::ResourceRebinding {
                                    from_version,
                                    to_version,
                                    ..
                                } if from_version != to_version
                            )
                        })
                        .count();
                    info!(
                        domain = domain.as_str(),
                        resource = resource.as_str(),
                        target_version = *version,
                        usage_count,
                        rebound_count,
                        "rebound resource usages"
                    );
                }
            }
            completed_result = Some(model_mutation_success_result(
                &results,
                &applied,
                ModelMutationOutcome {
                    classified_level,
                    planned_relocations,
                    latest_resolutions: &latest_resolutions,
                },
            ));
        }

        let result = if let Some(result) = completed_result {
            result
        } else {
            model_mutation_success_result(
                &results,
                &applied,
                ModelMutationOutcome {
                    classified_level: QuiesceLevel::Dynamic,
                    planned_relocations: 0,
                    latest_resolutions: &[],
                },
            )
        };
        if let Some(transaction_step) = transaction_step.as_ref()
            && recorded_transaction.is_none()
        {
            transaction_step.actual.begin_application(
                transaction_step
                    .planned_step
                    .impact
                    .planned()
                    .effects
                    .clone(),
            );
            match Box::pin(
                self.record_transaction_step(
                    transaction_step.transaction,
                    transaction_step
                        .actual
                        .apply_to(transaction_step.planned_step.impact.clone()),
                    result.clone(),
                    None,
                ),
            )
            .await
            {
                Ok(transaction) => {
                    recorded_transaction = Some(transaction);
                }
                Err(error) => {
                    let message =
                        format!("failed to record transaction model step progress: {error}");
                    *transaction_step.outcome.lock() = Some(Err(error));
                    return command_error(message);
                }
            }
        }
        if let Some(transaction_step) = transaction_step.as_ref()
            && let Some(transaction) = recorded_transaction
        {
            if transaction_application_deferred {
                *transaction_step.outcome.lock() = Some(Ok(transaction));
            } else {
                match Box::pin(self.record_transaction_application_completion(
                    &transaction,
                    transaction_application_failure,
                    Some(transaction_step.actual.snapshot()),
                ))
                .await
                {
                    Ok(transaction) => {
                        *transaction_step.outcome.lock() = Some(Ok(transaction));
                    }
                    Err(error) => {
                        let message = format!(
                            "failed to record transaction model application outcome: {error}"
                        );
                        *transaction_step.outcome.lock() = Some(Err(error));
                        return command_error(message);
                    }
                }
            }
        }
        result
    }

    pub(in crate::application) async fn process_client_statement(
        &self,
        client_statement: ClientStatement,
        query: &str,
        request_domain: Option<&DomainName>,
        session: InspectingSession<'_>,
    ) -> CommandResult {
        let statement = match client_statement {
            ClientStatement::UseDomain(domain) => {
                return command_error(format!(
                    "USE '{}' is a client-local command and must not be sent to the server",
                    domain.as_str()
                ));
            }
            ClientStatement::ListDomains => {
                return command_error(
                    "LIST DOMAINS is a client-local command; send a list domains request"
                        .to_string(),
                );
            }
            ClientStatement::AttachDomainClock => {
                return command_error(
                    "ATTACH DOMAIN CLOCK is a session-local command; send an attach domain clock \
                     request"
                        .to_string(),
                );
            }
            ClientStatement::DetachDomainClock => {
                return command_error(
                    "DETACH DOMAIN CLOCK is a session-local command; send a detach domain clock \
                     request"
                        .to_string(),
                );
            }
            ClientStatement::UploadResource(upload) => {
                return self.upload_resource_command(upload).await;
            }
            ClientStatement::DescribeBackup(_) => {
                return command_error(
                    "DESCRIBE BACKUP reads an archive on the client's machine and runs in the \
                     client"
                        .to_string(),
                );
            }
            ClientStatement::CreateSubscription(_) => {
                return command_error(
                    "CREATE SUBSCRIPTION must be sent as a subscribe request".to_string(),
                );
            }
            ClientStatement::DeleteSubscription(_) => {
                return command_error(
                    "DELETE SUBSCRIPTION must be sent as an unsubscribe request".to_string(),
                );
            }
            ClientStatement::BeginTransaction
            | ClientStatement::CommitTransaction
            | ClientStatement::RevertTransaction => {
                return command_error(
                    "transaction control commands must be handled by the session transaction"
                        .to_string(),
                );
            }
            ClientStatement::Server(statement) => statement,
        };
        if statement.is_model_mutation() {
            return self
                .process_model_mutation_batch(vec![statement], query, request_domain)
                .await;
        }

        if requires_request_domain(&statement) && request_domain.is_none() {
            return command_error("no active domain selected".to_string());
        }
        let domain = request_domain.cloned();

        if requires_leader(&statement) {
            let leader = self.inner.consensus.current_leader().await;
            if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
                return self.not_leader_response(query, leader.into()).await;
            }
            #[cfg(feature = "testing")]
            self.inner
                .runtime
                .pause_command_admission_if_armed(self.inner.consensus.local_node_id())
                .await;
        }

        if requires_existing_domain(&statement) {
            let domain = domain
                .as_ref()
                .verified("this statement requires a request domain, which was resolved above");
            if self.inner.consensus.current_domain(domain).await.is_none() {
                return command_error(format!("domain '{}' does not exist", domain.as_str()));
            }
        }

        match statement {
            Statement::CreateDomain(create) => self.create_domain(create).await,
            Statement::AlterDomain(alter) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.alter_domain(domain, alter).await
            }
            Statement::CreateUser(create) => self.create_user(create).await,
            Statement::CreateResource(create) => {
                self.create_resource(
                    domain.as_ref().verified(
                        "this statement requires a request domain, which was resolved above",
                    ),
                    create,
                )
                .await
            }
            Statement::UploadResource(upload) => self.upload_resource_command(upload).await,
            Statement::Backup(_) => command_error(
                "BACKUP runs only as an admitted command under its execution reference".to_string(),
            ),
            Statement::Restore(_) => command_error(
                "RESTORE reads an archive on the client's machine; a client sends it on the \
                 restore stream beside the session"
                    .to_string(),
            ),
            Statement::StartDomain(start) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.start_domain(domain, start).await
            }
            Statement::StopDomain(stop) => {
                // `STOP` names no domain, so it acts on the session's domain. It is excluded from
                // `requires_request_domain`, which leaves the session free of one here.
                let Some(domain) = domain.as_ref() else {
                    return command_error("no active domain selected".to_string());
                };
                self.stop_domain(domain, stop).await
            }
            Statement::Create(_)
            | Statement::AlterSchema(_)
            | Statement::AlterWireJsonSchema(_)
            | Statement::AlterWireCborSchema(_)
            | Statement::AlterWireAvroSchema(_)
            | Statement::AlterRelay(_)
            | Statement::AlterJunction(_)
            | Statement::AlterDeduplicator(_)
            | Statement::AlterReorderer(_)
            | Statement::AlterEmitter(_)
            | Statement::AlterIngestor(_)
            | Statement::AlterReingestor(_)
            | Statement::AlterGenerator(_)
            | Statement::AlterPlacement(_)
            | Statement::RebindResource(_)
            | Statement::ResetWasmState(_)
            | Statement::Drop(_) => {
                unreachable!("model mutations are handled before statement dispatch")
            }
            Statement::DropNode(drop) => self.drop_node(drop.node_id).await,
            Statement::CordonNode(cordon) => self.set_node_cordoned(cordon.node_id, true).await,
            Statement::UncordonNode(uncordon) => {
                self.set_node_cordoned(uncordon.node_id, false).await
            }
            Statement::DrainNode(drain) => self.drain_node(drain.node_id, None).await,
            Statement::Relocate(relocation) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.relocate(domain, relocation, None).await
            }
            Statement::DescribeRelocation(relocation) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_relocation(domain, relocation).await
            }
            Statement::DescribeRelay(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_stream(domain, describe).await
            }
            Statement::DescribeDomain(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_domain(domain, describe).await
            }
            Statement::DescribeEndpoint(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_endpoint(domain, describe).await
            }
            Statement::DescribeClient(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_client(domain, describe)
            }
            Statement::DescribeIngestor(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_ingestor(domain, describe).await
            }
            Statement::DescribeLookup(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_lookup(domain, describe).await
            }
            Statement::DescribeJunction(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_junction(domain, describe).await
            }
            Statement::DescribeDeduplicator(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_deduplicator(domain, describe).await
            }
            Statement::DescribeReingestor(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_reingestor(domain, describe).await
            }
            Statement::DescribeCorrelator(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_correlator(domain, describe).await
            }
            Statement::DescribeReorderer(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_reorderer(domain, describe).await
            }
            Statement::DescribeEmitter(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_emitter(domain, describe).await
            }
            Statement::DescribeWindowProcessor(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_window_processor(domain, describe).await
            }
            Statement::DescribeWasmProcessor(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_wasm_processor(domain, describe).await
            }
            Statement::DescribeUdf(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_udf(domain, describe)
            }
            Statement::DescribePlacement(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_placement(domain, describe).await
            }
            Statement::DescribeResource(describe) => {
                self.describe_resource(
                    domain.as_ref().verified(
                        "this statement requires a request domain, which was resolved above",
                    ),
                    describe,
                )
                .await
            }
            Statement::LookupQuery(query) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.lookup_query(domain, query).await
            }
            Statement::ShowCreate(show) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                let model = match self
                    .inner
                    .registry
                    .get_of_kind(domain, show.kind, &show.name)
                {
                    Ok(Some(model)) => model,
                    Ok(None) => {
                        let diagnostic = CommandDiagnostic {
                            message: format!(
                                "{} '{}' not found",
                                show.kind.as_str(),
                                show.name.as_str()
                            ),
                            span: find_identifier_span(query, &show.name),
                        };
                        let message = format!(
                            "{} '{}' does not exist in domain '{}'",
                            show.kind.as_str(),
                            show.name.as_str(),
                            domain.as_str()
                        );
                        return CommandResult {
                            diagnostics: vec![diagnostic],
                            ..CommandResult::new(CommandDisposition::Failed, message)
                        };
                    }
                    Err(error) => {
                        return command_error(format!(
                            "failed to read stored model for SHOW CREATE: {error:#}"
                        ));
                    }
                };

                let canonical = match model.to_public_plexus() {
                    Ok(v) => v,
                    Err(error) => {
                        let diagnostic = CommandDiagnostic::unlocated(
                            "model contains values that cannot be rendered as canonical Plexus"
                                .to_string(),
                        );
                        return CommandResult {
                            diagnostics: vec![diagnostic],
                            ..CommandResult::new(
                                CommandDisposition::Failed,
                                format!("failed to render canonical Plexus: {error:#}"),
                            )
                        };
                    }
                };

                command_ok(canonical)
            }
            Statement::ShowRelayMaterializedState(show) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.show_stream_materialized_state(domain, show).await
            }
            Statement::ShowUdfs(_) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.show_udfs(domain)
            }
            Statement::ShowPlacements(_) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.show_placements(domain).await
            }
            Statement::ShowIngestors(_) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.show_ingestors(domain).await
            }
            Statement::ShowEmitters(_) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.show_emitters(domain).await
            }
            Statement::ShowClusterStatus(_) => {
                let status =
                    render_cluster_status(&self.inner.cluster, &self.inner.consensus).await;
                let initialization = if self.inner.initialization.is_ready() {
                    "ready"
                } else {
                    "initializing"
                };
                command_ok(format!("{status}\nnode.initialization={initialization}"))
            }
            Statement::AlterCluster(_) => {
                command_error("ALTER CLUSTER SET requires durable command admission".to_string())
            }
            Statement::DescribeCluster(_) => {
                command_ok(describe_cluster(&self.inner.cluster, &self.inner.consensus).await)
            }
            Statement::ShowTransactions(_) => self.show_transactions().await,
            Statement::DescribeTransaction(describe) => {
                self.describe_transaction(describe, query, session).await
            }
        }
    }
}

#[cfg(test)]
mod tests;
