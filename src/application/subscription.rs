//! Read-only filtered views a session attaches to a relay.
//!
//! Layer: control plane.
//!
//! - **Owns.** Subscription creation and deletion, the per-session subscription set and the
//!   generation each subscription is opened with, the lifecycle every subscription moves through,
//!   and the filtering and sampling each subscription asks for.
//! - **Depends on.** The schedule for the relay definition a subscription describes, the runtime
//!   for relay receivers, the interconnect to make interest visible on every node, the Row encoder
//!   that writes selected Arrow rows into client frames, and the session's subscription lane.
//! - **Must not know.** Construction, inheritance, values or any other side effect a processor has.
//!
//! The session's ordered requests are the only owner of its subscriptions, and every subscription
//! generation moves through one lifecycle:
//!
//! - **Creating.** A subscribe request validates the statement against the relay's scheduled
//!   definition, takes this node's interest lease on the relay and waits until every live node
//!   sees it, and attaches a receiver under that definition. A refusal at any step releases what
//!   was taken, the request fails, and nothing of the generation remains.
//! - **Active.** The generation joins its session once the reply that announces its schema is
//!   queued, and only then do its rows flow. When that reply is not queued, because the request
//!   was cancelled, the session ended or the reply could not be encoded, the generation is
//!   abandoned unannounced, so a client never receives rows it cannot decode.
//! - **Ended by the server.** When its relay is redefined or removed, the generation releases its
//!   receiver and lease and sends a `SubscriptionEnded` naming why as its last frame. Its name
//!   stays with the session until the client deletes it, reuses the name, or ends the session.
//! - **Withdrawn by the client.** Deleting a generation, or ending its session, withdraws it: the
//!   frames it still has queued are discarded, every wait it is in ends at once even while the
//!   client reads nothing, and it releases its receiver and lease before the reply that deleted it
//!   is queued.
//!
//! A name reused after deletion opens a new generation, so frames about an earlier generation can
//! never be taken for a later one.

mod delivery;
mod interest;

use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU64, NonZeroUsize},
    sync::atomic::{AtomicU64, Ordering},
};

use ahash::{HashMap, HashMapExt};
use blake3::Hasher;
use error_stack::{Report, ResultExt as _};
use futures_util::{StreamExt, stream::FuturesUnordered};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto;
use nervix_client_wire::{RowsSkippedCause, SessionLimits, SubscriptionHandle, SubscriptionOpened};
use nervix_consensus::{
    CommandExecutionTransactionOperation, CommandExecutionTransactionRequest,
    CommandExecutionTransactionTarget, ReplicatedTransaction,
};
use nervix_interconnect::SubscriptionInterestVisibilityRequest as RemoteSubscriptionInterestVisibilityRequest;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeName, CommandExecutionReference, CreateRelay, CreateSchema,
    DomainName, FieldName, ParseAsType, RelayName, ScheduledModel, SchemaName, SubscriptionBinding,
    SubscriptionLiteral, SubscriptionName, TransactionPreviewIdentity, UserName,
};
use nervix_nspl::client_statement::{ClientStatement, ParsedClientStatement};
use nervix_recovery::Discarded as _;
use nonzero_ext::nonzero;
use sorted_vec::SortedSet;
use tokio::{sync::oneshot, task::JoinHandle};
use triomphe::Arc;

use self::delivery::SubscriptionDelivery;
pub(in crate::application) use self::interest::SubscriptionInterests;
#[cfg(test)]
use super::authentication::DEFAULT_USER;
use super::{
    command_result::{CommandDiagnostic, CommandDisposition, CommandResult},
    model_mutation::{
        append_command_result, command_batch_result, command_error, command_ok,
        command_results_message,
    },
    session::outbound::{SessionOutbound, SubscriptionWithdrawal},
    session_service::SessionServiceImpl,
    transaction::{InspectingSession, transaction_status},
};
use crate::{
    runtime::{
        BranchKey, CompiledSubscriptionPredicate, RelayRecordBatch, RelaySubscriptionDefinition,
        Runtime, RuntimeError, SubscriptionPredicateCompileContext, compile_subscription_predicate,
        execute_subscription_predicate_on_record, scheduled_relay_owner_nodes,
    },
    runtime_schema,
    subscription_row::{SubscriptionBranchSchema, SubscriptionRowOpening, subscription_row_schema},
    task_shutdown::JoinShutdown,
};

static SESSION_SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub(in crate::application) enum SessionCommandPlanError {
    #[error("transaction is already active")]
    TransactionAlreadyActive,
    #[error("COMMIT requires an active transaction")]
    CommitWithoutTransaction,
    #[error("REVERT requires an active transaction")]
    RevertWithoutTransaction,
    #[error(
        "DESCRIBE TRANSACTION must be executed separately; it reads a transaction and never joins \
         a multi-statement request"
    )]
    MixedTransactionInspection,
    #[error("transaction command is missing its expected queue position")]
    MissingQueuePosition,
    #[error("transaction queue position overflowed")]
    QueuePositionOverflow,
    #[error("multiple commands require BEGIN")]
    MultipleCommandsWithoutTransaction,
}

#[derive(Debug, Clone, Copy, strum::Display)]
#[strum(serialize_all = "lowercase")]
pub(in crate::application) enum ExpectedSubscriptionLiteral {
    String,
    Boolean,
    Numeric,
    Array,
    #[strum(serialize = "RFC3339 datetime string")]
    Datetime,
}

#[derive(Debug, thiserror::Error)]
pub(in crate::application) enum SubscriptionError {
    #[error("stream '{relay}' is not branched and does not accept WHERE bindings")]
    UnbranchedBindings { relay: RelayName },
    #[error("stream '{relay}' requires WHERE bindings for {fields:?}")]
    MissingBindings {
        relay: RelayName,
        fields: Vec<FieldName>,
    },
    #[error("subscription binding '{field}' is specified more than once")]
    DuplicateBinding { field: FieldName },
    #[error("subscription bindings for relay '{relay}' must exactly match {fields:?}")]
    BindingFieldsMismatch {
        relay: RelayName,
        fields: Vec<FieldName>,
    },
    #[error("branch field '{field}' is missing from schema '{schema}'")]
    MissingSchemaField {
        field: FieldName,
        schema: SchemaName,
    },
    #[error("missing binding for branch field '{field}'")]
    MissingBranchField { field: FieldName },
    #[error("failed to construct subscription branch key")]
    InvalidBranchKey,
    #[error("invalid batch sample rate")]
    InvalidSampleRate,
    #[error("batch sample rate must be between 0.0 and 1.0")]
    SampleRateOutOfRange,
    #[error("subscription binding '{field}' expects {expected} literal for type {ty:?}")]
    InvalidLiteral {
        field: FieldName,
        ty: ParseAsType,
        expected: ExpectedSubscriptionLiteral,
    },
    #[error(
        "subscription interest in relay '{relay}' in domain '{domain}' did not become visible on \
         every live node: {nodes:?}"
    )]
    InterestNotVisible {
        domain: DomainName,
        relay: RelayName,
        nodes: BTreeSet<ClusterNodeName>,
    },
    #[error(
        "failed to check subscription interest in relay '{relay}' in domain '{domain}' on node \
         '{node}'"
    )]
    InterestRequest {
        domain: DomainName,
        relay: RelayName,
        node: ClusterNodeName,
    },
    #[error("subscription interest visibility failed on node '{node}': {failure}")]
    InterestRemoteFailure {
        node: ClusterNodeName,
        failure: nervix_interconnect::RemoteOperationFailure,
    },
    #[error("stream '{relay}' references missing scheduled schema '{schema}'")]
    MissingScheduledSchema {
        relay: RelayName,
        schema: SchemaName,
    },
    #[error("stream '{relay}' has no resolved branch declaration in the schedule")]
    MissingScheduledBranch { relay: RelayName },
}

/// The most rows one frame of a subscription carries. A frame also stops at the session's frame
/// limit, so this bounds the rows a client decodes per frame rather than the frame's size.
const SUBSCRIPTION_ROWS_PER_FRAME: NonZeroUsize = nonzero!(256_usize);

/// Where a session's subscriptions queue their frames, and the limits those frames are held to.
#[derive(Clone)]
pub(in crate::application) struct SessionDelivery {
    pub(in crate::application) outbound: SessionOutbound,
    pub(in crate::application) limits: SessionLimits,
}

/// One subscription generation its session announced, until it is withdrawn.
struct SessionSubscription {
    handle: SubscriptionHandle,
    domain: DomainName,
    /// Discards the frames the generation still has queued and ends every wait of its delivery.
    withdrawal: SubscriptionWithdrawal,
    /// Finishes once delivery has stopped and released the relay receiver and the interest lease:
    /// when the generation is withdrawn, or when the server ended it.
    delivery: JoinHandle<()>,
}

impl SessionSubscription {
    /// Whether the generation still delivers. One the server ended keeps its name until the
    /// client deletes it, reuses the name, or ends the session.
    fn is_delivering(&self) -> bool {
        !self.delivery.is_finished()
    }

    /// Withdraws the generation and waits until its delivery has released everything it held.
    /// Nothing the generation queued reaches the client afterwards.
    async fn withdraw(self) -> RemovedSubscription {
        self.withdrawal.withdraw();
        self.delivery
            .join_after_shutdown("session subscription")
            .await;
        RemovedSubscription {
            handle: self.handle,
            domain: self.domain,
        }
    }
}

#[derive(Clone)]
pub(in crate::application) struct SubscriptionFilter {
    bindings: Vec<SubscriptionMatcher>,
}

#[derive(Clone)]
struct SubscriptionMatcher {
    field: FieldName,
    expected: runtime_schema::RuntimeValue,
}

pub(in crate::application) struct SessionSubscriptions {
    subscriptions: HashMap<SubscriptionName, SessionSubscription>,
    pub(in crate::application) user: UserName,
    pub(in crate::application) session_id: String,
    transaction_id: Option<String>,
    /// The generation the next subscription of this session opens with. A name reused after
    /// deletion gets a new one, so frames about the earlier subscription cannot be taken for it.
    next_generation: NonZeroU64,
}

/// The transaction binding a session holds, as the checks on that binding read it.
#[derive(Debug, Clone, Copy)]
pub(in crate::application) struct SessionBinding<'a> {
    pub(in crate::application) session_id: &'a str,
    pub(in crate::application) transaction_id: Option<&'a str>,
}

/// What a request that runs beside a session's ordered requests reads of that session: who it
/// belongs to, the transaction it is bound to and the subscriptions it holds, as the ordered
/// requests last left them.
#[derive(Debug, Clone)]
pub(in crate::application) struct SessionView {
    pub(in crate::application) user: UserName,
    pub(in crate::application) session_id: String,
    pub(in crate::application) transaction_id: Option<String>,
    pub(in crate::application) subscription_names: Vec<SubscriptionName>,
}

impl SessionView {
    pub(in crate::application) fn binding(&self) -> SessionBinding<'_> {
        SessionBinding {
            session_id: &self.session_id,
            transaction_id: self.transaction_id.as_deref(),
        }
    }

    pub(in crate::application) fn inspecting(&self) -> InspectingSession<'_> {
        InspectingSession {
            user: &self.user,
            attached_transaction: self.transaction_id.as_deref(),
        }
    }

    /// The names of the session's subscriptions that start with `prefix`.
    pub(in crate::application) fn matching_subscription_names(&self, prefix: &str) -> Vec<String> {
        let prefix = prefix.to_ascii_lowercase();
        self.subscription_names
            .iter()
            .filter(|name| prefix.is_empty() || name.as_str().starts_with(&prefix))
            .map(ToString::to_string)
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(in crate::application) struct PendingSessionCommand {
    pub(in crate::application) request_reference: CommandExecutionReference,
    pub(in crate::application) expected_transaction_position: Option<usize>,
    pub(in crate::application) source: String,
    pub(in crate::application) statement: ClientStatement,
    pub(in crate::application) domain: Option<DomainName>,
}

#[derive(Debug)]
pub(in crate::application) enum SessionCommandOperation {
    Begin {
        domain: Option<DomainName>,
    },
    Queue(PendingSessionCommand),
    Commit {
        /// The whole-transaction preview this commit expects to apply, when the request fences
        /// its commit against one.
        expected_preview: Option<TransactionPreviewIdentity>,
    },
    Revert,
    Execute(PendingSessionCommand),
}

/// A subscription generation that is attached and holds its interest lease, and delivers nothing
/// until the reply that announces it is queued.
pub(in crate::application) struct PendingSubscription {
    subscription: SessionSubscription,
    announce: oneshot::Sender<()>,
}

impl PendingSubscription {
    /// Abandons the generation unannounced. Its delivery stops at once and releases what it held.
    pub(in crate::application) async fn abandon(self) {
        let Self {
            subscription,
            announce,
        } = self;
        drop(announce);
        subscription.withdraw().await;
    }
}

/// A subscription the session opened, and the reply that announces it.
pub(in crate::application) struct OpenedSubscription {
    pub(in crate::application) opened: SubscriptionOpened,
    pub(in crate::application) message: String,
    /// Joins the session once the reply carrying `opened` is queued, and is abandoned otherwise.
    pub(in crate::application) pending: PendingSubscription,
}

/// A subscription the session deleted.
pub(in crate::application) struct DeletedSubscription {
    pub(in crate::application) handle: SubscriptionHandle,
    pub(in crate::application) message: String,
}

impl SessionSubscriptions {
    #[cfg(test)]
    pub(in crate::application) fn new() -> Self {
        Self::for_user(
            UserName::parse(DEFAULT_USER).expect("default user identifier must be valid"),
        )
    }

    pub(in crate::application) fn for_user(user: UserName) -> Self {
        Self {
            subscriptions: HashMap::new(),
            user,
            session_id: uuid::Uuid::now_v7().to_string(),
            transaction_id: None,
            next_generation: NonZeroU64::MIN,
        }
    }

    pub(in crate::application) fn transaction_active(&self) -> bool {
        self.transaction_id.is_some()
    }

    pub(in crate::application) fn binding(&self) -> SessionBinding<'_> {
        SessionBinding {
            session_id: &self.session_id,
            transaction_id: self.transaction_id.as_deref(),
        }
    }

    pub(in crate::application) fn inspecting(&self) -> InspectingSession<'_> {
        InspectingSession {
            user: &self.user,
            attached_transaction: self.transaction_id.as_deref(),
        }
    }

    /// The session as a request running beside its ordered requests reads it.
    pub(in crate::application) fn view(&self) -> SessionView {
        let mut subscription_names = self
            .subscriptions
            .keys()
            .filter(|name| self.contains_name(name))
            .cloned()
            .collect::<Vec<_>>();
        subscription_names.sort();
        SessionView {
            user: self.user.clone(),
            session_id: self.session_id.clone(),
            transaction_id: self.transaction_id.clone(),
            subscription_names,
        }
    }

    pub(in crate::application) fn plan_commands(
        &self,
        statements: Vec<ParsedClientStatement>,
        query: &str,
        request_domain: Option<&DomainName>,
        execution_reference: &CommandExecutionReference,
        expected_transaction_position: Option<usize>,
        expected_preview: Option<TransactionPreviewIdentity>,
    ) -> error_stack::Result<Vec<SessionCommandOperation>, SessionCommandPlanError> {
        let mut transaction_active = self.transaction_active();
        let mut transaction_position = expected_transaction_position;
        let multi_statement = statements.len() > 1;
        let mut operations = Vec::with_capacity(statements.len());
        // A preview describes the transaction as the client last saw it. It can fence a commit
        // only while this request has not itself opened the transaction or appended to it,
        // because either would move the transaction past the preview the client is holding.
        let mut preview_describes_transaction = transaction_active;

        for (statement_index, parsed) in statements.into_iter().enumerate() {
            let span = parsed.span.clone();
            match parsed.statement {
                ClientStatement::BeginTransaction => {
                    if transaction_active {
                        return Err(Report::new(
                            SessionCommandPlanError::TransactionAlreadyActive,
                        ));
                    }
                    transaction_active = true;
                    transaction_position = Some(0);
                    preview_describes_transaction = false;
                    operations.push(SessionCommandOperation::Begin {
                        domain: request_domain.cloned(),
                    });
                }
                ClientStatement::CommitTransaction => {
                    if !transaction_active {
                        return Err(Report::new(
                            SessionCommandPlanError::CommitWithoutTransaction,
                        ));
                    }
                    transaction_active = false;
                    let fenced_preview = if preview_describes_transaction {
                        expected_preview.clone()
                    } else {
                        None
                    };
                    preview_describes_transaction = false;
                    operations.push(SessionCommandOperation::Commit {
                        expected_preview: fenced_preview,
                    });
                }
                ClientStatement::RevertTransaction => {
                    if !transaction_active {
                        return Err(Report::new(
                            SessionCommandPlanError::RevertWithoutTransaction,
                        ));
                    }
                    transaction_active = false;
                    operations.push(SessionCommandOperation::Revert);
                }
                statement => {
                    let inspects_transaction = statement.inspects_transaction();
                    let request_reference = execution_reference.derive_step(statement_index);
                    let command = PendingSessionCommand {
                        request_reference,
                        expected_transaction_position: transaction_position,
                        source: query[span].to_string(),
                        statement,
                        domain: request_domain.cloned(),
                    };
                    if inspects_transaction {
                        // An inspection reads the transaction instead of joining it, so it is
                        // dispatched before queueing: it never becomes transaction content and
                        // never takes the queue position the next append expects. Sharing a
                        // request with other statements would make it part of their durable
                        // admission and replay, so it is always sent on its own.
                        if multi_statement {
                            return Err(Report::new(
                                SessionCommandPlanError::MixedTransactionInspection,
                            ));
                        }
                        operations.push(SessionCommandOperation::Execute(command));
                    } else if transaction_active {
                        let Some(current_position) = transaction_position else {
                            return Err(Report::new(SessionCommandPlanError::MissingQueuePosition));
                        };
                        transaction_position = current_position.checked_add(1);
                        if transaction_position.is_none() {
                            return Err(Report::new(
                                SessionCommandPlanError::QueuePositionOverflow,
                            ));
                        }
                        preview_describes_transaction = false;
                        operations.push(SessionCommandOperation::Queue(command));
                    } else if multi_statement {
                        return Err(Report::new(
                            SessionCommandPlanError::MultipleCommandsWithoutTransaction,
                        ));
                    } else {
                        operations.push(SessionCommandOperation::Execute(command));
                    }
                }
            }
        }

        Ok(operations)
    }

    pub(in crate::application) fn bind_transaction(&mut self, id: String) {
        self.transaction_id = Some(id);
    }

    pub(in crate::application) fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    pub(in crate::application) fn detach_transaction(&mut self) -> Option<String> {
        self.transaction_id.take()
    }

    /// The handle the next subscription named `name` opens with.
    fn next_handle(&mut self, name: SubscriptionName) -> SubscriptionHandle {
        let generation = self.next_generation;
        self.next_generation = generation
            .checked_add(1)
            .assured("a session cannot open 2^64 subscriptions");
        SubscriptionHandle { name, generation }
    }

    /// Whether a subscription named `name` still delivers.
    fn contains_name(&self, name: &SubscriptionName) -> bool {
        self.subscriptions
            .get(name)
            .is_some_and(SessionSubscription::is_delivering)
    }

    /// Admits a generation whose announcing reply is queued, and releases its rows.
    ///
    /// A generation the server ended under the same name leaves the session here. It already
    /// released everything it held, and the notice that ended it stays queued, so a client that
    /// reuses a name still learns how the earlier generation ended.
    pub(in crate::application) async fn activate(&mut self, pending: PendingSubscription) {
        let PendingSubscription {
            subscription,
            announce,
        } = pending;
        if let Some(ended) = self.subscriptions.remove(&subscription.handle.name) {
            ended
                .delivery
                .join_after_shutdown("ended session subscription")
                .await;
        }
        announce
            .send(())
            .discarded("a delivery that already stopped has no rows to release");
        self.subscriptions
            .insert(subscription.handle.name.clone(), subscription);
    }

    /// Withdraws the subscription named `name`, returning what identified it.
    async fn remove(&mut self, name: &SubscriptionName) -> Option<RemovedSubscription> {
        let subscription = self.subscriptions.remove(name)?;
        Some(subscription.withdraw().await)
    }

    /// Withdraws every subscription of the session, and waits until each has released what it
    /// held. All of them stop together rather than one after another.
    pub(in crate::application) async fn stop_all(&mut self) {
        let subscriptions = std::mem::take(&mut self.subscriptions);
        for subscription in subscriptions.values() {
            subscription.withdrawal.withdraw();
        }
        for (_, subscription) in subscriptions {
            tokio::task::consume_budget().await;
            subscription.withdraw().await;
        }
    }
}

/// A subscription taken out of its session.
struct RemovedSubscription {
    handle: SubscriptionHandle,
    domain: DomainName,
}

/// Rows of one relay batch a subscription passed over, and why.
struct SkippedRows {
    cause: RowsSkippedCause,
    rows: NonZeroU64,
    message: String,
}

/// The rows of one relay batch a subscription delivers, and the rows it had to skip.
struct SubscriptionSelection {
    rows: Vec<usize>,
    skipped: Option<SkippedRows>,
}

/// Which rows of one relay batch pass a subscription's filter and sampling.
///
/// The filter reads the domain's execution time once for the batch. A row the filter cannot
/// evaluate is skipped and counted, and the first failure is reported for the batch.
async fn select_subscription_rows(
    batch: &RelayRecordBatch,
    predicate: Option<&CompiledSubscriptionPredicate>,
    batch_sample_rate: Option<f64>,
    runtime: &Runtime,
    domain: &DomainName,
) -> SubscriptionSelection {
    let row_count = batch.record_batch().num_rows();
    let now = match predicate {
        Some(_) => match runtime.domain_execution_snapshot(domain) {
            Ok(snapshot) => Some(snapshot.now()),
            Err(error) => {
                let skipped = NonZeroU64::new(
                    u64::try_from(row_count)
                        .assured("supported targets have a pointer width no larger than u64"),
                )
                .map(|rows| SkippedRows {
                    cause: RowsSkippedCause::DomainTimeUnavailable,
                    rows,
                    message: format!(
                        "session subscription could not read domain execution time: {error}"
                    ),
                });
                return SubscriptionSelection {
                    rows: Vec::new(),
                    skipped,
                };
            }
        },
        None => None,
    };
    let mut rows = Vec::with_capacity(row_count);
    let mut failed_rows = 0_u64;
    let mut first_failure = None;
    for row in 0..row_count {
        tokio::task::consume_budget().await;
        if let (Some(predicate), Some(now)) = (predicate, now) {
            let passed = match batch.runtime_row(row) {
                Ok(record) => execute_subscription_predicate_on_record(predicate, &record, now)
                    .await
                    .map_err(|error| error.to_string()),
                Err(error) => Err(error.to_string()),
            };
            match passed {
                Ok(true) => {}
                Ok(false) => continue,
                Err(error) => {
                    failed_rows = failed_rows
                        .checked_add(1)
                        .assured("a batch holds fewer than 2^64 rows");
                    if first_failure.is_none() {
                        first_failure = Some(error);
                    }
                    continue;
                }
            }
        }
        let key = batch
            .branch_keys()
            .get(row)
            .verified("a relay batch carries one branch key per row");
        if !subscription_sample_passes(batch_sample_rate, key.as_ref()) {
            continue;
        }
        rows.push(row);
    }
    let skipped = match (NonZeroU64::new(failed_rows), first_failure) {
        (Some(failed_rows), Some(error)) => Some(SkippedRows {
            cause: RowsSkippedCause::FilterFailed,
            rows: failed_rows,
            message: format!("session subscription predicate failed: {error}"),
        }),
        _ => None,
    };
    SubscriptionSelection { rows, skipped }
}

pub(in crate::application) fn validate_subscription_bindings(
    relay: &RelayName,
    branching: &nervix_models::ResolvedBranching,
    bindings: &[SubscriptionBinding],
) -> error_stack::Result<SubscriptionFilter, SubscriptionError> {
    if branching.is_unbranched() {
        if bindings.is_empty() {
            return Ok(SubscriptionFilter {
                bindings: Vec::new(),
            });
        }
        return Err(Report::new(SubscriptionError::UnbranchedBindings {
            relay: relay.clone(),
        }));
    }

    let schema = branching
        .schema()
        .verified("a branched subscription target carries its resolved key schema");
    let branch_fields = branching.field_names().cloned().collect::<Vec<_>>();

    if bindings.is_empty() {
        return Err(Report::new(SubscriptionError::MissingBindings {
            relay: relay.clone(),
            fields: branch_fields,
        }));
    }

    let mut fields = HashMap::new();
    for field in &schema.fields {
        fields.insert(field.name.clone(), field.ty.clone());
    }

    let mut bound = HashMap::new();
    for binding in bindings {
        if bound
            .insert(binding.field.clone(), binding.value.clone())
            .is_some()
        {
            return Err(Report::new(SubscriptionError::DuplicateBinding {
                field: binding.field.clone(),
            }));
        }
    }

    let expected = SortedSet::from_unsorted(branch_fields.clone()).into_vec();
    let actual = SortedSet::from_unsorted(bound.keys().cloned().collect::<Vec<_>>()).into_vec();
    if expected != actual {
        return Err(Report::new(SubscriptionError::BindingFieldsMismatch {
            relay: relay.clone(),
            fields: branch_fields,
        }));
    }

    let mut matchers = Vec::new();
    for field in &branch_fields {
        let ty = fields.get(field).ok_or_else(|| {
            Report::new(SubscriptionError::MissingSchemaField {
                field: field.clone(),
                schema: schema.name.clone(),
            })
        })?;
        let literal = bound
            .get(field)
            .verified("the check above requires the bound keys to match the branch fields exactly");
        let expected = parse_subscription_literal(field, ty, literal)?;
        matchers.push(SubscriptionMatcher {
            field: field.clone(),
            expected,
        });
    }

    Ok(SubscriptionFilter { bindings: matchers })
}

pub(in crate::application) fn branch_key_from_filter(
    branching: &nervix_models::ResolvedBranching,
    filter: &SubscriptionFilter,
) -> error_stack::Result<Option<crate::runtime::BranchKey>, SubscriptionError> {
    if branching.is_unbranched() {
        return Ok(None);
    }
    let branch_fields = branching.field_names().collect::<Vec<_>>();
    let mut fields = Vec::with_capacity(branch_fields.len());
    for field in branch_fields {
        let Some(binding) = filter
            .bindings
            .iter()
            .find(|binding| binding.field == *field)
        else {
            return Err(Report::new(SubscriptionError::MissingBranchField {
                field: (*field).clone(),
            }));
        };
        fields.push((field.clone(), binding.expected.clone()));
    }
    let key = crate::runtime::BranchKey::from_fields(fields)
        .change_context(SubscriptionError::InvalidBranchKey)?;
    Ok(Some(key))
}

pub(in crate::application) fn render_subscription_literal(literal: &SubscriptionLiteral) -> String {
    match literal {
        SubscriptionLiteral::String(value) => format!("'{}'", value.replace('\'', "''")),
        SubscriptionLiteral::Number(value) => value.clone(),
        SubscriptionLiteral::Bool(value) => value.to_string(),
    }
}

fn parse_subscription_batch_sample_rate(
    rate: Option<&str>,
) -> error_stack::Result<Option<f64>, SubscriptionError> {
    let Some(rate) = rate else {
        return Ok(None);
    };
    let parsed = rate
        .parse::<f64>()
        .map_err(|_| Report::new(SubscriptionError::InvalidSampleRate))?;
    if (0.0..=1.0).contains(&parsed) {
        Ok(Some(parsed))
    } else {
        Err(Report::new(SubscriptionError::SampleRateOutOfRange))
    }
}

fn subscription_sample_passes(batch_sample_rate: Option<f64>, key: Option<&BranchKey>) -> bool {
    let Some(rate) = batch_sample_rate else {
        return true;
    };
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }

    let counter = SESSION_SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut hasher = Hasher::new();
    hasher.update(&counter.to_le_bytes());
    if let Some(key) = key {
        hasher.update(key.as_str().as_bytes());
    }
    let hash = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    let draw = u64::from_le_bytes(bytes).approx_into::<f64>() / u64::MAX.approx_into::<f64>();
    draw < rate
}

pub(in crate::application) fn parse_subscription_literal(
    field: &FieldName,
    ty: &ParseAsType,
    literal: &SubscriptionLiteral,
) -> error_stack::Result<runtime_schema::RuntimeValue, SubscriptionError> {
    use runtime_schema::RuntimeValue;

    let bad = |expected| {
        Report::new(SubscriptionError::InvalidLiteral {
            field: field.clone(),
            ty: ty.clone(),
            expected,
        })
    };

    match (ty, literal) {
        (ParseAsType::String, SubscriptionLiteral::String(v)) => {
            Ok(RuntimeValue::String(v.clone()))
        }
        (ParseAsType::Datetime, SubscriptionLiteral::String(v)) => {
            chrono::DateTime::parse_from_rfc3339(v)
                .map(RuntimeValue::Datetime)
                .map_err(|_| bad(ExpectedSubscriptionLiteral::Datetime))
        }
        (ParseAsType::Bool, SubscriptionLiteral::Bool(v)) => Ok(RuntimeValue::Bool(*v)),
        (ParseAsType::U8, SubscriptionLiteral::Number(v)) => v
            .parse()
            .map(RuntimeValue::U8)
            .map_err(|_| bad(ExpectedSubscriptionLiteral::Numeric)),
        (ParseAsType::I8, SubscriptionLiteral::Number(v)) => v
            .parse()
            .map(RuntimeValue::I8)
            .map_err(|_| bad(ExpectedSubscriptionLiteral::Numeric)),
        (ParseAsType::U16, SubscriptionLiteral::Number(v)) => v
            .parse()
            .map(RuntimeValue::U16)
            .map_err(|_| bad(ExpectedSubscriptionLiteral::Numeric)),
        (ParseAsType::I16, SubscriptionLiteral::Number(v)) => v
            .parse()
            .map(RuntimeValue::I16)
            .map_err(|_| bad(ExpectedSubscriptionLiteral::Numeric)),
        (ParseAsType::U32, SubscriptionLiteral::Number(v)) => v
            .parse()
            .map(RuntimeValue::U32)
            .map_err(|_| bad(ExpectedSubscriptionLiteral::Numeric)),
        (ParseAsType::I32, SubscriptionLiteral::Number(v)) => v
            .parse()
            .map(RuntimeValue::I32)
            .map_err(|_| bad(ExpectedSubscriptionLiteral::Numeric)),
        (ParseAsType::U64, SubscriptionLiteral::Number(v)) => v
            .parse()
            .map(RuntimeValue::U64)
            .map_err(|_| bad(ExpectedSubscriptionLiteral::Numeric)),
        (ParseAsType::I64, SubscriptionLiteral::Number(v)) => v
            .parse()
            .map(RuntimeValue::I64)
            .map_err(|_| bad(ExpectedSubscriptionLiteral::Numeric)),
        (ParseAsType::F32, SubscriptionLiteral::Number(v)) => v
            .parse()
            .map(RuntimeValue::F32)
            .map_err(|_| bad(ExpectedSubscriptionLiteral::Numeric)),
        (ParseAsType::F64, SubscriptionLiteral::Number(v)) => v
            .parse()
            .map(RuntimeValue::F64)
            .map_err(|_| bad(ExpectedSubscriptionLiteral::Numeric)),
        _ => Err(bad(match ty {
            ParseAsType::String | ParseAsType::Datetime => ExpectedSubscriptionLiteral::String,
            ParseAsType::Bool => ExpectedSubscriptionLiteral::Boolean,
            ParseAsType::Array { .. } | ParseAsType::Vec { .. } => {
                ExpectedSubscriptionLiteral::Array
            }
            _ => ExpectedSubscriptionLiteral::Numeric,
        })),
    }
}

/// The relay a subscription attaches to, as the cluster schedule describes it: the relay model,
/// the schema its records carry, and the branch key fields a subscription may bind.
pub(in crate::application) struct SubscriptionTarget {
    pub(in crate::application) relay: nervix_models::CreateRelay,
    pub(in crate::application) schema: nervix_models::CreateSchema,
    pub(in crate::application) branching: nervix_models::ResolvedBranching,
}

impl SessionServiceImpl {
    async fn wait_for_subscription_interest_visibility(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        minimum_version: u64,
    ) -> error_stack::Result<(), SubscriptionError> {
        let subscriber = self.inner.cluster.local_node_identity().await;
        // Subscription delivery may begin as soon as a prepared schedule activates. A member can
        // be application-unavailable while it finishes that activation, then immediately own this
        // relay. Include every live membership node in the visibility handshake so that owner
        // cannot publish before it sees the subscriber's interest.
        let target_nodes = self
            .inner
            .cluster
            .gossip_state()
            .await
            .live_node_ids()
            .into_iter()
            .filter(|node_id| node_id != subscriber.node_id())
            .collect::<BTreeSet<_>>();
        let mut checks = target_nodes
            .iter()
            .map(|node_id| {
                let subscriber = subscriber.clone();
                async move {
                    let result = self
                        .wait_for_subscription_interest_visibility_on_node(
                            node_id,
                            &subscriber,
                            domain,
                            relay,
                            minimum_version,
                        )
                        .await;
                    (node_id, result)
                }
            })
            .collect::<FuturesUnordered<_>>();
        let mut errors = BTreeMap::new();
        while let Some((node_id, result)) = checks.next().await {
            tokio::task::consume_budget().await;
            if let Err(error) = result {
                errors.insert(node_id.clone(), error);
            }
        }
        let nodes = errors.keys().cloned().collect();
        let Some((_, first_error)) = errors.into_iter().next() else {
            return Ok(());
        };
        Err(
            first_error.change_context(SubscriptionError::InterestNotVisible {
                domain: domain.clone(),
                relay: relay.clone(),
                nodes,
            }),
        )
    }

    async fn wait_for_subscription_interest_visibility_on_node(
        &self,
        target_node_id: &ClusterNodeName,
        subscriber: &ClusterNodeIdentity,
        domain: &DomainName,
        relay: &RelayName,
        minimum_version: u64,
    ) -> error_stack::Result<(), SubscriptionError> {
        let response = self
            .inner
            .interconnect
            .request(
                target_node_id,
                RemoteSubscriptionInterestVisibilityRequest {
                    subscriber: subscriber.clone(),
                    domain: domain.clone(),
                    relay: relay.clone(),
                    minimum_version,
                },
            )
            .await
            .change_context(SubscriptionError::InterestRequest {
                domain: domain.clone(),
                relay: relay.clone(),
                node: target_node_id.clone(),
            })?;
        response.result.map_err(|failure| {
            Report::new(SubscriptionError::InterestRemoteFailure {
                node: target_node_id.clone(),
                failure,
            })
        })
    }

    pub(in crate::application) async fn scheduled_stream_owner_nodes(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Vec<ClusterNodeName> {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Vec::new();
        };
        scheduled_relay_owner_nodes(domain_schedule, relay)
    }

    async fn process_pending_session_commands(
        &self,
        commands: Vec<PendingSessionCommand>,
        session: InspectingSession<'_>,
        explicit_batch: bool,
    ) -> CommandResult {
        let is_batch = explicit_batch || commands.len() > 1;
        let mut results = Vec::new();
        let mut commands = commands.into_iter().peekable();

        while let Some(command) = commands.next() {
            match command.statement {
                ClientStatement::Server(statement) if statement.is_model_mutation() => {
                    let domain = command.domain;
                    let mut sources = vec![command.source];
                    let mut statements = vec![statement];

                    while let Some(next) = commands.peek() {
                        if next.domain != domain {
                            break;
                        }
                        let ClientStatement::Server(statement) = &next.statement else {
                            break;
                        };
                        if !statement.is_model_mutation() {
                            break;
                        }
                        let next = commands.next().verified(
                            "the peek above observed this command and nothing consumed the \
                             iterator since",
                        );
                        let ClientStatement::Server(statement) = next.statement else {
                            unreachable!("peeked model mutation statement must be next");
                        };
                        sources.push(next.source);
                        statements.push(statement);
                    }

                    let mutation_query = sources.join("; ");
                    let result = self
                        .process_model_mutation_batch(statements, &mutation_query, domain.as_ref())
                        .await;
                    if !result.succeeded() {
                        return command_batch_result(results, result, is_batch);
                    }
                    append_command_result(&mut results, result);
                }
                statement => {
                    let result = self
                        .process_client_statement(
                            statement,
                            &command.source,
                            command.domain.as_ref(),
                            session,
                        )
                        .await;
                    if !result.succeeded() {
                        return command_batch_result(results, result, is_batch);
                    }
                    append_command_result(&mut results, result);
                }
            }
        }

        if results.is_empty() {
            return command_error("empty command".to_string());
        }
        if !is_batch {
            return results
                .pop()
                .verified("the empty check above already returned");
        }

        let message = command_results_message(&results);
        CommandResult {
            statements: results,
            ..command_ok(message)
        }
    }

    pub(in crate::application) async fn subscription_target_from_schedule(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> error_stack::Result<Option<SubscriptionTarget>, SubscriptionError> {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(ScheduledModel {
            config: ack_model,
            node: relay_node,
        }) = domain_schedule.scheduled::<CreateRelay>(relay)
        else {
            return Ok(None);
        };
        let Some(schema) = domain_schedule.configured::<CreateSchema>(&ack_model.schema) else {
            return Err(Report::new(SubscriptionError::MissingScheduledSchema {
                relay: relay.clone(),
                schema: ack_model.schema.clone(),
            }));
        };
        let branching = relay_node.resolved_branching.clone().ok_or_else(|| {
            Report::new(SubscriptionError::MissingScheduledBranch {
                relay: relay.clone(),
            })
        })?;
        Ok(Some(SubscriptionTarget {
            relay: ack_model.clone(),
            schema: schema.clone(),
            branching,
        }))
    }

    /// Creates a subscription generation for `subscription`, attached and holding its interest
    /// lease but delivering nothing until the reply that announces it is queued. A refusal leaves
    /// nothing behind.
    pub(in crate::application) async fn create_subscription(
        &self,
        domain: &DomainName,
        subscription: nervix_models::CreateSubscription,
        delivery: &SessionDelivery,
        subscriptions: &mut SessionSubscriptions,
    ) -> Result<OpenedSubscription, Box<CommandResult>> {
        if subscriptions.contains_name(&subscription.name) {
            return Err(Box::new(command_error(format!(
                "session subscription '{}' already exists",
                subscription.name
            ))));
        }

        let batch_sample_rate =
            match parse_subscription_batch_sample_rate(subscription.batch_sample_rate.as_deref()) {
                Ok(rate) => rate,
                Err(err) => {
                    let diagnostic = CommandDiagnostic::unlocated(err.to_string());
                    return Err(Box::new(CommandResult {
                        diagnostics: vec![diagnostic],
                        ..CommandResult::new(
                            CommandDisposition::Failed,
                            format!("failed to subscribe session '{}': {err}", subscription.name),
                        )
                    }));
                }
            };

        // The subscription describes the relay as the schedule declares it, which is the
        // definition the runtime executes it with and attaches it under.
        let target = match self
            .subscription_target_from_schedule(domain, &subscription.relay)
            .await
        {
            Ok(Some(target)) => target,
            Ok(None) => {
                let diagnostic = CommandDiagnostic::unlocated(format!(
                    "stream '{}' not found",
                    subscription.relay.as_str()
                ));
                let message = format!(
                    "stream '{}' does not exist in domain '{}'",
                    subscription.relay.as_str(),
                    domain.as_str()
                );
                return Err(Box::new(CommandResult {
                    diagnostics: vec![diagnostic],
                    ..CommandResult::new(CommandDisposition::Failed, message)
                }));
            }
            Err(err) => {
                return Err(Box::new(command_error(format!(
                    "failed to resolve relay for subscription: {err}"
                ))));
            }
        };
        let SubscriptionTarget {
            relay: _,
            schema: payload_schema,
            branching,
        } = target;
        let compiled_schema = Arc::new(runtime_schema::compile_schema(&payload_schema));
        let predicate = match subscription.where_clause.as_ref() {
            Some(expression) => {
                let udfs = self.inner.runtime.udf_executor(domain);
                let compiled_predicate = compile_subscription_predicate(
                    domain,
                    &subscription.name,
                    expression,
                    SubscriptionPredicateCompileContext::new(
                        compiled_schema.arrow_schema(),
                        compiled_schema.vm_sensitivity(),
                        udfs.as_ref(),
                    ),
                );
                match compiled_predicate {
                    Ok(predicate) => Some(predicate),
                    Err(err) => {
                        return Err(Box::new(command_error(format!(
                            "failed to compile session subscription '{}': {err}",
                            subscription.name
                        ))));
                    }
                }
            }
            None => None,
        };

        let branch_fields = branching.field_names().cloned().collect::<Vec<_>>();
        let branch = match &branching {
            nervix_models::ResolvedBranching::Unbranched => None,
            nervix_models::ResolvedBranching::Branched { branch, schema } => {
                Some(SubscriptionBranchSchema {
                    name: branch,
                    schema,
                    fields: &branch_fields,
                })
            }
        };
        let row_schema = match subscription_row_schema(&payload_schema, branch) {
            Ok(schema) => schema,
            Err(error) => {
                return Err(Box::new(command_error(format!(
                    "failed to describe the rows of relay '{}': {error}",
                    subscription.relay.as_str(),
                ))));
            }
        };
        let handle = subscriptions.next_handle(subscription.name.clone());
        let opening = SubscriptionRowOpening::new(
            handle.clone(),
            row_schema,
            delivery.limits,
            SUBSCRIPTION_ROWS_PER_FRAME,
        );
        let opening = match opening {
            Ok(opening) => opening,
            Err(error) => {
                return Err(Box::new(command_error(format!(
                    "failed to describe the rows of relay '{}': {error}",
                    subscription.relay.as_str(),
                ))));
            }
        };

        let relay = subscription.relay.clone();
        let runtime_revision = self.inner.consensus.current_runtime_state().await.revision;
        if let Err(err) = self.wait_for_runtime_revision(runtime_revision).await {
            return Err(Box::new(command_error(format!(
                "failed to subscribe to relay '{}': {err}",
                relay.as_str()
            ))));
        }
        let lease = self
            .inner
            .subscription_interests
            .acquire(domain, &relay)
            .await;
        let minimum_version = lease.advertisement_version().await;
        if let Err(error) = self
            .wait_for_subscription_interest_visibility(domain, &relay, minimum_version)
            .await
        {
            lease.release().await;
            return Err(Box::new(command_error(format!(
                "failed to register subscription interest for relay '{}' in domain '{}': {error}",
                relay.as_str(),
                domain.as_str(),
            ))));
        }
        let definition = RelaySubscriptionDefinition::new(compiled_schema, branching);
        let attached = self
            .inner
            .runtime
            .subscribe_stream(domain, &relay, &definition)
            .await;
        let receiver = match attached {
            Ok(receiver) => receiver,
            Err(RuntimeError::RelayRedefined { .. }) => {
                lease.release().await;
                return Err(Box::new(command_error(format!(
                    "failed to subscribe to relay '{}': it was redefined while the subscription \
                     was being created; subscribe again",
                    relay.as_str()
                ))));
            }
            Err(err) => {
                lease.release().await;
                return Err(Box::new(command_error(format!(
                    "failed to subscribe to relay '{}': {err}",
                    relay.as_str()
                ))));
            }
        };

        let (opened, encoder) = opening.open(domain.clone(), relay.clone());
        let lane = delivery.outbound.subscription_lane();
        let withdrawal = lane.withdrawal();
        let (announce, announced) = oneshot::channel();
        let generation = SubscriptionDelivery {
            handle: handle.clone(),
            domain: domain.clone(),
            relay: relay.clone(),
            predicate,
            behavior: subscription.delivery_behavior,
            batch_sample_rate,
            receiver,
            encoder,
            lane,
            limits: delivery.limits,
            lease,
            service: self.clone(),
        };
        let delivery_task = tokio::spawn(generation.run(announced));

        Ok(OpenedSubscription {
            opened,
            message: format!(
                "created subscription '{}' in domain '{}'",
                subscription.name,
                domain.as_str()
            ),
            pending: PendingSubscription {
                subscription: SessionSubscription {
                    handle,
                    domain: domain.clone(),
                    withdrawal,
                    delivery: delivery_task,
                },
                announce,
            },
        })
    }

    /// Withdraws the subscription the statement names. Its delivery has stopped and released the
    /// relay receiver and the interest lease before this returns.
    pub(in crate::application) async fn delete_subscription(
        &self,
        subscription: nervix_models::DeleteSubscription,
        subscriptions: &mut SessionSubscriptions,
    ) -> Result<DeletedSubscription, Box<CommandResult>> {
        let Some(removed) = subscriptions.remove(&subscription.name).await else {
            let diagnostic = CommandDiagnostic::unlocated(format!(
                "session subscription '{}' not found",
                subscription.name
            ));
            let message = format!(
                "session subscription '{}' does not exist",
                subscription.name
            );
            return Err(Box::new(CommandResult {
                diagnostics: vec![diagnostic],
                ..CommandResult::new(CommandDisposition::Failed, message)
            }));
        };
        Ok(DeletedSubscription {
            message: format!(
                "deleted subscription '{}' from domain '{}'",
                subscription.name,
                removed.domain.as_str()
            ),
            handle: removed.handle,
        })
    }
}

impl SessionServiceImpl {
    pub(in crate::application) async fn execute_durable_transaction_request(
        &self,
        owner: UserName,
        domain: DomainName,
        request: CommandExecutionTransactionRequest,
    ) -> CommandResult {
        let transaction_id = request.target.id().to_string();
        let activity = request.target.activity();
        let operation_count = request.operations.len();
        let request_count = if request.target.opens_transaction() {
            operation_count.checked_add(1).verified(
                "a transaction request cannot contain more operations than an addressable Vec",
            )
        } else {
            operation_count
        };
        let is_batch = request_count > 1;
        let mut results = Vec::new();
        let mut transaction = self
            .inner
            .consensus
            .current_transaction(&transaction_id)
            .await
            .map(|transaction| transaction_status(&transaction));

        if matches!(
            request.target,
            CommandExecutionTransactionTarget::New { .. }
        ) {
            let result = self
                .begin_identified_transaction(
                    transaction_id.clone(),
                    domain.clone(),
                    owner.clone(),
                    activity,
                )
                .await;
            if result.transaction.is_some() {
                transaction.clone_from(&result.transaction);
            }
            if !result.succeeded() || !is_batch {
                return result;
            }
            append_command_result(&mut results, result);
        }

        for operation in request.operations {
            tokio::task::consume_budget().await;
            let result = match operation {
                CommandExecutionTransactionOperation::Queue(statement) => {
                    self.queue_identified_transaction_statement(
                        transaction_id.clone(),
                        owner.clone(),
                        domain.clone(),
                        *statement,
                        activity,
                    )
                    .await
                }
                CommandExecutionTransactionOperation::Commit { expected_preview } => {
                    self.commit_identified_transaction(
                        transaction_id.clone(),
                        owner.clone(),
                        activity,
                        expected_preview,
                    )
                    .await
                }
                CommandExecutionTransactionOperation::Revert => {
                    self.revert_identified_transaction(
                        transaction_id.clone(),
                        owner.clone(),
                        activity,
                    )
                    .await
                }
            };

            if result.transaction.is_some() {
                transaction.clone_from(&result.transaction);
            }
            if !result.succeeded() {
                let mut result = command_batch_result(results, result, is_batch);
                if result.transaction.is_none() {
                    result.transaction = transaction;
                }
                return result;
            }
            if !is_batch {
                return result;
            }
            append_command_result(&mut results, result);
        }

        if results.is_empty() {
            return command_error("empty command".to_string());
        }
        let message = command_results_message(&results);
        CommandResult {
            statements: results,
            transaction,
            ..command_ok(message)
        }
    }

    pub(in crate::application) async fn process_session_command_operations(
        &self,
        operations: Vec<SessionCommandOperation>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let is_batch = operations.len() > 1;
        let mut results = Vec::new();
        let mut transaction = None;

        for operation in operations {
            tokio::task::consume_budget().await;
            let result = match operation {
                SessionCommandOperation::Begin { domain } => {
                    match self.resolve_transaction_domain(domain.as_ref()).await {
                        Err(error) => command_error(error.to_string()),
                        Ok(domain) => {
                            let id = uuid::Uuid::now_v7().to_string();
                            let transaction = ReplicatedTransaction::open(
                                id.clone(),
                                domain,
                                subscriptions.user.clone(),
                                self.transaction_activity(),
                            );
                            match self
                                .inner
                                .consensus
                                .open_transaction(transaction, self.inner.transaction_max_open)
                                .await
                            {
                                Ok(transaction) => {
                                    self.inner
                                        .transaction_bindings
                                        .insert(id.clone(), subscriptions.session_id.clone());
                                    subscriptions.bind_transaction(id.clone());
                                    let mut result =
                                        command_ok(format!("transaction started: id '{id}'"));
                                    result.transaction = Some(transaction_status(&transaction));
                                    result
                                }
                                Err(error) => {
                                    self.transaction_consensus_error_response(error).await
                                }
                            }
                        }
                    }
                }
                SessionCommandOperation::Queue(command) => {
                    self.queue_transaction_statement(command, subscriptions)
                        .await
                }
                SessionCommandOperation::Commit { expected_preview } => {
                    self.commit_bound_transaction(subscriptions, expected_preview)
                        .await
                }
                SessionCommandOperation::Revert => {
                    self.revert_bound_transaction(subscriptions).await
                }
                SessionCommandOperation::Execute(command) => {
                    self.process_pending_session_commands(
                        vec![command],
                        subscriptions.inspecting(),
                        false,
                    )
                    .await
                }
            };

            if result.transaction.is_some() {
                transaction.clone_from(&result.transaction);
            }
            if !result.succeeded() {
                let mut result = command_batch_result(results, result, is_batch);
                if result.transaction.is_none() {
                    result.transaction = transaction;
                }
                return result;
            }
            if !is_batch {
                return result;
            }
            append_command_result(&mut results, result);
        }

        if results.is_empty() {
            return command_error("empty command".to_string());
        }
        let message = command_results_message(&results);
        CommandResult {
            statements: results,
            transaction,
            ..command_ok(message)
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_client_wire::{
        RowSchema, ServerEvent, ServerMessage, SubscriptionEndReason, VerifiedFrame,
    };
    use nervix_models::{SchemaField, SubscriptionDeliveryBehavior};
    use tokio::time::{Duration, timeout};
    use tokio_util::sync::CancellationToken;

    use super::{
        super::{
            session::outbound::{self, SESSION_SUBSCRIPTION_CAPACITY, SessionFrames},
            test_fixtures::{TestService, build_test_service, named, string_branch_key},
        },
        *,
    };
    use crate::{
        runtime::{RelayBroadcast, RelaySubscriptionReceiver},
        runtime_schema::{CompiledSchema, RuntimeValue, test_runtime_row},
        subscription_row::SubscriptionRowOpening,
    };

    /// Generously longer than any step here takes, so only a hang reaches it.
    const WAIT: Duration = Duration::from_secs(30);

    fn default_domain() -> DomainName {
        named("default")
    }

    fn user_id_fields() -> Vec<SchemaField> {
        vec![SchemaField {
            name: named("user_id"),
            ty: ParseAsType::U32,
            optional: false,
            sensitive: false,
        }]
    }

    fn user_id_schema() -> Arc<CompiledSchema> {
        Arc::new(runtime_schema::compile_schema(&CreateSchema {
            name: named("event"),
            fields: user_id_fields(),
        }))
    }

    fn user_id_batch(schema: &Arc<CompiledSchema>, user_id: u32) -> RelayRecordBatch {
        RelayRecordBatch::unbranched_for_test(
            schema.clone(),
            test_runtime_row([("user_id".to_string(), RuntimeValue::U32(user_id))]),
        )
    }

    /// Session lanes whose transport the test reads, or leaves unread to hold them full.
    fn session_delivery() -> (SessionDelivery, SessionFrames) {
        let (outbound, frames) = outbound::channel(CancellationToken::new());
        let delivery = SessionDelivery {
            outbound,
            limits: SessionLimits::DEFAULT,
        };
        (delivery, frames)
    }

    /// A generation of `name` reading `relay` in the default domain as creation leaves it:
    /// attached to `receiver`, holding its interest lease, and delivering nothing until announced.
    async fn pending_subscription(
        service: &SessionServiceImpl,
        subscriptions: &mut SessionSubscriptions,
        delivery: &SessionDelivery,
        name: &str,
        relay: &str,
        receiver: RelaySubscriptionReceiver<RelayRecordBatch>,
    ) -> PendingSubscription {
        let domain = default_domain();
        let relay = named::<RelayName>(relay);
        let handle = subscriptions.next_handle(named(name));
        let schema = RowSchema {
            fields: user_id_fields(),
            branch: None,
        };
        let (_, encoder) = SubscriptionRowOpening::new(
            handle.clone(),
            schema,
            delivery.limits,
            SUBSCRIPTION_ROWS_PER_FRAME,
        )
        .assured("the test row limit fits the default collection limit")
        .open(domain.clone(), relay.clone());
        let lease = service
            .inner
            .subscription_interests
            .acquire(&domain, &relay)
            .await;
        let lane = delivery.outbound.subscription_lane();
        let withdrawal = lane.withdrawal();
        let (announce, announced) = oneshot::channel();
        let generation = SubscriptionDelivery {
            handle: handle.clone(),
            domain: domain.clone(),
            relay: relay.clone(),
            predicate: None,
            behavior: SubscriptionDeliveryBehavior::Blocking,
            batch_sample_rate: None,
            receiver,
            encoder,
            lane,
            limits: delivery.limits,
            lease,
            service: service.clone(),
        };
        PendingSubscription {
            subscription: SessionSubscription {
                handle,
                domain,
                withdrawal,
                delivery: tokio::spawn(generation.run(announced)),
            },
            announce,
        }
    }

    /// The next frame the transport would write, decoded.
    async fn next_event(frames: &mut SessionFrames) -> ServerEvent {
        let frame = timeout(WAIT, frames.next())
            .await
            .assured("the session queues the frame within the deadline")
            .assured("the test holds the session's producers");
        let frame = VerifiedFrame::verify(frame.into_bytes(), &SessionLimits::DEFAULT)
            .assured("the server encodes frames the session limits admit");
        let ServerMessage::Event(event) = ServerMessage::decode(&frame).assured("a frame decodes")
        else {
            panic!("a subscription sends only events");
        };
        event
    }

    fn remove_test_directory(path: std::path::PathBuf) {
        std::fs::remove_dir_all(path).discarded("the test directory is disposable");
    }

    #[test]
    fn parse_subscription_literal_enforces_declared_types() {
        let field = named("created_at");
        assert!(matches!(
            parse_subscription_literal(
                &field,
                &ParseAsType::Datetime,
                &SubscriptionLiteral::String("2025-01-02T03:04:05+00:00".to_string())
            ),
            Ok(runtime_schema::RuntimeValue::Datetime(_))
        ));
        assert!(matches!(
            parse_subscription_literal(
                &named("active"),
                &ParseAsType::Bool,
                &SubscriptionLiteral::Bool(true)
            ),
            Ok(runtime_schema::RuntimeValue::Bool(true))
        ));
        let err = parse_subscription_literal(
            &named("user_id"),
            &ParseAsType::U32,
            &SubscriptionLiteral::String("42".to_string()),
        )
        .expect_err("string should not satisfy numeric field");
        assert!(err.to_string().contains("expects numeric literal"));
    }

    #[test]
    fn numeric_binding_failures_keep_the_field_and_type_without_the_value() {
        use runtime_schema::RuntimeValue;

        let field: FieldName = named("account");
        let cases = [
            (ParseAsType::U8, RuntimeValue::U8(1)),
            (ParseAsType::I8, RuntimeValue::I8(1)),
            (ParseAsType::U16, RuntimeValue::U16(1)),
            (ParseAsType::I16, RuntimeValue::I16(1)),
            (ParseAsType::U32, RuntimeValue::U32(1)),
            (ParseAsType::I32, RuntimeValue::I32(1)),
            (ParseAsType::U64, RuntimeValue::U64(1)),
            (ParseAsType::I64, RuntimeValue::I64(1)),
            (ParseAsType::F32, RuntimeValue::F32(1.0_f32.into())),
            (ParseAsType::F64, RuntimeValue::F64(1.0_f64.into())),
        ];
        for (ty, expected) in cases {
            let value = parse_subscription_literal(
                &field,
                &ty,
                &SubscriptionLiteral::Number("1".to_string()),
            )
            .expect("one fits every numeric scalar type");
            assert_eq!(value, expected);
            let error = parse_subscription_literal(
                &field,
                &ty,
                &SubscriptionLiteral::Number("sensitive-invalid-number".to_string()),
            )
            .expect_err("invalid numeric text must fail");
            assert!(matches!(
                error.current_context(),
                SubscriptionError::InvalidLiteral {
                    field: actual_field,
                    ty: actual_type,
                    expected: ExpectedSubscriptionLiteral::Numeric,
                } if actual_field == &field && actual_type == &ty
            ));
            assert!(!format!("{error:?}").contains("sensitive-invalid-number"));
        }

        let error = parse_subscription_literal(
            &field,
            &ParseAsType::Datetime,
            &SubscriptionLiteral::String("sensitive-invalid-date".to_string()),
        )
        .expect_err("invalid datetime text must fail");
        assert!(matches!(
            error.current_context(),
            SubscriptionError::InvalidLiteral {
                expected: ExpectedSubscriptionLiteral::Datetime,
                ..
            }
        ));
        assert!(!format!("{error:?}").contains("sensitive-invalid-date"));
    }

    #[test]
    fn branch_binding_failures_identify_the_relay_and_fields() {
        let relay = named("events");
        let field: FieldName = named("tenant");
        let branching = nervix_models::ResolvedBranching::branched(
            named("by_tenant"),
            nervix_models::CreateSchema {
                name: named("tenant_key"),
                fields: vec![SchemaField {
                    name: field.clone(),
                    ty: ParseAsType::U32,
                    optional: false,
                    sensitive: false,
                }],
            },
        );
        let binding = SubscriptionBinding {
            field: field.clone(),
            value: SubscriptionLiteral::Number("7".to_string()),
        };
        let unbranched = nervix_models::ResolvedBranching::unbranched();
        let error =
            validate_subscription_bindings(&relay, &unbranched, std::slice::from_ref(&binding))
                .err()
                .expect("an unbranched relay cannot bind a branch field");
        assert!(
            matches!(error.current_context(), SubscriptionError::UnbranchedBindings { relay: actual } if actual == &relay)
        );

        let error = validate_subscription_bindings(&relay, &branching, &[])
            .err()
            .expect("the branch field is required");
        assert!(
            matches!(error.current_context(), SubscriptionError::MissingBindings { relay: actual, fields } if actual == &relay && fields == std::slice::from_ref(&field))
        );

        let error =
            validate_subscription_bindings(&relay, &branching, &[binding.clone(), binding.clone()])
                .err()
                .expect("the same field cannot be bound twice");
        assert!(
            matches!(error.current_context(), SubscriptionError::DuplicateBinding { field: actual } if actual == &field)
        );

        let error = validate_subscription_bindings(
            &relay,
            &branching,
            &[SubscriptionBinding {
                field: named("region"),
                value: binding.value.clone(),
            }],
        )
        .err()
        .expect("bindings must name exactly the declared branch fields");
        assert!(
            matches!(error.current_context(), SubscriptionError::BindingFieldsMismatch { relay: actual, fields } if actual == &relay && fields == std::slice::from_ref(&field))
        );

        let empty = validate_subscription_bindings(&relay, &unbranched, &[])
            .expect("an unbranched subscription needs no bindings");
        let error = branch_key_from_filter(&branching, &empty)
            .expect_err("a branch key cannot omit a declared field");
        assert!(
            matches!(error.current_context(), SubscriptionError::MissingBranchField { field: actual } if actual == &field)
        );
        let filter = validate_subscription_bindings(&relay, &branching, &[binding])
            .expect("the exact typed binding is valid");
        assert!(
            branch_key_from_filter(&branching, &filter)
                .expect("the validated filter forms a key")
                .is_some()
        );
    }

    #[tokio::test]
    async fn subscription_interest_request_preserves_the_remote_target() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(false).await;
        let domain: DomainName = named("accounts");
        let relay: RelayName = named("events");
        let subscriber = service.inner.cluster.local_node_identity().await;
        let peer = named("unavailable_peer");
        let error = service
            .wait_for_subscription_interest_visibility_on_node(
                &peer,
                &subscriber,
                &domain,
                &relay,
                1,
            )
            .await
            .expect_err("the peer has no transport route");
        assert!(
            matches!(error.current_context(), SubscriptionError::InterestRequest { node, .. } if node == &peer)
        );
        assert!(error.contains::<nervix_interconnect::RequestError>());
        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).expect("the test database is removed");
    }

    #[test]
    fn subscription_batch_sample_rate_is_validated() {
        assert!(matches!(
            parse_subscription_batch_sample_rate(None),
            Ok(None)
        ));
        assert!(matches!(
            parse_subscription_batch_sample_rate(Some("0.25")),
            Ok(Some(0.25))
        ));
        assert!(parse_subscription_batch_sample_rate(Some("1.1")).is_err());
        assert!(parse_subscription_batch_sample_rate(Some("bad")).is_err());
    }

    #[test]
    fn subscription_sampling_respects_extreme_rates() {
        let key = string_branch_key("tenant", "acme");
        assert!(subscription_sample_passes(None, key.as_ref()));
        assert!(subscription_sample_passes(Some(1.0), key.as_ref()));
        assert!(!subscription_sample_passes(Some(0.0), key.as_ref()));
    }

    #[tokio::test]
    async fn announced_subscriptions_track_names_generations_and_withdrawal() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let (delivery, _frames) = session_delivery();
        let mut subscriptions = SessionSubscriptions::new();
        let events = RelayBroadcast::with_capacity(NonZeroUsize::MIN);
        let pending = pending_subscription(
            &service,
            &mut subscriptions,
            &delivery,
            "live_events",
            "events",
            events.new_receiver(),
        )
        .await;
        let first_generation = pending.subscription.handle.generation;
        assert!(
            subscriptions.view().subscription_names.is_empty(),
            "a generation joins its session only once it is announced"
        );

        subscriptions.activate(pending).await;
        assert_eq!(
            subscriptions.view().matching_subscription_names("LIVE"),
            vec!["live_events".to_string()]
        );
        assert!(
            subscriptions
                .view()
                .matching_subscription_names("missing")
                .is_empty()
        );

        let removed = subscriptions
            .remove(&named("live_events"))
            .await
            .assured("the announced subscription is in the session");
        assert_eq!(removed.domain, default_domain());
        assert_eq!(removed.handle.generation, first_generation);
        assert_eq!(
            events.receiver_count(),
            0,
            "a withdrawn generation leaves its relay"
        );
        assert!(
            subscriptions
                .remove(&named("missing_events"))
                .await
                .is_none()
        );

        let reused = subscriptions.next_handle(named("live_events"));
        assert!(
            reused.generation > first_generation,
            "a reused name opens with a new generation"
        );
        remove_test_directory(path);
    }

    #[tokio::test]
    async fn deleting_two_same_relay_subscriptions_withdraws_the_interest_exactly() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let domain = default_domain();
        let relay = named::<RelayName>("events");
        let (delivery, _frames) = session_delivery();
        let mut subscriptions = SessionSubscriptions::new();
        let events = RelayBroadcast::with_capacity(NonZeroUsize::MIN);
        for name in ["first", "second"] {
            let pending = pending_subscription(
                &service,
                &mut subscriptions,
                &delivery,
                name,
                "events",
                events.new_receiver(),
            )
            .await;
            subscriptions.activate(pending).await;
        }
        let interests = &service.inner.subscription_interests;
        assert_eq!(interests.leases(&domain, &relay), 2);

        let deleted = service
            .delete_subscription(
                nervix_models::DeleteSubscription {
                    name: named("first"),
                },
                &mut subscriptions,
            )
            .await;
        assert!(deleted.is_ok(), "the first subscription is deleted");
        assert_eq!(
            interests.leases(&domain, &relay),
            1,
            "the other subscription of the relay keeps the node's interest"
        );

        let deleted = service
            .delete_subscription(
                nervix_models::DeleteSubscription {
                    name: named("second"),
                },
                &mut subscriptions,
            )
            .await;
        assert!(deleted.is_ok(), "the second subscription is deleted");
        assert_eq!(
            interests.leases(&domain, &relay),
            0,
            "deleting the relay's last subscription withdraws the node's interest"
        );
        remove_test_directory(path);
    }

    #[tokio::test]
    async fn an_unannounced_subscription_releases_everything_and_sends_nothing() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let domain = default_domain();
        let relay = named::<RelayName>("events");
        let (delivery, mut frames) = session_delivery();
        let mut subscriptions = SessionSubscriptions::new();
        let events = RelayBroadcast::with_capacity(NonZeroUsize::MIN);
        let schema = user_id_schema();
        let pending = pending_subscription(
            &service,
            &mut subscriptions,
            &delivery,
            "never_announced",
            "events",
            events.new_receiver(),
        )
        .await;
        assert!(
            events.publish_for_test(user_id_batch(&schema, 1)).await,
            "an unannounced generation takes the relay's batches rather than holding the relay"
        );
        assert!(events.publish_for_test(user_id_batch(&schema, 2)).await);

        timeout(WAIT, pending.abandon())
            .await
            .assured("abandoning a generation does not wait on its client");
        assert_eq!(
            service.inner.subscription_interests.leases(&domain, &relay),
            0
        );
        assert_eq!(events.receiver_count(), 0);
        assert!(subscriptions.view().subscription_names.is_empty());

        drop(delivery);
        assert!(
            timeout(WAIT, frames.next())
                .await
                .is_ok_and(|frame| frame.is_none()),
            "a generation that was never announced sends nothing"
        );
        remove_test_directory(path);
    }

    #[tokio::test]
    async fn deleting_a_blocking_subscription_whose_client_reads_nothing_releases_its_relay() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let domain = default_domain();
        let relay = named::<RelayName>("events");
        let (delivery, mut frames) = session_delivery();
        let mut subscriptions = SessionSubscriptions::new();
        let events = Arc::new(RelayBroadcast::with_capacity(NonZeroUsize::MIN));
        let schema = user_id_schema();
        let pending = pending_subscription(
            &service,
            &mut subscriptions,
            &delivery,
            "blocked",
            "events",
            events.new_receiver(),
        )
        .await;
        subscriptions.activate(pending).await;

        // The lane takes one frame per batch until it is full, the delivery holds the next
        // frame it cannot queue, and the relay receiver holds one more batch.
        let absorbed = SESSION_SUBSCRIPTION_CAPACITY
            .checked_add(2)
            .assured("the lane capacity is a small constant");
        for user_id in 0..absorbed {
            let user_id = u32::try_from(user_id).assured("a few test rows fit in u32");
            let published = timeout(
                WAIT,
                events.publish_for_test(user_id_batch(&schema, user_id)),
            )
            .await
            .assured("the subscription has room for this batch");
            assert!(published);
        }
        let held_schema = schema.clone();
        let held = events.clone();
        let held_publisher = tokio::spawn(async move {
            held.publish_for_test(user_id_batch(&held_schema, 999))
                .await
        });
        timeout(WAIT, async {
            while events.waiting_publishers() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .assured("a blocking subscription whose client reads nothing holds its relay's publisher");

        let removed = timeout(WAIT, subscriptions.remove(&named("blocked")))
            .await
            .assured("deleting a subscription does not wait on its client");
        assert!(removed.is_some());
        assert_eq!(
            service.inner.subscription_interests.leases(&domain, &relay),
            0
        );
        let delivered = timeout(WAIT, held_publisher)
            .await
            .assured("the withdrawn subscription releases its relay's publisher")
            .assured("the publisher does not panic");
        assert!(!delivered, "no subscriber was left to take the held batch");

        drop(delivery);
        assert!(
            timeout(WAIT, frames.next())
                .await
                .is_ok_and(|frame| frame.is_none()),
            "nothing a withdrawn generation queued reaches its client"
        );
        remove_test_directory(path);
    }

    #[tokio::test]
    async fn a_closed_relay_ends_the_subscription_with_a_typed_reason_as_its_last_frame() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let domain = default_domain();
        let relay = named::<RelayName>("events");
        let (delivery, mut frames) = session_delivery();
        let mut subscriptions = SessionSubscriptions::new();
        let events = RelayBroadcast::with_capacity(NonZeroUsize::MIN);
        let schema = user_id_schema();
        let pending = pending_subscription(
            &service,
            &mut subscriptions,
            &delivery,
            "live_events",
            "events",
            events.new_receiver(),
        )
        .await;
        let handle = pending.subscription.handle.clone();
        subscriptions.activate(pending).await;
        assert!(events.publish_for_test(user_id_batch(&schema, 7)).await);

        drop(events);
        let ServerEvent::SubscriptionRows(rows) = next_event(&mut frames).await else {
            panic!("the batch published before the relay closed is delivered first");
        };
        assert_eq!(rows.subscription(), &handle);
        let ServerEvent::SubscriptionEnded(ended) = next_event(&mut frames).await else {
            panic!("the relay closing ends the subscription");
        };
        assert_eq!(ended.subscription, handle);
        assert_eq!(
            ended.reason,
            SubscriptionEndReason::RelayRemoved,
            "the schedule declares no such relay, so it was removed"
        );
        assert!(ended.message.contains("subscription 'live_events' ended"));
        assert_eq!(
            service.inner.subscription_interests.leases(&domain, &relay),
            0
        );

        timeout(WAIT, async {
            while subscriptions.contains_name(&named("live_events")) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .assured("an ended generation stops delivering");
        let removed = subscriptions
            .remove(&named("live_events"))
            .await
            .assured("an ended generation stays deletable");
        assert_eq!(removed.handle, handle);
        remove_test_directory(path);
    }
}
