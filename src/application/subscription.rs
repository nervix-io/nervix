//! Read-only filtered views a session attaches to a relay.
//!
//! Layer: control plane.
//!
//! - **Owns.** Subscription creation and deletion, the per-session subscription set and the
//!   generation each subscription is opened with, cluster-wide subscription interest, and the
//!   filtering, sampling and Row delivery each subscription asks for.
//! - **Depends on.** The registry for schemas and schedules, the runtime for relay receivers, the
//!   interconnect to make interest visible on every node, and the Row encoder that writes selected
//!   Arrow rows into client frames.
//! - **Must not know.** Construction, inheritance, values or any other side effect a processor has.
use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU64, NonZeroUsize},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use ahash::{HashMap, HashMapExt};
use blake3::Hasher;
use futures_util::{StreamExt, stream::FuturesUnordered};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto;
use nervix_client_wire::{
    EncodedFrame, RowsSkippedCause, ServerFrame, SessionLimits, SubscriptionDeliveryLost,
    SubscriptionEndReason, SubscriptionEnded, SubscriptionHandle, SubscriptionOpened,
    SubscriptionRowsSkipped,
};
use nervix_consensus::{
    CommandExecutionTransactionOperation, CommandExecutionTransactionRequest,
    CommandExecutionTransactionTarget, ReplicatedTransaction,
};
use nervix_interconnect::SubscriptionInterestVisibilityRequest as RemoteSubscriptionInterestVisibilityRequest;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeName, CommandExecutionReference, CreateRelay, CreateSchema,
    DomainName, FieldName, ModelKind, ParseAsType, RelayName, ScheduledModel, SubscriptionBinding,
    SubscriptionDeliveryBehavior, SubscriptionLiteral, SubscriptionName,
    TransactionPreviewIdentity, UserName,
};
use nervix_nspl::client_statement::{ClientStatement, ParsedClientStatement};
use nonzero_ext::nonzero;
use sorted_vec::SortedSet;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tracing::debug;
use triomphe::Arc;

#[cfg(test)]
use super::authentication::DEFAULT_USER;
use super::{
    command_result::{CommandDiagnostic, CommandDisposition, CommandResult},
    model_mutation::{
        append_command_result, command_batch_result, command_error, command_ok,
        command_results_message,
    },
    session_service::SessionServiceImpl,
    transaction::{InspectingSession, transaction_status},
};
use crate::{
    runtime::{
        BranchKey, CompiledSubscriptionPredicate, RelayRecordBatch, RelaySubscriptionReceiver,
        Runtime, SubscriptionPredicateCompileContext, compile_subscription_predicate,
        execute_subscription_predicate_on_record, scheduled_relay_owner_nodes,
    },
    runtime_schema,
    subscription_row::{
        SubscriptionBranchSchema, SubscriptionRowEncoder, SubscriptionRowFrame,
        SubscriptionRowOpening, SubscriptionRowSelection, subscription_row_schema,
    },
    task_shutdown::JoinShutdown,
};

static SESSION_SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The most rows one frame of a subscription carries. A frame also stops at the session's frame
/// limit, so this bounds the rows a client decodes per frame rather than the frame's size.
const SUBSCRIPTION_ROWS_PER_FRAME: NonZeroUsize = nonzero!(256_usize);

/// The frames a session sends its client, in the order they are queued.
pub(in crate::application) type SessionOutbound = mpsc::Sender<EncodedFrame<ServerFrame>>;

/// Where a session's subscriptions deliver their frames, and the limits those frames are held to.
#[derive(Clone)]
pub(in crate::application) struct SessionDelivery {
    pub(in crate::application) outbound: SessionOutbound,
    pub(in crate::application) limits: SessionLimits,
}

struct SessionSubscription {
    handle: SubscriptionHandle,
    domain: DomainName,
    relay: RelayName,
    active: Arc<AtomicBool>,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
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

/// A subscription the session opened. Its rows wait for the reply that announces it.
pub(in crate::application) struct OpenedSubscription {
    pub(in crate::application) opened: SubscriptionOpened,
    pub(in crate::application) message: String,
    /// Fired once the reply carrying `opened` is queued. The subscription's rows follow that
    /// reply and never precede it; dropping this without firing ends the subscription unused.
    pub(in crate::application) release: oneshot::Sender<()>,
}

/// A subscription the session deleted.
pub(in crate::application) struct DeletedSubscription {
    pub(in crate::application) handle: SubscriptionHandle,
    pub(in crate::application) message: String,
}

/// Everything the delivery task of one subscription needs.
struct SessionSubscriptionTaskConfig {
    handle: SubscriptionHandle,
    predicate: Option<CompiledSubscriptionPredicate>,
    delivery_behavior: SubscriptionDeliveryBehavior,
    batch_sample_rate: Option<f64>,
    runtime: Runtime,
    receiver: RelaySubscriptionReceiver<RelayRecordBatch>,
    encoder: SubscriptionRowEncoder,
    delivery: SessionDelivery,
    opened: oneshot::Receiver<()>,
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
    ) -> Result<Vec<SessionCommandOperation>, String> {
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
                        return Err("transaction is already active".to_string());
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
                        return Err("COMMIT requires an active transaction".to_string());
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
                        return Err("REVERT requires an active transaction".to_string());
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
                            return Err("DESCRIBE TRANSACTION must be executed separately; it \
                                        reads a transaction and never joins a multi-statement \
                                        request"
                                .to_string());
                        }
                        operations.push(SessionCommandOperation::Execute(command));
                    } else if transaction_active {
                        let Some(current_position) = transaction_position else {
                            return Err("transaction command is missing its expected queue \
                                        position"
                                .to_string());
                        };
                        transaction_position = current_position.checked_add(1);
                        if transaction_position.is_none() {
                            return Err("transaction queue position overflowed".to_string());
                        }
                        preview_describes_transaction = false;
                        operations.push(SessionCommandOperation::Queue(command));
                    } else if multi_statement {
                        return Err("multiple commands require BEGIN".to_string());
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

    fn insert(
        &mut self,
        domain: DomainName,
        relay: RelayName,
        config: SessionSubscriptionTaskConfig,
    ) {
        let (stop_tx, stop_rx) = watch::channel(false);
        let active = Arc::new(AtomicBool::new(true));
        let task_active = active.clone();
        let handle = config.handle.clone();
        let task_domain = domain.clone();
        let task_relay = relay.clone();
        let task = tokio::spawn(async move {
            run_subscription_delivery(config, task_domain, task_relay, stop_rx).await;
            task_active.store(false, Ordering::Release);
        });

        self.subscriptions.insert(
            handle.name.clone(),
            SessionSubscription {
                handle,
                domain,
                relay,
                active,
                stop_tx,
                task,
            },
        );
    }

    fn contains_domain_stream(&self, domain: &DomainName, relay: &RelayName) -> bool {
        self.subscriptions.values().any(|subscription| {
            subscription.active.load(Ordering::Acquire)
                && subscription.domain == *domain
                && subscription.relay == *relay
        })
    }

    fn contains_name(&self, name: &SubscriptionName) -> bool {
        self.subscriptions
            .get(name)
            .is_some_and(|subscription| subscription.active.load(Ordering::Acquire))
    }

    /// Stops and joins the subscription named `name`, returning what identified it.
    async fn remove(&mut self, name: &SubscriptionName) -> Option<RemovedSubscription> {
        let subscription = self.subscriptions.remove(name)?;
        subscription.stop_tx.send_replace(true);
        subscription
            .task
            .join_after_shutdown("session subscription")
            .await;
        Some(RemovedSubscription {
            handle: subscription.handle,
            domain: subscription.domain,
            relay: subscription.relay,
        })
    }

    pub(in crate::application) async fn stop_all(&mut self, service: &SessionServiceImpl) {
        for (_, subscription) in self.subscriptions.drain() {
            subscription.stop_tx.send_replace(true);
            subscription
                .task
                .join_after_shutdown("session subscription")
                .await;
            service
                .unregister_subscription_interest(&subscription.domain, &subscription.relay)
                .await;
        }
    }
}

/// A subscription taken out of its session, and the relay it read.
struct RemovedSubscription {
    handle: SubscriptionHandle,
    domain: DomainName,
    relay: RelayName,
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

/// Queues one subscription's frames on its session.
struct SubscriptionSender {
    handle: SubscriptionHandle,
    delivery: SessionDelivery,
    behavior: SubscriptionDeliveryBehavior,
    /// Rows a dropping subscription discarded that its client has not been told about yet.
    dropped_rows: u64,
}

impl SubscriptionSender {
    /// Queues a notice about the subscription, waiting for room. `false` means the session is
    /// gone.
    async fn send_notice(
        &self,
        frame: Result<
            EncodedFrame<ServerFrame>,
            error_stack::Report<nervix_client_wire::WireEncodeError>,
        >,
    ) -> bool {
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                debug!(
                    subscription = %self.handle.name,
                    error = %error,
                    "a subscription notice does not fit a session frame"
                );
                return true;
            }
        };
        self.delivery.outbound.send(frame).await.is_ok()
    }

    async fn report_skipped(&self, skipped: SkippedRows) -> bool {
        let frame = SubscriptionRowsSkipped {
            subscription: self.handle.clone(),
            cause: skipped.cause,
            skipped_rows: skipped.rows,
            message: skipped.message,
        }
        .encode(&self.delivery.limits);
        self.send_notice(frame).await
    }

    async fn report_end(&self, message: String) -> bool {
        let frame = SubscriptionEnded {
            subscription: self.handle.clone(),
            reason: SubscriptionEndReason::RelayClosed,
            message,
        }
        .encode(&self.delivery.limits);
        self.send_notice(frame).await
    }

    /// Queues one frame of rows. A blocking subscription waits for room; a dropping one discards
    /// the frame when the session is full and reports the loss before the next rows it delivers.
    /// `false` means the session is gone.
    async fn send_rows(&mut self, frame: SubscriptionRowFrame) -> bool {
        let SubscriptionRowFrame { frame, rows } = frame;
        let rows = u64::try_from(rows.get())
            .assured("supported targets have a pointer width no larger than u64");
        match self.behavior {
            SubscriptionDeliveryBehavior::Blocking => {
                self.delivery.outbound.send(frame).await.is_ok()
            }
            SubscriptionDeliveryBehavior::Dropping => {
                if let Some(dropped_rows) = NonZeroU64::new(self.dropped_rows) {
                    let lost = SubscriptionDeliveryLost {
                        subscription: self.handle.clone(),
                        dropped_rows,
                    }
                    .encode(&self.delivery.limits);
                    match lost {
                        Ok(lost) => match self.delivery.outbound.try_send(lost) {
                            Ok(()) => self.dropped_rows = 0,
                            // The loss is still unreported, so these rows cannot go ahead of it.
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                self.count_dropped(rows);
                                return true;
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => return false,
                        },
                        Err(error) => {
                            debug!(
                                subscription = %self.handle.name,
                                error = %error,
                                "a subscription loss report does not fit a session frame"
                            );
                            self.dropped_rows = 0;
                        }
                    }
                }
                match self.delivery.outbound.try_send(frame) {
                    Ok(()) => true,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        self.count_dropped(rows);
                        true
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                }
            }
        }
    }

    fn count_dropped(&mut self, rows: u64) {
        self.dropped_rows = self.dropped_rows.checked_add(rows).assured(
            "the count restarts at every report, and no session drops 2^64 rows between two: at a \
             billion rows a second that takes centuries",
        );
    }
}

/// Delivers one subscription's rows until it is stopped, its relay closes or its session goes
/// away.
async fn run_subscription_delivery(
    config: SessionSubscriptionTaskConfig,
    domain: DomainName,
    relay: RelayName,
    mut stop_rx: watch::Receiver<bool>,
) {
    let SessionSubscriptionTaskConfig {
        handle,
        predicate,
        delivery_behavior,
        batch_sample_rate,
        runtime,
        mut receiver,
        encoder,
        delivery,
        opened,
    } = config;
    // Rows follow the reply that opened the subscription and never precede it. A session that
    // never queued that reply never announced the subscription, so it delivers nothing.
    if opened.await.is_err() {
        return;
    }
    let mut sender = SubscriptionSender {
        handle,
        delivery,
        behavior: delivery_behavior,
        dropped_rows: 0,
    };
    loop {
        tokio::task::consume_budget().await;
        tokio::select! {
            batch = receiver.recv() => {
                let Some(batch) = batch else {
                    let message = format!(
                        "session subscription '{}' was dropped because relay '{}' in domain '{}' \
                         was rebuilt after a schema or execution change; recreate the \
                         subscription against the current schema",
                        sender.handle.name, relay, domain,
                    );
                    sender.report_end(message).await;
                    return;
                };
                let selection = select_subscription_rows(
                    &batch,
                    predicate.as_ref(),
                    batch_sample_rate,
                    &runtime,
                    &domain,
                )
                .await;
                if let Some(skipped) = selection.skipped
                    && !sender.report_skipped(skipped).await
                {
                    return;
                }
                if selection.rows.is_empty() {
                    continue;
                }
                let frames = encoder.encode(
                    batch.record_batch(),
                    batch.branch_keys(),
                    SubscriptionRowSelection::Rows(&selection.rows),
                );
                let frames = match frames {
                    Ok(frames) => frames,
                    Err(error) => {
                        let rows = NonZeroU64::new(
                            u64::try_from(selection.rows.len()).assured(
                                "supported targets have a pointer width no larger than u64",
                            ),
                        )
                        .verified("the empty selection above already continued");
                        let skipped = SkippedRows {
                            cause: RowsSkippedCause::EncodingFailed,
                            rows,
                            message: format!(
                                "session subscription '{}' could not encode rows of relay '{}': \
                                 {error}",
                                sender.handle.name, relay,
                            ),
                        };
                        if !sender.report_skipped(skipped).await {
                            return;
                        }
                        continue;
                    }
                };
                for frame in frames {
                    tokio::task::consume_budget().await;
                    if !sender.send_rows(frame).await {
                        return;
                    }
                }
            }
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    return;
                }
            }
        }
    }
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
) -> Result<SubscriptionFilter, String> {
    if branching.is_unbranched() {
        if bindings.is_empty() {
            return Ok(SubscriptionFilter {
                bindings: Vec::new(),
            });
        }
        return Err(format!(
            "stream '{}' is not branched and does not accept WHERE bindings",
            relay.as_str()
        ));
    }

    let schema = branching
        .schema()
        .verified("a branched subscription target carries its resolved key schema");
    let branch_fields = branching.field_names().cloned().collect::<Vec<_>>();

    if bindings.is_empty() {
        return Err(format!(
            "stream '{}' requires WHERE bindings for ({})",
            relay.as_str(),
            branch_fields
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
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
            return Err(format!(
                "subscription binding '{}' is specified more than once",
                binding.field.as_str()
            ));
        }
    }

    let expected = SortedSet::from_unsorted(branch_fields.clone()).into_vec();
    let actual = SortedSet::from_unsorted(bound.keys().cloned().collect::<Vec<_>>()).into_vec();
    if expected != actual {
        return Err(format!(
            "subscription bindings for relay '{}' must exactly match ({})",
            relay.as_str(),
            branch_fields
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let mut matchers = Vec::new();
    for field in &branch_fields {
        let ty = fields.get(field).ok_or_else(|| {
            format!(
                "branch field '{}' is missing from schema '{}'",
                field.as_str(),
                schema.name.as_str()
            )
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
) -> Result<Option<crate::runtime::BranchKey>, String> {
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
            return Err(format!(
                "missing binding for branch field '{}'",
                field.as_str()
            ));
        };
        fields.push((field.clone(), binding.expected.clone()));
    }
    crate::runtime::BranchKey::from_fields(fields).map(Some)
}

pub(in crate::application) fn render_subscription_literal(literal: &SubscriptionLiteral) -> String {
    match literal {
        SubscriptionLiteral::String(value) => format!("'{}'", value.replace('\'', "''")),
        SubscriptionLiteral::Number(value) => value.clone(),
        SubscriptionLiteral::Bool(value) => value.to_string(),
    }
}

fn parse_subscription_batch_sample_rate(rate: Option<&str>) -> Result<Option<f64>, String> {
    let Some(rate) = rate else {
        return Ok(None);
    };
    let parsed = rate
        .parse::<f64>()
        .map_err(|error| format!("invalid batch sample rate '{rate}': {error}"))?;
    if (0.0..=1.0).contains(&parsed) {
        Ok(Some(parsed))
    } else {
        Err(format!(
            "invalid batch sample rate '{rate}': must be between 0.0 and 1.0"
        ))
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
) -> Result<runtime_schema::RuntimeValue, String> {
    use runtime_schema::RuntimeValue;

    let bad = |expected: &str| {
        format!(
            "subscription binding '{}' expects {} literal for type {:?}",
            field.as_str(),
            expected,
            ty
        )
    };

    match (ty, literal) {
        (ParseAsType::String, SubscriptionLiteral::String(v)) => {
            Ok(RuntimeValue::String(v.clone()))
        }
        (ParseAsType::Datetime, SubscriptionLiteral::String(v)) => {
            chrono::DateTime::parse_from_rfc3339(v)
                .map(RuntimeValue::Datetime)
                .map_err(|_| bad("RFC3339 datetime string"))
        }
        (ParseAsType::Bool, SubscriptionLiteral::Bool(v)) => Ok(RuntimeValue::Bool(*v)),
        (ParseAsType::U8, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U8).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I8, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I8).map_err(|_| bad("numeric"))
        }
        (ParseAsType::U16, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U16).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I16, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I16).map_err(|_| bad("numeric"))
        }
        (ParseAsType::U32, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U32).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I32, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I32).map_err(|_| bad("numeric"))
        }
        (ParseAsType::U64, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U64).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I64, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I64).map_err(|_| bad("numeric"))
        }
        (ParseAsType::F32, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::F32).map_err(|_| bad("numeric"))
        }
        (ParseAsType::F64, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::F64).map_err(|_| bad("numeric"))
        }
        _ => Err(bad(match ty {
            ParseAsType::String | ParseAsType::Datetime => "string",
            ParseAsType::Bool => "boolean",
            ParseAsType::Array { .. } | ParseAsType::Vec { .. } => "array",
            _ => "numeric",
        })),
    }
}

/// One relay whose subscription interest this node advertises to the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::application) struct SubscriptionInterestKey {
    domain: DomainName,
    relay: RelayName,
}

/// The relay a subscription attaches to, as the cluster schedule describes it: the relay model,
/// the schema its records carry, and the branch key fields a subscription may bind.
pub(in crate::application) struct SubscriptionTarget {
    pub(in crate::application) relay: nervix_models::CreateRelay,
    pub(in crate::application) schema: nervix_models::CreateSchema,
    pub(in crate::application) branching: nervix_models::ResolvedBranching,
}

impl SessionServiceImpl {
    async fn register_subscription_interest(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), String> {
        let key = SubscriptionInterestKey {
            domain: domain.clone(),
            relay: relay.clone(),
        };
        let first_interest = {
            let mut entry = self
                .inner
                .subscription_interest_counts
                .entry(key)
                .or_insert(0);
            *entry += 1;
            *entry == 1
        };
        if first_interest {
            self.inner
                .cluster
                .set_local_subscription_interest(domain.as_str(), relay.as_str(), true)
                .await;
        }
        if let Err(error) = self
            .wait_for_subscription_interest_visibility(domain, relay)
            .await
        {
            self.unregister_subscription_interest(domain, relay).await;
            return Err(error);
        }
        Ok(())
    }

    async fn wait_for_subscription_interest_visibility(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), String> {
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
        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "subscription interest in relay '{}' in domain '{}' did not become visible on \
                 every live node: {:?}",
                relay.as_str(),
                domain.as_str(),
                errors,
            ))
        }
    }

    async fn wait_for_subscription_interest_visibility_on_node(
        &self,
        target_node_id: &ClusterNodeName,
        subscriber: &ClusterNodeIdentity,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), String> {
        let response = self
            .inner
            .interconnect
            .request(
                target_node_id,
                RemoteSubscriptionInterestVisibilityRequest {
                    subscriber: subscriber.clone(),
                    domain: domain.clone(),
                    relay: relay.clone(),
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        response.result.map_err(|failure| failure.to_string())
    }

    async fn unregister_subscription_interest(&self, domain: &DomainName, relay: &RelayName) {
        let key = SubscriptionInterestKey {
            domain: domain.clone(),
            relay: relay.clone(),
        };
        let mut should_clear = false;
        if let Some(mut entry) = self.inner.subscription_interest_counts.get_mut(&key) {
            if *entry <= 1 {
                should_clear = true;
            } else {
                *entry -= 1;
            }
        }
        if should_clear {
            self.inner.subscription_interest_counts.remove(&key);
            self.inner
                .cluster
                .set_local_subscription_interest(domain.as_str(), relay.as_str(), false)
                .await;
        }
    }

    pub(in crate::application) async fn scheduled_stream_owner_nodes(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Vec<ClusterNodeName>, String> {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(Vec::new());
        };
        Ok(scheduled_relay_owner_nodes(domain_schedule, relay))
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
    ) -> Result<Option<SubscriptionTarget>, String> {
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
            return Err(format!(
                "stream '{}' references missing scheduled schema '{}'",
                relay.as_str(),
                ack_model.schema.as_str()
            ));
        };
        Ok(Some(SubscriptionTarget {
            relay: ack_model.clone(),
            schema: schema.clone(),
            branching: relay_node.resolved_branching.clone().ok_or_else(|| {
                format!(
                    "stream '{}' has no resolved branch declaration in the schedule",
                    relay.as_str()
                )
            })?,
        }))
    }

    async fn subscription_stream_schema(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Option<nervix_models::CreateSchema>, String> {
        match self.inner.registry.get::<CreateRelay>(domain, relay) {
            Ok(Some(ack_model)) => {
                match self
                    .inner
                    .registry
                    .get::<CreateSchema>(domain, &ack_model.schema)
                {
                    Ok(Some(schema)) => Ok(Some(schema)),
                    Ok(None) => Err(format!(
                        "stream '{}' references missing schema '{}'",
                        relay.as_str(),
                        ack_model.schema.as_str()
                    )),
                    Err(err) => Err(format!(
                        "failed to resolve schema '{}' for relay '{}': {err}",
                        ack_model.schema.as_str(),
                        relay.as_str()
                    )),
                }
            }
            Ok(None) => self
                .subscription_target_from_schedule(domain, relay)
                .await
                .map(|resolved| resolved.map(|target| target.schema)),
            Err(err) => Err(format!(
                "failed to resolve relay '{}' for subscription: {err}",
                relay.as_str()
            )),
        }
    }

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
                    let diagnostic = CommandDiagnostic::unlocated(err.clone());
                    return Err(Box::new(CommandResult {
                        diagnostics: vec![diagnostic],
                        ..CommandResult::new(
                            CommandDisposition::Failed,
                            format!("failed to subscribe session '{}': {err}", subscription.name),
                        )
                    }));
                }
            };

        let relay_missing = || {
            let diagnostic = CommandDiagnostic::unlocated(format!(
                "stream '{}' not found",
                subscription.relay.as_str()
            ));
            let message = format!(
                "stream '{}' does not exist in domain '{}'",
                subscription.relay.as_str(),
                domain.as_str()
            );
            CommandResult {
                diagnostics: vec![diagnostic],
                ..CommandResult::new(CommandDisposition::Failed, message)
            }
        };
        match self
            .inner
            .registry
            .contains(domain, ModelKind::Relay, &subscription.relay)
        {
            Ok(true) => {}
            Ok(false) => match self
                .subscription_target_from_schedule(domain, &subscription.relay)
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => return Err(Box::new(relay_missing())),
                Err(err) => {
                    return Err(Box::new(command_error(format!(
                        "failed to resolve relay for subscription: {err}"
                    ))));
                }
            },
            Err(err) => {
                return Err(Box::new(command_error(format!(
                    "failed to resolve relay for subscription: {err}"
                ))));
            }
        }

        let payload_schema = match self
            .subscription_stream_schema(domain, &subscription.relay)
            .await
        {
            Ok(Some(schema)) => schema,
            Ok(None) => return Err(Box::new(relay_missing())),
            Err(err) => {
                return Err(Box::new(command_error(format!(
                    "failed to resolve relay for subscription: {err}"
                ))));
            }
        };
        let predicate = match subscription.where_clause.as_ref() {
            Some(expression) => {
                let udfs = self.inner.runtime.udf_executor(domain);
                let compiled = runtime_schema::compile_schema(&payload_schema);
                let compiled_predicate = compile_subscription_predicate(
                    domain,
                    &subscription.name,
                    expression,
                    SubscriptionPredicateCompileContext::new(
                        compiled.arrow_schema(),
                        compiled.vm_sensitivity(),
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

        let branching = match self
            .subscription_target_from_schedule(domain, &subscription.relay)
            .await
        {
            Ok(Some(target)) => target.branching,
            Ok(None) => {
                return Err(Box::new(command_error(format!(
                    "stream '{}' has no scheduled branch declaration in domain '{}'",
                    subscription.relay.as_str(),
                    domain.as_str(),
                ))));
            }
            Err(error) => {
                return Err(Box::new(command_error(format!(
                    "failed to resolve branch declaration for relay '{}': {error}",
                    subscription.relay.as_str(),
                ))));
            }
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
        let receiver = match self.inner.runtime.subscribe_stream(domain, &relay).await {
            Ok(receiver) => receiver,
            Err(err) => {
                return Err(Box::new(command_error(format!(
                    "failed to subscribe to relay '{}': {err}",
                    relay.as_str()
                ))));
            }
        };

        if let Err(error) = self.register_subscription_interest(domain, &relay).await {
            return Err(Box::new(command_error(format!(
                "failed to register subscription interest for relay '{}' in domain '{}': {error}",
                relay.as_str(),
                domain.as_str(),
            ))));
        }
        let (opened, encoder) = opening.open(domain.clone(), relay.clone());
        let (release, released) = oneshot::channel();
        subscriptions.insert(
            domain.clone(),
            relay,
            SessionSubscriptionTaskConfig {
                handle,
                predicate,
                delivery_behavior: subscription.delivery_behavior,
                batch_sample_rate,
                runtime: self.inner.runtime.clone(),
                receiver,
                encoder,
                delivery: delivery.clone(),
                opened: released,
            },
        );

        Ok(OpenedSubscription {
            opened,
            message: format!(
                "created subscription '{}' in domain '{}'",
                subscription.name,
                domain.as_str()
            ),
            release,
        })
    }

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
        if !subscriptions.contains_domain_stream(&removed.domain, &removed.relay) {
            self.unregister_subscription_interest(&removed.domain, &removed.relay)
                .await;
        }
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
                        Err(message) => command_error(message),
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
    use std::sync::Arc as StdArc;

    use arrow_array::{RecordBatch, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};
    use nervix_client_wire::{
        RowSchema, ServerEvent, ServerMessage, SubscriptionEndReason, VerifiedFrame,
    };
    use nervix_models::{DomainName, SchemaField, SubscriptionDeliveryBehavior};
    use nervix_recovery::Discarded as _;
    use tokio::{sync::mpsc, time::Duration};

    use super::{
        super::test_fixtures::{TestService, build_test_service, named, string_branch_key},
        *,
    };
    use crate::runtime::Runtime;

    /// The delivery task configuration of a subscription whose rows hold one `user_id` field, with
    /// its opening reply already queued.
    fn task_config(
        subscriptions: &mut SessionSubscriptions,
        name: &str,
        receiver: RelaySubscriptionReceiver<RelayRecordBatch>,
        outbound: SessionOutbound,
    ) -> SessionSubscriptionTaskConfig {
        let handle = subscriptions.next_handle(named(name));
        let schema = RowSchema {
            fields: vec![SchemaField {
                name: named("user_id"),
                ty: ParseAsType::U32,
                optional: false,
                sensitive: false,
            }],
            branch: None,
        };
        let (_, encoder) = SubscriptionRowOpening::new(
            handle.clone(),
            schema,
            SessionLimits::DEFAULT,
            SUBSCRIPTION_ROWS_PER_FRAME,
        )
        .assured("the test row limit fits the default collection limit")
        .open(named("default"), named("events"));
        let (release, opened) = oneshot::channel();
        release
            .send(())
            .discarded("the configuration keeps the receiver until its task starts");
        SessionSubscriptionTaskConfig {
            handle,
            predicate: None,
            delivery_behavior: SubscriptionDeliveryBehavior::Blocking,
            batch_sample_rate: None,
            runtime: Runtime::default(),
            receiver,
            encoder,
            delivery: SessionDelivery {
                outbound,
                limits: SessionLimits::DEFAULT,
            },
            opened,
        }
    }

    /// One frame holding a row for each of `user_ids`, as the subscription's encoder writes it.
    fn row_frame(encoder: &SubscriptionRowEncoder, user_ids: &[u32]) -> SubscriptionRowFrame {
        let schema = Schema::new(vec![Field::new("user_id", DataType::UInt32, false)]);
        let column = UInt32Array::from(user_ids.to_vec());
        let batch = RecordBatch::try_new(StdArc::new(schema), vec![StdArc::new(column)])
            .assured("the column matches the one-field schema");
        let unbranched = vec![None; user_ids.len()];
        let frames = encoder
            .encode(&batch, &unbranched, SubscriptionRowSelection::All)
            .assured("the test rows match the subscription's row schema");
        let Ok([frame]) = <[SubscriptionRowFrame; 1]>::try_from(frames) else {
            panic!("a handful of rows fits one frame");
        };
        frame
    }

    /// The next frame the session would send, decoded.
    async fn next_event(frames: &mut mpsc::Receiver<EncodedFrame<ServerFrame>>) -> ServerEvent {
        let frame = tokio::time::timeout(Duration::from_secs(1), frames.recv())
            .await
            .assured("the sender queues its frames before the test reads them")
            .assured("the test holds the sender");
        let frame = VerifiedFrame::verify(frame.into_bytes(), &SessionLimits::DEFAULT)
            .assured("the server encodes frames the session limits admit");
        let ServerMessage::Event(event) = ServerMessage::decode(&frame).assured("a frame decodes")
        else {
            panic!("a subscription sends only events");
        };
        event
    }

    fn delivered_rows(event: ServerEvent) -> usize {
        let ServerEvent::SubscriptionRows(rows) = event else {
            panic!("expected subscription rows, found {event:?}");
        };
        rows.batch().len()
    }

    #[tokio::test]
    async fn a_dropping_subscription_reports_what_it_dropped_before_its_next_rows() {
        let mut subscriptions = SessionSubscriptions::new();
        // Room for two frames, so the third is dropped until the session drains its queue.
        let (outbound, mut frames) = mpsc::channel(2);
        let events = crate::runtime::RelayBroadcast::with_capacity(
            NonZeroUsize::new(4).assured("the test relay capacity is a nonzero literal"),
        );
        let SessionSubscriptionTaskConfig {
            handle,
            encoder,
            delivery,
            ..
        } = task_config(
            &mut subscriptions,
            "sampled_events",
            events.new_receiver(),
            outbound,
        );
        let mut sender = SubscriptionSender {
            handle: handle.clone(),
            delivery,
            behavior: SubscriptionDeliveryBehavior::Dropping,
            dropped_rows: 0,
        };

        assert!(sender.send_rows(row_frame(&encoder, &[1])).await);
        assert!(sender.send_rows(row_frame(&encoder, &[2, 3])).await);
        assert!(sender.send_rows(row_frame(&encoder, &[4])).await);
        assert_eq!(sender.dropped_rows, 1, "a full session drops the frame");
        // The loss is still unreported and finds no room either, so these rows are dropped too.
        assert!(sender.send_rows(row_frame(&encoder, &[5, 6])).await);
        assert_eq!(sender.dropped_rows, 3);

        assert_eq!(delivered_rows(next_event(&mut frames).await), 1);
        assert_eq!(delivered_rows(next_event(&mut frames).await), 2);
        assert!(sender.send_rows(row_frame(&encoder, &[7])).await);
        let ServerEvent::SubscriptionDeliveryLost(lost) = next_event(&mut frames).await else {
            panic!("the loss is reported before the rows that follow it");
        };
        assert_eq!(lost.subscription, handle);
        assert_eq!(lost.dropped_rows.get(), 3);
        assert_eq!(sender.dropped_rows, 0, "a reported loss starts a new count");
        assert_eq!(delivered_rows(next_event(&mut frames).await), 1);

        drop(frames);
        assert!(
            !sender.send_rows(row_frame(&encoder, &[8])).await,
            "rows for a session that is gone end the delivery"
        );
    }

    #[tokio::test]
    async fn skipped_rows_are_reported_with_their_cause_and_count() {
        let mut subscriptions = SessionSubscriptions::new();
        let (outbound, mut frames) = mpsc::channel(2);
        let events = crate::runtime::RelayBroadcast::with_capacity(
            NonZeroUsize::new(4).assured("the test relay capacity is a nonzero literal"),
        );
        let SessionSubscriptionTaskConfig {
            handle, delivery, ..
        } = task_config(
            &mut subscriptions,
            "filtered_events",
            events.new_receiver(),
            outbound,
        );
        let sender = SubscriptionSender {
            handle: handle.clone(),
            delivery,
            behavior: SubscriptionDeliveryBehavior::Blocking,
            dropped_rows: 0,
        };
        let skipped = SkippedRows {
            cause: RowsSkippedCause::FilterFailed,
            rows: NonZeroU64::new(2).assured("two is non-zero"),
            message: "session subscription predicate failed: division by zero".to_string(),
        };

        assert!(sender.report_skipped(skipped).await);

        let ServerEvent::SubscriptionRowsSkipped(reported) = next_event(&mut frames).await else {
            panic!("skipped rows are reported as such");
        };
        assert_eq!(reported.subscription, handle);
        assert_eq!(reported.cause, RowsSkippedCause::FilterFailed);
        assert_eq!(reported.skipped_rows.get(), 2);
        assert_eq!(
            reported.message,
            "session subscription predicate failed: division by zero"
        );
        drop(frames);
        let gone = SkippedRows {
            cause: RowsSkippedCause::EncodingFailed,
            rows: NonZeroU64::MIN,
            message: "rows could not be encoded".to_string(),
        };
        assert!(
            !sender.report_skipped(gone).await,
            "a report for a session that is gone ends the delivery"
        );
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
        assert!(err.contains("expects numeric literal"));
    }

    #[test]
    fn subscription_batch_sample_rate_is_validated() {
        assert_eq!(parse_subscription_batch_sample_rate(None), Ok(None));
        assert_eq!(
            parse_subscription_batch_sample_rate(Some("0.25")),
            Ok(Some(0.25))
        );
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
    async fn session_subscriptions_track_names_generations_and_cleanup_tasks() {
        let mut subscriptions = SessionSubscriptions::new();
        let (outbound, _frames) = mpsc::channel(4);
        let events = crate::runtime::RelayBroadcast::with_capacity(
            std::num::NonZeroUsize::new(4).expect("test relay capacity must be nonzero"),
        );
        let config = task_config(
            &mut subscriptions,
            "live_events",
            events.new_receiver(),
            outbound.clone(),
        );
        let first_generation = config.handle.generation;
        subscriptions.insert(
            DomainName::parse("default").expect("valid domain"),
            named("events"),
            config,
        );
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
            .expect("subscription should be removed");
        assert_eq!(removed.domain.as_str(), "default");
        assert_eq!(removed.relay.as_str(), "events");
        assert_eq!(removed.handle.generation, first_generation);
        assert!(
            subscriptions
                .remove(&named("missing_events"))
                .await
                .is_none()
        );

        let reused = task_config(
            &mut subscriptions,
            "live_events",
            events.new_receiver(),
            outbound,
        );
        assert!(
            reused.handle.generation > first_generation,
            "a reused name opens with a new generation"
        );
    }

    #[tokio::test]
    #[ignore = "CLIENT-WIRE-10 makes relay-interest accounting exact across every removal path"]
    async fn deleting_two_same_relay_subscriptions_clears_interest() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let domain =
            DomainName::parse("default").assured("the test domain is an identifier-shaped literal");
        let relay: RelayName = named("events");
        let key = SubscriptionInterestKey {
            domain: domain.clone(),
            relay: relay.clone(),
        };
        service
            .inner
            .subscription_interest_counts
            .insert(key.clone(), 2);

        let mut subscriptions = SessionSubscriptions::new();
        let events = crate::runtime::RelayBroadcast::with_capacity(
            std::num::NonZeroUsize::new(4).assured("the test relay capacity is a nonzero literal"),
        );
        for name in ["first", "second"] {
            let (outbound, _frames) = mpsc::channel(4);
            let config = task_config(&mut subscriptions, name, events.new_receiver(), outbound);
            subscriptions.insert(domain.clone(), relay.clone(), config);
        }

        for name in ["first", "second"] {
            let deleted = service
                .delete_subscription(
                    nervix_models::DeleteSubscription { name: named(name) },
                    &mut subscriptions,
                )
                .await;
            assert!(deleted.is_ok(), "subscription '{name}' deletion failed");
        }
        let leaked = service
            .inner
            .subscription_interest_counts
            .contains_key(&key);
        drop(service);
        std::fs::remove_dir_all(path).discarded("the test directory is disposable");

        assert!(
            !leaked,
            "deleting the final subscription left relay interest behind"
        );
    }

    #[tokio::test]
    async fn rebuilt_relay_ends_the_subscription_with_a_typed_reason() {
        let mut subscriptions = SessionSubscriptions::new();
        let (outbound, mut frames) = mpsc::channel(4);
        let events = crate::runtime::RelayBroadcast::with_capacity(
            std::num::NonZeroUsize::new(4).expect("test relay capacity must be nonzero"),
        );
        let config = task_config(
            &mut subscriptions,
            "live_events",
            events.new_receiver(),
            outbound,
        );
        let handle = config.handle.clone();
        subscriptions.insert(
            DomainName::parse("default").expect("valid domain"),
            named("events"),
            config,
        );

        drop(events);
        let frame = tokio::time::timeout(Duration::from_secs(1), frames.recv())
            .await
            .expect("the subscription end should arrive")
            .expect("the session channel should remain open");
        let frame = VerifiedFrame::verify(frame.into_bytes(), &SessionLimits::DEFAULT)
            .assured("the server encodes frames the session limits admit");
        let ServerMessage::Event(ServerEvent::SubscriptionEnded(ended)) =
            ServerMessage::decode(&frame).assured("a server frame decodes")
        else {
            panic!("the relay close ends the subscription");
        };
        assert_eq!(ended.subscription, handle);
        assert_eq!(ended.reason, SubscriptionEndReason::RelayClosed);
        assert!(
            ended
                .message
                .contains("subscription 'live_events' was dropped")
        );
        assert!(ended.message.contains("recreate the subscription"));
        tokio::task::yield_now().await;
        assert!(!subscriptions.contains_name(&named("live_events")));

        subscriptions
            .remove(&named("live_events"))
            .await
            .expect("closed subscription metadata should remain removable");
    }
}
