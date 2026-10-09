//! The client: one session with a server, the statements it executes, and the transaction it
//! holds.
//!
//! Layer: edges.
//!
//! - **Owns.** Sending requests on the current exchange, the redirects, retries and reconnects a
//!   reply calls for, the session's selected domain and transaction binding, the statements the
//!   client serves itself, and installing a new exchange, on which it restores what it holds before
//!   it attaches its transaction.
//! - **Depends on.** The exchange dispatcher, the restoration of a new exchange, the connector, the
//!   wire contract, and the language layer for splitting and classifying statements.
//! - **Must not know.** How a frame is routed off an exchange.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    AttachDisposition, AttachDomainClockRequest, AttachTransactionRequest, ClientMessage,
    ClientRequest, CommandRequest, DetachDomainClockRequest, DomainClockAttachOutcome,
    DomainClockDetachOutcome, DomainInfo, InspectTransactionRequest, InspectionOutcome, Leadership,
    ReplyBody, SubscribeRequest, SubscriptionType, UnsubscribeRequest,
};
use nervix_models::{
    Backup, CommandExecutionReference, CreateSubscription, DomainName, ResourceUploadIdentity,
    Restore, Statement, SubscriptionName, TransactionInspectionTarget, TransactionOperationNumber,
    TransactionPosition, TransactionPreviewIdentity, TransactionStatus, UploadResource,
};
use nervix_nspl::client_statement::{ClientStatement, ParsedClientStatement};
use nervix_primitives::{
    sync::{Arc, Mutex, StdArc as SharedClientArc},
    time::{Instant, sleep},
};
use tonic::transport::Channel;
use url::Url;

#[cfg(feature = "autocomplete")]
use crate::events::{AutocompleteOutcome, AutocompleteSuggestion};
use crate::{
    connection::{
        ConnectOptions, EndpointValidationError, GrpcConnector, ServerDirectory, TlsRequirement,
    },
    domain_clock::{AttachedDomainClock, DomainClockEvent},
    error::{ClientError, EventStreamKind, RequestKind},
    events::{ServerEvent, SubscriptionEvent, SubscriptionRequest},
    exchange::{EventQueueError, Exchange, ExchangeRequests, SESSION_LIMITS, SessionEvents},
    outcome::{CommandOutcome, Routing},
    restoration::Restoration,
    subscriptions::{
        Cancellation, DeleteAttempt, DeletionResolution, DeletionTarget, DesiredSubscriptionEvent,
        RestoreAttempt, SubscriptionContract, SubscriptionLifecycle,
    },
};

/// What a command expects of the transaction it runs against.
///
/// A position fences an append to the queue it was written for; a preview fences a commit to the
/// transaction the caller actually read. Both travel together because a command carries at most
/// one of each and neither means anything without the attached transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TransactionExpectation {
    /// The accepted-operation position an append expects.
    pub(crate) position: Option<TransactionPosition>,
    /// The whole-transaction preview a COMMIT expects to apply.
    pub(crate) preview: Option<TransactionPreviewIdentity>,
}

/// One logical command's identity and inputs. Keep this handle to retry after a cancelled waiter
/// or an uncertain transport result; every attempt uses the same domain, transaction fence and
/// durable execution reference.
#[derive(Debug, Clone)]
pub struct ExecutionHandle {
    query: String,
    route: StatementRoute,
    reference: CommandExecutionReference,
    domain: Option<DomainName>,
    expectation: TransactionExpectation,
    upload_identity: ResourceUploadIdentity,
}

impl ExecutionHandle {
    pub fn reference(&self) -> &CommandExecutionReference {
        &self.reference
    }

    pub fn domain(&self) -> Option<&DomainName> {
        self.domain.as_ref()
    }

    pub fn upload_identity(&self) -> &ResourceUploadIdentity {
        &self.upload_identity
    }

    pub(crate) fn can_have_admitted_command(&self) -> bool {
        matches!(
            self.route,
            StatementRoute::Command | StatementRoute::Backup(_) | StatementRoute::Restore(_)
        )
    }
}

/// Whether a lost session was replaced by a working one.
pub(crate) enum SessionRecovery {
    Unavailable,
    Ready,
}

/// Whether recovery may reuse a session another request has already restored.
#[derive(Clone, Copy)]
pub(crate) enum RecoveryMode {
    IfClosed,
    Replace,
    TransportOnlyIfClosed,
}

/// How the client serves a query.
#[derive(Debug, Clone)]
enum StatementRoute {
    /// A statement the client answers itself.
    Local(LocalStatement),
    /// A CREATE SUBSCRIPTION statement, sent as a subscribe request carrying its exact source,
    /// which starts `offset` bytes into the query.
    Subscribe {
        create: CreateSubscription,
        statement: String,
        offset: usize,
    },
    /// A DELETE SUBSCRIPTION statement, sent as an unsubscribe request.
    Unsubscribe(SubscriptionName),
    /// A batch the client refuses to send, for the reason given.
    Refused(&'static str),
    /// Statements the server executes as one command.
    Command,
    /// A BACKUP statement: the server executes it as a command, and the client downloads the
    /// archive it assembled to the file the statement names.
    Backup(Backup),
    /// A RESTORE statement: the client streams the archive its statement names to the leader,
    /// which runs the restore as a command under the execution's reference.
    Restore(Restore),
}

/// A statement the client serves itself, without sending it as a command.
#[derive(Debug, Clone)]
enum LocalStatement {
    UseDomain(DomainName),
    ListDomains,
    /// Sent as an attach request for the active domain.
    AttachDomainClock,
    /// Sent as a detach request for the active domain.
    DetachDomainClock,
    UploadResource(UploadResource),
}

impl StatementRoute {
    fn wait_timeout(&self, connector: &GrpcConnector) -> Duration {
        match self {
            Self::Backup(_) => connector.backup_wait_timeout(),
            _ => connector.retry_timeout(),
        }
    }

    fn is_subscription(&self) -> bool {
        matches!(self, Self::Subscribe { .. } | Self::Unsubscribe(_))
    }

    /// Classifies the statements `query` parsed into.
    fn of(query: &str, statements: Vec<ParsedClientStatement>) -> Self {
        if statements.len() > 1 {
            let serves_locally = statements
                .iter()
                .any(|parsed| parsed.statement.requires_local_handling());
            if serves_locally {
                return Self::Refused("client-local commands must be executed separately");
            }
            let manages_subscriptions = statements.iter().any(|parsed| {
                matches!(
                    parsed.statement,
                    ClientStatement::CreateSubscription(_) | ClientStatement::DeleteSubscription(_)
                )
            });
            if manages_subscriptions {
                return Self::Refused("subscription commands must be executed separately");
            }
            return Self::Command;
        }
        let Some(parsed) = statements.into_iter().next() else {
            return Self::Command;
        };
        let statement = parsed.source(query).to_string();
        let offset = parsed.span.start;
        match parsed.statement {
            ClientStatement::UseDomain(domain) => Self::Local(LocalStatement::UseDomain(domain)),
            ClientStatement::ListDomains => Self::Local(LocalStatement::ListDomains),
            ClientStatement::AttachDomainClock => Self::Local(LocalStatement::AttachDomainClock),
            ClientStatement::DetachDomainClock => Self::Local(LocalStatement::DetachDomainClock),
            ClientStatement::UploadResource(upload) => {
                Self::Local(LocalStatement::UploadResource(upload))
            }
            ClientStatement::CreateSubscription(create) => Self::Subscribe {
                create,
                statement,
                offset,
            },
            ClientStatement::DeleteSubscription(subscription) => {
                Self::Unsubscribe(subscription.name)
            }
            ClientStatement::DescribeBackup(_) => Self::Refused(
                "DESCRIBE BACKUP reads an archive file on this machine and is served by nervix-cli",
            ),
            ClientStatement::Server(Statement::Backup(backup)) => Self::Backup(backup),
            ClientStatement::Server(Statement::Restore(restore)) => Self::Restore(restore),
            ClientStatement::BeginTransaction
            | ClientStatement::CommitTransaction
            | ClientStatement::RevertTransaction
            | ClientStatement::Server(_) => Self::Command,
        }
    }
}

pub(crate) struct ClientInner {
    pub(crate) domain: Mutex<Option<DomainName>>,
    pub(crate) servers: Mutex<ServerDirectory>,
    pub(crate) connector: GrpcConnector,
    pub(crate) exchange: Mutex<Exchange>,
    pub(crate) reconnect_lock: Mutex<()>,
    pub(crate) command_lock: Mutex<()>,
    pub(crate) transaction: Mutex<Option<TransactionStatus>>,
    /// Identified previews keyed by target transaction and accepted-operation position. A newer
    /// position replaces an older one; a stale refusal never updates this cache.
    pub(crate) previews: Mutex<BTreeMap<(String, TransactionPosition), TransactionPreviewIdentity>>,
    pub(crate) events: SessionEvents,
}

#[derive(Clone)]
pub struct Client {
    pub(crate) inner: SharedClientArc<ClientInner>,
}

impl Client {
    pub(crate) const MAX_LEADER_ROUTING_ATTEMPTS: usize = 1200;
    const LEADER_ELECTION_RETRY_DELAY: Duration = Duration::from_millis(100);
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(1);

    pub async fn connect(
        server: impl AsRef<str>,
        domain: Option<DomainName>,
    ) -> error_stack::Result<Self, ClientError> {
        Self::connect_with_options(server, domain, ConnectOptions::default()).await
    }

    pub async fn connect_with_options(
        server: impl AsRef<str>,
        domain: Option<DomainName>,
        mut options: ConnectOptions,
    ) -> error_stack::Result<Self, ClientError> {
        let server = Url::parse(server.as_ref())
            .map_err(|error| Report::new(ClientError::InvalidServerUrl(error)))?;
        const MAX_DEADLINE: Duration = Duration::from_secs(24 * 60 * 60);
        if options.seed_servers.len() > 32 {
            return Err(Report::new(ClientError::TooManySeedServers {
                count: options.seed_servers.len(),
            }));
        }
        for (field, value) in [
            ("connect_timeout", options.connect_timeout),
            ("request_timeout", options.request_timeout),
            ("retry_timeout", options.retry_timeout),
            ("backup_wait_timeout", options.backup_wait_timeout),
        ] {
            if value < Duration::from_millis(1) || value > MAX_DEADLINE {
                return Err(Report::new(ClientError::InvalidDeadline { field }));
            }
        }
        if server.scheme() == "https" {
            // A client that began over TLS cannot silently reconnect over plaintext, even when
            // the caller did not explicitly request TLS for every seed and redirect.
            options.tls_requirement = Some(TlsRequirement::Required);
        }
        let mut connector = GrpcConnector::new(options)
            .map_err(|error| Report::new(ClientError::BuildAuthenticationMetadata(error)))?;
        connector
            .validate_server(&server)
            .map_err(EndpointValidationError::into_client)?;
        for seed in connector.seed_servers() {
            connector
                .validate_server(seed)
                .map_err(EndpointValidationError::into_client)?;
        }
        connector.load_dns().await?;
        let mut servers = ServerDirectory::with_seeds(server, connector.seed_servers());
        let events = SessionEvents::new();
        let mut last_error = None;
        let deadline = Instant::now() + connector.retry_timeout();
        for candidate in servers.reconnect_candidates() {
            nervix_primitives::task::consume_budget().await;
            let attempt = async {
                let channel = connector.connect(&candidate).await?;
                Exchange::open(channel, &connector, events.sinks.clone()).await
            };
            match nervix_primitives::time::timeout_at(deadline, attempt).await {
                Err(_) => {
                    return Err(
                        last_error.unwrap_or_else(|| Report::new(ClientError::RetryDeadline))
                    );
                }
                Ok(Err(report)) => last_error = Some(report),
                Ok(Ok(exchange)) => {
                    servers.connected(&candidate);
                    return Ok(Self::assemble(exchange, events, connector, domain, servers));
                }
            }
        }
        Err(last_error.assured("the primary server is always one configured candidate"))
    }

    /// A client whose session starts on `channel`. It can follow advertised redirects and later
    /// reconnect to servers it has learned, but it has no initial server address or seed.
    pub async fn from_channel(
        channel: Channel,
        domain: Option<DomainName>,
    ) -> error_stack::Result<Self, ClientError> {
        let mut connector = GrpcConnector::new(ConnectOptions::default())
            .map_err(|error| Report::new(ClientError::BuildAuthenticationMetadata(error)))?;
        connector.load_dns().await?;
        let events = SessionEvents::new();
        let exchange = Exchange::open(channel, &connector, events.sinks.clone()).await?;
        Ok(Self::assemble(
            exchange,
            events,
            connector,
            domain,
            ServerDirectory::connected_to(None),
        ))
    }

    pub(crate) fn assemble(
        exchange: Exchange,
        events: SessionEvents,
        connector: GrpcConnector,
        domain: Option<DomainName>,
        servers: ServerDirectory,
    ) -> Self {
        Self {
            inner: SharedClientArc::new(ClientInner {
                domain: Mutex::new(domain),
                servers: Mutex::new(servers),
                connector,
                exchange: Mutex::new(exchange),
                reconnect_lock: Mutex::new(()),
                command_lock: Mutex::new(()),
                transaction: Mutex::new(None),
                previews: Mutex::new(BTreeMap::new()),
                events,
            }),
        }
    }

    pub async fn domain(&self) -> Option<DomainName> {
        self.inner.domain.lock().await.clone()
    }

    pub async fn set_domain(&self, domain: Option<DomainName>) {
        *self.inner.domain.lock().await = domain;
    }

    pub async fn transaction_status(&self) -> Option<TransactionStatus> {
        self.inner.transaction.lock().await.clone()
    }

    /// The leadership the serving node last observed, when it has reported any.
    pub fn leadership(&self) -> Option<Leadership> {
        let observed = self.inner.events.leadership.borrow();
        Option::clone(&observed)
    }

    /// Records the transaction the server reports and adopts the domain it is bound to. A session
    /// that attaches or reconnects must follow the transaction's domain, because `USE` is rejected
    /// while a transaction is active and every queued statement must select that domain.
    pub(crate) async fn adopt_transaction_status(&self, status: TransactionStatus) {
        self.set_domain(Some(status.domain().clone())).await;
        *self.inner.transaction.lock().await = Some(status);
    }

    async fn active_transaction_status(&self) -> Option<TransactionStatus> {
        self.inner
            .transaction
            .lock()
            .await
            .clone()
            .filter(|status| status.lifecycle().is_active())
    }

    pub async fn execute(
        &self,
        query: impl Into<String>,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let query = query.into();
        let route = Self::route(&query);
        let deadline = Instant::now() + route.wait_timeout(&self.inner.connector);
        if route.is_subscription() {
            let execution = self.prepare_execution_with_route(query, route).await;
            return self.execute_prepared_after_lock(&execution, deadline).await;
        }
        let _command_guard =
            nervix_primitives::time::timeout_at(deadline, self.inner.command_lock.lock())
                .await
                .map_err(|_| Report::new(ClientError::RetryDeadline))?;
        let execution = self.prepare_execution_with_route(query, route).await;
        self.execute_prepared_after_lock(&execution, deadline).await
    }

    /// Captures the exact identity and inputs before a command is awaited. A caller can retain
    /// the returned handle across cancellation and retry that one logical command explicitly.
    pub async fn prepare_execution(&self, query: impl Into<String>) -> ExecutionHandle {
        let query = query.into();
        let route = Self::route(&query);
        self.prepare_execution_with_route(query, route).await
    }

    /// Recovers a BACKUP under its existing durable reference. Keep the original selected domain,
    /// scope, resource inclusion and capture options; the local destination may change.
    pub async fn prepare_backup_with_reference(
        &self,
        backup: &Backup,
        reference: &CommandExecutionReference,
    ) -> ExecutionHandle {
        let mut execution = self
            .prepare_execution_with_route(
                backup.to_canonical_nspl(),
                StatementRoute::Backup(backup.clone()),
            )
            .await;
        execution.reference = reference.clone();
        execution
    }

    fn route(query: &str) -> StatementRoute {
        match nervix_nspl::client_statement::parse_client_statement_sources(query) {
            Ok(statements) => StatementRoute::of(query, statements),
            Err(_) => StatementRoute::Command,
        }
    }

    async fn prepare_execution_with_route(
        &self,
        query: String,
        route: StatementRoute,
    ) -> ExecutionHandle {
        let reference = CommandExecutionReference::parse(uuid::Uuid::now_v7().to_string()).assured(
            "a hyphenated UUID is 36 ASCII hex digits and hyphens, within the execution reference \
             grammar",
        );
        let expectation = self.transaction_expectation().await;
        ExecutionHandle {
            query,
            route,
            reference,
            domain: self.domain().await,
            expectation,
            upload_identity: ResourceUploadIdentity::parse(uuid::Uuid::now_v7().to_string())
                .assured("a UUID string satisfies the upload identity grammar"),
        }
    }

    pub async fn execute_prepared(
        &self,
        execution: &ExecutionHandle,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let deadline = Instant::now() + execution.route.wait_timeout(&self.inner.connector);
        if execution.route.is_subscription() {
            return self.execute_prepared_after_lock(execution, deadline).await;
        }
        let _command_guard =
            nervix_primitives::time::timeout_at(deadline, self.inner.command_lock.lock())
                .await
                .map_err(|_| Report::new(ClientError::RetryDeadline))?;
        self.execute_prepared_after_lock(execution, deadline).await
    }

    async fn execute_prepared_after_lock(
        &self,
        execution: &ExecutionHandle,
        deadline: Instant,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        if let StatementRoute::Restore(restore) = &execution.route {
            // A client bound to a transaction refuses a restore, as it refuses every client-local
            // statement.
            if self.active_transaction_status().await.is_some() {
                return Ok(CommandOutcome::failed_locally(
                    "client-local commands are not allowed while a transaction is active"
                        .to_string(),
                ));
            }
            // An archive may take far longer to send than the retry deadline allows a command, so
            // a restore bounds each frame and each reply instead.
            return self
                .restore_with_reference(restore, &execution.reference, |_| {})
                .await;
        }
        let result = nervix_primitives::time::timeout_at(
            deadline,
            self.execute_prepared_within_budget(execution),
        )
        .await;
        let outcome = match result {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(report))
                if execution.can_have_admitted_command()
                    && report.current_context().can_hide_admitted_work() =>
            {
                return Err(report.change_context(ClientError::UncertainCommand {
                    reference: execution.reference.clone(),
                }));
            }
            Ok(Err(report)) => return Err(report),
            Err(_) if execution.can_have_admitted_command() => {
                let expired = Report::new(ClientError::RetryDeadline);
                return Err(expired.change_context(ClientError::UncertainCommand {
                    reference: execution.reference.clone(),
                }));
            }
            Err(_) => return Err(Report::new(ClientError::RetryDeadline)),
        };
        match self.download_backup_of(execution, outcome).await {
            Ok(outcome) => Ok(outcome),
            Err(report) => Err(report.change_context(ClientError::BackupDownload {
                reference: execution.reference.clone(),
            })),
        }
    }

    /// Downloads the archive a completed BACKUP assembled to the file its statement names. The
    /// download runs after the command's deadline has done its work, because an archive may take
    /// far longer to transfer than a command to run; its own frames bound it instead.
    async fn download_backup_of(
        &self,
        execution: &ExecutionHandle,
        mut outcome: CommandOutcome,
    ) -> Result<CommandOutcome, Report<crate::backup::BackupDownloadError>> {
        let StatementRoute::Backup(backup) = &execution.route else {
            return Ok(outcome);
        };
        if !outcome.succeeded() {
            return Ok(outcome);
        }
        let Some(summary) = outcome.backup.as_deref() else {
            return Ok(outcome);
        };
        let destination = PathBuf::from(&backup.destination);
        self.fetch_backup(summary, &execution.reference, &destination)
            .await?;
        outcome.message = format!(
            "{}; archive written to '{}'",
            outcome.message, backup.destination
        );
        Ok(outcome)
    }

    async fn execute_prepared_within_budget(
        &self,
        execution: &ExecutionHandle,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let outcome = self.execute_with_redirects(execution).await?;
        self.record_commit_basis(&outcome).await;
        if let Some(transaction) = outcome.transaction.clone() {
            self.adopt_transaction_status(transaction).await;
        }
        Ok(outcome)
    }

    /// Updates the basis a later COMMIT fences against from what the server just reported.
    pub(crate) async fn record_commit_basis(&self, outcome: &CommandOutcome) {
        let Some(basis) = outcome.commit_basis() else {
            return;
        };
        self.record_preview(basis).await;
    }

    /// The command lock serializes replies at equal positions. A response for an older queue
    /// position cannot displace the newer identified preview for that transaction.
    async fn record_preview(&self, preview: TransactionPreviewIdentity) {
        let mut cached = self.inner.previews.lock().await;
        let first = (preview.transaction_id.clone(), TransactionPosition::new(0));
        let last = (
            preview.transaction_id.clone(),
            TransactionPosition::new(usize::MAX),
        );
        let previous = cached
            .range(first..=last)
            .next_back()
            .map(|(key, _)| key.clone());
        if let Some(previous) = &previous
            && previous.1 > preview.position
        {
            return;
        }
        if let Some(previous) = previous {
            cached.remove(&previous);
        }
        let key = (preview.transaction_id.clone(), preview.position);
        cached.insert(key, preview);
    }

    /// What this session expects of the transaction the next command runs against.
    ///
    /// The cached basis fences a commit only while it still names the attached transaction, so a
    /// basis obtained for a different transaction can never decide this one's commit.
    pub(crate) async fn transaction_expectation(&self) -> TransactionExpectation {
        let Some(status) = self.active_transaction_status().await else {
            return TransactionExpectation::default();
        };
        let key = (
            status.transaction_id().to_string(),
            status.accepted_operations(),
        );
        let preview = self.inner.previews.lock().await.get(&key).cloned();
        TransactionExpectation {
            position: Some(status.accepted_operations()),
            preview,
        }
    }

    pub async fn attach_transaction(
        &self,
        id: impl Into<String>,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let id = id.into();
        match nervix_primitives::time::timeout(self.inner.connector.retry_timeout(), async {
            let _command_guard = self.inner.command_lock.lock().await;
            let outcome = self.attach_with_redirects(&id).await?;
            let outcome = CommandOutcome::from(outcome);
            if let Some(transaction) = outcome.transaction.clone() {
                self.adopt_transaction_status(transaction).await;
            }
            Ok(outcome)
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(Report::new(ClientError::RetryDeadline)),
        }
    }

    pub async fn list_domains(&self) -> error_stack::Result<Vec<DomainInfo>, ClientError> {
        let body = self.request(ClientRequest::ListDomains, None, None).await?;
        match body {
            ReplyBody::DomainList(list) => Ok(list.domains),
            other => Err(Report::new(ClientError::unexpected_reply(
                RequestKind::ListDomains,
                other,
            ))),
        }
    }

    /// Reads a transaction's impact report without attaching or changing it, following the
    /// leader when the serving node is not the leader.
    pub async fn inspect_transaction(
        &self,
        target: TransactionInspectionTarget,
        operation: Option<TransactionOperationNumber>,
    ) -> error_stack::Result<InspectionOutcome, ClientError> {
        let inspected =
            nervix_primitives::time::timeout(self.inner.connector.retry_timeout(), async {
                let _command_guard = self.inner.command_lock.lock().await;
                for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
                    nervix_primitives::task::consume_budget().await;
                    let request = ClientRequest::InspectTransaction(InspectTransactionRequest {
                        target: target.clone(),
                        operation,
                    });
                    let body = match self.request(request, None, None).await {
                        Ok(body) => body,
                        Err(report) if report.current_context().retryable_session_failure() => {
                            match self.recover_session(RecoveryMode::IfClosed).await? {
                                SessionRecovery::Ready => continue,
                                SessionRecovery::Unavailable => return Err(report),
                            }
                        }
                        Err(report) => return Err(report),
                    };
                    let outcome = match body {
                        ReplyBody::Inspection(outcome) => outcome,
                        other => {
                            return Err(Report::new(ClientError::unexpected_reply(
                                RequestKind::InspectTransaction,
                                other,
                            )));
                        }
                    };
                    match Routing::for_inspection(&outcome) {
                        Routing::Redirect(leader) if Self::retries_remain(attempt) => {
                            self.follow_leader(leader).await?;
                        }
                        Routing::AwaitElection if Self::await_retry(attempt).await => {}
                        _ => {
                            if let InspectionOutcome::Inspected(inspection) = &outcome {
                                let preview = TransactionPreviewIdentity {
                                    transaction_id: inspection
                                        .transaction
                                        .transaction_id()
                                        .to_string(),
                                    position: inspection.report.position(),
                                    planning_basis: inspection.report.planning_basis(),
                                };
                                self.record_preview(preview).await;
                            }
                            return Ok(outcome);
                        }
                    }
                }
                // Only a session that closed again on the last attempt leaves the loop.
                Err(Report::new(ClientError::SessionClosed))
            })
            .await;
        match inspected {
            Ok(result) => result,
            Err(_) => Err(Report::new(ClientError::RetryDeadline)),
        }
    }

    async fn execute_with_redirects(
        &self,
        execution: &ExecutionHandle,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            nervix_primitives::task::consume_budget().await;
            let outcome = match self.execute_once(execution).await {
                Ok(outcome) => outcome,
                Err(report) if report.current_context().retryable_session_failure() => {
                    match self.recover_session(RecoveryMode::IfClosed).await? {
                        SessionRecovery::Ready => continue,
                        SessionRecovery::Unavailable => return Err(report),
                    }
                }
                Err(report) => return Err(report),
            };
            match outcome.routing() {
                Routing::Complete => return Ok(outcome),
                Routing::Detached if self.active_transaction_status().await.is_some() => {
                    self.restore_transaction_binding().await?;
                }
                Routing::Detached => return Ok(outcome),
                Routing::Redirect(leader) => self.follow_leader(leader).await?,
                Routing::AwaitElection | Routing::AwaitOutcome
                    if Self::await_retry(attempt).await => {}
                Routing::AwaitElection | Routing::AwaitOutcome => return Ok(outcome),
            }
        }
        self.execute_once(execution).await
    }

    /// Serves a query once: locally, as a subscription request, or as a command.
    async fn execute_once(
        &self,
        execution: &ExecutionHandle,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let query = execution.query.as_str();
        let route = &execution.route;
        if let StatementRoute::Local(_) = route
            && self.active_transaction_status().await.is_some()
        {
            return Ok(CommandOutcome::failed_locally(
                "client-local commands are not allowed while a transaction is active".to_string(),
            ));
        }
        match route {
            StatementRoute::Refused(reason) => {
                Ok(CommandOutcome::failed_locally(reason.to_string()))
            }
            StatementRoute::Local(LocalStatement::UseDomain(domain)) => {
                let message = format!("using domain '{}'", domain.as_str());
                self.set_domain(Some(domain.clone())).await;
                Ok(CommandOutcome::completed_locally(message))
            }
            StatementRoute::Local(LocalStatement::ListDomains) => {
                let domains = self.list_domains().await?;
                Ok(CommandOutcome::completed_locally(format_domain_list(
                    &domains,
                )))
            }
            StatementRoute::Local(LocalStatement::AttachDomainClock) => {
                let Some(domain) = execution.domain.clone() else {
                    return Err(Report::new(ClientError::NoActiveDomain));
                };
                let request = ClientRequest::AttachDomainClock(AttachDomainClockRequest { domain });
                match self.request(request, None, None).await? {
                    ReplyBody::DomainClockAttach(outcome) => Ok(CommandOutcome::from(outcome)),
                    other => Err(Report::new(ClientError::unexpected_reply(
                        RequestKind::AttachDomainClock,
                        other,
                    ))),
                }
            }
            StatementRoute::Local(LocalStatement::DetachDomainClock) => {
                let Some(domain) = execution.domain.clone() else {
                    return Err(Report::new(ClientError::NoActiveDomain));
                };
                let request = ClientRequest::DetachDomainClock(DetachDomainClockRequest { domain });
                match self.request(request, None, None).await? {
                    ReplyBody::DomainClockDetach(outcome) => Ok(CommandOutcome::from(outcome)),
                    other => Err(Report::new(ClientError::unexpected_reply(
                        RequestKind::DetachDomainClock,
                        other,
                    ))),
                }
            }
            StatementRoute::Local(LocalStatement::UploadResource(upload)) => {
                let Some(domain) = execution.domain.clone() else {
                    return Err(Report::new(ClientError::NoActiveDomain));
                };
                self.upload_resource_from_directory_with_identity(
                    upload.identifier.as_str(),
                    PathBuf::from(&upload.source_path),
                    domain,
                    execution.upload_identity.clone(),
                    |_| {},
                )
                .await
            }
            StatementRoute::Subscribe {
                create,
                statement,
                offset,
            } => {
                let Some(domain) = execution.domain.clone() else {
                    return Err(Report::new(ClientError::NoActiveDomain));
                };
                let contract = SubscriptionContract {
                    domain,
                    subscription_type: SubscriptionType::Row,
                    create: create.clone(),
                };
                let exchange = self.inner.exchange.lock().await;
                let generation = exchange.generation.clone();
                let requests = exchange.requests();
                drop(exchange);
                let Some(attempt) = self.inner.events.sinks.desired.begin(contract, generation)
                else {
                    return Ok(CommandOutcome::failed_locally(format!(
                        "subscription '{}' is already desired or awaiting deletion",
                        create.name.as_str()
                    )));
                };
                let client = self.clone();
                let statement = statement.clone();
                let offset = *offset;
                nervix_primitives::task::spawn(async move {
                    client
                        .create_subscription(attempt, requests, statement, offset)
                        .await
                })
                .await
                .map_err(|error| Report::new(ClientError::SubscriptionTask(error)))?
            }
            StatementRoute::Unsubscribe(subscription) => {
                let exchange = self.inner.exchange.lock().await;
                let requests = exchange.requests();
                let generation = exchange.generation.clone();
                drop(exchange);
                let cancellation = self
                    .inner
                    .events
                    .sinks
                    .desired
                    .cancel(subscription, generation);
                let attempt = match cancellation {
                    Cancellation::InFlight => {
                        return Ok(CommandOutcome::failed_locally(format!(
                            "subscription '{}' is already being deleted",
                            subscription.as_str()
                        )));
                    }
                    Cancellation::Closed => {
                        return Ok(Self::held_by_no_session(subscription));
                    }
                    Cancellation::Ended => {
                        return Ok(Self::ended_by_the_server(subscription));
                    }
                    Cancellation::Delete(attempt) => attempt,
                };
                let client = self.clone();
                nervix_primitives::task::spawn(async move {
                    client.delete_subscription(attempt, requests).await
                })
                .await
                .map_err(|error| Report::new(ClientError::SubscriptionTask(error)))?
            }
            StatementRoute::Restore(restore) => {
                self.run_restore(restore, &execution.reference, |_| {})
                    .await
            }
            StatementRoute::Command | StatementRoute::Backup(_) => {
                let request = ClientRequest::Command(CommandRequest {
                    query: query.to_string(),
                    domain: execution.domain.clone(),
                    execution_reference: execution.reference.clone(),
                    expected_transaction_position: execution.expectation.position,
                    expected_preview: execution.expectation.preview.clone(),
                });
                let wait = match &execution.route {
                    StatementRoute::Backup(_) => Some(self.inner.connector.backup_wait_timeout()),
                    _ => None,
                };
                match self.request(request, None, wait).await? {
                    ReplyBody::Command(outcome) => {
                        if outcome.execution_reference != execution.reference {
                            return Err(Report::new(ClientError::ExecutionReferenceMismatch {
                                expected: execution.reference.clone(),
                                received: outcome.execution_reference,
                            }));
                        }
                        Ok(CommandOutcome::from(*outcome))
                    }
                    other => Err(Report::new(ClientError::unexpected_reply(
                        RequestKind::Command,
                        other,
                    ))),
                }
            }
        }
    }

    /// The outcome of deleting a subscription that no open session holds.
    fn held_by_no_session(subscription: &SubscriptionName) -> CommandOutcome {
        CommandOutcome::completed_locally(format!(
            "subscription '{}' deleted; no open session held it",
            subscription.as_str()
        ))
    }

    /// The outcome of deleting a subscription whose generation the server ended.
    fn ended_by_the_server(subscription: &SubscriptionName) -> CommandOutcome {
        CommandOutcome::completed_locally(format!(
            "subscription '{}' deleted; the server had already ended it",
            subscription.as_str()
        ))
    }

    /// The outcome of deleting a subscription whose session ended, and the subscription with it.
    fn closed_with_session(subscription: &SubscriptionName) -> CommandOutcome {
        CommandOutcome::completed_locally(format!(
            "subscription '{}' closed with its session",
            subscription.as_str()
        ))
    }

    /// Completes a registered creation even if its caller stops waiting. The contract, ticket and
    /// exchange generation decide whether the acknowledgement can make it desired.
    async fn create_subscription(
        &self,
        attempt: RestoreAttempt,
        exchange: Arc<ExchangeRequests>,
        statement: String,
        offset: usize,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let request = ClientRequest::Subscribe(SubscribeRequest {
            domain: attempt.contract.domain.clone(),
            statement,
            subscription_type: attempt.contract.subscription_type,
        });
        let response = self.request(request, Some(exchange), None).await;
        let outcome = match response {
            Ok(ReplyBody::Subscribe(outcome)) => outcome,
            Ok(other) => {
                self.inner.events.sinks.desired.created(&attempt, None);
                return Err(Report::new(ClientError::unexpected_reply(
                    RequestKind::Subscribe,
                    other,
                )));
            }
            Err(report) => {
                self.inner.events.sinks.desired.created(&attempt, None);
                return Err(report);
            }
        };
        let opened = match &outcome.disposition {
            nervix_client_wire::SubscribeDisposition::Opened(opened) => Some(&opened.subscription),
            nervix_client_wire::SubscribeDisposition::Failed => None,
        };
        self.inner.events.sinks.desired.created(&attempt, opened);
        let mut outcome = CommandOutcome::from(outcome);
        outcome.locate_diagnostics_in_query(offset);
        Ok(outcome)
    }

    /// Completes deletion even when its caller is cancelled. Waiting for an in-flight creation
    /// keeps the exchange reader draining, and avoids deleting before a late success is known.
    ///
    /// A subscription whose session ends before the deletion is answered ended with it, so that
    /// deletion is complete. A name the client never held is asked about on a new session.
    async fn delete_subscription(
        &self,
        attempt: DeleteAttempt,
        exchange: Arc<ExchangeRequests>,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let desired = &self.inner.events.sinks.desired;
        let mut changed = desired.watch();
        while desired.deletion_waits(&attempt) {
            nervix_primitives::task::consume_budget().await;
            changed
                .changed()
                .await
                .assured("the client holds the sender of its own subscription notifications");
        }
        match desired.deletion_target(&attempt) {
            DeletionTarget::SessionEnded => return Ok(Self::closed_with_session(&attempt.name)),
            DeletionTarget::NotOpened => return Ok(Self::held_by_no_session(&attempt.name)),
            DeletionTarget::Server => {}
        }
        let request = ClientRequest::Unsubscribe(UnsubscribeRequest {
            subscription: attempt.name.clone(),
        });
        let response = self.request(request, Some(exchange), None).await;
        match response {
            Ok(ReplyBody::Unsubscribe(outcome)) => {
                let outcome = CommandOutcome::from(outcome);
                let resolution = if outcome.succeeded() {
                    DeletionResolution::Deleted
                } else {
                    DeletionResolution::Refused
                };
                desired.deleted(&attempt, resolution);
                Ok(outcome)
            }
            Ok(other) => {
                desired.deleted(&attempt, DeletionResolution::Refused);
                Err(Report::new(ClientError::unexpected_reply(
                    RequestKind::Unsubscribe,
                    other,
                )))
            }
            Err(report) if report.current_context().retryable_session_failure() => {
                desired.deleted(&attempt, DeletionResolution::SessionEnded);
                if attempt.tracked {
                    return Ok(Self::closed_with_session(&attempt.name));
                }
                Err(report)
            }
            Err(report) => {
                desired.deleted(&attempt, DeletionResolution::Refused);
                Err(report)
            }
        }
    }

    async fn attach_with_redirects(
        &self,
        transaction_id: &str,
    ) -> error_stack::Result<nervix_client_wire::AttachOutcome, ClientError> {
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            nervix_primitives::task::consume_budget().await;
            let request = ClientRequest::AttachTransaction(AttachTransactionRequest {
                transaction_id: transaction_id.to_string(),
            });
            let body = match self.request(request, None, None).await {
                Ok(body) => body,
                Err(report) if report.current_context().retryable_session_failure() => {
                    match Box::pin(self.recover_session(RecoveryMode::TransportOnlyIfClosed))
                        .await?
                    {
                        SessionRecovery::Ready => continue,
                        SessionRecovery::Unavailable => return Err(report),
                    }
                }
                Err(report) => return Err(report),
            };
            let outcome = match body {
                ReplyBody::Attach(outcome) => outcome,
                other => {
                    return Err(Report::new(ClientError::unexpected_reply(
                        RequestKind::AttachTransaction,
                        other,
                    )));
                }
            };
            match Routing::for_attach(&outcome) {
                Routing::Redirect(leader) if Self::retries_remain(attempt) => {
                    self.inner
                        .connector
                        .validate_server(leader)
                        .map_err(EndpointValidationError::into_client)?;
                    self.inner.servers.lock().await.remember(leader);
                    self.reconnect(leader).await?;
                }
                Routing::AwaitElection if Self::await_retry(attempt).await => {}
                _ => return Ok(outcome),
            }
        }
        // Only a session that closed again on the last attempt leaves the loop.
        Err(Report::new(ClientError::SessionClosed))
    }

    /// Attaches the session's active transaction again on the node now serving it.
    async fn restore_transaction_binding(&self) -> error_stack::Result<(), ClientError> {
        let Some(previous) = self.active_transaction_status().await else {
            return Ok(());
        };
        let outcome = self
            .attach_with_redirects(previous.transaction_id())
            .await?;
        match outcome.disposition {
            AttachDisposition::Attached(status) | AttachDisposition::AlreadyFinished(status) => {
                self.adopt_transaction_status(status).await;
                Ok(())
            }
            AttachDisposition::Failed | AttachDisposition::NotLeader(_) => {
                *self.inner.transaction.lock().await = None;
                Err(Report::new(ClientError::AttachTransaction(outcome.message)))
            }
        }
    }

    /// Sends a request and waits for the reply that names it. A captured exchange keeps a
    /// subscription attempt on its registered generation. Read-only requests on the current
    /// exchange may reopen a lost session and be replayed within one deadline.
    async fn request(
        &self,
        request: ClientRequest,
        captured: Option<Arc<ExchangeRequests>>,
        timeout: Option<Duration>,
    ) -> error_stack::Result<ReplyBody, ClientError> {
        let kind = RequestKind::from(&request);
        let read_only = matches!(
            kind,
            RequestKind::ListDomains | RequestKind::Suggest | RequestKind::Choice
        );
        let deadline = Instant::now() + self.inner.connector.retry_timeout();
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            nervix_primitives::task::consume_budget().await;
            let exchange = match &captured {
                Some(exchange) => exchange.clone(),
                None => self.inner.exchange.lock().await.requests(),
            };
            let sent = nervix_primitives::time::timeout(
                timeout.unwrap_or(self.inner.connector.request_timeout()),
                async {
                    // Register before sending so a prompt reply always finds its waiter.
                    let Some(mut registered) = exchange.register() else {
                        return Err(Report::new(exchange.pending.lock().failure()));
                    };
                    let message = ClientMessage {
                        request_id: registered.request_id,
                        request: request.clone(),
                    };
                    let frame = message
                        .encode(&SESSION_LIMITS)
                        .change_context(ClientError::EncodeRequest { request: kind })?;
                    if exchange.frames.send(frame).await.is_err() {
                        exchange.pending.lock().close();
                        return Err(Report::new(exchange.pending.lock().failure()));
                    }
                    match registered.receive().await {
                        Some(body) => Ok(body),
                        None => match exchange.pending.lock().failure() {
                            ClientError::SessionClosed => {
                                Err(Report::new(ClientError::RequestInterrupted {
                                    request: kind,
                                }))
                            }
                            failure => Err(Report::new(failure)),
                        },
                    }
                },
            );
            let sent = if read_only {
                nervix_primitives::time::timeout_at(deadline, sent)
                    .await
                    .map_err(|_| Report::new(ClientError::RetryDeadline))?
            } else {
                sent.await
            };
            let sent = match sent {
                Ok(result) => result,
                Err(_) => {
                    exchange.pending.lock().close();
                    Err(Report::new(ClientError::RequestDeadline { request: kind }))
                }
            };
            if !read_only {
                return sent;
            }
            match sent {
                Ok(body) => return Ok(body),
                Err(report) if report.current_context().retryable_session_failure() => {
                    let recovered = nervix_primitives::time::timeout_at(
                        deadline,
                        Box::pin(self.recover_session(RecoveryMode::IfClosed)),
                    )
                    .await
                    .map_err(|_| Report::new(ClientError::RetryDeadline))??;
                    if let SessionRecovery::Unavailable = recovered {
                        return Err(report);
                    }
                    let allowed =
                        nervix_primitives::time::timeout_at(deadline, Self::await_retry(attempt))
                            .await
                            .map_err(|_| Report::new(ClientError::RetryDeadline))?;
                    if !allowed {
                        return Err(report);
                    }
                }
                Err(report) => return Err(report),
            }
        }
        Err(Report::new(ClientError::RetryDeadline))
    }

    pub async fn subscribe(
        &self,
        request: &SubscriptionRequest,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        self.execute(request.to_query()).await
    }

    pub async fn unsubscribe(
        &self,
        name: &str,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        self.execute(nervix_nspl::subscribe::delete_subscription_query(name))
            .await
    }

    /// The lifecycle currently retained for a desired subscription name. A subscription the server
    /// ended reads [`SubscriptionLifecycle::Ended`] until it is subscribed again under its name or
    /// deleted.
    pub fn subscription_lifecycle(&self, name: &SubscriptionName) -> Option<SubscriptionLifecycle> {
        self.inner.events.sinks.desired.lifecycle(name)
    }

    /// Waits for the next event of a subscription the client holds.
    ///
    /// The stream outlives the session. When the session ends, it reports
    /// [`SubscriptionEvent::Interrupted`] for every subscription the session held that the server
    /// had not ended, opens a new session, and opens each of them again as a new generation. An
    /// opening that session refuses or leaves unanswered is reported as
    /// [`SubscriptionEvent::RestorationFailed`] and sent again after a growing wait. A subscription
    /// the server ended is never opened again, and its [`SubscriptionEvent::Ended`] is reported
    /// once, after the events before it, even when its session ended before it was read. With
    /// nothing to restore it waits for the next session the client opens, and delivers the events
    /// of the subscriptions opened there. A failure to open a session is returned, and the next
    /// call tries again; [`ClientError::SessionClosed`] means the session ended and the client
    /// knows no server to open another on.
    pub async fn next_subscription(&self) -> error_stack::Result<SubscriptionEvent, ClientError> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut desired_changed = match self.inner.events.sinks.desired.next_event_or_changes()
            {
                DesiredSubscriptionEvent::Event(event) => return Ok(event),
                DesiredSubscriptionEvent::Waiting(changes) => changes,
            };
            let result = nervix_primitives::select! {
                result = self.inner.events.sinks.subscriptions.next() => result,
                changed = desired_changed.changed() => {
                    changed.assured(
                        "the client holds the sender of its own subscription notifications",
                    );
                    continue;
                }
            };
            match result {
                Ok(event) => {
                    let generation = self.inner.exchange.lock().await.generation.clone();
                    if self.inner.events.sinks.desired.admit(&event, &generation) {
                        return Ok(event);
                    }
                }
                Err(report) if *report.current_context() == EventQueueError::Overflow => {
                    return Err(report.change_context(ClientError::EventOverflow {
                        stream: EventStreamKind::Subscription,
                    }));
                }
                Err(_) => {
                    if let Some(event) = self.inner.events.sinks.desired.take_event() {
                        return Ok(event);
                    }
                    if self.inner.events.sinks.desired.has_acknowledged_desired() {
                        match self.recover_session(RecoveryMode::IfClosed).await? {
                            SessionRecovery::Ready => continue,
                            SessionRecovery::Unavailable => {
                                return Err(Report::new(ClientError::SessionClosed));
                            }
                        }
                    }
                    // Nothing waits to be restored, so the stream follows whichever session the
                    // client opens next.
                    if !self.can_reconnect().await {
                        return Err(Report::new(ClientError::SessionClosed));
                    }
                    nervix_primitives::select! {
                        () = self.inner.events.sinks.subscriptions.resumed() => {}
                        changed = desired_changed.changed() => {
                            changed.assured(
                                "the client holds the sender of its own subscription notifications",
                            );
                        }
                    }
                }
            }
        }
    }

    /// Waits for the next server notice.
    ///
    /// The stream outlives the session. Notices end with the session that delivered them,
    /// including the ones not read yet, and the stream continues with the notices of the next
    /// session the client opens; reading notices never opens a session itself.
    /// [`ClientError::EventOverflow`] reports that notices arrived faster than they were read and
    /// the ones the client held were dropped; the next call returns the notices that arrived after
    /// that gap. [`ClientError::SessionClosed`] means the session ended and the client knows no
    /// server to open another on.
    pub async fn next_server_event(&self) -> error_stack::Result<ServerEvent, ClientError> {
        let notices = &self.inner.events.sinks.notices;
        loop {
            nervix_primitives::task::consume_budget().await;
            match notices.next().await {
                Ok(event) => return Ok(event),
                Err(report) if *report.current_context() == EventQueueError::Overflow => {
                    return Err(report.change_context(ClientError::EventOverflow {
                        stream: EventStreamKind::ServerNotice,
                    }));
                }
                Err(_) => {}
            }
            if !self.can_reconnect().await {
                return Err(Report::new(ClientError::SessionClosed));
            }
            notices.resumed().await;
        }
    }

    /// Whether the client knows a server to open a new session on.
    async fn can_reconnect(&self) -> bool {
        self.inner.servers.lock().await.can_reconnect()
    }

    /// Attaches the session to the clock of `domain`, which it follows until it detaches, even
    /// across reconnects.
    ///
    /// An attached outcome carries the clock as the serving node has it installed.
    /// [`Client::domain_clock`] answers from the latest clock from then on, and
    /// [`Client::next_domain_clock_event`] reports state changes and accepted ticks after the reply.
    pub async fn attach_domain_clock(
        &self,
        domain: DomainName,
    ) -> error_stack::Result<DomainClockAttachOutcome, ClientError> {
        let request = ClientRequest::AttachDomainClock(AttachDomainClockRequest { domain });
        match self.clock_request(request).await? {
            ReplyBody::DomainClockAttach(outcome) => Ok(outcome),
            other => Err(Report::new(ClientError::unexpected_reply(
                RequestKind::AttachDomainClock,
                other,
            ))),
        }
    }

    /// Detaches the session from the clock of `domain`. Nothing about the domain's clock follows
    /// the reply.
    pub async fn detach_domain_clock(
        &self,
        domain: DomainName,
    ) -> error_stack::Result<DomainClockDetachOutcome, ClientError> {
        let request = ClientRequest::DetachDomainClock(DetachDomainClockRequest { domain });
        match self.clock_request(request).await? {
            ReplyBody::DomainClockDetach(outcome) => Ok(outcome),
            other => Err(Report::new(ClientError::unexpected_reply(
                RequestKind::DetachDomainClock,
                other,
            ))),
        }
    }

    /// The latest clock of a domain the session follows, with the arithmetic that projects it.
    /// `None` when the session does not follow the domain's clock.
    pub fn domain_clock(&self, domain: &DomainName) -> Option<AttachedDomainClock> {
        self.inner.events.sinks.clocks.latest(domain)
    }

    /// Waits for the next event about the domain clocks the session follows.
    ///
    /// Events are coalesced per domain, so a caller that reads late receives the newest state and
    /// tick, with the state first, rather than every intermediate observation. When the session
    /// holding an attachment ends, this reopens a session and attaches every followed clock again.
    /// An attach that session refuses or leaves unanswered is reported as
    /// [`DomainClockEvent::RestorationFailed`] and sent again after a growing wait. A failure to open
    /// a session is returned while the clocks keep waiting to be restored, and the next call tries
    /// again.
    /// A client that follows no clock waits until it attaches to one.
    pub async fn next_domain_clock_event(
        &self,
    ) -> error_stack::Result<DomainClockEvent, ClientError> {
        let clocks = &self.inner.events.sinks.clocks;
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut changed = clocks.watch();
            if let Some(event) = clocks.take_event() {
                return Ok(event);
            }
            if clocks.awaits_restoration() {
                let exchange = self.inner.exchange.lock().await.requests();
                let open = exchange.pending.lock().is_open();
                if !open {
                    let recovered = self.recover_session(RecoveryMode::IfClosed).await?;
                    if let SessionRecovery::Unavailable = recovered {
                        return Err(Report::new(ClientError::SessionClosed));
                    }
                    continue;
                }
            }
            changed
                .changed()
                .await
                .assured("the client holds the sender of its own clock notifications");
        }
    }

    /// Sends a domain clock request, reopening a lost session and sending the request again
    /// within the retry deadline.
    async fn clock_request(
        &self,
        request: ClientRequest,
    ) -> error_stack::Result<ReplyBody, ClientError> {
        let answered =
            nervix_primitives::time::timeout(self.inner.connector.retry_timeout(), async {
                for _ in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
                    nervix_primitives::task::consume_budget().await;
                    match self.request(request.clone(), None, None).await {
                        Ok(body) => return Ok(body),
                        Err(report) if report.current_context().retryable_session_failure() => {
                            match self.recover_session(RecoveryMode::IfClosed).await? {
                                SessionRecovery::Ready => {}
                                SessionRecovery::Unavailable => return Err(report),
                            }
                        }
                        Err(report) => return Err(report),
                    }
                }
                // Only a session that closed again on the last attempt leaves the loop.
                Err(Report::new(ClientError::SessionClosed))
            })
            .await;
        match answered {
            Ok(result) => result,
            Err(_) => Err(Report::new(ClientError::RetryDeadline)),
        }
    }

    /// Waits for the next complete domain list the server observes. Only the latest list is kept,
    /// so a caller that reads late gets the current list rather than every list in between.
    pub async fn next_domain_list(&self) -> error_stack::Result<Vec<DomainInfo>, ClientError> {
        let mut observed = self.inner.events.domains.lock().await;
        loop {
            nervix_primitives::task::consume_budget().await;
            if observed.changed().await.is_err() {
                return Err(Report::new(ClientError::SessionClosed));
            }
            let latest = Option::clone(&observed.borrow_and_update());
            if let Some(domains) = latest {
                return Ok(domains);
            }
        }
    }

    #[cfg(feature = "autocomplete")]
    pub async fn suggest(
        &self,
        input: impl Into<String>,
        cursor: usize,
        page_size: u16,
        continuation: Option<String>,
    ) -> error_stack::Result<AutocompleteOutcome, ClientError> {
        let input = input.into();
        let length = input.len();
        let domain = self.domain().await;
        let request = nervix_client_wire::SuggestRequest::new(input, cursor, domain)
            .change_context(ClientError::InvalidCursor { cursor, length })?;
        let request = request
            .with_page(page_size, continuation)
            .change_context(ClientError::InvalidCompletionPageSize { size: page_size })?;
        match self
            .request(ClientRequest::Suggest(request), None, None)
            .await?
        {
            ReplyBody::Suggest(outcome) => Ok(AutocompleteOutcome {
                status: outcome.status,
                continuation: outcome.continuation,
                suggestions: outcome
                    .suggestions
                    .into_iter()
                    .map(AutocompleteSuggestion::from)
                    .collect(),
            }),
            other => Err(Report::new(ClientError::unexpected_reply(
                RequestKind::Suggest,
                other,
            ))),
        }
    }

    /// Resolves one structured form control into typed, revision-fenced choices.
    pub async fn lookup_choices(
        &self,
        request: nervix_client_wire::ChoiceLookupRequest,
    ) -> error_stack::Result<nervix_client_wire::ChoiceOutcome, ClientError> {
        match self
            .request(ClientRequest::Choice(request), None, None)
            .await?
        {
            ReplyBody::Choice(outcome) => Ok(outcome),
            other => Err(Report::new(ClientError::unexpected_reply(
                RequestKind::Choice,
                other,
            ))),
        }
    }

    /// The channel the current exchange runs on.
    pub(crate) async fn current_channel(&self) -> Channel {
        self.inner.exchange.lock().await.channel.clone()
    }

    /// Whether a routed request may be sent again after `attempt`, counting from zero.
    pub(crate) fn retries_remain(attempt: usize) -> bool {
        let Some(next) = attempt.checked_add(1) else {
            return false;
        };
        next < Self::MAX_LEADER_ROUTING_ATTEMPTS
    }

    /// Waits before the request is sent again, and says whether it may be.
    pub(crate) async fn await_retry(attempt: usize) -> bool {
        if !Self::retries_remain(attempt) {
            return false;
        }
        let multiplier = 1_u32 << attempt.min(4);
        let delay = Self::LEADER_ELECTION_RETRY_DELAY
            .checked_mul(multiplier)
            .assured("a retry multiplier of at most sixteen keeps 100 ms below two seconds");
        sleep(delay.min(Self::MAX_RETRY_DELAY)).await;
        true
    }

    /// Moves the session to the leader at `leader`, so the request that needs it can be sent
    /// again there.
    pub(crate) async fn follow_leader(&self, leader: &Url) -> error_stack::Result<(), ClientError> {
        self.inner
            .connector
            .validate_server(leader)
            .map_err(EndpointValidationError::into_client)?;
        self.inner.servers.lock().await.remember(leader);
        match self.reconnect(leader).await {
            Ok(()) => self.restore_transaction_binding().await,
            Err(report) => match self.recover_session(RecoveryMode::Replace).await? {
                SessionRecovery::Ready => Ok(()),
                SessionRecovery::Unavailable => Err(report),
            },
        }
    }

    /// Replaces the exchange with a new one on `server`. Every request still waiting on the
    /// previous exchange observes the closed session.
    async fn reconnect(&self, server: &Url) -> error_stack::Result<(), ClientError> {
        let _reconnect_guard = self.inner.reconnect_lock.lock().await;
        self.reconnect_unlocked(server).await
    }

    async fn reconnect_unlocked(&self, server: &Url) -> error_stack::Result<(), ClientError> {
        let channel = self.inner.connector.connect(server).await?;
        let exchange = Exchange::open(
            channel,
            &self.inner.connector,
            self.inner.events.sinks.clone(),
        )
        .await?;
        self.inner.servers.lock().await.connected(server);
        self.install(exchange).await;
        Ok(())
    }

    /// Makes `exchange` the client's exchange, restores on it what the client holds, and ends
    /// the exchange it replaces.
    ///
    /// Every restoration request is sent before this returns, so a transaction the caller
    /// attaches afterwards follows them: a session that holds a transaction refuses both.
    pub(crate) async fn install(&self, exchange: Exchange) {
        let mut restoration = Restoration::new(&exchange, self.inner.connector.request_timeout());
        restoration.attach_followed_clocks().await;
        let previous = std::mem::replace(&mut *self.inner.exchange.lock().await, exchange);
        previous.close().await;
        restoration.attach_interrupted_clocks().await;
        restoration.open_subscriptions().await;
        restoration.open_producers().await;
        restoration.open_consumers().await;
        restoration.follow();
    }

    /// Reuses the session recovery owner when an endpoint moved while its exchange stayed open.
    /// The reconnect lock serializes this with installing a replacement exchange; each desired
    /// handle starts at most one open on the chosen exchange.
    pub(crate) async fn restore_interrupted_endpoints(&self) {
        let _reconnect_guard = self.inner.reconnect_lock.lock().await;
        let mut restoration = {
            let exchange = self.inner.exchange.lock().await;
            Restoration::new(&exchange, self.inner.connector.request_timeout())
        };
        restoration.open_producers().await;
        restoration.open_consumers().await;
        restoration.follow();
    }

    /// Reconnects a lost session and attaches its transaction again.
    pub(crate) async fn recover_session(
        &self,
        mode: RecoveryMode,
    ) -> error_stack::Result<SessionRecovery, ClientError> {
        let recovered = 'recovery: {
            let _reconnect_guard = self.inner.reconnect_lock.lock().await;
            let exchange = self.inner.exchange.lock().await.requests();
            if !matches!(mode, RecoveryMode::Replace) && exchange.pending.lock().is_open() {
                break 'recovery SessionRecovery::Ready;
            }
            if let Some(Leadership::Remote(endpoints)) = self.leadership()
                && let Some(endpoint) = endpoints.grpc_uri
                && self.inner.connector.validate_server(&endpoint).is_ok()
            {
                self.inner.servers.lock().await.remember(&endpoint);
            }
            let deadline = Instant::now() + self.inner.connector.retry_timeout();
            let mut delay = Self::LEADER_ELECTION_RETRY_DELAY;
            let mut last_error = None;
            loop {
                nervix_primitives::task::consume_budget().await;
                let candidates = self.inner.servers.lock().await.reconnect_candidates();
                if candidates.is_empty() {
                    break 'recovery SessionRecovery::Unavailable;
                }
                for server in candidates {
                    nervix_primitives::task::consume_budget().await;
                    match nervix_primitives::time::timeout_at(
                        deadline,
                        self.reconnect_unlocked(&server),
                    )
                    .await
                    {
                        Ok(Ok(())) => break 'recovery SessionRecovery::Ready,
                        Ok(Err(report)) => last_error = Some(report),
                        Err(_) => {
                            return Err(last_error
                                .unwrap_or_else(|| Report::new(ClientError::RetryDeadline)));
                        }
                    }
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(
                        last_error.unwrap_or_else(|| Report::new(ClientError::RetryDeadline))
                    );
                }
                sleep(delay.min(remaining)).await;
                delay = delay
                    .checked_mul(2)
                    .assured("a delay capped at one second doubles within Duration's range")
                    .min(Self::MAX_RETRY_DELAY);
            }
        };
        let SessionRecovery::Ready = recovered else {
            return Ok(SessionRecovery::Unavailable);
        };
        if !matches!(mode, RecoveryMode::TransportOnlyIfClosed) {
            self.restore_transaction_binding().await?;
        }
        Ok(SessionRecovery::Ready)
    }
}

/// The text `LIST DOMAINS` answers with.
fn format_domain_list(domains: &[DomainInfo]) -> String {
    if domains.is_empty() {
        return "no domains registered".to_string();
    }
    let mut lines = Vec::with_capacity(domains.len());
    lines.push("domains:".to_string());
    for domain in domains {
        lines.push(format!(
            "{} pace={} status={} start_version={}",
            domain.domain,
            domain.pace.as_ref(),
            domain.status.as_ref(),
            domain.start_version,
        ));
    }
    lines.join("\n")
}
