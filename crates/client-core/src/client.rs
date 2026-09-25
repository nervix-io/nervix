//! The client: one session with a server, the statements it executes, and the transaction it
//! holds.
//!
//! - **Owns.** Sending requests on the current exchange, the redirects, retries and reconnects a
//!   reply calls for, the session's selected domain and transaction binding, and the statements the
//!   client serves itself.
//! - **Depends on.** The exchange dispatcher, the connector, the wire contract, and the language
//!   layer for splitting and classifying statements.
//! - **Must not know.** How a frame is routed off an exchange.

use std::{path::PathBuf, sync::Arc as SharedClientArc, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    AttachDisposition, AttachTransactionRequest, ClientMessage, ClientRequest, CommandRequest,
    DomainInfo, InspectTransactionRequest, InspectionOutcome, Leadership, ReplyBody,
    SubscribeRequest, SubscriptionType, UnsubscribeRequest,
};
use nervix_models::{
    CommandExecutionReference, CreateSubscription, DomainName, ResourceUploadIdentity,
    SubscriptionName, TransactionInspectionTarget, TransactionOperationNumber, TransactionPosition,
    TransactionPreviewIdentity, TransactionStatus, UploadResource,
};
use nervix_nspl::client_statement::{ClientStatement, ParsedClientStatement};
use tokio::{
    sync::Mutex,
    time::{Instant, sleep},
};
use tonic::transport::Channel;
use triomphe::Arc;
use url::Url;

#[cfg(feature = "autocomplete")]
use crate::events::AutocompleteSuggestion;
use crate::{
    connection::{ConnectOptions, GrpcConnector, ServerDirectory, TlsRequirement},
    error::{ClientError, EventStreamKind, RequestKind},
    events::{ServerEvent, SubscriptionEvent, SubscriptionRequest},
    exchange::{EventQueueError, Exchange, ExchangeRequests, SESSION_LIMITS, SessionEvents},
    outcome::{CommandOutcome, Routing},
    subscriptions::{DeleteAttempt, RestoreAttempt, SubscriptionContract, SubscriptionLifecycle},
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
        matches!(self.route, StatementRoute::Command)
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
}

/// A statement the client serves without sending it.
#[derive(Debug, Clone)]
enum LocalStatement {
    UseDomain(DomainName),
    ListDomains,
    UploadResource(UploadResource),
}

impl StatementRoute {
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
    /// The preview the attached transaction's last accepted append made current. A COMMIT sends
    /// it so the server can refuse a transaction that moved since this session last read it.
    pub(crate) commit_basis: Mutex<Option<TransactionPreviewIdentity>>,
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
        Self::connect_with_options(server, domain, ConnectOptions::default())
            .await
            .map_err(error_stack::Report::new)
    }

    pub async fn connect_with_options(
        server: impl AsRef<str>,
        domain: Option<DomainName>,
        mut options: ConnectOptions,
    ) -> Result<Self, ClientError> {
        let server = Url::parse(server.as_ref()).map_err(ClientError::InvalidServerUrl)?;
        const MAX_DEADLINE: Duration = Duration::from_secs(24 * 60 * 60);
        if options.seed_servers.len() > 32 {
            return Err(ClientError::TooManySeedServers {
                count: options.seed_servers.len(),
            });
        }
        for (field, value) in [
            ("connect_timeout", options.connect_timeout),
            ("request_timeout", options.request_timeout),
            ("retry_timeout", options.retry_timeout),
        ] {
            if value < Duration::from_millis(1) || value > MAX_DEADLINE {
                return Err(ClientError::InvalidDeadline { field });
            }
        }
        if server.scheme() == "https" {
            // A client that began over TLS cannot silently reconnect over plaintext, even when
            // the caller did not explicitly request TLS for every seed and redirect.
            options.tls_requirement = Some(TlsRequirement::Required);
        }
        let connector =
            GrpcConnector::new(options).map_err(ClientError::BuildAuthenticationMetadata)?;
        connector.validate_server(&server)?;
        for seed in connector.seed_servers() {
            connector.validate_server(seed)?;
        }
        let mut servers = ServerDirectory::with_seeds(server, connector.seed_servers());
        let events = SessionEvents::new();
        let mut last_error = None;
        let deadline = Instant::now() + connector.retry_timeout();
        for candidate in servers.reconnect_candidates() {
            tokio::task::consume_budget().await;
            let attempt = async {
                let channel = connector.connect(&candidate).await?;
                Exchange::open(channel, &connector, events.sinks.clone()).await
            };
            match tokio::time::timeout_at(deadline, attempt).await {
                Err(_) => return Err(last_error.unwrap_or(ClientError::RetryDeadline)),
                Ok(Err(error)) => last_error = Some(error),
                Ok(Ok(exchange)) => {
                    servers.connected(&candidate);
                    return Ok(Self::assemble(exchange, events, connector, domain, servers));
                }
            }
        }
        Err(last_error.assured("the primary server is always one configured candidate"))
    }

    /// A client whose session runs on `channel`. It knows no server address, so a lost session
    /// cannot be reconnected.
    pub async fn from_channel(
        channel: Channel,
        domain: Option<DomainName>,
    ) -> error_stack::Result<Self, ClientError> {
        let connector = GrpcConnector::new(ConnectOptions::default())
            .map_err(ClientError::BuildAuthenticationMetadata)?;
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
                commit_basis: Mutex::new(None),
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
        // A cached basis names one transaction. Following the session to a different transaction
        // leaves it describing something this session is no longer committing.
        let mut basis = self.inner.commit_basis.lock().await;
        if let Some(preview) = basis.as_ref()
            && preview.transaction_id != status.transaction_id()
        {
            *basis = None;
        }
        drop(basis);
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

    pub async fn execute(&self, query: impl Into<String>) -> Result<CommandOutcome, ClientError> {
        let query = query.into();
        let route = Self::route(&query);
        let deadline = Instant::now() + self.inner.connector.retry_timeout();
        if route.is_subscription() {
            let execution = self.prepare_execution_with_route(query, route).await;
            return self.execute_prepared_after_lock(&execution, deadline).await;
        }
        let _command_guard = tokio::time::timeout_at(deadline, self.inner.command_lock.lock())
            .await
            .map_err(|_| ClientError::RetryDeadline)?;
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
    ) -> Result<CommandOutcome, ClientError> {
        let deadline = Instant::now() + self.inner.connector.retry_timeout();
        if execution.route.is_subscription() {
            return self.execute_prepared_after_lock(execution, deadline).await;
        }
        let _command_guard = tokio::time::timeout_at(deadline, self.inner.command_lock.lock())
            .await
            .map_err(|_| ClientError::RetryDeadline)?;
        self.execute_prepared_after_lock(execution, deadline).await
    }

    async fn execute_prepared_after_lock(
        &self,
        execution: &ExecutionHandle,
        deadline: Instant,
    ) -> Result<CommandOutcome, ClientError> {
        let result =
            tokio::time::timeout_at(deadline, self.execute_prepared_within_budget(execution)).await;
        match result {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(error))
                if execution.can_have_admitted_command() && error.can_hide_admitted_work() =>
            {
                Err(ClientError::UncertainCommand {
                    reference: execution.reference.clone(),
                    source: Box::new(error),
                })
            }
            Ok(Err(error)) => Err(error),
            Err(_) if execution.can_have_admitted_command() => Err(ClientError::UncertainCommand {
                reference: execution.reference.clone(),
                source: Box::new(ClientError::RetryDeadline),
            }),
            Err(_) => Err(ClientError::RetryDeadline),
        }
    }

    async fn execute_prepared_within_budget(
        &self,
        execution: &ExecutionHandle,
    ) -> Result<CommandOutcome, ClientError> {
        let outcome = self.execute_with_redirects(execution).await?;
        self.record_commit_basis(&outcome).await;
        if let Some(transaction) = outcome.transaction.clone() {
            self.adopt_transaction_status(transaction).await;
        }
        Ok(outcome)
    }

    /// Updates the basis a later COMMIT fences against from what the server just reported.
    pub(crate) async fn record_commit_basis(&self, outcome: &CommandOutcome) {
        if let Some(basis) = outcome.commit_basis() {
            *self.inner.commit_basis.lock().await = Some(basis.clone());
        }
    }

    /// What this session expects of the transaction the next command runs against.
    ///
    /// The cached basis fences a commit only while it still names the attached transaction, so a
    /// basis obtained for a different transaction can never decide this one's commit.
    pub(crate) async fn transaction_expectation(&self) -> TransactionExpectation {
        let Some(status) = self.active_transaction_status().await else {
            return TransactionExpectation::default();
        };
        let cached = self.inner.commit_basis.lock().await.clone();
        let preview = cached.filter(|preview| preview.transaction_id == status.transaction_id());
        TransactionExpectation {
            position: Some(status.accepted_operations()),
            preview,
        }
    }

    pub async fn attach_transaction(
        &self,
        id: impl Into<String>,
    ) -> Result<CommandOutcome, ClientError> {
        let id = id.into();
        match tokio::time::timeout(self.inner.connector.retry_timeout(), async {
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
            Err(_) => Err(ClientError::RetryDeadline),
        }
    }

    pub async fn list_domains(&self) -> Result<Vec<DomainInfo>, ClientError> {
        let body = self.request(ClientRequest::ListDomains, None).await?;
        match body {
            ReplyBody::DomainList(list) => Ok(list.domains),
            other => Err(ClientError::unexpected_reply(
                RequestKind::ListDomains,
                other,
            )),
        }
    }

    /// Reads a transaction's impact report without attaching or changing it, following the
    /// leader when the serving node is not the leader.
    pub async fn inspect_transaction(
        &self,
        target: TransactionInspectionTarget,
        operation: Option<TransactionOperationNumber>,
    ) -> Result<InspectionOutcome, ClientError> {
        let inspected = tokio::time::timeout(self.inner.connector.retry_timeout(), async {
            let _command_guard = self.inner.command_lock.lock().await;
            for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
                tokio::task::consume_budget().await;
                let request = ClientRequest::InspectTransaction(InspectTransactionRequest {
                    target: target.clone(),
                    operation,
                });
                let body = match self.request(request, None).await {
                    Ok(body) => body,
                    Err(error) if error.retryable_session_failure() => {
                        match self.recover_session(RecoveryMode::IfClosed).await? {
                            SessionRecovery::Ready => continue,
                            SessionRecovery::Unavailable => return Err(error),
                        }
                    }
                    Err(error) => return Err(error),
                };
                let outcome = match body {
                    ReplyBody::Inspection(outcome) => outcome,
                    other => {
                        return Err(ClientError::unexpected_reply(
                            RequestKind::InspectTransaction,
                            other,
                        ));
                    }
                };
                match Routing::for_inspection(&outcome) {
                    Routing::Redirect(leader) if Self::retries_remain(attempt) => {
                        self.follow_leader(leader).await?;
                    }
                    Routing::AwaitElection if Self::await_retry(attempt).await => {}
                    _ => return Ok(outcome),
                }
            }
            // Only a session that closed again on the last attempt leaves the loop.
            Err(ClientError::SessionClosed)
        })
        .await;
        match inspected {
            Ok(result) => result,
            Err(_) => Err(ClientError::RetryDeadline),
        }
    }

    async fn execute_with_redirects(
        &self,
        execution: &ExecutionHandle,
    ) -> Result<CommandOutcome, ClientError> {
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            tokio::task::consume_budget().await;
            let outcome = match self.execute_once(execution).await {
                Ok(outcome) => outcome,
                Err(error) if error.retryable_session_failure() => {
                    match self.recover_session(RecoveryMode::IfClosed).await? {
                        SessionRecovery::Ready => continue,
                        SessionRecovery::Unavailable => return Err(error),
                    }
                }
                Err(error) => return Err(error),
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
    ) -> Result<CommandOutcome, ClientError> {
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
            StatementRoute::Local(LocalStatement::UploadResource(upload)) => {
                let Some(domain) = execution.domain.clone() else {
                    return Err(ClientError::NoActiveDomain);
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
                    return Err(ClientError::NoActiveDomain);
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
                let result = tokio::spawn(async move {
                    client
                        .create_subscription(attempt, requests, statement, offset)
                        .await
                })
                .await
                .map_err(ClientError::SubscriptionTask)?;
                result.map_err(|report| {
                    ClientError::SubscriptionOperation(Box::new(report.into_error()))
                })
            }
            StatementRoute::Unsubscribe(subscription) => {
                let exchange = self.inner.exchange.lock().await;
                let requests = exchange.requests();
                let generation = exchange.generation.clone();
                drop(exchange);
                let Some(attempt) = self
                    .inner
                    .events
                    .sinks
                    .desired
                    .cancel(subscription, generation)
                else {
                    return Ok(CommandOutcome::failed_locally(format!(
                        "subscription '{}' is already being deleted",
                        subscription.as_str()
                    )));
                };
                let client = self.clone();
                let result =
                    tokio::spawn(
                        async move { client.delete_subscription(attempt, requests).await },
                    )
                    .await
                    .map_err(ClientError::SubscriptionTask)?;
                result.map_err(|report| {
                    ClientError::SubscriptionOperation(Box::new(report.into_error()))
                })
            }
            StatementRoute::Command => {
                let request = ClientRequest::Command(CommandRequest {
                    query: query.to_string(),
                    domain: execution.domain.clone(),
                    execution_reference: execution.reference.clone(),
                    expected_transaction_position: execution.expectation.position,
                    expected_preview: execution.expectation.preview.clone(),
                });
                match self.request(request, None).await? {
                    ReplyBody::Command(outcome) => {
                        if outcome.execution_reference != execution.reference {
                            return Err(ClientError::ExecutionReferenceMismatch {
                                expected: execution.reference.clone(),
                                received: outcome.execution_reference,
                            });
                        }
                        Ok(CommandOutcome::from(*outcome))
                    }
                    other => Err(ClientError::unexpected_reply(RequestKind::Command, other)),
                }
            }
        }
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
        let response = self.request(request, Some(exchange)).await;
        let outcome = match response {
            Ok(ReplyBody::Subscribe(outcome)) => outcome,
            Ok(other) => {
                self.inner.events.sinks.desired.created(&attempt, None);
                return Err(error_stack::Report::new(ClientError::unexpected_reply(
                    RequestKind::Subscribe,
                    other,
                )));
            }
            Err(error) => {
                self.inner.events.sinks.desired.created(&attempt, None);
                return Err(error_stack::Report::new(error));
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
    async fn delete_subscription(
        &self,
        attempt: DeleteAttempt,
        exchange: Arc<ExchangeRequests>,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let desired = &self.inner.events.sinks.desired;
        let mut changed = desired.watch();
        while desired.deletion_waits(&attempt) {
            tokio::task::consume_budget().await;
            if changed.changed().await.is_err() {
                return Err(error_stack::Report::new(ClientError::SessionClosed));
            }
        }
        if !desired.deletion_active(&attempt) {
            return Ok(CommandOutcome::completed_locally(format!(
                "subscription '{}' closed with its session",
                attempt.name.as_str()
            )));
        }
        let request = ClientRequest::Unsubscribe(UnsubscribeRequest {
            subscription: attempt.name.clone(),
        });
        let response = self.request(request, Some(exchange)).await;
        match response {
            Ok(ReplyBody::Unsubscribe(outcome)) => {
                let outcome = CommandOutcome::from(outcome);
                desired.deleted(&attempt, outcome.succeeded());
                Ok(outcome)
            }
            Ok(other) => {
                desired.deleted(&attempt, false);
                Err(error_stack::Report::new(ClientError::unexpected_reply(
                    RequestKind::Unsubscribe,
                    other,
                )))
            }
            Err(error) => {
                desired.deleted(&attempt, false);
                Err(error_stack::Report::new(error))
            }
        }
    }

    pub(crate) fn restore_subscriptions(
        &self,
        generation: Arc<()>,
        exchange: Arc<ExchangeRequests>,
    ) {
        let attempts = self.inner.events.sinks.desired.restore(generation.clone());
        for initial in attempts {
            let client = SharedClientArc::downgrade(&self.inner);
            let exchange = exchange.clone();
            let generation = generation.clone();
            tokio::spawn(async move {
                let name = initial.contract.create.name.clone();
                let mut attempt = initial;
                loop {
                    tokio::task::consume_budget().await;
                    let Some(inner) = client.upgrade() else {
                        return;
                    };
                    let active_client = Client { inner };
                    let statement = attempt.contract.request().statement;
                    let outcome = active_client
                        .create_subscription(attempt, exchange.clone(), statement, 0)
                        .await;
                    drop(active_client);
                    match outcome {
                        Ok(outcome) if outcome.succeeded() => return,
                        Ok(_) => {}
                        Err(error) => {
                            tracing::debug!(%error, "subscription restoration remains interrupted");
                        }
                    }
                    if !exchange.pending.lock().is_open() {
                        return;
                    }
                    sleep(Duration::from_secs(1)).await;
                    let Some(inner) = client.upgrade() else {
                        return;
                    };
                    let next = inner.events.sinks.desired.retry(&name, &generation);
                    let Some(next) = next else {
                        return;
                    };
                    attempt = next;
                }
            });
        }
    }

    async fn attach_with_redirects(
        &self,
        transaction_id: &str,
    ) -> Result<nervix_client_wire::AttachOutcome, ClientError> {
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            tokio::task::consume_budget().await;
            let request = ClientRequest::AttachTransaction(AttachTransactionRequest {
                transaction_id: transaction_id.to_string(),
            });
            let body = match self.request(request, None).await {
                Ok(body) => body,
                Err(error) if error.retryable_session_failure() => {
                    match Box::pin(self.recover_session(RecoveryMode::TransportOnlyIfClosed))
                        .await?
                    {
                        SessionRecovery::Ready => continue,
                        SessionRecovery::Unavailable => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            };
            let outcome = match body {
                ReplyBody::Attach(outcome) => outcome,
                other => {
                    return Err(ClientError::unexpected_reply(
                        RequestKind::AttachTransaction,
                        other,
                    ));
                }
            };
            match Routing::for_attach(&outcome) {
                Routing::Redirect(leader) if Self::retries_remain(attempt) => {
                    self.inner.connector.validate_server(leader)?;
                    self.inner.servers.lock().await.remember(leader);
                    self.reconnect(leader).await?;
                }
                Routing::AwaitElection if Self::await_retry(attempt).await => {}
                _ => return Ok(outcome),
            }
        }
        // Only a session that closed again on the last attempt leaves the loop.
        Err(ClientError::SessionClosed)
    }

    /// Attaches the session's active transaction again on the node now serving it.
    async fn restore_transaction_binding(&self) -> Result<(), ClientError> {
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
                Err(ClientError::AttachTransaction(outcome.message))
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
    ) -> Result<ReplyBody, ClientError> {
        let kind = RequestKind::from(&request);
        let read_only = matches!(kind, RequestKind::ListDomains | RequestKind::Suggest);
        let deadline = Instant::now() + self.inner.connector.retry_timeout();
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            tokio::task::consume_budget().await;
            let exchange = match &captured {
                Some(exchange) => exchange.clone(),
                None => self.inner.exchange.lock().await.requests(),
            };
            let sent = tokio::time::timeout(self.inner.connector.request_timeout(), async {
                // Register before sending so a prompt reply always finds its waiter.
                let Some(mut registered) = exchange.register() else {
                    return Err(exchange.pending.lock().failure());
                };
                let message = ClientMessage {
                    request_id: registered.request_id,
                    request: request.clone(),
                };
                let frame = message.encode(&SESSION_LIMITS).map_err(|report| {
                    ClientError::EncodeRequest {
                        request: kind,
                        source: report.current_context().clone(),
                    }
                })?;
                if exchange.frames.send(frame).await.is_err() {
                    exchange.pending.lock().close();
                    return Err(exchange.pending.lock().failure());
                }
                match registered.receive().await {
                    Some(body) => Ok(body),
                    None => match exchange.pending.lock().failure() {
                        ClientError::SessionClosed => {
                            Err(ClientError::RequestInterrupted { request: kind })
                        }
                        error => Err(error),
                    },
                }
            });
            let sent = if read_only {
                tokio::time::timeout_at(deadline, sent)
                    .await
                    .map_err(|_| ClientError::RetryDeadline)?
            } else {
                sent.await
            };
            let sent = match sent {
                Ok(result) => result,
                Err(_) => {
                    exchange.pending.lock().close();
                    Err(ClientError::RequestDeadline { request: kind })
                }
            };
            if !read_only {
                return sent;
            }
            match sent {
                Ok(body) => return Ok(body),
                Err(error) if error.retryable_session_failure() => {
                    let recovered = tokio::time::timeout_at(
                        deadline,
                        Box::pin(self.recover_session(RecoveryMode::IfClosed)),
                    )
                    .await
                    .map_err(|_| ClientError::RetryDeadline)??;
                    if let SessionRecovery::Unavailable = recovered {
                        return Err(error);
                    }
                    let allowed = tokio::time::timeout_at(deadline, Self::await_retry(attempt))
                        .await
                        .map_err(|_| ClientError::RetryDeadline)?;
                    if !allowed {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(ClientError::RetryDeadline)
    }

    pub async fn subscribe(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<CommandOutcome, ClientError> {
        self.execute(request.to_query()).await
    }

    pub async fn unsubscribe(&self, name: &str) -> Result<CommandOutcome, ClientError> {
        self.execute(nervix_nspl::subscribe::delete_subscription_query(name))
            .await
    }

    /// The lifecycle currently retained for a desired subscription name.
    pub fn subscription_lifecycle(&self, name: &SubscriptionName) -> Option<SubscriptionLifecycle> {
        self.inner.events.sinks.desired.lifecycle(name)
    }

    pub async fn next_subscription(&self) -> Result<SubscriptionEvent, ClientError> {
        loop {
            tokio::task::consume_budget().await;
            if let Some(interrupted) = self.inner.events.sinks.desired.take_interruption() {
                return Ok(SubscriptionEvent::Interrupted(interrupted));
            }
            let desired = &self.inner.events.sinks.desired;
            let mut desired_changed = desired.watch();
            let result = tokio::select! {
                result = self.inner.events.sinks.subscriptions.next() => result,
                changed = desired_changed.changed() => {
                    if changed.is_err() {
                        return Err(ClientError::SessionClosed);
                    }
                    continue;
                }
            };
            match result {
                Ok(event) => {
                    let generation = self.inner.exchange.lock().await.generation.clone();
                    if self
                        .inner
                        .events
                        .sinks
                        .desired
                        .can_deliver(event.subscription(), &generation)
                    {
                        return Ok(event);
                    }
                }
                Err(error) if *error.current_context() == EventQueueError::Overflow => {
                    return Err(ClientError::EventOverflow {
                        stream: EventStreamKind::Subscription,
                    });
                }
                Err(_) => {
                    if let Some(interrupted) = self.inner.events.sinks.desired.take_interruption() {
                        return Ok(SubscriptionEvent::Interrupted(interrupted));
                    }
                    if !self.inner.events.sinks.desired.has_acknowledged_desired() {
                        return Err(ClientError::SessionClosed);
                    }
                    match self.recover_session(RecoveryMode::IfClosed).await? {
                        SessionRecovery::Ready => continue,
                        SessionRecovery::Unavailable => return Err(ClientError::SessionClosed),
                    }
                }
            }
        }
    }

    pub async fn next_server_event(&self) -> Result<ServerEvent, ClientError> {
        match self.inner.events.sinks.notices.next().await {
            Ok(event) => Ok(event),
            Err(error) if *error.current_context() == EventQueueError::Overflow => {
                Err(ClientError::EventOverflow {
                    stream: EventStreamKind::ServerNotice,
                })
            }
            Err(_) => Err(ClientError::SessionClosed),
        }
    }

    /// Waits for the next complete domain list the server observes. Only the latest list is kept,
    /// so a caller that reads late gets the current list rather than every list in between.
    pub async fn next_domain_list(&self) -> Result<Vec<DomainInfo>, ClientError> {
        let mut observed = self.inner.events.domains.lock().await;
        loop {
            tokio::task::consume_budget().await;
            if observed.changed().await.is_err() {
                return Err(ClientError::SessionClosed);
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
    ) -> Result<Vec<AutocompleteSuggestion>, ClientError> {
        let input = input.into();
        let length = input.len();
        let domain = self.domain().await;
        let request = nervix_client_wire::SuggestRequest::new(input, cursor, domain)
            .map_err(|_| ClientError::InvalidCursor { cursor, length })?;
        match self.request(ClientRequest::Suggest(request), None).await? {
            ReplyBody::Suggest(outcome) => Ok(outcome
                .suggestions
                .into_iter()
                .map(AutocompleteSuggestion::from)
                .collect()),
            other => Err(ClientError::unexpected_reply(RequestKind::Suggest, other)),
        }
    }

    /// The channel the current exchange runs on.
    pub(crate) async fn current_channel(&self) -> Channel {
        self.inner.exchange.lock().await.requests().channel.clone()
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
    pub(crate) async fn follow_leader(&self, leader: &Url) -> Result<(), ClientError> {
        self.inner.connector.validate_server(leader)?;
        self.inner.servers.lock().await.remember(leader);
        match self.reconnect(leader).await {
            Ok(()) => self.restore_transaction_binding().await,
            Err(error) => match self.recover_session(RecoveryMode::Replace).await? {
                SessionRecovery::Ready => Ok(()),
                SessionRecovery::Unavailable => Err(error),
            },
        }
    }

    /// Replaces the exchange with a new one on `server`. Every request still waiting on the
    /// previous exchange observes the closed session.
    async fn reconnect(&self, server: &Url) -> Result<(), ClientError> {
        let _reconnect_guard = self.inner.reconnect_lock.lock().await;
        self.reconnect_unlocked(server).await
    }

    async fn reconnect_unlocked(&self, server: &Url) -> Result<(), ClientError> {
        let channel = self.inner.connector.connect(server).await?;
        let exchange = Exchange::open(
            channel,
            &self.inner.connector,
            self.inner.events.sinks.clone(),
        )
        .await?;
        self.inner.servers.lock().await.connected(server);
        let previous = std::mem::replace(&mut *self.inner.exchange.lock().await, exchange);
        previous.close().await;
        let exchange = self.inner.exchange.lock().await;
        self.restore_subscriptions(exchange.generation.clone(), exchange.requests());
        Ok(())
    }

    /// Reconnects a lost session and attaches its transaction again.
    pub(crate) async fn recover_session(
        &self,
        mode: RecoveryMode,
    ) -> Result<SessionRecovery, ClientError> {
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
                tokio::task::consume_budget().await;
                let candidates = self.inner.servers.lock().await.reconnect_candidates();
                if candidates.is_empty() {
                    break 'recovery SessionRecovery::Unavailable;
                }
                for server in candidates {
                    tokio::task::consume_budget().await;
                    match tokio::time::timeout_at(deadline, self.reconnect_unlocked(&server)).await
                    {
                        Ok(Ok(())) => break 'recovery SessionRecovery::Ready,
                        Ok(Err(error)) => last_error = Some(error),
                        Err(_) => return Err(last_error.unwrap_or(ClientError::RetryDeadline)),
                    }
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(last_error.unwrap_or(ClientError::RetryDeadline));
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
            "{} pace={} status={}",
            domain.domain,
            domain.pace.as_ref(),
            domain.status.as_ref()
        ));
    }
    lines.join("\n")
}
