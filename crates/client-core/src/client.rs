//! The client: one session with a server, the statements it executes, and the transaction it
//! holds.
//!
//! - **Owns.** Sending requests on the current exchange, the redirects, retries and reconnects a
//!   reply calls for, the session's selected domain and transaction binding, and the statements the
//!   client serves itself.
//! - **Depends on.** The exchange dispatcher, the connector, the wire contract, and the language
//!   layer for splitting and classifying statements.
//! - **Must not know.** How a frame is routed off an exchange.

use std::{path::PathBuf, time::Duration};

use meticulous::ResultExt as _;
use nervix_client_wire::{
    AttachDisposition, AttachTransactionRequest, ClientMessage, ClientRequest, CommandRequest,
    DomainInfo, InspectTransactionRequest, InspectionOutcome, Leadership, ReplyBody,
    SubscribeRequest, SubscriptionType, UnsubscribeRequest,
};
use nervix_models::{
    CommandExecutionReference, DomainName, SubscriptionName, TransactionInspectionTarget,
    TransactionOperationNumber, TransactionPosition, TransactionPreviewIdentity, TransactionStatus,
    UploadResource,
};
use nervix_nspl::client_statement::{ClientStatement, ParsedClientStatement};
use nervix_recovery::Discarded as _;
use tokio::{sync::Mutex, time::sleep};
use tonic::transport::Channel;
use triomphe::Arc;
use url::Url;

#[cfg(feature = "autocomplete")]
use crate::events::AutocompleteSuggestion;
use crate::{
    connection::{ConnectOptions, GrpcConnector, ServerDirectory},
    error::{ClientError, RequestKind},
    events::{ServerEvent, SubscriptionEvent, SubscriptionRequest},
    exchange::{Exchange, SESSION_LIMITS, SessionEvents},
    outcome::{CommandOutcome, Routing},
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

/// Whether a lost session was replaced by a working one.
pub(crate) enum SessionRecovery {
    Unavailable,
    Ready,
}

/// How the client serves a query.
enum StatementRoute<'q> {
    /// A statement the client answers itself.
    Local(LocalStatement),
    /// A CREATE SUBSCRIPTION statement, sent as a subscribe request carrying its exact source,
    /// which starts `offset` bytes into the query.
    Subscribe { statement: &'q str, offset: usize },
    /// A DELETE SUBSCRIPTION statement, sent as an unsubscribe request.
    Unsubscribe(SubscriptionName),
    /// A batch the client refuses to send, for the reason given.
    Refused(&'static str),
    /// Statements the server executes as one command.
    Command,
}

/// A statement the client serves without sending it.
enum LocalStatement {
    UseDomain(DomainName),
    ListDomains,
    UploadResource(UploadResource),
}

impl<'q> StatementRoute<'q> {
    /// Classifies the statements `query` parsed into.
    fn of(query: &'q str, statements: Vec<ParsedClientStatement>) -> Self {
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
        let statement = parsed.source(query);
        let offset = parsed.span.start;
        match parsed.statement {
            ClientStatement::UseDomain(domain) => Self::Local(LocalStatement::UseDomain(domain)),
            ClientStatement::ListDomains => Self::Local(LocalStatement::ListDomains),
            ClientStatement::UploadResource(upload) => {
                Self::Local(LocalStatement::UploadResource(upload))
            }
            ClientStatement::CreateSubscription(_) => Self::Subscribe { statement, offset },
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
    pub(crate) command_lock: Mutex<()>,
    pub(crate) transaction: Mutex<Option<TransactionStatus>>,
    /// The preview the attached transaction's last accepted append made current. A COMMIT sends
    /// it so the server can refuse a transaction that moved since this session last read it.
    pub(crate) commit_basis: Mutex<Option<TransactionPreviewIdentity>>,
    pub(crate) events: SessionEvents,
}

#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

impl Client {
    pub(crate) const MAX_LEADER_ROUTING_ATTEMPTS: usize = 20;
    const LEADER_ELECTION_RETRY_DELAY: Duration = Duration::from_millis(100);

    pub async fn connect(
        server: impl AsRef<str>,
        domain: Option<DomainName>,
    ) -> Result<Self, ClientError> {
        Self::connect_with_options(server, domain, ConnectOptions::default()).await
    }

    pub async fn connect_with_options(
        server: impl AsRef<str>,
        domain: Option<DomainName>,
        options: ConnectOptions,
    ) -> Result<Self, ClientError> {
        let server = Url::parse(server.as_ref()).map_err(ClientError::InvalidServerUrl)?;
        let connector =
            GrpcConnector::new(options).map_err(ClientError::BuildAuthenticationMetadata)?;
        let channel = connector.connect(&server).await?;
        let events = SessionEvents::new();
        let exchange = Exchange::open(channel, &connector, events.sinks.clone()).await?;
        Ok(Self::assemble(
            exchange,
            events,
            connector,
            domain,
            ServerDirectory::connected_to(Some(server)),
        ))
    }

    /// A client whose session runs on `channel`. It knows no server address, so a lost session
    /// cannot be reconnected.
    pub async fn from_channel(
        channel: Channel,
        domain: Option<DomainName>,
    ) -> Result<Self, ClientError> {
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
            inner: Arc::new(ClientInner {
                domain: Mutex::new(domain),
                servers: Mutex::new(servers),
                connector,
                exchange: Mutex::new(exchange),
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
        let _command_guard = self.inner.command_lock.lock().await;
        let execution_reference =
            CommandExecutionReference::parse(uuid::Uuid::now_v7().to_string()).assured(
                "a hyphenated UUID is 36 ASCII hex digits and hyphens, within the execution \
                 reference grammar",
            );
        let expectation = self.transaction_expectation().await;
        let outcome = self
            .execute_with_redirects(&query, &execution_reference, &expectation)
            .await?;
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
        let _command_guard = self.inner.command_lock.lock().await;
        let outcome = self.attach_with_redirects(&id.into()).await?;
        let outcome = CommandOutcome::from(outcome);
        if let Some(transaction) = outcome.transaction.clone() {
            self.adopt_transaction_status(transaction).await;
        }
        Ok(outcome)
    }

    pub async fn list_domains(&self) -> Result<Vec<DomainInfo>, ClientError> {
        let body = self.request(ClientRequest::ListDomains).await?;
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
        let _command_guard = self.inner.command_lock.lock().await;
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            tokio::task::consume_budget().await;
            let request = ClientRequest::InspectTransaction(InspectTransactionRequest {
                target: target.clone(),
                operation,
            });
            let body = match self.request(request).await {
                Ok(body) => body,
                Err(ClientError::SessionClosed) => match self.recover_session().await? {
                    SessionRecovery::Ready => continue,
                    SessionRecovery::Unavailable => return Err(ClientError::SessionClosed),
                },
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
    }

    async fn execute_with_redirects(
        &self,
        query: &str,
        execution_reference: &CommandExecutionReference,
        expectation: &TransactionExpectation,
    ) -> Result<CommandOutcome, ClientError> {
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            tokio::task::consume_budget().await;
            let outcome = match self
                .execute_once(query, execution_reference, expectation)
                .await
            {
                Ok(outcome) => outcome,
                Err(ClientError::SessionClosed) => match self.recover_session().await? {
                    SessionRecovery::Ready => continue,
                    SessionRecovery::Unavailable => return Err(ClientError::SessionClosed),
                },
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
        self.execute_once(query, execution_reference, expectation)
            .await
    }

    /// Serves a query once: locally, as a subscription request, or as a command.
    async fn execute_once(
        &self,
        query: &str,
        execution_reference: &CommandExecutionReference,
        expectation: &TransactionExpectation,
    ) -> Result<CommandOutcome, ClientError> {
        let route = match nervix_nspl::client_statement::parse_client_statement_sources(query) {
            Ok(statements) => StatementRoute::of(query, statements),
            // The server reports a query it cannot parse, with diagnostics.
            Err(_) => StatementRoute::Command,
        };
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
                self.set_domain(Some(domain)).await;
                Ok(CommandOutcome::completed_locally(message))
            }
            StatementRoute::Local(LocalStatement::ListDomains) => {
                let domains = self.list_domains().await?;
                Ok(CommandOutcome::completed_locally(format_domain_list(
                    &domains,
                )))
            }
            StatementRoute::Local(LocalStatement::UploadResource(upload)) => {
                self.upload_resource_from_directory(
                    upload.identifier.as_str(),
                    PathBuf::from(upload.source_path),
                    |_| {},
                )
                .await
            }
            StatementRoute::Subscribe { statement, offset } => {
                let Some(domain) = self.domain().await else {
                    return Err(ClientError::NoActiveDomain);
                };
                let request = ClientRequest::Subscribe(SubscribeRequest {
                    domain,
                    statement: statement.to_string(),
                    subscription_type: SubscriptionType::Row,
                });
                let outcome = match self.request(request).await? {
                    ReplyBody::Subscribe(outcome) => outcome,
                    other => {
                        return Err(ClientError::unexpected_reply(RequestKind::Subscribe, other));
                    }
                };
                let mut outcome = CommandOutcome::from(outcome);
                outcome.locate_diagnostics_in_query(offset);
                Ok(outcome)
            }
            StatementRoute::Unsubscribe(subscription) => {
                let request = ClientRequest::Unsubscribe(UnsubscribeRequest { subscription });
                match self.request(request).await? {
                    ReplyBody::Unsubscribe(outcome) => Ok(CommandOutcome::from(outcome)),
                    other => Err(ClientError::unexpected_reply(
                        RequestKind::Unsubscribe,
                        other,
                    )),
                }
            }
            StatementRoute::Command => {
                let request = ClientRequest::Command(CommandRequest {
                    query: query.to_string(),
                    domain: self.domain().await,
                    execution_reference: execution_reference.clone(),
                    expected_transaction_position: expectation.position,
                    expected_preview: expectation.preview.clone(),
                });
                match self.request(request).await? {
                    ReplyBody::Command(outcome) => Ok(CommandOutcome::from(*outcome)),
                    other => Err(ClientError::unexpected_reply(RequestKind::Command, other)),
                }
            }
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
            let body = match self.request(request).await {
                Ok(body) => body,
                Err(ClientError::SessionClosed) => match self.recover_transport().await? {
                    SessionRecovery::Ready => continue,
                    SessionRecovery::Unavailable => return Err(ClientError::SessionClosed),
                },
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

    /// Sends one request on the current exchange and waits for the reply that names it.
    async fn request(&self, request: ClientRequest) -> Result<ReplyBody, ClientError> {
        let kind = RequestKind::from(&request);
        let exchange = self.inner.exchange.lock().await.requests();
        // The waiter is registered before the frame is sent, so the reply always finds it.
        let registered = exchange.pending.lock().await.register();
        let Some(registered) = registered else {
            return Err(ClientError::SessionClosed);
        };
        let message = ClientMessage {
            request_id: registered.request_id,
            request,
        };
        let frame = match message.encode(&SESSION_LIMITS) {
            Ok(frame) => frame,
            Err(report) => {
                exchange
                    .pending
                    .lock()
                    .await
                    .take(registered.request_id)
                    .discarded("the request was never sent, so nothing answers it");
                return Err(ClientError::EncodeRequest {
                    request: kind,
                    source: report.current_context().clone(),
                });
            }
        };
        if exchange.frames.send(frame).await.is_err() {
            exchange
                .pending
                .lock()
                .await
                .take(registered.request_id)
                .discarded("the frame never reached the exchange, so nothing answers it");
            return Err(ClientError::SessionClosed);
        }
        // From here only the exchange's reader, or the exchange closing, settles the waiter, so
        // the request releases its hold on the exchange before it waits.
        drop(exchange);
        registered
            .reply
            .await
            .map_err(|_| ClientError::SessionClosed)
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

    pub async fn next_subscription(&self) -> Result<SubscriptionEvent, ClientError> {
        self.inner
            .events
            .subscriptions
            .lock()
            .await
            .recv()
            .await
            .ok_or(ClientError::SessionClosed)
    }

    pub async fn next_server_event(&self) -> Result<ServerEvent, ClientError> {
        self.inner
            .events
            .notices
            .lock()
            .await
            .recv()
            .await
            .ok_or(ClientError::SessionClosed)
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
        match self.request(ClientRequest::Suggest(request)).await? {
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
        sleep(Self::LEADER_ELECTION_RETRY_DELAY).await;
        true
    }

    /// Moves the session to the leader at `leader`, so the request that needs it can be sent
    /// again there.
    pub(crate) async fn follow_leader(&self, leader: &Url) -> Result<(), ClientError> {
        self.inner.servers.lock().await.remember(leader);
        match self.reconnect(leader).await {
            Ok(()) => self.restore_transaction_binding().await,
            Err(error) => match self.recover_session().await? {
                SessionRecovery::Ready => Ok(()),
                SessionRecovery::Unavailable => Err(error),
            },
        }
    }

    /// Replaces the exchange with a new one on `server`. Every request still waiting on the
    /// previous exchange observes the closed session.
    async fn reconnect(&self, server: &Url) -> Result<(), ClientError> {
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
        Ok(())
    }

    /// Reconnects a lost session and attaches its transaction again.
    pub(crate) async fn recover_session(&self) -> Result<SessionRecovery, ClientError> {
        let SessionRecovery::Ready = self.recover_transport().await? else {
            return Ok(SessionRecovery::Unavailable);
        };
        self.restore_transaction_binding().await?;
        Ok(SessionRecovery::Ready)
    }

    /// Reconnects a lost session to the first known server that accepts it.
    async fn recover_transport(&self) -> Result<SessionRecovery, ClientError> {
        let candidates = self.inner.servers.lock().await.reconnect_candidates();
        let mut last_error = None;
        for server in candidates {
            tokio::task::consume_budget().await;
            match self.reconnect(&server).await {
                Ok(()) => return Ok(SessionRecovery::Ready),
                Err(error) => last_error = Some(error),
            }
        }
        match last_error {
            Some(error) => Err(error),
            None => Ok(SessionRecovery::Unavailable),
        }
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
