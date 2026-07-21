use std::{
    collections::VecDeque,
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use async_tar::{Builder as AsyncTarBuilder, EntryType, Header, HeaderMode};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
pub use nervix_models::SubscriptionDeliveryBehavior;
use nervix_nspl::client_statement::ClientStatement;
pub use nervix_proto as proto;
use proto::{
    CommandRequest, ListDomainsRequest, SessionRequest,
    session_service_client::SessionServiceClient,
};
use rustls::crypto::aws_lc_rs;
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request,
    metadata::MetadataValue,
    transport::{Certificate, Channel, ClientTlsConfig},
};
use triomphe::Arc;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub message: String,
    pub span_start: u32,
    pub span_end: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutcome {
    pub success: bool,
    pub kind: CommandOutcomeKind,
    pub message: String,
    pub diagnostics: Vec<Diagnostic>,
    pub leader: Option<String>,
    pub leader_grpc_uri: Option<String>,
    pub already_existed: bool,
    pub transaction_active: Option<bool>,
    pub results: Vec<CommandOutcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandOutcomeKind {
    Unspecified,
    Ok,
    Error,
    NotLeader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionEvent {
    pub subscription: String,
    pub relay: String,
    pub payload: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerEventLevel {
    Unspecified,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerEvent {
    pub level: ServerEventLevel,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainInfo {
    pub id: String,
    pub pace: String,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuggestionKind {
    Text,
    LocalDirectoryLookup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutocompleteSuggestion {
    pub value: String,
    pub kind: SuggestionKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionRequest {
    pub name: String,
    pub relay: String,
    pub delivery_behavior: SubscriptionDeliveryBehavior,
    pub batch_sample_rate: Option<String>,
    pub filter_map: Option<String>,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid server URI")]
    InvalidServerUri(#[source] tonic::codegen::http::uri::InvalidUri),
    #[error("invalid server URL")]
    InvalidServerUrl(#[source] url::ParseError),
    #[error("TLS is required but the server URI is not https")]
    TlsRequired,
    #[error("failed to configure TLS for server connection")]
    ConfigureTls(#[source] tonic::transport::Error),
    #[error("failed to connect to server")]
    ConnectServer(#[source] tonic::transport::Error),
    #[error("failed to start session relay: {0}")]
    StartSession(#[source] Box<tonic::Status>),
    #[error("failed to build authentication metadata")]
    BuildAuthenticationMetadata,
    #[error("session relay closed")]
    SessionClosed,
    #[error("failed to build upload archive")]
    BuildUploadArchive,
    #[error("upload request failed: {0}")]
    UploadResource(#[source] Box<tonic::Status>),
    #[error("failed to load TLS CA certificate")]
    LoadTlsCaCertificate(#[source] std::io::Error),
}

enum PendingResponse {
    Command(oneshot::Sender<CommandOutcome>),
    DomainList(oneshot::Sender<Vec<DomainInfo>>),
    #[cfg(feature = "autocomplete")]
    Suggest(oneshot::Sender<Vec<AutocompleteSuggestion>>),
}

struct ClientInner {
    domain: Mutex<String>,
    current_server: Mutex<Option<String>>,
    known_servers: Mutex<Vec<String>>,
    grpc_connector: GrpcConnector,
    request_tx: Mutex<mpsc::Sender<SessionRequest>>,
    pending: Arc<Mutex<VecDeque<PendingResponse>>>,
    command_lock: Mutex<()>,
    transaction_active: Mutex<bool>,
    response_task: Mutex<Option<JoinHandle<()>>>,
    subscription_tx: mpsc::Sender<SubscriptionEvent>,
    subscription_rx: Mutex<mpsc::Receiver<SubscriptionEvent>>,
    server_tx: mpsc::Sender<ServerEvent>,
    server_rx: Mutex<mpsc::Receiver<ServerEvent>>,
    domain_tx: mpsc::Sender<Vec<DomainInfo>>,
    domain_rx: Mutex<mpsc::Receiver<Vec<DomainInfo>>>,
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsRequirement {
    Preferred,
    Required,
}

#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    pub tls_requirement: Option<TlsRequirement>,
    pub ca_certificate_pem: Option<Vec<u8>>,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl ConnectOptions {
    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    fn basic_authorization(&self) -> Option<String> {
        let username = self.username.as_ref()?;
        let password = self.password.as_ref()?;
        let encoded = BASE64_STANDARD.encode(format!("{username}:{password}"));
        Some(format!("Basic {encoded}"))
    }
}

#[derive(Debug, Clone)]
struct GrpcConnector {
    options: ConnectOptions,
}

impl GrpcConnector {
    fn new(options: ConnectOptions) -> Self {
        Self { options }
    }

    async fn connect(&self, server: &str) -> Result<Channel, ClientError> {
        let mut endpoint =
            Channel::from_shared(server.to_string()).map_err(ClientError::InvalidServerUri)?;
        let tls_requirement = self
            .options
            .tls_requirement
            .unwrap_or(TlsRequirement::Preferred);
        let server_url = Url::parse(server).map_err(ClientError::InvalidServerUrl)?;
        let is_https = server_url.scheme() == "https";
        if tls_requirement == TlsRequirement::Required && !is_https {
            return Err(ClientError::TlsRequired);
        }
        if is_https {
            let _ = aws_lc_rs::default_provider().install_default();
            let mut tls = ClientTlsConfig::new();
            if let Some(pem) = self.options.ca_certificate_pem.clone() {
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            endpoint = endpoint
                .tls_config(tls)
                .map_err(ClientError::ConfigureTls)?;
        }
        endpoint.connect().await.map_err(ClientError::ConnectServer)
    }
}

impl Client {
    pub async fn connect(
        server: impl AsRef<str>,
        domain: impl Into<String>,
    ) -> Result<Self, ClientError> {
        Self::connect_with_options(server, domain, ConnectOptions::default()).await
    }

    pub async fn connect_with_options(
        server: impl AsRef<str>,
        domain: impl Into<String>,
        options: ConnectOptions,
    ) -> Result<Self, ClientError> {
        let server = server.as_ref().to_string();
        let grpc_connector = GrpcConnector::new(options.clone());
        let channel = grpc_connector.connect(&server).await?;
        Self::from_channel_with_server(channel, domain.into(), Some(server), options).await
    }

    pub async fn from_channel(channel: Channel, domain: String) -> Result<Self, ClientError> {
        Self::from_channel_with_server(channel, domain, None, ConnectOptions::default()).await
    }

    async fn from_channel_with_server(
        channel: Channel,
        domain: String,
        current_server: Option<String>,
        connect_options: ConnectOptions,
    ) -> Result<Self, ClientError> {
        let (subscription_tx, subscription_rx) = mpsc::channel(128);
        let (server_tx, server_rx) = mpsc::channel(128);
        let (domain_tx, domain_rx) = mpsc::channel(16);
        let pending = Arc::new(Mutex::new(VecDeque::new()));
        let known_servers = current_server.iter().cloned().collect();
        let request_tx = start_session(
            channel,
            connect_options.basic_authorization(),
            pending.clone(),
            subscription_tx.clone(),
            server_tx.clone(),
            domain_tx.clone(),
        )
        .await?;
        let inner = Arc::new(ClientInner {
            domain: Mutex::new(domain),
            current_server: Mutex::new(current_server),
            known_servers: Mutex::new(known_servers),
            grpc_connector: GrpcConnector::new(connect_options),
            request_tx: Mutex::new(request_tx.0),
            pending,
            command_lock: Mutex::new(()),
            transaction_active: Mutex::new(false),
            response_task: Mutex::new(Some(request_tx.1)),
            subscription_tx,
            subscription_rx: Mutex::new(subscription_rx),
            server_tx,
            server_rx: Mutex::new(server_rx),
            domain_tx,
            domain_rx: Mutex::new(domain_rx),
        });

        Ok(Self { inner })
    }

    pub async fn domain(&self) -> String {
        self.inner.domain.lock().await.clone()
    }

    pub async fn set_domain(&self, domain: impl Into<String>) {
        *self.inner.domain.lock().await = domain.into();
    }

    pub async fn transaction_active(&self) -> bool {
        *self.inner.transaction_active.lock().await
    }

    pub async fn execute(&self, query: impl Into<String>) -> Result<CommandOutcome, ClientError> {
        let query = query.into();
        let _command_guard = self.inner.command_lock.lock().await;
        let outcome = self.execute_with_redirects(&query).await?;
        if let Some(active) = outcome.transaction_active {
            *self.inner.transaction_active.lock().await = active;
        }
        Ok(outcome)
    }

    pub async fn list_domains(&self) -> Result<Vec<DomainInfo>, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .await
            .push_back(PendingResponse::DomainList(tx));
        let request = SessionRequest {
            request: Some(proto::session_request::Request::ListDomains(
                ListDomainsRequest {},
            )),
        };
        let request_tx = self.inner.request_tx.lock().await.clone();
        if request_tx.send(request).await.is_err() {
            let _ = self.inner.pending.lock().await.pop_back();
            return Err(ClientError::SessionClosed);
        }
        rx.await.map_err(|_| ClientError::SessionClosed)
    }

    async fn execute_with_redirects(&self, query: &str) -> Result<CommandOutcome, ClientError> {
        const MAX_REDIRECTS: usize = 4;
        for _ in 0..=MAX_REDIRECTS {
            let outcome = match self.execute_once(query).await {
                Ok(outcome) => outcome,
                Err(ClientError::SessionClosed) => {
                    if self.recover_session().await? {
                        continue;
                    }
                    return Err(ClientError::SessionClosed);
                }
                Err(err) => return Err(err),
            };
            if outcome.kind != CommandOutcomeKind::NotLeader {
                return Ok(outcome);
            }
            let Some(leader_grpc_uri) = outcome.leader_grpc_uri.as_deref() else {
                return Ok(outcome);
            };
            self.remember_known_server(leader_grpc_uri).await;
            match self.reconnect(leader_grpc_uri).await {
                Ok(()) => {}
                Err(err) => match self.recover_session().await {
                    Ok(true) => {}
                    Ok(false) => return Err(err),
                    Err(recover_err) => return Err(recover_err),
                },
            }
        }
        self.execute_once(query).await
    }

    async fn execute_once(&self, query: &str) -> Result<CommandOutcome, ClientError> {
        if let Ok(statements) = nervix_nspl::client_statement::parse_client_statement_sources(query)
            && statements
                .iter()
                .any(|parsed| parsed.statement.requires_local_handling())
        {
            if statements.len() > 1 {
                return Ok(command_error_outcome(
                    "client-local commands must be executed separately".to_string(),
                ));
            }
            if self.transaction_active().await {
                return Ok(command_error_outcome(
                    "client-local commands are not allowed while a transaction is active"
                        .to_string(),
                ));
            }
            let parsed = statements
                .into_iter()
                .next()
                .expect("non-empty parsed statements must contain one statement");
            return self
                .execute_client_statement(parsed.statement, &parsed.source)
                .await;
        }
        self.execute_remote_once(query).await
    }

    async fn execute_client_statement(
        &self,
        statement: ClientStatement,
        source: &str,
    ) -> Result<CommandOutcome, ClientError> {
        match statement {
            ClientStatement::UseDomain(domain) => {
                self.set_domain(domain.to_string()).await;
                Ok(command_ok_outcome(format!(
                    "using domain '{}'",
                    domain.as_str()
                )))
            }
            ClientStatement::ListDomains => {
                let domains = self.list_domains().await?;
                Ok(command_ok_outcome(format_domain_list(&domains)))
            }
            ClientStatement::UploadResource(upload) => {
                self.upload_resource_from_directory(
                    upload.identifier.as_str(),
                    PathBuf::from(upload.source_path),
                    |_| {},
                )
                .await
            }
            ClientStatement::CreateSubscription(_)
            | ClientStatement::BeginTransaction
            | ClientStatement::CommitTransaction
            | ClientStatement::RevertTransaction
            | ClientStatement::DeleteSubscription(_)
            | ClientStatement::Server(_) => self.execute_remote_once(source).await,
        }
    }

    async fn execute_remote_once(&self, query: &str) -> Result<CommandOutcome, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .await
            .push_back(PendingResponse::Command(tx));
        let request = SessionRequest {
            request: Some(proto::session_request::Request::Command(CommandRequest {
                query: query.to_string(),
                domain: self.inner.domain.lock().await.clone(),
            })),
        };
        let request_tx = self.inner.request_tx.lock().await.clone();
        if request_tx.send(request).await.is_err() {
            let _ = self.inner.pending.lock().await.pop_back();
            return Err(ClientError::SessionClosed);
        }
        rx.await.map_err(|_| ClientError::SessionClosed)
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
            .subscription_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or(ClientError::SessionClosed)
    }

    pub async fn next_server_event(&self) -> Result<ServerEvent, ClientError> {
        self.inner
            .server_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or(ClientError::SessionClosed)
    }

    pub async fn next_domain_list(&self) -> Result<Vec<DomainInfo>, ClientError> {
        self.inner
            .domain_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or(ClientError::SessionClosed)
    }

    #[cfg(feature = "autocomplete")]
    pub async fn suggest(
        &self,
        input: impl Into<String>,
        cursor: u32,
    ) -> Result<Vec<AutocompleteSuggestion>, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .await
            .push_back(PendingResponse::Suggest(tx));
        let request = SessionRequest {
            request: Some(proto::session_request::Request::Suggest(
                proto::SuggestRequest {
                    input: input.into(),
                    cursor,
                    domain: self.inner.domain.lock().await.clone(),
                },
            )),
        };
        let request_tx = self.inner.request_tx.lock().await.clone();
        if request_tx.send(request).await.is_err() {
            let _ = self.inner.pending.lock().await.pop_back();
            return Err(ClientError::SessionClosed);
        }
        rx.await.map_err(|_| ClientError::SessionClosed)
    }

    pub async fn upload_resource_from_directory(
        &self,
        identifier: &str,
        directory: impl AsRef<Path>,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
    ) -> Result<CommandOutcome, ClientError> {
        const MAX_REDIRECTS: usize = 4;
        let directory = expand_user_path(directory.as_ref());
        if !directory.is_dir() {
            return Err(ClientError::BuildUploadArchive);
        }

        for _ in 0..=MAX_REDIRECTS {
            let current_server = self.inner.current_server.lock().await.clone();
            let server = current_server.ok_or(ClientError::SessionClosed)?;
            let channel = self.inner.grpc_connector.connect(&server).await?;
            let mut client = SessionServiceClient::new(channel);
            let (tx, rx) = mpsc::channel(8);
            let request_identifier = identifier.to_string();
            let request_directory = directory.clone();
            let progress_callback = on_progress.clone();
            tokio::spawn(async move {
                let (writer, mut reader) = tokio::io::duplex(64 * 1024);
                let build_task = tokio::spawn(async move {
                    let _ = relay_upload_archive(&request_directory, writer).await;
                });
                let _ = tx
                    .send(proto::UploadResourceRequest {
                        event: Some(proto::upload_resource_request::Event::Start(
                            proto::UploadResourceStart {
                                name: request_identifier,
                                total_bytes: 0,
                            },
                        )),
                    })
                    .await;
                let mut buffer = vec![0u8; 64 * 1024];
                loop {
                    tokio::task::consume_budget().await;
                    let read = match reader.read(&mut buffer).await {
                        Ok(read) => read,
                        Err(_) => return,
                    };
                    if read == 0 {
                        break;
                    }
                    progress_callback(u64::try_from(read).unwrap_or(0));
                    if tx
                        .send(proto::UploadResourceRequest {
                            event: Some(proto::upload_resource_request::Event::Chunk(
                                buffer[..read].to_vec().into(),
                            )),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let _ = build_task.await;
            });
            let response = client
                .upload_resource(request_with_auth(
                    ReceiverStream::new(rx),
                    self.inner.grpc_connector.options.basic_authorization(),
                )?)
                .await
                .map_err(|status| ClientError::UploadResource(Box::new(status)))?
                .into_inner();
            let outcome = CommandOutcome {
                success: response.success,
                kind: match proto::CommandResultKind::try_from(response.kind).ok() {
                    Some(proto::CommandResultKind::NotLeader) => CommandOutcomeKind::NotLeader,
                    Some(proto::CommandResultKind::Ok) if response.success => {
                        CommandOutcomeKind::Ok
                    }
                    _ if response.success => CommandOutcomeKind::Ok,
                    _ => CommandOutcomeKind::Error,
                },
                message: response.message,
                diagnostics: response.diagnostics.into_iter().map(Into::into).collect(),
                leader: (!response.leader.is_empty()).then_some(response.leader),
                leader_grpc_uri: (!response.leader_grpc_uri.is_empty())
                    .then_some(response.leader_grpc_uri),
                already_existed: false,
                transaction_active: None,
                results: Vec::new(),
            };
            if outcome.kind != CommandOutcomeKind::NotLeader {
                return Ok(outcome);
            }
            let Some(leader_grpc_uri) = outcome.leader_grpc_uri.as_deref() else {
                return Ok(outcome);
            };
            self.remember_known_server(leader_grpc_uri).await;
            match self.reconnect(leader_grpc_uri).await {
                Ok(()) => {}
                Err(err) => match self.recover_session().await {
                    Ok(true) => {}
                    Ok(false) => return Err(err),
                    Err(recover_err) => return Err(recover_err),
                },
            }
        }

        Ok(CommandOutcome {
            success: false,
            kind: CommandOutcomeKind::Error,
            message: "upload redirect loop exceeded".to_string(),
            diagnostics: Vec::new(),
            leader: None,
            leader_grpc_uri: None,
            already_existed: false,
            transaction_active: None,
            results: Vec::new(),
        })
    }

    async fn reconnect(&self, server: &str) -> Result<(), ClientError> {
        let channel = self.inner.grpc_connector.connect(server).await?;
        let (request_tx, response_task) = start_session(
            channel,
            self.inner.grpc_connector.options.basic_authorization(),
            self.inner.pending.clone(),
            self.inner.subscription_tx.clone(),
            self.inner.server_tx.clone(),
            self.inner.domain_tx.clone(),
        )
        .await?;
        *self.inner.current_server.lock().await = Some(server.to_string());
        self.remember_known_server(server).await;
        *self.inner.request_tx.lock().await = request_tx;
        if let Some(task) = self.inner.response_task.lock().await.replace(response_task) {
            task.abort();
        }
        Ok(())
    }

    async fn recover_session(&self) -> Result<bool, ClientError> {
        let current_server = self.inner.current_server.lock().await.clone();
        let known_servers = self.inner.known_servers.lock().await.clone();
        let candidates = reconnect_candidates(&known_servers, current_server.as_deref());
        if candidates.is_empty() {
            return Ok(false);
        }
        let mut last_error = None;
        for server in candidates {
            match self.reconnect(&server).await {
                Ok(()) => return Ok(true),
                Err(err) => last_error = Some(err),
            }
        }
        match last_error {
            Some(err) => Err(err),
            None => Ok(false),
        }
    }

    async fn remember_known_server(&self, server: &str) {
        let mut known_servers = self.inner.known_servers.lock().await;
        if !known_servers.iter().any(|known| known == server) {
            known_servers.push(server.to_string());
        }
    }
}

async fn start_session(
    channel: Channel,
    authorization: Option<String>,
    pending: Arc<Mutex<VecDeque<PendingResponse>>>,
    subscription_tx: mpsc::Sender<SubscriptionEvent>,
    server_tx: mpsc::Sender<ServerEvent>,
    domain_tx: mpsc::Sender<Vec<DomainInfo>>,
) -> Result<(mpsc::Sender<SessionRequest>, JoinHandle<()>), ClientError> {
    let mut client = SessionServiceClient::new(channel);
    let (request_tx, request_rx) = mpsc::channel(32);
    let mut response = client
        .session(request_with_auth(
            ReceiverStream::new(request_rx),
            authorization,
        )?)
        .await
        .map_err(|status| ClientError::StartSession(Box::new(status)))?
        .into_inner();
    let response_task = tokio::spawn(async move {
        while let Ok(Some(session_response)) = response.message().await {
            tokio::task::consume_budget().await;
            match session_response.event {
                Some(proto::session_response::Event::Subscription(event)) => {
                    match subscription_tx.send(event.into()).await {
                        Ok(()) => {}
                        Err(_) => break,
                    }
                }
                Some(proto::session_response::Event::Server(event)) => {
                    match server_tx.send(event.into()).await {
                        Ok(()) => {}
                        Err(_) => break,
                    }
                }
                Some(proto::session_response::Event::Result(result)) => {
                    let pending = pending.lock().await.pop_front();
                    if let Some(PendingResponse::Command(tx)) = pending {
                        let _ = tx.send(result.into());
                    }
                }
                Some(proto::session_response::Event::Domains(domains)) => {
                    let response_to_request = domains.response_to_request;
                    let domains = domains.domains.into_iter().map(Into::into).collect();
                    if response_to_request {
                        let pending = pending.lock().await.pop_front();
                        if let Some(PendingResponse::DomainList(tx)) = pending {
                            let _ = tx.send(domains);
                        }
                    } else if domain_tx.send(domains).await.is_err() {
                        break;
                    }
                }
                #[cfg(feature = "autocomplete")]
                Some(proto::session_response::Event::Suggest(suggest)) => {
                    let pending = pending.lock().await.pop_front();
                    if let Some(PendingResponse::Suggest(tx)) = pending {
                        let suggestions = suggest
                            .suggestions
                            .into_iter()
                            .map(|suggestion| AutocompleteSuggestion {
                                value: suggestion.value,
                                kind: match proto::SuggestionKind::try_from(suggestion.kind).ok() {
                                    Some(proto::SuggestionKind::LocalDirectoryLookup) => {
                                        SuggestionKind::LocalDirectoryLookup
                                    }
                                    _ => SuggestionKind::Text,
                                },
                            })
                            .collect();
                        let _ = tx.send(suggestions);
                    }
                }
                #[cfg(not(feature = "autocomplete"))]
                Some(proto::session_response::Event::Suggest(_)) => {}
                Some(proto::session_response::Event::Snapshot(_)) => {}
                None => {}
            }
        }
        clear_pending_responses(&pending).await;
    });
    Ok((request_tx, response_task))
}

fn request_with_auth<T>(
    message: T,
    authorization: Option<String>,
) -> Result<Request<T>, ClientError> {
    let mut request = Request::new(message);
    if let Some(authorization) = authorization {
        let value = MetadataValue::from_str(&authorization)
            .map_err(|_| ClientError::BuildAuthenticationMetadata)?;
        request.metadata_mut().insert("authorization", value);
    }
    Ok(request)
}

async fn clear_pending_responses(pending: &Arc<Mutex<VecDeque<PendingResponse>>>) {
    pending.lock().await.clear();
}

fn expand_user_path(path: &Path) -> PathBuf {
    let Some(raw) = path.to_str() else {
        return path.to_path_buf();
    };
    if raw == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| path.to_path_buf());
    }
    if let Some(stripped) = raw.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(stripped);
    }
    path.to_path_buf()
}

async fn relay_upload_archive(directory: &Path, writer: DuplexStream) -> Result<(), ClientError> {
    let entries = collect_upload_entries(directory)?;
    let mut builder = AsyncTarBuilder::new(writer);
    builder.mode(HeaderMode::Deterministic);
    let mut result = Ok(());

    for entry in entries {
        tokio::task::consume_budget().await;
        let mut header = Header::new_ustar();
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        let write_result = match entry {
            UploadArchiveEntry::Directory { relative } => {
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(EntryType::Directory);
                header.set_cksum();
                builder
                    .append_data(&mut header, &relative, tokio::io::empty())
                    .await
                    .map_err(|_| ClientError::BuildUploadArchive)
            }
            UploadArchiveEntry::File {
                full_path,
                relative,
                size,
            } => {
                header.set_size(size);
                header.set_mode(0o644);
                header.set_entry_type(EntryType::Regular);
                header.set_cksum();
                let file = File::open(&full_path)
                    .await
                    .map_err(|_| ClientError::BuildUploadArchive)?;
                builder
                    .append_data(&mut header, &relative, file)
                    .await
                    .map_err(|_| ClientError::BuildUploadArchive)
            }
        };

        if let Err(error) = write_result {
            result = Err(error);
            break;
        }
    }

    let mut writer = builder
        .into_inner()
        .await
        .map_err(|_| ClientError::BuildUploadArchive)?;
    let shutdown_result = writer
        .shutdown()
        .await
        .map_err(|_| ClientError::BuildUploadArchive);
    result?;
    shutdown_result
}

enum UploadArchiveEntry {
    Directory {
        relative: PathBuf,
    },
    File {
        full_path: PathBuf,
        relative: PathBuf,
        size: u64,
    },
}

fn collect_upload_entries(directory: &Path) -> Result<Vec<UploadArchiveEntry>, ClientError> {
    let mut entries = Vec::new();
    collect_upload_entries_recursive(directory, directory, &mut entries)?;
    Ok(entries)
}

fn collect_upload_entries_recursive(
    root: &Path,
    current: &Path,
    entries: &mut Vec<UploadArchiveEntry>,
) -> Result<(), ClientError> {
    let mut directory_entries = std::fs::read_dir(current)
        .map_err(|_| ClientError::BuildUploadArchive)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ClientError::BuildUploadArchive)?;
    directory_entries.sort_by_key(|entry| entry.file_name());
    for entry in directory_entries {
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .map_err(|_| ClientError::BuildUploadArchive)?
            .to_path_buf();
        let file_type = entry
            .file_type()
            .map_err(|_| ClientError::BuildUploadArchive)?;
        if file_type.is_dir() {
            entries.push(UploadArchiveEntry::Directory { relative });
            collect_upload_entries_recursive(root, &path, entries)?;
        } else if file_type.is_file() {
            let size = std::fs::metadata(&path)
                .map_err(|_| ClientError::BuildUploadArchive)?
                .len();
            entries.push(UploadArchiveEntry::File {
                full_path: path,
                relative,
                size,
            });
        }
    }
    Ok(())
}

fn reconnect_candidates(known_servers: &[String], current_server: Option<&str>) -> Vec<String> {
    let mut candidates = known_servers
        .iter()
        .filter(|server| Some(server.as_str()) != current_server)
        .cloned()
        .collect::<Vec<_>>();
    if let Some(current_server) = current_server
        && !candidates.iter().any(|server| server == current_server)
    {
        candidates.push(current_server.to_string());
    }
    candidates
}

fn format_domain_list(domains: &[DomainInfo]) -> String {
    if domains.is_empty() {
        return "no domains registered".to_string();
    }
    let mut lines = vec!["domains:".to_string()];
    lines.extend(domains.iter().map(|domain| {
        format!(
            "{} pace={} status={}",
            domain.id, domain.pace, domain.status
        )
    }));
    lines.join("\n")
}

fn command_ok_outcome(message: String) -> CommandOutcome {
    CommandOutcome {
        success: true,
        kind: CommandOutcomeKind::Ok,
        message,
        diagnostics: Vec::new(),
        leader: None,
        leader_grpc_uri: None,
        already_existed: false,
        transaction_active: None,
        results: Vec::new(),
    }
}

fn command_error_outcome(message: String) -> CommandOutcome {
    CommandOutcome {
        success: false,
        kind: CommandOutcomeKind::Error,
        message,
        diagnostics: Vec::new(),
        leader: None,
        leader_grpc_uri: None,
        already_existed: false,
        transaction_active: None,
        results: Vec::new(),
    }
}

impl SubscriptionRequest {
    pub fn new(name: impl Into<String>, relay: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            relay: relay.into(),
            delivery_behavior: SubscriptionDeliveryBehavior::Blocking,
            batch_sample_rate: None,
            filter_map: None,
        }
    }

    pub fn blocking(mut self) -> Self {
        self.delivery_behavior = SubscriptionDeliveryBehavior::Blocking;
        self
    }

    pub fn dropping(mut self) -> Self {
        self.delivery_behavior = SubscriptionDeliveryBehavior::Dropping;
        self
    }

    pub fn with_batch_sample_rate(mut self, batch_sample_rate: impl Into<String>) -> Self {
        self.batch_sample_rate = Some(batch_sample_rate.into());
        self
    }

    pub fn with_filter_map(mut self, filter_map: impl Into<String>) -> Self {
        self.filter_map = Some(filter_map.into());
        self
    }

    pub fn to_query(&self) -> String {
        nervix_nspl::subscribe::create_subscription_query(
            &self.name,
            &self.relay,
            self.delivery_behavior,
            self.batch_sample_rate.as_deref(),
            self.filter_map.as_deref(),
        )
    }
}

impl From<proto::Diagnostic> for Diagnostic {
    fn from(value: proto::Diagnostic) -> Self {
        Self {
            message: value.message,
            span_start: value.span_start,
            span_end: value.span_end,
        }
    }
}

impl From<proto::CommandResult> for CommandOutcome {
    fn from(value: proto::CommandResult) -> Self {
        Self {
            success: value.success,
            kind: CommandOutcomeKind::from_i32(value.kind),
            message: value.message,
            diagnostics: value.diagnostics.into_iter().map(Into::into).collect(),
            leader: (!value.leader.is_empty()).then_some(value.leader),
            leader_grpc_uri: (!value.leader_grpc_uri.is_empty()).then_some(value.leader_grpc_uri),
            already_existed: value.already_existed,
            transaction_active: value.transaction_active,
            results: value.results.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<proto::SubscriptionEvent> for SubscriptionEvent {
    fn from(value: proto::SubscriptionEvent) -> Self {
        Self {
            subscription: value.subscription,
            relay: value.relay,
            payload: value.payload,
        }
    }
}

impl From<proto::ServerEvent> for ServerEvent {
    fn from(value: proto::ServerEvent) -> Self {
        Self {
            level: ServerEventLevel::from_i32(value.level),
            message: value.message,
        }
    }
}

impl From<proto::DomainInfo> for DomainInfo {
    fn from(value: proto::DomainInfo) -> Self {
        Self {
            id: value.id,
            pace: value.pace,
            status: value.status,
        }
    }
}

impl ServerEventLevel {
    fn from_i32(value: i32) -> Self {
        match proto::ServerEventLevel::try_from(value) {
            Ok(proto::ServerEventLevel::Info) => Self::Info,
            Ok(proto::ServerEventLevel::Warn) => Self::Warn,
            Ok(proto::ServerEventLevel::Error) => Self::Error,
            Ok(proto::ServerEventLevel::Unspecified) | Err(_) => Self::Unspecified,
        }
    }
}

impl CommandOutcomeKind {
    fn from_i32(value: i32) -> Self {
        match proto::CommandResultKind::try_from(value) {
            Ok(proto::CommandResultKind::Ok) => Self::Ok,
            Ok(proto::CommandResultKind::Error) => Self::Error,
            Ok(proto::CommandResultKind::NotLeader) => Self::NotLeader,
            Ok(proto::CommandResultKind::Unspecified) | Err(_) => Self::Unspecified,
        }
    }
}

impl fmt::Display for ServerEventLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        };
        f.write_str(label)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        path::{Path, PathBuf},
    };

    use tokio::sync::{Mutex, mpsc, oneshot};
    use triomphe::Arc;

    use super::{
        Client, ClientError, ClientInner, CommandOutcome, CommandOutcomeKind, ConnectOptions,
        Diagnostic, GrpcConnector, PendingResponse, ServerEvent, ServerEventLevel,
        SubscriptionEvent, SubscriptionRequest, TlsRequirement, clear_pending_responses,
        expand_user_path, proto, reconnect_candidates,
    };

    fn test_client(domain: &str) -> Client {
        let (request_tx, request_rx) = mpsc::channel(1);
        drop(request_rx);
        let (subscription_tx, subscription_rx) = mpsc::channel(1);
        let (server_tx, server_rx) = mpsc::channel(1);
        let (domain_tx, domain_rx) = mpsc::channel(1);

        Client {
            inner: Arc::new(ClientInner {
                domain: Mutex::new(domain.to_string()),
                current_server: Mutex::new(None),
                known_servers: Mutex::new(Vec::new()),
                grpc_connector: GrpcConnector::new(ConnectOptions::default()),
                request_tx: Mutex::new(request_tx),
                pending: Arc::new(Mutex::new(VecDeque::new())),
                command_lock: Mutex::new(()),
                transaction_active: Mutex::new(false),
                response_task: Mutex::new(None),
                subscription_tx,
                subscription_rx: Mutex::new(subscription_rx),
                server_tx,
                server_rx: Mutex::new(server_rx),
                domain_tx,
                domain_rx: Mutex::new(domain_rx),
            }),
        }
    }

    #[tokio::test]
    async fn connect_rejects_plain_server_when_tls_is_required() {
        let error = GrpcConnector::new(ConnectOptions {
            tls_requirement: Some(TlsRequirement::Required),
            ca_certificate_pem: None,
            username: None,
            password: None,
        })
        .connect("http://127.0.0.1:47391")
        .await
        .expect_err("plain server should be rejected when tls is required");
        assert!(matches!(error, ClientError::TlsRequired));
    }

    #[test]
    fn subscription_query_is_rendered() {
        let request = SubscriptionRequest::new("live_orders", "orders");
        assert_eq!(
            request.to_query(),
            "CREATE SUBSCRIPTION live_orders TO orders;"
        );

        let filtered = SubscriptionRequest::new("acme_orders", "orders")
            .with_filter_map("SET seen = true UNSET raw WHERE tenant = \"acme\"");
        assert_eq!(
            filtered.to_query(),
            "CREATE SUBSCRIPTION acme_orders TO orders SET seen = true UNSET raw WHERE tenant = \
             \"acme\";"
        );

        let sampled = SubscriptionRequest::new("sampled_orders", "orders")
            .dropping()
            .with_batch_sample_rate("0.1")
            .with_filter_map("WHERE tenant = \"acme\"");
        assert_eq!(
            sampled.to_query(),
            "CREATE SUBSCRIPTION sampled_orders TO orders DROPPING BATCH SAMPLE RATE 0.1 WHERE \
             tenant = \"acme\";"
        );

        assert_eq!(
            nervix_nspl::subscribe::delete_subscription_query("sampled_orders"),
            "DELETE SUBSCRIPTION sampled_orders;"
        );
    }

    #[test]
    fn client_domain_can_be_updated() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let client = test_client("tenant_a");
        runtime.block_on(async {
            assert_eq!(client.domain().await, "tenant_a");
            client.set_domain("tenant_b").await;
            assert_eq!(client.domain().await, "tenant_b");
        });
    }

    #[test]
    fn server_event_level_display_uses_expected_labels() {
        assert_eq!(ServerEventLevel::Unspecified.to_string(), "UNSPECIFIED");
        assert_eq!(ServerEventLevel::Info.to_string(), "INFO");
        assert_eq!(ServerEventLevel::Warn.to_string(), "WARN");
        assert_eq!(ServerEventLevel::Error.to_string(), "ERROR");
    }

    #[test]
    fn proto_values_convert_into_public_types() {
        let diagnostic = Diagnostic::from(proto::Diagnostic {
            message: "bad token".to_string(),
            span_start: 3,
            span_end: 7,
        });
        assert_eq!(
            diagnostic,
            Diagnostic {
                message: "bad token".to_string(),
                span_start: 3,
                span_end: 7,
            }
        );

        let outcome = CommandOutcome::from(proto::CommandResult {
            success: false,
            message: "parse failed".to_string(),
            diagnostics: vec![proto::Diagnostic {
                message: "bad token".to_string(),
                span_start: 3,
                span_end: 7,
            }],
            kind: proto::CommandResultKind::NotLeader as i32,
            leader: "node-2".to_string(),
            leader_grpc_uri: "http://127.0.0.1:47393".to_string(),
            already_existed: true,
            leader_web_console_uri: String::new(),
            results: Vec::new(),
            transaction_active: Some(true),
        });
        assert_eq!(outcome.success, false);
        assert_eq!(outcome.kind, CommandOutcomeKind::NotLeader);
        assert_eq!(outcome.message, "parse failed");
        assert_eq!(outcome.diagnostics.len(), 1);
        assert_eq!(outcome.leader.as_deref(), Some("node-2"));
        assert_eq!(
            outcome.leader_grpc_uri.as_deref(),
            Some("http://127.0.0.1:47393")
        );
        assert!(outcome.already_existed);
        assert_eq!(outcome.transaction_active, Some(true));

        let subscription = SubscriptionEvent::from(proto::SubscriptionEvent {
            subscription: "sub_orders".to_string(),
            relay: "orders".to_string(),
            payload: "{\"id\":42}".to_string(),
        });
        assert_eq!(subscription.subscription, "sub_orders");
        assert_eq!(subscription.relay, "orders");
        assert_eq!(subscription.payload, "{\"id\":42}");

        let server = ServerEvent::from(proto::ServerEvent {
            level: proto::ServerEventLevel::Warn as i32,
            message: "watch out".to_string(),
        });
        assert_eq!(
            server,
            ServerEvent {
                level: ServerEventLevel::Warn,
                message: "watch out".to_string(),
            }
        );
        assert_eq!(
            ServerEvent::from(proto::ServerEvent {
                level: 999,
                message: "unknown".to_string(),
            })
            .level,
            ServerEventLevel::Unspecified
        );
    }

    #[test]
    fn reconnect_candidates_prefer_non_current_servers() {
        let candidates = reconnect_candidates(
            &[
                "http://node-1".to_string(),
                "http://node-2".to_string(),
                "http://node-3".to_string(),
            ],
            Some("http://node-2"),
        );
        assert_eq!(
            candidates,
            vec![
                "http://node-1".to_string(),
                "http://node-3".to_string(),
                "http://node-2".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn clear_pending_responses_drops_waiters() {
        let pending = Arc::new(Mutex::new(VecDeque::new()));
        let (command_tx, command_rx) = oneshot::channel();
        pending
            .lock()
            .await
            .push_back(PendingResponse::Command(command_tx));
        clear_pending_responses(&pending).await;
        assert!(pending.lock().await.is_empty());
        assert!(command_rx.await.is_err(), "sender should be dropped");
    }

    #[tokio::test]
    async fn execute_returns_session_closed_when_request_channel_is_closed() {
        let client = test_client("tenant_a");
        let err = client
            .execute("SHOW CLUSTER STATUS;")
            .await
            .expect_err("must fail");
        assert!(matches!(err, ClientError::SessionClosed));
    }

    #[tokio::test]
    async fn execute_rejects_mixed_client_local_multi_statement_request() {
        let client = test_client("default");
        let outcome = client
            .execute("USE prod; CREATE DOMAIN prod;")
            .await
            .expect("client-local multi-statement rejection should not use network");

        assert!(!outcome.success);
        assert_eq!(
            outcome.message,
            "client-local commands must be executed separately"
        );
    }

    #[tokio::test]
    async fn execute_rejects_client_local_command_during_transaction() {
        let client = test_client("default");
        *client.inner.transaction_active.lock().await = true;

        let outcome = client
            .execute("USE prod;")
            .await
            .expect("client-local transaction rejection should not use network");

        assert!(!outcome.success);
        assert_eq!(
            outcome.message,
            "client-local commands are not allowed while a transaction is active"
        );
    }

    #[tokio::test]
    async fn next_event_calls_return_session_closed_when_channels_are_closed() {
        let client = test_client("tenant_a");
        client.inner.subscription_rx.lock().await.close();
        client.inner.server_rx.lock().await.close();
        let subscription_err = client
            .next_subscription()
            .await
            .expect_err("must fail once channel is closed");
        assert!(matches!(subscription_err, ClientError::SessionClosed));

        let server_err = client
            .next_server_event()
            .await
            .expect_err("must fail once channel is closed");
        assert!(matches!(server_err, ClientError::SessionClosed));
    }

    #[cfg(feature = "autocomplete")]
    #[tokio::test]
    async fn suggest_returns_session_closed_when_request_channel_is_closed() {
        let client = test_client("tenant_a");
        let err = client.suggest("CREATE ", 7).await.expect_err("must fail");
        assert!(matches!(err, ClientError::SessionClosed));
    }

    #[test]
    fn expand_user_path_resolves_home_prefix() {
        let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
            return;
        };
        assert_eq!(expand_user_path(Path::new("~")), home);
        assert_eq!(expand_user_path(Path::new("~/proto")), home.join("proto"));
        assert_eq!(
            expand_user_path(Path::new("/tmp/proto")),
            PathBuf::from("/tmp/proto")
        );
    }
}
