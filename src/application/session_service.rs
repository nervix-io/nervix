//! The gRPC session every client statement arrives on.
//!
//! Layer: edges.
//!
//! - **Owns.** The session stream, the command and suggestion requests it carries, the resource
//!   upload RPCs, and the events a session observes.
//! - **Depends on.** The control-plane use cases it dispatches to, and the parser for the text a
//!   client sends.
//! - **Must not know.** How any use case reaches the rest of the cluster.

use std::sync::{Arc as StdArc, atomic::AtomicU64};

use ahash::RandomState;
use arch_into::ArchInto;
use dashmap::DashMap;
use nervix_consensus::{Administrator, ConsensusError, ConsensusRuntimeState, Proposer};
use nervix_interconnect::Transport;
use nervix_models::{
    ClusterNodeName, DomainName, DomainStatus, ModelKind, ModelName, ResourceId, ResourceName,
    ResourceUploadIdentity, ResourceUploadKey,
};
use nervix_nspl::{
    Token, Word,
    client_statement::{
        ClientStatement, parse_client_statement_sources, suggest_client_statement,
        upload_resource_path_fragment,
    },
    lex,
    schema::{Diagnostic as ParseDiagnostic, ParseFromSourceError},
};
use nervix_recovery::{Discarded, NoReceiver};
use parking_lot::RwLock;
use rustls::ServerConfig;
use sorted_vec::SortedSet;
use tokio::{
    sync::{Mutex as AsyncMutex, broadcast, mpsc},
    time::Duration,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::{Request, Response, Status};
use tracing::warn;
use triomphe::Arc;

use super::{
    authentication::{AuthRateLimiter, BasicAuthCredentials},
    describe_output::placement_runtime_node_ref_suggestions,
    model_mutation::{RequestDomainError, command_error, parse_request_domain},
    peer_grpc::grpc_uri_from_advertise_addr,
    resource::{
        ResourceUploadError, requested_resource_versions, resource_ref_suggestions,
        resource_version_suggestions,
    },
    scheduling::RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
    subscription::{SessionSubscriptions, SubscriptionInterestKey},
};
use crate::{
    cluster, proto,
    proto::{
        CommandRequest, CommandResult, CommandResultKind, Diagnostic, ServerEvent,
        ServerEventLevel, SessionRequest, SessionResponse, SuggestRequest, SuggestResponse,
        Suggestion as ApiSuggestion, SuggestionKind, UploadResourceRequest, UploadResourceResponse,
        WaitForResourceReadyRequest, WaitForResourceReadyResponse,
        session_service_server::SessionService,
    },
    registry::{Registry, RegistryError},
    resource::ResourceStore,
    runtime::{Runtime, RuntimeEvent},
};
/// How many events a session can fall behind before the bus drops the oldest.
pub(in crate::application) const SESSION_EVENT_CAPACITY: usize = 256;

/// The session event bus, and the one way a control-plane failure or a cluster transition reaches
/// the sessions attached to this node.
///
/// Unlike the runtime event bus this one carries no fan-out task, so its only subscribers are live
/// sessions, and a node serving none is the ordinary case rather than a startup window. An event
/// that finds no receiver is therefore expected, which is why publishing goes through these
/// methods: each one leaves a record that does not depend on anyone listening, and the send that
/// follows only offers the same fact to whoever is.
#[derive(Clone)]
pub(in crate::application) struct SessionEvents {
    sender: broadcast::Sender<ServerEvent>,
}

impl SessionEvents {
    pub(in crate::application) fn new(capacity: usize) -> Self {
        Self {
            sender: broadcast::channel(capacity).0,
        }
    }

    /// Report a control-plane failure this node recovered from.
    fn report_error(&self, message: impl Into<String>) {
        let message = message.into();
        warn!(error = %message, "server error reported to sessions");
        self.publish(ServerEventLevel::Error, message);
    }

    /// Offer a transition the cluster or consensus bus has already recorded.
    ///
    /// Those buses write their own `info` line before handing the text here, so this is a relay
    /// rather than a report and it logs nothing of its own.
    pub(in crate::application) fn relay_info(&self, message: String) {
        self.publish(ServerEventLevel::Info, message);
    }

    fn publish(&self, level: ServerEventLevel, message: String) {
        self.sender
            .send(ServerEvent {
                level: i32::from(level),
                message,
            })
            .discarded("the record this event carries is written before it is offered");
    }

    fn subscribe(&self) -> broadcast::Receiver<ServerEvent> {
        self.sender.subscribe()
    }
}

/// The handle every gRPC request, background reconciliation task, and HTTP server clones. It is
/// one `Arc` over the server's state, so handing the service to a spawned task costs a single
/// refcount rather than one per piece of state the server owns.
#[derive(Clone)]
pub(in crate::application) struct SessionServiceImpl {
    pub(in crate::application) inner: Arc<SessionServiceInner>,
}

/// Everything one Nervix server owns for as long as it serves. These fields are reached only
/// through a `SessionServiceImpl` handle and therefore hold their values directly. The ones that
/// keep an `Arc` of their own have a second owner outside the service, and each names it.
pub(in crate::application) struct SessionServiceInner {
    /// Started and shut down by the application, which outlives the service handle.
    pub(in crate::application) cluster: Arc<cluster::ClusterHandle>,
    /// Proposal authority and local observation; leadership is checked for each operation.
    pub(in crate::application) consensus: Proposer,
    /// Membership changes requested by authenticated cluster commands.
    pub(in crate::application) consensus_administrator: Administrator,
    /// Also held by the application and by the registry reconciliation tasks it spawns.
    pub(in crate::application) registry: Arc<Registry>,
    /// Also held by the application startup that opened it.
    pub(in crate::application) resource_store: Arc<ResourceStore>,
    /// Also held by the HTTPS server, which reads the current certificate on every accept.
    pub(in crate::application) http_tls_server_config: Arc<RwLock<Option<StdArc<ServerConfig>>>>,
    pub(in crate::application) runtime: Runtime,
    pub(in crate::application) replica_count: usize,
    pub(in crate::application) shutdown: CancellationToken,
    pub(in crate::application) events: SessionEvents,
    pub(in crate::application) subscription_interest_counts:
        DashMap<SubscriptionInterestKey, usize, RandomState>,
    pub(in crate::application) interconnect: Transport,
    pub(in crate::application) next_entity_gate_operation_id: AtomicU64,
    pub(in crate::application) service_tasks: TaskTracker,
    pub(in crate::application) configured_basic_auth: Option<BasicAuthCredentials>,
    pub(in crate::application) auth_rate_limiter: AuthRateLimiter,
    pub(in crate::application) failed_auth_rate_limit_keys: DashMap<String, (), RandomState>,
    pub(in crate::application) transaction_idle_timeout: Duration,
    pub(in crate::application) transaction_tombstone_retention: Duration,
    pub(in crate::application) transaction_max_statements: usize,
    pub(in crate::application) transaction_max_source_bytes: u64,
    pub(in crate::application) transaction_max_open: usize,
    pub(in crate::application) transaction_bindings: DashMap<String, String, RandomState>,
    /// Also held by every outstanding `TransactionExecutionLease`, which clears its entry on drop.
    pub(in crate::application) transaction_executions: Arc<DashMap<String, (), RandomState>>,
    pub(in crate::application) transaction_commit_execution: AsyncMutex<()>,
    /// Also held by a request while it installs. Calls with one durable identity share the lock,
    /// so only one of them can build and publish that assigned version on this leader.
    pub(in crate::application) resource_upload_executions:
        DashMap<ResourceUploadKey, StdArc<AsyncMutex<()>>, RandomState>,
    /// A reconciliation request holds this lock through download, verification and promotion.
    /// Repeated observations of the same missing version join that one installation.
    pub(in crate::application) resource_replication_executions:
        DashMap<ResourceId, StdArc<AsyncMutex<()>>, RandomState>,
}

#[tonic::async_trait]
impl SessionService for SessionServiceImpl {
    type SessionStream = ReceiverStream<Result<SessionResponse, Status>>;

    async fn session(
        &self,
        request: Request<tonic::Streaming<SessionRequest>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let authenticated_user = self.authenticate_grpc_metadata(request.metadata()).await?;
        let mut inbound = request.into_inner();
        let service = self.clone();
        let (tx, rx) = mpsc::channel(16);
        let mut event_rx = self.inner.events.subscribe();
        let mut runtime_event_rx = self.inner.runtime.subscribe_events();

        let service_tasks = service.inner.service_tasks.clone();
        service_tasks.spawn(async move {
            let mut subscriptions = SessionSubscriptions::for_user(authenticated_user);
            let mut clean_close = false;
            let shutdown = service.inner.shutdown.clone();
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        break;
                    }
                    inbound_request = tokio_stream::StreamExt::next(&mut inbound) => {
                        let Some(request) = inbound_request else {
                            clean_close = true;
                            break;
                        };
                        let request = match request {
                            Ok(request) => request,
                            Err(status) => {
                                tx.send(Err(status))
                                    .await
                                    .means_peer_left("session response stream");
                                subscriptions.stop_all(&service).await;
                                service.release_session_transaction_binding(&mut subscriptions);
                                return;
                            }
                        };

                        match request.request {
                            Some(proto::session_request::Request::Command(command)) => {
                                let result = service
                                    .process_command(
                                        command,
                                        &tx,
                                        &mut subscriptions,
                                    )
                                    .await;
                                let event = SessionResponse {
                                    event: Some(proto::session_response::Event::Result(result)),
                                };
                                if tx.send(Ok(event)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            Some(proto::session_request::Request::Suggest(suggest)) => {
                                let response = service
                                    .process_suggest(suggest, &subscriptions)
                                    .await;
                                let event = SessionResponse {
                                    event: Some(proto::session_response::Event::Suggest(response)),
                                };
                                if tx.send(Ok(event)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            Some(proto::session_request::Request::ListDomains(_)) => {
                                let event = service.domain_list_response(true).await;
                                if tx.send(Ok(event)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            Some(proto::session_request::Request::SetActiveDomain(_)) => {
                                tx.send(Err(Status::invalid_argument(
                                    "active domain selection is only supported by the web console \
                                     websocket",
                                )))
                                .await
                                .means_peer_left("session response stream");
                                subscriptions.stop_all(&service).await;
                                service.release_session_transaction_binding(&mut subscriptions);
                                return;
                            }
                            Some(proto::session_request::Request::AttachTransaction(request)) => {
                                let result = service
                                    .attach_transaction(request, &mut subscriptions)
                                    .await;
                                let event = SessionResponse {
                                    event: Some(proto::session_response::Event::Result(result)),
                                };
                                if tx.send(Ok(event)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            None => {
                                tx.send(Err(Status::invalid_argument(
                                    "session request payload is missing",
                                )))
                                .await
                                .means_peer_left("session response stream");
                                subscriptions.stop_all(&service).await;
                                service.release_session_transaction_binding(&mut subscriptions);
                                return;
                            }
                        }
                    }
                    server_event = event_rx.recv() => {
                        match server_event {
                            Ok(event) => {
                                let response = SessionResponse {
                                    event: Some(proto::session_response::Event::Server(event)),
                                };
                                if tx.send(Ok(response)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            // The session stays open and resumes from the newest event. Saying how
                            // many it skipped is what stops the gap from looking like quiet.
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                warn!(skipped, "session fell behind the server event bus");
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    runtime_event = runtime_event_rx.recv() => {
                        match runtime_event {
                            Ok(RuntimeEvent::Error(message)) => {
                                let response = SessionResponse {
                                    event: Some(proto::session_response::Event::Server(ServerEvent {
                                        level: i32::from(ServerEventLevel::Error),
                                        message,
                                    })),
                                };
                                if tx.send(Ok(response)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            // As above: the runtime errors the session missed are gone, so the
                            // count is the only record that they happened.
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                warn!(skipped, "session fell behind the runtime event bus");
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }

            subscriptions.stop_all(&service).await;
            if clean_close {
                service.clean_close_transaction(&mut subscriptions).await;
            } else {
                service.release_session_transaction_binding(&mut subscriptions);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn upload_resource(
        &self,
        request: Request<tonic::Streaming<UploadResourceRequest>>,
    ) -> Result<Response<UploadResourceResponse>, Status> {
        let authenticated_user = self.authenticate_grpc_metadata(request.metadata()).await?;
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            let leader_node = match leader.as_ref() {
                Some(leader_id) => self
                    .inner
                    .cluster
                    .gossip_state()
                    .await
                    .live_nodes
                    .into_iter()
                    .find(|node| node.node_id == *leader_id),
                None => None,
            };
            let mut leader_grpc_uri = String::new();
            if let Some(node) = leader_node
                && let Some(uri) = grpc_uri_from_advertise_addr(&node.grpc_advertise_addr)
            {
                leader_grpc_uri = uri;
            }
            return Ok(Response::new(UploadResourceResponse {
                success: false,
                message: "resource uploads must be sent to the cluster leader".to_string(),
                version: 0,
                diagnostics: Vec::new(),
                kind: i32::from(CommandResultKind::NotLeader),
                leader: match leader {
                    Some(leader) => leader.to_string(),
                    None => String::new(),
                },
                leader_grpc_uri,
                published: false,
                cluster_ready: false,
            }));
        }

        let mut inbound = request.into_inner();
        let Some(first) = inbound.message().await? else {
            return Err(Status::invalid_argument(
                "upload resource request relay is empty",
            ));
        };
        let Some(proto::upload_resource_request::Event::Start(start)) = first.event else {
            return Err(Status::invalid_argument(
                "upload resource relay must start with metadata",
            ));
        };
        let identifier = ModelName::parse(&start.name)
            .map_err(|_| Status::invalid_argument("upload resource name is invalid"))?;
        let upload_identity = ResourceUploadIdentity::parse(start.upload_identity)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let domain = match parse_request_domain(&start.domain) {
            Ok(domain) => domain,
            Err(RequestDomainError::Missing) => {
                return Err(Status::invalid_argument("no active domain selected"));
            }
            Err(RequestDomainError::Invalid) => {
                return Err(Status::invalid_argument(
                    "upload resource domain is invalid",
                ));
            }
        };

        let resources = self.inner.consensus.current_resources().await;
        if !resources.is_declared(&domain, &ResourceName::from(&identifier)) {
            return Ok(Response::new(UploadResourceResponse {
                success: false,
                message: format!("resource '{}' does not exist", identifier.as_str()),
                version: 0,
                diagnostics: Vec::new(),
                kind: i32::from(CommandResultKind::Error),
                leader: String::new(),
                leader_grpc_uri: String::new(),
                published: false,
                cluster_ready: false,
            }));
        }
        if start.total_bytes == 0 {
            return Err(Status::invalid_argument(
                "upload resource must declare its archive size",
            ));
        }
        self.inner
            .resource_store
            .validate_archive_bytes(start.total_bytes)
            .map_err(|error| Status::resource_exhausted(error.to_string()))?;

        let mut archive = self
            .inner
            .resource_store
            .create_archive_stager()
            .await
            .map_err(|error| {
                Status::internal(format!(
                    "failed to create temporary upload archive: {error}"
                ))
            })?;
        let mut total_received = 0u64;
        while let Some(message) = inbound.message().await? {
            tokio::task::consume_budget().await;
            let Some(proto::upload_resource_request::Event::Chunk(chunk)) = message.event else {
                return Err(Status::invalid_argument(
                    "unexpected upload resource control event",
                ));
            };
            let next_total = total_received
                .checked_add(chunk.len().arch_into())
                .ok_or_else(|| Status::invalid_argument("upload resource archive is too large"))?;
            if next_total > start.total_bytes {
                return Err(Status::invalid_argument(format!(
                    "upload size exceeds declared size {}",
                    start.total_bytes
                )));
            }
            self.inner
                .resource_store
                .validate_archive_bytes(next_total)
                .map_err(|error| Status::resource_exhausted(error.to_string()))?;
            let chunk = self
                .inner
                .resource_store
                .admit_staging_bytes(&chunk)
                .await
                .map_err(|error| Status::resource_exhausted(error.to_string()))?;
            archive.write_chunk(chunk).await.map_err(|error| {
                Status::internal(format!("failed to write upload resource chunk: {error}"))
            })?;
            total_received = next_total;
        }
        let archive = archive.finish().await.map_err(|error| {
            Status::internal(format!("failed to flush upload resource archive: {error}"))
        })?;

        if start.total_bytes != total_received || archive.archive_bytes() != total_received {
            return Ok(Response::new(UploadResourceResponse {
                success: false,
                message: format!(
                    "upload size mismatch: expected {}, received {}",
                    start.total_bytes, total_received
                ),
                version: 0,
                diagnostics: Vec::new(),
                kind: i32::from(CommandResultKind::Error),
                leader: String::new(),
                leader_grpc_uri: String::new(),
                published: false,
                cluster_ready: false,
            }));
        }

        let upload_key = ResourceUploadKey::new(
            authenticated_user,
            domain,
            ResourceName::from(&identifier),
            upload_identity,
        );
        match self
            .install_uploaded_resource_archive(
                upload_key,
                archive.path(),
                archive.root_checksum().to_string(),
            )
            .await
        {
            Ok(publication) => Ok(Response::new(UploadResourceResponse {
                success: true,
                message: format!("published resource version {}", publication.version),
                version: publication.version,
                diagnostics: Vec::new(),
                kind: i32::from(CommandResultKind::Ok),
                leader: String::new(),
                leader_grpc_uri: String::new(),
                published: true,
                cluster_ready: publication.cluster_ready,
            })),
            Err(error) => {
                let assigned_version = match error.downcast_ref::<ResourceUploadError>() {
                    Some(error) => error.assigned_version().unwrap_or(0),
                    None => 0,
                };
                let message = format!("{error:#}");
                let result = match error.downcast_ref::<ConsensusError>() {
                    Some(error) => self.consensus_error_response(error, message).await,
                    None => command_error(message),
                };
                Ok(Response::new(UploadResourceResponse {
                    success: false,
                    message: result.message,
                    version: assigned_version,
                    diagnostics: result.diagnostics,
                    kind: result.kind,
                    leader: result.leader,
                    leader_grpc_uri: result.leader_grpc_uri,
                    published: false,
                    cluster_ready: false,
                }))
            }
        }
    }

    async fn wait_for_resource_ready(
        &self,
        request: Request<WaitForResourceReadyRequest>,
    ) -> Result<Response<WaitForResourceReadyResponse>, Status> {
        let _authenticated_user = self.authenticate_grpc_metadata(request.metadata()).await?;
        let request = request.into_inner();
        let identifier = ResourceName::parse(&request.name)
            .map_err(|_| Status::invalid_argument("resource name is invalid"))?;
        let domain = parse_request_domain(&request.domain)
            .map_err(|_| Status::invalid_argument("resource domain is invalid"))?;
        let id = ResourceId::new(domain, identifier, request.version);
        let resources = self.inner.consensus.current_resources().await;
        if resources.version(&id).is_none() {
            return Err(Status::not_found(format!(
                "resource '{}@{}' is not published",
                id.identifier.as_str(),
                id.version
            )));
        }
        let timeout = Duration::from_millis(request.timeout_millis);
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Status::invalid_argument("resource readiness timeout is too large"))?;
        let cluster_ready = self.wait_for_resource_cluster_ready(&id, deadline).await;
        let message = if cluster_ready {
            format!(
                "resource '{}@{}' is ready on every live node",
                id.identifier.as_str(),
                id.version
            )
        } else {
            format!(
                "resource '{}@{}' is published but not ready on every live node before the \
                 deadline",
                id.identifier.as_str(),
                id.version
            )
        };
        Ok(Response::new(WaitForResourceReadyResponse {
            version: id.version,
            cluster_ready,
            message,
        }))
    }
}

pub(in crate::application) async fn apply_cluster_runtime_state(
    runtime: &Runtime,
    cluster: &cluster::ClusterHandle,
    local_node_id: &ClusterNodeName,
    state: ConsensusRuntimeState,
) -> Result<(), crate::runtime::RuntimeError> {
    let mut cluster_state = cluster.subscribe_state_changes().await;
    let has_running_domain = state
        .domains
        .values()
        .any(|domain| matches!(domain.status, DomainStatus::Running));
    runtime
        .apply_cluster_state(
            local_node_id,
            state.revision,
            &state.domains,
            &state.domain_clock_authorities,
            &state.schedule,
        )
        .await?;
    cluster
        .set_local_runtime_revision_ready(state.revision)
        .await;
    if !has_running_domain {
        return Ok(());
    }

    let node_unavailability_timeout = cluster.node_unavailability_timeout();
    // Applying a revision gets one start-time operation budget: peer-failure detection followed
    // by readiness propagation. A peer that disconnects after this starts has only the remaining
    // portion of that budget.
    let Some(readiness_timeout) =
        node_unavailability_timeout.checked_add(RUNTIME_REVISION_READINESS_PROPAGATION_BOUND)
    else {
        return Err(
            crate::runtime::RuntimeError::RuntimeRevisionReadinessDeadlineOverflow {
                node_unavailability_timeout,
                readiness_propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
            },
        );
    };
    let Some(deadline) = tokio::time::Instant::now().checked_add(readiness_timeout) else {
        return Err(
            crate::runtime::RuntimeError::RuntimeRevisionReadinessDeadlineOverflow {
                node_unavailability_timeout,
                readiness_propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
            },
        );
    };
    let mut deadline_elapsed = false;
    loop {
        tokio::task::consume_budget().await;
        let cluster_change = cluster_state.wait_for_change_or_next_unavailability();
        tokio::pin!(cluster_change);
        let gossip = cluster.availability_state().await;
        let expected_nodes = gossip.live_identities();
        let ready_nodes = cluster
            .nodes_ready_for_runtime_revision(state.revision)
            .await;
        let pending_nodes = expected_nodes
            .difference(&ready_nodes)
            .map(|identity| identity.node_id().clone())
            .collect::<Vec<_>>();
        if pending_nodes.is_empty() {
            break;
        }
        if deadline_elapsed {
            return Err(crate::runtime::RuntimeError::RuntimeRevisionReadiness {
                revision: state.revision,
                pending_nodes,
            });
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => {
                deadline_elapsed = true;
            }
            _ = &mut cluster_change => {}
        }
    }

    runtime.start_running_domain_ingestors().await
}

fn error_response(kind: &str, diagnostics: &[ParseDiagnostic]) -> CommandResult {
    CommandResult {
        success: false,
        message: kind.to_string(),
        diagnostics: diagnostics.iter().map(map_diagnostic).collect(),
        kind: i32::from(CommandResultKind::Error),
        ..Default::default()
    }
}

pub(in crate::application) fn create_registry_error_response(
    query: &str,
    domain: &DomainName,
    model_id: &ModelName,
    err: &error_stack::Report<RegistryError>,
) -> CommandResult {
    match err.current_context() {
        RegistryError::AlreadyExists { .. } => {
            let span = find_identifier_span(query, model_id).unwrap_or(0..0);
            CommandResult {
                success: false,
                message: format!(
                    "{} '{}' already exists in domain '{}'",
                    infer_kind_from_error_target(err, model_id).unwrap_or("model"),
                    model_id.as_str(),
                    domain.as_str()
                ),
                diagnostics: vec![Diagnostic {
                    message: format!("'{}' already exists", model_id.as_str()),
                    span_start: u32::try_from(span.start).unwrap_or(0),
                    span_end: u32::try_from(span.end).unwrap_or(0),
                }],
                kind: i32::from(CommandResultKind::Error),
                ..Default::default()
            }
        }
        RegistryError::NotFound { .. }
        | RegistryError::StoredModelKindMismatch { .. }
        | RegistryError::DeleteInUse { .. }
        | RegistryError::InvalidModel { .. } => {
            let span = find_identifier_span(query, model_id).unwrap_or(0..0);
            CommandResult {
                success: false,
                message: format!("{err}"),
                diagnostics: vec![Diagnostic {
                    message: format!("{err}"),
                    span_start: u32::try_from(span.start).unwrap_or(0),
                    span_end: u32::try_from(span.end).unwrap_or(0),
                }],
                kind: i32::from(CommandResultKind::Error),
                ..Default::default()
            }
        }
        RegistryError::MissingReference { reference, .. } => {
            // A reference that is not a model name has nothing to underline in the query, and a
            // diagnostic without a span is still the diagnostic the operator needs.
            let span = match ModelName::try_from(reference.as_str()) {
                Ok(id) => find_identifier_span(query, &id).unwrap_or(0..0),
                Err(_) => 0..0,
            };
            CommandResult {
                success: false,
                message: format!("{err}"),
                diagnostics: vec![Diagnostic {
                    message: format!("{err}"),
                    span_start: u32::try_from(span.start).unwrap_or(0),
                    span_end: u32::try_from(span.end).unwrap_or(0),
                }],
                kind: i32::from(CommandResultKind::Error),
                ..Default::default()
            }
        }
        _ => CommandResult {
            success: false,
            message: format!("{err}"),
            diagnostics: vec![Diagnostic {
                message: format!("{err}"),
                span_start: 0,
                span_end: 0,
            }],
            kind: i32::from(CommandResultKind::Error),
            ..Default::default()
        },
    }
}

fn infer_kind_from_error_target(
    err: &error_stack::Report<RegistryError>,
    model_id: &ModelName,
) -> Option<&'static str> {
    match err.current_context() {
        RegistryError::AlreadyExists { identifier, .. } if identifier == model_id.as_str() => {
            Some("model")
        }
        _ => None,
    }
}

fn map_diagnostic(d: &ParseDiagnostic) -> Diagnostic {
    Diagnostic {
        message: d.message.clone(),
        span_start: u32::try_from(d.span.start).unwrap_or(u32::MAX),
        span_end: u32::try_from(d.span.end).unwrap_or(u32::MAX),
    }
}

/// Where `identifier` appears in `query`, for a diagnostic that wants to underline it.
///
/// The query reached here because it failed validation, so it may also fail to lex. A diagnostic
/// without a span is still a diagnostic, which is why absence is the answer rather than an error.
pub(in crate::application) fn find_identifier_span(
    query: &str,
    identifier: &ModelName,
) -> Option<std::ops::Range<usize>> {
    let tokens = lex(query).ok()?;
    tokens.into_iter().find_map(|spanned| match spanned.token {
        Token::Word(Word::KnownWord { raw, .. }) | Token::Word(Word::UnknownWord(raw))
            if raw.eq_ignore_ascii_case(identifier.as_str()) =>
        {
            Some(spanned.span.into_range())
        }
        _ => None,
    })
}

pub(in crate::application) fn current_word_prefix(input: &str, cursor: usize) -> String {
    let end = cursor.min(input.len());
    let mut out = String::new();
    for ch in input[..end].chars().rev() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.insert(0, ch.to_ascii_lowercase());
        } else {
            break;
        }
    }
    out
}

/// Completion input split at the cursor: the source the grammar parses with the half-typed word
/// removed, where that word started, and the word itself for filtering the offers.
struct CompletionContext {
    grammar_input: String,
    grammar_cursor: usize,
    prefix: String,
}

fn completion_context(input: &str, cursor: usize) -> CompletionContext {
    let safe_cursor = cursor.min(input.len());
    let start = word_start(input, safe_cursor);
    let prefix = current_word_prefix(input, safe_cursor);

    let mut grammar_input = String::with_capacity(input.len() - (safe_cursor - start));
    grammar_input.push_str(&input[..start]);
    grammar_input.push_str(&input[safe_cursor..]);

    CompletionContext {
        grammar_input,
        grammar_cursor: start,
        prefix,
    }
}

pub(in crate::application) fn word_start(input: &str, cursor: usize) -> usize {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let boundary = input[..cursor.min(input.len())]
        .char_indices()
        .rev()
        .find(|(_, c)| !is_word(*c));
    match boundary {
        Some((index, character)) => index + character.len_utf8(),
        None => 0,
    }
}

impl SessionServiceImpl {
    pub(in crate::application) async fn apply_current_cluster_state(
        &self,
    ) -> Result<(), crate::runtime::RuntimeError> {
        let state = self.inner.consensus.current_runtime_state().await;
        apply_cluster_runtime_state(
            &self.inner.runtime,
            &self.inner.cluster,
            self.inner.consensus.local_node_id(),
            state,
        )
        .await
    }

    /// Report a control-plane failure this node recovered from to the sessions attached to it.
    pub(in crate::application) fn broadcast_error(&self, message: impl Into<String>) {
        self.inner.events.report_error(message);
    }

    pub(in crate::application) async fn process_suggest(
        &self,
        req: SuggestRequest,
        subscriptions: &SessionSubscriptions,
    ) -> SuggestResponse {
        let cursor = req.cursor.arch_into();
        let domain = parse_request_domain(&req.domain).ok();
        let queued = self
            .queued_configuration(subscriptions, domain.as_ref())
            .await;

        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
        } = completion_context(&req.input, cursor);
        let grammar = suggest_client_statement(&grammar_input, grammar_cursor);

        let mut suggestions = Vec::new();
        let mut semantic_kinds = Vec::new();
        let mut expects_resource_ref = false;
        let mut expects_session_subscription_ref = false;
        let mut expects_runtime_node_ref = false;
        let requested_resource_versions = requested_resource_versions(&req.input, cursor);
        for item in &grammar {
            if let Some(kind) = ModelKind::from_completion_label(item) {
                semantic_kinds.push(kind);
            } else if item == "ref:resource" {
                expects_resource_ref = true;
            } else if item == "ref:session_subscription" {
                expects_session_subscription_ref = true;
            } else if item == "ref:runtime_node" {
                expects_runtime_node_ref = true;
            } else if prefix.is_empty()
                || item
                    .to_ascii_lowercase()
                    .starts_with(&prefix.to_ascii_lowercase())
            {
                suggestions.push(item.clone());
            }
        }

        for kind in &semantic_kinds {
            if let Some(domain) = &domain
                && self.inner.consensus.current_domain(domain).await.is_some()
                && let Ok(ids) = self.inner.registry.resulting_identifiers(
                    domain,
                    *kind,
                    &prefix,
                    &queued.models,
                )
            {
                suggestions.extend(ids.into_iter().map(|id| id.to_string()));
            }
        }

        if expects_session_subscription_ref {
            suggestions.extend(subscriptions.matching_names(&prefix));
        }

        if expects_runtime_node_ref
            && let Some(domain) = &domain
            && self.inner.consensus.current_domain(domain).await.is_some()
        {
            suggestions.extend(placement_runtime_node_ref_suggestions(
                &self.inner.registry,
                domain,
                &prefix,
                &queued.models,
            ));
        }

        if let Some(domain) = &domain
            && (expects_resource_ref || requested_resource_versions.is_some())
        {
            let resources = self.inner.consensus.current_resources().await;
            if expects_resource_ref {
                suggestions.extend(resource_ref_suggestions(&resources, domain, &prefix));
                suggestions.extend(queued.resource_suggestions(&prefix));
            }
            if let Some(resource_identifier) = requested_resource_versions.as_ref() {
                suggestions.extend(resource_version_suggestions(
                    &resources,
                    domain,
                    resource_identifier,
                    &prefix,
                ));
            }
        }

        if grammar_input.contains("DOMAIN")
            || (semantic_kinds.is_empty()
                && !expects_resource_ref
                && !expects_session_subscription_ref
                && !expects_runtime_node_ref)
        {
            let domains = self.inner.consensus.current_domains().await;
            for id in domains.into_keys() {
                if prefix.is_empty() || id.as_str().starts_with(&prefix) {
                    suggestions.push(id.to_string());
                }
            }
        }

        let mut response_suggestions = SortedSet::from_unsorted(suggestions)
            .into_vec()
            .into_iter()
            .map(|value| ApiSuggestion {
                value,
                kind: i32::from(SuggestionKind::Text),
            })
            .collect::<Vec<_>>();

        if let Some(fragment) = upload_resource_path_fragment(&req.input, cursor) {
            response_suggestions.push(ApiSuggestion {
                value: fragment.to_string(),
                kind: i32::from(SuggestionKind::LocalDirectoryLookup),
            });
        }

        SuggestResponse {
            suggestions: response_suggestions,
        }
    }

    pub(in crate::application) async fn process_command(
        &self,
        req: CommandRequest,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let client_statements = match parse_client_statement_sources(&req.query) {
            Ok(statements) => statements,
            Err(ParseFromSourceError::Lex { diagnostics, .. }) => {
                return self
                    .command_with_transaction_status(
                        error_response("lex error", &diagnostics),
                        subscriptions,
                    )
                    .await;
            }
            Err(ParseFromSourceError::Parse { diagnostics, .. }) => {
                return self
                    .command_with_transaction_status(
                        error_response("parse error", &diagnostics),
                        subscriptions,
                    )
                    .await;
            }
        };

        let is_transaction_request = subscriptions.transaction_active()
            || client_statements.iter().any(|parsed| {
                matches!(
                    parsed.statement,
                    ClientStatement::BeginTransaction
                        | ClientStatement::CommitTransaction
                        | ClientStatement::RevertTransaction
                )
            });
        if is_transaction_request {
            let leader = self.inner.consensus.current_leader().await;
            if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
                return self
                    .command_with_transaction_status(
                        self.not_leader_response(&req.query, leader).await,
                        subscriptions,
                    )
                    .await;
            }
            #[cfg(feature = "testing")]
            self.inner
                .runtime
                .pause_command_admission_if_armed(self.inner.consensus.local_node_id())
                .await;
            self.drop_transaction_bindings_if_armed();
            if subscriptions.transaction_active()
                && let Err(error) = self.validate_session_transaction_binding(subscriptions)
            {
                return self
                    .command_with_transaction_status(error.into_command_result(), subscriptions)
                    .await;
            }
        }

        let operations =
            match subscriptions.plan_commands(client_statements, &req.query, &req.domain) {
                Ok(operations) => operations,
                Err(error) => {
                    return self
                        .command_with_transaction_status(command_error(error), subscriptions)
                        .await;
                }
            };

        let result = self
            .process_session_command_operations(operations, tx, subscriptions)
            .await;
        self.command_with_transaction_status(result, subscriptions)
            .await
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use tokio::sync::mpsc;

    use super::{
        super::{
            subscription::SessionSubscriptions,
            test_fixtures::{
                TestService, build_test_service, named, queue_in_transaction, suggestion_values,
            },
        },
        *,
    };
    use crate::{
        proto,
        proto::{CommandRequest, SuggestRequest},
    };

    #[test]
    fn completion_context_preserves_prefix_for_post_filtering() {
        let input = "CREATE SCHE";
        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
        } = completion_context(input, input.len());

        assert_eq!(grammar_input, "CREATE ");
        assert_eq!(grammar_cursor, "CREATE ".len());
        assert_eq!(prefix, "sche");
    }

    #[test]
    fn keyword_completion_is_filtered_by_original_prefix() {
        let input = "CREATE SCHE";
        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
        } = completion_context(input, input.len());
        let filtered = suggest_client_statement(&grammar_input, grammar_cursor)
            .into_iter()
            .filter(|item| {
                prefix.is_empty()
                    || item
                        .to_ascii_lowercase()
                        .starts_with(&prefix.to_ascii_lowercase())
            })
            .collect::<Vec<_>>();

        assert_eq!(filtered, vec!["SCHEMA".to_string()]);
    }

    #[test]
    fn diagnostic_and_registry_error_helpers_map_spans() {
        let parse_diagnostic = ParseDiagnostic {
            message: "unexpected token".to_string(),
            span: 3..7,
        };
        let mapped = map_diagnostic(&parse_diagnostic);
        assert_eq!(mapped.message, "unexpected token");
        assert_eq!(mapped.span_start, 3);
        assert_eq!(mapped.span_end, 7);

        let response = error_response("parse error", std::slice::from_ref(&parse_diagnostic));
        assert!(!response.success);
        assert_eq!(response.message, "parse error");
        assert_eq!(response.diagnostics, vec![mapped]);

        let query = "CREATE RELAY orders SCHEMA notification UNBRANCHED;";
        let identifier = named("orders");
        assert_eq!(find_identifier_span(query, &identifier), Some(13..19));

        let domain = DomainName::parse("default").expect("valid domain");
        let err = error_stack::Report::new(RegistryError::AlreadyExists {
            domain: "default".to_string(),
            identifier: "orders".to_string(),
        });
        let registry_response = create_registry_error_response(query, &domain, &identifier, &err);
        assert!(!registry_response.success);
        assert!(registry_response.message.contains("orders"));
        assert_eq!(registry_response.diagnostics.len(), 1);
        assert_eq!(registry_response.diagnostics[0].span_start, 13);
        assert_eq!(registry_response.diagnostics[0].span_end, 19);
        assert_eq!(
            infer_kind_from_error_target(&err, &identifier),
            Some("model")
        );

        let missing_target = error_stack::Report::new(RegistryError::NotFound {
            domain: "default".to_string(),
            identifier: "other".to_string(),
        });
        assert_eq!(
            infer_kind_from_error_target(&missing_target, &identifier),
            None
        );
    }

    #[tokio::test]
    async fn placement_member_completion_expands_all_schedulable_runtime_names() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();
        let configured = service
            .process_command(
                CommandRequest {
                    query: "BEGIN; CREATE SCHEMA placement_event ( id I64 ); CREATE RELAY \
                            plain_input SCHEMA placement_event UNBRANCHED; CREATE RELAY \
                            eligible_state SCHEMA placement_event UNBRANCHED WITH MATERIALIZED \
                            STATE LAST BY TIMESTAMP; CREATE RELAY plain_output SCHEMA \
                            placement_event UNBRANCHED; CREATE JUNCTION eligible_processor FROM \
                            plain_input UNBRANCHED TO plain_output INHERIT ALL FLUSH IMMEDIATE ON \
                            MESSAGE ERROR LOG; COMMIT;"
                        .to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(
            configured.success,
            "placement completion fixture should configure: {configured:?}"
        );

        let input = "CREATE PLACEMENT policy FROM ";
        let response = service
            .process_suggest(
                SuggestRequest {
                    input: input.to_string(),
                    cursor: u32::try_from(input.len())
                        .assured("the test suggestion input is smaller than u32::MAX bytes"),
                    domain: "default".to_string(),
                },
                &subscriptions,
            )
            .await;
        let values = response
            .suggestions
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect::<Vec<_>>();

        assert!(
            values.contains(&"eligible_processor".to_string()),
            "{values:?}"
        );
        assert!(values.contains(&"eligible_state".to_string()), "{values:?}");
        assert!(values.contains(&"plain_input".to_string()), "{values:?}");
        assert!(values.contains(&"plain_output".to_string()), "{values:?}");
        assert!(
            !values.contains(&"ref:runtime_node".to_string()),
            "{values:?}"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_offers_models_queued_in_the_open_transaction() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut subscriptions, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE SCHEMA queued_order ( order_id I64 );",
        )
        .await;

        let values =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(values.contains(&"queued_order".to_string()), "{values:?}");

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_hides_models_dropped_in_the_open_transaction() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "BEGIN; CREATE SCHEMA committed_order ( order_id I64 ); CREATE RELAY committed_orders \
             SCHEMA committed_order UNBRANCHED; COMMIT;",
        )
        .await;

        let committed = suggestion_values(&service, &subscriptions, "DROP RELAY ").await;
        assert!(
            committed.contains(&"committed_orders".to_string()),
            "{committed:?}"
        );

        queue_in_transaction(&service, &mut subscriptions, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "DROP RELAY committed_orders;",
        )
        .await;

        let dropped = suggestion_values(&service, &subscriptions, "DROP RELAY ").await;
        assert!(
            !dropped.contains(&"committed_orders".to_string()),
            "{dropped:?}"
        );

        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE RELAY committed_orders SCHEMA committed_order UNBRANCHED;",
        )
        .await;

        let recreated = suggestion_values(&service, &subscriptions, "DROP RELAY ").await;
        assert!(
            recreated.contains(&"committed_orders".to_string()),
            "{recreated:?}"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_keeps_queued_models_out_of_other_sessions() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut writer = SessionSubscriptions::new();
        let mut observer = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut writer, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut writer,
            &tx,
            "CREATE SCHEMA isolated_order ( order_id I64 );",
        )
        .await;

        let bound = suggestion_values(&service, &writer, "CREATE RELAY orders SCHEMA ").await;
        assert!(bound.contains(&"isolated_order".to_string()), "{bound:?}");

        let unbound = suggestion_values(&service, &observer, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            !unbound.contains(&"isolated_order".to_string()),
            "{unbound:?}"
        );

        writer.stop_all(&service).await;
        observer.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_drops_queued_models_until_a_detached_transaction_is_attached() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut subscriptions, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE SCHEMA detached_order ( order_id I64 );",
        )
        .await;
        let transaction_id = subscriptions
            .transaction_id()
            .expect("BEGIN must bind a transaction")
            .to_string();

        let bound =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(bound.contains(&"detached_order".to_string()), "{bound:?}");

        // A leadership change leaves the replicated transaction intact while the leader-local
        // binding is gone, which is what the session observes until it attaches again.
        service.inner.transaction_bindings.remove(&transaction_id);

        let detached =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            !detached.contains(&"detached_order".to_string()),
            "{detached:?}"
        );

        let attached = service
            .attach_transaction(
                proto::AttachTransactionRequest {
                    id: transaction_id.clone(),
                },
                &mut subscriptions,
            )
            .await;
        assert!(attached.success, "reattach must succeed: {attached:?}");

        let reattached =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            reattached.contains(&"detached_order".to_string()),
            "{reattached:?}"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_moves_queued_models_to_the_session_that_takes_the_transaction_over() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut first = SessionSubscriptions::new();
        let mut second = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut first, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut first,
            &tx,
            "CREATE SCHEMA takeover_order ( order_id I64 );",
        )
        .await;
        let transaction_id = first
            .transaction_id()
            .expect("BEGIN must bind a transaction")
            .to_string();

        let attached = service
            .attach_transaction(
                proto::AttachTransactionRequest {
                    id: transaction_id.clone(),
                },
                &mut second,
            )
            .await;
        assert!(attached.success, "takeover must succeed: {attached:?}");

        let displaced = suggestion_values(&service, &first, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            !displaced.contains(&"takeover_order".to_string()),
            "{displaced:?}"
        );

        let holder = suggestion_values(&service, &second, "CREATE RELAY orders SCHEMA ").await;
        assert!(holder.contains(&"takeover_order".to_string()), "{holder:?}");

        first.stop_all(&service).await;
        second.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn placement_member_completion_expands_queued_runtime_names() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut subscriptions, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE SCHEMA queued_event ( id I64 );",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE RELAY queued_state SCHEMA queued_event UNBRANCHED WITH MATERIALIZED STATE \
             LAST BY TIMESTAMP;",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE RELAY queued_plain SCHEMA queued_event UNBRANCHED;",
        )
        .await;

        let values =
            suggestion_values(&service, &subscriptions, "CREATE PLACEMENT policy FROM ").await;
        assert!(values.contains(&"queued_state".to_string()), "{values:?}");
        assert!(values.contains(&"queued_plain".to_string()), "{values:?}");

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }
}
