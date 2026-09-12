//! The console listener and the session requests the browser drives through it.
//!
//! Layer: edges.
//!
//! - **Owns.** The console asset routes, its WebSocket session, its resource upload path, and the
//!   snapshots it pushes to a connected browser.
//! - **Depends on.** The session service for commands and the consensus observer for leadership.
//! - **Must not know.** How a command is executed once the session service accepts it.

use std::{
    collections::BTreeSet,
    convert::Infallible,
    io,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::Arc as StdArc,
};

use arch_into::ArchInto;
use error_stack::{Report, ResultExt};
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request as HyperRequest, Response as HyperResponse, StatusCode,
    body::{Bytes, Incoming as HyperIncoming},
    header::{
        ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
        CONNECTION, LOCATION, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, UPGRADE,
    },
    server::conn::http1,
    service::service_fn,
    upgrade,
};
use hyper_util::rt::TokioIo;
use meticulous::ResultExt as _;
use nervix_consensus::ConsensusError;
use nervix_dataflow_graph::DataflowGraph;
use nervix_models::{
    ClusterNodeName, DomainName, DomainStatus, RelayName, ResourceName, ResourceUploadIdentity,
    ResourceUploadKey, UserName,
};
use nervix_nspl::client_statement::{ClientStatement, parse_client_statements};
use prost::Message as _;
use rustls::ServerConfig;
use tokio::{
    net::TcpListener,
    sync::mpsc,
    task::JoinSet,
    time::{Duration, interval},
};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, handshake::derive_accept_key, protocol::Role},
};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::warn;

use super::{
    AppError,
    authentication::{credentials_from_web_console_request, unauthorized_basic_response},
    domain_lifecycle::ActiveDomainError,
    http_endpoint::{is_websocket_upgrade_request, response_with_bytes, text_response},
    model_mutation::command_error,
    observation::dataflow_metric_target,
    peer_grpc::grpc_uri_from_advertise_addr,
    session_service::SessionServiceImpl,
    subscription::SessionSubscriptions,
};
use crate::{
    cluster, proto,
    proto::{
        ClusterSummary, CommandRequest, CommandResult, CommandResultKind, Diagnostic,
        DomainEntitySnapshot, DomainInfo, DomainList, DomainSnapshot, ServerEvent,
        ServerEventLevel, SessionRequest, SessionResponse, SetActiveDomainRequest,
    },
    resource::{ResourceStoreError, StagedResourceArchive},
};
const WEB_CONSOLE_INDEX: &[u8] = include_bytes!("../../crates/web-console/dist/index.html");

const WEB_CONSOLE_CSS: &[u8] = include_bytes!("../../crates/web-console/dist/console.css");

const WEB_CONSOLE_JS: &[u8] = include_bytes!("../../crates/web-console/dist/nervix-web-console.js");

const WEB_CONSOLE_WASM: &[u8] =
    include_bytes!("../../crates/web-console/dist/nervix-web-console_bg.wasm");

const WEB_CONSOLE_ICON: &[u8] = include_bytes!("../../crates/web-console/dist/nervix-icon.svg");

const WEB_CONSOLE_WS_PATH: &str = "/console/ws";

const WEB_CONSOLE_RESOURCE_UPLOAD_PATH: &str = "/console/resources/upload";

pub(in crate::application) const WEB_CONSOLE_AUTH_QUERY_PARAM: &str = "auth";

const WEB_CONSOLE_LEADERSHIP_CHECK_INTERVAL: Duration = Duration::from_millis(250);

const WEB_CONSOLE_GRAPH_SNAPSHOT_INTERVAL: Duration = Duration::from_millis(500);

fn web_console_upload_text_response(
    status: StatusCode,
    body: impl Into<Bytes>,
) -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(ACCESS_CONTROL_ALLOW_METHODS, "POST, OPTIONS")
        .header(ACCESS_CONTROL_ALLOW_HEADERS, "content-type")
        .body(Full::new(body.into()))
        .assured(
            "the status and header values are typed constants or generated ASCII, which the http \
             builder always accepts",
        )
}

fn redirect_response(location: &'static str) -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(StatusCode::PERMANENT_REDIRECT)
        .header(LOCATION, location)
        .body(Full::new(Bytes::new()))
        .assured(
            "the status and header values are typed constants or generated ASCII, which the http \
             builder always accepts",
        )
}

async fn handle_web_console_request(
    service: SessionServiceImpl,
    mut request: HyperRequest<HyperIncoming>,
) -> Result<HyperResponse<Full<Bytes>>, Infallible> {
    if request.method() == Method::GET && request.uri().path() == WEB_CONSOLE_WS_PATH {
        let Some(credentials) = credentials_from_web_console_request(&request) else {
            return Ok(unauthorized_basic_response());
        };
        let Some(authenticated_user) = service.authenticate_basic_credentials(&credentials).await
        else {
            return Ok(unauthorized_basic_response());
        };

        if !is_websocket_upgrade_request(&request) {
            return Ok(response_with_bytes(
                StatusCode::UPGRADE_REQUIRED,
                Bytes::new(),
                "text/plain; charset=utf-8",
            ));
        }

        let Some(sec_websocket_key) = request.headers().get(SEC_WEBSOCKET_KEY) else {
            return Ok(text_response(
                StatusCode::BAD_REQUEST,
                "missing websocket key",
            ));
        };
        let Ok(sec_websocket_key) = sec_websocket_key.to_str() else {
            return Ok(text_response(
                StatusCode::BAD_REQUEST,
                "missing websocket key",
            ));
        };
        let sec_websocket_key = sec_websocket_key.to_owned();

        let response = HyperResponse::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(
                SEC_WEBSOCKET_ACCEPT,
                derive_accept_key(sec_websocket_key.as_bytes()),
            )
            .body(Full::new(Bytes::new()))
            .assured(
                "the status and header values are typed constants or generated ASCII, which the \
                 http builder always accepts",
            );

        let on_upgrade = upgrade::on(&mut request);
        let service_tasks = service.inner.service_tasks.clone();
        service_tasks.spawn(async move {
            let upgraded = tokio::select! {
                _ = service.inner.shutdown.cancelled() => return,
                upgraded = on_upgrade => upgraded,
            };
            match upgraded {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    let mut websocket =
                        WebSocketStream::from_raw_socket(io, Role::Server, None).await;
                    let (tx, mut response_rx) = mpsc::channel(16);
                    let mut subscriptions = SessionSubscriptions::for_user(authenticated_user);
                    let mut leadership_check = interval(WEB_CONSOLE_LEADERSHIP_CHECK_INTERVAL);
                    let mut graph_snapshot = interval(WEB_CONSOLE_GRAPH_SNAPSHOT_INTERVAL);
                    let mut domains_rx = service.inner.consensus.subscribe_domains();
                    leadership_check.tick().await;
                    graph_snapshot.tick().await;
                    let mut leader_connected = false;
                    let mut active_domain = None::<DomainName>;
                    let mut clean_close = false;

                    loop {
                        tokio::task::consume_budget().await;
                        tokio::select! {
                            _ = service.inner.shutdown.cancelled() => break,
                            message = futures_util::StreamExt::next(&mut websocket) => {
                                let Some(message) = message else {
                                    break;
                                };
                                match message {
                                    Ok(Message::Binary(payload)) => {
                                        match proto::SessionRequest::decode(payload.as_ref()) {
                                            Ok(request) => {
                                                match request.request {
                                                    Some(proto::session_request::Request::SetActiveDomain(request)) => {
                                                        match service
                                                            .process_web_console_active_domain_request(
                                                                request,
                                                                &mut active_domain,
                                                            )
                                                            .await
                                                        {
                                                            Ok(response) => {
                                                                if !send_web_console_session_response(
                                                                    &mut websocket,
                                                                    response,
                                                                )
                                                                .await
                                                                {
                                                                    break;
                                                                }
                                                                if leader_connected
                                                                    && !send_web_console_state_responses(
                                                                        &mut websocket,
                                                                        &service,
                                                                        active_domain.as_ref(),
                                                                    )
                                                                    .await
                                                                {
                                                                    break;
                                                                }
                                                            }
                                                            Err(error) => {
                                                                if !send_web_console_session_response(
                                                                    &mut websocket,
                                                                    web_console_server_error_response(
                                                                        error.to_string(),
                                                                    ),
                                                                )
                                                                .await
                                                                {
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    _ => {
                                                        let response = service
                                                            .process_web_console_request(
                                                                request,
                                                                &tx,
                                                                &mut subscriptions,
                                                            )
                                                            .await;
                                                        if !send_web_console_session_response(
                                                            &mut websocket,
                                                            response,
                                                        )
                                                        .await
                                                        {
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            Err(error) => {
                                                warn!(
                                                    error = %error,
                                                    "failed to decode web console websocket protobuf request"
                                                );
                                                let response = web_console_server_error_response(
                                                    format!(
                                                        "failed to decode protobuf request: {error}"
                                                    ),
                                                );
                                                if !send_web_console_session_response(
                                                    &mut websocket,
                                                    response,
                                                )
                                                .await
                                                {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    Ok(Message::Ping(payload)) => {
                                        if websocket.send(Message::Pong(payload)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Ok(Message::Close(_)) => {
                                        clean_close = true;
                                        break;
                                    }
                                    Ok(_) => {}
                                    Err(error) => {
                                        warn!(error = %error, "web console websocket failed");
                                        break;
                                    }
                                }
                            }
                            response = response_rx.recv() => {
                                let Some(response) = response else {
                                    break;
                                };
                                match response {
                                    Ok(response) => {
                                        if !send_web_console_session_response(
                                            &mut websocket,
                                            response,
                                        )
                                        .await
                                        {
                                            break;
                                        }
                                    }
                                    Err(status) => {
                                        let response = web_console_server_error_response(
                                            status.message().to_string(),
                                        );
                                        if !send_web_console_session_response(
                                            &mut websocket,
                                            response,
                                        )
                                        .await
                                        {
                                            break;
                                        }
                                    }
                                }
                            }
                            _ = leadership_check.tick() => {
                                let Some(response) = service
                                    .web_console_leadership_response(leader_connected)
                                    .await
                                else {
                                    continue;
                                };
                                let close_after_send =
                                    response.event.as_ref().is_some_and(|event| {
                                        if let proto::session_response::Event::Result(result) =
                                            event
                                        {
                                            proto::CommandResultKind::try_from(result.kind).ok()
                                                == Some(proto::CommandResultKind::NotLeader)
                                        } else {
                                            false
                                        }
                                    });
                                let send_domain_snapshots =
                                    !close_after_send && !leader_connected;
                                if !send_web_console_session_response(
                                    &mut websocket,
                                    response,
                                )
                                .await
                                {
                                    break;
                                }
                                if close_after_send {
                                    break;
                                }
                                if send_domain_snapshots {
                                    leader_connected = true;
                                    let domain_response = service.domain_list_response(false).await;
                                    if !send_web_console_session_response(
                                        &mut websocket,
                                        domain_response,
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                    if !send_web_console_state_responses(
                                        &mut websocket,
                                        &service,
                                        active_domain.as_ref(),
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                }
                            }
                            _ = graph_snapshot.tick(), if leader_connected => {
                                if !send_web_console_state_responses(
                                    &mut websocket,
                                    &service,
                                    active_domain.as_ref(),
                                )
                                .await
                                {
                                    break;
                                }
                            }
                            changed = domains_rx.changed(), if leader_connected => {
                                if changed.is_err() {
                                    break;
                                }
                                if let Some(domain) = active_domain.as_ref()
                                    && service.inner.consensus.current_domain(domain).await.is_none()
                                {
                                    active_domain = None;
                                }
                                let domain_response = service.domain_list_response(false).await;
                                if !send_web_console_session_response(
                                    &mut websocket,
                                    domain_response,
                                )
                                .await
                                {
                                    break;
                                }
                                if !send_web_console_state_responses(
                                    &mut websocket,
                                    &service,
                                    active_domain.as_ref(),
                                )
                                .await
                                {
                                    break;
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
                }
                Err(error) => {
                    warn!(error = %error, "web console websocket upgrade failed");
                }
            }
        });

        return Ok(response);
    }

    if request.method() == Method::OPTIONS
        && request.uri().path() == WEB_CONSOLE_RESOURCE_UPLOAD_PATH
    {
        return Ok(web_console_upload_text_response(StatusCode::NO_CONTENT, ""));
    }

    if request.method() == Method::POST && request.uri().path() == WEB_CONSOLE_RESOURCE_UPLOAD_PATH
    {
        let Some(credentials) = credentials_from_web_console_request(&request) else {
            return Ok(unauthorized_basic_response());
        };
        let Some(authenticated_user) = service.authenticate_basic_credentials(&credentials).await
        else {
            return Ok(unauthorized_basic_response());
        };

        return Ok(service
            .handle_web_console_resource_upload(request, authenticated_user)
            .await);
    }

    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/") | (&Method::GET, "/console") => redirect_response("/console/"),
        (&Method::GET, "/console/") | (&Method::GET, "/console/index.html") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_INDEX),
            "text/html; charset=utf-8",
        ),
        (&Method::GET, "/console/console.css") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_CSS),
            "text/css; charset=utf-8",
        ),
        (&Method::GET, "/console/nervix-web-console.js") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_JS),
            "text/javascript; charset=utf-8",
        ),
        (&Method::GET, "/console/nervix-web-console_bg.wasm") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_WASM),
            "application/wasm",
        ),
        (&Method::GET, "/console/nervix-icon.svg") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_ICON),
            "image/svg+xml",
        ),
        (&Method::GET, path) if path.starts_with("/console/") => {
            text_response(StatusCode::NOT_FOUND, Bytes::from_static(b"not found"))
        }
        (&Method::GET, _) => text_response(StatusCode::NOT_FOUND, "not found"),
        _ => text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    };

    Ok(response)
}

async fn send_web_console_session_response<S>(
    websocket: &mut WebSocketStream<S>,
    response: SessionResponse,
) -> bool
where
    WebSocketStream<S>: SinkExt<Message> + Unpin,
{
    websocket
        .send(Message::Binary(response.encode_to_vec()))
        .await
        .is_ok()
}

async fn send_web_console_state_responses<S>(
    websocket: &mut WebSocketStream<S>,
    service: &SessionServiceImpl,
    active_domain: Option<&DomainName>,
) -> bool
where
    WebSocketStream<S>: SinkExt<Message> + Unpin,
{
    if !send_web_console_session_response(
        websocket,
        service.web_console_cluster_summary_response().await,
    )
    .await
    {
        return false;
    }
    for response in service
        .web_console_domain_snapshot_responses(active_domain)
        .await
    {
        if !send_web_console_session_response(websocket, response).await {
            return false;
        }
    }
    true
}

fn web_console_server_error_response(message: String) -> SessionResponse {
    SessionResponse {
        event: Some(proto::session_response::Event::Server(ServerEvent {
            level: i32::from(ServerEventLevel::Error),
            message,
        })),
    }
}

pub(in crate::application) fn web_console_query_param(
    query: Option<&str>,
    name: &str,
) -> Option<String> {
    url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
}

fn sanitized_upload_relative_path(raw: &str) -> Option<PathBuf> {
    let normalized = raw.replace('\\', "/");
    let mut path = PathBuf::new();
    for component in Path::new(&normalized).components() {
        match component {
            Component::Normal(part) => path.push(part),
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => return None,
        }
    }
    (!path.as_os_str().is_empty()).then_some(path)
}

fn web_console_bundle_error(error: Report<ResourceStoreError>) -> (StatusCode, String) {
    let status = match error.current_context() {
        ResourceStoreError::ArchiveQuotaExceeded { .. }
        | ResourceStoreError::ExtractedQuotaExceeded { .. }
        | ResourceStoreError::FileCountQuotaExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        ResourceStoreError::InvalidArchivePath
        | ResourceStoreError::InvalidResourcePath
        | ResourceStoreError::EmptyBundle => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, format!("{error:#}"))
}

pub(in crate::application) async fn serve_web_console_http(
    service: SessionServiceImpl,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>> {
    let mut connection_tasks = JoinSet::new();

    loop {
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() => {
                accepted.change_context(AppError::ServeWebConsole)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeWebConsole)?;
        let service = service.clone();
        connection_tasks.spawn(async move {
            let io = TokioIo::new(stream);
            let service = service.clone();
            if let Err(error) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| handle_web_console_request(service.clone(), request)),
                )
                .with_upgrades()
                .await
            {
                warn!(error = %error, "web console connection failed");
            }
        });
    }
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

pub(in crate::application) async fn serve_web_console_https(
    service: SessionServiceImpl,
    tls_server_config: StdArc<ServerConfig>,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>> {
    let tls_acceptor = TlsAcceptor::from(tls_server_config);
    let mut connection_tasks = JoinSet::new();

    loop {
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() => {
                accepted.change_context(AppError::ServeWebConsole)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeWebConsole)?;
        let tls_acceptor = tls_acceptor.clone();
        let service = service.clone();
        connection_tasks.spawn(async move {
            let stream = match tls_acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(error) => {
                    warn!(error = %error, "web console tls handshake failed");
                    return;
                }
            };
            let io = TokioIo::new(stream);
            let service = service.clone();
            if let Err(error) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| handle_web_console_request(service.clone(), request)),
                )
                .with_upgrades()
                .await
            {
                warn!(error = %error, "web console tls connection failed");
            }
        });
    }
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

pub(in crate::application) fn web_console_advertise_url(
    advertise_addr: Option<cluster::HostPort>,
    listen_addr: SocketAddr,
    https_listen_addr: Option<SocketAddr>,
) -> String {
    if let Some(addr) = advertise_addr {
        return format!("http://{addr}");
    }

    let (scheme, default_addr) = match https_listen_addr {
        Some(addr) => ("https", addr),
        None => ("http", listen_addr),
    };
    let addr = cluster::HostPort::from_socket_addr(default_addr);
    format!("{scheme}://{addr}")
}

impl SessionServiceImpl {
    async fn handle_web_console_resource_upload(
        &self,
        request: HyperRequest<HyperIncoming>,
        authenticated_user: UserName,
    ) -> HyperResponse<Full<Bytes>> {
        let Some(resource_name) = web_console_query_param(request.uri().query(), "resource") else {
            return web_console_upload_text_response(
                StatusCode::BAD_REQUEST,
                "missing resource query parameter",
            );
        };
        let identifier = match ResourceName::parse(resource_name.trim()) {
            Ok(identifier) => identifier,
            Err(_) => {
                return web_console_upload_text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid resource name",
                );
            }
        };
        let Some(domain_name) = web_console_query_param(request.uri().query(), "domain") else {
            return web_console_upload_text_response(
                StatusCode::BAD_REQUEST,
                "missing domain query parameter",
            );
        };
        let domain = match DomainName::parse(domain_name.trim()) {
            Ok(domain) => domain,
            Err(_) => {
                return web_console_upload_text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid domain name",
                );
            }
        };
        let Some(upload_identity) =
            web_console_query_param(request.uri().query(), "upload_identity")
        else {
            return web_console_upload_text_response(
                StatusCode::BAD_REQUEST,
                "missing upload_identity query parameter",
            );
        };
        let upload_identity = match ResourceUploadIdentity::parse(upload_identity.trim()) {
            Ok(identity) => identity,
            Err(error) => {
                return web_console_upload_text_response(
                    StatusCode::BAD_REQUEST,
                    error.to_string(),
                );
            }
        };
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            return web_console_upload_text_response(
                StatusCode::CONFLICT,
                "resource uploads must be sent to the cluster leader",
            );
        }
        let resources = self.inner.consensus.current_resources().await;
        if !resources.is_declared(&domain, &identifier) {
            return web_console_upload_text_response(
                StatusCode::NOT_FOUND,
                format!("resource '{}' does not exist", identifier.as_str()),
            );
        }
        let Some(content_type) = request
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
        else {
            return web_console_upload_text_response(
                StatusCode::BAD_REQUEST,
                "missing multipart content type",
            );
        };
        let boundary = match multer::parse_boundary(content_type) {
            Ok(boundary) => boundary,
            Err(_) => {
                return web_console_upload_text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid multipart boundary",
                );
            }
        };

        match self
            .stage_web_console_resource_upload(request, boundary)
            .await
        {
            Ok(archive) => match self
                .install_uploaded_resource_archive(
                    ResourceUploadKey::new(authenticated_user, domain, identifier, upload_identity),
                    archive.path(),
                    archive.root_checksum().to_string(),
                )
                .await
            {
                Ok(publication) => web_console_upload_text_response(
                    StatusCode::OK,
                    format!("published resource version {}", publication.version),
                ),
                Err(error) => {
                    let status = if let Some(ConsensusError::LeadershipLost { .. }) =
                        error.downcast_ref::<ConsensusError>()
                    {
                        StatusCode::CONFLICT
                    } else {
                        StatusCode::BAD_REQUEST
                    };
                    web_console_upload_text_response(status, format!("{error:#}"))
                }
            },
            Err((status, message)) => web_console_upload_text_response(status, message),
        }
    }

    async fn stage_web_console_resource_upload(
        &self,
        request: HyperRequest<HyperIncoming>,
        boundary: String,
    ) -> Result<StagedResourceArchive, (StatusCode, String)> {
        let mut staging = self
            .inner
            .resource_store
            .create_bundle_stager()
            .await
            .map_err(web_console_bundle_error)?;
        let stream = request.into_body().into_data_stream().map(|result| {
            result.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
        });
        let mut multipart = multer::Multipart::new(stream, boundary);
        let staging_result: Result<(), (StatusCode, String)> = async {
            while let Some(mut field) = multipart.next_field().await.map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("failed to read multipart field: {error}"),
                )
            })? {
                tokio::task::consume_budget().await;
                if field.name() != Some("file") {
                    continue;
                }
                let Some(file_name) = field.file_name().and_then(sanitized_upload_relative_path)
                else {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "upload contains an invalid file path".to_string(),
                    ));
                };
                staging
                    .create_file(&file_name)
                    .await
                    .map_err(web_console_bundle_error)?;
                while let Some(chunk) = field.chunk().await.map_err(|error| {
                    (
                        StatusCode::BAD_REQUEST,
                        format!("failed to read uploaded file chunk: {error}"),
                    )
                })? {
                    tokio::task::consume_budget().await;
                    let chunk = self
                        .inner
                        .resource_store
                        .admit_staging_bytes(&chunk)
                        .await
                        .map_err(web_console_bundle_error)?;
                    staging
                        .write_chunk(chunk)
                        .await
                        .map_err(web_console_bundle_error)?;
                }
            }
            Ok(())
        }
        .await;
        if let Err((status, message)) = staging_result {
            return match staging.abort().await {
                Ok(()) => Err((status, message)),
                Err(cleanup_error) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("{message}; failed to clean resource upload: {cleanup_error:#}"),
                )),
            };
        }

        staging.finish().await.map_err(web_console_bundle_error)
    }

    async fn process_web_console_request(
        &self,
        request: SessionRequest,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> SessionResponse {
        match request.request {
            Some(proto::session_request::Request::Command(command)) => {
                let result = self
                    .process_web_console_command(command, tx, subscriptions)
                    .await;
                SessionResponse {
                    event: Some(proto::session_response::Event::Result(result)),
                }
            }
            Some(proto::session_request::Request::Suggest(suggest)) => {
                let response = self.process_suggest(suggest, subscriptions).await;
                SessionResponse {
                    event: Some(proto::session_response::Event::Suggest(response)),
                }
            }
            Some(proto::session_request::Request::ListDomains(_)) => {
                self.domain_list_response(true).await
            }
            Some(proto::session_request::Request::SetActiveDomain(_)) => {
                web_console_server_error_response(
                    "active domain requests are handled by the websocket session".to_string(),
                )
            }
            Some(proto::session_request::Request::AttachTransaction(request)) => {
                let result = self.attach_transaction(request, subscriptions).await;
                SessionResponse {
                    event: Some(proto::session_response::Event::Result(result)),
                }
            }
            None => {
                web_console_server_error_response("session request payload is missing".to_string())
            }
        }
    }

    async fn process_web_console_active_domain_request(
        &self,
        request: SetActiveDomainRequest,
        active_domain: &mut Option<DomainName>,
    ) -> Result<SessionResponse, ActiveDomainError> {
        let domain = match DomainName::parse(request.domain.trim()) {
            Ok(domain) => domain,
            Err(_) => return Err(ActiveDomainError::Invalid),
        };
        if self.inner.consensus.current_domain(&domain).await.is_none() {
            return Err(ActiveDomainError::NotFound { domain });
        }
        *active_domain = Some(domain.clone());
        Ok(SessionResponse {
            event: Some(proto::session_response::Event::Server(ServerEvent {
                level: i32::from(ServerEventLevel::Info),
                message: format!("using domain '{}'", domain.as_str()),
            })),
        })
    }

    async fn process_web_console_command(
        &self,
        req: CommandRequest,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        if let Ok(statements) = parse_client_statements(&req.query)
            && statements.iter().any(|statement| {
                let ClientStatement::UploadResource(_) = statement else {
                    return false;
                };
                true
            })
        {
            return self
                .command_with_transaction_status(
                    command_error(
                        "UPLOAD RESOURCE is not supported in the web console".to_string(),
                    ),
                    subscriptions,
                )
                .await;
        }

        self.process_command(req, tx, subscriptions).await
    }
}
impl SessionServiceImpl {
    async fn web_console_leadership_response(
        &self,
        already_connected_to_leader: bool,
    ) -> Option<SessionResponse> {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            let result = self.not_leader_response("", leader).await;
            return Some(SessionResponse {
                event: Some(proto::session_response::Event::Result(result)),
            });
        }

        if already_connected_to_leader {
            return None;
        }

        Some(SessionResponse {
            event: Some(proto::session_response::Event::Server(ServerEvent {
                level: i32::from(ServerEventLevel::Info),
                message: format!(
                    "connected to leader '{}'",
                    self.inner.consensus.local_node_id()
                ),
            })),
        })
    }

    pub(in crate::application) async fn domain_list_response(
        &self,
        response_to_request: bool,
    ) -> SessionResponse {
        let domains = self
            .inner
            .consensus
            .current_domains()
            .await
            .into_values()
            .map(|domain| DomainInfo {
                id: domain.id.as_str().to_string(),
                pace: domain.config.pace.as_ref().to_string(),
                status: domain.status.as_ref().to_string(),
            })
            .collect();
        SessionResponse {
            event: Some(proto::session_response::Event::Domains(DomainList {
                domains,
                response_to_request,
            })),
        }
    }

    async fn web_console_cluster_summary_response(&self) -> SessionResponse {
        let running_domains = self
            .inner
            .consensus
            .current_domains()
            .await
            .into_values()
            .filter(|domain| domain.status == DomainStatus::Running)
            .count();
        let (nodes, relays) = self.inner.registry.active_graphs().into_iter().fold(
            (0_usize, 0_usize),
            |(nodes, relays), (_, graph)| {
                let counts = graph.dataflow_graph_counts();
                (nodes + counts.nodes, relays + counts.relays)
            },
        );
        SessionResponse {
            event: Some(proto::session_response::Event::Cluster(ClusterSummary {
                running_domains: running_domains.arch_into(),
                nodes: nodes.arch_into(),
                relays: relays.arch_into(),
            })),
        }
    }

    async fn web_console_domain_snapshot_responses(
        &self,
        active_domain: Option<&DomainName>,
    ) -> Vec<SessionResponse> {
        let resources = self.inner.consensus.current_resources().await;
        let domains = self.inner.consensus.current_domains().await;
        let resource_entities = resources
            .next_version_by_resource
            .iter()
            .filter(|counter| Some(&counter.domain) == active_domain)
            .map(|counter| DomainEntitySnapshot {
                kind: "resource".to_string(),
                identifier: counter.identifier.as_str().to_string(),
                detail: if counter.next_version > 1 {
                    format!("v{}", counter.next_version - 1)
                } else {
                    "catalog".to_string()
                },
            })
            .collect::<Vec<_>>();
        let active_graphs = self
            .inner
            .registry
            .active_graphs()
            .into_iter()
            .filter(|(domain, _)| active_domain.is_none_or(|active| domain == active))
            .collect::<Vec<_>>();
        let active_graph_domains = active_graphs
            .iter()
            .map(|(domain, _)| domain.clone())
            .collect::<BTreeSet<_>>();
        let mut responses = Vec::new();
        for (domain, graph) in active_graphs {
            tokio::task::consume_budget().await;
            if let Some(response) = self
                .web_console_domain_snapshot_response(
                    domain.clone(),
                    graph.to_dataflow_graph(domain.as_str()),
                    &resource_entities,
                )
                .await
            {
                responses.push(response);
            }
        }

        for domain in domains.keys() {
            tokio::task::consume_budget().await;
            if active_domain.is_some_and(|active| active != domain)
                || active_graph_domains.contains(domain)
            {
                continue;
            }
            if let Some(response) = self
                .web_console_domain_snapshot_response(
                    domain.clone(),
                    DataflowGraph::new(domain.as_str()),
                    &resource_entities,
                )
                .await
            {
                responses.push(response);
            }
        }
        responses
    }

    async fn web_console_domain_snapshot_response(
        &self,
        domain: DomainName,
        mut dataflow_graph: DataflowGraph,
        resource_entities: &[DomainEntitySnapshot],
    ) -> Option<SessionResponse> {
        dataflow_graph.statistics = self.inner.runtime.dataflow_domain_statistics(&domain);
        for node in &mut dataflow_graph.nodes {
            let Some((kind, identifier)) = dataflow_metric_target(&node.id) else {
                continue;
            };
            let health = self
                .dataflow_node_status_for_graph(&domain, &kind, &identifier)
                .await;
            node.status = health.status;
            node.status_detail = health.detail;
            node.reconnect_wait_millis = health.reconnect_wait_millis;
            if kind == "RELAY" {
                node.statistics = self
                    .inner
                    .runtime
                    .dataflow_relay_buffer_statistics(&domain, &RelayName::from(&identifier));
                let existing = node
                    .branches
                    .iter()
                    .map(|branch| branch.branch.clone())
                    .collect::<BTreeSet<_>>();
                node.branches.extend(
                    self.inner
                        .runtime
                        .dataflow_relay_branch_statistics(&domain, &RelayName::from(&identifier))
                        .into_iter()
                        .filter(|branch| !existing.contains(&branch.branch)),
                );
            }
        }
        for edge in &mut dataflow_graph.edges {
            let Some(metric) = edge.metric.as_ref() else {
                continue;
            };
            edge.statistics = self.inner.runtime.dataflow_edge_statistics(&domain, metric);
            edge.branches = self
                .inner
                .runtime
                .dataflow_edge_branch_statistics(&domain, metric);
        }
        match dataflow_graph.serialize() {
            Ok(graph_bytes) => Some(SessionResponse {
                event: Some(proto::session_response::Event::Snapshot(DomainSnapshot {
                    domain: domain.as_str().to_string(),
                    dataflow_graph: graph_bytes.into(),
                    entities: self
                        .inner
                        .registry
                        .active_domain_entities(&domain)
                        .into_iter()
                        .map(|entity| DomainEntitySnapshot {
                            kind: entity.kind.as_str().to_string(),
                            identifier: entity.identifier.as_str().to_string(),
                            detail: entity.kind.as_str().replace('_', " ").to_ascii_uppercase(),
                        })
                        .chain(resource_entities.iter().cloned())
                        .collect(),
                })),
            }),
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    error = %error,
                    "failed to serialize web console domain snapshot"
                );
                None
            }
        }
    }

    pub(in crate::application) async fn consensus_error_response(
        &self,
        error: &ConsensusError,
        message: String,
    ) -> CommandResult {
        match error {
            ConsensusError::LeadershipLost { leader_id } => {
                self.not_leader_response("", leader_id.clone()).await
            }
            _ => command_error(message),
        }
    }

    pub(in crate::application) async fn not_leader_response(
        &self,
        query: &str,
        leader: Option<ClusterNodeName>,
    ) -> CommandResult {
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
        if let Some(node) = leader_node.as_ref()
            && let Some(uri) = grpc_uri_from_advertise_addr(&node.grpc_advertise_addr)
        {
            leader_grpc_uri = uri;
        }
        let leader_web_console_uri = match leader_node {
            Some(node) => node.web_console_advertise_addr,
            None => String::new(),
        };
        let diagnostic = match leader.as_ref() {
            Some(leader) => format!("retry this command on leader '{leader}'"),
            None => "retry this command on the current leader".to_string(),
        };
        CommandResult {
            success: false,
            message: "not-a-leader".to_string(),
            diagnostics: vec![Diagnostic {
                message: diagnostic,
                span_start: 0,
                span_end: u32::try_from(query.len()).unwrap_or(0),
            }],
            kind: i32::from(CommandResultKind::NotLeader),
            leader: match leader {
                Some(leader) => leader.to_string(),
                None => String::new(),
            },
            leader_grpc_uri,
            leader_web_console_uri,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{CreateSchema, ModelName};
    use tokio::sync::mpsc;

    use super::{
        super::test_fixtures::{TestService, build_test_service, named, test_addr},
        *,
    };
    use crate::{
        cluster, proto,
        proto::{SessionRequest, SuggestRequest},
    };

    #[test]
    fn web_console_advertise_url_uses_https_listener_when_available() {
        assert_eq!(
            web_console_advertise_url(None, test_addr(17420), Some(test_addr(17443))),
            "https://127.0.0.1:17443"
        );
        assert_eq!(
            web_console_advertise_url(
                Some(cluster::HostPort::from_socket_addr(test_addr(17420))),
                test_addr(17420),
                Some(test_addr(17443))
            ),
            "http://127.0.0.1:17420"
        );
        assert_eq!(
            web_console_advertise_url(None, test_addr(17420), None),
            "http://127.0.0.1:17420"
        );
    }

    #[tokio::test]
    async fn web_console_command_request_invokes_session_command_processor() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let response = service
            .process_web_console_request(
                SessionRequest {
                    request: Some(proto::session_request::Request::Command(CommandRequest {
                        query: "CREATE SCHEMA web_console_event ( user_id U32 );".to_string(),
                        domain: "default".to_string(),
                    })),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        let Some(proto::session_response::Event::Result(result)) = response.event else {
            panic!("web console command should return a command result");
        };
        assert!(result.success, "expected command success: {result:?}");
        let schema = registry
            .get::<CreateSchema>(
                &DomainName::parse("default").expect("valid domain"),
                named::<ModelName>("web_console_event"),
            )
            .expect("registry get should succeed");
        assert!(schema.is_some());

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn web_console_rejects_upload_and_supports_suggest_requests() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let upload_response = service
            .process_web_console_request(
                SessionRequest {
                    request: Some(proto::session_request::Request::Command(CommandRequest {
                        query: "UPLOAD RESOURCE proto VERSION '/tmp/proto';".to_string(),
                        domain: "default".to_string(),
                    })),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        let Some(proto::session_response::Event::Result(upload_result)) = upload_response.event
        else {
            panic!("web console upload rejection should return a command result");
        };
        assert!(!upload_result.success);
        assert!(
            upload_result
                .message
                .contains("not supported in the web console")
        );

        let suggest_response = service
            .process_web_console_request(
                SessionRequest {
                    request: Some(proto::session_request::Request::Suggest(SuggestRequest {
                        input: "SHOW ".to_string(),
                        cursor: 5,
                        domain: "default".to_string(),
                    })),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        let Some(proto::session_response::Event::Suggest(suggest)) = suggest_response.event else {
            panic!("web console suggest should return a suggestion response");
        };
        assert!(
            suggest
                .suggestions
                .iter()
                .any(|suggestion| suggestion.value == "CLUSTER"),
            "expected CLUSTER suggestion, got: {:?}",
            suggest.suggestions
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }
}
