//! The console listener and the HTTP routes the browser drives through it.
//!
//! Layer: edges.
//!
//! - **Owns.** The console asset routes, authenticating and upgrading the console WebSocket, and
//!   the multipart resource upload path.
//! - **Depends on.** The session engine, which serves the upgraded WebSocket, and the resource
//!   installation the upload path shares with every other upload.
//! - **Must not know.** What a session request does once the WebSocket carries it.

use std::{
    convert::Infallible,
    io,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::Arc as StdArc,
};

use error_stack::{Report, ResultExt};
use futures_util::StreamExt;
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
use nervix_client_wire::SessionLimits;
use nervix_consensus::ConsensusError;
use nervix_models::{
    DomainName, NodeEndpoint, NodeServiceUrl, NodeServiceUrlParseError, ResourceName,
    ResourceUploadIdentity, ResourceUploadKey, UserName,
};
use rustls::ServerConfig;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{handshake::derive_accept_key, protocol::Role},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{
    AppError,
    authentication::{credentials_from_web_console_request, unauthorized_basic_response},
    http_endpoint::{is_websocket_upgrade_request, response_with_bytes, text_response},
    session::websocket::console_websocket_config,
    session_service::SessionServiceImpl,
};
use crate::resource::{ResourceStoreError, StagedResourceArchive};

const WEB_CONSOLE_INDEX: &[u8] = include_bytes!("../../crates/web-console/dist/index.html");

const WEB_CONSOLE_CSS: &[u8] = include_bytes!("../../crates/web-console/dist/console.css");

const WEB_CONSOLE_JS: &[u8] = include_bytes!("../../crates/web-console/dist/nervix-web-console.js");

const WEB_CONSOLE_WASM: &[u8] =
    include_bytes!("../../crates/web-console/dist/nervix-web-console_bg.wasm");

const WEB_CONSOLE_ICON: &[u8] = include_bytes!("../../crates/web-console/dist/nervix-icon.svg");

const WEB_CONSOLE_WS_PATH: &str = "/console/ws";
const WEB_CONSOLE_AUTH_CHECK_PATH: &str = "/console/auth";

const WEB_CONSOLE_RESOURCE_UPLOAD_PATH: &str = "/console/resources/upload";

pub(in crate::application) const WEB_CONSOLE_AUTH_QUERY_PARAM: &str = "auth";

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
    if request.method() == Method::GET && request.uri().path() == WEB_CONSOLE_AUTH_CHECK_PATH {
        let Some(credentials) = credentials_from_web_console_request(&request) else {
            return Ok(text_response(
                StatusCode::UNAUTHORIZED,
                "authentication failed",
            ));
        };
        let Some(_) = service.authenticate_basic_credentials(&credentials).await else {
            return Ok(text_response(
                StatusCode::UNAUTHORIZED,
                "authentication failed",
            ));
        };
        return Ok(text_response(StatusCode::NO_CONTENT, ""));
    }

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
                _ = service.inner.admission_shutdown.cancelled() => return,
                upgraded = on_upgrade => upgraded,
            };
            let upgraded = match upgraded {
                Ok(upgraded) => upgraded,
                Err(error) => {
                    warn!(error = %error, "web console websocket upgrade failed");
                    return;
                }
            };
            let limits = SessionLimits::DEFAULT;
            let config = console_websocket_config(&limits);
            let websocket = WebSocketStream::from_raw_socket(
                TokioIo::new(upgraded),
                Role::Server,
                Some(config),
            )
            .await;
            service
                .serve_console_session(authenticated_user, limits, websocket)
                .await;
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
    advertise_addr: Option<NodeEndpoint>,
    listen_addr: SocketAddr,
    https_listen_addr: Option<SocketAddr>,
) -> error_stack::Result<NodeServiceUrl, NodeServiceUrlParseError> {
    if let Some(endpoint) = advertise_addr {
        return NodeServiceUrl::new("http", &endpoint);
    }

    let (scheme, default_addr) = match https_listen_addr {
        Some(addr) => ("https", addr),
        None => ("http", listen_addr),
    };
    NodeServiceUrl::new(scheme, &NodeEndpoint::from(default_addr))
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
                Ok(installation) => web_console_upload_text_response(
                    StatusCode::OK,
                    format!("uploaded resource version {}", installation.version),
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
}

#[cfg(test)]
mod tests {
    use super::{super::test_fixtures::test_addr, *};

    #[test]
    fn web_console_advertise_url_uses_https_listener_when_available() {
        assert_eq!(
            web_console_advertise_url(None, test_addr(17420), Some(test_addr(17443)))
                .assured("a listen address forms a url")
                .as_str(),
            "https://127.0.0.1:17443"
        );
        assert_eq!(
            web_console_advertise_url(
                Some(NodeEndpoint::from(test_addr(17420))),
                test_addr(17420),
                Some(test_addr(17443))
            )
            .assured("an advertised endpoint forms a url")
            .as_str(),
            "http://127.0.0.1:17420"
        );
        assert_eq!(
            web_console_advertise_url(None, test_addr(17420), None)
                .assured("a listen address forms a url")
                .as_str(),
            "http://127.0.0.1:17420"
        );
    }
}
