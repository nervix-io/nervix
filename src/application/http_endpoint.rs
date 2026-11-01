//! The HTTP and HTTPS listener that feeds configured endpoints.
//!
//! Layer: edges.
//!
//! - **Owns.** Endpoint request routing, the WebSocket upgrade, reading the metadata and the UTF-8
//!   header values of a request and a WebSocket frame, and the responses an endpoint returns to a
//!   client.
//! - **Depends on.** The runtime's endpoint dispatch and its signaling protocols.
//! - **Must not know.** Transactions, scheduling or how an ingested payload is processed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{convert::Infallible, net::SocketAddr, time::Duration};

use error_stack::{Report, ResultExt};
use futures_util::SinkExt;
use http_body_util::{BodyExt, Either, Empty, Full};
use hyper::{
    Method, Request as HyperRequest, Response as HyperResponse, StatusCode,
    body::{Bytes, Incoming as HyperIncoming},
    header::{
        CONNECTION, HOST, RETRY_AFTER, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY,
        SEC_WEBSOCKET_VERSION, UPGRADE,
    },
    server::conn::http1,
    service::service_fn,
    upgrade,
};
use hyper_util::rt::TokioIo;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    IngestMessageHeaders, IngestMetadataRow, IngestRowHeaders, RetainedIngestHeaders,
};
use nervix_connector_websockets::{
    SignalingDataEmitter, WebsocketDataFrame, WebsocketSignalingSession,
};
use nervix_models::EndpointType;
use nervix_primitives::{
    net::TcpListener,
    sync::{Arc, CancellationToken},
    task::JoinSet,
};
use nervix_recovery::NoReceiver;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::derive_accept_key,
        protocol::{CloseFrame, Role, frame::coding::CloseCode},
    },
};
use tracing::warn;

use super::{
    AppError, node_initialization::NodeInitialization, service_tasks::ServiceTasks,
    tls::HttpsListenerCertificates,
};
use crate::runtime::{
    EndpointDispatchOutcome, EndpointRequestOutcome, EndpointResolution,
    HttpEndpointRequestMetadata, ResolvedEndpointRoute, Runtime, WebsocketsEndpointFrameMetadata,
};

type EndpointResponseBody = Either<Empty<Bytes>, Full<Bytes>>;

fn empty_body() -> Empty<Bytes> {
    Empty::new()
}

pub(in crate::application) fn response_with_status(
    status: StatusCode,
) -> HyperResponse<Empty<Bytes>> {
    HyperResponse::builder()
        .status(status)
        .body(empty_body())
        .assured(
            "the status and header values are typed constants or generated ASCII, which the http \
             builder always accepts",
        )
}

fn endpoint_rejection_response(retry_after: Option<Duration>) -> HyperResponse<Empty<Bytes>> {
    let mut response = HyperResponse::builder().status(StatusCode::SERVICE_UNAVAILABLE);
    if let Some(retry_after) = retry_after {
        // `Retry-After` is whole seconds, so a sub-second remainder rounds the wait up.
        let seconds = retry_after
            .as_secs()
            .checked_add(u64::from(retry_after.subsec_nanos() > 0))
            .assured("a Duration's whole seconds leave room for the rounding increment");
        response = response.header(RETRY_AFTER, seconds.to_string());
    }
    response.body(empty_body()).assured(
        "the status and header values are typed constants or generated ASCII, which the http \
         builder always accepts",
    )
}

fn endpoint_dispatch_response(
    outcome: EndpointDispatchOutcome,
) -> HyperResponse<EndpointResponseBody> {
    let Some(reply) = outcome.acknowledgement() else {
        let response = if outcome.is_accepted() {
            response_with_status(StatusCode::ACCEPTED)
        } else {
            endpoint_rejection_response(outcome.retry_after())
        };
        return response.map(Either::Left);
    };
    let status = match reply.outcome {
        EndpointRequestOutcome::Completed => StatusCode::OK,
        EndpointRequestOutcome::Refused => StatusCode::BAD_REQUEST,
        EndpointRequestOutcome::NotAdmitted(_)
        | EndpointRequestOutcome::ProcessingFailed(_)
        | EndpointRequestOutcome::Interrupted => StatusCode::SERVICE_UNAVAILABLE,
    };
    let mut response = response_with_bytes(status, reply.body(), "application/json");
    if status == StatusCode::SERVICE_UNAVAILABLE {
        let rejection = endpoint_rejection_response(Some(reply.retry_after));
        if let Some(value) = rejection.headers().get(RETRY_AFTER) {
            response.headers_mut().insert(RETRY_AFTER, value.clone());
        }
    }
    response.map(Either::Right)
}

pub(in crate::application) fn response_with_bytes(
    status: StatusCode,
    body: impl Into<Bytes>,
    content_type: &'static str,
) -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, content_type)
        .body(Full::new(body.into()))
        .assured(
            "the status and header values are typed constants or generated ASCII, which the http \
             builder always accepts",
        )
}

pub(in crate::application) fn text_response(
    status: StatusCode,
    body: impl Into<Bytes>,
) -> HyperResponse<Full<Bytes>> {
    response_with_bytes(status, body, "text/plain; charset=utf-8")
}

fn header_contains_token(value: &hyper::header::HeaderValue, expected: &str) -> bool {
    value.to_str().ok().is_some_and(|raw| {
        raw.split(',')
            .any(|part| part.trim().eq_ignore_ascii_case(expected))
    })
}

pub(in crate::application) fn is_websocket_upgrade_request(
    request: &HyperRequest<HyperIncoming>,
) -> bool {
    request.method() == Method::GET
        && request
            .headers()
            .get(CONNECTION)
            .is_some_and(|value| header_contains_token(value, "upgrade"))
        && request
            .headers()
            .get(UPGRADE)
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
        && request
            .headers()
            .get(SEC_WEBSOCKET_VERSION)
            .is_some_and(|value| value.as_bytes() == b"13")
}

/// One server-side WebSocket connection, which serves its session and hands every data frame it
/// dispatches the route it selected when it opened, the snapshot of its upgrade request's headers,
/// the peer that opened it and the path its upgrade request named.
struct EndpointWebsocketConnection {
    route: ResolvedEndpointRoute,
    headers: Arc<RetainedIngestHeaders>,
    remote_addr: SocketAddr,
    path: String,
}

impl EndpointWebsocketConnection {
    /// Dispatches `frame` to the ingestors bound to the connection's route, each reading the frame's
    /// metadata and the connection's headers.
    async fn dispatch(
        &self,
        runtime: &Runtime,
        frame: &WebsocketDataFrame,
        shutdown: &CancellationToken,
    ) -> EndpointDispatchOutcome {
        let metadata = WebsocketsEndpointFrameMetadata {
            frame_type: frame.frame_type(),
            remote_addr: self.remote_addr,
            path: &self.path,
        };
        let row = IngestMetadataRow {
            fields: &metadata,
            headers: IngestRowHeaders::Shared(&self.headers),
            position: None,
        };
        self.route
            .dispatch(runtime, frame.payload(), row, shutdown)
            .await
    }

    /// Serves the connection once its upgrade completes: runs the route's signaling protocol when
    /// it has one, then dispatches every data frame until the client closes the connection, a
    /// reply cannot be sent or the node shuts down.
    async fn serve(
        self,
        runtime: Runtime,
        on_upgrade: upgrade::OnUpgrade,
        host: String,
        shutdown: CancellationToken,
    ) {
        let upgraded = nervix_primitives::select! {
            _ = shutdown.cancelled() => return,
            upgraded = on_upgrade => upgraded,
        };
        let upgraded = match upgraded {
            Ok(upgraded) => upgraded,
            Err(error) => {
                warn!(
                    error = %error,
                    host,
                    path = self.path.as_str(),
                    "http upgrade failed"
                );
                return;
            }
        };
        let io = TokioIo::new(upgraded);
        let mut websocket = WebSocketStream::from_raw_socket(io, Role::Server, None).await;

        let connector = EndpointSignalingDataEmitter {
            runtime: &runtime,
            connection: &self,
            shutdown: &shutdown,
        };
        if let Some(protocol) = self.route.signaling_protocol() {
            let session = WebsocketSignalingSession::new(protocol.clone());
            let session_result = nervix_primitives::select! {
                _ = shutdown.cancelled() => return,
                result = session.run(&mut websocket, &connector) => result,
            };
            if let Err(error) = session_result {
                warn!(
                    error = %error,
                    host,
                    path = self.path.as_str(),
                    "websocket signaling failed"
                );
                return;
            }
        }

        loop {
            nervix_primitives::task::consume_budget().await;
            let message = nervix_primitives::select! {
                _ = shutdown.cancelled() => break,
                message = futures_util::StreamExt::next(&mut websocket) => message,
            };
            let Some(message) = message else {
                break;
            };
            let frame = match message {
                Ok(Message::Text(text)) => WebsocketDataFrame::Text(text),
                Ok(Message::Binary(bytes)) => WebsocketDataFrame::Binary(bytes),
                Ok(Message::Ping(payload)) => {
                    if websocket.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                    continue;
                }
                Ok(Message::Close(_)) => break,
                Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => continue,
                Err(error) => {
                    warn!(
                        error = %error,
                        host,
                        path = self.path.as_str(),
                        "websocket session failed"
                    );
                    break;
                }
            };
            let outcome = self.dispatch(&runtime, &frame, &shutdown).await;
            if let Some(reply) = connector.reply(outcome) {
                if websocket.send(reply).await.is_err() {
                    break;
                }
            } else if !outcome.is_accepted() {
                websocket
                    .send(Message::Close(Some(CloseFrame {
                        code: CloseCode::Again,
                        reason: "Try Again Later".into(),
                    })))
                    .await
                    .means_peer_left("websocket ingest client");
                break;
            }
        }
    }
}

/// Ingests data frames that arrive on a server-side endpoint while its handshake is running.
struct EndpointSignalingDataEmitter<'a> {
    runtime: &'a Runtime,
    connection: &'a EndpointWebsocketConnection,
    shutdown: &'a CancellationToken,
}

impl SignalingDataEmitter for EndpointSignalingDataEmitter<'_> {
    async fn accept(&self, frame: WebsocketDataFrame) {
        self.connection
            .dispatch(self.runtime, &frame, self.shutdown)
            .await;
    }

    async fn accept_and_reply(&self, frame: WebsocketDataFrame) -> Option<Message> {
        let outcome = self
            .connection
            .dispatch(self.runtime, &frame, self.shutdown)
            .await;
        self.reply(outcome)
    }
}

impl EndpointSignalingDataEmitter<'_> {
    fn reply(&self, outcome: EndpointDispatchOutcome) -> Option<Message> {
        let reply = outcome.acknowledgement()?;
        let Some(protocol) = self.connection.route.signaling_protocol() else {
            warn!(
                "the selected endpoint route cannot encode its acknowledgement without a \
                 signaling protocol"
            );
            return Some(Message::Close(Some(CloseFrame {
                code: CloseCode::Error,
                reason: "acknowledgement protocol unavailable".into(),
            })));
        };
        match protocol.encode_acknowledgement(reply.value()) {
            Ok(frame) => Some(frame),
            Err(error) => {
                warn!(error = %format_args!("{error:#}"), "failed to encode an endpoint acknowledgement");
                Some(Message::Close(Some(CloseFrame {
                    code: CloseCode::Error,
                    reason: "acknowledgement encoding failed".into(),
                })))
            }
        }
    }
}

/// The headers of one borrowed request, skipping values that are not UTF-8.
struct HyperRequestHeaders<'a>(&'a hyper::HeaderMap);

impl HyperRequestHeaders<'_> {
    /// How many of the request's header values are not UTF-8, which no ingestor reads.
    fn undecodable_values(&self) -> usize {
        self.0
            .values()
            .filter(|value| std::str::from_utf8(value.as_bytes()).is_err())
            .count()
    }
}

impl IngestMessageHeaders for HyperRequestHeaders<'_> {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in self.0 {
            if let Ok(value) = std::str::from_utf8(value.as_bytes()) {
                visit(name.as_str(), value);
            }
        }
    }
}

async fn handle_http_request(
    runtime: Runtime,
    initialization: Arc<NodeInitialization>,
    request_tasks: ServiceTasks,
    shutdown: CancellationToken,
    remote_addr: SocketAddr,
    request: HyperRequest<HyperIncoming>,
) -> Result<HyperResponse<EndpointResponseBody>, Infallible> {
    if shutdown.is_cancelled() || !initialization.is_ready() {
        return Ok(endpoint_rejection_response(None).map(Either::Left));
    }
    dispatch_http_request(runtime, request_tasks, shutdown, remote_addr, request).await
}

async fn dispatch_http_request(
    runtime: Runtime,
    request_tasks: ServiceTasks,
    shutdown: CancellationToken,
    remote_addr: SocketAddr,
    mut request: HyperRequest<HyperIncoming>,
) -> Result<HyperResponse<EndpointResponseBody>, Infallible> {
    let mut host = String::new();
    if let Some(value) = request.headers().get(HOST)
        && let Ok(value) = value.to_str()
    {
        host = value.to_string();
    }
    let path = request.uri().path().to_string();

    let route = match runtime.resolve_endpoint(&host, &path) {
        EndpointResolution::Routed(route) => route,
        EndpointResolution::Unrouted => {
            return Ok(response_with_status(StatusCode::NOT_FOUND).map(Either::Left));
        }
        // Before its first revision the node cannot tell a route the cluster binds from one no
        // domain declares, so a missing route is temporarily unavailable rather than not found.
        EndpointResolution::Uninstalled => {
            return Ok(endpoint_rejection_response(None).map(Either::Left));
        }
    };
    if route.endpoint_type() == EndpointType::Websockets {
        if !is_websocket_upgrade_request(&request) {
            return Ok(response_with_status(StatusCode::UPGRADE_REQUIRED).map(Either::Left));
        }
        if let Some(refusal) = route.refusal() {
            return Ok(endpoint_dispatch_response(refusal));
        }

        let Some(sec_websocket_key) = request.headers().get(SEC_WEBSOCKET_KEY) else {
            return Ok(response_with_status(StatusCode::BAD_REQUEST).map(Either::Left));
        };
        let Ok(sec_websocket_key) = sec_websocket_key.to_str() else {
            return Ok(response_with_status(StatusCode::BAD_REQUEST).map(Either::Left));
        };
        let sec_websocket_key = sec_websocket_key.to_owned();

        let request_headers = HyperRequestHeaders(request.headers());
        route.count_dropped_header_values(request_headers.undecodable_values());
        // The session outlives the upgrade request, so its handshake headers are copied
        // once here and shared by every later frame.
        let connection = EndpointWebsocketConnection {
            route,
            headers: Arc::new(RetainedIngestHeaders::capture(&request_headers)),
            remote_addr,
            path,
        };

        let response = HyperResponse::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(
                SEC_WEBSOCKET_ACCEPT,
                derive_accept_key(sec_websocket_key.as_bytes()),
            )
            .body(empty_body())
            .assured(
                "the status and header values are typed constants or generated ASCII, which the \
                 http builder always accepts",
            );

        let on_upgrade = upgrade::on(&mut request);
        request_tasks.spawn(connection.serve(runtime, on_upgrade, host, shutdown));

        return Ok(response.map(Either::Left));
    }

    if route.endpoint_type() == EndpointType::Http {
        if request.method() != Method::POST {
            return Ok(response_with_status(StatusCode::METHOD_NOT_ALLOWED).map(Either::Left));
        }
        if let Some(refusal) = route.refusal() {
            return Ok(endpoint_dispatch_response(refusal));
        }
        let body = match request.body_mut().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(error) => {
                warn!(error = %error, host, path, "failed to read http request body");
                return Ok(response_with_status(StatusCode::BAD_REQUEST).map(Either::Left));
            }
        };
        if shutdown.is_cancelled() {
            return Ok(endpoint_rejection_response(None).map(Either::Left));
        }

        let request_headers = HyperRequestHeaders(request.headers());
        route.count_dropped_header_values(request_headers.undecodable_values());
        let metadata = HttpEndpointRequestMetadata::new(
            request.method().as_str(),
            &path,
            request.uri().query(),
            remote_addr,
            &host,
        );
        let row = IngestMetadataRow {
            fields: &metadata,
            headers: IngestRowHeaders::Borrowed(&request_headers),
            position: None,
        };
        let outcome = route
            .dispatch(&runtime, body.as_ref(), row, &shutdown)
            .await;
        return Ok(endpoint_dispatch_response(outcome));
    }

    Ok(response_with_status(StatusCode::NOT_FOUND).map(Either::Left))
}

/// Admission shutdown refuses further requests while finishing active responses. The caller's
/// existing node shutdown deadline bounds joining these connections.
async fn serve_endpoint_connection<S>(
    stream: S,
    remote_addr: SocketAddr,
    runtime: Runtime,
    initialization: Arc<NodeInitialization>,
    request_tasks: ServiceTasks,
    shutdown: CancellationToken,
) -> Result<(), hyper::Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let request_shutdown = shutdown.clone();
    let connection = http1::Builder::new()
        .serve_connection(
            TokioIo::new(stream),
            service_fn(move |request| {
                handle_http_request(
                    runtime.clone(),
                    initialization.clone(),
                    request_tasks.clone(),
                    request_shutdown.clone(),
                    remote_addr,
                    request,
                )
            }),
        )
        .with_upgrades();
    let mut connection = std::pin::pin!(connection);
    nervix_primitives::select! {
        result = &mut connection => result,
        () = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    }
}

pub(in crate::application) async fn serve_http(
    runtime: Runtime,
    initialization: Arc<NodeInitialization>,
    request_tasks: ServiceTasks,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>> {
    let mut connection_tasks = JoinSet::new();

    loop {
        let accepted = nervix_primitives::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() => {
                accepted.change_context(AppError::ServeHttp)
            }
        };
        let (stream, remote_addr) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeHttp)?;
        let runtime = runtime.clone();
        let initialization = initialization.clone();
        let request_tasks = request_tasks.clone();
        let request_shutdown = shutdown.clone();
        connection_tasks.spawn(async move {
            if let Err(error) = serve_endpoint_connection(
                stream,
                remote_addr,
                runtime,
                initialization,
                request_tasks,
                request_shutdown,
            )
            .await
            {
                warn!(error = %error, "http connection failed");
            }
        });
    }
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

pub(in crate::application) async fn serve_https(
    runtime: Runtime,
    initialization: Arc<NodeInitialization>,
    request_tasks: ServiceTasks,
    certificates: HttpsListenerCertificates,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>> {
    let mut connection_tasks = JoinSet::new();

    loop {
        let accepted = nervix_primitives::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() => {
                accepted.change_context(AppError::ServeHttps)
            }
        };
        let (stream, remote_addr) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeHttps)?;
        let runtime = runtime.clone();
        let initialization = initialization.clone();
        let request_tasks = request_tasks.clone();
        let request_shutdown = shutdown.clone();
        let certificates = certificates.clone();
        connection_tasks.spawn(async move {
            let Some(tls_config) = certificates.server_config() else {
                warn!("https connection rejected because no VHOST TLS configuration is loaded");
                return;
            };
            let acceptor = TlsAcceptor::from(tls_config);
            let accepted = nervix_primitives::select! {
                () = request_shutdown.cancelled() => return,
                accepted = acceptor.accept(stream) => accepted,
            };
            match accepted {
                Ok(tls_stream) => {
                    if let Err(error) = serve_endpoint_connection(
                        tls_stream,
                        remote_addr,
                        runtime,
                        initialization,
                        request_tasks,
                        request_shutdown,
                    )
                    .await
                    {
                        warn!(error = %error, "https connection failed");
                    }
                }
                Err(error) => {
                    warn!(error = %error, "tls accept failed");
                }
            }
        });
    }
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// HTTP ENDPOINT and WEBSOCKETS ENDPOINT ingestors declare repeats by name with lowercase
    /// names: a request's fields are visited under their lowercase names, every value of a
    /// repeated field together and in the order the request carried them.
    #[test]
    fn request_fields_are_visited_lowercase_with_every_value_of_a_field_together() {
        let mut fields = hyper::HeaderMap::new();
        for (name, value) in [
            ("X-Trace", "a"),
            ("Tenant", "x"),
            ("x-trace", "b"),
            ("TENANT", "y"),
        ] {
            fields.append(
                hyper::header::HeaderName::from_bytes(name.as_bytes())
                    .expect("each name is a valid field name"),
                hyper::header::HeaderValue::from_static(value),
            );
        }
        let mut visited: Vec<(String, String)> = Vec::new();
        HyperRequestHeaders(&fields)
            .visit(&mut |name, value| visited.push((name.to_string(), value.to_string())));

        // The order of different names is not declared, so only each name's values are compared,
        // after checking that a name's values arrive together.
        let mut finished = std::collections::BTreeSet::new();
        let mut current: Option<&str> = None;
        let mut values: std::collections::BTreeMap<&str, Vec<&str>> =
            std::collections::BTreeMap::new();
        for (name, value) in &visited {
            if current != Some(name.as_str()) {
                if let Some(previous) = current {
                    finished.insert(previous);
                }
                assert!(
                    !finished.contains(name.as_str()),
                    "every value of {name} is visited together: {visited:?}"
                );
                current = Some(name);
            }
            values
                .entry(name.as_str())
                .or_default()
                .push(value.as_str());
        }
        let expected = std::collections::BTreeMap::from([
            ("tenant", vec!["x", "y"]),
            ("x-trace", vec!["a", "b"]),
        ]);
        assert_eq!(values, expected);
    }

    /// A request's header value reaches its ingestors when it is UTF-8, beyond ASCII included, and
    /// is skipped and counted when it is not.
    #[test]
    fn request_header_values_are_read_when_utf8_and_counted_when_not() {
        let mut fields = hyper::HeaderMap::new();
        fields.append(
            "x-city",
            hyper::header::HeaderValue::from_bytes("Zürich ✓".as_bytes())
                .expect("UTF-8 bytes are a valid field value"),
        );
        fields.append(
            "x-latin1",
            hyper::header::HeaderValue::from_bytes(b"Z\xfcrich")
                .expect("an obs-text byte is a valid field value"),
        );
        fields.append("x-plain", hyper::header::HeaderValue::from_static("ascii"));
        let headers = HyperRequestHeaders(&fields);
        let mut visited: Vec<(String, String)> = Vec::new();
        headers.visit(&mut |name, value| visited.push((name.to_string(), value.to_string())));
        visited.sort();

        assert_eq!(
            visited,
            vec![
                ("x-city".to_string(), "Zürich ✓".to_string()),
                ("x-plain".to_string(), "ascii".to_string()),
            ]
        );
        assert_eq!(headers.undecodable_values(), 1);
    }

    #[test]
    fn http_request_helpers_detect_upgrade_tokens() {
        let header = hyper::header::HeaderValue::from_static("keep-alive, Upgrade");
        assert!(header_contains_token(&header, "upgrade"));
        assert!(!header_contains_token(&header, "websocket"));
    }
}
