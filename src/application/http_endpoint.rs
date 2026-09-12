//! The HTTP and HTTPS listener that feeds configured endpoints.
//!
//! Layer: edges.
//!
//! - **Owns.** Endpoint request routing, the WebSocket upgrade, and the responses an endpoint
//!   returns to a client.
//! - **Depends on.** The runtime's endpoint dispatch and its signaling protocols.
//! - **Must not know.** Transactions, scheduling or how an ingested payload is processed.

use std::{convert::Infallible, sync::Arc as StdArc};

use error_stack::{Report, ResultExt};
use futures_util::SinkExt;
use http_body_util::{BodyExt, Empty, Full};
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
use nervix_recovery::NoReceiver;
use parking_lot::RwLock;
use rustls::ServerConfig;
use tokio::{net::TcpListener, task::JoinSet, time::Duration};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::derive_accept_key,
        protocol::{CloseFrame, Role, frame::coding::CloseCode},
    },
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::warn;
use triomphe::Arc;

use super::AppError;
use crate::runtime::{
    IngestMessageHeaders, RetainedIngestHeaders, Runtime, SignalingDataSink,
    WebsocketSignalingSession,
};
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

/// Ingests data frames that arrive on a server-side endpoint while its handshake is running.
struct EndpointSignalingDataSink<'a> {
    runtime: &'a Runtime,
    host: &'a str,
    path: &'a str,
    headers: &'a RetainedIngestHeaders,
}

impl SignalingDataSink for EndpointSignalingDataSink<'_> {
    async fn accept(&self, payload: Vec<u8>) {
        self.runtime
            .dispatch_websocket_payload(self.host, self.path, payload.as_slice(), self.headers)
            .await;
    }
}

/// The headers of one borrowed request, skipping values that are not UTF-8.
struct HyperRequestHeaders<'a>(&'a hyper::HeaderMap);

impl IngestMessageHeaders for HyperRequestHeaders<'_> {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in self.0 {
            if let Ok(value) = value.to_str() {
                visit(name.as_str(), value);
            }
        }
    }
}

async fn handle_http_request(
    runtime: Runtime,
    request_tasks: TaskTracker,
    shutdown: CancellationToken,
    mut request: HyperRequest<HyperIncoming>,
) -> Result<HyperResponse<Empty<Bytes>>, Infallible> {
    let mut host = String::new();
    if let Some(value) = request.headers().get(HOST)
        && let Ok(value) = value.to_str()
    {
        host = value.to_string();
    }
    let path = request.uri().path().to_string();

    if runtime.has_websocket_endpoint(&host, &path).await {
        if !is_websocket_upgrade_request(&request) {
            return Ok(response_with_status(StatusCode::UPGRADE_REQUIRED));
        }
        let admission = runtime.websocket_endpoint_admission(&host, &path).await;
        if !admission.is_accepted() {
            return Ok(endpoint_rejection_response(admission.retry_after));
        }
        // The session outlives the upgrade request, so its handshake headers are copied
        // once here and appended from that copy for every later frame.
        let headers = RetainedIngestHeaders::capture(&HyperRequestHeaders(request.headers()));

        let Some(sec_websocket_key) = request.headers().get(SEC_WEBSOCKET_KEY) else {
            return Ok(response_with_status(StatusCode::BAD_REQUEST));
        };
        let Ok(sec_websocket_key) = sec_websocket_key.to_str() else {
            return Ok(response_with_status(StatusCode::BAD_REQUEST));
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
            .body(empty_body())
            .assured(
                "the status and header values are typed constants or generated ASCII, which the \
                 http builder always accepts",
            );

        let on_upgrade = upgrade::on(&mut request);
        request_tasks.spawn(async move {
            let upgraded = tokio::select! {
                _ = shutdown.cancelled() => return,
                upgraded = on_upgrade => upgraded,
            };
            match upgraded {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    let mut websocket =
                        WebSocketStream::from_raw_socket(io, Role::Server, None).await;

                    if let Some(protocol) = runtime
                        .websocket_endpoint_signaling_protocol(&host, &path)
                        .await
                    {
                        let session = WebsocketSignalingSession::new(protocol);
                        let sink = EndpointSignalingDataSink {
                            runtime: &runtime,
                            host: &host,
                            path: &path,
                            headers: &headers,
                        };
                        let session_result = tokio::select! {
                            _ = shutdown.cancelled() => return,
                            result = session.run(&mut websocket, &sink) => result,
                        };
                        if let Err(error) = session_result {
                            warn!(
                                error = %error,
                                host,
                                path,
                                "websocket signaling failed"
                            );
                            return;
                        }
                    }

                    loop {
                        let message = tokio::select! {
                            _ = shutdown.cancelled() => break,
                            message = futures_util::StreamExt::next(&mut websocket) => message,
                        };
                        let Some(message) = message else {
                            break;
                        };
                        match message {
                            Ok(Message::Text(payload)) => {
                                let outcome = runtime
                                    .dispatch_websocket_payload(
                                        &host,
                                        &path,
                                        payload.as_bytes(),
                                        &headers,
                                    )
                                    .await;
                                if !outcome.is_accepted() {
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
                            Ok(Message::Binary(payload)) => {
                                let outcome = runtime
                                    .dispatch_websocket_payload(
                                        &host,
                                        &path,
                                        payload.as_ref(),
                                        &headers,
                                    )
                                    .await;
                                if !outcome.is_accepted() {
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
                            Ok(Message::Ping(payload)) => {
                                if websocket.send(Message::Pong(payload)).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
                            Err(error) => {
                                warn!(error = %error, host, path, "websocket session failed");
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(error = %error, host, path, "http upgrade failed");
                }
            }
        });

        return Ok(response);
    }

    if runtime.has_http_endpoint(&host, &path).await {
        if request.method() != Method::POST {
            return Ok(response_with_status(StatusCode::METHOD_NOT_ALLOWED));
        }
        let body = match request.body_mut().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(error) => {
                warn!(error = %error, host, path, "failed to read http request body");
                return Ok(response_with_status(StatusCode::BAD_REQUEST));
            }
        };

        let outcome = runtime
            .dispatch_http_payload(
                &host,
                &path,
                body.as_ref(),
                &HyperRequestHeaders(request.headers()),
            )
            .await;
        return Ok(if outcome.is_accepted() {
            response_with_status(StatusCode::ACCEPTED)
        } else {
            endpoint_rejection_response(outcome.retry_after)
        });
    }

    Ok(response_with_status(StatusCode::NOT_FOUND))
}

pub(in crate::application) async fn serve_http(
    runtime: Runtime,
    request_tasks: TaskTracker,
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
                accepted.change_context(AppError::ServeHttp)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeHttp)?;
        let runtime = runtime.clone();
        let request_tasks = request_tasks.clone();
        let request_shutdown = shutdown.clone();
        connection_tasks.spawn(async move {
            let io = TokioIo::new(stream);
            if let Err(error) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| {
                        handle_http_request(
                            runtime.clone(),
                            request_tasks.clone(),
                            request_shutdown.clone(),
                            request,
                        )
                    }),
                )
                .with_upgrades()
                .await
            {
                warn!(error = %error, "http connection failed");
            }
        });
    }
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

pub(in crate::application) async fn serve_https(
    runtime: Runtime,
    request_tasks: TaskTracker,
    http_tls_server_config: Arc<RwLock<Option<StdArc<ServerConfig>>>>,
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
                accepted.change_context(AppError::ServeHttps)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeHttps)?;
        let runtime = runtime.clone();
        let request_tasks = request_tasks.clone();
        let request_shutdown = shutdown.clone();
        let http_tls_server_config = http_tls_server_config.clone();
        connection_tasks.spawn(async move {
            let Some(tls_config) = http_tls_server_config.read().clone() else {
                warn!("https connection rejected because no VHOST TLS configuration is loaded");
                return;
            };
            let acceptor = TlsAcceptor::from(tls_config);
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    let io = TokioIo::new(tls_stream);
                    if let Err(error) = http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |request| {
                                handle_http_request(
                                    runtime.clone(),
                                    request_tasks.clone(),
                                    request_shutdown.clone(),
                                    request,
                                )
                            }),
                        )
                        .with_upgrades()
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
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        super::{subscription::format_stream_message, test_fixtures::string_branch_key},
        *,
    };
    use crate::{runtime::RelayMessage, runtime_schema};

    #[test]
    fn http_request_helpers_detect_upgrade_and_format_messages() {
        let header = hyper::header::HeaderValue::from_static("keep-alive, Upgrade");
        assert!(header_contains_token(&header, "upgrade"));
        assert!(!header_contains_token(&header, "websocket"));

        let message = RelayMessage {
            key: string_branch_key("tenant", "acme"),
            record: runtime_schema::test_runtime_row([(
                "user_id".to_string(),
                runtime_schema::RuntimeValue::U32(42),
            )]),
            acks: crate::runtime_ack::AckSet::empty(),
        };
        let no_sensitive_fields = nervix_vm::SchemaSensitivity::default();
        assert_eq!(
            format_stream_message(&message, &no_sensitive_fields),
            r#"key={"tenant":"acme"} payload={"user_id":42}"#
        );
        let sensitive_user_id = nervix_vm::SchemaSensitivity::from_sensitive_fields(["user_id"]);
        assert_eq!(
            format_stream_message(&message, &sensitive_user_id),
            r#"key={"tenant":"acme"} payload={"user_id":"<masked>"}"#
        );

        let no_key = RelayMessage {
            key: None,
            record: runtime_schema::test_runtime_row([(
                "user_id".to_string(),
                runtime_schema::RuntimeValue::U32(42),
            )]),
            acks: crate::runtime_ack::AckSet::empty(),
        };
        assert_eq!(
            format_stream_message(&no_key, &no_sensitive_fields),
            r#"{"user_id":42}"#
        );
    }
}
