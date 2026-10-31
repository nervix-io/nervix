//! The outbound HTTP emitter's transport against loopback endpoints and proxies.
//!
//! Layer: test harness.
//! - **Owns.** Loopback endpoints and proxies that answer with exact bytes, an HTTP/2 endpoint
//!   served by hyper, resolvers with chosen answers, and the assertions that hold the emitter's
//!   requests, routes, redirects, budgets and descriptions to its contract.
//! - **Depends on.** The emitter, hyper's server, rustls and rcgen, and the node resolver.
//! - **Must not know.** The host, relays, branches or acknowledgements.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use std::net::SocketAddr;

use base64::Engine as _;
use error_stack::{AttachmentKind, FrameKind};
use meticulous::ResultExt as _;
use nervix_models::{
    HttpApplicationHeaders, HttpBodyMode, HttpHeaderName, HttpHeaderValue, HttpMethod, HttpOrigin,
    Timestamp,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::{response::ResponseHeadError, *};

fn prepared(method: &str, body: Option<Vec<u8>>) -> EmitterHttpRequest {
    let origin = HttpOrigin::parse("https://api.example.com:8443")
        .assured("the test origin has a valid HTTPS scheme, host and port");
    let target = origin
        .target("/v1/a/../events?q=a+b")
        .assured("the test target is origin-relative");
    let mut headers = HttpApplicationHeaders::default();
    headers
        .insert(
            HttpHeaderName::parse("X-Tenant")
                .assured("X-Tenant is a valid application header name"),
            HttpHeaderValue::parse("north").assured("north is a valid application header value"),
        )
        .assured("one short header fits the application envelope");
    let mode = match body {
        Some(_) => HttpBodyMode::Codec,
        None => HttpBodyMode::WithoutBody,
    };
    EmitterHttpRequest {
        id: EmitterRecordId::new(3),
        method: HttpMethod::parse(method, mode).assured("the test method is a valid HTTP token"),
        target,
        headers,
        body,
        occurred_at: Timestamp::from_unix_nanos(1),
    }
}

/// Every field of `request`, by name and value, in order.
fn fields(request: &Request<Full<Bytes>>) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    for (name, value) in request.headers() {
        let value = value
            .to_str()
            .assured("the test fields are visible ASCII")
            .to_string();
        fields.push((name.as_str().to_string(), value));
    }
    fields
}

fn field(name: &str, value: &str) -> (String, String) {
    (name.to_string(), value.to_string())
}

#[test]
fn an_http1_request_names_its_target_in_origin_form_with_the_transport_fields() {
    let request = prepared("PATCH", Some(br#"{"id":1}"#.to_vec()));
    let url = request.target.url();
    let sent =
        HttpRequestEmitter::http_request(&request, url, HttpVersion::Http1, RequestForm::Direct)
            .assured("valid prepared fields convert");
    assert_eq!(sent.method().as_str(), "PATCH");
    assert_eq!(sent.uri().to_string(), "/v1/events?q=a+b");
    assert_eq!(
        fields(&sent),
        vec![
            field("host", "api.example.com:8443"),
            field("connection", "close"),
            field("accept", "*/*"),
            field("x-tenant", "north"),
        ]
    );
}

#[test]
fn an_http2_request_names_its_absolute_target_and_leaves_connection_fields_to_the_protocol() {
    let request = prepared("purge", None);
    let url = request.target.url();
    let sent =
        HttpRequestEmitter::http_request(&request, url, HttpVersion::Http2, RequestForm::Direct)
            .assured("valid prepared fields convert");
    assert_eq!(sent.method().as_str(), "purge");
    assert_eq!(
        sent.uri().to_string(),
        "https://api.example.com:8443/v1/events?q=a+b"
    );
    assert_eq!(
        fields(&sent),
        vec![field("accept", "*/*"), field("x-tenant", "north")]
    );
}

#[test]
fn a_forwarded_request_names_its_absolute_target_and_carries_the_proxy_credentials() {
    let request = prepared("POST", Some(b"{}".to_vec()));
    let url = request.target.url();
    let sent = HttpRequestEmitter::http_request(
        &request,
        url,
        HttpVersion::Http1,
        RequestForm::Forwarded {
            authorization: Some("Basic c2NlbmFyaW86c2VjcmV0"),
        },
    )
    .assured("valid prepared fields convert");
    assert_eq!(
        sent.uri().to_string(),
        "https://api.example.com:8443/v1/events?q=a+b"
    );
    assert_eq!(
        fields(&sent),
        vec![
            field("host", "api.example.com:8443"),
            field("connection", "close"),
            field("proxy-authorization", "Basic c2NlbmFyaW86c2VjcmV0"),
            field("accept", "*/*"),
            field("x-tenant", "north"),
        ]
    );
    let credentials = sent
        .headers()
        .get(PROXY_AUTHORIZATION)
        .assured("the forwarded request carries the proxy's credentials");
    assert!(credentials.is_sensitive());
}

#[test]
fn a_bodyless_request_declares_an_empty_length_only_when_its_method_defines_content() {
    for method in ["POST", "PUT", "PATCH", "post", "Patch"] {
        let request = prepared(method, None);
        let sent = HttpRequestEmitter::http_request(
            &request,
            request.target.url(),
            HttpVersion::Http1,
            RequestForm::Direct,
        )
        .assured("valid prepared fields convert");
        assert_eq!(
            sent.headers()
                .get(CONTENT_LENGTH)
                .map(HeaderValue::as_bytes),
            Some(&b"0"[..]),
            "{method} defines its request content, so its absence is declared"
        );
    }
    for method in ["DELETE", "OPTIONS", "GET", "purge"] {
        let request = prepared(method, None);
        let sent = HttpRequestEmitter::http_request(
            &request,
            request.target.url(),
            HttpVersion::Http1,
            RequestForm::Direct,
        )
        .assured("valid prepared fields convert");
        assert!(
            sent.headers().get(CONTENT_LENGTH).is_none(),
            "{method} defines no request content, so no length is declared"
        );
    }
}

#[test]
fn prepared_accept_replaces_the_default() {
    let mut request = prepared("GET", None);
    request
        .headers
        .insert(
            HttpHeaderName::parse("Accept").assured("Accept is a valid application header name"),
            HttpHeaderValue::parse("application/json")
                .assured("application/json is a valid header value"),
        )
        .assured("two short headers fit the application envelope");
    let sent = HttpRequestEmitter::http_request(
        &request,
        request.target.url(),
        HttpVersion::Http1,
        RequestForm::Direct,
    )
    .assured("valid prepared fields convert");
    let accepts: Vec<&[u8]> = sent
        .headers()
        .get_all(ACCEPT)
        .iter()
        .map(HeaderValue::as_bytes)
        .collect();
    assert_eq!(accepts, vec![&b"application/json"[..]]);
}

#[test]
fn a_tunnel_request_names_the_target_authority_with_its_port_and_the_proxy_credentials() {
    for (target, authority) in [
        ("https://api.example.com", "api.example.com:443"),
        ("https://[::1]:9443", "[::1]:9443"),
    ] {
        let url = Url::parse(target).assured("the test targets are valid URLs");
        let request = HttpRequestEmitter::tunnel_request(&url, Some("Basic c2NlbmFyaW86c2VjcmV0"))
            .assured("a target with a host and port has a tunnel");
        assert_eq!(request.method(), Method::CONNECT);
        assert_eq!(request.uri().to_string(), authority);
        assert_eq!(
            fields(&request),
            vec![
                field("host", authority),
                field("proxy-authorization", "Basic c2NlbmFyaW86c2VjcmV0"),
            ]
        );
    }
}

fn final_response(status: u16, fields: &[(&'static str, &'static str)]) -> FinalResponse {
    let mut response = Response::new(());
    *response.status_mut() = http::StatusCode::from_u16(status).assured("the test status is valid");
    for (name, value) in fields {
        response
            .headers_mut()
            .append(*name, HeaderValue::from_static(value));
    }
    FinalResponse::read(&response).assured("the test status is in range")
}

#[test]
fn a_retryable_status_carries_only_the_delay_its_retry_after_asks_for() {
    let limited = final_response(429, &[("retry-after", "30")]);
    let error = HttpRequestEmitter::retryable_status_error(
        HttpAttemptError::RetryableStatus { status: 429 },
        &limited,
    );
    assert_eq!(
        error.downcast_ref::<EmitterRetryDelay>(),
        Some(&EmitterRetryDelay(Duration::from_secs(30)))
    );
    assert_eq!(
        *error.current_context(),
        EmitterWriteError::Publish {
            connector: HTTP_EMITTER
        }
    );

    let unstated = final_response(401, &[]);
    let error = HttpRequestEmitter::retryable_status_error(
        HttpAttemptError::Authentication { status: 401 },
        &unstated,
    );
    assert!(error.downcast_ref::<EmitterRetryDelay>().is_none());
}

#[test]
fn tls_server_name_uses_the_origin_host() {
    let origin = HttpOrigin::parse("https://127.0.0.1:8443")
        .assured("the test origin has a valid IP address");
    let target = origin
        .target("/")
        .assured("the root path is a valid target");
    let name = HttpRequestEmitter::server_name(target.url())
        .assured("a parsed HTTP target has a TLS server name");
    assert_eq!(name, ServerName::from(std::net::Ipv4Addr::LOCALHOST));
}

/// The description a failed attempt carries, which the emitter reports as its transient error.
fn description(error: &Report<EmitterWriteError>) -> Option<String> {
    for frame in error.frames() {
        if let FrameKind::Attachment(AttachmentKind::Printable(attachment)) = frame.kind() {
            return Some(attachment.to_string());
        }
    }
    None
}

#[test]
fn a_failed_attempt_describes_its_status_or_every_cause_beneath_it() {
    let status =
        HttpRequestEmitter::publish_error(Report::new(HttpAttemptError::RetryableStatus {
            status: 503,
        }));
    assert_eq!(
        *status.current_context(),
        EmitterWriteError::Publish {
            connector: HTTP_EMITTER
        }
    );
    assert_eq!(
        description(&status).as_deref(),
        Some("HTTP endpoint answered with retryable status 503")
    );

    let handshake = HttpRequestEmitter::publish_error(
        Report::new(std::io::Error::other(
            "invalid peer certificate: UnknownIssuer",
        ))
        .change_context(HttpAttemptError::Tls),
    );
    assert_eq!(
        description(&handshake).as_deref(),
        Some("HTTP TLS handshake failed: invalid peer certificate: UnknownIssuer")
    );

    let status = HttpRequestEmitter::publish_error(
        Report::new(ResponseHeadError::StatusOutOfRange { status: 600 })
            .change_context(HttpAttemptError::Response),
    );
    assert_eq!(
        description(&status).as_deref(),
        Some(
            "HTTP response header exchange failed: HTTP response status 600 is outside the range \
             HTTP defines"
        )
    );
}

/// An emitter whose client follows `redirects` and sends through `proxy`, trusting `ca` beside the
/// platform's roots, resolving through the host's own resolver; the tests below dial only literal
/// loopback addresses.
async fn emitter_with(
    redirects: Redirects,
    proxy: Option<&str>,
    ca: Option<&std::path::Path>,
) -> HttpRequestEmitter {
    let dns = DnsResolver::load(nervix_dns::DnsConfiguration::system())
        .await
        .assured("the host resolver configuration loads in tests");
    let mut declared = vec![ClientConfigEntry {
        key: "timeout_ms".to_string(),
        value: "10000".to_string(),
    }];
    if let Some(proxy) = proxy {
        declared.push(ClientConfigEntry {
            key: "proxy".to_string(),
            value: proxy.to_string(),
        });
    }
    if let Some(ca) = ca {
        declared.push(ClientConfigEntry {
            key: "tls_ca_file".to_string(),
            value: ca.display().to_string(),
        });
    }
    if let Redirects::Followed { .. } = redirects {
        declared.push(ClientConfigEntry {
            key: "follow_redirects".to_string(),
            value: "true".to_string(),
        });
    }
    HttpRequestEmitter::open(HttpRequestEmitterConfig {
        config: declared,
        dns,
    })
    .assured("the tests declare only valid clients")
}

/// A `POST` of `body` to `target` on `origin`.
fn post(origin: &str, target: &str, body: &[u8]) -> EmitterHttpRequest {
    let origin = HttpOrigin::parse(origin).assured("the test origin is an HTTP origin");
    EmitterHttpRequest {
        id: EmitterRecordId::new(4),
        method: HttpMethod::parse("POST", HttpBodyMode::Codec)
            .assured("POST is a valid HTTP token"),
        target: origin
            .target(target)
            .assured("the test target is origin-relative"),
        headers: HttpApplicationHeaders::default(),
        body: Some(body.to_vec()),
        occurred_at: Timestamp::from_unix_nanos(1),
    }
}

/// Answers the first connections `listener` accepts, one for each of `answers`, in order, and
/// returns what each carried: its request head and the body its `content-length` declares.
async fn serve_connections(
    listener: nervix_primitives::net::TcpListener,
    answers: Vec<&'static [u8]>,
) -> Vec<String> {
    let mut carried = Vec::new();
    for answer in answers {
        nervix_primitives::task::consume_budget().await;
        let (mut stream, _) = listener
            .accept()
            .await
            .assured("the emitter connects for every answer");
        let mut head = Vec::new();
        let mut byte = [0_u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            nervix_primitives::task::consume_budget().await;
            stream
                .read_exact(&mut byte)
                .await
                .assured("the emitter writes its whole request head");
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).assured("the test request heads are UTF-8");
        let mut length = 0_usize;
        for line in head.lines() {
            if let Some(value) = line.strip_prefix("content-length: ") {
                length = value
                    .parse()
                    .assured("the emitter declares a decimal length");
            }
        }
        let mut body = vec![0_u8; length];
        stream
            .read_exact(&mut body)
            .await
            .assured("the emitter writes the body it declares");
        stream
            .write_all(answer)
            .await
            .assured("the emitter reads the final head it waits for");
        let body = String::from_utf8(body).assured("the test bodies are UTF-8");
        carried.push(format!("{head}{body}"));
    }
    carried
}

async fn loopback_listener() -> (nervix_primitives::net::TcpListener, SocketAddr) {
    let listener = nervix_primitives::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the test host has a free loopback port");
    let address = listener
        .local_addr()
        .assured("a bound listener has a local address");
    (listener, address)
}

#[nervix_primitives::test]
async fn hyper_sends_the_prepared_request_byte_for_byte_and_the_head_delivers_it() {
    let (listener, address) = loopback_listener().await;
    let served = nervix_primitives::task::spawn(serve_connections(
        listener,
        vec![b"HTTP/1.1 202 Accepted\r\nContent-Length: 1048576\r\n\r\npartial"],
    ));
    let mut connector = emitter_with(Redirects::Refused, None, None).await;

    let outcome = connector
        .publish(vec![post(
            &format!("http://{address}"),
            "/events?tenant=north",
            b"{\"id\":1}",
        )])
        .await
        .into_parts();

    assert_eq!(outcome.delivered, vec![EmitterRecordId::new(4)]);
    let carried = served.await.assured("the endpoint served the request");
    assert_eq!(
        carried,
        vec![format!(
            "POST /events?tenant=north HTTP/1.1\r\nhost: {address}\r\nconnection: \
             close\r\naccept: */*\r\ncontent-length: 8\r\n\r\n{{\"id\":1}}"
        )]
    );
}

#[nervix_primitives::test]
async fn a_followed_redirect_repeats_the_prepared_request_at_its_location() {
    let (listener, address) = loopback_listener().await;
    let served = nervix_primitives::task::spawn(serve_connections(
        listener,
        vec![
            b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /moved?attempt=2#part\r\nContent-Length: \
              0\r\n\r\n",
            b"HTTP/1.1 204 No Content\r\n\r\n",
        ],
    ));
    let mut connector = emitter_with(Redirects::Followed { limit: 10 }, None, None).await;

    let outcome = connector
        .publish(vec![post(
            &format!("http://{address}"),
            "/events",
            b"{\"id\":1}",
        )])
        .await
        .into_parts();

    assert_eq!(outcome.delivered, vec![EmitterRecordId::new(4)]);
    let carried = served.await.assured("the endpoint served both requests");
    let expected = |target: &str| {
        format!(
            "POST {target} HTTP/1.1\r\nhost: {address}\r\nconnection: close\r\naccept: \
             */*\r\ncontent-length: 8\r\n\r\n{{\"id\":1}}"
        )
    };
    assert_eq!(
        carried,
        vec![expected("/events"), expected("/moved?attempt=2")]
    );
}

#[nervix_primitives::test]
async fn a_declared_proxy_forwards_a_plain_request_in_absolute_form_with_its_credentials() {
    let (listener, proxy) = loopback_listener().await;
    let served = nervix_primitives::task::spawn(serve_connections(
        listener,
        vec![b"HTTP/1.1 204 No Content\r\n\r\n"],
    ));
    let mut connector = emitter_with(
        Redirects::Refused,
        Some(&format!("http://scenario:p%40ss@{proxy}")),
        None,
    )
    .await;

    // Only the proxy could resolve this host, so its answer proves the request went through it.
    let outcome = connector
        .publish(vec![post("http://origin.invalid:8080", "/events", b"{}")])
        .await
        .into_parts();

    assert_eq!(outcome.delivered, vec![EmitterRecordId::new(4)]);
    let carried = served.await.assured("the proxy served the request");
    let credentials = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("scenario:p@ss")
    );
    assert_eq!(
        carried,
        vec![format!(
            "POST http://origin.invalid:8080/events HTTP/1.1\r\nhost: \
             origin.invalid:8080\r\nconnection: close\r\nproxy-authorization: \
             {credentials}\r\naccept: */*\r\ncontent-length: 2\r\n\r\n{{}}"
        )]
    );
}

#[nervix_primitives::test]
async fn a_tunnel_the_proxy_refuses_keeps_the_request_pending_without_its_credentials() {
    let (listener, proxy) = loopback_listener().await;
    let served = nervix_primitives::task::spawn(serve_connections(
        listener,
        vec![b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n"],
    ));
    let mut connector = emitter_with(
        Redirects::Refused,
        Some(&format!("http://scenario:hidden-secret@{proxy}")),
        None,
    )
    .await;

    let outcome = connector
        .publish(vec![post(
            "https://origin.invalid:8443",
            "/events/secret-7",
            b"{}",
        )])
        .await
        .into_parts();

    assert!(outcome.delivered.is_empty() && outcome.rejected.is_empty());
    let failure = outcome
        .infrastructure_error
        .assured("a refused tunnel leaves its request unresolved");
    assert_eq!(
        description(&failure).as_deref(),
        Some("HTTP proxy refused the tunnel with status 407")
    );
    let carried = served
        .await
        .assured("the proxy answered the tunnel request");
    let credentials = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("scenario:hidden-secret")
    );
    assert_eq!(
        carried,
        vec![format!(
            "CONNECT origin.invalid:8443 HTTP/1.1\r\nhost: \
             origin.invalid:8443\r\nproxy-authorization: {credentials}\r\n\r\n"
        )]
    );
}

#[nervix_primitives::test]
async fn a_proxy_that_cannot_be_reached_is_described_beneath_its_own_failure() {
    let (unused, proxy) = loopback_listener().await;
    drop(unused);
    let mut connector = emitter_with(
        Redirects::Refused,
        Some(&format!("http://scenario:hidden-secret@{proxy}")),
        None,
    )
    .await;

    let outcome = connector
        .publish(vec![post(
            "http://origin.invalid:8080",
            "/events/secret-7",
            b"{}",
        )])
        .await
        .into_parts();

    let failure = outcome
        .infrastructure_error
        .assured("an unreachable proxy leaves its request unresolved");
    let described = description(&failure).assured("a failed attempt is always described");
    assert!(
        described.starts_with("HTTP proxy could not be reached: HTTP connection failed: ")
            && !described.contains("secret"),
        "the proxy's own failure follows, and neither its credentials nor the target: {described}"
    );
}

#[nervix_primitives::test]
async fn a_response_head_hyper_refuses_fails_the_attempt_with_hyper_s_cause() {
    let (listener, address) = loopback_listener().await;
    let served = nervix_primitives::task::spawn(serve_connections(
        listener,
        vec![b"HTTP/1.1 200 OK\r\nContent-Length: nonsense\r\n\r\n"],
    ));
    let mut connector = emitter_with(Redirects::Refused, None, None).await;

    let outcome = connector
        .publish(vec![post(&format!("http://{address}"), "/events", b"{}")])
        .await
        .into_parts();

    assert!(outcome.delivered.is_empty() && outcome.rejected.is_empty());
    let failure = outcome
        .infrastructure_error
        .assured("a refused head leaves its request unresolved");
    assert_eq!(
        description(&failure).as_deref(),
        Some("HTTP response header exchange failed: invalid content-length parsed")
    );
    served.await.assured("the endpoint answered the request");
}

/// A certificate authority and a server certificate it issued for `127.0.0.1`, written beside each
/// other: the authority's certificate for the client to trust, and the server's chain and key.
struct TestTls {
    _files: tempfile::TempDir,
    ca: std::path::PathBuf,
    server: StdArc<rustls::ServerConfig>,
}

impl TestTls {
    fn issue() -> Self {
        let authority_key = rcgen::KeyPair::generate().assured("rcgen generates keys");
        let mut authority = rcgen::CertificateParams::new(Vec::<String>::new())
            .assured("an authority needs no name");
        authority.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let authority = authority
            .self_signed(&authority_key)
            .assured("an authority signs itself");
        let server_key = rcgen::KeyPair::generate().assured("rcgen generates keys");
        let server = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()])
            .assured("a loopback address is a valid subject name")
            .signed_by(&server_key, &authority, &authority_key)
            .assured("the authority issues the server certificate");
        let files = tempfile::tempdir().assured("the test host has a temporary directory");
        let ca = files.path().join("ca.pem");
        std::fs::write(&ca, authority.pem()).assured("the temporary directory is writable");
        let chain = vec![server.der().clone()];
        let key = rustls_pki_types::PrivateKeyDer::Pkcs8(
            rustls_pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()),
        );
        nervix_connector::install_rustls_crypto_provider();
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .assured("the issued chain matches its key");
        config.alpn_protocols = vec![HTTP2_PROTOCOL.to_vec(), HTTP1_PROTOCOL.to_vec()];
        Self {
            _files: files,
            ca,
            server: StdArc::new(config),
        }
    }
}

/// Serves one HTTPS connection with hyper's HTTP/2 server and returns the version and target of
/// the one request it answered.
async fn serve_http2(
    listener: nervix_primitives::net::TcpListener,
    tls: StdArc<rustls::ServerConfig>,
) -> (http::Version, String) {
    let (stream, _) = listener
        .accept()
        .await
        .assured("the emitter connects to the endpoint");
    let stream = tokio_rustls::TlsAcceptor::from(tls)
        .accept(stream)
        .await
        .assured("the emitter completes TLS");
    let (seen_sender, seen) = nervix_primitives::sync::oneshot::channel();
    let seen_sender = nervix_primitives::sync::blocking::Mutex::new(Some(seen_sender));
    let service = hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
        let sent = seen_sender.lock().take();
        if let Some(sent) = sent {
            sent.send((request.version(), request.uri().to_string()))
                .discarded("the test reads what the server saw after the request completes");
        }
        async {
            let mut answer = Response::new(Full::new(Bytes::new()));
            *answer.status_mut() = http::StatusCode::NO_CONTENT;
            Ok::<_, std::convert::Infallible>(answer)
        }
    });
    let serving = hyper::server::conn::http2::Builder::new(ConnectionTasks::default())
        .serve_connection(TokioIo::new(stream), service);
    serving
        .await
        .discarded("the emitter drops its connection once the head arrives");
    seen.await.assured("the server saw the request")
}

#[nervix_primitives::test]
async fn an_endpoint_whose_tls_offers_http2_receives_the_request_over_http2() {
    let tls = TestTls::issue();
    let (listener, address) = loopback_listener().await;
    let served = nervix_primitives::task::spawn(serve_http2(listener, tls.server.clone()));
    let mut connector = emitter_with(Redirects::Refused, None, Some(&tls.ca)).await;

    let outcome = connector
        .publish(vec![post(
            &format!("https://{address}"),
            "/events?tenant=north",
            b"{}",
        )])
        .await
        .into_parts();

    assert_eq!(
        outcome.delivered,
        vec![EmitterRecordId::new(4)],
        "{:?}",
        outcome.infrastructure_error
    );
    let (version, target) = served.await.assured("the endpoint served the request");
    assert_eq!(version, http::Version::HTTP_2);
    assert_eq!(target, format!("https://{address}/events?tenant=north"));
}

/// The bytes every HTTP/2 connection from a client opens with, RFC 9113 section 3.4.
const HTTP2_CLIENT_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
/// The type of an HTTP/2 DATA frame, RFC 9113 section 6.1.
const HTTP2_DATA_FRAME: u8 = 0x0;

/// Reads what the client writes on `connection`, as an endpoint that then stops reading, up to the
/// head of its first DATA frame: the preface and every frame before it, whose payloads it discards.
/// The rest of that frame does not fit what `connection` holds, so the client's writes stay blocked
/// for as long as nothing reads them.
async fn read_until_the_first_body_frame(connection: &mut tokio::io::DuplexStream) {
    let mut preface = [0_u8; 24];
    connection
        .read_exact(&mut preface)
        .await
        .assured("the client writes the HTTP/2 preface");
    assert_eq!(&preface, HTTP2_CLIENT_PREFACE);
    loop {
        nervix_primitives::task::consume_budget().await;
        let mut head = [0_u8; 9];
        connection
            .read_exact(&mut head)
            .await
            .assured("the client writes whole frame heads");
        if head[3] == HTTP2_DATA_FRAME {
            return;
        }
        let length = u32::from_be_bytes([0, head[0], head[1], head[2]]);
        let length = usize::try_from(length).assured("a 24-bit frame length fits in usize");
        let mut payload = vec![0_u8; length];
        connection
            .read_exact(&mut payload)
            .await
            .assured("the client writes whole frames");
    }
}

#[nervix_primitives::test]
async fn an_http2_exchange_ends_its_connection_even_while_its_writes_are_blocked() {
    // Smaller than one DATA frame, so a client in the middle of one can write nothing more until
    // the endpoint reads.
    let (client, mut endpoint) = tokio::io::duplex(1024);
    let url = Url::parse("https://endpoint.example/events").assured("the test URL is valid");
    let request = post("https://endpoint.example", "/events", &vec![b'x'; 1 << 20]);
    let prepared =
        HttpRequestEmitter::http_request(&request, &url, HttpVersion::Http2, RequestForm::Direct)
            .assured("the test request is valid");
    let established = Established {
        stream: Box::new(client),
        version: HttpVersion::Http2,
    };

    // The exchange ends unanswered here, as an attempt's timeout ends one: its future is dropped.
    nervix_primitives::select! {
        _ = HttpRequestEmitter::exchange(established, prepared) => {
            panic!("an endpoint that never answers settles no exchange")
        }
        () = read_until_the_first_body_frame(&mut endpoint) => {}
    }

    // Writing nothing tells whether the client's end of the connection still exists.
    let ended = nervix_primitives::time::timeout(Duration::from_secs(30), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            match endpoint.write(&[]).await {
                Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => return,
                Err(error) => panic!("probing the connection failed: {error}"),
                Ok(_) => nervix_primitives::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await;
    assert!(
        ended.is_ok(),
        "the HTTP/2 connection outlived its exchange while its writes were blocked"
    );
}

#[nervix_primitives::test]
async fn a_refused_connection_keeps_its_cause_and_the_emitter_keeps_its_client() {
    let (unused, address) = loopback_listener().await;
    drop(unused);
    let origin = HttpOrigin::parse(&format!("http://{address}"))
        .assured("a loopback address and port form an HTTP origin");
    let mut headers = HttpApplicationHeaders::default();
    headers
        .insert(
            HttpHeaderName::parse("Authorization")
                .assured("Authorization is a valid application header name"),
            HttpHeaderValue::parse("Bearer s3cr3t")
                .assured("a bearer token is a valid header value"),
        )
        .assured("one short header fits the application envelope");
    let request = EmitterHttpRequest {
        id: EmitterRecordId::new(0),
        method: HttpMethod::parse("POST", HttpBodyMode::WithoutBody)
            .assured("POST is a valid HTTP token"),
        target: origin
            .target("/events/secret-7")
            .assured("the test target is origin-relative"),
        headers,
        body: None,
        occurred_at: Timestamp::from_unix_nanos(1),
    };
    let mut connector = emitter_with(Redirects::Refused, None, None).await;
    assert!(
        connector.keeps_client_on_publish_failure(),
        "a retry sends the pending request through the same emitter"
    );

    let outcome = connector.publish(vec![request]).await.into_parts();

    assert!(outcome.delivered.is_empty());
    assert!(outcome.rejected.is_empty());
    let failure = outcome
        .infrastructure_error
        .assured("a refused connection leaves its request unresolved");
    let described = description(&failure).assured("a failed attempt is always described");
    assert!(
        described.starts_with("HTTP connection failed: ") && !described.contains("secret"),
        "the refusal's own cause follows, and neither the target nor a header value: {described}"
    );
}

/// A bodyless `POST` to `origin`, the request every budget test below sends.
fn bodyless_post(origin: &str) -> EmitterHttpRequest {
    let origin = HttpOrigin::parse(origin).assured("the test origin is an HTTP origin");
    EmitterHttpRequest {
        id: EmitterRecordId::new(0),
        method: HttpMethod::parse("POST", HttpBodyMode::WithoutBody)
            .assured("POST is a valid HTTP token"),
        target: origin
            .target("/events")
            .assured("the test target is origin-relative"),
        headers: HttpApplicationHeaders::default(),
        body: None,
        occurred_at: Timestamp::from_unix_nanos(1),
    }
}

/// A emitter whose requests resolve through a resolver that reads `hosts` and asks only `server`.
///
/// Each try of the resolver outlasts every budget below, so a lookup against a server that never
/// answers ends exactly when the emitter's budget for it does. The returned directory holds the
/// resolver's files for as long as the emitter lives.
async fn emitter_resolving_through(
    server: SocketAddr,
    hosts: &str,
    timeout: Duration,
) -> (HttpRequestEmitter, tempfile::TempDir) {
    let files = tempfile::tempdir().assured("the test host has a temporary directory");
    let resolver_configuration = files.path().join("resolv.conf");
    let hosts_file = files.path().join("hosts");
    std::fs::write(
        &resolver_configuration,
        "search --\noptions timeout:30 attempts:1\n",
    )
    .assured("the temporary directory is writable");
    std::fs::write(&hosts_file, hosts).assured("the temporary directory is writable");
    let dns = DnsResolver::load(nervix_dns::DnsConfiguration {
        resolver_configuration,
        hosts_file,
        name_servers: nervix_dns::NameServers::Explicit(vec![server]),
    })
    .await
    .assured("the test's resolver configuration is valid");
    let timeout_ms = timeout.as_millis().to_string();
    let connector = HttpRequestEmitter::open(HttpRequestEmitterConfig {
        config: vec![ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: timeout_ms,
        }],
        dns,
    })
    .assured("a client with a timeout is valid");
    (connector, files)
}

/// A name server that never answers holds the lookup for the lookup's share of `timeout_ms`,
/// and no longer: the attempt fails as a lookup, and the rest of the timeout, which the address
/// attempts and the request would have used, is never spent on it.
#[nervix_primitives::test(start_paused = true)]
async fn a_silent_name_server_holds_the_lookup_for_its_share_of_the_timeout() {
    let silent = nervix_primitives::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the test host has a free loopback UDP port");
    let server = silent
        .local_addr()
        .assured("a bound socket has a local address");
    let timeout = Duration::from_secs(8);
    let (mut connector, _files) = emitter_resolving_through(server, "", timeout).await;
    let share = timeout
        .checked_div(2)
        .assured("dividing by a nonzero constant cannot fail");

    let started = Instant::now();
    let outcome = connector
        .publish(vec![bodyless_post("http://silent.nervix.test:8080")])
        .await
        .into_parts();
    let elapsed = started.elapsed();

    assert!(
        elapsed >= share && elapsed < timeout,
        "the lookup ended {elapsed:?} into a {timeout:?} timeout, not at its {share:?} share"
    );
    let failure = outcome
        .infrastructure_error
        .assured("a name that never resolves leaves its request unresolved");
    assert_eq!(
        description(&failure).as_deref(),
        Some(
            "HTTP DNS resolution failed: resolving 'silent.nervix.test' failed: no answer arrived \
             in time"
        )
    );
}

/// Answers the request of the first connection `listener` accepts with `204 No Content`.
async fn answer_one_request(listener: nervix_primitives::net::TcpListener) {
    let (mut stream, _) = listener
        .accept()
        .await
        .assured("the emitter connects to the answering address");
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        nervix_primitives::task::consume_budget().await;
        stream
            .read_exact(&mut byte)
            .await
            .assured("the emitter writes its whole request head");
        head.push(byte[0]);
    }
    stream
        .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
        .await
        .assured("the emitter reads the final head it waits for");
}

/// A listener at `address` whose accept queue one connection holds full, so the kernel drops
/// every later handshake. The returned listener and connection keep the queue full while held.
async fn held_address(
    address: SocketAddr,
) -> (
    nervix_primitives::net::TcpListener,
    nervix_primitives::net::TcpStream,
) {
    let socket =
        nervix_primitives::net::TcpSocket::new_v4().assured("the test host has TCP sockets");
    socket
        .bind(address)
        .assured("every 127.0.0.0/8 address is local, so another one can take the same port");
    let listener = socket.listen(0).assured("a bound socket can listen");
    let queued = TcpStream::connect(address)
        .await
        .assured("an empty accept queue admits one connection");
    (listener, queued)
}

/// When the only answer never completes its handshake, its attempt ends with its share, the
/// whole of what the lookup left, and the attempt's description names that share rather than a
/// request timeout, because no request was ever sent.
#[nervix_primitives::test(start_paused = true)]
async fn an_answer_that_never_connects_ends_its_attempt_with_its_share() {
    let held = SocketAddr::from((std::net::Ipv4Addr::new(127, 0, 7, 2), 8080));
    let (_listener, _queued) = held_address(held).await;
    let silent = nervix_primitives::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the test host has a free loopback UDP port");
    let timeout = Duration::from_secs(6);
    let (mut connector, _files) = emitter_resolving_through(
        silent
            .local_addr()
            .assured("a bound socket has a local address"),
        &format!("{} one.nervix.test\n", held.ip()),
        timeout,
    )
    .await;

    let started = Instant::now();
    let outcome = connector
        .publish(vec![bodyless_post("http://one.nervix.test:8080")])
        .await
        .into_parts();
    let elapsed = started.elapsed();

    assert!(
        elapsed >= timeout,
        "the only answer's share is the whole timeout, but the attempt ended after {elapsed:?}"
    );
    let failure = outcome
        .infrastructure_error
        .assured("an answer that never connects leaves its request unresolved");
    assert_eq!(
        description(&failure).as_deref(),
        Some(
            "HTTP connection failed: no connection was accepted within the address's share of \
             timeout_ms"
        )
    );
}

/// An answer whose handshake never completes holds its attempt for its share of what the lookup
/// left, and no longer, so the next answer still connects and the request is delivered within
/// the same `timeout_ms`.
#[nervix_primitives::test]
async fn an_answer_that_never_connects_leaves_the_next_answer_its_share() {
    let serving = nervix_primitives::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the test host has a free loopback port");
    let port = serving
        .local_addr()
        .assured("a bound listener has a local address")
        .port();
    let held = SocketAddr::from((std::net::Ipv4Addr::new(127, 0, 7, 1), port));
    let (_held_listener, _queued) = held_address(held).await;
    let answering = nervix_primitives::task::spawn(answer_one_request(serving));
    let silent = nervix_primitives::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the test host has a free loopback UDP port");
    let hosts = format!("{} two.nervix.test\n127.0.0.1 two.nervix.test\n", held.ip());
    let (mut connector, _files) = emitter_resolving_through(
        silent
            .local_addr()
            .assured("a bound socket has a local address"),
        &hosts,
        Duration::from_secs(4),
    )
    .await;

    let outcome = connector
        .publish(vec![bodyless_post(&format!(
            "http://two.nervix.test:{port}"
        ))])
        .await
        .into_parts();

    assert!(
        outcome.infrastructure_error.is_none(),
        "the held answer consumed the whole timeout: {:?}",
        outcome.infrastructure_error
    );
    assert_eq!(outcome.delivered, vec![EmitterRecordId::new(0)]);
    answering
        .await
        .assured("the answering task completes once it has answered");
}
