//! Outbound HTTP request emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** One bounded physical HTTP/1.1 exchange at a time, exact prepared request bytes and
//!   the length they declare, TLS and DNS transport, how one attempt's timeout is divided between
//!   its lookup, its address attempts and its request, response header validation, one outcome
//!   for each request, the retry delay a retryable response asks for, and the description of a
//!   failed attempt, which names its cause and never an evaluated target, header value or body.
//! - **Depends on.** The connector contract and its actual-UTC read, shared client TLS settings,
//!   node DNS resolver and its operation budget, vocabulary request fields, Tokio I/O, rustls and
//!   the HTTP/1.1 response parser.
//! - **Must not know.** How request fields or bodies were prepared, runtime batches, relays,
//!   branches, acknowledgements, registry state or when the host retries.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

#[cfg(test)]
mod request_head_tests;
mod response;

use std::time::Duration;

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
#[cfg(test)]
use nervix_connector::HttpRequestEmitter as _;
use nervix_connector::{
    ClientConfigError, ConfiguredDeadline, EmitterFinish, EmitterFinishResult, EmitterHost,
    EmitterHttpRequest, EmitterLifecycle, EmitterOpenError, EmitterRecordId, EmitterRetryDelay,
    EmitterStartResult, EmitterWriteError, PerRecordOutcome, RustlsClientConfigProvider,
    physical_time::actual_utc_now,
};
use nervix_dns::{AddressAttempt, DnsResolver, LookupShare, OperationBudget};
use nervix_models::{ClientConfigEntry, CreateClientHttp};
use nervix_primitives::{net::TcpStream, sync::StdArc, time::Instant};
use rustls::ClientConfig as RustlsClientConfig;
use rustls_pki_types::ServerName;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio_rustls::TlsConnector;
use url::{Host, Position, Url};

use self::response::{Disposition, FinalResponse, read_final_headers};

const HTTP_EMITTER: &str = "HTTP";

/// How many times its name lookup's share goes into a request's `timeout_ms`. A policy input:
/// whatever the name servers do, the address attempts, TLS and the request keep the rest of every
/// attempt's timeout.
const LOOKUP_SHARE_DIVISOR: u32 = 2;

const _: () = assert!(
    LOOKUP_SHARE_DIVISOR > 1,
    "the lookup must leave part of the timeout to the connection and the request"
);

/// The emitter's resolved client settings and its node-owned resolver.
pub struct HttpRequestEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
}

/// Each publish call sends its prepared requests sequentially. A fresh HTTP/1.1 connection serves
/// one request; dropping it after final headers means no incomplete response body can be reused.
pub struct HttpRequestEmitter {
    dns: DnsResolver,
    tls: StdArc<RustlsClientConfig>,
    timeout: Duration,
}

trait HttpStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> HttpStream for T {}

#[derive(Debug, Error)]
enum HttpAttemptError {
    #[error("HTTP request timed out before complete final response headers")]
    Timeout,
    #[error("HTTP DNS resolution failed")]
    Dns,
    #[error("HTTP connection failed")]
    Connection,
    #[error("HTTP TLS handshake failed")]
    Tls,
    #[error("HTTP request send failed")]
    Send,
    #[error("HTTP response header exchange failed")]
    Response,
    #[error("HTTP request has an invalid destination")]
    Destination,
    #[error("HTTP endpoint answered with authentication or authorization status {status}")]
    Authentication { status: u16 },
    #[error("HTTP endpoint answered with retryable status {status}")]
    RetryableStatus { status: u16 },
}

type HttpAttemptResult<T> = Result<T, Report<HttpAttemptError>>;

/// A resolved address that accepted no connection before its share of the attempt's timeout ended.
#[derive(Debug, Error)]
#[error("no connection was accepted within the address's share of timeout_ms")]
struct AddressShareElapsed;

impl HttpRequestEmitter {
    pub fn new(config: HttpRequestEmitterConfig, _host: EmitterHost) -> EmitterStartResult<Self> {
        let configured = ConfiguredDeadline::timeout(&config.config).change_context(
            EmitterOpenError::InvalidConfiguration {
                connector: HTTP_EMITTER,
            },
        )?;
        let Some(timeout) = configured else {
            return Err(Report::new(ClientConfigError::MissingRequired {
                connector: HTTP_EMITTER,
                key: CreateClientHttp::TRANSPORT.timeout.key().to_string(),
            })
            .change_context(EmitterOpenError::InvalidConfiguration {
                connector: HTTP_EMITTER,
            }));
        };
        let timeout = timeout.bound();
        let tls = RustlsClientConfigProvider::new(&config.config)
            .build_with_default_roots()
            .change_context(EmitterOpenError::InvalidConfiguration {
                connector: HTTP_EMITTER,
            })?;
        // rustls requires a standard Arc. The client speaks HTTP/1.1 so every interim block can
        // be inspected, including those that higher-level clients discard before the final reply.
        let mut tls = (*tls).clone();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            dns: config.dns,
            tls: StdArc::new(tls),
            timeout,
        })
    }

    /// The most of a request's `timeout_ms` its name lookup may take.
    fn lookup_share(&self) -> LookupShare {
        let share = self
            .timeout
            .checked_div(LOOKUP_SHARE_DIVISOR)
            .assured("LOOKUP_SHARE_DIVISOR is a nonzero constant");
        LookupShare::AtMost(share)
    }

    /// One attempt, whose DNS, connection, TLS, request send and complete final headers share one
    /// physical timeout. One budget divides it: the lookup takes at most its share, each resolved
    /// address an equal share of what is left when its attempt starts, and TLS, the send and the
    /// final headers whatever remains once an address accepts.
    ///
    /// Each failure keeps its cause beneath the attempt's own: the resolver's lookup failure, the
    /// socket or TLS error, or the share an address ran out of. They describe the connection, never
    /// the request.
    async fn send(&self, request: &EmitterHttpRequest) -> HttpAttemptResult<FinalResponse> {
        let budget = OperationBudget::start(self.timeout, self.lookup_share());
        let url = request.target.url();
        let connection = self.connect(url, &budget).await?;
        let exchanged = nervix_primitives::time::timeout(
            budget.remaining(),
            self.exchange(connection, url, request),
        )
        .await;
        match exchanged {
            Ok(result) => result,
            Err(_) => Err(Report::new(HttpAttemptError::Timeout)),
        }
    }

    /// TLS for an `https` target, then the request and its complete final headers.
    async fn exchange(
        &self,
        connection: TcpStream,
        url: &Url,
        request: &EmitterHttpRequest,
    ) -> HttpAttemptResult<FinalResponse> {
        let mut stream = self.secure(connection, url).await?;
        let head = Self::request_head(request);
        stream
            .write_all(&head)
            .await
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Send))?;
        if let Some(body) = &request.body {
            stream
                .write_all(body)
                .await
                .map_err(|error| Report::new(error).change_context(HttpAttemptError::Send))?;
        }
        stream
            .flush()
            .await
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Send))?;
        read_final_headers(&mut *stream)
            .await
            .change_context(HttpAttemptError::Response)
    }

    /// The connection of the first resolved address that accepts one within its share of `budget`,
    /// the addresses tried in resolution order.
    async fn connect(&self, url: &Url, budget: &OperationBudget) -> HttpAttemptResult<TcpStream> {
        let host = url
            .host_str()
            .ok_or_else(|| Report::new(HttpAttemptError::Destination))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| Report::new(HttpAttemptError::Destination))?;
        let addresses = self
            .dns
            .resolve(host, port, budget.lookup())
            .await
            .change_context(HttpAttemptError::Dns)?;
        let mut last_failure = None;
        for attempt in budget.attempts(&addresses) {
            nervix_primitives::task::consume_budget().await;
            match Self::connect_address(attempt).await {
                Ok(connection) => return Ok(connection),
                Err(failure) => last_failure = Some(failure),
            }
        }
        // No address accepted a connection; the failure of the last one tried describes it.
        match last_failure {
            Some(failure) => Err(failure),
            None => Err(Report::new(HttpAttemptError::Connection)),
        }
    }

    /// Connects to one resolved address within its share of the attempt's timeout.
    async fn connect_address(attempt: AddressAttempt) -> HttpAttemptResult<TcpStream> {
        let connected =
            nervix_primitives::time::timeout(attempt.budget, TcpStream::connect(attempt.address))
                .await;
        match connected {
            Ok(Ok(connection)) => Ok(connection),
            Ok(Err(error)) => Err(Report::new(error).change_context(HttpAttemptError::Connection)),
            Err(_) => {
                Err(Report::new(AddressShareElapsed).change_context(HttpAttemptError::Connection))
            }
        }
    }

    /// The stream an attempt writes to: `connection` itself, or TLS over it for an `https` target.
    async fn secure(
        &self,
        connection: TcpStream,
        url: &Url,
    ) -> HttpAttemptResult<Box<dyn HttpStream>> {
        if url.scheme() != "https" {
            return Ok(Box::new(connection));
        }
        let server_name = Self::server_name(url)?;
        let connector = TlsConnector::from(self.tls.clone());
        let tls = connector
            .connect(server_name, connection)
            .await
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Tls))?;
        Ok(Box::new(tls))
    }

    fn server_name(url: &Url) -> HttpAttemptResult<ServerName<'static>> {
        match url.host() {
            Some(Host::Domain(domain)) => ServerName::try_from(domain.to_owned())
                .map_err(|_| Report::new(HttpAttemptError::Destination)),
            Some(Host::Ipv4(address)) => Ok(ServerName::from(address)),
            Some(Host::Ipv6(address)) => Ok(ServerName::from(address)),
            None => Err(Report::new(HttpAttemptError::Destination)),
        }
    }

    /// The transport generates Host, Connection and Content-Length: a body's length, or zero for a
    /// bodyless request whose method defines request content, as RFC 9110 section 8.6 asks of a
    /// client, so a server that requires a declared length accepts it. Any other bodyless request
    /// declares no length. It defaults Accept to */* only when no application Accept header was
    /// prepared. It generates neither Accept-Encoding nor Content-Type, cookies or credentials.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "compares prepared header names through the external \
                                   iterator; its local predicate is analyzed; no internal \
                                   runtime ownership is inferred")
    )]
    fn request_head(request: &EmitterHttpRequest) -> Vec<u8> {
        let url = request.target.url();
        let authority = &url[Position::BeforeHost..Position::AfterPort];
        let mut head = format!(
            "{} {} HTTP/1.1\r\nhost: {authority}\r\nconnection: close\r\n",
            request.method.as_str(),
            request.target.as_str(),
        )
        .into_bytes();
        if !request
            .headers
            .iter()
            .any(|(name, _)| name.as_str().eq_ignore_ascii_case("accept"))
        {
            head.extend_from_slice(b"accept: */*\r\n");
        }
        for (name, value) in request.headers.iter() {
            head.extend_from_slice(name.as_str().as_bytes());
            head.extend_from_slice(b": ");
            head.extend_from_slice(value.as_str().as_bytes());
            head.extend_from_slice(b"\r\n");
        }
        match &request.body {
            Some(body) => {
                head.extend_from_slice(format!("content-length: {}\r\n", body.len()).as_bytes());
            }
            None if request.method.defines_content() => {
                head.extend_from_slice(b"content-length: 0\r\n");
            }
            None => {}
        }
        head.extend_from_slice(b"\r\n");
        head
    }

    /// The failure of an attempt, described by the attempt's own error and every cause beneath
    /// it, such as `HTTP endpoint answered with retryable status 503` or
    /// `HTTP TLS handshake failed: invalid peer certificate: UnknownIssuer`.
    ///
    /// The emitter reports the description as its transient error while the request stays
    /// pending. Neither the attempt's errors nor their causes carry the evaluated target, a header
    /// value or a body, so the description names none of them.
    fn publish_error(error: Report<HttpAttemptError>) -> Report<EmitterWriteError> {
        let description = format!("{error:#}");
        error
            .change_context(EmitterWriteError::Publish {
                connector: HTTP_EMITTER,
            })
            .attach_printable(description)
    }

    /// The failure of an attempt whose endpoint answered with a status to retry, carrying the
    /// delay the response's one valid `Retry-After` asks for. Its date is compared with actual UTC
    /// now, as the response has just arrived, and the host waits for this delay when it is longer
    /// than its own backoff.
    fn retryable_status_error(
        error: HttpAttemptError,
        response: FinalResponse,
    ) -> Report<EmitterWriteError> {
        let failure = Self::publish_error(Report::new(error));
        match response.retry_delay(actual_utc_now()) {
            Some(delay) => failure.attach(EmitterRetryDelay(delay)),
            None => failure,
        }
    }
}

impl HttpRequestEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Http(nervix_models::HttpEmitterEntity::Request);
}

#[async_trait]
impl EmitterLifecycle for HttpRequestEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// Each request's response headers arrive before the publish returns, so the client holds
    /// nothing.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }

    /// The emitter holds no connection or staged work between attempts, so keeping it loses
    /// nothing, and the emitter's retry sends the pending request without reopening an emitter whose
    /// successful reopening would clear the failure the request is still waiting out.
    fn keeps_client_on_publish_failure(&self) -> bool {
        true
    }
}

#[async_trait]
impl nervix_connector::HttpRequestEmitter for HttpRequestEmitter {
    async fn publish(
        &mut self,
        requests: Vec<EmitterHttpRequest>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(requests.len());
        for request in requests {
            nervix_primitives::task::consume_budget().await;
            let response = match self.send(&request).await {
                Ok(response) => response,
                Err(error) => {
                    outcome.fail(Self::publish_error(error));
                    return outcome;
                }
            };
            let status = response.status();
            // A delivered or rejected record is final, so its `Retry-After` is never read.
            match status.disposition() {
                Disposition::Delivered => outcome.deliver(request.id),
                Disposition::Rejected => outcome.reject(request.rejected(format!(
                    "HTTP endpoint answered with status {}",
                    status.code()
                ))),
                Disposition::AuthenticationFailure => {
                    outcome.fail(Self::retryable_status_error(
                        HttpAttemptError::Authentication {
                            status: status.code(),
                        },
                        response,
                    ));
                    return outcome;
                }
                Disposition::RetryableFailure => {
                    outcome.fail(Self::retryable_status_error(
                        HttpAttemptError::RetryableStatus {
                            status: status.code(),
                        },
                        response,
                    ));
                    return outcome;
                }
            }
        }
        outcome
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::net::SocketAddr;

    use error_stack::{AttachmentKind, FrameKind};
    use meticulous::ResultExt as _;
    use nervix_models::{
        HttpApplicationHeaders, HttpBodyMode, HttpHeaderName, HttpHeaderValue, HttpMethod,
        HttpOrigin, Timestamp,
    };

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
                HttpHeaderValue::parse("north")
                    .assured("north is a valid application header value"),
            )
            .assured("one short header fits the application envelope");
        EmitterHttpRequest {
            id: EmitterRecordId::new(3),
            method: HttpMethod::parse(method, HttpBodyMode::WithoutBody)
                .assured("the test method is a valid HTTP token"),
            target,
            headers,
            body,
            occurred_at: Timestamp::from_unix_nanos(1),
        }
    }

    #[test]
    fn encoded_request_uses_exact_fields_and_explicit_transport_headers() {
        let request = prepared("PATCH", Some(br#"{"id":1}"#.to_vec()));
        let head = String::from_utf8(HttpRequestEmitter::request_head(&request))
            .assured("the prepared test fields are all UTF-8");
        assert_eq!(
            head,
            "PATCH /v1/events?q=a+b HTTP/1.1\r\nhost: api.example.com:8443\r\nconnection: \
             close\r\naccept: */*\r\nx-tenant: north\r\ncontent-length: 8\r\n\r\n"
        );
    }

    #[test]
    fn absent_body_adds_no_framing_or_content_type() {
        let request = prepared("purge", None);
        let head = String::from_utf8(HttpRequestEmitter::request_head(&request))
            .assured("the prepared test fields are all UTF-8");
        assert!(head.starts_with("purge /v1/events?q=a+b HTTP/1.1\r\n"));
        assert!(!head.contains("content-length"));
        assert!(!head.contains("content-type"));
        assert!(!head.contains("accept-encoding"));
        assert_eq!(
            request.body, None,
            "the emitter has not constructed an empty body"
        );
    }

    #[test]
    fn a_bodyless_request_declares_an_empty_length_only_when_its_method_defines_content() {
        for method in ["POST", "PUT", "PATCH", "post", "Patch"] {
            let head = String::from_utf8(HttpRequestEmitter::request_head(&prepared(method, None)))
                .assured("the prepared test fields are all UTF-8");
            assert!(
                head.ends_with("\r\ncontent-length: 0\r\n\r\n"),
                "{method} defines its request content, so its absence is declared: {head}"
            );
        }
        for method in ["DELETE", "OPTIONS", "GET", "purge"] {
            let head = String::from_utf8(HttpRequestEmitter::request_head(&prepared(method, None)))
                .assured("the prepared test fields are all UTF-8");
            assert!(
                !head.contains("content-length"),
                "{method} defines no request content, so no length is declared: {head}"
            );
        }
    }

    #[test]
    fn prepared_accept_replaces_the_default() {
        let mut request = prepared("GET", None);
        request
            .headers
            .insert(
                HttpHeaderName::parse("Accept")
                    .assured("Accept is a valid application header name"),
                HttpHeaderValue::parse("application/json")
                    .assured("application/json is a valid header value"),
            )
            .assured("two short headers fit the application envelope");
        let head = String::from_utf8(HttpRequestEmitter::request_head(&request))
            .assured("the prepared test fields are all UTF-8");
        assert!(head.contains("accept: application/json\r\n"));
        assert!(!head.contains("accept: */*"));
    }

    async fn final_response(head: &'static [u8]) -> FinalResponse {
        let (mut client, mut server) = tokio::io::duplex(1024);
        nervix_primitives::task::spawn(async move {
            use tokio::io::AsyncWriteExt as _;
            server
                .write_all(head)
                .await
                .assured("the in-memory server writes its whole response head");
        });
        read_final_headers(&mut client)
            .await
            .assured("the fixture response head is complete and valid")
    }

    #[nervix_primitives::test]
    async fn a_retryable_status_carries_only_the_delay_its_retry_after_asks_for() {
        let limited = final_response(
            b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 30\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        let error = HttpRequestEmitter::retryable_status_error(
            HttpAttemptError::RetryableStatus { status: 429 },
            limited,
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

        let unstated =
            final_response(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n").await;
        let error = HttpRequestEmitter::retryable_status_error(
            HttpAttemptError::Authentication { status: 401 },
            unstated,
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

        let headers = HttpRequestEmitter::publish_error(
            Report::new(ResponseHeadError::Excessive).change_context(HttpAttemptError::Response),
        );
        assert_eq!(
            description(&headers).as_deref(),
            Some(
                "HTTP response header exchange failed: HTTP response headers exceed 128 fields or \
                 64 KiB"
            )
        );
    }

    #[nervix_primitives::test]
    async fn a_refused_connection_keeps_its_cause_and_the_emitter_keeps_its_client() {
        let unused = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
            .await
            .assured("the test host has a free loopback port");
        let port = unused
            .local_addr()
            .assured("a bound listener has a local address")
            .port();
        drop(unused);
        let origin = HttpOrigin::parse(&format!("http://127.0.0.1:{port}"))
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
        let mut connector = HttpRequestEmitter {
            dns: DnsResolver::load(nervix_dns::DnsConfiguration::system())
                .await
                .assured("the host resolver configuration loads in tests"),
            tls: RustlsClientConfigProvider::new(&[])
                .build_with_default_roots()
                .assured("the platform's default roots load in tests"),
            timeout: Duration::from_secs(5),
        };
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
            "the refusal's own cause follows, and neither the target nor a header value: \
             {described}"
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
        let connector = HttpRequestEmitter {
            dns,
            tls: RustlsClientConfigProvider::new(&[])
                .build_with_default_roots()
                .assured("the platform's default roots load in tests"),
            timeout,
        };
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
                "HTTP DNS resolution failed: resolving 'silent.nervix.test' failed: no answer \
                 arrived in time"
            )
        );
    }

    /// Answers the request of the first connection `listener` accepts with `204 No Content`.
    async fn answer_one_request(listener: nervix_primitives::net::TcpListener) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
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
}
