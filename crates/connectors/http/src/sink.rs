//! Outbound HTTP request sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** One bounded physical HTTP/1.1 exchange at a time, exact prepared request bytes,
//!   TLS and DNS transport, response header validation, one outcome for each request, and the
//!   retry delay a retryable response asks for.
//! - **Depends on.** The connector contract and its actual-UTC read, shared client TLS settings,
//!   node DNS resolver, vocabulary request fields, Tokio I/O, rustls and the HTTP/1.1 response
//!   parser.
//! - **Must not know.** How request fields or bodies were prepared, runtime batches, relays,
//!   branches, acknowledgements, registry state or when the host retries.

mod response;

use std::{net::SocketAddr, sync::Arc as StdArc, time::Duration};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    HttpRequestSink, PerRecordOutcome, RustlsClientConfigSource, SinkHost, SinkHttpRequest,
    SinkLifecycle, SinkPublishError, SinkRecordId, SinkRetryDelay, SinkStartError, SinkStartResult,
    client_config_value, physical_time::actual_utc_now,
};
use nervix_dns::DnsResolver;
use nervix_models::ClientConfigEntry;
use rustls::ClientConfig as RustlsClientConfig;
use rustls_pki_types::ServerName;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt as _},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;
use url::{Host, Position, Url};

use self::response::{Disposition, FinalResponse, read_final_headers};

const HTTP_SINK: &str = "HTTP";

/// The emitter's resolved client settings and its node-owned resolver.
pub struct HttpSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
}

/// Each publish call sends its prepared requests sequentially. A fresh HTTP/1.1 connection serves
/// one request; dropping it after final headers means no incomplete response body can be reused.
pub struct HttpSink {
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

impl HttpSink {
    pub fn new(config: HttpSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let timeout_text = client_config_value(&config.config, "timeout_ms", HTTP_SINK)
            .change_context(SinkStartError::InvalidConfiguration { sink: HTTP_SINK })?;
        let timeout_ms = timeout_text.parse::<u64>().map_err(|_| {
            Report::new(SinkStartError::InvalidConfiguration { sink: HTTP_SINK })
                .attach_printable("HTTP timeout_ms must be a positive integer")
        })?;
        let timeout = Duration::from_millis(timeout_ms);
        if timeout_ms == 0 || std::time::Instant::now().checked_add(timeout).is_none() {
            return Err(
                Report::new(SinkStartError::InvalidConfiguration { sink: HTTP_SINK })
                    .attach_printable("HTTP timeout_ms cannot be scheduled"),
            );
        }
        let tls = RustlsClientConfigSource::new(&config.config)
            .build_with_default_roots()
            .change_context(SinkStartError::InvalidConfiguration { sink: HTTP_SINK })?;
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

    async fn send(&self, request: &SinkHttpRequest) -> HttpAttemptResult<FinalResponse> {
        match tokio::time::timeout(self.timeout, self.exchange(request)).await {
            Ok(result) => result,
            Err(_) => Err(Report::new(HttpAttemptError::Timeout)),
        }
    }

    /// DNS, connection, TLS, request send and complete final headers share one physical timeout.
    async fn exchange(&self, request: &SinkHttpRequest) -> HttpAttemptResult<FinalResponse> {
        let url = request.target.url();
        let mut stream = self.connect(url).await?;
        let head = Self::request_head(request);
        stream
            .write_all(&head)
            .await
            .map_err(|_| Report::new(HttpAttemptError::Send))?;
        if let Some(body) = &request.body {
            stream
                .write_all(body)
                .await
                .map_err(|_| Report::new(HttpAttemptError::Send))?;
        }
        stream
            .flush()
            .await
            .map_err(|_| Report::new(HttpAttemptError::Send))?;
        read_final_headers(&mut *stream)
            .await
            .change_context(HttpAttemptError::Response)
    }

    async fn connect(&self, url: &Url) -> HttpAttemptResult<Box<dyn HttpStream>> {
        let host = url
            .host_str()
            .ok_or_else(|| Report::new(HttpAttemptError::Destination))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| Report::new(HttpAttemptError::Destination))?;
        let addresses = self
            .dns
            .resolve(host, port, self.timeout)
            .await
            .map_err(|_| Report::new(HttpAttemptError::Dns))?;
        let mut connected = None;
        for address in addresses {
            nervix_primitives::task::consume_budget().await;
            if let Ok(stream) = Self::connect_address(address).await {
                connected = Some(stream);
                break;
            }
        }
        let stream = connected.ok_or_else(|| Report::new(HttpAttemptError::Connection))?;
        let stream: Box<dyn HttpStream> = if url.scheme() == "https" {
            let server_name = Self::server_name(url)?;
            let connector = TlsConnector::from(self.tls.clone());
            let tls = connector
                .connect(server_name, stream)
                .await
                .map_err(|_| Report::new(HttpAttemptError::Tls))?;
            Box::new(tls)
        } else {
            Box::new(stream)
        };
        Ok(stream)
    }

    async fn connect_address(address: SocketAddr) -> std::io::Result<TcpStream> {
        TcpStream::connect(address).await
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

    /// The transport generates Host, Connection and, with a body, Content-Length. It defaults
    /// Accept to */* only when no application Accept header was prepared. It generates neither
    /// Accept-Encoding nor Content-Type, cookies or credentials.
    fn request_head(request: &SinkHttpRequest) -> Vec<u8> {
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
        if let Some(body) = &request.body {
            head.extend_from_slice(format!("content-length: {}\r\n", body.len()).as_bytes());
        }
        head.extend_from_slice(b"\r\n");
        head
    }

    fn publish_error(error: Report<HttpAttemptError>) -> Report<SinkPublishError> {
        error.change_context(SinkPublishError::Publish { sink: HTTP_SINK })
    }

    /// The failure of an attempt whose endpoint answered with a status to retry, carrying the
    /// delay the response's one valid `Retry-After` asks for. Its date is compared with actual UTC
    /// now, as the response has just arrived, and the host waits for this delay when it is longer
    /// than its own backoff.
    fn retryable_status_error(
        error: HttpAttemptError,
        response: FinalResponse,
    ) -> Report<SinkPublishError> {
        let failure = Self::publish_error(Report::new(error));
        match response.retry_delay(actual_utc_now()) {
            Some(delay) => failure.attach(SinkRetryDelay(delay)),
            None => failure,
        }
    }
}

#[async_trait]
impl SinkLifecycle for HttpSink {}

#[async_trait]
impl HttpRequestSink for HttpSink {
    async fn publish(&mut self, requests: Vec<SinkHttpRequest>) -> PerRecordOutcome<SinkRecordId> {
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
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        HttpApplicationHeaders, HttpBodyMode, HttpHeaderName, HttpHeaderValue, HttpMethod,
        HttpOrigin, Timestamp,
    };

    use super::*;

    fn prepared(method: &str, body: Option<Vec<u8>>) -> SinkHttpRequest {
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
        SinkHttpRequest {
            id: SinkRecordId::new(3),
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
        let head = String::from_utf8(HttpSink::request_head(&request))
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
        let head = String::from_utf8(HttpSink::request_head(&request))
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
        let head = String::from_utf8(HttpSink::request_head(&request))
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
        let error = HttpSink::retryable_status_error(
            HttpAttemptError::RetryableStatus { status: 429 },
            limited,
        );
        assert_eq!(
            error.downcast_ref::<SinkRetryDelay>(),
            Some(&SinkRetryDelay(Duration::from_secs(30)))
        );
        assert_eq!(
            *error.current_context(),
            SinkPublishError::Publish { sink: HTTP_SINK }
        );

        let unstated =
            final_response(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n").await;
        let error = HttpSink::retryable_status_error(
            HttpAttemptError::Authentication { status: 401 },
            unstated,
        );
        assert!(error.downcast_ref::<SinkRetryDelay>().is_none());
    }

    #[test]
    fn tls_server_name_uses_the_origin_host() {
        let origin = HttpOrigin::parse("https://127.0.0.1:8443")
            .assured("the test origin has a valid IP address");
        let target = origin
            .target("/")
            .assured("the root path is a valid target");
        let name = HttpSink::server_name(target.url())
            .assured("a parsed HTTP target has a TLS server name");
        assert_eq!(name, ServerName::from(std::net::Ipv4Addr::LOCALHOST));
    }
}
