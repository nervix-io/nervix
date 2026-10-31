//! Outbound HTTP request emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** One bounded physical HTTP exchange at a time; the connection each request travels:
//!   DNS and address attempts within its budget, TLS with the HTTP versions it offers, and the
//!   declared proxy's tunnel; the exact prepared request it hands hyper, which speaks HTTP/1.1, or
//!   HTTP/2 where TLS negotiates it, over that connection; how one attempt's timeout is divided
//!   between its requests and, within each, between its lookup, its address attempts and its
//!   exchange; the redirects its client follows; one outcome for each request; the retry delay a
//!   retryable response asks for; and the description of a failed attempt, which names its cause
//!   and never an evaluated target, header value, body or proxy credential.
//! - **Depends on.** The connector contract and its actual-UTC read, shared client TLS and proxy
//!   settings, the client transport both entities share, node DNS resolver and its operation
//!   budget, vocabulary request fields, Tokio I/O, rustls, and hyper, which alone speaks HTTP.
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
mod request_tests;
mod response;

use std::{borrow::Cow, future::Future, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use error_stack::{Report, ResultExt as _};
use http::{
    HeaderName, HeaderValue, Method, Request, Response, Uri,
    header::{ACCEPT, CONNECTION, CONTENT_LENGTH, HOST, PROXY_AUTHORIZATION},
};
use http_body_util::Full;
use hyper::client::conn::{http1, http2};
use hyper_util::rt::TokioIo;
use meticulous::OptionExt as _;
#[cfg(test)]
use nervix_connector::HttpRequestEmitter as _;
use nervix_connector::{
    DeclaredProxy, EmitterFinish, EmitterFinishResult, EmitterHost, EmitterHttpRequest,
    EmitterLifecycle, EmitterOpenError, EmitterRecordId, EmitterRetryDelay, EmitterStartResult,
    EmitterWriteError, HttpClientConfig, PerRecordOutcome, RustlsClientConfigProvider,
    physical_time::actual_utc_now,
};
use nervix_dns::{AddressAttempt, DnsResolver, LookupShare, OperationBudget};
use nervix_models::ClientConfigEntry;
use nervix_primitives::{
    net::TcpStream,
    sync::{CancellationToken, StdArc},
    time::Instant,
};
use nervix_recovery::Discarded as _;
use rustls::ClientConfig as RustlsClientConfig;
use rustls_pki_types::ServerName;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsConnector;
use url::{Host, Position, Url};

use self::response::{Disposition, FinalResponse};
use super::transport::{ClientTransport, RedirectAnswer, Redirects};

const HTTP_EMITTER: &str = "HTTP";

/// How many times its name lookup's share goes into what a request has of its attempt's
/// `timeout_ms`. A policy input: whatever the name servers do, the address attempts, TLS and the
/// request keep the rest of it.
const LOOKUP_SHARE_DIVISOR: u32 = 2;

const _: () = assert!(
    LOOKUP_SHARE_DIVISOR > 1,
    "the lookup must leave part of the timeout to the connection and the request"
);

/// The ALPN identifier of HTTP/2, which an endpoint's TLS may choose.
const HTTP2_PROTOCOL: &[u8] = b"h2";
/// The ALPN identifier of HTTP/1.1.
const HTTP1_PROTOCOL: &[u8] = b"http/1.1";

/// The emitter's resolved client settings and its node-owned resolver.
pub struct HttpRequestEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
}

/// Each publish call sends its prepared requests sequentially. A fresh connection serves one
/// request; dropping it after final headers means no incomplete response body can be reused.
pub struct HttpRequestEmitter {
    dns: DnsResolver,
    /// TLS to an endpoint, offering HTTP/2 and HTTP/1.1.
    endpoint_tls: StdArc<RustlsClientConfig>,
    /// TLS to an `https` proxy, which carries tunnels and forwarded requests over HTTP/1.1.
    proxy_tls: StdArc<RustlsClientConfig>,
    /// The one deadline of an attempt: a request and every redirect it follows.
    timeout: Duration,
    redirects: Redirects,
    proxy: Option<DeclaredProxy>,
}

trait HttpStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> HttpStream for T {}

/// The HTTP version a connection speaks: HTTP/2 where its TLS negotiated it, HTTP/1.1 otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpVersion {
    Http1,
    Http2,
}

/// A connection ready for hyper: its stream and the version it speaks.
struct Established {
    stream: Box<dyn HttpStream>,
    version: HttpVersion,
}

/// How a request names its target, which the route it travels decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestForm<'a> {
    /// Straight to the endpoint, or inside a proxy's tunnel to it.
    Direct,
    /// To a proxy that forwards it, with the credentials the proxy asks for.
    Forwarded { authorization: Option<&'a str> },
}

/// Runs, as tasks of the node's runtime, what hyper's HTTP/2 client starts for an exchange: the
/// connection itself, which owns its stream, the request's body and the wait for its response.
///
/// Dropping hyper's handles only asks such a connection to close, which an endpoint that keeps a
/// stream open, or never grants the window the rest of a body needs, can put off indefinitely. So
/// each task ends when its exchange ends, by its final head, its failure or its timeout, and the
/// connection ends with it.
#[derive(Debug, Clone, Default)]
struct ConnectionTasks {
    /// Cancelled when the exchange that started these tasks ends.
    exchange: CancellationToken,
}

impl<F> hyper::rt::Executor<F> for ConnectionTasks
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        // Nothing waits for the task's outcome; it ends with its exchange at the latest.
        nervix_primitives::task::spawn(self.exchange.clone().run_until_cancelled_owned(future));
    }
}

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
    /// A prepared field HTTP's own types refuse, which the vocabulary's rules already exclude.
    #[error("HTTP request field could not be sent")]
    Field,
    #[error("HTTP endpoint answered with authentication or authorization status {status}")]
    Authentication { status: u16 },
    #[error("HTTP endpoint answered with retryable status {status}")]
    RetryableStatus { status: u16 },
    /// The client's declared proxy was not reached: its lookup, its connection or its TLS failed,
    /// as the cause beneath states.
    #[error("HTTP proxy could not be reached")]
    Proxy,
    /// The proxy did not answer the request for a tunnel, or did not hand the tunnel over.
    #[error("HTTP proxy tunnel request failed")]
    Tunnel,
    #[error("HTTP proxy refused the tunnel with status {status}")]
    TunnelRefused { status: u16 },
}

type HttpAttemptResult<T> = Result<T, Report<HttpAttemptError>>;

/// A resolved address that accepted no connection before its share of the attempt's timeout ended.
#[derive(Debug, Error)]
#[error("no connection was accepted within the address's share of timeout_ms")]
struct AddressShareElapsed;

impl HttpRequestEmitter {
    pub fn new(config: HttpRequestEmitterConfig, _host: EmitterHost) -> EmitterStartResult<Self> {
        Self::open(config)
    }

    /// The emitter `config` declares: its client transport, its proxy and its TLS settings, read
    /// locally without contacting the endpoint.
    fn open(config: HttpRequestEmitterConfig) -> EmitterStartResult<Self> {
        let transport = ClientTransport::declared(&config.config, HTTP_EMITTER).change_context(
            EmitterOpenError::InvalidConfiguration {
                connector: HTTP_EMITTER,
            },
        )?;
        let proxy = HttpClientConfig::new(&config.config, HTTP_EMITTER, &config.dns)
            .proxy()
            .change_context(EmitterOpenError::InvalidConfiguration {
                connector: HTTP_EMITTER,
            })?;
        let tls = RustlsClientConfigProvider::new(&config.config)
            .build_with_default_roots()
            .change_context(EmitterOpenError::InvalidConfiguration {
                connector: HTTP_EMITTER,
            })?;
        // rustls requires a standard Arc. An endpoint's TLS chooses the HTTP version among those
        // offered; a proxy's carries HTTP/1.1, which tunnels and forwarded requests speak.
        let mut endpoint_tls = (*tls).clone();
        endpoint_tls.alpn_protocols = vec![HTTP2_PROTOCOL.to_vec(), HTTP1_PROTOCOL.to_vec()];
        let mut proxy_tls = (*tls).clone();
        proxy_tls.alpn_protocols = vec![HTTP1_PROTOCOL.to_vec()];
        Ok(Self {
            dns: config.dns,
            endpoint_tls: StdArc::new(endpoint_tls),
            proxy_tls: StdArc::new(proxy_tls),
            timeout: transport.timeout,
            redirects: transport.redirects,
            proxy,
        })
    }

    /// The most of `available` a request's name lookup may take.
    fn lookup_share(available: Duration) -> LookupShare {
        let share = available
            .checked_div(LOOKUP_SHARE_DIVISOR)
            .assured("LOOKUP_SHARE_DIVISOR is a nonzero constant");
        LookupShare::AtMost(share)
    }

    /// One attempt: the request, and every redirect its client follows, which sends the same
    /// prepared request again to the redirect's `Location`. They share the attempt's one physical
    /// `timeout_ms`, so each request has what is left of it when it starts.
    async fn send(&self, request: &EmitterHttpRequest) -> HttpAttemptResult<FinalResponse> {
        let attempt = OperationBudget::start(self.timeout, LookupShare::Whole);
        let mut url = Cow::Borrowed(request.target.url());
        let mut followed = 0_usize;
        loop {
            nervix_primitives::task::consume_budget().await;
            let response = self.send_once(&url, request, attempt.remaining()).await?;
            let answer = RedirectAnswer {
                url: &url,
                method: request.method.as_str(),
                followed,
                status: response.status().code(),
                location: response.location(),
            };
            let Some(next) = self.redirects.next(&answer) else {
                return Ok(response);
            };
            url = Cow::Owned(next);
            followed = followed
                .checked_add(1)
                .verified("a followed redirect is below the declared limit");
        }
    }

    /// One request of an attempt, to `url`, within `available`: its DNS, connection, the proxy's TLS
    /// and tunnel, TLS, the exchange and its complete final headers. One budget divides
    /// `available`: the lookup of the endpoint's host, or the declared proxy's, takes at most its
    /// share, each resolved address an equal share of what is left when its attempt starts, and
    /// the rest whatever remains once an address accepts.
    ///
    /// Each failure keeps its cause beneath the attempt's own: the resolver's lookup failure, the
    /// socket or TLS error, the share an address ran out of, or hyper's error, beneath the proxy's
    /// own failure when it is the proxy that was not reached. They describe the connection, never
    /// the request.
    async fn send_once(
        &self,
        url: &Url,
        request: &EmitterHttpRequest,
        available: Duration,
    ) -> HttpAttemptResult<FinalResponse> {
        let budget = OperationBudget::start(available, Self::lookup_share(available));
        let exchanged = match &self.proxy {
            None => {
                let connection = self.connect(url, &budget).await?;
                nervix_primitives::time::timeout(
                    budget.remaining(),
                    self.exchange_directly(connection, url, request),
                )
                .await
            }
            Some(proxy) => {
                let connection = self
                    .connect(proxy.address(), &budget)
                    .await
                    .change_context(HttpAttemptError::Proxy)?;
                nervix_primitives::time::timeout(
                    budget.remaining(),
                    self.exchange_through(proxy, connection, url, request),
                )
                .await
            }
        };
        match exchanged {
            Ok(result) => result,
            Err(_) => Err(Report::new(HttpAttemptError::Timeout)),
        }
    }

    /// TLS for an `https` target, then the request in the version its TLS negotiated.
    async fn exchange_directly(
        &self,
        connection: TcpStream,
        url: &Url,
        request: &EmitterHttpRequest,
    ) -> HttpAttemptResult<FinalResponse> {
        let established = Self::secure(Box::new(connection), url, &self.endpoint_tls).await?;
        let prepared = Self::http_request(request, url, established.version, RequestForm::Direct)?;
        Self::exchange(established, prepared).await
    }

    /// TLS for an `https` proxy; then, for an `https` target, a tunnel through the proxy, TLS for
    /// the target inside it and the request in the version that TLS negotiated; for an `http`
    /// target, the request in absolute form, which the proxy forwards with its credentials.
    async fn exchange_through(
        &self,
        proxy: &DeclaredProxy,
        connection: TcpStream,
        url: &Url,
        request: &EmitterHttpRequest,
    ) -> HttpAttemptResult<FinalResponse> {
        let to_proxy = Self::secure(Box::new(connection), proxy.address(), &self.proxy_tls)
            .await
            .change_context(HttpAttemptError::Proxy)?;
        if url.scheme() == "https" {
            let tunnel = Self::tunnel(to_proxy, url, proxy.authorization()).await?;
            let established = Self::secure(tunnel, url, &self.endpoint_tls).await?;
            let prepared =
                Self::http_request(request, url, established.version, RequestForm::Direct)?;
            return Self::exchange(established, prepared).await;
        }
        let form = RequestForm::Forwarded {
            authorization: proxy.authorization(),
        };
        let prepared = Self::http_request(request, url, to_proxy.version, form)?;
        Self::exchange(to_proxy, prepared).await
    }

    /// Sends `request` over `connection` with hyper and returns its complete final head. hyper
    /// drives the connection only until that head arrives; the connection is then dropped with the
    /// response, so its body is neither awaited nor read and the connection is never reused. Over
    /// HTTP/2 the tasks hyper started for the exchange end with it, whether it returns or its
    /// timeout drops it.
    async fn exchange(
        connection: Established,
        request: Request<Full<Bytes>>,
    ) -> HttpAttemptResult<FinalResponse> {
        let io = TokioIo::new(connection.stream);
        match connection.version {
            HttpVersion::Http1 => {
                let (mut sender, driver) = http1::handshake(io)
                    .await
                    .map_err(|error| Report::new(error).change_context(HttpAttemptError::Send))?;
                Self::final_head(sender.send_request(request), driver).await
            }
            HttpVersion::Http2 => {
                let tasks = ConnectionTasks::default();
                let _ends_tasks_with_exchange = tasks.exchange.clone().drop_guard();
                let (mut sender, driver) = http2::Builder::new(tasks)
                    .handshake(io)
                    .await
                    .map_err(|error| Report::new(error).change_context(HttpAttemptError::Send))?;
                Self::final_head(sender.send_request(request), driver).await
            }
        }
    }

    /// The final head `sent` resolves with, while `driver` performs the connection's I/O.
    async fn final_head<B>(
        sent: impl Future<Output = hyper::Result<Response<B>>>,
        driver: impl Future<Output = hyper::Result<()>>,
    ) -> HttpAttemptResult<FinalResponse> {
        let answered = Self::answered(sent, driver).await;
        let response = answered
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Response))?;
        FinalResponse::read(&response).change_context(HttpAttemptError::Response)
    }

    /// What `sent` resolves with, while `driver` performs the connection's I/O. A connection can
    /// end in the same step that delivers its response's head, which then waits in hyper's channel,
    /// and one that ended without it has already settled the request with the reason; either way
    /// the request's own outcome is the answer, and it is ready once the driver has ended.
    async fn answered<T>(
        sent: impl Future<Output = hyper::Result<T>>,
        driver: impl Future<Output = hyper::Result<()>>,
    ) -> hyper::Result<T> {
        let mut sent = std::pin::pin!(sent);
        let driver = std::pin::pin!(driver);
        nervix_primitives::select! {
            biased;
            answered = &mut sent => answered,
            ended = driver => {
                ended.discarded("hyper settles the request with the reason its connection ended");
                (&mut sent).await
            }
        }
    }

    /// Asks the proxy on `connection` for a tunnel to `url`'s host and port with a `CONNECT`
    /// request hyper sends, carrying the credentials the proxy asks for, and returns the tunnel
    /// hyper hands over once the proxy answers with a `2xx`.
    async fn tunnel(
        connection: Established,
        url: &Url,
        authorization: Option<&str>,
    ) -> HttpAttemptResult<Box<dyn HttpStream>> {
        let request = Self::tunnel_request(url, authorization)?;
        let (mut sender, driver) = http1::handshake(TokioIo::new(connection.stream))
            .await
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Tunnel))?;
        let mut driver = std::pin::pin!(driver.with_upgrades());
        let mut sent = std::pin::pin!(sender.send_request(request));
        // The driver ends once it has handed the tunnel over, which can happen in the same step
        // that delivers the proxy's answer, as can its ending when the proxy refuses and closes.
        let mut ended = None;
        let answered = nervix_primitives::select! {
            biased;
            answered = &mut sent => answered,
            driven = &mut driver => {
                ended = Some(driven);
                (&mut sent).await
            }
        };
        let response = answered
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Tunnel))?;
        if !response.status().is_success() {
            return Err(Report::new(HttpAttemptError::TunnelRefused {
                status: response.status().as_u16(),
            }));
        }
        let upgrade = hyper::upgrade::on(response);
        let handed_over = match ended {
            Some(driven) => driven,
            None => driver.await,
        };
        handed_over.map_err(|error| Report::new(error).change_context(HttpAttemptError::Tunnel))?;
        let tunnel = upgrade
            .await
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Tunnel))?;
        Ok(Box::new(TokioIo::new(tunnel)))
    }

    /// The request for a tunnel to `url`'s host and port: its authority with the port written out,
    /// as `CONNECT` requires, which it also names as its `Host`, and the proxy's credentials.
    fn tunnel_request(
        url: &Url,
        authorization: Option<&str>,
    ) -> HttpAttemptResult<Request<Full<Bytes>>> {
        let host = url
            .host_str()
            .ok_or_else(|| Report::new(HttpAttemptError::Destination))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| Report::new(HttpAttemptError::Destination))?;
        let authority = format!("{host}:{port}");
        let uri = Uri::try_from(authority.as_str())
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Destination))?;
        let host = HeaderValue::try_from(authority)
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Destination))?;
        let mut request = Request::new(Full::new(Bytes::new()));
        *request.method_mut() = Method::CONNECT;
        *request.uri_mut() = uri;
        request.headers_mut().append(HOST, host);
        if let Some(authorization) = authorization {
            let authorization = Self::credentials(authorization)?;
            request
                .headers_mut()
                .append(PROXY_AUTHORIZATION, authorization);
        }
        Ok(request)
    }

    /// The proxy's credentials as a field value hyper never prints.
    fn credentials(authorization: &str) -> HttpAttemptResult<HeaderValue> {
        let mut value = HeaderValue::try_from(authorization)
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Field))?;
        value.set_sensitive(true);
        Ok(value)
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

    /// The connection an exchange continues on: `stream` itself, which speaks HTTP/1.1, or TLS over
    /// it to `url`'s host for an `https` URL, whether `url` names the endpoint or the proxy, which
    /// speaks the version its negotiation chose among those `tls` offers.
    async fn secure(
        stream: Box<dyn HttpStream>,
        url: &Url,
        tls: &StdArc<RustlsClientConfig>,
    ) -> HttpAttemptResult<Established> {
        if url.scheme() != "https" {
            return Ok(Established {
                stream,
                version: HttpVersion::Http1,
            });
        }
        let server_name = Self::server_name(url)?;
        let connector = TlsConnector::from(tls.clone());
        let secured = connector
            .connect(server_name, stream)
            .await
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Tls))?;
        let negotiated = secured.get_ref().1.alpn_protocol();
        let version = if negotiated == Some(HTTP2_PROTOCOL) {
            HttpVersion::Http2
        } else {
            HttpVersion::Http1
        };
        Ok(Established {
            stream: Box::new(secured),
            version,
        })
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

    /// The request hyper sends for `request` to `url` over a connection speaking `version`, along
    /// the route `form` names: exactly the prepared method, target, headers and body, and the
    /// fields the transport owns.
    ///
    /// The target is the path and query for HTTP/1.1 straight to the endpoint, and the absolute URL
    /// for HTTP/2, whose `:authority` hyper takes from it, and for a request a proxy forwards. Over
    /// HTTP/1.1 the request names its `Host` and asks for its connection to close, and through a
    /// forwarding proxy it carries the proxy's credentials. `Accept: */*` is added only when the
    /// application wrote no `Accept`. A body's length is declared by hyper from the body, and a
    /// bodyless request declares a zero length only when its method defines request content, as
    /// RFC 9110 section 8.6 asks of a client. No `Accept-Encoding`, `Content-Type`, cookie or
    /// credential is added.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "compares prepared header names through the external \
                                   iterator; its local predicate is analyzed; no internal \
                                   runtime ownership is inferred")
    )]
    fn http_request(
        request: &EmitterHttpRequest,
        url: &Url,
        version: HttpVersion,
        form: RequestForm<'_>,
    ) -> HttpAttemptResult<Request<Full<Bytes>>> {
        let method = Method::from_bytes(request.method.as_str().as_bytes())
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Field))?;
        let absolute = version == HttpVersion::Http2 || form != RequestForm::Direct;
        let target = if absolute {
            &url[..Position::AfterQuery]
        } else {
            &url[Position::BeforePath..Position::AfterQuery]
        };
        let uri = Uri::try_from(target)
            .map_err(|error| Report::new(error).change_context(HttpAttemptError::Destination))?;
        let body = match &request.body {
            Some(body) => Bytes::copy_from_slice(body),
            None => Bytes::new(),
        };
        let mut prepared = Request::new(Full::new(body));
        *prepared.method_mut() = method;
        *prepared.uri_mut() = uri;
        let headers = prepared.headers_mut();
        if version == HttpVersion::Http1 {
            let authority = HeaderValue::try_from(&url[Position::BeforeHost..Position::AfterPort])
                .map_err(|error| {
                    Report::new(error).change_context(HttpAttemptError::Destination)
                })?;
            headers.append(HOST, authority);
            headers.append(CONNECTION, HeaderValue::from_static("close"));
        }
        if let RequestForm::Forwarded {
            authorization: Some(authorization),
        } = form
        {
            headers.append(PROXY_AUTHORIZATION, Self::credentials(authorization)?);
        }
        if !request
            .headers
            .iter()
            .any(|(name, _)| name.as_str().eq_ignore_ascii_case("accept"))
        {
            headers.append(ACCEPT, HeaderValue::from_static("*/*"));
        }
        for (name, value) in request.headers.iter() {
            let name = HeaderName::from_bytes(name.as_str().as_bytes())
                .map_err(|error| Report::new(error).change_context(HttpAttemptError::Field))?;
            let value = HeaderValue::from_bytes(value.as_str().as_bytes())
                .map_err(|error| Report::new(error).change_context(HttpAttemptError::Field))?;
            headers.append(name, value);
        }
        if request.body.is_none() && request.method.defines_content() {
            headers.append(CONTENT_LENGTH, HeaderValue::from_static("0"));
        }
        Ok(prepared)
    }

    /// The failure of an attempt, described by the attempt's own error and every cause beneath
    /// it, such as `HTTP endpoint answered with retryable status 503` or
    /// `HTTP TLS handshake failed: invalid peer certificate: UnknownIssuer`.
    ///
    /// The emitter reports the description as its transient error while the request stays
    /// pending. Neither the attempt's errors nor their causes carry the evaluated target, a header
    /// value, a body or the proxy's credentials, so the description names none of them.
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
        response: &FinalResponse,
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
                        &response,
                    ));
                    return outcome;
                }
                Disposition::RetryableFailure => {
                    outcome.fail(Self::retryable_status_error(
                        HttpAttemptError::RetryableStatus {
                            status: status.code(),
                        },
                        &response,
                    ));
                    return outcome;
                }
            }
        }
        outcome
    }
}

#[cfg(test)]
#[path = "emitter_tests.rs"]
mod tests;
