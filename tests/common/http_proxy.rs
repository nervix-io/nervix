//! A forward HTTP proxy that stands in for one an operator provisioned, so a scenario can see which
//! requests a client sent through it and which it sent past it.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One loopback listener, the connections it accepts, the one request or tunnel each
//!   connection carries to its origin, the record of what each carried, the credentials it may
//!   require, and the bounded stop that ends all of them.
//! - **Depends on.** Tokio, the HTTP/1.1 request grammar through `httparse`, URL parsing and
//!   Base64 for the `Basic` credentials it checks.
//! - **Must not know.** Scenario state, nodes, Plexus, or what the requests it carries mean.
//!
//! # What one connection carries
//!
//! A connection carries one request. `CONNECT <host>:<port>` opens a tunnel: the proxy dials the
//! authority, answers `200`, and copies bytes both ways until both sides close, so it never sees
//! what TLS carries inside. A request in absolute form, `GET http://<host>:<port>/<path>`, is
//! forwarded: the proxy dials the URL's authority, writes the request line again in origin form,
//! drops `Proxy-Authorization`, `Proxy-Connection` and `Connection`, asks the origin to close once
//! it has answered, and copies the rest both ways. The connection then ends, so a client that keeps
//! connections alive opens a new one for its next request, and every request it sends is recorded.
//!
//! The proxy resolves names itself, as a real one does, and every name it is asked for resolves to
//! the loopback address where a scenario's receivers listen. A scenario can therefore name its
//! receiver by a host the nodes cannot resolve, such as one under `.invalid`, and a request reaches
//! that receiver only through the proxy.
//!
//! A proxy started with credentials answers `407` to a request whose `Proxy-Authorization` does not
//! carry exactly those `Basic` credentials, and records the refusal. A request in origin form, which
//! a client sends only to the origin itself, is answered `400` and recorded as misdirected.
//!
//! # Bounds
//!
//! A request head and the number of recorded requests each have a limit. Every wait a connection
//! makes observes the proxy's cancellation, and stopping gives connections
//! [`PROXY_CONNECTION_STOP_BUDGET`] to end before it aborts the ones that remain.

use std::{
    fmt, io,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use base64::Engine as _;
use meticulous::OptionExt as _;
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{Arc, CancellationToken, blocking::Mutex, watch},
    task::{AbortOnDropHandle, JoinSet},
};
use nervix_recovery::Discarded as _;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use url::{Position, Url};

/// How long stopping waits for connections to end on their own before it aborts them, in
/// seconds. A policy input: every wait a connection makes also waits for the proxy's cancellation.
const PROXY_CONNECTION_STOP_SECONDS: u64 = 5;
/// How long joining the accept loop may take once its connections are accounted for, in seconds.
const PROXY_ACCEPT_LOOP_JOIN_SECONDS: u64 = 1;
pub(crate) const PROXY_CONNECTION_STOP_BUDGET: Duration =
    Duration::from_secs(PROXY_CONNECTION_STOP_SECONDS);
/// The whole stop, connections and accept loop together.
pub(crate) const PROXY_STOP_BUDGET: Duration =
    Duration::from_secs(PROXY_CONNECTION_STOP_SECONDS + PROXY_ACCEPT_LOOP_JOIN_SECONDS);

/// The largest request head the proxy reads. A policy input: an HTTP REQUEST emitter's head at
/// every one of its limits fits, with room for the proxy's own fields.
const MAX_PROXY_HEAD_BYTES: usize = 256 * 1024;
/// The most header fields one request head may carry. A policy input with the same margin.
const MAX_PROXY_HEADERS: usize = 512;
/// The most requests one proxy records. A policy input.
const MAX_RECORDED_REQUESTS: usize = 4096;
/// How many bytes one read takes from a connection.
const READ_BYTES: usize = 4096;

const TUNNEL_ESTABLISHED: &[u8] = b"HTTP/1.1 200 Connection Established\r\n\r\n";
const AUTHENTICATION_REQUIRED: &[u8] = b"HTTP/1.1 407 Proxy Authentication Required\r\n\
    proxy-authenticate: Basic realm=\"scenario\"\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
const MISDIRECTED: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
const UNREACHABLE: &[u8] =
    b"HTTP/1.1 502 Bad Gateway\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";

/// What the proxy did with the one request a connection carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProxiedRequest {
    /// A `CONNECT` tunnel to `authority`, as the request line names it.
    Tunnel { authority: String },
    /// A request in absolute form forwarded to its origin, by its request line as the client
    /// wrote it.
    Forward { request_line: String },
    /// A request answered `407` because it did not carry the proxy's credentials.
    Refused { request_line: String },
    /// A request in origin form, which the proxy cannot route and answered `400`.
    Misdirected { request_line: String },
}

/// The `Basic` credentials a proxy requires, by the `Proxy-Authorization` value that carries them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProxyCredentials {
    authorization: String,
}

impl ProxyCredentials {
    pub(crate) fn basic(user: &str, password: &str) -> Self {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
        Self {
            authorization: format!("Basic {encoded}"),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum ProxyStartError {
    #[error("the HTTP proxy could not listen at {address}")]
    Bind {
        address: SocketAddr,
        #[source]
        source: io::Error,
    },
}

#[derive(Debug, Error)]
pub(crate) enum ProxyWaitError {
    #[error("the HTTP proxy recorded no {expected:?} within {waited:?}; it recorded {recorded:?}")]
    Request {
        expected: ProxiedRequest,
        waited: Duration,
        recorded: Vec<ProxiedRequest>,
    },
}

/// What one connection could not do, which ends it without a record.
#[derive(Debug, Error)]
enum ConnectionFault {
    #[error("the request head exceeds {limit} bytes")]
    HeadTooLarge { limit: usize },
    #[error("the request head is malformed")]
    Malformed(#[source] httparse::Error),
    #[error("the request target names no authority")]
    Target,
    #[error("the connection failed")]
    Io(#[source] io::Error),
}

struct ProxyState {
    credentials: Option<ProxyCredentials>,
    recorded: Mutex<Vec<ProxiedRequest>>,
    recorded_count: watch::Sender<usize>,
}

impl ProxyState {
    fn record(&self, request: ProxiedRequest) {
        let mut recorded = self.recorded.lock();
        if recorded.len() >= MAX_RECORDED_REQUESTS {
            return;
        }
        recorded.push(request);
        self.recorded_count.send_replace(recorded.len());
    }

    fn has(&self, expected: &ProxiedRequest) -> bool {
        self.recorded
            .lock()
            .iter()
            .any(|request| request == expected)
    }

    /// Whether `authorization` is the value the proxy requires, or the proxy requires none.
    fn admits(&self, authorization: Option<&[u8]>) -> bool {
        let Some(credentials) = &self.credentials else {
            return true;
        };
        authorization == Some(credentials.authorization.as_bytes())
    }
}

/// A running proxy. Dropping it aborts every task it started; [`HttpProxy::stop`] ends them within
/// [`PROXY_STOP_BUDGET`].
pub(crate) struct HttpProxy {
    address: SocketAddr,
    state: Arc<ProxyState>,
    cancellation: CancellationToken,
    accept_loop: AbortOnDropHandle<()>,
}

impl fmt::Debug for HttpProxy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpProxy")
            .field("address", &self.address)
            .field("recorded", &*self.state.recorded_count.borrow())
            .finish()
    }
}

impl HttpProxy {
    pub(crate) async fn start(
        address: SocketAddr,
        credentials: Option<ProxyCredentials>,
    ) -> Result<Self, ProxyStartError> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|source| ProxyStartError::Bind { address, source })?;
        let address = listener
            .local_addr()
            .map_err(|source| ProxyStartError::Bind { address, source })?;
        let (recorded_count, _) = watch::channel(0);
        let state = Arc::new(ProxyState {
            credentials,
            recorded: Mutex::new(Vec::new()),
            recorded_count,
        });
        let cancellation = CancellationToken::new();
        let accept_loop = AcceptLoop {
            listener,
            state: state.clone(),
            cancellation: cancellation.clone(),
        };
        Ok(Self {
            address,
            state,
            cancellation,
            accept_loop: AbortOnDropHandle::new(nervix_primitives::task::spawn(accept_loop.run())),
        })
    }

    pub(crate) fn port(&self) -> u16 {
        self.address.port()
    }

    pub(crate) fn recorded(&self) -> Vec<ProxiedRequest> {
        self.state.recorded.lock().clone()
    }

    /// Waits until the proxy has recorded `expected`.
    pub(crate) async fn wait_for(
        &self,
        expected: ProxiedRequest,
        within: Duration,
    ) -> Result<(), ProxyWaitError> {
        let mut recorded_count = self.state.recorded_count.subscribe();
        let state = self.state.clone();
        let waited = nervix_primitives::time::timeout(
            within,
            recorded_count.wait_for(|_| state.has(&expected)),
        )
        .await;
        match waited {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(ProxyWaitError::Request {
                expected,
                waited: within,
                recorded: self.recorded(),
            }),
        }
    }

    /// Stops accepting and ends every connection, within [`PROXY_STOP_BUDGET`].
    pub(crate) async fn stop(self) {
        self.cancellation.cancel();
        let Self {
            mut accept_loop, ..
        } = self;
        let joined = nervix_primitives::time::timeout(PROXY_STOP_BUDGET, &mut accept_loop).await;
        if joined.is_err() {
            accept_loop.abort();
            accept_loop
                .await
                .discarded("an aborted accept loop has nothing left to report");
        }
    }
}

struct AcceptLoop {
    listener: TcpListener,
    state: Arc<ProxyState>,
    cancellation: CancellationToken,
}

impl AcceptLoop {
    async fn run(self) {
        let mut connections = JoinSet::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                biased;
                () = self.cancellation.cancelled() => break,
                Some(joined) = connections.join_next(), if !connections.is_empty() => {
                    joined.discarded("a connection's own outcome is its record");
                }
                accepted = self.listener.accept() => {
                    let Ok((client, _)) = accepted else {
                        continue;
                    };
                    let connection = Connection {
                        state: self.state.clone(),
                        cancellation: self.cancellation.clone(),
                    };
                    connections.spawn(connection.serve(client));
                }
            }
        }
        let deadline = nervix_primitives::time::Instant::now() + PROXY_CONNECTION_STOP_BUDGET;
        loop {
            nervix_primitives::task::consume_budget().await;
            match nervix_primitives::time::timeout_at(deadline, connections.join_next()).await {
                Ok(Some(joined)) => {
                    joined.discarded("a connection's own outcome is its record");
                }
                Ok(None) => break,
                Err(_) => {
                    connections.abort_all();
                    while connections.join_next().await.is_some() {
                        nervix_primitives::task::consume_budget().await;
                    }
                    break;
                }
            }
        }
    }
}

/// The request head a connection opened with, and the bytes that arrived after it.
struct RequestHead {
    method: String,
    target: String,
    request_line: String,
    /// Every field the client wrote, in order, by its name and exact value bytes.
    fields: Vec<(String, Vec<u8>)>,
    /// Bytes read past the head: the start of a request body, or of what a tunnel carries.
    rest: Vec<u8>,
}

impl RequestHead {
    fn field(&self, name: &str) -> Option<&[u8]> {
        self.fields
            .iter()
            .find(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_slice())
    }

    /// The head written again for the origin: the request line in origin form, without the fields
    /// that speak to the proxy or keep the connection, and asking the origin to close once it has
    /// answered.
    fn for_origin(&self, url: &Url) -> Vec<u8> {
        let origin_form = &url[Position::BeforePath..Position::AfterQuery];
        let mut head = format!("{} {origin_form} HTTP/1.1\r\n", self.method).into_bytes();
        for (name, value) in &self.fields {
            let hop_by_hop = name.eq_ignore_ascii_case("proxy-authorization")
                || name.eq_ignore_ascii_case("proxy-connection")
                || name.eq_ignore_ascii_case("connection");
            if hop_by_hop {
                continue;
            }
            head.extend_from_slice(name.as_bytes());
            head.extend_from_slice(b": ");
            head.extend_from_slice(value);
            head.extend_from_slice(b"\r\n");
        }
        head.extend_from_slice(b"connection: close\r\n\r\n");
        head
    }
}

struct Connection {
    state: Arc<ProxyState>,
    cancellation: CancellationToken,
}

impl Connection {
    async fn serve(self, client: TcpStream) {
        let cancellation = self.cancellation.clone();
        nervix_primitives::select! {
            () = cancellation.cancelled() => {}
            carried = self.carry(client) => {
                carried.discarded("a connection the proxy could not carry records nothing");
            }
        }
    }

    async fn carry(&self, mut client: TcpStream) -> Result<(), ConnectionFault> {
        let head = Self::read_head(&mut client).await?;
        if !self.state.admits(head.field("proxy-authorization")) {
            self.state.record(ProxiedRequest::Refused {
                request_line: head.request_line.clone(),
            });
            return Self::answer(&mut client, AUTHENTICATION_REQUIRED).await;
        }
        if head.method.eq_ignore_ascii_case("CONNECT") {
            return self.tunnel(client, head).await;
        }
        let Ok(url) = Url::parse(&head.target) else {
            self.state.record(ProxiedRequest::Misdirected {
                request_line: head.request_line.clone(),
            });
            return Self::answer(&mut client, MISDIRECTED).await;
        };
        self.forward(client, head, url).await
    }

    async fn tunnel(
        &self,
        mut client: TcpStream,
        head: RequestHead,
    ) -> Result<(), ConnectionFault> {
        self.state.record(ProxiedRequest::Tunnel {
            authority: head.target.clone(),
        });
        let Some((host, port)) = head.target.rsplit_once(':') else {
            return Err(ConnectionFault::Target);
        };
        let Ok(port) = port.parse::<u16>() else {
            return Err(ConnectionFault::Target);
        };
        let Ok(mut origin) = TcpStream::connect(Self::origin_address(host, port)).await else {
            return Self::answer(&mut client, UNREACHABLE).await;
        };
        Self::answer(&mut client, TUNNEL_ESTABLISHED).await?;
        origin
            .write_all(&head.rest)
            .await
            .map_err(ConnectionFault::Io)?;
        tokio::io::copy_bidirectional(&mut client, &mut origin)
            .await
            .map_err(ConnectionFault::Io)?;
        Ok(())
    }

    async fn forward(
        &self,
        mut client: TcpStream,
        head: RequestHead,
        url: Url,
    ) -> Result<(), ConnectionFault> {
        self.state.record(ProxiedRequest::Forward {
            request_line: head.request_line.clone(),
        });
        let Some(host) = url.host_str() else {
            return Err(ConnectionFault::Target);
        };
        let Some(port) = url.port_or_known_default() else {
            return Err(ConnectionFault::Target);
        };
        let Ok(mut origin) = TcpStream::connect(Self::origin_address(host, port)).await else {
            return Self::answer(&mut client, UNREACHABLE).await;
        };
        origin
            .write_all(&head.for_origin(&url))
            .await
            .map_err(ConnectionFault::Io)?;
        origin
            .write_all(&head.rest)
            .await
            .map_err(ConnectionFault::Io)?;
        tokio::io::copy_bidirectional(&mut client, &mut origin)
            .await
            .map_err(ConnectionFault::Io)?;
        Ok(())
    }

    /// Where the proxy dials `host`: the address itself when it is a literal IPv4 address, and the
    /// loopback address, where a scenario's receivers listen, for every name.
    fn origin_address(host: &str, port: u16) -> SocketAddr {
        match host.parse::<Ipv4Addr>() {
            Ok(address) => SocketAddr::from((address, port)),
            Err(_) => SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        }
    }

    async fn answer(client: &mut TcpStream, response: &[u8]) -> Result<(), ConnectionFault> {
        client
            .write_all(response)
            .await
            .map_err(ConnectionFault::Io)?;
        client.flush().await.map_err(ConnectionFault::Io)
    }

    async fn read_head(client: &mut TcpStream) -> Result<RequestHead, ConnectionFault> {
        let mut buffer = Vec::with_capacity(READ_BYTES);
        loop {
            nervix_primitives::task::consume_budget().await;
            if let Some(head) = Self::parse_head(&buffer)? {
                return Ok(head);
            }
            if buffer.len() > MAX_PROXY_HEAD_BYTES {
                return Err(ConnectionFault::HeadTooLarge {
                    limit: MAX_PROXY_HEAD_BYTES,
                });
            }
            let mut chunk = [0_u8; READ_BYTES];
            let read = client.read(&mut chunk).await.map_err(ConnectionFault::Io)?;
            if read == 0 {
                return Err(ConnectionFault::Io(io::ErrorKind::UnexpectedEof.into()));
            }
            buffer.extend_from_slice(&chunk[..read]);
        }
    }

    fn parse_head(buffer: &[u8]) -> Result<Option<RequestHead>, ConnectionFault> {
        let mut fields = vec![httparse::EMPTY_HEADER; MAX_PROXY_HEADERS];
        let mut request = httparse::Request::new(&mut fields);
        let parsed = request.parse(buffer).map_err(ConnectionFault::Malformed)?;
        let httparse::Status::Complete(length) = parsed else {
            return Ok(None);
        };
        let method = request
            .method
            .verified("a complete request head has its method")
            .to_string();
        let target = request
            .path
            .verified("a complete request head has its target")
            .to_string();
        let version = request
            .version
            .verified("a complete request head has its version");
        let mut copied = Vec::with_capacity(request.headers.len());
        for field in request.headers.iter() {
            copied.push((field.name.to_string(), field.value.to_vec()));
        }
        Ok(Some(RequestHead {
            request_line: format!("{method} {target} HTTP/1.{version}"),
            method,
            target,
            fields: copied,
            rest: buffer[length..].to_vec(),
        }))
    }
}
