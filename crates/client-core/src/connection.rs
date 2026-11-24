//! Reaching a server: TLS selection, credentials, and the servers a client has learned about.
//!
//! Layer: edges.
//!
//! - **Owns.** The connect options, the channel a server is reached over with every HTTP/2 limit
//!   it takes and the connector it dials through with its TCP options and connection attempt
//!   delay, the `authorization` metadata every call carries, and the directory of servers a lost
//!   session reconnects to.
//! - **Depends on.** tonic's transport, hyper-util's connector, the cryptography boundary and the
//!   native DNS resolver.
//! - **Must not know.** What the calls on a channel carry.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    str::FromStr as _,
    time::Duration,
};

use arch_into::ArchInto as _;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use error_stack::{Report, ResultExt as _};
use hyper_util::client::legacy::connect::HttpConnector;
use indexmap::IndexSet;
use meticulous::OptionExt as _;
use nervix_dns::{DnsConfiguration, DnsResolver};
use nervix_recovery::Discarded as _;
use tonic::{
    Request,
    metadata::AsciiMetadataValue,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use url::Url;

use crate::{ClientCapabilities, error::ClientError};

#[derive(Debug, thiserror::Error)]
pub(crate) enum EndpointValidationError {
    #[error("the server URL is not a plain HTTP or HTTPS origin")]
    InvalidOrigin,
    #[error("the server URL does not satisfy the TLS requirement")]
    TlsRequired,
}

impl EndpointValidationError {
    /// The client failure a rejected server endpoint stands for, above the report of why it was
    /// rejected.
    pub(crate) fn into_client(report: Report<Self>) -> Report<ClientError> {
        let failure = match report.current_context() {
            Self::InvalidOrigin => ClientError::InvalidServerEndpoint,
            Self::TlsRequired => ClientError::TlsRequired,
        };
        report.change_context(failure)
    }
}

/// Whether a connection with no call open is pinged. It is not: such a connection protects no call,
/// and the next call's connect and request deadlines find a dead one.
const KEEPALIVE_WHILE_IDLE: bool = false;

/// Whether hyper sizes the receive windows from its bandwidth-delay estimate. It does not: the
/// estimate replaces the declared windows and grows them past what the client declared.
const ADAPTIVE_WINDOW: bool = false;

/// The largest frame the client accepts from the node: the frame every HTTP/2 peer must accept,
/// which the node's listener also fixes. The receive windows, not the frame size, bound what a
/// connection buffers.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 9113 section 6.5.2 starts SETTINGS_MAX_FRAME_SIZE at 16,384 octets, the \
                  frame every HTTP/2 peer must accept"
    )
)]
pub(crate) const MAX_FRAME_BYTES: u32 = 16 * 1024;

/// The header compression table the node may use for what it sends the client.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 7541 section 4.2 and RFC 9113 section 6.5.2 start \
                  SETTINGS_HEADER_TABLE_SIZE at 4,096 octets"
    )
)]
pub(crate) const HEADER_TABLE_BYTES: u32 = 4096;

/// The calls the client opens on a new connection before the node's first SETTINGS frame tells it
/// how many the node allows.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 9113 section 6.5.2 recommends that a peer allow no fewer than 100 \
                  concurrent streams"
    )
)]
pub(crate) const INITIAL_MAX_SEND_STREAMS: usize = 100;

/// The calls the node may open towards the client: none. The client sends ENABLE_PUSH=0, so the
/// node opens no stream, and the stream limit it advertises says the same.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 9113 section 8.4: a client that disables server push receives no stream the \
                  server opens, and a SETTINGS_MAX_CONCURRENT_STREAMS of zero prevents the server \
                  from opening any"
    )
)]
pub(crate) const NODE_OPENED_STREAMS: u32 = 0;

/// The calls the node opened and reset that may wait for the client to accept them: none, since
/// the node opens no call towards the client.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 9113 section 8.4: a client that disables server push accepts no stream the \
                  server opens, so none can wait unaccepted"
    )
)]
pub(crate) const PENDING_ACCEPT_RESET_STREAMS: usize = 0;

/// Whether the client sends a small write at once rather than holding it back while an earlier
/// segment waits for its acknowledgement. It does: pings, window updates and the session's request
/// frames are small writes the node waits for.
const TCP_NODELAY: bool = true;

/// The kernel keepalive of the client's TCP connections: none. HTTP/2 pings watch every connection
/// that has a call open, and an idle connection protects no call: the next call's connect and
/// request deadlines find a dead one.
const TCP_KEEPALIVE: Option<Duration> = None;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsRequirement {
    Preferred,
    Required,
}

#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Native DNS configuration, or an existing resolver shared with this client's owner.
    pub dns: ConnectDns,
    pub tls_requirement: Option<TlsRequirement>,
    pub ca_certificate_pem: Option<Vec<u8>>,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Additional configured gRPC endpoints available for initial connection and recovery.
    pub seed_servers: Vec<Url>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub retry_timeout: Duration,
    /// Overall BACKUP command wait, including redirects and reconnects, before archive download.
    pub backup_wait_timeout: Duration,
    pub retry_backoff: Duration,
    pub retry_max_backoff: Duration,
    /// The first wait before a restoration its new session refused is sent again on that session.
    pub restoration_backoff: Duration,
    /// The ceiling the waits between refused restorations on one session double up to.
    pub restoration_max_backoff: Duration,
    pub subscription_record_capacity: NonZeroUsize,
    pub subscription_retained_frames: NonZeroUsize,
    pub subscriptions_retained_in_full: NonZeroUsize,
    pub upload_frame_capacity: NonZeroUsize,
    pub restore_frame_capacity: NonZeroUsize,
    /// Request frames one session queues before a caller sending another waits for the transport.
    pub request_frame_capacity: NonZeroUsize,
    /// Server notices one session retains for a caller that has not read them.
    pub notice_capacity: NonZeroUsize,
    /// Bytes of unread server notices one session retains, each notice charged its text and its
    /// in-memory header.
    pub notice_bytes: NonZeroUsize,
    /// How long a connection with a call open may go without call data or a ping answer from the
    /// server before the client pings it.
    pub keepalive_interval: Duration,
    /// How long that ping may go unanswered before the connection is lost.
    pub keepalive_timeout: Duration,
    /// How many bytes of one call the server may send before the client reads them.
    pub stream_window_bytes: NonZeroU32,
    /// How many bytes of every call on one connection together the server may send before the
    /// client reads them.
    pub connection_window_bytes: NonZeroU32,
    /// How many calls wait for a connection while it is opened before the next caller waits for
    /// one of them to be sent.
    pub call_queue_capacity: NonZeroUsize,
    /// The largest header list, headers or trailers, the client accepts from the server on one
    /// call: each field's name and value plus 32 bytes, as HTTP/2 counts it.
    pub header_list_bytes: NonZeroU32,
    /// How many request bytes one call queues for the connection before it waits for the server
    /// to read.
    pub send_buffer_bytes: NonZeroU32,
    /// How many calls the client may reset for the server's protocol errors before it ends the
    /// connection.
    pub max_error_reset_streams: NonZeroU32,
    /// How many calls the client cancelled it remembers at once, so that frames the server sent
    /// for them before it learned of the cancellation are ignored.
    pub remembered_reset_streams: NonZeroU32,
    /// How long the client remembers each call it cancelled.
    pub reset_stream_duration: Duration,
    /// How long a connection attempt to an address of one family runs before an address of the
    /// other family is tried too, when a server's host name has both.
    pub connection_attempt_delay: Duration,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        let capabilities = ClientCapabilities::default();
        Self {
            dns: ConnectDns::Configuration(DnsConfiguration::system()),
            tls_requirement: None,
            ca_certificate_pem: None,
            username: None,
            password: None,
            seed_servers: Vec::new(),
            connect_timeout: capabilities.connect_timeout(),
            request_timeout: capabilities.request_timeout(),
            retry_timeout: capabilities.retry_timeout(),
            backup_wait_timeout: capabilities.backup_wait_timeout(),
            retry_backoff: capabilities.retry_backoff(),
            retry_max_backoff: capabilities.retry_max_backoff(),
            restoration_backoff: capabilities.restoration_backoff(),
            restoration_max_backoff: capabilities.restoration_max_backoff(),
            subscription_record_capacity: capabilities.subscription_record_capacity(),
            subscription_retained_frames: capabilities.subscription_retained_frames(),
            subscriptions_retained_in_full: capabilities.subscriptions_retained_in_full(),
            upload_frame_capacity: capabilities.upload_frame_capacity(),
            restore_frame_capacity: capabilities.restore_frame_capacity(),
            request_frame_capacity: capabilities.request_frame_capacity(),
            notice_capacity: capabilities.notice_capacity(),
            notice_bytes: capabilities.notice_bytes(),
            keepalive_interval: capabilities.keepalive_interval(),
            keepalive_timeout: capabilities.keepalive_timeout(),
            stream_window_bytes: capabilities.stream_window_bytes(),
            connection_window_bytes: capabilities.connection_window_bytes(),
            call_queue_capacity: capabilities.call_queue_capacity(),
            header_list_bytes: capabilities.header_list_bytes(),
            send_buffer_bytes: capabilities.send_buffer_bytes(),
            max_error_reset_streams: capabilities.max_error_reset_streams(),
            remembered_reset_streams: capabilities.remembered_reset_streams(),
            reset_stream_duration: capabilities.reset_stream_duration(),
            connection_attempt_delay: capabilities.connection_attempt_delay(),
        }
    }
}

/// Where a native client's Hickory resolver comes from.
#[derive(Debug, Clone)]
pub enum ConnectDns {
    /// Load the named resolver and hosts files once for this client.
    Configuration(DnsConfiguration),
    /// Share a resolver already owned by the caller's runtime.
    Resolver(DnsResolver),
}

impl ConnectOptions {
    /// The per-list bound for configured seeds and learned endpoints in a client's directory.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the client session protocol has the Rust client reconnect through at most \
                      32 endpoints it learned, and its configured seed list shares that bound"
        )
    )]
    pub const MAX_SEED_SERVERS: usize = 32;

    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    fn basic_authorization(&self) -> Option<String> {
        let username = self.username.as_ref()?;
        let password = self.password.as_ref()?;
        let encoded = BASE64_STANDARD.encode(format!("{username}:{password}"));
        Some(format!("Basic {encoded}"))
    }
}

/// Opens channels with one client's TLS policy and presents its credentials on every call.
#[derive(Clone)]
pub(crate) struct GrpcConnector {
    options: ConnectionParameters,
    capabilities: ClientCapabilities,
    dns: Option<DnsResolver>,
    /// The `authorization` metadata, built once from the credentials when the client has them.
    authorization: Option<AsciiMetadataValue>,
}

#[derive(Clone)]
struct ConnectionParameters {
    dns: ConnectDns,
    tls_requirement: Option<TlsRequirement>,
    ca_certificate_pem: Option<Vec<u8>>,
    seed_servers: Vec<Url>,
}

impl GrpcConnector {
    pub(crate) fn capabilities(&self) -> ClientCapabilities {
        self.capabilities
    }

    pub(crate) fn new(options: ConnectOptions) -> error_stack::Result<Self, ClientError> {
        let capabilities = ClientCapabilities::from_options(&options)?;
        let authorization =
            match options.basic_authorization() {
                Some(authorization) => Some(AsciiMetadataValue::from_str(&authorization).map_err(
                    |source| Report::new(ClientError::BuildAuthenticationMetadata(source)),
                )?),
                None => None,
            };
        Ok(Self {
            options: ConnectionParameters {
                dns: options.dns,
                tls_requirement: options.tls_requirement,
                ca_certificate_pem: options.ca_certificate_pem,
                seed_servers: options.seed_servers,
            },
            capabilities,
            dns: None,
            authorization,
        })
    }

    /// Load DNS at the client's safe setup boundary, or reuse its owner's resolver.
    pub(crate) async fn load_dns(&mut self) -> error_stack::Result<(), ClientError> {
        let dns = match &self.options.dns {
            ConnectDns::Configuration(configuration) => {
                DnsResolver::load(configuration.clone(), nervix_dns::DnsSettings::default())
                    .await
                    .change_context(ClientError::LoadDnsConfiguration)?
            }
            ConnectDns::Resolver(dns) => dns.clone(),
        };
        self.dns = Some(dns);
        Ok(())
    }

    /// The endpoint `server` is reached at: its validated origin, TLS when the origin asks for
    /// it, the connection deadline, the queue of calls waiting for a connection, the TCP options,
    /// and every HTTP/2 limit every connection of this client takes, each from a client capability
    /// or a fixed value.
    pub(crate) fn endpoint(&self, server: &Url) -> error_stack::Result<Endpoint, ClientError> {
        self.validate_server(server)
            .map_err(EndpointValidationError::into_client)?;
        let is_https = server.scheme() == "https";
        let mut endpoint = Channel::from_shared(server.as_str().to_string())
            .map_err(|error| Report::new(ClientError::InvalidServerUri(error)))?;
        let send_buffer_bytes: usize = self.capabilities.send_buffer_bytes().get().arch_into();
        let max_error_reset_streams: usize = self
            .capabilities
            .max_error_reset_streams()
            .get()
            .arch_into();
        let remembered_reset_streams: usize = self
            .capabilities
            .remembered_reset_streams()
            .get()
            .arch_into();
        endpoint = endpoint
            .connect_timeout(self.capabilities.connect_timeout())
            .buffer_size(self.capabilities.call_queue_capacity().get())
            .tcp_nodelay(TCP_NODELAY)
            .tcp_keepalive(TCP_KEEPALIVE)
            .http2_keep_alive_interval(self.capabilities.keepalive_interval())
            .keep_alive_timeout(self.capabilities.keepalive_timeout())
            .keep_alive_while_idle(KEEPALIVE_WHILE_IDLE)
            .initial_stream_window_size(self.capabilities.stream_window_bytes().get())
            .initial_connection_window_size(self.capabilities.connection_window_bytes().get())
            .http2_adaptive_window(ADAPTIVE_WINDOW)
            .http2_max_header_list_size(self.capabilities.header_list_bytes().get())
            .http2_header_table_size(HEADER_TABLE_BYTES)
            .max_frame_size(MAX_FRAME_BYTES)
            .http2_initial_max_send_streams(INITIAL_MAX_SEND_STREAMS)
            .http2_max_concurrent_streams(NODE_OPENED_STREAMS)
            .http2_max_send_buf_size(send_buffer_bytes)
            .http2_max_local_error_reset_streams(max_error_reset_streams)
            .http2_max_concurrent_reset_streams(remembered_reset_streams)
            .http2_reset_stream_duration(self.capabilities.reset_stream_duration())
            .http2_max_pending_accept_reset_streams(Some(PENDING_ACCEPT_RESET_STREAMS));
        if is_https {
            nervix_crypto::tls::install_default_provider();
            let mut tls =
                ClientTlsConfig::new().crypto_provider(nervix_crypto::tls::fips_provider());
            if let Some(pem) = self.options.ca_certificate_pem.clone() {
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            endpoint = endpoint
                .tls_config(tls)
                .map_err(|error| Report::new(ClientError::ConfigureTls(error)))?;
        }
        Ok(endpoint)
    }

    pub(crate) async fn connect(&self, server: &Url) -> error_stack::Result<Channel, ClientError> {
        let endpoint = self.endpoint(server)?;
        endpoint
            .connect_with_connector(self.http_connector())
            .await
            .map_err(|error| Report::new(ClientError::ConnectServer(error)))
    }

    /// The connector every connection of this client dials through: the loaded resolver, the TCP
    /// options, the connection deadline, and how long an attempt to a host name's first address
    /// family runs before its other family is tried too.
    pub(crate) fn http_connector(&self) -> HttpConnector<DnsResolver> {
        let dns = self
            .dns
            .as_ref()
            .assured("Client::connect_with_options loads DNS before opening any server channel");
        let mut connector = HttpConnector::new_with_resolver(dns.clone());
        connector.enforce_http(false);
        connector.set_nodelay(TCP_NODELAY);
        connector.set_keepalive(TCP_KEEPALIVE);
        connector.set_connect_timeout(Some(self.capabilities.connect_timeout()));
        connector.set_happy_eyeballs_timeout(Some(self.capabilities.connection_attempt_delay()));
        connector
    }

    pub(crate) fn validate_server(
        &self,
        server: &Url,
    ) -> error_stack::Result<(), EndpointValidationError> {
        if !matches!(server.scheme(), "http" | "https")
            || !server.has_host()
            || !server.username().is_empty()
            || server.password().is_some()
            || server.path() != "/"
            || server.query().is_some()
            || server.fragment().is_some()
        {
            return Err(error_stack::Report::new(
                EndpointValidationError::InvalidOrigin,
            ));
        }
        if self.options.tls_requirement == Some(TlsRequirement::Required)
            && server.scheme() != "https"
        {
            return Err(error_stack::Report::new(
                EndpointValidationError::TlsRequired,
            ));
        }
        Ok(())
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        self.capabilities.request_timeout()
    }

    pub(crate) fn connect_timeout(&self) -> Duration {
        self.capabilities.connect_timeout()
    }

    pub(crate) fn retry_timeout(&self) -> Duration {
        self.capabilities.retry_timeout()
    }

    pub(crate) fn backup_wait_timeout(&self) -> Duration {
        self.capabilities.backup_wait_timeout()
    }

    pub(crate) fn seed_servers(&self) -> &[Url] {
        &self.options.seed_servers
    }

    /// Presents the client's credentials on a call.
    pub(crate) fn authorize<T>(&self, request: &mut Request<T>) {
        if let Some(authorization) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", authorization.clone());
        }
    }
}

/// The configured seeds and bounded discovered endpoints a client can reconnect to.
#[derive(Debug, Default)]
pub(crate) struct ServerDirectory {
    current: Option<Url>,
    seeds: IndexSet<Url>,
    discovered: IndexSet<Url>,
}

impl ServerDirectory {
    /// A directory whose only server is the one the first exchange runs on, when it is known.
    pub(crate) fn connected_to(server: Option<Url>) -> Self {
        let mut directory = Self::default();
        if let Some(server) = server {
            directory.seeds.insert(server.clone());
            directory.current = Some(server);
        }
        directory
    }

    pub(crate) fn with_seeds(primary: Url, seeds: &[Url]) -> Self {
        let mut directory = Self::default();
        directory.seeds.insert(primary);
        for seed in seeds {
            directory.seeds.insert(seed.clone());
        }
        directory
    }

    pub(crate) fn remember(&mut self, server: &Url) {
        if self.seeds.contains(server) || self.discovered.contains(server) {
            return;
        }
        if self.discovered.len() == ConnectOptions::MAX_SEED_SERVERS {
            self.discovered
                .shift_remove_index(0)
                .discarded("the oldest discovered endpoint gives way to a fresh one");
        }
        self.discovered.insert(server.clone());
    }

    /// Records that the exchange now runs on `server`.
    pub(crate) fn connected(&mut self, server: &Url) {
        self.current = Some(server.clone());
        self.remember(server);
    }

    /// Whether any server is known to open a new session on when the exchange is lost.
    pub(crate) fn can_reconnect(&self) -> bool {
        self.current.is_some() || !self.seeds.is_empty() || !self.discovered.is_empty()
    }

    /// The servers to try when the exchange is lost: every known server other than the current
    /// one, in the order they were learned, and then the current one.
    pub(crate) fn reconnect_candidates(&self) -> Vec<Url> {
        let mut candidates = Vec::with_capacity(self.seeds.len() + self.discovered.len());
        for server in self.seeds.iter().chain(&self.discovered) {
            if self.current.as_ref() != Some(server) {
                candidates.push(server.clone());
            }
        }
        if let Some(current) = &self.current {
            candidates.push(current.clone());
        }
        candidates
    }
}
