//! A `nervix-server` executable started as a real operating-system process.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One isolated server process: its ports, database directory, interconnect
//!   credentials, command-line options, captured log, the signals delivered to it, the clients a
//!   scenario holds open against it, and the status it exits with.
//! - **Depends on.** The executable Cargo builds beside this test target, the shared test port
//!   reservations, the test interconnect certificate authority, and the session client.
//! - **Must not know.** Server internals. Everything it observes crosses the process boundary: the
//!   gRPC API, HTTP/2 frames, the files the process writes, the log the process writes, and how
//!   the process exits.
//!
//! The in-process cluster fixture runs nodes as Tokio tasks and stops them through their shutdown
//! coordinator, so it cannot show whether the process boundary delivers a signal to that
//! coordinator at all. Scenarios about signals, exit statuses and process startup use this fixture
//! instead.

use std::{
    fs::OpenOptions,
    io,
    net::{Ipv4Addr, SocketAddr},
    os::unix::process::ExitStatusExt as _,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use bytes::{BufMut as _, Bytes, BytesMut};
use meticulous::OptionExt as _;
use nervix_recovery::Discarded as _;
use nervix_server::proto::{UploadResourceRequest, UploadResourceStart, upload_resource_request};
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use prost::Message as _;
use tempfile::TempDir;
use tokio::{
    net::TcpStream,
    process::{Child, Command},
    sync::watch,
    time::{sleep, timeout},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

use super::{
    cluster::{
        InterconnectTestCa, TEST_AUTH_PASSWORD, TEST_AUTH_USERNAME, TestCertificateValidity,
        TestSession, open_raw_session, publish_http_uri_with_headers, run_command_via_client,
        test_basic_authorization,
    },
    node_liveness::{LastReadinessOutcome, ReadinessProbeOutcome},
    phase_deadline::PhaseDeadline,
    port_pool::{next_port, release_test_ports},
    status_request::{
        STATUS_REQUEST_TIMEOUT, STATUS_REQUESTS_PER_STARTUP, StatusEndpoint, StatusTransport,
    },
};

/// The identity the process runs as and its certificate names. Each process forms its own
/// single-node cluster, so one identity serves every process.
const CLUSTER_ID: &str = "cucumber";
const NODE_ID: &str = "node-1";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const _: () = assert!(
    match STATUS_REQUEST_TIMEOUT.checked_mul(STATUS_REQUESTS_PER_STARTUP) {
        Some(requests) => requests.as_nanos() <= STARTUP_TIMEOUT.as_nanos(),
        None => false,
    },
    "a server process startup must outlast its stalled readiness requests"
);
/// Waiting ends as soon as the process exits, so this bound only has to hold on a machine running
/// many scenarios at once.
const EXIT_TIMEOUT: Duration = Duration::from_secs(120);
const LOG_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const LOG_TAIL_LINES: usize = 80;
const UPLOAD_RESOURCE_PATH: &str = "/io.nervix.api.v1.SessionService/UploadResource";
const HELD_UPLOAD_IDENTITY: &str = "held-upload";
/// The archive size a held upload declares. A slow upload sends one byte per interval, so it
/// would take far longer than any scenario to finish.
const HELD_UPLOAD_DECLARED_BYTES: u64 = 1024 * 1024;
const SLOW_UPLOAD_CHUNK_INTERVAL: Duration = Duration::from_millis(100);
const HTTP_LOAD_INTERVAL: Duration = Duration::from_millis(10);
/// gRPC prefixes every message on a stream with a compression flag byte and a four-byte big-endian
/// message length.
const GRPC_MESSAGE_PREFIX_BYTES: usize = 5;
const UNCOMPRESSED_GRPC_MESSAGE: u8 = 0;

/// How a scenario executes the server binary.
#[derive(Clone, Debug)]
pub(crate) enum ServerProcessLaunch {
    /// Execute the binary directly, the way the container image's exec-form command does.
    Direct,
    /// Execute a separately built server binary, such as the release subject of a benchmark.
    Executable(PathBuf),
    /// Lower the soft and hard open-file limits before executing the binary. A shell applies the
    /// limit and then replaces itself with the server, so the server keeps the shell's process.
    OpenFileLimit(u32),
}

impl ServerProcessLaunch {
    fn command(&self) -> Command {
        let default_executable = Path::new(env!("CARGO_BIN_EXE_nervix-server"));
        match self {
            Self::Direct => Command::new(default_executable),
            Self::Executable(executable) => Command::new(executable),
            Self::OpenFileLimit(limit) => {
                let mut command = Command::new("bash");
                command
                    .arg("-c")
                    // The soft limit goes first: a hard limit below the current soft limit is
                    // rejected, and bash would exit on its own without running the server.
                    .arg(r#"ulimit -Sn "$1" && ulimit -Hn "$1" && shift && exec "$@""#)
                    .arg("bash")
                    .arg(limit.to_string())
                    .arg(default_executable);
                command
            }
        }
    }
}

/// A command-line option a scenario sets on the server process in addition to the fixture's own.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ServerProcessOption {
    /// `--drain-timeout`.
    DrainTimeout(Duration),
    /// `--state-snapshot-interval`.
    StateSnapshotInterval(Duration),
    /// `--shutdown-timeout`.
    ShutdownTimeout(Duration),
    /// `--transaction-idle-timeout`.
    TransactionIdleTimeout(Duration),
    /// `--transaction-tombstone-retention`.
    TransactionTombstoneRetention(Duration),
}

impl ServerProcessOption {
    fn apply_to(self, command: &mut Command) {
        match self {
            Self::DrainTimeout(timeout) => {
                command
                    .arg("--drain-timeout")
                    .arg(humantime::format_duration(timeout).to_string());
            }
            Self::StateSnapshotInterval(interval) => {
                command
                    .arg("--state-snapshot-interval")
                    .arg(humantime::format_duration(interval).to_string());
            }
            Self::ShutdownTimeout(timeout) => {
                command
                    .arg("--shutdown-timeout")
                    .arg(humantime::format_duration(timeout).to_string());
            }
            Self::TransactionIdleTimeout(timeout) => {
                command
                    .arg("--transaction-idle-timeout")
                    .arg(humantime::format_duration(timeout).to_string());
            }
            Self::TransactionTombstoneRetention(retention) => {
                command
                    .arg("--transaction-tombstone-retention")
                    .arg(humantime::format_duration(retention).to_string());
            }
        }
    }
}

/// How far a held upload has gone when its client stops making progress.
#[derive(Clone, Copy, Debug)]
pub(crate) enum HeldUploadProgress {
    /// The stream is open and nothing was sent, so the server waits for the first message.
    AwaitingFirstMessage,
    /// The archive was declared and one byte of it sent, so the server waits for the next chunk.
    AwaitingNextChunk,
    /// The archive was declared and one byte of it arrives per interval, so the upload is always
    /// in progress and never finishes.
    SendingSlowly,
}

/// The listeners one server process binds, reserved against other scenarios while it owns them.
#[derive(Debug)]
struct ServerProcessPorts {
    grpc: u16,
    http: u16,
    https: u16,
    observability: u16,
    web_console: u16,
    interconnect: u16,
}

impl ServerProcessPorts {
    fn allocate() -> io::Result<Self> {
        Ok(Self {
            grpc: next_port()?,
            http: next_port()?,
            https: next_port()?,
            observability: next_port()?,
            web_console: next_port()?,
            interconnect: next_port()?,
        })
    }

    fn all(&self) -> [u16; 6] {
        [
            self.grpc,
            self.http,
            self.https,
            self.observability,
            self.web_console,
            self.interconnect,
        ]
    }
}

impl Drop for ServerProcessPorts {
    fn drop(&mut self) {
        release_test_ports(&self.all());
    }
}

/// The stable launch configuration retained when a scenario restarts the same server.
#[derive(Debug)]
struct ServerProcessConfiguration {
    launch: ServerProcessLaunch,
    options: Vec<ServerProcessOption>,
    ports: ServerProcessPorts,
    certificate_authority: PathBuf,
    certificate: PathBuf,
    private_key: PathBuf,
    temp_dir: PathBuf,
}

impl ServerProcessConfiguration {
    fn spawn(&self, root: &Path) -> io::Result<Child> {
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("server.log"))?;
        let error_log = log.try_clone()?;

        let mut command = self.launch.command();
        command
            .arg("--addr")
            .arg(loopback(self.ports.grpc))
            .arg("--grpc-advertise-addr")
            .arg(loopback(self.ports.grpc))
            .arg("--http-listen-addr")
            .arg(loopback(self.ports.http))
            .arg("--https-listen-addr")
            .arg(loopback(self.ports.https))
            .arg("--observability-listen-addr")
            .arg(loopback(self.ports.observability))
            .arg("--web-console-listen-addr")
            .arg(loopback(self.ports.web_console))
            .arg("--cluster-id")
            .arg(CLUSTER_ID)
            .arg("--node-id")
            .arg(NODE_ID)
            .arg("--interconnect-listen-addr")
            .arg(loopback(self.ports.interconnect))
            .arg("--interconnect-advertise-addr")
            .arg(loopback(self.ports.interconnect))
            .arg("--interconnect-tls-ca")
            .arg(&self.certificate_authority)
            .arg("--interconnect-tls-cert")
            .arg(&self.certificate)
            .arg("--interconnect-tls-key")
            .arg(&self.private_key)
            .arg("--allow-bootstrap")
            .arg("--default-user")
            .arg(TEST_AUTH_USERNAME)
            .arg("--init-default-user-password")
            .arg(TEST_AUTH_PASSWORD)
            .arg("--db-path")
            .arg(root.join("db"))
            .arg("--temp-dir")
            .arg(&self.temp_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(error_log))
            .kill_on_drop(true);
        for option in &self.options {
            option.apply_to(&mut command);
        }
        // Any `NERVIX_*` variable the scenario runner carries would silently reconfigure the
        // server, and `RUST_LOG` would replace the log filter the server ships with.
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("NERVIX_") {
                command.env_remove(&name);
            }
        }
        command.env_remove("RUST_LOG");
        command.spawn()
    }
}

/// A started `nervix-server` process and everything it was given.
///
/// Dropping it kills the process, so a failed scenario never leaves a server running.
#[derive(Debug)]
pub(crate) struct ServerProcess {
    child: Child,
    exit_status: Option<ExitStatus>,
    configuration: ServerProcessConfiguration,
    /// Byte offset where the current process launch began in the shared append-only log.
    log_start: u64,
    root: TempDir,
}

impl ServerProcess {
    pub(crate) fn start(
        launch: ServerProcessLaunch,
        options: &[ServerProcessOption],
    ) -> io::Result<Self> {
        let root = tempfile::Builder::new()
            .prefix("nervix-server-process-")
            .tempdir()?;
        let certificate_authority = InterconnectTestCa::new(&root)?;
        let (certificate, private_key) = certificate_authority.issue_node_with_identity(
            CLUSTER_ID,
            NODE_ID,
            TestCertificateValidity::Current,
            root.path(),
        )?;
        let temp_dir = root.path().join("temp");
        std::fs::create_dir_all(&temp_dir)?;
        let configuration = ServerProcessConfiguration {
            launch,
            options: options.to_vec(),
            ports: ServerProcessPorts::allocate()?,
            certificate_authority: certificate_authority.path.clone(),
            certificate,
            private_key,
            temp_dir,
        };
        let child = configuration.spawn(root.path())?;

        Ok(Self {
            child,
            exit_status: None,
            configuration,
            log_start: 0,
            root,
        })
    }

    /// Reopens this process's existing database on the same isolated ports.
    pub(crate) async fn restart(&mut self) -> io::Result<()> {
        if self.observe_exit()?.is_none() {
            return Err(io::Error::other(
                "cannot restart nervix-server before its preceding process exits",
            ));
        }
        self.log_start = std::fs::metadata(self.root.path().join("server.log"))?.len();
        self.child = self.configuration.spawn(self.root.path())?;
        self.exit_status = None;
        self.wait_until_ready().await
    }

    /// Waits until the server answers an authenticated command, which also proves that its
    /// configured default user exists. Every readiness probe receives only the time left before
    /// the startup deadline, so a server that never answers ends the wait at that deadline.
    pub(crate) async fn wait_until_ready(&mut self) -> io::Result<()> {
        let deadline = PhaseDeadline::after(STARTUP_TIMEOUT);
        let endpoint = self.status_endpoint();
        let mut last_readiness = LastReadinessOutcome::NoCompletedProbe;
        loop {
            tokio::task::consume_budget().await;
            if let Some(status) = self.observe_exit()? {
                return Err(io::Error::other(format!(
                    "nervix-server exited during startup with {}; last readiness outcome: \
                     {last_readiness}\n{}",
                    describe_exit(status),
                    self.log_tail()
                )));
            }
            if deadline.has_passed() {
                return Err(io::Error::other(format!(
                    "nervix-server did not accept commands within {}; last readiness outcome: \
                     {last_readiness}\n{}",
                    humantime::format_duration(STARTUP_TIMEOUT),
                    self.log_tail()
                )));
            }
            let outcome = ReadinessProbeOutcome::probe(&endpoint, deadline).await;
            if outcome.is_ready() {
                return Ok(());
            }
            last_readiness = LastReadinessOutcome::Observed(outcome);
            deadline.pause(POLL_INTERVAL).await;
        }
    }

    fn status_endpoint(&self) -> StatusEndpoint {
        StatusEndpoint::new(
            SocketAddr::from((Ipv4Addr::LOCALHOST, self.configuration.ports.grpc)),
            StatusTransport::Plaintext,
            test_basic_authorization(),
        )
    }

    /// Runs NSPL through an authenticated session whose active domain is `domain`.
    pub(crate) async fn run_commands(&self, domain: &str, commands: &str) -> io::Result<String> {
        run_command_via_client(&self.grpc_uri(), domain, commands).await
    }

    pub(crate) async fn open_session(&self, domain: &str) -> io::Result<TestSession> {
        open_raw_session(&self.grpc_uri(), domain).await
    }

    /// Posts `payload` to an HTTP endpoint the process serves for virtual host `host`, returning
    /// once the endpoint has admitted it.
    pub(crate) async fn publish_http(
        &self,
        host: &str,
        path: &str,
        payload: &str,
    ) -> io::Result<()> {
        let uri = format!("http://{}{path}", loopback(self.configuration.ports.http));
        publish_http_uri_with_headers(uri, host, payload.as_bytes(), "application/json", &[]).await
    }

    /// Repeats one HTTP publish until the recovered runtime has installed the endpoint.
    pub(crate) async fn publish_http_eventually(
        &mut self,
        host: &str,
        path: &str,
        payload: &str,
    ) -> io::Result<()> {
        let admitted = timeout(LOG_TIMEOUT, self.poll_http_admission(host, path, payload)).await;
        match admitted {
            Ok(result) => result,
            Err(_) => Err(io::Error::other(format!(
                "nervix-server did not admit an HTTP payload within {}\n{}",
                humantime::format_duration(LOG_TIMEOUT),
                self.log_tail()
            ))),
        }
    }

    async fn poll_http_admission(
        &mut self,
        host: &str,
        path: &str,
        payload: &str,
    ) -> io::Result<()> {
        loop {
            tokio::task::consume_budget().await;
            if let Some(status) = self.observe_exit()? {
                return Err(io::Error::other(format!(
                    "nervix-server exited with {} before it admitted the HTTP payload\n{}",
                    describe_exit(status),
                    self.log_tail()
                )));
            }
            if self.publish_http(host, path, payload).await.is_ok() {
                return Ok(());
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    /// Starts a bounded-rate HTTP load that replaces `{{load_id}}` in each payload template.
    pub(crate) fn start_http_load(
        &self,
        host: &str,
        path: &str,
        first_id: u64,
        payload_templates: Vec<String>,
    ) -> io::Result<ServerProcessHttpLoad> {
        if payload_templates.is_empty() {
            return Err(io::Error::other(
                "server process HTTP load requires at least one payload template",
            ));
        }
        if payload_templates
            .iter()
            .any(|payload| !payload.contains("{{load_id}}"))
        {
            return Err(io::Error::other(
                "every server process HTTP load payload must contain {{load_id}}",
            ));
        }

        let uri = format!("http://{}{path}", loopback(self.configuration.ports.http));
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let (observation_tx, observation) = watch::channel(HttpLoadObservation::default());
        let host = host.to_string();
        let task = tokio::spawn(async move {
            run_http_load(
                task_cancellation,
                observation_tx,
                uri,
                host,
                first_id,
                payload_templates,
            )
            .await;
        });
        Ok(ServerProcessHttpLoad {
            cancellation,
            observation,
            _task: AbortOnDropHandle::new(task),
        })
    }

    pub(crate) fn send_signal(&self, signal: Signal) -> io::Result<()> {
        let Some(pid) = self.child.id() else {
            return Err(io::Error::other(format!(
                "cannot deliver {signal} to nervix-server: the process has already been reaped"
            )));
        };
        let pid = i32::try_from(pid).map_err(io::Error::other)?;
        kill(Pid::from_raw(pid), signal).map_err(io::Error::other)
    }

    pub(crate) async fn wait_for_exit(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.exit_status {
            return Ok(status);
        }
        let waited = timeout(EXIT_TIMEOUT, self.child.wait()).await;
        let status = match waited {
            Ok(status) => status?,
            Err(_) => {
                return Err(io::Error::other(format!(
                    "nervix-server did not exit within {}\n{}",
                    humantime::format_duration(EXIT_TIMEOUT),
                    self.log_tail()
                )));
            }
        };
        self.exit_status = Some(status);
        Ok(status)
    }

    pub(crate) fn has_exited(&self) -> bool {
        self.exit_status.is_some()
    }

    fn observe_exit(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.exit_status {
            return Ok(Some(status));
        }
        let status = self.child.try_wait()?;
        self.exit_status = status;
        Ok(status)
    }

    /// Everything the current process launch has written to standard output and standard error.
    pub(crate) fn log(&self) -> io::Result<String> {
        let bytes = std::fs::read(self.root.path().join("server.log"))?;
        let log_start = usize::try_from(self.log_start).map_err(io::Error::other)?;
        let current = bytes.get(log_start..).ok_or_else(|| {
            io::Error::other(format!(
                "server log shrank below the current launch offset {}",
                self.log_start
            ))
        })?;
        Ok(String::from_utf8_lossy(current).into_owned())
    }

    pub(crate) async fn wait_for_log(&mut self, fragment: &str) -> io::Result<()> {
        let found = timeout(LOG_TIMEOUT, self.poll_for_log(fragment)).await;
        match found {
            Ok(result) => result,
            Err(_) => Err(io::Error::other(format!(
                "nervix-server did not log {fragment:?} within {}\n{}",
                humantime::format_duration(LOG_TIMEOUT),
                self.log_tail()
            ))),
        }
    }

    async fn poll_for_log(&mut self, fragment: &str) -> io::Result<()> {
        loop {
            tokio::task::consume_budget().await;
            // The exit is read before the log, so a process that logged the fragment and then
            // exited is still seen to have logged it.
            let exit = self.observe_exit()?;
            if self.log()?.contains(fragment) {
                return Ok(());
            }
            if let Some(status) = exit {
                return Err(io::Error::other(format!(
                    "nervix-server exited with {} without logging {fragment:?}\n{}",
                    describe_exit(status),
                    self.log_tail()
                )));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    pub(crate) fn log_tail(&self) -> String {
        let log = match self.log() {
            Ok(log) => log,
            Err(error) => return format!("the server log is unreadable: {error}"),
        };
        let mut tail: Vec<&str> = log.lines().rev().take(LOG_TAIL_LINES).collect();
        tail.reverse();
        format!("last {} server log lines:\n{}", tail.len(), tail.join("\n"))
    }

    /// Opens an authenticated `UploadResource` stream for `resource` in `domain` and leaves it at
    /// `progress`.
    ///
    /// The server's handler waits on this client for as long as the returned value lives: for the
    /// first upload message, or for the next archive chunk.
    pub(crate) async fn hold_resource_upload(
        &mut self,
        domain: &str,
        resource: &str,
        progress: HeldUploadProgress,
    ) -> io::Result<HeldResourceUpload> {
        let stream = TcpStream::connect(loopback(self.configuration.ports.grpc)).await?;
        let (send_request, mut connection) = h2::client::handshake(stream)
            .await
            .map_err(io::Error::other)?;
        let mut ping_pong = connection
            .ping_pong()
            .ok_or_else(|| io::Error::other("the HTTP/2 connection yielded no ping handle"))?;
        let connection = AbortOnDropHandle::new(tokio::spawn(async move {
            connection.await.discarded(
                "the scenario observes the server process, not the connection it holds open",
            );
        }));
        let mut send_request = send_request.ready().await.map_err(io::Error::other)?;
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!(
                "http://{}{UPLOAD_RESOURCE_PATH}",
                loopback(self.configuration.ports.grpc)
            ))
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header(http::header::TE, "trailers")
            .header(http::header::AUTHORIZATION, test_basic_authorization())
            .body(())
            .map_err(io::Error::other)?;
        let (response, mut request_body) = send_request
            .send_request(request, false)
            .map_err(io::Error::other)?;
        let body = match progress {
            HeldUploadProgress::AwaitingFirstMessage => {
                // A connection processes its frames in order, so the server's answer to this ping
                // proves it has already accepted the upload stream opened before it.
                ping_pong
                    .ping(h2::Ping::opaque())
                    .await
                    .map_err(io::Error::other)?;
                HeldUploadBody::Idle {
                    _stream: request_body,
                }
            }
            HeldUploadProgress::AwaitingNextChunk => {
                request_body
                    .send_data(upload_start_frame(domain, resource)?, false)
                    .map_err(io::Error::other)?;
                request_body
                    .send_data(upload_chunk_frame()?, false)
                    .map_err(io::Error::other)?;
                self.wait_for_staged_upload_archive().await?;
                HeldUploadBody::Idle {
                    _stream: request_body,
                }
            }
            HeldUploadProgress::SendingSlowly => {
                request_body
                    .send_data(upload_start_frame(domain, resource)?, false)
                    .map_err(io::Error::other)?;
                self.wait_for_staged_upload_archive().await?;
                let sender =
                    tokio::spawn(trickle_upload_chunks(request_body, upload_chunk_frame()?));
                HeldUploadBody::Trickling {
                    _sender: AbortOnDropHandle::new(sender),
                }
            }
        };
        Ok(HeldResourceUpload {
            _body: body,
            _response: response,
            _connection: connection,
        })
    }

    /// Waits until the server has staged an upload archive, which it creates only after it read
    /// and accepted the upload's first message.
    async fn wait_for_staged_upload_archive(&mut self) -> io::Result<()> {
        let staged = timeout(LOG_TIMEOUT, self.poll_for_staged_upload_archive()).await;
        match staged {
            Ok(result) => result,
            Err(_) => Err(io::Error::other(format!(
                "nervix-server did not stage an upload archive within {}\n{}",
                humantime::format_duration(LOG_TIMEOUT),
                self.log_tail()
            ))),
        }
    }

    async fn poll_for_staged_upload_archive(&mut self) -> io::Result<()> {
        let resources = self.root.path().join("db").join("resources");
        loop {
            tokio::task::consume_budget().await;
            if let Some(status) = self.observe_exit()? {
                return Err(io::Error::other(format!(
                    "nervix-server exited with {} before it staged an upload archive\n{}",
                    describe_exit(status),
                    self.log_tail()
                )));
            }
            if staged_upload_archive_exists(&resources)? {
                return Ok(());
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    pub(crate) fn grpc_uri(&self) -> String {
        format!("http://{}", loopback(self.configuration.ports.grpc))
    }

    pub(crate) fn observability_uri(&self, path: &str) -> String {
        format!(
            "http://{}{path}",
            loopback(self.configuration.ports.observability)
        )
    }

    pub(crate) fn web_console_websocket_uri(&self) -> String {
        format!(
            "ws://{}/console/ws",
            loopback(self.configuration.ports.web_console)
        )
    }

    pub(crate) fn process_id(&self) -> io::Result<u32> {
        self.child
            .id()
            .ok_or_else(|| io::Error::other("nervix-server process has already been reaped"))
    }
}

#[derive(Clone, Debug, Default)]
struct HttpLoadObservation {
    admitted: u64,
    failure: Option<String>,
}

/// HTTP traffic that remains active until its process exits or the scenario drops it.
pub(crate) struct ServerProcessHttpLoad {
    cancellation: CancellationToken,
    observation: watch::Receiver<HttpLoadObservation>,
    _task: AbortOnDropHandle<()>,
}

impl std::fmt::Debug for ServerProcessHttpLoad {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerProcessHttpLoad")
            .field("observation", &*self.observation.borrow())
            .finish_non_exhaustive()
    }
}

impl ServerProcessHttpLoad {
    pub(crate) async fn wait_for_admissions(&mut self, expected: u64) -> io::Result<()> {
        let observed = timeout(LOG_TIMEOUT, self.poll_for_admissions(expected)).await;
        match observed {
            Ok(result) => result,
            Err(_) => Err(io::Error::other(format!(
                "server process admitted {} of {expected} HTTP load payloads within {}",
                self.observation.borrow().admitted,
                humantime::format_duration(LOG_TIMEOUT),
            ))),
        }
    }

    async fn poll_for_admissions(&mut self, expected: u64) -> io::Result<()> {
        loop {
            tokio::task::consume_budget().await;
            let current = self.observation.borrow().clone();
            if current.admitted >= expected {
                return Ok(());
            }
            if let Some(failure) = current.failure {
                return Err(io::Error::other(format!(
                    "server process HTTP load stopped after {} admissions: {failure}",
                    current.admitted
                )));
            }
            self.observation.changed().await.map_err(|_| {
                io::Error::other("server process HTTP load ended without a terminal observation")
            })?;
        }
    }
}

impl Drop for ServerProcessHttpLoad {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

async fn run_http_load(
    cancellation: CancellationToken,
    observation: watch::Sender<HttpLoadObservation>,
    uri: String,
    host: String,
    first_id: u64,
    payload_templates: Vec<String>,
) {
    let mut admitted = 0_u64;
    let mut load_id = first_id;
    let mut template_index = 0_usize;
    loop {
        tokio::task::consume_budget().await;
        let payload_template = payload_templates
            .get(template_index)
            .assured("the template index is kept below the non-empty template list length");
        let payload = payload_template.replace("{{load_id}}", &load_id.to_string());
        let published = tokio::select! {
            () = cancellation.cancelled() => return,
            result = publish_http_uri_with_headers(
                uri.clone(),
                &host,
                payload.as_bytes(),
                "application/json",
                &[],
            ) => result,
        };
        if let Err(error) = published {
            observation.send_replace(HttpLoadObservation {
                admitted,
                failure: Some(error.to_string()),
            });
            return;
        }

        let Some(next_admitted) = admitted.checked_add(1) else {
            observation.send_replace(HttpLoadObservation {
                admitted,
                failure: Some("HTTP load admission count overflowed u64".to_string()),
            });
            return;
        };
        admitted = next_admitted;
        observation.send_replace(HttpLoadObservation {
            admitted,
            failure: None,
        });

        let Some(next_load_id) = load_id.checked_add(1) else {
            observation.send_replace(HttpLoadObservation {
                admitted,
                failure: Some("HTTP load identity overflowed u64".to_string()),
            });
            return;
        };
        load_id = next_load_id;
        let Some(next_template_index) = template_index.checked_add(1) else {
            observation.send_replace(HttpLoadObservation {
                admitted,
                failure: Some("HTTP load template index overflowed usize".to_string()),
            });
            return;
        };
        template_index = if next_template_index == payload_templates.len() {
            0
        } else {
            next_template_index
        };

        tokio::select! {
            () = cancellation.cancelled() => return,
            () = sleep(HTTP_LOAD_INTERVAL) => {}
        }
    }
}

/// An accepted upload stream kept open until it is dropped.
pub(crate) struct HeldResourceUpload {
    _body: HeldUploadBody,
    _response: h2::client::ResponseFuture,
    _connection: AbortOnDropHandle<()>,
}

/// The client half of a held upload's request stream.
enum HeldUploadBody {
    /// The stream stays open and sends nothing more.
    Idle { _stream: h2::SendStream<Bytes> },
    /// A task keeps sending chunks until the upload is dropped.
    Trickling { _sender: AbortOnDropHandle<()> },
}

/// Sends one chunk per interval until the server closes the stream or the upload is dropped.
async fn trickle_upload_chunks(mut body: h2::SendStream<Bytes>, chunk: Bytes) {
    loop {
        tokio::task::consume_budget().await;
        sleep(SLOW_UPLOAD_CHUNK_INTERVAL).await;
        if body.send_data(chunk.clone(), false).is_err() {
            return;
        }
    }
}

fn staged_upload_archive_exists(resources: &Path) -> io::Result<bool> {
    let entries = match std::fs::read_dir(resources) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".archive-") && name.ends_with(".staging") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn upload_start_frame(domain: &str, resource: &str) -> io::Result<Bytes> {
    grpc_message_frame(&UploadResourceRequest {
        event: Some(upload_resource_request::Event::Start(UploadResourceStart {
            name: resource.to_string(),
            total_bytes: HELD_UPLOAD_DECLARED_BYTES,
            domain: domain.to_string(),
            upload_identity: HELD_UPLOAD_IDENTITY.to_string(),
        })),
    })
}

fn upload_chunk_frame() -> io::Result<Bytes> {
    grpc_message_frame(&UploadResourceRequest {
        event: Some(upload_resource_request::Event::Chunk(vec![0].into())),
    })
}

/// Frames one message the way gRPC carries it on an HTTP/2 stream.
fn grpc_message_frame(message: &UploadResourceRequest) -> io::Result<Bytes> {
    let encoded = message.encode_to_vec();
    let length = u32::try_from(encoded.len()).map_err(io::Error::other)?;
    let capacity = GRPC_MESSAGE_PREFIX_BYTES
        .checked_add(encoded.len())
        .ok_or_else(|| io::Error::other("an upload message frame does not fit in memory"))?;
    let mut frame = BytesMut::with_capacity(capacity);
    frame.put_u8(UNCOMPRESSED_GRPC_MESSAGE);
    frame.put_u32(length);
    frame.put_slice(&encoded);
    Ok(frame.freeze())
}

pub(crate) fn describe_exit(status: ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }
    if let Some(signal) = status.signal() {
        return format!("termination by signal {signal}");
    }
    status.to_string()
}

fn loopback(port: u16) -> String {
    format!("127.0.0.1:{port}")
}
