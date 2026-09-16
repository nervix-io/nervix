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
    fs::{File, OpenOptions},
    io,
    os::unix::process::ExitStatusExt as _,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use bytes::{BufMut as _, Bytes, BytesMut};
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
    time::{sleep, timeout},
};
use tokio_util::task::AbortOnDropHandle;

use super::cluster::{
    InterconnectTestCa, TEST_AUTH_PASSWORD, TEST_AUTH_USERNAME, TestCertificateValidity,
    TestSession, next_port, open_raw_session, publish_http_uri_with_headers, release_test_ports,
    run_command_via_client, server_accepts_commands, test_basic_authorization,
};

/// The identity the process runs as and its certificate names. Each process forms its own
/// single-node cluster, so one identity serves every process.
const CLUSTER_ID: &str = "cucumber";
const NODE_ID: &str = "node-1";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
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
    /// `--shutdown-timeout`.
    ShutdownTimeout(Duration),
}

impl ServerProcessOption {
    fn apply_to(self, command: &mut Command) {
        match self {
            Self::DrainTimeout(timeout) => {
                command
                    .arg("--drain-timeout")
                    .arg(humantime::format_duration(timeout).to_string());
            }
            Self::ShutdownTimeout(timeout) => {
                command
                    .arg("--shutdown-timeout")
                    .arg(humantime::format_duration(timeout).to_string());
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

/// A started `nervix-server` process and everything it was given.
///
/// Dropping it kills the process, so a failed scenario never leaves a server running.
#[derive(Debug)]
pub(crate) struct ServerProcess {
    child: Child,
    exit_status: Option<ExitStatus>,
    ports: ServerProcessPorts,
    root: TempDir,
    launch: ServerProcessLaunch,
    options: Vec<ServerProcessOption>,
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
        certificate_authority.issue_node_with_identity(
            CLUSTER_ID,
            NODE_ID,
            TestCertificateValidity::Current,
            root.path(),
        )?;
        std::fs::create_dir_all(root.path().join("temp"))?;
        let ports = ServerProcessPorts::allocate()?;
        let child = spawn_server_process(&launch, options, root.path(), &ports, false)?;

        Ok(Self {
            child,
            exit_status: None,
            ports,
            root,
            launch,
            options: options.to_vec(),
        })
    }

    /// Waits until the server answers an authenticated command, which also proves that its
    /// configured default user exists.
    pub(crate) async fn wait_until_ready(&mut self) -> io::Result<()> {
        let ready = timeout(STARTUP_TIMEOUT, self.poll_until_ready()).await;
        match ready {
            Ok(result) => result,
            Err(_) => Err(io::Error::other(format!(
                "nervix-server did not accept commands within {}\n{}",
                humantime::format_duration(STARTUP_TIMEOUT),
                self.log_tail()
            ))),
        }
    }

    async fn poll_until_ready(&mut self) -> io::Result<()> {
        let grpc_uri = self.grpc_uri();
        loop {
            tokio::task::consume_budget().await;
            if let Some(status) = self.observe_exit()? {
                return Err(io::Error::other(format!(
                    "nervix-server exited during startup with {}\n{}",
                    describe_exit(status),
                    self.log_tail()
                )));
            }
            if server_accepts_commands(&grpc_uri).await? {
                return Ok(());
            }
            sleep(POLL_INTERVAL).await;
        }
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
        let uri = format!("http://{}{path}", loopback(self.ports.http));
        publish_http_uri_with_headers(uri, host, payload.as_bytes(), "application/json", &[]).await
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

    /// Starts the same executable again with the same ports, credentials and persisted database.
    pub(crate) async fn restart(&mut self) -> io::Result<()> {
        if self.observe_exit()?.is_none() {
            return Err(io::Error::other(
                "cannot restart nervix-server while its previous process is still running",
            ));
        }
        self.child = spawn_server_process(
            &self.launch,
            &self.options,
            self.root.path(),
            &self.ports,
            true,
        )?;
        self.exit_status = None;
        self.wait_until_ready().await
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

    /// Everything the process has written to standard output and standard error so far.
    pub(crate) fn log(&self) -> io::Result<String> {
        let bytes = std::fs::read(self.root.path().join("server.log"))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
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
        let stream = TcpStream::connect(loopback(self.ports.grpc)).await?;
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
                loopback(self.ports.grpc)
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
        format!("http://{}", loopback(self.ports.grpc))
    }

    pub(crate) fn observability_uri(&self, path: &str) -> String {
        format!("http://{}{path}", loopback(self.ports.observability))
    }

    pub(crate) fn web_console_websocket_uri(&self) -> String {
        format!("ws://{}/console/ws", loopback(self.ports.web_console))
    }

    pub(crate) fn process_id(&self) -> io::Result<u32> {
        self.child
            .id()
            .ok_or_else(|| io::Error::other("nervix-server process has already been reaped"))
    }
}

fn spawn_server_process(
    launch: &ServerProcessLaunch,
    options: &[ServerProcessOption],
    root: &Path,
    ports: &ServerProcessPorts,
    append_log: bool,
) -> io::Result<Child> {
    let log_path = root.join("server.log");
    let log = if append_log {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?
    } else {
        File::create(log_path)?
    };
    let error_log = log.try_clone()?;
    let mut command = launch.command();
    command
        .arg("--addr")
        .arg(loopback(ports.grpc))
        .arg("--grpc-advertise-addr")
        .arg(loopback(ports.grpc))
        .arg("--http-listen-addr")
        .arg(loopback(ports.http))
        .arg("--https-listen-addr")
        .arg(loopback(ports.https))
        .arg("--observability-listen-addr")
        .arg(loopback(ports.observability))
        .arg("--web-console-listen-addr")
        .arg(loopback(ports.web_console))
        .arg("--cluster-id")
        .arg(CLUSTER_ID)
        .arg("--node-id")
        .arg(NODE_ID)
        .arg("--interconnect-listen-addr")
        .arg(loopback(ports.interconnect))
        .arg("--interconnect-advertise-addr")
        .arg(loopback(ports.interconnect))
        .arg("--interconnect-tls-ca")
        .arg(root.join("interconnect-ca.pem"))
        .arg("--interconnect-tls-cert")
        .arg(root.join("interconnect.pem"))
        .arg("--interconnect-tls-key")
        .arg(root.join("interconnect-key.pem"))
        .arg("--allow-bootstrap")
        .arg("--default-user")
        .arg(TEST_AUTH_USERNAME)
        .arg("--init-default-user-password")
        .arg(TEST_AUTH_PASSWORD)
        .arg("--db-path")
        .arg(root.join("db"))
        .arg("--temp-dir")
        .arg(root.join("temp"))
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(error_log))
        .kill_on_drop(true);
    for option in options {
        option.apply_to(&mut command);
    }
    // Any `NERVIX_*` variable the scenario runner carries would silently reconfigure the server,
    // and `RUST_LOG` would replace the log filter the server ships with.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("NERVIX_") {
            command.env_remove(&name);
        }
    }
    command.env_remove("RUST_LOG");
    command.spawn()
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
