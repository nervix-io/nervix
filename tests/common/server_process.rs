//! A `nervix-server` executable started as a real operating-system process.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One isolated server process: its ports, database directory, interconnect
//!   credentials, captured log, the signals delivered to it, and the status it exits with.
//! - **Depends on.** The executable Cargo builds beside this test target, the shared test port
//!   reservations, and the test interconnect certificate authority.
//! - **Must not know.** Server internals. Everything it observes crosses the process boundary: the
//!   gRPC API, HTTP/2 frames, the log the process writes, and how the process exits.
//!
//! The in-process cluster fixture runs nodes as Tokio tasks and stops them through their shutdown
//! coordinator, so it cannot show whether the process boundary delivers a signal to that
//! coordinator at all. Scenarios about signals, exit statuses and process startup use this fixture
//! instead.

use std::{
    fs::File,
    io,
    os::unix::process::ExitStatusExt as _,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use bytes::Bytes;
use nervix_recovery::Discarded as _;
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use tempfile::TempDir;
use tokio::{
    net::TcpStream,
    process::{Child, Command},
    time::{sleep, timeout},
};
use tokio_util::task::AbortOnDropHandle;

use super::cluster::{
    InterconnectTestCa, TEST_AUTH_PASSWORD, TEST_AUTH_USERNAME, TestCertificateValidity, next_port,
    release_test_ports, server_accepts_commands, test_basic_authorization,
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

/// How a scenario executes the server binary.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ServerProcessLaunch {
    /// Execute the binary directly, the way the container image's exec-form command does.
    Direct,
    /// Lower the soft and hard open-file limits before executing the binary. A shell applies the
    /// limit and then replaces itself with the server, so the server keeps the shell's process.
    OpenFileLimit(u32),
}

impl ServerProcessLaunch {
    fn command(self) -> Command {
        let executable = Path::new(env!("CARGO_BIN_EXE_nervix-server"));
        match self {
            Self::Direct => Command::new(executable),
            Self::OpenFileLimit(limit) => {
                let mut command = Command::new("bash");
                command
                    .arg("-c")
                    // The soft limit goes first: a hard limit below the current soft limit is
                    // rejected, and bash would exit on its own without running the server.
                    .arg(r#"ulimit -Sn "$1" && ulimit -Hn "$1" && shift && exec "$@""#)
                    .arg("bash")
                    .arg(limit.to_string())
                    .arg(executable);
                command
            }
        }
    }
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
    log_path: PathBuf,
    _root: TempDir,
}

impl ServerProcess {
    pub(crate) fn start(launch: ServerProcessLaunch) -> io::Result<Self> {
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
        let ports = ServerProcessPorts::allocate()?;
        let log_path = root.path().join("server.log");
        let log = File::create(&log_path)?;
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
            .arg(&certificate_authority.path)
            .arg("--interconnect-tls-cert")
            .arg(&certificate)
            .arg("--interconnect-tls-key")
            .arg(&private_key)
            .arg("--allow-bootstrap")
            .arg("--default-user")
            .arg(TEST_AUTH_USERNAME)
            .arg("--init-default-user-password")
            .arg(TEST_AUTH_PASSWORD)
            .arg("--db-path")
            .arg(root.path().join("db"))
            .arg("--temp-dir")
            .arg(&temp_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(error_log))
            .kill_on_drop(true);
        // Any `NERVIX_*` variable the scenario runner carries would silently reconfigure the
        // server, and `RUST_LOG` would replace the log filter the server ships with.
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("NERVIX_") {
                command.env_remove(&name);
            }
        }
        command.env_remove("RUST_LOG");
        let child = command.spawn()?;

        Ok(Self {
            child,
            exit_status: None,
            ports,
            log_path,
            _root: root,
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
        let grpc_uri = format!("http://{}", loopback(self.ports.grpc));
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

    /// Everything the process has written to standard output and standard error so far.
    pub(crate) fn log(&self) -> io::Result<String> {
        let bytes = std::fs::read(&self.log_path)?;
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

    /// Opens an authenticated `UploadResource` stream whose request body never arrives.
    ///
    /// The server's handler waits for the first upload message, and graceful shutdown of the gRPC
    /// listener waits for that handler, so the stream holds shutdown open for as long as the
    /// returned value lives.
    pub(crate) async fn hold_resource_upload(&self) -> io::Result<HeldResourceUpload> {
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
        let (response, request_body) = send_request
            .send_request(request, false)
            .map_err(io::Error::other)?;
        // A connection processes its frames in order, so the server's answer to this ping proves
        // it has already accepted the upload stream opened before it.
        ping_pong
            .ping(h2::Ping::opaque())
            .await
            .map_err(io::Error::other)?;
        Ok(HeldResourceUpload {
            _request_body: request_body,
            _response: response,
            _connection: connection,
        })
    }
}

/// An accepted upload stream kept open until it is dropped.
pub(crate) struct HeldResourceUpload {
    _request_body: h2::SendStream<Bytes>,
    _response: h2::client::ResponseFuture,
    _connection: AbortOnDropHandle<()>,
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
