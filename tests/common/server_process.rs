//! A `nervix-server` executable started as a real operating-system process.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One isolated server process: its ports, database directory, interconnect
//!   credentials, command-line options, captured log, the startup of each launch, the relaunch on
//!   fresh ports of a launch another socket kept from binding a drawn port, and the description of
//!   a startup that fails, the sockets a scenario has hold its ports, the signals delivered to it,
//!   the clients a scenario holds open against it, and the status it exits with.
//! - **Depends on.** This test executable, which a launch runs again under the server's name, and
//!   the read of it before each launch, the shared test port reservations, the test interconnect
//!   certificate authority,
//!   the session client, the readiness history, the observation of a running process, the report a
//!   process writes when its startup fails, and the node-startup classification and launch limit.
//! - **Must not know.** Server internals. Everything it observes crosses the process boundary: the
//!   gRPC API, HTTP/2 frames, the files the process writes, the log the process writes, and how
//!   the process exits.
//!
//! The in-process cluster fixture runs nodes as Tokio tasks and stops them through their shutdown
//! coordinator, so it cannot show whether the process boundary delivers a signal to that
//! coordinator at all. Scenarios about signals, exit statuses and process startup use this fixture
//! instead.

use std::{
    fmt,
    fs::OpenOptions,
    future, io,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU64,
    os::unix::process::ExitStatusExt as _,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use bytes::{BufMut as _, Bytes, BytesMut};
use meticulous::OptionExt as _;
use nervix_client_wire::{
    RequestId, SessionLimits, UploadChunk, UploadStart, grpc::UPLOAD_RESOURCE_PATH,
};
use nervix_dns::{DnsConfiguration, NameServers};
use nervix_models::{DomainName, ResourceName, ResourceUploadIdentity};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{CancellationToken, watch},
    task::AbortOnDropHandle,
    time::{sleep, timeout},
};
use nervix_recovery::Discarded as _;
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use tempfile::TempDir;
use tokio::process::{Child, Command};

use super::{
    cluster::{
        InterconnectTestCa, TEST_AUTH_PASSWORD, TEST_AUTH_USERNAME, TestCertificateValidity,
        TestSession, open_raw_session, publish_http_uri_with_headers, run_command_via_client,
        test_basic_authorization,
    },
    node_liveness::{ReadinessHistory, ReadinessProbeOutcome},
    node_startup::{NODE_START_ATTEMPTS, StartupPorts},
    phase_deadline::PhaseDeadline,
    port_pool::{next_port, next_ports, release_test_ports},
    process_observation::ProcessObservation,
    process_program::{SERVER_PROGRAM, scenario_executable, server_command},
    process_report::reported_context,
    server_executable::{PreparedExecutable, ServerExecutable},
    status_request::{
        STATUS_REQUEST_TIMEOUT, STATUS_REQUESTS_PER_STARTUP, StatusEndpoint, StatusTransport,
    },
};

/// The identity the process runs as and its certificate names. Each process forms its own
/// single-node cluster, so one identity serves every process.
const CLUSTER_ID: &str = "cucumber";
const NODE_ID: &str = "node-1";
/// How long one launch has, from its start until the process accepts commands. Reading the
/// executable, spawning the process and waiting for its readiness all share it.
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
    /// Execute this test executable as the server, directly, the way the container image's
    /// exec-form command executes the server.
    Direct,
    /// Execute a server binary a recipe built and named, such as the release subject of a
    /// benchmark.
    Executable(PathBuf),
    /// Lower the soft and hard open-file limits before executing the binary. A shell applies the
    /// limit and then replaces itself with the server, so the server keeps the shell's process.
    OpenFileLimit(u32),
}

impl ServerProcessLaunch {
    /// The file the launch runs. A launch that lowers the open-file limit starts a shell that
    /// replaces itself with this test executable, so that is the one it runs.
    fn executable_path(&self) -> PathBuf {
        match self {
            Self::Direct | Self::OpenFileLimit(_) => scenario_executable(),
            Self::Executable(executable) => executable.clone(),
        }
    }

    fn command(&self) -> Command {
        match self {
            Self::Direct => server_command(),
            Self::Executable(executable) => Command::new(executable),
            Self::OpenFileLimit(limit) => {
                let mut command = Command::new("bash");
                command
                    .arg("-c")
                    // The soft limit goes first: a hard limit below the current soft limit is
                    // rejected, and bash would exit on its own without running the server. The
                    // shell gives the process the server's name, as `server_command` does.
                    .arg(r#"ulimit -Sn "$1" && ulimit -Hn "$1" && exec -a "$2" "${@:3}""#)
                    .arg("bash")
                    .arg(limit.to_string())
                    .arg(SERVER_PROGRAM)
                    .arg(scenario_executable());
                command
            }
        }
    }
}

/// A setting a scenario gives the server process in addition to the fixture's own.
#[derive(Clone, Debug)]
pub(crate) enum ServerProcessOption {
    /// The files and authority the node's resolver loads.
    Dns(DnsConfiguration),
    /// Enable the node's own exporter with a distinct service identity for each process.
    TraceExport { endpoint: String, service: String },
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
    /// A command-line option with its value written exactly as the scenario gives it, whether or
    /// not the server can read it.
    Written { option: String, value: String },
    /// An environment variable set exactly as the scenario gives it, whether or not the server
    /// can read it.
    Environment { variable: String, value: String },
}

impl ServerProcessOption {
    fn apply_to(&self, command: &mut Command, node_id: &str) {
        match self {
            Self::Dns(configuration) => {
                command
                    .arg("--dns-resolver-config")
                    .arg(&configuration.resolver_configuration)
                    .arg("--dns-hosts-file")
                    .arg(&configuration.hosts_file);
                if let NameServers::Explicit(servers) = &configuration.name_servers {
                    for server in servers {
                        command.arg("--dns-name-server").arg(server.to_string());
                    }
                }
            }
            Self::TraceExport { endpoint, service } => {
                command
                    .arg("--otel-enabled")
                    .arg("--otel-otlp-endpoint")
                    .arg(endpoint)
                    .arg("--otel-service-name")
                    .arg(format!("{service}-{node_id}"));
            }
            Self::DrainTimeout(timeout) => {
                command
                    .arg("--drain-timeout")
                    .arg(humantime::format_duration(*timeout).to_string());
            }
            Self::StateSnapshotInterval(interval) => {
                command
                    .arg("--state-snapshot-interval")
                    .arg(humantime::format_duration(*interval).to_string());
            }
            Self::ShutdownTimeout(timeout) => {
                command
                    .arg("--shutdown-timeout")
                    .arg(humantime::format_duration(*timeout).to_string());
            }
            Self::TransactionIdleTimeout(timeout) => {
                command
                    .arg("--transaction-idle-timeout")
                    .arg(humantime::format_duration(*timeout).to_string());
            }
            Self::TransactionTombstoneRetention(retention) => {
                command
                    .arg("--transaction-tombstone-retention")
                    .arg(humantime::format_duration(*retention).to_string());
            }
            Self::Written { option, value } => {
                command.arg(option).arg(value);
            }
            Self::Environment { variable, value } => {
                command.env(variable, value);
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
    /// The ports a relaunch moved the process off. They stay reserved until the fixture is
    /// dropped, with the ports it binds then: a peer or a client may still dial an address the
    /// process published before a restart, and dropping the fixture ends the scenario that pointed
    /// them at it.
    given_up: Vec<u16>,
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
            given_up: Vec::new(),
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

    /// The listeners `selection` names.
    fn selected(&mut self, selection: StartupPorts) -> Vec<&mut u16> {
        match selection {
            StartupPorts::All => vec![
                &mut self.grpc,
                &mut self.http,
                &mut self.https,
                &mut self.observability,
                &mut self.web_console,
                &mut self.interconnect,
            ],
            StartupPorts::Grpc => vec![&mut self.grpc],
            StartupPorts::Http => vec![&mut self.http],
            StartupPorts::Https => vec![&mut self.https],
            StartupPorts::Observability => vec![&mut self.observability],
            StartupPorts::WebConsole => vec![&mut self.web_console],
            StartupPorts::Interconnect => vec![&mut self.interconnect],
        }
    }

    /// The ports the listeners `selection` names bind.
    fn ports(&mut self, selection: StartupPorts) -> Vec<u16> {
        self.selected(selection)
            .into_iter()
            .map(|port| *port)
            .collect()
    }

    /// Moves the listeners `selection` names to freshly drawn ports, keeping the ports they leave
    /// reserved until the fixture is dropped.
    fn move_to_fresh(&mut self, selection: StartupPorts) -> io::Result<()> {
        let listeners = self.selected(selection);
        let replacements = next_ports(listeners.len())?;
        let mut given_up = Vec::with_capacity(listeners.len());
        for (listener, replacement) in listeners.into_iter().zip(replacements) {
            given_up.push(*listener);
            *listener = replacement;
        }
        self.given_up.extend(given_up);
        Ok(())
    }
}

impl Drop for ServerProcessPorts {
    fn drop(&mut self) {
        release_test_ports(&self.all());
        release_test_ports(&self.given_up);
    }
}

/// Ports another socket holds so the process cannot bind them, the way an unrelated socket on a
/// busy runner can take a port the harness drew before the process binds it. A scenario holds them
/// to cause that on purpose.
struct HeldPorts {
    selection: StartupPorts,
    ports: Vec<u16>,
    /// One task per port, each owning a listener on it that closes every connection it accepts, so
    /// a readiness probe that dials a held port fails at once instead of waiting out its request
    /// deadline. Dropping a task closes its listener.
    _holders: Vec<AbortOnDropHandle<()>>,
}

impl fmt::Debug for HeldPorts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HeldPorts")
            .field("selection", &self.selection)
            .field("ports", &self.ports)
            .finish_non_exhaustive()
    }
}

impl HeldPorts {
    /// Holds `ports`, which the listeners `selection` names bind, until the result is dropped.
    async fn hold(selection: StartupPorts, ports: Vec<u16>) -> io::Result<Self> {
        let mut holders = Vec::with_capacity(ports.len());
        for port in &ports {
            nervix_primitives::task::consume_budget().await;
            // A Unix listener sets SO_REUSEADDR, so it takes a port whose connections a killed
            // process left in TIME_WAIT, and it listens, which refuses the port to a process
            // binding it whatever options that process binds with.
            let listener =
                TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, *port))).await?;
            let holder = nervix_primitives::task::spawn(close_every_connection(listener));
            holders.push(AbortOnDropHandle::new(holder));
        }
        Ok(Self {
            selection,
            ports,
            _holders: holders,
        })
    }
}

/// Accepts every connection `listener` receives and closes it at once.
async fn close_every_connection(listener: TcpListener) {
    loop {
        nervix_primitives::task::consume_budget().await;
        let Ok((connection, _)) = listener.accept().await else {
            // A listener that can no longer accept still holds its port, which is all a hold
            // needs; a probe that dials it then waits out its request deadline instead.
            future::pending::<()>().await;
            return;
        };
        drop(connection);
    }
}

/// The stable launch configuration retained when a scenario restarts the same server.
#[derive(Debug)]
struct ServerProcessConfiguration {
    launch: ServerProcessLaunch,
    options: Vec<ServerProcessOption>,
    ports: ServerProcessPorts,
    node_id: String,
    bootstrap_host: Option<String>,
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
            .arg(&self.node_id)
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
        if let Some(bootstrap_host) = &self.bootstrap_host {
            command.arg("--cluster-bootstrap-host").arg(bootstrap_host);
        } else {
            command.arg("--allow-bootstrap");
        }
        // Any `NERVIX_*` variable the scenario runner carries would silently reconfigure the
        // server, and `RUST_LOG` would replace the log filter the server ships with.
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("NERVIX_") {
                command.env_remove(&name);
            }
        }
        command.env_remove("RUST_LOG");
        // Options go last, so a variable a scenario sets survives the removal above.
        for option in &self.options {
            option.apply_to(&mut command, &self.node_id);
        }
        command.spawn()
    }

    /// Starts one launch: reads the executable the launch runs, in file order, and then spawns the
    /// process. `deadline` is the startup's, which started before its first launch read the
    /// executable, so reading the executable, spawning the process and waiting until it accepts
    /// commands all share it, across every launch of the startup.
    async fn launch(&self, root: &Path, deadline: PhaseDeadline) -> io::Result<Launched> {
        let executable = ServerExecutable::new(self.launch.executable_path());
        let prepared = executable.prepare(deadline).await.map_err(|error| {
            io::Error::other(format!("nervix-server could not be launched: {error:#}"))
        })?;
        let child = self.spawn(root)?;
        Ok(Launched {
            child,
            executable: prepared,
        })
    }
}

/// The startup of a server process: its first launch and every launch it relaunched.
#[derive(Debug)]
struct LaunchStartup {
    /// The deadline the startup must accept commands by. It started before the first launch read
    /// the executable, and every relaunch shares it.
    deadline: PhaseDeadline,
    /// What reading the executable before the current launch did.
    executable: PreparedExecutable,
    /// Which launch of the startup is current, counted from one.
    launch: u32,
    /// The launches of this startup that ended because another socket held a port they were to
    /// bind, in the order they ran.
    lost_binds: Vec<LostBindLaunch>,
}

impl LaunchStartup {
    /// The startup a first launch begins.
    fn first(deadline: PhaseDeadline, executable: PreparedExecutable) -> Self {
        Self {
            deadline,
            executable,
            launch: 1,
            lost_binds: Vec::new(),
        }
    }
}

/// A process one launch spawned, and what reading its executable before the spawn did.
struct Launched {
    child: Child,
    executable: PreparedExecutable,
}

/// How a launch whose process exited before it accepted commands ended.
struct LaunchEnding {
    status: ExitStatus,
    /// The readiness probes of the launch until its process exited.
    readiness: ReadinessHistory,
    retry: LaunchRetry,
}

/// Whether launching the process again could change how a launch ended.
enum LaunchRetry {
    /// The process's report names a listener another process bound first, which a launch on
    /// fresh ports for that listener can change.
    LostBind {
        /// The report's outermost context.
        reported: String,
        /// The listeners the next launch moves to fresh ports.
        ports: StartupPorts,
    },
    /// The process decided its ending from its configuration, its stored state or its own code,
    /// or wrote no report that says otherwise, so the next launch repeats it.
    Terminal,
}

impl LaunchRetry {
    /// How the report `log` holds classifies the launch that wrote it.
    fn reported_in(log: &str) -> Self {
        let Some(reported) = reported_context(log) else {
            return Self::Terminal;
        };
        match StartupPorts::lost_in_report(&reported) {
            Some(ports) => Self::LostBind { reported, ports },
            None => Self::Terminal,
        }
    }
}

/// A launch that exited because another socket held a port it was to bind, and the listeners the
/// next launch of the same startup moved to fresh ports.
#[derive(Debug)]
struct LostBindLaunch {
    /// Which launch of the startup it was, counted from one.
    launch: u32,
    /// How much of the startup's deadline had passed when the harness saw the process exit.
    elapsed: Duration,
    status: ExitStatus,
    /// The outermost context of the report the process wrote as it exited.
    reported: String,
    /// The listeners the next launch moved to fresh ports.
    moved: StartupPorts,
}

impl fmt::Display for LostBindLaunch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "launch {}/{NODE_START_ATTEMPTS} [transient] exited {:?} into its {} deadline with \
             {}: {}; the next launch moves its {} listener to a fresh port",
            self.launch,
            self.elapsed,
            humantime::format_duration(STARTUP_TIMEOUT),
            describe_exit(self.status),
            self.reported,
            self.moved
        )
    }
}

/// A started `nervix-server` process and everything it was given.
///
/// Dropping it kills the process, so a failed scenario never leaves a server running.
#[derive(Debug)]
pub(crate) struct ServerProcess {
    child: Child,
    exit_status: Option<ExitStatus>,
    /// The ports another socket holds so the process cannot bind them, when a scenario had any
    /// held.
    held: Option<HeldPorts>,
    configuration: ServerProcessConfiguration,
    /// The current startup: its first launch and every launch it relaunched.
    startup: LaunchStartup,
    /// Byte offset where the current process launch began in the shared append-only log.
    log_start: u64,
    root: TempDir,
}

impl ServerProcess {
    pub(crate) async fn start(
        launch: ServerProcessLaunch,
        options: &[ServerProcessOption],
    ) -> io::Result<Self> {
        Self::start_with_held_ports(launch, options, None).await
    }

    /// Starts a process whose listeners `held` names another socket holds from the moment the
    /// harness draws their ports, before the process can bind them.
    pub(crate) async fn start_holding(
        launch: ServerProcessLaunch,
        options: &[ServerProcessOption],
        held: StartupPorts,
    ) -> io::Result<Self> {
        Self::start_with_held_ports(launch, options, Some(held)).await
    }

    async fn start_with_held_ports(
        launch: ServerProcessLaunch,
        options: &[ServerProcessOption],
        held: Option<StartupPorts>,
    ) -> io::Result<Self> {
        let root = tempfile::Builder::new()
            .prefix("nervix-server-process-")
            .tempdir()?;
        let certificate_authority = InterconnectTestCa::new(&root)?;
        Self::start_cluster_member(
            root,
            &certificate_authority,
            NODE_ID,
            None,
            launch,
            options,
            held,
        )
        .await
    }

    /// Starts one independently persisted member under a shared cluster certificate authority.
    /// Another socket holds the ports of the listeners `held` names, when it names any, from the
    /// moment they are drawn.
    pub(crate) async fn start_cluster_member(
        root: TempDir,
        certificate_authority: &InterconnectTestCa,
        node_id: &str,
        bootstrap_host: Option<&str>,
        launch: ServerProcessLaunch,
        options: &[ServerProcessOption],
        held: Option<StartupPorts>,
    ) -> io::Result<Self> {
        let (certificate, private_key) = certificate_authority.issue_node_with_identity(
            CLUSTER_ID,
            node_id,
            TestCertificateValidity::Current,
            root.path(),
        )?;
        let temp_dir = root.path().join("temp");
        std::fs::create_dir_all(&temp_dir)?;
        let mut ports = ServerProcessPorts::allocate()?;
        let held = match held {
            Some(selection) => {
                let held_ports = ports.ports(selection);
                Some(HeldPorts::hold(selection, held_ports).await?)
            }
            None => None,
        };
        let configuration = ServerProcessConfiguration {
            launch,
            options: options.to_vec(),
            ports,
            node_id: node_id.to_string(),
            bootstrap_host: bootstrap_host.map(str::to_string),
            certificate_authority: certificate_authority.path.clone(),
            certificate,
            private_key,
            temp_dir,
        };
        let deadline = PhaseDeadline::after(STARTUP_TIMEOUT);
        let launched = configuration.launch(root.path(), deadline).await?;

        Ok(Self {
            child: launched.child,
            exit_status: None,
            held,
            configuration,
            startup: LaunchStartup::first(deadline, launched.executable),
            log_start: 0,
            root,
        })
    }

    /// Has another socket hold the ports of the listeners `selection` names until the fixture is
    /// dropped, so the next launch cannot bind them. The process must have exited, because a
    /// running process holds its own ports.
    pub(crate) async fn hold_ports(&mut self, selection: StartupPorts) -> io::Result<()> {
        if self.observe_exit()?.is_none() {
            return Err(io::Error::other(
                "cannot hold the ports of a nervix-server that is still running",
            ));
        }
        if self.held.is_some() {
            return Err(io::Error::other(
                "another socket already holds ports of this nervix-server",
            ));
        }
        let ports = self.configuration.ports.ports(selection);
        self.held = Some(HeldPorts::hold(selection, ports).await?);
        Ok(())
    }

    /// Checks that the current startup ended exactly one launch because another socket held the
    /// ports of the listeners `selection` names, that it moved exactly those listeners, and that
    /// the process now binds other ports for them.
    pub(crate) fn verify_one_relaunch_off_held_ports(
        &mut self,
        selection: StartupPorts,
    ) -> io::Result<()> {
        let Some(held) = &self.held else {
            return Err(io::Error::other(
                "no other socket holds a port of this nervix-server",
            ));
        };
        if held.selection != selection {
            return Err(io::Error::other(format!(
                "another socket holds the {} ports of this nervix-server, not its {selection} \
                 ports",
                held.selection
            )));
        }
        let [relaunched] = self.startup.lost_binds.as_slice() else {
            return Err(io::Error::other(format!(
                "the startup ended {} launches because another socket held a port, not one: {:?}",
                self.startup.lost_binds.len(),
                self.startup.lost_binds
            )));
        };
        if relaunched.moved != selection {
            return Err(io::Error::other(format!(
                "the startup moved its {} listeners, not its {selection} listeners: {relaunched}",
                relaunched.moved
            )));
        }
        let current = self.configuration.ports.ports(selection);
        // A selection names at most the six listeners a process binds, so both lists are that
        // short.
        for port in &current {
            if held.ports.contains(port) {
                return Err(io::Error::other(format!(
                    "the {selection} listeners still bind held port {port}: they bind {current:?} \
                     and another socket holds {:?}",
                    held.ports
                )));
            }
        }
        Ok(())
    }

    /// Reopens this process's existing database on the same isolated ports, unless another socket
    /// takes one of them first and a relaunch moves that listener to a fresh port.
    pub(crate) async fn restart(&mut self) -> io::Result<()> {
        self.restart_without_waiting().await?;
        self.wait_until_ready().await
    }

    /// Starts the next process before checking readiness. A whole cluster must bring up its
    /// persisted voters together because one voter alone cannot form the old quorum.
    pub(crate) async fn restart_without_waiting(&mut self) -> io::Result<()> {
        if self.observe_exit()?.is_none() {
            return Err(io::Error::other(
                "cannot restart nervix-server before its preceding process exits",
            ));
        }
        self.log_start = std::fs::metadata(self.root.path().join("server.log"))?.len();
        let deadline = PhaseDeadline::after(STARTUP_TIMEOUT);
        let launched = self
            .configuration
            .launch(self.root.path(), deadline)
            .await?;
        self.child = launched.child;
        self.startup = LaunchStartup::first(deadline, launched.executable);
        self.exit_status = None;
        Ok(())
    }

    /// Waits until the server answers an authenticated command, which also proves that its
    /// configured default user exists. The wait ends at the startup deadline, which started
    /// before the first launch read its executable, and every readiness probe receives only the
    /// time left before it, so a server that never answers ends the wait there.
    ///
    /// A launch whose process exits because another socket held a port it was to bind is launched
    /// again on fresh ports for that listener, within the same deadline and at most
    /// [`NODE_START_ATTEMPTS`] launches in all. Every other exit ends the startup at once.
    ///
    /// A failure describes the whole startup: how reading the executable went, every kind of
    /// answer the probes got and when, every launch it relaunched, and, when the deadline passed,
    /// what the still-running process and its host were doing at that moment, before anything
    /// kills the process.
    pub(crate) async fn wait_until_ready(&mut self) -> io::Result<()> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(ending) = self.wait_for_launch().await? else {
                self.report_ready();
                return Ok(());
            };
            let LaunchEnding {
                status,
                readiness,
                retry,
            } = ending;
            let LaunchRetry::LostBind { reported, ports } = retry else {
                return Err(self.exited_during_startup(status, &readiness));
            };
            if self.startup.launch >= NODE_START_ATTEMPTS || self.startup.deadline.has_passed() {
                return Err(self.exited_during_startup(status, &readiness));
            }
            let lost = LostBindLaunch {
                launch: self.startup.launch,
                elapsed: self.startup.deadline.elapsed(),
                status,
                reported,
                moved: ports,
            };
            self.relaunch_on_fresh_ports(lost).await?;
        }
    }

    /// Waits until the current launch accepts commands, returning nothing, or until its process
    /// exits, returning how the launch ended. A deadline that passes first fails the wait with
    /// what the still-running process and its host were doing.
    async fn wait_for_launch(&mut self) -> io::Result<Option<LaunchEnding>> {
        let deadline = self.startup.deadline;
        // A relaunch can move the gRPC listener, so each launch is probed where it listens.
        let endpoint = self.status_endpoint();
        let mut readiness = ReadinessHistory::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            if let Some(status) = self.observe_exit()? {
                let retry = LaunchRetry::reported_in(&self.log()?);
                return Ok(Some(LaunchEnding {
                    status,
                    readiness,
                    retry,
                }));
            }
            if deadline.has_passed() {
                let observation = ProcessObservation::of(self.process_id()?).await;
                return Err(io::Error::other(format!(
                    "nervix-server did not accept commands within {}; last readiness outcome: \
                     {}\nbefore the process was spawned, the launch {}\nreadiness probes since \
                     the launch began: {readiness}\n{observation}\n{}{}",
                    humantime::format_duration(STARTUP_TIMEOUT),
                    readiness.last(),
                    self.startup.executable,
                    self.relaunched_launches(),
                    self.log_tail()
                )));
            }
            let outcome = ReadinessProbeOutcome::probe(&endpoint, deadline).await;
            if outcome.is_ready() {
                return Ok(None);
            }
            readiness.record(deadline.elapsed(), outcome);
            deadline.pause(POLL_INTERVAL).await;
        }
    }

    /// Moves the listeners a lost bind names to freshly drawn ports and launches the process
    /// again, within the startup's deadline. The process that lost the bind has exited, so it
    /// holds nothing a new launch could meet.
    async fn relaunch_on_fresh_ports(&mut self, lost: LostBindLaunch) -> io::Result<()> {
        if let Err(error) = self.configuration.ports.move_to_fresh(lost.moved) {
            return Err(io::Error::other(format!(
                "fresh ports for the next launch of nervix-server could not be drawn after \
                 {lost}: {error}\n{}{}",
                self.relaunched_launches(),
                self.log_tail()
            )));
        }
        eprintln!(
            "server process '{}' startup {lost}; {:?} of its {} deadline left",
            self.configuration.node_id,
            self.startup.deadline.remaining(),
            humantime::format_duration(STARTUP_TIMEOUT)
        );
        let next_launch = self.startup.launch.checked_add(1).assured(
            "a startup relaunches only while its launch is below the attempt limit, far below \
             u32::MAX",
        );
        self.startup.lost_binds.push(lost);
        self.log_start = std::fs::metadata(self.root.path().join("server.log"))?.len();
        let launched = self
            .configuration
            .launch(self.root.path(), self.startup.deadline)
            .await;
        let launched = match launched {
            Ok(launched) => launched,
            Err(error) => {
                return Err(io::Error::other(format!(
                    "{error}\n{}",
                    self.relaunched_launches()
                )));
            }
        };
        self.child = launched.child;
        self.exit_status = None;
        self.startup.executable = launched.executable;
        self.startup.launch = next_launch;
        Ok(())
    }

    /// Reports a startup that became ready after a relaunch, so a run's relaunches can be read
    /// from its output together with how each startup ended.
    fn report_ready(&self) {
        if self.startup.lost_binds.is_empty() {
            return;
        }
        eprintln!(
            "server process '{}' startup: ready on launch {}/{NODE_START_ATTEMPTS} after {:?}; \
             {:?} of its {} deadline left",
            self.configuration.node_id,
            self.startup.launch,
            self.startup.deadline.elapsed(),
            self.startup.deadline.remaining(),
            humantime::format_duration(STARTUP_TIMEOUT)
        );
    }

    /// The failure of a startup whose current launch exited, describing the whole startup.
    fn exited_during_startup(&self, status: ExitStatus, readiness: &ReadinessHistory) -> io::Error {
        io::Error::other(format!(
            "nervix-server exited during startup with {}; last readiness outcome: {}\nbefore the \
             process was spawned, the launch {}\nreadiness probes since the launch began: \
             {readiness}\n{}{}",
            describe_exit(status),
            readiness.last(),
            self.startup.executable,
            self.relaunched_launches(),
            self.log_tail()
        ))
    }

    /// The launches the current startup relaunched, as one line ending in a newline, or nothing
    /// when it relaunched none.
    fn relaunched_launches(&self) -> String {
        if self.startup.lost_binds.is_empty() {
            return String::new();
        }
        let mut described = String::from(
            "launches this startup relaunched after another socket held a port they were to bind: ",
        );
        let mut separated = false;
        for lost in &self.startup.lost_binds {
            if separated {
                described.push_str("; ");
            }
            described.push_str(&lost.to_string());
            separated = true;
        }
        described.push('\n');
        described
    }

    pub(crate) fn status_endpoint(&self) -> StatusEndpoint {
        StatusEndpoint::new(
            SocketAddr::from((Ipv4Addr::LOCALHOST, self.configuration.ports.grpc)),
            StatusTransport::Plaintext,
            test_basic_authorization(),
        )
    }

    /// Runs Plexus through an authenticated session whose active domain is `domain`.
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
            nervix_primitives::task::consume_budget().await;
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
        let task = nervix_primitives::task::spawn(async move {
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

    /// How many processes the current startup launched: its first launch and every relaunch.
    pub(crate) fn launches(&self) -> u32 {
        self.startup.launch
    }

    fn observe_exit(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.exit_status {
            return Ok(Some(status));
        }
        let status = self.child.try_wait()?;
        self.exit_status = status;
        Ok(status)
    }

    /// Everything every launch of this fixture has written to standard output and standard error.
    #[cfg(feature = "deloxide")]
    pub(crate) fn whole_log(&self) -> io::Result<String> {
        let bytes = std::fs::read(self.root.path().join("server.log"))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
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
            nervix_primitives::task::consume_budget().await;
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
        let connection = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
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
                let sender = nervix_primitives::task::spawn(trickle_upload_chunks(
                    request_body,
                    upload_chunk_frame()?,
                ));
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
            nervix_primitives::task::consume_budget().await;
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

    pub(crate) fn interconnect_endpoint(&self) -> String {
        loopback(self.configuration.ports.interconnect)
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
            nervix_primitives::task::consume_budget().await;
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
        nervix_primitives::task::consume_budget().await;
        let payload_template = payload_templates
            .get(template_index)
            .assured("the template index is kept below the non-empty template list length");
        let payload = payload_template.replace("{{load_id}}", &load_id.to_string());
        let published = nervix_primitives::select! {
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

        nervix_primitives::select! {
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
        nervix_primitives::task::consume_budget().await;
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
    let start = UploadStart {
        request_id: RequestId::new(NonZeroU64::MIN),
        domain: DomainName::parse(domain).map_err(io::Error::other)?,
        resource: ResourceName::parse(resource).map_err(io::Error::other)?,
        upload_identity: ResourceUploadIdentity::parse(HELD_UPLOAD_IDENTITY)
            .map_err(io::Error::other)?,
        total_bytes: NonZeroU64::new(HELD_UPLOAD_DECLARED_BYTES)
            .ok_or_else(|| io::Error::other("a held upload declares a non-zero size"))?,
    };
    let frame = start
        .encode(&SessionLimits::DEFAULT)
        .map_err(io::Error::other)?;
    grpc_message_frame(frame.bytes())
}

fn upload_chunk_frame() -> io::Result<Bytes> {
    let frame = UploadChunk::encode(&[0], &SessionLimits::DEFAULT).map_err(io::Error::other)?;
    grpc_message_frame(frame.bytes())
}

/// Frames one upload frame the way gRPC carries it on an HTTP/2 stream.
fn grpc_message_frame(encoded: &[u8]) -> io::Result<Bytes> {
    let length = u32::try_from(encoded.len()).map_err(io::Error::other)?;
    let capacity = GRPC_MESSAGE_PREFIX_BYTES
        .checked_add(encoded.len())
        .ok_or_else(|| io::Error::other("an upload message frame does not fit in memory"))?;
    let mut frame = BytesMut::with_capacity(capacity);
    frame.put_u8(UNCOMPRESSED_GRPC_MESSAGE);
    frame.put_u32(length);
    frame.put_slice(encoded);
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
