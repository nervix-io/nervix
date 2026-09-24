//! Ephemeral test-container cleanup after the owning process disappears.
//!
//! Outside the layer order: test harness infrastructure.
//!
//! - **Owns.** The Ryuk sidecar, its process-liveness connection, and the cleanup session label.
//! - **Depends on.** Testcontainers, the Docker socket, and Tokio networking.
//! - **Must not know.** Nervix runtime state or the services provisioned by the test environment.

use std::{env, io, path::PathBuf, time::Duration};

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt as _, ReuseDirective,
    core::{IntoContainerPort as _, Mount, WaitFor},
    runners::AsyncRunner as _,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
};
use uuid::Uuid;

pub(crate) const SESSION_LABEL: &str = "com.nervix.testcontainers.reaper-session";

const DISABLED_ENV: &str = "TESTCONTAINERS_RYUK_DISABLED";
const PRIVILEGED_ENV: &str = "TESTCONTAINERS_RYUK_PRIVILEGED";
const SOCKET_OVERRIDE_ENV: &str = "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE";
const DOCKER_HOST_ENV: &str = "DOCKER_HOST";
const RYUK_PORT: u16 = 8080;
const RYUK_IMAGE: &str = "testcontainers/ryuk";
const RYUK_TAG: &str = "0.14.0";
const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(crate) struct ResourceReaper {
    state: ReaperState,
}

#[derive(Debug)]
enum ReaperState {
    Disabled,
    Pending {
        session: String,
    },
    Running {
        session: String,
        guard: Box<RyukGuard>,
    },
}

impl ResourceReaper {
    pub(crate) fn new(enabled: bool) -> Self {
        if !enabled {
            return Self {
                state: ReaperState::Disabled,
            };
        }
        Self {
            state: ReaperState::Pending {
                session: Uuid::now_v7().as_simple().to_string(),
            },
        }
    }

    pub(crate) fn session_label(&self) -> Option<(&'static str, &str)> {
        match &self.state {
            ReaperState::Disabled => None,
            ReaperState::Pending { session } | ReaperState::Running { session, .. } => {
                Some((SESSION_LABEL, session))
            }
        }
    }

    pub(crate) async fn ensure_running(&mut self) -> io::Result<()> {
        let ReaperState::Pending { session } = &self.state else {
            return Ok(());
        };
        if boolean_environment(DISABLED_ENV)? {
            self.state = ReaperState::Disabled;
            return Ok(());
        }
        let session = session.clone();
        let guard = RyukGuard::start(&session).await?;
        self.state = ReaperState::Running {
            session,
            guard: Box::new(guard),
        };
        Ok(())
    }

    pub(crate) async fn shutdown(&mut self, teardown_succeeded: bool) -> io::Result<()> {
        let state = std::mem::replace(&mut self.state, ReaperState::Disabled);
        let ReaperState::Running { guard, .. } = state else {
            return Ok(());
        };
        if teardown_succeeded {
            (*guard).remove().await
        } else {
            (*guard).release_to_cleanup();
            Ok(())
        }
    }
}

struct RyukGuard {
    connection: TcpStream,
    container: ContainerAsync<GenericImage>,
}

impl std::fmt::Debug for RyukGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RyukGuard")
            .field("container_id", &self.container.id())
            .finish_non_exhaustive()
    }
}

impl RyukGuard {
    async fn start(session: &str) -> io::Result<Self> {
        let socket = docker_socket_path()?;
        let socket = socket.to_str().ok_or_else(|| {
            io::Error::other(format!(
                "Docker socket path is not valid UTF-8: {}",
                socket.display()
            ))
        })?;
        let container = GenericImage::new(RYUK_IMAGE, RYUK_TAG)
            .with_exposed_port(RYUK_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Started"))
            .with_mount(Mount::bind_mount(socket, "/var/run/docker.sock"))
            .with_container_name(format!("nervix-ryuk-{session}"))
            .with_privileged(boolean_environment(PRIVILEGED_ENV)?)
            .with_host_config_modifier(|host_config| host_config.auto_remove = Some(true))
            // Ryuk must outlive this handle when dependency teardown fails. Its Docker
            // auto-remove setting removes it after its liveness connection closes and it prunes.
            .with_reuse(ReuseDirective::Always)
            .start()
            .await
            .map_err(|error| {
                io::Error::other(format!("Ryuk container failed to start: {error}"))
            })?;

        let connection = match Self::connect_and_register(&container, session).await {
            Ok(connection) => connection,
            Err(error) => {
                let cleanup = container.rm().await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(io::Error::other(format!(
                        "{error}; the unusable Ryuk container could not be removed: \
                         {cleanup_error}"
                    ))),
                };
            }
        };
        Ok(Self {
            connection,
            container,
        })
    }

    async fn connect_and_register(
        container: &ContainerAsync<GenericImage>,
        session: &str,
    ) -> io::Result<TcpStream> {
        let host = container
            .get_host()
            .await
            .map_err(|error| io::Error::other(format!("Ryuk host lookup failed: {error}")))?;
        let port = container
            .get_host_port_ipv4(RYUK_PORT.tcp())
            .await
            .map_err(|error| io::Error::other(format!("Ryuk port lookup failed: {error}")))?;
        let endpoint = (host.to_string(), port);
        let mut connection = tokio::time::timeout(CONTROL_TIMEOUT, TcpStream::connect(endpoint))
            .await
            .map_err(|_| io::Error::other("timed out connecting to Ryuk"))??;
        connection.set_nodelay(true)?;

        let filter = format!("label={SESSION_LABEL}={session}\n");
        tokio::time::timeout(CONTROL_TIMEOUT, connection.write_all(filter.as_bytes()))
            .await
            .map_err(|_| io::Error::other("timed out registering the Ryuk cleanup filter"))??;
        let mut acknowledgement = [0_u8; 4];
        tokio::time::timeout(CONTROL_TIMEOUT, connection.read_exact(&mut acknowledgement))
            .await
            .map_err(|_| io::Error::other("timed out waiting for Ryuk to accept the filter"))??;
        if acknowledgement != *b"ACK\n" {
            return Err(io::Error::other(format!(
                "Ryuk returned an invalid filter acknowledgement: {acknowledgement:?}"
            )));
        }
        Ok(connection)
    }

    async fn remove(self) -> io::Result<()> {
        let Self {
            connection,
            container,
        } = self;
        let id = container.id().to_string();
        let result = container.rm().await.map_err(|error| {
            io::Error::other(format!("failed to remove Ryuk container {id}: {error}"))
        });
        drop(connection);
        result
    }

    fn release_to_cleanup(self) {
        let Self {
            connection,
            container,
        } = self;
        drop(connection);
        // Reuse prevents Testcontainers' handle drop from racing Ryuk's own cleanup. Docker's
        // auto-remove setting deletes the sidecar when it exits after pruning the session.
        drop(container);
    }
}

fn boolean_environment(name: &str) -> io::Result<bool> {
    match env::var(name).as_deref() {
        Err(env::VarError::NotPresent) | Ok("") | Ok("0") | Ok("false") | Ok("FALSE") => Ok(false),
        Ok("1") | Ok("true") | Ok("TRUE") => Ok(true),
        Ok(value) => Err(io::Error::other(format!(
            "{name} must be 'true', 'false', '1', or '0', found {value:?}"
        ))),
        Err(env::VarError::NotUnicode(_)) => {
            Err(io::Error::other(format!("{name} must contain valid UTF-8")))
        }
    }
}

fn docker_socket_path() -> io::Result<PathBuf> {
    if let Some(socket) = nonempty_environment(SOCKET_OVERRIDE_ENV)? {
        return existing_socket(PathBuf::from(socket));
    }
    if let Some(host) = nonempty_environment(DOCKER_HOST_ENV)? {
        if let Some(socket) = host.strip_prefix("unix://") {
            return existing_socket(PathBuf::from(socket));
        }
        return Err(io::Error::other(format!(
            "Ryuk requires {SOCKET_OVERRIDE_ENV} when {DOCKER_HOST_ENV} is not a Unix socket"
        )));
    }

    let mut candidates = vec![PathBuf::from("/var/run/docker.sock")];
    if let Some(runtime) = nonempty_environment("XDG_RUNTIME_DIR")? {
        candidates.push(PathBuf::from(runtime).join("docker.sock"));
    }
    if let Some(home) = nonempty_environment("HOME")? {
        let home = PathBuf::from(home);
        candidates.push(home.join(".docker/run/docker.sock"));
        candidates.push(home.join(".docker/desktop/docker.sock"));
    }
    candidates
        .into_iter()
        .find(|path| path.exists())
        .ok_or_else(|| {
            io::Error::other(format!(
                "Ryuk could not find the Docker socket; set {SOCKET_OVERRIDE_ENV} or disable it \
                 with {DISABLED_ENV}=true"
            ))
        })
}

fn nonempty_environment(name: &str) -> io::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            Err(io::Error::other(format!("{name} must contain valid UTF-8")))
        }
    }
}

fn existing_socket(path: PathBuf) -> io::Result<PathBuf> {
    if path.exists() {
        Ok(path)
    } else {
        Err(io::Error::other(format!(
            "configured Docker socket does not exist: {}",
            path.display()
        )))
    }
}
