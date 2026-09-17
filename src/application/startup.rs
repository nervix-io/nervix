//! Turning parsed arguments into a configured node.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading the argument set into the listeners, stores and cluster identity one
//!   `Application` holds, and releasing what it opened when a later step fails.
//! - **Depends on.** The argument definition and the stores it opens.
//! - **Must not know.** What the configured node goes on to run.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc as StdArc,
};

use error_stack::{Report, ResultExt};
use fjall::Database;
use nervix_consensus::{Consensus, ConsensusSettings, RaftRetentionPolicy};
use nervix_execution::{Executor, MemoryClass, StorageClass};
use nervix_interconnect::{HandlerRegistrationError, Transport};
use nervix_models::NodeEndpoint;
use nervix_recovery::Discarded as _;
use thiserror::Error;
use triomphe::Arc;

use super::{Application, Args, error, error::AppError, shutdown::ShutdownCoordinator};
use crate::{
    ConfiguredFaultInjection, cluster,
    memory_pressure::MemoryPressureConfig,
    registry::Registry,
    resource::{ResourceStore, ResourceStoreLimits},
    runtime::Runtime,
};

const CONSENSUS_DATABASE_DIRECTORY: &str = "consensus";
const CONSENSUS_KEYSPACE_PREFIX: &str = "raft_";
const DATABASE_OPEN_RESERVATION_BYTES: u64 = 4096;

#[derive(Debug, Error)]
enum NodeDatabaseOpenError {
    #[error("failed to open the node database: {0}")]
    Open(#[source] fjall::Error),
    #[error("consensus keyspaces are present in the node database")]
    ConsensusNotDedicated,
}

pub(in crate::application) struct ApplicationStartup {
    pub(in crate::application) db: Database,
    pub(in crate::application) consensus_path: PathBuf,
    /// A `std` reference count because the runtime publishes the same store through `ArcSwap`.
    pub(in crate::application) resource_store: StdArc<ResourceStore>,
    pub(in crate::application) registry: Arc<Registry>,
    pub(in crate::application) runtime: Runtime,
    pub(in crate::application) consensus: Option<Consensus>,
    pub(in crate::application) interconnect: Option<Transport>,
}

impl ApplicationStartup {
    pub(in crate::application) fn consensus_database_path(db_path: &Path) -> PathBuf {
        db_path.join(CONSENSUS_DATABASE_DIRECTORY)
    }

    pub(in crate::application) async fn open_node_database(
        path: PathBuf,
        executor: &Executor,
    ) -> Result<Database, Report<AppError>> {
        let reservation = executor
            .reserve(MemoryClass::Management, DATABASE_OPEN_RESERVATION_BYTES)
            .await
            .change_context(AppError::OpenRegistry)?;
        let opened_path = path.clone();
        let opened = executor
            .run_storage(StorageClass::Filesystem, reservation, move |_, _| {
                let db = Database::builder(opened_path)
                    .open()
                    .map_err(NodeDatabaseOpenError::Open)?;
                if db
                    .list_keyspace_names()
                    .iter()
                    .any(|name| name.starts_with(CONSENSUS_KEYSPACE_PREFIX))
                {
                    return Err(NodeDatabaseOpenError::ConsensusNotDedicated);
                }
                Ok(db)
            })
            .await
            .change_context(AppError::OpenRegistry)?;
        match opened {
            Ok(db) => Ok(db),
            Err(NodeDatabaseOpenError::Open(error)) => {
                error!(db_path = %path.display(), error = %error, "failed to open node fjall database");
                Err(Report::new(AppError::OpenRegistry).attach_printable(error))
            }
            Err(NodeDatabaseOpenError::ConsensusNotDedicated) => {
                error!(db_path = %path.display(), "consensus keyspaces found in node fjall database");
                Err(Report::new(AppError::ConsensusStorageLayout))
            }
        }
    }

    pub(in crate::application) async fn open_consensus(
        &self,
        settings: ConsensusSettings,
        _fault_injection: &ConfiguredFaultInjection,
    ) -> Result<Consensus, Report<AppError>> {
        #[cfg(feature = "testing")]
        let node_id = settings.node_id.clone();
        #[cfg(feature = "testing")]
        let consensus = Consensus::open_with_test_probe(
            &self.consensus_path,
            settings,
            _fault_injection.consensus_test_probe(&node_id),
        )
        .await
        .change_context(AppError::StartConsensus)?;
        #[cfg(not(feature = "testing"))]
        let consensus = Consensus::open(&self.consensus_path, settings)
            .await
            .change_context(AppError::StartConsensus)?;
        #[cfg(feature = "testing")]
        _fault_injection.register_consensus(node_id, &consensus);
        Ok(consensus)
    }

    pub(in crate::application) async fn terminate(self) {
        self.runtime.shutdown().await;
        if let Some(consensus) = &self.consensus {
            consensus.shutdown().await;
        }
        if let Some(interconnect) = &self.interconnect {
            interconnect.shutdown().await;
        }
        if let Err(error) = tokio::task::spawn_blocking(move || drop(self)).await {
            error!(error = %error, "failed to join application startup cleanup task");
        }
    }

    pub(in crate::application) async fn require_handler_registration(
        self,
        cluster: &cluster::ClusterHandle,
        registration: Result<(), Report<HandlerRegistrationError>>,
    ) -> Result<Self, Report<AppError>> {
        let Err(error) = registration else {
            return Ok(self);
        };
        cluster
            .shutdown()
            .await
            .discarded("the handler-registration error remains the startup failure");
        self.terminate().await;
        Err(error.change_context(AppError::StartInterconnect))
    }
}

impl TryFrom<Args> for Application {
    type Error = Report<AppError>;

    fn try_from(args: Args) -> Result<Self, Self::Error> {
        let addr = args
            .addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseAddress)?;
        let grpc_https_listen_addr = args
            .grpc_https_listen_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<SocketAddr>()
                    .change_context(AppError::ParseGrpcHttpsListenAddress)
            })
            .transpose()?;
        let grpc_advertise_addr = match args.grpc_advertise_addr.as_deref() {
            Some(addr) => addr.parse::<NodeEndpoint>().map_err(|error| {
                Report::new(AppError::ParseGrpcAdvertiseAddress).attach_printable(error)
            })?,
            None => addr.into(),
        };
        let grpc_https_advertise_addr = args
            .grpc_https_advertise_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<NodeEndpoint>().map_err(|error| {
                    Report::new(AppError::ParseGrpcHttpsAdvertiseAddress).attach_printable(error)
                })
            })
            .transpose()?;
        let http_listen_addr = args
            .http_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseHttpListenAddress)?;
        let https_listen_addr = args
            .https_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseHttpsListenAddress)?;
        let observability_listen_addr = args
            .observability_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseObservabilityListenAddress)?;
        let web_console_listen_addr = args
            .web_console_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseWebConsoleListenAddress)?;
        let web_console_advertise_addr = args
            .web_console_advertise_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<NodeEndpoint>().map_err(|error| {
                    Report::new(AppError::ParseWebConsoleListenAddress).attach_printable(error)
                })
            })
            .transpose()?;
        let web_console_https_listen_addr = args
            .web_console_https_listen_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<SocketAddr>()
                    .change_context(AppError::ParseWebConsoleHttpsListenAddress)
            })
            .transpose()?;
        let interconnect_listen_addr = match args.interconnect_listen_addr.as_deref() {
            Some(addr) => addr
                .parse::<SocketAddr>()
                .change_context(AppError::ParseInterconnectListenAddress)?,
            None => cluster::derive_peer_addr(addr)
                .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?,
        };
        let interconnect_advertise_addr = match args.interconnect_advertise_addr.as_deref() {
            Some(addr) => addr.parse::<NodeEndpoint>().map_err(|error| {
                Report::new(AppError::ParseInterconnectAdvertiseAddress).attach_printable(error)
            })?,
            None => {
                let port = grpc_advertise_addr
                    .port()
                    .checked_add(1)
                    .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?;
                grpc_advertise_addr.with_port(port)
            }
        };
        let memory_pressure = match (args.memory_high_watermark, args.memory_low_watermark) {
            (Some(high_watermark), Some(low_watermark)) => {
                let config = MemoryPressureConfig::builder()
                    .high_watermark(high_watermark)
                    .low_watermark(low_watermark)
                    .check_interval(args.memory_pressure_check_interval)
                    .resume_jitter(args.memory_pressure_resume_jitter)
                    .build();
                config.validate().map_err(|error| {
                    Report::new(AppError::InvalidMemoryPressureConfig).attach_printable(error)
                })?;
                Some(config)
            }
            (Some(_), None) => {
                return Err(Report::new(AppError::MissingMemoryPressureLowWatermark));
            }
            (None, Some(_)) => {
                return Err(Report::new(AppError::MissingMemoryPressureHighWatermark));
            }
            (None, None) => None,
        };
        Ok(Self::builder()
            .addr(addr)
            .grpc_mode(args.grpc_mode)
            .grpc_https_listen_addr(grpc_https_listen_addr)
            .grpc_https_advertise_addr(grpc_https_advertise_addr)
            .http_listen_addr(http_listen_addr)
            .https_listen_addr(https_listen_addr)
            .observability_listen_addr(observability_listen_addr)
            .web_console_listen_addr(web_console_listen_addr)
            .web_console_advertise_addr(web_console_advertise_addr)
            .web_console_https_listen_addr(web_console_https_listen_addr)
            .web_console_tls_cert(args.web_console_tls_cert)
            .web_console_tls_key(args.web_console_tls_key)
            .cluster_id(args.cluster_id)
            .node_id(args.node_id)
            .grpc_advertise_addr(grpc_advertise_addr)
            .interconnect_listen_addr(interconnect_listen_addr)
            .interconnect_advertise_addr(interconnect_advertise_addr)
            .interconnect_tls_ca(args.interconnect_tls_ca)
            .interconnect_tls_cert(args.interconnect_tls_cert)
            .interconnect_tls_key(args.interconnect_tls_key)
            .allow_bootstrap(args.allow_bootstrap)
            .default_user(args.default_user)
            .init_default_user_password(args.init_default_user_password)
            .node_unavailability_timeout(args.node_unavailability_timeout)
            .raft_heartbeat_interval(args.raft_heartbeat_interval)
            .raft_election_timeout_min(args.raft_election_timeout_min)
            .raft_election_timeout_max(args.raft_election_timeout_max)
            .raft_retention(RaftRetentionPolicy {
                snapshot_entry_threshold: args.raft_snapshot_entry_threshold,
                snapshot_byte_threshold: args.raft_snapshot_byte_threshold.as_u64(),
                covered_entries_retained: args.raft_covered_log_entries_retained,
                covered_bytes_retained: args.raft_covered_log_bytes_retained.as_u64(),
                retained_log_cap_bytes: args.raft_retained_log_cap.as_u64(),
                ..RaftRetentionPolicy::default()
            })
            .transaction_idle_timeout(args.transaction_idle_timeout)
            .transaction_tombstone_retention(args.transaction_tombstone_retention)
            .transaction_max_statements(args.transaction_max_statements)
            .transaction_max_source_bytes(args.transaction_max_source_bytes)
            .transaction_max_open(args.transaction_max_open)
            .replica_count(args.replica_count)
            .state_snapshot_interval(args.state_snapshot_interval)
            .memory_pressure(memory_pressure)
            .shutdown(ShutdownCoordinator::new(args.shutdown_timeout))
            .drain_timeout(args.drain_timeout)
            .cluster_bootstrap_host(args.cluster_bootstrap_host)
            .db_path(PathBuf::from(args.db_path))
            .temp_dir(args.temp_dir)
            .resource_store_limits(ResourceStoreLimits {
                max_archive_bytes: args.resource_max_archive_bytes.as_u64(),
                max_extracted_bytes: args.resource_max_extracted_bytes.as_u64(),
                max_file_count: args.resource_max_file_count,
            })
            .build())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fjall::Database;
    use nervix_models::ClusterNodeName;

    use super::{Application, ApplicationStartup, CONSENSUS_KEYSPACE_PREFIX};
    use crate::application::test_fixtures::{test_addr, test_tls_files};

    #[tokio::test]
    async fn startup_failure_releases_the_split_databases_before_returning() {
        let root = tempfile::tempdir().expect("temporary root should be created");
        let db_path = root.path().join("db");
        let listen_addr = test_addr(0);
        let node_id = ClusterNodeName::parse("node-1").expect("valid name");
        let tls_files = test_tls_files("startup-failure-test", &node_id);
        let application = Application::builder()
            .addr(listen_addr)
            .http_listen_addr(listen_addr)
            .https_listen_addr(listen_addr)
            .observability_listen_addr(listen_addr)
            .web_console_listen_addr(listen_addr)
            .cluster_id("startup-failure-test".to_string())
            .node_id(node_id)
            .grpc_advertise_addr(listen_addr.into())
            .interconnect_listen_addr(listen_addr)
            .interconnect_advertise_addr(listen_addr.into())
            .interconnect_tls_ca(tls_files.ca.clone())
            .interconnect_tls_cert(tls_files.certificate.clone())
            .interconnect_tls_key(tls_files.private_key.clone())
            .allow_bootstrap(true)
            .node_unavailability_timeout(Duration::from_secs(1))
            .raft_heartbeat_interval(Duration::from_millis(100))
            .raft_election_timeout_min(Duration::from_millis(300))
            .raft_election_timeout_max(Duration::from_millis(600))
            .cluster_bootstrap_host(Some("invalid host name:1".to_string()))
            .db_path(db_path.clone())
            .graceful_shutdown_drain(false)
            .build();

        let error = application
            .run()
            .await
            .expect_err("invalid cluster advertise host should fail startup");
        assert!(
            format!("{error:?}").contains("failed to start cluster membership"),
            "unexpected startup error: {error:?}"
        );

        let (node_keyspaces, consensus_keyspaces) = tokio::task::spawn_blocking(move || {
            let consensus_path = ApplicationStartup::consensus_database_path(&db_path);
            let db = Database::builder(db_path).open()?;
            let consensus_db = Database::builder(consensus_path).open()?;
            let node_keyspaces = db.list_keyspace_names();
            let consensus_keyspaces = consensus_db.list_keyspace_names();
            drop(consensus_db);
            drop(db);
            Ok::<_, fjall::Error>((node_keyspaces, consensus_keyspaces))
        })
        .await
        .expect("database open task should join")
        .expect("application startup failure must release both database locks");
        assert!(
            node_keyspaces
                .iter()
                .all(|name| !name.starts_with(CONSENSUS_KEYSPACE_PREFIX)),
            "the node database must not contain consensus keyspaces"
        );
        assert_eq!(consensus_keyspaces.len(), 4);
        assert!(
            consensus_keyspaces
                .iter()
                .all(|name| name.starts_with(CONSENSUS_KEYSPACE_PREFIX)),
            "the consensus database must contain only consensus keyspaces"
        );
    }
}
