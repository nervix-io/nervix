//! Turning parsed arguments into a configured node.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading the argument set into the listeners, stores and cluster identity one
//!   `Application` holds, and releasing what it opened when a later step fails.
//! - **Depends on.** The argument definition and the stores it opens.
//! - **Must not know.** What the configured node goes on to run.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use error_stack::{Report, ResultExt};
use fjall::Database;
use nervix_consensus::{Consensus, ConsensusSettings, RaftRetentionPolicy};
use nervix_dns::{DnsConfiguration, NameServers};
use nervix_execution::{Executor, MemoryClass, StorageClass};
use nervix_interconnect::{HandlerRegistrationError, Transport};
use nervix_models::NodeEndpoint;
use nervix_primitives::sync::{Arc, StdArc};
use nervix_recovery::Discarded as _;
use thiserror::Error;

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
/// What opening or closing the node's stores is charged. The databases allocate their own caches,
/// so the charge only admits the storage job that opens or closes them.
const DATABASE_RESERVATION_BYTES: u64 = 4096;

#[derive(Debug, Error)]
enum NodeDatabaseOpenError {
    #[error("failed to open the node database")]
    Open,
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
            .reserve(MemoryClass::Management, DATABASE_RESERVATION_BYTES)
            .await
            .change_context(AppError::OpenRegistry)?;
        let opened_path = path.clone();
        let opened = executor
            .run_storage(StorageClass::Filesystem, reservation, move |_, _| {
                let db = Database::builder(opened_path)
                    .open()
                    .change_context(NodeDatabaseOpenError::Open)?;
                if db
                    .list_keyspace_names()
                    .iter()
                    .any(|name| name.starts_with(CONSENSUS_KEYSPACE_PREFIX))
                {
                    return Err(Report::new(NodeDatabaseOpenError::ConsensusNotDedicated));
                }
                Ok(db)
            })
            .await
            .change_context(AppError::OpenRegistry)?;
        match opened {
            Ok(db) => Ok(db),
            Err(report) => {
                let context = match report.current_context() {
                    NodeDatabaseOpenError::Open => {
                        error!(db_path = %path.display(), error = ?report, "failed to open node fjall database");
                        AppError::OpenRegistry
                    }
                    NodeDatabaseOpenError::ConsensusNotDedicated => {
                        error!(db_path = %path.display(), "consensus keyspaces found in node fjall database");
                        AppError::ConsensusStorageLayout
                    }
                };
                Err(report.change_context(context))
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

    /// Installs every domain's stored schedule on the runtime before the node serves. A failure
    /// keeps the registry's or the runtime's report beneath the step that failed, which names the
    /// domain once one is being installed.
    pub(in crate::application) async fn apply_stored_runtime_changes(
        &self,
    ) -> error_stack::Result<(), AppError> {
        let startup_runtime_changes = self
            .registry
            .startup_runtime_changes()
            .change_context(AppError::ReadStartupRuntimeChanges)?;
        for changes in startup_runtime_changes {
            let revision = changes.execution_revision().change_context_lazy(|| {
                AppError::PlanStartupRuntime {
                    domain: changes.domain.clone(),
                }
            })?;
            self.runtime
                .apply_changes(&changes.domain, revision)
                .await
                .change_context_lazy(|| AppError::ApplyStartupRuntime {
                    domain: changes.domain.clone(),
                })?;
        }
        Ok(())
    }

    pub(in crate::application) async fn terminate(self) {
        self.runtime
            .shutdown(self.runtime.domain_drain_timeout())
            .await;
        if let Some(consensus) = &self.consensus {
            consensus.shutdown().await;
        }
        if let Some(interconnect) = &self.interconnect {
            interconnect.shutdown().await;
        }
        let executor = self.runtime.executor().clone();
        if let Err(error) = Self::close_stores(&executor, move || drop(self)).await {
            error!(error = ?error, "failed to close the stores of a node that did not start");
        }
    }

    /// Drop `stores`, which own the node's databases, on a filesystem storage worker: closing a
    /// database flushes its journal and joins its background work synchronously.
    pub(in crate::application) async fn close_stores(
        executor: &Executor,
        stores: impl FnOnce() + Send + 'static,
    ) -> Result<(), Report<AppError>> {
        let reservation = executor
            .reserve(MemoryClass::Management, DATABASE_RESERVATION_BYTES)
            .await
            .change_context(AppError::CloseStores)?;
        executor
            .run_storage(StorageClass::Filesystem, reservation, move |_, _| stores())
            .await
            .change_context(AppError::CloseStores)
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
        // An empty list is how the command line says no server was named, so the resolver
        // configuration's own servers apply.
        let name_servers = if args.dns_name_servers.is_empty() {
            NameServers::ResolverConfiguration
        } else {
            NameServers::Explicit(args.dns_name_servers)
        };
        let dns = DnsConfiguration {
            resolver_configuration: args.dns_resolver_config,
            hosts_file: args.dns_hosts_file,
            name_servers,
        };
        let memory_pressure = match (args.memory_high_watermark, args.memory_low_watermark) {
            (Some(high_watermark), Some(low_watermark)) => {
                let config = MemoryPressureConfig::builder()
                    .high_watermark(high_watermark)
                    .low_watermark(low_watermark)
                    .check_interval(args.memory_pressure_check_interval)
                    .resume_jitter(args.memory_pressure_resume_jitter)
                    .build();
                config
                    .validate()
                    .change_context(AppError::InvalidMemoryPressureConfig)?;
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
            .command_execution(args.command_execution)
            .transaction_max_statements(args.transaction_max_statements)
            .transaction_max_source_bytes(args.transaction_max_source_bytes)
            .transaction_max_open(args.transaction_max_open)
            .replica_count(args.replica_count)
            .state_snapshot_interval(args.state_snapshot_interval)
            .restore_staging_max_bytes(args.restore_staging_max_bytes.as_u64())
            .memory_pressure(memory_pressure)
            .shutdown(ShutdownCoordinator::new(args.shutdown_timeout))
            .drain_timeout(args.drain_timeout)
            .cluster_bootstrap_host(args.cluster_bootstrap_host)
            .dns(dns)
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
    use nervix_execution::{Executor, MemoryClass, StorageClass};
    use nervix_models::ClusterNodeName;

    use super::{
        Application, ApplicationStartup, CONSENSUS_KEYSPACE_PREFIX, DATABASE_RESERVATION_BYTES,
    };
    use crate::application::test_fixtures::{test_addr, test_tls_files};

    /// Closing the node's stores while one of their databases' background workers fails. Shuttle
    /// builds take their channels from the model, and the database's workers are threads no model
    /// runs, so these tests are built only outside Shuttle.
    #[cfg(not(feature = "shuttle"))]
    mod failing_database_worker {
        use std::time::Duration;

        use fjall::{
            Database, KeyspaceCreateOptions,
            compaction::{
                Leveled,
                filter::{
                    CompactionFilter, CompactionFilterResult, Context, Factory, ItemAccessor,
                },
            },
        };
        use meticulous::ResultExt as _;
        use nervix_execution::Executor;
        use nervix_primitives::sync::{StdArc, blocking::mpsc, oneshot};
        use nervix_recovery::{Discarded as _, NoReceiver as _};

        use super::ApplicationStartup;

        /// How long the test below waits for a database's own work: a compaction reaching its
        /// filter, or a close returning once the database's last background worker has left.
        const DATABASE_WORK_BOUND: Duration = Duration::from_secs(60);

        /// How many writes flood the worker channel of the database the test below closes. Each
        /// write queues a memtable rotation that no worker takes, and there are many times more of
        /// them than the channel holds, so the channel stays full for far longer than a close takes
        /// between two of its attempts to hand a worker its stop message.
        const FLOODING_WRITES: u32 = 20_000;

        /// The compaction filters of the keyspace the test below compacts. Every compaction stops
        /// at its first item, hands the test the sender that lets it go, waits for it, and then
        /// fails, which ends the database worker running it by error and without taking anything
        /// from the worker channel.
        struct FailingCompactions {
            held: mpsc::Sender<mpsc::Sender<()>>,
        }

        impl Factory for FailingCompactions {
            fn name(&self) -> &str {
                "failing_compactions"
            }

            fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
                Box::new(FailingCompaction {
                    held: Some(self.held.clone()),
                })
            }
        }

        /// One compaction of [`FailingCompactions`]. It keeps the sender it reports that it is
        /// held through until its first item.
        struct FailingCompaction {
            held: Option<mpsc::Sender<mpsc::Sender<()>>>,
        }

        impl CompactionFilter for FailingCompaction {
            fn filter_item(&mut self, _: ItemAccessor<'_>, _: &Context) -> CompactionFilterResult {
                if let Some(held) = self.held.take() {
                    let (release, released) = mpsc::channel();
                    held.send(release)
                        .means_peer_left("the test waiting for the compaction to be held");
                    released.recv().discarded(
                        "a test that drops the release instead of sending it lets the compaction \
                         fail all the same",
                    );
                }
                Err(fjall::LsmError::Io(std::io::Error::other(
                    "the test fails every compaction of this keyspace",
                )))
            }
        }

        /// The node's stores close when the last background worker of one of their databases
        /// fails while the close waits for it with the worker channel full: such a worker leaves
        /// without taking its stop message, and nothing will receive from that channel again.
        ///
        /// The database has one worker, which a compaction holds until the close has begun and
        /// the test has flooded the channel. The compaction then fails. Closing a fjall 3.1.10
        /// database blocks in a send on the full channel at this point and never returns.
        #[nervix_primitives::test]
        async fn stores_close_when_the_last_database_worker_fails_while_the_close_waits_for_it() {
            let directory = tempfile::tempdir().assured("the fixture can create a directory");
            let (held, compaction_held) = mpsc::channel();
            let failing_compactions: StdArc<dyn Factory> = StdArc::new(FailingCompactions { held });
            let db = Database::builder(directory.path())
                .worker_threads(1)
                .with_compaction_filter_factories(StdArc::new(move |keyspace| {
                    if keyspace == "compacted" {
                        Some(StdArc::clone(&failing_compactions))
                    } else {
                        None
                    }
                }))
                .open()
                .assured("a new directory opens as a database");
            let compacted = db
                .keyspace("compacted", || {
                    KeyspaceCreateOptions::default()
                        .compaction_strategy(StdArc::new(Leveled::default().with_l0_threshold(1)))
                })
                .assured("the new database creates a keyspace");
            let flooded = db
                .keyspace("flooded", || {
                    KeyspaceCreateOptions::default().max_memtable_size(1)
                })
                .assured("the new database creates a keyspace");

            // The first table moves down a level whole. The second overlaps it, so the only
            // worker merges the two and stops at the filter.
            for _ in 0..2 {
                compacted
                    .insert("key", "value")
                    .assured("the open keyspace accepts a write");
                compacted
                    .rotate_memtable_and_wait()
                    .assured("the idle worker flushes the memtable");
            }
            let release = compaction_held
                .recv_timeout(DATABASE_WORK_BOUND)
                .assured("merging the two tables reaches the compaction filter");

            let executor = Executor::default();
            let (closing, close_started) = oneshot::channel();
            let close = nervix_primitives::task::spawn({
                let executor = executor.clone();
                async move {
                    ApplicationStartup::close_stores(&executor, move || {
                        closing
                            .send(())
                            .means_peer_left("the test waiting for the close to begin");
                        drop(db);
                    })
                    .await
                }
            });
            close_started
                .await
                .assured("the filesystem workers begin closing the stores");

            // Every write overflows its tiny memtable and queues a rotation the held worker never
            // takes, so the close finds the channel full while its only worker still runs.
            for write in 0..FLOODING_WRITES {
                flooded
                    .insert(write.to_be_bytes(), "value")
                    .assured("an open keyspace accepts writes while its database closes");
            }
            release
                .send(())
                .discarded("the held compaction fails whether it receives the release or not");

            let closed = nervix_primitives::time::timeout(DATABASE_WORK_BOUND, close)
                .await
                .assured("closing the stores returns once the database's last worker has left");
            closed
                .assured("the close task does not panic")
                .assured("the filesystem workers close the stores");
            drop(flooded);
            drop(compacted);
            let snapshot = executor.snapshot();
            assert_eq!(snapshot.filesystem_storage.completed, 1);
        }
    }

    #[nervix_primitives::test]
    async fn opening_the_node_database_retains_the_typed_storage_cause() {
        use meticulous::{OptionExt as _, ResultExt as _};
        let file = tempfile::NamedTempFile::new().assured("the fixture can create a file");
        let executor = Executor::default();
        let failure = ApplicationStartup::open_node_database(file.path().to_path_buf(), &executor)
            .await
            .err()
            .assured("a file cannot be opened as a database directory");
        assert!(matches!(
            failure.current_context(),
            crate::application::error::AppError::OpenRegistry
        ));
        assert!(failure.contains::<fjall::Error>());
    }

    #[nervix_primitives::test]
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

        let executor = Executor::default();
        let reservation = executor
            .try_reserve(MemoryClass::Management, DATABASE_RESERVATION_BYTES)
            .expect("the test's own executor has management room");
        let (node_keyspaces, consensus_keyspaces) = executor
            .run_storage(StorageClass::Filesystem, reservation, move |_, _| {
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
            .expect("the database inspection job should run")
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

    #[nervix_primitives::test]
    async fn stores_are_closed_on_the_filesystem_storage_workers() {
        let executor = Executor::default();
        let closed = nervix_primitives::sync::StdArc::new(
            nervix_primitives::sync::atomic::AtomicBool::new(false),
        );
        let closing = nervix_primitives::sync::StdArc::clone(&closed);

        ApplicationStartup::close_stores(&executor, move || {
            closing.store(true, nervix_primitives::sync::atomic::Ordering::Release);
        })
        .await
        .expect("the filesystem workers close the stores");

        assert!(closed.load(nervix_primitives::sync::atomic::Ordering::Acquire));
        let snapshot = executor.snapshot();
        assert_eq!(snapshot.filesystem_storage.admitted, 1);
        assert_eq!(snapshot.filesystem_storage.completed, 1);
        assert_eq!(snapshot.management_memory.reserved_bytes, 0);
    }
}
