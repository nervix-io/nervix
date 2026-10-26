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

use std::path::{Path, PathBuf};

use error_stack::{Report, ResultExt};
use fjall::Database;
use nervix_consensus::{Consensus, ConsensusSettings};
use nervix_execution::{Executor, MemoryClass, StorageClass};
use nervix_interconnect::{HandlerRegistrationError, Transport};
use nervix_primitives::sync::{Arc, StdArc};
use nervix_recovery::Discarded as _;
use thiserror::Error;

use super::{Application, Args, NodeIdentity, NodeSettings, error, error::AppError};
use crate::{
    ConfiguredFaultInjection, cluster, registry::Registry, resource::ResourceStore,
    runtime::Runtime,
};

const CONSENSUS_DATABASE_DIRECTORY: &str = "consensus";
const CONSENSUS_KEYSPACE_PREFIX: &str = "raft_";
/// What a storage job at startup or shutdown is charged when it holds nothing of its own: opening or
/// closing the node's stores, whose databases allocate their own caches, and the startup
/// preflight's reads of small kernel files and its probe files. The charge only admits the job.
pub(in crate::application) const STORAGE_JOB_RESERVATION_BYTES: u64 = 4096;

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
            .reserve(MemoryClass::Management, STORAGE_JOB_RESERVATION_BYTES)
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
            .reserve(MemoryClass::Management, STORAGE_JOB_RESERVATION_BYTES)
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

    /// Converts each options group once. Every setting is validated here, before the node opens
    /// a store, binds a listener or starts a task.
    fn try_from(args: Args) -> Result<Self, Self::Error> {
        let identity = NodeIdentity::try_from(args.node)?;
        let settings = NodeSettings::try_from(args.settings)?;
        Ok(Self::builder()
            .identity(identity)
            .settings(settings)
            .build())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fjall::Database;
    use nervix_consensus::RaftTimings;
    use nervix_execution::{Executor, MemoryClass, StorageClass};
    use nervix_models::ClusterNodeName;

    use super::{
        Application, ApplicationStartup, CONSENSUS_KEYSPACE_PREFIX, NodeIdentity, NodeSettings,
        STORAGE_JOB_RESERVATION_BYTES,
    };
    use crate::{
        application::test_fixtures::{test_addr, test_tls_files},
        cluster::{MembershipSettings, MembershipValues},
    };

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
        let identity = NodeIdentity::builder()
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
            .cluster_bootstrap_host(Some("invalid host name:1".to_string()))
            .db_path(db_path.clone())
            .build();
        let settings = NodeSettings {
            raft: RaftTimings::resolve(Some(Duration::from_millis(50)))
                .expect("the test's Raft heartbeat is valid"),
            membership: MembershipSettings::resolve(MembershipValues {
                node_unavailability_timeout: Some(Duration::from_secs(1)),
                ..MembershipValues::default()
            })
            .expect("the test's membership settings are valid"),
            ..NodeSettings::default()
        };
        let application = Application::builder()
            .identity(identity)
            .settings(settings)
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
            .try_reserve(MemoryClass::Management, STORAGE_JOB_RESERVATION_BYTES)
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
