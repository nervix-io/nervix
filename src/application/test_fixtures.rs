//! Fixtures the application's unit tests share.
//!
//! A fixture lives here only when tests in more than one application module build the same
//! value: parsed arguments, TLS material, a running session service, and the models a command
//! is given. A fixture used by one module belongs in that module's own test module instead.

use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use ahash::RandomState;
use clap::Parser;
use dashmap::DashMap;
use fjall::Database;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{Consensus, ConsensusSettings, Proposer, RaftRetentionPolicy};
use nervix_interconnect::{TlsConfigBundle, Transport};
use nervix_models::{
    AckMode, BranchSelection, ClusterNodeName, CreateDeduplicator, CreateEmitter, CreateIngestor,
    CreateJunction, CreateSchema, DomainConfig, DomainName, DomainPace, DomainStartPoint,
    DomainState, DomainStatus, EmitSink, IngestSource, KafkaOffsetMode, Model, ModelKind,
    ModelName, NodeRef, PlacementGroupSchedule, ProcessorInputs, ProcessorOutputs, ScheduledNode,
};
use nonzero_ext::nonzero;
use parking_lot::RwLock;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType,
};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc},
    time::Duration,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::Status;
use triomphe::Arc;

use super::{
    Args,
    session_service::{SessionEvents, SessionServiceImpl, SessionServiceInner},
    subscription::SessionSubscriptions,
    transaction::{
        DEFAULT_TRANSACTION_IDLE_TIMEOUT, DEFAULT_TRANSACTION_MAX_OPEN,
        DEFAULT_TRANSACTION_MAX_SOURCE_BYTES, DEFAULT_TRANSACTION_MAX_STATEMENTS,
        DEFAULT_TRANSACTION_TOMBSTONE_RETENTION,
    },
};
use crate::{
    cluster,
    proto::{
        CommandRequest, CommandResult, SessionResponse, SuggestRequest,
        TransactionState as ApiTransactionState,
    },
    registry::Registry,
    resource::ResourceStore,
    runtime::Runtime,
    runtime_schema,
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

pub(in crate::application) struct TestTlsFiles {
    _directory: tempfile::TempDir,
    pub(in crate::application) ca: PathBuf,
    pub(in crate::application) certificate: PathBuf,
    pub(in crate::application) private_key: PathBuf,
}

pub(in crate::application) fn test_tls_files(
    cluster_id: &str,
    node_id: &ClusterNodeName,
) -> TestTlsFiles {
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().expect("test CA key should generate");
    let ca = ca_params
        .self_signed(&ca_key)
        .expect("test CA should self-sign");

    let mut node_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("test endpoint SANs should be valid");
    node_params.subject_alt_names.push(SanType::URI(
        format!("nervix://cluster/{cluster_id}/node/{node_id}")
            .try_into()
            .expect("test identity URI should be IA5"),
    ));
    node_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    node_params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let node_key = KeyPair::generate().expect("test node key should generate");
    let certificate = node_params
        .signed_by(&node_key, &ca, &ca_key)
        .expect("test node certificate should sign");

    let directory = tempfile::tempdir().expect("test TLS directory should be created");
    let ca_path = directory.path().join("ca.pem");
    let certificate_path = directory.path().join("node.pem");
    let private_key_path = directory.path().join("node-key.pem");
    std::fs::write(&ca_path, ca.pem()).expect("test CA should be written");
    std::fs::write(&certificate_path, certificate.pem())
        .expect("test certificate should be written");
    std::fs::write(&private_key_path, node_key.serialize_pem())
        .expect("test private key should be written");
    TestTlsFiles {
        _directory: directory,
        ca: ca_path,
        certificate: certificate_path,
        private_key: private_key_path,
    }
}

fn test_db_path() -> PathBuf {
    let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("nervix-session-test-{id}"))
}

pub(in crate::application) fn test_addr(base_port: u16) -> std::net::SocketAddr {
    format!("127.0.0.1:{base_port}")
        .parse()
        .expect("valid socket addr")
}

pub(in crate::application) fn test_args(extra: &[&str]) -> Args {
    let mut args = vec![
        "nervix-server",
        "--node-id",
        "node-1",
        "--interconnect-tls-ca",
        "ca.pem",
        "--interconnect-tls-cert",
        "node.pem",
        "--interconnect-tls-key",
        "node-key.pem",
    ];
    args.extend_from_slice(extra);
    Args::parse_from(args)
}

pub(in crate::application) fn try_test_args(extra: &[&str]) -> Result<Args, clap::Error> {
    let mut args = vec![
        "nervix-server",
        "--node-id",
        "node-1",
        "--interconnect-tls-ca",
        "ca.pem",
        "--interconnect-tls-cert",
        "node.pem",
        "--interconnect-tls-key",
        "node-key.pem",
    ];
    args.extend_from_slice(extra);
    Args::try_parse_from(args)
}

pub(in crate::application) fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).expect("valid name")
}

pub(in crate::application) fn command_transaction_state(
    result: &CommandResult,
) -> Option<ApiTransactionState> {
    result
        .transaction
        .as_ref()
        .and_then(|status| ApiTransactionState::try_from(status.state).ok())
}

pub(in crate::application) fn string_branch_key(
    field: &str,
    value: &str,
) -> Option<crate::runtime::BranchKey> {
    crate::runtime::BranchKey::from_fields([(
        named(field),
        runtime_schema::RuntimeValue::String(value.to_string()),
    )])
    .expect("test branch key must be non-empty")
    .into()
}

fn test_node_name(id: impl std::fmt::Display) -> ClusterNodeName {
    ClusterNodeName::parse(&format!("test-node-{id}"))
        .expect("test node names satisfy the name grammar")
}

/// Builds the service the way `run` does, with test defaults for everything the caller does
/// not supply. Every scenario in this module needs the same shape, so they share one builder
/// rather than repeating the field list.
fn test_session_service(
    cluster: Arc<cluster::ClusterHandle>,
    consensus: &Consensus,
    registry: Arc<Registry>,
    resource_store: Arc<ResourceStore>,
    interconnect: Transport,
) -> SessionServiceImpl {
    SessionServiceImpl {
        inner: Arc::new(SessionServiceInner {
            cluster,
            consensus: consensus.proposer(),
            consensus_administrator: consensus.administrator(),
            registry,
            resource_store,
            http_tls_server_config: Arc::new(RwLock::new(None)),
            runtime: Runtime::new(),
            replica_count: 0,
            shutdown: CancellationToken::new(),
            events: SessionEvents::new(16),
            subscription_interest_counts: DashMap::with_hasher(RandomState::new()),
            interconnect,
            next_entity_gate_operation_id: AtomicU64::new(1),
            service_tasks: TaskTracker::new(),
            configured_basic_auth: None,
            auth_rate_limiter: SessionServiceImpl::new_auth_rate_limiter(),
            failed_auth_rate_limit_keys: DashMap::with_hasher(RandomState::new()),
            transaction_idle_timeout: DEFAULT_TRANSACTION_IDLE_TIMEOUT,
            transaction_tombstone_retention: DEFAULT_TRANSACTION_TOMBSTONE_RETENTION,
            transaction_max_statements: DEFAULT_TRANSACTION_MAX_STATEMENTS,
            transaction_max_source_bytes: DEFAULT_TRANSACTION_MAX_SOURCE_BYTES,
            transaction_max_open: DEFAULT_TRANSACTION_MAX_OPEN,
            transaction_bindings: DashMap::with_hasher(RandomState::new()),
            transaction_executions: Arc::new(DashMap::with_hasher(RandomState::new())),
            transaction_commit_execution: AsyncMutex::new(()),
            resource_upload_executions: DashMap::with_hasher(RandomState::new()),
            resource_replication_executions: DashMap::with_hasher(RandomState::new()),
        }),
    }
}

async fn test_interconnect(cluster_id: &str, node_id: &ClusterNodeName) -> Transport {
    let files = test_tls_files(cluster_id, node_id);
    let tls = TlsConfigBundle::from_pem_files(&files.ca, &files.certificate, &files.private_key)
        .expect("test TLS bundle should load");
    let addr = "127.0.0.1:0"
        .parse()
        .expect("ephemeral interconnect address must parse");
    let (transport, _rx) = Transport::bind(
        addr,
        "127.0.0.1",
        cluster_id,
        node_id.clone(),
        tls,
        Default::default(),
        nervix_execution::Executor::default(),
    )
    .await
    .expect("test transport should bind");
    transport
}

/// The model a schedule entry of `kind` carries in these tests.
///
/// A schedule entry reports the kind of the configuration it holds, so a test that wants an
/// entry of a kind has to configure a node of that kind.
fn model_of_kind(identifier_raw: &str, kind: ModelKind) -> Model {
    match kind {
        ModelKind::Ingestor => Model::Ingestor(CreateIngestor {
            name: named(identifier_raw),
            output_routes: ProcessorOutputs::new(Vec::new()),
            decode_using_codec: named("events_codec"),
            timestamp_source: None,
            source: IngestSource::Kafka {
                client: named("kafka_main"),
                topic: named("notifications"),
                offset_mode: KafkaOffsetMode::Domain,
                instances: nonzero!(1u64),
                mode: nervix_models::KafkaIngestMode::AckSequential {
                    timeout: "5s".to_string(),
                    retry_policy: nervix_models::RetryPolicy {
                        backoff: "1s".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                quiesce: nervix_models::IngestQuiesceMode::Suspend,
            },
            general_error_policy: nervix_models::GeneralErrorPolicy::Log,
            filter_where: None,
        }),
        ModelKind::Emitter => Model::Emitter(CreateEmitter {
            name: named(identifier_raw),
            from: ProcessorInputs::new(Vec::new(), Vec::new()),
            encode_using_codec: None,
            sink: Box::new(EmitSink::Syslog {
                client: named("syslog_forwarder"),
            }),
            flush_policy: nervix_models::FlushPolicy::Immediate,
            error_policies: nervix_models::ErrorPolicies::handled_by_log(),
            publishing_mode: nervix_models::EmitterPublishingMode::NoAck {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "1s".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            mode: AckMode::Attached,
            construction: nervix_models::RouteConstruction::default(),
            materialized_state: Vec::new(),
        }),
        ModelKind::Junction => Model::Junction(CreateJunction {
            name: named(identifier_raw),
            from: ProcessorInputs::new(Vec::new(), Vec::new()),
            output_routes: ProcessorOutputs::new(Vec::new()),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }),
        ModelKind::Deduplicator => Model::Deduplicator(CreateDeduplicator {
            name: named(identifier_raw),
            from: ProcessorInputs::new(Vec::new(), Vec::new()),
            output_routes: ProcessorOutputs::new(Vec::new()),
            deduplicate_on: Vec::new(),
            max_time: "1m".to_string(),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }),
        ModelKind::Client => Model::ClientSyslog(nervix_models::CreateClientSyslog {
            name: named(identifier_raw),
            mount: None,
            config: Vec::new(),
        }),
        ModelKind::Schema => Model::Schema(CreateSchema {
            name: named(identifier_raw),
            fields: Vec::new(),
        }),
        other => panic!("no schedule fixture configures a {} node", other.as_str()),
    }
}

pub(in crate::application) fn node_named(raw: &str) -> ClusterNodeName {
    ClusterNodeName::parse(raw).expect("valid name")
}

pub(in crate::application) fn scheduled_node(
    identifier_raw: &str,
    kind: ModelKind,
) -> ScheduledNode {
    ScheduledNode::new(model_of_kind(identifier_raw, kind)).placed_on(
        Some(ClusterNodeName::parse("node-1").expect("valid name")),
        vec![ClusterNodeName::parse("node-1").expect("valid name")],
    )
}

pub(in crate::application) fn scheduled_node_on(
    identifier_raw: &str,
    kind: ModelKind,
    node: &str,
) -> ScheduledNode {
    let node = ClusterNodeName::parse(node).expect("valid name");
    ScheduledNode::new(model_of_kind(identifier_raw, kind))
        .placed_on(Some(node.clone()), vec![node])
}

pub(in crate::application) fn placement_member(identifier_raw: &str, kind: ModelKind) -> NodeRef {
    NodeRef::new(kind, named::<ModelName>(identifier_raw))
}

pub(in crate::application) fn placement_group(
    members: Vec<NodeRef>,
    primary_node: &ClusterNodeName,
) -> PlacementGroupSchedule {
    PlacementGroupSchedule {
        members,
        primary_node: Some(primary_node.clone()),
    }
}

pub(in crate::application) async fn create_test_domain(consensus: &Proposer, raw: &str) {
    let domain = DomainName::parse(raw).expect("valid domain");
    let state = DomainState {
        id: domain,
        config: DomainConfig {
            pace: DomainPace::Paced,
            period: "30s".to_string(),
            skew: "1s".to_string(),
            placement: nervix_models::PlacementPolicy::Neutral,
        },
        status: DomainStatus::Stopped,
        start_version: 0,
        last_start: DomainStartPoint::Resume,
        clock: None,
    };

    for attempt in 0..50 {
        if consensus.put_domain(state.clone()).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(attempt < 49, "test domain should persist");
    }
}

/// A session service built on a throwaway database, handed back with the registry that
/// service shares and the directory the test must remove when it finishes.
pub(in crate::application) struct TestService {
    pub(in crate::application) service: SessionServiceImpl,
    pub(in crate::application) registry: Arc<Registry>,
    pub(in crate::application) path: PathBuf,
}

pub(in crate::application) async fn build_test_service(
    create_default_domain_flag: bool,
) -> TestService {
    let path = test_db_path();
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("test db directory should exist");
    let db = Database::builder(&path)
        .open()
        .expect("database should open");
    let registry = Arc::new(
        Registry::from_database(db.clone(), Some(path.as_path())).expect("registry should open"),
    );
    let id = u16::try_from(NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed))
        .verified("this suite reserves far fewer than u16::MAX test identifiers");
    let grpc_addr = test_addr(
        64000u16
            .checked_add(id)
            .assured("the test id fits inside the port block this suite reserves"),
    );
    let expected_leader = test_node_name(id);
    let interconnect = test_interconnect("test", &expected_leader).await;
    let executor = nervix_execution::Executor::default();
    let consensus = Consensus::from_database(
        db,
        ConsensusSettings {
            cluster_name: "test".to_string(),
            node_id: expected_leader.clone(),
            interconnect_advertise_addr: interconnect.local_addr().to_string(),
            interconnect: interconnect.clone(),
            executor: executor.clone(),
            raft_heartbeat_interval: Duration::from_millis(50),
            raft_election_timeout_min: Duration::from_millis(150),
            raft_election_timeout_max: Duration::from_millis(300),
            raft_retention: RaftRetentionPolicy::default(),
        },
    )
    .await
    .expect("consensus should open");
    consensus
        .administrator()
        .maybe_initialize()
        .await
        .expect("single-node consensus should initialize");
    if create_default_domain_flag {
        create_test_domain(&consensus.proposer(), "default").await;
    }
    for _ in 0..50 {
        if consensus.observer().current_leader().await.as_ref() == Some(&expected_leader) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let interconnect_addr = interconnect.local_addr();
    let cluster = Arc::new(
        cluster::start_cluster(cluster::ClusterSettings {
            cluster_id: "test".to_string(),
            node_id: expected_leader,
            grpc_listen_addr: grpc_addr,
            grpc_advertise_addr: grpc_addr.to_string(),
            web_console_advertise_addr: format!("http://{}", grpc_addr),
            interconnect_advertise_addr: interconnect_addr.into(),
            bootstrap_host: None,
            interconnect: interconnect.clone(),
            node_unavailability_timeout: Duration::from_secs(10),
        })
        .await
        .expect("cluster should start"),
    );
    let service = test_session_service(
        cluster,
        &consensus,
        registry.clone(),
        Arc::new(
            ResourceStore::open(path.join("resources"), executor)
                .expect("resource store should open"),
        ),
        interconnect,
    );
    TestService {
        service,
        registry,
        path,
    }
}

pub(in crate::application) async fn suggestion_values(
    service: &SessionServiceImpl,
    subscriptions: &SessionSubscriptions,
    input: &str,
) -> Vec<String> {
    service
        .process_suggest(
            SuggestRequest {
                input: input.to_string(),
                cursor: u32::try_from(input.len())
                    .assured("the test suggestion input is smaller than u32::MAX bytes"),
                domain: "default".to_string(),
            },
            subscriptions,
        )
        .await
        .suggestions
        .into_iter()
        .map(|suggestion| suggestion.value)
        .collect()
}

pub(in crate::application) async fn queue_in_transaction(
    service: &SessionServiceImpl,
    subscriptions: &mut SessionSubscriptions,
    tx: &mpsc::Sender<Result<SessionResponse, Status>>,
    query: &str,
) {
    let result = service
        .process_command(
            CommandRequest {
                query: query.to_string(),
                domain: "default".to_string(),
            },
            tx,
            subscriptions,
        )
        .await;
    assert!(
        result.success,
        "queueing {query:?} should succeed: {result:?}"
    );
}
