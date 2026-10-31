//! The control plane and the edges it is reached through.
//!
//! Layer: control plane and edges.
//!
//! - **Owns.** The session service, transaction lifecycle, domain lifecycle commands, users and
//!   authentication, resource upload and replication, subscription management, the domain clock,
//!   leader-side coordination, and the listeners for HTTP endpoints, the authenticated
//!   interconnect, metrics and the console.
//! - **Depends on.** The registry for decisions, the runtime for execution, consensus and the
//!   interconnect for cluster state, the proto wire types, and the language layer: this module is
//!   the session adapter and the one place in the server that may name the parser.
//! - **Must not know.** Connector internals, VM IR, Arrow batches or branch-local runtime state. It
//!   commands the data plane and reads what the data plane reports.
//!
//! This module breaks its own contract: it is a single file in which one `SessionServiceImpl` is at
//! once the gRPC adapter, the transaction manager, the leader-side scheduler, the describe fan-out,
//! the domain-clock owner, the subscription manager, the resource uploader and four HTTP servers.
//! Separating the use cases from the adapters is a later move; every piece it is split into
//! inherits the contract above.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        edge,
        reason = "the server session adapter owns its permitted parser dependency"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "session, transaction, authentication and resource operations belong to the \
                  control plane and its edges"
    )
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use admitted_connection::AdmittingListener;
use ahash::{HashMap, HashMapExt, HashSet, RandomState};
use authentication::user_credentials;
use background_task::{
    await_background_task_shutdown, join_public_listeners, request_shutdown_on_completion,
};
use clap::Parser;
use clap_complete::Shell;
#[cfg(feature = "deloxide")]
pub use cli::DiagnosticArgs;
pub use cli::{
    Args, CapacityArgs, Command, ConsensusArgs, MembershipArgs, MemoryPressureArgs, NodeArgs,
    ResourceArgs, RuntimeArgs, SchedulingArgs, SettingOverride, SettingsArgs, ShutdownArgs,
    TracingArgs, TransactionArgs,
};
use cli_completions::print_completions;
pub use cli_values::{HumanBytes, HumanDuration, InternalTransportMode, RefusedCliValue};
#[cfg(test)]
use cli_values::{parse_human_bytes, parse_human_duration, parse_trace_sample_ratio};
use client_consumers::ClientConsumerRouter;
use client_producers::ClientProducerRouter;
use domain_clock::{
    DomainClockRetirements, DomainClockTask, reconcile_domain_clock_tasks,
    run_domain_clock_authority_reconciliation,
};
use error_stack::{Report, ResultExt};
use futures_util::{
    StreamExt,
    future::join_all,
    stream::{self},
};
use http_endpoint::{serve_http, serve_https};
use interconnect_relay::InterconnectRelayPayloadLane;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{ConsensusSettings, TransactionState};
use nervix_interconnect::{
    ActivateOwnershipHandoffStateRequest as RemoteActivateOwnershipHandoffStateRequest,
    ApplicationHealthProbe,
    CaptureOwnershipHandoffStateRequest as RemoteCaptureOwnershipHandoffStateRequest,
    ConfirmOwnershipHandoffStateRequest as RemoteConfirmOwnershipHandoffStateRequest,
    ControlEnvelope, DataflowNodeStatusRequest as RemoteDataflowNodeStatusRequest,
    DataflowNodeStatusResponse as RemoteDataflowNodeStatusResponse,
    DescribeIngestorRequest as RemoteDescribeIngestorRequest,
    DescribeLookupRequest as RemoteDescribeLookupRequest,
    DescribeLookupResponse as RemoteDescribeLookupResponse,
    DescribeMetricsRequest as RemoteDescribeMetricsRequest,
    DescribeMetricsResponse as RemoteDescribeMetricsResponse,
    DescribeRelayRequest as RemoteDescribeRelayRequest,
    DescribeRelayResponse as RemoteDescribeRelayResponse,
    DiscardOwnershipHandoffStateRequest as RemoteDiscardOwnershipHandoffStateRequest,
    DomainClockProgressRequest as RemoteDomainClockProgressRequest,
    DomainDrainStatusRequest as RemoteDomainDrainStatusRequest,
    DomainDrainStatusResponse as RemoteDomainDrainStatusResponse,
    EntityDrainStatusRequest as RemoteEntityDrainStatusRequest,
    EntityDrainStatusResponse as RemoteEntityDrainStatusResponse,
    EntityGateReleaseRequest as RemoteEntityGateReleaseRequest,
    EntityGateReleaseResponse as RemoteEntityGateReleaseResponse,
    EntityGateRequest as RemoteEntityGateRequest, EntityGateResponse as RemoteEntityGateResponse,
    Envelope, LookupRequest as RemoteLookupRequest, LookupResponse as RemoteLookupResponse,
    MAX_CONCURRENT_HEALTH_PROBES, OwnershipHandoffFailure, PeerResolver,
    PrepareForcedOwnershipRecoveryRequest as RemotePrepareForcedOwnershipRecoveryRequest,
    RemoteOperationFailure, RemoteOperationSubject, RuntimeErrorEvent as RemoteRuntimeErrorEvent,
    StreamHandlerError, StreamingResponse,
    SubscriptionInterestVisibilityRequest as RemoteSubscriptionInterestVisibilityRequest,
    SubscriptionInterestVisibilityResponse as RemoteSubscriptionInterestVisibilityResponse,
    Transport, TransportIdentity,
};
use nervix_models::{
    ClusterNodeName, DomainName, DomainStatus, ModelKind, NodeEndpoint, NodeServiceUrl, UserName,
};
use nervix_primitives::{
    collections::DashMap,
    net::TcpListener,
    sync::{StdArc, broadcast, watch},
    time::sleep,
};
use observability_http::serve_observability_http;
use ownership_handoff::{FORCED_OWNERSHIP_RECOVERY_BUDGET, ForcedOwnershipRecoveryCoordinator};
#[cfg(feature = "testing")]
use scheduling::VOTER_OBSERVATION_GRACE;
use scheduling::{
    KafkaPartitionWatcherKey, KafkaPartitionWatcherTask, LEADER_KAFKA_PARTITION_WATCH_INTERVAL,
    StartupVoterObservations,
};
use session::grpc::SessionGrpcService;
pub use session_service::SessionServiceImpl;
use session_service::{
    RuntimeStateApplication, SESSION_EVENT_CAPACITY, SessionEvents, SessionServiceInner,
    apply_current_cluster_runtime_state,
};
use startup::ApplicationStartup;
use subscription::{SubscriptionInterests, SubscriptionSampler};
use tls::{
    HttpsListenerCertificates, InterconnectTlsPaths, load_grpc_tls_server_config,
    load_web_console_tls_server_config, reload_interconnect_tls,
};
use web_console::{serve_web_console_http, serve_web_console_https, web_console_advertise_url};

use crate::{
    metrics::NodeObservations,
    registry::Registry,
    resource_interconnect::{
        FetchResourceArchive, PublishResourceReplica, ResourceInterconnectError,
    },
    runtime::{DescribeStateSnapshot, FetchStateSnapshot},
};

mod admitted_connection;
mod authentication;
mod background_task;
mod backup;
mod cli;
mod cli_completions;
mod cli_values;
mod client_consumers;
mod client_description;
mod client_producers;
mod cluster_description;
mod cluster_status;
mod command_execution;
mod command_result;
mod completion;
mod configured_choices;
mod describe_output;
mod domain_clock;
mod domain_lifecycle;
mod entity_gate;
mod error;
mod http_endpoint;
mod interconnect_relay;
mod join_supervision;
mod leader_redirect;
mod model_mutation;
mod model_validation;
mod node_identity;
mod node_settings;
mod observability_http;
mod observation;
mod ownership_handoff;
mod preflight;
mod relocation;
mod resource;
mod restore;
mod runtime_admission;
mod runtime_reconciliation;
mod schedule_planning;
mod scheduling;
mod service_tasks;
mod session;
#[cfg(test)]
pub(crate) use session::{ClockDeliveryOrder, NextClockFrame};
mod session_service;
mod shutdown;
mod startup;
mod state_replication_requests;
mod statement_routing;
mod subscription;
mod termination_signals;
#[cfg(test)]
pub(crate) mod test_fixtures;
mod tls;
mod tracing_setup;
mod transaction;
mod wasm_state_recovery;
mod wasm_state_reset;
mod web_console;

pub use command_execution::{
    CommandExecutionCapacity, CommandExecutionPolicy, CommandRetryValidity,
};
use join_supervision::{JoinOutcome, JoinStanding, JoinSupervision};
use nervix_primitives::sync::Arc;
pub use node_identity::NodeIdentity;
pub use node_settings::NodeSettings;
use preflight::{Preflight, PreflightOutcome};
pub use scheduling::{ReplicaCount, SchedulingSettings};
use service_tasks::ServiceTasks;
use shutdown::BeforeDeadline;
pub use shutdown::{
    DrainTimeout, ShutdownCoordinator, ShutdownDeadline, ShutdownOutcome, ShutdownPhase,
    ShutdownPhaseOutcome, ShutdownRequest, ShutdownRequestOutcome, ShutdownSettings,
    ShutdownSettingsError, ShutdownTimeout,
};
use tonic::transport::Server;
use tracing::{Instrument as _, debug, error, info, warn};
pub use transaction::{
    TransactionBounds, TransactionIdleTimeout, TransactionLimits, TransactionMaxOpen,
    TransactionMaxSourceBytes, TransactionMaxStatements, TransactionTombstoneRetention,
};
use typed_builder::TypedBuilder;

use crate::{
    ConfiguredFaultInjection, cluster,
    memory_pressure::MemoryPressureController,
    resource::ResourceStore,
    runtime::{
        EntityGateLease, OwnershipHandoffError, OwnershipHandoffResult, Runtime,
        RuntimeEnvironment, RuntimeEvent,
    },
};

#[cfg(not(test))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(test)]
#[global_allocator]
static GLOBAL: alloc_count::AllocCounter<tikv_jemallocator::Jemalloc> =
    alloc_count::AllocCounter(tikv_jemallocator::Jemalloc);

/// One live peer whose advertised interconnect endpoint this health round can reach.
struct ReachablePeer {
    node_id: ClusterNodeName,
    endpoint: NodeEndpoint,
}

/// One node: who it is and where it is reached, the settings it runs with, and the shutdown it
/// stops within.
#[derive(Debug, Clone, TypedBuilder)]
pub struct Application {
    pub identity: NodeIdentity,
    #[builder(default)]
    pub settings: NodeSettings,
    #[builder(default)]
    #[doc(hidden)]
    pub fault_injection: ConfiguredFaultInjection,
    /// Bound by the shutdown timeout the settings hold, so the two can never disagree.
    #[builder(
        default = ShutdownCoordinator::new(settings.shutdown.shutdown_timeout()),
        setter(skip)
    )]
    pub shutdown: ShutdownCoordinator,
    #[builder(default = true)]
    pub graceful_shutdown_drain: bool,
}

/// Runs this process as the server, from its own command line to its exit. The `nervix-server`
/// executable is this call, and a test process that re-executes itself under the server's name
/// makes the same call, so a real process in a test starts the way a deployed one does. No Shuttle
/// model runs a whole process and Shuttle's runtime has no stack size to set, so a Shuttle build has
/// no such entry point.
#[cfg(not(feature = "shuttle"))]
pub fn run_server_process() -> Result<(), Report<AppError>> {
    let args = Args::parse();
    // Registered before any other thread exists, so neither SIGINT nor SIGTERM can end the process
    // by its default action at any later point in its life.
    let termination_signals = TerminationSignals::register()?;
    // A diagnostic node installs its deadlock detector before any tracked lock or runtime worker
    // exists, and after the signals, so the detector's threads never see one by its default action.
    #[cfg(feature = "deloxide")]
    nervix_deadlock::DiagnosticRun::start_selected(
        args.diagnostics
            .deadlock_evidence
            .clone()
            .map(nervix_deadlock::EvidenceDirectory::new),
        nervix_primitives::deadlock::DiagnosticSelection::for_build(
            args.diagnostics.deadlock_active_only,
        ),
    )
    .change_context(AppError::StartDeadlockDiagnostics)?;
    let runtime = nervix_primitives::runtime::Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024) // Set custom stack size here
        .enable_all()
        .build()
        .change_context(AppError::BuildRuntime)?;

    let tracing_guard = {
        let _entered = runtime.enter();
        init_tracing(&args.tracing)?
    };
    runtime.block_on(run_cli(args, termination_signals, &tracing_guard))
}

/// Runs the server command line with termination signals the caller has already registered, so
/// neither signal can end the process by its default action while the application starts.
pub async fn run_cli(
    args: Args,
    termination_signals: TerminationSignals,
    tracing: &TracingGuard,
) -> Result<(), Report<AppError>> {
    if let Some(Command::Completions { shell }) = args.subcommand.clone() {
        print_completions(shell);
        return Ok(());
    }

    let application = Application::try_from(args)?;
    termination_signals.supervise(application.shutdown.clone())?;
    application.run_with_tracing(Some(tracing)).await
}

impl Application {
    pub async fn run(self) -> Result<(), Report<AppError>> {
        self.run_with_tracing(None).await
    }

    async fn run_with_tracing(
        self,
        tracing: Option<&TracingGuard>,
    ) -> Result<(), Report<AppError>> {
        let addr = self.identity.addr;
        let grpc_mode = self.identity.grpc_mode;
        let grpc_listen_addr = match grpc_mode {
            InternalTransportMode::Http => addr,
            InternalTransportMode::Https => self
                .identity
                .grpc_https_listen_addr
                .ok_or_else(|| Report::new(AppError::MissingGrpcHttpsListenAddress))?,
        };
        let http_listen_addr = self.identity.http_listen_addr;
        let https_listen_addr = self.identity.https_listen_addr;
        let observability_listen_addr = self.identity.observability_listen_addr;
        let web_console_listen_addr = self.identity.web_console_listen_addr;
        let web_console_https_listen_addr = self.identity.web_console_https_listen_addr;
        let web_console_advertise_url = web_console_advertise_url(
            self.identity.web_console_advertise_addr.clone(),
            web_console_listen_addr,
            web_console_https_listen_addr,
        )
        .change_context(AppError::BuildConsoleAdvertiseUrl)?;
        let graceful_shutdown_drain = self.graceful_shutdown_drain;
        let drain_timeout = self.settings.shutdown.drain_timeout();
        let cluster_id = self.identity.cluster_id.clone();
        let node_id = self.identity.node_id.clone();
        let grpc_advertise_addr = match grpc_mode {
            InternalTransportMode::Http => self.identity.grpc_advertise_addr.clone(),
            InternalTransportMode::Https => self
                .identity
                .grpc_https_advertise_addr
                .clone()
                .ok_or_else(|| Report::new(AppError::MissingGrpcHttpsAdvertiseAddress))?,
        };
        let grpc_advertise_url = NodeServiceUrl::new(grpc_mode.scheme(), &grpc_advertise_addr)
            .change_context(AppError::BuildClientAdvertiseUrl)?;
        let interconnect_listen_addr = self.identity.interconnect_listen_addr;
        let interconnect_advertise_addr = self.identity.interconnect_advertise_addr.clone();
        let interconnect_tls_paths = InterconnectTlsPaths {
            ca: self.identity.interconnect_tls_ca.clone(),
            certificate: self.identity.interconnect_tls_cert.clone(),
            private_key: self.identity.interconnect_tls_key.clone(),
        };
        let allow_bootstrap = self.identity.allow_bootstrap;
        let cluster_bootstrap_host = self.identity.cluster_bootstrap_host.clone();
        let default_user = self.identity.default_user.clone();
        let init_default_user_password = self.identity.init_default_user_password.clone();
        let settings = self.settings;
        let node_unavailability_timeout = settings.membership.node_unavailability_timeout();
        let raft_timings = settings.raft;
        let raft_retention = settings.raft_retention;
        let transaction_idle_timeout = settings.transactions.idle_timeout();
        let transaction_tombstone_retention = settings.transactions.tombstone_retention();
        let transaction_max_statements = settings.transactions.max_statements();
        let transaction_max_source_bytes = settings.transactions.max_source_bytes();
        let transaction_max_open = settings.transactions.max_open();
        let replica_count = settings.scheduling.replica_count();
        let runtime_settings = settings.runtime;
        let state_snapshot_interval = runtime_settings.state_snapshot_interval();
        let memory_pressure_controller = {
            let mapping_input = settings
                .memory_pressure
                .config()
                .map(MemoryPressureController::new)
                .transpose();
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    error!(
                        error = format!("{error:#}"),
                        "failed to initialize memory pressure monitor"
                    );
                    error.change_context(AppError::InitMemoryPressureMonitor)
                }),
            }
        }?;
        let db_path = self.identity.db_path.clone();
        let temp_dir = self.identity.temp_dir.clone();
        let resource_store_limits = settings.resources.limits();
        let shutdown_coordinator = self.shutdown.clone();
        let shutdown = shutdown_coordinator.drain_support_token();
        let fault_injection = self.fault_injection.clone();
        let grpc_tls_server_config = if grpc_mode.is_tls() {
            let config = match load_grpc_tls_server_config().await {
                Ok(config) => config,
                Err(error) => {
                    error!(?error, "failed to build grpc tls server config");
                    return Err(error);
                }
            };
            Some(config)
        } else {
            None
        };
        let web_console_tls_server_config = match (
            &self.identity.web_console_tls_cert,
            &self.identity.web_console_tls_key,
        ) {
            (Some(cert_path), Some(key_path)) => {
                let Some(_) = web_console_https_listen_addr else {
                    return Err(Report::new(AppError::MissingWebConsoleHttpsListenAddress));
                };
                let config = match load_web_console_tls_server_config(cert_path, key_path) {
                    Ok(config) => config,
                    Err(error) => {
                        error!(?error, "failed to build web console tls server config");
                        return Err(error);
                    }
                };
                Some(config)
            }
            (Some(_), None) => {
                return Err(Report::new(AppError::MissingWebConsoleTlsPrivateKey));
            }
            (None, Some(_)) => {
                return Err(Report::new(AppError::MissingWebConsoleTlsCertificate));
            }
            (None, None) => {
                if web_console_https_listen_addr.is_some() {
                    return Err(Report::new(AppError::MissingWebConsoleTlsCertificate));
                }
                None
            }
        };
        let dns_configuration = self.identity.dns.clone();
        let preflight = Preflight {
            settings: &settings,
            db_path: &db_path,
            temp_dir: &temp_dir,
            dns: &dns_configuration,
        }
        .run()
        .instrument(tracing::info_span!("startup_preflight", node_id = %node_id))
        .await;
        let PreflightOutcome {
            executor,
            dns,
            memory,
            restore_staging_max_bytes,
        } = match preflight {
            Ok(outcome) => outcome,
            Err(error) => {
                error!(
                    error = format!("{error:#}"),
                    "the startup preflight refused this node"
                );
                return Err(error.change_context(AppError::Preflight));
            }
        };
        let grpc_listener = TcpListener::bind(grpc_listen_addr)
            .await
            .change_context(AppError::BindGrpcListenAddress)?;
        let http_listener = TcpListener::bind(http_listen_addr)
            .await
            .change_context(AppError::BindHttpListenAddress)?;
        let https_listener = TcpListener::bind(https_listen_addr)
            .await
            .change_context(AppError::BindHttpsListenAddress)?;
        let observability_listener = TcpListener::bind(observability_listen_addr)
            .await
            .change_context(AppError::BindObservabilityListenAddress)?;
        let web_console_listener = TcpListener::bind(web_console_listen_addr)
            .await
            .change_context(AppError::BindWebConsoleListenAddress)?;
        let web_console_https_listener = match (
            web_console_https_listen_addr,
            web_console_tls_server_config.as_ref(),
        ) {
            (Some(addr), Some(_)) => Some(
                TcpListener::bind(addr)
                    .await
                    .change_context(AppError::BindWebConsoleHttpsListenAddress)?,
            ),
            _ => None,
        };
        let interconnect_tls_material = interconnect_tls_paths
            .read()
            .await
            .change_context(AppError::LoadInterconnectTls)?;
        let interconnect_tls_fingerprint = interconnect_tls_material.fingerprint();
        let interconnect_tls = interconnect_tls_material
            .tls_bundle()
            .change_context(AppError::LoadInterconnectTls)?;
        if let Some(tracing) = tracing {
            tracing.install_dns(&dns);
        }
        info!(
            resolver_configuration = %dns_configuration.resolver_configuration.display(),
            hosts_file = %dns_configuration.hosts_file.display(),
            name_servers = ?dns_configuration.name_servers,
            "loaded the name resolver configuration"
        );

        info!(
            grpc_mode = grpc_mode.scheme(),
            grpc_listen_addr = %grpc_listen_addr,
            grpc_advertise_addr = %grpc_advertise_url,
            http_listen_addr = %http_listen_addr,
            https_listen_addr = %https_listen_addr,
            observability_listen_addr = %observability_listen_addr,
            web_console_listen_addr = %web_console_listen_addr,
            interconnect_listen_addr = %interconnect_listen_addr,
            interconnect_advertise_addr = %interconnect_advertise_addr,
            allow_bootstrap,
            node_unavailability_timeout = ?node_unavailability_timeout,
            raft_heartbeat_interval = ?raft_timings.heartbeat_interval(),
            raft_election_timeout_min = ?raft_timings.election_timeout_min(),
            raft_election_timeout_max = ?raft_timings.election_timeout_max(),
            gossip_interval = ?settings.membership.gossip_interval(),
            clock_skew_tolerance = ?settings.membership.clock_skew_tolerance(),
            memory = %memory,
            replica_count,
            state_snapshot_interval = ?state_snapshot_interval,
            cluster_id,
            %node_id,
            bootstrap = cluster_bootstrap_host.as_deref().unwrap_or(""),
            db_path = db_path.display().to_string(),
            temp_dir = temp_dir.display().to_string(),
            resource_max_archive_bytes = resource_store_limits.max_archive_bytes,
            resource_max_extracted_bytes = resource_store_limits.max_extracted_bytes,
            resource_max_file_count = resource_store_limits.max_file_count,
            "starting nervix server"
        );

        let db = ApplicationStartup::open_node_database(db_path.clone(), &executor).await?;
        let registry = Arc::new(
            match Registry::from_database(db.clone(), Some(db_path.as_path())) {
                Ok(registry) => registry,
                Err(err) => {
                    error!(db_path = db_path.display().to_string(), error = %err, "failed to open registry");
                    return Err(err.change_context(AppError::OpenRegistry));
                }
            },
        );
        let runtime = {
            let mapping_input = Runtime::with_environment(
                RuntimeEnvironment {
                    executor,
                    dns: Some(dns.clone()),
                    db: Some(db.clone()),
                    restore_staging_max_bytes,
                    fault_injection: fault_injection.clone(),
                    temp_dir: temp_dir.clone(),
                },
                runtime_settings,
            );
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    error!(
                        error = format!("{error:#}"),
                        "failed to initialize runtime persistence"
                    );
                    error.change_context(AppError::OpenRuntimeState)
                }),
            }
        }?;
        let resource_store = StdArc::new({
            let mapping_input = ResourceStore::open_with_limits(
                db_path.join("resources"),
                runtime.executor().clone(),
                resource_store_limits,
            );
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(err) => Err({
                    error!(db_path = db_path.display().to_string(), error = %err, "failed to open resource store");
                    Report::new(AppError::OpenResourceStore)
                }),
            }
        }?);
        {
            let mapping_input = resource_store.cleanup_abandoned_staging().await;
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(err) => Err({
                    error!(db_path = db_path.display().to_string(), error = %err, "failed to clean resource staging paths");
                    Report::new(AppError::OpenResourceStore)
                }),
            }
        }?;
        let mut startup = ApplicationStartup {
            db,
            consensus_path: ApplicationStartup::consensus_database_path(&db_path),
            resource_store,
            registry,
            runtime,
            consensus: None,
            interconnect: None,
        };
        startup
            .runtime
            .attach_resource_store(startup.resource_store.clone());
        info!(
            runtime_state_store_enabled = startup.runtime.has_state_store(),
            runtime_state_snapshot_interval = ?startup.runtime.state_snapshot_interval(),
            "initialized runtime persistence"
        );
        if let Err(error) = startup.apply_stored_runtime_changes().await {
            error!(
                error = format!("{error:#}"),
                "failed to apply startup runtime changes"
            );
            startup.terminate().await;
            return Err(error);
        }
        let interconnect_result = Transport::bind(
            interconnect_listen_addr,
            TransportIdentity {
                cluster_id: cluster_id.clone(),
                node_id: node_id.clone(),
                advertised_host: interconnect_advertise_addr.host().to_string(),
                join_terms: settings.join_terms(),
            },
            interconnect_tls,
            Default::default(),
            startup.runtime.executor().clone(),
            PeerResolver::new(dns),
        )
        .await
        .change_context(AppError::StartInterconnect);
        let (interconnect, interconnect_rx) = match interconnect_result {
            Ok(interconnect) => interconnect,
            Err(error) => {
                startup.terminate().await;
                return Err(error);
            }
        };
        startup.interconnect = Some(interconnect.clone());

        let consensus_settings = ConsensusSettings {
            cluster_name: cluster_id.clone(),
            node_id: node_id.clone(),
            interconnect_advertise_addr: interconnect_advertise_addr.clone(),
            interconnect: interconnect.clone(),
            executor: startup.runtime.executor().clone(),
            raft_timings,
            raft_retention,
        };
        let consensus_result = startup
            .open_consensus(consensus_settings, &fault_injection)
            .await;
        let consensus = match consensus_result {
            Ok(consensus) => consensus,
            Err(error) => {
                startup.terminate().await;
                return Err(error);
            }
        };
        let recovered_members = consensus.observer().stored_membership_nodes();
        startup.consensus = Some(consensus);
        let join_standing = if recovered_members.contains_key(&node_id) {
            JoinStanding::RecoveredMember
        } else {
            JoinStanding::Newcomer
        };
        let mut recovered_endpoints = BTreeMap::new();
        for (member, address) in recovered_members {
            if member == node_id {
                continue;
            }
            let endpoint = match address.parse::<NodeEndpoint>() {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    let report = Report::new(error)
                        .change_context(AppError::StartCluster)
                        .attach_printable(format!("persisted Raft endpoint for node '{member}'"));
                    startup.terminate().await;
                    return Err(report);
                }
            };
            recovered_endpoints.insert(member, endpoint);
        }
        let has_bootstrap_candidates =
            cluster_bootstrap_host.is_some() || !recovered_endpoints.is_empty();
        let cluster_result = cluster::start_cluster(cluster::ClusterSettings {
            cluster_id,
            node_id: node_id.clone(),
            grpc_listen_addr,
            client_advertise_url: grpc_advertise_url.clone(),
            console_advertise_url: web_console_advertise_url.clone(),
            interconnect_advertise_addr,
            bootstrap_host: cluster_bootstrap_host.clone(),
            recovered_members: recovered_endpoints,
            interconnect: interconnect.clone(),
            membership: settings.membership,
            prerequisites: interconnect.local_prerequisites(),
            fault_injection: fault_injection.clone(),
        })
        .await
        .change_context(AppError::StartCluster);
        let cluster = match cluster_result {
            Ok(cluster) => cluster,
            Err(error) => {
                startup.terminate().await;
                return Err(error);
            }
        };
        let cluster = Arc::new(cluster);
        let local_health_identity = cluster.local_node_identity().await;
        #[cfg(feature = "testing")]
        let health_fault_injection = fault_injection.clone();
        let health_handler = interconnect.register_handler::<ApplicationHealthProbe, _, _>({
            move |context, _request| {
                let local_health_identity = local_health_identity.clone();
                #[cfg(feature = "testing")]
                let health_fault_injection = health_fault_injection.clone();
                async move {
                    #[cfg(feature = "testing")]
                    health_fault_injection
                        .pause_health_response_if_armed(
                            context.peer_node_id(),
                            local_health_identity.node_id(),
                        )
                        .await;
                    #[cfg(not(feature = "testing"))]
                    drop(context);
                    #[cfg(feature = "testing")]
                    let local_health_identity = health_fault_injection
                        .health_response_identity(context.peer_node_id(), local_health_identity);
                    local_health_identity
                }
            }
        });
        let startup = startup
            .require_handler_registration(&cluster, health_handler)
            .await?;
        let application_revision_handler = completion::register_application_revision_handler(
            cluster.clone(),
            &interconnect,
            #[cfg(feature = "testing")]
            fault_injection.clone(),
        );
        let startup = startup
            .require_handler_registration(&cluster, application_revision_handler)
            .await?;
        let completion_peers_handler = completion::register_completion_peers_handler(
            cluster.clone(),
            startup
                .consensus
                .as_ref()
                .verified("startup assigns consensus before registering completion handlers")
                .observer(),
            &interconnect,
        );
        let startup = startup
            .require_handler_registration(&cluster, completion_peers_handler)
            .await?;
        let clock_probe_handler = cluster::register_clock_probe_handler(&interconnect);
        let startup = startup
            .require_handler_registration(&cluster, clock_probe_handler)
            .await?;
        let ApplicationStartup {
            db,
            consensus_path: _,
            resource_store,
            registry,
            runtime,
            consensus,
            interconnect,
        } = startup;
        let consensus =
            consensus.verified("startup assigns this handle before it reaches this point");
        let interconnect =
            interconnect.verified("startup assigns this handle before it reaches this point");
        let mut interconnect_rx = interconnect_rx;
        let runtime_admission = Arc::new(runtime_admission::RuntimeAdmission::new());
        runtime.attach_remote_dispatcher(cluster.clone(), interconnect.clone());
        let node_observations = NodeObservations::new(
            runtime.executor().clone(),
            interconnect.clone(),
            consensus.observer(),
            cluster.prerequisites_view(),
        );
        runtime
            .metrics()
            .install_node_observations(node_observations.clone());
        let mut background_tasks = Vec::new();
        let scheduler_delay_shutdown = shutdown.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            node_observations
                .sample_scheduler_delay(scheduler_delay_shutdown)
                .await;
        }));
        #[cfg(feature = "testing")]
        fault_injection.register_executor(node_id.clone(), runtime.executor().clone());
        #[cfg(feature = "testing")]
        let scheduler_mode = runtime.scheduler_mode();

        let cluster_for_reconcile = cluster.clone();
        #[cfg(feature = "testing")]
        let fault_injection_for_reconcile = fault_injection.clone();
        let consensus_for_reconcile = consensus.proposer();
        let registry_for_reconcile = registry.clone();
        let runtime_for_reconcile = runtime.clone();
        let interconnect_for_reconcile = interconnect.clone();
        let local_node_for_reconcile = node_id.clone();
        let reconcile_shutdown = shutdown.clone();
        let cluster_for_membership_reconcile = cluster.clone();
        let administrator_for_membership_reconcile = consensus.administrator();
        let membership_reconcile_shutdown = shutdown.clone();
        let interconnect_tls_transport = interconnect.clone();
        let interconnect_tls_shutdown = shutdown.clone();
        let interconnect_tls_cluster = cluster.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            reload_interconnect_tls(
                interconnect_tls_transport,
                interconnect_tls_cluster,
                interconnect_tls_paths,
                interconnect_tls_fingerprint,
                interconnect_tls_shutdown,
            )
            .await;
        }));
        let (join_refusal_sender, join_refusal) = watch::channel(None);
        if has_bootstrap_candidates {
            let supervision = JoinSupervision {
                transport: interconnect.clone(),
                cluster: cluster.clone(),
                observer: consensus.observer(),
                local_node: node_id.clone(),
                standing: join_standing,
                clock_skew_tolerance: settings.membership.clock_skew_tolerance(),
                probe_retry_interval: settings.membership.gossip_interval(),
            };
            let join_shutdown = shutdown.clone();
            let join_coordinator = shutdown_coordinator.clone();
            background_tasks.push(nervix_primitives::task::spawn(async move {
                let outcome = supervision.run(join_shutdown).await;
                let JoinOutcome::Refused(refusal) = outcome else {
                    return;
                };
                error!(refusal = %refusal, "this node was refused at join and stops");
                join_refusal_sender.send_replace(Some(refusal));
                match join_coordinator.request_stop() {
                    ShutdownRequestOutcome::Accepted(_) => {}
                    ShutdownRequestOutcome::AlreadyRequested(_) => {
                        debug!("the node was already stopping when its join was refused");
                    }
                }
            }));
        }
        if let Some(controller) = memory_pressure_controller {
            let memory_runtime = runtime.clone();
            let memory_shutdown = shutdown.clone();
            background_tasks.push(nervix_primitives::task::spawn(async move {
                controller.run(memory_runtime, memory_shutdown).await;
            }));
        }
        background_tasks.push(nervix_primitives::task::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            let mut initialized = false;
            loop {
                nervix_primitives::task::consume_budget().await;
                if membership_reconcile_shutdown.is_cancelled() {
                    break;
                }
                if allow_bootstrap && !initialized {
                    match administrator_for_membership_reconcile
                        .maybe_initialize()
                        .await
                    {
                        Ok(did_initialize) => {
                            initialized = did_initialize;
                        }
                        Err(error) => {
                            warn!(%error, "raft bootstrap attempt failed");
                        }
                    }
                }
                if let Err(error) = administrator_for_membership_reconcile
                    .reconcile_nodes(cluster_for_membership_reconcile.gossip_state())
                    .await
                {
                    warn!(%error, "raft membership reconciliation failed");
                }
                // Published whether or not a membership change failed: the verdicts were judged
                // before any change was attempted, and a node that stopped leading withdraws them.
                let verdicts = administrator_for_membership_reconcile.admission_verdicts();
                cluster_for_membership_reconcile
                    .publish_admission_verdicts(verdicts.as_ref())
                    .await;
                nervix_primitives::select! {
                    _ = membership_reconcile_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        #[cfg(feature = "testing")]
        {
            let mut leadership_transfer_rx = runtime.subscribe_leadership_transfers();
            let consensus_for_leadership_transfer = consensus.administrator();
            let leadership_transfer_shutdown = shutdown.clone();
            let leadership_transfer_local_node_id = node_id.clone();
            background_tasks.push(nervix_primitives::task::spawn(async move {
                loop {
                    nervix_primitives::task::consume_budget().await;
                    nervix_primitives::select! {
                        _ = leadership_transfer_shutdown.cancelled() => break,
                        request = leadership_transfer_rx.recv() => {
                            match request {
                                Ok(request)
                                    if request.from_node_id == leadership_transfer_local_node_id =>
                                {
                                    if let Err(error) = consensus_for_leadership_transfer
                                        .transfer_leadership_to(request.to_node_id.clone())
                                        .await
                                    {
                                        warn!(
                                            from_node_id = %request.from_node_id,
                                            to_node_id = %request.to_node_id,
                                            error = %error,
                                            "test leadership transfer request failed"
                                        );
                                    }
                                }
                                Ok(_) => {}
                                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                    warn!(
                                        skipped,
                                        "test leadership transfer request receiver lagged"
                                    );
                                }
                                Err(broadcast::error::RecvError::Closed) => break,
                            }
                        }
                    }
                }
            }));
        }
        background_tasks.push(nervix_primitives::task::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            #[cfg(feature = "testing")]
            {
                let observation = fault_injection_for_reconcile
                    .wait_for_initial_voter_heartbeat_if_armed(consensus_for_reconcile.local_node_id());
                if reconcile_shutdown.run_until_cancelled(observation).await.is_none() {
                    return;
                }
            }
            let reconcile_started = nervix_primitives::time::Instant::now();
            let mut startup_voter_observations = StartupVoterObservations::default();
            let mut default_user_resolved = false;
            let mut missing_init_default_user_password_warned = false;
            loop {
                nervix_primitives::task::consume_budget().await;
                // A pass waits on consensus writes and on requests to peers, and neither completes
                // once the peers have stopped. The pass therefore ends with drain support rather
                // than holding terminal teardown until the grace period aborts the whole task.
                let reconcile_pass = async {
                    // Keep process-local live observations through follower passes and leadership
                    // changes. A dead verdict alone can also mean an initial relayed heartbeat.
                    startup_voter_observations.observe(
                        &cluster_for_reconcile.gossip_state().await,
                        reconcile_started.elapsed(),
                    );
                    if consensus_for_reconcile.current_leader().await.as_ref()
                        != Some(consensus_for_reconcile.local_node_id())
                    {
                        return;
                    }
                    let orphaned_alter_committing_domains = consensus_for_reconcile
                        .current_transactions()
                        .await
                        .into_values()
                        .filter(|transaction| {
                            matches!(transaction.state, TransactionState::Committing(_))
                        })
                        .map(|transaction| transaction.domain)
                        .collect::<HashSet<_>>();
                    for (domain, state) in consensus_for_reconcile.current_domains().await {
                        if let DomainStatus::Paused = state.status
                            && !orphaned_alter_committing_domains.contains(&domain)
                            && !runtime_for_reconcile.domain_alter_is_active(&domain)
                        {
                            match consensus_for_reconcile
                                .resume_domain(domain.clone(), None)
                                .await
                            {
                                Ok(()) => {
                                    info!(
                                        domain = domain.as_str(),
                                        "resumed orphaned ALTER pause"
                                    );
                                }
                                Err(error) => {
                                    warn!(
                                        domain = domain.as_str(),
                                        error = %error,
                                        "failed to resume orphaned ALTER pause"
                                    );
                                }
                            }
                        }
                    }
                    if !default_user_resolved {
                        match UserName::parse(&default_user) {
                            Ok(default_user_id)
                                if consensus_for_reconcile
                                    .current_user(&default_user_id)
                                    .await
                                    .is_none() =>
                            {
                                if let Some(password) = init_default_user_password.clone() {
                                    match user_credentials(
                                        runtime_for_reconcile.executor(),
                                        default_user_id.clone(),
                                        password,
                                    )
                                    .await
                                    {
                                        Ok(user) => {
                                            let user_name = user.name.clone();
                                            if let Err(error) =
                                                consensus_for_reconcile.create_user(user).await
                                            {
                                                warn!(
                                                    error = %format_args!("{error:#}"),
                                                    "failed to create configured default user"
                                                );
                                            } else {
                                                default_user_resolved = true;
                                                info!(
                                                    user = user_name.as_str(),
                                                    "created configured default user"
                                                );
                                            }
                                        }
                                        Err(error) => {
                                            warn!(
                                                error = %format_args!("{error:#}"),
                                                "failed to prepare configured default user"
                                            );
                                            default_user_resolved = true;
                                        }
                                    }
                                } else if !missing_init_default_user_password_warned {
                                    warn!(
                                        user = default_user.as_str(),
                                        "default user is not configured; set \
                                         --init-default-user-password or \
                                         NERVIX_INIT_DEFAULT_USER_PASSWORD before first startup"
                                    );
                                    missing_init_default_user_password_warned = true;
                                }
                            }
                            Ok(_) => {
                                default_user_resolved = true;
                            }
                            Err(error) => {
                                if !missing_init_default_user_password_warned {
                                    warn!(
                                        user = default_user.as_str(),
                                        error = %format_args!("{error:#}"),
                                        "configured default user name is invalid"
                                    );
                                    missing_init_default_user_password_warned = true;
                                }
                            }
                        }
                    }
                    loop {
                        nervix_primitives::task::consume_budget().await;
                        let Ok(automatic_schedule_input) =
                            consensus_for_reconcile.automatic_schedule_input().await
                        else {
                            break;
                        };
                        let committing_domains = consensus_for_reconcile
                            .current_transactions()
                            .await
                            .into_values()
                            .filter(|transaction| {
                                matches!(transaction.state, TransactionState::Committing(_))
                            })
                            .map(|transaction| transaction.domain)
                            .collect::<HashSet<_>>();
                        let health_snapshot = cluster_for_reconcile.peer_health_snapshot();
                        let health_scheduling_revision = health_snapshot.scheduling_revision();
                        let scheduling_availability =
                            cluster_for_reconcile.availability_state().await;
                        let live_node_ids = scheduling_availability.live_node_ids();
                        let placement_candidate_node_ids =
                            scheduling_availability.placement_candidate_node_ids();
                        let mut live_node_incarnations = BTreeMap::new();
                        for node in &scheduling_availability.live_nodes {
                            if scheduling_availability
                                .dead_node_ids
                                .contains(&node.node_id)
                            {
                                continue;
                            }
                            live_node_incarnations
                                .insert(node.node_id.clone(), node.incarnation);
                        }
                        let topology = automatic_schedule_input.topology();
                        // A leader that has just started cannot yet tell a voter that is still
                        // starting from one that failed, and treating it as failed would move its
                        // work without the state it holds. Wait for a live observation of every
                        // voter in this process, bounded by the startup grace. Chitchat's initial
                        // dead verdict from one relayed sample cannot satisfy that observation.
                        let observation_elapsed = reconcile_started.elapsed();
                        startup_voter_observations
                            .observe(&scheduling_availability, observation_elapsed);
                        let unobserved_voters = startup_voter_observations
                            .unobserved_voters(topology.voters(), observation_elapsed);
                        #[cfg(feature = "testing")]
                        if observation_elapsed < VOTER_OBSERVATION_GRACE {
                            fault_injection_for_reconcile
                                .pause_startup_voter_observation_if_armed(
                                    consensus_for_reconcile.local_node_id(),
                                    &scheduling_availability.dead_node_ids,
                                )
                                .await;
                        }
                        if !unobserved_voters.is_empty() {
                            debug!(
                                unobserved_voters = unobserved_voters.len(),
                                "automatic scheduling waits for gossip to observe every voter"
                            );
                            break;
                        }
                        #[cfg(feature = "testing")]
                        fault_injection_for_reconcile.end_startup_voter_observation();
                        let live_voters = live_node_ids
                            .into_iter()
                            .filter(|node| topology.voters().contains(node))
                            .collect::<Vec<_>>();
                        let schedulable_node_ids = placement_candidate_node_ids
                            .into_iter()
                            .filter(|node| {
                                topology.voters().contains(node)
                                    && !topology.cordoned().contains(node)
                            })
                            .collect::<Vec<_>>();
                        let current_schedule = &automatic_schedule_input.runtime_state().schedule;
                        let live_voter_set = live_voters.iter().cloned().collect::<BTreeSet<_>>();
                        let schedulable_node_set = schedulable_node_ids
                            .iter()
                            .cloned()
                            .collect::<BTreeSet<_>>();
                        let active_graphs = registry_for_reconcile.active_graphs();
                        let active_domains = active_graphs
                            .iter()
                            .map(|(domain, _)| domain.clone())
                            .collect::<HashSet<_>>();
                        let mut automatic_decision_selected = false;
                        let mut automatic_decision_published = false;
                        for domain_schedule in current_schedule.domains.values() {
                            nervix_primitives::task::consume_budget().await;
                            if active_domains.contains(&domain_schedule.domain)
                                || committing_domains.contains(&domain_schedule.domain)
                                || runtime_for_reconcile
                                    .domain_alter_is_active(&domain_schedule.domain)
                            {
                                continue;
                            }
                            let mut failover_schedule = domain_schedule.clone();
                            let failover_moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
                                &mut failover_schedule,
                                None,
                                &live_voter_set,
                                &schedulable_node_set,
                            );
                            if failover_moves.is_empty() {
                                continue;
                            }
                            for failover_move in &failover_moves {
                                if let Some(replica) = failover_move.promoted_replica.as_ref() {
                                    info!(
                                        domain = domain_schedule.domain.as_str(),
                                        node = failover_move.label,
                                        promoted_replica = %replica,
                                        "failover promoted live replica to primary"
                                    );
                                } else if let Some(fallback_node) =
                                    failover_move.fallback_node.as_ref()
                                {
                                    warn!(
                                        domain = domain_schedule.domain.as_str(),
                                        node = failover_move.label,
                                        %fallback_node,
                                        "failover found no live replica; moving scheduled node without \
                                         local replicated state"
                                    );
                                }
                            }
                            ForcedOwnershipRecoveryCoordinator {
                                runtime: &runtime_for_reconcile,
                                interconnect: &interconnect_for_reconcile,
                                local_node_id: &local_node_for_reconcile,
                                node_incarnations: &live_node_incarnations,
                            }
                            .prepare_schedule(domain_schedule, &mut failover_schedule)
                            .await;
                            automatic_decision_selected = true;
                            if !cluster_for_reconcile.peer_health_scheduling_revision_is_current(
                                health_scheduling_revision,
                            )
                            {
                                break;
                            }
                            let current_availability =
                                cluster_for_reconcile.availability_state().await;
                            if !schedule_planning::DomainSchedulePlanningSnapshot::same_eligibility(
                                &scheduling_availability,
                                &current_availability,
                                automatic_schedule_input.topology().voters(),
                                automatic_schedule_input.topology().cordoned(),
                            ) {
                                break;
                            }
                            match consensus_for_reconcile
                                .apply_automatic_domain_schedule(
                                    automatic_schedule_input.fence(),
                                    automatic_schedule_input
                                        .planning_inputs(&domain_schedule.domain)
                                        .cloned()
                                        .verified(
                                            "the automatic runtime snapshot and planning inputs \
                                             contain the same domains",
                                        ),
                                    Some(failover_schedule),
                                )
                                .await
                            {
                                Ok(()) => automatic_decision_published = true,
                                Err(error) => {
                                    warn!(%error, "failed to republish domain schedule after node failover");
                                }
                            }
                            break;
                        }
                        if !automatic_decision_selected {
                            for (domain, graph) in active_graphs {
                                nervix_primitives::task::consume_budget().await;
                                if committing_domains.contains(&domain)
                                    || runtime_for_reconcile.domain_alter_is_active(&domain)
                                {
                                    continue;
                                }
                                let Some(domain_state) =
                                    automatic_schedule_input.runtime_state().domains.get(&domain)
                                else {
                                    continue;
                                };
                                #[cfg(feature = "testing")]
                                let mut schedule = graph.schedule_for_domain_with_mode(
                                    &domain,
                                    &schedulable_node_ids,
                                    replica_count,
                                    domain_state.config.placement,
                                    scheduler_mode,
                                );
                                #[cfg(not(feature = "testing"))]
                                let mut schedule = graph.schedule_for_domain(
                                    &domain,
                                    &schedulable_node_ids,
                                    replica_count,
                                    domain_state.config.placement,
                                );
                                let current_domain = current_schedule.domain(&domain);
                                let mut failover_existing = current_domain.cloned();
                                if let Some(existing) = &mut failover_existing {
                                    let failover_moves =
                                        SessionServiceImpl::failover_unavailable_scheduled_nodes(
                                            existing,
                                            Some(&schedule),
                                            &live_voter_set,
                                            &schedulable_node_set,
                                        );
                                    for failover_move in &failover_moves {
                                        if let Some(replica) = failover_move.promoted_replica.as_ref() {
                                            info!(
                                                domain = domain.as_str(),
                                                node = failover_move.label,
                                                promoted_replica = %replica,
                                                "failover promoted live replica to primary"
                                            );
                                        } else if let Some(fallback_node) =
                                            failover_move.fallback_node.as_ref()
                                        {
                                            warn!(
                                                domain = domain.as_str(),
                                                node = failover_move.label,
                                                %fallback_node,
                                                "failover found no live replica; moving scheduled node \
                                                 without local replicated state"
                                            );
                                        }
                                    }
                                }
                                SessionServiceImpl::merge_existing_schedule_data(
                                    &mut schedule,
                                    failover_existing.as_ref(),
                                    &live_voters,
                                );
                                if current_domain == Some(&schedule) {
                                    continue;
                                }
                                if let Some(current_domain) = current_domain {
                                    ForcedOwnershipRecoveryCoordinator {
                                        runtime: &runtime_for_reconcile,
                                        interconnect: &interconnect_for_reconcile,
                                        local_node_id: &local_node_for_reconcile,
                                        node_incarnations: &live_node_incarnations,
                                    }
                                    .prepare_schedule(current_domain, &mut schedule)
                                    .await;
                                }
                                if !cluster_for_reconcile.peer_health_scheduling_revision_is_current(
                                    health_scheduling_revision,
                                )
                                {
                                    break;
                                }
                                let current_availability =
                                    cluster_for_reconcile.availability_state().await;
                                if !schedule_planning::DomainSchedulePlanningSnapshot::same_eligibility(
                                    &scheduling_availability,
                                    &current_availability,
                                    automatic_schedule_input.topology().voters(),
                                    automatic_schedule_input.topology().cordoned(),
                                ) {
                                    break;
                                }
                                match consensus_for_reconcile
                                    .apply_automatic_domain_schedule(
                                        automatic_schedule_input.fence(),
                                        automatic_schedule_input
                                            .planning_inputs(&domain)
                                            .cloned()
                                            .verified(
                                                "the automatic runtime snapshot and planning \
                                                 inputs contain the same domains",
                                            ),
                                        Some(schedule),
                                    )
                                    .await
                                {
                                    Ok(()) => automatic_decision_published = true,
                                    Err(error) => {
                                        warn!(%error, "failed to republish domain schedule after membership or schedulability change");
                                    }
                                }
                                break;
                            }
                        }
                        if !automatic_decision_published {
                            break;
                        }
                    }
                };
                let Some(()) = reconcile_shutdown.run_until_cancelled(reconcile_pass).await else {
                    break;
                };
                #[cfg(feature = "testing")]
                fault_injection_for_reconcile
                    .finish_startup_voter_observation(consensus_for_reconcile.local_node_id());
                nervix_primitives::select! {
                    _ = reconcile_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        let interconnect_for_health = interconnect.clone();
        let cluster_for_health = cluster.clone();
        let mut health_topology = cluster.subscribe_live_node_states().await;
        let local_node_id = node_id;
        let mut awaiting_initial_bootstrap_peer = has_bootstrap_candidates;
        let health_shutdown = shutdown.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            loop {
                nervix_primitives::task::consume_budget().await;
                if health_shutdown.is_cancelled() {
                    break;
                }
                drop(health_topology.borrow_and_update());
                let gossip = cluster_for_health.gossip_state().await;
                let mut probed_nodes = gossip
                    .live_nodes
                    .into_iter()
                    .map(|node| (node.node_id.clone(), node))
                    .collect::<BTreeMap<_, _>>();
                let previous_health = cluster_for_health.peer_health_snapshot();
                for (node_id, node) in cluster_for_health.known_nodes().await {
                    if !probed_nodes.contains_key(&node_id)
                        && previous_health.retains_known_node(&node)
                    {
                        probed_nodes.insert(node_id, node);
                    }
                }
                let live_node_ids = probed_nodes.keys().cloned().collect::<BTreeSet<_>>();
                // A peer whose interconnect endpoint has never been discovered is neither
                // observable nor reachable. A previously established target stays reachable
                // while application health determines whether a gossip miss is a real failure.
                let mut reachable_peers = Vec::new();
                let mut health_endpoints = Vec::new();
                for node in probed_nodes.into_values() {
                    if node.node_id == local_node_id {
                        continue;
                    }
                    let identity = node.identity();
                    let Some(endpoint) = node.interconnect_endpoint else {
                        continue;
                    };
                    health_endpoints.push(cluster::PeerHealthEndpoint::new(
                        identity,
                        endpoint.clone(),
                    ));
                    reachable_peers.push(ReachablePeer {
                        node_id: node.node_id,
                        endpoint,
                    });
                }
                let health_targets = cluster_for_health
                    .replace_peer_health_endpoints(health_endpoints)
                    .into_iter()
                    .map(|target| (target.node_id().clone(), target))
                    .collect::<BTreeMap<_, _>>();

                // Every peer is dialled at the endpoint it advertised. The transport resolves that
                // endpoint for each connection attempt, inside the attempt's setup deadline, so a
                // name that does not resolve fails the probe as an unreachable peer would.
                let mut outbound_targets = BTreeMap::new();
                let mut scheduled_probes = Vec::new();
                for peer in reachable_peers {
                    let Some(health_target) = health_targets.get(&peer.node_id).cloned() else {
                        continue;
                    };
                    outbound_targets.insert(peer.node_id, peer.endpoint);
                    scheduled_probes.push(health_target);
                }
                if !outbound_targets.is_empty() {
                    awaiting_initial_bootstrap_peer = false;
                }
                if !awaiting_initial_bootstrap_peer {
                    interconnect_for_health.replace_live_nodes(&live_node_ids);
                    interconnect_for_health.replace_outbound_targets(&outbound_targets);
                }

                let probe_results = stream::iter(scheduled_probes.into_iter().map(|target| {
                    let interconnect = interconnect_for_health.clone();
                    async move {
                        let outcome = match interconnect
                            .request(target.node_id(), ApplicationHealthProbe)
                            .await
                        {
                            Ok(identity) if &identity == target.identity() => {
                                cluster::PeerHealthProbeOutcome::Healthy(identity)
                            }
                            Ok(identity) => {
                                tracing::debug!(
                                    ?target,
                                    ?identity,
                                    "application health probe returned a different identity"
                                );
                                cluster::PeerHealthProbeOutcome::Failure
                            }
                            Err(error) if error.current_context().is_capacity_exhaustion() => {
                                cluster::PeerHealthProbeOutcome::CapacityExhausted
                            }
                            Err(error) => {
                                tracing::debug!(
                                    ?target,
                                    error = %format_args!("{error:#}"),
                                    "application health probe failed"
                                );
                                cluster::PeerHealthProbeOutcome::Failure
                            }
                        };
                        cluster::PeerHealthProbeResult::new(
                            target,
                            outcome,
                            nervix_primitives::time::Instant::now(),
                        )
                    }
                }))
                .buffer_unordered(MAX_CONCURRENT_HEALTH_PROBES);
                tokio::pin!(probe_results);
                // Let each bounded probe finish even when gossip publishes another update.
                // Cancelling the round on every update can indefinitely hide an unreachable
                // peer's one-second timeout. Publication checks the current incarnation and
                // endpoint, so a result superseded by discovery is still discarded.
                loop {
                    nervix_primitives::task::consume_budget().await;
                    nervix_primitives::select! {
                        _ = health_shutdown.cancelled() => return,
                        result = probe_results.next() => match result {
                            Some(result) => {
                                cluster_for_health.record_peer_health_result(result).await;
                            }
                            None => break,
                        }
                    }
                }
                nervix_primitives::select! {
                    _ = health_shutdown.cancelled() => break,
                    changed = health_topology.changed() => {
                        changed.assured(
                            "the cluster handle retains its Chitchat state sender for the server lifetime",
                        );
                    }
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        background_tasks.push(completion::spawn_authoritative_revision_reporting(
            cluster.clone(),
            consensus.observer(),
            shutdown.clone(),
        ));

        let certificates =
            HttpsListenerCertificates::new(&resource_store, &fault_injection, &cluster);
        certificates
            .register_installation_handler(&interconnect)
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let certificates_for_schedule = certificates.clone();
        let runtime_for_schedule = runtime.clone();
        let registry_for_schedule = registry.clone();
        let schedule_rx = consensus.observer().subscribe_schedule();
        let domains_rx = consensus.observer().subscribe_domains();
        let consensus_for_schedule = consensus.observer();
        let cluster_for_schedule = cluster.clone();
        let interconnect_for_schedule = interconnect.clone();
        let schedule_shutdown = shutdown.clone();
        let schedule_runtime_admission = runtime_admission.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            let application = RuntimeStateApplication {
                runtime: &runtime_for_schedule,
                https_certificates: &certificates_for_schedule,
                cluster: &cluster_for_schedule,
                interconnect: &interconnect_for_schedule,
                registry: &registry_for_schedule,
                consensus: &consensus_for_schedule,
                admission: &schedule_runtime_admission,
                shutdown: &schedule_shutdown,
            };
            runtime_reconciliation::RuntimeReconciliation::new(
                schedule_rx,
                domains_rx,
                schedule_shutdown.clone(),
            )
            .run(|| apply_current_cluster_runtime_state(application))
            .await;
        }));
        let events = SessionEvents::new(SESSION_EVENT_CAPACITY);
        let service = SessionServiceImpl {
            inner: Arc::new(SessionServiceInner {
                cluster: cluster.clone(),
                consensus: consensus.proposer(),
                consensus_administrator: consensus.administrator(),
                registry,
                resource_store,
                https_certificates: certificates,
                runtime: runtime.clone(),
                runtime_admission: runtime_admission.clone(),
                replica_count,
                admission_shutdown: shutdown_coordinator.admission_token(),
                drain_support_shutdown: shutdown.clone(),
                events: events.clone(),
                subscription_interests: SubscriptionInterests::new(
                    cluster.clone(),
                    runtime.metrics(),
                ),
                subscription_sampler: SubscriptionSampler::default(),
                interconnect: interconnect.clone(),
                client_producers: ClientProducerRouter::new(
                    runtime.clone(),
                    interconnect.clone(),
                    consensus.proposer().local_node_id().clone(),
                ),
                client_consumers: ClientConsumerRouter::new(
                    runtime.clone(),
                    interconnect.clone(),
                    consensus.proposer().local_node_id().clone(),
                ),
                service_tasks: ServiceTasks::default(),
                auth_rate_limiter: SessionServiceImpl::new_auth_rate_limiter(),
                failed_auth_rate_limit_keys: DashMap::with_hasher(RandomState::new()),
                transaction_idle_timeout,
                transaction_tombstone_retention,
                transaction_max_statements,
                transaction_max_source_bytes,
                transaction_max_open,
                transaction_bindings: DashMap::with_hasher(RandomState::new()),
                command_execution_policy: settings.commands,
                command_executions: Default::default(),
                transaction_executions: DashMap::with_hasher(RandomState::new()),
                transaction_recovery: Default::default(),
                ownership_handoff_operations: nervix_primitives::sync::Mutex::new(()),
                resource_upload_executions: DashMap::with_hasher(RandomState::new()),
                resource_replication_executions: DashMap::with_hasher(RandomState::new()),
                retained_backups: Default::default(),
                captured_backup_sections: DashMap::with_hasher(RandomState::new()),
                restored_state_uploads: DashMap::with_hasher(RandomState::new()),
                restore_archives: Default::default(),
            }),
        };
        #[cfg(feature = "testing")]
        service.register_wasm_state_reset_test_coordinator(&fault_injection, shutdown.clone());
        let domain_clock_progress_service = service.clone();
        interconnect
            .register_handler::<RemoteDomainClockProgressRequest, _, _>(move |context, request| {
                let service = domain_clock_progress_service.clone();
                async move {
                    service.handle_domain_clock_progress(context.peer_node_id(), request);
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let resource_archive_service = service.clone();
        interconnect
            .register_stream_handler::<FetchResourceArchive, _, _>(move |_context, request| {
                let service = resource_archive_service.clone();
                async move {
                    let reader = service
                        .inner
                        .resource_store
                        .open_archive(&request.id)
                        .await
                        .map_err(StreamHandlerError::with_cause)?;
                    let archive_bytes = reader.archive_bytes();
                    let chunks = stream::unfold(Some(reader), |reader| async move {
                        let mut reader = reader?;
                        match reader.next_chunk().await {
                            Ok(Some(chunk)) => Some((Ok(chunk), Some(reader))),
                            Ok(None) => None,
                            Err(error) => Some((Err(StreamHandlerError::with_cause(error)), None)),
                        }
                    });
                    Ok(StreamingResponse::new(archive_bytes, chunks))
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let resource_replica_service = service.clone();
        service
            .register_backup_state_handlers(&interconnect)
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        interconnect
            .register_handler::<PublishResourceReplica, _, _>(move |context, request| {
                let service = resource_replica_service.clone();
                async move {
                    if request.replica.key.node.node_id() != context.peer_node_id() {
                        return Err(ResourceInterconnectError::ReplicaOrigin {
                            authenticated: context.peer_node_id().clone(),
                            declared: request.replica.key.node.node_id().clone(),
                        });
                    }
                    service
                        .inner
                        .consensus
                        .put_resource_replica(request.replica)
                        .await
                        .map_err(ResourceInterconnectError::replica_publish)
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        service
            .inner
            .client_producers
            .serve_links(&interconnect)
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        service
            .inner
            .client_consumers
            .serve_links(&interconnect)
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let describe_ingestor_service = service.clone();
        interconnect
            .register_handler::<RemoteDescribeIngestorRequest, _, _>(move |_context, request| {
                let service = describe_ingestor_service.clone();
                async move { service.handle_describe_ingestor_request(request).await }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let describe_snapshot_service = service.clone();
        interconnect
            .register_handler::<DescribeStateSnapshot, _, _>(move |_context, request| {
                let service = describe_snapshot_service.clone();
                async move {
                    let subject = RemoteOperationSubject::state(&request.placement);
                    match crate::runtime::RuntimeStatePlacement::from_remote(request.placement) {
                        Ok(placement) => {
                            service
                                .inner
                                .runtime
                                .describe_sealed_materialized_snapshot(
                                    &placement,
                                    request.after_revision,
                                )
                                .await
                        }
                        Err(error) => {
                            Err(RemoteOperationFailure::failed(subject, error.to_string()))
                        }
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let fetch_snapshot_service = service.clone();
        interconnect
            .register_stream_handler::<FetchStateSnapshot, _, _>(move |_context, request| {
                let service = fetch_snapshot_service.clone();
                async move {
                    service
                        .inner
                        .runtime
                        .stream_sealed_materialized_snapshot(request)
                        .await
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        service.register_state_replication_interconnect_handlers()?;

        let describe_relay_service = service.clone();
        interconnect
            .register_handler::<RemoteDescribeRelayRequest, _, _>(move |_context, request| {
                let service = describe_relay_service.clone();
                async move {
                    RemoteDescribeRelayResponse {
                        result: service.handle_describe_stream_request(request).await,
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let dataflow_status_service = service.clone();
        interconnect
            .register_handler::<RemoteDataflowNodeStatusRequest, _, _>(move |_context, request| {
                let service = dataflow_status_service.clone();
                async move {
                    RemoteDataflowNodeStatusResponse {
                        result: service.handle_dataflow_node_status_request(request).await,
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let domain_drain_service = service.clone();
        interconnect
            .register_handler::<RemoteDomainDrainStatusRequest, _, _>(move |_context, request| {
                let service = domain_drain_service.clone();
                async move {
                    let result = match service.apply_current_cluster_state().await {
                        Ok(()) => {
                            service
                                .inner
                                .runtime
                                .force_flush_domain_if_idle(&request.domain);
                            Ok(service.local_domain_drain_status(&request.domain))
                        }
                        Err(error) => Err(RemoteOperationFailure::failed(
                            RemoteOperationSubject::domain(&request.domain),
                            format!("{error:#}"),
                        )),
                    };
                    RemoteDomainDrainStatusResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let entity_gate_service = service.clone();
        interconnect
            .register_handler::<RemoteEntityGateRequest, _, _>(move |_context, request| {
                let service = entity_gate_service.clone();
                async move {
                    let subject = RemoteOperationSubject::domain(&request.domain);
                    let deadline = nervix_primitives::time::Instant::now()
                        .checked_add(Duration::from_millis(request.deadline_millis));
                    let result = match deadline {
                        Some(deadline) => service
                            .inner
                            .runtime
                            .engage_entity_gate_operation(
                                &request.coordination,
                                &request.domain,
                                &request.relays,
                                &request.affected_entities,
                                request.purpose,
                                EntityGateLease {
                                    deadline,
                                    reason: &request.reason,
                                },
                            )
                            .await
                            .map_err(|error| {
                                error.current_context().as_remote_failure(subject.clone())
                            }),
                        None => Err(RemoteOperationFailure::failed(
                            subject,
                            "entity gate deadline exceeds the monotonic clock",
                        )),
                    };
                    #[cfg(feature = "testing")]
                    if result.is_ok() {
                        service
                            .pause_entity_gate_response_if_armed(&request.domain)
                            .await;
                    }
                    RemoteEntityGateResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        service.register_wasm_state_reset_service(shutdown.clone())?;
        service.register_wasm_state_recovery_interconnect_handler()?;
        service.register_stopping_node_drain_handler()?;
        service.register_wasm_state_recovery_coordinator(shutdown.clone());

        let entity_drain_service = service.clone();
        interconnect
            .register_handler::<RemoteEntityDrainStatusRequest, _, _>(move |_context, request| {
                let service = entity_drain_service.clone();
                async move {
                    let result = service.local_entity_drain_status(
                        &request.coordination,
                        &request.domain,
                        &request.relays,
                        &request.affected_entities,
                        request.purpose,
                    );
                    if let Ok(status) = &result
                        && (status.buffered_relay_batches != 0
                            || status.node_work_items != 0
                            || status.outstanding_acks != 0)
                    {
                        service
                            .inner
                            .runtime
                            .force_flush_domain_if_idle(&request.domain);
                    }
                    RemoteEntityDrainStatusResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let entity_gate_release_service = service.clone();
        interconnect
            .register_handler::<RemoteEntityGateReleaseRequest, _, _>(move |_context, request| {
                let service = entity_gate_release_service.clone();
                async move {
                    RemoteEntityGateReleaseResponse {
                        result: service
                            .inner
                            .runtime
                            .release_entity_gate_operation(&request.coordination, &request.domain)
                            .await
                            .map_err(|error| {
                                error.current_context().as_remote_failure(
                                    RemoteOperationSubject::domain(&request.domain),
                                )
                            }),
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let describe_metrics_service = service.clone();
        interconnect
            .register_handler::<RemoteDescribeMetricsRequest, _, _>(move |_context, request| {
                let service = describe_metrics_service.clone();
                async move {
                    RemoteDescribeMetricsResponse {
                        result: service.handle_describe_metrics_request(request).await,
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let describe_lookup_service = service.clone();
        interconnect
            .register_handler::<RemoteDescribeLookupRequest, _, _>(move |_context, request| {
                let service = describe_lookup_service.clone();
                async move {
                    RemoteDescribeLookupResponse {
                        result: service.handle_describe_lookup_request(request).await,
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let lookup_service = service.clone();
        interconnect
            .register_handler::<RemoteLookupRequest, _, _>(move |_context, request| {
                let service = lookup_service.clone();
                async move {
                    let subject = RemoteOperationSubject::entity(
                        &request.domain,
                        ModelKind::Lookup,
                        request.name.clone(),
                    );
                    let result = match service.handle_lookup_request(request).await {
                        Ok(Some(record)) => {
                            let mapping_input = record
                                .encode_arrow_ipc(service.inner.runtime.executor())
                                .await
                                .map(|body| Some(body.to_vec()));
                            match mapping_input {
                                Ok(mapped_value) => Ok(mapped_value),
                                Err(error) => {
                                    Err(RemoteOperationFailure::failed(subject, error.to_string()))
                                }
                            }
                        }
                        Ok(None) => Ok(None),
                        Err(failure) => Err(failure),
                    };
                    RemoteLookupResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let subscription_interest_service = service.clone();
        interconnect
            .register_handler::<RemoteSubscriptionInterestVisibilityRequest, _, _>(
                move |context, request| {
                    let service = subscription_interest_service.clone();
                    async move {
                        let result = if request.subscriber.node_id() != context.peer_node_id() {
                            Err(RemoteOperationFailure::rejected(
                                RemoteOperationSubject::subscription_interest(
                                    &request.domain,
                                    &request.relay,
                                    &request.subscriber,
                                ),
                            ))
                        } else {
                            service
                                .inner
                                .cluster
                                .wait_for_subscription_interest(
                                    &request.subscriber,
                                    request.domain.as_str(),
                                    request.relay.as_str(),
                                    request.minimum_version,
                                )
                                .await;
                            Ok(())
                        };
                        RemoteSubscriptionInterestVisibilityResponse { result }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let capture_handoff_service = service.clone();
        interconnect
            .register_handler::<RemoteCaptureOwnershipHandoffStateRequest, _, _>(
                move |_context, request| {
                    let service = capture_handoff_service.clone();
                    async move {
                        let result: OwnershipHandoffResult<_> = async {
                            if request.source != *service.inner.consensus.local_node_id() {
                                return Err(OwnershipHandoffError::participant(format!(
                                    "ownership handoff capture targets source node '{}' but \
                                     reached '{}'",
                                    request.source,
                                    service.inner.consensus.local_node_id()
                                )));
                            }
                            SessionServiceImpl::verify_ownership_handoff_node_incarnation(
                                &service.live_node_incarnations().await,
                                &request.source,
                                request.source_incarnation,
                                "source",
                            )?;
                            let scheduled = service
                                .prepare_owner_control_request(
                                    &request.domain,
                                    request.entity.kind,
                                    request.entity.identifier.clone(),
                                )
                                .await
                                .map_err(|failure| {
                                    OwnershipHandoffError::participant(failure.to_string())
                                })?;
                            if !scheduled.is_primary_on(service.inner.consensus.local_node_id()) {
                                return Err(OwnershipHandoffError::participant(format!(
                                    "{} '{}' is not owned by this source node",
                                    request.entity.kind.as_str(),
                                    request.entity.identifier.as_str()
                                )));
                            }
                            service
                                .inner
                                .runtime
                                .capture_ownership_handoff_state(
                                    &request.coordination,
                                    &request.domain,
                                    &request.entity,
                                    request.unit,
                                    request.base_schedule_fingerprint,
                                )
                                .await
                        }
                        .await;
                        match result {
                            Ok(checkpoints) => Ok(checkpoints),
                            Err(error) => Err(OwnershipHandoffError::remote_rejection(&error)),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        service
            .register_prepare_ownership_handoff_handler(&interconnect)
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let forced_recovery_service = service.clone();
        interconnect
            .register_handler::<RemotePrepareForcedOwnershipRecoveryRequest, _, _>(
                move |_context, request| {
                    let service = forced_recovery_service.clone();
                    async move {
                        let result: OwnershipHandoffResult<_> = async {
                            if request.destination != *service.inner.consensus.local_node_id() {
                                return Err(OwnershipHandoffError::participant(format!(
                                    "forced ownership recovery for {} '{}' targets node '{}' but \
                                     reached '{}'",
                                    request.entity.kind.as_str(),
                                    request.entity.identifier.as_str(),
                                    request.destination,
                                    service.inner.consensus.local_node_id()
                                )));
                            }
                            SessionServiceImpl::verify_ownership_handoff_node_incarnation(
                                &service.live_node_incarnations().await,
                                &request.destination,
                                request.destination_incarnation,
                                "destination",
                            )?;
                            let deadline = nervix_primitives::time::Instant::now()
                                + FORCED_OWNERSHIP_RECOVERY_BUDGET;
                            service
                                .inner
                                .runtime
                                .prepare_forced_ownership_recovery(request, deadline)
                                .await
                        }
                        .await;
                        match result {
                            Ok(preparation) => Ok(preparation),
                            Err(error) => Err(OwnershipHandoffError::remote_rejection(&error)),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let confirm_handoff_service = service.clone();
        interconnect
            .register_handler::<RemoteConfirmOwnershipHandoffStateRequest, _, _>(
                move |_context, request| {
                    let service = confirm_handoff_service.clone();
                    async move {
                        match service.confirm_local_ownership_handoff_state(request).await {
                            Ok(()) => Ok(()),
                            Err(error) => Err(OwnershipHandoffError::remote_rejection(&error)),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let activate_handoff_service = service.clone();
        interconnect
            .register_handler::<RemoteActivateOwnershipHandoffStateRequest, _, _>(
                move |_context, request| {
                    let service = activate_handoff_service.clone();
                    async move {
                        let result = service
                            .activate_local_ownership_handoff_state(&request)
                            .await;
                        match result {
                            Ok(()) => Ok(()),
                            Err(error) => Err(OwnershipHandoffError::remote_rejection(&error)),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let discard_handoff_service = service.clone();
        interconnect
            .register_handler::<RemoteDiscardOwnershipHandoffStateRequest, _, _>(
                move |_context, request| {
                    let service = discard_handoff_service.clone();
                    async move {
                        let result = service
                            .inner
                            .runtime
                            .discard_prepared_ownership_handoff_state(
                                &request.coordination,
                                &request.operation_id,
                                &request.domain,
                                &request.entity,
                                request.unit,
                            );
                        match result {
                            Ok(()) => Ok(()),
                            Err(error) => Err(OwnershipHandoffFailure::rejected(error.to_string())),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        service
            .register_ownership_handoff_reconciliation_handler(&interconnect)
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let ownership_handoff_reconciliation_service = service.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            ownership_handoff_reconciliation_service
                .run_ownership_handoff_preparation_reconciliation()
                .await;
        }));

        let transaction_service = service.clone();
        let transaction_shutdown = shutdown.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            let mut observed_leadership = None;
            loop {
                nervix_primitives::task::consume_budget().await;
                let is_leader = transaction_service
                    .inner
                    .consensus
                    .current_leader()
                    .await
                    .as_ref()
                    == Some(transaction_service.inner.consensus.local_node_id());
                if observed_leadership != Some(is_leader) {
                    transaction_service.inner.transaction_bindings.clear();
                    let schedule = transaction_service.inner.consensus.current_schedule().await;
                    match transaction_service
                        .inner
                        .registry
                        .synchronize_cluster_schedule(&schedule)
                    {
                        Ok(()) => observed_leadership = Some(is_leader),
                        Err(error) => {
                            warn!(
                                is_leader,
                                error = %error,
                                "failed to synchronize registry after leadership state change"
                            );
                            nervix_primitives::select! {
                                _ = transaction_shutdown.cancelled() => break,
                                _ = sleep(Duration::from_millis(250)) => {}
                            }
                            continue;
                        }
                    }
                }
                if is_leader {
                    transaction_service.reconcile_transactions_once().await;
                }
                // A backup archive outlives leadership on the node that assembled it, so every
                // node releases the archives whose retry validity ended.
                transaction_service.sweep_retained_backups();
                transaction_service.sweep_restore_archives();
                nervix_primitives::select! {
                    _ = transaction_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_millis(250)) => {}
                }
            }
        }));

        let restore_maintenance_service = service.clone();
        let restore_maintenance_shutdown = shutdown.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    _ = restore_maintenance_shutdown.cancelled() => break,
                    _ = restore_maintenance_service.sweep_restore_checkpoint_staging() => {}
                }
                nervix_primitives::select! {
                    _ = restore_maintenance_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));

        let clock_authority_service = service.clone();
        let clock_authority_shutdown = shutdown.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            run_domain_clock_authority_reconciliation(
                clock_authority_service,
                clock_authority_shutdown,
            )
            .await;
        }));

        let runtime_event_service = service.clone();
        let runtime_event_shutdown = shutdown.clone();
        let mut runtime_event_rx = runtime.subscribe_events();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    _ = runtime_event_shutdown.cancelled() => break,
                    received = runtime_event_rx.recv() => match received {
                        Ok(RuntimeEvent::Error(message)) => {
                            let mut node_ids =
                                runtime_event_service.inner.cluster.live_node_ids().await;
                            node_ids.sort();
                            node_ids.dedup();
                            for node_id in node_ids {
                                nervix_primitives::task::consume_budget().await;
                                if node_id == runtime_event_service.inner.consensus.local_node_id().clone() {
                                    continue;
                                }
                                if let Err(error) = runtime_event_service
                                    .dispatch_interconnect_control(
                                        &node_id,
                                        ControlEnvelope::RuntimeErrorEvent(
                                            RemoteRuntimeErrorEvent {
                                                message: message.clone(),
                                            },
                                        ),
                                    )
                                    .await
                                {
                                    warn!(
                                        %node_id,
                                        error = %error,
                                        "failed to fan out runtime error event"
                                    );
                                }
                            }
                        }
                        // The peers never learn about the errors this node skipped, so the count
                        // is logged here to keep the gap attributable to load rather than silence.
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "runtime error fan-out fell behind the event bus");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }));

        let resource_service = service.clone();
        let resource_shutdown = shutdown.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            loop {
                nervix_primitives::task::consume_budget().await;
                if resource_shutdown.is_cancelled() {
                    break;
                }
                resource_service.reconcile_resources_once().await;
                nervix_primitives::select! {
                    _ = resource_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));

        let clock_service = service.clone();
        let clock_shutdown = shutdown.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            let mut domain_state_rx = clock_service.inner.runtime.subscribe_domain_state();
            // The clock owner installs committed authority fences ahead of graph installation and
            // advertises each runtime revision its domain clocks match, so it follows committed
            // domain and schedule changes itself.
            let mut committed_domains_rx = clock_service.inner.consensus.subscribe_domains();
            let mut committed_schedule_rx = clock_service.inner.consensus.subscribe_schedule();
            let mut tasks: HashMap<DomainName, DomainClockTask> = HashMap::new();
            let mut retirements = DomainClockRetirements::default();

            loop {
                nervix_primitives::task::consume_budget().await;
                let state = clock_service.inner.consensus.current_runtime_state().await;
                clock_service.install_committed_domain_clocks(&state).await;
                reconcile_domain_clock_tasks(
                    &clock_service,
                    &clock_shutdown,
                    &state,
                    &mut tasks,
                    &mut retirements,
                )
                .await;
                nervix_primitives::select! {
                    _ = clock_shutdown.cancelled() => break,
                    changed = domain_state_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    changed = committed_domains_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    changed = committed_schedule_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = retirements.join_next(), if !retirements.is_empty() => {}
                }
            }

            for (_, task) in tasks {
                task.task.stop().await;
            }
            retirements.stop_all().await;
        }));

        let kafka_schedule_service = service.clone();
        let kafka_schedule_shutdown = shutdown.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            let mut schedule_rx = kafka_schedule_service.inner.consensus.subscribe_schedule();
            let mut tasks: HashMap<KafkaPartitionWatcherKey, KafkaPartitionWatcherTask> =
                HashMap::new();

            loop {
                nervix_primitives::task::consume_budget().await;
                let schedule = kafka_schedule_service
                    .inner
                    .consensus
                    .current_schedule()
                    .await;
                kafka_schedule_service
                    .reconcile_kafka_partition_watchers(&schedule, &mut tasks)
                    .await;
                nervix_primitives::select! {
                    _ = kafka_schedule_shutdown.cancelled() => break,
                    changed = schedule_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = sleep(LEADER_KAFKA_PARTITION_WATCH_INTERVAL) => {}
                }
            }

            for (_, watcher) in tasks {
                watcher.task.stop().await;
            }
        }));

        let (interconnect_relay_payload_lane, interconnect_relay_payload_rx) =
            InterconnectRelayPayloadLane::new();
        let relay_payload_shutdown = shutdown.clone();
        let runtime_for_relay_payloads = runtime.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            InterconnectRelayPayloadLane::run(
                interconnect_relay_payload_rx,
                runtime_for_relay_payloads,
                relay_payload_shutdown,
            )
            .await;
        }));

        let interconnect_shutdown = shutdown.clone();
        let runtime_for_interconnect = runtime.clone();
        let service_for_interconnect = service.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    _ = interconnect_shutdown.cancelled() => break,
                    message = interconnect_rx.recv() => {
                        let Some(message) = message else {
                            break;
                        };
                        debug!(
                            peer_addr = %message.peer_addr,
                            peer_node_id = %message.peer_node_id,
                            "received interconnect envelope"
                        );
                        let Some(envelope) = interconnect_relay_payload_lane
                            .route(
                                &message.peer_node_id,
                                message.envelope,
                                message.relay_admission,
                            )
                        else {
                            continue;
                        };
                        match envelope {
                            Envelope::RelayPayload(_) => {
                                unreachable!("relay payloads are routed to their dedicated lane")
                            }
                            Envelope::Ack(ack) => {
                                runtime_for_interconnect.handle_remote_ack_resolution(ack);
                            }
                            Envelope::Control(ControlEnvelope::Terminate) => {}
                            Envelope::Control(
                                ControlEnvelope::Request(_) | ControlEnvelope::Response(_),
                            ) => {
                                unreachable!("typed requests are consumed by the interconnect")
                            }
                            Envelope::Control(ControlEnvelope::StateReplicationAck(ack)) => {
                                let placement = match crate::runtime::RuntimeStatePlacement::from_remote(
                                    ack.placement,
                                ) {
                                    Ok(placement) => placement,
                                    Err(error) => {
                                        warn!(error = %error, "failed to decode state replication ack placement");
                                        continue;
                                    }
                                };
                                service_for_interconnect.inner.runtime.handle_state_replication_ack(
                                    &message.peer_node_id,
                                    crate::runtime::StateSyncAck {
                                        placement,
                                        lsm: ack.lsm,
                                    },
                                );
                            }
                            Envelope::Control(ControlEnvelope::StateCheckpointAvailable(
                                checkpoint,
                            )) => {
                                service_for_interconnect
                                    .inner
                                    .runtime
                                    .handle_state_checkpoint_available(
                                        &message.peer_node_id,
                                        checkpoint,
                                    );
                            }
                            Envelope::Control(ControlEnvelope::RuntimeErrorEvent(event)) => {
                                service_for_interconnect.broadcast_error(event.message);
                            }
                        }
                    }
                }
            }
        }));
        let cluster_events = events.clone();
        let mut cluster_event_rx = cluster.subscribe_events();
        let cluster_events_shutdown = shutdown.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    _ = cluster_events_shutdown.cancelled() => break,
                    received = cluster_event_rx.recv() => match received {
                        Ok(message) => cluster_events.relay_info(message),
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "cluster event relay fell behind the event bus");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }));
        let consensus_events = events.clone();
        let mut consensus_event_rx = consensus.observer().subscribe_events();
        let consensus_events_shutdown = shutdown.clone();
        background_tasks.push(nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    _ = consensus_events_shutdown.cancelled() => break,
                    received = consensus_event_rx.recv() => match received {
                        Ok(message) => consensus_events.relay_info(message),
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "consensus event relay fell behind the event bus");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }));

        info!(mode = grpc_mode.scheme(), addr = %grpc_listen_addr, "nervix gRPC server listening");
        info!(addr = %http_listen_addr, "nervix HTTP server listening");
        info!(addr = %https_listen_addr, "nervix HTTPS server listening");
        info!(addr = %observability_listen_addr, "nervix observability server listening");
        info!(addr = %web_console_listen_addr, "nervix web console server listening");
        if let Some(addr) = web_console_https_listen_addr {
            info!(addr = %addr, "nervix web console TLS server listening");
        }

        let listener_shutdown = shutdown_coordinator.admission_token();

        let grpc_service = service.clone();
        let grpc_shutdown = listener_shutdown.clone();
        let api_server = async move {
            let mut builder = Server::builder();
            builder = if grpc_mode.is_tls() {
                builder
                    .tls_config(
                        grpc_tls_server_config.ok_or_else(|| Report::new(AppError::LoadGrpcTls))?,
                    )
                    .change_context(AppError::LoadGrpcTls)?
            } else {
                builder
            };
            // Closing admission ends every connection this listener accepted, so the server's
            // graceful shutdown never waits for a client to finish or abandon a request.
            let grpc_incoming =
                AdmittingListener::new(grpc_listener, grpc_shutdown.clone()).into_connections();
            builder
                .add_service(SessionGrpcService::new(grpc_service.clone()))
                .serve_with_incoming_shutdown(grpc_incoming, grpc_shutdown.cancelled_owned())
                .await
                .map_err(|e| Report::new(e).change_context(AppError::Serve))
        };
        let http_server = serve_http(
            runtime.clone(),
            service.inner.service_tasks.clone(),
            http_listener,
            listener_shutdown.clone(),
        );
        let https_server = serve_https(
            runtime.clone(),
            service.inner.service_tasks.clone(),
            service.inner.https_certificates.clone(),
            https_listener,
            listener_shutdown.clone(),
        );
        let observability_server = serve_observability_http(
            consensus.observer(),
            runtime.clone(),
            observability_listener,
            listener_shutdown.clone(),
        );
        let web_console_server = serve_web_console_http(
            service.clone(),
            web_console_listener,
            listener_shutdown.clone(),
        );
        let web_console_https_shutdown = listener_shutdown;
        let web_console_https_service = service.clone();
        let web_console_https_server = async move {
            if let (Some(config), Some(listener)) =
                (web_console_tls_server_config, web_console_https_listener)
            {
                serve_web_console_https(
                    web_console_https_service,
                    config,
                    listener,
                    web_console_https_shutdown.clone(),
                )
                .await
            } else {
                web_console_https_shutdown.cancelled().await;
                Ok(())
            }
        };

        let public_listeners_shutdown = shutdown_coordinator.clone();
        let public_listeners = nervix_primitives::task::spawn(async move {
            let (
                api_result,
                http_result,
                https_result,
                observability_result,
                web_console_result,
                web_console_https_result,
            ) = tokio::join!(
                request_shutdown_on_completion(api_server, public_listeners_shutdown.clone()),
                request_shutdown_on_completion(http_server, public_listeners_shutdown.clone()),
                request_shutdown_on_completion(https_server, public_listeners_shutdown.clone()),
                request_shutdown_on_completion(
                    observability_server,
                    public_listeners_shutdown.clone()
                ),
                request_shutdown_on_completion(
                    web_console_server,
                    public_listeners_shutdown.clone()
                ),
                request_shutdown_on_completion(
                    web_console_https_server,
                    public_listeners_shutdown.clone()
                ),
            );
            api_result
                .and(http_result)
                .and(https_result)
                .and(observability_result)
                .and(web_console_result)
                .and(web_console_https_result)
        });

        // The node serves until its first stop request, whether a termination signal, the caller
        // embedding the application, or a failed listener made it. That request's deadline bounds
        // everything that follows.
        let deadline = shutdown_coordinator.requested().await.deadline();

        let advertisement = deadline.bound(cluster.mark_local_terminating()).await;
        let advertisement_outcome = match advertisement {
            BeforeDeadline::Finished(()) => ShutdownPhaseOutcome::Completed,
            BeforeDeadline::Expired => {
                warn!(
                    "shutdown deadline expired before this node advertised that it is terminating"
                );
                ShutdownPhaseOutcome::Forced
            }
        };
        shutdown_coordinator.stop_admission();
        let listeners = join_public_listeners(public_listeners, deadline).await;
        let stop_admission_outcome = advertisement_outcome
            .combine(listeners.outcome)
            .unless_deadline_passed(deadline);
        shutdown_coordinator.begin_drain_support(stop_admission_outcome);

        let drain_support_outcome = if graceful_shutdown_drain {
            let drain = service.drain_local_node_before_shutdown(drain_timeout, deadline);
            let drained = deadline.bound(drain).await;
            match drained {
                BeforeDeadline::Finished(outcome) => outcome.unless_deadline_passed(deadline),
                BeforeDeadline::Expired => {
                    warn!("shutdown deadline expired while this node drained its admitted work");
                    ShutdownPhaseOutcome::Forced
                }
            }
        } else {
            ShutdownPhaseOutcome::Completed.unless_deadline_passed(deadline)
        };
        shutdown_coordinator.begin_terminal_teardown(drain_support_outcome);

        let background_task_shutdowns = background_tasks.into_iter().map(|task| {
            await_background_task_shutdown(task, "application background task", deadline)
        });
        let background_task_outcomes = join_all(background_task_shutdowns).await;
        let mut terminal_teardown_outcome = ShutdownPhaseOutcome::Completed;
        for task_outcome in background_task_outcomes {
            terminal_teardown_outcome = terminal_teardown_outcome.combine(task_outcome);
        }
        let service_tasks_outcome = service.inner.service_tasks.shut_down(deadline).await;
        terminal_teardown_outcome = terminal_teardown_outcome.combine(service_tasks_outcome);
        // The services below own this node's remaining tasks, connections, and storage, and
        // stopping them is what releases those, so each stop runs to completion rather than being
        // abandoned at the deadline. A server process whose deadline passes meanwhile is ended by
        // its deadline supervision.
        runtime.shutdown(deadline.remaining()).await;
        consensus.shutdown().await;
        let cluster_shutdown_result = cluster
            .shutdown()
            .await
            .change_context(AppError::ShutdownCluster);
        interconnect.shutdown().await;

        let executor = runtime.executor().clone();
        #[cfg(feature = "testing")]
        let stopping_fault_injection = fault_injection.clone();
        #[cfg(feature = "testing")]
        let stopping_node = consensus.proposer().local_node_id().clone();
        let database_owner_outcome = ApplicationStartup::close_stores(&executor, move || {
            drop(service);
            drop(runtime);
            drop(consensus);
            drop(cluster);
            drop(interconnect);
            #[cfg(feature = "testing")]
            stopping_fault_injection.flush_node_database_if_armed(&stopping_node, &db);
            drop(db);
        })
        .await;
        if let Err(error) = &database_owner_outcome {
            error!(error = ?error, "failed to close the node's stores");
        }

        if cluster_shutdown_result.is_err() || database_owner_outcome.is_err() {
            terminal_teardown_outcome =
                terminal_teardown_outcome.combine(ShutdownPhaseOutcome::Abandoned);
        }
        shutdown_coordinator.finish(terminal_teardown_outcome.unless_deadline_passed(deadline));

        let refused_join = join_refusal.borrow().clone();
        if let Some(refusal) = refused_join {
            return Err(Report::new(refusal).change_context(AppError::JoinRefused));
        }
        listeners.result?;
        cluster_shutdown_result?;
        database_owner_outcome?;
        let outcome = shutdown_coordinator
            .outcome()
            .verified("the terminal-teardown phase finished above");
        if outcome.deadline_expired() {
            return Err(Report::new(AppError::ShutdownDeadlineExpired));
        }

        Ok(())
    }
}

/// What the edges expose. Everything else this module and its submodules declare is
/// `pub(in crate::application)` or narrower, so the binary and the test harness reach the server
/// only through the names below.
pub use error::AppError;
pub use termination_signals::TerminationSignals;
pub use tracing_setup::{
    TraceSampleRatio, TraceSampleRatioError, TracingGuard, init_tracing, init_tracing_to_file,
};
pub use transaction::{InspectedReportError, InspectingSession, TransactionInspectionOutcome};

#[cfg(test)]
#[path = "application/tests.rs"]
mod tests;
