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

use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    fs::OpenOptions,
    future::Future,
    io,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64},
    path::{Component, Path, PathBuf},
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use ahash::{HashMap, HashMapExt, HashSet, RandomState};
use arch_into::ArchInto as _;
#[cfg(feature = "testing")]
use argon2::{Algorithm, Params, Version};
use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use async_tar::{Builder as AsyncTarBuilder, EntryType, Header, HeaderMode};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use blake3::Hasher;
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{Shell, generate};
use dashmap::DashMap;
use error_stack::{Report, ResultExt};
use fjall::Database;
use futures_util::{
    SinkExt, StreamExt,
    stream::{self, FuturesUnordered},
};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use http_body_util::{BodyExt, Empty, Full};
use hyper::{
    Method, Request as HyperRequest, Response as HyperResponse, StatusCode,
    body::{Bytes, Incoming as HyperIncoming},
    header::{
        ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
        AUTHORIZATION, CONNECTION, HOST, LOCATION, RETRY_AFTER, SEC_WEBSOCKET_ACCEPT,
        SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE, WWW_AUTHENTICATE,
    },
    server::conn::http1,
    service::service_fn,
    upgrade,
};
use hyper_util::rt::TokioIo;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto as _;
use nervix_client_core::{
    Client as NervixClient, ConnectOptions as ClientConnectOptions,
    TlsRequirement as ClientTlsRequirement,
};
use nervix_consensus::{
    Administrator, Consensus, ConsensusError, ConsensusRuntimeState, ConsensusSettings,
    ConsensusTransactionError, Observer, Proposer, ReplicatedTransaction, TransactionCommandResult,
    TransactionCommitAdvance, TransactionDiagnostic, TransactionOutcome, TransactionQueueLimits,
    TransactionState, TransactionStatement, TransactionStepEffect, TransactionStepResult,
    UserCredentials,
};
use nervix_dataflow_graph::{DataflowGraph, DataflowNodeHealth, DataflowNodeStatus};
use nervix_execution::MemoryClass;
use nervix_interconnect::{
    ActivateOwnershipHandoffStateRequest as RemoteActivateOwnershipHandoffStateRequest,
    CaptureOwnershipHandoffStateRequest as RemoteCaptureOwnershipHandoffStateRequest,
    ConfirmOwnershipHandoffStateRequest as RemoteConfirmOwnershipHandoffStateRequest,
    ControlEnvelope, DataflowNodeStatusEnvelope,
    DataflowNodeStatusRequest as RemoteDataflowNodeStatusRequest,
    DataflowNodeStatusResponse as RemoteDataflowNodeStatusResponse,
    DescribeIngestorRequest as RemoteDescribeIngestorRequest,
    DescribeLookupRequest as RemoteDescribeLookupRequest,
    DescribeLookupResponse as RemoteDescribeLookupResponse,
    DescribeMetricsEnvelope as RemoteDescribeMetricsEnvelope,
    DescribeMetricsRequest as RemoteDescribeMetricsRequest,
    DescribeMetricsResponse as RemoteDescribeMetricsResponse,
    DescribeRelayRequest as RemoteDescribeRelayRequest,
    DescribeRelayResponse as RemoteDescribeRelayResponse,
    DiscardOwnershipHandoffStateRequest as RemoteDiscardOwnershipHandoffStateRequest,
    DomainClockProgressEnvelope, DomainDrainStatusEnvelope,
    DomainDrainStatusRequest as RemoteDomainDrainStatusRequest,
    DomainDrainStatusResponse as RemoteDomainDrainStatusResponse,
    EmitterPublishingDrainStateEnvelope, EmitterPublishingDrainStatusEnvelope,
    EntityDrainStatusEnvelope, EntityDrainStatusRequest as RemoteEntityDrainStatusRequest,
    EntityDrainStatusResponse as RemoteEntityDrainStatusResponse, EntityGatePurpose,
    EntityGateReleaseRequest as RemoteEntityGateReleaseRequest,
    EntityGateReleaseResponse as RemoteEntityGateReleaseResponse,
    EntityGateRequest as RemoteEntityGateRequest, EntityGateResponse as RemoteEntityGateResponse,
    Envelope, IngestorDescribeEnvelope, LookupDescribeEnvelope,
    LookupRequest as RemoteLookupRequest, LookupResponse as RemoteLookupResponse,
    OwnershipHandoffFailure, PeerTarget,
    PrepareForcedOwnershipRecoveryRequest as RemotePrepareForcedOwnershipRecoveryRequest,
    PrepareOwnershipHandoffStateRequest as RemotePrepareOwnershipHandoffStateRequest, RelayPayload,
    RuntimeErrorEvent as RemoteRuntimeErrorEvent, StateSyncRequest as RemoteStateSyncRequest,
    StateSyncResponse as RemoteStateSyncResponse,
    SubscriptionInterestVisibilityRequest as RemoteSubscriptionInterestVisibilityRequest,
    SubscriptionInterestVisibilityResponse as RemoteSubscriptionInterestVisibilityResponse,
    TlsConfigBundle, Transport,
};
use nervix_models::{
    AlterDomain, BranchSelection, ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName,
    CreateBranch, CreateCorrelator, CreateDeduplicator, CreateDomain, CreateEmitter,
    CreateEndpoint, CreateInferencer, CreateIngestor, CreateJunction, CreateLookup,
    CreatePlacement, CreateReingestor, CreateRelay, CreateReorderer, CreateResource, CreateSchema,
    CreateStatement, CreateUdf, CreateUser, CreateVhost, CreateWasmProcessor,
    CreateWindowProcessor, DescribeCorrelator, DescribeDeduplicator, DescribeDomain,
    DescribeEmitter, DescribeEndpoint, DescribeIngestor, DescribeJunction, DescribeLookup,
    DescribePlacement, DescribeReingestor, DescribeRelay, DescribeReorderer, DescribeResource,
    DescribeUdf, DescribeWasmProcessor, DescribeWindowProcessor, DomainClockAdvancement,
    DomainClockAuthority, DomainClockPeriod, DomainClockProgress, DomainClockState, DomainConfig,
    DomainName, DomainPace, DomainStartPoint, DomainState, DomainStatus, DomainTick, EmitSink,
    FieldName, IcebergCatalog, InferencerTensorDimension, InferencerTensorSchema, IngestSource,
    IngestTimestampSource, IngestorName, KafkaOffsetMode, KafkaPartitionSchedule, LookupName,
    LookupQuery, Model, ModelKind, ModelName, MongoDbConflictAction, MySqlConflictAction, NodeRef,
    OwnershipStateRecoveryOutcome, OwnershipStateReset, OwnershipStateResetCause,
    OwnershipTransition, ParseAsType, PlacementGroupSchedule, PlacementName, PlacementPolicy,
    PostgresConflictAction, ProcessorInputs, ProcessorOutputs, QuiesceLevel, RelayName, ResourceId,
    ResourceName, ResourceNodeState, ResourceNodeStatus, ResourceReplicaKey, ScheduledModel,
    ScheduledNode, ShowRelayMaterializedState, StartDomain, Statement, StopDomain,
    SubscriptionBinding, SubscriptionDeliveryBehavior, SubscriptionLiteral, SubscriptionName,
    Timestamp, UniquelyKindedModel, UploadResource, UserName, VhostTlsResource, expression_to_nspl,
    ingest_quiesce_to_nspl,
};
use nervix_nspl::{
    Token, Word,
    client_statement::{
        ClientStatement, ParsedClientStatement, parse_client_statement_sources,
        parse_client_statements, suggest_client_statement, upload_resource_path_fragment,
    },
    lex,
    schema::{Diagnostic as ParseDiagnostic, ParseFromSourceError},
};
use nervix_recovery::{Discarded as _, NoReceiver as _, Reported as _};
use nervix_vm::window::{WindowAggregateDemand, WindowAggregateProgram, lower_window_assignments};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    trace::{Sampler, SdkTracerProvider},
};
use ort::{
    session::Session,
    value::{TensorElementType, ValueType},
};
use parking_lot::{Mutex as ParkingMutex, RwLock};
use prost::Message as ProstMessage;
use rdkafka::{config::ClientConfig, consumer::StreamConsumer};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::ResolvesServerCertUsingSni,
    sign::CertifiedKey,
};
use rustls_pki_types::pem::{Error as PemError, PemObject};
use sorted_vec::SortedSet;
use tempfile::TempPath;
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex as AsyncMutex, broadcast, mpsc, watch},
    task::{JoinHandle, JoinSet},
    time::{Duration, interval, sleep},
};
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::derive_accept_key,
        protocol::{CloseFrame, Role, frame::coding::CloseCode},
    },
};

use crate::{
    registry::{
        ActiveGraph, PlacementEndpointPairPlan, PlacementPlan, PlacementRequireGroupPlan,
        PlacementRulePlan, Registry, RegistryError, RegistryMutation,
    },
    resource_interconnect::{
        FetchResourceArchiveChunk, PublishResourceReplica,
        ResourceArchiveChunk as InterconnectResourceArchiveChunk, ResourceInterconnectError,
    },
};

const REMOTE_DESCRIBE_RELAY_TIMEOUT: Duration = Duration::from_secs(1);
const SUBSCRIPTION_INTEREST_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(5);
const SUBSCRIPTION_INTEREST_CHECK_TIMEOUT: Duration = Duration::from_millis(250);
const RUNTIME_REVISION_READINESS_TIMEOUT: Duration = Duration::from_secs(30);
const RUNTIME_REVISION_READINESS_POLL_INTERVAL: Duration = Duration::from_millis(25);
const ENTITY_GATE_RELEASE_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const FORCED_OWNERSHIP_RECOVERY_BUDGET: Duration = Duration::from_secs(5);
const BACKGROUND_TASK_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);
const INTERCONNECT_TLS_RELOAD_INTERVAL: Duration = Duration::from_secs(1);
const OBSERVABILITY_LIVEZ_PATH: &str = "/livez";
const OBSERVABILITY_READYZ_PATH: &str = "/readyz";
const OBSERVABILITY_METRICS_PATH: &str = "/metrics";
mod relocation;

const WEB_CONSOLE_INDEX: &[u8] = include_bytes!("../crates/web-console/dist/index.html");
const WEB_CONSOLE_CSS: &[u8] = include_bytes!("../crates/web-console/dist/console.css");
const WEB_CONSOLE_JS: &[u8] = include_bytes!("../crates/web-console/dist/nervix-web-console.js");
const WEB_CONSOLE_WASM: &[u8] =
    include_bytes!("../crates/web-console/dist/nervix-web-console_bg.wasm");
const WEB_CONSOLE_ICON: &[u8] = include_bytes!("../crates/web-console/dist/nervix-icon.svg");
const WEB_CONSOLE_WS_PATH: &str = "/console/ws";
const WEB_CONSOLE_RESOURCE_UPLOAD_PATH: &str = "/console/resources/upload";
const WEB_CONSOLE_AUTH_QUERY_PARAM: &str = "auth";
const WEB_CONSOLE_LEADERSHIP_CHECK_INTERVAL: Duration = Duration::from_millis(250);
const WEB_CONSOLE_GRAPH_SNAPSHOT_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_USER: &str = "default";
const BASIC_AUTH_REALM: &str = "Nervix";
const AUTH_RATE_LIMIT_PER_SECOND: u32 = 10;
const DEFAULT_TRANSACTION_IDLE_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const DEFAULT_TRANSACTION_TOMBSTONE_RETENTION: Duration = Duration::from_secs(15 * 60);
const DEFAULT_TRANSACTION_MAX_STATEMENTS: usize = 256;
const DEFAULT_TRANSACTION_MAX_SOURCE_BYTES: u64 = 1024 * 1024;
const DEFAULT_TRANSACTION_MAX_OPEN: usize = 1024;

#[derive(Debug, Clone)]
struct DrainOutstanding {
    domain: DomainName,
    node: Option<ClusterNodeName>,
    active_ingestors: u64,
    active_generators: u64,
    outstanding_acks: u64,
    buffered_emitter_messages: u64,
    emitter_publishing: Vec<EmitterPublishingDrainStatusEnvelope>,
    status_error: Option<String>,
}

impl DrainOutstanding {
    fn write_emitter_publishing(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        if self.emitter_publishing.is_empty() {
            return Ok(());
        }
        formatter.write_str(&emitter_publishing_drain_summary(&self.emitter_publishing))
    }
}

fn emitter_publishing_drain_summary(statuses: &[EmitterPublishingDrainStatusEnvelope]) -> String {
    if statuses.is_empty() {
        return String::new();
    }
    let statuses = statuses
        .iter()
        .map(|status| {
            let mut detail = format!(
                "{}:{}(pending={}",
                status.emitter.as_str(),
                status.state.as_str(),
                status.pending_messages,
            );
            if let Some(backoff) = status.retry_backoff_millis {
                detail.push_str(&format!(", retry_backoff_ms={backoff}"));
            }
            if let Some(wait) = status.retry_wait_millis {
                detail.push_str(&format!(", retry_wait_ms={wait}"));
            }
            detail.push(')');
            detail
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(", publishing=[{statuses}]")
}

fn emitter_publishing_drain_status_envelope(
    status: crate::runtime::EmitterPublishingDrainStatus,
) -> EmitterPublishingDrainStatusEnvelope {
    EmitterPublishingDrainStatusEnvelope {
        emitter: status.emitter,
        state: match status.state {
            crate::runtime::EmitterPublishingDrainState::AwaitingConfirmation => {
                EmitterPublishingDrainStateEnvelope::AwaitingConfirmation
            }
            crate::runtime::EmitterPublishingDrainState::RetryingInfrastructure => {
                EmitterPublishingDrainStateEnvelope::RetryingInfrastructure
            }
            crate::runtime::EmitterPublishingDrainState::RetryingIcebergCommit => {
                EmitterPublishingDrainStateEnvelope::RetryingIcebergCommit
            }
        },
        pending_messages: status.pending_messages.arch_into(),
        retry_backoff_millis: status
            .retry_backoff
            .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
        retry_wait_millis: status
            .retry_wait
            .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
    }
}

impl std::fmt::Display for DrainOutstanding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total = [
            self.active_ingestors,
            self.active_generators,
            self.outstanding_acks,
            self.buffered_emitter_messages,
        ]
        .into_iter()
        .try_fold(0_u64, u64::checked_add)
        .assured("every count totals work items this cluster already holds in memory");
        if let Some(node) = &self.node {
            write!(
                formatter,
                "timed out draining domain '{}' on node '{}': {} outstanding work item(s) \
                 (ingestors={}, generators={}, acknowledgements={}, emitter_buffers={}",
                self.domain.as_str(),
                node,
                total,
                self.active_ingestors,
                self.active_generators,
                self.outstanding_acks,
                self.buffered_emitter_messages,
            )?;
            self.write_emitter_publishing(formatter)?;
            formatter.write_str(")")
        } else if let Some(status_error) = &self.status_error {
            write!(
                formatter,
                "timed out draining domain '{}': {status_error}",
                self.domain.as_str()
            )
        } else {
            write!(
                formatter,
                "timed out draining domain '{}' because no node reported drain status",
                self.domain.as_str()
            )
        }
    }
}

#[derive(Debug, Error)]
enum DomainAlterError {
    #[error("domain '{domain}' already has a model alteration in progress")]
    ConcurrentAlter { domain: DomainName },
    #[error("{outstanding}")]
    QuiesceTimeout { outstanding: DrainOutstanding },
    #[error(
        "timed out draining domain '{domain}' for {operation}: pending_node={pending_node}, \
         relay_buffers={buffered_relay_batches}, node_work_items={node_work_items}, \
         outstanding_acks={outstanding_acks}{emitter_publishing}"
    )]
    EntityQuiesceTimeout {
        domain: DomainName,
        operation: &'static str,
        pending_node: ClusterNodeName,
        buffered_relay_batches: usize,
        node_work_items: usize,
        outstanding_acks: usize,
        emitter_publishing: String,
    },
    #[error("failed {operation} gate in domain '{domain}': {reason}")]
    EntityGate {
        domain: DomainName,
        operation: &'static str,
        reason: String,
    },
    #[error("failed to pause domain '{domain}' for model alteration: {reason}")]
    PauseDomain { domain: DomainName, reason: String },
    #[error("failed to stop ingestion in domain '{domain}' for model alteration: {reason}")]
    StopIngestion { domain: DomainName, reason: String },
    #[error("failed to resume domain '{domain}' after model alteration: {reason}")]
    ResumeDomain { domain: DomainName, reason: String },
    #[error("failed to restore ingestion in domain '{domain}' after model alteration: {reason}")]
    RestoreIngestion { domain: DomainName, reason: String },
    #[error("failed to roll back model alteration in domain '{domain}': {reason}")]
    Rollback { domain: DomainName, reason: String },
}

struct ClusterEntityGate {
    operation_id: u64,
    domain: DomainName,
    /// Nodes whose gate engagement was attempted and not yet released. Membership decides both
    /// what still needs releasing and what a repeated attempt must not duplicate, so this is a set.
    nodes: BTreeSet<ClusterNodeName>,
    release_owner: Option<SessionServiceImpl>,
}

struct PendingClusterEntityGateRelease {
    operation_id: u64,
    domain: DomainName,
    nodes: BTreeSet<ClusterNodeName>,
}

struct PlannedOwnershipHandoff {
    operation_id: String,
    base_schedule_fingerprint: [u8; 32],
    target_schedule_fingerprint: [u8; 32],
    node_incarnations: BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
    gate: ClusterEntityGate,
    moves: Vec<PlannedOwnershipMove>,
    started_at: tokio::time::Instant,
    preparation_deadline: tokio::time::Instant,
    activation_deadline: tokio::time::Instant,
}

struct InterconnectRelayPayloadLane {
    sender: mpsc::UnboundedSender<RelayPayload>,
}

impl InterconnectRelayPayloadLane {
    fn new() -> (Self, mpsc::UnboundedReceiver<RelayPayload>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Self { sender }, receiver)
    }

    /// Moves relay payload work off the ordered control lane without awaiting it. The dedicated
    /// receiver still processes payloads in arrival order, while gate release and status controls
    /// remain runnable when one payload is waiting for a relay gate.
    fn route(&self, envelope: Envelope) -> Option<Envelope> {
        let Envelope::RelayPayload(payload) = envelope else {
            return Some(envelope);
        };
        if self.sender.send(payload).is_err() {
            warn!("interconnect relay payload lane is unavailable");
        }
        None
    }
}

/// One entity-gate engagement the leader asks a node to perform: the operation it belongs to, the
/// domain relays and entities it freezes, how long the node may take, and why. The local and remote
/// paths consume the same value, so the two cannot describe different gates.
#[derive(Clone, Copy)]
struct EntityGateEngagement<'a> {
    operation_id: u64,
    domain: &'a DomainName,
    relays: &'a [RelayName],
    affected_entities: &'a [NodeRef],
    purpose: EntityGatePurpose,
    deadline: tokio::time::Instant,
    reason: &'a str,
}

/// A model as `DESCRIBE` reads it: the configuration it was created with, and the schedule entry
/// that places it while its domain is running.
struct DescribedModel<M> {
    config: M,
    scheduled: Option<ScheduledNode>,
}

struct OnnxModelMetadata {
    inputs: HashMap<String, OnnxTensorMetadata>,
    outputs: HashMap<String, OnnxTensorMetadata>,
}

struct OnnxTensorMetadata {
    value_type: ValueType,
}

impl OnnxModelMetadata {
    fn validate_binding_names(&self, processor: &CreateInferencer) -> Result<(), String> {
        self.validate_direction_binding_names(
            processor,
            "input",
            processor
                .inputs
                .iter()
                .map(|mapping| mapping.tensor.as_str()),
            &self.inputs,
        )?;
        self.validate_direction_binding_names(
            processor,
            "output",
            processor
                .output_schema
                .iter()
                .map(|declaration| declaration.tensor.as_str()),
            &self.outputs,
        )
    }

    fn validate_direction_binding_names<'a>(
        &self,
        processor: &CreateInferencer,
        direction: &str,
        tensors: impl IntoIterator<Item = &'a str>,
        model_tensors: &HashMap<String, OnnxTensorMetadata>,
    ) -> Result<(), String> {
        let mut declared = HashSet::default();
        for tensor in tensors {
            if !declared.insert(tensor) {
                return Err(format!(
                    "inferencer '{}' has duplicate {} binding for ONNX tensor '{}'",
                    processor.name.as_str(),
                    direction,
                    tensor
                ));
            }
            if !model_tensors.contains_key(tensor) {
                return Err(format!(
                    "inferencer '{}' missing ONNX {} tensor '{}'",
                    processor.name.as_str(),
                    direction,
                    tensor
                ));
            }
        }
        let mut missing_bindings = model_tensors
            .keys()
            .filter(|tensor| !declared.contains(tensor.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        missing_bindings.sort();
        if let Some(tensor) = missing_bindings.first() {
            return Err(format!(
                "inferencer '{}' missing {} binding for ONNX {} tensor '{}'",
                processor.name.as_str(),
                if direction == "input" {
                    "INPUTS"
                } else {
                    "OUTPUT SCHEMA"
                },
                direction,
                tensor
            ));
        }
        Ok(())
    }
}

impl OnnxTensorMetadata {
    fn validate_declared_schema(
        &self,
        processor: &CreateInferencer,
        direction: &str,
        tensor: &str,
        schema: &InferencerTensorSchema,
    ) -> Result<(), String> {
        let ValueType::Tensor { ty, shape, .. } = &self.value_type else {
            return Err(format!(
                "inferencer '{}' {} tensor '{}' expected dense ONNX tensor, got {}",
                processor.name.as_str(),
                direction,
                tensor,
                self.value_type
            ));
        };
        if *ty != TensorElementType::Float32 {
            return Err(format!(
                "inferencer '{}' {} tensor '{}' has incompatible element type: ONNX {} vs \
                 declared F32",
                processor.name.as_str(),
                direction,
                tensor,
                ty
            ));
        }
        let incompatible_shape = shape.len() != schema.dimensions.len()
            || shape
                .iter()
                .zip(&schema.dimensions)
                .any(|(actual, declared)| match declared {
                    InferencerTensorDimension::Fixed(declared) => {
                        *actual >= 0 && *actual != i64::from(declared.get())
                    }
                    InferencerTensorDimension::Dynamic => *actual >= 0,
                    InferencerTensorDimension::Batch => *actual >= 0,
                });
        if incompatible_shape {
            return Err(format!(
                "inferencer '{}' {} tensor '{}' has incompatible shape: ONNX {:?} vs declared {:?}",
                processor.name.as_str(),
                direction,
                tensor,
                shape.as_ref(),
                schema.dimensions
            ));
        }
        Ok(())
    }
}
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::{
    Request, Response, Status,
    metadata::MetadataMap,
    transport::{Identity as TonicIdentity, Server, ServerTlsConfig},
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{
    EnvFilter, fmt, fmt::writer::BoxMakeWriter, layer::SubscriberExt, util::SubscriberInitExt,
};
use triomphe::Arc;
use typed_builder::TypedBuilder;

use crate::{
    ConfiguredFaultInjection, cluster,
    domain_clock_authority::DomainClockAuthorityCandidates,
    memory_pressure::{MemoryPressureConfig, MemoryPressureController},
    proto,
    proto::{
        ClusterSummary, CommandRequest, CommandResult, CommandResultKind, Diagnostic,
        DomainEntitySnapshot, DomainInfo, DomainList, DomainSnapshot, ServerEvent,
        ServerEventLevel, SessionRequest, SessionResponse, SetActiveDomainRequest, SuggestRequest,
        SuggestResponse, Suggestion as ApiSuggestion, SuggestionKind,
        TransactionState as ApiTransactionState, TransactionStatus as ApiTransactionStatus,
        UploadResourceRequest, UploadResourceResponse,
        session_service_server::{SessionService, SessionServiceServer},
    },
    resource::{ResourceEntryContent, ResourceManifestEntry, ResourceStore},
    runtime::{
        CompiledProgramWithMaterializedInterest, EntityGateLease, IngestMessageHeaders,
        IngestorDescribe as RuntimeIngestorDescribe, KafkaIngestor, OwnershipHandoffError,
        OwnershipHandoffResult, RelayMessage, RelayRecordBatch, RelaySubscriptionReceiver,
        RelaySubscriptionRecvError, RetainedIngestHeaders, Runtime, RuntimeEvent,
        RuntimeMaterializedRelaySpec, RuntimeVmCompileContext, SignalingDataSink,
        WebsocketSignalingSession, compile_session_filter_map_program,
        execute_filter_map_on_record, scheduled_relay_owner_nodes,
    },
    runtime_schema,
    task_shutdown::JoinShutdown as _,
};

const LEADER_KAFKA_PARTITION_WATCH_INTERVAL: Duration = Duration::from_secs(1);
static SESSION_SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);

struct SessionSubscription {
    domain: DomainName,
    relay: RelayName,
    active: Arc<AtomicBool>,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

#[derive(Clone)]
struct SubscriptionFilter {
    bindings: Vec<SubscriptionMatcher>,
}

#[derive(Clone)]
struct SubscriptionMatcher {
    field: FieldName,
    expected: runtime_schema::RuntimeValue,
}

struct SessionSubscriptions {
    subscriptions: HashMap<SubscriptionName, SessionSubscription>,
    user: UserName,
    session_id: String,
    transaction_id: Option<String>,
}

#[derive(Debug, Clone)]
struct PendingSessionCommand {
    source: String,
    statement: ClientStatement,
    domain: String,
}

#[derive(Debug)]
enum SessionCommandOperation {
    Begin { domain: String },
    Queue(PendingSessionCommand),
    Commit,
    Revert,
    Execute(PendingSessionCommand),
}

struct SessionSubscriptionTaskConfig {
    filter_map: Option<CompiledProgramWithMaterializedInterest>,
    sensitivity: nervix_vm::SchemaSensitivity,
    delivery_behavior: SubscriptionDeliveryBehavior,
    batch_sample_rate: Option<f64>,
    runtime: Runtime,
    materialized_stream_owner_nodes: HashMap<RelayName, Option<ClusterNodeName>>,
    receiver: RelaySubscriptionReceiver<RelayRecordBatch>,
    tx: mpsc::Sender<Result<SessionResponse, Status>>,
}

impl SessionSubscriptions {
    #[cfg(test)]
    fn new() -> Self {
        Self::for_user(
            UserName::parse(DEFAULT_USER).expect("default user identifier must be valid"),
        )
    }

    fn for_user(user: UserName) -> Self {
        Self {
            subscriptions: HashMap::new(),
            user,
            session_id: uuid::Uuid::now_v7().to_string(),
            transaction_id: None,
        }
    }

    fn transaction_active(&self) -> bool {
        self.transaction_id.is_some()
    }

    fn plan_commands(
        &self,
        statements: Vec<ParsedClientStatement>,
        query: &str,
        request_domain: &str,
    ) -> Result<Vec<SessionCommandOperation>, String> {
        let mut transaction_active = self.transaction_active();
        let multi_statement = statements.len() > 1;
        let mut operations = Vec::with_capacity(statements.len());

        for parsed in statements {
            let span = parsed.span.clone();
            match parsed.statement {
                ClientStatement::BeginTransaction => {
                    if transaction_active {
                        return Err("transaction is already active".to_string());
                    }
                    transaction_active = true;
                    operations.push(SessionCommandOperation::Begin {
                        domain: request_domain.to_string(),
                    });
                }
                ClientStatement::CommitTransaction => {
                    if !transaction_active {
                        return Err("COMMIT requires an active transaction".to_string());
                    }
                    transaction_active = false;
                    operations.push(SessionCommandOperation::Commit);
                }
                ClientStatement::RevertTransaction => {
                    if !transaction_active {
                        return Err("REVERT requires an active transaction".to_string());
                    }
                    transaction_active = false;
                    operations.push(SessionCommandOperation::Revert);
                }
                statement => {
                    let command = PendingSessionCommand {
                        source: query[span].to_string(),
                        statement,
                        domain: request_domain.to_string(),
                    };
                    if transaction_active {
                        operations.push(SessionCommandOperation::Queue(command));
                    } else if multi_statement {
                        return Err("multiple commands require BEGIN".to_string());
                    } else {
                        operations.push(SessionCommandOperation::Execute(command));
                    }
                }
            }
        }

        Ok(operations)
    }

    fn bind_transaction(&mut self, id: String) {
        self.transaction_id = Some(id);
    }

    fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    fn detach_transaction(&mut self) -> Option<String> {
        self.transaction_id.take()
    }

    fn insert(
        &mut self,
        name: SubscriptionName,
        domain: DomainName,
        relay: RelayName,
        config: SessionSubscriptionTaskConfig,
    ) {
        let SessionSubscriptionTaskConfig {
            filter_map,
            sensitivity,
            delivery_behavior,
            batch_sample_rate,
            runtime,
            materialized_stream_owner_nodes,
            receiver,
            tx,
        } = config;
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let active = Arc::new(AtomicBool::new(true));
        let task_active = active.clone();
        let task_domain = domain.clone();
        let event_name = name.clone();
        let event_stream = relay.clone();
        let task = tokio::spawn(async move {
            let mut receiver = receiver;
            'subscription_loop: loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    batch = receiver.recv() => {
                        match batch {
                            Ok(batch) => {
                                let messages = match batch.try_into_messages() {
                                    Ok(messages) => messages,
                                    Err(error_and_batch) => {
                                        let (error, _) = *error_and_batch;
                                        let event = SessionResponse {
                                            event: Some(proto::session_response::Event::Server(
                                                ServerEvent {
                                                    level: i32::from(ServerEventLevel::Error),
                                                    message: format!(
                                                        "session subscription '{}' failed to expand relay batch: {}",
                                                        event_name, error
                                                    ),
                                                },
                                            )),
                                        };
                                        if tx.send(Ok(event)).await.is_err() {
                                            break 'subscription_loop;
                                        }
                                        continue;
                                    }
                                };
                                for message in messages {
                                    tokio::task::consume_budget().await;
                                    let Some(message) = (match filter_map.as_ref() {
                                        Some(filter_map) => {
                                            let execution_snapshot = match runtime
                                                .domain_execution_snapshot(&task_domain)
                                            {
                                                Ok(snapshot) => snapshot,
                                                Err(error) => {
                                                    let event = SessionResponse {
                                                        event: Some(proto::session_response::Event::Server(
                                                            ServerEvent {
                                                                level: i32::from(ServerEventLevel::Error),
                                                                message: format!(
                                                                    "session subscription '{}' could not read domain execution time: {}",
                                                                    event_name, error
                                                                ),
                                                            },
                                                        )),
                                                    };
                                                    if tx.send(Ok(event)).await.is_err() {
                                                        break 'subscription_loop;
                                                    }
                                                    continue;
                                                }
                                            };
                                            let side_inputs = match runtime
                                                .load_materialized_side_inputs(
                                                    &task_domain,
                                                    &message.key,
                                                    &filter_map.materialized_interest,
                                                    &materialized_stream_owner_nodes,
                                                )
                                                .await
                                            {
                                                Ok(values) => values,
                                                Err(error) => {
                                                    let event = SessionResponse {
                                                        event: Some(proto::session_response::Event::Server(
                                                            ServerEvent {
                                                                level: i32::from(ServerEventLevel::Error),
                                                                message: format!(
                                                                    "session subscription '{}' failed to load materialized side inputs: {}",
                                                                    event_name, error
                                                                ),
                                                            },
                                                        )),
                                                    };
                                                    if tx.send(Ok(event)).await.is_err() {
                                                        break 'subscription_loop;
                                                    }
                                                    continue;
                                                }
                                            };
                                            match execute_filter_map_on_record(
                                                &event_name,
                                                filter_map,
                                                message.record.clone(),
                                                message.key.as_ref(),
                                                None,
                                                &side_inputs,
                                                execution_snapshot.now(),
                                            )
                                            .await
                                            {
                                            Ok(Some(record)) => Some(RelayMessage {
                                                key: message.key,
                                                record,
                                                acks: message.acks,
                                            }),
                                            Ok(None) => None,
                                            Err(error) => {
                                                let event = SessionResponse {
                                                    event: Some(proto::session_response::Event::Server(
                                                        ServerEvent {
                                                            level: i32::from(ServerEventLevel::Error),
                                                            message: format!(
                                                                "session subscription '{}' FILTER-MAP failed: {}",
                                                                event_name, error
                                                            ),
                                                        },
                                                    )),
                                                };
                                                if tx.send(Ok(event)).await.is_err() {
                                                    break 'subscription_loop;
                                                }
                                                continue;
                                            }
                                            }
                                        }
                                        None => Some(message),
                                    }) else {
                                        continue;
                                    };
                                    if !subscription_sample_passes(batch_sample_rate, &message) {
                                        continue;
                                    }
                                    let payload = format_stream_message(&message, &sensitivity);
                                    let event = SessionResponse {
                                        event: Some(proto::session_response::Event::Subscription(
                                            proto::SubscriptionEvent {
                                                subscription: event_name.as_str().to_string(),
                                                relay: event_stream.as_str().to_string(),
                                                payload,
                                            }
                                        )),
                                    };
                                    match delivery_behavior {
                                        SubscriptionDeliveryBehavior::Blocking => {
                                            if tx.send(Ok(event)).await.is_err() {
                                                break 'subscription_loop;
                                            }
                                        }
                                        SubscriptionDeliveryBehavior::Dropping => {
                                            match tx.try_send(Ok(event)) {
                                                Ok(()) => {}
                                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                    break 'subscription_loop;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(RelaySubscriptionRecvError::Closed) => {
                                let event = SessionResponse {
                                    event: Some(proto::session_response::Event::Server(
                                        ServerEvent {
                                            level: i32::from(ServerEventLevel::Error),
                                            message: format!(
                                                "session subscription '{}' was dropped because \
                                                 relay '{}' in domain '{}' was rebuilt after a \
                                                 schema or execution change; recreate the \
                                                 subscription against the current schema",
                                                event_name,
                                                event_stream,
                                                task_domain,
                                            ),
                                        },
                                    )),
                                };
                                tx.send(Ok(event))
                                    .await
                                    .means_peer_left("session subscription stream");
                                break 'subscription_loop;
                            }
                            Err(RelaySubscriptionRecvError::Overflowed(_)) => continue,
                        }
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break 'subscription_loop;
                        }
                    }
                }
            }
            task_active.store(false, Ordering::Release);
        });

        self.subscriptions.insert(
            name,
            SessionSubscription {
                domain,
                relay,
                active,
                stop_tx,
                task,
            },
        );
    }

    fn contains_domain_stream(&self, domain: &DomainName, relay: &RelayName) -> bool {
        self.subscriptions.values().any(|subscription| {
            subscription.active.load(Ordering::Acquire)
                && subscription.domain == *domain
                && subscription.relay == *relay
        })
    }

    fn contains_name(&self, name: &SubscriptionName) -> bool {
        self.subscriptions
            .get(name)
            .is_some_and(|subscription| subscription.active.load(Ordering::Acquire))
    }

    fn matching_names(&self, prefix: &str) -> Vec<String> {
        let prefix = prefix.to_ascii_lowercase();
        self.subscriptions
            .keys()
            .filter(|name| {
                self.contains_name(name)
                    && (prefix.is_empty() || name.as_str().starts_with(&prefix))
            })
            .map(ToString::to_string)
            .collect()
    }

    async fn remove(&mut self, name: &SubscriptionName) -> Option<(DomainName, RelayName)> {
        let subscription = self.subscriptions.remove(name)?;
        subscription.stop_tx.send_replace(true);
        subscription
            .task
            .join_after_shutdown("session subscription")
            .await;
        Some((subscription.domain, subscription.relay))
    }

    async fn stop_all(&mut self, service: &SessionServiceImpl) {
        for (_, subscription) in self.subscriptions.drain() {
            subscription.stop_tx.send_replace(true);
            subscription
                .task
                .join_after_shutdown("session subscription")
                .await;
            service
                .unregister_subscription_interest(&subscription.domain, &subscription.relay)
                .await;
        }
    }
}

fn format_stream_message(
    message: &RelayMessage,
    sensitivity: &nervix_vm::SchemaSensitivity,
) -> String {
    let payload = message
        .record
        .to_json_string_masking(sensitivity)
        .unwrap_or_else(|error| format!("<invalid Arrow row: {error}>"));
    match message.key.as_ref() {
        Some(key) => format!("key={} payload={}", key.as_str(), payload),
        None => payload,
    }
}

fn validate_subscription_bindings(
    relay: &RelayName,
    branching: &[FieldName],
    schema: &nervix_models::CreateSchema,
    bindings: &[SubscriptionBinding],
) -> Result<SubscriptionFilter, String> {
    if branching.is_empty() {
        if bindings.is_empty() {
            return Ok(SubscriptionFilter {
                bindings: Vec::new(),
            });
        }
        return Err(format!(
            "stream '{}' is not branched and does not accept WHERE bindings",
            relay.as_str()
        ));
    }

    if bindings.is_empty() {
        return Err(format!(
            "stream '{}' requires WHERE bindings for ({})",
            relay.as_str(),
            branching
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let mut fields = HashMap::new();
    for field in &schema.fields {
        fields.insert(field.name.clone(), field.ty.clone());
    }

    let mut bound = HashMap::new();
    for binding in bindings {
        if bound
            .insert(binding.field.clone(), binding.value.clone())
            .is_some()
        {
            return Err(format!(
                "subscription binding '{}' is specified more than once",
                binding.field.as_str()
            ));
        }
    }

    let expected = SortedSet::from_unsorted(branching.to_vec()).into_vec();
    let actual = SortedSet::from_unsorted(bound.keys().cloned().collect::<Vec<_>>()).into_vec();
    if expected != actual {
        return Err(format!(
            "subscription bindings for relay '{}' must exactly match ({})",
            relay.as_str(),
            branching
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let mut matchers = Vec::new();
    for field in branching {
        let ty = fields.get(field).ok_or_else(|| {
            format!(
                "branch field '{}' is missing from schema '{}'",
                field.as_str(),
                schema.name.as_str()
            )
        })?;
        let literal = bound
            .get(field)
            .verified("the check above requires the bound keys to match the branch fields exactly");
        let expected = parse_subscription_literal(field, ty, literal)?;
        matchers.push(SubscriptionMatcher {
            field: field.clone(),
            expected,
        });
    }

    Ok(SubscriptionFilter { bindings: matchers })
}

fn branch_key_from_filter(
    branching: &[FieldName],
    filter: &SubscriptionFilter,
) -> Result<Option<crate::runtime::BranchKey>, String> {
    if branching.is_empty() {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(branching.len());
    for field in branching {
        let Some(binding) = filter
            .bindings
            .iter()
            .find(|binding| binding.field == *field)
        else {
            return Err(format!(
                "missing binding for branch field '{}'",
                field.as_str()
            ));
        };
        fields.push((field.clone(), binding.expected.clone()));
    }
    crate::runtime::BranchKey::from_fields(fields).map(Some)
}

fn render_subscription_literal(literal: &SubscriptionLiteral) -> String {
    match literal {
        SubscriptionLiteral::String(value) => format!("'{}'", value.replace('\'', "''")),
        SubscriptionLiteral::Number(value) => value.clone(),
        SubscriptionLiteral::Bool(value) => value.to_string(),
    }
}

fn parse_subscription_batch_sample_rate(rate: Option<&str>) -> Result<Option<f64>, String> {
    let Some(rate) = rate else {
        return Ok(None);
    };
    let parsed = rate
        .parse::<f64>()
        .map_err(|error| format!("invalid batch sample rate '{rate}': {error}"))?;
    if (0.0..=1.0).contains(&parsed) {
        Ok(Some(parsed))
    } else {
        Err(format!(
            "invalid batch sample rate '{rate}': must be between 0.0 and 1.0"
        ))
    }
}

fn subscription_sample_passes(batch_sample_rate: Option<f64>, message: &RelayMessage) -> bool {
    let Some(rate) = batch_sample_rate else {
        return true;
    };
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }

    let counter = SESSION_SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut hasher = Hasher::new();
    hasher.update(&counter.to_le_bytes());
    if let Some(key) = message.key.as_ref() {
        hasher.update(key.as_str().as_bytes());
    }
    let hash = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    let draw = u64::from_le_bytes(bytes).approx_into::<f64>() / u64::MAX.approx_into::<f64>();
    draw < rate
}

fn parse_subscription_literal(
    field: &FieldName,
    ty: &ParseAsType,
    literal: &SubscriptionLiteral,
) -> Result<runtime_schema::RuntimeValue, String> {
    use runtime_schema::RuntimeValue;

    let bad = |expected: &str| {
        format!(
            "subscription binding '{}' expects {} literal for type {:?}",
            field.as_str(),
            expected,
            ty
        )
    };

    match (ty, literal) {
        (ParseAsType::String, SubscriptionLiteral::String(v)) => {
            Ok(RuntimeValue::String(v.clone()))
        }
        (ParseAsType::Datetime, SubscriptionLiteral::String(v)) => {
            chrono::DateTime::parse_from_rfc3339(v)
                .map(RuntimeValue::Datetime)
                .map_err(|_| bad("RFC3339 datetime string"))
        }
        (ParseAsType::Bool, SubscriptionLiteral::Bool(v)) => Ok(RuntimeValue::Bool(*v)),
        (ParseAsType::U8, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U8).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I8, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I8).map_err(|_| bad("numeric"))
        }
        (ParseAsType::U16, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U16).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I16, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I16).map_err(|_| bad("numeric"))
        }
        (ParseAsType::U32, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U32).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I32, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I32).map_err(|_| bad("numeric"))
        }
        (ParseAsType::U64, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U64).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I64, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I64).map_err(|_| bad("numeric"))
        }
        (ParseAsType::F32, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::F32).map_err(|_| bad("numeric"))
        }
        (ParseAsType::F64, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::F64).map_err(|_| bad("numeric"))
        }
        _ => Err(bad(match ty {
            ParseAsType::String | ParseAsType::Datetime => "string",
            ParseAsType::Bool => "boolean",
            ParseAsType::Array { .. } | ParseAsType::Vec { .. } => "array",
            _ => "numeric",
        })),
    }
}

fn empty_body() -> Empty<Bytes> {
    Empty::new()
}

fn response_with_status(status: StatusCode) -> HyperResponse<Empty<Bytes>> {
    HyperResponse::builder()
        .status(status)
        .body(empty_body())
        .assured(
            "the status and header values are typed constants or generated ASCII, which the http \
             builder always accepts",
        )
}

fn endpoint_rejection_response(retry_after: Option<Duration>) -> HyperResponse<Empty<Bytes>> {
    let mut response = HyperResponse::builder().status(StatusCode::SERVICE_UNAVAILABLE);
    if let Some(retry_after) = retry_after {
        // `Retry-After` is whole seconds, so a sub-second remainder rounds the wait up.
        let seconds = retry_after
            .as_secs()
            .checked_add(u64::from(retry_after.subsec_nanos() > 0))
            .assured("a Duration's whole seconds leave room for the rounding increment");
        response = response.header(RETRY_AFTER, seconds.to_string());
    }
    response.body(empty_body()).assured(
        "the status and header values are typed constants or generated ASCII, which the http \
         builder always accepts",
    )
}

fn response_with_bytes(
    status: StatusCode,
    body: impl Into<Bytes>,
    content_type: &'static str,
) -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, content_type)
        .body(Full::new(body.into()))
        .assured(
            "the status and header values are typed constants or generated ASCII, which the http \
             builder always accepts",
        )
}

fn text_response(status: StatusCode, body: impl Into<Bytes>) -> HyperResponse<Full<Bytes>> {
    response_with_bytes(status, body, "text/plain; charset=utf-8")
}

fn web_console_upload_text_response(
    status: StatusCode,
    body: impl Into<Bytes>,
) -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(ACCESS_CONTROL_ALLOW_METHODS, "POST, OPTIONS")
        .header(ACCESS_CONTROL_ALLOW_HEADERS, "content-type")
        .body(Full::new(body.into()))
        .assured(
            "the status and header values are typed constants or generated ASCII, which the http \
             builder always accepts",
        )
}

fn redirect_response(location: &'static str) -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(StatusCode::PERMANENT_REDIRECT)
        .header(LOCATION, location)
        .body(Full::new(Bytes::new()))
        .assured(
            "the status and header values are typed constants or generated ASCII, which the http \
             builder always accepts",
        )
}

fn header_contains_token(value: &hyper::header::HeaderValue, expected: &str) -> bool {
    value.to_str().ok().is_some_and(|raw| {
        raw.split(',')
            .any(|part| part.trim().eq_ignore_ascii_case(expected))
    })
}

fn is_websocket_upgrade_request(request: &HyperRequest<HyperIncoming>) -> bool {
    request.method() == Method::GET
        && request
            .headers()
            .get(CONNECTION)
            .is_some_and(|value| header_contains_token(value, "upgrade"))
        && request
            .headers()
            .get(UPGRADE)
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
        && request
            .headers()
            .get(SEC_WEBSOCKET_VERSION)
            .is_some_and(|value| value.as_bytes() == b"13")
}

/// Ingests data frames that arrive on a server-side endpoint while its handshake is running.
struct EndpointSignalingDataSink<'a> {
    runtime: &'a Runtime,
    host: &'a str,
    path: &'a str,
    headers: &'a RetainedIngestHeaders,
}

impl SignalingDataSink for EndpointSignalingDataSink<'_> {
    async fn accept(&self, payload: Vec<u8>) {
        self.runtime
            .dispatch_websocket_payload(self.host, self.path, payload.as_slice(), self.headers)
            .await;
    }
}

/// The headers of one borrowed request, skipping values that are not UTF-8.
struct HyperRequestHeaders<'a>(&'a hyper::HeaderMap);

impl IngestMessageHeaders for HyperRequestHeaders<'_> {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in self.0 {
            if let Ok(value) = value.to_str() {
                visit(name.as_str(), value);
            }
        }
    }
}

async fn handle_http_request(
    runtime: Runtime,
    request_tasks: TaskTracker,
    shutdown: CancellationToken,
    mut request: HyperRequest<HyperIncoming>,
) -> Result<HyperResponse<Empty<Bytes>>, Infallible> {
    let mut host = String::new();
    if let Some(value) = request.headers().get(HOST)
        && let Ok(value) = value.to_str()
    {
        host = value.to_string();
    }
    let path = request.uri().path().to_string();

    if runtime.has_websocket_endpoint(&host, &path).await {
        if !is_websocket_upgrade_request(&request) {
            return Ok(response_with_status(StatusCode::UPGRADE_REQUIRED));
        }
        let admission = runtime.websocket_endpoint_admission(&host, &path).await;
        if !admission.is_accepted() {
            return Ok(endpoint_rejection_response(admission.retry_after));
        }
        // The session outlives the upgrade request, so its handshake headers are copied
        // once here and appended from that copy for every later frame.
        let headers = RetainedIngestHeaders::capture(&HyperRequestHeaders(request.headers()));

        let Some(sec_websocket_key) = request.headers().get(SEC_WEBSOCKET_KEY) else {
            return Ok(response_with_status(StatusCode::BAD_REQUEST));
        };
        let Ok(sec_websocket_key) = sec_websocket_key.to_str() else {
            return Ok(response_with_status(StatusCode::BAD_REQUEST));
        };
        let sec_websocket_key = sec_websocket_key.to_owned();

        let response = HyperResponse::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(
                SEC_WEBSOCKET_ACCEPT,
                derive_accept_key(sec_websocket_key.as_bytes()),
            )
            .body(empty_body())
            .assured(
                "the status and header values are typed constants or generated ASCII, which the \
                 http builder always accepts",
            );

        let on_upgrade = upgrade::on(&mut request);
        request_tasks.spawn(async move {
            let upgraded = tokio::select! {
                _ = shutdown.cancelled() => return,
                upgraded = on_upgrade => upgraded,
            };
            match upgraded {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    let mut websocket =
                        WebSocketStream::from_raw_socket(io, Role::Server, None).await;

                    if let Some(protocol) = runtime
                        .websocket_endpoint_signaling_protocol(&host, &path)
                        .await
                    {
                        let session = WebsocketSignalingSession::new(protocol);
                        let sink = EndpointSignalingDataSink {
                            runtime: &runtime,
                            host: &host,
                            path: &path,
                            headers: &headers,
                        };
                        let session_result = tokio::select! {
                            _ = shutdown.cancelled() => return,
                            result = session.run(&mut websocket, &sink) => result,
                        };
                        if let Err(error) = session_result {
                            warn!(
                                error = %error,
                                host,
                                path,
                                "websocket signaling failed"
                            );
                            return;
                        }
                    }

                    loop {
                        let message = tokio::select! {
                            _ = shutdown.cancelled() => break,
                            message = futures_util::StreamExt::next(&mut websocket) => message,
                        };
                        let Some(message) = message else {
                            break;
                        };
                        match message {
                            Ok(Message::Text(payload)) => {
                                let outcome = runtime
                                    .dispatch_websocket_payload(
                                        &host,
                                        &path,
                                        payload.as_bytes(),
                                        &headers,
                                    )
                                    .await;
                                if !outcome.is_accepted() {
                                    websocket
                                        .send(Message::Close(Some(CloseFrame {
                                            code: CloseCode::Again,
                                            reason: "Try Again Later".into(),
                                        })))
                                        .await
                                        .means_peer_left("websocket ingest client");
                                    break;
                                }
                            }
                            Ok(Message::Binary(payload)) => {
                                let outcome = runtime
                                    .dispatch_websocket_payload(
                                        &host,
                                        &path,
                                        payload.as_ref(),
                                        &headers,
                                    )
                                    .await;
                                if !outcome.is_accepted() {
                                    websocket
                                        .send(Message::Close(Some(CloseFrame {
                                            code: CloseCode::Again,
                                            reason: "Try Again Later".into(),
                                        })))
                                        .await
                                        .means_peer_left("websocket ingest client");
                                    break;
                                }
                            }
                            Ok(Message::Ping(payload)) => {
                                if websocket.send(Message::Pong(payload)).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
                            Err(error) => {
                                warn!(error = %error, host, path, "websocket session failed");
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(error = %error, host, path, "http upgrade failed");
                }
            }
        });

        return Ok(response);
    }

    if runtime.has_http_endpoint(&host, &path).await {
        if request.method() != Method::POST {
            return Ok(response_with_status(StatusCode::METHOD_NOT_ALLOWED));
        }
        let body = match request.body_mut().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(error) => {
                warn!(error = %error, host, path, "failed to read http request body");
                return Ok(response_with_status(StatusCode::BAD_REQUEST));
            }
        };

        let outcome = runtime
            .dispatch_http_payload(
                &host,
                &path,
                body.as_ref(),
                &HyperRequestHeaders(request.headers()),
            )
            .await;
        return Ok(if outcome.is_accepted() {
            response_with_status(StatusCode::ACCEPTED)
        } else {
            endpoint_rejection_response(outcome.retry_after)
        });
    }

    Ok(response_with_status(StatusCode::NOT_FOUND))
}

async fn serve_http(
    runtime: Runtime,
    request_tasks: TaskTracker,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>> {
    let mut connection_tasks = JoinSet::new();

    loop {
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() => {
                accepted.change_context(AppError::ServeHttp)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeHttp)?;
        let runtime = runtime.clone();
        let request_tasks = request_tasks.clone();
        let request_shutdown = shutdown.clone();
        connection_tasks.spawn(async move {
            let io = TokioIo::new(stream);
            if let Err(error) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| {
                        handle_http_request(
                            runtime.clone(),
                            request_tasks.clone(),
                            request_shutdown.clone(),
                            request,
                        )
                    }),
                )
                .with_upgrades()
                .await
            {
                warn!(error = %error, "http connection failed");
            }
        });
    }
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

async fn serve_https(
    runtime: Runtime,
    request_tasks: TaskTracker,
    http_tls_server_config: Arc<RwLock<Option<StdArc<ServerConfig>>>>,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>> {
    let mut connection_tasks = JoinSet::new();

    loop {
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() => {
                accepted.change_context(AppError::ServeHttps)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeHttps)?;
        let runtime = runtime.clone();
        let request_tasks = request_tasks.clone();
        let request_shutdown = shutdown.clone();
        let http_tls_server_config = http_tls_server_config.clone();
        connection_tasks.spawn(async move {
            let Some(tls_config) = http_tls_server_config.read().clone() else {
                warn!("https connection rejected because no VHOST TLS configuration is loaded");
                return;
            };
            let acceptor = TlsAcceptor::from(tls_config);
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    let io = TokioIo::new(tls_stream);
                    if let Err(error) = http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |request| {
                                handle_http_request(
                                    runtime.clone(),
                                    request_tasks.clone(),
                                    request_shutdown.clone(),
                                    request,
                                )
                            }),
                        )
                        .with_upgrades()
                        .await
                    {
                        warn!(error = %error, "https connection failed");
                    }
                }
                Err(error) => {
                    warn!(error = %error, "tls accept failed");
                }
            }
        });
    }
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

async fn handle_observability_request(
    consensus: Observer,
    runtime: crate::runtime::Runtime,
    request: HyperRequest<HyperIncoming>,
) -> Result<HyperResponse<Full<Bytes>>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, OBSERVABILITY_LIVEZ_PATH) => text_response(StatusCode::OK, "live\n"),
        (&Method::GET, OBSERVABILITY_READYZ_PATH) => {
            if consensus.current_leader().await.is_some() {
                text_response(StatusCode::OK, "ready\n")
            } else {
                text_response(StatusCode::SERVICE_UNAVAILABLE, "leader unknown\n")
            }
        }
        (&Method::GET, OBSERVABILITY_METRICS_PATH) => {
            text_response(StatusCode::OK, runtime.metrics().prometheus_text())
        }
        (&Method::GET, _) => text_response(StatusCode::NOT_FOUND, "not found"),
        _ => text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    };

    Ok(response)
}

async fn serve_observability_http(
    consensus: Observer,
    runtime: crate::runtime::Runtime,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>> {
    let mut connection_tasks = JoinSet::new();

    loop {
        tokio::task::consume_budget().await;
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() => {
                accepted.change_context(AppError::ServeObservability)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeObservability)?;
        let consensus = consensus.clone();
        let runtime = runtime.clone();
        connection_tasks.spawn(async move {
            let io = TokioIo::new(stream);
            if let Err(error) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| {
                        handle_observability_request(consensus.clone(), runtime.clone(), request)
                    }),
                )
                .await
            {
                warn!(error = %error, "observability connection failed");
            }
        });
    }
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

async fn handle_web_console_request(
    service: SessionServiceImpl,
    mut request: HyperRequest<HyperIncoming>,
) -> Result<HyperResponse<Full<Bytes>>, Infallible> {
    if request.method() == Method::GET && request.uri().path() == WEB_CONSOLE_WS_PATH {
        let Some(credentials) = credentials_from_web_console_request(&request) else {
            return Ok(unauthorized_basic_response());
        };
        let Some(authenticated_user) = service.authenticate_basic_credentials(&credentials).await
        else {
            return Ok(unauthorized_basic_response());
        };

        if !is_websocket_upgrade_request(&request) {
            return Ok(response_with_bytes(
                StatusCode::UPGRADE_REQUIRED,
                Bytes::new(),
                "text/plain; charset=utf-8",
            ));
        }

        let Some(sec_websocket_key) = request.headers().get(SEC_WEBSOCKET_KEY) else {
            return Ok(text_response(
                StatusCode::BAD_REQUEST,
                "missing websocket key",
            ));
        };
        let Ok(sec_websocket_key) = sec_websocket_key.to_str() else {
            return Ok(text_response(
                StatusCode::BAD_REQUEST,
                "missing websocket key",
            ));
        };
        let sec_websocket_key = sec_websocket_key.to_owned();

        let response = HyperResponse::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(
                SEC_WEBSOCKET_ACCEPT,
                derive_accept_key(sec_websocket_key.as_bytes()),
            )
            .body(Full::new(Bytes::new()))
            .assured(
                "the status and header values are typed constants or generated ASCII, which the \
                 http builder always accepts",
            );

        let on_upgrade = upgrade::on(&mut request);
        let service_tasks = service.inner.service_tasks.clone();
        service_tasks.spawn(async move {
            let upgraded = tokio::select! {
                _ = service.inner.shutdown.cancelled() => return,
                upgraded = on_upgrade => upgraded,
            };
            match upgraded {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    let mut websocket =
                        WebSocketStream::from_raw_socket(io, Role::Server, None).await;
                    let (tx, mut response_rx) = mpsc::channel(16);
                    let mut subscriptions = SessionSubscriptions::for_user(authenticated_user);
                    let mut leadership_check = interval(WEB_CONSOLE_LEADERSHIP_CHECK_INTERVAL);
                    let mut graph_snapshot = interval(WEB_CONSOLE_GRAPH_SNAPSHOT_INTERVAL);
                    let mut domains_rx = service.inner.consensus.subscribe_domains();
                    leadership_check.tick().await;
                    graph_snapshot.tick().await;
                    let mut leader_connected = false;
                    let mut active_domain = None::<DomainName>;
                    let mut clean_close = false;

                    loop {
                        tokio::task::consume_budget().await;
                        tokio::select! {
                            _ = service.inner.shutdown.cancelled() => break,
                            message = futures_util::StreamExt::next(&mut websocket) => {
                                let Some(message) = message else {
                                    break;
                                };
                                match message {
                                    Ok(Message::Binary(payload)) => {
                                        match proto::SessionRequest::decode(payload.as_ref()) {
                                            Ok(request) => {
                                                match request.request {
                                                    Some(proto::session_request::Request::SetActiveDomain(request)) => {
                                                        match service
                                                            .process_web_console_active_domain_request(
                                                                request,
                                                                &mut active_domain,
                                                            )
                                                            .await
                                                        {
                                                            Ok(response) => {
                                                                if !send_web_console_session_response(
                                                                    &mut websocket,
                                                                    response,
                                                                )
                                                                .await
                                                                {
                                                                    break;
                                                                }
                                                                if leader_connected
                                                                    && !send_web_console_state_responses(
                                                                        &mut websocket,
                                                                        &service,
                                                                        active_domain.as_ref(),
                                                                    )
                                                                    .await
                                                                {
                                                                    break;
                                                                }
                                                            }
                                                            Err(error) => {
                                                                if !send_web_console_session_response(
                                                                    &mut websocket,
                                                                    web_console_server_error_response(
                                                                        error.to_string(),
                                                                    ),
                                                                )
                                                                .await
                                                                {
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    _ => {
                                                        let response = service
                                                            .process_web_console_request(
                                                                request,
                                                                &tx,
                                                                &mut subscriptions,
                                                            )
                                                            .await;
                                                        if !send_web_console_session_response(
                                                            &mut websocket,
                                                            response,
                                                        )
                                                        .await
                                                        {
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            Err(error) => {
                                                warn!(
                                                    error = %error,
                                                    "failed to decode web console websocket protobuf request"
                                                );
                                                let response = web_console_server_error_response(
                                                    format!(
                                                        "failed to decode protobuf request: {error}"
                                                    ),
                                                );
                                                if !send_web_console_session_response(
                                                    &mut websocket,
                                                    response,
                                                )
                                                .await
                                                {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    Ok(Message::Ping(payload)) => {
                                        if websocket.send(Message::Pong(payload)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Ok(Message::Close(_)) => {
                                        clean_close = true;
                                        break;
                                    }
                                    Ok(_) => {}
                                    Err(error) => {
                                        warn!(error = %error, "web console websocket failed");
                                        break;
                                    }
                                }
                            }
                            response = response_rx.recv() => {
                                let Some(response) = response else {
                                    break;
                                };
                                match response {
                                    Ok(response) => {
                                        if !send_web_console_session_response(
                                            &mut websocket,
                                            response,
                                        )
                                        .await
                                        {
                                            break;
                                        }
                                    }
                                    Err(status) => {
                                        let response = web_console_server_error_response(
                                            status.message().to_string(),
                                        );
                                        if !send_web_console_session_response(
                                            &mut websocket,
                                            response,
                                        )
                                        .await
                                        {
                                            break;
                                        }
                                    }
                                }
                            }
                            _ = leadership_check.tick() => {
                                let Some(response) = service
                                    .web_console_leadership_response(leader_connected)
                                    .await
                                else {
                                    continue;
                                };
                                let close_after_send =
                                    response.event.as_ref().is_some_and(|event| {
                                        if let proto::session_response::Event::Result(result) =
                                            event
                                        {
                                            proto::CommandResultKind::try_from(result.kind).ok()
                                                == Some(proto::CommandResultKind::NotLeader)
                                        } else {
                                            false
                                        }
                                    });
                                let send_domain_snapshots =
                                    !close_after_send && !leader_connected;
                                if !send_web_console_session_response(
                                    &mut websocket,
                                    response,
                                )
                                .await
                                {
                                    break;
                                }
                                if close_after_send {
                                    break;
                                }
                                if send_domain_snapshots {
                                    leader_connected = true;
                                    let domain_response = service.domain_list_response(false).await;
                                    if !send_web_console_session_response(
                                        &mut websocket,
                                        domain_response,
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                    if !send_web_console_state_responses(
                                        &mut websocket,
                                        &service,
                                        active_domain.as_ref(),
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                }
                            }
                            _ = graph_snapshot.tick(), if leader_connected => {
                                if !send_web_console_state_responses(
                                    &mut websocket,
                                    &service,
                                    active_domain.as_ref(),
                                )
                                .await
                                {
                                    break;
                                }
                            }
                            changed = domains_rx.changed(), if leader_connected => {
                                if changed.is_err() {
                                    break;
                                }
                                if let Some(domain) = active_domain.as_ref()
                                    && !domains_rx.borrow().contains_key(domain)
                                {
                                    active_domain = None;
                                }
                                let domain_response = service.domain_list_response(false).await;
                                if !send_web_console_session_response(
                                    &mut websocket,
                                    domain_response,
                                )
                                .await
                                {
                                    break;
                                }
                                if !send_web_console_state_responses(
                                    &mut websocket,
                                    &service,
                                    active_domain.as_ref(),
                                )
                                .await
                                {
                                    break;
                                }
                            }
                        }
                    }
                    subscriptions.stop_all(&service).await;
                    if clean_close {
                        service.clean_close_transaction(&mut subscriptions).await;
                    } else {
                        service.release_session_transaction_binding(&mut subscriptions);
                    }
                }
                Err(error) => {
                    warn!(error = %error, "web console websocket upgrade failed");
                }
            }
        });

        return Ok(response);
    }

    if request.method() == Method::OPTIONS
        && request.uri().path() == WEB_CONSOLE_RESOURCE_UPLOAD_PATH
    {
        return Ok(web_console_upload_text_response(StatusCode::NO_CONTENT, ""));
    }

    if request.method() == Method::POST && request.uri().path() == WEB_CONSOLE_RESOURCE_UPLOAD_PATH
    {
        let Some(credentials) = credentials_from_web_console_request(&request) else {
            return Ok(unauthorized_basic_response());
        };
        if service
            .authenticate_basic_credentials(&credentials)
            .await
            .is_none()
        {
            return Ok(unauthorized_basic_response());
        }

        return Ok(service.handle_web_console_resource_upload(request).await);
    }

    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/") | (&Method::GET, "/console") => redirect_response("/console/"),
        (&Method::GET, "/console/") | (&Method::GET, "/console/index.html") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_INDEX),
            "text/html; charset=utf-8",
        ),
        (&Method::GET, "/console/console.css") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_CSS),
            "text/css; charset=utf-8",
        ),
        (&Method::GET, "/console/nervix-web-console.js") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_JS),
            "text/javascript; charset=utf-8",
        ),
        (&Method::GET, "/console/nervix-web-console_bg.wasm") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_WASM),
            "application/wasm",
        ),
        (&Method::GET, "/console/nervix-icon.svg") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_ICON),
            "image/svg+xml",
        ),
        (&Method::GET, path) if path.starts_with("/console/") => {
            text_response(StatusCode::NOT_FOUND, Bytes::from_static(b"not found"))
        }
        (&Method::GET, _) => text_response(StatusCode::NOT_FOUND, "not found"),
        _ => text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    };

    Ok(response)
}

async fn send_web_console_session_response<S>(
    websocket: &mut WebSocketStream<S>,
    response: SessionResponse,
) -> bool
where
    WebSocketStream<S>: SinkExt<Message> + Unpin,
{
    websocket
        .send(Message::Binary(response.encode_to_vec()))
        .await
        .is_ok()
}

async fn send_web_console_state_responses<S>(
    websocket: &mut WebSocketStream<S>,
    service: &SessionServiceImpl,
    active_domain: Option<&DomainName>,
) -> bool
where
    WebSocketStream<S>: SinkExt<Message> + Unpin,
{
    if !send_web_console_session_response(
        websocket,
        service.web_console_cluster_summary_response().await,
    )
    .await
    {
        return false;
    }
    for response in service
        .web_console_domain_snapshot_responses(active_domain)
        .await
    {
        if !send_web_console_session_response(websocket, response).await {
            return false;
        }
    }
    true
}

fn web_console_server_error_response(message: String) -> SessionResponse {
    SessionResponse {
        event: Some(proto::session_response::Event::Server(ServerEvent {
            level: i32::from(ServerEventLevel::Error),
            message,
        })),
    }
}

fn web_console_query_param(query: Option<&str>, name: &str) -> Option<String> {
    url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
}

/// The credentials a `Basic` authorization token carries, or `None` when it carries none.
///
/// A token that is not base64, not UTF-8, or not `user:password` is a malformed header rather than
/// a wrong password, and the caller answers both the same way. Nothing about the token is reported,
/// because it is the secret.
fn credentials_from_basic_token(token: &str) -> Option<BasicAuthCredentials> {
    let decoded = BASE64_STANDARD.decode(token).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    if username.is_empty() {
        return None;
    }
    Some(BasicAuthCredentials {
        username: username.to_string(),
        password: password.to_string(),
    })
}

fn credentials_from_basic_authorization(value: &str) -> Option<BasicAuthCredentials> {
    let (scheme, token) = value.trim().split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Basic") {
        return None;
    }
    credentials_from_basic_token(token.trim())
}

fn credentials_from_metadata(metadata: &MetadataMap) -> Option<BasicAuthCredentials> {
    let value = metadata.get("authorization")?;
    let Ok(value) = value.to_str() else {
        return None;
    };
    credentials_from_basic_authorization(value)
}

fn credentials_from_web_console_request(
    request: &HyperRequest<HyperIncoming>,
) -> Option<BasicAuthCredentials> {
    if let Some(value) = request.headers().get(AUTHORIZATION)
        && let Ok(value) = value.to_str()
        && let Some(credentials) = credentials_from_basic_authorization(value)
    {
        return Some(credentials);
    }
    let token = web_console_query_param(request.uri().query(), WEB_CONSOLE_AUTH_QUERY_PARAM)?;
    credentials_from_basic_token(&token)
}

fn unauthorized_basic_response() -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            WWW_AUTHENTICATE,
            format!("Basic realm=\"{BASIC_AUTH_REALM}\""),
        )
        .body(Full::new(Bytes::from_static(b"authentication failed")))
        .assured(
            "the status and header values are typed constants or generated ASCII, which the http \
             builder always accepts",
        )
}

fn sanitized_upload_relative_path(raw: &str) -> Option<PathBuf> {
    let normalized = raw.replace('\\', "/");
    let mut path = PathBuf::new();
    for component in Path::new(&normalized).components() {
        match component {
            Component::Normal(part) => path.push(part),
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => return None,
        }
    }
    (!path.as_os_str().is_empty()).then_some(path)
}

async fn build_web_console_upload_archive(
    directory: &Path,
    identifier: ModelName,
) -> Result<(TempPath, String), (StatusCode, String)> {
    let archive = tempfile::NamedTempFile::new().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create temporary upload archive".to_string(),
        )
    })?;
    let archive_path = archive.into_temp_path();
    let file = File::create(&archive_path).await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to open temporary upload archive".to_string(),
        )
    })?;
    write_web_console_upload_archive(directory, file)
        .await
        .map_err(|message| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "failed to build archive for resource '{}': {message}",
                    identifier.as_str()
                ),
            )
        })?;

    let mut hasher = Hasher::new();
    let mut file = File::open(&archive_path).await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to read temporary upload archive".to_string(),
        )
    })?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        tokio::task::consume_budget().await;
        let read = file.read(&mut buffer).await.map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to hash temporary upload archive".to_string(),
            )
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let hash = hasher.finalize();
    Ok((archive_path, encode_hex(hash.as_bytes())))
}

async fn write_web_console_upload_archive(directory: &Path, writer: File) -> Result<(), String> {
    let entries = collect_web_console_upload_entries(directory)?;
    let mut builder = AsyncTarBuilder::new(writer);
    builder.mode(HeaderMode::Deterministic);

    for entry in entries {
        tokio::task::consume_budget().await;
        let mut header = Header::new_ustar();
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        match entry {
            WebConsoleUploadArchiveEntry::Directory { relative } => {
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(EntryType::Directory);
                header.set_cksum();
                builder
                    .append_data(&mut header, &relative, tokio::io::empty())
                    .await
                    .map_err(|error| error.to_string())?;
            }
            WebConsoleUploadArchiveEntry::File {
                full_path,
                relative,
                size,
            } => {
                header.set_size(size);
                header.set_mode(0o644);
                header.set_entry_type(EntryType::Regular);
                header.set_cksum();
                let file = File::open(&full_path)
                    .await
                    .map_err(|error| error.to_string())?;
                builder
                    .append_data(&mut header, &relative, file)
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
    }

    let mut writer = builder
        .into_inner()
        .await
        .map_err(|error| error.to_string())?;
    writer.flush().await.map_err(|error| error.to_string())
}

enum WebConsoleUploadArchiveEntry {
    Directory {
        relative: PathBuf,
    },
    File {
        full_path: PathBuf,
        relative: PathBuf,
        size: u64,
    },
}

fn collect_web_console_upload_entries(
    directory: &Path,
) -> Result<Vec<WebConsoleUploadArchiveEntry>, String> {
    let mut entries = Vec::new();
    collect_web_console_upload_entries_recursive(directory, directory, &mut entries)?;
    Ok(entries)
}

fn collect_web_console_upload_entries_recursive(
    root: &Path,
    current: &Path,
    entries: &mut Vec<WebConsoleUploadArchiveEntry>,
) -> Result<(), String> {
    let mut directory_entries = std::fs::read_dir(current)
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    directory_entries.sort_by_key(|entry| entry.file_name());
    for entry in directory_entries {
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .map_err(|error| error.to_string())?
            .to_path_buf();
        let file_type = entry.file_type().map_err(|error| error.to_string())?;
        if file_type.is_dir() {
            entries.push(WebConsoleUploadArchiveEntry::Directory { relative });
            collect_web_console_upload_entries_recursive(root, &path, entries)?;
        } else if file_type.is_file() {
            let size = std::fs::metadata(&path)
                .map_err(|error| error.to_string())?
                .len();
            entries.push(WebConsoleUploadArchiveEntry::File {
                full_path: path,
                relative,
                size,
            });
        }
    }
    Ok(())
}

async fn serve_web_console_http(
    service: SessionServiceImpl,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>> {
    let mut connection_tasks = JoinSet::new();

    loop {
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() => {
                accepted.change_context(AppError::ServeWebConsole)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeWebConsole)?;
        let service = service.clone();
        connection_tasks.spawn(async move {
            let io = TokioIo::new(stream);
            let service = service.clone();
            if let Err(error) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| handle_web_console_request(service.clone(), request)),
                )
                .with_upgrades()
                .await
            {
                warn!(error = %error, "web console connection failed");
            }
        });
    }
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

async fn serve_web_console_https(
    service: SessionServiceImpl,
    tls_server_config: StdArc<ServerConfig>,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>> {
    let tls_acceptor = TlsAcceptor::from(tls_server_config);
    let mut connection_tasks = JoinSet::new();

    loop {
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() => {
                accepted.change_context(AppError::ServeWebConsole)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeWebConsole)?;
        let tls_acceptor = tls_acceptor.clone();
        let service = service.clone();
        connection_tasks.spawn(async move {
            let stream = match tls_acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(error) => {
                    warn!(error = %error, "web console tls handshake failed");
                    return;
                }
            };
            let io = TokioIo::new(stream);
            let service = service.clone();
            if let Err(error) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| handle_web_console_request(service.clone(), request)),
                )
                .with_upgrades()
                .await
            {
                warn!(error = %error, "web console tls connection failed");
            }
        });
    }
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug, Clone)]
#[command(name = "nervix-server")]
#[command(about = "NSPL gRPC server")]
pub struct Args {
    #[arg(long, env = "NERVIX_ADDR", default_value = "127.0.0.1:47391")]
    pub addr: String,
    #[arg(long, env = "NERVIX_GRPC_MODE", value_enum, default_value_t = InternalTransportMode::Http)]
    pub grpc_mode: InternalTransportMode,
    #[arg(long, env = "NERVIX_GRPC_HTTPS_LISTEN_ADDR")]
    pub grpc_https_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_GRPC_HTTPS_ADVERTISE_ADDR")]
    pub grpc_https_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_HTTP_LISTEN_ADDR", default_value = "0.0.0.0:8080")]
    pub http_listen_addr: String,
    #[arg(long, env = "NERVIX_HTTPS_LISTEN_ADDR", default_value = "0.0.0.0:8443")]
    pub https_listen_addr: String,
    #[arg(
        long,
        env = "NERVIX_OBSERVABILITY_LISTEN_ADDR",
        default_value = "0.0.0.0:9090"
    )]
    pub observability_listen_addr: String,
    #[arg(
        long,
        env = "NERVIX_WEB_CONSOLE_LISTEN_ADDR",
        default_value = "0.0.0.0:47420"
    )]
    pub web_console_listen_addr: String,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_ADVERTISE_ADDR")]
    pub web_console_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_HTTPS_LISTEN_ADDR")]
    pub web_console_https_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_TLS_CERT")]
    pub web_console_tls_cert: Option<PathBuf>,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_TLS_KEY")]
    pub web_console_tls_key: Option<PathBuf>,
    #[arg(long, env = "NERVIX_CLUSTER_ID", default_value = "default")]
    pub cluster_id: String,
    #[arg(long, env = "NERVIX_NODE_ID")]
    pub node_id: ClusterNodeName,
    #[arg(long, env = "NERVIX_GRPC_ADVERTISE_ADDR")]
    pub grpc_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_LISTEN_ADDR")]
    pub interconnect_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_ADVERTISE_ADDR")]
    pub interconnect_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_TLS_CA")]
    pub interconnect_tls_ca: PathBuf,
    #[arg(long, env = "NERVIX_INTERCONNECT_TLS_CERT")]
    pub interconnect_tls_cert: PathBuf,
    #[arg(long, env = "NERVIX_INTERCONNECT_TLS_KEY")]
    pub interconnect_tls_key: PathBuf,
    #[arg(long, env = "NERVIX_ALLOW_BOOTSTRAP", default_value_t = false)]
    pub allow_bootstrap: bool,
    #[arg(long, env = "NERVIX_DEFAULT_USER", default_value = DEFAULT_USER)]
    pub default_user: String,
    #[arg(
        long,
        env = "NERVIX_INIT_DEFAULT_USER_PASSWORD",
        hide_env_values = true
    )]
    pub init_default_user_password: Option<String>,
    #[arg(
        long,
        env = "NERVIX_NODE_UNAVAILABILITY_TIMEOUT",
        default_value = "10s",
        value_parser = parse_human_duration
    )]
    pub node_unavailability_timeout: Duration,
    #[arg(
        long,
        env = "NERVIX_RAFT_HEARTBEAT_INTERVAL",
        default_value = "250ms",
        value_parser = parse_human_duration
    )]
    pub raft_heartbeat_interval: Duration,
    #[arg(
        long,
        env = "NERVIX_RAFT_ELECTION_TIMEOUT_MIN",
        default_value = "1500ms",
        value_parser = parse_human_duration
    )]
    pub raft_election_timeout_min: Duration,
    #[arg(
        long,
        env = "NERVIX_RAFT_ELECTION_TIMEOUT_MAX",
        default_value = "3000ms",
        value_parser = parse_human_duration
    )]
    pub raft_election_timeout_max: Duration,
    #[arg(
        long,
        env = "NERVIX_TRANSACTION_IDLE_TIMEOUT",
        default_value = "15m",
        value_parser = parse_human_duration
    )]
    pub transaction_idle_timeout: Duration,
    #[arg(
        long,
        env = "NERVIX_TRANSACTION_TOMBSTONE_RETENTION",
        default_value = "15m",
        value_parser = parse_human_duration
    )]
    pub transaction_tombstone_retention: Duration,
    #[arg(
        long,
        env = "NERVIX_TRANSACTION_MAX_STATEMENTS",
        default_value_t = DEFAULT_TRANSACTION_MAX_STATEMENTS
    )]
    pub transaction_max_statements: usize,
    #[arg(
        long,
        env = "NERVIX_TRANSACTION_MAX_SOURCE_BYTES",
        default_value_t = DEFAULT_TRANSACTION_MAX_SOURCE_BYTES
    )]
    pub transaction_max_source_bytes: u64,
    #[arg(
        long,
        env = "NERVIX_TRANSACTION_MAX_OPEN",
        default_value_t = DEFAULT_TRANSACTION_MAX_OPEN
    )]
    pub transaction_max_open: usize,
    #[arg(long, env = "NERVIX_REPLICA_COUNT", default_value_t = 0)]
    pub replica_count: usize,
    #[arg(
        long,
        env = "NERVIX_STATE_SNAPSHOT_INTERVAL",
        default_value = "30s",
        value_parser = parse_human_duration
    )]
    pub state_snapshot_interval: Duration,
    #[arg(
        long,
        env = "NERVIX_MEMORY_HIGH_WATERMARK",
        value_parser = parse_human_bytes,
        help = "Allocated jemalloc bytes that pause all ingestors"
    )]
    pub memory_high_watermark: Option<ubyte::ByteUnit>,
    #[arg(
        long,
        env = "NERVIX_MEMORY_LOW_WATERMARK",
        value_parser = parse_human_bytes,
        help = "Allocated jemalloc bytes that allow paused ingestors to resume"
    )]
    pub memory_low_watermark: Option<ubyte::ByteUnit>,
    #[arg(
        long,
        env = "NERVIX_MEMORY_PRESSURE_CHECK_INTERVAL",
        default_value = "500ms",
        value_parser = parse_human_duration,
        help = "Interval between jemalloc memory pressure checks"
    )]
    pub memory_pressure_check_interval: Duration,
    #[arg(
        long,
        env = "NERVIX_MEMORY_PRESSURE_RESUME_JITTER",
        default_value = "1s",
        value_parser = parse_human_duration,
        help = "Maximum jitter before each paused ingestor resume attempt"
    )]
    pub memory_pressure_resume_jitter: Duration,
    #[arg(
        long,
        env = "NERVIX_DRAIN_TIMEOUT",
        default_value = "30s",
        value_parser = parse_human_duration,
        help = "Maximum time to wait for drain operations before continuing"
    )]
    pub drain_timeout: Duration,
    #[arg(long, env = "NERVIX_CLUSTER_BOOTSTRAP_HOST")]
    pub cluster_bootstrap_host: Option<String>,
    #[arg(long, env = "NERVIX_DB_PATH", default_value = "./.nervix-db")]
    pub db_path: String,
    #[arg(
        long,
        env = "NERVIX_TEMP_DIR",
        default_value = crate::runtime::DEFAULT_TEMP_DIR,
        help = "Directory used for local temporary files such as Iceberg emitter staging"
    )]
    pub temp_dir: PathBuf,
    #[arg(
        long,
        env = "NERVIX_OTEL_ENABLED",
        default_value_t = false,
        help = "Enable optional OpenTelemetry OTLP trace export"
    )]
    pub otel_enabled: bool,
    #[arg(
        long,
        env = "NERVIX_OTEL_OTLP_ENDPOINT",
        default_value = "http://127.0.0.1:4317",
        help = "OpenTelemetry OTLP gRPC endpoint used when trace export is enabled"
    )]
    pub otel_otlp_endpoint: String,
    #[arg(
        long,
        env = "NERVIX_OTEL_SERVICE_NAME",
        default_value = "nervix",
        help = "OpenTelemetry service name used when trace export is enabled"
    )]
    pub otel_service_name: String,
    #[arg(
        long,
        env = "NERVIX_OTEL_TRACE_SAMPLE_RATIO",
        default_value_t = 1.0,
        value_parser = parse_trace_sample_ratio,
        help = "OpenTelemetry parent-based trace sample ratio used when trace export is enabled"
    )]
    pub otel_trace_sample_ratio: f64,
    #[command(subcommand)]
    pub subcommand: Option<Command>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Generate shell completion scripts
    Completions {
        /// Target shell
        shell: Shell,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct KafkaPartitionWatcherKey {
    domain: DomainName,
    ingestor: IngestorName,
}

/// One relay whose subscription interest this node advertises to the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SubscriptionInterestKey {
    domain: DomainName,
    relay: RelayName,
}

/// One statement of a model-mutation batch that reached the registry: which statement it was,
/// the model it changed, and the message its own result reports.
struct AppliedModelMutation {
    index: usize,
    model: ModelName,
    message: String,
}

/// A domain schedule computed from a candidate graph, with how many runtime nodes it moves off
/// the node that owns them today.
struct PreparedDomainSchedule {
    schedule: Option<nervix_models::DomainSchedule>,
    relocations: usize,
}

/// The schedule change one model mutation makes: what the domain is scheduled as now, what it
/// would be scheduled as, and how many runtime nodes that move relocates.
#[derive(Default)]
struct ScheduleTransition {
    expected_schedule: Option<nervix_models::DomainSchedule>,
    prepared_schedule: Option<nervix_models::DomainSchedule>,
    planned_relocations: usize,
}

/// The hash map a lookup query reaches, as the cluster schedule describes it: the lookup model,
/// the scheduled node that owns it, and the declared type of its key field.
struct LookupTarget {
    lookup: CreateLookup,
    node: ScheduledNode,
    key_ty: ParseAsType,
}

/// The relay a subscription attaches to, as the cluster schedule describes it: the relay model,
/// the schema its records carry, and the branch key fields a subscription may bind.
struct SubscriptionTarget {
    relay: nervix_models::CreateRelay,
    schema: nervix_models::CreateSchema,
    branching: Vec<FieldName>,
}

/// A running background task and the token that stops it.
struct BackgroundTask {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

impl BackgroundTask {
    fn request_stop(&self) {
        self.cancel.cancel();
    }

    fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    async fn join(self) {
        self.handle.join_after_shutdown("background task").await;
    }

    /// Stops the task and waits for it to finish, so the caller never drops a task that is still
    /// touching the state it is about to replace.
    async fn stop(self) {
        self.request_stop();
        self.join().await;
    }
}

#[derive(Clone)]
struct InterconnectTlsPaths {
    ca: PathBuf,
    certificate: PathBuf,
    private_key: PathBuf,
}

struct InterconnectTlsMaterial {
    ca: Vec<u8>,
    certificate: Vec<u8>,
    private_key: Vec<u8>,
}

impl InterconnectTlsPaths {
    async fn read(&self) -> io::Result<InterconnectTlsMaterial> {
        let ca = tokio::fs::read(&self.ca).await?;
        let certificate = tokio::fs::read(&self.certificate).await?;
        let private_key = tokio::fs::read(&self.private_key).await?;
        Ok(InterconnectTlsMaterial {
            ca,
            certificate,
            private_key,
        })
    }
}

impl InterconnectTlsMaterial {
    fn fingerprint(&self) -> blake3::Hash {
        let mut hasher = Hasher::new();
        hasher.update(b"interconnect-ca\0");
        hasher.update(&self.ca);
        hasher.update(b"interconnect-certificate\0");
        hasher.update(&self.certificate);
        hasher.update(b"interconnect-private-key\0");
        hasher.update(&self.private_key);
        hasher.finalize()
    }

    fn tls_bundle(&self) -> Result<TlsConfigBundle, Report<nervix_interconnect::TlsConfigError>> {
        TlsConfigBundle::from_pem(&self.ca, &self.certificate, &self.private_key)
    }
}

async fn reload_interconnect_tls(
    transport: Transport,
    paths: InterconnectTlsPaths,
    initial_fingerprint: blake3::Hash,
    shutdown: CancellationToken,
) {
    let mut applied_fingerprint = initial_fingerprint;
    let mut pending_fingerprint = None;
    let mut reported_failure = None;
    let mut ticker = interval(INTERCONNECT_TLS_RELOAD_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::task::consume_budget().await;
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }

        let material = match paths.read().await {
            Ok(material) => material,
            Err(error) => {
                let failure = error.to_string();
                if reported_failure.as_ref() != Some(&failure) {
                    warn!(error = %error, "failed to read replacement interconnect TLS files");
                    reported_failure = Some(failure);
                }
                pending_fingerprint = None;
                continue;
            }
        };
        let fingerprint = material.fingerprint();
        if fingerprint == applied_fingerprint {
            pending_fingerprint = None;
            reported_failure = None;
            continue;
        }
        if pending_fingerprint != Some(fingerprint) {
            pending_fingerprint = Some(fingerprint);
            reported_failure = None;
            continue;
        }

        let replacement = match material.tls_bundle() {
            Ok(replacement) => replacement,
            Err(error) => {
                let failure = error.to_string();
                if reported_failure.as_ref() != Some(&failure) {
                    warn!(error = %error, "replacement interconnect TLS files are invalid");
                    reported_failure = Some(failure);
                }
                continue;
            }
        };
        if let Err(error) = transport.replace_tls(replacement).await {
            let failure = error.to_string();
            if reported_failure.as_ref() != Some(&failure) {
                warn!(error = %error, "failed to replace interconnect TLS credentials");
                reported_failure = Some(failure);
            }
            continue;
        }

        applied_fingerprint = fingerprint;
        pending_fingerprint = None;
        reported_failure = None;
        info!("reloaded interconnect TLS credentials");
    }
}

/// One Kafka partition watcher the leader runs: the ingestor it watches for, and the task doing
/// the watching.
struct KafkaPartitionWatcherTask {
    spec: KafkaPartitionWatcherSpec,
    task: BackgroundTask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KafkaPartitionWatcherSpec {
    domain: DomainName,
    ingestor: IngestorName,
    topic: String,
    instances: NonZeroU64,
    client: nervix_models::CreateClientKafka,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DrainMove {
    label: String,
    promoted_replica: Option<ClusterNodeName>,
    fallback_node: Option<ClusterNodeName>,
}

#[derive(Clone, Copy)]
enum AssignmentRelocation {
    Planned,
    Failure,
}

impl AssignmentRelocation {
    fn target(
        self,
        desired_target: Option<ClusterNodeName>,
        existing_replica: Option<ClusterNodeName>,
    ) -> Option<ClusterNodeName> {
        match self {
            Self::Planned => desired_target.or(existing_replica),
            Self::Failure => existing_replica.or(desired_target),
        }
    }

    fn retains_former_replica(self) -> bool {
        match self {
            Self::Planned => true,
            Self::Failure => false,
        }
    }

    fn ownership_transition(
        self,
        source: ClusterNodeName,
        destination: ClusterNodeName,
        node: &ScheduledNode,
        promoted_replica: bool,
    ) -> OwnershipTransition {
        let state_recovery = match self {
            Self::Planned => OwnershipStateRecoveryOutcome::Complete,
            Self::Failure if promoted_replica => OwnershipStateRecoveryOutcome::Unverified,
            Self::Failure => OwnershipStateRecoveryOutcome::Reset,
        };
        let resets = if state_recovery == OwnershipStateRecoveryOutcome::Reset {
            node.ownership_state_components()
                .into_iter()
                .map(|component| OwnershipStateReset {
                    component,
                    cause: OwnershipStateResetCause::MissingCheckpoint,
                })
                .collect()
        } else {
            Vec::new()
        };
        OwnershipTransition {
            id: uuid::Uuid::now_v7().to_string(),
            source,
            destination,
            state_recovery,
            resets,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedOwnershipMove {
    entity: NodeRef,
    former_owner: ClusterNodeName,
    destination: ClusterNodeName,
    replicas: Vec<ClusterNodeName>,
    promoted_replica: bool,
}

struct ForcedOwnershipRecoveryCoordinator<'a> {
    runtime: &'a Runtime,
    interconnect: &'a Transport,
    local_node_id: &'a ClusterNodeName,
    node_incarnations: &'a BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
}

impl ForcedOwnershipRecoveryCoordinator<'_> {
    async fn prepare_schedule(
        &self,
        current: &nervix_models::DomainSchedule,
        target: &mut nervix_models::DomainSchedule,
    ) {
        let base_schedule_fingerprint =
            match Runtime::ownership_handoff_schedule_fingerprint(current) {
                Ok(fingerprint) => fingerprint,
                Err(reason) => {
                    self.reset_every_move(
                        current,
                        target,
                        OwnershipStateResetCause::InvalidCheckpoint,
                    );
                    warn!(
                        domain = current.domain.as_str(),
                        error = %reason,
                        "forced ownership recovery could not fingerprint the committed schedule"
                    );
                    return;
                }
            };
        let target_schedule_fingerprint =
            match Runtime::ownership_handoff_schedule_fingerprint(target) {
                Ok(fingerprint) => fingerprint,
                Err(reason) => {
                    self.reset_every_move(
                        current,
                        target,
                        OwnershipStateResetCause::InvalidCheckpoint,
                    );
                    warn!(
                        domain = current.domain.as_str(),
                        error = %reason,
                        "forced ownership recovery could not fingerprint the target schedule"
                    );
                    return;
                }
            };
        struct PreparedForcedMove {
            moved: PlannedOwnershipMove,
            transition_id: String,
            result: OwnershipHandoffResult<nervix_interconnect::ForcedOwnershipRecoveryPreparation>,
        }

        let moves = planned_ownership_moves(Some(current), Some(target));
        let mut preparations = FuturesUnordered::new();
        for moved in moves {
            tokio::task::consume_budget().await;
            let Some(transition) = target
                .nodes
                .get(&moved.entity)
                .and_then(|node| node.ownership_transition.as_ref())
            else {
                continue;
            };
            if transition.state_recovery == OwnershipStateRecoveryOutcome::Complete {
                continue;
            }
            let transition_id = transition.id.clone();
            let destination_incarnation = self.node_incarnations.get(&moved.destination).copied();
            preparations.push(async move {
                let result = match destination_incarnation {
                    Some(destination_incarnation) => {
                        let deadline =
                            tokio::time::Instant::now() + FORCED_OWNERSHIP_RECOVERY_BUDGET;
                        let preparation = async {
                            let request = RemotePrepareForcedOwnershipRecoveryRequest {
                                operation_id: transition_id.clone(),
                                source: moved.former_owner.clone(),
                                destination: moved.destination.clone(),
                                destination_incarnation,
                                domain: current.domain.clone(),
                                entity: moved.entity.clone(),
                                base_schedule_fingerprint,
                                target_schedule_fingerprint,
                            };
                            if moved.destination == *self.local_node_id {
                                return self
                                    .runtime
                                    .prepare_forced_ownership_recovery(request, deadline)
                                    .await;
                            }
                            let response = self
                                .interconnect
                                .request(&moved.destination, request)
                                .await
                                .map_err(|error| {
                                    OwnershipHandoffError::transport(error.to_string())
                                })?;
                            response.map_err(|failure| {
                                OwnershipHandoffError::participant(failure.to_string())
                            })
                        };
                        match tokio::time::timeout_at(deadline, preparation).await {
                            Ok(result) => result,
                            Err(_) => Err(OwnershipHandoffError::deadline(
                                "state preparation exceeded its five-second budget",
                            )),
                        }
                    }
                    None => Err(OwnershipHandoffError::participant(format!(
                        "destination node '{}' has no live process incarnation",
                        moved.destination
                    ))),
                };
                PreparedForcedMove {
                    moved,
                    transition_id,
                    result,
                }
            });
        }
        while let Some(preparation) = preparations.next().await {
            tokio::task::consume_budget().await;
            let moved = preparation.moved;
            let node = target
                .nodes
                .get_mut(&moved.entity)
                .verified("the forced recovery move was derived from this target schedule");
            match preparation.result {
                Ok(prepared) => {
                    node.ownership_transition = Some(OwnershipTransition {
                        id: preparation.transition_id,
                        source: moved.former_owner.clone(),
                        destination: moved.destination.clone(),
                        state_recovery: prepared.state_recovery,
                        resets: prepared.resets,
                    });
                    warn!(
                        domain = current.domain.as_str(),
                        kind = moved.entity.kind.as_str(),
                        name = moved.entity.identifier.as_str(),
                        source = %moved.former_owner,
                        destination = %moved.destination,
                        state_recovery = prepared.state_recovery.as_ref(),
                        "forced ownership recovery prepared destination state"
                    );
                }
                Err(reason) => {
                    Self::mark_reset(
                        node,
                        preparation.transition_id,
                        moved.former_owner.clone(),
                        moved.destination.clone(),
                        OwnershipStateResetCause::MissingCheckpoint,
                    );
                    warn!(
                        domain = current.domain.as_str(),
                        kind = moved.entity.kind.as_str(),
                        name = moved.entity.identifier.as_str(),
                        source = %moved.former_owner,
                        destination = %moved.destination,
                        error = %reason,
                        "forced ownership recovery is publishing with recreated runtime state"
                    );
                }
            }
        }
    }

    fn reset_every_move(
        &self,
        current: &nervix_models::DomainSchedule,
        target: &mut nervix_models::DomainSchedule,
        cause: OwnershipStateResetCause,
    ) {
        for moved in planned_ownership_moves(Some(current), Some(target)) {
            let node = target
                .nodes
                .get_mut(&moved.entity)
                .verified("the forced recovery move was derived from this target schedule");
            Self::mark_reset(
                node,
                uuid::Uuid::now_v7().to_string(),
                moved.former_owner,
                moved.destination,
                cause,
            );
        }
    }

    fn mark_reset(
        node: &mut ScheduledNode,
        id: String,
        source: ClusterNodeName,
        destination: ClusterNodeName,
        cause: OwnershipStateResetCause,
    ) {
        node.ownership_transition = Some(OwnershipTransition {
            id,
            source,
            destination,
            state_recovery: OwnershipStateRecoveryOutcome::Reset,
            resets: node
                .ownership_state_components()
                .into_iter()
                .map(|component| OwnershipStateReset { component, cause })
                .collect(),
        });
    }
}

type AuthRateLimiter = DefaultKeyedRateLimiter<String>;

/// How many events a session can fall behind before the bus drops the oldest.
const SESSION_EVENT_CAPACITY: usize = 256;

/// The session event bus, and the one way a control-plane failure or a cluster transition reaches
/// the sessions attached to this node.
///
/// Unlike the runtime event bus this one carries no fan-out task, so its only subscribers are live
/// sessions, and a node serving none is the ordinary case rather than a startup window. An event
/// that finds no receiver is therefore expected, which is why publishing goes through these
/// methods: each one leaves a record that does not depend on anyone listening, and the send that
/// follows only offers the same fact to whoever is.
#[derive(Clone)]
struct SessionEvents {
    sender: broadcast::Sender<ServerEvent>,
}

impl SessionEvents {
    fn new(capacity: usize) -> Self {
        Self {
            sender: broadcast::channel(capacity).0,
        }
    }

    /// Report a control-plane failure this node recovered from.
    fn report_error(&self, message: impl Into<String>) {
        let message = message.into();
        warn!(error = %message, "server error reported to sessions");
        self.publish(ServerEventLevel::Error, message);
    }

    /// Offer a transition the cluster or consensus bus has already recorded.
    ///
    /// Those buses write their own `info` line before handing the text here, so this is a relay
    /// rather than a report and it logs nothing of its own.
    fn relay_info(&self, message: String) {
        self.publish(ServerEventLevel::Info, message);
    }

    fn publish(&self, level: ServerEventLevel, message: String) {
        self.sender
            .send(ServerEvent {
                level: i32::from(level),
                message,
            })
            .discarded("the record this event carries is written before it is offered");
    }

    fn subscribe(&self) -> broadcast::Receiver<ServerEvent> {
        self.sender.subscribe()
    }
}

/// The handle every gRPC request, background reconciliation task, and HTTP server clones. It is
/// one `Arc` over the server's state, so handing the service to a spawned task costs a single
/// refcount rather than one per piece of state the server owns.
#[derive(Clone)]
struct SessionServiceImpl {
    inner: Arc<SessionServiceInner>,
}

/// Everything one Nervix server owns for as long as it serves. These fields are reached only
/// through a `SessionServiceImpl` handle and therefore hold their values directly. The ones that
/// keep an `Arc` of their own have a second owner outside the service, and each names it.
struct SessionServiceInner {
    /// Started and shut down by the application, which outlives the service handle.
    cluster: Arc<cluster::ClusterHandle>,
    /// Proposal authority and local observation; leadership is checked for each operation.
    consensus: Proposer,
    /// Membership changes requested by authenticated cluster commands.
    consensus_administrator: Administrator,
    /// Also held by the application and by the registry reconciliation tasks it spawns.
    registry: Arc<Registry>,
    /// Also held by the application startup that opened it.
    resource_store: Arc<ResourceStore>,
    /// Also held by the HTTPS server, which reads the current certificate on every accept.
    http_tls_server_config: Arc<RwLock<Option<StdArc<ServerConfig>>>>,
    runtime: Runtime,
    replica_count: usize,
    shutdown: CancellationToken,
    events: SessionEvents,
    subscription_interest_counts: DashMap<SubscriptionInterestKey, usize, RandomState>,
    interconnect: Transport,
    next_entity_gate_operation_id: AtomicU64,
    service_tasks: TaskTracker,
    configured_basic_auth: Option<BasicAuthCredentials>,
    auth_rate_limiter: AuthRateLimiter,
    failed_auth_rate_limit_keys: DashMap<String, (), RandomState>,
    transaction_idle_timeout: Duration,
    transaction_tombstone_retention: Duration,
    transaction_max_statements: usize,
    transaction_max_source_bytes: u64,
    transaction_max_open: usize,
    transaction_bindings: DashMap<String, String, RandomState>,
    /// Also held by every outstanding `TransactionExecutionLease`, which clears its entry on drop.
    transaction_executions: Arc<DashMap<String, (), RandomState>>,
    transaction_commit_execution: AsyncMutex<()>,
}

struct TransactionExecutionLease {
    executions: Arc<DashMap<String, (), RandomState>>,
    id: String,
}

#[derive(Debug, Error)]
enum TransactionCommitError {
    #[error(transparent)]
    Proposal(#[from] ConsensusTransactionError),
    #[error("transaction '{id}' is unknown")]
    UnknownTransaction { id: String },
    #[error("transaction '{id}' is still open")]
    TransactionOpen { id: String },
    #[error("transaction '{id}' model step completed without recording progress")]
    MissingProgress { id: String },
    #[error("transaction '{id}' has invalid recorded commit progress")]
    InvalidProgress { id: String },
    #[error("failed to synchronize registry before resuming transaction '{id}'")]
    SynchronizeRegistry { id: String },
    #[error("failed to recover transaction '{id}' domain quiescence")]
    RecoverQuiescence { id: String },
    #[error("failed to prepare schedule for domain '{domain}': {reason}")]
    PrepareSchedule { domain: DomainName, reason: String },
    #[error("transaction '{id}' commit task failed")]
    TaskJoin { id: String },
}

impl TransactionCommitError {
    fn consensus_error(&self) -> Option<&ConsensusError> {
        match self {
            Self::Proposal(ConsensusTransactionError::Consensus(error)) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug, Error)]
enum ResourceUploadError {
    #[error("failed to allocate version for resource '{identifier}'")]
    AllocateVersion { identifier: ModelName },
    #[error("failed to install resource '{identifier}'")]
    InstallArchive { identifier: ModelName },
    #[error(
        "failed to publish resource '{}@{}'{cleanup_suffix}",
        .id.identifier.as_str(),
        .id.version
    )]
    PublishVersion {
        id: ResourceId,
        cleanup_suffix: String,
    },
    #[error("failed to publish resource replica '{}@{}'", .id.identifier.as_str(), .id.version)]
    PublishReplica { id: ResourceId },
    #[error(
        "failed to replicate resource '{}@{}': {reason}",
        .id.identifier.as_str(),
        .id.version
    )]
    WaitForReplicas { id: ResourceId, reason: String },
}

struct TransactionModelStepContext<'a> {
    transaction: &'a ReplicatedTransaction,
    first_statement: usize,
    statement_count: usize,
    outcome:
        &'a ParkingMutex<Option<Result<ReplicatedTransaction, Report<TransactionCommitError>>>>,
}

impl Drop for TransactionExecutionLease {
    fn drop(&mut self) {
        self.executions.remove(&self.id);
    }
}

impl ClusterEntityGate {
    fn new(service: &SessionServiceImpl, operation_id: u64, domain: &DomainName) -> Self {
        Self {
            operation_id,
            domain: domain.clone(),
            nodes: BTreeSet::new(),
            release_owner: Some(service.clone()),
        }
    }

    /// Records a node before sending its engagement request. A response timeout is ambiguous: the
    /// remote node may already own the durable lease, so cleanup must include every attempted node
    /// and rely on idempotent release.
    fn record_attempt(&mut self, node: ClusterNodeName) {
        self.nodes.insert(node);
    }

    fn mark_released(&mut self, node: &ClusterNodeName) {
        self.nodes.remove(node);
    }

    fn schedule_remaining_releases(&mut self) {
        let Some(owner) = self.release_owner.take() else {
            return;
        };
        if self.nodes.is_empty() {
            return;
        }
        owner.schedule_cluster_entity_gate_release(PendingClusterEntityGateRelease {
            operation_id: self.operation_id,
            domain: self.domain.clone(),
            nodes: std::mem::take(&mut self.nodes),
        });
    }

    fn defer_release_to_lease_deadline(mut self) {
        self.release_owner = None;
    }
}

impl Drop for ClusterEntityGate {
    fn drop(&mut self) {
        self.schedule_remaining_releases();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BasicAuthCredentials {
    username: String,
    password: String,
}

#[derive(Debug, Error)]
enum GrpcAuthenticationError {
    #[error("authentication required")]
    Required,
    #[error("authentication failed")]
    Failed,
}

impl From<GrpcAuthenticationError> for Status {
    fn from(error: GrpcAuthenticationError) -> Self {
        Self::unauthenticated(error.to_string())
    }
}

#[derive(Debug, Error)]
enum ActiveDomainError {
    #[error("invalid active domain")]
    Invalid,
    #[error("domain '{domain}' does not exist")]
    NotFound { domain: DomainName },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DomainClockTaskSpec {
    clock: DomainClockState,
    period: DomainClockPeriod,
    generation: u64,
    authority_revision: nervix_models::DomainClockAuthorityRevision,
    authority: ClusterNodeIdentity,
    targets: BTreeSet<ClusterNodeIdentity>,
}

struct DomainClockTask {
    spec: DomainClockTaskSpec,
    task: BackgroundTask,
}

/// Producers whose authority was revoked while they finish an in-flight fenced delivery.
///
/// Installing the committed replacement cannot wait for transport or a test-held delivery from
/// the previous producer. Its immutable specification still carries the superseded fence, and a
/// successor for the same domain starts only after every retiring local task has finished.
#[derive(Default)]
struct DomainClockRetirements {
    tasks: HashMap<DomainName, Vec<BackgroundTask>>,
}

impl DomainClockRetirements {
    fn contains(&self, domain: &DomainName) -> bool {
        self.tasks.contains_key(domain)
    }

    fn retire(&mut self, domain: DomainName, task: BackgroundTask) {
        task.request_stop();
        self.tasks.entry(domain).or_default().push(task);
    }

    async fn reap(&mut self) {
        let domains = self.tasks.keys().cloned().collect::<Vec<_>>();
        for domain in domains {
            tokio::task::consume_budget().await;
            let Some(tasks) = self.tasks.remove(&domain) else {
                continue;
            };
            let mut pending = Vec::new();
            for task in tasks {
                tokio::task::consume_budget().await;
                if task.is_finished() {
                    task.join().await;
                } else {
                    pending.push(task);
                }
            }
            if !pending.is_empty() {
                self.tasks.insert(domain, pending);
            }
        }
    }

    async fn stop_all(self) {
        for tasks in self.tasks.into_values() {
            tokio::task::consume_budget().await;
            for task in tasks {
                tokio::task::consume_budget().await;
                task.stop().await;
            }
        }
    }
}

struct DownloadedResourceArchive {
    path: TempPath,
    root_checksum: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "lower")]
pub enum InternalTransportMode {
    Http,
    Https,
}

impl InternalTransportMode {
    fn scheme(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    fn is_tls(self) -> bool {
        matches!(self, Self::Https)
    }
}

const VHOST_TLS_CERT_PATH: &str = "tls.crt";
const VHOST_TLS_KEY_PATH: &str = "tls.key";
const VHOST_TLS_CA_PATH: &str = "ca.crt";
const INTERNAL_TLS_CA_FILE: &str = "ca.pem";
const INTERNAL_TLS_CERT_FILE: &str = "node.pem";
const INTERNAL_TLS_KEY_FILE: &str = "node-key.pem";

#[derive(Debug, Error)]
pub enum AppError {
    #[error("failed to build the Tokio runtime")]
    BuildRuntime,
    #[error("failed to parse server address")]
    ParseAddress,
    #[error("failed to bind gRPC listen address")]
    BindGrpcListenAddress,
    #[error("failed to parse HTTP listen address")]
    ParseHttpListenAddress,
    #[error("failed to bind HTTP listen address")]
    BindHttpListenAddress,
    #[error("failed to parse HTTPS listen address")]
    ParseHttpsListenAddress,
    #[error("failed to parse observability listen address")]
    ParseObservabilityListenAddress,
    #[error("failed to parse web console listen address")]
    ParseWebConsoleListenAddress,
    #[error("failed to parse web console https listen address")]
    ParseWebConsoleHttpsListenAddress,
    #[error("failed to bind HTTPS listen address")]
    BindHttpsListenAddress,
    #[error("failed to bind observability listen address")]
    BindObservabilityListenAddress,
    #[error("failed to bind web console listen address")]
    BindWebConsoleListenAddress,
    #[error("failed to bind web console https listen address")]
    BindWebConsoleHttpsListenAddress,
    #[error("failed to parse gRPC advertise address")]
    ParseGrpcAdvertiseAddress,
    #[error("failed to parse gRPC https listen address")]
    ParseGrpcHttpsListenAddress,
    #[error("failed to parse gRPC https advertise address")]
    ParseGrpcHttpsAdvertiseAddress,
    #[error("failed to parse interconnect listen address")]
    ParseInterconnectListenAddress,
    #[error("failed to parse interconnect advertise address")]
    ParseInterconnectAdvertiseAddress,
    #[error("failed to derive interconnect address from gRPC address")]
    DeriveInterconnectAddress,
    #[error("gRPC https mode requires an https listen address")]
    MissingGrpcHttpsListenAddress,
    #[error("gRPC https mode requires an https advertise address")]
    MissingGrpcHttpsAdvertiseAddress,
    #[error("web console https listener requires a TLS certificate")]
    MissingWebConsoleTlsCertificate,
    #[error("web console https listener requires a TLS private key")]
    MissingWebConsoleTlsPrivateKey,
    #[error("web console TLS certificate/key requires an https listen address")]
    MissingWebConsoleHttpsListenAddress,
    #[error("failed to open registry")]
    OpenRegistry,
    #[error("failed to open resource store")]
    OpenResourceStore,
    #[error("failed to start consensus")]
    StartConsensus,
    #[error("failed to synchronize registry from consensus schedule: {0}")]
    SynchronizeRegistry(String),
    #[error("failed to apply startup runtime changes: {0}")]
    ApplyStartupRuntime(String),
    #[error("failed to open runtime state store")]
    OpenRuntimeState,
    #[error("failed to load interconnect tls configuration")]
    LoadInterconnectTls,
    #[error("failed to load gRPC tls configuration")]
    LoadGrpcTls,
    #[error("failed to load web console tls configuration")]
    LoadWebConsoleTls,
    #[error("failed to start interconnect transport")]
    StartInterconnect,
    #[error("failed to register an interconnect request handler")]
    RegisterInterconnectRequestHandler,
    #[error("failed to start cluster membership")]
    StartCluster,
    #[error("failed to stop cluster membership")]
    ShutdownCluster,
    #[error("memory high watermark requires memory low watermark")]
    MissingMemoryPressureLowWatermark,
    #[error("memory low watermark requires memory high watermark")]
    MissingMemoryPressureHighWatermark,
    #[error("invalid memory pressure configuration")]
    InvalidMemoryPressureConfig,
    #[error("failed to initialize memory pressure monitor")]
    InitMemoryPressureMonitor,
    #[error("gRPC server failed")]
    Serve,
    #[error("HTTP server failed")]
    ServeHttp,
    #[error("HTTPS server failed")]
    ServeHttps,
    #[error("observability server failed")]
    ServeObservability,
    #[error("web console server failed")]
    ServeWebConsole,
    #[error("failed to initialize tracing")]
    InitTracing,
}

#[derive(Debug, Clone, TypedBuilder)]
pub struct Application {
    pub addr: SocketAddr,
    #[builder(default = InternalTransportMode::Http)]
    pub grpc_mode: InternalTransportMode,
    #[builder(default)]
    pub grpc_https_listen_addr: Option<SocketAddr>,
    #[builder(default)]
    pub grpc_https_advertise_addr: Option<cluster::HostPort>,
    pub http_listen_addr: SocketAddr,
    pub https_listen_addr: SocketAddr,
    pub observability_listen_addr: SocketAddr,
    #[builder(default = SocketAddr::from(([127, 0, 0, 1], 0)))]
    pub web_console_listen_addr: SocketAddr,
    #[builder(default)]
    pub web_console_advertise_addr: Option<cluster::HostPort>,
    #[builder(default)]
    pub web_console_https_listen_addr: Option<SocketAddr>,
    #[builder(default)]
    pub web_console_tls_cert: Option<PathBuf>,
    #[builder(default)]
    pub web_console_tls_key: Option<PathBuf>,
    pub cluster_id: String,
    pub node_id: ClusterNodeName,
    pub grpc_advertise_addr: cluster::HostPort,
    pub interconnect_listen_addr: SocketAddr,
    pub interconnect_advertise_addr: cluster::HostPort,
    pub interconnect_tls_ca: PathBuf,
    pub interconnect_tls_cert: PathBuf,
    pub interconnect_tls_key: PathBuf,
    pub allow_bootstrap: bool,
    #[builder(default = DEFAULT_USER.to_string())]
    pub default_user: String,
    #[builder(default)]
    pub init_default_user_password: Option<String>,
    pub node_unavailability_timeout: Duration,
    pub raft_heartbeat_interval: Duration,
    pub raft_election_timeout_min: Duration,
    pub raft_election_timeout_max: Duration,
    #[builder(default = DEFAULT_TRANSACTION_IDLE_TIMEOUT)]
    pub transaction_idle_timeout: Duration,
    #[builder(default = DEFAULT_TRANSACTION_TOMBSTONE_RETENTION)]
    pub transaction_tombstone_retention: Duration,
    #[builder(default = DEFAULT_TRANSACTION_MAX_STATEMENTS)]
    pub transaction_max_statements: usize,
    #[builder(default = DEFAULT_TRANSACTION_MAX_SOURCE_BYTES)]
    pub transaction_max_source_bytes: u64,
    #[builder(default = DEFAULT_TRANSACTION_MAX_OPEN)]
    pub transaction_max_open: usize,
    #[builder(default = 0)]
    pub replica_count: usize,
    #[builder(default = Duration::from_secs(30))]
    pub state_snapshot_interval: Duration,
    #[builder(default)]
    pub memory_pressure: Option<MemoryPressureConfig>,
    pub cluster_bootstrap_host: Option<String>,
    pub db_path: PathBuf,
    #[builder(default = PathBuf::from(crate::runtime::DEFAULT_TEMP_DIR))]
    pub temp_dir: PathBuf,
    #[builder(default)]
    #[doc(hidden)]
    pub fault_injection: ConfiguredFaultInjection,
    #[builder(default=CancellationToken::new())]
    pub shutdown: CancellationToken,
    #[builder(default = true)]
    pub graceful_shutdown_drain: bool,
    #[builder(default = DEFAULT_DRAIN_TIMEOUT)]
    pub drain_timeout: Duration,
}

struct ApplicationStartup {
    db: Database,
    resource_store: Arc<ResourceStore>,
    registry: Arc<Registry>,
    runtime: Runtime,
    consensus: Option<Consensus>,
    interconnect: Option<Transport>,
}

impl ApplicationStartup {
    async fn terminate(self) {
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
            Some(addr) => addr.parse::<cluster::HostPort>().map_err(|error| {
                Report::new(AppError::ParseGrpcAdvertiseAddress).attach_printable(error)
            })?,
            None => addr.into(),
        };
        let grpc_https_advertise_addr = args
            .grpc_https_advertise_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<cluster::HostPort>().map_err(|error| {
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
                addr.parse::<cluster::HostPort>().map_err(|error| {
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
            Some(addr) => addr.parse::<cluster::HostPort>().map_err(|error| {
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
            .transaction_idle_timeout(args.transaction_idle_timeout)
            .transaction_tombstone_retention(args.transaction_tombstone_retention)
            .transaction_max_statements(args.transaction_max_statements)
            .transaction_max_source_bytes(args.transaction_max_source_bytes)
            .transaction_max_open(args.transaction_max_open)
            .replica_count(args.replica_count)
            .state_snapshot_interval(args.state_snapshot_interval)
            .memory_pressure(memory_pressure)
            .drain_timeout(args.drain_timeout)
            .cluster_bootstrap_host(args.cluster_bootstrap_host)
            .db_path(PathBuf::from(args.db_path))
            .temp_dir(args.temp_dir)
            .build())
    }
}

#[tonic::async_trait]
impl SessionService for SessionServiceImpl {
    type SessionStream = ReceiverStream<Result<SessionResponse, Status>>;

    async fn session(
        &self,
        request: Request<tonic::Streaming<SessionRequest>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let authenticated_user = self.authenticate_grpc_metadata(request.metadata()).await?;
        let mut inbound = request.into_inner();
        let service = self.clone();
        let (tx, rx) = mpsc::channel(16);
        let mut event_rx = self.inner.events.subscribe();
        let mut runtime_event_rx = self.inner.runtime.subscribe_events();

        let service_tasks = service.inner.service_tasks.clone();
        service_tasks.spawn(async move {
            let mut subscriptions = SessionSubscriptions::for_user(authenticated_user);
            let mut clean_close = false;
            let shutdown = service.inner.shutdown.clone();
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        break;
                    }
                    inbound_request = tokio_stream::StreamExt::next(&mut inbound) => {
                        let Some(request) = inbound_request else {
                            clean_close = true;
                            break;
                        };
                        let request = match request {
                            Ok(request) => request,
                            Err(status) => {
                                tx.send(Err(status))
                                    .await
                                    .means_peer_left("session response stream");
                                subscriptions.stop_all(&service).await;
                                service.release_session_transaction_binding(&mut subscriptions);
                                return;
                            }
                        };

                        match request.request {
                            Some(proto::session_request::Request::Command(command)) => {
                                let result = service
                                    .process_command(
                                        command,
                                        &tx,
                                        &mut subscriptions,
                                    )
                                    .await;
                                let event = SessionResponse {
                                    event: Some(proto::session_response::Event::Result(result)),
                                };
                                if tx.send(Ok(event)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            Some(proto::session_request::Request::Suggest(suggest)) => {
                                let response = service
                                    .process_suggest(suggest, &subscriptions)
                                    .await;
                                let event = SessionResponse {
                                    event: Some(proto::session_response::Event::Suggest(response)),
                                };
                                if tx.send(Ok(event)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            Some(proto::session_request::Request::ListDomains(_)) => {
                                let event = service.domain_list_response(true).await;
                                if tx.send(Ok(event)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            Some(proto::session_request::Request::SetActiveDomain(_)) => {
                                tx.send(Err(Status::invalid_argument(
                                    "active domain selection is only supported by the web console \
                                     websocket",
                                )))
                                .await
                                .means_peer_left("session response stream");
                                subscriptions.stop_all(&service).await;
                                service.release_session_transaction_binding(&mut subscriptions);
                                return;
                            }
                            Some(proto::session_request::Request::AttachTransaction(request)) => {
                                let result = service
                                    .attach_transaction(request, &mut subscriptions)
                                    .await;
                                let event = SessionResponse {
                                    event: Some(proto::session_response::Event::Result(result)),
                                };
                                if tx.send(Ok(event)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            None => {
                                tx.send(Err(Status::invalid_argument(
                                    "session request payload is missing",
                                )))
                                .await
                                .means_peer_left("session response stream");
                                subscriptions.stop_all(&service).await;
                                service.release_session_transaction_binding(&mut subscriptions);
                                return;
                            }
                        }
                    }
                    server_event = event_rx.recv() => {
                        match server_event {
                            Ok(event) => {
                                let response = SessionResponse {
                                    event: Some(proto::session_response::Event::Server(event)),
                                };
                                if tx.send(Ok(response)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            // The session stays open and resumes from the newest event. Saying how
                            // many it skipped is what stops the gap from looking like quiet.
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                warn!(skipped, "session fell behind the server event bus");
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    runtime_event = runtime_event_rx.recv() => {
                        match runtime_event {
                            Ok(RuntimeEvent::Error(message)) => {
                                let response = SessionResponse {
                                    event: Some(proto::session_response::Event::Server(ServerEvent {
                                        level: i32::from(ServerEventLevel::Error),
                                        message,
                                    })),
                                };
                                if tx.send(Ok(response)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    service.release_session_transaction_binding(&mut subscriptions);
                                    return;
                                }
                            }
                            // As above: the runtime errors the session missed are gone, so the
                            // count is the only record that they happened.
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                warn!(skipped, "session fell behind the runtime event bus");
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }

            subscriptions.stop_all(&service).await;
            if clean_close {
                service.clean_close_transaction(&mut subscriptions).await;
            } else {
                service.release_session_transaction_binding(&mut subscriptions);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn upload_resource(
        &self,
        request: Request<tonic::Streaming<UploadResourceRequest>>,
    ) -> Result<Response<UploadResourceResponse>, Status> {
        let _authenticated_user = self.authenticate_grpc_metadata(request.metadata()).await?;
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            let leader_node = match leader.as_ref() {
                Some(leader_id) => self
                    .inner
                    .cluster
                    .gossip_state()
                    .await
                    .live_nodes
                    .into_iter()
                    .find(|node| node.node_id == *leader_id),
                None => None,
            };
            let mut leader_grpc_uri = String::new();
            if let Some(node) = leader_node
                && let Some(uri) = grpc_uri_from_advertise_addr(&node.grpc_advertise_addr)
            {
                leader_grpc_uri = uri;
            }
            return Ok(Response::new(UploadResourceResponse {
                success: false,
                message: "resource uploads must be sent to the cluster leader".to_string(),
                version: 0,
                diagnostics: Vec::new(),
                kind: i32::from(CommandResultKind::NotLeader),
                leader: match leader {
                    Some(leader) => leader.to_string(),
                    None => String::new(),
                },
                leader_grpc_uri,
            }));
        }

        let mut inbound = request.into_inner();
        let Some(first) = inbound.message().await? else {
            return Err(Status::invalid_argument(
                "upload resource request relay is empty",
            ));
        };
        let Some(proto::upload_resource_request::Event::Start(start)) = first.event else {
            return Err(Status::invalid_argument(
                "upload resource relay must start with metadata",
            ));
        };
        let identifier = ModelName::parse(&start.name)
            .map_err(|_| Status::invalid_argument("upload resource name is invalid"))?;
        let domain = match parse_request_domain(&start.domain) {
            Ok(domain) => domain,
            Err(RequestDomainError::Missing) => {
                return Err(Status::invalid_argument("no active domain selected"));
            }
            Err(RequestDomainError::Invalid) => {
                return Err(Status::invalid_argument(
                    "upload resource domain is invalid",
                ));
            }
        };

        let resources = self.inner.consensus.current_resources().await;
        if !resources.is_declared(&domain, &ResourceName::from(&identifier)) {
            return Ok(Response::new(UploadResourceResponse {
                success: false,
                message: format!("resource '{}' does not exist", identifier.as_str()),
                version: 0,
                diagnostics: Vec::new(),
                kind: i32::from(CommandResultKind::Error),
                leader: String::new(),
                leader_grpc_uri: String::new(),
            }));
        }

        let temp_archive = tempfile::NamedTempFile::new()
            .map_err(|_| Status::internal("failed to create temporary upload archive"))?;
        let temp_path = temp_archive.into_temp_path();
        let mut file = File::create(&temp_path)
            .await
            .map_err(|_| Status::internal("failed to open temporary upload archive"))?;
        let mut hasher = Hasher::new();
        let mut total_received = 0u64;
        while let Some(message) = inbound.message().await? {
            tokio::task::consume_budget().await;
            let Some(proto::upload_resource_request::Event::Chunk(chunk)) = message.event else {
                return Err(Status::invalid_argument(
                    "unexpected upload resource control event",
                ));
            };
            hasher.update(&chunk);
            file.write_all(&chunk)
                .await
                .map_err(|_| Status::internal("failed to write upload resource chunk"))?;
            total_received = total_received
                .checked_add(chunk.len().arch_into())
                .ok_or_else(|| Status::invalid_argument("upload resource archive is too large"))?;
        }
        file.flush()
            .await
            .map_err(|_| Status::internal("failed to flush upload resource archive"))?;
        drop(file);

        if start.total_bytes != 0 && start.total_bytes != total_received {
            return Ok(Response::new(UploadResourceResponse {
                success: false,
                message: format!(
                    "upload size mismatch: expected {}, received {}",
                    start.total_bytes, total_received
                ),
                version: 0,
                diagnostics: Vec::new(),
                kind: i32::from(CommandResultKind::Error),
                leader: String::new(),
                leader_grpc_uri: String::new(),
            }));
        }

        let root_checksum = {
            let hash = hasher.finalize();
            encode_hex(hash.as_bytes())
        };
        match self
            .install_uploaded_resource_archive(&domain, identifier, &temp_path, root_checksum)
            .await
        {
            Ok(version) => Ok(Response::new(UploadResourceResponse {
                success: true,
                message: format!("uploaded resource version {version}"),
                version,
                diagnostics: Vec::new(),
                kind: i32::from(CommandResultKind::Ok),
                leader: String::new(),
                leader_grpc_uri: String::new(),
            })),
            Err(error) => {
                let message = format!("{error:#}");
                let result = match error.downcast_ref::<ConsensusError>() {
                    Some(error) => self.consensus_error_response(error, message).await,
                    None => command_error(message),
                };
                Ok(Response::new(UploadResourceResponse {
                    success: false,
                    message: result.message,
                    version: 0,
                    diagnostics: result.diagnostics,
                    kind: result.kind,
                    leader: result.leader,
                    leader_grpc_uri: result.leader_grpc_uri,
                }))
            }
        }
    }
}

async fn apply_cluster_runtime_state(
    runtime: &Runtime,
    cluster: &cluster::ClusterHandle,
    local_node_id: &ClusterNodeName,
    state: ConsensusRuntimeState,
) -> Result<(), crate::runtime::RuntimeError> {
    let has_running_domain = state
        .domains
        .values()
        .any(|domain| matches!(domain.status, DomainStatus::Running));
    runtime
        .apply_cluster_state(
            local_node_id,
            state.revision,
            &state.domains,
            &state.domain_clock_authorities,
            &state.schedule,
        )
        .await?;
    cluster
        .set_local_runtime_revision_ready(state.revision)
        .await;
    if !has_running_domain {
        return Ok(());
    }

    let deadline = tokio::time::Instant::now() + RUNTIME_REVISION_READINESS_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        let gossip = cluster.gossip_state().await;
        let expected_nodes = gossip.live_identities();
        let ready_nodes = cluster
            .nodes_ready_for_runtime_revision(state.revision)
            .await;
        let pending_nodes = expected_nodes
            .difference(&ready_nodes)
            .map(|identity| identity.node_id().clone())
            .collect::<Vec<_>>();
        if pending_nodes.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(crate::runtime::RuntimeError::RuntimeRevisionReadiness {
                revision: state.revision,
                pending_nodes,
            });
        }
        tokio::time::sleep(RUNTIME_REVISION_READINESS_POLL_INTERVAL).await;
    }

    runtime.start_running_domain_ingestors().await
}

impl SessionServiceImpl {
    fn new_auth_rate_limiter() -> AuthRateLimiter {
        let quota = Quota::per_second(
            NonZeroU32::new(AUTH_RATE_LIMIT_PER_SECOND)
                .assured("AUTH_RATE_LIMIT_PER_SECOND is a positive constant"),
        );
        RateLimiter::keyed(quota)
    }

    async fn apply_current_cluster_state(&self) -> Result<(), crate::runtime::RuntimeError> {
        let state = self.inner.consensus.current_runtime_state().await;
        apply_cluster_runtime_state(
            &self.inner.runtime,
            &self.inner.cluster,
            self.inner.consensus.local_node_id(),
            state,
        )
        .await
    }

    async fn authenticate_grpc_metadata(
        &self,
        metadata: &MetadataMap,
    ) -> Result<UserName, GrpcAuthenticationError> {
        let Some(credentials) = credentials_from_metadata(metadata) else {
            return Err(GrpcAuthenticationError::Required);
        };
        self.authenticate_basic_credentials(&credentials)
            .await
            .ok_or(GrpcAuthenticationError::Failed)
    }

    async fn authenticate_basic_credentials(
        &self,
        credentials: &BasicAuthCredentials,
    ) -> Option<UserName> {
        let Ok(user_name) = UserName::parse(&credentials.username) else {
            return None;
        };
        let user = self.inner.consensus.current_user(&user_name).await?;
        let auth_rate_limit_key = user_name.as_str().to_string();
        if self
            .inner
            .failed_auth_rate_limit_keys
            .contains_key(&auth_rate_limit_key)
        {
            self.inner
                .auth_rate_limiter
                .until_key_ready(&auth_rate_limit_key)
                .await;
        }
        let verified = verify_password_hash(user.password_hash, credentials.password.clone()).await;
        if verified {
            self.inner
                .failed_auth_rate_limit_keys
                .remove(&auth_rate_limit_key);
        } else {
            self.inner
                .failed_auth_rate_limit_keys
                .insert(auth_rate_limit_key, ());
        }
        verified.then_some(user_name)
    }

    fn next_entity_gate_operation_id(&self) -> u64 {
        self.inner
            .next_entity_gate_operation_id
            .fetch_add(1, Ordering::Relaxed)
    }

    /// Report a control-plane failure this node recovered from to the sessions attached to it.
    fn broadcast_error(&self, message: impl Into<String>) {
        self.inner.events.report_error(message);
    }

    /// Validates the bindings a planned batch would activate: everything that has to reach outside
    /// the registry (domain pace, resource storage, ONNX metadata) and therefore cannot live in
    /// `DomainState::build`, which follower synchronization and startup replay also run.
    ///
    /// This runs over the candidate models the batch produces rather than over the statements that
    /// produced them, so `CREATE` and every present and future `ALTER` share one boundary.
    async fn validate_changed_model_bindings(
        &self,
        domain: &DomainName,
        pace: DomainPace,
        planned: &crate::registry::PlannedMutations,
    ) -> Result<(), String> {
        for model in planned.changed_models() {
            tokio::task::consume_budget().await;
            match model {
                Model::Ingestor(ingestor) => {
                    if let DomainPace::Paced = pace
                        && ingestor.timestamp_source.is_none()
                    {
                        return Err(format!(
                            "paced domain '{}' requires ingestor '{}' to declare TIMESTAMP NOW or \
                             TIMESTAMP AT <field>",
                            domain.as_str(),
                            ingestor.name.as_str()
                        ));
                    }
                }
                Model::Vhost(vhost) => {
                    if let Some(tls) = vhost.tls.as_ref() {
                        self.validate_vhost_tls_binding(domain, tls)
                            .await
                            .map_err(|error| {
                                format!(
                                    "invalid TLS resource for VHOST '{}': {error}",
                                    vhost.name.as_str()
                                )
                            })?;
                    }
                }
                Model::Lookup(lookup) => {
                    self.validate_lookup_binding(domain, lookup)
                        .await
                        .map_err(|error| {
                            format!("invalid HASH MAP '{}': {error}", lookup.name.as_str())
                        })?;
                }
                Model::Inferencer(processor) => {
                    self.validate_inferencer_binding(domain, processor)
                        .await
                        .map_err(|error| {
                            format!("invalid INFERENCER '{}': {error}", processor.name.as_str())
                        })?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Compiles the UDF family the batch would leave active. A UDF body is only usable once every
    /// sibling it may call compiles with it, so this takes the whole candidate set rather than the
    /// changed members.
    async fn prepare_planned_domain_udfs(
        &self,
        planned: &crate::registry::PlannedMutations,
    ) -> Result<Option<crate::runtime::CompiledDomainUdfs>, String> {
        let changed_udfs = planned
            .changed_models()
            .into_iter()
            .filter_map(|model| match model {
                Model::Udf(udf) => Some(udf.name.as_str().to_string()),
                _ => None,
            })
            .collect::<Vec<_>>();
        if changed_udfs.is_empty() {
            return Ok(None);
        }
        let domain_udfs = planned
            .candidate_models_of_kind(ModelKind::Udf)
            .into_iter()
            .filter_map(|model| match model {
                Model::Udf(udf) => Some(udf.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        self.inner
            .runtime
            .prepare_domain_udfs(domain_udfs)
            .await
            .map(Some)
            .map_err(|error| format!("invalid UDF '{}': {error}", changed_udfs.join(", ")))
    }

    async fn validate_vhost_tls_binding(
        &self,
        domain: &DomainName,
        tls: &VhostTlsResource,
    ) -> Result<(), String> {
        let resources = self.inner.consensus.current_resources().await;
        let id = resolve_resource_id(&resources, domain, &tls.resource, tls.version)?;
        load_vhost_tls_materials(&self.inner.resource_store, &id).await?;
        Ok(())
    }

    async fn validate_lookup_binding(
        &self,
        domain: &DomainName,
        lookup: &CreateLookup,
    ) -> Result<(), String> {
        let resources = self.inner.consensus.current_resources().await;
        let id = resolve_resource_id(&resources, domain, &lookup.resource, None)?;
        let path = self
            .inner
            .resource_store
            .resolve_content_path(&id, &lookup.path)
            .map_err(|error| error.to_string())?;
        ensure_file_exists(&path, "lookup").await
    }

    async fn validate_inferencer_binding(
        &self,
        domain: &DomainName,
        processor: &CreateInferencer,
    ) -> Result<(), String> {
        processor.execution_mode().map_err(|error| {
            format!(
                "inferencer '{}' has invalid tensor schemas: {}",
                processor.name.as_str(),
                error
            )
        })?;
        let resources = self.inner.consensus.current_resources().await;
        let id = resolve_resource_id(
            &resources,
            domain,
            &processor.resource,
            processor.resource_version,
        )?;
        let path = self
            .inner
            .resource_store
            .resolve_content_path(&id, &processor.file)
            .map_err(|error| error.to_string())?;
        ensure_file_exists(&path, "ONNX model").await?;
        if path.extension().and_then(|extension| extension.to_str()) != Some("onnx") {
            return Err(format!(
                "model file '{}' must have .onnx extension",
                processor.file
            ));
        }
        self.validate_inferencer_model_metadata(processor, &path)
            .await?;
        Ok(())
    }

    async fn validate_inferencer_model_metadata(
        &self,
        processor: &CreateInferencer,
        path: &std::path::Path,
    ) -> Result<(), String> {
        let path = path.to_path_buf();
        let processor_name = processor.name.as_str().to_string();
        let processor_file = processor.file.clone();
        let model_metadata = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::task::spawn_blocking(move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    Self::inspect_onnx_model_metadata(&processor_name, &processor_file, &path)
                }))
                .map_err(|panic| {
                    let reason = if let Some(message) = panic.downcast_ref::<&str>() {
                        *message
                    } else if let Some(message) = panic.downcast_ref::<String>() {
                        message.as_str()
                    } else {
                        "unknown panic"
                    };
                    format!(
                        "failed to inspect ONNX model '{}' for inferencer '{}': {}",
                        processor_file, processor_name, reason
                    )
                })?
            }),
        )
        .await
        .map_err(|_| {
            format!(
                "timed out inspecting ONNX model '{}' for inferencer '{}'",
                processor.file,
                processor.name.as_str()
            )
        })?
        .map_err(|error| {
            format!(
                "failed to inspect ONNX model '{}' for inferencer '{}': {}",
                processor.file,
                processor.name.as_str(),
                error
            )
        })??;

        model_metadata.validate_binding_names(processor)?;

        for mapping in &processor.inputs {
            let model_type = model_metadata.inputs.get(&mapping.tensor).verified(
                "validate_binding_names above rejected any mapping this metadata does not carry",
            );
            model_type.validate_declared_schema(
                processor,
                "input",
                &mapping.tensor,
                &mapping.schema,
            )?;
        }

        for declaration in &processor.output_schema {
            let model_type = model_metadata.outputs.get(&declaration.tensor).verified(
                "validate_binding_names above rejected any mapping this metadata does not carry",
            );
            model_type.validate_declared_schema(
                processor,
                "output",
                &declaration.tensor,
                &declaration.schema,
            )?;
        }

        Ok(())
    }

    fn inspect_onnx_model_metadata(
        processor_name: &str,
        processor_file: &str,
        path: &std::path::Path,
    ) -> Result<OnnxModelMetadata, String> {
        let mut builder = Session::builder().map_err(|error| {
            format!(
                "failed to initialize ONNX session builder for inferencer '{}': {}",
                processor_name, error
            )
        })?;
        let session = builder.commit_from_file(path).map_err(|error| {
            format!(
                "failed to inspect ONNX model '{}' for inferencer '{}': {}",
                processor_file, processor_name, error
            )
        })?;
        let model_inputs = session
            .inputs()
            .iter()
            .map(|input| {
                (
                    input.name().to_string(),
                    OnnxTensorMetadata {
                        value_type: input.dtype().clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let model_outputs = session
            .outputs()
            .iter()
            .map(|output| {
                (
                    output.name().to_string(),
                    OnnxTensorMetadata {
                        value_type: output.dtype().clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        Ok(OnnxModelMetadata {
            inputs: model_inputs,
            outputs: model_outputs,
        })
    }

    async fn refresh_http_tls_server_config(&self) -> Result<(), String> {
        nervix_interconnect::install_rustls_crypto_provider();
        let resources = self.inner.consensus.current_resources().await;
        let domains = self.inner.consensus.current_domains().await;
        let mut resolver = ResolvesServerCertUsingSni::new();
        let mut configured_tls = false;

        for domain_id in domains.keys() {
            tokio::task::consume_budget().await;
            let Ok(vhost_ids) =
                self.inner
                    .registry
                    .list_identifiers(domain_id, ModelKind::Vhost, "")
            else {
                continue;
            };

            for vhost_id in vhost_ids {
                tokio::task::consume_budget().await;
                let Ok(Some(vhost)) = self.inner.registry.get::<CreateVhost>(domain_id, &vhost_id)
                else {
                    continue;
                };
                let Some(tls) = vhost.tls.as_ref() else {
                    continue;
                };

                let id =
                    match resolve_resource_id(&resources, domain_id, &tls.resource, tls.version) {
                        Ok(id) => id,
                        Err(error) => {
                            warn!(
                                domain = domain_id.as_str(),
                                vhost = vhost.name.as_str(),
                                resource = tls.resource.as_str(),
                                error,
                                "failed to resolve VHOST TLS resource version"
                            );
                            continue;
                        }
                    };
                let version = id.version;
                let certified_key =
                    match load_vhost_tls_materials(&self.inner.resource_store, &id).await {
                        Ok(materials) => materials.certified_key,
                        Err(error) => {
                            warn!(
                                domain = domain_id.as_str(),
                                vhost = vhost.name.as_str(),
                                resource = tls.resource.as_str(),
                                version,
                                error,
                                "failed to load VHOST TLS materials"
                            );
                            continue;
                        }
                    };

                let mut applied_hostname = false;
                for hostname in &vhost.hostnames {
                    if let Err(error) = resolver.add(hostname, certified_key.clone()) {
                        warn!(
                            domain = domain_id.as_str(),
                            vhost = vhost.name.as_str(),
                            hostname,
                            resource = tls.resource.as_str(),
                            version,
                            error = %error,
                            "failed to add VHOST TLS hostname to SNI resolver"
                        );
                        continue;
                    }
                    applied_hostname = true;
                }
                configured_tls |= applied_hostname;
            }
        }

        let mut guard = self.inner.http_tls_server_config.write();
        if configured_tls {
            let config = ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(StdArc::new(resolver));
            *guard = Some(StdArc::new(config));
        } else {
            *guard = None;
        }
        Ok(())
    }

    async fn publish_resource_replica(&self, replica: ResourceNodeStatus) -> Result<(), String> {
        let Some(leader_id) = self.inner.consensus.current_leader().await else {
            return Err(
                "failed to publish resource replica: cluster leader is unknown".to_string(),
            );
        };

        if leader_id == self.inner.consensus.local_node_id().clone() {
            return self
                .inner
                .consensus
                .put_resource_replica(replica)
                .await
                .map_err(|error| format!("failed to publish resource replica: {error}"));
        }

        self.inner
            .interconnect
            .request(&leader_id, PublishResourceReplica { replica })
            .await
            .map_err(|error| format!("failed to publish resource replica: {error}"))?
            .map_err(|error| format!("failed to publish resource replica: {error}"))
    }

    async fn reconcile_resources_once(&self) {
        let local_node_id = self.inner.consensus.local_node_id().clone();
        let resources = self.inner.consensus.current_resources().await;
        let live_nodes = self.inner.cluster.gossip_state().await.live_nodes;

        // Replicas indexed by the resource version they hold and then by the node holding it. The
        // loop below asks about one resource on one node at a time, so both questions resolve by
        // key instead of scanning every replica for every resource and every live node.
        let mut replicas_by_resource: HashMap<
            ResourceId,
            HashMap<&ClusterNodeName, &ResourceNodeStatus>,
        > = HashMap::default();
        for replica in resources.replicas.iter() {
            replicas_by_resource
                .entry(replica.key.version_key().resource_id())
                .or_default()
                .insert(&replica.key.node_id, replica);
        }

        for resource in resources.versions.iter().cloned() {
            tokio::task::consume_budget().await;

            let local_key = ResourceReplicaKey::new(
                resource.id.domain.clone(),
                resource.id.identifier.clone(),
                resource.id.version,
                local_node_id.clone(),
            );
            let resource_replicas = replicas_by_resource.get(&resource.id);
            let holds_current_resource = |node_id: &ClusterNodeName| {
                resource_replicas.is_some_and(|replicas| {
                    replicas.get(node_id).is_some_and(|replica| {
                        replica.state == ResourceNodeState::Ready
                            && replica.root_checksum.as_deref()
                                == Some(resource.root_checksum.as_str())
                    })
                })
            };
            if holds_current_resource(&local_node_id) {
                continue;
            }

            let Some(source_node) = live_nodes.iter().find(|node| {
                node.node_id != local_node_id && holds_current_resource(&node.node_id)
            }) else {
                continue;
            };

            let failed_replica =
                |root_checksum: Option<String>, error: String| ResourceNodeStatus {
                    key: local_key.clone(),
                    state: ResourceNodeState::Failed,
                    root_checksum,
                    last_verified_at: None,
                    source_node_id: Some(source_node.node_id.clone()),
                    error: Some(error),
                };

            let archive = match fetch_resource_archive(
                &self.inner.interconnect,
                &source_node.node_id,
                &resource.id,
            )
            .await
            {
                Ok(archive) => archive,
                Err(error) => {
                    if let Err(publish_error) = self
                        .publish_resource_replica(failed_replica(None, error))
                        .await
                    {
                        self.broadcast_error(publish_error);
                    }
                    continue;
                }
            };

            if archive.root_checksum != resource.root_checksum {
                if let Err(error) = self
                    .publish_resource_replica(failed_replica(
                        Some(archive.root_checksum.clone()),
                        format!(
                            "resource checksum mismatch: expected {}, got {}",
                            resource.root_checksum, archive.root_checksum
                        ),
                    ))
                    .await
                {
                    self.broadcast_error(error);
                }
                continue;
            }

            let manifest = match self
                .inner
                .resource_store
                .install_from_archive_path(
                    resource.id.clone(),
                    &archive.path,
                    archive.root_checksum.clone(),
                    resource.created_by_node.clone(),
                    resource.created_at,
                )
                .await
            {
                Ok(manifest) => manifest,
                Err(error) => {
                    if let Err(publish_error) = self
                        .publish_resource_replica(failed_replica(None, error.to_string()))
                        .await
                    {
                        self.broadcast_error(publish_error);
                    }
                    continue;
                }
            };

            if manifest.resource.root_checksum != resource.root_checksum {
                let actual_checksum = manifest.resource.root_checksum.clone();
                if let Err(error) = self
                    .publish_resource_replica(failed_replica(
                        Some(actual_checksum.clone()),
                        format!(
                            "resource checksum mismatch: expected {}, got {}",
                            resource.root_checksum, actual_checksum
                        ),
                    ))
                    .await
                {
                    self.broadcast_error(error);
                }
                continue;
            }

            if let Err(error) = self
                .publish_resource_replica(ResourceNodeStatus {
                    key: local_key,
                    state: ResourceNodeState::Ready,
                    root_checksum: Some(resource.root_checksum.clone()),
                    last_verified_at: Some(current_timestamp()),
                    source_node_id: Some(source_node.node_id.clone()),
                    error: None,
                })
                .await
            {
                self.broadcast_error(error);
            } else if let Err(error) = self.refresh_http_tls_server_config().await {
                self.broadcast_error(format!("failed to refresh HTTP TLS config: {error}"));
            }
        }
    }

    async fn register_subscription_interest(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), String> {
        let key = SubscriptionInterestKey {
            domain: domain.clone(),
            relay: relay.clone(),
        };
        let first_interest = {
            let mut entry = self
                .inner
                .subscription_interest_counts
                .entry(key)
                .or_insert(0);
            *entry += 1;
            *entry == 1
        };
        if first_interest {
            self.inner
                .cluster
                .set_local_subscription_interest(domain.as_str(), relay.as_str(), true)
                .await;
        }
        if let Err(error) = self
            .wait_for_subscription_interest_visibility(domain, relay)
            .await
        {
            self.unregister_subscription_interest(domain, relay).await;
            return Err(error);
        }
        Ok(())
    }

    async fn wait_for_subscription_interest_visibility(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), String> {
        let local_node_id = self.inner.consensus.local_node_id();
        let mut pending_nodes = self
            .inner
            .cluster
            .live_node_ids()
            .await
            .into_iter()
            .filter(|node_id| node_id != local_node_id)
            .collect::<BTreeSet<_>>();
        let deadline = tokio::time::Instant::now() + SUBSCRIPTION_INTEREST_VISIBILITY_TIMEOUT;
        let mut last_errors = HashMap::new();

        while !pending_nodes.is_empty() {
            tokio::task::consume_budget().await;
            for node_id in pending_nodes.clone() {
                tokio::task::consume_budget().await;
                match self
                    .subscription_interest_is_visible(&node_id, local_node_id, domain, relay)
                    .await
                {
                    Ok(true) => {
                        pending_nodes.remove(&node_id);
                        last_errors.remove(&node_id);
                    }
                    Ok(false) => {}
                    Err(error) => {
                        last_errors.insert(node_id, error);
                    }
                }
            }
            if pending_nodes.is_empty() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for subscription interest in relay '{}' in domain '{}' to \
                     become visible on nodes {:?}; last errors: {:?}",
                    relay.as_str(),
                    domain.as_str(),
                    pending_nodes,
                    last_errors,
                ));
            }
        }
        Ok(())
    }

    async fn subscription_interest_is_visible(
        &self,
        target_node_id: &ClusterNodeName,
        subscriber_node_id: &ClusterNodeName,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<bool, String> {
        let response = self
            .inner
            .interconnect
            .request_with_timeout(
                target_node_id,
                RemoteSubscriptionInterestVisibilityRequest {
                    subscriber_node_id: subscriber_node_id.clone(),
                    domain: domain.clone(),
                    relay: relay.clone(),
                },
                SUBSCRIPTION_INTEREST_CHECK_TIMEOUT,
            )
            .await
            .map_err(|error| error.to_string())?;
        response.result
    }

    async fn unregister_subscription_interest(&self, domain: &DomainName, relay: &RelayName) {
        let key = SubscriptionInterestKey {
            domain: domain.clone(),
            relay: relay.clone(),
        };
        let mut should_clear = false;
        if let Some(mut entry) = self.inner.subscription_interest_counts.get_mut(&key) {
            if *entry <= 1 {
                should_clear = true;
            } else {
                *entry -= 1;
            }
        }
        if should_clear {
            self.inner.subscription_interest_counts.remove(&key);
            self.inner
                .cluster
                .set_local_subscription_interest(domain.as_str(), relay.as_str(), false)
                .await;
        }
    }

    async fn scheduled_stream_owner_nodes(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Vec<ClusterNodeName>, String> {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(Vec::new());
        };
        Ok(scheduled_relay_owner_nodes(domain_schedule, relay))
    }

    async fn dispatch_interconnect_control(
        &self,
        node_id: &ClusterNodeName,
        envelope: ControlEnvelope,
    ) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            let result = async {
                self.inner
                    .interconnect
                    .send(node_id, Envelope::Control(envelope.clone()))
                    .await
                    .map_err(|error| {
                        format!("failed to send interconnect control to '{node_id}': {error}")
                    })
            }
            .await;
            let error = match result {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            if tokio::time::Instant::now() >= deadline {
                return Err(error);
            }
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn domain_clock_authority_candidates(&self) -> DomainClockAuthorityCandidates {
        let gossip = self.inner.cluster.gossip_state().await;
        let live_identities = gossip.live_identities();
        let live_node_ids = live_identities
            .iter()
            .map(|identity| identity.node_id().clone())
            .collect::<Vec<_>>();
        let voters = self
            .inner
            .consensus
            .live_voter_ids(live_node_ids)
            .await
            .into_iter()
            .collect::<BTreeSet<_>>();
        DomainClockAuthorityCandidates::new(
            live_identities
                .into_iter()
                .filter(|identity| voters.contains(identity.node_id())),
        )
    }

    async fn selected_domain_clock_authority(
        &self,
        domain_id: &DomainName,
    ) -> Option<ClusterNodeIdentity> {
        self.domain_clock_authority_candidates()
            .await
            .owner_for(domain_id)
    }

    async fn reconcile_domain_clock_authorities(&self) {
        let state = self.inner.consensus.current_runtime_state().await;
        let candidates = self.domain_clock_authority_candidates().await;
        for (domain_id, domain) in state.domains {
            tokio::task::consume_budget().await;
            if matches!(domain.config.pace, DomainPace::Unpaced)
                || matches!(domain.status, DomainStatus::Stopped)
            {
                continue;
            }
            let expected = state
                .domain_clock_authorities
                .get(&domain_id)
                .cloned()
                .unwrap_or_else(DomainClockAuthority::initial);
            let owner = candidates.owner_for(&domain_id);
            if expected.owner() == owner.as_ref() {
                continue;
            }
            if let Err(error) = self
                .inner
                .consensus
                .reconcile_domain_clock_authority(
                    domain_id.clone(),
                    domain.start_version,
                    expected,
                    owner,
                )
                .await
            {
                warn!(
                    domain = domain_id.as_str(),
                    error = %error,
                    "failed to reconcile committed domain-clock authority"
                );
            }
        }
    }

    fn handle_domain_clock_progress(
        &self,
        authenticated_node: &ClusterNodeName,
        envelope: DomainClockProgressEnvelope,
    ) {
        if let Err(error) = self.inner.runtime.handle_domain_clock_progress(
            &envelope.domain_id,
            authenticated_node,
            &envelope.progress,
        ) {
            self.broadcast_error(format!(
                "failed to apply domain clock progress for '{}': {error}",
                envelope.domain_id.as_str(),
            ));
        }
    }

    async fn describe_stream(&self, domain: &DomainName, describe: DescribeRelay) -> CommandResult {
        let SubscriptionTarget {
            relay: ack_model,
            schema,
            branching,
        } = match self
            .subscription_target_from_schedule(domain, &describe.relay)
            .await
        {
            Ok(Some(target)) => target,
            Ok(None) => {
                return CommandResult {
                    success: false,
                    message: format!(
                        "stream '{}' does not exist in domain '{}'",
                        describe.relay.as_str(),
                        domain.as_str()
                    ),
                    diagnostics: vec![Diagnostic {
                        message: format!("stream '{}' not found", describe.relay.as_str()),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
            Err(message) => {
                return CommandResult {
                    success: false,
                    diagnostics: vec![Diagnostic {
                        message: message.clone(),
                        span_start: 0,
                        span_end: 0,
                    }],
                    message,
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };

        let schedule = self.inner.consensus.current_schedule().await;
        let scheduled_relay = if let Some(domain_schedule) = schedule.domain(domain) {
            domain_schedule.nodes.get(&NodeRef::new(
                ModelKind::Relay,
                ModelName::from(&describe.relay),
            ))
        } else {
            None
        };
        if describe.bindings.is_empty() {
            let metrics = match self
                .describe_metrics_for_scheduled_node(
                    domain,
                    ModelKind::Relay,
                    &describe.relay,
                    scheduled_relay,
                )
                .await
            {
                Ok(metrics) => metrics,
                Err(message) => return command_error(message),
            };
            return command_ok(append_metrics_lines(
                format_relay_describe_output(&ack_model, &branching, scheduled_relay),
                metrics,
            ));
        }

        let filter = match validate_subscription_bindings(
            &ack_model.name,
            &branching,
            &schema,
            &describe.bindings,
        ) {
            Ok(filter) => filter,
            Err(message) => {
                return CommandResult {
                    success: false,
                    diagnostics: vec![Diagnostic {
                        message: message.clone(),
                        span_start: 0,
                        span_end: 0,
                    }],
                    message,
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };
        let key = match branch_key_from_filter(&branching, &filter) {
            Ok(key) => key,
            Err(message) => {
                return CommandResult {
                    success: false,
                    diagnostics: vec![Diagnostic {
                        message: message.clone(),
                        span_start: 0,
                        span_end: 0,
                    }],
                    message,
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };

        if let Some(domain_state) = self.inner.consensus.current_domain(domain).await
            && let DomainStatus::Stopped = domain_state.status
        {
            return command_ok("not exists".to_string());
        }

        let owner_nodes = match self
            .scheduled_stream_owner_nodes(domain, &describe.relay)
            .await
        {
            Ok(owner_nodes) => owner_nodes,
            Err(message) => {
                return CommandResult {
                    success: false,
                    diagnostics: vec![Diagnostic {
                        message: message.clone(),
                        span_start: 0,
                        span_end: 0,
                    }],
                    message,
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };

        let local_node_id = self.inner.consensus.local_node_id().clone();
        let mut exists = false;
        if owner_nodes.is_empty() || owner_nodes.iter().any(|owner| owner == &local_node_id) {
            match self
                .inner
                .runtime
                .describe_local_stream_exists(domain, &describe.relay, &key)
            {
                Ok(local_exists) => exists |= local_exists,
                Err(error) => {
                    return CommandResult {
                        success: false,
                        diagnostics: vec![Diagnostic {
                            message: error.to_string(),
                            span_start: 0,
                            span_end: 0,
                        }],
                        message: error.to_string(),
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
            }
        }
        for owner in owner_nodes {
            if owner == local_node_id {
                continue;
            }
            let response = self
                .inner
                .interconnect
                .request_with_timeout(
                    &owner,
                    RemoteDescribeRelayRequest {
                        domain: domain.clone(),
                        relay: describe.relay.clone(),
                        bindings: describe.bindings.clone(),
                    },
                    REMOTE_DESCRIBE_RELAY_TIMEOUT,
                )
                .await;
            match response {
                Ok(RemoteDescribeRelayResponse {
                    result: Ok(remote_exists),
                }) => exists |= remote_exists,
                Ok(RemoteDescribeRelayResponse {
                    result: Err(message),
                }) => {
                    return CommandResult {
                        success: false,
                        diagnostics: vec![Diagnostic {
                            message: message.clone(),
                            span_start: 0,
                            span_end: 0,
                        }],
                        message,
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
                Err(error) => {
                    warn!(
                        %owner,
                        domain = domain.as_str(),
                        relay = describe.relay.as_str(),
                        error = %error,
                        "timed out waiting for remote DESCRIBE RELAY response"
                    );
                    continue;
                }
            }
        }

        let mut lines = vec![if exists {
            "exists".to_string()
        } else {
            "not exists".to_string()
        }];
        if exists {
            lines.push(format!("capacity: {}", ack_model.buffer));
        }
        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Relay,
                &describe.relay,
                scheduled_relay,
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        lines.extend(metrics);

        CommandResult {
            success: true,
            message: lines.join("\n"),
            diagnostics: Vec::new(),
            kind: i32::from(CommandResultKind::Ok),
            ..Default::default()
        }
    }

    async fn handle_describe_stream_request(
        &self,
        request: RemoteDescribeRelayRequest,
    ) -> Result<bool, String> {
        self.prepare_stream_owner_control_request(&request.domain, &request.relay)
            .await?;
        let Some(SubscriptionTarget {
            relay: ack_model,
            schema,
            branching,
        }) = self
            .subscription_target_from_schedule(&request.domain, &request.relay)
            .await?
        else {
            return Err(format!(
                "stream '{}' does not exist in domain '{}'",
                request.relay.as_str(),
                request.domain.as_str()
            ));
        };

        let filter = validate_subscription_bindings(
            &ack_model.name,
            &branching,
            &schema,
            &request.bindings,
        )?;
        let key = branch_key_from_filter(&branching, &filter)?;
        match self
            .inner
            .runtime
            .describe_local_stream_exists(&request.domain, &request.relay, &key)
        {
            Ok(exists) => Ok(exists),
            Err(crate::runtime::RuntimeError::RelayNotInstantiated { .. }) => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }

    async fn describe_domain(
        &self,
        domain: &DomainName,
        _describe: DescribeDomain,
    ) -> CommandResult {
        let Some(domain_state) = self.inner.consensus.current_domain(domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        let mut lines = vec![
            format!("domain: {}", domain.as_str()),
            format!("status: {:?}", domain_state.status).to_ascii_lowercase(),
        ];
        lines.extend(self.inner.runtime.describe_domain_statistics(domain));
        lines.push("placement:".to_string());
        lines.push(format!(
            "  default policy: {}",
            domain_state.config.placement.as_ref()
        ));
        let placement_plan = self
            .inner
            .registry
            .placement_plan(domain, domain_state.config.placement);
        let rule_count = match placement_plan.as_ref() {
            Some(plan) => plan.rules.len(),
            None => 0,
        };
        lines.push(format!("  rule count: {rule_count}"));
        if let Some(plan) = placement_plan {
            let schedule = self.inner.consensus.current_schedule().await;
            let domain_schedule = schedule.domain(domain);
            for (index, group) in plan.require_groups.iter().enumerate() {
                lines.push(format!("  group {}:", index + 1));
                lines.push(format!(
                    "    members: {}",
                    format_placement_runtime_nodes(&group.members)
                ));
                let host = match placement_group_host(domain_schedule, &group.members) {
                    Some(host) => host.as_str(),
                    None => "(unassigned)",
                };
                lines.push(format!("    host: {host}"));
                for bond in &group.bonds {
                    lines.push(format!(
                        "    bond: {} <-> {} ({})",
                        format_placement_runtime_node(&bond.left, &group.members),
                        format_placement_runtime_node(&bond.right, &group.members),
                        placement_claim_owner(&bond.winning_rules),
                    ));
                }
            }
        }
        command_ok(lines.join("\n"))
    }

    async fn show_placements(&self, domain: &DomainName) -> CommandResult {
        let Some(domain_state) = self.inner.consensus.current_domain(domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        let Some(plan) = self
            .inner
            .registry
            .placement_plan(domain, domain_state.config.placement)
        else {
            return command_ok("no placements".to_string());
        };
        if plan.rules.is_empty() {
            return command_ok("no placements".to_string());
        }
        let lines = plan
            .rules
            .iter()
            .map(|rule| {
                let coverage = placement_rule_coverage_status(rule);
                let rank = match rule.rank {
                    Some(rank) => rank.to_string(),
                    None => "unranked".to_string(),
                };
                format!(
                    "{} policy={} rank={} coverage={coverage}",
                    rule.name.as_str(),
                    rule.policy.as_ref(),
                    rank,
                )
            })
            .collect::<Vec<_>>();
        command_ok(lines.join("\n"))
    }

    async fn describe_placement(
        &self,
        domain: &DomainName,
        describe: DescribePlacement,
    ) -> CommandResult {
        let Some(domain_state) = self.inner.consensus.current_domain(domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        let Some(plan) = self
            .inner
            .registry
            .placement_plan(domain, domain_state.config.placement)
        else {
            return command_error(format!("placement '{}' not found", describe.name.as_str()));
        };
        // The plan was just built for this describe and is walked once for the single named rule,
        // so an index over it would cost the walk it replaces.
        let Some(rule) = plan
            .rules
            .iter()
            .find(|rule| rule.name == ModelName::from(&describe.name))
        else {
            return command_error(format!("placement '{}' not found", describe.name.as_str()));
        };
        let form = match self
            .inner
            .registry
            .get::<CreatePlacement>(domain, &describe.name)
        {
            Ok(Some(placement)) => placement.to_canonical_nspl().ok(),
            Ok(None) => None,
            Err(error) => return command_error(error.to_string()),
        };
        let mut lines = vec![format!("placement: {}", rule.name.as_str())];
        if let Some(form) = form {
            lines.push(format!("form: {form}"));
        }
        lines.push(format!("policy: {}", rule.policy.as_ref()));
        let rank = match rule.rank {
            Some(rank) => rank.to_string(),
            None => "unranked".to_string(),
        };
        lines.push(format!("rank: {rank}"));
        let rule_runtime_nodes = placement_rule_runtime_nodes(rule);
        let from_runtime_nodes = placement_rule_endpoint_nodes(rule, true);
        let to_runtime_nodes = placement_rule_endpoint_nodes(rule, false);
        lines.push(format!(
            "from: {}",
            format_placement_runtime_nodes_in_context(&from_runtime_nodes, &rule_runtime_nodes)
        ));
        lines.push(format!(
            "to: {}",
            format_placement_runtime_nodes_in_context(&to_runtime_nodes, &rule_runtime_nodes)
        ));
        for endpoint in &rule.endpoint_pairs {
            lines.push(format!(
                "pair: {} -> {}",
                format_placement_runtime_node(&endpoint.source, &rule_runtime_nodes),
                format_placement_runtime_node(&endpoint.destination, &rule_runtime_nodes),
            ));
            lines.push(format!("connected: {}", endpoint.connected));
            if endpoint.connected {
                lines.push(format!(
                    "covered: {}",
                    format_placement_runtime_nodes(&ordered_placement_corridor(endpoint))
                ));
            } else {
                lines.push("covered: (none)".to_string());
            }
            for witness in &endpoint.witnesses {
                lines.push(format!(
                    "witness: {}",
                    witness
                        .path
                        .iter()
                        .map(|node| format_placement_runtime_node(node, &rule_runtime_nodes))
                        .collect::<Vec<_>>()
                        .join(" -> ")
                ));
            }
        }
        for claim in &rule.claims {
            lines.push(format!(
                "effective pair: {} <-> {}",
                format_placement_runtime_node(&claim.left, &rule_runtime_nodes),
                format_placement_runtime_node(&claim.right, &rule_runtime_nodes),
            ));
            lines.push(format!(
                "effective policy: {}",
                claim.effective_policy.as_ref()
            ));
            if claim.effective {
                lines.push(format!(
                    "winning claim: {}",
                    placement_claim_owner(&claim.winning_rules)
                ));
            } else {
                lines.push(format!(
                    "overridden by: {}",
                    placement_claim_owner(&claim.winning_rules)
                ));
            }
        }
        let schedule = self.inner.consensus.current_schedule().await;
        let domain_schedule = schedule.domain(domain);
        for group in placement_groups_claimed_by_rule(&plan, rule) {
            lines.push(format!(
                "group members: {}",
                format_placement_runtime_nodes(&group.members)
            ));
            let host = match placement_group_host(domain_schedule, &group.members) {
                Some(host) => host.as_str(),
                None => "(unassigned)",
            };
            lines.push(format!("group host: {host}"));
            for bond in &group.bonds {
                lines.push(format!(
                    "bond: {} <-> {} ({})",
                    format_placement_runtime_node(&bond.left, &group.members),
                    format_placement_runtime_node(&bond.right, &group.members),
                    placement_claim_owner(&bond.winning_rules),
                ));
            }
        }
        command_ok(lines.join("\n"))
    }

    async fn describe_endpoint(
        &self,
        domain: &DomainName,
        describe: DescribeEndpoint,
    ) -> CommandResult {
        match self
            .inner
            .registry
            .get::<CreateEndpoint>(domain, &describe.name)
        {
            Ok(Some(endpoint)) => command_ok(format_endpoint_describe_output(
                &ModelName::from(&describe.name),
                &endpoint,
            )),
            Ok(None) => command_error(format!("endpoint '{}' not found", describe.name.as_str())),
            Err(error) => command_error(error.to_string()),
        }
    }

    async fn describe_ingestor(
        &self,
        domain: &DomainName,
        describe: DescribeIngestor,
    ) -> CommandResult {
        let (ingestor, ingestor_node) = match self
            .ingestor_target_from_schedule(domain, &describe.ingestor)
            .await
        {
            Ok(Some(target)) => target,
            Ok(None) => {
                return command_error(format!(
                    "ingestor '{}' does not exist in domain '{}'",
                    describe.ingestor.as_str(),
                    domain.as_str()
                ));
            }
            Err(message) => return command_error(message),
        };

        let local_node_id = self.inner.consensus.local_node_id();
        let summary = if ingestor_node.executes_on(local_node_id) {
            self.inner
                .runtime
                .describe_local_ingestor(domain, &describe.ingestor)
                .map(|summary| {
                    (
                        summary,
                        self.inner.runtime.describe_metrics_for(
                            domain,
                            "INGESTOR",
                            &describe.ingestor,
                        ),
                    )
                })
        } else if let Some(owner) = ingestor_node.execution_node() {
            match self
                .inner
                .interconnect
                .request(
                    owner,
                    RemoteDescribeIngestorRequest {
                        domain: domain.clone(),
                        name: describe.ingestor.clone(),
                    },
                )
                .await
            {
                Ok(Ok(summary)) => Ok(runtime_ingestor_describe_from_envelope(summary)),
                Ok(Err(message)) => Err(message),
                Err(error) => Err(error.to_string()),
            }
        } else {
            Ok((
                RuntimeIngestorDescribe {
                    running: false,
                    ready: false,
                    quiesce_state: None,
                    quiesce_counters: Default::default(),
                    memory_backpressure_paused: self
                        .inner
                        .runtime
                        .ingestors_paused_for_memory_pressure(),
                    transient_error: None,
                    reconnect_backoff: None,
                    reconnect_wait_millis: None,
                    kafka_domain_offsets: None,
                },
                self.inner
                    .runtime
                    .describe_metrics_for(domain, "INGESTOR", &describe.ingestor),
            ))
        };

        match summary {
            Ok((summary, metrics)) => command_ok(append_metrics_lines(
                format_ingestor_describe_output(
                    &describe.ingestor,
                    &ingestor,
                    &ingestor_node,
                    &summary,
                ),
                metrics,
            )),
            Err(message) => command_error(message),
        }
    }

    async fn dataflow_node_status_for_graph(
        &self,
        domain: &DomainName,
        kind: &str,
        identifier: &ModelName,
    ) -> DataflowNodeHealth {
        dataflow_node_status_from_envelope(
            self.dataflow_node_status_envelope_for_graph(domain, kind, identifier)
                .await,
        )
    }

    async fn dataflow_node_status_envelope_for_graph(
        &self,
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
    ) -> DataflowNodeStatusEnvelope {
        let identifier = identifier.into();
        let Ok(model_kind) = kind.to_ascii_lowercase().parse::<ModelKind>() else {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier.clone());
        };
        if model_kind != ModelKind::Ingestor && model_kind != ModelKind::Emitter {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier.clone());
        }
        let Some(node) = self
            .scheduled_model_node(domain, model_kind, identifier.clone())
            .await
        else {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier.clone());
        };
        let local_node_id = self.inner.consensus.local_node_id();
        if node.executes_on(local_node_id) {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier.clone());
        }
        let Some(owner) = node.execution_node() else {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier.clone());
        };
        let response = self
            .inner
            .interconnect
            .request_with_timeout(
                owner,
                RemoteDataflowNodeStatusRequest {
                    domain: domain.clone(),
                    kind: model_kind,
                    name: identifier.clone(),
                },
                Duration::from_secs(2),
            )
            .await;
        match response {
            Ok(RemoteDataflowNodeStatusResponse { result: Ok(status) }) => status,
            _ => self.local_dataflow_node_status_envelope(domain, kind, identifier),
        }
    }

    fn local_dataflow_node_status_envelope(
        &self,
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
    ) -> DataflowNodeStatusEnvelope {
        let identifier = identifier.into();
        let health = self
            .inner
            .runtime
            .dataflow_node_status(domain, kind, identifier.clone());
        let transient = self
            .inner
            .runtime
            .dataflow_node_transient_state(domain, kind, identifier);
        dataflow_node_status_to_envelope(
            health.status,
            health.detail,
            transient.error,
            transient.reconnect_backoff,
            health
                .reconnect_wait_millis
                .or(transient.reconnect_wait_millis),
        )
    }

    async fn handle_dataflow_node_status_request(
        &self,
        request: RemoteDataflowNodeStatusRequest,
    ) -> Result<DataflowNodeStatusEnvelope, String> {
        self.prepare_owner_control_request(&request.domain, request.kind, &request.name)
            .await?;
        let health = self.inner.runtime.dataflow_node_status(
            &request.domain,
            request.kind.as_str(),
            &request.name,
        );
        let transient = self.inner.runtime.dataflow_node_transient_state(
            &request.domain,
            request.kind.as_str(),
            &request.name,
        );
        Ok(dataflow_node_status_to_envelope(
            health.status,
            health.detail,
            transient.error,
            transient.reconnect_backoff,
            health
                .reconnect_wait_millis
                .or(transient.reconnect_wait_millis),
        ))
    }

    fn local_domain_drain_status(&self, domain: &DomainName) -> DomainDrainStatusEnvelope {
        let status = self.inner.runtime.domain_drain_status(domain);
        let emitter_publishing = status
            .emitter_publishing
            .into_iter()
            .map(emitter_publishing_drain_status_envelope)
            .collect();
        DomainDrainStatusEnvelope {
            active_ingestors: status.active_ingestors.arch_into(),
            active_generators: status.active_generators.arch_into(),
            outstanding_acks: status.outstanding_acks.arch_into(),
            buffered_emitter_messages: status.buffered_emitter_messages.arch_into(),
            emitter_publishing,
        }
    }

    async fn domain_drain_status_on_node(
        &self,
        node_id: &ClusterNodeName,
        domain: &DomainName,
    ) -> Result<DomainDrainStatusEnvelope, String> {
        if node_id == self.inner.consensus.local_node_id() {
            self.inner.runtime.force_flush_domain_if_idle(domain);
            return Ok(self.local_domain_drain_status(domain));
        }
        self.inner
            .interconnect
            .request_with_timeout(
                node_id,
                RemoteDomainDrainStatusRequest {
                    domain: domain.clone(),
                },
                Duration::from_secs(2),
            )
            .await
            .map_err(|error| error.to_string())?
            .result
    }

    fn local_entity_drain_status(
        &self,
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
    ) -> EntityDrainStatusEnvelope {
        let status =
            self.inner
                .runtime
                .entity_drain_status(domain, relays, affected_entities, purpose);
        let emitter_publishing = status
            .emitter_publishing
            .into_iter()
            .map(emitter_publishing_drain_status_envelope)
            .collect();
        EntityDrainStatusEnvelope {
            buffered_relay_batches: status.buffered_relay_batches.arch_into(),
            node_work_items: status.node_work_items.arch_into(),
            outstanding_acks: status.outstanding_acks.arch_into(),
            emitter_publishing,
        }
    }

    async fn engage_entity_gate_on_node(
        &self,
        node_id: &ClusterNodeName,
        engagement: EntityGateEngagement<'_>,
    ) -> Result<(), String> {
        let EntityGateEngagement {
            operation_id,
            domain,
            relays,
            affected_entities,
            purpose,
            deadline,
            reason,
        } = engagement;
        if node_id == self.inner.consensus.local_node_id() {
            return self
                .inner
                .runtime
                .engage_entity_gate_operation(
                    operation_id,
                    domain,
                    relays,
                    affected_entities,
                    purpose,
                    EntityGateLease { deadline, reason },
                )
                .await;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let deadline_millis = u64::try_from(remaining.as_millis().max(1)).unwrap_or(u64::MAX);
        self.inner
            .interconnect
            .request_with_timeout(
                node_id,
                RemoteEntityGateRequest {
                    operation_id,
                    domain: domain.clone(),
                    relays: relays.to_vec(),
                    affected_entities: affected_entities.to_vec(),
                    purpose,
                    deadline_millis,
                    reason: reason.to_string(),
                },
                remaining.min(Duration::from_secs(2)),
            )
            .await
            .map_err(|error| error.to_string())?
            .result
    }

    async fn entity_drain_status_on_node(
        &self,
        node_id: &ClusterNodeName,
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
        deadline: tokio::time::Instant,
    ) -> Result<EntityDrainStatusEnvelope, String> {
        if node_id == self.inner.consensus.local_node_id() {
            let status = self.local_entity_drain_status(domain, relays, affected_entities, purpose);
            if status.buffered_relay_batches != 0
                || status.node_work_items != 0
                || status.outstanding_acks != 0
            {
                self.inner.runtime.force_flush_domain_if_idle(domain);
            }
            return Ok(status);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        self.inner
            .interconnect
            .request_with_timeout(
                node_id,
                RemoteEntityDrainStatusRequest {
                    domain: domain.clone(),
                    relays: relays.to_vec(),
                    affected_entities: affected_entities.to_vec(),
                    purpose,
                },
                remaining.min(Duration::from_secs(2)),
            )
            .await
            .map_err(|error| error.to_string())?
            .result
    }

    async fn release_entity_gate_on_node(
        &self,
        node_id: &ClusterNodeName,
        operation_id: u64,
        domain: &DomainName,
    ) -> Result<(), String> {
        if node_id == self.inner.consensus.local_node_id() {
            return self
                .inner
                .runtime
                .release_entity_gate_operation(operation_id, domain)
                .await;
        }
        self.inner
            .interconnect
            .request(
                node_id,
                RemoteEntityGateReleaseRequest {
                    operation_id,
                    domain: domain.clone(),
                },
            )
            .await
            .map_err(|error| error.to_string())?
            .result
    }

    fn schedule_cluster_entity_gate_release(&self, release: PendingClusterEntityGateRelease) {
        let service = self.clone();
        self.inner.service_tasks.spawn(async move {
            service.retry_cluster_entity_gate_release(release).await;
        });
    }

    async fn retry_cluster_entity_gate_release(
        &self,
        mut release: PendingClusterEntityGateRelease,
    ) {
        while !release.nodes.is_empty() {
            tokio::task::consume_budget().await;
            let nodes = release.nodes.clone();
            for node in nodes {
                tokio::task::consume_budget().await;
                let result = tokio::select! {
                    _ = self.inner.shutdown.cancelled() => return,
                    result = self.release_entity_gate_on_node(
                        &node,
                        release.operation_id,
                        &release.domain,
                    ) => result,
                };
                match result {
                    Ok(()) => {
                        release.nodes.remove(&node);
                    }
                    Err(error) => {
                        debug!(
                            domain = release.domain.as_str(),
                            operation_id = release.operation_id,
                            %node,
                            error,
                            "entity gate release retry remains pending"
                        );
                    }
                }
            }
            if release.nodes.is_empty() {
                return;
            }
            tokio::select! {
                _ = self.inner.shutdown.cancelled() => return,
                _ = sleep(ENTITY_GATE_RELEASE_RETRY_INTERVAL) => {}
            }
        }
    }

    /// Cluster nodes the cluster considers usable: gossip peers that are not marked unavailable.
    ///
    /// Scheduling and failover read liveness this way, so every leader-orchestrated hold must too.
    /// A node marked unavailable cannot answer a gate request, and contacting it only spends the
    /// request deadline before the hold fails.
    async fn available_node_ids(&self) -> Vec<ClusterNodeName> {
        self.available_node_incarnations()
            .await
            .into_keys()
            .collect()
    }

    async fn available_node_incarnations(
        &self,
    ) -> BTreeMap<ClusterNodeName, ClusterNodeIncarnation> {
        let gossip = self.inner.cluster.gossip_state().await;
        gossip
            .live_nodes
            .into_iter()
            .filter(|node| !gossip.dead_node_ids.contains(&node.node_id))
            .map(|node| (node.node_id, node.incarnation))
            .collect()
    }

    async fn live_node_incarnations(&self) -> BTreeMap<ClusterNodeName, ClusterNodeIncarnation> {
        self.inner
            .cluster
            .gossip_state()
            .await
            .live_nodes
            .into_iter()
            .map(|node| (node.node_id, node.incarnation))
            .collect()
    }

    fn verify_ownership_handoff_node_incarnation(
        current: &BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
        node: &ClusterNodeName,
        expected: ClusterNodeIncarnation,
        role: &str,
    ) -> OwnershipHandoffResult<()> {
        let Some(actual) = current.get(node) else {
            return Err(OwnershipHandoffError::participant(format!(
                "{role} node '{node}' is unavailable during ownership handoff"
            )));
        };
        if *actual != expected {
            return Err(OwnershipHandoffError::participant(format!(
                "{role} node '{node}' changed process incarnation during ownership handoff"
            )));
        }
        Ok(())
    }

    async fn verify_planned_ownership_handoff_incarnations(
        &self,
        handoff: &PlannedOwnershipHandoff,
    ) -> OwnershipHandoffResult<()> {
        let current = self.live_node_incarnations().await;
        for moved in &handoff.moves {
            tokio::task::consume_budget().await;
            let source = *handoff
                .node_incarnations
                .get(&moved.former_owner)
                .verified("every planned former owner has a bound incarnation");
            Self::verify_ownership_handoff_node_incarnation(
                &current,
                &moved.former_owner,
                source,
                "source",
            )?;
            let destination = *handoff
                .node_incarnations
                .get(&moved.destination)
                .verified("every planned destination has a bound incarnation");
            Self::verify_ownership_handoff_node_incarnation(
                &current,
                &moved.destination,
                destination,
                "destination",
            )?;
        }
        Ok(())
    }

    async fn engage_cluster_entity_gates(
        &self,
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
        deadline: tokio::time::Instant,
    ) -> Result<ClusterEntityGate, Report<DomainAlterError>> {
        let mut nodes = self.available_node_ids().await;
        if !nodes
            .iter()
            .any(|node| node == self.inner.consensus.local_node_id())
        {
            nodes.push(self.inner.consensus.local_node_id().clone());
        }
        nodes.sort();
        nodes.dedup();
        let operation_id = self.next_entity_gate_operation_id();
        let mut gate = ClusterEntityGate::new(self, operation_id, domain);
        let reason = match purpose {
            EntityGatePurpose::ModelAlteration => "leader-orchestrated entity alteration",
            EntityGatePurpose::OwnershipHandoff => "leader-orchestrated ownership handoff",
        };
        for node in &nodes {
            tokio::task::consume_budget().await;
            gate.record_attempt(node.clone());
            if let Err(error) = self
                .engage_entity_gate_on_node(
                    node,
                    EntityGateEngagement {
                        operation_id,
                        domain,
                        relays,
                        affected_entities,
                        purpose,
                        deadline,
                        reason,
                    },
                )
                .await
            {
                self.release_cluster_entity_gates(gate).await;
                return Err(Report::new(DomainAlterError::EntityGate {
                    domain: domain.clone(),
                    operation: purpose.operation_name(),
                    reason: format!("failed to engage entity gates on node '{node}': {error}"),
                }));
            }
        }
        Ok(gate)
    }

    async fn begin_planned_ownership_handoff(
        &self,
        domain: &DomainName,
        current: Option<&nervix_models::DomainSchedule>,
        planned: Option<&nervix_models::DomainSchedule>,
    ) -> Result<Option<PlannedOwnershipHandoff>, Report<DomainAlterError>> {
        let moves = planned_ownership_moves(current, planned);
        if moves.is_empty() {
            return Ok(None);
        }
        let current = current
            .verified("an ownership move can only be derived from a current domain schedule");
        let planned = planned
            .verified("an ownership move can only be derived from a planned domain schedule");
        let base_schedule_fingerprint = Runtime::ownership_handoff_schedule_fingerprint(current)
            .map_err(|reason| {
                Report::new(DomainAlterError::EntityGate {
                    domain: domain.clone(),
                    operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                    reason: reason.to_string(),
                })
            })?;
        let target_schedule_fingerprint = Runtime::ownership_handoff_schedule_fingerprint(planned)
            .map_err(|reason| {
                Report::new(DomainAlterError::EntityGate {
                    domain: domain.clone(),
                    operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                    reason: reason.to_string(),
                })
            })?;
        let first_move = moves
            .first()
            .verified("the empty ownership move set returned before planning a handoff");
        let first_node = planned
            .nodes
            .get(&first_move.entity)
            .verified("every ownership move was derived from the planned schedule");
        let Some(first_transition) = first_node.ownership_transition.as_ref() else {
            return Err(Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: "planned schedule does not identify its ownership handoff transition"
                    .to_string(),
            }));
        };
        let operation_id = first_transition.id.clone();
        for moved in &moves {
            let node = planned
                .nodes
                .get(&moved.entity)
                .verified("every ownership move was derived from the planned schedule");
            let Some(transition) = node.ownership_transition.as_ref() else {
                return Err(Report::new(DomainAlterError::EntityGate {
                    domain: domain.clone(),
                    operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                    reason: format!(
                        "planned {} '{}' does not identify its ownership handoff transition",
                        moved.entity.kind.as_str(),
                        moved.entity.identifier.as_str()
                    ),
                }));
            };
            if transition.id != operation_id
                || transition.source != moved.former_owner
                || transition.destination != moved.destination
                || transition.state_recovery != OwnershipStateRecoveryOutcome::Complete
                || !transition.resets.is_empty()
            {
                return Err(Report::new(DomainAlterError::EntityGate {
                    domain: domain.clone(),
                    operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                    reason: format!(
                        "planned {} '{}' has an inconsistent ownership handoff transition",
                        moved.entity.kind.as_str(),
                        moved.entity.identifier.as_str()
                    ),
                }));
            }
        }
        let node_incarnations = self.available_node_incarnations().await;
        if let Some(moved) = moves
            .iter()
            .find(|moved| !node_incarnations.contains_key(&moved.former_owner))
        {
            return Err(Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: format!(
                    "former owner '{}' is unavailable before ownership handoff",
                    moved.former_owner
                ),
            }));
        }
        if let Some(moved) = moves
            .iter()
            .find(|moved| !node_incarnations.contains_key(&moved.destination))
        {
            return Err(Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: format!(
                    "destination node '{}' is unavailable before ownership handoff",
                    moved.destination
                ),
            }));
        }
        let affected_entities = moves
            .iter()
            .map(|moved| moved.entity.clone())
            .collect::<Vec<_>>();
        let relays = Runtime::ownership_handoff_relays_for_schedule(current, &affected_entities);
        let former_owners = moves
            .iter()
            .map(|moved| moved.former_owner.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let started_at = tokio::time::Instant::now();
        let phase_budget = self.inner.runtime.entity_gate_deadline();
        let preparation_deadline = started_at.checked_add(phase_budget).ok_or_else(|| {
            Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: "ownership handoff preparation deadline exceeds the runtime instant range"
                    .to_string(),
            })
        })?;
        let activation_deadline =
            preparation_deadline
                .checked_add(phase_budget)
                .ok_or_else(|| {
                    Report::new(DomainAlterError::EntityGate {
                        domain: domain.clone(),
                        operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                        reason: "ownership handoff activation deadline exceeds the runtime \
                                 instant range"
                            .to_string(),
                    })
                })?;
        let gate = self
            .engage_cluster_entity_gates(
                domain,
                &relays,
                &affected_entities,
                EntityGatePurpose::OwnershipHandoff,
                activation_deadline,
            )
            .await?;
        #[cfg(feature = "testing")]
        self.inner.runtime.pause_entity_gate_if_armed(domain).await;
        if let Err(error) = self
            .wait_for_cluster_entity_drain(
                &gate,
                &relays,
                &affected_entities,
                EntityGatePurpose::OwnershipHandoff,
                &former_owners,
                preparation_deadline,
            )
            .await
        {
            self.release_cluster_entity_gates(gate).await;
            return Err(error);
        }
        let mut prepared = Vec::new();
        for moved in &moves {
            tokio::task::consume_budget().await;
            let result = tokio::time::timeout_at(preparation_deadline, async {
                let checkpoints = self
                    .capture_ownership_handoff_state(
                        &operation_id,
                        domain,
                        moved,
                        *node_incarnations
                            .get(&moved.former_owner)
                            .verified("every former owner was found in live gossip above"),
                        base_schedule_fingerprint,
                    )
                    .await?;
                self.prepare_ownership_handoff_state(RemotePrepareOwnershipHandoffStateRequest {
                    operation_id: operation_id.clone(),
                    source: moved.former_owner.clone(),
                    destination: moved.destination.clone(),
                    source_incarnation: *node_incarnations
                        .get(&moved.former_owner)
                        .verified("every former owner was found in live gossip above"),
                    destination_incarnation: *node_incarnations
                        .get(&moved.destination)
                        .verified("every destination was found in live gossip above"),
                    domain: domain.clone(),
                    entity: moved.entity.clone(),
                    base_schedule_fingerprint,
                    target_schedule_fingerprint,
                    checkpoints,
                })
                .await
            })
            .await;
            match result {
                Ok(Ok(())) => prepared.push(moved.clone()),
                Ok(Err(reason)) => {
                    self.discard_ownership_handoff_state(&operation_id, domain, &prepared)
                        .await;
                    self.release_cluster_entity_gates(gate).await;
                    return Err(Report::new(DomainAlterError::EntityGate {
                        domain: domain.clone(),
                        operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                        reason: format!(
                            "failed to prepare {} '{}' from node '{}' on node '{}': {reason}",
                            moved.entity.kind.as_str(),
                            moved.entity.identifier.as_str(),
                            moved.former_owner,
                            moved.destination
                        ),
                    }));
                }
                Err(_) => {
                    self.discard_ownership_handoff_state(&operation_id, domain, &prepared)
                        .await;
                    self.release_cluster_entity_gates(gate).await;
                    return Err(Report::new(DomainAlterError::EntityGate {
                        domain: domain.clone(),
                        operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                        reason: format!(
                            "timed out preparing {} '{}' from node '{}' on node '{}'",
                            moved.entity.kind.as_str(),
                            moved.entity.identifier.as_str(),
                            moved.former_owner,
                            moved.destination
                        ),
                    }));
                }
            }
        }
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_ownership_handoff_after_preparation_if_armed(domain)
            .await;
        let handoff = PlannedOwnershipHandoff {
            operation_id,
            base_schedule_fingerprint,
            target_schedule_fingerprint,
            node_incarnations,
            gate,
            moves,
            started_at,
            preparation_deadline,
            activation_deadline,
        };
        if let Err(reason) = self
            .verify_planned_ownership_handoff_incarnations(&handoff)
            .await
        {
            self.abort_planned_ownership_handoff(domain, handoff).await;
            return Err(Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: reason.to_string(),
            }));
        }
        if let Err(reason) = self.confirm_planned_ownership_handoff(&handoff).await {
            self.abort_planned_ownership_handoff(domain, handoff).await;
            return Err(Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: reason.to_string(),
            }));
        }
        Ok(Some(handoff))
    }

    async fn capture_ownership_handoff_state(
        &self,
        operation_id: &str,
        domain: &DomainName,
        moved: &PlannedOwnershipMove,
        source_incarnation: ClusterNodeIncarnation,
        base_schedule_fingerprint: [u8; 32],
    ) -> OwnershipHandoffResult<Vec<nervix_interconnect::OwnershipHandoffCheckpoint>> {
        Self::verify_ownership_handoff_node_incarnation(
            &self.live_node_incarnations().await,
            &moved.former_owner,
            source_incarnation,
            "source",
        )?;
        if moved.former_owner == *self.inner.consensus.local_node_id() {
            let scheduled = self
                .prepare_owner_control_request(
                    domain,
                    moved.entity.kind,
                    moved.entity.identifier.clone(),
                )
                .await
                .map_err(OwnershipHandoffError::participant)?;
            if !scheduled.is_primary_on(self.inner.consensus.local_node_id()) {
                return Err(OwnershipHandoffError::participant(format!(
                    "{} '{}' is not owned by source node '{}'",
                    moved.entity.kind.as_str(),
                    moved.entity.identifier.as_str(),
                    moved.former_owner
                )));
            }
            return self
                .inner
                .runtime
                .capture_ownership_handoff_state(domain, &moved.entity, base_schedule_fingerprint)
                .await;
        }
        let response = self
            .inner
            .interconnect
            .request(
                &moved.former_owner,
                RemoteCaptureOwnershipHandoffStateRequest {
                    operation_id: operation_id.to_string(),
                    source: moved.former_owner.clone(),
                    source_incarnation,
                    domain: domain.clone(),
                    entity: moved.entity.clone(),
                    base_schedule_fingerprint,
                },
            )
            .await
            .map_err(|error| OwnershipHandoffError::transport(error.to_string()))?;
        response.map_err(|failure| OwnershipHandoffError::participant(failure.to_string()))
    }

    async fn prepare_ownership_handoff_state(
        &self,
        request: RemotePrepareOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<()> {
        let current_incarnations = self.live_node_incarnations().await;
        Self::verify_ownership_handoff_node_incarnation(
            &current_incarnations,
            &request.source,
            request.source_incarnation,
            "source",
        )?;
        Self::verify_ownership_handoff_node_incarnation(
            &current_incarnations,
            &request.destination,
            request.destination_incarnation,
            "destination",
        )?;
        let scheduled = self
            .scheduled_model_node(
                &request.domain,
                request.entity.kind,
                request.entity.identifier.clone(),
            )
            .await
            .ok_or_else(|| {
                OwnershipHandoffError::schedule(format!(
                    "{} '{}' is absent from the committed schedule",
                    request.entity.kind.as_str(),
                    request.entity.identifier.as_str()
                ))
            })?;
        if scheduled.execution_node() != Some(&request.source) {
            return Err(OwnershipHandoffError::participant(format!(
                "{} '{}' is no longer owned by source node '{}'",
                request.entity.kind.as_str(),
                request.entity.identifier.as_str(),
                request.source
            )));
        }
        if request.destination == *self.inner.consensus.local_node_id() {
            return self
                .inner
                .runtime
                .prepare_ownership_handoff_state(request)
                .await;
        }
        let destination = request.destination.clone();
        let response = self
            .inner
            .interconnect
            .request(&destination, request)
            .await
            .map_err(|error| OwnershipHandoffError::transport(error.to_string()))?;
        response.map_err(|failure| OwnershipHandoffError::participant(failure.to_string()))
    }

    async fn confirm_planned_ownership_handoff(
        &self,
        handoff: &PlannedOwnershipHandoff,
    ) -> OwnershipHandoffResult<()> {
        for moved in &handoff.moves {
            tokio::task::consume_budget().await;
            let request = RemoteConfirmOwnershipHandoffStateRequest {
                operation_id: handoff.operation_id.clone(),
                source: moved.former_owner.clone(),
                destination: moved.destination.clone(),
                source_incarnation: *handoff
                    .node_incarnations
                    .get(&moved.former_owner)
                    .verified("every planned former owner has a bound incarnation"),
                destination_incarnation: *handoff
                    .node_incarnations
                    .get(&moved.destination)
                    .verified("every planned destination has a bound incarnation"),
                domain: handoff.gate.domain.clone(),
                entity: moved.entity.clone(),
                base_schedule_fingerprint: handoff.base_schedule_fingerprint,
                target_schedule_fingerprint: handoff.target_schedule_fingerprint,
            };
            self.confirm_ownership_handoff_on_node(
                &moved.former_owner,
                request.clone(),
                handoff.preparation_deadline,
            )
            .await?;
            tokio::task::consume_budget().await;
            self.confirm_ownership_handoff_on_node(
                &moved.destination,
                request,
                handoff.preparation_deadline,
            )
            .await?;
        }
        Ok(())
    }

    async fn confirm_ownership_handoff_on_node(
        &self,
        node: &ClusterNodeName,
        request: RemoteConfirmOwnershipHandoffStateRequest,
        deadline: tokio::time::Instant,
    ) -> OwnershipHandoffResult<()> {
        let confirmation = async {
            if node == self.inner.consensus.local_node_id() {
                return self.confirm_local_ownership_handoff_state(request).await;
            }
            loop {
                tokio::task::consume_budget().await;
                match self.inner.interconnect.request(node, request.clone()).await {
                    Ok(Ok(())) => return Ok(()),
                    Ok(Err(failure)) => {
                        return Err(OwnershipHandoffError::participant(failure.to_string()));
                    }
                    Err(error) => {
                        debug!(
                            %node,
                            error = %error,
                            "ownership handoff participant confirmation is waiting for interconnect"
                        );
                        sleep(ENTITY_GATE_RELEASE_RETRY_INTERVAL).await;
                    }
                }
            }
        };
        match tokio::time::timeout_at(deadline, confirmation).await {
            Ok(result) => result,
            Err(_) => Err(OwnershipHandoffError::deadline(format!(
                "timed out confirming ownership handoff participant node '{node}'"
            ))),
        }
    }

    async fn confirm_local_ownership_handoff_state(
        &self,
        request: RemoteConfirmOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<()> {
        let local_node = self.inner.consensus.local_node_id();
        if local_node == &request.source {
            Self::verify_ownership_handoff_node_incarnation(
                &self.live_node_incarnations().await,
                local_node,
                request.source_incarnation,
                "source",
            )?;
            let schedule = self.inner.consensus.current_schedule().await;
            let current = schedule.domain(&request.domain).ok_or_else(|| {
                OwnershipHandoffError::schedule(format!(
                    "domain '{}' has no committed schedule while confirming ownership handoff",
                    request.domain.as_str()
                ))
            })?;
            if Runtime::ownership_handoff_schedule_fingerprint(current)?
                != request.base_schedule_fingerprint
            {
                return Err(OwnershipHandoffError::schedule(format!(
                    "domain '{}' changed schedule before ownership handoff publication",
                    request.domain.as_str()
                )));
            }
            let scheduled = current.nodes.get(&request.entity).ok_or_else(|| {
                OwnershipHandoffError::schedule(format!(
                    "{} '{}' is absent from the ownership handoff base schedule",
                    request.entity.kind.as_str(),
                    request.entity.identifier.as_str()
                ))
            })?;
            if !scheduled.is_primary_on(&request.source) {
                return Err(OwnershipHandoffError::participant(format!(
                    "{} '{}' is no longer owned by source node '{}'",
                    request.entity.kind.as_str(),
                    request.entity.identifier.as_str(),
                    request.source
                )));
            }
            let entity = nervix_models::DomainNodeRef::node_in(
                request.domain,
                request.entity.kind,
                request.entity.identifier,
            );
            if !self
                .inner
                .runtime
                .ownership_handoff_entity_is_frozen(&entity)
            {
                return Err(OwnershipHandoffError::participant(format!(
                    "source node '{}' no longer holds the ownership handoff freeze",
                    request.source
                )));
            }
            return Ok(());
        }
        if local_node == &request.destination {
            Self::verify_ownership_handoff_node_incarnation(
                &self.live_node_incarnations().await,
                local_node,
                request.destination_incarnation,
                "destination",
            )?;
            return self
                .inner
                .runtime
                .verify_ownership_handoff_preparation(&request);
        }
        Err(OwnershipHandoffError::participant(format!(
            "ownership handoff confirmation names nodes '{}' and '{}' but reached '{}'",
            request.source, request.destination, local_node
        )))
    }

    async fn discard_ownership_handoff_state(
        &self,
        operation_id: &str,
        domain: &DomainName,
        moves: &[PlannedOwnershipMove],
    ) {
        for moved in moves {
            tokio::task::consume_budget().await;
            if moved.destination == *self.inner.consensus.local_node_id() {
                if let Err(error) = self.inner.runtime.discard_prepared_ownership_handoff_state(
                    operation_id,
                    domain,
                    &moved.entity,
                ) {
                    warn!(
                        domain = domain.as_str(),
                        destination = %moved.destination,
                        error = %error,
                        "failed to discard abandoned ownership handoff state"
                    );
                }
                continue;
            }
            let result = self
                .inner
                .interconnect
                .request(
                    &moved.destination,
                    RemoteDiscardOwnershipHandoffStateRequest {
                        operation_id: operation_id.to_string(),
                        domain: domain.clone(),
                        entity: moved.entity.clone(),
                    },
                )
                .await;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(error) => Some(error.to_string()),
            };
            if let Some(error) = error {
                warn!(
                    domain = domain.as_str(),
                    destination = %moved.destination,
                    error = %error,
                    "failed to discard abandoned ownership handoff state"
                );
            }
        }
    }

    async fn abort_planned_ownership_handoff(
        &self,
        domain: &DomainName,
        handoff: PlannedOwnershipHandoff,
    ) {
        self.discard_ownership_handoff_state(&handoff.operation_id, domain, &handoff.moves)
            .await;
        self.release_cluster_entity_gates(handoff.gate).await;
    }

    async fn activate_planned_ownership_handoff(
        &self,
        domain: &DomainName,
        handoff: &PlannedOwnershipHandoff,
    ) -> OwnershipHandoffResult<()> {
        self.verify_planned_ownership_handoff_incarnations(handoff)
            .await?;
        for moved in &handoff.moves {
            tokio::task::consume_budget().await;
            let source_incarnation = *handoff
                .node_incarnations
                .get(&moved.former_owner)
                .verified("every planned former owner has a bound incarnation");
            let destination_incarnation = *handoff
                .node_incarnations
                .get(&moved.destination)
                .verified("every planned destination has a bound incarnation");
            let request = RemoteActivateOwnershipHandoffStateRequest {
                operation_id: handoff.operation_id.clone(),
                source: moved.former_owner.clone(),
                destination: moved.destination.clone(),
                source_incarnation,
                destination_incarnation,
                domain: domain.clone(),
                entity: moved.entity.clone(),
                base_schedule_fingerprint: handoff.base_schedule_fingerprint,
                target_schedule_fingerprint: handoff.target_schedule_fingerprint,
            };
            let activation = async {
                if request.destination == *self.inner.consensus.local_node_id() {
                    return self
                        .inner
                        .runtime
                        .verify_ownership_handoff_activation(&request);
                }
                let destination = request.destination.clone();
                let response = self
                    .inner
                    .interconnect
                    .request(&destination, request)
                    .await
                    .map_err(|error| OwnershipHandoffError::transport(error.to_string()))?;
                response.map_err(|failure| OwnershipHandoffError::participant(failure.to_string()))
            };
            match tokio::time::timeout_at(handoff.activation_deadline, activation).await {
                Ok(Ok(())) => {}
                Ok(Err(reason)) => return Err(reason),
                Err(_) => {
                    return Err(OwnershipHandoffError::deadline(format!(
                        "timed out waiting for {} '{}' to activate on node '{}'",
                        moved.entity.kind.as_str(),
                        moved.entity.identifier.as_str(),
                        moved.destination
                    )));
                }
            }
        }
        Ok(())
    }

    async fn finish_planned_ownership_handoff(
        &self,
        domain: &DomainName,
        handoff: PlannedOwnershipHandoff,
    ) -> OwnershipHandoffResult<()> {
        let activation = loop {
            tokio::task::consume_budget().await;
            match self
                .activate_planned_ownership_handoff(domain, &handoff)
                .await
            {
                Ok(()) => break Ok(()),
                Err(error) if tokio::time::Instant::now() < handoff.activation_deadline => {
                    debug!(
                        domain = domain.as_str(),
                        error = %error,
                        "planned ownership handoff activation will retry"
                    );
                    sleep(ENTITY_GATE_RELEASE_RETRY_INTERVAL).await;
                }
                Err(error) => break Err(error),
            }
        };
        if let Err(error) = activation {
            let hold_duration = handoff.started_at.elapsed();
            for moved in &handoff.moves {
                warn!(
                    domain = domain.as_str(),
                    kind = moved.entity.kind.as_ref(),
                    name = moved.entity.identifier.as_str(),
                    former_owner = %moved.former_owner,
                    destination = %moved.destination,
                    hold_duration_millis = hold_duration.as_millis(),
                    promoted_replica = moved.promoted_replica,
                    error = %error,
                    "planned ownership handoff destination did not confirm activation"
                );
            }
            handoff.gate.defer_release_to_lease_deadline();
            return Err(error);
        }
        let hold_duration = handoff.started_at.elapsed();
        for moved in &handoff.moves {
            info!(
                domain = domain.as_str(),
                kind = moved.entity.kind.as_ref(),
                name = moved.entity.identifier.as_str(),
                former_owner = %moved.former_owner,
                destination = %moved.destination,
                hold_duration_millis = hold_duration.as_millis(),
                promoted_replica = moved.promoted_replica,
                "planned ownership handoff completed"
            );
            if !moved.promoted_replica {
                warn!(
                    domain = domain.as_str(),
                    kind = moved.entity.kind.as_ref(),
                    name = moved.entity.identifier.as_str(),
                    former_owner = %moved.former_owner,
                    destination = %moved.destination,
                    "planned ownership handoff moved a runtime node without replicated state"
                );
            }
        }
        self.discard_ownership_handoff_state(&handoff.operation_id, domain, &handoff.moves)
            .await;
        self.release_cluster_entity_gates(handoff.gate).await;
        Ok(())
    }

    fn defer_planned_ownership_handoff_release(
        &self,
        domain: &DomainName,
        handoff: PlannedOwnershipHandoff,
        error: &crate::runtime::RuntimeError,
    ) {
        let hold_duration = handoff.started_at.elapsed();
        for moved in &handoff.moves {
            warn!(
                domain = domain.as_str(),
                kind = moved.entity.kind.as_ref(),
                name = moved.entity.identifier.as_str(),
                former_owner = %moved.former_owner,
                destination = %moved.destination,
                hold_duration_millis = hold_duration.as_millis(),
                promoted_replica = moved.promoted_replica,
                error = %error,
                "planned ownership handoff activation failed; gate remains held until its deadline"
            );
        }
        handoff.gate.defer_release_to_lease_deadline();
    }

    async fn wait_for_cluster_entity_drain(
        &self,
        gate: &ClusterEntityGate,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
        required_live_nodes: &[ClusterNodeName],
        deadline: tokio::time::Instant,
    ) -> Result<(), Report<DomainAlterError>> {
        let domain = &gate.domain;
        let mut polling = interval(Duration::from_millis(25));
        polling.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_status = None::<(ClusterNodeName, EntityDrainStatusEnvelope)>;
        loop {
            tokio::task::consume_budget().await;
            polling.tick().await;
            if !required_live_nodes.is_empty() {
                let live_nodes = self
                    .available_node_ids()
                    .await
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                if let Some(node) = required_live_nodes
                    .iter()
                    .find(|node| !live_nodes.contains(*node))
                {
                    return Err(Report::new(DomainAlterError::EntityGate {
                        domain: domain.clone(),
                        operation: purpose.operation_name(),
                        reason: format!(
                            "former owner '{node}' became unavailable during ownership handoff"
                        ),
                    }));
                }
            }
            let mut all_drained = true;
            for node in &gate.nodes {
                tokio::task::consume_budget().await;
                match self
                    .entity_drain_status_on_node(
                        node,
                        domain,
                        relays,
                        affected_entities,
                        purpose,
                        deadline,
                    )
                    .await
                {
                    Ok(status)
                        if status.buffered_relay_batches == 0
                            && status.node_work_items == 0
                            && status.outstanding_acks == 0 => {}
                    Ok(status) => {
                        all_drained = false;
                        last_status = Some((node.clone(), status));
                    }
                    Err(error) => {
                        if required_live_nodes.iter().any(|required| required == node) {
                            return Err(Report::new(DomainAlterError::EntityGate {
                                domain: domain.clone(),
                                operation: purpose.operation_name(),
                                reason: format!(
                                    "former owner '{node}' became unavailable during ownership \
                                     handoff: {error}"
                                ),
                            }));
                        }
                        all_drained = false;
                        if tokio::time::Instant::now() >= deadline {
                            warn!(
                                domain = domain.as_str(),
                                %node, error, "failed to retrieve entity drain status"
                            );
                        }
                    }
                }
            }
            if all_drained {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                let Some((pending_node, last_status)) = last_status.or_else(|| {
                    // A gate holding no nodes has nothing left to drain, so there is no pending
                    // node to name and no timeout to report.
                    Some((
                        gate.nodes.first().cloned()?,
                        EntityDrainStatusEnvelope {
                            buffered_relay_batches: 0,
                            node_work_items: 0,
                            outstanding_acks: 0,
                            emitter_publishing: Vec::new(),
                        },
                    ))
                }) else {
                    return Ok(());
                };
                return Err(Report::new(DomainAlterError::EntityQuiesceTimeout {
                    domain: domain.clone(),
                    operation: purpose.operation_name(),
                    pending_node,
                    buffered_relay_batches: last_status.buffered_relay_batches.arch_into(),
                    node_work_items: last_status.node_work_items.arch_into(),
                    outstanding_acks: last_status.outstanding_acks.arch_into(),
                    emitter_publishing: emitter_publishing_drain_summary(
                        &last_status.emitter_publishing,
                    ),
                }));
            }
        }
    }

    async fn release_cluster_entity_gates(&self, mut gate: ClusterEntityGate) {
        let domain = gate.domain.clone();
        for node in gate.nodes.clone() {
            tokio::task::consume_budget().await;
            match self
                .release_entity_gate_on_node(&node, gate.operation_id, &domain)
                .await
            {
                Ok(()) => gate.mark_released(&node),
                Err(error) => {
                    warn!(
                        domain = domain.as_str(),
                        %node, error, "failed to release entity gates; scheduling retry"
                    );
                    self.broadcast_error(format!(
                        "failed to release entity gates on node '{node}' in domain '{}': {error}; \
                         release will retry in the background",
                        domain.as_str()
                    ));
                }
            }
        }
        gate.schedule_remaining_releases();
    }

    async fn pause_and_drain_domain_for_alter(
        &self,
        domain: &DomainName,
    ) -> Result<(), Report<DomainAlterError>> {
        self.inner
            .consensus
            .pause_domain(domain.clone())
            .await
            .map_err(|error| {
                let reason = error.to_string();
                Report::new(error).change_context(DomainAlterError::PauseDomain {
                    domain: domain.clone(),
                    reason,
                })
            })?;

        if let Err(error) = self.apply_current_cluster_state().await {
            return Err(self
                .abort_domain_alter_pause(
                    domain,
                    Report::new(DomainAlterError::StopIngestion {
                        domain: domain.clone(),
                        reason: error.to_string(),
                    }),
                )
                .await);
        }

        match self.wait_for_paused_domain_drain(domain).await {
            Ok(()) => Ok(()),
            Err(reason) => Err(self.abort_domain_alter_pause(domain, reason).await),
        }
    }

    async fn wait_for_paused_domain_drain(
        &self,
        domain: &DomainName,
    ) -> Result<(), Report<DomainAlterError>> {
        let mut nodes = self.inner.cluster.live_node_ids().await;
        if !nodes
            .iter()
            .any(|node| node == self.inner.consensus.local_node_id())
        {
            nodes.push(self.inner.consensus.local_node_id().clone());
        }
        nodes.sort();
        nodes.dedup();

        let deadline = tokio::time::Instant::now() + self.inner.runtime.domain_drain_timeout();
        let mut polling = interval(Duration::from_millis(50));
        polling.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_pending = None::<(ClusterNodeName, DomainDrainStatusEnvelope)>;
        let mut last_status_error = None;

        loop {
            tokio::task::consume_budget().await;
            polling.tick().await;
            let mut all_drained = true;
            for node in &nodes {
                tokio::task::consume_budget().await;
                match self.domain_drain_status_on_node(node, domain).await {
                    Ok(status)
                        if status.active_ingestors == 0
                            && status.active_generators == 0
                            && status.outstanding_acks == 0
                            && status.buffered_emitter_messages == 0 => {}
                    Ok(status) => {
                        all_drained = false;
                        last_pending = Some((node.clone(), status));
                    }
                    Err(error) => {
                        all_drained = false;
                        last_status_error = Some(error);
                        if tokio::time::Instant::now() >= deadline {
                            break;
                        }
                    }
                }
            }
            if all_drained {
                return Ok(());
            }
            if tokio::time::Instant::now() < deadline {
                continue;
            }

            let outstanding = if let Some((node, status)) = last_pending {
                DrainOutstanding {
                    domain: domain.clone(),
                    node: Some(node),
                    active_ingestors: status.active_ingestors,
                    active_generators: status.active_generators,
                    outstanding_acks: status.outstanding_acks,
                    buffered_emitter_messages: status.buffered_emitter_messages,
                    emitter_publishing: status.emitter_publishing,
                    status_error: last_status_error,
                }
            } else {
                DrainOutstanding {
                    domain: domain.clone(),
                    node: None,
                    active_ingestors: 0,
                    active_generators: 0,
                    outstanding_acks: 0,
                    buffered_emitter_messages: 0,
                    emitter_publishing: Vec::new(),
                    status_error: last_status_error,
                }
            };
            return Err(Report::new(DomainAlterError::QuiesceTimeout {
                outstanding,
            }));
        }
    }

    async fn abort_domain_alter_pause(
        &self,
        domain: &DomainName,
        reason: Report<DomainAlterError>,
    ) -> Report<DomainAlterError> {
        match self.resume_domain_after_alter(domain).await {
            Ok(()) => reason,
            Err(resume_error) => {
                let reason = format!("{reason}; automatic resume failed: {resume_error}");
                resume_error.change_context(DomainAlterError::Rollback {
                    domain: domain.clone(),
                    reason,
                })
            }
        }
    }

    async fn resume_domain_after_alter(
        &self,
        domain: &DomainName,
    ) -> Result<(), Report<DomainAlterError>> {
        self.inner
            .consensus
            .resume_domain(domain.clone())
            .await
            .map_err(|error| {
                let reason = error.to_string();
                Report::new(error).change_context(DomainAlterError::ResumeDomain {
                    domain: domain.clone(),
                    reason,
                })
            })?;
        self.apply_current_cluster_state().await.map_err(|error| {
            Report::new(DomainAlterError::RestoreIngestion {
                domain: domain.clone(),
                reason: error.to_string(),
            })
        })
    }

    /// Stops a domain whose start could not be completed, and says so when the stop fails too.
    ///
    /// The caller is on its way to returning the start failure, and this rollback is what keeps
    /// the cluster from holding a domain the operator was told did not start. A rollback that
    /// fails leaves exactly that state, so the reason is appended to the caller's message rather
    /// than dropped: nothing else in the command's answer would mention it.
    async fn roll_back_started_domain(&self, domain_id: &DomainName) -> String {
        let mut failures = Vec::new();
        if let Err(error) = self.inner.consensus.stop_domain(domain_id.clone()).await {
            failures.push(format!("stopping it again failed: {error}"));
        }
        if let Err(error) = self.apply_current_cluster_state().await {
            failures.push(format!("reapplying the cluster state failed: {error}"));
        }
        if failures.is_empty() {
            return String::new();
        }
        format!(
            "; the domain may still be running because {}",
            failures.join(" and ")
        )
    }

    /// Restores the pre-alteration models and schedule after a committed batch failed to reach the
    /// cluster. Every quiesce level needs the restore, because the registry commit already landed;
    /// only a domain-paused alteration additionally has to resume the domain.
    async fn rollback_model_alteration(
        &self,
        domain: &DomainName,
        planned: crate::registry::PlannedMutations,
        classified_level: QuiesceLevel,
    ) -> Result<(), Report<DomainAlterError>> {
        let runtime_changes = self
            .inner
            .registry
            .rollback_committed(planned)
            .map_err(|error| {
                Report::new(DomainAlterError::Rollback {
                    domain: domain.clone(),
                    reason: format!("registry rollback failed: {error}"),
                })
            })?;
        self.publish_domain_schedule(domain, runtime_changes.graph)
            .await
            .map_err(|error| {
                Report::new(DomainAlterError::Rollback {
                    domain: domain.clone(),
                    reason: format!("old schedule restore failed: {error}"),
                })
            })?;
        if classified_level.requires_domain_pause() {
            self.resume_domain_after_alter(domain).await
        } else {
            Ok(())
        }
    }

    async fn describe_metrics_for_scheduled_node(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        scheduled_node: Option<&ScheduledNode>,
    ) -> Result<Vec<String>, String> {
        let identifier = identifier.into();
        self.describe_runtime_for_scheduled_node(domain, kind, identifier, scheduled_node)
            .await
            .map(|details| details.metrics)
    }

    async fn describe_runtime_for_scheduled_node(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        scheduled_node: Option<&ScheduledNode>,
    ) -> Result<RemoteDescribeMetricsEnvelope, String> {
        let identifier = identifier.into();
        let metric_kind = kind.as_str().to_ascii_uppercase();
        let Some(node) = scheduled_node else {
            return Ok(self.local_runtime_describe(domain, kind, identifier, &metric_kind));
        };
        let local_node_id = self.inner.consensus.local_node_id();
        if node.executes_on(local_node_id) {
            return Ok(self.local_runtime_describe(domain, kind, identifier, &metric_kind));
        }
        let Some(owner) = node.execution_node() else {
            return Ok(self.local_runtime_describe(domain, kind, identifier, &metric_kind));
        };

        self.inner
            .interconnect
            .request(
                owner,
                RemoteDescribeMetricsRequest {
                    domain: domain.clone(),
                    kind,
                    name: identifier.clone(),
                },
            )
            .await
            .map_err(|error| error.to_string())?
            .result
    }

    fn local_runtime_describe(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        metric_kind: &str,
    ) -> RemoteDescribeMetricsEnvelope {
        let identifier = identifier.into();
        let state = if let ModelKind::WasmProcessor = kind {
            self.inner
                .runtime
                .describe_wasm_processor_state_for(domain, identifier.clone())
        } else {
            Vec::new()
        };
        RemoteDescribeMetricsEnvelope {
            metrics: self
                .inner
                .runtime
                .describe_metrics_for(domain, metric_kind, identifier),
            state,
        }
    }

    async fn handle_describe_metrics_request(
        &self,
        request: RemoteDescribeMetricsRequest,
    ) -> Result<RemoteDescribeMetricsEnvelope, String> {
        self.prepare_owner_control_request(&request.domain, request.kind, &request.name)
            .await?;
        let metric_kind = request.kind.as_str().to_ascii_uppercase();
        Ok(self.local_runtime_describe(&request.domain, request.kind, &request.name, &metric_kind))
    }

    async fn describe_lookup(
        &self,
        domain: &DomainName,
        describe: DescribeLookup,
    ) -> CommandResult {
        let lookup_target = match self
            .lookup_target_from_schedule(domain, &describe.name)
            .await
        {
            Ok(target) => target,
            Err(message) => return command_error(message),
        };
        let Some(LookupTarget {
            lookup,
            node: lookup_node,
            ..
        }) = lookup_target
        else {
            return command_error(format!(
                "hash map '{}' does not exist in domain '{}'",
                describe.name.as_str(),
                domain.as_str()
            ));
        };

        let local_node_id = self.inner.consensus.local_node_id();
        let summary = if lookup_node.executes_on(local_node_id) {
            match self
                .inner
                .runtime
                .describe_local_lookup(domain, &describe.name)
            {
                Ok(description) => Ok(LookupDescribeEnvelope {
                    resource: lookup.resource.clone(),
                    resource_version: description.resource_version,
                    path: lookup.path.clone(),
                    decode_using_codec: lookup.decode_using_codec.clone(),
                    key_field: lookup.key_field.clone(),
                    entry_count: description.entry_count.arch_into(),
                }),
                Err(message) => Err(message),
            }
        } else if let Some(owner) = lookup_node.execution_node() {
            match self
                .inner
                .interconnect
                .request(
                    owner,
                    RemoteDescribeLookupRequest {
                        domain: domain.clone(),
                        name: describe.name.clone(),
                    },
                )
                .await
            {
                Ok(response) => response.result,
                Err(error) => Err(error.to_string()),
            }
        } else {
            Err(format!(
                "hash map '{}' in domain '{}' has no execution node",
                describe.name.as_str(),
                domain.as_str()
            ))
        };

        match summary {
            Ok(summary) => {
                let metrics = match self
                    .describe_metrics_for_scheduled_node(
                        domain,
                        ModelKind::Lookup,
                        &describe.name,
                        Some(&lookup_node),
                    )
                    .await
                {
                    Ok(metrics) => metrics,
                    Err(message) => return command_error(message),
                };
                command_ok(append_metrics_lines(
                    format_lookup_describe_output(&describe.name, &lookup_node, &summary),
                    metrics,
                ))
            }
            Err(message) => command_error(message),
        }
    }

    async fn handle_describe_lookup_request(
        &self,
        request: RemoteDescribeLookupRequest,
    ) -> Result<LookupDescribeEnvelope, String> {
        self.prepare_owner_control_request(&request.domain, ModelKind::Lookup, &request.name)
            .await?;
        let description = self
            .inner
            .runtime
            .describe_local_lookup(&request.domain, &request.name)?;
        Ok(LookupDescribeEnvelope {
            resource: description.model.resource,
            resource_version: description.resource_version,
            path: description.model.path,
            decode_using_codec: description.model.decode_using_codec,
            key_field: description.model.key_field,
            entry_count: description.entry_count.arch_into(),
        })
    }

    async fn describe_deduplicator(
        &self,
        domain: &DomainName,
        describe: DescribeDeduplicator,
    ) -> CommandResult {
        let DescribedModel {
            config: deduplicator,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateDeduplicator>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "deduplicator '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read deduplicator '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Deduplicator,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_deduplicator_describe_output(
                &describe.name,
                &deduplicator,
                scheduled_node.as_ref(),
            ),
            metrics,
        ))
    }

    async fn describe_junction(
        &self,
        domain: &DomainName,
        describe: DescribeJunction,
    ) -> CommandResult {
        let DescribedModel {
            config: junction,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateJunction>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "junction '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read junction '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Junction,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_junction_describe_output(&describe.name, &junction, scheduled_node.as_ref()),
            metrics,
        ))
    }

    async fn describe_reingestor(
        &self,
        domain: &DomainName,
        describe: DescribeReingestor,
    ) -> CommandResult {
        let DescribedModel {
            config: reingestor,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateReingestor>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "reingestor '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read reingestor '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Reingestor,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_reingestor_describe_output(&describe.name, &reingestor, scheduled_node.as_ref()),
            metrics,
        ))
    }

    async fn describe_correlator(
        &self,
        domain: &DomainName,
        describe: DescribeCorrelator,
    ) -> CommandResult {
        let DescribedModel {
            config: correlator,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateCorrelator>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "correlator '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read correlator '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Correlator,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_correlator_describe_output(&describe.name, &correlator, scheduled_node.as_ref()),
            metrics,
        ))
    }

    async fn describe_reorderer(
        &self,
        domain: &DomainName,
        describe: DescribeReorderer,
    ) -> CommandResult {
        let DescribedModel {
            config: reorderer,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateReorderer>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "reorderer '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read reorderer '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Reorderer,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_reorderer_describe_output(&describe.name, &reorderer, scheduled_node.as_ref()),
            metrics,
        ))
    }

    async fn describe_emitter(
        &self,
        domain: &DomainName,
        describe: DescribeEmitter,
    ) -> CommandResult {
        let DescribedModel {
            config: emitter,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateEmitter>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "emitter '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read emitter '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let status = self
            .dataflow_node_status_envelope_for_graph(
                domain,
                ModelKind::Emitter.as_str(),
                &describe.name,
            )
            .await;

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Emitter,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_emitter_describe_output(
                &describe.name,
                &emitter,
                scheduled_node.as_ref(),
                Some(&status),
            ),
            metrics,
        ))
    }

    async fn describe_window_processor(
        &self,
        domain: &DomainName,
        describe: DescribeWindowProcessor,
    ) -> CommandResult {
        let processor = match self
            .inner
            .registry
            .get::<CreateWindowProcessor>(domain, &describe.name)
        {
            Ok(Some(processor)) => processor,
            Ok(None) => {
                return command_error(format!(
                    "window processor '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read window processor '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let aggregate = match processor
            .output_routes
            .routes
            .iter()
            .map(|output| {
                lower_window_assignments(&output.construction).map(|program| program.inner)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(programs) if !programs.is_empty() => {
                WindowAggregateProgram::combine_route_programs(&programs)
            }
            Ok(_) => {
                return command_error("window processor has no output routes".to_string());
            }
            Err(error) => {
                return command_error(format!(
                    "failed to lower aggregate outputs for window processor '{}': {error}",
                    describe.name.as_str()
                ));
            }
        };

        let scheduled_node = self
            .scheduled_model_node(domain, ModelKind::WindowProcessor, &describe.name)
            .await;

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::WindowProcessor,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_window_processor_describe_output(
                &describe.name,
                &processor,
                &aggregate,
                scheduled_node.as_ref(),
            ),
            metrics,
        ))
    }

    async fn describe_wasm_processor(
        &self,
        domain: &DomainName,
        describe: DescribeWasmProcessor,
    ) -> CommandResult {
        let processor = match self
            .inner
            .registry
            .get::<CreateWasmProcessor>(domain, &describe.name)
        {
            Ok(Some(processor)) => processor,
            Ok(None) => {
                return command_error(format!(
                    "wasm processor '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read wasm processor '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let scheduled_node = self
            .scheduled_model_node(domain, ModelKind::WasmProcessor, &describe.name)
            .await;

        let runtime_details = match self
            .describe_runtime_for_scheduled_node(
                domain,
                ModelKind::WasmProcessor,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_wasm_processor_describe_output(
                &describe.name,
                &processor,
                scheduled_node.as_ref(),
                runtime_details.state,
            ),
            runtime_details.metrics,
        ))
    }

    /// The `M` named `identifier` in `domain`, with the schedule entry that places it.
    ///
    /// `DESCRIBE` answers on any node, and a node that has not stored the domain's models still
    /// holds the schedule it was given, so the schedule is the second source for the same
    /// configuration. Both sources are keyed by `M`'s kind and hand back an `M`, so a description
    /// either has the model or does not.
    async fn described_model<M: UniquelyKindedModel>(
        &self,
        domain: &DomainName,
        identifier: impl Into<ModelName>,
    ) -> Result<Option<DescribedModel<M>>, Report<RegistryError>> {
        let identifier = identifier.into();
        let scheduled = self
            .scheduled_model_node(domain, M::KIND, identifier.clone())
            .await;
        if let Some(config) = self.inner.registry.get::<M>(domain, identifier)? {
            return Ok(Some(DescribedModel { config, scheduled }));
        }
        let Some(scheduled) = scheduled else {
            return Ok(None);
        };
        let config = M::from_model((*scheduled.config).clone()).assured(
            "a schedule keys every entry by the kind of the configuration it carries, and this \
             entry was resolved under this model's kind",
        );
        Ok(Some(DescribedModel {
            config,
            scheduled: Some(scheduled),
        }))
    }

    async fn scheduled_model_node(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Option<ScheduledNode> {
        let identifier = identifier.into();
        let schedule = self.inner.consensus.current_schedule().await;
        let domain_schedule = schedule.domain(domain)?;
        domain_schedule
            .nodes
            .get(&NodeRef::new(kind, identifier))
            .cloned()
    }

    async fn prepare_owner_control_request(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Result<ScheduledNode, String> {
        let identifier = identifier.into();
        self.prepare_control_request_domain(domain).await?;
        let node = self
            .scheduled_model_node(domain, kind, identifier.clone())
            .await
            .ok_or_else(|| {
                format!(
                    "{} '{}' does not exist in domain '{}'",
                    kind.as_str().to_ascii_lowercase(),
                    identifier.as_str(),
                    domain.as_str()
                )
            })?;
        let local_node_id = self.inner.consensus.local_node_id();
        if !node.executes_on(local_node_id) {
            let owner = match node.execution_node() {
                Some(owner) => owner.as_str(),
                None => "-",
            };
            return Err(format!(
                "{} '{}' in domain '{}' is owned by '{owner}' but request reached '{}'",
                kind.as_str().to_ascii_lowercase(),
                identifier.as_str(),
                domain.as_str(),
                local_node_id
            ));
        }
        Ok(node)
    }

    async fn prepare_assigned_control_request(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Result<ScheduledNode, String> {
        let identifier = identifier.into();
        self.prepare_control_request_domain(domain).await?;
        let node = self
            .scheduled_model_node(domain, kind, identifier.clone())
            .await
            .ok_or_else(|| {
                format!(
                    "{} '{}' does not exist in domain '{}'",
                    kind.as_str().to_ascii_lowercase(),
                    identifier.as_str(),
                    domain.as_str()
                )
            })?;
        let local_node_id = self.inner.consensus.local_node_id();
        if !node.is_assigned_to(local_node_id) {
            return Err(format!(
                "{} '{}' in domain '{}' is not assigned to '{}'",
                kind.as_str().to_ascii_lowercase(),
                identifier.as_str(),
                domain.as_str(),
                local_node_id
            ));
        }
        Ok(node)
    }

    async fn prepare_stream_owner_control_request(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), String> {
        self.prepare_control_request_domain(domain).await?;
        let owner_nodes = self.scheduled_stream_owner_nodes(domain, relay).await?;
        let local_node_id = self.inner.consensus.local_node_id();
        if !owner_nodes.iter().any(|owner| owner == local_node_id) {
            return Err(format!(
                "stream '{}' in domain '{}' is not owned by '{}'",
                relay.as_str(),
                domain.as_str(),
                local_node_id
            ));
        }
        Ok(())
    }

    async fn prepare_control_request_domain(&self, domain: &DomainName) -> Result<(), String> {
        if self.inner.consensus.current_domain(domain).await.is_none() {
            return Err(format!("domain '{}' does not exist", domain.as_str()));
        }
        self.reconcile_running_domain_runtime(domain).await
    }

    async fn lookup_query(&self, domain: &DomainName, query: LookupQuery) -> CommandResult {
        let lookup_target = match self.lookup_target_from_schedule(domain, &query.name).await {
            Ok(target) => target,
            Err(message) => return command_error(message),
        };
        let Some(LookupTarget {
            lookup,
            node: lookup_node,
            key_ty,
        }) = lookup_target
        else {
            return command_error(format!(
                "hash map '{}' does not exist in domain '{}'",
                query.name.as_str(),
                domain.as_str()
            ));
        };

        let parsed = match parse_subscription_literal(&lookup.key_field, &key_ty, &query.key) {
            Ok(value) => value,
            Err(message) => return command_error(message),
        };
        let key = parsed.to_key_fragment();
        let local_node_id = self.inner.consensus.local_node_id();
        let local_record = if lookup_node.is_assigned_to(local_node_id) {
            Some(
                self.inner
                    .runtime
                    .query_local_lookup(domain, &query.name, &key),
            )
        } else {
            None
        };
        let record = match local_record {
            Some(Ok(record)) => Ok(record),
            Some(Err(local_error)) => {
                let mut targets = Vec::new();
                if let Some(owner) = lookup_node.execution_node()
                    && owner != local_node_id
                {
                    targets.push(owner.clone());
                }
                for assigned in &lookup_node.assigned_nodes {
                    if assigned != local_node_id && !targets.contains(assigned) {
                        targets.push(assigned.clone());
                    }
                }
                if targets.is_empty() {
                    Err(local_error)
                } else {
                    self.lookup_query_remote_candidates(domain, &query.name, &key, targets)
                        .await
                }
            }
            None => {
                let mut targets = Vec::new();
                if let Some(owner) = lookup_node.execution_node() {
                    targets.push(owner.clone());
                }
                for assigned in &lookup_node.assigned_nodes {
                    if !targets.contains(assigned) {
                        targets.push(assigned.clone());
                    }
                }
                if targets.is_empty() {
                    Err(format!(
                        "hash map '{}' in domain '{}' has no execution node",
                        query.name.as_str(),
                        domain.as_str()
                    ))
                } else {
                    self.lookup_query_remote_candidates(domain, &query.name, &key, targets)
                        .await
                }
            }
        };

        match record {
            Ok(Some(record)) => match record.row_to_json_string(0) {
                Ok(json) => command_ok(json),
                Err(message) => command_error(message),
            },
            Ok(None) => command_error(format!(
                "hash map '{}' has no entry for key {}",
                query.name.as_str(),
                render_subscription_literal(&query.key)
            )),
            Err(message) => command_error(message),
        }
    }

    async fn lookup_query_remote_candidates(
        &self,
        domain: &DomainName,
        name: impl Into<ModelName>,
        key: &str,
        targets: Vec<ClusterNodeName>,
    ) -> Result<Option<runtime_schema::RuntimeRecordBatch>, String> {
        let name = name.into();
        let mut errors = Vec::new();
        for target in targets {
            let response = self
                .inner
                .interconnect
                .request(
                    &target,
                    RemoteLookupRequest {
                        domain: domain.clone(),
                        name: LookupName::from(&name),
                        key: key.to_string(),
                    },
                )
                .await;
            match response {
                Ok(RemoteLookupResponse { result }) => match result {
                    Ok(None) => return Ok(None),
                    Ok(Some(bytes)) => return self.decode_lookup_record(bytes).await.map(Some),
                    Err(message) => errors.push(message),
                },
                Err(error) => errors.push(error.to_string()),
            }
        }
        Err(errors
            .into_iter()
            .next()
            .unwrap_or_else(|| "lookup has no remote execution node".to_string()))
    }

    async fn handle_lookup_request(
        &self,
        request: RemoteLookupRequest,
    ) -> Result<Option<runtime_schema::RuntimeRecordBatch>, String> {
        self.prepare_assigned_control_request(&request.domain, ModelKind::Lookup, &request.name)
            .await?;
        self.inner
            .runtime
            .query_local_lookup(&request.domain, &request.name, &request.key)
    }

    /// Decode one remote lookup answer through the node's admission, so a large answer is charged
    /// and runs off the async workers like every other body.
    async fn decode_lookup_record(
        &self,
        bytes: Vec<u8>,
    ) -> Result<runtime_schema::RuntimeRecordBatch, String> {
        let executor = self.inner.runtime.executor();
        let body = executor
            .charge_owned(MemoryClass::Commands, bytes)
            .await
            .map_err(|error| error.to_string())?;
        runtime_schema::RuntimeRecordBatch::decode_arrow_ipc(executor, body)
            .await
            .map_err(|error| error.to_string())
    }

    /// The configuration this session's bound transaction has queued for `domain`. Queued
    /// configuration follows the binding, so a session that holds no transaction, one displaced by
    /// a takeover, one whose transaction this node does not hold, and one whose transaction
    /// configures another domain all fall back to committed configuration alone.
    async fn queued_configuration(
        &self,
        subscriptions: &SessionSubscriptions,
        domain: Option<&DomainName>,
    ) -> QueuedConfiguration {
        let (Some(domain), Some(id)) = (domain, subscriptions.transaction_id()) else {
            return QueuedConfiguration::default();
        };
        if self
            .validate_session_transaction_binding(subscriptions)
            .is_err()
        {
            return QueuedConfiguration::default();
        }
        let Some(transaction) = self.inner.consensus.current_transaction(id).await else {
            return QueuedConfiguration::default();
        };
        if &transaction.domain != domain {
            return QueuedConfiguration::default();
        }

        let mut queued = QueuedConfiguration::default();
        for statement in transaction
            .statements
            .iter()
            .map(|queued_statement| &queued_statement.statement)
        {
            if statement.is_model_mutation() {
                queued
                    .models
                    .push(Self::transaction_registry_mutation(statement));
            } else if let Statement::CreateResource(create) = statement {
                queued.resources.insert(create.identifier.clone());
            }
        }
        queued
    }

    async fn process_suggest(
        &self,
        req: SuggestRequest,
        subscriptions: &SessionSubscriptions,
    ) -> SuggestResponse {
        let cursor = req.cursor.arch_into();
        let domain = parse_request_domain(&req.domain).ok();
        let queued = self
            .queued_configuration(subscriptions, domain.as_ref())
            .await;

        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
        } = completion_context(&req.input, cursor);
        let grammar = suggest_client_statement(&grammar_input, grammar_cursor);

        let mut suggestions = Vec::new();
        let mut semantic_kinds = Vec::new();
        let mut expects_resource_ref = false;
        let mut expects_session_subscription_ref = false;
        let mut expects_runtime_node_ref = false;
        let requested_resource_versions = requested_resource_versions(&req.input, cursor);
        for item in &grammar {
            if let Some(kind) = ModelKind::from_completion_label(item) {
                semantic_kinds.push(kind);
            } else if item == "ref:resource" {
                expects_resource_ref = true;
            } else if item == "ref:session_subscription" {
                expects_session_subscription_ref = true;
            } else if item == "ref:runtime_node" {
                expects_runtime_node_ref = true;
            } else if prefix.is_empty()
                || item
                    .to_ascii_lowercase()
                    .starts_with(&prefix.to_ascii_lowercase())
            {
                suggestions.push(item.clone());
            }
        }

        for kind in &semantic_kinds {
            if let Some(domain) = &domain
                && self.inner.consensus.current_domain(domain).await.is_some()
                && let Ok(ids) = self.inner.registry.resulting_identifiers(
                    domain,
                    *kind,
                    &prefix,
                    &queued.models,
                )
            {
                suggestions.extend(ids.into_iter().map(|id| id.to_string()));
            }
        }

        if expects_session_subscription_ref {
            suggestions.extend(subscriptions.matching_names(&prefix));
        }

        if expects_runtime_node_ref
            && let Some(domain) = &domain
            && self.inner.consensus.current_domain(domain).await.is_some()
        {
            suggestions.extend(placement_runtime_node_ref_suggestions(
                &self.inner.registry,
                domain,
                &prefix,
                &queued.models,
            ));
        }

        if let Some(domain) = &domain
            && (expects_resource_ref || requested_resource_versions.is_some())
        {
            let resources = self.inner.consensus.current_resources().await;
            if expects_resource_ref {
                suggestions.extend(resource_ref_suggestions(&resources, domain, &prefix));
                suggestions.extend(queued.resource_suggestions(&prefix));
            }
            if let Some(resource_identifier) = requested_resource_versions.as_ref() {
                suggestions.extend(resource_version_suggestions(
                    &resources,
                    domain,
                    resource_identifier,
                    &prefix,
                ));
            }
        }

        if grammar_input.contains("DOMAIN")
            || (semantic_kinds.is_empty()
                && !expects_resource_ref
                && !expects_session_subscription_ref
                && !expects_runtime_node_ref)
        {
            let domains = self.inner.consensus.current_domains().await;
            for id in domains.into_keys() {
                if prefix.is_empty() || id.as_str().starts_with(&prefix) {
                    suggestions.push(id.to_string());
                }
            }
        }

        let mut response_suggestions = SortedSet::from_unsorted(suggestions)
            .into_vec()
            .into_iter()
            .map(|value| ApiSuggestion {
                value,
                kind: i32::from(SuggestionKind::Text),
            })
            .collect::<Vec<_>>();

        if let Some(fragment) = upload_resource_path_fragment(&req.input, cursor) {
            response_suggestions.push(ApiSuggestion {
                value: fragment.to_string(),
                kind: i32::from(SuggestionKind::LocalDirectoryLookup),
            });
        }

        SuggestResponse {
            suggestions: response_suggestions,
        }
    }

    async fn process_command(
        &self,
        req: CommandRequest,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let client_statements = match parse_client_statement_sources(&req.query) {
            Ok(statements) => statements,
            Err(ParseFromSourceError::Lex { diagnostics, .. }) => {
                return self
                    .command_with_transaction_status(
                        error_response("lex error", &diagnostics),
                        subscriptions,
                    )
                    .await;
            }
            Err(ParseFromSourceError::Parse { diagnostics, .. }) => {
                return self
                    .command_with_transaction_status(
                        error_response("parse error", &diagnostics),
                        subscriptions,
                    )
                    .await;
            }
        };

        let is_transaction_request = subscriptions.transaction_active()
            || client_statements.iter().any(|parsed| {
                matches!(
                    parsed.statement,
                    ClientStatement::BeginTransaction
                        | ClientStatement::CommitTransaction
                        | ClientStatement::RevertTransaction
                )
            });
        if is_transaction_request {
            let leader = self.inner.consensus.current_leader().await;
            if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
                return self
                    .command_with_transaction_status(
                        self.not_leader_response(&req.query, leader).await,
                        subscriptions,
                    )
                    .await;
            }
            #[cfg(feature = "testing")]
            self.inner
                .runtime
                .pause_command_admission_if_armed(self.inner.consensus.local_node_id())
                .await;
            self.drop_transaction_bindings_if_armed();
            if subscriptions.transaction_active()
                && let Err(error) = self.validate_session_transaction_binding(subscriptions)
            {
                return self
                    .command_with_transaction_status(error.into_command_result(), subscriptions)
                    .await;
            }
        }

        let operations =
            match subscriptions.plan_commands(client_statements, &req.query, &req.domain) {
                Ok(operations) => operations,
                Err(error) => {
                    return self
                        .command_with_transaction_status(command_error(error), subscriptions)
                        .await;
                }
            };

        let result = self
            .process_session_command_operations(operations, tx, subscriptions)
            .await;
        self.command_with_transaction_status(result, subscriptions)
            .await
    }

    async fn transaction_consensus_error_response(
        &self,
        error: ConsensusTransactionError,
    ) -> CommandResult {
        let message = error.to_string();
        match error {
            ConsensusTransactionError::Consensus(error) => {
                self.consensus_error_response(&error, message).await
            }
            _ => command_error(message),
        }
    }

    async fn command_with_transaction_status(
        &self,
        mut result: CommandResult,
        subscriptions: &SessionSubscriptions,
    ) -> CommandResult {
        if result.transaction.is_none()
            && let Some(id) = subscriptions.transaction_id()
            && let Some(transaction) = self.inner.consensus.current_transaction(id).await
        {
            result.transaction = Some(transaction_status(&transaction));
        }
        result
    }

    /// Drops this node's leader-local transaction bindings when a test arms it, reproducing the
    /// soft state a node does not carry across a leadership change.
    fn drop_transaction_bindings_if_armed(&self) {
        #[cfg(feature = "testing")]
        if self
            .inner
            .runtime
            .take_armed_transaction_binding_drop(self.inner.consensus.local_node_id())
        {
            self.inner.transaction_bindings.clear();
        }
    }

    fn validate_session_transaction_binding(
        &self,
        subscriptions: &SessionSubscriptions,
    ) -> Result<(), SessionTransactionBindingError> {
        let Some(id) = subscriptions.transaction_id() else {
            return Err(SessionTransactionBindingError::Unbound);
        };
        match self.inner.transaction_bindings.get(id) {
            Some(binding) if binding.value() == &subscriptions.session_id => Ok(()),
            Some(_) => Err(SessionTransactionBindingError::TakenOver { id: id.to_string() }),
            None => Err(SessionTransactionBindingError::Detached { id: id.to_string() }),
        }
    }

    fn release_session_transaction_binding(&self, subscriptions: &mut SessionSubscriptions) {
        let Some(id) = subscriptions.detach_transaction() else {
            return;
        };
        if self
            .inner
            .transaction_bindings
            .get(&id)
            .is_some_and(|binding| binding.value() == &subscriptions.session_id)
        {
            self.inner.transaction_bindings.remove(&id);
        }
    }

    async fn clean_close_transaction(&self, subscriptions: &mut SessionSubscriptions) {
        let Some(id) = subscriptions.transaction_id().map(ToOwned::to_owned) else {
            return;
        };
        if self.inner.consensus.current_leader().await.as_ref()
            != Some(self.inner.consensus.local_node_id())
            || self
                .inner
                .transaction_bindings
                .get(&id)
                .is_none_or(|binding| binding.value() != &subscriptions.session_id)
        {
            return;
        }
        if let Some(transaction) = self.inner.consensus.current_transaction(&id).await
            && matches!(transaction.state, TransactionState::Open)
            && let Err(error) = self
                .inner
                .consensus
                .revert_transaction(id.clone(), subscriptions.user.clone(), current_timestamp())
                .await
        {
            warn!(
                transaction_id = id,
                error = %error,
                "failed to revert transaction during clean session close"
            );
        }
        self.release_session_transaction_binding(subscriptions);
    }

    async fn attach_transaction(
        &self,
        request: proto::AttachTransactionRequest,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            return self
                .not_leader_response(&format!("ATTACH TRANSACTION {}", request.id), leader)
                .await;
        }
        let Some(transaction) = self.inner.consensus.current_transaction(&request.id).await else {
            return command_error(format!("transaction '{}' is unknown", request.id));
        };
        if transaction.owner != subscriptions.user {
            return command_error(format!(
                "transaction '{}' belongs to another user",
                request.id
            ));
        }
        if let TransactionState::Finished(finished) = &transaction.state {
            let mut recorded = transaction_commit_result(&transaction);
            recorded.transaction = None;
            let mut result = command_error(format!(
                "transaction '{}' finished with outcome {}",
                request.id,
                finished.outcome.as_str()
            ));
            if !recorded.message.is_empty() {
                result.results.push(recorded);
            }
            result.transaction = Some(transaction_status(&transaction));
            return result;
        }

        let transaction = match self
            .inner
            .consensus
            .touch_transaction(
                request.id.clone(),
                subscriptions.user.clone(),
                current_timestamp(),
            )
            .await
        {
            Ok(transaction) => transaction,
            Err(error) => return self.transaction_consensus_error_response(error).await,
        };
        self.release_session_transaction_binding(subscriptions);
        self.inner
            .transaction_bindings
            .insert(request.id.clone(), subscriptions.session_id.clone());
        subscriptions.bind_transaction(request.id.clone());
        let mut result = command_ok(format!("attached transaction '{}'", request.id));
        result.transaction = Some(transaction_status(&transaction));
        result
    }

    async fn process_session_command_operations(
        &self,
        operations: Vec<SessionCommandOperation>,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let is_batch = operations.len() > 1;
        let mut results = Vec::new();
        let mut transaction = None;

        for operation in operations {
            tokio::task::consume_budget().await;
            let result = match operation {
                SessionCommandOperation::Begin { domain } => {
                    match self.resolve_transaction_domain(&domain).await {
                        Err(message) => command_error(message),
                        Ok(domain) => {
                            let id = uuid::Uuid::now_v7().to_string();
                            let transaction = ReplicatedTransaction::open(
                                id.clone(),
                                domain,
                                subscriptions.user.clone(),
                                current_timestamp(),
                            );
                            match self
                                .inner
                                .consensus
                                .open_transaction(transaction, self.inner.transaction_max_open)
                                .await
                            {
                                Ok(transaction) => {
                                    self.inner
                                        .transaction_bindings
                                        .insert(id.clone(), subscriptions.session_id.clone());
                                    subscriptions.bind_transaction(id.clone());
                                    let mut result =
                                        command_ok(format!("transaction started: id '{id}'"));
                                    result.transaction = Some(transaction_status(&transaction));
                                    result
                                }
                                Err(error) => {
                                    self.transaction_consensus_error_response(error).await
                                }
                            }
                        }
                    }
                }
                SessionCommandOperation::Queue(command) => {
                    self.queue_transaction_statement(command, subscriptions)
                        .await
                }
                SessionCommandOperation::Commit => {
                    self.commit_bound_transaction(tx, subscriptions).await
                }
                SessionCommandOperation::Revert => {
                    self.revert_bound_transaction(subscriptions).await
                }
                SessionCommandOperation::Execute(command) => {
                    self.process_pending_session_commands(vec![command], tx, subscriptions, false)
                        .await
                }
            };

            if result.transaction.is_some() {
                transaction.clone_from(&result.transaction);
            }
            if !result.success {
                let mut result = command_batch_result(results, result, is_batch);
                if result.transaction.is_none() {
                    result.transaction = transaction;
                }
                return result;
            }
            if !is_batch {
                return result;
            }
            append_command_result(&mut results, result);
        }

        if results.is_empty() {
            return command_error("empty command".to_string());
        }
        CommandResult {
            success: true,
            message: command_results_message(&results),
            diagnostics: Vec::new(),
            kind: i32::from(CommandResultKind::Ok),
            results,
            transaction,
            ..Default::default()
        }
    }

    /// Resolves the domain a `BEGIN` binds its transaction to. The domain must already exist,
    /// because a transaction can no longer create one and every statement it queues belongs to it.
    async fn resolve_transaction_domain(&self, request_domain: &str) -> Result<DomainName, String> {
        let domain = match parse_request_domain(request_domain) {
            Ok(domain) => domain,
            Err(RequestDomainError::Missing) => {
                return Err("no active domain selected".to_string());
            }
            Err(RequestDomainError::Invalid) => return Err("invalid domain".to_string()),
        };
        if self.inner.consensus.current_domain(&domain).await.is_none() {
            return Err(format!("domain '{}' does not exist", domain.as_str()));
        }
        Ok(domain)
    }

    async fn queue_transaction_statement(
        &self,
        command: PendingSessionCommand,
        subscriptions: &SessionSubscriptions,
    ) -> CommandResult {
        if let Err(error) = self.validate_session_transaction_binding(subscriptions) {
            return error.into_command_result();
        }
        let Some(id) = subscriptions.transaction_id() else {
            return command_error("no transaction is attached to this session".to_string());
        };
        let ClientStatement::Server(statement) = command.statement else {
            return command_error(
                "session-scoped and client-local statements cannot be queued in a transaction"
                    .to_string(),
            );
        };
        if !is_queueable_transaction_statement(&statement) {
            return command_error(format!(
                "{} cannot be queued in a transaction; a transaction applies to one existing \
                 domain and queues only that domain's configuration statements",
                transaction_statement_label(&statement)
            ));
        }
        let domain = match parse_request_domain(&command.domain) {
            Ok(domain) => domain,
            Err(RequestDomainError::Missing) => {
                return command_error("no active domain selected".to_string());
            }
            Err(RequestDomainError::Invalid) => {
                return command_error("invalid domain".to_string());
            }
        };
        let queued = TransactionStatement {
            source: command.source,
            statement,
        };
        let Some(transaction) = self.inner.consensus.current_transaction(id).await else {
            return command_error(format!("transaction '{id}' is unknown"));
        };
        let limits = TransactionQueueLimits {
            max_statements: self.inner.transaction_max_statements,
            max_source_bytes: self.inner.transaction_max_source_bytes,
        };
        if let Err(error) =
            transaction.validate_queue_admission(&subscriptions.user, &domain, &queued, limits)
        {
            return command_error(error.to_string());
        }
        let quiesce_level = match self
            .preflight_transaction_statement(&transaction, &queued)
            .await
        {
            Ok(quiesce_level) => quiesce_level,
            Err(error) => return command_error(error),
        };
        match self
            .inner
            .consensus
            .queue_transaction_statement(
                id.to_string(),
                subscriptions.user.clone(),
                domain,
                current_timestamp(),
                queued,
                limits,
            )
            .await
        {
            Ok(transaction) => {
                let message = match quiesce_level {
                    Some(quiesce_level) => quiesce_level_message(quiesce_level),
                    None => String::new(),
                };
                let mut result = command_ok(message);
                result.transaction = Some(transaction_status(&transaction));
                result
            }
            Err(error) => self.transaction_consensus_error_response(error).await,
        }
    }

    async fn preflight_transaction_statement(
        &self,
        transaction: &ReplicatedTransaction,
        candidate: &TransactionStatement,
    ) -> Result<Option<QuiesceLevel>, String> {
        let (mut domains, resources) = tokio::join!(
            self.inner.consensus.current_domains(),
            self.inner.consensus.current_resources(),
        );
        let mut resource_names = resources
            .next_version_by_resource
            .iter()
            .filter(|counter| counter.domain == transaction.domain)
            .map(|counter| counter.identifier.clone())
            .collect::<BTreeSet<_>>();
        let domain_id = &transaction.domain;
        let mut model_mutations = Vec::<RegistryMutation>::new();
        let candidate_is_model_mutation = candidate.statement.is_model_mutation();
        let mut candidate_quiesce_level =
            candidate_is_model_mutation.then_some(QuiesceLevel::Dynamic);

        for queued in transaction
            .statements
            .iter()
            .chain(std::iter::once(candidate))
        {
            match &queued.statement {
                Statement::AlterDomain(alter) => {
                    let domain = domains
                        .get_mut(domain_id)
                        .ok_or_else(|| format!("domain '{}' does not exist", domain_id.as_str()))?;
                    if let DomainStatus::Paused = domain.status {
                        return Err(format!(
                            "domain '{}' is paused by a model alteration",
                            domain_id.as_str()
                        ));
                    }
                    domain.config.placement = alter.policy;
                }
                Statement::StartDomain(start) => {
                    let domain = domains
                        .get_mut(domain_id)
                        .ok_or_else(|| format!("domain '{}' does not exist", domain_id.as_str()))?;
                    validate_domain_config(&domain.config)?;
                    if let DomainStatus::Running = domain.status {
                        return Err(format!(
                            "domain '{}' is already running",
                            domain_id.as_str()
                        ));
                    }
                    if let DomainStatus::Paused = domain.status {
                        return Err(format!(
                            "domain '{}' is paused for a model alteration",
                            domain_id.as_str()
                        ));
                    }
                    domain.status = DomainStatus::Running;
                    domain.last_start = start.start.clone();
                    domain.start_version = domain.start_version.checked_add(1).assured(
                        "a domain cannot be started 2^64 times in the lifetime of a cluster",
                    );
                }
                Statement::StopDomain(_) => {
                    let domain = domains
                        .get_mut(domain_id)
                        .ok_or_else(|| format!("domain '{}' does not exist", domain_id.as_str()))?;
                    if let DomainStatus::Stopped = domain.status {
                        return Err(format!(
                            "domain '{}' is already stopped",
                            domain_id.as_str()
                        ));
                    }
                    domain.status = DomainStatus::Stopped;
                    domain.clock = None;
                }
                Statement::CreateResource(create) => {
                    if !resource_names.insert(create.identifier.clone()) && !create.if_not_exists {
                        return Err(format!(
                            "resource '{}' already exists",
                            create.identifier.as_str()
                        ));
                    }
                }
                statement if statement.is_model_mutation() => {
                    let domain = domains
                        .get(domain_id)
                        .ok_or_else(|| format!("domain '{}' does not exist", domain_id.as_str()))?;
                    if let DomainStatus::Paused = domain.status {
                        return Err(format!(
                            "domain '{}' is paused by a model alteration",
                            domain_id.as_str()
                        ));
                    }
                    if let Statement::Create(create) = statement
                        && create.if_not_exists
                        && self
                            .inner
                            .registry
                            .contains(domain_id, create.body.kind(), create.body.name())
                            .map_err(|error| error.to_string())?
                    {
                        continue;
                    }
                    model_mutations.push(Self::transaction_registry_mutation(statement));
                }
                _ => {
                    return Err(format!(
                        "{} is not valid transaction content",
                        transaction_statement_label(&queued.statement)
                    ));
                }
            }
        }

        if model_mutations.is_empty() {
            return Ok(candidate_quiesce_level);
        }
        tokio::task::consume_budget().await;
        let preflight = self
            .inner
            .registry
            .preflight_transaction_mutations(domain_id, &model_mutations)
            .map_err(|error| format!("transaction statement failed preflight: {error}"))?;
        if candidate_is_model_mutation {
            candidate_quiesce_level = preflight.mutation_quiesce_levels().last().copied();
        }
        let Some(planned) = preflight.planned() else {
            return Ok(candidate_quiesce_level);
        };
        let domain = domains
            .get(domain_id)
            .verified("replaying the transaction resolved this domain before reaching the step");
        self.validate_changed_model_bindings(domain_id, domain.config.pace, planned)
            .await?;
        self.prepare_planned_domain_udfs(planned).await?;
        self.prepare_domain_schedule(
            domain_id,
            planned.candidate_graph(),
            domain.config.placement,
        )
        .await?;
        Ok(candidate_quiesce_level)
    }

    fn transaction_registry_mutation(statement: &Statement) -> RegistryMutation {
        match statement {
            Statement::Create(create) => RegistryMutation::Create(create.body.clone()),
            Statement::AlterSchema(alter) => RegistryMutation::AlterSchema(alter.clone()),
            Statement::AlterWireJsonSchema(alter) => {
                RegistryMutation::AlterWireJsonSchema(alter.clone())
            }
            Statement::AlterWireCborSchema(alter) => {
                RegistryMutation::AlterWireCborSchema(alter.clone())
            }
            Statement::AlterWireAvroSchema(alter) => {
                RegistryMutation::AlterWireAvroSchema(alter.clone())
            }
            Statement::AlterRelay(alter) => RegistryMutation::AlterRelay(alter.clone()),
            Statement::AlterJunction(alter) => RegistryMutation::AlterJunction(alter.clone()),
            Statement::AlterDeduplicator(alter) => {
                RegistryMutation::AlterDeduplicator(alter.clone())
            }
            Statement::AlterReorderer(alter) => RegistryMutation::AlterReorderer(alter.clone()),
            Statement::AlterEmitter(alter) => RegistryMutation::AlterEmitter(alter.clone()),
            Statement::AlterIngestor(alter) => RegistryMutation::AlterIngestor(alter.clone()),
            Statement::AlterReingestor(alter) => RegistryMutation::AlterReingestor(alter.clone()),
            Statement::AlterGenerator(alter) => RegistryMutation::AlterGenerator(alter.clone()),
            Statement::AlterPlacement(alter) => RegistryMutation::AlterPlacement(alter.clone()),
            Statement::Drop(drop) => RegistryMutation::Drop(drop.clone()),
            _ => unreachable!("transaction registry mutation requires a model mutation"),
        }
    }

    async fn revert_bound_transaction(
        &self,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        if let Err(error) = self.validate_session_transaction_binding(subscriptions) {
            return error.into_command_result();
        }
        let Some(id) = subscriptions.transaction_id().map(ToOwned::to_owned) else {
            return command_error("REVERT requires an active transaction".to_string());
        };
        let previous = self.inner.consensus.current_transaction(&id).await;
        match self
            .inner
            .consensus
            .revert_transaction(id.clone(), subscriptions.user.clone(), current_timestamp())
            .await
        {
            Ok(transaction) => {
                let dropped = match previous {
                    Some(transaction) => transaction.statements.len(),
                    None => 0,
                };
                self.release_session_transaction_binding(subscriptions);
                let mut result = command_ok(format!(
                    "transaction reverted: dropped {dropped} command(s); id '{id}'"
                ));
                result.transaction = Some(transaction_status(&transaction));
                result
            }
            Err(error) => self.transaction_consensus_error_response(error).await,
        }
    }

    async fn commit_bound_transaction(
        &self,
        _tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        if let Err(error) = self.validate_session_transaction_binding(subscriptions) {
            return error.into_command_result();
        }
        let Some(id) = subscriptions.transaction_id().map(ToOwned::to_owned) else {
            return command_error("COMMIT requires an active transaction".to_string());
        };
        let started = match self
            .inner
            .consensus
            .start_transaction_commit(id.clone(), subscriptions.user.clone(), current_timestamp())
            .await
        {
            Ok(transaction) => transaction,
            Err(error) => return self.transaction_consensus_error_response(error).await,
        };
        let finished = if started.statements.is_empty() {
            self.inner
                .consensus
                .finish_empty_transaction_commit(id.clone(), current_timestamp())
                .await
                .map_err(|error| Report::new(TransactionCommitError::Proposal(error)))
        } else {
            // A replicated commit owns its execution independently of the session. Keep its
            // model-mutation future off the session's poll stack as well.
            let service = self.clone();
            let commit_id = id.clone();
            match tokio::spawn(async move { service.execute_replicated_commit(&commit_id).await })
                .await
            {
                Ok(result) => result,
                Err(error) => Err(Report::new(error)
                    .change_context(TransactionCommitError::TaskJoin { id: id.clone() })),
            }
        };
        match finished {
            Ok(transaction) => {
                if matches!(transaction.state, TransactionState::Finished(_)) {
                    self.release_session_transaction_binding(subscriptions);
                }
                transaction_commit_result(&transaction)
            }
            Err(error) => {
                let proposal_error = error
                    .current_context()
                    .consensus_error()
                    .or_else(|| error.downcast_ref::<ConsensusError>());
                let mut result =
                    if let Some(ConsensusError::LeadershipLost { leader_id }) = proposal_error {
                        self.not_leader_response("COMMIT", leader_id.clone()).await
                    } else {
                        command_error(format!(
                            "transaction '{id}' commit remains in progress after an execution \
                             error: {error}"
                        ))
                    };
                if let Some(transaction) = self.inner.consensus.current_transaction(&id).await {
                    result.transaction = Some(transaction_status(&transaction));
                }
                result
            }
        }
    }

    async fn execute_replicated_commit(
        &self,
        id: &str,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let _commit_execution = self.inner.transaction_commit_execution.lock().await;
        let mut wait = interval(Duration::from_millis(100));
        let lease = loop {
            tokio::task::consume_budget().await;
            match self.inner.transaction_executions.entry(id.to_string()) {
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    entry.insert(());
                    break TransactionExecutionLease {
                        executions: self.inner.transaction_executions.clone(),
                        id: id.to_string(),
                    };
                }
                dashmap::mapref::entry::Entry::Occupied(_) => {
                    if let Some(transaction) = self.inner.consensus.current_transaction(id).await
                        && matches!(transaction.state, TransactionState::Finished(_))
                    {
                        return Ok(transaction);
                    }
                    wait.tick().await;
                }
            }
        };

        let result = self.run_replicated_commit(id).await;
        drop(lease);
        result
    }

    async fn run_replicated_commit(
        &self,
        id: &str,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        self.inner
            .registry
            .synchronize_cluster_schedule(&self.inner.consensus.current_schedule().await)
            .change_context(TransactionCommitError::SynchronizeRegistry { id: id.to_string() })?;
        loop {
            tokio::task::consume_budget().await;
            let leader_id = self.inner.consensus.current_leader().await;
            if leader_id.as_ref() != Some(self.inner.consensus.local_node_id()) {
                return Err(Report::new(TransactionCommitError::Proposal(
                    ConsensusTransactionError::Consensus(ConsensusError::LeadershipLost {
                        leader_id,
                    }),
                )));
            }
            let transaction = self
                .inner
                .consensus
                .current_transaction(id)
                .await
                .ok_or_else(|| {
                    Report::new(TransactionCommitError::UnknownTransaction { id: id.to_string() })
                })?;
            let progress = match &transaction.state {
                TransactionState::Committing(progress) => progress,
                TransactionState::Finished(_) => return Ok(transaction),
                TransactionState::Open => {
                    return Err(Report::new(TransactionCommitError::TransactionOpen {
                        id: id.to_string(),
                    }));
                }
            };
            let first_statement = progress.next_statement;
            let Some(first) = transaction.statements.get(first_statement) else {
                return self
                    .inner
                    .consensus
                    .finish_empty_transaction_commit(id.to_string(), current_timestamp())
                    .await
                    .map_err(|error| Report::new(TransactionCommitError::Proposal(error)));
            };
            self.recover_transaction_quiescence(&transaction, first_statement)
                .await?;

            if first.statement.is_model_mutation() {
                let domain = transaction.domain.clone();
                let mut statements = Vec::new();
                let mut sources = Vec::new();
                for queued in transaction.statements.iter().skip(first_statement) {
                    if !queued.statement.is_model_mutation() {
                        break;
                    }
                    statements.push(queued.statement.clone());
                    sources.push(queued.source.clone());
                }
                let statement_count = statements.len();
                let outcome = ParkingMutex::new(None);
                let result = self
                    .process_model_mutation_batch_with_transaction(
                        statements,
                        &sources.join("; "),
                        domain.as_str(),
                        Some(TransactionModelStepContext {
                            transaction: &transaction,
                            first_statement,
                            statement_count,
                            outcome: &outcome,
                        }),
                    )
                    .await;
                let recorded = outcome.lock().take();
                let advanced = match recorded {
                    Some(Ok(transaction)) => transaction,
                    Some(Err(error)) => return Err(error),
                    None if result.kind == i32::from(CommandResultKind::NotLeader) => {
                        return Err(Report::new(TransactionCommitError::Proposal(
                            ConsensusTransactionError::Consensus(ConsensusError::LeadershipLost {
                                leader_id: self.inner.consensus.current_leader().await,
                            }),
                        )));
                    }
                    None if !result.success => {
                        self.record_transaction_step(
                            &transaction,
                            first_statement,
                            statement_count,
                            result,
                            None,
                            None,
                        )
                        .await?
                    }
                    None => {
                        return Err(Report::new(TransactionCommitError::MissingProgress {
                            id: id.to_string(),
                        }));
                    }
                };
                self.pause_transaction_commit_if_armed(&advanced).await;
                if matches!(advanced.state, TransactionState::Finished(_)) {
                    return Ok(advanced);
                }
                continue;
            }

            let advanced = self
                .execute_transaction_configuration_step(&transaction, first_statement)
                .await?;
            self.pause_transaction_commit_if_armed(&advanced).await;
            if matches!(advanced.state, TransactionState::Finished(_)) {
                return Ok(advanced);
            }
        }
    }

    async fn pause_transaction_commit_if_armed(&self, _transaction: &ReplicatedTransaction) {
        #[cfg(feature = "testing")]
        if let TransactionState::Committing(_) = _transaction.state {
            self.inner
                .runtime
                .pause_transaction_commit_after_progress_if_armed(
                    self.inner.consensus.local_node_id(),
                    _transaction.completed_statement_count(),
                )
                .await;
        }
    }

    /// A model-mutation step pauses the transaction's domain and resumes it once the step
    /// finishes. When a new leader adopts a commit mid-flight, that pause may still be recorded in
    /// replicated state; resume it unless the step about to run needs it held.
    async fn recover_transaction_quiescence(
        &self,
        transaction: &ReplicatedTransaction,
        current_statement: usize,
    ) -> Result<(), Report<TransactionCommitError>> {
        if transaction
            .statements
            .get(current_statement)
            .is_some_and(|statement| statement.statement.is_model_mutation())
        {
            return Ok(());
        }
        let mut completed_model_mutation = false;
        for result in transaction.commit_results() {
            tokio::task::consume_budget().await;
            let Some(statements) = result
                .first_statement
                .checked_add(result.statement_count)
                .and_then(|end| transaction.statements.get(result.first_statement..end))
            else {
                return Err(Report::new(TransactionCommitError::InvalidProgress {
                    id: transaction.id.clone(),
                }));
            };
            if !statements.is_empty()
                && statements
                    .iter()
                    .all(|statement| statement.statement.is_model_mutation())
            {
                completed_model_mutation = true;
                break;
            }
        }
        if !completed_model_mutation {
            return Ok(());
        }
        let domain = &transaction.domain;
        let Some(state) = self.inner.consensus.current_domain(domain).await else {
            return Ok(());
        };
        if let DomainStatus::Paused = state.status {
            self.apply_current_cluster_state().await.change_context(
                TransactionCommitError::RecoverQuiescence {
                    id: transaction.id.clone(),
                },
            )?;
            self.wait_for_paused_domain_drain(domain)
                .await
                .change_context(TransactionCommitError::RecoverQuiescence {
                    id: transaction.id.clone(),
                })?;
            self.resume_domain_after_alter(domain)
                .await
                .change_context(TransactionCommitError::RecoverQuiescence {
                    id: transaction.id.clone(),
                })?;
        }
        Ok(())
    }

    async fn record_transaction_step(
        &self,
        transaction: &ReplicatedTransaction,
        first_statement: usize,
        statement_count: usize,
        result: CommandResult,
        quiesce_level: Option<QuiesceLevel>,
        effect: Option<TransactionStepEffect>,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let next_statement = first_statement
            .checked_add(statement_count)
            .assured("a recorded commit step counts statements of the transaction it belongs to");
        let completion = if result.success {
            (next_statement == transaction.statements.len())
                .then_some(TransactionOutcome::Committed)
        } else {
            Some(TransactionOutcome::Failed {
                failing_step: first_statement,
                error: result.message.clone(),
            })
        };
        let effect = if result.success { effect } else { None };
        let planned_relocations = match effect.as_ref() {
            Some(
                TransactionStepEffect::ReplaceDomainSchedule {
                    expected_schedule,
                    schedule,
                    ..
                }
                | TransactionStepEffect::PutDomainAndSchedule {
                    expected_schedule,
                    schedule,
                    ..
                },
            ) => {
                let count =
                    planned_ownership_moves(expected_schedule.as_deref(), schedule.as_deref())
                        .len();
                (count > 0).then_some(count)
            }
            _ => None,
        };
        self.inner
            .consensus
            .advance_transaction_commit(TransactionCommitAdvance {
                id: transaction.id.clone(),
                expected_next_statement: first_statement,
                next_statement,
                at: current_timestamp(),
                result: TransactionStepResult {
                    first_statement,
                    statement_count,
                    quiesce_level,
                    planned_relocations,
                    result: replicated_command_result(&result),
                },
                effect,
                completion,
            })
            .await
            .map_err(|error| Report::new(TransactionCommitError::Proposal(error)))
    }

    async fn execute_transaction_configuration_step(
        &self,
        transaction: &ReplicatedTransaction,
        statement_index: usize,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let queued = transaction.statements.get(statement_index).verified(
            "the caller checked this index against the same statement list before dispatching the \
             step",
        );
        let mut ownership_handoff = None;
        let mut step_quiesce_level = None;
        let domain_id = &transaction.domain;
        let _alter_guard = if let Statement::AlterDomain(_) = &queued.statement {
            let Some(guard) = self.inner.runtime.try_begin_domain_alter(domain_id) else {
                return self
                    .record_transaction_step(
                        transaction,
                        statement_index,
                        1,
                        command_error(
                            DomainAlterError::ConcurrentAlter {
                                domain: domain_id.clone(),
                            }
                            .to_string(),
                        ),
                        None,
                        None,
                    )
                    .await;
            };
            Some(guard)
        } else {
            None
        };
        let (result, effect) = match &queued.statement {
            Statement::AlterDomain(alter) => {
                let Some(previous) = self.inner.consensus.current_domain(domain_id).await else {
                    return self
                        .record_transaction_step(
                            transaction,
                            statement_index,
                            1,
                            command_error(format!(
                                "domain '{}' does not exist",
                                domain_id.as_str()
                            )),
                            None,
                            None,
                        )
                        .await;
                };
                if let DomainStatus::Paused = previous.status {
                    (
                        command_error(format!(
                            "domain '{}' is paused by a model alteration",
                            domain_id.as_str()
                        )),
                        None,
                    )
                } else if previous.config.placement == alter.policy {
                    step_quiesce_level = Some(QuiesceLevel::Dynamic);
                    (
                        command_ok(format!(
                            "domain '{}' placement is already {}; {}\nplanned relocations: 0",
                            domain_id.as_str(),
                            alter.policy.as_ref(),
                            quiesce_level_message(QuiesceLevel::Dynamic)
                        )),
                        None,
                    )
                } else {
                    let graph = self.inner.registry.active_graph(domain_id);
                    let expected_schedule = self
                        .inner
                        .consensus
                        .current_schedule()
                        .await
                        .domain(domain_id)
                        .cloned();
                    let PreparedDomainSchedule {
                        mut schedule,
                        relocations,
                    } = self
                        .prepare_domain_schedule(domain_id, graph, alter.policy)
                        .await
                        .map_err(|reason| {
                            Report::new(TransactionCommitError::PrepareSchedule {
                                domain: domain_id.clone(),
                                reason,
                            })
                        })?;
                    let quiesce_level =
                        if matches!(previous.status, DomainStatus::Running) && relocations > 0 {
                            QuiesceLevel::EntityPause
                        } else {
                            QuiesceLevel::Dynamic
                        };
                    step_quiesce_level = Some(quiesce_level);
                    let mut next = previous.clone();
                    next.config.placement = alter.policy;
                    if let Some(schedule) = schedule.as_mut() {
                        mark_complete_ownership_transitions(expected_schedule.as_ref(), schedule);
                    }
                    let handoff = if relocations > 0 {
                        self.begin_planned_ownership_handoff(
                            domain_id,
                            expected_schedule.as_ref(),
                            schedule.as_ref(),
                        )
                        .await
                    } else {
                        Ok(None)
                    };
                    match handoff {
                        Ok(handoff) => {
                            ownership_handoff = handoff;
                            (
                                command_ok(format!(
                                    "set domain '{}' placement to {}; {}\nplanned relocations: \
                                     {relocations}",
                                    domain_id.as_str(),
                                    alter.policy.as_ref(),
                                    quiesce_level_message(quiesce_level)
                                )),
                                Some(TransactionStepEffect::PutDomainAndSchedule {
                                    expected_domain: Box::new(previous),
                                    expected_schedule: expected_schedule.map(Box::new),
                                    domain: Box::new(next),
                                    schedule: schedule.map(Box::new),
                                }),
                            )
                        }
                        Err(error) => (command_error(error.to_string()), None),
                    }
                }
            }
            Statement::CreateResource(create) => {
                let resources = self.inner.consensus.current_resources().await;
                if resources.is_declared(domain_id, &create.identifier) {
                    if create.if_not_exists {
                        (
                            command_ok_already_existed(format!(
                                "resource '{}' already exists",
                                create.identifier.as_str()
                            )),
                            None,
                        )
                    } else {
                        (
                            command_error(format!(
                                "resource '{}' already exists",
                                create.identifier.as_str()
                            )),
                            None,
                        )
                    }
                } else {
                    (
                        command_ok(format!("created resource '{}'", create.identifier.as_str())),
                        Some(TransactionStepEffect::CreateResourceCatalog {
                            identifier: create.identifier.clone(),
                        }),
                    )
                }
            }
            Statement::StartDomain(start) => {
                let Some(domain) = self.inner.consensus.current_domain(domain_id).await else {
                    return self
                        .record_transaction_step(
                            transaction,
                            statement_index,
                            1,
                            command_error(format!(
                                "domain '{}' does not exist",
                                domain_id.as_str()
                            )),
                            None,
                            None,
                        )
                        .await;
                };
                if let Err(message) = validate_domain_config(&domain.config) {
                    (command_error(message), None)
                } else if let DomainStatus::Running = domain.status {
                    (
                        command_error(format!(
                            "domain '{}' is already running",
                            domain_id.as_str()
                        )),
                        None,
                    )
                } else if let DomainStatus::Paused = domain.status {
                    (
                        command_error(format!(
                            "domain '{}' is paused for a model alteration",
                            domain_id.as_str()
                        )),
                        None,
                    )
                } else {
                    let wall_started_at = current_timestamp();
                    let (mut logical_start, time_rate) = start.start.resolve_at(wall_started_at);
                    if let DomainPace::Paced = domain.config.pace
                        && let DomainStartPoint::Resume = &start.start
                        && let Ok(Some(resume_at)) =
                            self.inner.runtime.current_paced_domain_time(domain_id)
                    {
                        logical_start = resume_at;
                    }
                    let concrete_start = match &start.start {
                        DomainStartPoint::Resume => DomainStartPoint::Resume,
                        DomainStartPoint::Now { .. } => DomainStartPoint::At {
                            timestamp: logical_start,
                            time_rate,
                        },
                        DomainStartPoint::At { .. } => start.start.clone(),
                    };
                    let clock = DomainClockState::new(wall_started_at, logical_start, time_rate);
                    let authority = if let DomainPace::Paced = domain.config.pace {
                        match self.selected_domain_clock_authority(domain_id).await {
                            Some(authority) => Ok(Some(authority)),
                            None => Err(format!(
                                "no live voter is available to own the clock for domain '{}'",
                                domain_id.as_str()
                            )),
                        }
                    } else {
                        Ok(None)
                    };
                    match authority {
                        Ok(authority) => (
                            command_ok(format!("starting domain '{}'", domain_id.as_str())),
                            Some(TransactionStepEffect::StartDomain {
                                domain_id: domain_id.clone(),
                                expected_start_version: domain.start_version,
                                start: concrete_start,
                                clock: matches!(domain.config.pace, DomainPace::Paced)
                                    .then_some(clock),
                                authority,
                            }),
                        ),
                        Err(message) => (command_error(message), None),
                    }
                }
            }
            Statement::StopDomain(_) => {
                let Some(domain) = self.inner.consensus.current_domain(domain_id).await else {
                    return self
                        .record_transaction_step(
                            transaction,
                            statement_index,
                            1,
                            command_error(format!(
                                "domain '{}' does not exist",
                                domain_id.as_str()
                            )),
                            None,
                            None,
                        )
                        .await;
                };
                if let DomainStatus::Stopped = domain.status {
                    (
                        command_error(format!(
                            "domain '{}' is already stopped",
                            domain_id.as_str()
                        )),
                        None,
                    )
                } else {
                    (
                        command_ok(format!("stopped domain '{}'", domain_id.as_str())),
                        Some(TransactionStepEffect::StopDomain {
                            domain_id: domain_id.clone(),
                            expected_start_version: domain.start_version,
                        }),
                    )
                }
            }
            _ => (
                command_error(format!(
                    "{} is not valid transaction content",
                    transaction_statement_label(&queued.statement)
                )),
                None,
            ),
        };

        let succeeded = result.success;
        let advanced = match self
            .record_transaction_step(
                transaction,
                statement_index,
                1,
                result,
                step_quiesce_level,
                effect,
            )
            .await
        {
            Ok(advanced) => advanced,
            Err(error) => {
                if let Some(handoff) = ownership_handoff.take() {
                    self.abort_planned_ownership_handoff(domain_id, handoff)
                        .await;
                }
                return Err(error);
            }
        };
        if succeeded {
            let activation_error = self.apply_current_cluster_state().await.err();
            if let Some(error) = &activation_error {
                self.broadcast_error(format!(
                    "failed to reconcile runtime after transaction '{}' step {}: {error}",
                    transaction.id,
                    statement_index
                        .checked_add(1)
                        .assured("the index names a statement of a transaction held in memory")
                ));
            }
            if let Some(handoff) = ownership_handoff.take() {
                if let Some(error) = &activation_error {
                    self.defer_planned_ownership_handoff_release(domain_id, handoff, error);
                } else if let Err(error) = self
                    .finish_planned_ownership_handoff(domain_id, handoff)
                    .await
                {
                    self.broadcast_error(format!(
                        "failed to confirm ownership state activation after transaction '{}' step \
                         {}: {error}",
                        transaction.id,
                        statement_index
                            .checked_add(1)
                            .assured("the index names a statement of a transaction held in memory")
                    ));
                }
            }
        }
        if let Some(handoff) = ownership_handoff {
            self.abort_planned_ownership_handoff(domain_id, handoff)
                .await;
        }
        Ok(advanced)
    }

    async fn reconcile_transactions_once(&self) {
        if self.inner.consensus.current_leader().await.as_ref()
            != Some(self.inner.consensus.local_node_id())
        {
            return;
        }
        let now = current_timestamp();
        let idle_before = subtract_timestamp_duration(now, self.inner.transaction_idle_timeout);
        let finished_before =
            subtract_timestamp_duration(now, self.inner.transaction_tombstone_retention);
        let transactions = self.inner.consensus.current_transactions().await;

        for transaction in transactions.values() {
            tokio::task::consume_budget().await;
            match &transaction.state {
                TransactionState::Open
                    if !self
                        .inner
                        .transaction_bindings
                        .contains_key(&transaction.id)
                        && transaction.last_activity_at <= idle_before =>
                {
                    match self
                        .inner
                        .consensus
                        .expire_transaction(transaction.id.clone(), now, idle_before)
                        .await
                    {
                        Ok(expired)
                            if matches!(
                                expired.finished_outcome(),
                                Some(TransactionOutcome::Expired)
                            ) =>
                        {
                            self.inner.transaction_bindings.remove(&transaction.id);
                            info!(
                                transaction_id = transaction.id,
                                owner = transaction.owner.as_str(),
                                "expired orphaned NSPL transaction"
                            );
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(
                                transaction_id = transaction.id,
                                error = %error,
                                "failed to expire orphaned NSPL transaction"
                            );
                        }
                    }
                }
                TransactionState::Committing(_) => {
                    if let Err(error) = self.execute_replicated_commit(&transaction.id).await {
                        warn!(
                            transaction_id = transaction.id,
                            error = %error, "failed to resume replicated NSPL commit"
                        );
                    }
                }
                TransactionState::Finished(_) => {
                    self.inner.transaction_bindings.remove(&transaction.id);
                }
                TransactionState::Open => {}
            }
        }
        if let Err(error) = self
            .inner
            .consensus
            .remove_finished_transactions(finished_before)
            .await
        {
            warn!(error = %error, "failed to remove expired transaction tombstones");
        }
    }

    async fn process_pending_session_commands(
        &self,
        commands: Vec<PendingSessionCommand>,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
        explicit_batch: bool,
    ) -> CommandResult {
        let is_batch = explicit_batch || commands.len() > 1;
        let mut results = Vec::new();
        let mut commands = commands.into_iter().peekable();

        while let Some(command) = commands.next() {
            match command.statement {
                ClientStatement::Server(statement) if statement.is_model_mutation() => {
                    let domain = command.domain;
                    let mut sources = vec![command.source];
                    let mut statements = vec![statement];

                    while let Some(next) = commands.peek() {
                        if next.domain != domain {
                            break;
                        }
                        let ClientStatement::Server(statement) = &next.statement else {
                            break;
                        };
                        if !statement.is_model_mutation() {
                            break;
                        }
                        let next = commands.next().verified(
                            "the peek above observed this command and nothing consumed the \
                             iterator since",
                        );
                        let ClientStatement::Server(statement) = next.statement else {
                            unreachable!("peeked model mutation statement must be next");
                        };
                        sources.push(next.source);
                        statements.push(statement);
                    }

                    let mutation_query = sources.join("; ");
                    let result = self
                        .process_model_mutation_batch(statements, &mutation_query, &domain)
                        .await;
                    if !result.success {
                        return command_batch_result(results, result, is_batch);
                    }
                    append_command_result(&mut results, result);
                }
                statement => {
                    let result = self
                        .process_client_statement(
                            statement,
                            &command.source,
                            &command.domain,
                            tx,
                            subscriptions,
                        )
                        .await;
                    if !result.success {
                        return command_batch_result(results, result, is_batch);
                    }
                    append_command_result(&mut results, result);
                }
            }
        }

        if results.is_empty() {
            return command_error("empty command".to_string());
        }
        if !is_batch {
            return results
                .pop()
                .verified("the empty check above already returned");
        }

        CommandResult {
            success: true,
            message: command_results_message(&results),
            diagnostics: Vec::new(),
            kind: i32::from(CommandResultKind::Ok),
            results,
            ..Default::default()
        }
    }

    async fn process_model_mutation_batch(
        &self,
        statements: Vec<Statement>,
        query: &str,
        request_domain: &str,
    ) -> CommandResult {
        self.process_model_mutation_batch_with_transaction(statements, query, request_domain, None)
            .await
    }

    async fn process_model_mutation_batch_with_transaction(
        &self,
        statements: Vec<Statement>,
        query: &str,
        request_domain: &str,
        transaction_step: Option<TransactionModelStepContext<'_>>,
    ) -> CommandResult {
        let domain = match parse_request_domain(request_domain) {
            Ok(domain) => domain,
            Err(RequestDomainError::Missing) => {
                return command_error("no active domain selected".to_string());
            }
            Err(RequestDomainError::Invalid) => {
                return command_error("invalid domain".to_string());
            }
        };

        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            return self.not_leader_response(query, leader).await;
        }

        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_command_admission_if_armed(self.inner.consensus.local_node_id())
            .await;

        let _alter_guard = match self.inner.runtime.try_begin_domain_alter(&domain) {
            Some(guard) => guard,
            None => {
                return command_error(
                    DomainAlterError::ConcurrentAlter {
                        domain: domain.clone(),
                    }
                    .to_string(),
                );
            }
        };
        let Some(domain_state) = self.inner.consensus.current_domain(&domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        let adopted_domain_pause =
            matches!(domain_state.status, DomainStatus::Paused) && transaction_step.is_some();
        if let DomainStatus::Paused = domain_state.status
            && !adopted_domain_pause
        {
            return command_error(format!(
                "domain '{}' is paused by a model alteration",
                domain.as_str()
            ));
        }
        if let Err(error) = self.reconcile_running_domain_runtime(&domain).await {
            return command_error(error);
        }

        let mut results = vec![None; statements.len()];
        let mut mutations = Vec::new();
        let mut applied = Vec::<AppliedModelMutation>::new();
        let mut refresh_http_tls = false;

        for (index, statement) in statements.into_iter().enumerate() {
            match statement {
                Statement::Create(create) => {
                    let if_not_exists = create.if_not_exists;
                    let model = create.body;
                    let model_id = model.name();
                    let model_kind = model.kind();
                    if self
                        .inner
                        .registry
                        .contains(&domain, model_kind, &model_id)
                        .unwrap_or(false)
                        && if_not_exists
                    {
                        results[index] = Some(command_ok_already_existed(format!(
                            "model '{}' already exists in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        )));
                        continue;
                    }

                    refresh_http_tls |= model_kind == ModelKind::Vhost;
                    applied.push(AppliedModelMutation {
                        index,
                        model: model_id.clone(),
                        message: String::new(),
                    });
                    mutations.push(RegistryMutation::Create(model));
                }
                Statement::AlterSchema(alter) => {
                    let model_id = alter.schema.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered schema '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterSchema(alter));
                }
                Statement::AlterWireJsonSchema(alter) => {
                    let model_id = alter.schema.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered JSON wire schema '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterWireJsonSchema(alter));
                }
                Statement::AlterWireCborSchema(alter) => {
                    let model_id = alter.schema.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered CBOR wire schema '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterWireCborSchema(alter));
                }
                Statement::AlterWireAvroSchema(alter) => {
                    let model_id = alter.schema.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered AVRO wire schema '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterWireAvroSchema(alter));
                }
                Statement::AlterRelay(alter) => {
                    let model_id = alter.relay.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered relay '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterRelay(alter));
                }
                Statement::AlterJunction(alter) => {
                    let model_id = alter.junction.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered junction '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterJunction(alter));
                }
                Statement::AlterDeduplicator(alter) => {
                    let model_id = alter.deduplicator.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered deduplicator '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterDeduplicator(alter));
                }
                Statement::AlterReorderer(alter) => {
                    let model_id = alter.reorderer.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered reorderer '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterReorderer(alter));
                }
                Statement::AlterEmitter(alter) => {
                    let model_id = alter.emitter.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered emitter '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterEmitter(alter));
                }
                Statement::AlterIngestor(alter) => {
                    let model_id = alter.ingestor.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered ingestor '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterIngestor(alter));
                }
                Statement::AlterReingestor(alter) => {
                    let model_id = alter.reingestor.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered reingestor '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterReingestor(alter));
                }
                Statement::AlterGenerator(alter) => {
                    let model_id = alter.generator.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered generator '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::AlterGenerator(alter));
                }
                Statement::AlterPlacement(alter) => {
                    let model_id = alter.placement.clone();
                    applied.push(AppliedModelMutation {
                        index,
                        model: ModelName::from(&model_id),
                        message: format!(
                            "altered placement '{}' in domain '{}'",
                            model_id.as_str(),
                            domain.as_str(),
                        ),
                    });
                    mutations.push(RegistryMutation::AlterPlacement(alter));
                }
                Statement::Drop(drop) => {
                    let model_id = drop.name.clone();
                    refresh_http_tls |= drop.kind == ModelKind::Vhost;
                    applied.push(AppliedModelMutation {
                        index,
                        model: model_id.clone(),
                        message: format!(
                            "dropped model '{}' from domain '{}'",
                            model_id.as_str(),
                            domain.as_str()
                        ),
                    });
                    mutations.push(RegistryMutation::Drop(drop));
                }
                _ => unreachable!("model mutation batch contains a non-mutation statement"),
            }
        }

        let mut completed_result = None;
        if !mutations.is_empty() {
            let error_target = applied
                .first()
                .map(|mutation| mutation.model.clone())
                .verified(
                    "every arm that records a mutation records an applied model in the same step",
                );
            let planned = match self.inner.registry.plan_mutations(&domain, &mutations) {
                Ok(planned) => planned,
                Err(err) => {
                    warn!(
                        domain = domain.as_str(),
                        error = %err,
                        "failed to plan model mutation batch"
                    );
                    return create_registry_error_response(query, &domain, &error_target, &err);
                }
            };
            if let Err(error) = self
                .validate_changed_model_bindings(&domain, domain_state.config.pace, &planned)
                .await
            {
                return command_error(error);
            }
            let prepared_udfs = match self.prepare_planned_domain_udfs(&planned).await {
                Ok(prepared) => prepared,
                Err(error) => return command_error(error),
            };
            let base_classified_level = if let DomainStatus::Running = domain_state.status {
                planned.quiesce().level()
            } else {
                QuiesceLevel::Dynamic
            };
            let affected_entities = planned.quiesce().affected_entities().to_vec();
            let is_noop = planned.is_noop();
            let mut cluster_entity_gate = None;
            let mut ownership_handoff = None;
            let ScheduleTransition {
                expected_schedule,
                mut prepared_schedule,
                planned_relocations,
            } = if !is_noop {
                #[cfg(feature = "testing")]
                if self
                    .inner
                    .runtime
                    .take_armed_schedule_publication_fault(&domain)
                {
                    let error = format!(
                        "injected schedule publication fault for domain '{}'",
                        domain.as_str()
                    );
                    return CommandResult {
                        success: false,
                        message: format!(
                            "failed to publish schedule for domain '{}'",
                            domain.as_str()
                        ),
                        diagnostics: vec![Diagnostic {
                            message: error,
                            span_start: 0,
                            span_end: u32::try_from(query.len()).unwrap_or(0),
                        }],
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
                let expected_schedule = self
                    .inner
                    .consensus
                    .current_schedule()
                    .await
                    .domain(&domain)
                    .cloned();
                match self
                    .prepare_domain_schedule(
                        &domain,
                        planned.candidate_graph(),
                        domain_state.config.placement,
                    )
                    .await
                {
                    Ok(prepared) => ScheduleTransition {
                        expected_schedule,
                        prepared_schedule: prepared.schedule,
                        planned_relocations: prepared.relocations,
                    },
                    Err(error) => return command_error(error),
                }
            } else {
                ScheduleTransition::default()
            };
            let classified_level = if matches!(domain_state.status, DomainStatus::Running)
                && planned_relocations > 0
            {
                base_classified_level.max(QuiesceLevel::EntityPause)
            } else {
                base_classified_level
            };
            let requires_domain_pause = classified_level.requires_domain_pause();
            if let Some(prepared_schedule) = prepared_schedule.as_mut() {
                mark_complete_ownership_transitions(expected_schedule.as_ref(), prepared_schedule);
            }
            let transaction_schedule = transaction_step
                .is_some()
                .then(|| (expected_schedule.clone(), prepared_schedule.clone()));

            if is_noop {
                info!(
                    domain = domain.as_str(),
                    "model mutation batch has no model diff; skipping persistence and schedule \
                     publication"
                );
            }
            if !is_noop
                && requires_domain_pause
                && !adopted_domain_pause
                && let Err(error) = self.pause_and_drain_domain_for_alter(&domain).await
            {
                let response = match error.downcast_ref::<ConsensusError>() {
                    Some(cause) => {
                        self.consensus_error_response(cause, error.to_string())
                            .await
                    }
                    None => command_error(error.to_string()),
                };
                if response.kind == i32::from(CommandResultKind::NotLeader)
                    && let Some(step) = transaction_step.as_ref()
                {
                    *step.outcome.lock() = Some(Err(error.change_context(
                        TransactionCommitError::RecoverQuiescence {
                            id: step.transaction.id.clone(),
                        },
                    )));
                }
                return response;
            }
            if !is_noop && base_classified_level.requires_entity_pause() {
                let relays = self
                    .inner
                    .runtime
                    .entity_pause_relays(&domain, &affected_entities);
                let deadline =
                    tokio::time::Instant::now() + self.inner.runtime.entity_gate_deadline();
                let gate = match self
                    .engage_cluster_entity_gates(
                        &domain,
                        &relays,
                        &affected_entities,
                        EntityGatePurpose::ModelAlteration,
                        deadline,
                    )
                    .await
                {
                    Ok(gate) => gate,
                    Err(error) => return command_error(error.to_string()),
                };
                #[cfg(feature = "testing")]
                self.inner.runtime.pause_entity_gate_if_armed(&domain).await;
                if let Err(error) = self
                    .wait_for_cluster_entity_drain(
                        &gate,
                        &relays,
                        &affected_entities,
                        EntityGatePurpose::ModelAlteration,
                        &[],
                        deadline,
                    )
                    .await
                {
                    self.release_cluster_entity_gates(gate).await;
                    return command_error(error.to_string());
                }
                cluster_entity_gate = Some(gate);
            }
            if !is_noop && planned_relocations > 0 {
                ownership_handoff = match self
                    .begin_planned_ownership_handoff(
                        &domain,
                        expected_schedule.as_ref(),
                        prepared_schedule.as_ref(),
                    )
                    .await
                {
                    Ok(handoff) => handoff,
                    Err(error) => {
                        if let Some(gate) = cluster_entity_gate.take() {
                            self.release_cluster_entity_gates(gate).await;
                        }
                        return command_error(error.to_string());
                    }
                };
            }

            if !is_noop {
                let mut rollback_plan = Some(planned.clone());
                let _runtime_changes = match self.inner.registry.commit_planned(planned) {
                    Ok(changes) => changes,
                    Err(err) => {
                        if let Some(handoff) = ownership_handoff.take() {
                            self.abort_planned_ownership_handoff(&domain, handoff).await;
                        }
                        if let Some(gate) = cluster_entity_gate.take() {
                            self.release_cluster_entity_gates(gate).await;
                        }
                        if let RegistryError::ConcurrentMutation { .. } = err.current_context() {
                            error!(
                                domain = domain.as_str(),
                                error = %err,
                                "registry base-model CAS fired while the exclusive domain ALTER \
                                 lock was held"
                            );
                        }
                        let resume_error = if requires_domain_pause {
                            self.resume_domain_after_alter(&domain).await.err()
                        } else {
                            None
                        };
                        warn!(
                            domain = domain.as_str(),
                            error = %err,
                            "failed to apply model mutation batch"
                        );
                        if let Some(resume_error) = resume_error {
                            return command_error(format!(
                                "failed to apply model mutation batch: {err}; {resume_error}"
                            ));
                        }
                        return create_registry_error_response(query, &domain, &error_target, &err);
                    }
                };
                if let Some(prepared_udfs) = prepared_udfs {
                    self.inner
                        .runtime
                        .install_prepared_domain_udfs(&domain, prepared_udfs);
                }

                if let Some(transaction_step) = transaction_step.as_ref() {
                    let step_result = model_mutation_success_result(
                        &results,
                        &applied,
                        classified_level,
                        planned_relocations,
                    );
                    let effect = TransactionStepEffect::ReplaceDomainSchedule {
                        domain: domain.clone(),
                        expected_schedule: transaction_schedule
                            .as_ref()
                            .verified(
                                "the schedule is prepared exactly when a transaction step is \
                                 present, and this branch has one",
                            )
                            .0
                            .clone()
                            .map(Box::new),
                        schedule: transaction_schedule
                            .clone()
                            .verified(
                                "the schedule is prepared exactly when a transaction step is \
                                 present, and this branch has one",
                            )
                            .1
                            .map(Box::new),
                    };
                    match self
                        .record_transaction_step(
                            transaction_step.transaction,
                            transaction_step.first_statement,
                            transaction_step.statement_count,
                            step_result,
                            Some(classified_level),
                            Some(effect),
                        )
                        .await
                    {
                        Ok(transaction) => {
                            *transaction_step.outcome.lock() = Some(Ok(transaction));
                            let activation_error = self.apply_current_cluster_state().await.err();
                            if let Some(handoff) = ownership_handoff.take() {
                                if let Some(error) = &activation_error {
                                    self.defer_planned_ownership_handoff_release(
                                        &domain, handoff, error,
                                    );
                                } else {
                                    if let Err(error) = self
                                        .finish_planned_ownership_handoff(&domain, handoff)
                                        .await
                                    {
                                        self.broadcast_error(format!(
                                            "failed to confirm ownership state activation for \
                                             committed transaction model step in domain '{}': \
                                             {error}",
                                            domain.as_str()
                                        ));
                                    }
                                }
                            }
                            if let Some(error) = activation_error {
                                self.broadcast_error(format!(
                                    "failed to reconcile committed transaction model step in \
                                     domain '{}': {error}",
                                    domain.as_str()
                                ));
                            }
                        }
                        Err(error) => {
                            if let Some(handoff) = ownership_handoff.take() {
                                self.abort_planned_ownership_handoff(&domain, handoff).await;
                            }
                            if let Some(gate) = cluster_entity_gate.take() {
                                self.release_cluster_entity_gates(gate).await;
                            }
                            let rollback_error = if let Some(plan) = rollback_plan.take() {
                                match self.inner.registry.rollback_committed(plan) {
                                    Ok(_) => None,
                                    Err(rollback) => Some(rollback.to_string()),
                                }
                            } else {
                                None
                            };
                            let resume_error = if requires_domain_pause {
                                self.resume_domain_after_alter(&domain).await.err()
                            } else {
                                None
                            };
                            let error = match rollback_error {
                                Some(rollback) => error.attach(format!(
                                    "local registry rollback also failed: {rollback}"
                                )),
                                None => error,
                            };
                            let error = match resume_error {
                                Some(resume) => error
                                    .attach(format!("the domain also remains paused: {resume}")),
                                None => error,
                            };
                            let message = format!(
                                "failed to atomically publish transaction model step for domain \
                                 '{}': {error}",
                                domain.as_str()
                            );
                            *transaction_step.outcome.lock() = Some(Err(error));
                            return command_error(message);
                        }
                    }
                } else {
                    if let Err(error) = self
                        .inner
                        .consensus
                        .replace_domain_schedule(
                            domain.clone(),
                            expected_schedule.clone(),
                            prepared_schedule.clone(),
                        )
                        .await
                    {
                        let err = error.to_string();
                        if let Some(handoff) = ownership_handoff.take() {
                            self.abort_planned_ownership_handoff(&domain, handoff).await;
                        }
                        if let Some(gate) = cluster_entity_gate.take() {
                            self.release_cluster_entity_gates(gate).await;
                        }
                        if let Some(rollback_plan) = rollback_plan.take()
                            && let Err(rollback_error) = self
                                .rollback_model_alteration(&domain, rollback_plan, classified_level)
                                .await
                        {
                            return self
                                .consensus_error_response(
                                    &error,
                                    format!(
                                        "failed to publish model alteration schedule for domain \
                                         '{}': {err}; {rollback_error}",
                                        domain.as_str()
                                    ),
                                )
                                .await;
                        }
                        self.broadcast_error(format!(
                            "schedule publish failed in domain '{}': {}",
                            domain.as_str(),
                            err
                        ));
                        warn!(
                            domain = domain.as_str(),
                            error = %err,
                            "failed to publish schedule for model mutation batch"
                        );
                        if let ConsensusError::LeadershipLost { leader_id } = &error {
                            return self.not_leader_response(query, leader_id.clone()).await;
                        }
                        return CommandResult {
                            success: false,
                            message: format!(
                                "failed to publish schedule for domain '{}'",
                                domain.as_str()
                            ),
                            diagnostics: vec![Diagnostic {
                                message: err,
                                span_start: 0,
                                span_end: u32::try_from(query.len()).unwrap_or(0),
                            }],
                            kind: i32::from(CommandResultKind::Error),
                            ..Default::default()
                        };
                    }
                    if let Err(error) = self.apply_current_cluster_state().await {
                        if let Some(handoff) = ownership_handoff.take() {
                            self.defer_planned_ownership_handoff_release(&domain, handoff, &error);
                        }
                        if let Some(gate) = cluster_entity_gate.take() {
                            self.release_cluster_entity_gates(gate).await;
                        }
                        let paused = if requires_domain_pause {
                            match self.resume_domain_after_alter(&domain).await {
                                Ok(()) => String::new(),
                                Err(resume) => {
                                    format!("; the domain also remains paused: {resume}")
                                }
                            }
                        } else {
                            String::new()
                        };
                        return command_error(format!(
                            "committed models and schedule for domain '{}', but the destination \
                             failed to activate: {error}{paused}",
                            domain.as_str()
                        ));
                    }
                    if let Some(handoff) = ownership_handoff.take()
                        && let Err(error) = self
                            .finish_planned_ownership_handoff(&domain, handoff)
                            .await
                    {
                        if let Some(gate) = cluster_entity_gate.take() {
                            self.release_cluster_entity_gates(gate).await;
                        }
                        let paused = if requires_domain_pause {
                            match self.resume_domain_after_alter(&domain).await {
                                Ok(()) => String::new(),
                                Err(resume) => {
                                    format!("; the domain also remains paused: {resume}")
                                }
                            }
                        } else {
                            String::new()
                        };
                        return command_error(format!(
                            "committed models and schedule for domain '{}', but ownership state \
                             activation did not complete: {error}{paused}",
                            domain.as_str(),
                        ));
                    }
                }

                if requires_domain_pause {
                    if let Err(error) = self.wait_for_paused_domain_drain(&domain).await {
                        if transaction_step.is_some() {
                            self.broadcast_error(format!(
                                "committed transaction model step in domain '{}' is waiting for \
                                 quiescence recovery: {error}",
                                domain.as_str()
                            ));
                        } else {
                            if let Some(rollback_plan) = rollback_plan.take()
                                && let Err(rollback_error) = self
                                    .rollback_model_alteration(
                                        &domain,
                                        rollback_plan,
                                        classified_level,
                                    )
                                    .await
                            {
                                return command_error(format!("{error}; {rollback_error}"));
                            }
                            return command_error(error.to_string());
                        }
                    }
                    if let Err(error) = self.resume_domain_after_alter(&domain).await {
                        if transaction_step.is_some() {
                            self.broadcast_error(format!(
                                "failed to release transaction-owned pause in domain '{}': {error}",
                                domain.as_str()
                            ));
                        } else {
                            if let Some(rollback_plan) = rollback_plan.take()
                                && let Err(rollback_error) = self
                                    .rollback_model_alteration(
                                        &domain,
                                        rollback_plan,
                                        classified_level,
                                    )
                                    .await
                            {
                                return command_error(format!("{error}; {rollback_error}"));
                            }
                            return command_error(error.to_string());
                        }
                    }
                }
            }
            if let Some(gate) = cluster_entity_gate {
                self.release_cluster_entity_gates(gate).await;
            }

            if refresh_http_tls && let Err(error) = self.refresh_http_tls_server_config().await {
                self.broadcast_error(format!("failed to refresh HTTP TLS config: {error}"));
            }
            completed_result = Some(model_mutation_success_result(
                &results,
                &applied,
                classified_level,
                planned_relocations,
            ));
        }

        let result = if let Some(result) = completed_result {
            result
        } else {
            model_mutation_success_result(&results, &applied, QuiesceLevel::Dynamic, 0)
        };
        if let Some(transaction_step) = transaction_step
            && transaction_step.outcome.lock().is_none()
        {
            match self
                .record_transaction_step(
                    transaction_step.transaction,
                    transaction_step.first_statement,
                    transaction_step.statement_count,
                    result.clone(),
                    Some(QuiesceLevel::Dynamic),
                    None,
                )
                .await
            {
                Ok(transaction) => {
                    *transaction_step.outcome.lock() = Some(Ok(transaction));
                }
                Err(error) => {
                    let message =
                        format!("failed to record transaction model step progress: {error}");
                    *transaction_step.outcome.lock() = Some(Err(error));
                    return command_error(message);
                }
            }
        }
        result
    }

    async fn process_client_statement(
        &self,
        client_statement: ClientStatement,
        query: &str,
        request_domain: &str,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let statement = match client_statement {
            ClientStatement::UseDomain(domain) => {
                return command_error(format!(
                    "USE '{}' is a client-local command and must not be sent to the server",
                    domain.as_str()
                ));
            }
            ClientStatement::ListDomains => {
                return command_error(
                    "LIST DOMAINS is a protobuf-level client command".to_string(),
                );
            }
            ClientStatement::UploadResource(upload) => {
                return self.upload_resource_command(upload).await;
            }
            ClientStatement::CreateSubscription(subscription) => {
                let domain = match parse_request_domain(request_domain) {
                    Ok(domain) => domain,
                    Err(RequestDomainError::Missing) => {
                        return command_error("no active domain selected".to_string());
                    }
                    Err(RequestDomainError::Invalid) => {
                        return command_error("invalid domain".to_string());
                    }
                };
                if self.inner.consensus.current_domain(&domain).await.is_none() {
                    return command_error(format!("domain '{}' does not exist", domain.as_str()));
                }
                if let Err(error) = self.reconcile_running_domain_runtime(&domain).await {
                    return command_error(error);
                }
                return self
                    .create_subscription(&domain, subscription, tx, subscriptions)
                    .await;
            }
            ClientStatement::DeleteSubscription(subscription) => {
                return self.delete_subscription(subscription, subscriptions).await;
            }
            ClientStatement::BeginTransaction
            | ClientStatement::CommitTransaction
            | ClientStatement::RevertTransaction => {
                return command_error(
                    "transaction control commands must be handled by the session transaction"
                        .to_string(),
                );
            }
            ClientStatement::Server(statement) => statement,
        };
        if statement.is_model_mutation() {
            return self
                .process_model_mutation_batch(vec![statement], query, request_domain)
                .await;
        }

        let domain = if requires_request_domain(&statement) {
            match parse_request_domain(request_domain) {
                Ok(domain) => Some(domain),
                Err(RequestDomainError::Missing) => {
                    return command_error("no active domain selected".to_string());
                }
                Err(RequestDomainError::Invalid) => {
                    return command_error("invalid domain".to_string());
                }
            }
        } else {
            match parse_request_domain(request_domain) {
                Ok(domain) => Some(domain),
                Err(RequestDomainError::Missing) => None,
                Err(RequestDomainError::Invalid) => {
                    return command_error("invalid domain".to_string());
                }
            }
        };

        if requires_leader(&statement) {
            let leader = self.inner.consensus.current_leader().await;
            if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
                return self.not_leader_response(query, leader).await;
            }
            #[cfg(feature = "testing")]
            self.inner
                .runtime
                .pause_command_admission_if_armed(self.inner.consensus.local_node_id())
                .await;
        }

        if requires_existing_domain(&statement) {
            let domain = domain
                .as_ref()
                .verified("this statement requires a request domain, which was resolved above");
            if self.inner.consensus.current_domain(domain).await.is_none() {
                return command_error(format!("domain '{}' does not exist", domain.as_str()));
            }
        }

        if requires_runtime_reconcile(&statement) {
            let domain = domain
                .as_ref()
                .verified("this statement requires a request domain, which was resolved above");
            if let Err(error) = self.reconcile_running_domain_runtime(domain).await {
                return command_error(error);
            }
        }

        match statement {
            Statement::CreateDomain(create) => self.create_domain(create).await,
            Statement::AlterDomain(alter) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.alter_domain(domain, alter).await
            }
            Statement::CreateUser(create) => self.create_user(create).await,
            Statement::CreateResource(create) => {
                self.create_resource(
                    domain.as_ref().verified(
                        "this statement requires a request domain, which was resolved above",
                    ),
                    create,
                )
                .await
            }
            Statement::UploadResource(upload) => self.upload_resource_command(upload).await,
            Statement::StartDomain(start) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.start_domain(domain, start).await
            }
            Statement::StopDomain(stop) => {
                // `STOP` names no domain, so it acts on the session's domain. It is excluded from
                // `requires_request_domain`, which leaves the session free of one here.
                let Some(domain) = domain.as_ref() else {
                    return command_error("no active domain selected".to_string());
                };
                self.stop_domain(domain, stop).await
            }
            Statement::Create(_)
            | Statement::AlterSchema(_)
            | Statement::AlterWireJsonSchema(_)
            | Statement::AlterWireCborSchema(_)
            | Statement::AlterWireAvroSchema(_)
            | Statement::AlterRelay(_)
            | Statement::AlterJunction(_)
            | Statement::AlterDeduplicator(_)
            | Statement::AlterReorderer(_)
            | Statement::AlterEmitter(_)
            | Statement::AlterIngestor(_)
            | Statement::AlterReingestor(_)
            | Statement::AlterGenerator(_)
            | Statement::AlterPlacement(_)
            | Statement::Drop(_) => {
                unreachable!("model mutations are handled before statement dispatch")
            }
            Statement::DropNode(drop) => self.drop_node(drop.node_id).await,
            Statement::CordonNode(cordon) => self.set_node_cordoned(cordon.node_id, true).await,
            Statement::UncordonNode(uncordon) => {
                self.set_node_cordoned(uncordon.node_id, false).await
            }
            Statement::DrainNode(drain) => self.drain_node(drain.node_id).await,
            Statement::Relocate(relocation) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.relocate(domain, relocation).await
            }
            Statement::DescribeRelocation(relocation) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_relocation(domain, relocation).await
            }
            Statement::DescribeRelay(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_stream(domain, describe).await
            }
            Statement::DescribeDomain(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_domain(domain, describe).await
            }
            Statement::DescribeEndpoint(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_endpoint(domain, describe).await
            }
            Statement::DescribeIngestor(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_ingestor(domain, describe).await
            }
            Statement::DescribeLookup(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_lookup(domain, describe).await
            }
            Statement::DescribeJunction(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_junction(domain, describe).await
            }
            Statement::DescribeDeduplicator(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_deduplicator(domain, describe).await
            }
            Statement::DescribeReingestor(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_reingestor(domain, describe).await
            }
            Statement::DescribeCorrelator(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_correlator(domain, describe).await
            }
            Statement::DescribeReorderer(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_reorderer(domain, describe).await
            }
            Statement::DescribeEmitter(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_emitter(domain, describe).await
            }
            Statement::DescribeWindowProcessor(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_window_processor(domain, describe).await
            }
            Statement::DescribeWasmProcessor(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_wasm_processor(domain, describe).await
            }
            Statement::DescribeUdf(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_udf(domain, describe)
            }
            Statement::DescribePlacement(describe) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.describe_placement(domain, describe).await
            }
            Statement::DescribeResource(describe) => {
                self.describe_resource(
                    domain.as_ref().verified(
                        "this statement requires a request domain, which was resolved above",
                    ),
                    describe,
                )
                .await
            }
            Statement::LookupQuery(query) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.lookup_query(domain, query).await
            }
            Statement::ShowCreate(show) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                let name_span = find_identifier_span(query, &show.name).unwrap_or(0..0);
                let model = match self
                    .inner
                    .registry
                    .get_of_kind(domain, show.kind, &show.name)
                {
                    Ok(Some(model)) => model,
                    Ok(None) => {
                        return CommandResult {
                            success: false,
                            message: format!(
                                "{} '{}' does not exist in domain '{}'",
                                show.kind.as_str(),
                                show.name.as_str(),
                                domain.as_str()
                            ),
                            diagnostics: vec![Diagnostic {
                                message: format!(
                                    "{} '{}' not found",
                                    show.kind.as_str(),
                                    show.name.as_str()
                                ),
                                span_start: u32::try_from(name_span.start).unwrap_or(0),
                                span_end: u32::try_from(name_span.end).unwrap_or(0),
                            }],
                            kind: i32::from(CommandResultKind::Error),
                            ..Default::default()
                        };
                    }
                    Err(_) => {
                        return CommandResult {
                            success: false,
                            message: "failed to read stored model for SHOW CREATE".to_string(),
                            diagnostics: vec![Diagnostic {
                                message: "failed to read stored model for SHOW CREATE".to_string(),
                                span_start: 0,
                                span_end: 0,
                            }],
                            kind: i32::from(CommandResultKind::Error),
                            ..Default::default()
                        };
                    }
                };

                let canonical = match model.to_canonical_nspl() {
                    Ok(v) => v,
                    Err(_) => {
                        return CommandResult {
                            success: false,
                            message: "failed to render canonical NSPL".to_string(),
                            diagnostics: vec![Diagnostic {
                                message: "model contains values that cannot be rendered as \
                                          canonical NSPL"
                                    .to_string(),
                                span_start: 0,
                                span_end: 0,
                            }],
                            kind: i32::from(CommandResultKind::Error),
                            ..Default::default()
                        };
                    }
                };

                CommandResult {
                    success: true,
                    message: canonical,
                    diagnostics: Vec::new(),
                    kind: i32::from(CommandResultKind::Ok),
                    ..Default::default()
                }
            }
            Statement::ShowRelayMaterializedState(show) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.show_stream_materialized_state(domain, show).await
            }
            Statement::ShowUdfs(_) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.show_udfs(domain)
            }
            Statement::ShowPlacements(_) => {
                let domain = domain
                    .as_ref()
                    .verified("this statement requires a request domain, which was resolved above");
                self.show_placements(domain).await
            }
            Statement::ShowClusterStatus(_) => CommandResult {
                success: true,
                message: render_cluster_status(&self.inner.cluster, &self.inner.consensus).await,
                diagnostics: Vec::new(),
                kind: i32::from(CommandResultKind::Ok),
                ..Default::default()
            },
            Statement::ShowTransactions(_) => self.show_transactions().await,
        }
    }

    async fn show_transactions(&self) -> CommandResult {
        let now = current_timestamp();
        let transactions = self.inner.consensus.current_transactions().await;
        let message = if transactions.is_empty() {
            "no transactions".to_string()
        } else {
            transactions
                .values()
                .map(|transaction| {
                    let age = now
                        .as_datetime()
                        .signed_duration_since(*transaction.created_at.as_datetime())
                        .to_std()
                        .unwrap_or_default();
                    let idle = now
                        .as_datetime()
                        .signed_duration_since(*transaction.last_activity_at.as_datetime())
                        .to_std()
                        .unwrap_or_default();
                    format!(
                        "id={} owner={} domain={} state={} pending={} progress={}/{} age={} \
                         idle={}",
                        transaction.id,
                        transaction.owner.as_str(),
                        transaction.domain.as_str(),
                        transaction.state.as_str(),
                        transaction.pending_statement_count(),
                        transaction.completed_statement_count(),
                        transaction.statement_count,
                        humantime::format_duration(age),
                        humantime::format_duration(idle),
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        };
        command_ok(message)
    }

    async fn handle_web_console_resource_upload(
        &self,
        request: HyperRequest<HyperIncoming>,
    ) -> HyperResponse<Full<Bytes>> {
        let Some(resource_name) = web_console_query_param(request.uri().query(), "resource") else {
            return web_console_upload_text_response(
                StatusCode::BAD_REQUEST,
                "missing resource query parameter",
            );
        };
        let identifier = match ResourceName::parse(resource_name.trim()) {
            Ok(identifier) => identifier,
            Err(_) => {
                return web_console_upload_text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid resource name",
                );
            }
        };
        let Some(domain_name) = web_console_query_param(request.uri().query(), "domain") else {
            return web_console_upload_text_response(
                StatusCode::BAD_REQUEST,
                "missing domain query parameter",
            );
        };
        let domain = match DomainName::parse(domain_name.trim()) {
            Ok(domain) => domain,
            Err(_) => {
                return web_console_upload_text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid domain name",
                );
            }
        };
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            return web_console_upload_text_response(
                StatusCode::CONFLICT,
                "resource uploads must be sent to the cluster leader",
            );
        }
        let resources = self.inner.consensus.current_resources().await;
        if !resources.is_declared(&domain, &identifier) {
            return web_console_upload_text_response(
                StatusCode::NOT_FOUND,
                format!("resource '{}' does not exist", identifier.as_str()),
            );
        }
        let Some(content_type) = request
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
        else {
            return web_console_upload_text_response(
                StatusCode::BAD_REQUEST,
                "missing multipart content type",
            );
        };
        let boundary = match multer::parse_boundary(content_type) {
            Ok(boundary) => boundary,
            Err(_) => {
                return web_console_upload_text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid multipart boundary",
                );
            }
        };

        match self
            .stage_web_console_resource_upload(request, boundary, ModelName::from(&identifier))
            .await
        {
            Ok((archive_path, root_checksum)) => match self
                .install_uploaded_resource_archive(
                    &domain,
                    ModelName::from(&identifier),
                    &archive_path,
                    root_checksum,
                )
                .await
            {
                Ok(version) => web_console_upload_text_response(
                    StatusCode::OK,
                    format!("uploaded resource version {version}"),
                ),
                Err(error) => {
                    let status = if let Some(ConsensusError::LeadershipLost { .. }) =
                        error.downcast_ref::<ConsensusError>()
                    {
                        StatusCode::CONFLICT
                    } else {
                        StatusCode::BAD_REQUEST
                    };
                    web_console_upload_text_response(status, format!("{error:#}"))
                }
            },
            Err((status, message)) => web_console_upload_text_response(status, message),
        }
    }

    async fn stage_web_console_resource_upload(
        &self,
        request: HyperRequest<HyperIncoming>,
        boundary: String,
        identifier: ModelName,
    ) -> Result<(TempPath, String), (StatusCode, String)> {
        let upload_dir = tempfile::tempdir().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create temporary upload directory".to_string(),
            )
        })?;
        let stream = request.into_body().into_data_stream().map(|result| {
            result.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
        });
        let mut multipart = multer::Multipart::new(stream, boundary);
        let mut file_count = 0_u64;
        while let Some(mut field) = multipart.next_field().await.map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                format!("failed to read multipart field: {error}"),
            )
        })? {
            tokio::task::consume_budget().await;
            if field.name() != Some("file") {
                continue;
            }
            let Some(file_name) = field.file_name().and_then(sanitized_upload_relative_path) else {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "upload contains an invalid file path".to_string(),
                ));
            };
            let destination = upload_dir.path().join(file_name);
            if let Some(parent) = destination.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to create temporary upload subdirectory".to_string(),
                    )
                })?;
            }
            let mut file = File::create(&destination).await.map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to create temporary uploaded file".to_string(),
                )
            })?;
            while let Some(chunk) = field.chunk().await.map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("failed to read uploaded file chunk: {error}"),
                )
            })? {
                tokio::task::consume_budget().await;
                file.write_all(&chunk).await.map_err(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to write temporary uploaded file".to_string(),
                    )
                })?;
            }
            file.flush().await.map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to flush temporary uploaded file".to_string(),
                )
            })?;
            file_count = file_count
                .checked_add(1)
                .assured("the files counted here were each written to the local filesystem");
        }
        if file_count == 0 {
            return Err((
                StatusCode::BAD_REQUEST,
                "upload contains no files".to_string(),
            ));
        }

        build_web_console_upload_archive(upload_dir.path(), identifier).await
    }

    async fn process_web_console_request(
        &self,
        request: SessionRequest,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> SessionResponse {
        match request.request {
            Some(proto::session_request::Request::Command(command)) => {
                let result = self
                    .process_web_console_command(command, tx, subscriptions)
                    .await;
                SessionResponse {
                    event: Some(proto::session_response::Event::Result(result)),
                }
            }
            Some(proto::session_request::Request::Suggest(suggest)) => {
                let response = self.process_suggest(suggest, subscriptions).await;
                SessionResponse {
                    event: Some(proto::session_response::Event::Suggest(response)),
                }
            }
            Some(proto::session_request::Request::ListDomains(_)) => {
                self.domain_list_response(true).await
            }
            Some(proto::session_request::Request::SetActiveDomain(_)) => {
                web_console_server_error_response(
                    "active domain requests are handled by the websocket session".to_string(),
                )
            }
            Some(proto::session_request::Request::AttachTransaction(request)) => {
                let result = self.attach_transaction(request, subscriptions).await;
                SessionResponse {
                    event: Some(proto::session_response::Event::Result(result)),
                }
            }
            None => {
                web_console_server_error_response("session request payload is missing".to_string())
            }
        }
    }

    async fn process_web_console_active_domain_request(
        &self,
        request: SetActiveDomainRequest,
        active_domain: &mut Option<DomainName>,
    ) -> Result<SessionResponse, ActiveDomainError> {
        let domain = match DomainName::parse(request.domain.trim()) {
            Ok(domain) => domain,
            Err(_) => return Err(ActiveDomainError::Invalid),
        };
        if self.inner.consensus.current_domain(&domain).await.is_none() {
            return Err(ActiveDomainError::NotFound { domain });
        }
        *active_domain = Some(domain.clone());
        Ok(SessionResponse {
            event: Some(proto::session_response::Event::Server(ServerEvent {
                level: i32::from(ServerEventLevel::Info),
                message: format!("using domain '{}'", domain.as_str()),
            })),
        })
    }

    async fn process_web_console_command(
        &self,
        req: CommandRequest,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        if let Ok(statements) = parse_client_statements(&req.query)
            && statements.iter().any(|statement| {
                let ClientStatement::UploadResource(_) = statement else {
                    return false;
                };
                true
            })
        {
            return self
                .command_with_transaction_status(
                    command_error(
                        "UPLOAD RESOURCE is not supported in the web console".to_string(),
                    ),
                    subscriptions,
                )
                .await;
        }

        self.process_command(req, tx, subscriptions).await
    }

    async fn reconcile_running_domain_runtime(&self, domain: &DomainName) -> Result<(), String> {
        let state = self.inner.consensus.current_runtime_state().await;
        let Some(domain_state) = state.domains.get(domain) else {
            return Ok(());
        };
        if !matches!(domain_state.status, DomainStatus::Running) {
            return Ok(());
        }
        self.inner
            .runtime
            .apply_cluster_state(
                self.inner.consensus.local_node_id(),
                state.revision,
                &state.domains,
                &state.domain_clock_authorities,
                &state.schedule,
            )
            .await
            .map_err(|error| {
                format!(
                    "failed to restore runtime for running domain '{}': {error}",
                    domain.as_str()
                )
            })?;
        self.inner
            .runtime
            .start_running_domain_ingestors()
            .await
            .map_err(|error| {
                format!(
                    "failed to restore runtime for running domain '{}': {error}",
                    domain.as_str()
                )
            })
    }

    async fn create_domain(&self, create: CreateStatement<CreateDomain>) -> CommandResult {
        if self
            .inner
            .consensus
            .current_domain(&create.id)
            .await
            .is_some()
        {
            if create.if_not_exists {
                return command_ok_already_existed(format!(
                    "domain '{}' already exists",
                    create.id.as_str()
                ));
            }
            return command_error(format!("domain '{}' already exists", create.id.as_str()));
        }
        if let Err(message) = validate_domain_config(&create.config) {
            return command_error(message);
        }
        let create = create.body;
        let state = DomainState {
            id: create.id.clone(),
            config: create.config,
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: DomainStartPoint::Resume,
            clock: None,
        };
        match self.inner.consensus.put_domain(state).await {
            Ok(()) => {
                if let Err(error) = self.apply_current_cluster_state().await {
                    self.broadcast_error(format!(
                        "failed to reconcile runtime after creating domain '{}': {error}",
                        create.id.as_str(),
                    ));
                }
                command_ok(format!("created domain '{}'", create.id.as_str()))
            }
            Err(error) => {
                self.consensus_error_response(
                    &error,
                    format!("failed to create domain '{}': {error}", create.id.as_str()),
                )
                .await
            }
        }
    }

    async fn alter_domain(&self, domain: &DomainName, alter: AlterDomain) -> CommandResult {
        let _alter_guard = match self.inner.runtime.try_begin_domain_alter(domain) {
            Some(guard) => guard,
            None => {
                return command_error(
                    DomainAlterError::ConcurrentAlter {
                        domain: domain.clone(),
                    }
                    .to_string(),
                );
            }
        };
        let Some(previous_state) = self.inner.consensus.current_domain(domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        if let DomainStatus::Paused = previous_state.status {
            return command_error(format!(
                "domain '{}' is paused by a model alteration",
                domain.as_str()
            ));
        }
        if previous_state.config.placement == alter.policy {
            return command_ok(format!(
                "domain '{}' placement is already {}; {}\nplanned relocations: 0",
                domain.as_str(),
                alter.policy.as_ref(),
                quiesce_level_message(QuiesceLevel::Dynamic),
            ));
        }

        let current_schedule = self.inner.consensus.current_schedule().await;
        let previous_schedule = current_schedule.domain(domain).cloned();
        let live_node_ids = self.inner.cluster.live_node_ids().await;
        let live_voters = self
            .inner
            .consensus
            .live_voter_ids(live_node_ids.clone())
            .await;
        let cluster_nodes = self
            .inner
            .consensus
            .schedulable_live_voter_ids(live_node_ids)
            .await;
        let mut next_schedule = self.inner.registry.active_graph(domain).map(|graph| {
            #[cfg(feature = "testing")]
            let mut schedule = graph.schedule_for_domain_with_mode(
                domain,
                &cluster_nodes,
                self.inner.replica_count,
                alter.policy,
                self.inner.runtime.scheduler_mode(),
            );
            #[cfg(not(feature = "testing"))]
            let mut schedule = graph.schedule_for_domain(
                domain,
                &cluster_nodes,
                self.inner.replica_count,
                alter.policy,
            );
            Self::merge_existing_schedule_data(
                &mut schedule,
                previous_schedule.as_ref(),
                &live_voters,
            );
            schedule
        });
        if let Some(next_schedule) = next_schedule.as_mut() {
            mark_complete_ownership_transitions(previous_schedule.as_ref(), next_schedule);
        }
        let relocations =
            planned_relocation_count(previous_schedule.as_ref(), next_schedule.as_ref());
        let quiesce_level =
            if matches!(previous_state.status, DomainStatus::Running) && relocations > 0 {
                QuiesceLevel::EntityPause
            } else {
                QuiesceLevel::Dynamic
            };
        let mut next_state = previous_state.clone();
        next_state.config.placement = alter.policy;

        #[cfg(feature = "testing")]
        if self
            .inner
            .runtime
            .take_armed_schedule_publication_fault(domain)
        {
            return command_error(format!(
                "injected schedule publication fault for domain '{}'",
                domain.as_str()
            ));
        }
        let handoff = if relocations > 0 {
            self.begin_planned_ownership_handoff(
                domain,
                previous_schedule.as_ref(),
                next_schedule.as_ref(),
            )
            .await
        } else {
            Ok(None)
        };
        let handoff = match handoff {
            Ok(handoff) => handoff,
            Err(error) => return command_error(error.to_string()),
        };
        if let Err(error) = self
            .inner
            .consensus
            .put_domain_and_schedule(
                Some(previous_state),
                previous_schedule,
                next_state,
                next_schedule,
            )
            .await
        {
            if let Some(handoff) = handoff {
                self.abort_planned_ownership_handoff(domain, handoff).await;
            }
            return self
                .consensus_error_response(
                    &error,
                    format!(
                        "failed to alter placement for domain '{}': {error}",
                        domain.as_str()
                    ),
                )
                .await;
        }
        let activation_error = self.apply_current_cluster_state().await.err();
        if let Some(error) = activation_error {
            if let Some(handoff) = handoff {
                self.defer_planned_ownership_handoff_release(domain, handoff, &error);
            }
            return command_error(format!(
                "committed placement and schedule for domain '{}', but the destination failed to \
                 activate: {error}",
                domain.as_str()
            ));
        }
        if let Some(handoff) = handoff
            && let Err(error) = self.finish_planned_ownership_handoff(domain, handoff).await
        {
            return command_error(format!(
                "committed placement and schedule for domain '{}', but ownership state activation \
                 did not complete: {error}",
                domain.as_str()
            ));
        }

        command_ok(format!(
            "set domain '{}' placement to {}; {}\nplanned relocations: {relocations}",
            domain.as_str(),
            alter.policy.as_ref(),
            quiesce_level_message(quiesce_level),
        ))
    }

    async fn create_user(&self, create: CreateStatement<CreateUser>) -> CommandResult {
        let if_not_exists = create.if_not_exists;
        let create = create.body;
        if self
            .inner
            .consensus
            .current_user(&create.name)
            .await
            .is_some()
        {
            if if_not_exists {
                return command_ok_already_existed(format!(
                    "user '{}' already exists",
                    create.name.as_str()
                ));
            }
            return command_error(format!("user '{}' already exists", create.name.as_str()));
        }
        let user = match user_credentials(create.name.clone(), create.password).await {
            Ok(user) => user,
            Err(error) => {
                return command_error(format!(
                    "failed to hash password for user '{}': {error}",
                    create.name.as_str()
                ));
            }
        };
        match self.inner.consensus.create_user(user).await {
            Ok(()) => command_ok(format!("created user '{}'", create.name.as_str())),
            Err(error) => {
                self.consensus_error_response(
                    &error,
                    format!("failed to create user '{}': {error}", create.name.as_str()),
                )
                .await
            }
        }
    }

    async fn create_resource(
        &self,
        domain: &DomainName,
        create: CreateStatement<CreateResource>,
    ) -> CommandResult {
        let resources = self.inner.consensus.current_resources().await;
        if resources.is_declared(domain, &create.identifier) {
            if create.if_not_exists {
                return command_ok_already_existed(format!(
                    "resource '{}' already exists",
                    create.identifier.as_str()
                ));
            }
            return command_error(format!(
                "resource '{}' already exists",
                create.identifier.as_str()
            ));
        }
        match self
            .inner
            .consensus
            .create_resource_catalog(domain, &create.identifier)
            .await
        {
            Ok(()) => command_ok(format!("created resource '{}'", create.identifier.as_str())),
            Err(error) => {
                self.consensus_error_response(
                    &error,
                    format!(
                        "failed to create resource '{}': {error}",
                        create.identifier.as_str()
                    ),
                )
                .await
            }
        }
    }

    async fn upload_resource_command(&self, upload: UploadResource) -> CommandResult {
        command_error(format!(
            "UPLOAD RESOURCE '{}' must be executed by a client that supports local uploads from \
             '{}'",
            upload.identifier.as_str(),
            upload.source_path
        ))
    }

    async fn install_uploaded_resource_archive(
        &self,
        domain: &DomainName,
        identifier: ModelName,
        archive_path: &Path,
        root_checksum: String,
    ) -> Result<u64, Report<ResourceUploadError>> {
        let created_at = current_timestamp();
        let version = self
            .inner
            .consensus
            .allocate_resource_version(domain, &ResourceName::from(&identifier))
            .await
            .change_context(ResourceUploadError::AllocateVersion {
                identifier: identifier.clone(),
            })?;
        let id = ResourceId::new(domain.clone(), ResourceName::from(&identifier), version);
        let manifest = self
            .inner
            .resource_store
            .install_from_archive_path(
                id.clone(),
                archive_path,
                root_checksum,
                self.inner.consensus.local_node_id().clone(),
                created_at,
            )
            .await
            .change_context(ResourceUploadError::InstallArchive { identifier })?;

        if let Err(error) = self
            .inner
            .consensus
            .put_resource_version(manifest.resource.clone())
            .await
        {
            let cleanup_suffix = match self
                .inner
                .resource_store
                .remove_version(&manifest.resource.id)
            {
                Ok(()) => String::new(),
                Err(cleanup_error) => {
                    format!("; local cleanup also failed: {cleanup_error}")
                }
            };
            return Err(
                Report::new(error).change_context(ResourceUploadError::PublishVersion {
                    id: manifest.resource.id,
                    cleanup_suffix,
                }),
            );
        }

        self.inner
            .consensus
            .put_resource_replica(ResourceNodeStatus {
                key: ResourceReplicaKey::new(
                    manifest.resource.id.domain.clone(),
                    manifest.resource.id.identifier.clone(),
                    manifest.resource.id.version,
                    self.inner.consensus.local_node_id().clone(),
                ),
                state: ResourceNodeState::Ready,
                root_checksum: Some(manifest.resource.root_checksum.clone()),
                last_verified_at: Some(created_at),
                source_node_id: Some(self.inner.consensus.local_node_id().clone()),
                error: None,
            })
            .await
            .change_context(ResourceUploadError::PublishReplica {
                id: manifest.resource.id.clone(),
            })?;

        self.wait_for_resource_cluster_ready(&manifest.resource.id)
            .await
            .map_err(|reason| {
                Report::new(ResourceUploadError::WaitForReplicas {
                    id: manifest.resource.id.clone(),
                    reason,
                })
            })?;
        self.inner
            .runtime
            .sync_resource_versions(&self.inner.consensus.current_resources().await);
        if let Err(error) = self.refresh_http_tls_server_config().await {
            self.broadcast_error(format!("failed to refresh HTTP TLS config: {error}"));
        }
        Ok(manifest.resource.id.version)
    }

    async fn start_domain(&self, domain_id: &DomainName, start: StartDomain) -> CommandResult {
        let Some(domain) = self.inner.consensus.current_domain(domain_id).await else {
            return command_error(format!("domain '{}' does not exist", domain_id.as_str()));
        };
        if let Err(message) = validate_domain_config(&domain.config) {
            return command_error(message);
        }
        if let DomainStatus::Running = domain.status {
            return command_error(format!(
                "domain '{}' is already running",
                domain_id.as_str()
            ));
        }
        if let DomainStatus::Paused = domain.status {
            return command_error(format!(
                "domain '{}' is paused for a model alteration",
                domain_id.as_str()
            ));
        }
        let wall_started_at = current_timestamp();
        let (mut logical_start, time_rate) = start.start.resolve_at(wall_started_at);
        if let DomainPace::Paced = domain.config.pace
            && let DomainStartPoint::Resume = &start.start
            && let Ok(Some(resume_at)) = self.inner.runtime.current_paced_domain_time(domain_id)
        {
            logical_start = resume_at;
        }
        let concrete_start = match &start.start {
            DomainStartPoint::Resume => DomainStartPoint::Resume,
            DomainStartPoint::Now { .. } => DomainStartPoint::At {
                timestamp: logical_start,
                time_rate,
            },
            DomainStartPoint::At { .. } => start.start.clone(),
        };
        let clock = DomainClockState::new(wall_started_at, logical_start, time_rate);
        let authority = if let DomainPace::Paced = domain.config.pace {
            let Some(authority) = self.selected_domain_clock_authority(domain_id).await else {
                return command_error(format!(
                    "no live voter is available to own the clock for domain '{}'",
                    domain_id.as_str()
                ));
            };
            Some(authority)
        } else {
            None
        };
        match self
            .inner
            .consensus
            .start_domain(
                domain_id.clone(),
                concrete_start,
                matches!(domain.config.pace, DomainPace::Paced).then_some(clock.clone()),
                authority,
            )
            .await
        {
            Ok(()) => {
                if let Err(error) = self.apply_current_cluster_state().await {
                    let rollback = self.roll_back_started_domain(domain_id).await;
                    return command_error(format!(
                        "failed to start domain '{}': {error}{rollback}",
                        domain_id.as_str()
                    ));
                }
                command_ok(format!("starting domain '{}'", domain_id.as_str()))
            }
            Err(error) => {
                self.consensus_error_response(
                    &error,
                    format!("failed to start domain '{}': {error}", domain_id.as_str()),
                )
                .await
            }
        }
    }

    async fn stop_domain(&self, domain_id: &DomainName, _stop: StopDomain) -> CommandResult {
        let Some(domain) = self.inner.consensus.current_domain(domain_id).await else {
            return command_error(format!("domain '{}' does not exist", domain_id.as_str()));
        };
        if let DomainStatus::Stopped = domain.status {
            return command_error(format!(
                "domain '{}' is already stopped",
                domain_id.as_str()
            ));
        }
        match self.inner.consensus.stop_domain(domain_id.clone()).await {
            Ok(()) => {
                if let Err(error) = self.apply_current_cluster_state().await {
                    self.broadcast_error(format!(
                        "failed to reconcile runtime after stopping domain '{}': {error}",
                        domain_id.as_str(),
                    ));
                }
                command_ok(format!("stopped domain '{}'", domain_id.as_str()))
            }
            Err(error) => {
                self.consensus_error_response(
                    &error,
                    format!("failed to stop domain '{}': {error}", domain_id.as_str()),
                )
                .await
            }
        }
    }

    async fn show_stream_materialized_state(
        &self,
        domain: &DomainName,
        show: ShowRelayMaterializedState,
    ) -> CommandResult {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return command_error(format!(
                "domain '{}' has no active schedule",
                domain.as_str()
            ));
        };
        let Some(relay_node) = domain_schedule
            .nodes
            .get(&NodeRef::new(
                ModelKind::Relay,
                ModelName::from(&show.relay),
            ))
            .filter(|node| {
                matches!(node.config.as_ref(), Model::Relay(relay) if relay.materialized_state.is_some())
            })
        else {
            return command_error(format!(
                "stream '{}' in domain '{}' is not materialized",
                show.relay.as_str(),
                domain.as_str()
            ));
        };

        let entries = match self
            .inner
            .runtime
            .local_materialized_stream_state(domain, &show.relay)
        {
            Ok(entries) if !entries.is_empty() => entries,
            Ok(_) if !relay_node.executes_on(self.inner.consensus.local_node_id()) => {
                if let Some(primary_node) = relay_node.primary_node() {
                    match self
                        .inner
                        .runtime
                        .remote_materialized_stream_state(primary_node, domain, &show.relay)
                        .await
                    {
                        Ok(entries) => entries,
                        Err(message) => return command_error(message),
                    }
                } else {
                    Vec::new()
                }
            }
            Ok(entries) => entries,
            Err(message) => return command_error(message),
        };

        let message = if entries.is_empty() {
            format_materialized_stream_state_output(&show.relay, relay_node, Vec::new())
        } else {
            let entry_lines = entries
                .into_iter()
                .map(|(key, record)| {
                    let metadata = &record.metadata;
                    format!(
                        "key={} payload={} low={} high={}",
                        if key.is_empty() {
                            "(root)"
                        } else {
                            key.as_str()
                        },
                        runtime_schema::remote_runtime_record_to_json_string(&record),
                        metadata.ingested_at_low_watermark,
                        metadata.ingested_at_high_watermark
                    )
                })
                .collect::<Vec<_>>();
            format_materialized_stream_state_output(&show.relay, relay_node, entry_lines)
        };
        command_ok(message)
    }

    fn describe_udf(&self, domain: &DomainName, describe: DescribeUdf) -> CommandResult {
        let model = match self.inner.registry.get::<CreateUdf>(domain, &describe.name) {
            Ok(Some(udf)) => udf,
            Ok(None) => {
                return command_error(format!(
                    "UDF '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!("failed to read UDF: {error}"));
            }
        };
        let arguments = model
            .arguments
            .iter()
            .map(|argument| {
                format!(
                    "{} {}{}",
                    argument.name.as_str(),
                    argument.ty,
                    if argument.optional { " OPTIONAL" } else { "" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let references = if let Some(graph) = self.inner.registry.active_graph(domain) {
            let mut references = Vec::new();
            for edge in graph.edges() {
                if edge.kind == crate::registry::EdgeKind::RequiredBy
                    && edge.from == ModelName::from(&model.name)
                {
                    references.push(edge.to);
                }
            }
            references.sort();
            references.dedup();
            if references.is_empty() {
                "(none)".to_string()
            } else {
                references
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        } else {
            "(none)".to_string()
        };
        command_ok(format!(
            "name: {}\nlanguage: {}\nsignature: ({arguments}) -> {}{}\nvolatile: {}\ncode_hash: \
             {}\nreferencing_nodes: {references}",
            model.name.as_str(),
            model.language.as_ref(),
            model.returns.ty,
            if model.returns.optional {
                " OPTIONAL"
            } else {
                ""
            },
            model.volatile,
            model.code_hash
        ))
    }

    fn show_udfs(&self, domain: &DomainName) -> CommandResult {
        match self
            .inner
            .registry
            .list_identifiers(domain, ModelKind::Udf, "")
        {
            Ok(identifiers) if identifiers.is_empty() => command_ok("(none)".to_string()),
            Ok(identifiers) => command_ok(
                identifiers
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            Err(error) => command_error(format!("failed to list UDFs: {error}")),
        }
    }

    async fn describe_resource(
        &self,
        domain: &DomainName,
        describe: DescribeResource,
    ) -> CommandResult {
        if describe.version.is_none() {
            let resources = self.inner.consensus.current_resources().await;
            if !resources.is_declared(domain, &describe.identifier) {
                return command_error(format!(
                    "resource '{}' does not exist",
                    describe.identifier.as_str()
                ));
            }
            let versions = resources
                .versions
                .iter()
                .filter(|resource| {
                    resource.id.domain == *domain && resource.id.identifier == describe.identifier
                })
                .cloned()
                .collect::<Vec<_>>();
            let version_numbers = if versions.is_empty() {
                "(none)".to_string()
            } else {
                versions
                    .iter()
                    .map(|resource| resource.id.version.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            let mut lines = vec![
                format!("resource: {}", describe.identifier.as_str()),
                format!("versions: {version_numbers}"),
            ];
            lines.push("version_details:".to_string());
            if versions.is_empty() {
                lines.push("- none".to_string());
            } else {
                for resource in &versions {
                    lines.push(SessionServiceImpl::format_resource_version_summary(
                        resource,
                    ));
                    lines.push("  entries:".to_string());
                    lines.extend(self.resource_version_entry_lines(resource));
                }
            }
            return command_ok(lines.join("\n"));
        }

        let version = describe
            .version
            .verified("the branch above returned for the absent case");
        let id = ResourceId::new(domain.clone(), describe.identifier.clone(), version);
        let resources = self.inner.consensus.current_resources().await;
        let Some(resource) = resources
            .versions
            .iter()
            .find(|resource| resource.id == id)
            .cloned()
        else {
            return command_error(format!(
                "resource '{}@{}' does not exist",
                describe.identifier.as_str(),
                version
            ));
        };

        let replicas = resources
            .replicas
            .iter()
            .filter(|replica| replica.key.version_key().resource_id() == id)
            .cloned()
            .collect::<Vec<_>>();
        let gossip = self.inner.cluster.gossip_state().await;
        let live_node_ids = gossip
            .live_nodes
            .iter()
            .map(|node| node.node_id.clone())
            .collect::<BTreeSet<_>>();
        let mut live_node_ids = live_node_ids;
        if live_node_ids.is_empty() {
            live_node_ids.insert(self.inner.consensus.local_node_id().clone());
        }
        let dead_node_ids = gossip.dead_node_ids;
        let node_ids = live_node_ids
            .iter()
            .cloned()
            .chain(dead_node_ids.iter().cloned())
            .chain(replicas.iter().map(|replica| replica.key.node_id.clone()))
            .collect::<BTreeSet<_>>();
        let ready_live_nodes = live_node_ids
            .iter()
            .filter(|node_id| {
                replicas.iter().any(|replica| {
                    &replica.key.node_id == *node_id
                        && replica.state.as_ref() == "ready"
                        && replica.root_checksum.as_deref() == Some(resource.root_checksum.as_str())
                })
            })
            .count();
        let cluster_ready = !live_node_ids.is_empty() && ready_live_nodes == live_node_ids.len();

        let mut lines = vec![
            format!(
                "resource: {}@{}",
                resource.id.identifier.as_str(),
                resource.id.version
            ),
            format!("root_checksum: {}", resource.root_checksum),
            format!("manifest_checksum: {}", resource.manifest_checksum),
            format!("file_count: {}", resource.file_count),
            format!("total_bytes: {}", resource.total_bytes),
            format!("created_by_node: {}", resource.created_by_node),
            format!("created_at: {}", resource.created_at),
            format!(
                "cluster_ready: {}",
                if cluster_ready { "true" } else { "false" }
            ),
            "entries:".to_string(),
        ];
        lines.extend(self.resource_version_entry_lines(&resource));
        lines.extend([
            format!(
                "alive_nodes: {}",
                if live_node_ids.is_empty() {
                    "(none)".to_string()
                } else {
                    live_node_ids.iter().cloned().collect::<Vec<_>>().join(",")
                }
            ),
            format!(
                "dead_nodes: {}",
                if dead_node_ids.is_empty() {
                    "(none)".to_string()
                } else {
                    dead_node_ids.iter().cloned().collect::<Vec<_>>().join(",")
                }
            ),
            "nodes:".to_string(),
        ]);

        if node_ids.is_empty() {
            lines.push("- none".to_string());
        } else {
            for node_id in node_ids {
                let topology = if live_node_ids.contains(&node_id) {
                    "alive"
                } else if dead_node_ids.contains(&node_id) {
                    "dead"
                } else {
                    "unknown"
                };
                let replica = replicas
                    .iter()
                    .find(|replica| replica.key.node_id == node_id);
                let state = match replica {
                    Some(replica) => replica.state.as_ref(),
                    None if live_node_ids.contains(&node_id) => "pending",
                    None => "untracked",
                };
                let checksum = if let Some(replica) = replica {
                    replica.root_checksum.as_deref().unwrap_or("-")
                } else {
                    "-"
                };
                let verified_at = if let Some(replica) = replica
                    && let Some(value) = replica.last_verified_at
                {
                    value.to_string()
                } else {
                    "-".to_string()
                };
                let source = match replica.and_then(|replica| replica.source_node_id.as_ref()) {
                    Some(source) => source.as_str(),
                    None => "-",
                };
                let error = if let Some(replica) = replica {
                    replica.error.as_deref().unwrap_or("-")
                } else {
                    "-"
                };
                lines.push(format!(
                    "- {} topology={} state={} checksum={} verified_at={} source={} error={}",
                    node_id, topology, state, checksum, verified_at, source, error,
                ));
            }
        }

        command_ok(lines.join("\n"))
    }

    fn format_resource_version_summary(resource: &nervix_models::ResourceVersion) -> String {
        format!(
            "- version={} root_checksum={} manifest_checksum={} file_count={} total_bytes={} \
             created_by_node={} created_at={}",
            resource.id.version,
            resource.root_checksum,
            resource.manifest_checksum,
            resource.file_count,
            resource.total_bytes,
            resource.created_by_node,
            resource.created_at
        )
    }

    fn resource_version_entry_lines(
        &self,
        resource: &nervix_models::ResourceVersion,
    ) -> Vec<String> {
        match self.inner.resource_store.read_manifest(&resource.id) {
            Ok(manifest) if manifest.entries.is_empty() => vec!["  - none".to_string()],
            Ok(manifest) => manifest
                .entries
                .iter()
                .map(SessionServiceImpl::format_resource_manifest_entry)
                .collect(),
            Err(error) => vec![format!("  - unavailable error={error}")],
        }
    }

    fn format_resource_manifest_entry(entry: &ResourceManifestEntry) -> String {
        let (entry_type, size, checksum) = match &entry.content {
            ResourceEntryContent::File { size, checksum } => ("file", *size, checksum.as_str()),
            ResourceEntryContent::Directory => ("directory", 0, "-"),
        };
        format!(
            "  - type={} path={} size={} checksum={}",
            entry_type, entry.path, size, checksum
        )
    }

    async fn wait_for_resource_cluster_ready(&self, id: &ResourceId) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            tokio::task::consume_budget().await;
            let resources = self.inner.consensus.current_resources().await;
            let replicas = resources
                .replicas
                .iter()
                .filter(|replica| replica.key.version_key().resource_id() == *id)
                .collect::<Vec<_>>();
            let gossip = self.inner.cluster.gossip_state().await;
            let live_node_ids = gossip
                .live_nodes
                .iter()
                .map(|node| &node.node_id)
                .collect::<BTreeSet<_>>();
            let all_ready = !live_node_ids.is_empty()
                && live_node_ids.iter().all(|node_id| {
                    replicas.iter().any(|replica| {
                        replica.key.node_id == **node_id
                            && replica.state == ResourceNodeState::Ready
                    })
                });
            if all_ready {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for resource '{}@{}' to finish replicating",
                    id.identifier.as_str(),
                    id.version
                ));
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn publish_domain_schedule(
        &self,
        domain: &DomainName,
        graph: Option<ActiveGraph>,
    ) -> Result<usize, String> {
        #[cfg(feature = "testing")]
        if self
            .inner
            .runtime
            .take_armed_schedule_publication_fault(domain)
        {
            return Err(format!(
                "injected schedule publication fault for domain '{}'",
                domain.as_str()
            ));
        }
        let default_policy = self
            .inner
            .consensus
            .current_domain(domain)
            .await
            .ok_or_else(|| format!("domain '{}' does not exist", domain.as_str()))?
            .config
            .placement;
        let expected_schedule = self
            .inner
            .consensus
            .current_schedule()
            .await
            .domain(domain)
            .cloned();
        let PreparedDomainSchedule {
            schedule,
            relocations,
        } = self
            .prepare_domain_schedule(domain, graph, default_policy)
            .await?;
        self.inner
            .consensus
            .replace_domain_schedule(domain.clone(), expected_schedule, schedule)
            .await
            .map_err(|error| error.to_string())?;
        self.apply_current_cluster_state().await.map_err(|error| {
            format!(
                "failed to instantiate runtime schedule for domain '{}': {error}",
                domain.as_str()
            )
        })?;
        Ok(relocations)
    }

    async fn prepare_domain_schedule(
        &self,
        domain: &DomainName,
        graph: Option<ActiveGraph>,
        placement: PlacementPolicy,
    ) -> Result<PreparedDomainSchedule, String> {
        let live_node_ids = self.inner.cluster.live_node_ids().await;
        let live_voters = self
            .inner
            .consensus
            .live_voter_ids(live_node_ids.clone())
            .await;
        let cluster_nodes = self
            .inner
            .consensus
            .schedulable_live_voter_ids(live_node_ids)
            .await;
        let current = self.inner.consensus.current_schedule().await;
        let schedule = match graph {
            Some(graph) => {
                #[cfg(feature = "testing")]
                let mut schedule = graph.schedule_for_domain_with_mode(
                    domain,
                    &cluster_nodes,
                    self.inner.replica_count,
                    placement,
                    self.inner.runtime.scheduler_mode(),
                );
                #[cfg(not(feature = "testing"))]
                let mut schedule = graph.schedule_for_domain(
                    domain,
                    &cluster_nodes,
                    self.inner.replica_count,
                    placement,
                );
                Self::merge_existing_schedule_data(
                    &mut schedule,
                    current.domain(domain),
                    &live_voters,
                );
                prefer_former_owners_as_replicas(
                    current.domain(domain),
                    &mut schedule,
                    &live_voters,
                );
                Some(schedule)
            }
            None => None,
        };
        let relocations = planned_relocation_count(current.domain(domain), schedule.as_ref());
        Ok(PreparedDomainSchedule {
            schedule,
            relocations,
        })
    }

    async fn drop_node(&self, node_id: ClusterNodeName) -> CommandResult {
        let gossip = self.inner.cluster.gossip_state().await;
        let is_live = gossip
            .live_nodes
            .iter()
            .any(|live_node| live_node.node_id == node_id);
        if is_live && !gossip.dead_node_ids.contains(&node_id) {
            return command_error(format!(
                "cannot drop live node '{node_id}'; stop the node before removing it"
            ));
        }

        let membership_nodes = self.inner.consensus.membership_nodes().await;
        if membership_nodes.contains_key(&node_id) {
            let voters = self.inner.consensus.membership_voter_ids().await;
            let live_node_ids = gossip
                .live_nodes
                .iter()
                .filter(|live_node| !gossip.dead_node_ids.contains(&live_node.node_id))
                .map(|live_node| live_node.node_id.clone())
                .collect::<BTreeSet<_>>();
            if let Some(message) = Self::drop_node_quorum_error(&node_id, &voters, &live_node_ids) {
                return command_error(message);
            }
        }

        let current_schedule = self.inner.consensus.current_schedule().await;
        match self.inner.consensus_administrator.drop_node(&node_id).await {
            Ok(()) => {}
            Err(error) => {
                return self
                    .consensus_error_response(
                        &error,
                        format!("failed to drop node '{node_id}': {error}"),
                    )
                    .await;
            }
        }

        let live_node_ids = self.inner.cluster.live_node_ids().await;
        let live_voters = self
            .inner
            .consensus
            .live_voter_ids(live_node_ids.clone())
            .await;
        let schedulable_nodes = self
            .inner
            .consensus
            .schedulable_live_voter_ids(live_node_ids)
            .await;
        let (cluster_nodes, preservable_nodes) =
            Self::drop_node_schedule_node_sets(&live_voters, &schedulable_nodes);
        for (domain, graph) in self.inner.registry.active_graphs() {
            let Some(domain_state) = self.inner.consensus.current_domain(&domain).await else {
                continue;
            };
            #[cfg(feature = "testing")]
            let mut schedule = graph.schedule_for_domain_with_mode(
                &domain,
                cluster_nodes,
                self.inner.replica_count,
                domain_state.config.placement,
                self.inner.runtime.scheduler_mode(),
            );
            #[cfg(not(feature = "testing"))]
            let mut schedule = graph.schedule_for_domain(
                &domain,
                cluster_nodes,
                self.inner.replica_count,
                domain_state.config.placement,
            );
            Self::merge_existing_schedule_data(
                &mut schedule,
                current_schedule.domain(&domain),
                preservable_nodes,
            );
            if let Err(error) = self
                .inner
                .consensus
                .replace_domain_schedule(
                    domain.clone(),
                    current_schedule.domain(&domain).cloned(),
                    Some(schedule),
                )
                .await
            {
                return command_error(format!(
                    "dropped node '{node_id}', but failed to republish schedule for domain '{}': \
                     {error}",
                    domain.as_str()
                ));
            }
        }

        command_ok(format!("dropped node '{node_id}'"))
    }

    fn drop_node_schedule_node_sets<'nodes>(
        live_voters: &'nodes [ClusterNodeName],
        schedulable_nodes: &'nodes [ClusterNodeName],
    ) -> (&'nodes [ClusterNodeName], &'nodes [ClusterNodeName]) {
        (schedulable_nodes, live_voters)
    }

    fn drop_node_quorum_error(
        node_id: &ClusterNodeName,
        voters: &BTreeSet<ClusterNodeName>,
        live_node_ids: &BTreeSet<ClusterNodeName>,
    ) -> Option<String> {
        if voters.is_empty() {
            return None;
        }

        let live_voters = voters
            .iter()
            .filter(|voter| live_node_ids.contains(*voter))
            .count();
        let required_voters = Self::raft_quorum_size(voters.len());
        if live_voters >= required_voters {
            return None;
        }

        Some(format!(
            "cannot drop node '{node_id}' because raft quorum is unavailable: {live_voters} live \
             voter(s), {required_voters} required from current voters [{}]. Start enough existing \
             voters or restore the StatefulSet before changing membership",
            voters.iter().cloned().collect::<Vec<_>>().join(",")
        ))
    }

    fn raft_quorum_size(voter_count: usize) -> usize {
        voter_count / 2 + 1
    }

    async fn set_node_cordoned(&self, node_id: ClusterNodeName, cordoned: bool) -> CommandResult {
        let membership = self.inner.consensus.membership_nodes().await;
        if !membership.contains_key(&node_id) {
            return command_error(format!("node '{node_id}' is not a raft member"));
        }

        if let Err(error) = self
            .inner
            .consensus
            .set_node_cordoned(node_id.clone(), cordoned)
            .await
        {
            let action = if cordoned { "cordon" } else { "uncordon" };
            return self
                .consensus_error_response(
                    &error,
                    format!("failed to {action} node '{node_id}': {error}"),
                )
                .await;
        }

        let action = if cordoned { "cordoned" } else { "uncordoned" };
        command_ok(format!("{action} node '{node_id}'"))
    }

    async fn drain_node(&self, node_id: ClusterNodeName) -> CommandResult {
        let membership = self.inner.consensus.membership_nodes().await;
        if !membership.contains_key(&node_id) {
            return command_error(format!("node '{node_id}' is not a raft member"));
        }

        if let Err(error) = self
            .inner
            .consensus
            .set_node_cordoned(node_id.clone(), true)
            .await
        {
            return self
                .consensus_error_response(
                    &error,
                    format!("failed to cordon node '{node_id}' before drain: {error}"),
                )
                .await;
        }

        let initial_schedule = self.inner.consensus.current_schedule().await;
        let total = initial_schedule
            .domains
            .values()
            .flat_map(|schedule| schedule.nodes.values())
            .filter(|node| node.execution_node() == Some(&node_id))
            .count();
        let mut moved = 0usize;
        let mut outcomes = Vec::new();
        let mut failed = false;
        let mut failed_units = BTreeSet::<(DomainName, String)>::new();
        let mut failed_domains = BTreeSet::<DomainName>::new();
        loop {
            let live_node_ids = self.inner.cluster.live_node_ids().await;
            let live_voters = self
                .inner
                .consensus
                .live_voter_ids(live_node_ids.clone())
                .await;
            let replacement_nodes = self
                .inner
                .consensus
                .schedulable_live_voter_ids(live_node_ids)
                .await;
            if replacement_nodes.is_empty() {
                failed = true;
                outcomes.push(format!(
                    "- owner={node_id} failed: no live schedulable raft voters remain"
                ));
                break;
            }
            let live_voter_set = live_voters.iter().cloned().collect::<BTreeSet<_>>();
            let replacement_node_set = replacement_nodes.iter().cloned().collect::<BTreeSet<_>>();
            let mut handled_this_iteration = false;

            for (domain, graph) in self.inner.registry.active_graphs() {
                if failed_domains.contains(&domain) {
                    continue;
                }
                let Some(_alter_guard) = self.inner.runtime.try_begin_domain_alter(&domain) else {
                    failed = true;
                    failed_domains.insert(domain.clone());
                    outcomes.push(format!(
                        "- domain={} owner={node_id} failed: a model or schedule change is \
                         already in progress",
                        domain.as_str()
                    ));
                    continue;
                };
                let Some(domain_state) = self.inner.consensus.current_domain(&domain).await else {
                    continue;
                };
                let current_schedule = self.inner.consensus.current_schedule().await;
                #[cfg(feature = "testing")]
                let desired = graph.schedule_for_domain_with_mode(
                    &domain,
                    &replacement_nodes,
                    self.inner.replica_count,
                    domain_state.config.placement,
                    self.inner.runtime.scheduler_mode(),
                );
                #[cfg(not(feature = "testing"))]
                let desired = graph.schedule_for_domain(
                    &domain,
                    &replacement_nodes,
                    self.inner.replica_count,
                    domain_state.config.placement,
                );
                let Some(current_domain) = current_schedule.domain(&domain) else {
                    if let Err(error) = self
                        .inner
                        .consensus
                        .replace_domain_schedule(domain.clone(), None, Some(desired))
                        .await
                    {
                        if let ConsensusError::LeadershipLost { .. } = &error {
                            return self
                                .consensus_error_response(
                                    &error,
                                    format!(
                                        "failed to publish initial drain schedule for domain \
                                         '{domain}': {error}"
                                    ),
                                )
                                .await;
                        }
                        failed = true;
                        failed_domains.insert(domain.clone());
                        outcomes.push(format!(
                            "- domain={} owner={node_id} failed: could not publish initial \
                             schedule: {error}",
                            domain.as_str()
                        ));
                        handled_this_iteration = true;
                        break;
                    }
                    if let Err(error) = self.apply_current_cluster_state().await {
                        failed = true;
                        failed_domains.insert(domain.clone());
                        outcomes.push(format!(
                            "- domain={} owner={node_id} failed: could not activate initial \
                             schedule: {error}",
                            domain.as_str()
                        ));
                    }
                    handled_this_iteration = true;
                    break;
                };

                let mut next = current_domain.clone();
                let excluded = failed_units
                    .iter()
                    .filter_map(|(failed_domain, label)| {
                        (failed_domain == &domain).then_some(label.clone())
                    })
                    .collect::<BTreeSet<_>>();
                let Some(drain_move) = Self::move_next_scheduled_node_for_drain_excluding(
                    &mut next,
                    &desired,
                    &node_id,
                    &live_voter_set,
                    &replacement_node_set,
                    &excluded,
                ) else {
                    continue;
                };
                let unit_key = (domain.clone(), drain_move.label.clone());
                mark_complete_ownership_transitions(Some(current_domain), &mut next);
                let planned_moves = planned_ownership_moves(Some(current_domain), Some(&next));
                let mut handoff = match self
                    .begin_planned_ownership_handoff(&domain, Some(current_domain), Some(&next))
                    .await
                {
                    Ok(handoff) => handoff,
                    Err(error) => {
                        failed = true;
                        failed_units.insert(unit_key);
                        if planned_moves.is_empty() {
                            outcomes.push(format!(
                                "- {} owner={node_id} failed: {error}",
                                drain_move.label
                            ));
                        } else {
                            outcomes.extend(planned_moves.iter().map(|moved| {
                                format!(
                                    "- kind={} name={} owner={} failed: {error}",
                                    moved.entity.kind.as_ref(),
                                    moved.entity.identifier.as_str(),
                                    moved.former_owner
                                )
                            }));
                        }
                        handled_this_iteration = true;
                        break;
                    }
                };
                if let Err(error) = self
                    .inner
                    .consensus
                    .replace_domain_schedule(
                        domain.clone(),
                        Some(current_domain.clone()),
                        Some(next),
                    )
                    .await
                {
                    if let Some(handoff) = handoff.take() {
                        self.abort_planned_ownership_handoff(&domain, handoff).await;
                    }
                    if let ConsensusError::LeadershipLost { .. } = &error {
                        return self
                            .consensus_error_response(
                                &error,
                                format!(
                                    "failed to commit drain schedule for domain '{domain}': \
                                     {error}"
                                ),
                            )
                            .await;
                    }
                    failed = true;
                    failed_units.insert(unit_key);
                    outcomes.extend(planned_moves.iter().map(|moved| {
                        format!(
                            "- kind={} name={} owner={} failed: schedule commit failed: {error}",
                            moved.entity.kind.as_ref(),
                            moved.entity.identifier.as_str(),
                            moved.former_owner
                        )
                    }));
                    handled_this_iteration = true;
                    break;
                }
                moved = moved
                    .checked_add(planned_moves.len())
                    .assured("the moves counted here are schedule entries held in memory");
                let local_activation_error = self.apply_current_cluster_state().await.err();
                let mut handoff_activation_error = None;
                if let Some(handoff) = handoff {
                    debug_assert_eq!(handoff.moves, planned_moves);
                    if let Some(error) = &local_activation_error {
                        self.defer_planned_ownership_handoff_release(&domain, handoff, error);
                    } else if let Err(error) = self
                        .finish_planned_ownership_handoff(&domain, handoff)
                        .await
                    {
                        handoff_activation_error = Some(error.to_string());
                    }
                }
                let activation_error = match local_activation_error {
                    Some(error) => Some(error.to_string()),
                    None => handoff_activation_error,
                };
                for ownership_move in &planned_moves {
                    if activation_error.is_none() {
                        outcomes.push(format_planned_ownership_move(ownership_move));
                    } else if let Some(error) = &activation_error {
                        failed = true;
                        outcomes.push(format!(
                            "- kind={} name={} owner={} failed: destination '{}' did not \
                             activate: {error}",
                            ownership_move.entity.kind.as_ref(),
                            ownership_move.entity.identifier.as_str(),
                            ownership_move.former_owner,
                            ownership_move.destination
                        ));
                    }
                }
                handled_this_iteration = true;
                break;
            }

            if !handled_this_iteration {
                break;
            }
        }

        let level = if total == 0 {
            QuiesceLevel::Dynamic
        } else {
            QuiesceLevel::EntityPause
        };
        let mut message = format!(
            "drained node '{node_id}' (moved {moved} of {total} scheduled graph node(s))\n{}",
            quiesce_level_message(level)
        );
        if !outcomes.is_empty() {
            message.push('\n');
            message.push_str(&outcomes.join("\n"));
        }
        if failed {
            command_error(message)
        } else {
            command_ok(message)
        }
    }

    async fn drain_local_node_before_shutdown(&self) {
        let local_node_id = self.inner.consensus.local_node_id().clone();
        let live_node_ids = self.inner.cluster.live_node_ids().await;
        let drain_targets = self
            .inner
            .consensus
            .schedulable_live_voter_ids(live_node_ids)
            .await;
        if !drain_targets
            .iter()
            .any(|node_id| *node_id != local_node_id)
        {
            warn!(
                node_id = %local_node_id,
                "skipping graceful shutdown drain: no live schedulable replacement nodes remain"
            );
            return;
        }
        let leader = self.inner.consensus.current_leader().await;
        match leader.as_ref() {
            Some(leader_id) if *leader_id == local_node_id => {
                let result = self.drain_node(local_node_id.clone()).await;
                if result.success {
                    info!(
                        node_id = %local_node_id,
                        message = result.message,
                        "drained local node before graceful shutdown"
                    );
                    self.uncordon_local_node_after_shutdown_drain(&local_node_id)
                        .await;
                } else {
                    warn!(
                        node_id = %local_node_id,
                        message = result.message,
                        "failed to drain local node before graceful shutdown"
                    );
                    self.uncordon_local_node_after_shutdown_drain(&local_node_id)
                        .await;
                }
            }
            Some(leader_id) => {
                let Some(leader_grpc_uri) = self.leader_grpc_uri(leader_id).await else {
                    warn!(
                        node_id = %local_node_id,
                        leader = %leader_id,
                        "failed to drain local node before graceful shutdown: leader grpc uri is \
                         unknown"
                    );
                    return;
                };
                match NervixClient::connect_with_options(
                    &leader_grpc_uri,
                    "default",
                    grpc_client_connect_options(
                        &leader_grpc_uri,
                        self.inner.configured_basic_auth.as_ref(),
                    ),
                )
                .await
                {
                    Ok(client) => {
                        match client.execute(format!("DRAIN NODE {local_node_id};")).await {
                            Ok(outcome) if outcome.success => {
                                info!(
                                    node_id = %local_node_id,
                                    leader = %leader_id,
                                    message = outcome.message,
                                    "drained local node through leader before graceful shutdown"
                                );
                                self.uncordon_local_node_through_leader_after_shutdown_drain(
                                    &client,
                                    &local_node_id,
                                    leader_id,
                                )
                                .await;
                            }
                            Ok(outcome) => {
                                warn!(
                                    node_id = %local_node_id,
                                    leader = %leader_id,
                                    message = outcome.message,
                                    "failed to drain local node through leader before graceful \
                                     shutdown"
                                );
                                self.uncordon_local_node_through_leader_after_shutdown_drain(
                                    &client,
                                    &local_node_id,
                                    leader_id,
                                )
                                .await;
                            }
                            Err(error) => {
                                warn!(
                                    node_id = %local_node_id,
                                    leader = %leader_id,
                                    error = %error,
                                    "failed to drain local node through leader before graceful shutdown"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        warn!(
                            node_id = %local_node_id,
                            leader = %leader_id,
                            leader_grpc_uri,
                            error = %error,
                            "failed to connect to leader for graceful shutdown drain"
                        );
                    }
                }
            }
            None => {
                warn!(
                    node_id = %local_node_id,
                    "failed to drain local node before graceful shutdown: raft leader is unknown"
                );
            }
        }
    }

    async fn uncordon_local_node_after_shutdown_drain(&self, local_node_id: &ClusterNodeName) {
        match self
            .inner
            .consensus
            .set_node_cordoned(local_node_id.clone(), false)
            .await
        {
            Ok(()) => {
                info!(
                    node_id = %local_node_id,
                    "cleared shutdown drain cordon before graceful shutdown"
                );
            }
            Err(error) => {
                warn!(
                    node_id = %local_node_id,
                    error = %error,
                    "failed to clear shutdown drain cordon before graceful shutdown"
                );
            }
        }
    }

    async fn uncordon_local_node_through_leader_after_shutdown_drain(
        &self,
        client: &NervixClient,
        local_node_id: &ClusterNodeName,
        leader_id: &ClusterNodeName,
    ) {
        match client
            .execute(format!("UNCORDON NODE {local_node_id};"))
            .await
        {
            Ok(outcome) if outcome.success => {
                info!(
                    node_id = %local_node_id,
                    leader = %leader_id,
                    message = outcome.message,
                    "cleared shutdown drain cordon through leader"
                );
            }
            Ok(outcome) => {
                warn!(
                    node_id = %local_node_id,
                    leader = %leader_id,
                    message = outcome.message,
                    "failed to clear shutdown drain cordon through leader"
                );
            }
            Err(error) => {
                warn!(
                    node_id = %local_node_id,
                    leader = %leader_id,
                    error = %error,
                    "failed to clear shutdown drain cordon through leader"
                );
            }
        }
    }

    async fn leader_grpc_uri(&self, leader_id: &ClusterNodeName) -> Option<String> {
        self.inner
            .cluster
            .gossip_state()
            .await
            .live_nodes
            .into_iter()
            .find(|node| node.node_id == *leader_id)
            .and_then(|node| grpc_uri_from_advertise_addr(&node.grpc_advertise_addr))
    }

    #[cfg(test)]
    fn move_next_scheduled_node_for_drain(
        schedule: &mut nervix_models::DomainSchedule,
        desired: &nervix_models::DomainSchedule,
        node_id: &ClusterNodeName,
        live_nodes: &BTreeSet<ClusterNodeName>,
        target_nodes: &BTreeSet<ClusterNodeName>,
    ) -> Option<DrainMove> {
        Self::move_next_scheduled_node_for_drain_excluding(
            schedule,
            desired,
            node_id,
            live_nodes,
            target_nodes,
            &BTreeSet::new(),
        )
    }

    fn move_next_scheduled_node_for_drain_excluding(
        schedule: &mut nervix_models::DomainSchedule,
        desired: &nervix_models::DomainSchedule,
        node_id: &ClusterNodeName,
        live_nodes: &BTreeSet<ClusterNodeName>,
        target_nodes: &BTreeSet<ClusterNodeName>,
        // Failed units are keyed by their human-readable label, which is a node id for a single
        // node and a bracketed member list for a placement group, so this stays a string set.
        excluded: &BTreeSet<String>,
    ) -> Option<DrainMove> {
        let mut groups = desired.placement_groups.iter().collect::<Vec<_>>();
        groups.sort_by(|left, right| {
            left.members
                .first()
                .map(|member| (member.kind.as_ref(), &member.identifier))
                .cmp(
                    &right
                        .members
                        .first()
                        .map(|member| (member.kind.as_ref(), &member.identifier)),
                )
                .then_with(|| left.members.len().cmp(&right.members.len()))
        });
        for group in groups {
            let label = format!(
                "placement group [{}]",
                format_placement_runtime_nodes(&group.members)
            );
            if excluded.contains(&label) {
                continue;
            }
            let needs_relocation = group.members.iter().any(|member| {
                schedule
                    .nodes
                    .get(member)
                    .is_some_and(|node| node.execution_node() == Some(node_id))
            });
            if needs_relocation {
                return Self::relocate_placement_group_assignment(
                    schedule,
                    desired,
                    group,
                    node_id,
                    live_nodes,
                    target_nodes,
                    AssignmentRelocation::Planned,
                );
            }
        }

        let grouped_members = desired
            .placement_groups
            .iter()
            .flat_map(|group| group.members.iter().cloned())
            .collect::<HashSet<_>>();
        let mut node_indices = (0..schedule.nodes.len()).collect::<Vec<_>>();
        node_indices.sort_by(|left, right| {
            let left = &schedule.nodes[*left];
            let right = &schedule.nodes[*right];
            left.kind()
                .as_ref()
                .cmp(right.kind().as_ref())
                .then_with(|| left.identifier.cmp(&right.identifier))
        });
        for node_index in node_indices {
            let node = &schedule.nodes[node_index];
            if grouped_members.contains(&node.identity()) {
                continue;
            }
            if node.execution_node() != Some(node_id) {
                continue;
            }
            let label = format!("{} {}", node.kind().as_ref(), node.identifier.as_str());
            if excluded.contains(&label) {
                continue;
            }

            let Some(desired_node) = desired.nodes.get(&node.identity()) else {
                continue;
            };
            if desired_node.assigned_nodes.is_empty()
                || desired_node
                    .assigned_nodes
                    .iter()
                    .any(|assigned| assigned == node_id)
            {
                continue;
            }

            let node = &mut schedule.nodes[node_index];
            return Self::relocate_scheduled_node_assignment(
                node,
                desired_node,
                node_id,
                live_nodes,
                target_nodes,
                AssignmentRelocation::Planned,
            );
        }
        None
    }

    fn relocate_placement_group_assignment(
        schedule: &mut nervix_models::DomainSchedule,
        desired: &nervix_models::DomainSchedule,
        group: &PlacementGroupSchedule,
        unavailable_node_id: &ClusterNodeName,
        live_nodes: &BTreeSet<ClusterNodeName>,
        target_nodes: &BTreeSet<ClusterNodeName>,
        relocation: AssignmentRelocation,
    ) -> Option<DrainMove> {
        let retain_former_replica = relocation.retains_former_replica();
        let current_nodes = group
            .members
            .iter()
            .map(|member| schedule.nodes.get(member).cloned())
            .collect::<Option<Vec<_>>>()?;
        let desired_nodes = group
            .members
            .iter()
            .map(|member| desired.nodes.get(member).cloned())
            .collect::<Option<Vec<_>>>()?;
        let old_primary = if let Some(candidate) = schedule
            .placement_groups
            .iter()
            .find(|candidate| placement_group_members_equal(&candidate.members, &group.members))
            && let Some(primary) = candidate.primary_node.as_ref()
        {
            Some(primary.clone())
        } else if let Some(node) = current_nodes.first() {
            node.primary_node.clone()
        } else {
            None
        };
        let preserved_primary = old_primary.as_ref().filter(|primary| {
            *primary != unavailable_node_id
                && live_nodes.contains(*primary)
                && current_nodes.iter().all(|node| {
                    node.primary_node.as_ref() == Some(*primary)
                        && node.assigned_nodes.contains(*primary)
                })
        });
        let desired_target = if let Some(node_id) = group.primary_node.as_ref()
            && target_nodes.contains(node_id)
        {
            Some(node_id.clone())
        } else if let Some(node) = desired_nodes.first()
            && let Some(node_id) = node.primary_node.as_ref()
            && target_nodes.contains(node_id)
        {
            Some(node_id.clone())
        } else if let Some(node) = desired_nodes.first() {
            node.assigned_nodes
                .iter()
                .find(|node_id| target_nodes.contains(*node_id))
                .cloned()
        } else {
            None
        };
        let mut common_replicas = current_nodes
            .first()?
            .assigned_nodes
            .iter()
            .filter(|node_id| *node_id != unavailable_node_id)
            .filter(|node_id| target_nodes.contains(*node_id))
            .cloned()
            .collect::<Vec<_>>();
        common_replicas.retain(|candidate| {
            current_nodes
                .iter()
                .all(|node| node.assigned_nodes.contains(candidate))
        });
        let target = if let Some(primary) = preserved_primary {
            primary.clone()
        } else if let Some(target) =
            relocation.target(desired_target, common_replicas.first().cloned())
        {
            target
        } else {
            target_nodes.first()?.clone()
        };
        let primary_changed = old_primary.as_ref() != Some(&target);
        let promoted_replica = (primary_changed
            && current_nodes
                .iter()
                .all(|node| node.assigned_nodes.contains(&target)))
        .then_some(target.clone());

        for ((member, current_node), desired_node) in
            group.members.iter().zip(current_nodes).zip(desired_nodes)
        {
            let replica_slots = current_node
                .assigned_nodes
                .len()
                .max(desired_node.assigned_nodes.len())
                .max(1);
            let mut assigned_nodes = vec![target.clone()];
            if retain_former_replica
                && live_nodes.contains(unavailable_node_id)
                && unavailable_node_id != &target
            {
                assigned_nodes.push(unavailable_node_id.clone());
            }
            for assigned in desired_node
                .assigned_nodes
                .iter()
                .chain(&current_node.assigned_nodes)
                .chain(target_nodes)
            {
                if (target_nodes.contains(assigned)
                    || retain_former_replica
                        && assigned == unavailable_node_id
                        && live_nodes.contains(assigned))
                    && !assigned_nodes.contains(assigned)
                {
                    assigned_nodes.push(assigned.clone());
                }
            }
            assigned_nodes.truncate(replica_slots);
            let node = schedule.nodes.get_mut(member).verified(
                "the early return above required every group member to resolve in this same \
                 schedule",
            );
            if primary_changed && let Some(source) = old_primary.as_ref() {
                node.ownership_transition = Some(relocation.ownership_transition(
                    source.clone(),
                    target.clone(),
                    node,
                    promoted_replica.is_some(),
                ));
            }
            node.primary_node = Some(target.clone());
            node.assigned_nodes = assigned_nodes;
        }
        if let Some(current_group) = schedule
            .placement_groups
            .iter_mut()
            .find(|candidate| placement_group_members_equal(&candidate.members, &group.members))
        {
            current_group.primary_node = Some(target.clone());
        }
        Some(DrainMove {
            label: format!(
                "placement group [{}]",
                format_placement_runtime_nodes(&group.members)
            ),
            promoted_replica: promoted_replica.clone(),
            fallback_node: (primary_changed && promoted_replica.is_none()).then_some(target),
        })
    }

    fn relocate_scheduled_node_assignment(
        node: &mut ScheduledNode,
        desired_node: &ScheduledNode,
        unavailable_node_id: &ClusterNodeName,
        live_nodes: &BTreeSet<ClusterNodeName>,
        target_nodes: &BTreeSet<ClusterNodeName>,
        relocation: AssignmentRelocation,
    ) -> Option<DrainMove> {
        let retain_former_replica = relocation.retains_former_replica();
        if !node.is_assigned_to(unavailable_node_id) {
            return None;
        }

        let label = format!("{} {}", node.kind().as_ref(), node.identifier.as_str());
        let old_primary = node.primary_node.clone();
        let preserved_primary = old_primary
            .as_ref()
            .filter(|primary| *primary != unavailable_node_id && live_nodes.contains(*primary))
            .cloned();
        let desired_target = if let Some(node_id) = desired_node.primary_node.as_ref()
            && target_nodes.contains(node_id)
        {
            Some(node_id.clone())
        } else {
            desired_node
                .assigned_nodes
                .iter()
                .find(|node_id| target_nodes.contains(*node_id))
                .cloned()
        };
        let existing_replica = node
            .assigned_nodes
            .iter()
            .filter(|assigned| *assigned != unavailable_node_id)
            .find(|assigned| target_nodes.contains(*assigned))
            .cloned();
        let target = if let Some(primary) = preserved_primary {
            primary
        } else if let Some(target) = relocation.target(desired_target, existing_replica) {
            target
        } else {
            target_nodes.first()?.clone()
        };
        let replica_slots = node
            .assigned_nodes
            .len()
            .max(desired_node.assigned_nodes.len())
            .max(1);
        let mut assigned_nodes = vec![target.clone()];
        if retain_former_replica
            && live_nodes.contains(unavailable_node_id)
            && unavailable_node_id != &target
        {
            assigned_nodes.push(unavailable_node_id.clone());
        }
        for assigned in desired_node
            .assigned_nodes
            .iter()
            .chain(&node.assigned_nodes)
            .chain(target_nodes)
        {
            if (target_nodes.contains(assigned)
                || retain_former_replica
                    && assigned == unavailable_node_id
                    && live_nodes.contains(assigned))
                && !assigned_nodes.contains(assigned)
            {
                assigned_nodes.push(assigned.clone());
            }
        }
        assigned_nodes.truncate(replica_slots);
        let primary_changed = old_primary.as_ref() != Some(&target);
        let promoted_replica =
            (primary_changed && node.assigned_nodes.contains(&target)).then_some(target.clone());
        if primary_changed && let Some(source) = old_primary {
            node.ownership_transition = Some(relocation.ownership_transition(
                source,
                target.clone(),
                node,
                promoted_replica.is_some(),
            ));
        }
        node.primary_node = Some(target.clone());
        node.assigned_nodes = assigned_nodes;
        Some(DrainMove {
            label,
            promoted_replica: promoted_replica.clone(),
            fallback_node: (primary_changed && promoted_replica.is_none()).then_some(target),
        })
    }

    fn failover_unavailable_scheduled_nodes(
        schedule: &mut nervix_models::DomainSchedule,
        desired: Option<&nervix_models::DomainSchedule>,
        live_nodes: &BTreeSet<ClusterNodeName>,
        target_nodes: &BTreeSet<ClusterNodeName>,
    ) -> Vec<DrainMove> {
        let mut moves = Vec::new();
        if target_nodes.is_empty() {
            return moves;
        }

        let generated_desired = desired.is_none().then(|| {
            let mut generated = schedule.clone();
            for node in generated.nodes.values_mut() {
                let replica_slots = node.assigned_nodes.len().max(1);
                node.assigned_nodes = target_nodes.iter().take(replica_slots).cloned().collect();
                node.primary_node = node.assigned_nodes.first().cloned();
            }
            for group in &mut generated.placement_groups {
                group.primary_node = target_nodes.first().cloned();
            }
            generated
        });
        let desired = desired
            .or(generated_desired.as_ref())
            .verified("the generated schedule is built exactly when no desired schedule was given");

        let groups = schedule.placement_groups.clone();
        let mut grouped_members = HashSet::default();
        for group in groups {
            grouped_members.extend(group.members.iter().cloned());
            let current_nodes = group
                .members
                .iter()
                .map(|member| schedule.nodes.get(member).cloned())
                .collect::<Option<Vec<_>>>();
            let Some(current_nodes) = current_nodes else {
                continue;
            };
            let has_unavailable_assignment = current_nodes.iter().any(|node| {
                node.assigned_nodes
                    .iter()
                    .any(|node_id| !live_nodes.contains(node_id))
            });
            if !has_unavailable_assignment {
                continue;
            }
            let unavailable_node_id = if let Some(node_id) = group.primary_node.as_ref()
                && !live_nodes.contains(node_id)
            {
                Some(node_id.clone())
            } else {
                current_nodes
                    .iter()
                    .flat_map(|node| node.assigned_nodes.iter())
                    .find(|node_id| !live_nodes.contains(*node_id))
                    .cloned()
            };
            let Some(unavailable_node_id) = unavailable_node_id else {
                continue;
            };
            let desired_group = desired
                .placement_groups
                .iter()
                .find(|candidate| placement_group_members_equal(&candidate.members, &group.members))
                .unwrap_or(&group);
            if let Some(failover_move) = Self::relocate_placement_group_assignment(
                schedule,
                desired,
                desired_group,
                &unavailable_node_id,
                live_nodes,
                target_nodes,
                AssignmentRelocation::Failure,
            ) {
                moves.push(failover_move);
            }
        }

        for node in schedule.nodes.values_mut() {
            if grouped_members.contains(&node.identity()) {
                continue;
            }
            if node.assigned_nodes.is_empty() {
                continue;
            }
            let unavailable_node_ids = node
                .assigned_nodes
                .iter()
                .filter(|node_id| !live_nodes.contains(*node_id))
                .cloned()
                .collect::<Vec<_>>();
            if unavailable_node_ids.is_empty() {
                continue;
            }

            let Some(desired_node) = desired.nodes.get(&node.identity()) else {
                continue;
            };

            for unavailable_node_id in unavailable_node_ids {
                if let Some(failover_move) = Self::relocate_scheduled_node_assignment(
                    node,
                    desired_node,
                    &unavailable_node_id,
                    live_nodes,
                    target_nodes,
                    AssignmentRelocation::Failure,
                ) {
                    moves.push(failover_move);
                }
            }
        }

        moves
    }

    fn merge_existing_schedule_data(
        schedule: &mut nervix_models::DomainSchedule,
        existing: Option<&nervix_models::DomainSchedule>,
        live_node_ids: &[ClusterNodeName],
    ) {
        let Some(existing) = existing else {
            return;
        };
        let live_node_ids = live_node_ids.iter().cloned().collect::<BTreeSet<_>>();

        for node in schedule.nodes.values_mut() {
            if let Some(existing_node) = existing.nodes.get(&node.identity()) {
                node.kafka_partition_schedule = existing_node.kafka_partition_schedule.clone();
            }
        }

        let mut grouped_members = HashSet::default();
        for group_index in 0..schedule.placement_groups.len() {
            let members = schedule.placement_groups[group_index].members.clone();
            grouped_members.extend(members.iter().cloned());
            let existing_nodes = members
                .iter()
                .map(|member| existing.nodes.get(member))
                .collect::<Option<Vec<_>>>();
            let common_primary = if let Some(nodes) = existing_nodes.as_ref()
                && let Some(first) = nodes.first()
                && let Some(primary) = first.primary_node.as_ref()
                && live_node_ids.contains(primary)
                && nodes.iter().all(|node| {
                    node.primary_node.as_ref() == Some(primary)
                        && node.assigned_nodes.contains(primary)
                }) {
                Some(primary.clone())
            } else {
                None
            };

            if let (Some(existing_nodes), Some(primary)) = (existing_nodes, common_primary) {
                for (member, existing_node) in members.iter().zip(existing_nodes) {
                    let Some(node) = schedule.nodes.get_mut(member) else {
                        continue;
                    };
                    let desired_assigned_nodes = node.assigned_nodes.clone();
                    let replica_slots = existing_node
                        .assigned_nodes
                        .len()
                        .max(desired_assigned_nodes.len())
                        .max(1);
                    let mut assigned_nodes = vec![primary.clone()];
                    for assigned in existing_node
                        .assigned_nodes
                        .iter()
                        .chain(&desired_assigned_nodes)
                    {
                        if live_node_ids.contains(assigned) && !assigned_nodes.contains(assigned) {
                            assigned_nodes.push(assigned.clone());
                        }
                    }
                    assigned_nodes.truncate(replica_slots);
                    node.primary_node = Some(primary.clone());
                    node.assigned_nodes = assigned_nodes;
                }
            }
            schedule.placement_groups[group_index].primary_node = if let Some(member) =
                members.first()
                && let Some(node) = schedule.nodes.get(member)
            {
                node.primary_node.clone()
            } else {
                None
            };
        }

        for node in schedule.nodes.values_mut() {
            if grouped_members.contains(&node.identity()) {
                continue;
            }
            let Some(existing_node) = existing.nodes.get(&node.identity()) else {
                continue;
            };
            if Self::scheduled_node_should_follow_desired_assignment(node) {
                continue;
            }
            if existing_node.assigned_nodes.is_empty() {
                continue;
            }
            if existing_node
                .assigned_nodes
                .iter()
                .all(|node_id| live_node_ids.contains(node_id))
            {
                node.primary_node = existing_node.primary_node.clone();
                node.assigned_nodes = existing_node.assigned_nodes.clone();
                continue;
            }

            let desired_node = node.clone();
            let target_nodes = desired_node
                .assigned_nodes
                .iter()
                .filter(|node_id| live_node_ids.contains(*node_id))
                .cloned()
                .collect::<BTreeSet<_>>();
            let desired_primary_node = node.primary_node.clone();
            let desired_assigned_nodes = node.assigned_nodes.clone();
            node.primary_node = existing_node.primary_node.clone();
            node.assigned_nodes = existing_node.assigned_nodes.clone();
            let unavailable_nodes = existing_node
                .assigned_nodes
                .iter()
                .filter(|node_id| !live_node_ids.contains(*node_id))
                .cloned()
                .collect::<Vec<_>>();
            let mut relocated = false;
            for unavailable_node_id in unavailable_nodes {
                if Self::relocate_scheduled_node_assignment(
                    node,
                    &desired_node,
                    &unavailable_node_id,
                    &live_node_ids,
                    &target_nodes,
                    AssignmentRelocation::Failure,
                )
                .is_some()
                {
                    relocated = true;
                }
            }
            if !relocated {
                node.primary_node = desired_primary_node;
                node.assigned_nodes = desired_assigned_nodes;
            }
        }

        for node in schedule.nodes.values_mut() {
            let Some(existing_node) = existing.nodes.get(&node.identity()) else {
                continue;
            };
            if node.primary_node == existing_node.primary_node {
                node.ownership_transition = existing_node.ownership_transition.clone();
            }
        }
    }

    fn scheduled_node_should_follow_desired_assignment(node: &ScheduledNode) -> bool {
        node.config.executes_on_every_cluster_node()
    }

    fn kafka_partition_watcher_specs(
        &self,
        schedule: &nervix_models::ClusterSchedule,
    ) -> Vec<KafkaPartitionWatcherSpec> {
        let mut specs = Vec::new();
        for domain_schedule in schedule.domains.values() {
            for node in domain_schedule.nodes.values() {
                let Model::Ingestor(ingestor) = node.config.as_ref() else {
                    continue;
                };
                let IngestSource::Kafka {
                    client,
                    topic,
                    offset_mode: KafkaOffsetMode::Domain,
                    instances,
                    ..
                } = &ingestor.source
                else {
                    continue;
                };
                let Some(client_node) = domain_schedule
                    .nodes
                    .get(&NodeRef::new(ModelKind::Client, ModelName::from(client)))
                else {
                    continue;
                };
                let Model::ClientKafka(client_model) = client_node.config.as_ref() else {
                    continue;
                };
                specs.push(KafkaPartitionWatcherSpec {
                    domain: domain_schedule.domain.clone(),
                    ingestor: ingestor.name.clone(),
                    topic: topic.as_str().to_string(),
                    instances: *instances,
                    client: client_model.clone(),
                });
            }
        }
        specs.sort_by(|left, right| {
            left.domain
                .cmp(&right.domain)
                .then_with(|| left.ingestor.cmp(&right.ingestor))
        });
        specs
    }

    async fn publish_kafka_partition_schedule(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        topic: &str,
        instances: NonZeroU64,
        observed_partitions: Vec<i32>,
    ) -> Result<(), String> {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            return Ok(());
        }

        let current = self.inner.consensus.current_schedule().await;
        let Some(existing_domain_schedule) = current.domain(domain) else {
            return Ok(());
        };
        let mut next_domain_schedule = existing_domain_schedule.clone();
        let Some(ingestor_node) = next_domain_schedule.nodes.get_mut(&NodeRef::new(
            ModelKind::Ingestor,
            ModelName::from(ingestor),
        )) else {
            return Ok(());
        };
        let Model::Ingestor(ingestor_model) = ingestor_node.config.as_ref() else {
            return Ok(());
        };
        let IngestSource::Kafka {
            topic: scheduled_topic,
            offset_mode: KafkaOffsetMode::Domain,
            instances: scheduled_instances,
            ..
        } = &ingestor_model.source
        else {
            return Ok(());
        };
        if scheduled_topic.as_str() != topic || *scheduled_instances != instances {
            return Ok(());
        }

        let mut next_schedule =
            KafkaPartitionSchedule::new(*scheduled_instances, observed_partitions, 0);
        if let Some(existing_schedule) = ingestor_node.kafka_partition_schedule.as_ref() {
            if existing_schedule.observed_partitions == next_schedule.observed_partitions
                && existing_schedule.instance_assignments == next_schedule.instance_assignments
            {
                return Ok(());
            }
            next_schedule.rebalance_epoch = existing_schedule
                .rebalance_epoch
                .checked_add(1)
                .assured("a cluster cannot observe 2^64 partition rebalances");
        }
        ingestor_node.kafka_partition_schedule = Some(next_schedule);
        self.inner
            .consensus
            .replace_domain_schedule(
                domain.clone(),
                Some(existing_domain_schedule.clone()),
                Some(next_domain_schedule),
            )
            .await
            .map_err(|error| error.to_string())
    }

    async fn reconcile_kafka_partition_watchers(
        &self,
        schedule: &nervix_models::ClusterSchedule,
        tasks: &mut HashMap<KafkaPartitionWatcherKey, KafkaPartitionWatcherTask>,
    ) {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            for (_, watcher) in tasks.drain() {
                watcher.task.stop().await;
            }
            return;
        }

        let desired = self
            .kafka_partition_watcher_specs(schedule)
            .into_iter()
            .map(|spec| {
                (
                    KafkaPartitionWatcherKey {
                        domain: spec.domain.clone(),
                        ingestor: spec.ingestor.clone(),
                    },
                    spec,
                )
            })
            .collect::<HashMap<_, _>>();

        let mut stale_keys = Vec::new();
        for (key, watcher) in tasks.iter() {
            if desired.get(key) != Some(&watcher.spec) {
                stale_keys.push(key.clone());
            }
        }
        for key in stale_keys {
            if let Some(watcher) = tasks.remove(&key) {
                watcher.task.stop().await;
            }
        }

        for (key, spec) in desired {
            if tasks.get(&key).is_some_and(|watcher| watcher.spec == spec) {
                continue;
            }
            let cancel = CancellationToken::new();
            let cancel_child = cancel.clone();
            let service = self.clone();
            let spec_for_task = spec.clone();
            let handle = tokio::spawn(async move {
                let resolved = match service.inner.runtime.resolve_client_config(
                    &spec_for_task.domain,
                    spec_for_task.client.mount.as_ref(),
                    &spec_for_task.client.config,
                ) {
                    Ok(resolved) => resolved,
                    Err(error) => {
                        service.broadcast_error(format!(
                            "failed to resolve Kafka partition watcher client config for ingestor \
                             '{}' in domain '{}': {}",
                            spec_for_task.ingestor.as_str(),
                            spec_for_task.domain.as_str(),
                            error
                        ));
                        return;
                    }
                };
                let _mounts = resolved.mounts;
                let mut client_config = ClientConfig::new();
                for entry in &resolved.entries {
                    client_config.set(&entry.key, &entry.value);
                }
                client_config.set(
                    "group.id",
                    format!(
                        "nervix_schedule_watch_{}_{}",
                        spec_for_task.domain.as_str(),
                        spec_for_task.ingestor.as_str()
                    ),
                );
                client_config.set("enable.partition.eof", "false");
                client_config.set("enable.auto.commit", "false");
                let consumer: StreamConsumer = match client_config.create() {
                    Ok(consumer) => consumer,
                    Err(error) => {
                        service.broadcast_error(format!(
                            "failed to create Kafka partition watcher for ingestor '{}' in domain \
                             '{}': {}",
                            spec_for_task.ingestor.as_str(),
                            spec_for_task.domain.as_str(),
                            error
                        ));
                        return;
                    }
                };
                let mut last_observed = None::<Vec<i32>>;
                loop {
                    tokio::task::consume_budget().await;
                    let mut partitions = match KafkaIngestor::topic_partitions(
                        &consumer,
                        spec_for_task.topic.as_str(),
                    ) {
                        Ok(partitions) => partitions,
                        Err(error) => {
                            service.broadcast_error(format!(
                                "failed to inspect Kafka partitions for ingestor '{}' in domain \
                                 '{}': {}",
                                spec_for_task.ingestor.as_str(),
                                spec_for_task.domain.as_str(),
                                error
                            ));
                            tokio::select! {
                                _ = service.inner.shutdown.cancelled() => break,
                                _ = cancel_child.cancelled() => break,
                                _ = sleep(LEADER_KAFKA_PARTITION_WATCH_INTERVAL) => continue,
                            }
                        }
                    };
                    partitions.sort_unstable();
                    if last_observed.as_ref() != Some(&partitions) {
                        if let Err(error) = service
                            .publish_kafka_partition_schedule(
                                &spec_for_task.domain,
                                &spec_for_task.ingestor,
                                spec_for_task.topic.as_str(),
                                spec_for_task.instances,
                                partitions.clone(),
                            )
                            .await
                        {
                            service.broadcast_error(format!(
                                "failed to publish Kafka partition schedule for ingestor '{}' in \
                                 domain '{}': {}",
                                spec_for_task.ingestor.as_str(),
                                spec_for_task.domain.as_str(),
                                error
                            ));
                        } else {
                            last_observed = Some(partitions);
                        }
                    }
                    tokio::select! {
                        _ = service.inner.shutdown.cancelled() => break,
                        _ = cancel_child.cancelled() => break,
                        _ = sleep(LEADER_KAFKA_PARTITION_WATCH_INTERVAL) => {}
                    }
                }
            });
            tasks.insert(
                key,
                KafkaPartitionWatcherTask {
                    spec,
                    task: BackgroundTask { cancel, handle },
                },
            );
        }
    }

    async fn subscription_target_from_schedule(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Option<SubscriptionTarget>, String> {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(ScheduledModel {
            config: ack_model,
            node: relay_node,
        }) = domain_schedule.scheduled::<CreateRelay>(relay)
        else {
            return Ok(None);
        };
        let Some(schema) = domain_schedule.configured::<CreateSchema>(&ack_model.schema) else {
            return Err(format!(
                "stream '{}' references missing scheduled schema '{}'",
                relay.as_str(),
                ack_model.schema.as_str()
            ));
        };
        Ok(Some(SubscriptionTarget {
            relay: ack_model.clone(),
            schema: schema.clone(),
            branching: relay_node.effective_branching.clone().unwrap_or_default(),
        }))
    }

    async fn subscription_stream_schema(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Option<nervix_models::CreateSchema>, String> {
        match self.inner.registry.get::<CreateRelay>(domain, relay) {
            Ok(Some(ack_model)) => {
                match self
                    .inner
                    .registry
                    .get::<CreateSchema>(domain, &ack_model.schema)
                {
                    Ok(Some(schema)) => Ok(Some(schema)),
                    Ok(None) => Err(format!(
                        "stream '{}' references missing schema '{}'",
                        relay.as_str(),
                        ack_model.schema.as_str()
                    )),
                    Err(err) => Err(format!(
                        "failed to resolve schema '{}' for relay '{}': {err}",
                        ack_model.schema.as_str(),
                        relay.as_str()
                    )),
                }
            }
            Ok(None) => self
                .subscription_target_from_schedule(domain, relay)
                .await
                .map(|resolved| resolved.map(|target| target.schema)),
            Err(err) => Err(format!(
                "failed to resolve relay '{}' for subscription: {err}",
                relay.as_str()
            )),
        }
    }

    async fn subscription_branch_schema(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Option<StdArc<arrow_schema::Schema>>, String> {
        match self.inner.registry.get::<CreateRelay>(domain, relay) {
            Ok(Some(relay_model)) => {
                let Some(branch_ref) = relay_model.branching.branch() else {
                    return Ok(None);
                };
                let branch = match self.inner.registry.get::<CreateBranch>(domain, branch_ref) {
                    Ok(Some(branch)) => branch,
                    Ok(None) => {
                        return Err(format!(
                            "stream '{}' references missing branch '{}'",
                            relay.as_str(),
                            branch_ref.as_str()
                        ));
                    }
                    Err(err) => {
                        return Err(format!(
                            "failed to resolve branch '{}' for relay '{}': {err}",
                            branch_ref.as_str(),
                            relay.as_str()
                        ));
                    }
                };
                match self
                    .inner
                    .registry
                    .get::<CreateSchema>(domain, &branch.schema)
                {
                    Ok(Some(schema)) => {
                        Ok(Some(runtime_schema::compile_schema(&schema).arrow_schema()))
                    }
                    Ok(None) => Err(format!(
                        "stream '{}' references missing branch schema '{}'",
                        relay.as_str(),
                        branch.schema.as_str()
                    )),
                    Err(err) => Err(format!(
                        "failed to resolve branch schema '{}' for relay '{}': {err}",
                        branch.schema.as_str(),
                        relay.as_str()
                    )),
                }
            }
            Ok(None) => {
                self.subscription_branch_schema_from_schedule(domain, relay)
                    .await
            }
            Err(err) => Err(format!(
                "failed to resolve relay '{}' for subscription: {err}",
                relay.as_str()
            )),
        }
    }

    async fn subscription_branch_schema_from_schedule(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Option<StdArc<arrow_schema::Schema>>, String> {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(ScheduledModel {
            config: relay_model,
            node: relay_node,
        }) = domain_schedule.scheduled::<CreateRelay>(relay)
        else {
            return Ok(None);
        };
        if let Some(branch_ref) = relay_model.branching.branch() {
            let Some(branch) = domain_schedule.configured::<CreateBranch>(branch_ref) else {
                return Err(format!(
                    "stream '{}' references missing scheduled branch '{}'",
                    relay.as_str(),
                    branch_ref.as_str()
                ));
            };
            let Some(schema) = domain_schedule.configured::<CreateSchema>(&branch.schema) else {
                return Err(format!(
                    "stream '{}' references missing scheduled branch schema '{}'",
                    relay.as_str(),
                    branch.schema.as_str()
                ));
            };
            return Ok(Some(runtime_schema::compile_schema(schema).arrow_schema()));
        }

        let branching = relay_node
            .effective_branching
            .as_deref()
            .unwrap_or_default();
        if branching.is_empty() {
            return Ok(None);
        }
        let Some(schema) = domain_schedule.configured::<CreateSchema>(&relay_model.schema) else {
            return Err(format!(
                "stream '{}' references missing scheduled schema '{}'",
                relay.as_str(),
                relay_model.schema.as_str()
            ));
        };
        let mut fields = Vec::with_capacity(branching.len());
        for branch_field in branching {
            let Some(field) = schema
                .fields
                .iter()
                .find(|field| field.name == *branch_field)
            else {
                return Err(format!(
                    "stream '{}' inferred branch field '{}' from its branching, but the field is \
                     missing from schema '{}'",
                    relay.as_str(),
                    branch_field.as_str(),
                    schema.name.as_str()
                ));
            };
            fields.push(field.clone());
        }
        Ok(Some(
            runtime_schema::compile_schema(&nervix_models::CreateSchema {
                name: schema.name.clone(),
                fields,
            })
            .arrow_schema(),
        ))
    }

    async fn subscription_materialized_context(
        &self,
        domain: &DomainName,
    ) -> Result<
        (
            HashMap<RelayName, RuntimeMaterializedRelaySpec>,
            HashMap<RelayName, Option<ClusterNodeName>>,
        ),
        String,
    > {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok((HashMap::default(), HashMap::default()));
        };

        let mut specs = HashMap::default();
        let mut owners = HashMap::default();
        for relay_node in domain_schedule
            .nodes
            .values()
            .filter(|node| node.kind() == ModelKind::Relay)
        {
            let Model::Relay(ack_model) = relay_node.config.as_ref() else {
                continue;
            };
            if ack_model.materialized_state.is_none() {
                continue;
            }
            let Some(schema_node) = domain_schedule.nodes.get(&NodeRef::new(
                ModelKind::Schema,
                ModelName::from(&ack_model.schema),
            )) else {
                return Err(format!(
                    "stream '{}' references missing scheduled schema '{}'",
                    ack_model.name.as_str(),
                    ack_model.schema.as_str()
                ));
            };
            let Model::Schema(schema) = schema_node.config.as_ref() else {
                return Err("scheduled schema node has invalid model kind".to_string());
            };
            let schema = runtime_schema::compile_schema(schema);
            specs.insert(
                ack_model.name.clone(),
                RuntimeMaterializedRelaySpec::new(
                    schema.arrow_schema(),
                    schema.vm_sensitivity(),
                    relay_node.effective_branching.clone().unwrap_or_default(),
                ),
            );
            owners.insert(ack_model.name.clone(), relay_node.primary_node().cloned());
        }

        Ok((specs, owners))
    }

    async fn lookup_target_from_schedule(
        &self,
        domain: &DomainName,
        name: impl Into<ModelName>,
    ) -> Result<Option<LookupTarget>, String> {
        let name = name.into();
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(lookup_node) = domain_schedule
            .nodes
            .get(&NodeRef::new(ModelKind::Lookup, name.clone()))
        else {
            return Ok(None);
        };
        let Model::Lookup(lookup) = lookup_node.config.as_ref() else {
            return Err("scheduled lookup node has invalid model kind".to_string());
        };
        let Some(codec_node) = domain_schedule.nodes.get(&NodeRef::new(
            ModelKind::Codec,
            ModelName::from(&lookup.decode_using_codec),
        )) else {
            return Err(format!(
                "lookup '{}' references missing scheduled codec '{}'",
                name.as_str(),
                lookup.decode_using_codec.as_str()
            ));
        };
        let Model::Codec(codec) = codec_node.config.as_ref() else {
            return Err("scheduled codec node has invalid model kind".to_string());
        };
        let Some(schema_node) = domain_schedule.nodes.get(&NodeRef::new(
            ModelKind::Schema,
            ModelName::from(&codec.schema),
        )) else {
            return Err(format!(
                "lookup '{}' references missing scheduled schema '{}'",
                name.as_str(),
                codec.schema.as_str()
            ));
        };
        let Model::Schema(schema) = schema_node.config.as_ref() else {
            return Err("scheduled schema node has invalid model kind".to_string());
        };
        let Some(field) = schema
            .fields
            .iter()
            .find(|field| field.name == lookup.key_field)
        else {
            return Err(format!(
                "lookup '{}' key field '{}' is missing from schema '{}'",
                name.as_str(),
                lookup.key_field.as_str(),
                schema.name.as_str()
            ));
        };
        Ok(Some(LookupTarget {
            lookup: lookup.clone(),
            node: lookup_node.clone(),
            key_ty: field.ty.clone(),
        }))
    }

    async fn ingestor_target_from_schedule(
        &self,
        domain: &DomainName,
        name: impl Into<ModelName>,
    ) -> Result<Option<(CreateIngestor, ScheduledNode)>, String> {
        let name = name.into();
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(ingestor_node) = domain_schedule
            .nodes
            .get(&NodeRef::new(ModelKind::Ingestor, name))
        else {
            return Ok(None);
        };
        let Model::Ingestor(ingestor) = ingestor_node.config.as_ref() else {
            return Err("scheduled ingestor node has invalid model kind".to_string());
        };
        Ok(Some((ingestor.clone(), ingestor_node.clone())))
    }

    async fn create_subscription(
        &self,
        domain: &DomainName,
        subscription: nervix_models::CreateSubscription,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        if subscriptions.contains_name(&subscription.name) {
            return CommandResult {
                success: false,
                message: format!(
                    "session subscription '{}' already exists",
                    subscription.name
                ),
                diagnostics: vec![Diagnostic {
                    message: format!(
                        "session subscription '{}' already exists",
                        subscription.name
                    ),
                    span_start: 0,
                    span_end: 0,
                }],
                kind: i32::from(CommandResultKind::Error),
                ..Default::default()
            };
        }

        let batch_sample_rate =
            match parse_subscription_batch_sample_rate(subscription.batch_sample_rate.as_deref()) {
                Ok(rate) => rate,
                Err(err) => {
                    return CommandResult {
                        success: false,
                        message: format!(
                            "failed to subscribe session '{}': {err}",
                            subscription.name
                        ),
                        diagnostics: vec![Diagnostic {
                            message: err,
                            span_start: 0,
                            span_end: 0,
                        }],
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
            };

        match self
            .inner
            .registry
            .contains(domain, ModelKind::Relay, &subscription.relay)
        {
            Ok(true) => {}
            Ok(false) => match self
                .subscription_target_from_schedule(domain, &subscription.relay)
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return CommandResult {
                        success: false,
                        message: format!(
                            "stream '{}' does not exist in domain '{}'",
                            subscription.relay.as_str(),
                            domain.as_str()
                        ),
                        diagnostics: vec![Diagnostic {
                            message: format!("stream '{}' not found", subscription.relay.as_str()),
                            span_start: 0,
                            span_end: 0,
                        }],
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
                Err(err) => {
                    return CommandResult {
                        success: false,
                        message: format!("failed to resolve relay for subscription: {err}"),
                        diagnostics: vec![Diagnostic {
                            message: format!("failed to resolve relay for subscription: {err}"),
                            span_start: 0,
                            span_end: 0,
                        }],
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
            },
            Err(err) => {
                return CommandResult {
                    success: false,
                    message: format!("failed to resolve relay for subscription: {err}"),
                    diagnostics: vec![Diagnostic {
                        message: format!("failed to resolve relay for subscription: {err}"),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        }

        let relay_target = self
            .subscription_target_from_schedule(domain, &subscription.relay)
            .await;
        let relay_branching = match relay_target {
            Ok(Some(target)) => target.branching,
            Ok(None) | Err(_) => Vec::new(),
        };
        let relay_branch_schema = match self
            .subscription_branch_schema(domain, &subscription.relay)
            .await
        {
            Ok(schema) => schema,
            Err(err) => {
                return CommandResult {
                    success: false,
                    message: format!(
                        "failed to resolve relay branch schema for subscription: {err}"
                    ),
                    diagnostics: vec![Diagnostic {
                        message: format!(
                            "failed to resolve relay branch schema for subscription: {err}"
                        ),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };
        let (materialized_stream_specs, materialized_stream_owner_nodes) =
            match self.subscription_materialized_context(domain).await {
                Ok(context) => context,
                Err(err) => {
                    return CommandResult {
                        success: false,
                        message: format!(
                            "failed to resolve materialized relays for subscription: {err}"
                        ),
                        diagnostics: vec![Diagnostic {
                            message: format!(
                                "failed to resolve materialized relays for subscription: {err}"
                            ),
                            span_start: 0,
                            span_end: 0,
                        }],
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
            };
        let (filter_map, subscription_sensitivity) = match self
            .subscription_stream_schema(domain, &subscription.relay)
            .await
        {
            Ok(Some(schema)) => {
                let udfs = self.inner.runtime.udf_executor(domain);
                let schema = runtime_schema::compile_schema(&schema);
                let input_sensitivity = schema.vm_sensitivity();
                let filter_map = match compile_session_filter_map_program(
                    domain,
                    &subscription.relay,
                    subscription.where_clause.as_ref(),
                    schema.arrow_schema(),
                    input_sensitivity.clone(),
                    RuntimeVmCompileContext {
                        available_materialized_streams: &materialized_stream_specs,
                        available_lookups: &HashMap::default(),
                        current_branching: &relay_branching,
                        current_branch_schema: relay_branch_schema.as_ref(),
                        current_branch_sensitivity: None,
                        udfs: udfs.as_ref(),
                    },
                ) {
                    Ok(filter_map) => filter_map,
                    Err(err) => {
                        return CommandResult {
                            success: false,
                            message: format!(
                                "failed to compile session subscription '{}': {err}",
                                subscription.name
                            ),
                            diagnostics: vec![Diagnostic {
                                message: format!(
                                    "failed to compile session subscription '{}': {err}",
                                    subscription.name
                                ),
                                span_start: 0,
                                span_end: 0,
                            }],
                            kind: i32::from(CommandResultKind::Error),
                            ..Default::default()
                        };
                    }
                };
                let sensitivity = match filter_map.as_ref() {
                    Some(filter_map) => filter_map.output_sensitivity.clone(),
                    None => input_sensitivity,
                };
                (filter_map, sensitivity)
            }
            Ok(None) => {
                return CommandResult {
                    success: false,
                    message: format!(
                        "stream '{}' does not exist in domain '{}'",
                        subscription.relay.as_str(),
                        domain.as_str()
                    ),
                    diagnostics: vec![Diagnostic {
                        message: format!("stream '{}' not found", subscription.relay.as_str()),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
            Err(err) => {
                return CommandResult {
                    success: false,
                    message: format!("failed to resolve relay for subscription: {err}"),
                    diagnostics: vec![Diagnostic {
                        message: format!("failed to resolve relay for subscription: {err}"),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };

        let relay = subscription.relay.clone();
        let receiver = match self.inner.runtime.subscribe_stream(domain, &relay).await {
            Ok(receiver) => receiver,
            Err(err) => {
                return CommandResult {
                    success: false,
                    message: format!("failed to subscribe to relay '{}': {err}", relay.as_str()),
                    diagnostics: vec![Diagnostic {
                        message: format!(
                            "failed to subscribe to relay '{}': {err}",
                            relay.as_str()
                        ),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };

        if let Err(error) = self.register_subscription_interest(domain, &relay).await {
            return command_error(format!(
                "failed to register subscription interest for relay '{}' in domain '{}': {error}",
                relay.as_str(),
                domain.as_str(),
            ));
        }
        subscriptions.insert(
            subscription.name.clone(),
            domain.clone(),
            relay.clone(),
            SessionSubscriptionTaskConfig {
                filter_map,
                sensitivity: subscription_sensitivity,
                delivery_behavior: subscription.delivery_behavior,
                batch_sample_rate,
                runtime: self.inner.runtime.clone(),
                materialized_stream_owner_nodes,
                receiver,
                tx: tx.clone(),
            },
        );

        CommandResult {
            success: true,
            message: format!(
                "created subscription '{}' in domain '{}'",
                subscription.name,
                domain.as_str()
            ),
            diagnostics: Vec::new(),
            kind: i32::from(CommandResultKind::Ok),
            ..Default::default()
        }
    }

    async fn delete_subscription(
        &self,
        subscription: nervix_models::DeleteSubscription,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        match subscriptions.remove(&subscription.name).await {
            Some((subscription_domain, relay)) => {
                if !subscriptions.contains_domain_stream(&subscription_domain, &relay.clone()) {
                    self.unregister_subscription_interest(&subscription_domain, &relay.clone())
                        .await;
                }
                CommandResult {
                    success: true,
                    message: format!(
                        "deleted subscription '{}' from domain '{}'",
                        subscription.name,
                        subscription_domain.as_str()
                    ),
                    diagnostics: Vec::new(),
                    kind: i32::from(CommandResultKind::Ok),
                    ..Default::default()
                }
            }
            None => CommandResult {
                success: false,
                message: format!(
                    "session subscription '{}' does not exist",
                    subscription.name
                ),
                diagnostics: vec![Diagnostic {
                    message: format!("session subscription '{}' not found", subscription.name),
                    span_start: 0,
                    span_end: 0,
                }],
                kind: i32::from(CommandResultKind::Error),
                ..Default::default()
            },
        }
    }
}

/// Why a session may not act on the transaction it names. `Detached` is a recoverable routing
/// condition rather than a user error: the leader simply has no binding for this session, so the
/// client is told to attach the transaction again and retry.
#[derive(Debug, PartialEq, Eq, Error)]
enum SessionTransactionBindingError {
    #[error("no transaction is attached to this session")]
    Unbound,
    #[error("transaction '{id}' was taken over by another session")]
    TakenOver { id: String },
    #[error("transaction '{id}' is not attached to this leader; attach it before continuing")]
    Detached { id: String },
}

impl SessionTransactionBindingError {
    fn into_command_result(self) -> CommandResult {
        let detached = matches!(self, Self::Detached { .. });
        let mut result = command_error(self.to_string());
        if detached {
            result.kind = i32::from(CommandResultKind::TransactionDetached);
        }
        result
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RequestDomainError {
    Missing,
    Invalid,
}

fn parse_request_domain(raw: &str) -> Result<DomainName, RequestDomainError> {
    if raw.trim().is_empty() {
        Err(RequestDomainError::Missing)
    } else {
        DomainName::parse(raw.trim()).map_err(|_| RequestDomainError::Invalid)
    }
}

fn runtime_ingestor_describe_to_envelope(
    summary: RuntimeIngestorDescribe,
    metrics: Vec<String>,
) -> IngestorDescribeEnvelope {
    IngestorDescribeEnvelope {
        running: summary.running,
        ready: summary.ready,
        quiesce_state: summary.quiesce_state,
        quiesce_buffered_records: summary.quiesce_counters.buffered_records.arch_into(),
        quiesce_buffered_bytes: summary.quiesce_counters.buffered_bytes.arch_into(),
        quiesce_dropped_total: summary.quiesce_counters.dropped_total,
        quiesce_rejected_total: summary.quiesce_counters.rejected_total,
        memory_backpressure_paused: summary.memory_backpressure_paused,
        transient_error: summary.transient_error,
        reconnect_backoff: summary.reconnect_backoff,
        reconnect_wait_millis: summary.reconnect_wait_millis,
        kafka_domain_offsets: summary.kafka_domain_offsets.map(|kafka| {
            nervix_interconnect::KafkaDomainOffsetDescribeEnvelope {
                topic: kafka.topic,
                instances: kafka.instances,
                observed_partitions: kafka.observed_partitions,
                rebalance_epoch: kafka.rebalance_epoch,
                instance_assignments: kafka.instance_assignments,
            }
        }),
        metrics,
    }
}

fn runtime_ingestor_describe_from_envelope(
    summary: IngestorDescribeEnvelope,
) -> (RuntimeIngestorDescribe, Vec<String>) {
    (
        RuntimeIngestorDescribe {
            running: summary.running,
            ready: summary.ready,
            quiesce_state: summary.quiesce_state,
            quiesce_counters: crate::runtime::IngestorQuiesceCounters {
                buffered_records: summary.quiesce_buffered_records.arch_into(),
                buffered_bytes: summary.quiesce_buffered_bytes.arch_into(),
                dropped_total: summary.quiesce_dropped_total,
                rejected_total: summary.quiesce_rejected_total,
            },
            memory_backpressure_paused: summary.memory_backpressure_paused,
            transient_error: summary.transient_error,
            reconnect_backoff: summary.reconnect_backoff,
            reconnect_wait_millis: summary.reconnect_wait_millis,
            kafka_domain_offsets: summary.kafka_domain_offsets.map(|kafka| {
                crate::runtime::KafkaDomainOffsetDescribe {
                    topic: kafka.topic,
                    instances: kafka.instances,
                    observed_partitions: kafka.observed_partitions,
                    rebalance_epoch: kafka.rebalance_epoch,
                    instance_assignments: kafka.instance_assignments,
                }
            }),
        },
        summary.metrics,
    )
}

fn dataflow_node_status_to_envelope(
    status: DataflowNodeStatus,
    detail: Option<String>,
    transient_error: Option<String>,
    reconnect_backoff: Option<String>,
    reconnect_wait_millis: Option<u64>,
) -> DataflowNodeStatusEnvelope {
    let status = match status {
        DataflowNodeStatus::Ok => "OK",
        DataflowNodeStatus::Waiting => "WAITING",
        DataflowNodeStatus::Error => "ERROR",
    };
    DataflowNodeStatusEnvelope {
        status: status.to_string(),
        detail,
        transient_error,
        reconnect_backoff,
        reconnect_wait_millis,
    }
}

fn dataflow_node_status_from_envelope(envelope: DataflowNodeStatusEnvelope) -> DataflowNodeHealth {
    DataflowNodeHealth {
        status: if envelope.status.eq_ignore_ascii_case("ERROR") {
            DataflowNodeStatus::Error
        } else if envelope.status.eq_ignore_ascii_case("WAITING") {
            DataflowNodeStatus::Waiting
        } else {
            DataflowNodeStatus::Ok
        },
        detail: envelope.detail,
        reconnect_wait_millis: envelope.reconnect_wait_millis,
    }
}

fn format_timestamp_source(source: Option<&IngestTimestampSource>) -> &'static str {
    match source {
        Some(IngestTimestampSource::Now) => "NOW",
        Some(IngestTimestampSource::At(_)) => "AT",
        None => "-",
    }
}

fn format_millis_duration(millis: u64) -> String {
    humantime::format_duration(Duration::from_millis(millis)).to_string()
}

fn format_ingestor_source(source: &IngestSource) -> &'static str {
    match source {
        IngestSource::Http { .. } => "HTTP",
        IngestSource::Kafka { .. } => "KAFKA",
        IngestSource::Pulsar { .. } => "PULSAR",
        IngestSource::Mqtt { .. } => "MQTT",
        IngestSource::Nats { .. } => "NATS",
        IngestSource::RabbitMq { .. } => "RABBITMQ",
        IngestSource::RedisPubSub { .. } => "REDIS",
        IngestSource::Prometheus { .. } => "PROMETHEUS",
        IngestSource::ZeroMq { .. } => "ZEROMQ",
        IngestSource::Sqs { .. } => "SQS",
        IngestSource::Endpoint { .. } => "ENDPOINT",
        IngestSource::Websockets { .. } => "WEBSOCKETS",
        IngestSource::Syslog { .. } => "SYSLOG",
    }
}

fn format_endpoint_describe_output(name: &ModelName, endpoint: &CreateEndpoint) -> String {
    [
        format!("endpoint: {}", name.as_str()),
        "kind: ENDPOINT".to_string(),
        format!("vhost: {}", endpoint.on_vhost.as_str()),
        format!("path: {}", endpoint.path),
        format!("type: {}", endpoint.endpoint_type.as_ref()),
    ]
    .join("\n")
}

fn format_kafka_offset_mode(offset_mode: &KafkaOffsetMode) -> String {
    match offset_mode {
        KafkaOffsetMode::ConsumerGroup(group) => {
            format!("CONSUMER GROUP {}", group.as_str())
        }
        KafkaOffsetMode::Domain => "DOMAIN".to_string(),
    }
}

fn format_ingestor_describe_output(
    name: impl Into<ModelName>,
    ingestor: &CreateIngestor,
    ingestor_node: &ScheduledNode,
    summary: &RuntimeIngestorDescribe,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("ingestor: {}", name.as_str()),
        "kind: INGESTOR".to_string(),
        format!("source: {}", format_ingestor_source(&ingestor.source)),
        format!(
            "streams: {}",
            ingestor
                .output_routes
                .relays()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!("codec: {}", ingestor.decode_using_codec.as_str()),
        format!(
            "owner: {}",
            match ingestor_node.execution_node() {
                Some(owner) => owner.as_str(),
                None => "-",
            }
        ),
        format!(
            "timestamp: {}",
            format_timestamp_source(ingestor.timestamp_source.as_ref())
        ),
        format!(
            "status: {}",
            if summary.quiesce_state.is_some() {
                "quiesced"
            } else if summary.running {
                "running"
            } else {
                "stopped"
            }
        ),
        format!("ready: {}", if summary.ready { "true" } else { "false" }),
        format!(
            "quiesce: {}",
            ingest_quiesce_to_nspl(ingestor.source.quiesce())
        ),
        format!(
            "quiesce state: {}",
            summary.quiesce_state.as_deref().unwrap_or("none")
        ),
        format!(
            "nervix_ingestor_quiesce_buffered_records: {}",
            summary.quiesce_counters.buffered_records
        ),
        format!(
            "nervix_ingestor_quiesce_buffered_bytes: {}",
            summary.quiesce_counters.buffered_bytes
        ),
        format!(
            "nervix_ingestor_quiesce_dropped_total: {}",
            summary.quiesce_counters.dropped_total
        ),
        format!(
            "nervix_ingestor_quiesce_rejected_total: {}",
            summary.quiesce_counters.rejected_total
        ),
    ];
    lines.extend(format_processor_output_lines(&ingestor.output_routes));
    let memory_backpressure_state = if summary.memory_backpressure_paused {
        "active"
    } else {
        "inactive"
    };
    lines.push(format!("memory-backpressure: {memory_backpressure_state}"));
    lines.push(format!(
        "transient error: {}",
        summary.transient_error.as_deref().unwrap_or("-")
    ));
    lines.push(format!(
        "reconnect backoff: {}",
        summary.reconnect_backoff.as_deref().unwrap_or("-")
    ));
    let reconnect_wait = match summary.reconnect_wait_millis {
        Some(millis) => format_millis_duration(millis),
        None => "-".to_string(),
    };
    lines.push(format!("reconnect wait: {reconnect_wait}"));

    if let IngestSource::Kafka {
        topic,
        offset_mode,
        instances,
        ..
    } = &ingestor.source
    {
        lines.push(format!("kafka topic: {}", topic.as_str()));
        lines.push(format!(
            "kafka offset mode: {}",
            format_kafka_offset_mode(offset_mode)
        ));
        lines.push(format!("kafka instances: {instances}"));
        if let Some(kafka) = summary.kafka_domain_offsets.as_ref() {
            lines.push(format!(
                "kafka observed partitions: {}",
                kafka
                    .observed_partitions
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ));
            lines.push(format!("kafka rebalance epoch: {}", kafka.rebalance_epoch));
            for (instance_idx, partitions) in kafka.instance_assignments.iter().enumerate() {
                let rendered = if partitions.is_empty() {
                    "-".to_string()
                } else {
                    partitions
                        .iter()
                        .map(i32::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                };
                lines.push(format!(
                    "kafka instance {instance_idx} partitions: {rendered}"
                ));
            }
        } else if let KafkaOffsetMode::Domain = offset_mode {
            lines.push("kafka observed partitions: -".to_string());
            lines.push("kafka rebalance epoch: 0".to_string());
            for instance_idx in 0..instances.get() {
                lines.push(format!("kafka instance {instance_idx} partitions: -"));
            }
        }
    } else if let IngestSource::Pulsar {
        topic,
        subscription,
        instances,
        ..
    } = &ingestor.source
    {
        lines.push(format!("pulsar topic: {}", topic.as_str()));
        lines.push(format!("pulsar subscription: {}", subscription.as_str()));
        lines.push(format!("pulsar instances: {instances}"));
    }

    lines.join("\n")
}

fn format_branch_selection(branched_by: &BranchSelection) -> &str {
    match branched_by.branch() {
        Some(name) => name.as_str(),
        None => "UNBRANCHED",
    }
}

fn format_output_branch(branch: Option<&nervix_models::OutputBranch>) -> &str {
    match branch {
        Some(nervix_models::OutputBranch::BranchedBy { branch, .. }) => branch.as_str(),
        Some(nervix_models::OutputBranch::Unbranched) => "UNBRANCHED",
        None => "NODE-WIDE",
    }
}

fn append_metrics_lines(mut output: String, metrics: Vec<String>) -> String {
    if metrics.is_empty() {
        return output;
    }
    output.push('\n');
    output.push_str(&metrics.join("\n"));
    output
}

fn format_relay_describe_output(
    relay: &nervix_models::CreateRelay,
    branching: &[FieldName],
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let mut lines = vec![
        format!("relay: {}", relay.name.as_str()),
        "kind: RELAY".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("schema: {}", relay.schema.as_str()),
        format!("branched by: {}", {
            if let Some(branch) = relay.branching.branch() {
                branch.as_str()
            } else if relay.branching.is_unbranched() {
                "UNBRANCHED"
            } else {
                "-"
            }
        }),
        format!(
            "branch fields: {}",
            if branching.is_empty() {
                "-".to_string()
            } else {
                branching
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        ),
        format!("capacity: {}", relay.buffer),
        format!(
            "materialized state: {}",
            if relay.materialized_state.is_some() {
                "present"
            } else {
                "none"
            }
        ),
    ]);
    if !branching.is_empty() {
        lines.push("branch-local describe: use WHERE bindings".to_string());
    }
    lines.join("\n")
}

fn format_schedule_placement_lines(scheduled_node: Option<&ScheduledNode>) -> Vec<String> {
    let owner = match scheduled_node.and_then(ScheduledNode::execution_node) {
        Some(owner) => owner.as_str(),
        None => "-",
    };
    let mut replicas = "-".to_string();
    if let Some(scheduled_node) = scheduled_node {
        let rendered = format_replica_nodes(scheduled_node);
        if !rendered.is_empty() {
            replicas = rendered;
        }
    }
    vec![format!("owner: {owner}"), format!("replicas: {replicas}")]
}

fn format_replica_nodes(scheduled_node: &ScheduledNode) -> String {
    scheduled_node
        .replica_nodes()
        .iter()
        .map(|node| node.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_processor_output_lines(outputs: &ProcessorOutputs) -> Vec<String> {
    let mut lines = Vec::new();
    let output_count = outputs.outputs().count();
    lines.push(format!("outputs: {output_count}"));

    for (index, output) in outputs.routes.iter().enumerate() {
        let flush = match &output.flush_policy {
            Some(nervix_models::FlushPolicy::Each {
                interval,
                max_batch_size,
            }) => format!("{interval} max-batch-size={max_batch_size}"),
            Some(nervix_models::FlushPolicy::Immediate) => "IMMEDIATE".to_string(),
            None => "none".to_string(),
        };
        lines.push(format!(
            "output {index}: into={} construction={} branch={} flush={flush}",
            output.relay.as_str(),
            if !output.construction.is_empty() {
                "present"
            } else {
                "none"
            },
            format_output_branch(output.branch.as_ref())
        ));
    }

    lines
}

fn processor_input_names(inputs: &ProcessorInputs) -> String {
    inputs
        .relays()
        .iter()
        .map(|name| name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_lookup_describe_output(
    name: impl Into<ModelName>,
    scheduled_node: &ScheduledNode,
    summary: &LookupDescribeEnvelope,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("hash map: {}", name.as_str()),
        "kind: HASH MAP".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(Some(scheduled_node)));
    lines.extend([
        format!("key: {}", summary.key_field.as_str()),
        format!(
            "resource: {}@{}",
            summary.resource.as_str(),
            summary.resource_version
        ),
        format!("path: {}", summary.path),
        format!("codec: {}", summary.decode_using_codec.as_str()),
        format!("entries: {}", summary.entry_count),
    ]);
    lines.join("\n")
}

fn format_expression_list(expressions: &[nervix_models::Expression]) -> String {
    expressions
        .iter()
        .map(|expression| {
            expression_to_nspl(expression).unwrap_or_else(|error| format!("<invalid: {error}>"))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_deduplicator_describe_output(
    name: impl Into<ModelName>,
    deduplicator: &CreateDeduplicator,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("deduplicator: {}", name.as_str()),
        "kind: DEDUPLICATOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&deduplicator.from)),
        format!("mode: {}", deduplicator.mode.as_ref()),
        format!(
            "deduplicate on: {}",
            format_expression_list(&deduplicator.deduplicate_on)
        ),
        format!("max time: {}", deduplicator.max_time),
        format!(
            "filter-where: {}",
            if deduplicator.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        "branch-local: true".to_string(),
        "persistent state: true".to_string(),
        "replicated state: true".to_string(),
        "state structures: 1".to_string(),
        "structure 0:".to_string(),
        "  function: DEDUPLICATE_ON".to_string(),
        "  storage: recent_key_set".to_string(),
        format!(
            "  key expressions: {}",
            format_expression_list(&deduplicator.deduplicate_on)
        ),
        format!("  max time: {}", deduplicator.max_time),
    ]);
    lines.extend(format_processor_output_lines(&deduplicator.output_routes));
    lines.join("\n")
}

fn format_junction_describe_output(
    name: impl Into<ModelName>,
    junction: &CreateJunction,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("junction: {}", name.as_str()),
        "kind: JUNCTION".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&junction.from)),
        format!("branch: {}", format_branch_selection(&junction.branched_by)),
        format!("mode: {}", junction.mode.as_ref()),
        format!(
            "filter-where: {}",
            if junction.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        "branch-local: true".to_string(),
    ]);
    lines.extend(format_processor_output_lines(&junction.output_routes));
    lines.join("\n")
}

fn format_reingestor_describe_output(
    name: impl Into<ModelName>,
    reingestor: &CreateReingestor,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("reingestor: {}", name.as_str()),
        "kind: REINGESTOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&reingestor.from)),
        format!("mode: {}", reingestor.mode.as_ref()),
        format!(
            "filter-where: {}",
            if reingestor.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
    ]);
    lines.extend(format_processor_output_lines(&reingestor.output_routes));
    lines.join("\n")
}

fn format_correlator_describe_output(
    name: impl Into<ModelName>,
    correlator: &CreateCorrelator,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("correlator: {}", name.as_str()),
        "kind: CORRELATOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("left: {}", processor_input_names(&correlator.left)),
        format!("right: {}", processor_input_names(&correlator.right)),
        format!(
            "branch: {}",
            format_branch_selection(&correlator.branched_by)
        ),
        format!("mode: {}", correlator.mode.as_ref()),
        format!("match: {}", correlator.match_policy.as_ref()),
        format!(
            "correlate where: {}",
            expression_to_nspl(&correlator.correlate_where)
                .unwrap_or_else(|error| format!("<invalid: {error}>"))
        ),
        format!("max time: {}", correlator.max_time),
        format!(
            "filter-where: {}",
            if correlator.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        format!(
            "timeout left: {}",
            format_correlation_timeout_action(&correlator.timeout_policy.left)
        ),
        format!(
            "timeout right: {}",
            format_correlation_timeout_action(&correlator.timeout_policy.right)
        ),
        "branch-local: true".to_string(),
        "persistent state: true".to_string(),
        "replicated state: true".to_string(),
    ]);
    lines.extend(format_processor_output_lines(&correlator.output_routes));
    lines.join("\n")
}

fn format_correlation_timeout_action(action: &nervix_models::CorrelationTimeoutAction) -> String {
    match action {
        nervix_models::CorrelationTimeoutAction::Drop => "DROP".to_string(),
        nervix_models::CorrelationTimeoutAction::SendTo { relay } => {
            format!("SEND TO {}", relay.as_str())
        }
    }
}

fn format_reorderer_describe_output(
    name: impl Into<ModelName>,
    reorderer: &CreateReorderer,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("reorderer: {}", name.as_str()),
        "kind: REORDERER".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&reorderer.from)),
        format!("mode: {}", reorderer.mode.as_ref()),
        format!("order by: {}", format_expression_list(&reorderer.order_by)),
        format!("max time: {}", reorderer.max_time),
        format!(
            "filter-where: {}",
            if reorderer.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        "branch-local: true".to_string(),
        "persistent state: true".to_string(),
        "replicated state: true".to_string(),
    ]);
    lines.extend(format_processor_output_lines(&reorderer.output_routes));
    lines.join("\n")
}

fn format_emitter_describe_output(
    name: impl Into<ModelName>,
    emitter: &CreateEmitter,
    scheduled_node: Option<&ScheduledNode>,
    status: Option<&DataflowNodeStatusEnvelope>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("emitter: {}", name.as_str()),
        "kind: EMITTER".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    if let Some(status) = status {
        lines.extend([
            format!("status: {}", status.status),
            // What the node is doing when it is neither working nor broken, such as waiting for a
            // connection from a shared client's pool.
            format!("detail: {}", status.detail.as_deref().unwrap_or("-")),
            format!(
                "transient error: {}",
                status.transient_error.as_deref().unwrap_or("-")
            ),
            format!(
                "reconnect backoff: {}",
                status.reconnect_backoff.as_deref().unwrap_or("-")
            ),
            format!(
                "reconnect wait: {}",
                match status.reconnect_wait_millis {
                    Some(millis) => format!("{millis}ms"),
                    None => "-".to_string(),
                }
            ),
        ]);
    }
    lines.extend([
        format!(
            "from: {}",
            emitter
                .from
                .relays()
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!(
            "codec: {}",
            match emitter.encode_using_codec.as_ref() {
                Some(name) => name.as_str(),
                None => "none",
            }
        ),
        format!("sink: {}", format_emit_sink(&emitter.sink)),
        format!("flush: {}", emitter.flush_policy.to_canonical_nspl()),
        format!(
            "publishing mode: {}",
            emitter.publishing_mode.to_canonical_nspl()
        ),
        format!(
            "filter-map: {}",
            if !emitter.construction.is_empty() {
                "present"
            } else {
                "none"
            }
        ),
    ]);
    lines.join("\n")
}

fn format_emit_sink(sink: &EmitSink) -> String {
    match sink {
        EmitSink::Kafka { client, topic } => {
            format!("KAFKA client={} topic={}", client.as_str(), topic.as_str())
        }
        EmitSink::Pulsar { client, topic } => {
            format!("PULSAR client={} topic={}", client.as_str(), topic.as_str())
        }
        EmitSink::RabbitMq { client, queue } => {
            format!(
                "RABBITMQ client={} queue={}",
                client.as_str(),
                queue.as_str()
            )
        }
        EmitSink::Redis { client, channel } => {
            format!(
                "REDIS client={} channel={}",
                client.as_str(),
                channel.as_str()
            )
        }
        EmitSink::Mqtt { client, topic } => {
            format!("MQTT client={} topic={}", client.as_str(), topic.as_str())
        }
        EmitSink::Nats { client, subject } => {
            format!(
                "NATS client={} subject={}",
                client.as_str(),
                subject.as_str()
            )
        }
        EmitSink::ZeroMq { client } => format!("ZEROMQ client={}", client.as_str()),
        EmitSink::Syslog { client } => format!("SYSLOG client={}", client.as_str()),
        EmitSink::Sqs {
            client,
            queue,
            fifo_group,
        } => {
            let fifo = match fifo_group.as_ref() {
                Some(group) => {
                    let value = match group {
                        nervix_models::SqsFifoGroup::FromBranch => "FROM BRANCH".to_string(),
                        nervix_models::SqsFifoGroup::Expression(expression) => {
                            nervix_models::expression_to_nspl(expression)
                                .unwrap_or_else(|_| "<unrenderable expression>".to_string())
                        }
                    };
                    format!(" fifo_group={value}")
                }
                None => String::new(),
            };
            format!(
                "SQS client={} queue={}{}",
                client.as_str(),
                queue.as_str(),
                fifo
            )
        }
        EmitSink::Sentry { client } => format!("SENTRY client={}", client.as_str()),
        EmitSink::Otel { client, signal, .. } => {
            let signal = match signal {
                nervix_models::OtelSignal::Logs => "logs".to_string(),
                nervix_models::OtelSignal::Traces => "traces".to_string(),
                nervix_models::OtelSignal::Metric(metric) => {
                    format!("metric {}", metric.name)
                }
            };
            format!("OTEL client={} signal={signal}", client.as_str())
        }
        EmitSink::ClickHouse {
            client,
            table,
            max_batch,
            ..
        } => format!(
            "CLICKHOUSE client={} table={} max_batch={}",
            client.as_str(),
            table.as_str(),
            max_batch
        ),
        EmitSink::Postgres {
            client,
            table,
            conflict_action,
            max_batch,
            ..
        } => {
            let conflict = match conflict_action {
                PostgresConflictAction::None => String::new(),
                PostgresConflictAction::DoNothing { target } => {
                    let target = if target.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", target.join(","))
                    };
                    format!(" conflict=ON CONFLICT{target} DO NOTHING")
                }
                PostgresConflictAction::DoUpdate { target } => {
                    let target = if target.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", target.join(","))
                    };
                    format!(" conflict=ON CONFLICT{target} DO UPDATE")
                }
            };
            format!(
                "POSTGRES client={} table={}{} max_batch={}",
                client.as_str(),
                table.as_str(),
                conflict,
                max_batch
            )
        }
        EmitSink::MySql {
            client,
            table,
            conflict_action,
            max_batch,
            ..
        } => {
            let conflict = match conflict_action {
                MySqlConflictAction::None => String::new(),
                MySqlConflictAction::DoNothing => " conflict=ON CONFLICT DO NOTHING".to_string(),
                MySqlConflictAction::DoUpdate => " conflict=ON CONFLICT DO UPDATE".to_string(),
            };
            format!(
                "MYSQL client={} table={}{} max_batch={}",
                client.as_str(),
                table.as_str(),
                conflict,
                max_batch
            )
        }
        EmitSink::MongoDb {
            client,
            collection,
            conflict_action,
            max_batch,
            ..
        } => {
            let conflict = match conflict_action {
                MongoDbConflictAction::None => String::new(),
                MongoDbConflictAction::DoNothing { target } => {
                    format!(" conflict=ON CONFLICT ({}) DO NOTHING", target.join(","))
                }
                MongoDbConflictAction::DoUpdate { target } => {
                    format!(" conflict=ON CONFLICT ({}) DO UPDATE", target.join(","))
                }
            };
            format!(
                "MONGODB client={} collection={}{} max_batch={}",
                client.as_str(),
                collection.as_str(),
                conflict,
                max_batch
            )
        }
        EmitSink::Iceberg {
            backend,
            client,
            table,
            values: _,
            location,
            catalog,
            commit_each,
            max_commit_size,
        } => {
            let catalog = match catalog {
                IcebergCatalog::Rest { client } => format!("rest client={}", client.as_str()),
            };
            format!(
                "ICEBERG backend={} client={} table={} location={} catalog={} commit_each={} \
                 max_commit_size={}",
                backend.as_ref(),
                client.as_str(),
                table.as_str(),
                location,
                catalog,
                commit_each,
                max_commit_size
            )
        }
    }
}

fn format_window_processor_describe_output(
    name: impl Into<ModelName>,
    processor: &CreateWindowProcessor,
    aggregate: &WindowAggregateProgram,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("window processor: {}", name.as_str()),
        "kind: WINDOW PROCESSOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&processor.from)),
        format!("mode: {:?}", processor.mode),
        format!("width: {}", processor.width.to_describe_string()),
        format!("step: {}", processor.step.to_describe_string()),
        format!(
            "filter-where: {}",
            if processor.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        "branch-local: true".to_string(),
        format!("aggregate structures: {}", aggregate.demands().len()),
    ]);
    lines.extend(format_processor_output_lines(&processor.output_routes));
    let references = aggregate.demand_reference_counts();
    for demand in aggregate.demands() {
        lines.extend(format_window_aggregate_demand(demand, &references));
    }
    lines.join("\n")
}

fn format_wasm_processor_describe_output(
    name: impl Into<ModelName>,
    processor: &nervix_models::CreateWasmProcessor,
    scheduled_node: Option<&ScheduledNode>,
    state_lines: Vec<String>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("wasm processor: {}", name.as_str()),
        "kind: WASM PROCESSOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    let version = match processor.resource_version {
        Some(version) => version.to_string(),
        None => "latest".to_string(),
    };
    lines.extend([
        format!("from: {}", processor_input_names(&processor.from)),
        format!("mode: {}", processor.mode.as_ref()),
        format!("resource: {}", processor.resource.as_str()),
        format!("resource version: {version}"),
        format!("file: {}", processor.file),
        format!("max fuel: {}", processor.limits.max_fuel),
        format!("max memory: {} bytes", processor.limits.max_memory_bytes),
        format!("ABI serialization: {}", nervix_wasm::ABI_SERIALIZATION_NAME),
        format!(
            "filter-where: {}",
            if processor.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        "flush: guest-controlled".to_string(),
        "branch-local: true".to_string(),
        "persistent state: true".to_string(),
        "replicated state: true".to_string(),
    ]);
    lines.extend(format_processor_output_lines(&processor.output_routes));
    lines.extend(state_lines);
    lines.join("\n")
}

fn format_materialized_stream_state_output(
    relay: &RelayName,
    scheduled_node: &ScheduledNode,
    entries: Vec<String>,
) -> String {
    let mut lines = vec![
        format!("materialized relay: {}", relay.as_str()),
        "kind: RELAY".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(Some(scheduled_node)));
    if entries.is_empty() {
        lines.push(format!(
            "relay '{}' materialized state is empty",
            relay.as_str()
        ));
    } else {
        lines.extend(entries);
    }
    lines.join("\n")
}

fn format_window_aggregate_demand(
    demand: &WindowAggregateDemand,
    references: &[usize],
) -> Vec<String> {
    let mut lines = vec![
        format!("structure {}:", demand.id),
        format!(
            "  functions: {}",
            demand
                .functions
                .iter()
                .map(|function| function.nspl_name())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!("  storage: {}", demand.storage.nspl_name()),
        format!(
            "  references: {}",
            references.get(demand.id).copied().unwrap_or(0)
        ),
    ];
    if let Some(input) = &demand.input {
        lines.push(format!("  input: {}", format_window_aggregate_input(input)));
    }
    if let Some(config) = &demand.linear_histogram {
        lines.push(format!("  buckets: {}", config.buckets));
        lines.push(format!("  min: {}", format_f64_for_describe(config.min)));
        lines.push(format!("  max: {}", format_f64_for_describe(config.max)));
        lines.push(format!(
            "  delay: {}",
            humantime::format_duration(config.delay)
        ));
    }
    lines
}

fn format_window_aggregate_input(expr: &nervix_vm::program::Expr) -> String {
    match expr {
        nervix_vm::program::Expr::FieldRef(field_ref) => {
            format!("{}.{}", field_ref.relay, field_ref.field)
        }
        _ => format!("{expr:?}"),
    }
}

fn format_f64_for_describe(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

fn format_placement_runtime_nodes(nodes: &[NodeRef]) -> String {
    format_placement_runtime_nodes_in_context(nodes, nodes)
}

fn format_placement_runtime_nodes_in_context(nodes: &[NodeRef], context: &[NodeRef]) -> String {
    nodes
        .iter()
        .map(|node| format_placement_runtime_node(node, context))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_placement_runtime_node(node: &NodeRef, context: &[NodeRef]) -> String {
    let kind_collision = context
        .iter()
        .any(|candidate| candidate.identifier == node.identifier && candidate.kind != node.kind);
    if kind_collision {
        format!("{}:{}", node.kind.as_ref(), node.identifier.as_str())
    } else {
        node.identifier.to_string()
    }
}

fn placement_rule_runtime_nodes(rule: &PlacementRulePlan) -> Vec<NodeRef> {
    let mut nodes = Vec::new();
    for endpoint in &rule.endpoint_pairs {
        for node in std::iter::once(&endpoint.source)
            .chain(std::iter::once(&endpoint.destination))
            .chain(endpoint.corridor.iter())
        {
            if !nodes.contains(node) {
                nodes.push(node.clone());
            }
        }
    }
    nodes
}

fn placement_rule_endpoint_nodes(rule: &PlacementRulePlan, sources: bool) -> Vec<NodeRef> {
    let mut nodes = Vec::new();
    for endpoint in &rule.endpoint_pairs {
        let node = if sources {
            &endpoint.source
        } else {
            &endpoint.destination
        };
        if !nodes.contains(node) {
            nodes.push(node.clone());
        }
    }
    nodes
}

fn placement_rule_coverage_status(rule: &PlacementRulePlan) -> &'static str {
    let connected_pairs = rule
        .endpoint_pairs
        .iter()
        .filter(|pair| pair.connected)
        .count();
    if connected_pairs == 0 {
        return "empty";
    }

    let effective_claims = rule.claims.iter().filter(|claim| claim.effective).count();
    if !rule.claims.is_empty() && effective_claims == 0 {
        return "overridden";
    }
    if connected_pairs == rule.endpoint_pairs.len() && effective_claims == rule.claims.len() {
        "effective"
    } else {
        "partial"
    }
}

/// Configuration a bound transaction has queued but not yet applied. Completion resolves
/// identifiers against it so a session sees the names its own queued statements define, and stops
/// seeing the names they drop, before the transaction commits.
#[derive(Debug, Default)]
struct QueuedConfiguration {
    models: Vec<RegistryMutation>,
    resources: BTreeSet<ResourceName>,
}

impl QueuedConfiguration {
    /// Queued resource names matching `prefix`. A queued resource has no versions to suggest,
    /// because uploading one is not transaction content.
    fn resource_suggestions(&self, prefix: &str) -> Vec<String> {
        let prefix = prefix.to_ascii_lowercase();
        self.resources
            .iter()
            .filter(|identifier| identifier.as_str().starts_with(&prefix))
            .map(|name| name.to_string())
            .collect()
    }
}

fn placement_runtime_node_ref_suggestions(
    registry: &Registry,
    domain: &DomainName,
    prefix: &str,
    queued: &[RegistryMutation],
) -> Vec<String> {
    let Ok(models) = registry.resulting_models(domain, queued) else {
        return Vec::new();
    };

    let prefix = prefix.to_ascii_lowercase();
    let eligible = models
        .iter()
        .filter(|model| {
            placement_member_model_is_eligible(model) && model.name().as_str().starts_with(&prefix)
        })
        .map(|model| model.name())
        .collect::<Vec<_>>();

    let mut counts = HashMap::<ModelName, usize>::default();
    for identifier in &eligible {
        *counts.entry(identifier.clone()).or_default() += 1;
    }
    eligible
        .into_iter()
        .filter(|identifier| counts.get(identifier) == Some(&1))
        .map(|identifier| identifier.to_string())
        .collect()
}

fn placement_member_model_is_eligible(model: &Model) -> bool {
    match model {
        Model::Generator(_)
        | Model::Inferencer(_)
        | Model::WasmProcessor(_)
        | Model::Reingestor(_)
        | Model::Lookup(_)
        | Model::Junction(_)
        | Model::Deduplicator(_)
        | Model::Correlator(_)
        | Model::Reorderer(_)
        | Model::WindowProcessor(_)
        | Model::Emitter(_) => true,
        Model::Ingestor(ingestor) => !matches!(&ingestor.source, IngestSource::Endpoint { .. }),
        Model::Relay(_) => true,
        _ => false,
    }
}

fn ordered_placement_corridor(endpoint: &PlacementEndpointPairPlan) -> Vec<NodeRef> {
    let longest_witness = endpoint
        .witnesses
        .iter()
        .max_by_key(|witness| witness.path.len());
    let mut ordered = if let Some(witness) = longest_witness {
        witness.path.clone()
    } else if endpoint.source == endpoint.destination {
        vec![endpoint.source.clone()]
    } else {
        vec![endpoint.source.clone(), endpoint.destination.clone()]
    };
    let mut seen = HashSet::default();
    let mut unique = Vec::with_capacity(endpoint.corridor.len());
    for node in ordered.drain(..).chain(endpoint.corridor.iter().cloned()) {
        if seen.insert(node.clone()) {
            unique.push(node);
        }
    }
    unique
}

fn placement_claim_owner(rules: &[PlacementName]) -> String {
    if rules.is_empty() {
        "domain default".to_string()
    } else {
        rules
            .iter()
            .map(|rule| rule.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn placement_group_members_equal(left: &[NodeRef], right: &[NodeRef]) -> bool {
    let right = right.iter().collect::<HashSet<_>>();
    left.len() == right.len() && left.iter().all(|member| right.contains(member))
}

fn placement_group_host<'a>(
    schedule: Option<&'a nervix_models::DomainSchedule>,
    members: &[NodeRef],
) -> Option<&'a ClusterNodeName> {
    let schedule = schedule?;
    let group = schedule
        .placement_groups
        .iter()
        .find(|group| placement_group_members_equal(&group.members, members))?;
    group.primary_node.as_ref()
}

fn placement_groups_claimed_by_rule<'a>(
    plan: &'a PlacementPlan,
    rule: &PlacementRulePlan,
) -> Vec<&'a PlacementRequireGroupPlan> {
    let require_claims = rule
        .claims
        .iter()
        .filter(|claim| {
            claim.effective && claim.effective_policy == PlacementPolicy::RequireColocation
        })
        .collect::<Vec<_>>();
    plan.require_groups
        .iter()
        .filter(|group| {
            require_claims.iter().any(|claim| {
                group.members.contains(&claim.left) && group.members.contains(&claim.right)
            })
        })
        .collect()
}

fn planned_relocation_count(
    current: Option<&nervix_models::DomainSchedule>,
    planned: Option<&nervix_models::DomainSchedule>,
) -> usize {
    planned_ownership_moves(current, planned).len()
}

fn prefer_former_owners_as_replicas(
    current: Option<&nervix_models::DomainSchedule>,
    planned: &mut nervix_models::DomainSchedule,
    live_nodes: &[ClusterNodeName],
) {
    let Some(current) = current else {
        return;
    };
    let live_nodes = live_nodes.iter().collect::<BTreeSet<_>>();
    for (identity, planned_node) in &mut planned.nodes {
        let Some(current_node) = current.nodes.get(identity) else {
            continue;
        };
        let (Some(former_owner), Some(destination)) = (
            current_node.execution_node(),
            planned_node.execution_node().cloned(),
        ) else {
            continue;
        };
        let replica_slots = planned_node.assigned_nodes.len();
        if *former_owner == destination || replica_slots < 2 || !live_nodes.contains(former_owner) {
            continue;
        }
        let mut assigned_nodes = vec![destination, former_owner.clone()];
        for assigned in &planned_node.assigned_nodes {
            if !assigned_nodes.contains(assigned) {
                assigned_nodes.push(assigned.clone());
            }
        }
        assigned_nodes.truncate(replica_slots);
        planned_node.assigned_nodes = assigned_nodes;
    }
}

fn planned_ownership_moves(
    current: Option<&nervix_models::DomainSchedule>,
    planned: Option<&nervix_models::DomainSchedule>,
) -> Vec<PlannedOwnershipMove> {
    let (Some(current), Some(planned)) = (current, planned) else {
        return Vec::new();
    };
    let mut moves = Vec::new();
    for (identity, planned_node) in &planned.nodes {
        let Some(current_node) = current.nodes.get(identity) else {
            continue;
        };
        let Some(former_owner) = current_node.execution_node() else {
            continue;
        };
        let Some(destination) = planned_node.execution_node() else {
            continue;
        };
        if former_owner == destination {
            continue;
        }
        moves.push(PlannedOwnershipMove {
            entity: NodeRef {
                kind: planned_node.kind(),
                identifier: planned_node.identifier.clone(),
            },
            former_owner: former_owner.clone(),
            destination: destination.clone(),
            replicas: planned_node.replica_nodes().into_iter().cloned().collect(),
            promoted_replica: current_node.is_assigned_to(destination),
        });
    }
    moves.sort_by(|left, right| left.entity.cmp(&right.entity));
    moves
}

fn mark_complete_ownership_transitions(
    current: Option<&nervix_models::DomainSchedule>,
    planned: &mut nervix_models::DomainSchedule,
) {
    let transition_id = uuid::Uuid::now_v7().to_string();
    for moved in planned_ownership_moves(current, Some(planned)) {
        let node = planned
            .nodes
            .get_mut(&moved.entity)
            .verified("every planned ownership move was derived from this target schedule");
        node.ownership_transition = Some(OwnershipTransition {
            id: transition_id.clone(),
            source: moved.former_owner,
            destination: moved.destination,
            state_recovery: OwnershipStateRecoveryOutcome::Complete,
            resets: Vec::new(),
        });
    }
}

fn format_planned_ownership_move(moved: &PlannedOwnershipMove) -> String {
    let replicas = if moved.replicas.is_empty() {
        "none".to_string()
    } else {
        moved.replicas.join(",")
    };
    format!(
        "- kind={} name={} from={} to={} replicas={} promoted_replica={}",
        moved.entity.kind.as_ref(),
        moved.entity.identifier.as_str(),
        moved.former_owner,
        moved.destination,
        replicas,
        if moved.promoted_replica { "yes" } else { "no" }
    )
}

fn requires_request_domain(statement: &Statement) -> bool {
    !matches!(
        statement,
        Statement::CreateDomain(_)
            | Statement::CreateUser(_)
            | Statement::StopDomain(_)
            | Statement::ShowClusterStatus(_)
            | Statement::ShowTransactions(_)
            | Statement::DropNode(_)
            | Statement::CordonNode(_)
            | Statement::UncordonNode(_)
            | Statement::DrainNode(_)
    )
}

fn requires_existing_domain(statement: &Statement) -> bool {
    !matches!(
        statement,
        Statement::CreateDomain(_)
            | Statement::CreateUser(_)
            | Statement::StopDomain(_)
            | Statement::ShowClusterStatus(_)
            | Statement::ShowTransactions(_)
            | Statement::DropNode(_)
            | Statement::CordonNode(_)
            | Statement::UncordonNode(_)
            | Statement::DrainNode(_)
    )
}

fn requires_runtime_reconcile(statement: &Statement) -> bool {
    requires_existing_domain(statement) && !matches!(statement, Statement::StartDomain(_))
}

fn requires_leader(statement: &Statement) -> bool {
    !matches!(
        statement,
        Statement::ShowClusterStatus(_)
            | Statement::ShowTransactions(_)
            | Statement::DescribeResource(_)
            | Statement::DescribeDomain(_)
            | Statement::DescribeEndpoint(_)
            | Statement::DescribeIngestor(_)
            | Statement::DescribeRelay(_)
            | Statement::DescribeLookup(_)
            | Statement::DescribeJunction(_)
            | Statement::DescribeDeduplicator(_)
            | Statement::DescribeReingestor(_)
            | Statement::DescribeCorrelator(_)
            | Statement::DescribeReorderer(_)
            | Statement::DescribeEmitter(_)
            | Statement::DescribeUdf(_)
            | Statement::DescribeWasmProcessor(_)
            | Statement::DescribeWindowProcessor(_)
            | Statement::DescribePlacement(_)
            | Statement::DescribeRelocation(_)
            | Statement::LookupQuery(_)
            | Statement::ShowCreate(_)
            | Statement::ShowUdfs(_)
            | Statement::ShowPlacements(_)
            | Statement::ShowRelayMaterializedState(_)
    )
}

fn validate_domain_config(config: &DomainConfig) -> Result<(), String> {
    if let DomainPace::Paced = config.pace {
        domain_clock_period(config)?;
        let skew = humantime::parse_duration(&config.skew)
            .map_err(|err| format!("invalid domain skew '{}': {err}", config.skew))?;
        u64::try_from(skew.as_nanos()).map_err(|_| {
            format!(
                "invalid domain skew '{}': duration does not fit in 64-bit nanoseconds",
                config.skew
            )
        })?;
    }
    Ok(())
}

fn domain_clock_period(config: &DomainConfig) -> Result<DomainClockPeriod, String> {
    config
        .period
        .parse::<DomainClockPeriod>()
        .map_err(|error| format!("invalid domain period '{}': {error}", config.period))
}

fn current_timestamp() -> Timestamp {
    Timestamp::now()
}

fn subtract_timestamp_duration(timestamp: Timestamp, duration: Duration) -> Timestamp {
    // A skew window extends to the first representable instant when its lower edge would precede
    // the timestamp model's range.
    timestamp
        .checked_sub(duration)
        .unwrap_or_else(|_| Timestamp::from_unix_nanos(i64::MIN))
}

async fn hash_password(password: String) -> Result<String, String> {
    tokio::task::spawn_blocking(move || {
        let mut rng = OsRng;
        let salt = SaltString::generate(&mut rng);
        password_argon2()
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("password hash task failed: {error}"))?
}

async fn verify_password_hash(password_hash: String, password: String) -> bool {
    tokio::task::spawn_blocking(move || {
        let Ok(parsed_hash) = PasswordHash::new(&password_hash) else {
            return false;
        };
        password_argon2()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    })
    .await
    .unwrap_or(false)
}

#[cfg(not(feature = "testing"))]
fn password_argon2() -> Argon2<'static> {
    Argon2::default()
}

#[cfg(feature = "testing")]
const TESTING_ARGON2_MEMORY_COST: u32 = 8;
#[cfg(feature = "testing")]
const TESTING_ARGON2_TIME_COST: u32 = 1;
#[cfg(feature = "testing")]
const TESTING_ARGON2_PARALLELISM: u32 = 1;

#[cfg(feature = "testing")]
fn password_argon2() -> Argon2<'static> {
    let params = Params::new(
        TESTING_ARGON2_MEMORY_COST,
        TESTING_ARGON2_TIME_COST,
        TESTING_ARGON2_PARALLELISM,
        None,
    )
    .assured("the testing cost constants are inside the ranges Argon2 accepts");
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

async fn user_credentials(name: UserName, password: String) -> Result<UserCredentials, String> {
    let password_hash = hash_password(password).await?;
    Ok(UserCredentials {
        name,
        password_hash,
    })
}

fn command_ok(message: String) -> CommandResult {
    command_ok_with_state(message, false)
}

fn command_ok_already_existed(message: String) -> CommandResult {
    command_ok_with_state(message, true)
}

fn command_ok_with_state(message: String, already_existed: bool) -> CommandResult {
    CommandResult {
        success: true,
        message,
        diagnostics: Vec::new(),
        kind: i32::from(CommandResultKind::Ok),
        already_existed,
        ..Default::default()
    }
}

fn append_command_result(results: &mut Vec<CommandResult>, result: CommandResult) {
    if result.results.is_empty() {
        results.push(result);
    } else {
        results.extend(result.results);
    }
}

fn command_batch_result(
    mut previous_results: Vec<CommandResult>,
    result: CommandResult,
    is_batch: bool,
) -> CommandResult {
    if !is_batch {
        return result;
    }

    let transaction = result.transaction.clone();
    let leader = result.leader.clone();
    let leader_grpc_uri = result.leader_grpc_uri.clone();
    let leader_web_console_uri = result.leader_web_console_uri.clone();
    append_command_result(&mut previous_results, result);
    let success = previous_results.iter().all(|result| result.success);
    let diagnostics = match previous_results.last() {
        Some(result) => result.diagnostics.clone(),
        None => Vec::new(),
    };
    let failure_kind = match previous_results.last() {
        Some(result) => result.kind,
        None => i32::from(CommandResultKind::Error),
    };
    CommandResult {
        success,
        message: command_results_message(&previous_results),
        diagnostics,
        kind: if success {
            i32::from(CommandResultKind::Ok)
        } else {
            failure_kind
        },
        results: previous_results,
        transaction,
        leader,
        leader_grpc_uri,
        leader_web_console_uri,
        ..Default::default()
    }
}

fn command_results_message(results: &[CommandResult]) -> String {
    results
        .iter()
        .map(|result| result.message.as_str())
        .filter(|message| !message.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn append_command_output(message: &mut String, output: &str) {
    if !message.is_empty() {
        message.push_str("; ");
    }
    message.push_str(output);
}

fn quiesce_level_message(level: QuiesceLevel) -> String {
    format!("quiesce level: {}", level.as_str())
}

fn model_mutation_success_result(
    existing_results: &[Option<CommandResult>],
    applied: &[AppliedModelMutation],
    classified_level: QuiesceLevel,
    planned_relocations: usize,
) -> CommandResult {
    let mut results = existing_results.to_vec();
    let mut first_applied = true;
    for mutation in applied {
        let mut message = mutation.message.clone();
        if first_applied {
            append_command_output(&mut message, &quiesce_level_message(classified_level));
            append_command_output(
                &mut message,
                &format!("planned relocations: {planned_relocations}"),
            );
            first_applied = false;
        }
        results[mutation.index] = Some(CommandResult {
            success: true,
            message,
            diagnostics: Vec::new(),
            kind: i32::from(CommandResultKind::Ok),
            ..Default::default()
        });
    }
    let results = results
        .into_iter()
        .map(|result| {
            result.verified(
                "each statement either filled its own slot or was recorded in applied, which this \
                 function fills",
            )
        })
        .collect::<Vec<_>>();
    CommandResult {
        success: true,
        message: command_results_message(&results),
        diagnostics: Vec::new(),
        kind: i32::from(CommandResultKind::Ok),
        results,
        ..Default::default()
    }
}

fn command_error(message: String) -> CommandResult {
    CommandResult {
        success: false,
        diagnostics: vec![Diagnostic {
            message: message.clone(),
            span_start: 0,
            span_end: 0,
        }],
        message,
        kind: i32::from(CommandResultKind::Error),
        ..Default::default()
    }
}

fn transaction_status(transaction: &ReplicatedTransaction) -> ApiTransactionStatus {
    /// How a transaction ended, as the API reports it. Only a failure carries an error and the
    /// step it failed on; every other state reports neither.
    struct ReportedOutcome {
        state: ApiTransactionState,
        error: String,
        failing_step: Option<u64>,
    }

    impl ReportedOutcome {
        fn without_error(state: ApiTransactionState) -> Self {
            Self {
                state,
                error: String::new(),
                failing_step: None,
            }
        }
    }

    let ReportedOutcome {
        state,
        error,
        failing_step,
    } = match &transaction.state {
        TransactionState::Open => ReportedOutcome::without_error(ApiTransactionState::Open),
        TransactionState::Committing(_) => {
            ReportedOutcome::without_error(ApiTransactionState::Committing)
        }
        TransactionState::Finished(finished) => match &finished.outcome {
            TransactionOutcome::Committed => {
                ReportedOutcome::without_error(ApiTransactionState::Committed)
            }
            TransactionOutcome::Failed {
                failing_step,
                error,
            } => ReportedOutcome {
                state: ApiTransactionState::Failed,
                error: error.clone(),
                failing_step: failing_step.checked_add(1).map(|step| step.arch_into()),
            },
            TransactionOutcome::Reverted => {
                ReportedOutcome::without_error(ApiTransactionState::Reverted)
            }
            TransactionOutcome::Expired => {
                ReportedOutcome::without_error(ApiTransactionState::Expired)
            }
        },
    };
    ApiTransactionStatus {
        id: transaction.id.clone(),
        domain: transaction.domain.to_string(),
        state: i32::from(state),
        pending_count: transaction.pending_statement_count().arch_into(),
        completed_count: transaction.completed_statement_count().arch_into(),
        total_count: transaction.statement_count.arch_into(),
        error,
        failing_step,
    }
}

fn replicated_command_result(result: &CommandResult) -> TransactionCommandResult {
    TransactionCommandResult {
        success: result.success,
        message: result.message.clone(),
        diagnostics: result
            .diagnostics
            .iter()
            .map(|diagnostic| TransactionDiagnostic {
                message: diagnostic.message.clone(),
                span_start: diagnostic.span_start,
                span_end: diagnostic.span_end,
            })
            .collect(),
        already_existed: result.already_existed,
    }
}

fn transaction_commit_result(transaction: &ReplicatedTransaction) -> CommandResult {
    let success = matches!(
        transaction.finished_outcome(),
        Some(TransactionOutcome::Committed)
    );
    let quiesce_level = transaction
        .commit_results()
        .iter()
        .filter_map(|step| step.quiesce_level)
        .max();
    let planned_relocations = transaction
        .commit_results()
        .iter()
        .filter_map(|step| step.planned_relocations)
        .sum::<usize>();
    let mut message = match transaction.finished_outcome() {
        Some(TransactionOutcome::Committed) => String::new(),
        Some(TransactionOutcome::Failed { error, .. }) => error.clone(),
        Some(outcome) => format!("transaction finished with outcome {}", outcome.as_str()),
        None => "transaction commit is still in progress".to_string(),
    };
    if let Some(quiesce_level) = quiesce_level {
        append_command_output(&mut message, &quiesce_level_message(quiesce_level));
    }
    if planned_relocations > 0 {
        append_command_output(
            &mut message,
            &format!("planned relocations: {planned_relocations}"),
        );
    }
    let mut reported = transaction
        .commit_results()
        .iter()
        .rev()
        .find(|step| !step.result.success);
    if reported.is_none() {
        reported = transaction.commit_results().last();
    }
    let diagnostics = match reported {
        Some(step) => step
            .result
            .diagnostics
            .iter()
            .map(|diagnostic| Diagnostic {
                message: diagnostic.message.clone(),
                span_start: diagnostic.span_start,
                span_end: diagnostic.span_end,
            })
            .collect(),
        None => Vec::new(),
    };
    let mut result = CommandResult {
        success,
        message,
        diagnostics,
        kind: if success {
            i32::from(CommandResultKind::Ok)
        } else {
            i32::from(CommandResultKind::Error)
        },
        ..Default::default()
    };
    result.transaction = Some(transaction_status(transaction));
    result
}

fn is_queueable_transaction_statement(statement: &Statement) -> bool {
    statement.is_model_mutation()
        || matches!(
            statement,
            Statement::AlterDomain(_)
                | Statement::StartDomain(_)
                | Statement::StopDomain(_)
                | Statement::CreateResource(_)
        )
}

fn transaction_statement_label(statement: &Statement) -> &'static str {
    match statement {
        Statement::CreateDomain(_) => "CREATE DOMAIN",
        Statement::CreateUser(_) => "CREATE USER",
        Statement::UploadResource(_) => "UPLOAD RESOURCE",
        Statement::DropNode(_) => "DROP NODE",
        Statement::CordonNode(_) => "CORDON",
        Statement::UncordonNode(_) => "UNCORDON",
        Statement::DrainNode(_) => "DRAIN",
        Statement::Relocate(_) => "RELOCATE",
        Statement::LookupQuery(_) => "LOOKUP",
        Statement::ShowCreate(_)
        | Statement::ShowUdfs(_)
        | Statement::ShowPlacements(_)
        | Statement::ShowRelayMaterializedState(_)
        | Statement::ShowClusterStatus(_)
        | Statement::ShowTransactions(_) => "SHOW",
        Statement::DescribeRelay(_)
        | Statement::DescribeDomain(_)
        | Statement::DescribeIngestor(_)
        | Statement::DescribeResource(_)
        | Statement::DescribeLookup(_)
        | Statement::DescribeEndpoint(_)
        | Statement::DescribeJunction(_)
        | Statement::DescribeDeduplicator(_)
        | Statement::DescribeReingestor(_)
        | Statement::DescribeCorrelator(_)
        | Statement::DescribeReorderer(_)
        | Statement::DescribeEmitter(_)
        | Statement::DescribeWindowProcessor(_)
        | Statement::DescribeWasmProcessor(_)
        | Statement::DescribeUdf(_)
        | Statement::DescribePlacement(_)
        | Statement::DescribeRelocation(_) => "DESCRIBE",
        _ => "statement",
    }
}

async fn fetch_resource_archive(
    interconnect: &Transport,
    source_node: &ClusterNodeName,
    id: &ResourceId,
) -> Result<DownloadedResourceArchive, String> {
    let temp_archive = tempfile::NamedTempFile::new()
        .map_err(|error| format!("failed to create temporary resource archive: {error}"))?;
    let temp_path = temp_archive.into_temp_path();
    let mut file = File::create(&temp_path)
        .await
        .map_err(|error| format!("failed to open temporary resource archive: {error}"))?;
    let mut hasher = Hasher::new();
    let mut offset = 0_u64;
    loop {
        tokio::task::consume_budget().await;
        let chunk = interconnect
            .request(
                source_node,
                FetchResourceArchiveChunk {
                    id: id.clone(),
                    offset,
                },
            )
            .await
            .map_err(|error| format!("resource fetch request failed: {error}"))?
            .map_err(|error| format!("resource fetch failed: {error}"))?;
        if chunk.bytes.is_empty() && !chunk.eof {
            return Err("resource fetch made no progress".to_string());
        }
        hasher.update(&chunk.bytes);
        file.write_all(&chunk.bytes)
            .await
            .map_err(|error| format!("failed to write temporary resource archive: {error}"))?;
        let chunk_bytes = u64::try_from(chunk.bytes.len())
            .map_err(|error| format!("resource chunk length is invalid: {error}"))?;
        offset = offset
            .checked_add(chunk_bytes)
            .ok_or_else(|| "resource archive offset overflowed".to_string())?;
        if chunk.eof {
            break;
        }
    }
    file.flush()
        .await
        .map_err(|error| format!("failed to flush temporary resource archive: {error}"))?;
    drop(file);
    let hash = hasher.finalize();
    Ok(DownloadedResourceArchive {
        path: temp_path,
        root_checksum: encode_hex(hash.as_bytes()),
    })
}

fn error_response(kind: &str, diagnostics: &[ParseDiagnostic]) -> CommandResult {
    CommandResult {
        success: false,
        message: kind.to_string(),
        diagnostics: diagnostics.iter().map(map_diagnostic).collect(),
        kind: i32::from(CommandResultKind::Error),
        ..Default::default()
    }
}

impl SessionServiceImpl {
    async fn web_console_leadership_response(
        &self,
        already_connected_to_leader: bool,
    ) -> Option<SessionResponse> {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            let result = self.not_leader_response("", leader).await;
            return Some(SessionResponse {
                event: Some(proto::session_response::Event::Result(result)),
            });
        }

        if already_connected_to_leader {
            return None;
        }

        Some(SessionResponse {
            event: Some(proto::session_response::Event::Server(ServerEvent {
                level: i32::from(ServerEventLevel::Info),
                message: format!(
                    "connected to leader '{}'",
                    self.inner.consensus.local_node_id()
                ),
            })),
        })
    }

    async fn domain_list_response(&self, response_to_request: bool) -> SessionResponse {
        let domains = self
            .inner
            .consensus
            .current_domains()
            .await
            .into_values()
            .map(|domain| DomainInfo {
                id: domain.id.as_str().to_string(),
                pace: domain.config.pace.as_ref().to_string(),
                status: domain.status.as_ref().to_string(),
            })
            .collect();
        SessionResponse {
            event: Some(proto::session_response::Event::Domains(DomainList {
                domains,
                response_to_request,
            })),
        }
    }

    async fn web_console_cluster_summary_response(&self) -> SessionResponse {
        let running_domains = self
            .inner
            .consensus
            .current_domains()
            .await
            .into_values()
            .filter(|domain| domain.status == DomainStatus::Running)
            .count();
        let (nodes, relays) = self.inner.registry.active_graphs().into_iter().fold(
            (0_usize, 0_usize),
            |(nodes, relays), (_, graph)| {
                let counts = graph.dataflow_graph_counts();
                (nodes + counts.nodes, relays + counts.relays)
            },
        );
        SessionResponse {
            event: Some(proto::session_response::Event::Cluster(ClusterSummary {
                running_domains: running_domains.arch_into(),
                nodes: nodes.arch_into(),
                relays: relays.arch_into(),
            })),
        }
    }

    async fn web_console_domain_snapshot_responses(
        &self,
        active_domain: Option<&DomainName>,
    ) -> Vec<SessionResponse> {
        let resources = self.inner.consensus.current_resources().await;
        let domains = self.inner.consensus.current_domains().await;
        let resource_entities = resources
            .next_version_by_resource
            .iter()
            .filter(|counter| Some(&counter.domain) == active_domain)
            .map(|counter| DomainEntitySnapshot {
                kind: "resource".to_string(),
                identifier: counter.identifier.as_str().to_string(),
                detail: if counter.next_version > 1 {
                    format!("v{}", counter.next_version - 1)
                } else {
                    "catalog".to_string()
                },
            })
            .collect::<Vec<_>>();
        let active_graphs = self
            .inner
            .registry
            .active_graphs()
            .into_iter()
            .filter(|(domain, _)| active_domain.is_none_or(|active| domain == active))
            .collect::<Vec<_>>();
        let active_graph_domains = active_graphs
            .iter()
            .map(|(domain, _)| domain.clone())
            .collect::<BTreeSet<_>>();
        let mut responses = Vec::new();
        for (domain, graph) in active_graphs {
            tokio::task::consume_budget().await;
            if let Some(response) = self
                .web_console_domain_snapshot_response(
                    domain.clone(),
                    graph.to_dataflow_graph(domain.as_str()),
                    &resource_entities,
                )
                .await
            {
                responses.push(response);
            }
        }

        for domain in domains.keys() {
            tokio::task::consume_budget().await;
            if active_domain.is_some_and(|active| active != domain)
                || active_graph_domains.contains(domain)
            {
                continue;
            }
            if let Some(response) = self
                .web_console_domain_snapshot_response(
                    domain.clone(),
                    DataflowGraph::new(domain.as_str()),
                    &resource_entities,
                )
                .await
            {
                responses.push(response);
            }
        }
        responses
    }

    async fn web_console_domain_snapshot_response(
        &self,
        domain: DomainName,
        mut dataflow_graph: DataflowGraph,
        resource_entities: &[DomainEntitySnapshot],
    ) -> Option<SessionResponse> {
        dataflow_graph.statistics = self.inner.runtime.dataflow_domain_statistics(&domain);
        for node in &mut dataflow_graph.nodes {
            let Some((kind, identifier)) = dataflow_metric_target(&node.id) else {
                continue;
            };
            let health = self
                .dataflow_node_status_for_graph(&domain, &kind, &identifier)
                .await;
            node.status = health.status;
            node.status_detail = health.detail;
            node.reconnect_wait_millis = health.reconnect_wait_millis;
            if kind == "RELAY" {
                node.statistics = self
                    .inner
                    .runtime
                    .dataflow_relay_buffer_statistics(&domain, &RelayName::from(&identifier));
                let existing = node
                    .branches
                    .iter()
                    .map(|branch| branch.branch.clone())
                    .collect::<BTreeSet<_>>();
                node.branches.extend(
                    self.inner
                        .runtime
                        .dataflow_relay_branch_statistics(&domain, &RelayName::from(&identifier))
                        .into_iter()
                        .filter(|branch| !existing.contains(&branch.branch)),
                );
            }
        }
        for edge in &mut dataflow_graph.edges {
            let Some(metric) = edge.metric.as_ref() else {
                continue;
            };
            edge.statistics = self.inner.runtime.dataflow_edge_statistics(&domain, metric);
            edge.branches = self
                .inner
                .runtime
                .dataflow_edge_branch_statistics(&domain, metric);
        }
        match dataflow_graph.serialize() {
            Ok(graph_bytes) => Some(SessionResponse {
                event: Some(proto::session_response::Event::Snapshot(DomainSnapshot {
                    domain: domain.as_str().to_string(),
                    dataflow_graph: graph_bytes.into(),
                    entities: self
                        .inner
                        .registry
                        .active_domain_entities(&domain)
                        .into_iter()
                        .map(|entity| DomainEntitySnapshot {
                            kind: entity.kind.as_str().to_string(),
                            identifier: entity.identifier.as_str().to_string(),
                            detail: entity.kind.as_str().replace('_', " ").to_ascii_uppercase(),
                        })
                        .chain(resource_entities.iter().cloned())
                        .collect(),
                })),
            }),
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    error = %error,
                    "failed to serialize web console domain snapshot"
                );
                None
            }
        }
    }

    async fn consensus_error_response(
        &self,
        error: &ConsensusError,
        message: String,
    ) -> CommandResult {
        match error {
            ConsensusError::LeadershipLost { leader_id } => {
                self.not_leader_response("", leader_id.clone()).await
            }
            _ => command_error(message),
        }
    }

    async fn not_leader_response(
        &self,
        query: &str,
        leader: Option<ClusterNodeName>,
    ) -> CommandResult {
        let leader_node = match leader.as_ref() {
            Some(leader_id) => self
                .inner
                .cluster
                .gossip_state()
                .await
                .live_nodes
                .into_iter()
                .find(|node| node.node_id == *leader_id),
            None => None,
        };
        let mut leader_grpc_uri = String::new();
        if let Some(node) = leader_node.as_ref()
            && let Some(uri) = grpc_uri_from_advertise_addr(&node.grpc_advertise_addr)
        {
            leader_grpc_uri = uri;
        }
        let leader_web_console_uri = match leader_node {
            Some(node) => node.web_console_advertise_addr,
            None => String::new(),
        };
        let diagnostic = match leader.as_ref() {
            Some(leader) => format!("retry this command on leader '{leader}'"),
            None => "retry this command on the current leader".to_string(),
        };
        CommandResult {
            success: false,
            message: "not-a-leader".to_string(),
            diagnostics: vec![Diagnostic {
                message: diagnostic,
                span_start: 0,
                span_end: u32::try_from(query.len()).unwrap_or(0),
            }],
            kind: i32::from(CommandResultKind::NotLeader),
            leader: match leader {
                Some(leader) => leader.to_string(),
                None => String::new(),
            },
            leader_grpc_uri,
            leader_web_console_uri,
            ..Default::default()
        }
    }
}

/// The kind and model a dataflow metric identifier addresses, or `None` when `id` is not one.
fn dataflow_metric_target(id: &str) -> Option<(String, ModelName)> {
    let (kind, identifier) = id.split_once(':')?;
    Some((
        kind.to_ascii_uppercase(),
        ModelName::parse(identifier).ok()?,
    ))
}

fn grpc_uri_from_advertise_addr(addr: &str) -> Option<String> {
    if addr.is_empty() {
        None
    } else if addr.starts_with("http://") || addr.starts_with("https://") {
        Some(addr.to_string())
    } else {
        Some(format!("http://{addr}"))
    }
}

fn grpc_client_connect_options(
    server: &str,
    credentials: Option<&BasicAuthCredentials>,
) -> ClientConnectOptions {
    ClientConnectOptions {
        tls_requirement: Some(ClientTlsRequirement::Preferred),
        ca_certificate_pem: server
            .starts_with("https://")
            .then(|| std::fs::read(internal_tls_path(INTERNAL_TLS_CA_FILE)).ok())
            .flatten(),
        username: credentials.map(|credentials| credentials.username.clone()),
        password: credentials.map(|credentials| credentials.password.clone()),
    }
}

fn create_registry_error_response(
    query: &str,
    domain: &DomainName,
    model_id: &ModelName,
    err: &error_stack::Report<RegistryError>,
) -> CommandResult {
    match err.current_context() {
        RegistryError::AlreadyExists { .. } => {
            let span = find_identifier_span(query, model_id).unwrap_or(0..0);
            CommandResult {
                success: false,
                message: format!(
                    "{} '{}' already exists in domain '{}'",
                    infer_kind_from_error_target(err, model_id).unwrap_or("model"),
                    model_id.as_str(),
                    domain.as_str()
                ),
                diagnostics: vec![Diagnostic {
                    message: format!("'{}' already exists", model_id.as_str()),
                    span_start: u32::try_from(span.start).unwrap_or(0),
                    span_end: u32::try_from(span.end).unwrap_or(0),
                }],
                kind: i32::from(CommandResultKind::Error),
                ..Default::default()
            }
        }
        RegistryError::NotFound { .. }
        | RegistryError::StoredModelKindMismatch { .. }
        | RegistryError::DeleteInUse { .. }
        | RegistryError::InvalidModel { .. } => {
            let span = find_identifier_span(query, model_id).unwrap_or(0..0);
            CommandResult {
                success: false,
                message: format!("{err}"),
                diagnostics: vec![Diagnostic {
                    message: format!("{err}"),
                    span_start: u32::try_from(span.start).unwrap_or(0),
                    span_end: u32::try_from(span.end).unwrap_or(0),
                }],
                kind: i32::from(CommandResultKind::Error),
                ..Default::default()
            }
        }
        RegistryError::MissingReference { reference, .. } => {
            // A reference that is not a model name has nothing to underline in the query, and a
            // diagnostic without a span is still the diagnostic the operator needs.
            let span = match ModelName::try_from(reference.as_str()) {
                Ok(id) => find_identifier_span(query, &id).unwrap_or(0..0),
                Err(_) => 0..0,
            };
            CommandResult {
                success: false,
                message: format!("{err}"),
                diagnostics: vec![Diagnostic {
                    message: format!("{err}"),
                    span_start: u32::try_from(span.start).unwrap_or(0),
                    span_end: u32::try_from(span.end).unwrap_or(0),
                }],
                kind: i32::from(CommandResultKind::Error),
                ..Default::default()
            }
        }
        _ => CommandResult {
            success: false,
            message: format!("{err}"),
            diagnostics: vec![Diagnostic {
                message: format!("{err}"),
                span_start: 0,
                span_end: 0,
            }],
            kind: i32::from(CommandResultKind::Error),
            ..Default::default()
        },
    }
}

fn infer_kind_from_error_target(
    err: &error_stack::Report<RegistryError>,
    model_id: &ModelName,
) -> Option<&'static str> {
    match err.current_context() {
        RegistryError::AlreadyExists { identifier, .. } if identifier == model_id.as_str() => {
            Some("model")
        }
        _ => None,
    }
}

fn map_diagnostic(d: &ParseDiagnostic) -> Diagnostic {
    Diagnostic {
        message: d.message.clone(),
        span_start: u32::try_from(d.span.start).unwrap_or(u32::MAX),
        span_end: u32::try_from(d.span.end).unwrap_or(u32::MAX),
    }
}

/// Where `identifier` appears in `query`, for a diagnostic that wants to underline it.
///
/// The query reached here because it failed validation, so it may also fail to lex. A diagnostic
/// without a span is still a diagnostic, which is why absence is the answer rather than an error.
fn find_identifier_span(query: &str, identifier: &ModelName) -> Option<std::ops::Range<usize>> {
    let tokens = lex(query).ok()?;
    tokens.into_iter().find_map(|spanned| match spanned.token {
        Token::Word(Word::KnownWord { raw, .. }) | Token::Word(Word::UnknownWord(raw))
            if raw.eq_ignore_ascii_case(identifier.as_str()) =>
        {
            Some(spanned.span.into_range())
        }
        _ => None,
    })
}

fn current_word_prefix(input: &str, cursor: usize) -> String {
    let end = cursor.min(input.len());
    let mut out = String::new();
    for ch in input[..end].chars().rev() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.insert(0, ch.to_ascii_lowercase());
        } else {
            break;
        }
    }
    out
}

/// Completion input split at the cursor: the source the grammar parses with the half-typed word
/// removed, where that word started, and the word itself for filtering the offers.
struct CompletionContext {
    grammar_input: String,
    grammar_cursor: usize,
    prefix: String,
}

fn completion_context(input: &str, cursor: usize) -> CompletionContext {
    let safe_cursor = cursor.min(input.len());
    let start = word_start(input, safe_cursor);
    let prefix = current_word_prefix(input, safe_cursor);

    let mut grammar_input = String::with_capacity(input.len() - (safe_cursor - start));
    grammar_input.push_str(&input[..start]);
    grammar_input.push_str(&input[safe_cursor..]);

    CompletionContext {
        grammar_input,
        grammar_cursor: start,
        prefix,
    }
}

struct VhostTlsMaterials {
    certified_key: CertifiedKey,
}

fn grpc_base_url(mode: InternalTransportMode, advertise_addr: &cluster::HostPort) -> String {
    format!("{}://{}", mode.scheme(), advertise_addr.url_authority())
}

fn internal_tls_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tls")
        .join("dev")
        .join(file_name)
}

fn load_web_console_tls_server_config(
    cert_path: &Path,
    key_path: &Path,
) -> Result<StdArc<ServerConfig>, Report<AppError>> {
    nervix_interconnect::install_rustls_crypto_provider();
    let cert_chain = load_certificates_from_pem_file(cert_path)
        .map_err(|error| Report::new(AppError::LoadWebConsoleTls).attach_printable(error))?;
    let private_key = load_private_key_from_pem_file(key_path)
        .map_err(|error| Report::new(AppError::LoadWebConsoleTls).attach_printable(error))?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|error| {
            Report::new(AppError::LoadWebConsoleTls).attach_printable(error.to_string())
        })?;
    Ok(StdArc::new(config))
}

async fn load_grpc_tls_server_config() -> Result<ServerTlsConfig, Report<AppError>> {
    nervix_interconnect::install_rustls_crypto_provider();
    let cert_path = internal_tls_path(INTERNAL_TLS_CERT_FILE);
    let key_path = internal_tls_path(INTERNAL_TLS_KEY_FILE);
    let cert_pem = tokio::fs::read(cert_path)
        .await
        .map_err(|error| Report::new(AppError::LoadGrpcTls).attach_printable(error.to_string()))?;
    let key_pem = tokio::fs::read(key_path)
        .await
        .map_err(|error| Report::new(AppError::LoadGrpcTls).attach_printable(error.to_string()))?;
    Ok(ServerTlsConfig::new().identity(TonicIdentity::from_pem(cert_pem, key_pem)))
}

/// Resolves a model's resource reference to the concrete version it binds to. Resources are
/// domain-owned, so a name only resolves against versions uploaded into the referencing domain.
fn resolve_resource_id(
    resources: &nervix_models::ResourceVersionStatus,
    domain: &DomainName,
    identifier: &ResourceName,
    requested_version: Option<u64>,
) -> Result<ResourceId, String> {
    if let Some(version) = requested_version {
        let id = ResourceId::new(domain.clone(), identifier.clone(), version);
        if resources.versions.iter().any(|resource| resource.id == id) {
            return Ok(id);
        }
        return Err(format!(
            "resource '{}@{}' does not exist in domain '{}'",
            identifier.as_str(),
            version,
            domain.as_str()
        ));
    }

    let latest = resources
        .versions
        .iter()
        .filter(|resource| resource.id.domain == *domain && resource.id.identifier == *identifier)
        .map(|resource| resource.id.version)
        .max();
    let Some(version) = latest else {
        return Err(format!(
            "resource '{}' has no uploaded versions in domain '{}'",
            identifier.as_str(),
            domain.as_str()
        ));
    };
    Ok(ResourceId::new(domain.clone(), identifier.clone(), version))
}

async fn load_vhost_tls_materials(
    resource_store: &ResourceStore,
    id: &ResourceId,
) -> Result<VhostTlsMaterials, String> {
    let cert_path = resource_store
        .resolve_content_path(id, VHOST_TLS_CERT_PATH)
        .map_err(|error| error.to_string())?;
    let key_path = resource_store
        .resolve_content_path(id, VHOST_TLS_KEY_PATH)
        .map_err(|error| error.to_string())?;
    let ca_path = resource_store
        .resolve_content_path(id, VHOST_TLS_CA_PATH)
        .map_err(|error| error.to_string())?;

    ensure_file_exists(&cert_path, "tls certificate").await?;
    ensure_file_exists(&key_path, "tls private key").await?;
    ensure_file_exists(&ca_path, "tls CA certificate").await?;

    let _roots = load_root_store_from_pem_file(&ca_path)?;
    let cert_chain = load_certificates_from_pem_file(&cert_path)?;
    let private_key = load_private_key_from_pem_file(&key_path)?;
    let provider = rustls::crypto::CryptoProvider::get_default()
        .ok_or_else(|| "rustls crypto provider is not installed".to_string())?;
    let certified_key = CertifiedKey::from_der(cert_chain, private_key, provider)
        .map_err(|error| error.to_string())?;

    Ok(VhostTlsMaterials { certified_key })
}

async fn ensure_file_exists(path: &Path, label: &str) -> Result<(), String> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|_| format!("{label} file '{}' does not exist", path.display()))?;
    if metadata.is_file() {
        Ok(())
    } else {
        Err(format!("{label} path '{}' is not a file", path.display()))
    }
}

fn load_root_store_from_pem_file(path: &Path) -> Result<RootCertStore, String> {
    let certs = load_certificates_from_pem_file(path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(|error| error.to_string())?;
    }
    Ok(roots)
}

fn load_certificates_from_pem_file(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(map_pem_error_to_string)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_pem_error_to_string)?;
    if certs.is_empty() {
        return Err(format!("no certificates found in '{}'", path.display()));
    }
    Ok(certs)
}

fn load_private_key_from_pem_file(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path).map_err(map_pem_error_to_string)
}

fn map_pem_error_to_string(error: PemError) -> String {
    match error {
        PemError::NoItemsFound => "no PEM items found".to_string(),
        other => other.to_string(),
    }
}

fn resource_ref_suggestions(
    resources: &nervix_models::ResourceVersionStatus,
    domain: &DomainName,
    prefix: &str,
) -> Vec<String> {
    let mut suggestions = Vec::new();
    for counter in &resources.next_version_by_resource {
        if counter.domain == *domain
            && (prefix.is_empty() || counter.identifier.as_str().starts_with(prefix))
        {
            suggestions.push(counter.identifier.to_string());
        }
    }
    suggestions
}

fn resource_version_suggestions(
    resources: &nervix_models::ResourceVersionStatus,
    domain: &DomainName,
    identifier: &ResourceName,
    prefix: &str,
) -> Vec<String> {
    let mut suggestions = Vec::new();
    for resource in &resources.versions {
        if resource.id.domain != *domain || resource.id.identifier != *identifier {
            continue;
        }
        let version = resource.id.version.to_string();
        if prefix.is_empty() || version.starts_with(prefix) {
            suggestions.push(version);
        }
    }
    suggestions
}

fn requested_resource_versions(input: &str, cursor: usize) -> Option<ResourceName> {
    let safe_cursor = cursor.min(input.len());
    let raw_prefix = &input[..safe_cursor];
    let upper = raw_prefix.to_ascii_uppercase();
    let version_index = upper.find(" VERSION ")?;
    let before_version = raw_prefix[..version_index].trim_end();
    let resource_prefix = "DESCRIBE RESOURCE ";
    if !before_version
        .to_ascii_uppercase()
        .starts_with(resource_prefix)
    {
        return None;
    }
    let identifier = before_version[resource_prefix.len()..].trim();
    if identifier.is_empty() {
        return None;
    }
    ResourceName::parse(identifier).ok()
}

fn word_start(input: &str, cursor: usize) -> usize {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let boundary = input[..cursor.min(input.len())]
        .char_indices()
        .rev()
        .find(|(_, c)| !is_word(*c));
    match boundary {
        Some((index, character)) => index + character.len_utf8(),
        None => 0,
    }
}

fn parse_human_duration(input: &str) -> Result<Duration, String> {
    humantime::parse_duration(input).map_err(|err| err.to_string())
}

fn parse_human_bytes(input: &str) -> Result<ubyte::ByteUnit, String> {
    input
        .parse::<ubyte::ByteUnit>()
        .map_err(|err| err.to_string())
}

fn parse_trace_sample_ratio(input: &str) -> Result<f64, String> {
    let ratio = input
        .parse::<f64>()
        .map_err(|err| format!("invalid trace sample ratio: {err}"))?;
    if (0.0..=1.0).contains(&ratio) {
        Ok(ratio)
    } else {
        Err("trace sample ratio must be between 0.0 and 1.0".to_string())
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").assured("writing a byte into a String cannot fail");
    }
    out
}

async fn render_cluster_status(cluster: &cluster::ClusterHandle, consensus: &Observer) -> String {
    let gossip = cluster.gossip_state().await;
    let mut lines = Vec::new();

    lines.push("[chitchat]".to_string());
    lines.extend(cluster.status_lines().await);
    lines.push(String::new());
    lines.push("[raft]".to_string());
    lines.extend(consensus.status_lines().await);
    lines.push(String::new());
    lines.push("[interconnect]".to_string());
    lines.extend(cluster.interconnect_status_section());
    lines.push(String::new());
    lines.push("[domains]".to_string());
    lines.extend(consensus.domain_status_lines().await);
    lines.push(String::new());
    lines.push("[schedule]".to_string());
    lines.extend(render_cluster_schedule_lines(
        &consensus.current_schedule().await,
    ));
    lines.push(String::new());
    lines.push("[warnings]".to_string());

    let gossip_ids = gossip
        .live_nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let membership = consensus.membership_nodes().await;
    let raft_ids = membership
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();

    let mut warned = false;
    for missing in gossip_ids.difference(&raft_ids) {
        warned = true;
        lines.push(format!(
            "- gossip node '{missing}' is not present in raft membership"
        ));
    }
    for missing in raft_ids.difference(&gossip_ids) {
        warned = true;
        lines.push(format!(
            "- raft member '{missing}' is not currently visible in gossip"
        ));
    }
    for dead in gossip.dead_node_ids.intersection(&raft_ids) {
        warned = true;
        let source = if cluster.is_interconnect_unavailable(dead) {
            "interconnect"
        } else {
            "chitchat"
        };
        lines.push(format!(
            "- raft member '{dead}' is marked unavailable by {source}"
        ));
    }
    if !warned {
        lines.push("- none".to_string());
    }

    lines.join("\n")
}

fn render_cluster_schedule_lines(schedule: &nervix_models::ClusterSchedule) -> Vec<String> {
    if schedule.domains.is_empty() {
        return vec!["- none".to_string()];
    }

    let mut lines = Vec::new();
    for domain in schedule.domains.values() {
        if domain.nodes.is_empty() {
            lines.push(format!("- domain={} nodes=none", domain.domain.as_str()));
            continue;
        }

        for node in domain.nodes.values() {
            let owner = match node.execution_node() {
                Some(owner) => owner.as_str(),
                None => "-",
            };
            lines.push(format!(
                "- domain={} kind={} name={} owner={owner} replicas={}{}",
                domain.domain.as_str(),
                node.kind().as_str(),
                node.identifier.as_str(),
                format_schedule_status_replicas(node),
                format_ownership_transition(node)
            ));
        }
    }
    lines
}

fn format_schedule_status_replicas(node: &ScheduledNode) -> String {
    let replicas = node.replica_nodes();
    if replicas.is_empty() {
        "-".to_string()
    } else {
        replicas
            .iter()
            .map(|node| node.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn format_ownership_transition(node: &ScheduledNode) -> String {
    let Some(transition) = node.ownership_transition.as_ref() else {
        return String::new();
    };
    let mut rendered = format!(
        " transition_from={} state_recovery={}",
        transition.source,
        transition.state_recovery.as_ref()
    );
    if !transition.resets.is_empty() {
        rendered.push_str(" resets=");
        rendered.push_str(
            &transition
                .resets
                .iter()
                .map(|reset| format!("{}:{}", reset.component.as_ref(), reset.cause.as_ref()))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    rendered
}

async fn reconcile_domain_clock_tasks(
    service: &SessionServiceImpl,
    shutdown: &CancellationToken,
    tasks: &mut HashMap<DomainName, DomainClockTask>,
    retirements: &mut DomainClockRetirements,
) {
    retirements.reap().await;
    let state = service.inner.consensus.current_runtime_state().await;
    let local_identity = service.inner.cluster.local_node_identity().await;
    let gossip = service.inner.cluster.gossip_state().await;
    let targets = gossip.live_identities();
    let ready_nodes = service
        .inner
        .cluster
        .nodes_ready_for_runtime_revision(state.revision)
        .await;
    let all_targets_ready = !targets.is_empty()
        && targets
            .iter()
            .all(|identity| ready_nodes.contains(identity));

    let mut desired = HashMap::<DomainName, DomainClockTaskSpec>::new();
    if all_targets_ready {
        for (domain_id, domain) in &state.domains {
            tokio::task::consume_budget().await;
            if matches!(domain.status, DomainStatus::Stopped)
                || matches!(domain.config.pace, DomainPace::Unpaced)
            {
                continue;
            }
            let Some(clock) = domain.clock.clone() else {
                continue;
            };
            let Some(authority) = state.domain_clock_authorities.get(domain_id) else {
                continue;
            };
            let Some(owner) = authority.owner() else {
                continue;
            };
            if owner != &local_identity {
                continue;
            }
            let period = match domain_clock_period(&domain.config) {
                Ok(period) => period,
                Err(error) => {
                    warn!(
                        domain = domain_id.as_str(),
                        error, "committed paced domain has an invalid clock period"
                    );
                    continue;
                }
            };
            desired.insert(
                domain_id.clone(),
                DomainClockTaskSpec {
                    clock,
                    period,
                    generation: domain.start_version,
                    authority_revision: authority.revision(),
                    authority: owner.clone(),
                    targets: targets.clone(),
                },
            );
        }
    }

    let existing = tasks.keys().cloned().collect::<Vec<_>>();
    for domain_id in existing {
        let unchanged = match (tasks.get(&domain_id), desired.get(&domain_id)) {
            (Some(task), Some(spec)) => &task.spec == spec,
            _ => false,
        };
        if !unchanged && let Some(task) = tasks.remove(&domain_id) {
            retirements.retire(domain_id, task.task);
        }
    }

    for (domain_id, spec) in desired {
        tokio::task::consume_budget().await;
        if tasks.contains_key(&domain_id) || retirements.contains(&domain_id) {
            continue;
        }
        let token = shutdown.child_token();
        let task_service = service.clone();
        let task_domain_id = domain_id.clone();
        let task_token = token.clone();
        let task_spec = spec.clone();
        let handle = tokio::spawn(async move {
            run_domain_clock(task_service, task_domain_id, task_spec, task_token).await;
        });
        tasks.insert(
            domain_id,
            DomainClockTask {
                spec,
                task: BackgroundTask {
                    cancel: token,
                    handle,
                },
            },
        );
    }
}

async fn run_domain_clock(
    service: SessionServiceImpl,
    domain_id: DomainName,
    spec: DomainClockTaskSpec,
    shutdown: CancellationToken,
) {
    let mut next_tick_id = 1;
    loop {
        tokio::task::consume_budget().await;
        if shutdown.is_cancelled() {
            break;
        }
        let wall_time = current_timestamp();
        let due = match spec
            .clock
            .due_advancement(spec.period, next_tick_id, wall_time)
        {
            Ok(due) => due,
            Err(error) => {
                service.inner.runtime.report_error(format!(
                    "domain clock projection for '{}' failed: {error}",
                    domain_id.as_str()
                ));
                warn!(
                    domain = domain_id.as_str(),
                    error = %error,
                    "domain clock arithmetic failed"
                );
                break;
            }
        };
        if let Some(advancement) = due {
            emit_domain_clock_progress(&service, &domain_id, &spec, &mut next_tick_id, advancement)
                .await;
            continue;
        }
        let next_boundary = match spec.clock.tick_boundary(spec.period, next_tick_id) {
            Ok(boundary) => boundary,
            Err(error) => {
                service.inner.runtime.report_error(format!(
                    "domain clock boundary for '{}' failed: {error}",
                    domain_id.as_str()
                ));
                warn!(
                    domain = domain_id.as_str(),
                    error = %error,
                    "domain clock boundary arithmetic failed"
                );
                break;
            }
        };
        let reached_logical = match spec.clock.logical_time_at(wall_time) {
            Ok(reached) => reached,
            Err(error) => {
                service.inner.runtime.report_error(format!(
                    "domain clock projection for '{}' failed: {error}",
                    domain_id.as_str()
                ));
                warn!(domain = domain_id.as_str(), error = %error, "domain clock projection failed");
                break;
            }
        };
        let wait = match spec
            .clock
            .wall_duration_until(reached_logical, next_boundary.logical_timestamp())
        {
            Ok(wait) => wait,
            Err(error) => {
                service.inner.runtime.report_error(format!(
                    "domain clock rate conversion for '{}' failed: {error}",
                    domain_id.as_str()
                ));
                warn!(domain = domain_id.as_str(), error = %error, "domain clock rate conversion failed");
                break;
            }
        };
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = sleep(wait.clamp(Duration::from_millis(1), Duration::from_millis(250))) => {}
        }
    }
}

async fn emit_domain_clock_progress(
    service: &SessionServiceImpl,
    domain_id: &DomainName,
    spec: &DomainClockTaskSpec,
    next_tick_id: &mut u64,
    advancement: DomainClockAdvancement,
) {
    #[cfg(feature = "testing")]
    let progress_was_paused = service
        .inner
        .runtime
        .pause_domain_clock_progress_if_armed(domain_id, service.inner.consensus.local_node_id())
        .await;
    let wall_clock = current_timestamp();
    let boundary = advancement.boundary();
    let tick = DomainTick {
        tick_id: boundary.tick_id(),
        logical_timestamp: boundary.logical_timestamp(),
        wall_clock,
        period: spec.period,
    };
    *next_tick_id = advancement.next_tick_id();
    let progress = DomainClockProgress {
        generation: spec.generation,
        authority_revision: spec.authority_revision,
        authority: spec.authority.clone(),
        tick,
    };
    for target in &spec.targets {
        tokio::task::consume_budget().await;
        if target == &spec.authority {
            service.handle_domain_clock_progress(
                spec.authority.node_id(),
                DomainClockProgressEnvelope {
                    domain_id: domain_id.clone(),
                    progress: progress.clone(),
                },
            );
            continue;
        }
        if let Err(error) = service
            .dispatch_interconnect_control(
                target.node_id(),
                ControlEnvelope::DomainClockProgress(DomainClockProgressEnvelope {
                    domain_id: domain_id.clone(),
                    progress: progress.clone(),
                }),
            )
            .await
        {
            warn!(
                domain = domain_id.as_str(),
                node = %target,
                error = %error,
                "failed to deliver domain tick"
            );
        }
    }
    #[cfg(feature = "testing")]
    if progress_was_paused {
        service.inner.runtime.mark_domain_clock_progress_delivered(
            domain_id,
            service.inner.consensus.local_node_id(),
        );
    }
}

const DEFAULT_TRACE_FILTER: &str =
    "info,nervix=info,registry=info,openraft::core::heartbeat::worker=error,\
     openraft::replication=error,openraft::engine::handler::replication_handler=error";
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

pub struct TracingGuard {
    tracer_provider: Option<SdkTracerProvider>,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if let Some(tracer_provider) = self.tracer_provider.take() {
            tracer_provider
                .shutdown()
                .reported("flushing the tracer provider on shutdown");
        }
    }
}

pub fn init_tracing(args: &Args) -> Result<TracingGuard, Report<AppError>> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_TRACE_FILTER));
    let fmt_layer = fmt::layer().with_ansi(false);

    if args.otel_enabled {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(args.otel_otlp_endpoint.clone())
            .build()
            .change_context(AppError::InitTracing)?;
        let resource = Resource::builder()
            .with_service_name(args.otel_service_name.clone())
            .build();
        let tracer_provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                args.otel_trace_sample_ratio,
            ))))
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build();
        let tracer = tracer_provider.tracer("nervix");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(otel_layer)
            .try_init()
            .change_context(AppError::InitTracing)?;

        Ok(TracingGuard {
            tracer_provider: Some(tracer_provider),
        })
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init()
            .change_context(AppError::InitTracing)?;

        Ok(TracingGuard {
            tracer_provider: None,
        })
    }
}

#[derive(Clone)]
struct SharedFileWriter(Arc<ParkingMutex<std::fs::File>>);

impl io::Write for SharedFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().flush()
    }
}

fn web_console_advertise_url(
    advertise_addr: Option<cluster::HostPort>,
    listen_addr: SocketAddr,
    https_listen_addr: Option<SocketAddr>,
) -> String {
    if let Some(addr) = advertise_addr {
        return format!("http://{addr}");
    }

    let (scheme, default_addr) = match https_listen_addr {
        Some(addr) => ("https", addr),
        None => ("http", listen_addr),
    };
    let addr = cluster::HostPort::from_socket_addr(default_addr);
    format!("{scheme}://{addr}")
}

/// What a scenario run records beyond the node's own default.
///
/// The interconnect reports why a connection attempt failed at `debug`: a refused dial, a setup
/// deadline, a rejected handshake. A scenario that fails on "peer never became connected" is
/// undiagnosable without those lines — the cluster status only says the peer is unavailable, not
/// what went wrong reaching it — and the failures that need them appear under whole-suite load,
/// where re-running the feature alone does not reproduce them. They are per connection event
/// rather than per message, so keeping them on costs a handful of lines per scenario.
const TEST_TRACE_FILTER: &str = "nervix_interconnect=debug";

pub fn init_tracing_to_file(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let file = Arc::new(ParkingMutex::new(file));
    let make_writer = BoxMakeWriter::new(move || SharedFileWriter(file.clone()));
    fmt()
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new(format!("{DEFAULT_TRACE_FILTER},{TEST_TRACE_FILTER}"))
        }))
        .with_writer(make_writer)
        .try_init()
        .discarded(
            "the first call in this process installed the subscriber this one would replace",
        );
    Ok(())
}

pub async fn run_cli(args: Args) -> Result<(), Report<AppError>> {
    if let Some(Command::Completions { shell }) = args.subcommand.clone() {
        print_completions(shell);
        return Ok(());
    }

    let shutdown = CancellationToken::new();
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_shutdown.cancel();
        }
    });

    let mut application = Application::try_from(args)?;
    application.shutdown = shutdown;
    application.run().await
}

impl Application {
    pub async fn run(self) -> Result<(), Report<AppError>> {
        let addr = self.addr;
        let grpc_mode = self.grpc_mode;
        let grpc_listen_addr = match grpc_mode {
            InternalTransportMode::Http => addr,
            InternalTransportMode::Https => self
                .grpc_https_listen_addr
                .ok_or_else(|| Report::new(AppError::MissingGrpcHttpsListenAddress))?,
        };
        let http_listen_addr = self.http_listen_addr;
        let https_listen_addr = self.https_listen_addr;
        let observability_listen_addr = self.observability_listen_addr;
        let web_console_listen_addr = self.web_console_listen_addr;
        let web_console_https_listen_addr = self.web_console_https_listen_addr;
        let web_console_advertise_url = web_console_advertise_url(
            self.web_console_advertise_addr,
            web_console_listen_addr,
            web_console_https_listen_addr,
        );
        let graceful_shutdown_drain = self.graceful_shutdown_drain;
        let drain_timeout = self.drain_timeout;
        let cluster_id = self.cluster_id.clone();
        let node_id = self.node_id.clone();
        let grpc_advertise_addr = match grpc_mode {
            InternalTransportMode::Http => self.grpc_advertise_addr,
            InternalTransportMode::Https => self
                .grpc_https_advertise_addr
                .ok_or_else(|| Report::new(AppError::MissingGrpcHttpsAdvertiseAddress))?,
        };
        let grpc_advertise_url = grpc_base_url(grpc_mode, &grpc_advertise_addr);
        let interconnect_listen_addr = self.interconnect_listen_addr;
        let interconnect_advertise_addr = self.interconnect_advertise_addr.clone();
        let interconnect_tls_paths = InterconnectTlsPaths {
            ca: self.interconnect_tls_ca.clone(),
            certificate: self.interconnect_tls_cert.clone(),
            private_key: self.interconnect_tls_key.clone(),
        };
        let allow_bootstrap = self.allow_bootstrap;
        let cluster_bootstrap_host = self.cluster_bootstrap_host.clone();
        let default_user = self.default_user.clone();
        let init_default_user_password = self.init_default_user_password.clone();
        let configured_basic_auth =
            init_default_user_password
                .as_ref()
                .map(|password| BasicAuthCredentials {
                    username: default_user.clone(),
                    password: password.clone(),
                });
        let node_unavailability_timeout = self.node_unavailability_timeout;
        let raft_heartbeat_interval = self.raft_heartbeat_interval;
        let raft_election_timeout_min = self.raft_election_timeout_min;
        let raft_election_timeout_max = self.raft_election_timeout_max;
        let transaction_idle_timeout = self.transaction_idle_timeout;
        let transaction_tombstone_retention = self.transaction_tombstone_retention;
        let transaction_max_statements = self.transaction_max_statements;
        let transaction_max_source_bytes = self.transaction_max_source_bytes;
        let transaction_max_open = self.transaction_max_open;
        let replica_count = self.replica_count;
        let state_snapshot_interval = self.state_snapshot_interval;
        let memory_pressure_controller = self
            .memory_pressure
            .map(MemoryPressureController::new)
            .transpose()
            .map_err(|error| {
                error!(?error, "failed to initialize memory pressure monitor");
                Report::new(AppError::InitMemoryPressureMonitor).attach_printable(error)
            })?;
        let db_path = self.db_path.clone();
        let temp_dir = self.temp_dir.clone();
        let shutdown = self.shutdown.clone();
        let fault_injection = self.fault_injection.clone();
        let grpc_tls_server_config = if grpc_mode.is_tls() {
            Some(load_grpc_tls_server_config().await.map_err(|error| {
                error!(?error, "failed to build grpc tls server config");
                error
            })?)
        } else {
            None
        };
        let web_console_tls_server_config =
            match (&self.web_console_tls_cert, &self.web_console_tls_key) {
                (Some(cert_path), Some(key_path)) => {
                    let Some(_) = web_console_https_listen_addr else {
                        return Err(Report::new(AppError::MissingWebConsoleHttpsListenAddress));
                    };
                    Some(
                        load_web_console_tls_server_config(cert_path, key_path).map_err(
                            |error| {
                                error!(?error, "failed to build web console tls server config");
                                error
                            },
                        )?,
                    )
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
            raft_heartbeat_interval = ?raft_heartbeat_interval,
            raft_election_timeout_min = ?raft_election_timeout_min,
            raft_election_timeout_max = ?raft_election_timeout_max,
            replica_count,
            state_snapshot_interval = ?state_snapshot_interval,
            cluster_id,
            %node_id,
            bootstrap = cluster_bootstrap_host.as_deref().unwrap_or(""),
            db_path = db_path.display().to_string(),
            temp_dir = temp_dir.display().to_string(),
            "starting nervix server"
        );

        let db = Database::builder(&db_path)
            .open()
            .map_err(|err| {
                error!(db_path = db_path.display().to_string(), error = %err, "failed to open shared fjall database");
                err
            })
            .change_context(AppError::OpenRegistry)?;
        let registry = Arc::new(
            match Registry::from_database(db.clone(), Some(db_path.as_path())) {
                Ok(registry) => registry,
                Err(err) => {
                    error!(db_path = db_path.display().to_string(), error = %err, "failed to open registry");
                    return Err(err.change_context(AppError::OpenRegistry));
                }
            },
        );
        let runtime = Runtime::with_persistence_and_temp_dir(
            Some(db.clone()),
            state_snapshot_interval,
            fault_injection.clone(),
            temp_dir.clone(),
        )
        .map_err(|error| {
            error!(error = %error, "failed to initialize runtime persistence");
            Report::new(AppError::OpenRuntimeState)
        })?;
        let resource_store = Arc::new(
            ResourceStore::open(db_path.join("resources"), runtime.executor().clone()).map_err(|err| {
                error!(db_path = db_path.display().to_string(), error = %err, "failed to open resource store");
                Report::new(AppError::OpenResourceStore)
            })?,
        );
        let mut startup = ApplicationStartup {
            db,
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
        let startup_runtime_changes = match startup.registry.startup_runtime_changes() {
            Ok(changes) => changes,
            Err(error) => {
                let error = Report::new(AppError::ApplyStartupRuntime(error.to_string()));
                startup.terminate().await;
                return Err(error);
            }
        };
        for changes in startup_runtime_changes {
            if let Err(error) = startup.runtime.apply_changes(changes).await {
                error!(error = %error, "failed to apply startup runtime changes");
                let error = Report::new(AppError::ApplyStartupRuntime(error.to_string()));
                startup.terminate().await;
                return Err(error);
            }
        }
        let interconnect_result = Transport::bind(
            interconnect_listen_addr,
            interconnect_advertise_addr.host(),
            cluster_id.clone(),
            node_id.clone(),
            interconnect_tls,
            Default::default(),
            startup.runtime.executor().clone(),
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

        let consensus_result = Consensus::from_database(
            startup.db.clone(),
            ConsensusSettings {
                cluster_name: cluster_id.clone(),
                node_id: node_id.clone(),
                interconnect: interconnect.clone(),
                node_unavailability_timeout,
                raft_heartbeat_interval,
                raft_election_timeout_min,
                raft_election_timeout_max,
            },
        )
        .await
        .change_context(AppError::StartConsensus);
        let consensus = match consensus_result {
            Ok(consensus) => consensus,
            Err(error) => {
                startup.terminate().await;
                return Err(error);
            }
        };
        startup.consensus = Some(consensus);
        let consensus = startup
            .consensus
            .as_ref()
            .verified("startup assigns this handle before it reaches this point");
        if let Err(error) = startup
            .registry
            .synchronize_cluster_schedule(&consensus.observer().current_schedule().await)
        {
            let error = Report::new(AppError::SynchronizeRegistry(error.to_string()));
            startup.terminate().await;
            return Err(error);
        }
        startup.runtime.attach_resources(
            startup.resource_store.clone(),
            consensus.observer().current_resources().await,
        );

        let cluster_result = cluster::start_cluster(cluster::ClusterSettings {
            cluster_id,
            node_id: node_id.clone(),
            grpc_listen_addr,
            grpc_advertise_addr: grpc_advertise_url.clone(),
            web_console_advertise_addr: web_console_advertise_url.clone(),
            interconnect_advertise_addr,
            bootstrap_host: cluster_bootstrap_host.clone(),
            interconnect: interconnect.clone(),
            node_unavailability_timeout,
        })
        .await
        .change_context(AppError::StartCluster);
        let cluster = match cluster_result {
            Ok(cluster) => Arc::new(cluster),
            Err(error) => {
                startup.terminate().await;
                return Err(error);
            }
        };
        let ApplicationStartup {
            db,
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
        runtime.attach_remote_dispatcher(node_id.clone(), cluster.clone(), interconnect.clone());
        #[cfg(feature = "testing")]
        fault_injection.register_bulk_executor(node_id.clone(), runtime.executor().clone());
        #[cfg(feature = "testing")]
        let scheduler_mode = runtime.scheduler_mode();

        let cluster_for_reconcile = cluster.clone();
        let consensus_for_reconcile = consensus.proposer();
        let administrator_for_reconcile = consensus.administrator();
        let registry_for_reconcile = registry.clone();
        let runtime_for_reconcile = runtime.clone();
        let interconnect_for_reconcile = interconnect.clone();
        let local_node_for_reconcile = node_id.clone();
        let reconcile_shutdown = shutdown.clone();
        let mut background_tasks = Vec::new();
        let interconnect_tls_transport = interconnect.clone();
        let interconnect_tls_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            reload_interconnect_tls(
                interconnect_tls_transport,
                interconnect_tls_paths,
                interconnect_tls_fingerprint,
                interconnect_tls_shutdown,
            )
            .await;
        }));
        if let Some(controller) = memory_pressure_controller {
            let memory_runtime = runtime.clone();
            let memory_shutdown = shutdown.clone();
            background_tasks.push(tokio::spawn(async move {
                controller.run(memory_runtime, memory_shutdown).await;
            }));
        }
        #[cfg(feature = "testing")]
        {
            let mut leadership_transfer_rx = runtime.subscribe_leadership_transfers();
            let consensus_for_leadership_transfer = consensus.administrator();
            let leadership_transfer_shutdown = shutdown.clone();
            let leadership_transfer_local_node_id = node_id.clone();
            background_tasks.push(tokio::spawn(async move {
                loop {
                    tokio::task::consume_budget().await;
                    tokio::select! {
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
        background_tasks.push(tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            let mut initialized = false;
            let mut default_user_resolved = false;
            let mut missing_init_default_user_password_warned = false;
            loop {
                tokio::task::consume_budget().await;
                if reconcile_shutdown.is_cancelled() {
                    break;
                }
                let gossip = cluster_for_reconcile.gossip_state().await;
                if allow_bootstrap && !initialized {
                    match administrator_for_reconcile.maybe_initialize().await {
                        Ok(did_initialize) => {
                            initialized = did_initialize;
                        }
                        Err(err) => {
                            warn!(error = %err, "raft bootstrap attempt failed");
                        }
                    }
                }
                if let Err(err) = administrator_for_reconcile.reconcile_nodes(gossip).await {
                    warn!(error = %err, "raft membership reconciliation failed");
                }
                if consensus_for_reconcile.current_leader().await.as_ref()
                    == Some(consensus_for_reconcile.local_node_id())
                {
                    let committing_domains = consensus_for_reconcile
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
                            && !committing_domains.contains(&domain)
                            && !runtime_for_reconcile.domain_alter_is_active(&domain)
                        {
                            match consensus_for_reconcile.resume_domain(domain.clone()).await {
                                Ok(()) => {
                                    info!(
                                        domain = domain.as_str(),
                                        "resumed orphaned ALTER pause after leadership \
                                         acquisition"
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
                                    match user_credentials(default_user_id.clone(), password).await {
                                        Ok(user) => {
                                            let user_name = user.name.clone();
                                            if let Err(error) =
                                                consensus_for_reconcile.create_user(user).await
                                            {
                                                warn!(
                                                    error = %error,
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
                                                error = %error,
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
                            Err(_) => {
                                if !missing_init_default_user_password_warned {
                                    warn!(
                                        user = default_user.as_str(),
                                        "configured default user name is invalid"
                                    );
                                    missing_init_default_user_password_warned = true;
                                }
                            }
                        }
                    }
                    let scheduling_gossip = cluster_for_reconcile.gossip_state().await;
                    let live_node_ids = scheduling_gossip
                        .live_nodes
                        .iter()
                        .filter(|node| !scheduling_gossip.dead_node_ids.contains(&node.node_id))
                        .map(|node| node.node_id.clone())
                        .collect::<Vec<_>>();
                    let live_node_incarnations = scheduling_gossip
                        .live_nodes
                        .iter()
                        .filter(|node| !scheduling_gossip.dead_node_ids.contains(&node.node_id))
                        .map(|node| (node.node_id.clone(), node.incarnation))
                        .collect::<BTreeMap<_, _>>();
                    let live_voters = consensus_for_reconcile
                        .live_voter_ids(live_node_ids.clone())
                        .await;
                    let schedulable_node_ids = consensus_for_reconcile
                        .schedulable_live_voter_ids(live_node_ids)
                        .await;
                    let current_schedule = consensus_for_reconcile.current_schedule().await;
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
                    for domain_schedule in current_schedule.domains.values() {
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
                        if let Err(err) = consensus_for_reconcile
                            .replace_domain_schedule(
                                domain_schedule.domain.clone(),
                                Some(domain_schedule.clone()),
                                Some(failover_schedule),
                            )
                            .await
                        {
                            warn!(error = %err, "failed to republish domain schedule after node failover");
                        }
                    }
                    for (domain, graph) in active_graphs {
                        if committing_domains.contains(&domain)
                            || runtime_for_reconcile.domain_alter_is_active(&domain)
                        {
                            continue;
                        }
                        let Some(domain_state) =
                            consensus_for_reconcile.current_domain(&domain).await
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
                        if let Err(err) = consensus_for_reconcile
                            .replace_domain_schedule(
                                domain,
                                current_domain.cloned(),
                                Some(schedule),
                            )
                            .await
                        {
                            warn!(error = %err, "failed to republish domain schedule after membership or schedulability change");
                        }
                    }
                }
                tokio::select! {
                    _ = reconcile_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        let interconnect_for_membership = interconnect.clone();
        let cluster_for_interconnect = cluster.clone();
        let local_node_id = node_id;
        let mut awaiting_initial_bootstrap_peer = cluster_bootstrap_host.is_some();
        let interconnect_membership_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            loop {
                tokio::task::consume_budget().await;
                if interconnect_membership_shutdown.is_cancelled() {
                    break;
                }
                let gossip = cluster_for_interconnect.gossip_state().await;
                let live_node_ids = gossip
                    .live_nodes
                    .iter()
                    .map(|node| node.node_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                struct PeerConnectionPlan {
                    node_id: ClusterNodeName,
                    target_label: String,
                }

                let mut plans = Vec::new();
                let mut outbound_targets = BTreeMap::new();
                for node in gossip.live_nodes {
                    if node.node_id == local_node_id {
                        continue;
                    }
                    let Ok(target_addr) = node
                        .interconnect_advertise_addr
                        .parse::<cluster::HostPort>()
                    else {
                        cluster_for_interconnect.record_interconnect_failure(&node.node_id, None);
                        continue;
                    };
                    let target_label = target_addr.to_string();
                    let targets = match target_addr.resolve_all().await {
                        Ok(addrs) => addrs
                            .into_iter()
                            .map(|addr| PeerTarget::new(addr, target_addr.host()))
                            .collect::<BTreeSet<_>>(),
                        Err(_err) => {
                            cluster_for_interconnect.record_interconnect_failure(
                                &node.node_id,
                                Some(target_label.clone()),
                            );
                            continue;
                        }
                    };
                    outbound_targets.insert(node.node_id.clone(), targets.clone());
                    plans.push(PeerConnectionPlan {
                        node_id: node.node_id,
                        target_label,
                    });
                }
                if !outbound_targets.is_empty() {
                    awaiting_initial_bootstrap_peer = false;
                }
                if !awaiting_initial_bootstrap_peer {
                    interconnect_for_membership.replace_live_nodes(&live_node_ids);
                    cluster_for_interconnect.retain_interconnect_live_set(&live_node_ids);
                    interconnect_for_membership.replace_outbound_targets(&outbound_targets);
                }

                for plan in plans {
                    tokio::task::consume_budget().await;
                    if interconnect_for_membership.is_connected_to(&plan.node_id) {
                        cluster_for_interconnect
                            .record_interconnect_connected(&plan.node_id, plan.target_label);
                    } else {
                        cluster_for_interconnect
                            .record_interconnect_failure(&plan.node_id, Some(plan.target_label));
                    }
                }
                tokio::select! {
                    _ = interconnect_membership_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        let runtime_for_schedule = runtime.clone();
        let registry_for_schedule = registry.clone();
        let mut schedule_rx = consensus.observer().subscribe_schedule();
        let consensus_for_schedule = consensus.observer();
        let cluster_for_schedule = cluster.clone();
        let schedule_local_node_id = consensus.observer().local_node_id().clone();
        let schedule_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            let initial_state = consensus_for_schedule.current_runtime_state().await;
            if consensus_for_schedule.current_leader().await.as_ref()
                != Some(&schedule_local_node_id)
                && let Err(error) =
                    registry_for_schedule.synchronize_cluster_schedule(&initial_state.schedule)
            {
                warn!(
                    error = %error,
                    "failed to synchronize registry from initial cluster schedule"
                );
            }
            if let Err(error) = apply_cluster_runtime_state(
                &runtime_for_schedule,
                &cluster_for_schedule,
                &schedule_local_node_id,
                initial_state,
            )
            .await
            {
                warn!(error = %error, "failed to apply initial cluster schedule");
            }
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    _ = schedule_shutdown.cancelled() => break,
                    changed = schedule_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let state = consensus_for_schedule.current_runtime_state().await;
                        if consensus_for_schedule.current_leader().await.as_ref()
                            != Some(&schedule_local_node_id)
                            && let Err(error) =
                                registry_for_schedule.synchronize_cluster_schedule(&state.schedule)
                        {
                            warn!(
                                error = %error,
                                "failed to synchronize registry from updated cluster schedule"
                            );
                        }
                        if let Err(error) = apply_cluster_runtime_state(
                            &runtime_for_schedule,
                            &cluster_for_schedule,
                            &schedule_local_node_id,
                            state,
                        )
                        .await
                        {
                            warn!(error = %error, "failed to apply updated cluster schedule");
                        }
                    }
                }
            }
        }));
        let runtime_for_resources = runtime.clone();
        let mut resources_rx = consensus.observer().subscribe_resources();
        let resources_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            runtime_for_resources.update_resource_versions(resources_rx.borrow().clone());
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    _ = resources_shutdown.cancelled() => break,
                    changed = resources_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        runtime_for_resources
                            .update_resource_versions(resources_rx.borrow().clone());
                    }
                }
            }
        }));
        let events = SessionEvents::new(SESSION_EVENT_CAPACITY);
        let service = SessionServiceImpl {
            inner: Arc::new(SessionServiceInner {
                cluster: cluster.clone(),
                consensus: consensus.proposer(),
                consensus_administrator: consensus.administrator(),
                registry,
                resource_store,
                http_tls_server_config: Arc::new(RwLock::new(None)),
                runtime: runtime.clone(),
                replica_count,
                shutdown: shutdown.clone(),
                events: events.clone(),
                subscription_interest_counts: DashMap::with_hasher(RandomState::new()),
                interconnect: interconnect.clone(),
                next_entity_gate_operation_id: AtomicU64::new(1),
                service_tasks: TaskTracker::new(),
                configured_basic_auth,
                auth_rate_limiter: SessionServiceImpl::new_auth_rate_limiter(),
                failed_auth_rate_limit_keys: DashMap::with_hasher(RandomState::new()),
                transaction_idle_timeout,
                transaction_tombstone_retention,
                transaction_max_statements,
                transaction_max_source_bytes,
                transaction_max_open,
                transaction_bindings: DashMap::with_hasher(RandomState::new()),
                transaction_executions: Arc::new(DashMap::with_hasher(RandomState::new())),
                transaction_commit_execution: AsyncMutex::new(()),
            }),
        };
        let resource_archive_service = service.clone();
        interconnect
            .register_handler::<FetchResourceArchiveChunk, _, _>(move |_context, request| {
                let service = resource_archive_service.clone();
                async move {
                    service
                        .inner
                        .resource_store
                        .read_archive_chunk(&request.id, request.offset)
                        .await
                        .map(|chunk| InterconnectResourceArchiveChunk {
                            bytes: chunk.bytes,
                            eof: chunk.eof,
                        })
                        .map_err(ResourceInterconnectError::archive_read)
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let resource_replica_service = service.clone();
        interconnect
            .register_handler::<PublishResourceReplica, _, _>(move |context, request| {
                let service = resource_replica_service.clone();
                async move {
                    if &request.replica.key.node_id != context.peer_node_id() {
                        return Err(ResourceInterconnectError::ReplicaOrigin {
                            authenticated: context.peer_node_id().clone(),
                            declared: request.replica.key.node_id.clone(),
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
        let describe_ingestor_service = service.clone();
        interconnect
            .register_handler::<RemoteDescribeIngestorRequest, _, _>(move |_context, request| {
                let service = describe_ingestor_service.clone();
                async move {
                    service
                        .prepare_owner_control_request(
                            &request.domain,
                            ModelKind::Ingestor,
                            &request.name,
                        )
                        .await?;
                    let summary = service
                        .inner
                        .runtime
                        .describe_local_ingestor(&request.domain, &request.name)?;
                    let metrics = service.inner.runtime.describe_metrics_for(
                        &request.domain,
                        "INGESTOR",
                        &request.name,
                    );
                    Ok(runtime_ingestor_describe_to_envelope(summary, metrics))
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let state_sync_service = service.clone();
        interconnect
            .register_handler::<RemoteStateSyncRequest, _, _>(move |_context, request| {
                let service = state_sync_service.clone();
                async move {
                    let result =
                        match crate::runtime::RuntimeStatePlacement::from_remote(request.placement)
                        {
                            Ok(placement) => {
                                if !service
                                    .inner
                                    .runtime
                                    .runtime_state_placement_is_assigned_locally(&placement)
                                {
                                    return RemoteStateSyncResponse {
                                        result: Err(format!(
                                            "this node is not currently assigned {:?} state for \
                                             {} '{}'",
                                            placement.state,
                                            placement.kind.as_str(),
                                            placement.identifier.as_str()
                                        )),
                                    };
                                }
                                service
                                    .inner
                                    .runtime
                                    .handle_state_sync_request(&placement, request.after_lsm)
                                    .await
                            }
                            Err(error) => Err(error),
                        };
                    RemoteStateSyncResponse {
                        result: result.map(|snapshot| {
                            snapshot.map(|snapshot| nervix_interconnect::StateSnapshotEnvelope {
                                lsm: snapshot.lsm,
                                schema_fingerprint: snapshot.schema_fingerprint,
                                payload: snapshot.payload,
                            })
                        }),
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

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
                    let result = service
                        .apply_current_cluster_state()
                        .await
                        .map_err(|error| error.to_string())
                        .map(|()| {
                            service
                                .inner
                                .runtime
                                .force_flush_domain_if_idle(&request.domain);
                            service.local_domain_drain_status(&request.domain)
                        });
                    RemoteDomainDrainStatusResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let entity_gate_service = service.clone();
        interconnect
            .register_handler::<RemoteEntityGateRequest, _, _>(move |_context, request| {
                let service = entity_gate_service.clone();
                async move {
                    let deadline = tokio::time::Instant::now()
                        .checked_add(Duration::from_millis(request.deadline_millis));
                    let result = match deadline {
                        Some(deadline) => {
                            service
                                .inner
                                .runtime
                                .engage_entity_gate_operation(
                                    request.operation_id,
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
                        }
                        None => Err("entity gate deadline exceeds the monotonic clock".to_string()),
                    };
                    RemoteEntityGateResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let entity_drain_service = service.clone();
        interconnect
            .register_handler::<RemoteEntityDrainStatusRequest, _, _>(move |_context, request| {
                let service = entity_drain_service.clone();
                async move {
                    let status = service.local_entity_drain_status(
                        &request.domain,
                        &request.relays,
                        &request.affected_entities,
                        request.purpose,
                    );
                    if status.buffered_relay_batches != 0
                        || status.node_work_items != 0
                        || status.outstanding_acks != 0
                    {
                        service
                            .inner
                            .runtime
                            .force_flush_domain_if_idle(&request.domain);
                    }
                    RemoteEntityDrainStatusResponse { result: Ok(status) }
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
                            .release_entity_gate_operation(request.operation_id, &request.domain)
                            .await,
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
                    let result = match service.handle_lookup_request(request).await {
                        Ok(Some(record)) => record
                            .encode_arrow_ipc(service.inner.runtime.executor())
                            .await
                            .map(|body| Some(body.to_vec()))
                            .map_err(|error| error.to_string()),
                        Ok(None) => Ok(None),
                        Err(error) => Err(error),
                    };
                    RemoteLookupResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let subscription_visibility_service = service.clone();
        interconnect
            .register_handler::<RemoteSubscriptionInterestVisibilityRequest, _, _>(
                move |context, request| {
                    let service = subscription_visibility_service.clone();
                    async move {
                        let result = if &request.subscriber_node_id != context.peer_node_id() {
                            Err(format!(
                                "authenticated node '{}' cannot query interest for '{}'",
                                context.peer_node_id(),
                                request.subscriber_node_id,
                            ))
                        } else {
                            Ok(service
                                .inner
                                .cluster
                                .nodes_with_subscription_interest(
                                    request.domain.as_str(),
                                    request.relay.as_str(),
                                )
                                .await
                                .contains(&request.subscriber_node_id))
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
                                .map_err(OwnershipHandoffError::participant)?;
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
                                    &request.domain,
                                    &request.entity,
                                    request.base_schedule_fingerprint,
                                )
                                .await
                        }
                        .await;
                        match result {
                            Ok(checkpoints) => Ok(checkpoints),
                            Err(error) => Err(OwnershipHandoffFailure::rejected(error.to_string())),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let prepare_handoff_service = service.clone();
        interconnect
            .register_handler::<RemotePrepareOwnershipHandoffStateRequest, _, _>(
                move |_context, request| {
                    let service = prepare_handoff_service.clone();
                    async move {
                        let result: OwnershipHandoffResult<_> = async {
                            if request.destination != *service.inner.consensus.local_node_id() {
                                return Err(OwnershipHandoffError::participant(format!(
                                    "ownership handoff for {} '{}' targets node '{}' but reached \
                                     '{}'",
                                    request.entity.kind.as_str(),
                                    request.entity.identifier.as_str(),
                                    request.destination,
                                    service.inner.consensus.local_node_id()
                                )));
                            }
                            let current_incarnations = service.live_node_incarnations().await;
                            SessionServiceImpl::verify_ownership_handoff_node_incarnation(
                                &current_incarnations,
                                &request.source,
                                request.source_incarnation,
                                "source",
                            )?;
                            SessionServiceImpl::verify_ownership_handoff_node_incarnation(
                                &current_incarnations,
                                &request.destination,
                                request.destination_incarnation,
                                "destination",
                            )?;
                            service
                                .prepare_control_request_domain(&request.domain)
                                .await
                                .map_err(OwnershipHandoffError::schedule)?;
                            let scheduled = service
                                .scheduled_model_node(
                                    &request.domain,
                                    request.entity.kind,
                                    request.entity.identifier.clone(),
                                )
                                .await
                                .ok_or_else(|| {
                                    OwnershipHandoffError::schedule(format!(
                                        "{} '{}' is absent from the committed schedule",
                                        request.entity.kind.as_str(),
                                        request.entity.identifier.as_str()
                                    ))
                                })?;
                            if scheduled.execution_node() != Some(&request.source) {
                                return Err(OwnershipHandoffError::participant(format!(
                                    "{} '{}' is no longer owned by source node '{}'",
                                    request.entity.kind.as_str(),
                                    request.entity.identifier.as_str(),
                                    request.source
                                )));
                            }
                            service
                                .inner
                                .runtime
                                .prepare_ownership_handoff_state(request)
                                .await
                        }
                        .await;
                        match result {
                            Ok(()) => Ok(()),
                            Err(error) => Err(OwnershipHandoffFailure::rejected(error.to_string())),
                        }
                    }
                },
            )
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
                            let deadline =
                                tokio::time::Instant::now() + FORCED_OWNERSHIP_RECOVERY_BUDGET;
                            service
                                .inner
                                .runtime
                                .prepare_forced_ownership_recovery(request, deadline)
                                .await
                        }
                        .await;
                        match result {
                            Ok(preparation) => Ok(preparation),
                            Err(error) => Err(OwnershipHandoffFailure::rejected(error.to_string())),
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
                            Err(error) => Err(OwnershipHandoffFailure::rejected(error.to_string())),
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
                        let result: OwnershipHandoffResult<_> = async {
                            if request.destination != *service.inner.consensus.local_node_id() {
                                return Err(OwnershipHandoffError::participant(format!(
                                    "ownership handoff for {} '{}' targets node '{}' but reached \
                                     '{}'",
                                    request.entity.kind.as_str(),
                                    request.entity.identifier.as_str(),
                                    request.destination,
                                    service.inner.consensus.local_node_id()
                                )));
                            }
                            let current_incarnations = service.live_node_incarnations().await;
                            SessionServiceImpl::verify_ownership_handoff_node_incarnation(
                                &current_incarnations,
                                &request.source,
                                request.source_incarnation,
                                "source",
                            )?;
                            SessionServiceImpl::verify_ownership_handoff_node_incarnation(
                                &current_incarnations,
                                &request.destination,
                                request.destination_incarnation,
                                "destination",
                            )?;
                            let deadline = tokio::time::Instant::now()
                                + service.inner.runtime.entity_gate_deadline();
                            let target_schedule = loop {
                                tokio::task::consume_budget().await;
                                let schedule = service.inner.consensus.current_schedule().await;
                                let current =
                                    schedule.domain(&request.domain).ok_or_else(|| {
                                        OwnershipHandoffError::schedule(format!(
                                            "domain '{}' has no committed schedule while \
                                             activating ownership handoff",
                                            request.domain.as_str()
                                        ))
                                    })?;
                                let fingerprint =
                                    Runtime::ownership_handoff_schedule_fingerprint(current)?;
                                if fingerprint == request.target_schedule_fingerprint {
                                    let node =
                                        current.nodes.get(&request.entity).ok_or_else(|| {
                                            OwnershipHandoffError::schedule(format!(
                                                "{} '{}' is absent from the ownership handoff \
                                                 target schedule",
                                                request.entity.kind.as_str(),
                                                request.entity.identifier.as_str()
                                            ))
                                        })?;
                                    if !node.is_primary_on(&request.destination) {
                                        return Err(OwnershipHandoffError::participant(format!(
                                            "{} '{}' is not owned by destination node '{}' in the \
                                             committed target schedule",
                                            request.entity.kind.as_str(),
                                            request.entity.identifier.as_str(),
                                            request.destination
                                        )));
                                    }
                                    break current.clone();
                                }
                                if fingerprint != request.base_schedule_fingerprint {
                                    return Err(OwnershipHandoffError::schedule(format!(
                                        "domain '{}' advanced to a different schedule before \
                                         ownership handoff activation",
                                        request.domain.as_str()
                                    )));
                                }
                                if tokio::time::Instant::now() >= deadline {
                                    return Err(OwnershipHandoffError::deadline(format!(
                                        "timed out waiting for the committed ownership handoff \
                                         schedule in domain '{}'",
                                        request.domain.as_str()
                                    )));
                                }
                                tokio::time::sleep(Duration::from_millis(25)).await;
                            };
                            let activation_needed = service
                                .inner
                                .runtime
                                .authorize_persisted_ownership_handoff_activation(&request)?;
                            service
                                .apply_current_cluster_state()
                                .await
                                .map_err(|error| OwnershipHandoffError::state(error.to_string()))?;
                            if activation_needed
                                && service
                                    .inner
                                    .runtime
                                    .verify_ownership_handoff_activation(&request)
                                    .is_err()
                            {
                                service
                                    .inner
                                    .runtime
                                    .rebuild_ownership_handoff_target(
                                        service.inner.consensus.local_node_id(),
                                        &request.domain,
                                        target_schedule,
                                    )
                                    .await
                                    .map_err(|error| {
                                        OwnershipHandoffError::state(error.to_string())
                                    })?;
                            }
                            service
                                .inner
                                .runtime
                                .verify_ownership_handoff_activation(&request)
                        }
                        .await;
                        match result {
                            Ok(()) => Ok(()),
                            Err(error) => Err(OwnershipHandoffFailure::rejected(error.to_string())),
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
                                &request.operation_id,
                                &request.domain,
                                &request.entity,
                            );
                        match result {
                            Ok(()) => Ok(()),
                            Err(error) => Err(OwnershipHandoffFailure::rejected(error.to_string())),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let transaction_service = service.clone();
        let transaction_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            let mut observed_leadership = None;
            loop {
                tokio::task::consume_budget().await;
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
                            tokio::select! {
                                _ = transaction_shutdown.cancelled() => break,
                                _ = sleep(Duration::from_millis(250)) => {}
                            }
                            continue;
                        }
                    }
                }
                if is_leader {
                    transaction_service.reconcile_transactions_once().await;
                    transaction_service
                        .reconcile_domain_clock_authorities()
                        .await;
                }
                tokio::select! {
                    _ = transaction_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_millis(250)) => {}
                }
            }
        }));

        let runtime_event_service = service.clone();
        let runtime_event_shutdown = shutdown.clone();
        let mut runtime_event_rx = runtime.subscribe_events();
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    _ = runtime_event_shutdown.cancelled() => break,
                    received = runtime_event_rx.recv() => match received {
                        Ok(RuntimeEvent::Error(message)) => {
                            let mut node_ids =
                                runtime_event_service.inner.cluster.live_node_ids().await;
                            node_ids.sort();
                            node_ids.dedup();
                            for node_id in node_ids {
                                tokio::task::consume_budget().await;
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
                                        error,
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
        background_tasks.push(tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            loop {
                tokio::task::consume_budget().await;
                if resource_shutdown.is_cancelled() {
                    break;
                }
                resource_service.reconcile_resources_once().await;
                tokio::select! {
                    _ = resource_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        if let Err(error) = service.refresh_http_tls_server_config().await {
            service.broadcast_error(format!("failed to refresh HTTP TLS config: {error}"));
        }

        let domain_service = service.clone();
        let domain_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            let mut domains_rx = domain_service.inner.consensus.subscribe_domains();
            let mut tasks: HashMap<DomainName, DomainClockTask> = HashMap::new();
            let mut retirements = DomainClockRetirements::default();
            if let Err(error) = domain_service.apply_current_cluster_state().await {
                warn!(error = %error, "failed to apply cluster schedule after initial domain sync");
            }

            loop {
                tokio::task::consume_budget().await;
                reconcile_domain_clock_tasks(
                    &domain_service,
                    &domain_shutdown,
                    &mut tasks,
                    &mut retirements,
                )
                .await;
                tokio::select! {
                    _ = domain_shutdown.cancelled() => break,
                    changed = domains_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        if let Err(error) = domain_service.apply_current_cluster_state().await {
                            warn!(error = %error, "failed to apply cluster schedule after domain sync");
                        }
                    }
                    _ = sleep(Duration::from_millis(100)) => {}
                }
            }

            for (_, task) in tasks {
                task.task.stop().await;
            }
            retirements.stop_all().await;
        }));

        let kafka_schedule_service = service.clone();
        let kafka_schedule_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            let mut schedule_rx = kafka_schedule_service.inner.consensus.subscribe_schedule();
            let mut tasks: HashMap<KafkaPartitionWatcherKey, KafkaPartitionWatcherTask> =
                HashMap::new();

            loop {
                tokio::task::consume_budget().await;
                let schedule = schedule_rx.borrow().clone();
                kafka_schedule_service
                    .reconcile_kafka_partition_watchers(&schedule, &mut tasks)
                    .await;
                tokio::select! {
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

        let (interconnect_relay_payload_lane, mut interconnect_relay_payload_rx) =
            InterconnectRelayPayloadLane::new();
        let relay_payload_shutdown = shutdown.clone();
        let runtime_for_relay_payloads = runtime.clone();
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                let payload = tokio::select! {
                    _ = relay_payload_shutdown.cancelled() => break,
                    payload = interconnect_relay_payload_rx.recv() => {
                        let Some(payload) = payload else {
                            break;
                        };
                        payload
                    }
                };
                let result = tokio::select! {
                    _ = relay_payload_shutdown.cancelled() => break,
                    result = runtime_for_relay_payloads.handle_remote_stream(payload) => result,
                };
                if let Err(error) = result {
                    warn!(error = %error, "failed to process remote relay payload");
                }
            }
        }));

        let interconnect_shutdown = shutdown.clone();
        let runtime_for_interconnect = runtime.clone();
        let service_for_interconnect = service.clone();
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
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
                            .route(message.envelope)
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
                            Envelope::Control(ControlEnvelope::DomainClockProgress(progress)) => {
                                service_for_interconnect.handle_domain_clock_progress(
                                    &message.peer_node_id,
                                    progress,
                                );
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
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
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
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
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

        let grpc_service = service.clone();
        let grpc_shutdown = shutdown.clone();
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
            let grpc_incoming = stream::unfold(grpc_listener, |listener| async {
                let accepted = listener.accept().await.map(|(relay, _)| {
                    relay
                        .set_nodelay(true)
                        .reported("disabling Nagle on an accepted gRPC connection");
                    relay
                });
                Some((accepted, listener))
            });
            builder
                .add_service(SessionServiceServer::new(grpc_service.clone()))
                .serve_with_incoming_shutdown(grpc_incoming, grpc_shutdown.cancelled_owned())
                .await
                .map_err(|e| Report::new(e).change_context(AppError::Serve))
        };
        let http_server = serve_http(
            runtime.clone(),
            service.inner.service_tasks.clone(),
            http_listener,
            shutdown.clone(),
        );
        let https_server = serve_https(
            runtime.clone(),
            service.inner.service_tasks.clone(),
            service.inner.http_tls_server_config.clone(),
            https_listener,
            shutdown.clone(),
        );
        let observability_server = serve_observability_http(
            consensus.observer(),
            runtime.clone(),
            observability_listener,
            shutdown.clone(),
        );
        let web_console_server =
            serve_web_console_http(service.clone(), web_console_listener, shutdown.clone());
        let web_console_https_shutdown = shutdown.clone();
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

        let server_shutdown = self.shutdown.clone();
        let (
            api_result,
            http_result,
            https_result,
            observability_result,
            web_console_result,
            web_console_https_result,
        ) = tokio::join!(
            cancel_shutdown_on_completion(api_server, server_shutdown.clone()),
            cancel_shutdown_on_completion(http_server, server_shutdown.clone()),
            cancel_shutdown_on_completion(https_server, server_shutdown.clone()),
            cancel_shutdown_on_completion(observability_server, server_shutdown.clone()),
            cancel_shutdown_on_completion(web_console_server, server_shutdown.clone()),
            cancel_shutdown_on_completion(web_console_https_server, server_shutdown.clone()),
        );
        let result = api_result
            .and(http_result)
            .and(https_result)
            .and(observability_result)
            .and(web_console_result)
            .and(web_console_https_result);

        if graceful_shutdown_drain {
            match tokio::time::timeout(drain_timeout, service.drain_local_node_before_shutdown())
                .await
            {
                Ok(()) => {}
                Err(_) => {
                    warn!(
                        timeout = ?drain_timeout,
                        "timed out draining local node before graceful shutdown"
                    );
                }
            }
        }

        for task in background_tasks {
            await_background_task_shutdown(task, "application background task").await;
        }
        service.inner.service_tasks.close();
        service.inner.service_tasks.wait().await;
        runtime.shutdown().await;
        consensus.shutdown().await;
        let cluster_shutdown_result = cluster
            .shutdown()
            .await
            .change_context(AppError::ShutdownCluster);
        interconnect.shutdown().await;

        tokio::task::spawn_blocking(move || {
            drop(service);
            drop(runtime);
            drop(consensus);
            drop(cluster);
            drop(interconnect);
            drop(db);
        })
        .await
        .map_err(|error| {
            error!(error = %error, "failed to join database owner shutdown task");
            Report::new(AppError::OpenRegistry)
        })?;

        result?;
        cluster_shutdown_result?;

        Ok(())
    }
}

async fn cancel_shutdown_on_completion<F>(
    server: F,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>>
where
    F: Future<Output = Result<(), Report<AppError>>>,
{
    let result = server.await;
    shutdown.cancel();
    result
}

async fn await_background_task_shutdown(mut task: JoinHandle<()>, task_kind: &'static str) {
    match tokio::time::timeout(BACKGROUND_TASK_SHUTDOWN_GRACE_PERIOD, &mut task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            if error.is_cancelled() {
                warn!(task_kind, "shutdown task was cancelled");
            } else {
                error!(task_kind, error = %error, "shutdown task join failed");
            }
        }
        Err(_) => {
            warn!(
                task_kind,
                grace_period = %humantime::format_duration(BACKGROUND_TASK_SHUTDOWN_GRACE_PERIOD),
                "shutdown task exceeded grace period; aborting"
            );
            task.abort();
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                error!(task_kind, error = %error, "aborted shutdown task join failed");
            }
        }
    }
}

fn print_completions(shell: Shell) {
    let mut command = Args::command();
    let bin_name = command.get_name().to_string();
    generate(shell, &mut command, bin_name, &mut std::io::stdout());
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use nervix_models::{
        AckMode, CreateDomain, CreateResource, CreateSchema, CreateStatement, DomainConfig,
        DomainPace, DomainSchedule, DomainState, DomainStatus, KafkaPartitionSchedule, Model,
        ModelKind, NodeRef, PlacementGroupSchedule, ResourceVersion, ResourceVersionCounter,
        ResourceVersionStatus, ScheduledNode, SubscriptionLiteral,
    };
    use nonzero_ext::nonzero;
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose, SanType,
    };
    use sorted_vec::SortedVec;

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct TestTlsFiles {
        _directory: tempfile::TempDir,
        ca: PathBuf,
        certificate: PathBuf,
        private_key: PathBuf,
    }

    fn test_tls_files(cluster_id: &str, node_id: &ClusterNodeName) -> TestTlsFiles {
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

    fn test_addr(base_port: u16) -> std::net::SocketAddr {
        format!("127.0.0.1:{base_port}")
            .parse()
            .expect("valid socket addr")
    }

    fn test_args(extra: &[&str]) -> Args {
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

    fn try_test_args(extra: &[&str]) -> Result<Args, clap::Error> {
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

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    fn command_transaction_state(result: &CommandResult) -> Option<ApiTransactionState> {
        result
            .transaction
            .as_ref()
            .and_then(|status| ApiTransactionState::try_from(status.state).ok())
    }

    fn string_branch_key(field: &str, value: &str) -> Option<crate::runtime::BranchKey> {
        crate::runtime::BranchKey::from_fields([(
            named(field),
            runtime_schema::RuntimeValue::String(value.to_string()),
        )])
        .expect("test branch key must be non-empty")
        .into()
    }

    #[test]
    fn interconnect_control_lane_never_waits_for_relay_payload_processing() {
        let (lane, mut payloads) = InterconnectRelayPayloadLane::new();
        let routed = RelayPayload {
            kind: nervix_interconnect::RelayPayloadKind::Routed,
            domain: DomainName::parse("default").expect("valid domain"),
            relay: named("incoming"),
            key: None,
            batch_ipc: nervix_execution::Executor::default()
                .try_charge_owned(MemoryClass::Relay, Vec::new())
                .expect("an empty test body always fits the relay class"),
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: None,
        };

        assert!(lane.route(Envelope::RelayPayload(routed)).is_none());
        assert!(payloads.try_recv().is_ok());
        assert!(matches!(
            lane.route(Envelope::Control(ControlEnvelope::Terminate)),
            Some(Envelope::Control(ControlEnvelope::Terminate))
        ));
    }

    #[tokio::test]
    async fn startup_failure_releases_the_shared_database_before_returning() {
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

        tokio::task::spawn_blocking(move || Database::builder(db_path).open())
            .await
            .expect("database open task should join")
            .expect("application startup failure must release the database lock");
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn testing_feature_hashes_passwords_with_lean_argon2_params() {
        let password_hash = hash_password("secret".to_string())
            .await
            .expect("password hash should be created");
        let parsed_hash =
            PasswordHash::new(&password_hash).expect("password hash should parse as PHC");

        assert_eq!(
            parsed_hash
                .params
                .get("m")
                .and_then(|value| value.decimal().ok()),
            Some(TESTING_ARGON2_MEMORY_COST)
        );
        assert_eq!(
            parsed_hash
                .params
                .get("t")
                .and_then(|value| value.decimal().ok()),
            Some(TESTING_ARGON2_TIME_COST)
        );
        assert_eq!(
            parsed_hash
                .params
                .get("p")
                .and_then(|value| value.decimal().ok()),
            Some(TESTING_ARGON2_PARALLELISM)
        );
        assert!(verify_password_hash(password_hash, "secret".to_string()).await);
    }

    #[test]
    fn args_parse_observability_listen_addr() {
        let args = test_args(&["--observability-listen-addr", "127.0.0.1:19090"]);
        let app = Application::try_from(args).expect("args should parse");
        assert_eq!(app.observability_listen_addr, test_addr(19090));
    }

    #[test]
    fn args_parse_temp_dir() {
        let args = test_args(&["--temp-dir", "/tmp/nervix-temp"]);
        let app = Application::try_from(args).expect("args should parse");
        assert_eq!(app.temp_dir, PathBuf::from("/tmp/nervix-temp"));
    }

    #[test]
    fn args_parse_web_console_listen_addr() {
        let args = test_args(&[
            "--web-console-listen-addr",
            "127.0.0.1:17420",
            "--web-console-https-listen-addr",
            "127.0.0.1:17443",
            "--web-console-tls-cert",
            "tls/dev/node.pem",
            "--web-console-tls-key",
            "tls/dev/node-key.pem",
        ]);
        let app = Application::try_from(args).expect("args should parse");
        assert_eq!(app.web_console_listen_addr, test_addr(17420));
        assert_eq!(app.web_console_https_listen_addr, Some(test_addr(17443)));
        assert_eq!(
            app.web_console_tls_cert,
            Some(PathBuf::from("tls/dev/node.pem"))
        );
        assert_eq!(
            app.web_console_tls_key,
            Some(PathBuf::from("tls/dev/node-key.pem"))
        );
    }

    #[test]
    fn web_console_advertise_url_uses_https_listener_when_available() {
        assert_eq!(
            web_console_advertise_url(None, test_addr(17420), Some(test_addr(17443))),
            "https://127.0.0.1:17443"
        );
        assert_eq!(
            web_console_advertise_url(
                Some(cluster::HostPort::from_socket_addr(test_addr(17420))),
                test_addr(17420),
                Some(test_addr(17443))
            ),
            "http://127.0.0.1:17420"
        );
        assert_eq!(
            web_console_advertise_url(None, test_addr(17420), None),
            "http://127.0.0.1:17420"
        );
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
            }),
        }
    }

    async fn test_interconnect(cluster_id: &str, node_id: &ClusterNodeName) -> Transport {
        let files = test_tls_files(cluster_id, node_id);
        let tls =
            TlsConfigBundle::from_pem_files(&files.ca, &files.certificate, &files.private_key)
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

    fn node_named(raw: &str) -> ClusterNodeName {
        ClusterNodeName::parse(raw).expect("valid name")
    }

    fn scheduled_node(identifier_raw: &str, kind: ModelKind) -> ScheduledNode {
        ScheduledNode::new(model_of_kind(identifier_raw, kind)).placed_on(
            Some(ClusterNodeName::parse("node-1").expect("valid name")),
            vec![ClusterNodeName::parse("node-1").expect("valid name")],
        )
    }

    fn scheduled_node_on(identifier_raw: &str, kind: ModelKind, node: &str) -> ScheduledNode {
        let node = ClusterNodeName::parse(node).expect("valid name");
        ScheduledNode::new(model_of_kind(identifier_raw, kind))
            .placed_on(Some(node.clone()), vec![node])
    }

    fn placement_member(identifier_raw: &str, kind: ModelKind) -> NodeRef {
        NodeRef::new(kind, named::<ModelName>(identifier_raw))
    }

    fn placement_group(
        members: Vec<NodeRef>,
        primary_node: &ClusterNodeName,
    ) -> PlacementGroupSchedule {
        PlacementGroupSchedule {
            members,
            primary_node: Some(primary_node.clone()),
        }
    }

    async fn create_test_domain(consensus: &Proposer, raw: &str) {
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
    struct TestService {
        service: SessionServiceImpl,
        registry: Arc<Registry>,
        path: PathBuf,
    }

    async fn build_test_service(create_default_domain_flag: bool) -> TestService {
        let path = test_db_path();
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("test db directory should exist");
        let db = Database::builder(&path)
            .open()
            .expect("database should open");
        let registry = Arc::new(
            Registry::from_database(db.clone(), Some(path.as_path()))
                .expect("registry should open"),
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
        let consensus = Consensus::from_database(
            db,
            ConsensusSettings {
                cluster_name: "test".to_string(),
                node_id: expected_leader.clone(),
                interconnect: interconnect.clone(),
                node_unavailability_timeout: Duration::from_secs(10),
                raft_heartbeat_interval: Duration::from_millis(50),
                raft_election_timeout_min: Duration::from_millis(150),
                raft_election_timeout_max: Duration::from_millis(300),
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
                ResourceStore::open(
                    path.join("resources"),
                    nervix_execution::Executor::default(),
                )
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

    #[tokio::test]
    async fn dropping_cluster_gate_owner_releases_local_durable_hold() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        let domain = DomainName::parse("default").expect("valid domain");
        let operation_id = 41;
        service
            .inner
            .runtime
            .engage_entity_gate_operation(
                operation_id,
                &domain,
                &[],
                &[],
                EntityGatePurpose::ModelAlteration,
                EntityGateLease {
                    deadline: tokio::time::Instant::now() + Duration::from_secs(30),
                    reason: "canceled coordinator test",
                },
            )
            .await
            .expect("local gate hold should engage");
        assert!(
            service
                .inner
                .runtime
                .entity_gate_operation_is_held(operation_id, &domain)
        );

        let mut gate = ClusterEntityGate::new(&service, operation_id, &domain);
        gate.record_attempt(service.inner.consensus.local_node_id().clone());
        drop(gate);

        tokio::time::timeout(Duration::from_secs(2), async {
            while service
                .inner
                .runtime
                .entity_gate_operation_is_held(operation_id, &domain)
            {
                tokio::task::consume_budget().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped coordinator guard should release its local durable hold");

        service.inner.shutdown.cancel();
        service.inner.service_tasks.close();
        service.inner.service_tasks.wait().await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn completion_context_preserves_prefix_for_post_filtering() {
        let input = "CREATE SCHE";
        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
        } = completion_context(input, input.len());

        assert_eq!(grammar_input, "CREATE ");
        assert_eq!(grammar_cursor, "CREATE ".len());
        assert_eq!(prefix, "sche");
    }

    #[test]
    fn keyword_completion_is_filtered_by_original_prefix() {
        let input = "CREATE SCHE";
        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
        } = completion_context(input, input.len());
        let filtered = suggest_client_statement(&grammar_input, grammar_cursor)
            .into_iter()
            .filter(|item| {
                prefix.is_empty()
                    || item
                        .to_ascii_lowercase()
                        .starts_with(&prefix.to_ascii_lowercase())
            })
            .collect::<Vec<_>>();

        assert_eq!(filtered, vec!["SCHEMA".to_string()]);
    }

    #[test]
    fn placement_runtime_node_rendering_qualifies_only_kind_collisions() {
        let nodes = vec![
            placement_member("shared", ModelKind::Junction),
            placement_member("shared", ModelKind::Deduplicator),
            placement_member("sink", ModelKind::Emitter),
        ];

        assert_eq!(
            format_placement_runtime_nodes(&nodes),
            "junction:shared, deduplicator:shared, sink"
        );
        assert_eq!(
            format_placement_runtime_nodes(&[
                placement_member("source", ModelKind::Junction),
                placement_member("sink", ModelKind::Emitter),
            ]),
            "source, sink"
        );
    }

    #[test]
    fn drop_node_quorum_error_allows_available_current_quorum() {
        let voters = BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ]);
        let live_node_ids = BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]);

        assert!(
            SessionServiceImpl::drop_node_quorum_error(
                &ClusterNodeName::parse("node-2").expect("valid name"),
                &voters,
                &live_node_ids
            )
            .is_none()
        );
    }

    #[test]
    fn drop_node_rebuild_uses_only_schedulable_nodes_for_new_assignments() {
        let live_voters = vec![
            named::<ClusterNodeName>("node-live"),
            named::<ClusterNodeName>("node-cordoned"),
        ];
        let schedulable_nodes = vec![named::<ClusterNodeName>("node-live")];

        let (new_assignment_candidates, preservable_nodes) =
            SessionServiceImpl::drop_node_schedule_node_sets(&live_voters, &schedulable_nodes);

        assert_eq!(new_assignment_candidates, schedulable_nodes);
        assert_eq!(preservable_nodes, live_voters);
    }

    #[test]
    fn move_next_scheduled_node_for_drain_moves_only_one_node_in_canonical_order() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("ingest_notifications", ModelKind::Ingestor, "node-2"),
                scheduled_node_on("emit_notifications", ModelKind::Emitter, "node-2"),
            ],
            Vec::new(),
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("ingest_notifications", ModelKind::Ingestor, "node-1"),
                scheduled_node_on("emit_notifications", ModelKind::Emitter, "node-3"),
            ],
            Vec::new(),
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "emitter emit_notifications".to_string(),
                promoted_replica: None,
                fallback_node: Some(ClusterNodeName::parse("node-3").expect("valid name")),
            })
        );
        assert_eq!(
            schedule.nodes[0].assigned_nodes,
            vec![named::<ClusterNodeName>("node-2")]
        );
        assert_eq!(
            schedule.nodes[1].assigned_nodes,
            vec![named::<ClusterNodeName>("node-3")]
        );
    }

    #[test]
    fn planned_drain_uses_canonical_runtime_node_order() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("zeta", ModelKind::Junction, "node-2"),
                scheduled_node_on("alpha", ModelKind::Junction, "node-2"),
            ],
            Vec::new(),
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("zeta", ModelKind::Junction, "node-1"),
                scheduled_node_on("alpha", ModelKind::Junction, "node-1"),
            ],
            Vec::new(),
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
            ]),
            &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "junction alpha".to_string(),
                promoted_replica: None,
                fallback_node: Some(named::<ClusterNodeName>("node-1")),
            })
        );
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-2"))
        );
        assert_eq!(
            schedule.nodes[1].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
    }

    #[test]
    fn planned_drain_uses_the_first_member_to_order_placement_groups() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let alpha_members = vec![placement_member("alpha", ModelKind::Junction)];
        let zeta_members = vec![placement_member("zeta", ModelKind::Junction)];
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("zeta", ModelKind::Junction, "node-2"),
                scheduled_node_on("alpha", ModelKind::Junction, "node-2"),
            ],
            vec![
                placement_group(
                    zeta_members.clone(),
                    &ClusterNodeName::parse("node-2").expect("valid name"),
                ),
                placement_group(
                    alpha_members.clone(),
                    &ClusterNodeName::parse("node-2").expect("valid name"),
                ),
            ],
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("zeta", ModelKind::Junction, "node-1"),
                scheduled_node_on("alpha", ModelKind::Junction, "node-1"),
            ],
            vec![
                placement_group(
                    zeta_members,
                    &ClusterNodeName::parse("node-1").expect("valid name"),
                ),
                placement_group(
                    alpha_members,
                    &ClusterNodeName::parse("node-1").expect("valid name"),
                ),
            ],
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
            ]),
            &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "placement group [alpha]".to_string(),
                promoted_replica: None,
                fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
            })
        );
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-2"))
        );
        assert_eq!(
            schedule.nodes[1].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
    }

    #[test]
    fn planned_drain_prefers_policy_target_and_retains_former_owner_as_first_replica() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![
                        node_named("node-2"),
                        node_named("node-3"),
                        node_named("node-4"),
                    ],
                ),
            ],
            Vec::new(),
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "deduplicator dedup_notifications".to_string(),
                promoted_replica: None,
                fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
            })
        );
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert_eq!(
            schedule.nodes[0].assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3"),
            ]
        );
    }

    #[test]
    fn drain_require_group_prefers_policy_target_over_common_replica() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let members = vec![
            placement_member("corridor_source", ModelKind::Junction),
            placement_member("corridor_sink", ModelKind::Junction),
        ];
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("corridor_source", ModelKind::Junction).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
                scheduled_node("corridor_sink", ModelKind::Junction).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            vec![placement_group(
                members.clone(),
                &ClusterNodeName::parse("node-2").expect("valid name"),
            )],
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("corridor_source", ModelKind::Junction).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-3")],
                ),
                scheduled_node("corridor_sink", ModelKind::Junction).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-3")],
                ),
            ],
            vec![placement_group(
                members,
                &ClusterNodeName::parse("node-1").expect("valid name"),
            )],
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "placement group [corridor_source, corridor_sink]".to_string(),
                promoted_replica: None,
                fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
            })
        );
        assert!(
            schedule
                .nodes
                .values()
                .all(|node| node.primary_node.as_ref()
                    == Some(&ClusterNodeName::parse("node-1").expect("valid name")))
        );
        assert_eq!(
            schedule.placement_groups[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert!(schedule.nodes.values().all(|node| {
            node.assigned_nodes
                == vec![
                    named::<ClusterNodeName>("node-1"),
                    named::<ClusterNodeName>("node-2"),
                ]
        }));
    }

    #[test]
    fn planned_drain_replaces_an_unavailable_replica_with_the_former_owner() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );
        let desired = DomainSchedule::new(
            domain,
            vec![scheduled_node_on(
                "dedup_notifications",
                ModelKind::Deduplicator,
                "node-1",
            )],
            Vec::new(),
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
            ]),
            &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "deduplicator dedup_notifications".to_string(),
                promoted_replica: None,
                fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
            })
        );
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert_eq!(
            schedule.nodes[0].assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2")
            ]
        );
    }

    #[test]
    fn planned_schedule_move_prefers_the_former_owner_for_the_first_replica_slot() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let current = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );
        let mut planned = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );

        prefer_former_owners_as_replicas(
            Some(&current),
            &mut planned,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
        );

        assert_eq!(
            planned.nodes[0].assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2")
            ]
        );
    }

    #[test]
    fn merge_existing_schedule_data_prefers_policy_target_when_primary_dies() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-4")],
                ),
            ],
            Vec::new(),
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![
                        node_named("node-2"),
                        node_named("node-3"),
                        node_named("node-4"),
                    ],
                ),
            ],
            Vec::new(),
        );

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
        );

        assert_eq!(
            next.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert_eq!(
            next.nodes[0].assigned_nodes,
            vec![named::<ClusterNodeName>("node-1")]
        );
    }

    #[test]
    fn merge_existing_schedule_data_falls_back_to_fresh_assignment_without_live_replica() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![scheduled_node_on(
                "dedup_notifications",
                ModelKind::Deduplicator,
                "node-1",
            )],
            Vec::new(),
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[ClusterNodeName::parse("node-1").expect("valid name")],
        );

        assert_eq!(
            next.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert_eq!(
            next.nodes[0].assigned_nodes,
            vec![named::<ClusterNodeName>("node-1")]
        );
    }

    #[test]
    fn merge_existing_schedule_data_preserves_matching_ingestor_schedule_and_assignment() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let preserved_schedule = KafkaPartitionSchedule::new(nonzero!(2u64), vec![0, 1], 7);
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("ingest_notifications", ModelKind::Ingestor).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("ingest_notifications", ModelKind::Ingestor, "node-1")
                    .with_kafka_partitions(preserved_schedule.clone()),
            ],
            Vec::new(),
        );

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
        );

        assert_eq!(
            next.nodes[0].kafka_partition_schedule,
            Some(preserved_schedule)
        );
        assert_eq!(
            next.nodes[0].assigned_nodes,
            vec![named::<ClusterNodeName>("node-1")]
        );
    }

    #[test]
    fn merge_existing_schedule_data_ignores_non_matching_nodes() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("ingest_notifications", ModelKind::Ingestor),
                scheduled_node("kafka_main", ModelKind::Client),
            ],
            Vec::new(),
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("other_ingestor", ModelKind::Ingestor).with_kafka_partitions(
                    KafkaPartitionSchedule::new(nonzero!(2u64), vec![0, 1], 3),
                ),
                scheduled_node("ingest_notifications", ModelKind::Client)
                    .with_kafka_partitions(KafkaPartitionSchedule::new(nonzero!(1u64), vec![0], 2)),
            ],
            Vec::new(),
        );

        SessionServiceImpl::merge_existing_schedule_data(&mut next, Some(&existing), &[]);

        assert_eq!(next.nodes[0].kafka_partition_schedule, None);
        assert_eq!(next.nodes[1].kafka_partition_schedule, None);
    }

    #[test]
    fn merge_existing_schedule_data_rejects_a_split_require_group() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let members = vec![
            placement_member("corridor_source", ModelKind::Junction),
            placement_member("corridor_sink", ModelKind::Junction),
        ];
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-1"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-1"),
            ],
            vec![placement_group(
                members.clone(),
                &ClusterNodeName::parse("node-1").expect("valid name"),
            )],
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-2"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-3"),
            ],
            vec![placement_group(
                members,
                &ClusterNodeName::parse("node-2").expect("valid name"),
            )],
        );

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
        );

        assert!(next.nodes.values().all(|node| node.primary_node.as_ref()
            == Some(&ClusterNodeName::parse("node-1").expect("valid name"))));
        assert_eq!(
            next.placement_groups[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
    }

    #[test]
    fn merge_existing_schedule_data_preserves_an_intact_require_group() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let members = vec![
            placement_member("corridor_source", ModelKind::Junction),
            placement_member("corridor_sink", ModelKind::Junction),
        ];
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-1"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-1"),
            ],
            vec![placement_group(
                members.clone(),
                &ClusterNodeName::parse("node-1").expect("valid name"),
            )],
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-2"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-2"),
            ],
            vec![placement_group(
                members,
                &ClusterNodeName::parse("node-2").expect("valid name"),
            )],
        );

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
            ],
        );

        assert!(next.nodes.values().all(|node| node.primary_node.as_ref()
            == Some(&ClusterNodeName::parse("node-2").expect("valid name"))));
        assert_eq!(
            next.placement_groups[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-2"))
        );
    }

    #[test]
    fn drain_relocates_a_require_group_as_one_unit() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let members = vec![
            placement_member("corridor_source", ModelKind::Junction),
            placement_member("corridor_sink", ModelKind::Junction),
        ];
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-2"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-2"),
            ],
            vec![placement_group(
                members.clone(),
                &ClusterNodeName::parse("node-2").expect("valid name"),
            )],
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-1"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-1"),
            ],
            vec![placement_group(
                members,
                &ClusterNodeName::parse("node-1").expect("valid name"),
            )],
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert!(moved.is_some());
        assert!(
            schedule
                .nodes
                .values()
                .all(|node| node.primary_node.as_ref()
                    == Some(&ClusterNodeName::parse("node-1").expect("valid name")))
        );
        assert_eq!(
            schedule.placement_groups[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
    }

    #[test]
    fn failover_relocates_a_require_group_to_one_target() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let members = vec![
            placement_member("corridor_source", ModelKind::Junction),
            placement_member("corridor_sink", ModelKind::Junction),
        ];
        let mut schedule = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("corridor_source", ModelKind::Junction).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
                scheduled_node("corridor_sink", ModelKind::Junction).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-1")],
                ),
            ],
            vec![placement_group(
                members,
                &ClusterNodeName::parse("node-2").expect("valid name"),
            )],
        );

        let moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
            &mut schedule,
            None,
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert!(!moves.is_empty());
        let group_host = schedule.placement_groups[0]
            .primary_node
            .as_ref()
            .expect("require group must retain a host");
        assert!(
            schedule
                .nodes
                .values()
                .all(|node| node.primary_node.as_ref() == Some(group_host))
        );
    }

    #[test]
    fn failover_does_not_promote_a_cordoned_live_replica() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );

        let moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
            &mut schedule,
            None,
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
        );

        assert!(!moves.is_empty());
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
    }

    #[test]
    fn failover_promotes_live_replica_before_policy_target() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );

        let moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
            &mut schedule,
            Some(&desired),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert_eq!(
            moves,
            vec![DrainMove {
                label: "deduplicator dedup_notifications".to_string(),
                promoted_replica: Some(ClusterNodeName::parse("node-3").expect("valid name")),
                fallback_node: None,
            }]
        );
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-3"))
        );
        assert_eq!(
            schedule.nodes[0].assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-3"),
                named::<ClusterNodeName>("node-1")
            ]
        );
    }

    #[test]
    fn resource_ref_suggestions_expand_known_resource_names_in_the_active_domain() {
        let tenant = DomainName::parse("tenant").expect("valid domain");
        let other = DomainName::parse("other").expect("valid domain");
        let resources = ResourceVersionStatus {
            next_version_by_resource: SortedVec::from_unsorted(vec![
                ResourceVersionCounter {
                    domain: tenant.clone(),
                    identifier: named("fraud_model"),
                    next_version: 2,
                },
                ResourceVersionCounter {
                    domain: tenant.clone(),
                    identifier: named("proto"),
                    next_version: 1,
                },
                ResourceVersionCounter {
                    domain: other.clone(),
                    identifier: named("promo_model"),
                    next_version: 1,
                },
            ]),
            ..Default::default()
        };

        assert_eq!(
            resource_ref_suggestions(&resources, &tenant, "pr"),
            vec!["proto".to_string()]
        );
        assert_eq!(
            resource_ref_suggestions(&resources, &tenant, ""),
            vec!["fraud_model".to_string(), "proto".to_string()]
        );
        assert_eq!(
            resource_ref_suggestions(&resources, &other, ""),
            vec!["promo_model".to_string()]
        );
    }

    #[test]
    fn resource_version_suggestions_expand_known_versions() {
        let tenant = DomainName::parse("tenant").expect("valid domain");
        let other = DomainName::parse("other").expect("valid domain");
        let resources = ResourceVersionStatus {
            versions: SortedVec::from_unsorted(vec![
                ResourceVersion {
                    id: nervix_models::ResourceId::new(tenant.clone(), named("proto"), 1),
                    root_checksum: "a".to_string(),
                    manifest_checksum: "a".to_string(),
                    file_count: 1,
                    total_bytes: 1,
                    created_at: Timestamp::from_unix_nanos(1),
                    created_by_node: ClusterNodeName::parse("node-1").expect("valid name"),
                },
                ResourceVersion {
                    id: nervix_models::ResourceId::new(tenant.clone(), named("proto"), 12),
                    root_checksum: "b".to_string(),
                    manifest_checksum: "b".to_string(),
                    file_count: 1,
                    total_bytes: 1,
                    created_at: Timestamp::from_unix_nanos(1),
                    created_by_node: ClusterNodeName::parse("node-1").expect("valid name"),
                },
            ]),
            ..Default::default()
        };

        assert_eq!(
            resource_version_suggestions(&resources, &tenant, &named("proto"), ""),
            vec!["1".to_string(), "12".to_string()]
        );
        assert_eq!(
            resource_version_suggestions(&resources, &tenant, &named("proto"), "1"),
            vec!["1".to_string(), "12".to_string()]
        );
        assert!(
            resource_version_suggestions(&resources, &other, &named("proto"), "").is_empty(),
            "another domain must not see this domain's resource versions"
        );
        assert_eq!(
            requested_resource_versions(
                "DESCRIBE RESOURCE proto VERSION ",
                "DESCRIBE RESOURCE proto VERSION ".len()
            ),
            Some(named("proto"))
        );
    }

    #[test]
    fn upload_resource_path_fragment_is_detected_for_upload_resource_path() {
        assert_eq!(
            upload_resource_path_fragment(
                "UPLOAD RESOURCE proto VERSION '/tmp/pro",
                "UPLOAD RESOURCE proto VERSION '/tmp/pro".len(),
            ),
            Some("/tmp/pro")
        );
        assert_eq!(
            upload_resource_path_fragment(
                "UPLOAD RESOURCE proto VERSION ",
                "UPLOAD RESOURCE proto VERSION ".len(),
            ),
            Some("")
        );
        assert_eq!(
            upload_resource_path_fragment(
                "UPLOAD RESOURCE proto VERSION ",
                "UPLOAD RESOURCE proto VERSION '".len(),
            ),
            Some("")
        );
        assert_eq!(
            upload_resource_path_fragment(
                "DESCRIBE RESOURCE proto VERSION ",
                "DESCRIBE RESOURCE proto VERSION ".len(),
            ),
            None
        );
    }

    #[test]
    fn validate_domain_config_accepts_paced_domains_with_valid_period() {
        let config = DomainConfig {
            pace: DomainPace::Paced,
            period: "30s".to_string(),
            skew: "1s".to_string(),
            placement: nervix_models::PlacementPolicy::Neutral,
        };

        assert!(validate_domain_config(&config).is_ok());
    }

    #[test]
    fn validate_domain_config_accepts_unpaced_domains_without_tick_period() {
        let config = DomainConfig {
            pace: DomainPace::Unpaced,
            period: "not-a-duration".to_string(),
            skew: "not-a-duration".to_string(),
            placement: nervix_models::PlacementPolicy::Neutral,
        };

        assert!(validate_domain_config(&config).is_ok());
    }

    #[test]
    fn request_domain_helpers_cover_current_state_and_validation() {
        assert_eq!(parse_request_domain(""), Err(RequestDomainError::Missing));
        assert_eq!(
            parse_request_domain(" tenant_a "),
            Ok(DomainName::parse("tenant_a").expect("valid domain"))
        );
        assert_eq!(
            parse_request_domain("bad.domain"),
            Err(RequestDomainError::Invalid)
        );
    }

    #[test]
    fn parse_and_text_encoding_helpers_roundtrip() {
        assert_eq!(current_word_prefix("CREATE SCHE", 11), "sche");
        assert_eq!(word_start("CREATE SCHE", 11), 7);
        assert_eq!(
            parse_human_duration("1500ms").expect("valid duration"),
            Duration::from_millis(1500)
        );
        assert_eq!(
            parse_human_bytes("1.5MiB").expect("valid bytes"),
            ubyte::ByteUnit::Mebibyte(1) + ubyte::ByteUnit::Kibibyte(512)
        );
        assert_eq!(parse_trace_sample_ratio("0.25"), Ok(0.25));
        assert!(parse_trace_sample_ratio("1.25").is_err());

        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let hex = encode_hex(&bytes);
        assert_eq!(hex, "deadbeef");

        assert!(parse_human_duration("oops").is_err());
        assert!(parse_human_bytes("oops").is_err());
    }

    #[test]
    fn server_args_parse_memory_pressure_options() {
        let args = test_args(&[
            "--memory-high-watermark",
            "2MiB",
            "--memory-low-watermark",
            "1MiB",
            "--memory-pressure-check-interval",
            "250ms",
            "--memory-pressure-resume-jitter",
            "500ms",
        ]);
        let app = Application::try_from(args).expect("args should parse");
        let memory_pressure = app.memory_pressure.expect("memory pressure configured");

        assert_eq!(memory_pressure.high_watermark, ubyte::ByteUnit::Mebibyte(2));
        assert_eq!(memory_pressure.low_watermark, ubyte::ByteUnit::Mebibyte(1));
        assert_eq!(memory_pressure.check_interval, Duration::from_millis(250));
        assert_eq!(memory_pressure.resume_jitter, Duration::from_millis(500));
    }

    #[test]
    fn server_args_reject_incomplete_memory_pressure_watermarks() {
        let args = test_args(&["--memory-high-watermark", "2MiB"]);

        let error = Application::try_from(args).expect_err("low watermark is required");
        assert!(format!("{error:?}").contains("memory high watermark requires"));
    }

    #[test]
    fn server_args_parse_opentelemetry_options() {
        let args = test_args(&[
            "--otel-enabled",
            "--otel-otlp-endpoint",
            "http://collector:4317",
            "--otel-service-name",
            "nervix-test",
            "--otel-trace-sample-ratio",
            "0.5",
        ]);

        assert!(args.otel_enabled);
        assert_eq!(args.otel_otlp_endpoint, "http://collector:4317");
        assert_eq!(args.otel_service_name, "nervix-test");
        assert_eq!(args.otel_trace_sample_ratio, 0.5);
    }

    #[test]
    fn server_args_do_not_require_opentelemetry_options() {
        let args = test_args(&[]);

        assert!(!args.otel_enabled);
        assert_eq!(args.otel_otlp_endpoint, "http://127.0.0.1:4317");
        assert_eq!(args.otel_service_name, "nervix");
        assert_eq!(args.otel_trace_sample_ratio, 1.0);
    }

    #[test]
    fn server_args_only_require_opentelemetry_enable_flag_to_enable_export() {
        let args = test_args(&["--otel-enabled"]);

        assert!(args.otel_enabled);
        assert_eq!(args.otel_otlp_endpoint, "http://127.0.0.1:4317");
        assert_eq!(args.otel_service_name, "nervix");
        assert_eq!(args.otel_trace_sample_ratio, 1.0);
    }

    #[test]
    fn server_args_reject_invalid_opentelemetry_sample_ratio() {
        let result = try_test_args(&["--otel-trace-sample-ratio", "2.0"]);

        assert!(result.is_err());
    }

    #[test]
    fn diagnostic_and_registry_error_helpers_map_spans() {
        let parse_diagnostic = ParseDiagnostic {
            message: "unexpected token".to_string(),
            span: 3..7,
        };
        let mapped = map_diagnostic(&parse_diagnostic);
        assert_eq!(mapped.message, "unexpected token");
        assert_eq!(mapped.span_start, 3);
        assert_eq!(mapped.span_end, 7);

        let response = error_response("parse error", std::slice::from_ref(&parse_diagnostic));
        assert!(!response.success);
        assert_eq!(response.message, "parse error");
        assert_eq!(response.diagnostics, vec![mapped]);

        let query = "CREATE RELAY orders SCHEMA notification UNBRANCHED;";
        let identifier = named("orders");
        assert_eq!(find_identifier_span(query, &identifier), Some(13..19));

        let domain = DomainName::parse("default").expect("valid domain");
        let err = error_stack::Report::new(RegistryError::AlreadyExists {
            domain: "default".to_string(),
            identifier: "orders".to_string(),
        });
        let registry_response = create_registry_error_response(query, &domain, &identifier, &err);
        assert!(!registry_response.success);
        assert!(registry_response.message.contains("orders"));
        assert_eq!(registry_response.diagnostics.len(), 1);
        assert_eq!(registry_response.diagnostics[0].span_start, 13);
        assert_eq!(registry_response.diagnostics[0].span_end, 19);
        assert_eq!(
            infer_kind_from_error_target(&err, &identifier),
            Some("model")
        );

        let missing_target = error_stack::Report::new(RegistryError::NotFound {
            domain: "default".to_string(),
            identifier: "other".to_string(),
        });
        assert_eq!(
            infer_kind_from_error_target(&missing_target, &identifier),
            None
        );
    }

    #[test]
    fn parse_subscription_literal_enforces_declared_types() {
        let field = named("created_at");
        assert!(matches!(
            parse_subscription_literal(
                &field,
                &ParseAsType::Datetime,
                &SubscriptionLiteral::String("2025-01-02T03:04:05+00:00".to_string())
            ),
            Ok(runtime_schema::RuntimeValue::Datetime(_))
        ));
        assert!(matches!(
            parse_subscription_literal(
                &named("active"),
                &ParseAsType::Bool,
                &SubscriptionLiteral::Bool(true)
            ),
            Ok(runtime_schema::RuntimeValue::Bool(true))
        ));
        let err = parse_subscription_literal(
            &named("user_id"),
            &ParseAsType::U32,
            &SubscriptionLiteral::String("42".to_string()),
        )
        .expect_err("string should not satisfy numeric field");
        assert!(err.contains("expects numeric literal"));
    }

    #[test]
    fn subscription_batch_sample_rate_is_validated() {
        assert_eq!(parse_subscription_batch_sample_rate(None), Ok(None));
        assert_eq!(
            parse_subscription_batch_sample_rate(Some("0.25")),
            Ok(Some(0.25))
        );
        assert!(parse_subscription_batch_sample_rate(Some("1.1")).is_err());
        assert!(parse_subscription_batch_sample_rate(Some("bad")).is_err());
    }

    #[test]
    fn subscription_sampling_respects_extreme_rates() {
        let message = RelayMessage {
            key: string_branch_key("tenant", "acme"),
            record: runtime_schema::test_runtime_row([]),
            acks: crate::runtime_ack::AckSet::empty(),
        };
        assert!(subscription_sample_passes(None, &message));
        assert!(subscription_sample_passes(Some(1.0), &message));
        assert!(!subscription_sample_passes(Some(0.0), &message));
    }

    #[test]
    fn http_request_helpers_detect_upgrade_and_format_messages() {
        let header = hyper::header::HeaderValue::from_static("keep-alive, Upgrade");
        assert!(header_contains_token(&header, "upgrade"));
        assert!(!header_contains_token(&header, "websocket"));

        let message = RelayMessage {
            key: string_branch_key("tenant", "acme"),
            record: runtime_schema::test_runtime_row([(
                "user_id".to_string(),
                runtime_schema::RuntimeValue::U32(42),
            )]),
            acks: crate::runtime_ack::AckSet::empty(),
        };
        let no_sensitive_fields = nervix_vm::SchemaSensitivity::default();
        assert_eq!(
            format_stream_message(&message, &no_sensitive_fields),
            r#"key={"tenant":"acme"} payload={"user_id":42}"#
        );
        let sensitive_user_id = nervix_vm::SchemaSensitivity::from_sensitive_fields(["user_id"]);
        assert_eq!(
            format_stream_message(&message, &sensitive_user_id),
            r#"key={"tenant":"acme"} payload={"user_id":"<masked>"}"#
        );

        let no_key = RelayMessage {
            key: None,
            record: runtime_schema::test_runtime_row([(
                "user_id".to_string(),
                runtime_schema::RuntimeValue::U32(42),
            )]),
            acks: crate::runtime_ack::AckSet::empty(),
        };
        assert_eq!(
            format_stream_message(&no_key, &no_sensitive_fields),
            r#"{"user_id":42}"#
        );
    }

    #[tokio::test]
    async fn session_subscriptions_track_names_and_cleanup_tasks() {
        let mut subscriptions = SessionSubscriptions::new();
        let (tx, _rx) = mpsc::channel(4);
        let events = crate::runtime::RelayBroadcast::with_capacity(
            std::num::NonZeroUsize::new(4).expect("test relay capacity must be nonzero"),
        );
        let events_rx = events.new_receiver();
        subscriptions.insert(
            named("live_events"),
            DomainName::parse("default").expect("valid domain"),
            named("events"),
            SessionSubscriptionTaskConfig {
                filter_map: None,
                sensitivity: nervix_vm::SchemaSensitivity::default(),
                delivery_behavior: SubscriptionDeliveryBehavior::Blocking,
                batch_sample_rate: None,
                runtime: Runtime::default(),
                materialized_stream_owner_nodes: HashMap::default(),
                receiver: events_rx,
                tx,
            },
        );
        assert_eq!(
            subscriptions.matching_names("LIVE"),
            vec!["live_events".to_string()]
        );
        assert!(subscriptions.matching_names("missing").is_empty());

        let removed = subscriptions
            .remove(&named("live_events"))
            .await
            .expect("subscription should be removed");
        assert_eq!(removed.0.as_str(), "default");
        assert_eq!(removed.1.as_str(), "events");
        assert!(
            subscriptions
                .remove(&named("missing_events"))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn rebuilt_relay_drops_subscription_with_clear_session_error() {
        let mut subscriptions = SessionSubscriptions::new();
        let (tx, mut rx) = mpsc::channel(4);
        let events = crate::runtime::RelayBroadcast::with_capacity(
            std::num::NonZeroUsize::new(4).expect("test relay capacity must be nonzero"),
        );
        subscriptions.insert(
            named("live_events"),
            DomainName::parse("default").expect("valid domain"),
            named("events"),
            SessionSubscriptionTaskConfig {
                filter_map: None,
                sensitivity: nervix_vm::SchemaSensitivity::default(),
                delivery_behavior: SubscriptionDeliveryBehavior::Blocking,
                batch_sample_rate: None,
                runtime: Runtime::default(),
                materialized_stream_owner_nodes: HashMap::default(),
                receiver: events.new_receiver(),
                tx,
            },
        );

        drop(events);
        let response = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("subscription close error should arrive")
            .expect("session channel should remain open")
            .expect("session response should succeed");
        let Some(proto::session_response::Event::Server(event)) = response.event else {
            panic!("expected server error event");
        };
        assert_eq!(event.level, i32::from(ServerEventLevel::Error));
        assert!(
            event
                .message
                .contains("subscription 'live_events' was dropped")
        );
        assert!(event.message.contains("recreate the subscription"));
        tokio::task::yield_now().await;
        assert!(!subscriptions.contains_name(&named("live_events")));

        let _ = subscriptions
            .remove(&named("live_events"))
            .await
            .expect("closed subscription metadata should remain removable");
    }

    #[tokio::test]
    async fn create_domain_if_not_exists_returns_already_existed() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;

        let first = service
            .create_domain(CreateStatement::new(
                CreateDomain {
                    id: DomainName::parse("prod").expect("valid domain"),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
                        period: "0ms".to_string(),
                        skew: "0ms".to_string(),
                        placement: nervix_models::PlacementPolicy::Neutral,
                    },
                },
                false,
            ))
            .await;
        assert!(first.success);
        assert!(!first.already_existed);

        let duplicate = service
            .create_domain(CreateStatement::new(
                CreateDomain {
                    id: DomainName::parse("prod").expect("valid domain"),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
                        period: "0ms".to_string(),
                        skew: "0ms".to_string(),
                        placement: nervix_models::PlacementPolicy::Neutral,
                    },
                },
                true,
            ))
            .await;
        assert!(duplicate.success);
        assert!(duplicate.already_existed);
        assert!(duplicate.message.contains("already exists"));

        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn create_resource_if_not_exists_returns_already_existed() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let default = DomainName::parse("default").expect("valid domain");
        create_test_domain(&service.inner.consensus, "other").await;
        let other = DomainName::parse("other").expect("valid domain");

        let first = service
            .create_resource(
                &default,
                CreateStatement::new(
                    CreateResource {
                        identifier: named("fraud_model"),
                    },
                    false,
                ),
            )
            .await;
        assert!(first.success);
        assert!(!first.already_existed);

        let duplicate = service
            .create_resource(
                &default,
                CreateStatement::new(
                    CreateResource {
                        identifier: named("fraud_model"),
                    },
                    true,
                ),
            )
            .await;
        assert!(duplicate.success);
        assert!(duplicate.already_existed);
        assert!(duplicate.message.contains("already exists"));

        let same_name_other_domain = service
            .create_resource(
                &other,
                CreateStatement::new(
                    CreateResource {
                        identifier: named("fraud_model"),
                    },
                    false,
                ),
            )
            .await;
        assert!(
            same_name_other_domain.success,
            "resources are domain-owned: {}",
            same_name_other_domain.message
        );
        assert!(!same_name_other_domain.already_existed);

        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_create_if_not_exists_returns_already_existed_for_models() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let first = service
            .process_command(
                CommandRequest {
                    query: "CREATE IF NOT EXISTS SCHEMA notification ( user_id U32 );".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(first.success);
        assert!(!first.already_existed);

        let duplicate = service
            .process_command(
                CommandRequest {
                    query: "CREATE IF NOT EXISTS SCHEMA notification ( user_id U32 );".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(duplicate.success);
        assert!(duplicate.already_existed);
        assert!(duplicate.message.contains("already exists"));

        let schema = registry
            .get::<CreateSchema>(
                &DomainName::parse("default").expect("valid domain"),
                named::<ModelName>("notification"),
            )
            .expect("registry get should succeed")
            .expect("schema should exist");
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].name.as_str(), "user_id");

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_rejects_implicit_semicolon_batch() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(false).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let result = service
            .process_command(
                CommandRequest {
                    query: "CREATE DOMAIN prod; CREATE SCHEMA notification ( user_id U32 )"
                        .to_string(),
                    domain: "prod".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(!result.success);
        assert_eq!(result.message, "multiple commands require BEGIN");
        assert_eq!(command_transaction_state(&result), None);
        assert!(
            registry
                .get::<CreateSchema>(
                    &DomainName::parse("prod").expect("valid domain"),
                    named::<ModelName>("notification"),
                )
                .expect("registry get should succeed")
                .is_none(),
            "implicit batch must not create later models"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_commits_explicit_transaction_without_trailing_semicolon() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(false).await;
        create_test_domain(&service.inner.consensus, "prod").await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let result = service
            .process_command(
                CommandRequest {
                    query: "BEGIN; CREATE RELAY notifications SCHEMA notification UNBRANCHED; \
                            CREATE SCHEMA notification ( user_id U32 ); COMMIT"
                        .to_string(),
                    domain: "prod".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(result.success, "command must succeed: {}", result.message);
        assert_eq!(
            command_transaction_state(&result),
            Some(ApiTransactionState::Committed)
        );
        assert!(result.message.contains("quiesce level: DYNAMIC"));
        let commit = result
            .results
            .last()
            .expect("COMMIT result must be retained");
        assert_eq!(commit.message, "quiesce level: DYNAMIC");

        let schema = registry
            .get::<CreateSchema>(
                &DomainName::parse("prod").expect("valid domain"),
                named::<ModelName>("notification"),
            )
            .expect("registry get should succeed");
        assert!(
            schema.is_some(),
            "batch should create schema in prod domain"
        );
        let relay = registry
            .get::<CreateRelay>(
                &DomainName::parse("prod").expect("valid domain"),
                named::<ModelName>("notifications"),
            )
            .expect("registry get should succeed");
        assert!(
            relay.is_some(),
            "model create batch should resolve relay references atomically"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_queues_transaction_across_requests_and_reverts() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let begin = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(begin.success);
        assert_eq!(
            command_transaction_state(&begin),
            Some(ApiTransactionState::Open)
        );

        let queued = service
            .process_command(
                CommandRequest {
                    query: "CREATE SCHEMA queued_event ( user_id U32 );".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(queued.success);
        assert_eq!(queued.message, "quiesce level: DYNAMIC");
        assert_eq!(
            command_transaction_state(&queued),
            Some(ApiTransactionState::Open)
        );
        assert!(
            registry
                .get::<CreateSchema>(
                    &DomainName::parse("default").expect("valid domain"),
                    named::<ModelName>("queued_event"),
                )
                .expect("registry get should succeed")
                .is_none(),
            "queued command must not execute before COMMIT"
        );

        let reverted = service
            .process_command(
                CommandRequest {
                    query: "REVERT;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(reverted.success);
        assert!(
            reverted
                .message
                .starts_with("transaction reverted: dropped 1 command(s); id '")
        );
        assert_eq!(
            command_transaction_state(&reverted),
            Some(ApiTransactionState::Reverted)
        );
        assert!(
            registry
                .get::<CreateSchema>(
                    &DomainName::parse("default").expect("valid domain"),
                    named::<ModelName>("queued_event"),
                )
                .expect("registry get should succeed")
                .is_none(),
            "reverted command must not persist"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_rejects_begin_inside_begin() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let begin = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(begin.success);
        assert_eq!(
            command_transaction_state(&begin),
            Some(ApiTransactionState::Open)
        );

        let nested = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(!nested.success);
        assert_eq!(nested.message, "transaction is already active");
        assert_eq!(
            command_transaction_state(&nested),
            Some(ApiTransactionState::Open)
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_batch_returns_prior_successes_before_error() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(false).await;
        create_test_domain(&service.inner.consensus, "prod").await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let result = service
            .process_command(
                CommandRequest {
                    query: "BEGIN; CREATE SCHEMA duplicated ( user_id U32 ); CREATE SCHEMA \
                            duplicated ( user_id U32 ); COMMIT"
                        .to_string(),
                    domain: "prod".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(!result.success);
        assert_eq!(
            command_transaction_state(&result),
            Some(ApiTransactionState::Open)
        );
        assert!(result.message.contains("transaction started"));
        assert!(result.message.contains("already exists"));
        assert!(
            registry
                .get::<CreateSchema>(
                    &DomainName::parse("prod").expect("valid domain"),
                    named::<ModelName>("duplicated"),
                )
                .expect("registry get should succeed")
                .is_none(),
            "queue preflight failure must not execute the admitted prefix"
        );
        let transaction = service
            .inner
            .consensus
            .current_transaction(
                subscriptions
                    .transaction_id()
                    .expect("failed queue preflight must leave the transaction attached"),
            )
            .await
            .expect("open transaction must remain replicated");
        assert_eq!(transaction.pending_statement_count(), 1);

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_rejects_domain_and_user_creation_inside_a_transaction() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        for query in [
            "BEGIN; CREATE DOMAIN alpha; COMMIT",
            "BEGIN; CREATE USER alpha WITH PASSWORD 'secret'; COMMIT",
        ] {
            let result = service
                .process_command(
                    CommandRequest {
                        query: query.to_string(),
                        domain: "default".to_string(),
                    },
                    &tx,
                    &mut subscriptions,
                )
                .await;

            assert!(!result.success, "'{query}' must be rejected");
            assert!(
                result.message.contains("cannot be queued in a transaction"),
                "'{query}' produced: {}",
                result.message
            );

            let reverted = service
                .process_command(
                    CommandRequest {
                        query: "REVERT;".to_string(),
                        domain: "default".to_string(),
                    },
                    &tx,
                    &mut subscriptions,
                )
                .await;
            assert!(
                reverted.success,
                "revert must succeed: {}",
                reverted.message
            );
        }

        assert!(
            service
                .inner
                .consensus
                .current_domain(&DomainName::parse("alpha").expect("valid domain"))
                .await
                .is_none(),
            "a rejected CREATE DOMAIN must not reach the control plane"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_rejects_begin_without_an_existing_domain() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let missing = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: "absent".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(!missing.success);
        assert_eq!(missing.message, "domain 'absent' does not exist");
        assert!(!subscriptions.transaction_active());

        let unselected = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: String::new(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(!unselected.success);
        assert_eq!(unselected.message, "no active domain selected");
        assert!(!subscriptions.transaction_active());

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_rejects_statements_selecting_another_domain() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        create_test_domain(&service.inner.consensus, "other").await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let begin = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(begin.success, "begin must succeed: {}", begin.message);
        assert_eq!(
            begin
                .transaction
                .as_ref()
                .map(|status| status.domain.as_str()),
            Some("default")
        );

        let foreign = service
            .process_command(
                CommandRequest {
                    query: "CREATE SCHEMA foreign_event ( user_id U32 );".to_string(),
                    domain: "other".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(!foreign.success);
        assert!(
            foreign.message.contains("is bound to domain 'default'"),
            "unexpected message: {}",
            foreign.message
        );
        let transaction = service
            .inner
            .consensus
            .current_transaction(
                subscriptions
                    .transaction_id()
                    .expect("the transaction must stay attached"),
            )
            .await
            .expect("open transaction must remain replicated");
        assert_eq!(transaction.pending_statement_count(), 0);

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn attaching_to_committed_transaction_returns_the_recorded_aggregate() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        create_test_domain(&service.inner.consensus, "attach_results").await;
        let (tx, _rx) = mpsc::channel(16);
        let mut owner = SessionSubscriptions::new();

        let committed = service
            .process_command(
                CommandRequest {
                    query: "BEGIN; CREATE SCHEMA notification ( user_id U32 ); COMMIT".to_string(),
                    domain: "attach_results".to_string(),
                },
                &tx,
                &mut owner,
            )
            .await;
        assert!(
            committed.success,
            "commit must succeed: {}",
            committed.message
        );
        let transaction_id = committed
            .transaction
            .as_ref()
            .expect("commit result must carry transaction status")
            .id
            .clone();

        let mut observer = SessionSubscriptions::new();
        let attached = service
            .attach_transaction(
                proto::AttachTransactionRequest { id: transaction_id },
                &mut observer,
            )
            .await;

        assert!(
            !attached.success,
            "finished transaction attach must be terminal"
        );
        assert_eq!(
            attached.transaction.as_ref().map(|status| status.state),
            Some(i32::from(ApiTransactionState::Committed))
        );
        assert!(attached.message.contains("finished with outcome COMMITTED"));
        assert_eq!(attached.results.len(), 1);
        assert_eq!(attached.results[0].message, "quiesce level: DYNAMIC");

        owner.stop_all(&service).await;
        observer.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_model_create_batch_is_atomic_on_registry_failure() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(false).await;
        create_test_domain(&service.inner.consensus, "prod").await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let result = service
            .process_command(
                CommandRequest {
                    query: "BEGIN; CREATE RELAY notifications SCHEMA missing_schema UNBRANCHED; \
                            CREATE SCHEMA notification ( user_id U32 ); COMMIT"
                        .to_string(),
                    domain: "prod".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(!result.success);
        assert_eq!(
            command_transaction_state(&result),
            Some(ApiTransactionState::Failed)
        );

        let domain = DomainName::parse("prod").expect("valid domain");
        let relay = registry
            .get::<CreateRelay>(&domain, named::<ModelName>("notifications"))
            .expect("registry get should succeed");
        assert!(relay.is_none(), "failed model batch must not persist relay");
        let schema = registry
            .get::<CreateSchema>(&domain, named::<ModelName>("notification"))
            .expect("registry get should succeed");
        assert!(
            schema.is_none(),
            "failed model batch must not persist schema"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn web_console_command_request_invokes_session_command_processor() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let response = service
            .process_web_console_request(
                SessionRequest {
                    request: Some(proto::session_request::Request::Command(CommandRequest {
                        query: "CREATE SCHEMA web_console_event ( user_id U32 );".to_string(),
                        domain: "default".to_string(),
                    })),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        let Some(proto::session_response::Event::Result(result)) = response.event else {
            panic!("web console command should return a command result");
        };
        assert!(result.success, "expected command success: {result:?}");
        let schema = registry
            .get::<CreateSchema>(
                &DomainName::parse("default").expect("valid domain"),
                named::<ModelName>("web_console_event"),
            )
            .expect("registry get should succeed");
        assert!(schema.is_some());

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn web_console_rejects_upload_and_supports_suggest_requests() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let upload_response = service
            .process_web_console_request(
                SessionRequest {
                    request: Some(proto::session_request::Request::Command(CommandRequest {
                        query: "UPLOAD RESOURCE proto VERSION '/tmp/proto';".to_string(),
                        domain: "default".to_string(),
                    })),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        let Some(proto::session_response::Event::Result(upload_result)) = upload_response.event
        else {
            panic!("web console upload rejection should return a command result");
        };
        assert!(!upload_result.success);
        assert!(
            upload_result
                .message
                .contains("not supported in the web console")
        );

        let suggest_response = service
            .process_web_console_request(
                SessionRequest {
                    request: Some(proto::session_request::Request::Suggest(SuggestRequest {
                        input: "SHOW ".to_string(),
                        cursor: 5,
                        domain: "default".to_string(),
                    })),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        let Some(proto::session_response::Event::Suggest(suggest)) = suggest_response.event else {
            panic!("web console suggest should return a suggestion response");
        };
        assert!(
            suggest
                .suggestions
                .iter()
                .any(|suggestion| suggestion.value == "CLUSTER"),
            "expected CLUSTER suggestion, got: {:?}",
            suggest.suggestions
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn placement_member_completion_expands_all_schedulable_runtime_names() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();
        let configured = service
            .process_command(
                CommandRequest {
                    query: "BEGIN; CREATE SCHEMA placement_event ( id I64 ); CREATE RELAY \
                            plain_input SCHEMA placement_event UNBRANCHED; CREATE RELAY \
                            eligible_state SCHEMA placement_event UNBRANCHED WITH MATERIALIZED \
                            STATE LAST BY TIMESTAMP; CREATE RELAY plain_output SCHEMA \
                            placement_event UNBRANCHED; CREATE JUNCTION eligible_processor FROM \
                            plain_input UNBRANCHED TO plain_output INHERIT ALL FLUSH IMMEDIATE ON \
                            MESSAGE ERROR LOG; COMMIT;"
                        .to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(
            configured.success,
            "placement completion fixture should configure: {configured:?}"
        );

        let input = "CREATE PLACEMENT policy FROM ";
        let response = service
            .process_suggest(
                SuggestRequest {
                    input: input.to_string(),
                    cursor: u32::try_from(input.len())
                        .assured("the test suggestion input is smaller than u32::MAX bytes"),
                    domain: "default".to_string(),
                },
                &subscriptions,
            )
            .await;
        let values = response
            .suggestions
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect::<Vec<_>>();

        assert!(
            values.contains(&"eligible_processor".to_string()),
            "{values:?}"
        );
        assert!(values.contains(&"eligible_state".to_string()), "{values:?}");
        assert!(values.contains(&"plain_input".to_string()), "{values:?}");
        assert!(values.contains(&"plain_output".to_string()), "{values:?}");
        assert!(
            !values.contains(&"ref:runtime_node".to_string()),
            "{values:?}"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    async fn suggestion_values(
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

    async fn queue_in_transaction(
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

    #[tokio::test]
    async fn completion_offers_models_queued_in_the_open_transaction() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut subscriptions, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE SCHEMA queued_order ( order_id I64 );",
        )
        .await;

        let values =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(values.contains(&"queued_order".to_string()), "{values:?}");

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_hides_models_dropped_in_the_open_transaction() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "BEGIN; CREATE SCHEMA committed_order ( order_id I64 ); CREATE RELAY committed_orders \
             SCHEMA committed_order UNBRANCHED; COMMIT;",
        )
        .await;

        let committed = suggestion_values(&service, &subscriptions, "DROP RELAY ").await;
        assert!(
            committed.contains(&"committed_orders".to_string()),
            "{committed:?}"
        );

        queue_in_transaction(&service, &mut subscriptions, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "DROP RELAY committed_orders;",
        )
        .await;

        let dropped = suggestion_values(&service, &subscriptions, "DROP RELAY ").await;
        assert!(
            !dropped.contains(&"committed_orders".to_string()),
            "{dropped:?}"
        );

        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE RELAY committed_orders SCHEMA committed_order UNBRANCHED;",
        )
        .await;

        let recreated = suggestion_values(&service, &subscriptions, "DROP RELAY ").await;
        assert!(
            recreated.contains(&"committed_orders".to_string()),
            "{recreated:?}"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_keeps_queued_models_out_of_other_sessions() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut writer = SessionSubscriptions::new();
        let mut observer = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut writer, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut writer,
            &tx,
            "CREATE SCHEMA isolated_order ( order_id I64 );",
        )
        .await;

        let bound = suggestion_values(&service, &writer, "CREATE RELAY orders SCHEMA ").await;
        assert!(bound.contains(&"isolated_order".to_string()), "{bound:?}");

        let unbound = suggestion_values(&service, &observer, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            !unbound.contains(&"isolated_order".to_string()),
            "{unbound:?}"
        );

        writer.stop_all(&service).await;
        observer.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_drops_queued_models_until_a_detached_transaction_is_attached() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut subscriptions, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE SCHEMA detached_order ( order_id I64 );",
        )
        .await;
        let transaction_id = subscriptions
            .transaction_id()
            .expect("BEGIN must bind a transaction")
            .to_string();

        let bound =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(bound.contains(&"detached_order".to_string()), "{bound:?}");

        // A leadership change leaves the replicated transaction intact while the leader-local
        // binding is gone, which is what the session observes until it attaches again.
        service.inner.transaction_bindings.remove(&transaction_id);

        let detached =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            !detached.contains(&"detached_order".to_string()),
            "{detached:?}"
        );

        let attached = service
            .attach_transaction(
                proto::AttachTransactionRequest {
                    id: transaction_id.clone(),
                },
                &mut subscriptions,
            )
            .await;
        assert!(attached.success, "reattach must succeed: {attached:?}");

        let reattached =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            reattached.contains(&"detached_order".to_string()),
            "{reattached:?}"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_moves_queued_models_to_the_session_that_takes_the_transaction_over() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut first = SessionSubscriptions::new();
        let mut second = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut first, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut first,
            &tx,
            "CREATE SCHEMA takeover_order ( order_id I64 );",
        )
        .await;
        let transaction_id = first
            .transaction_id()
            .expect("BEGIN must bind a transaction")
            .to_string();

        let attached = service
            .attach_transaction(
                proto::AttachTransactionRequest {
                    id: transaction_id.clone(),
                },
                &mut second,
            )
            .await;
        assert!(attached.success, "takeover must succeed: {attached:?}");

        let displaced = suggestion_values(&service, &first, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            !displaced.contains(&"takeover_order".to_string()),
            "{displaced:?}"
        );

        let holder = suggestion_values(&service, &second, "CREATE RELAY orders SCHEMA ").await;
        assert!(holder.contains(&"takeover_order".to_string()), "{holder:?}");

        first.stop_all(&service).await;
        second.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn placement_member_completion_expands_queued_runtime_names() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut subscriptions, &tx, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE SCHEMA queued_event ( id I64 );",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE RELAY queued_state SCHEMA queued_event UNBRANCHED WITH MATERIALIZED STATE \
             LAST BY TIMESTAMP;",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            &tx,
            "CREATE RELAY queued_plain SCHEMA queued_event UNBRANCHED;",
        )
        .await;

        let values =
            suggestion_values(&service, &subscriptions, "CREATE PLACEMENT policy FROM ").await;
        assert!(values.contains(&"queued_state".to_string()), "{values:?}");
        assert!(values.contains(&"queued_plain".to_string()), "{values:?}");

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn show_placements_reports_fully_overridden_effective_coverage() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();
        let configured = service
            .process_command(
                CommandRequest {
                    query: "BEGIN; CREATE SCHEMA placement_event ( id I64 ); CREATE RELAY \
                            placement_input SCHEMA placement_event UNBRANCHED; CREATE RELAY \
                            placement_middle SCHEMA placement_event UNBRANCHED; CREATE RELAY \
                            placement_output SCHEMA placement_event UNBRANCHED; CREATE JUNCTION \
                            corridor_source FROM placement_input UNBRANCHED TO placement_middle \
                            INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG; CREATE JUNCTION \
                            corridor_sink FROM placement_middle UNBRANCHED TO placement_output \
                            INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG; CREATE PLACEMENT \
                            weak_glue FROM corridor_source TO corridor_sink REQUIRE COLOCATION \
                            RANK 2; CREATE PLACEMENT strong_cut FROM corridor_source TO \
                            corridor_sink NEUTRAL RANK 1; COMMIT;"
                        .to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(
            configured.success,
            "placement coverage fixture should configure: {configured:?}"
        );

        let output = service
            .show_placements(&DomainName::parse("default").expect("valid domain"))
            .await;
        assert!(output.success, "SHOW PLACEMENTS should succeed: {output:?}");
        assert!(
            output
                .message
                .lines()
                .any(|line| line.starts_with("weak_glue ") && line.ends_with("coverage=overridden")),
            "unexpected SHOW PLACEMENTS output: {}",
            output.message
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_preserves_detached_deduplicator_and_emitter_modes() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();
        let commands = [
            "CREATE SCHEMA notification ( user_id I64 );",
            "CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );",
            "CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA \
             notification;",
            "CREATE RELAY notifications SCHEMA notification UNBRANCHED;",
            "CREATE RELAY forwarded_notifications SCHEMA notification UNBRANCHED;",
            "CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' \
             };",
            "CREATE INGESTOR notifications_ingestor FROM KAFKA kafka_main TOPIC notifications \
             OFFSET BY CONSUMER GROUP notifications_group MODE NO_ACK PARALLEL ON QUIESCE SUSPEND \
             DECODE USING notification_codec TIMESTAMP NOW TO notifications INHERIT ALL \
             UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL \
             ERROR LOG;",
            "CREATE DETACHED DEDUPLICATOR passthrough FROM notifications DEDUPLICATE ON \
             input.user_id MAX TIME 10m UNBRANCHED TO forwarded_notifications INHERIT ALL FLUSH \
             EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
            "CREATE DETACHED EMITTER kafka_forward FROM notifications TO KAFKA kafka_main TOPIC \
             notifications_out MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING \
             notification_codec INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR \
             LOG ON GENERAL ERROR LOG;",
        ];

        for command in commands {
            let result = service
                .process_command(
                    CommandRequest {
                        query: command.to_string(),
                        domain: "default".to_string(),
                    },
                    &tx,
                    &mut subscriptions,
                )
                .await;
            assert!(
                result.success,
                "command must succeed: {command}: {}",
                result.message
            );
        }

        let deduplicator = registry
            .get::<CreateDeduplicator>(
                &DomainName::parse("default").expect("valid domain"),
                ModelName::parse("passthrough").expect("valid model name"),
            )
            .expect("registry get should succeed")
            .expect("deduplicator should exist");
        let emitter = registry
            .get::<CreateEmitter>(
                &DomainName::parse("default").expect("valid domain"),
                ModelName::parse("kafka_forward").expect("valid model name"),
            )
            .expect("registry get should succeed")
            .expect("emitter should exist");

        assert_eq!(deduplicator.mode, AckMode::Detached);
        assert_eq!(emitter.mode, AckMode::Detached);

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_creates_junction_model() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();
        for command in [
            "CREATE SCHEMA notification ( user_id I64 );",
            "CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );",
            "CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA \
             notification;",
            "CREATE RELAY notifications_a SCHEMA notification UNBRANCHED;",
            "CREATE RELAY notifications_b SCHEMA notification UNBRANCHED;",
            "CREATE RELAY notifications_all SCHEMA notification UNBRANCHED;",
            "CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' \
             };",
            "CREATE INGESTOR ingest_a FROM KAFKA kafka_main TOPIC notifications_a OFFSET BY \
             CONSUMER GROUP notifications_a_group MODE NO_ACK PARALLEL ON QUIESCE SUSPEND DECODE \
             USING notification_codec TIMESTAMP NOW TO notifications_a INHERIT ALL UNBRANCHED \
             FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            "CREATE INGESTOR ingest_b FROM KAFKA kafka_main TOPIC notifications_b OFFSET BY \
             CONSUMER GROUP notifications_b_group MODE NO_ACK PARALLEL ON QUIESCE SUSPEND DECODE \
             USING notification_codec TIMESTAMP NOW TO notifications_b INHERIT ALL UNBRANCHED \
             FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            "CREATE JUNCTION join_streams FROM notifications_a, notifications_b UNBRANCHED TO \
             notifications_all INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR \
             LOG;",
        ] {
            let result = service
                .process_command(
                    CommandRequest {
                        query: command.to_string(),
                        domain: "default".to_string(),
                    },
                    &tx,
                    &mut subscriptions,
                )
                .await;
            assert!(
                result.success,
                "command must succeed: {command}: {}",
                result.message
            );
        }

        let junction = registry
            .get::<CreateJunction>(
                &DomainName::parse("default").expect("valid domain"),
                ModelName::parse("join_streams").expect("valid model name"),
            )
            .expect("registry get should succeed")
            .expect("junction should exist");
        assert_eq!(junction.from.relays().len(), 2);
        assert_eq!(
            junction
                .output_routes
                .relays()
                .next()
                .expect("junction should declare an output")
                .as_str(),
            "notifications_all"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_creates_deduplicator_model() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();
        for command in [
            "CREATE SCHEMA transaction ( transaction_id STRING, amount I64 );",
            "CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT ( transaction_id string, amount \
             integer );",
            "CREATE CODEC transaction_codec FROM WIRE JSON SCHEMA transaction_wire TO SCHEMA \
             transaction;",
            "CREATE RELAY inbound SCHEMA transaction UNBRANCHED;",
            "CREATE RELAY deduped SCHEMA transaction UNBRANCHED;",
            "CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' \
             };",
            "CREATE INGESTOR inbound_ingestor FROM KAFKA kafka_main TOPIC inbound OFFSET BY \
             CONSUMER GROUP inbound_group MODE NO_ACK PARALLEL ON QUIESCE SUSPEND DECODE USING \
             transaction_codec TIMESTAMP NOW TO inbound INHERIT ALL UNBRANCHED FLUSH EACH 100ms \
             MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            "CREATE DEDUPLICATOR dedup_txns FROM inbound DEDUPLICATE ON input.transaction_id MAX \
             TIME 10m UNBRANCHED TO deduped INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
             MESSAGE ERROR LOG;",
        ] {
            let result = service
                .process_command(
                    CommandRequest {
                        query: command.to_string(),
                        domain: "default".to_string(),
                    },
                    &tx,
                    &mut subscriptions,
                )
                .await;
            assert!(
                result.success,
                "command must succeed: {command}: {}",
                result.message
            );
        }

        let deduplicator = registry
            .get::<CreateDeduplicator>(
                &DomainName::parse("default").expect("valid domain"),
                ModelName::parse("dedup_txns").expect("valid model name"),
            )
            .expect("registry get should succeed")
            .expect("deduplicator should exist");
        assert_eq!(
            deduplicator
                .from
                .first()
                .expect("deduplicator should declare an input")
                .as_str(),
            "inbound"
        );
        assert_eq!(
            deduplicator
                .output_routes
                .relays()
                .next()
                .expect("deduplicator should declare an output")
                .as_str(),
            "deduped"
        );
        assert_eq!(
            deduplicator.deduplicate_on,
            vec![nervix_nspl::parse_expression("input.transaction_id").expect("valid expression")]
        );
        assert_eq!(deduplicator.max_time, "10m");
        assert_eq!(deduplicator.mode, nervix_models::AckMode::Attached);

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_describes_resource_metadata() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let expected_leader = service.inner.consensus.local_node_id().clone();
        let resource_store = service.inner.resource_store.clone();
        let proposer = service.inner.consensus.clone();
        let source_v1 = path.join("resource-source-v1");
        std::fs::create_dir_all(source_v1.join("nested"))
            .expect("test resource directory should exist");
        std::fs::write(source_v1.join("alpha.txt"), "alpha")
            .expect("test resource file should write");
        std::fs::write(source_v1.join("nested").join("beta.txt"), "beta")
            .expect("test resource file should write");
        let resource_domain = DomainName::parse("default").expect("valid domain");
        let manifest_v1 = resource_store
            .install_from_directory(
                nervix_models::ResourceId::new(resource_domain.clone(), named("fraud_model"), 1),
                &source_v1,
                expected_leader.clone(),
                Timestamp::from_unix_nanos(77),
            )
            .await
            .expect("resource version should install");
        let source_v2 = path.join("resource-source-v2");
        std::fs::create_dir_all(&source_v2).expect("test resource directory should exist");
        std::fs::write(source_v2.join("model.onnx"), "model")
            .expect("test resource file should write");
        let manifest_v2 = resource_store
            .install_from_directory(
                nervix_models::ResourceId::new(resource_domain.clone(), named("fraud_model"), 2),
                &source_v2,
                expected_leader.clone(),
                Timestamp::from_unix_nanos(79),
            )
            .await
            .expect("resource version should install");
        proposer
            .create_resource_catalog(&resource_domain, &named("fraud_model"))
            .await
            .expect("resource catalog should persist");
        proposer
            .put_resource_version(manifest_v1.resource.clone())
            .await
            .expect("resource version should persist");
        proposer
            .put_resource_version(manifest_v2.resource.clone())
            .await
            .expect("resource version should persist");
        proposer
            .put_resource_replica(nervix_models::ResourceNodeStatus {
                key: nervix_models::ResourceReplicaKey::new(
                    resource_domain.clone(),
                    named("fraud_model"),
                    1,
                    expected_leader.clone(),
                ),
                state: nervix_models::ResourceNodeState::Ready,
                root_checksum: Some(manifest_v1.resource.root_checksum.clone()),
                last_verified_at: Some(Timestamp::from_unix_nanos(78)),
                source_node_id: Some(expected_leader.clone()),
                error: None,
            })
            .await
            .expect("resource replica should persist");
        proposer
            .put_domain(DomainState {
                id: DomainName::parse("default").expect("valid domain"),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    period: "0ms".to_string(),
                    skew: "0ms".to_string(),
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: DomainStatus::Stopped,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            })
            .await
            .expect("domain should persist");
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let result = service
            .process_command(
                CommandRequest {
                    query: "DESCRIBE RESOURCE fraud_model VERSION 1;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(result.success, "command must succeed: {}", result.message);
        assert!(result.message.contains("resource: fraud_model@1"));
        assert!(result.message.contains("cluster_ready: true"));
        assert!(result.message.contains(&format!(
            "- {} topology=alive state=ready checksum={}",
            expected_leader, manifest_v1.resource.root_checksum
        )));
        assert!(result.message.contains("entries:"));
        assert!(
            result
                .message
                .contains("- type=directory path=nested size=0 checksum=-")
        );
        assert!(
            result
                .message
                .contains("- type=file path=nested/beta.txt size=4 checksum=")
        );

        let result = service
            .process_command(
                CommandRequest {
                    query: "DESCRIBE RESOURCE fraud_model;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(result.success, "command must succeed: {}", result.message);
        assert!(result.message.contains("resource: fraud_model"));
        assert!(result.message.contains("versions: 1,2"));
        assert!(result.message.contains("version_details:"));
        assert!(result.message.contains("- version=1 root_checksum="));
        assert!(result.message.contains("manifest_checksum="));
        assert!(result.message.contains("file_count=2 total_bytes=9"));
        assert!(result.message.contains("- version=2 root_checksum="));
        assert!(result.message.contains("file_count=1 total_bytes=5"));
        assert!(result.message.contains("  entries:"));
        assert!(
            result
                .message
                .contains("- type=file path=alpha.txt size=5 checksum=")
        );
        assert!(
            result
                .message
                .contains("- type=file path=model.onnx size=5 checksum=")
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn parse_server_statement_accepts_junction_from_application_crate() {
        let parsed = nervix_nspl::server_statement::parse_server_statement(
            "CREATE JUNCTION join_streams FROM ss1, ss2 UNBRANCHED TO ss10 INHERIT ALL FLUSH EACH \
             100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        )
        .expect("junction statement should parse");
        let Statement::Create(model) = parsed else {
            panic!("expected create statement");
        };
        assert!(matches!(model.body.as_ref(), Model::Junction(_)));
    }

    #[test]
    fn parse_server_statement_accepts_deduplicator_from_application_crate() {
        let parsed = nervix_nspl::server_statement::parse_server_statement(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON input.transaction_id MAX TIME \
             10m UNBRANCHED TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE \
             ERROR LOG;",
        )
        .expect("deduplicator statement should parse");
        let Statement::Create(model) = parsed else {
            panic!("expected create statement");
        };
        assert!(matches!(model.body.as_ref(), Model::Deduplicator(_)));
    }

    #[test]
    fn start_domain_does_not_reconcile_runtime_in_generic_pre_dispatch() {
        let statement = Statement::StartDomain(StartDomain {
            start: DomainStartPoint::Resume,
        });
        assert!(requires_existing_domain(&statement));
        assert!(!requires_runtime_reconcile(&statement));
    }
}
