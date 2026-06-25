use std::{
    collections::BTreeSet,
    convert::Infallible,
    fs::OpenOptions,
    io,
    net::SocketAddr,
    num::NonZeroU32,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use ahash::{HashMap, HashMapExt, RandomState};
#[cfg(feature = "testing")]
use argon2::{Algorithm, Params, Version};
use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use async_tar::{Builder as AsyncTarBuilder, EntryType, Header, HeaderMode};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use blake3::Hasher;
use chrono::TimeDelta;
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{Shell, generate};
use dashmap::DashMap;
use ed25519_dalek::VerifyingKey;
use error_stack::{Report, ResultExt};
use fjall::Database;
use futures_util::{SinkExt, StreamExt, stream};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use http_body_util::{BodyExt, Empty, Full};
use hyper::{
    Method, Request as HyperRequest, Response as HyperResponse, StatusCode,
    body::{Bytes, Incoming as HyperIncoming},
    header::{
        ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
        AUTHORIZATION, CONNECTION, HOST, LOCATION, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY,
        SEC_WEBSOCKET_VERSION, UPGRADE, WWW_AUTHENTICATE,
    },
    server::conn::http1,
    service::service_fn,
    upgrade,
};
use hyper_util::rt::TokioIo;
use nervix_client_core::{
    Client as NervixClient, ConnectOptions as ClientConnectOptions,
    TlsRequirement as ClientTlsRequirement,
};
use nervix_consensus::{
    AppendEntriesRequest as RaftAppendEntriesRequest, ConsensusHandle, ConsensusSettings,
    InstallSnapshotRequest as RaftInstallSnapshotRequest, RAFT_APPEND_ENTRIES_PATH,
    RAFT_CONTENT_TYPE_CBOR, RAFT_INSTALL_SNAPSHOT_PATH, RAFT_TRANSFER_LEADER_PATH, RAFT_VOTE_PATH,
    TransferLeaderRequest as RaftTransferLeaderRequest, TypeConfig, UserCredentials,
    VoteRequest as RaftVoteRequest,
};
use nervix_dataflow_graph::{DataflowGraph, DataflowNodeStatus};
use nervix_interconnect::{
    ControlEnvelope, DataflowNodeStatusEnvelope,
    DataflowNodeStatusRequest as RemoteDataflowNodeStatusRequest,
    DataflowNodeStatusResponse as RemoteDataflowNodeStatusResponse,
    DescribeIngestorRequest as RemoteDescribeIngestorRequest,
    DescribeIngestorResponse as RemoteDescribeIngestorResponse,
    DescribeLookupRequest as RemoteDescribeLookupRequest,
    DescribeLookupResponse as RemoteDescribeLookupResponse,
    DescribeMetricsRequest as RemoteDescribeMetricsRequest,
    DescribeMetricsResponse as RemoteDescribeMetricsResponse,
    DescribeRelayRequest as RemoteDescribeRelayRequest,
    DescribeRelayResponse as RemoteDescribeRelayResponse, DomainClockStart, DomainClockStop,
    DomainTickEnvelope, Envelope, IngestorDescribeEnvelope, LocalIdentity, LookupDescribeEnvelope,
    LookupRequest as RemoteLookupRequest, LookupResponse as RemoteLookupResponse, PeerVerifier,
    StateSyncResponse as RemoteStateSyncResponse, TlsConfigBundle, Transport,
    TransportMode as InterconnectTransportMode,
};
use nervix_models::{
    CreateCorrelator, CreateDeduplicator, CreateDomain, CreateEmitter, CreateEndpoint,
    CreateInferencer, CreateIngestor, CreateLookup, CreateReingestor, CreateReorderer,
    CreateResource, CreateStatement, CreateUser, CreateWindowProcessor, DescribeCorrelator,
    DescribeDeduplicator, DescribeDomain, DescribeEmitter, DescribeEndpoint, DescribeIngestor,
    DescribeLookup, DescribeReingestor, DescribeRelay, DescribeReorderer, DescribeResource,
    DescribeWasmProcessor, DescribeWindowProcessor, Domain, DomainConfig, DomainPace,
    DomainStartPoint, DomainState, DomainStatus, DomainTick, EmitSink, IcebergCatalog, Identifier,
    IngestSource, IngestTimestampSource, KafkaOffsetMode, KafkaPartitionSchedule, LookupQuery,
    Model, ModelKind, MongoDbConflictAction, MySqlConflictAction, ParseAsType,
    PostgresConflictAction, ProcessorOutputs, ResourceNodeState, ResourceNodeStatus,
    ResourceReplicaKey, ScheduledNode, ShowRelayMaterializedState, StartDomain, Statement,
    StopDomain, SubscriptionBinding, SubscriptionDeliveryBehavior, SubscriptionLiteral, Timestamp,
    UploadResource, VhostTlsResource,
};
use nervix_nspl::{
    Token, Word,
    client_statement::{
        ClientStatement, parse_client_statements, suggest_client_statement,
        upload_resource_path_fragment,
    },
    lex,
    schema::{Diagnostic as ParseDiagnostic, ParseFromSourceError},
    window_processor::aggregate::{
        WindowAggregateDemand, WindowAggregateProgram, parse_aggregate_program,
    },
};
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
use registry::{ActiveGraph, Registry, RegistryError};
use reqwest::Client as HttpClient;
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
    sync::{Notify, broadcast, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::{Duration, interval, sleep},
};
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, handshake::derive_accept_key, protocol::Role},
};

const REMOTE_DESCRIBE_RELAY_TIMEOUT: Duration = Duration::from_secs(1);
const BACKGROUND_TASK_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);
const OBSERVABILITY_LIVEZ_PATH: &str = "/livez";
const OBSERVABILITY_READYZ_PATH: &str = "/readyz";
const OBSERVABILITY_METRICS_PATH: &str = "/metrics";
const WEB_CONSOLE_INDEX: &[u8] = include_bytes!("../crates/web-console/dist/index.html");
const WEB_CONSOLE_CSS: &[u8] = include_bytes!("../crates/web-console/dist/console.css");
const WEB_CONSOLE_ECHARTS_JS: &[u8] =
    include_bytes!("../crates/web-console/dist/echarts-5.5.1.min.js");
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
type OnnxModelMetadata = (HashMap<String, ValueType>, HashMap<String, ValueType>);
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
use typed_builder::TypedBuilder;

use crate::{
    cluster,
    memory_pressure::{MemoryPressureConfig, MemoryPressureController},
    proto,
    proto::{
        CommandRequest, CommandResult, CommandResultKind, Diagnostic, DomainEntitySnapshot,
        DomainInfo, DomainList, DomainSnapshot, ServerEvent, ServerEventLevel, SessionRequest,
        SessionResponse, SetActiveDomainRequest, SuggestRequest, SuggestResponse,
        Suggestion as ApiSuggestion, SuggestionKind, UploadResourceRequest, UploadResourceResponse,
        session_service_server::{SessionService, SessionServiceServer},
    },
    resource::{ResourceEntryType, ResourceManifestEntry, ResourceStore},
    runtime::{
        CompiledProgramWithMaterializedInterest, IngestHeaders,
        IngestorDescribe as RuntimeIngestorDescribe, KafkaIngestor, RelayMessage, RelayRecordBatch,
        RelaySubscriptionReceiver, RelaySubscriptionRecvError, Runtime, RuntimeEvent,
        RuntimeMaterializedRelaySpec, RuntimeTestHooks, RuntimeVmCompileContext,
        WebsocketSignalingSession, compile_session_filter_map_program,
        execute_filter_map_on_record, scheduled_parametrized_stream_owner_nodes,
    },
    runtime_schema,
};

const LEADER_KAFKA_PARTITION_WATCH_INTERVAL: Duration = Duration::from_secs(1);
static SESSION_SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);

struct SessionSubscription {
    domain: Domain,
    relay: Identifier,
    definition: String,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

#[derive(Clone)]
struct SubscriptionFilter {
    bindings: Vec<SubscriptionMatcher>,
}

#[derive(Clone)]
struct SubscriptionMatcher {
    field: Identifier,
    expected: runtime_schema::RuntimeValue,
}

struct SessionSubscriptions {
    subscriptions: HashMap<String, SessionSubscription>,
}

struct SessionSubscriptionTaskConfig {
    filter_map: Option<CompiledProgramWithMaterializedInterest>,
    sensitivity: nervix_vm::SchemaSensitivity,
    delivery_behavior: SubscriptionDeliveryBehavior,
    batch_sample_rate: Option<f64>,
    runtime: Arc<Runtime>,
    materialized_stream_owner_nodes: HashMap<Identifier, Option<String>>,
    receiver: RelaySubscriptionReceiver<RelayRecordBatch>,
    tx: mpsc::Sender<Result<SessionResponse, Status>>,
}

impl SessionSubscriptions {
    fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    fn insert(
        &mut self,
        domain: Domain,
        definition: String,
        relay: Identifier,
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
        let task_domain = domain.clone();
        let event_definition = definition.clone();
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
                                                    level: ServerEventLevel::Error as i32,
                                                    message: format!(
                                                        "session subscription '{}' failed to expand relay batch: {}",
                                                        event_definition, error
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
                                        Some(filter_map) => match execute_filter_map_on_record(
                                            filter_map,
                                            augment_subscription_record_with_side_inputs(
                                                message.record.clone(),
                                                &match runtime
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
                                                                    level: ServerEventLevel::Error as i32,
                                                                    message: format!(
                                                                        "session subscription '{}' failed to load materialized side inputs: {}",
                                                                        event_definition, error
                                                                    ),
                                                                },
                                                            )),
                                                        };
                                                        if tx.send(Ok(event)).await.is_err() {
                                                            break 'subscription_loop;
                                                        }
                                                        continue;
                                                    }
                                                },
                                            ),
                                            message.key.as_ref(),
                                            None,
                                            current_timestamp(),
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
                                                            level: ServerEventLevel::Error as i32,
                                                            message: format!(
                                                                "session subscription '{}' FILTER-MAP failed: {}",
                                                                event_definition, error
                                                            ),
                                                        },
                                                    )),
                                                };
                                                if tx.send(Ok(event)).await.is_err() {
                                                    break 'subscription_loop;
                                                }
                                                continue;
                                            }
                                        },
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
                                                subscription: event_definition.clone(),
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
                            Err(RelaySubscriptionRecvError::Closed) => break 'subscription_loop,
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
        });

        self.subscriptions.insert(
            definition.clone(),
            SessionSubscription {
                domain,
                relay,
                definition,
                stop_tx,
                task,
            },
        );
    }

    fn contains_domain_stream(&self, domain: &Domain, relay: &Identifier) -> bool {
        self.subscriptions
            .values()
            .any(|subscription| subscription.domain == *domain && subscription.relay == *relay)
    }

    async fn remove(&mut self, key: &str) -> Option<(Domain, Identifier, String)> {
        let subscription = self.subscriptions.remove(key)?;
        let _ = subscription.stop_tx.send(true);
        let _ = subscription.task.await;
        Some((
            subscription.domain,
            subscription.relay,
            subscription.definition,
        ))
    }

    async fn stop_all(self, service: &SessionServiceImpl) {
        for (_, subscription) in self.subscriptions {
            let _ = subscription.stop_tx.send(true);
            let _ = subscription.task.await;
            service
                .unregister_subscription_interest(&subscription.domain, &subscription.relay)
                .await;
        }
    }
}

fn augment_subscription_record_with_side_inputs(
    record: runtime_schema::RuntimeRecord,
    side_inputs: &HashMap<String, runtime_schema::RuntimeValue>,
) -> runtime_schema::RuntimeRecord {
    if side_inputs.is_empty() {
        return record;
    }
    let metadata = record.metadata().clone();
    let mut fields = record
        .to_remote()
        .fields
        .into_iter()
        .map(|field| {
            (
                field.name,
                runtime_schema::RuntimeValue::from_remote(field.value),
            )
        })
        .collect::<HashMap<_, _>>();
    for (name, value) in side_inputs {
        fields.insert(name.clone(), value.clone());
    }
    runtime_schema::RuntimeRecord::from_fields_with_metadata(fields, metadata)
}

fn format_stream_message(
    message: &RelayMessage,
    sensitivity: &nervix_vm::SchemaSensitivity,
) -> String {
    let payload = message.record.to_json_string_masking(sensitivity);
    match message.key.as_ref() {
        Some(key) => format!("key={} payload={}", key.as_str(), payload),
        None => payload,
    }
}

fn validate_subscription_bindings(
    relay: &Identifier,
    parameterization: &[Identifier],
    schema: &nervix_models::CreateSchema,
    bindings: &[SubscriptionBinding],
) -> Result<SubscriptionFilter, String> {
    if parameterization.is_empty() {
        if bindings.is_empty() {
            return Ok(SubscriptionFilter {
                bindings: Vec::new(),
            });
        }
        return Err(format!(
            "stream '{}' is not parameterized and does not accept WHERE bindings",
            relay.as_str()
        ));
    }

    if bindings.is_empty() {
        return Err(format!(
            "stream '{}' requires WHERE bindings for ({})",
            relay.as_str(),
            parameterization
                .iter()
                .map(Identifier::as_str)
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

    let expected = SortedSet::from_unsorted(parameterization.to_vec()).into_vec();
    let actual = SortedSet::from_unsorted(bound.keys().cloned().collect::<Vec<_>>()).into_vec();
    if expected != actual {
        return Err(format!(
            "subscription bindings for relay '{}' must exactly match ({})",
            relay.as_str(),
            parameterization
                .iter()
                .map(Identifier::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let mut matchers = Vec::new();
    for field in parameterization {
        let ty = fields.get(field).ok_or_else(|| {
            format!(
                "parameter field '{}' is missing from schema '{}'",
                field.as_str(),
                schema.name.as_str()
            )
        })?;
        let literal = bound
            .get(field)
            .expect("validated binding set must include parameter field");
        let expected = parse_subscription_literal(field, ty, literal)?;
        matchers.push(SubscriptionMatcher {
            field: field.clone(),
            expected,
        });
    }

    Ok(SubscriptionFilter { bindings: matchers })
}

fn branch_key_from_filter(
    parameterization: &[Identifier],
    filter: &SubscriptionFilter,
) -> Result<Option<crate::runtime::BranchKey>, String> {
    if parameterization.is_empty() {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(parameterization.len());
    for field in parameterization {
        let Some(binding) = filter
            .bindings
            .iter()
            .find(|binding| binding.field == *field)
        else {
            return Err(format!(
                "missing binding for parameter field '{}'",
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

fn session_subscription_definition(
    relay: &Identifier,
    delivery_behavior: SubscriptionDeliveryBehavior,
    batch_sample_rate: Option<&str>,
    filter_map: Option<&str>,
) -> String {
    let mut definition = format!("TO {}", relay.as_str());
    if delivery_behavior != SubscriptionDeliveryBehavior::Blocking {
        definition.push(' ');
        definition.push_str(delivery_behavior.as_ref());
    }
    if let Some(batch_sample_rate) = batch_sample_rate {
        definition.push_str(" BATCH SAMPLE RATE ");
        definition.push_str(batch_sample_rate);
    }
    if let Some(filter_map) = filter_map {
        definition.push(' ');
        definition.push_str(filter_map);
    }
    definition
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
    let draw = u64::from_le_bytes(bytes) as f64 / u64::MAX as f64;
    draw < rate
}

fn parse_subscription_literal(
    field: &Identifier,
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
        .expect("empty response must build")
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
        .expect("byte response must build")
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
        .expect("web console upload response must build")
}

fn redirect_response(location: &'static str) -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(StatusCode::PERMANENT_REDIRECT)
        .header(LOCATION, location)
        .body(Full::new(Bytes::new()))
        .expect("redirect response must build")
}

fn try_take_length_delimited_frame(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    if buffer.len() < 4 {
        return None;
    }

    let len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
    if buffer.len() < 4 + len {
        return None;
    }
    let payload = buffer[4..4 + len].to_vec();
    buffer.drain(..4 + len);
    Some(payload)
}

const RESOURCE_ARCHIVE_PATH_PREFIX: &str = "/resources/";
const RESOURCE_REPLICA_PATH: &str = "/resources/replicas";

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

fn ingest_headers_from_hyper(headers: &hyper::HeaderMap) -> IngestHeaders {
    let mut values = IngestHeaders::new();
    for (name, value) in headers {
        if let Ok(value) = value.to_str() {
            values.push((name.as_str().to_string(), value.to_string()));
        }
    }
    values
}

async fn handle_http_request(
    runtime: Arc<Runtime>,
    mut request: HyperRequest<HyperIncoming>,
) -> Result<HyperResponse<Empty<Bytes>>, Infallible> {
    let host = request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let path = request.uri().path().to_string();

    if runtime.has_websocket_endpoint(&host, &path).await {
        if !is_websocket_upgrade_request(&request) {
            return Ok(response_with_status(StatusCode::UPGRADE_REQUIRED));
        }
        let headers = ingest_headers_from_hyper(request.headers());

        let Some(sec_websocket_key) = request
            .headers()
            .get(SEC_WEBSOCKET_KEY)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
        else {
            return Ok(response_with_status(StatusCode::BAD_REQUEST));
        };

        let response = HyperResponse::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(
                SEC_WEBSOCKET_ACCEPT,
                derive_accept_key(sec_websocket_key.as_bytes()),
            )
            .body(empty_body())
            .expect("websocket upgrade response must build");

        let on_upgrade = upgrade::on(&mut request);
        tokio::spawn(async move {
            match on_upgrade.await {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    let mut websocket =
                        WebSocketStream::from_raw_socket(io, Role::Server, None).await;

                    if let Some(protocol) = runtime
                        .websocket_endpoint_signaling_protocol(&host, &path)
                        .await
                    {
                        let session = match WebsocketSignalingSession::new(protocol) {
                            Ok(session) => session,
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    host,
                                    path,
                                    "websocket signaling protocol is invalid"
                                );
                                return;
                            }
                        };
                        let buffered_payloads = match session.run(&mut websocket).await {
                            Ok(buffered_payloads) => buffered_payloads,
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    host,
                                    path,
                                    "websocket signaling failed"
                                );
                                return;
                            }
                        };
                        for payload in buffered_payloads {
                            runtime
                                .dispatch_websocket_payload(
                                    &host,
                                    &path,
                                    payload.as_slice(),
                                    headers.clone(),
                                )
                                .await;
                        }
                    }

                    while let Some(message) = futures_util::StreamExt::next(&mut websocket).await {
                        match message {
                            Ok(Message::Text(payload)) => {
                                runtime
                                    .dispatch_websocket_payload(
                                        &host,
                                        &path,
                                        payload.as_bytes(),
                                        headers.clone(),
                                    )
                                    .await;
                            }
                            Ok(Message::Binary(payload)) => {
                                runtime
                                    .dispatch_websocket_payload(
                                        &host,
                                        &path,
                                        payload.as_ref(),
                                        headers.clone(),
                                    )
                                    .await;
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
        let headers = ingest_headers_from_hyper(request.headers());

        let body = match request.body_mut().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(error) => {
                warn!(error = %error, host, path, "failed to read http request body");
                return Ok(response_with_status(StatusCode::BAD_REQUEST));
            }
        };

        runtime
            .dispatch_http_payload(&host, &path, body.as_ref(), headers)
            .await;
        return Ok(response_with_status(StatusCode::ACCEPTED));
    }

    Ok(response_with_status(StatusCode::NOT_FOUND))
}

async fn serve_http(
    runtime: Arc<Runtime>,
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
        connection_tasks.spawn(async move {
            let io = TokioIo::new(stream);
            if let Err(error) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| handle_http_request(runtime.clone(), request)),
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
    runtime: Arc<Runtime>,
    http_tls_server_config: Arc<RwLock<Option<Arc<ServerConfig>>>>,
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
                                handle_http_request(runtime.clone(), request)
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

async fn read_cbor_request_body<T: serde::de::DeserializeOwned>(
    request: &mut HyperRequest<HyperIncoming>,
    context: &str,
) -> Result<T, HyperResponse<Full<Bytes>>> {
    let body = request
        .body_mut()
        .collect()
        .await
        .map_err(|error| {
            warn!(error = %error, context, "failed to read cluster api request body");
            text_response(
                StatusCode::BAD_REQUEST,
                format!("failed to read request body: {error}"),
            )
        })?
        .to_bytes();

    decode_cbor(body.as_ref()).map_err(|error| {
        text_response(
            StatusCode::BAD_REQUEST,
            format!("invalid {context} payload: {error}"),
        )
    })
}

fn parse_resource_archive_request_path(path: &str) -> Option<(Identifier, u64)> {
    let suffix = path.strip_prefix(RESOURCE_ARCHIVE_PATH_PREFIX)?;
    let mut parts = suffix.split('/');
    let identifier = Identifier::parse(parts.next()?).ok()?;
    let version = parts.next()?.parse::<u64>().ok()?;
    if parts.next()? != "archive" || parts.next().is_some() {
        return None;
    }
    Some((identifier, version))
}

async fn handle_cluster_api_request(
    consensus: Arc<ConsensusHandle>,
    resource_store: Arc<ResourceStore>,
    mut request: HyperRequest<HyperIncoming>,
) -> Result<HyperResponse<Full<Bytes>>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, path) if parse_resource_archive_request_path(path).is_some() => {
            let (identifier, version) =
                parse_resource_archive_request_path(path).expect("path checked above");
            match resource_store.read_archive_bytes(&identifier, version) {
                Ok(bytes) => response_with_bytes(StatusCode::OK, bytes, "application/x-tar"),
                Err(_) => text_response(StatusCode::NOT_FOUND, "resource archive not found"),
            }
        }
        (&Method::POST, RESOURCE_REPLICA_PATH) => {
            match read_cbor_request_body::<ResourceNodeStatus>(&mut request, "resource_replica")
                .await
            {
                Ok(replica) => match consensus.put_resource_replica(replica).await {
                    Ok(()) => response_with_bytes(
                        StatusCode::OK,
                        Vec::<u8>::new(),
                        RAFT_CONTENT_TYPE_CBOR,
                    ),
                    Err(error) => text_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("put_resource_replica failed: {error}"),
                    ),
                },
                Err(response) => response,
            }
        }
        (&Method::POST, RAFT_APPEND_ENTRIES_PATH) => {
            match read_cbor_request_body::<RaftAppendEntriesRequest<TypeConfig>>(
                &mut request,
                "append_entries",
            )
            .await
            {
                Ok(req) => match consensus.append_entries(req).await {
                    Ok(response) => match encode_cbor(&response) {
                        Ok(body) => {
                            response_with_bytes(StatusCode::OK, body, RAFT_CONTENT_TYPE_CBOR)
                        }
                        Err(error) => text_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("append_entries encode failed: {error}"),
                        ),
                    },
                    Err(error) => text_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("append_entries failed: {error}"),
                    ),
                },
                Err(response) => response,
            }
        }
        (&Method::POST, RAFT_VOTE_PATH) => {
            match read_cbor_request_body::<RaftVoteRequest<TypeConfig>>(&mut request, "vote").await
            {
                Ok(req) => match consensus.vote(req).await {
                    Ok(response) => match encode_cbor(&response) {
                        Ok(body) => {
                            response_with_bytes(StatusCode::OK, body, RAFT_CONTENT_TYPE_CBOR)
                        }
                        Err(error) => text_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("vote encode failed: {error}"),
                        ),
                    },
                    Err(error) => text_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("vote failed: {error}"),
                    ),
                },
                Err(response) => response,
            }
        }
        (&Method::POST, RAFT_TRANSFER_LEADER_PATH) => {
            match read_cbor_request_body::<RaftTransferLeaderRequest<TypeConfig>>(
                &mut request,
                "transfer_leader",
            )
            .await
            {
                Ok(req) => match consensus.transfer_leader(req).await {
                    Ok(()) => response_with_bytes(
                        StatusCode::OK,
                        Vec::<u8>::new(),
                        RAFT_CONTENT_TYPE_CBOR,
                    ),
                    Err(error) => text_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("transfer_leader failed: {error}"),
                    ),
                },
                Err(response) => response,
            }
        }
        (&Method::POST, RAFT_INSTALL_SNAPSHOT_PATH) => {
            let mut snapshot = match consensus.begin_receiving_snapshot().await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    return Ok(text_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("begin_receiving_snapshot failed: {error}"),
                    ));
                }
            };
            let mut buffer = Vec::new();
            let mut vote = None;
            let mut meta = None;
            let mut expected_offset = 0u64;
            let mut saw_done = false;
            let mut body = request.into_body();

            let mut error_response = None;
            'stream: while let Some(frame_result) = body.frame().await {
                let frame = match frame_result {
                    Ok(frame) => frame,
                    Err(error) => {
                        error_response = Some(text_response(
                            StatusCode::BAD_REQUEST,
                            format!("failed to read snapshot relay: {error}"),
                        ));
                        break;
                    }
                };
                let Ok(data) = frame.into_data() else {
                    continue;
                };
                buffer.extend_from_slice(&data);
                while let Some(payload) = try_take_length_delimited_frame(&mut buffer) {
                    let chunk: RaftInstallSnapshotRequest<TypeConfig> = match decode_cbor(&payload)
                    {
                        Ok(chunk) => chunk,
                        Err(error) => {
                            error_response = Some(text_response(
                                StatusCode::BAD_REQUEST,
                                format!("invalid snapshot chunk: {error}"),
                            ));
                            break 'stream;
                        }
                    };

                    if saw_done {
                        error_response = Some(text_response(
                            StatusCode::BAD_REQUEST,
                            "snapshot relay received extra data after done=true",
                        ));
                        break 'stream;
                    }

                    if chunk.offset != expected_offset {
                        error_response = Some(text_response(
                            StatusCode::BAD_REQUEST,
                            format!(
                                "unexpected snapshot chunk offset {}, expected {}",
                                chunk.offset, expected_offset
                            ),
                        ));
                        break 'stream;
                    }

                    if let Some(existing_vote) = &vote
                        && existing_vote != &chunk.vote
                    {
                        error_response = Some(text_response(
                            StatusCode::BAD_REQUEST,
                            "snapshot relay vote changed mid-stream",
                        ));
                        break 'stream;
                    }
                    if let Some(existing_meta) = &meta
                        && existing_meta != &chunk.meta
                    {
                        error_response = Some(text_response(
                            StatusCode::BAD_REQUEST,
                            "snapshot relay metadata changed mid-stream",
                        ));
                        break 'stream;
                    }

                    vote = Some(chunk.vote.clone());
                    meta = Some(chunk.meta.clone());
                    if let Err(error) = std::io::Write::write_all(&mut snapshot, &chunk.data) {
                        error_response = Some(text_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to write snapshot chunk: {error}"),
                        ));
                        break 'stream;
                    }
                    expected_offset = expected_offset.saturating_add(chunk.data.len() as u64);
                    saw_done = chunk.done;
                }
            }

            if let Some(response) = error_response {
                response
            } else if !buffer.is_empty() {
                text_response(
                    StatusCode::BAD_REQUEST,
                    "snapshot relay ended with a partial frame",
                )
            } else if !saw_done {
                text_response(
                    StatusCode::BAD_REQUEST,
                    "snapshot relay ended before done=true",
                )
            } else {
                let Some(vote) = vote else {
                    return Ok(text_response(
                        StatusCode::BAD_REQUEST,
                        "snapshot relay was empty",
                    ));
                };
                let Some(meta) = meta else {
                    return Ok(text_response(
                        StatusCode::BAD_REQUEST,
                        "snapshot relay was empty",
                    ));
                };
                match consensus.install_full_snapshot(vote, meta, snapshot).await {
                    Ok(response) => match encode_cbor(&response) {
                        Ok(body) => {
                            response_with_bytes(StatusCode::OK, body, RAFT_CONTENT_TYPE_CBOR)
                        }
                        Err(error) => text_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("snapshot encode failed: {error}"),
                        ),
                    },
                    Err(error) => text_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("install_snapshot failed: {error}"),
                    ),
                }
            }
        }
        (&Method::GET, "/raft/ping") => {
            response_with_bytes(StatusCode::OK, Vec::<u8>::new(), RAFT_CONTENT_TYPE_CBOR)
        }
        (&Method::GET, _) | (&Method::POST, _) => text_response(StatusCode::NOT_FOUND, "not found"),
        _ => text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    };

    Ok(response)
}

async fn handle_observability_request(
    consensus: Arc<ConsensusHandle>,
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
    consensus: Arc<ConsensusHandle>,
    runtime: crate::runtime::Runtime,
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
        if !service.authenticate_basic_credentials(&credentials).await {
            return Ok(unauthorized_basic_response());
        }

        if !is_websocket_upgrade_request(&request) {
            return Ok(response_with_bytes(
                StatusCode::UPGRADE_REQUIRED,
                Bytes::new(),
                "text/plain; charset=utf-8",
            ));
        }

        let Some(sec_websocket_key) = request
            .headers()
            .get(SEC_WEBSOCKET_KEY)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
        else {
            return Ok(text_response(
                StatusCode::BAD_REQUEST,
                "missing websocket key",
            ));
        };

        let response = HyperResponse::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(
                SEC_WEBSOCKET_ACCEPT,
                derive_accept_key(sec_websocket_key.as_bytes()),
            )
            .body(Full::new(Bytes::new()))
            .expect("web console websocket upgrade response must build");

        let on_upgrade = upgrade::on(&mut request);
        tokio::spawn(async move {
            match on_upgrade.await {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    let mut websocket =
                        WebSocketStream::from_raw_socket(io, Role::Server, None).await;
                    let (tx, mut response_rx) = mpsc::channel(16);
                    let mut subscriptions = SessionSubscriptions::new();
                    let mut leadership_check = interval(WEB_CONSOLE_LEADERSHIP_CHECK_INTERVAL);
                    let mut graph_snapshot = interval(WEB_CONSOLE_GRAPH_SNAPSHOT_INTERVAL);
                    let mut domains_rx = service.consensus.subscribe_domains();
                    leadership_check.tick().await;
                    graph_snapshot.tick().await;
                    let mut leader_connected = false;
                    let mut active_domain = None::<Domain>;

                    loop {
                        tokio::task::consume_budget().await;
                        tokio::select! {
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
                                                                    && !send_web_console_domain_snapshot_responses(
                                                                        &mut websocket,
                                                                        &service,
                                                                        active_domain.as_ref(),
                                                                    )
                                                                    .await
                                                                {
                                                                    break;
                                                                }
                                                            }
                                                            Err(response) => {
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
                                    Ok(Message::Close(_)) => break,
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
                                    if !send_web_console_domain_snapshot_responses(
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
                                if !send_web_console_domain_snapshot_responses(
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
                                if !send_web_console_domain_snapshot_responses(
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
        if !service.authenticate_basic_credentials(&credentials).await {
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
        (&Method::GET, "/console/echarts-5.5.1.min.js") => response_with_bytes(
            StatusCode::OK,
            Bytes::from_static(WEB_CONSOLE_ECHARTS_JS),
            "text/javascript; charset=utf-8",
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

async fn send_web_console_domain_snapshot_responses<S>(
    websocket: &mut WebSocketStream<S>,
    service: &SessionServiceImpl,
    active_domain: Option<&Domain>,
) -> bool
where
    WebSocketStream<S>: SinkExt<Message> + Unpin,
{
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
            level: ServerEventLevel::Error as i32,
            message,
        })),
    }
}

fn web_console_query_param(query: Option<&str>, name: &str) -> Option<String> {
    url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
}

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
    metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(credentials_from_basic_authorization)
}

fn credentials_from_web_console_request(
    request: &HyperRequest<HyperIncoming>,
) -> Option<BasicAuthCredentials> {
    request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(credentials_from_basic_authorization)
        .or_else(|| {
            web_console_query_param(request.uri().query(), WEB_CONSOLE_AUTH_QUERY_PARAM)
                .and_then(|token| credentials_from_basic_token(&token))
        })
}

fn unauthorized_basic_response() -> HyperResponse<Full<Bytes>> {
    HyperResponse::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            WWW_AUTHENTICATE,
            format!("Basic realm=\"{BASIC_AUTH_REALM}\""),
        )
        .body(Full::new(Bytes::from_static(b"authentication failed")))
        .expect("basic authentication response must build")
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
    identifier: Identifier,
) -> Result<(TempPath, String), (StatusCode, String)> {
    let archive = tempfile::NamedTempFile::new().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create temporary upload archive".to_string(),
        )
    })?;
    let archive_path = archive.into_temp_path();
    let file = File::create(<TempPath as AsRef<Path>>::as_ref(&archive_path))
        .await
        .map_err(|_| {
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
    let mut file = File::open(<TempPath as AsRef<Path>>::as_ref(&archive_path))
        .await
        .map_err(|_| {
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
    tls_server_config: Arc<ServerConfig>,
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

async fn serve_cluster_api_http(
    consensus: Arc<ConsensusHandle>,
    resource_store: Arc<ResourceStore>,
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
                accepted.change_context(AppError::ServeClusterApi)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeClusterApi)?;
        let consensus = consensus.clone();
        let resource_store = resource_store.clone();
        connection_tasks.spawn(async move {
            let io = TokioIo::new(stream);
            if let Err(error) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| {
                        handle_cluster_api_request(
                            consensus.clone(),
                            resource_store.clone(),
                            request,
                        )
                    }),
                )
                .await
            {
                warn!(error = %error, "cluster api connection failed");
            }
        });
    }
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}

async fn serve_cluster_api_https(
    consensus: Arc<ConsensusHandle>,
    resource_store: Arc<ResourceStore>,
    cluster_api_tls_server_config: Arc<ServerConfig>,
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
                accepted.change_context(AppError::ServeClusterApi)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeClusterApi)?;
        let consensus = consensus.clone();
        let resource_store = resource_store.clone();
        let acceptor = TlsAcceptor::from(cluster_api_tls_server_config.clone());
        connection_tasks.spawn(async move {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    let io = TokioIo::new(tls_stream);
                    if let Err(error) = http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |request| {
                                handle_cluster_api_request(
                                    consensus.clone(),
                                    resource_store.clone(),
                                    request,
                                )
                            }),
                        )
                        .await
                    {
                        warn!(error = %error, "cluster api https connection failed");
                    }
                }
                Err(error) => {
                    warn!(error = %error, "cluster api tls accept failed");
                }
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
#[command(name = "nervix")]
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
    pub node_id: String,
    #[arg(long, env = "NERVIX_GRPC_ADVERTISE_ADDR")]
    pub grpc_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_CLUSTER_LISTEN_ADDR")]
    pub cluster_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_CLUSTER_ADVERTISE_ADDR")]
    pub cluster_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_CLUSTER_API_LISTEN_ADDR")]
    pub cluster_api_listen_addr: String,
    #[arg(long, env = "NERVIX_CLUSTER_API_ADVERTISE_ADDR")]
    pub cluster_api_advertise_addr: String,
    #[arg(long, env = "NERVIX_CLUSTER_API_MODE", value_enum, default_value_t = InternalTransportMode::Http)]
    pub cluster_api_mode: InternalTransportMode,
    #[arg(long, env = "NERVIX_CLUSTER_API_HTTPS_LISTEN_ADDR")]
    pub cluster_api_https_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_CLUSTER_API_HTTPS_ADVERTISE_ADDR")]
    pub cluster_api_https_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_LISTEN_ADDR")]
    pub interconnect_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_ADVERTISE_ADDR")]
    pub interconnect_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_MODE", value_enum, default_value_t = InternalTransportMode::Http)]
    pub interconnect_mode: InternalTransportMode,
    #[arg(long, env = "NERVIX_INTERCONNECT_HTTPS_LISTEN_ADDR")]
    pub interconnect_https_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_HTTPS_ADVERTISE_ADDR")]
    pub interconnect_https_advertise_addr: Option<String>,
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

type PendingClusterCommands = Arc<DashMap<u64, PendingClusterCommand, RandomState>>;

enum PendingClusterCommand {
    DescribeRelay(oneshot::Sender<Result<bool, String>>),
    DescribeIngestor(oneshot::Sender<Result<IngestorDescribeEnvelope, String>>),
    DataflowNodeStatus(oneshot::Sender<Result<DataflowNodeStatusEnvelope, String>>),
    DescribeMetrics(oneshot::Sender<Result<Vec<String>, String>>),
    DescribeLookup(oneshot::Sender<Result<LookupDescribeEnvelope, String>>),
    LookupQuery(oneshot::Sender<Result<Option<runtime_schema::DecodedRecord>, String>>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct KafkaPartitionWatcherKey {
    domain: Domain,
    ingestor: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KafkaPartitionWatcherSpec {
    domain: Domain,
    ingestor: Identifier,
    topic: String,
    instances: u64,
    client: nervix_models::CreateClientKafka,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DrainMove {
    label: String,
    promoted_replica: Option<String>,
    fallback_node: Option<String>,
}

type AuthRateLimiter = DefaultKeyedRateLimiter<String>;

#[derive(Clone)]
struct SessionServiceImpl {
    cluster: Arc<cluster::ClusterHandle>,
    consensus: Arc<ConsensusHandle>,
    registry: Arc<Registry>,
    resource_store: Arc<ResourceStore>,
    cluster_api_clients: Arc<ClusterApiClients>,
    http_tls_server_config: Arc<RwLock<Option<Arc<ServerConfig>>>>,
    runtime: Arc<Runtime>,
    replica_count: usize,
    shutdown: CancellationToken,
    events: broadcast::Sender<ServerEvent>,
    subscription_interest_counts: Arc<DashMap<(Domain, Identifier), usize, RandomState>>,
    interconnect: Arc<Transport>,
    domain_clocks: Arc<DashMap<Domain, DomainClockRuntimeState, RandomState>>,
    domain_clock_events: Arc<Notify>,
    next_cluster_command_correlation_id: Arc<AtomicU64>,
    pending_cluster_commands: PendingClusterCommands,
    service_tasks: TaskTracker,
    configured_basic_auth: Option<BasicAuthCredentials>,
    auth_rate_limiter: Arc<AuthRateLimiter>,
    failed_auth_rate_limit_keys: Arc<DashMap<String, (), RandomState>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BasicAuthCredentials {
    username: String,
    password: String,
}

#[derive(Clone)]
struct DomainClockRuntimeState {
    wall_started_at: Timestamp,
    logical_start: Timestamp,
    time_rate: String,
    next_tick_id: u64,
}

struct DownloadedResourceArchive {
    path: TempPath,
    root_checksum: String,
}

#[derive(Clone)]
struct ClusterApiClients {
    http: HttpClient,
    https: HttpClient,
}

impl ClusterApiClients {
    fn build() -> Result<Self, Report<AppError>> {
        Ok(Self {
            http: build_cluster_api_http_client(InternalTransportMode::Http)?,
            https: build_cluster_api_http_client(InternalTransportMode::Https)?,
        })
    }

    fn for_url(&self, url: &str) -> &HttpClient {
        if url.starts_with("https://") {
            &self.https
        } else {
            &self.http
        }
    }
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

    fn interconnect_transport_mode(self) -> InterconnectTransportMode {
        match self {
            Self::Http => InterconnectTransportMode::Plain,
            Self::Https => InterconnectTransportMode::Tls,
        }
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
    #[error("failed to parse cluster listen address")]
    ParseClusterListenAddress,
    #[error("failed to parse cluster advertise address")]
    ParseClusterAddress,
    #[error("failed to parse cluster api listen address")]
    ParseClusterApiListenAddress,
    #[error("failed to bind cluster api listen address")]
    BindClusterApiListenAddress,
    #[error("failed to parse cluster api advertise address")]
    ParseClusterApiAdvertiseAddress,
    #[error("failed to parse cluster api https listen address")]
    ParseClusterApiHttpsListenAddress,
    #[error("failed to bind cluster api https listen address")]
    BindClusterApiHttpsListenAddress,
    #[error("failed to parse cluster api https advertise address")]
    ParseClusterApiHttpsAdvertiseAddress,
    #[error("failed to parse interconnect listen address")]
    ParseInterconnectListenAddress,
    #[error("failed to parse interconnect advertise address")]
    ParseInterconnectAdvertiseAddress,
    #[error("failed to parse interconnect https listen address")]
    ParseInterconnectHttpsListenAddress,
    #[error("failed to parse interconnect https advertise address")]
    ParseInterconnectHttpsAdvertiseAddress,
    #[error("failed to derive cluster address from server address")]
    DeriveClusterAddress,
    #[error("failed to derive interconnect address from cluster api address")]
    DeriveInterconnectAddress,
    #[error("cluster api https mode requires an https listen address")]
    MissingClusterApiHttpsListenAddress,
    #[error("cluster api https mode requires an https advertise address")]
    MissingClusterApiHttpsAdvertiseAddress,
    #[error("gRPC https mode requires an https listen address")]
    MissingGrpcHttpsListenAddress,
    #[error("gRPC https mode requires an https advertise address")]
    MissingGrpcHttpsAdvertiseAddress,
    #[error("interconnect https mode requires an https listen address")]
    MissingInterconnectHttpsListenAddress,
    #[error("interconnect https mode requires an https advertise address")]
    MissingInterconnectHttpsAdvertiseAddress,
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
    #[error("failed to apply startup runtime changes: {0}")]
    ApplyStartupRuntime(String),
    #[error("failed to open runtime state store")]
    OpenRuntimeState,
    #[error("failed to load interconnect tls configuration")]
    LoadInterconnectTls,
    #[error("failed to load cluster api tls configuration")]
    LoadClusterApiTls,
    #[error("failed to load gRPC tls configuration")]
    LoadGrpcTls,
    #[error("failed to load web console tls configuration")]
    LoadWebConsoleTls,
    #[error("failed to start interconnect transport")]
    StartInterconnect,
    #[error("failed to start cluster membership")]
    StartCluster,
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
    #[error("cluster api server failed")]
    ServeClusterApi,
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
    pub node_id: String,
    pub grpc_advertise_addr: cluster::HostPort,
    pub cluster_listen_addr: SocketAddr,
    pub cluster_advertise_addr: cluster::HostPort,
    #[builder(default = InternalTransportMode::Http)]
    pub cluster_api_mode: InternalTransportMode,
    pub cluster_api_listen_addr: SocketAddr,
    pub cluster_api_advertise_addr: cluster::HostPort,
    #[builder(default)]
    pub cluster_api_https_listen_addr: Option<SocketAddr>,
    #[builder(default)]
    pub cluster_api_https_advertise_addr: Option<cluster::HostPort>,
    #[builder(default = InternalTransportMode::Http)]
    pub interconnect_mode: InternalTransportMode,
    pub interconnect_listen_addr: SocketAddr,
    pub interconnect_advertise_addr: cluster::HostPort,
    #[builder(default)]
    pub interconnect_https_listen_addr: Option<SocketAddr>,
    #[builder(default)]
    pub interconnect_https_advertise_addr: Option<cluster::HostPort>,
    pub allow_bootstrap: bool,
    #[builder(default = DEFAULT_USER.to_string())]
    pub default_user: String,
    #[builder(default)]
    pub init_default_user_password: Option<String>,
    pub node_unavailability_timeout: Duration,
    pub raft_heartbeat_interval: Duration,
    pub raft_election_timeout_min: Duration,
    pub raft_election_timeout_max: Duration,
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
    pub runtime_test_hooks: RuntimeTestHooks,
    #[builder(default=CancellationToken::new())]
    pub shutdown: CancellationToken,
    #[builder(default = true)]
    pub graceful_shutdown_drain: bool,
    #[builder(default = DEFAULT_DRAIN_TIMEOUT)]
    pub drain_timeout: Duration,
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
        let cluster_listen_addr = match args.cluster_listen_addr.as_deref() {
            Some(addr) => addr
                .parse::<SocketAddr>()
                .change_context(AppError::ParseClusterListenAddress)?,
            None => cluster::derive_peer_addr(addr)
                .ok_or_else(|| Report::new(AppError::DeriveClusterAddress))?,
        };
        let cluster_advertise_addr = match args.cluster_advertise_addr.as_deref() {
            Some(addr) => addr.parse::<cluster::HostPort>().map_err(|error| {
                Report::new(AppError::ParseClusterAddress).attach_printable(error)
            })?,
            None => cluster_listen_addr.into(),
        };
        let cluster_api_listen_addr = args
            .cluster_api_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseClusterApiListenAddress)?;
        let cluster_api_advertise_addr = args
            .cluster_api_advertise_addr
            .parse::<cluster::HostPort>()
            .map_err(|error| {
                Report::new(AppError::ParseClusterApiAdvertiseAddress).attach_printable(error)
            })?;
        let cluster_api_https_listen_addr = args
            .cluster_api_https_listen_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<SocketAddr>()
                    .change_context(AppError::ParseClusterApiHttpsListenAddress)
            })
            .transpose()?;
        let cluster_api_https_advertise_addr = args
            .cluster_api_https_advertise_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<cluster::HostPort>().map_err(|error| {
                    Report::new(AppError::ParseClusterApiHttpsAdvertiseAddress)
                        .attach_printable(error)
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
            None => cluster::derive_interconnect_addr(cluster_api_listen_addr)
                .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?,
        };
        let interconnect_advertise_addr = match args.interconnect_advertise_addr.as_deref() {
            Some(addr) => addr.parse::<cluster::HostPort>().map_err(|error| {
                Report::new(AppError::ParseInterconnectAdvertiseAddress).attach_printable(error)
            })?,
            None => cluster::derive_interconnect_host_port(&cluster_api_advertise_addr)
                .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?,
        };
        let interconnect_https_listen_addr = match args.interconnect_https_listen_addr.as_deref() {
            Some(addr) => Some(
                addr.parse::<SocketAddr>()
                    .change_context(AppError::ParseInterconnectHttpsListenAddress)?,
            ),
            None => match cluster_api_https_listen_addr {
                Some(addr) => Some(
                    cluster::derive_interconnect_addr(addr)
                        .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?,
                ),
                None => None,
            },
        };
        let interconnect_https_advertise_addr =
            match args.interconnect_https_advertise_addr.as_deref() {
                Some(addr) => Some(addr.parse::<cluster::HostPort>().map_err(|error| {
                    Report::new(AppError::ParseInterconnectHttpsAdvertiseAddress)
                        .attach_printable(error)
                })?),
                None => match cluster_api_https_advertise_addr {
                    Some(ref addr) => Some(
                        cluster::derive_interconnect_host_port(addr)
                            .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?,
                    ),
                    None => None,
                },
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
            .cluster_listen_addr(cluster_listen_addr)
            .cluster_advertise_addr(cluster_advertise_addr)
            .cluster_api_mode(args.cluster_api_mode)
            .cluster_api_listen_addr(cluster_api_listen_addr)
            .cluster_api_advertise_addr(cluster_api_advertise_addr)
            .cluster_api_https_listen_addr(cluster_api_https_listen_addr)
            .cluster_api_https_advertise_addr(cluster_api_https_advertise_addr)
            .interconnect_mode(args.interconnect_mode)
            .interconnect_listen_addr(interconnect_listen_addr)
            .interconnect_advertise_addr(interconnect_advertise_addr)
            .interconnect_https_listen_addr(interconnect_https_listen_addr)
            .interconnect_https_advertise_addr(interconnect_https_advertise_addr)
            .allow_bootstrap(args.allow_bootstrap)
            .default_user(args.default_user)
            .init_default_user_password(args.init_default_user_password)
            .node_unavailability_timeout(args.node_unavailability_timeout)
            .raft_heartbeat_interval(args.raft_heartbeat_interval)
            .raft_election_timeout_min(args.raft_election_timeout_min)
            .raft_election_timeout_max(args.raft_election_timeout_max)
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
        self.authenticate_grpc_metadata(request.metadata()).await?;
        let mut inbound = request.into_inner();
        let service = self.clone();
        let (tx, rx) = mpsc::channel(16);
        let mut event_rx = self.events.subscribe();
        let mut runtime_event_rx = self.runtime.subscribe_events();

        let service_tasks = service.service_tasks.clone();
        service_tasks.spawn(async move {
            let mut subscriptions = SessionSubscriptions::new();
            let shutdown = service.shutdown.clone();
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        break;
                    }
                    inbound_request = tokio_stream::StreamExt::next(&mut inbound) => {
                        let Some(request) = inbound_request else {
                            break;
                        };
                        let request = match request {
                            Ok(request) => request,
                            Err(status) => {
                                let _ = tx.send(Err(status)).await;
                                subscriptions.stop_all(&service).await;
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
                                    return;
                                }
                            }
                            Some(proto::session_request::Request::ListDomains(_)) => {
                                let event = service.domain_list_response(true).await;
                                if tx.send(Ok(event)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    return;
                                }
                            }
                            Some(proto::session_request::Request::SetActiveDomain(_)) => {
                                let _ = tx
                                    .send(Err(Status::invalid_argument(
                                        "active domain selection is only supported by the web console websocket",
                                    )))
                                    .await;
                                subscriptions.stop_all(&service).await;
                                return;
                            }
                            None => {
                                let _ = tx
                                    .send(Err(Status::invalid_argument(
                                        "session request payload is missing",
                                    )))
                                    .await;
                                subscriptions.stop_all(&service).await;
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
                                    return;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {}
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    runtime_event = runtime_event_rx.recv() => {
                        match runtime_event {
                            Ok(RuntimeEvent::Error(message)) => {
                                let response = SessionResponse {
                                    event: Some(proto::session_response::Event::Server(ServerEvent {
                                        level: ServerEventLevel::Error as i32,
                                        message,
                                    })),
                                };
                                if tx.send(Ok(response)).await.is_err() {
                                    subscriptions.stop_all(&service).await;
                                    return;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {}
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }

            subscriptions.stop_all(&service).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn upload_resource(
        &self,
        request: Request<tonic::Streaming<UploadResourceRequest>>,
    ) -> Result<Response<UploadResourceResponse>, Status> {
        self.authenticate_grpc_metadata(request.metadata()).await?;
        let leader = self.consensus.current_leader().await;
        if leader.as_deref() != Some(self.consensus.local_node_id()) {
            let leader_grpc_uri = match leader.as_deref() {
                Some(leader_id) => self
                    .cluster
                    .gossip_state()
                    .await
                    .live_nodes
                    .into_iter()
                    .find(|node| node.node_id == leader_id)
                    .and_then(|node| grpc_uri_from_advertise_addr(&node.grpc_advertise_addr))
                    .unwrap_or_default(),
                None => String::new(),
            };
            return Ok(Response::new(UploadResourceResponse {
                success: false,
                message: "resource uploads must be sent to the cluster leader".to_string(),
                version: 0,
                diagnostics: Vec::new(),
                kind: CommandResultKind::NotLeader as i32,
                leader: leader.unwrap_or_default(),
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
        let identifier = Identifier::parse(&start.name)
            .map_err(|_| Status::invalid_argument("upload resource name is invalid"))?;

        let resources = self.consensus.current_resources().await;
        if !resources
            .next_version_by_identifier
            .iter()
            .any(|(known_identifier, _)| known_identifier == &identifier)
        {
            return Ok(Response::new(UploadResourceResponse {
                success: false,
                message: format!("resource '{}' does not exist", identifier.as_str()),
                version: 0,
                diagnostics: Vec::new(),
                kind: CommandResultKind::Error as i32,
                leader: String::new(),
                leader_grpc_uri: String::new(),
            }));
        }

        let temp_archive = tempfile::NamedTempFile::new()
            .map_err(|_| Status::internal("failed to create temporary upload archive"))?;
        let temp_path = temp_archive.into_temp_path();
        let mut file = File::create(<TempPath as AsRef<std::path::Path>>::as_ref(&temp_path))
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
            total_received = total_received.saturating_add(u64::try_from(chunk.len()).unwrap_or(0));
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
                kind: CommandResultKind::Error as i32,
                leader: String::new(),
                leader_grpc_uri: String::new(),
            }));
        }

        let root_checksum = {
            let hash = hasher.finalize();
            encode_hex(hash.as_bytes())
        };
        match self
            .install_uploaded_resource_archive(
                identifier,
                <TempPath as AsRef<std::path::Path>>::as_ref(&temp_path),
                root_checksum,
            )
            .await
        {
            Ok(version) => Ok(Response::new(UploadResourceResponse {
                success: true,
                message: format!("uploaded resource version {version}"),
                version,
                diagnostics: Vec::new(),
                kind: CommandResultKind::Ok as i32,
                leader: String::new(),
                leader_grpc_uri: String::new(),
            })),
            Err(message) => {
                let leader = self.consensus.current_leader().await;
                let leader_grpc_uri = match leader.as_deref() {
                    Some(leader_id) if leader_id != self.consensus.local_node_id() => self
                        .cluster
                        .gossip_state()
                        .await
                        .live_nodes
                        .into_iter()
                        .find(|node| node.node_id == leader_id)
                        .and_then(|node| grpc_uri_from_advertise_addr(&node.grpc_advertise_addr))
                        .unwrap_or_default(),
                    _ => String::new(),
                };
                let kind = if leader.as_deref() != Some(self.consensus.local_node_id()) {
                    CommandResultKind::NotLeader as i32
                } else {
                    CommandResultKind::Error as i32
                };
                Ok(Response::new(UploadResourceResponse {
                    success: false,
                    message,
                    version: 0,
                    diagnostics: Vec::new(),
                    kind,
                    leader: leader.unwrap_or_default(),
                    leader_grpc_uri,
                }))
            }
        }
    }
}

impl SessionServiceImpl {
    fn new_auth_rate_limiter() -> Arc<AuthRateLimiter> {
        let quota = Quota::per_second(
            NonZeroU32::new(AUTH_RATE_LIMIT_PER_SECOND).expect("auth rate limit must be positive"),
        );
        Arc::new(RateLimiter::keyed(quota))
    }

    async fn authenticate_grpc_metadata(&self, metadata: &MetadataMap) -> Result<(), Status> {
        let Some(credentials) = credentials_from_metadata(metadata) else {
            return Err(Status::unauthenticated("authentication required"));
        };
        if self.authenticate_basic_credentials(&credentials).await {
            Ok(())
        } else {
            Err(Status::unauthenticated("authentication failed"))
        }
    }

    async fn authenticate_basic_credentials(&self, credentials: &BasicAuthCredentials) -> bool {
        let Ok(user_name) = Identifier::parse(&credentials.username) else {
            return false;
        };
        let Some(user) = self.consensus.current_user(&user_name).await else {
            return false;
        };
        let auth_rate_limit_key = user_name.as_str().to_string();
        if self
            .failed_auth_rate_limit_keys
            .contains_key(&auth_rate_limit_key)
        {
            self.auth_rate_limiter
                .until_key_ready(&auth_rate_limit_key)
                .await;
        }
        let verified = verify_password_hash(user.password_hash, credentials.password.clone()).await;
        if verified {
            self.failed_auth_rate_limit_keys
                .remove(&auth_rate_limit_key);
        } else {
            self.failed_auth_rate_limit_keys
                .insert(auth_rate_limit_key, ());
        }
        verified
    }

    fn next_cluster_command_correlation_id(&self) -> u64 {
        self.next_cluster_command_correlation_id
            .fetch_add(1, Ordering::Relaxed)
    }

    fn broadcast_error(&self, message: impl Into<String>) {
        let _ = self.events.send(ServerEvent {
            level: ServerEventLevel::Error as i32,
            message: message.into(),
        });
    }

    async fn validate_vhost_tls_binding(&self, tls: &VhostTlsResource) -> Result<(), String> {
        let resources = self.consensus.current_resources().await;
        let version = resolve_vhost_tls_resource_version(&resources, tls)?;
        load_vhost_tls_materials(&self.resource_store, &tls.resource, version).await?;
        Ok(())
    }

    async fn validate_lookup_binding(&self, lookup: &CreateLookup) -> Result<(), String> {
        let resources = self.consensus.current_resources().await;
        let version = resolve_latest_resource_version(&resources, &lookup.resource)?;
        let path = self
            .resource_store
            .resolve_content_path(&lookup.resource, version, &lookup.path)
            .map_err(|error| error.to_string())?;
        ensure_file_exists(&path, "lookup").await
    }

    async fn validate_inferencer_binding(
        &self,
        domain: &Domain,
        processor: &CreateInferencer,
    ) -> Result<(), String> {
        let resources = self.consensus.current_resources().await;
        let version =
            resolve_resource_version(&resources, &processor.resource, processor.resource_version)?;
        let path = self
            .resource_store
            .resolve_content_path(&processor.resource, version, &processor.file)
            .map_err(|error| error.to_string())?;
        ensure_file_exists(&path, "ONNX model").await?;
        if path.extension().and_then(|extension| extension.to_str()) != Some("onnx") {
            return Err(format!(
                "model file '{}' must have .onnx extension",
                processor.file
            ));
        }
        self.validate_inferencer_model_metadata(domain, processor, &path)
            .await?;
        Ok(())
    }

    async fn validate_inferencer_model_metadata(
        &self,
        domain: &Domain,
        processor: &CreateInferencer,
        path: &std::path::Path,
    ) -> Result<(), String> {
        let path = path.to_path_buf();
        let processor_name = processor.name.as_str().to_string();
        let processor_file = processor.file.clone();
        let (model_inputs, model_outputs) = tokio::time::timeout(
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

        for mapping in &processor.inputs {
            if mapping.relay != processor.from_relay {
                return Err(format!(
                    "inferencer '{}' INPUTS tensor '{}' must map from relay '{}', got '{}'",
                    processor.name.as_str(),
                    mapping.tensor,
                    processor.from_relay.as_str(),
                    mapping.relay.as_str()
                ));
            }
            let Some(model_type) = model_inputs.get(&mapping.tensor) else {
                return Err(format!(
                    "inferencer '{}' missing ONNX input tensor '{}'",
                    processor.name.as_str(),
                    mapping.tensor
                ));
            };
            let field_type = self
                .relay_field_type(domain, &mapping.relay, &mapping.field)
                .await?;
            validate_inferencer_tensor_type(
                processor,
                "input",
                &mapping.tensor,
                &mapping.relay,
                &mapping.field,
                &field_type,
                model_type,
            )?;
        }

        for mapping in &processor.outputs {
            if !processor
                .output_routes
                .relays()
                .any(|relay| relay == &mapping.relay)
            {
                return Err(format!(
                    "inferencer '{}' OUTPUTS tensor '{}' must map into a declared output relay, \
                     got '{}'",
                    processor.name.as_str(),
                    mapping.tensor,
                    mapping.relay.as_str()
                ));
            }
            let Some(model_type) = model_outputs.get(&mapping.tensor) else {
                return Err(format!(
                    "inferencer '{}' missing ONNX output tensor '{}'",
                    processor.name.as_str(),
                    mapping.tensor
                ));
            };
            let field_type = self
                .relay_field_type(domain, &mapping.relay, &mapping.field)
                .await?;
            validate_inferencer_tensor_type(
                processor,
                "output",
                &mapping.tensor,
                &mapping.relay,
                &mapping.field,
                &field_type,
                model_type,
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
            .map(|input| (input.name().to_string(), input.dtype().clone()))
            .collect::<HashMap<_, _>>();
        let model_outputs = session
            .outputs()
            .iter()
            .map(|output| (output.name().to_string(), output.dtype().clone()))
            .collect::<HashMap<_, _>>();
        Ok((model_inputs, model_outputs))
    }

    async fn relay_field_type(
        &self,
        domain: &Domain,
        relay: &Identifier,
        field: &Identifier,
    ) -> Result<ParseAsType, String> {
        let schema = self
            .subscription_stream_schema(domain, relay)
            .await?
            .ok_or_else(|| format!("stream '{}' does not exist", relay.as_str()))?;
        schema
            .fields
            .iter()
            .find(|candidate| candidate.name == *field)
            .map(|field| field.ty.clone())
            .ok_or_else(|| {
                format!(
                    "field '{}.{}' does not exist",
                    relay.as_str(),
                    field.as_str()
                )
            })
    }

    async fn refresh_http_tls_server_config(&self) -> Result<(), String> {
        nervix_interconnect::install_rustls_crypto_provider();
        let resources = self.consensus.current_resources().await;
        let domains = self.consensus.current_domains().await;
        let mut resolver = ResolvesServerCertUsingSni::new();
        let mut configured_tls = false;

        for domain_id in domains.keys() {
            tokio::task::consume_budget().await;
            let Ok(vhost_ids) = self
                .registry
                .list_identifiers(domain_id, ModelKind::Vhost, "")
            else {
                continue;
            };

            for vhost_id in vhost_ids {
                tokio::task::consume_budget().await;
                let Ok(Some(Model::Vhost(vhost))) =
                    self.registry.get(domain_id, ModelKind::Vhost, &vhost_id)
                else {
                    continue;
                };
                let Some(tls) = vhost.tls.as_ref() else {
                    continue;
                };

                let version = match resolve_vhost_tls_resource_version(&resources, tls) {
                    Ok(version) => version,
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
                let certified_key =
                    match load_vhost_tls_materials(&self.resource_store, &tls.resource, version)
                        .await
                    {
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

        let mut guard = self.http_tls_server_config.write();
        if configured_tls {
            let config = ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(resolver));
            *guard = Some(Arc::new(config));
        } else {
            *guard = None;
        }
        Ok(())
    }

    async fn publish_resource_replica(&self, replica: ResourceNodeStatus) -> Result<(), String> {
        let Some(leader_id) = self.consensus.current_leader().await else {
            return Err(
                "failed to publish resource replica: cluster leader is unknown".to_string(),
            );
        };

        if leader_id == self.consensus.local_node_id() {
            return self
                .consensus
                .put_resource_replica(replica)
                .await
                .map_err(|error| format!("failed to publish resource replica: {error}"));
        }

        let gossip = self.cluster.gossip_state().await;
        let Some(leader_node) = gossip
            .live_nodes
            .into_iter()
            .find(|node| node.node_id == leader_id)
        else {
            return Err(format!(
                "failed to publish resource replica: leader node '{}' is not available",
                leader_id
            ));
        };

        post_resource_replica(
            self.cluster_api_clients.as_ref(),
            &leader_node.cluster_api_advertise_addr,
            &replica,
        )
        .await
    }

    async fn reconcile_resources_once(&self) {
        let local_node_id = self.consensus.local_node_id().to_string();
        let resources = self.consensus.current_resources().await;
        let live_nodes = self.cluster.gossip_state().await.live_nodes;

        for resource in resources.versions.iter().cloned() {
            tokio::task::consume_budget().await;

            let local_replica = resources
                .replicas
                .iter()
                .find(|replica| {
                    replica.key.identifier == resource.id.identifier
                        && replica.key.version == resource.id.version
                        && replica.key.node_id == local_node_id
                })
                .cloned();
            if local_replica.as_ref().is_some_and(|replica| {
                replica.state == ResourceNodeState::Ready
                    && replica.root_checksum.as_deref() == Some(resource.root_checksum.as_str())
            }) {
                continue;
            }

            let Some(source_node) = live_nodes.iter().find(|node| {
                node.node_id != local_node_id
                    && resources.replicas.iter().any(|replica| {
                        replica.key.identifier == resource.id.identifier
                            && replica.key.version == resource.id.version
                            && replica.key.node_id == node.node_id
                            && replica.state == ResourceNodeState::Ready
                            && replica.root_checksum.as_deref()
                                == Some(resource.root_checksum.as_str())
                    })
            }) else {
                continue;
            };

            let archive = match fetch_resource_archive(
                self.cluster_api_clients.as_ref(),
                &source_node.cluster_api_advertise_addr,
                &resource.id.identifier,
                resource.id.version,
            )
            .await
            {
                Ok(archive) => archive,
                Err(error) => {
                    if let Err(publish_error) = self
                        .publish_resource_replica(ResourceNodeStatus {
                            key: ResourceReplicaKey::new(
                                resource.id.identifier.clone(),
                                resource.id.version,
                                local_node_id.clone(),
                            ),
                            state: ResourceNodeState::Failed,
                            root_checksum: None,
                            last_verified_at: None,
                            source_node_id: Some(source_node.node_id.clone()),
                            error: Some(error),
                        })
                        .await
                    {
                        self.broadcast_error(publish_error);
                    }
                    continue;
                }
            };

            if archive.root_checksum != resource.root_checksum {
                if let Err(error) = self
                    .publish_resource_replica(ResourceNodeStatus {
                        key: ResourceReplicaKey::new(
                            resource.id.identifier.clone(),
                            resource.id.version,
                            local_node_id.clone(),
                        ),
                        state: ResourceNodeState::Failed,
                        root_checksum: Some(archive.root_checksum.clone()),
                        last_verified_at: None,
                        source_node_id: Some(source_node.node_id.clone()),
                        error: Some(format!(
                            "resource checksum mismatch: expected {}, got {}",
                            resource.root_checksum, archive.root_checksum
                        )),
                    })
                    .await
                {
                    self.broadcast_error(error);
                }
                continue;
            }

            let manifest = match self
                .resource_store
                .install_from_archive_path(
                    resource.id.identifier.clone(),
                    resource.id.version,
                    <TempPath as AsRef<std::path::Path>>::as_ref(&archive.path),
                    archive.root_checksum.clone(),
                    resource.created_by_node.clone(),
                    resource.created_at,
                )
                .await
            {
                Ok(manifest) => manifest,
                Err(error) => {
                    if let Err(publish_error) = self
                        .publish_resource_replica(ResourceNodeStatus {
                            key: ResourceReplicaKey::new(
                                resource.id.identifier.clone(),
                                resource.id.version,
                                local_node_id.clone(),
                            ),
                            state: ResourceNodeState::Failed,
                            root_checksum: None,
                            last_verified_at: None,
                            source_node_id: Some(source_node.node_id.clone()),
                            error: Some(error.to_string()),
                        })
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
                    .publish_resource_replica(ResourceNodeStatus {
                        key: ResourceReplicaKey::new(
                            resource.id.identifier.clone(),
                            resource.id.version,
                            local_node_id.clone(),
                        ),
                        state: ResourceNodeState::Failed,
                        root_checksum: Some(actual_checksum.clone()),
                        last_verified_at: None,
                        source_node_id: Some(source_node.node_id.clone()),
                        error: Some(format!(
                            "resource checksum mismatch: expected {}, got {}",
                            resource.root_checksum, actual_checksum
                        )),
                    })
                    .await
                {
                    self.broadcast_error(error);
                }
                continue;
            }

            if let Err(error) = self
                .publish_resource_replica(ResourceNodeStatus {
                    key: ResourceReplicaKey::new(
                        resource.id.identifier.clone(),
                        resource.id.version,
                        local_node_id.clone(),
                    ),
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

    async fn register_subscription_interest(&self, domain: &Domain, relay: &Identifier) {
        let key = (domain.clone(), relay.clone());
        let mut entry = self.subscription_interest_counts.entry(key).or_insert(0);
        *entry += 1;
        if *entry == 1 {
            self.cluster
                .set_local_subscription_interest(domain.as_str(), relay.as_str(), true)
                .await;
        }
    }

    async fn unregister_subscription_interest(&self, domain: &Domain, relay: &Identifier) {
        let key = (domain.clone(), relay.clone());
        let mut should_clear = false;
        if let Some(mut entry) = self.subscription_interest_counts.get_mut(&key) {
            if *entry <= 1 {
                should_clear = true;
            } else {
                *entry -= 1;
            }
        }
        if should_clear {
            self.subscription_interest_counts.remove(&key);
            self.cluster
                .set_local_subscription_interest(domain.as_str(), relay.as_str(), false)
                .await;
        }
    }

    async fn scheduled_stream_owner_nodes(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<Vec<String>, String> {
        let schedule = self.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(Vec::new());
        };
        Ok(scheduled_parametrized_stream_owner_nodes(
            domain_schedule,
            relay,
        ))
    }

    async fn dispatch_interconnect_control(
        &self,
        node_id: &str,
        envelope: ControlEnvelope,
    ) -> Result<(), String> {
        let target = self
            .cluster
            .gossip_state()
            .await
            .live_nodes
            .into_iter()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| format!("node '{}' is not live", node_id))?;
        let addr = target
            .interconnect_advertise_addr
            .parse::<SocketAddr>()
            .map_err(|error| format!("invalid interconnect address for '{}': {error}", node_id))?;
        let mode = match target.interconnect_mode.as_str() {
            "https" => InterconnectTransportMode::Tls,
            _ => InterconnectTransportMode::Plain,
        };
        let connection = self
            .interconnect
            .connection_for(addr, "localhost", mode)
            .await
            .map_err(|error| {
                format!("failed to connect interconnect for '{}': {error}", node_id)
            })?;
        connection
            .send(Envelope::Control(envelope))
            .await
            .map_err(|error| {
                format!(
                    "failed to send interconnect control to '{}': {error}",
                    node_id
                )
            })
    }

    async fn start_domain_clock(
        &self,
        domain_id: Domain,
        wall_started_at: Timestamp,
        logical_start: Timestamp,
        time_rate: String,
    ) -> Result<(), String> {
        let owner = self.domain_clock_owner(&domain_id).await.ok_or_else(|| {
            format!(
                "no live node available to own domain '{}'",
                domain_id.as_str()
            )
        })?;
        let start = DomainClockStart {
            domain_id: domain_id.clone(),
            wall_started_at,
            logical_start,
            time_rate,
        };
        if owner == self.consensus.local_node_id() {
            self.handle_domain_clock_start(start);
            Ok(())
        } else {
            self.dispatch_interconnect_control(&owner, ControlEnvelope::DomainClockStart(start))
                .await
        }
    }

    async fn stop_domain_clock(&self, domain_id: &Domain) -> Result<(), String> {
        let owner = self.domain_clock_owner(domain_id).await.ok_or_else(|| {
            format!(
                "no live node available to own domain '{}'",
                domain_id.as_str()
            )
        })?;
        if owner == self.consensus.local_node_id() {
            self.handle_domain_clock_stop(DomainClockStop {
                domain_id: domain_id.clone(),
            });
            Ok(())
        } else {
            self.dispatch_interconnect_control(
                &owner,
                ControlEnvelope::DomainClockStop(DomainClockStop {
                    domain_id: domain_id.clone(),
                }),
            )
            .await
        }
    }

    fn handle_domain_clock_start(&self, start: DomainClockStart) {
        let domain_id = start.domain_id.clone();
        self.runtime.handle_domain_clock_start(
            &domain_id,
            start.logical_start,
            start.wall_started_at,
            &start.time_rate,
        );
        self.domain_clocks.insert(
            domain_id.clone(),
            DomainClockRuntimeState {
                wall_started_at: start.wall_started_at,
                logical_start: start.logical_start,
                time_rate: start.time_rate,
                next_tick_id: 1,
            },
        );
        let service = self.clone();
        self.service_tasks.spawn(async move {
            emit_due_domain_ticks(&service, &domain_id).await;
        });
        self.domain_clock_events.notify_waiters();
    }

    fn handle_domain_clock_stop(&self, stop: DomainClockStop) {
        self.runtime.handle_domain_clock_stop(&stop.domain_id);
        self.domain_clocks.remove(&stop.domain_id);
        self.domain_clock_events.notify_waiters();
    }

    fn handle_domain_tick(&self, tick: DomainTickEnvelope) {
        self.runtime.handle_domain_tick(&tick.domain_id, &tick.tick);
    }

    async fn domain_clock_owner(&self, domain_id: &Domain) -> Option<String> {
        let mut live_nodes = self.cluster.live_node_ids().await;
        live_nodes.sort();
        if live_nodes.is_empty() {
            return None;
        }
        let mut hash: usize = 0;
        for byte in domain_id.as_str().bytes() {
            hash = hash.wrapping_mul(131).wrapping_add(byte as usize);
        }
        live_nodes.get(hash % live_nodes.len()).cloned()
    }

    async fn domain_tick_target_nodes(&self, domain_id: &Domain) -> Vec<String> {
        let Some(domain) = self.consensus.current_domain(domain_id).await else {
            return Vec::new();
        };
        if let DomainStatus::Stopped = domain.status {
            return Vec::new();
        }
        let mut nodes = self.cluster.live_node_ids().await;
        nodes.sort();
        nodes.dedup();
        nodes
    }

    async fn describe_stream(&self, domain: &Domain, describe: DescribeRelay) -> CommandResult {
        let (ack_model, schema, parameterization) = match self
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
                    kind: CommandResultKind::Error as i32,
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
                    kind: CommandResultKind::Error as i32,
                    ..Default::default()
                };
            }
        };

        if describe.bindings.is_empty() {
            return command_ok(append_metrics_lines(
                format_relay_describe_output(&ack_model, &parameterization),
                self.runtime
                    .describe_metrics_for(domain, "RELAY", &describe.relay),
            ));
        }

        let filter = match validate_subscription_bindings(
            &ack_model.name,
            &parameterization,
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
                    kind: CommandResultKind::Error as i32,
                    ..Default::default()
                };
            }
        };
        let key = match branch_key_from_filter(&parameterization, &filter) {
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
                    kind: CommandResultKind::Error as i32,
                    ..Default::default()
                };
            }
        };

        if let Some(domain_state) = self.consensus.current_domain(domain).await
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
                    kind: CommandResultKind::Error as i32,
                    ..Default::default()
                };
            }
        };

        let local_node_id = self.consensus.local_node_id().to_string();
        let mut exists = false;
        if owner_nodes.is_empty() || owner_nodes.iter().any(|owner| owner == &local_node_id) {
            match self
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
                        kind: CommandResultKind::Error as i32,
                        ..Default::default()
                    };
                }
            }
        }
        for owner in owner_nodes {
            if owner == local_node_id {
                continue;
            }
            let correlation_id = self.next_cluster_command_correlation_id();
            let (tx, rx) = oneshot::channel();
            self.pending_cluster_commands
                .insert(correlation_id, PendingClusterCommand::DescribeRelay(tx));
            if let Err(message) = self
                .dispatch_interconnect_control(
                    &owner,
                    ControlEnvelope::DescribeRelayRequest(RemoteDescribeRelayRequest {
                        correlation_id,
                        domain: domain.clone(),
                        relay: describe.relay.clone(),
                        bindings: describe.bindings.clone(),
                    }),
                )
                .await
            {
                self.pending_cluster_commands.remove(&correlation_id);
                return CommandResult {
                    success: false,
                    diagnostics: vec![Diagnostic {
                        message: message.clone(),
                        span_start: 0,
                        span_end: 0,
                    }],
                    message,
                    kind: CommandResultKind::Error as i32,
                    ..Default::default()
                };
            }
            match tokio::time::timeout(REMOTE_DESCRIBE_RELAY_TIMEOUT, rx).await {
                Ok(Ok(Ok(remote_exists))) => exists |= remote_exists,
                Ok(Ok(Err(message))) => {
                    return CommandResult {
                        success: false,
                        diagnostics: vec![Diagnostic {
                            message: message.clone(),
                            span_start: 0,
                            span_end: 0,
                        }],
                        message,
                        kind: CommandResultKind::Error as i32,
                        ..Default::default()
                    };
                }
                Ok(Err(_)) => {
                    warn!(
                        owner,
                        domain = domain.as_str(),
                        relay = describe.relay.as_str(),
                        "remote DESCRIBE RELAY response channel closed"
                    );
                    continue;
                }
                Err(_) => {
                    self.pending_cluster_commands.remove(&correlation_id);
                    warn!(
                        owner,
                        domain = domain.as_str(),
                        relay = describe.relay.as_str(),
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
        lines.extend(
            self.runtime
                .describe_metrics_for(domain, "RELAY", &describe.relay),
        );

        CommandResult {
            success: true,
            message: lines.join("\n"),
            diagnostics: Vec::new(),
            kind: CommandResultKind::Ok as i32,
            ..Default::default()
        }
    }

    async fn handle_describe_stream_request(
        &self,
        request: RemoteDescribeRelayRequest,
    ) -> Result<bool, String> {
        self.prepare_stream_owner_control_request(&request.domain, &request.relay)
            .await?;
        let Some((ack_model, schema, parameterization)) = self
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
            &parameterization,
            &schema,
            &request.bindings,
        )?;
        let key = branch_key_from_filter(&parameterization, &filter)?;
        match self
            .runtime
            .describe_local_stream_exists(&request.domain, &request.relay, &key)
        {
            Ok(exists) => Ok(exists),
            Err(crate::runtime::RuntimeError::RelayNotInstantiated { .. }) => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }

    fn handle_describe_stream_response(&self, response: RemoteDescribeRelayResponse) {
        if let Some((_, PendingClusterCommand::DescribeRelay(sender))) = self
            .pending_cluster_commands
            .remove(&response.correlation_id)
        {
            let _ = sender.send(response.result);
        }
    }

    async fn describe_domain(&self, domain: &Domain, _describe: DescribeDomain) -> CommandResult {
        let Some(domain_state) = self.consensus.current_domain(domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        let mut lines = vec![
            format!("domain: {}", domain.as_str()),
            format!("status: {:?}", domain_state.status).to_ascii_lowercase(),
        ];
        lines.extend(self.runtime.describe_domain_statistics(domain));
        command_ok(lines.join("\n"))
    }

    async fn describe_endpoint(
        &self,
        domain: &Domain,
        describe: DescribeEndpoint,
    ) -> CommandResult {
        match self
            .registry
            .get(domain, ModelKind::Endpoint, &describe.name)
        {
            Ok(Some(Model::Endpoint(endpoint))) => {
                command_ok(format_endpoint_describe_output(&describe.name, &endpoint))
            }
            Ok(Some(_)) => command_error(format!(
                "model '{}' is not an endpoint",
                describe.name.as_str()
            )),
            Ok(None) => command_error(format!("endpoint '{}' not found", describe.name.as_str())),
            Err(error) => command_error(error.to_string()),
        }
    }

    async fn describe_ingestor(
        &self,
        domain: &Domain,
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

        let local_node_id = self.consensus.local_node_id();
        let summary = if ingestor_node.executes_on(local_node_id) {
            self.runtime
                .describe_local_ingestor(domain, &describe.ingestor)
                .map(|summary| {
                    (
                        summary,
                        self.runtime
                            .describe_metrics_for(domain, "INGESTOR", &describe.ingestor),
                    )
                })
        } else if let Some(owner) = ingestor_node.execution_node() {
            let correlation_id = self.next_cluster_command_correlation_id();
            let (tx, rx) = oneshot::channel();
            self.pending_cluster_commands
                .insert(correlation_id, PendingClusterCommand::DescribeIngestor(tx));
            if let Err(message) = self
                .dispatch_interconnect_control(
                    owner,
                    ControlEnvelope::DescribeIngestorRequest(RemoteDescribeIngestorRequest {
                        correlation_id,
                        domain: domain.clone(),
                        name: describe.ingestor.clone(),
                    }),
                )
                .await
            {
                self.pending_cluster_commands.remove(&correlation_id);
                return command_error(message);
            }
            match tokio::time::timeout(Duration::from_secs(5), rx).await {
                Ok(Ok(Ok(summary))) => Ok(runtime_ingestor_describe_from_envelope(summary)),
                Ok(Ok(Err(message))) => Err(message),
                Ok(Err(_)) => Err("describe ingestor response channel closed".to_string()),
                Err(_) => {
                    self.pending_cluster_commands.remove(&correlation_id);
                    Err(format!(
                        "timed out waiting for DESCRIBE INGESTOR response from '{}'",
                        owner
                    ))
                }
            }
        } else {
            Ok((
                RuntimeIngestorDescribe {
                    running: false,
                    ready: false,
                    memory_backpressure_paused: self.runtime.ingestors_paused_for_memory_pressure(),
                    transient_error: None,
                    reconnect_backoff: None,
                    reconnect_wait_millis: None,
                    kafka_domain_offsets: None,
                },
                self.runtime
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

    async fn handle_describe_ingestor_request(
        &self,
        request: RemoteDescribeIngestorRequest,
    ) -> Result<IngestorDescribeEnvelope, String> {
        self.prepare_owner_control_request(&request.domain, ModelKind::Ingestor, &request.name)
            .await?;
        let summary = self
            .runtime
            .describe_local_ingestor(&request.domain, &request.name)?;
        let metrics = self
            .runtime
            .describe_metrics_for(&request.domain, "INGESTOR", &request.name);
        Ok(runtime_ingestor_describe_to_envelope(summary, metrics))
    }

    fn handle_describe_ingestor_response(&self, response: RemoteDescribeIngestorResponse) {
        if let Some((_, PendingClusterCommand::DescribeIngestor(sender))) = self
            .pending_cluster_commands
            .remove(&response.correlation_id)
        {
            let _ = sender.send(response.result);
        }
    }

    async fn dataflow_node_status_for_graph(
        &self,
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
    ) -> (DataflowNodeStatus, Option<String>, Option<u64>) {
        dataflow_node_status_from_envelope(
            self.dataflow_node_status_envelope_for_graph(domain, kind, identifier)
                .await,
        )
    }

    async fn dataflow_node_status_envelope_for_graph(
        &self,
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
    ) -> DataflowNodeStatusEnvelope {
        let Ok(model_kind) = kind.to_ascii_lowercase().parse::<ModelKind>() else {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier);
        };
        if model_kind != ModelKind::Ingestor && model_kind != ModelKind::Emitter {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier);
        }
        let Some(node) = self
            .scheduled_model_node(domain, model_kind, identifier)
            .await
        else {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier);
        };
        let local_node_id = self.consensus.local_node_id();
        if node.executes_on(local_node_id) {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier);
        }
        let Some(owner) = node.execution_node() else {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier);
        };
        let correlation_id = self.next_cluster_command_correlation_id();
        let (tx, rx) = oneshot::channel();
        self.pending_cluster_commands.insert(
            correlation_id,
            PendingClusterCommand::DataflowNodeStatus(tx),
        );
        if self
            .dispatch_interconnect_control(
                owner,
                ControlEnvelope::DataflowNodeStatusRequest(RemoteDataflowNodeStatusRequest {
                    correlation_id,
                    domain: domain.clone(),
                    kind: model_kind,
                    name: identifier.clone(),
                }),
            )
            .await
            .is_err()
        {
            self.pending_cluster_commands.remove(&correlation_id);
            return self.local_dataflow_node_status_envelope(domain, kind, identifier);
        }
        match tokio::time::timeout(Duration::from_secs(2), rx).await {
            Ok(Ok(Ok(status))) => status,
            _ => {
                self.pending_cluster_commands.remove(&correlation_id);
                self.local_dataflow_node_status_envelope(domain, kind, identifier)
            }
        }
    }

    fn local_dataflow_node_status_envelope(
        &self,
        domain: &Domain,
        kind: &str,
        identifier: &Identifier,
    ) -> DataflowNodeStatusEnvelope {
        let (status, detail, reconnect_wait_millis) =
            self.runtime.dataflow_node_status(domain, kind, identifier);
        let (transient_error, reconnect_backoff, transient_wait_millis) = self
            .runtime
            .dataflow_node_transient_state(domain, kind, identifier);
        dataflow_node_status_to_envelope(
            status,
            detail,
            transient_error,
            reconnect_backoff,
            reconnect_wait_millis.or(transient_wait_millis),
        )
    }

    async fn handle_dataflow_node_status_request(
        &self,
        request: RemoteDataflowNodeStatusRequest,
    ) -> Result<DataflowNodeStatusEnvelope, String> {
        self.prepare_owner_control_request(&request.domain, request.kind, &request.name)
            .await?;
        let (status, detail, reconnect_wait_millis) = self.runtime.dataflow_node_status(
            &request.domain,
            request.kind.as_str(),
            &request.name,
        );
        let (transient_error, reconnect_backoff, transient_wait_millis) = self
            .runtime
            .dataflow_node_transient_state(&request.domain, request.kind.as_str(), &request.name);
        Ok(dataflow_node_status_to_envelope(
            status,
            detail,
            transient_error,
            reconnect_backoff,
            reconnect_wait_millis.or(transient_wait_millis),
        ))
    }

    fn handle_dataflow_node_status_response(&self, response: RemoteDataflowNodeStatusResponse) {
        if let Some((_, PendingClusterCommand::DataflowNodeStatus(sender))) = self
            .pending_cluster_commands
            .remove(&response.correlation_id)
        {
            let _ = sender.send(response.result);
        }
    }

    async fn describe_metrics_for_scheduled_node(
        &self,
        domain: &Domain,
        kind: ModelKind,
        identifier: &Identifier,
        scheduled_node: Option<&ScheduledNode>,
    ) -> Result<Vec<String>, String> {
        let metric_kind = kind.as_str().to_ascii_uppercase();
        let Some(node) = scheduled_node else {
            return Ok(self
                .runtime
                .describe_metrics_for(domain, &metric_kind, identifier));
        };
        let local_node_id = self.consensus.local_node_id();
        if node.executes_on(local_node_id) {
            return Ok(self
                .runtime
                .describe_metrics_for(domain, &metric_kind, identifier));
        }
        let Some(owner) = node.execution_node() else {
            return Ok(self
                .runtime
                .describe_metrics_for(domain, &metric_kind, identifier));
        };

        let correlation_id = self.next_cluster_command_correlation_id();
        let (tx, rx) = oneshot::channel();
        self.pending_cluster_commands
            .insert(correlation_id, PendingClusterCommand::DescribeMetrics(tx));
        if let Err(message) = self
            .dispatch_interconnect_control(
                owner,
                ControlEnvelope::DescribeMetricsRequest(RemoteDescribeMetricsRequest {
                    correlation_id,
                    domain: domain.clone(),
                    kind,
                    name: identifier.clone(),
                }),
            )
            .await
        {
            self.pending_cluster_commands.remove(&correlation_id);
            return Err(message);
        }
        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("describe metrics response channel closed".to_string()),
            Err(_) => {
                self.pending_cluster_commands.remove(&correlation_id);
                Err(format!(
                    "timed out waiting for DESCRIBE {} metrics response from '{}'",
                    kind.as_str(),
                    owner
                ))
            }
        }
    }

    async fn handle_describe_metrics_request(
        &self,
        request: RemoteDescribeMetricsRequest,
    ) -> Result<Vec<String>, String> {
        self.prepare_owner_control_request(&request.domain, request.kind, &request.name)
            .await?;
        let metric_kind = request.kind.as_str().to_ascii_uppercase();
        Ok(self
            .runtime
            .describe_metrics_for(&request.domain, &metric_kind, &request.name))
    }

    fn handle_describe_metrics_response(&self, response: RemoteDescribeMetricsResponse) {
        if let Some((_, PendingClusterCommand::DescribeMetrics(sender))) = self
            .pending_cluster_commands
            .remove(&response.correlation_id)
        {
            let _ = sender.send(response.result);
        }
    }

    async fn describe_lookup(&self, domain: &Domain, describe: DescribeLookup) -> CommandResult {
        let lookup_target = match self
            .lookup_target_from_schedule(domain, &describe.name)
            .await
        {
            Ok(target) => target,
            Err(message) => return command_error(message),
        };
        let Some((lookup, lookup_node, _)) = lookup_target else {
            return command_error(format!(
                "hash map '{}' does not exist in domain '{}'",
                describe.name.as_str(),
                domain.as_str()
            ));
        };

        let local_node_id = self.consensus.local_node_id();
        let summary = if lookup_node.executes_on(local_node_id) {
            match self.runtime.describe_local_lookup(domain, &describe.name) {
                Ok((_, resource_version, entry_count)) => Ok(LookupDescribeEnvelope {
                    resource: lookup.resource.clone(),
                    resource_version,
                    path: lookup.path.clone(),
                    decode_using_codec: lookup.decode_using_codec.clone(),
                    key_field: lookup.key_field.clone(),
                    entry_count: entry_count as u64,
                }),
                Err(message) => Err(message),
            }
        } else if let Some(owner) = lookup_node.execution_node() {
            let correlation_id = self.next_cluster_command_correlation_id();
            let (tx, rx) = oneshot::channel();
            self.pending_cluster_commands
                .insert(correlation_id, PendingClusterCommand::DescribeLookup(tx));
            if let Err(message) = self
                .dispatch_interconnect_control(
                    owner,
                    ControlEnvelope::DescribeLookupRequest(RemoteDescribeLookupRequest {
                        correlation_id,
                        domain: domain.clone(),
                        name: describe.name.clone(),
                    }),
                )
                .await
            {
                self.pending_cluster_commands.remove(&correlation_id);
                return command_error(message);
            }
            match tokio::time::timeout(Duration::from_secs(5), rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err("describe lookup response channel closed".to_string()),
                Err(_) => {
                    self.pending_cluster_commands.remove(&correlation_id);
                    Err(format!(
                        "timed out waiting for DESCRIBE HASH MAP response from '{}'",
                        owner
                    ))
                }
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
        let (lookup, resource_version, entry_count) = self
            .runtime
            .describe_local_lookup(&request.domain, &request.name)?;
        Ok(LookupDescribeEnvelope {
            resource: lookup.resource,
            resource_version,
            path: lookup.path,
            decode_using_codec: lookup.decode_using_codec,
            key_field: lookup.key_field,
            entry_count: entry_count as u64,
        })
    }

    fn handle_describe_lookup_response(&self, response: RemoteDescribeLookupResponse) {
        if let Some((_, PendingClusterCommand::DescribeLookup(sender))) = self
            .pending_cluster_commands
            .remove(&response.correlation_id)
        {
            let _ = sender.send(response.result);
        }
    }

    async fn describe_deduplicator(
        &self,
        domain: &Domain,
        describe: DescribeDeduplicator,
    ) -> CommandResult {
        let scheduled_node = self
            .scheduled_model_node(domain, ModelKind::Deduplicator, &describe.name)
            .await;
        let model = match self
            .registry
            .get(domain, ModelKind::Deduplicator, &describe.name)
        {
            Ok(Some(model)) => model,
            Ok(None) => {
                let Some(scheduled_node) = scheduled_node.as_ref() else {
                    return command_error(format!(
                        "deduplicator '{}' does not exist in domain '{}'",
                        describe.name.as_str(),
                        domain.as_str()
                    ));
                };
                (*scheduled_node.config).clone()
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read deduplicator '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let Model::Deduplicator(deduplicator) = model else {
            return command_error(format!(
                "model '{}' in domain '{}' is not a deduplicator",
                describe.name.as_str(),
                domain.as_str()
            ));
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

    async fn describe_reingestor(
        &self,
        domain: &Domain,
        describe: DescribeReingestor,
    ) -> CommandResult {
        let scheduled_node = self
            .scheduled_model_node(domain, ModelKind::Reingestor, &describe.name)
            .await;
        let model = match self
            .registry
            .get(domain, ModelKind::Reingestor, &describe.name)
        {
            Ok(Some(model)) => model,
            Ok(None) => {
                let Some(scheduled_node) = scheduled_node.as_ref() else {
                    return command_error(format!(
                        "reingestor '{}' does not exist in domain '{}'",
                        describe.name.as_str(),
                        domain.as_str()
                    ));
                };
                (*scheduled_node.config).clone()
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read reingestor '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let Model::Reingestor(reingestor) = model else {
            return command_error(format!(
                "model '{}' in domain '{}' is not a reingestor",
                describe.name.as_str(),
                domain.as_str()
            ));
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
        domain: &Domain,
        describe: DescribeCorrelator,
    ) -> CommandResult {
        let scheduled_node = self
            .scheduled_model_node(domain, ModelKind::Correlator, &describe.name)
            .await;
        let model = match self
            .registry
            .get(domain, ModelKind::Correlator, &describe.name)
        {
            Ok(Some(model)) => model,
            Ok(None) => {
                let Some(scheduled_node) = scheduled_node.as_ref() else {
                    return command_error(format!(
                        "correlator '{}' does not exist in domain '{}'",
                        describe.name.as_str(),
                        domain.as_str()
                    ));
                };
                (*scheduled_node.config).clone()
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read correlator '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let Model::Correlator(correlator) = model else {
            return command_error(format!(
                "model '{}' in domain '{}' is not a correlator",
                describe.name.as_str(),
                domain.as_str()
            ));
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
        domain: &Domain,
        describe: DescribeReorderer,
    ) -> CommandResult {
        let scheduled_node = self
            .scheduled_model_node(domain, ModelKind::Reorderer, &describe.name)
            .await;
        let model = match self
            .registry
            .get(domain, ModelKind::Reorderer, &describe.name)
        {
            Ok(Some(model)) => model,
            Ok(None) => {
                let Some(scheduled_node) = scheduled_node.as_ref() else {
                    return command_error(format!(
                        "reorderer '{}' does not exist in domain '{}'",
                        describe.name.as_str(),
                        domain.as_str()
                    ));
                };
                (*scheduled_node.config).clone()
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read reorderer '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let Model::Reorderer(reorderer) = model else {
            return command_error(format!(
                "model '{}' in domain '{}' is not a reorderer",
                describe.name.as_str(),
                domain.as_str()
            ));
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

    async fn describe_emitter(&self, domain: &Domain, describe: DescribeEmitter) -> CommandResult {
        let scheduled_node = self
            .scheduled_model_node(domain, ModelKind::Emitter, &describe.name)
            .await;
        let model = match self
            .registry
            .get(domain, ModelKind::Emitter, &describe.name)
        {
            Ok(Some(model)) => model,
            Ok(None) => {
                let Some(scheduled_node) = scheduled_node.as_ref() else {
                    return command_error(format!(
                        "emitter '{}' does not exist in domain '{}'",
                        describe.name.as_str(),
                        domain.as_str()
                    ));
                };
                (*scheduled_node.config).clone()
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read emitter '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let Model::Emitter(emitter) = model else {
            return command_error(format!(
                "model '{}' in domain '{}' is not an emitter",
                describe.name.as_str(),
                domain.as_str()
            ));
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
        domain: &Domain,
        describe: DescribeWindowProcessor,
    ) -> CommandResult {
        let model = match self
            .registry
            .get(domain, ModelKind::WindowProcessor, &describe.name)
        {
            Ok(Some(model)) => model,
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
        let Model::WindowProcessor(processor) = model else {
            return command_error(format!(
                "model '{}' in domain '{}' is not a window processor",
                describe.name.as_str(),
                domain.as_str()
            ));
        };
        let aggregate = match parse_aggregate_program(&processor.aggregate) {
            Ok(program) => program.inner,
            Err(error) => {
                return command_error(format!(
                    "failed to parse aggregate definition for window processor '{}': {error:?}",
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
        domain: &Domain,
        describe: DescribeWasmProcessor,
    ) -> CommandResult {
        let model = match self
            .registry
            .get(domain, ModelKind::WasmProcessor, &describe.name)
        {
            Ok(Some(model)) => model,
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
        let Model::WasmProcessor(processor) = model else {
            return command_error(format!(
                "model '{}' in domain '{}' is not a wasm processor",
                describe.name.as_str(),
                domain.as_str()
            ));
        };
        let scheduled_node = self
            .scheduled_model_node(domain, ModelKind::WasmProcessor, &describe.name)
            .await;

        let metrics = match self
            .describe_metrics_for_scheduled_node(
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
                self.runtime
                    .describe_wasm_processor_state_for(domain, &describe.name),
            ),
            metrics,
        ))
    }

    async fn scheduled_model_node(
        &self,
        domain: &Domain,
        kind: ModelKind,
        identifier: &Identifier,
    ) -> Option<ScheduledNode> {
        let schedule = self.consensus.current_schedule().await;
        schedule.domain(domain).and_then(|domain_schedule| {
            domain_schedule
                .nodes
                .iter()
                .find(|node| node.kind == kind && node.identifier == *identifier)
                .cloned()
        })
    }

    async fn prepare_owner_control_request(
        &self,
        domain: &Domain,
        kind: ModelKind,
        identifier: &Identifier,
    ) -> Result<ScheduledNode, String> {
        self.prepare_control_request_domain(domain).await?;
        let node = self
            .scheduled_model_node(domain, kind, identifier)
            .await
            .ok_or_else(|| {
                format!(
                    "{} '{}' does not exist in domain '{}'",
                    kind.as_str().to_ascii_lowercase(),
                    identifier.as_str(),
                    domain.as_str()
                )
            })?;
        let local_node_id = self.consensus.local_node_id();
        if !node.executes_on(local_node_id) {
            return Err(format!(
                "{} '{}' in domain '{}' is owned by '{}' but request reached '{}'",
                kind.as_str().to_ascii_lowercase(),
                identifier.as_str(),
                domain.as_str(),
                node.execution_node().unwrap_or("-"),
                local_node_id
            ));
        }
        Ok(node)
    }

    async fn prepare_assigned_control_request(
        &self,
        domain: &Domain,
        kind: ModelKind,
        identifier: &Identifier,
    ) -> Result<ScheduledNode, String> {
        self.prepare_control_request_domain(domain).await?;
        let node = self
            .scheduled_model_node(domain, kind, identifier)
            .await
            .ok_or_else(|| {
                format!(
                    "{} '{}' does not exist in domain '{}'",
                    kind.as_str().to_ascii_lowercase(),
                    identifier.as_str(),
                    domain.as_str()
                )
            })?;
        let local_node_id = self.consensus.local_node_id();
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
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<(), String> {
        self.prepare_control_request_domain(domain).await?;
        let owner_nodes = self.scheduled_stream_owner_nodes(domain, relay).await?;
        let local_node_id = self.consensus.local_node_id();
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

    async fn prepare_control_request_domain(&self, domain: &Domain) -> Result<(), String> {
        if self.consensus.current_domain(domain).await.is_none() {
            return Err(format!("domain '{}' does not exist", domain.as_str()));
        }
        self.reconcile_running_domain_runtime(domain).await
    }

    async fn lookup_query(&self, domain: &Domain, query: LookupQuery) -> CommandResult {
        let lookup_target = match self.lookup_target_from_schedule(domain, &query.name).await {
            Ok(target) => target,
            Err(message) => return command_error(message),
        };
        let Some((lookup, lookup_node, key_ty)) = lookup_target else {
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
        let local_node_id = self.consensus.local_node_id();
        let local_record = if lookup_node.is_assigned_to(local_node_id) {
            Some(self.runtime.query_local_lookup(domain, &query.name, &key))
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
                    targets.push(owner.to_string());
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
                    targets.push(owner.to_string());
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
            Ok(Some(record)) => command_ok(record.to_json_string()),
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
        domain: &Domain,
        name: &Identifier,
        key: &str,
        targets: Vec<String>,
    ) -> Result<Option<runtime_schema::DecodedRecord>, String> {
        let mut errors = Vec::new();
        for target in targets {
            let correlation_id = self.next_cluster_command_correlation_id();
            let (tx, rx) = oneshot::channel();
            self.pending_cluster_commands
                .insert(correlation_id, PendingClusterCommand::LookupQuery(tx));
            if let Err(message) = self
                .dispatch_interconnect_control(
                    &target,
                    ControlEnvelope::LookupRequest(RemoteLookupRequest {
                        correlation_id,
                        domain: domain.clone(),
                        name: name.clone(),
                        key: key.to_string(),
                    }),
                )
                .await
            {
                self.pending_cluster_commands.remove(&correlation_id);
                errors.push(message);
                continue;
            }
            match tokio::time::timeout(Duration::from_secs(5), rx).await {
                Ok(Ok(result)) => match result {
                    Ok(record) => return Ok(record),
                    Err(message) => errors.push(message),
                },
                Ok(Err(_)) => errors.push("lookup response channel closed".to_string()),
                Err(_) => {
                    self.pending_cluster_commands.remove(&correlation_id);
                    errors.push(format!(
                        "timed out waiting for LOOKUP response from '{}'",
                        target
                    ));
                }
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
    ) -> Result<Option<runtime_schema::DecodedRecord>, String> {
        self.prepare_assigned_control_request(&request.domain, ModelKind::Lookup, &request.name)
            .await?;
        self.runtime
            .query_local_lookup(&request.domain, &request.name, &request.key)
    }

    fn handle_lookup_response(&self, response: RemoteLookupResponse) {
        if let Some((_, PendingClusterCommand::LookupQuery(sender))) = self
            .pending_cluster_commands
            .remove(&response.correlation_id)
        {
            let _ = sender.send(
                response
                    .result
                    .map(|record| record.map(runtime_schema::DecodedRecord::from_remote)),
            );
        }
    }

    async fn process_suggest(
        &self,
        req: SuggestRequest,
        _subscriptions: &SessionSubscriptions,
    ) -> SuggestResponse {
        let cursor = usize::try_from(req.cursor).unwrap_or(req.input.len());
        let domain = parse_request_domain(&req.domain).ok();

        let (grammar_input, grammar_cursor, prefix) = completion_context(&req.input, cursor);
        let grammar = suggest_client_statement(&grammar_input, grammar_cursor);

        let mut suggestions = Vec::new();
        let mut semantic_kinds = Vec::new();
        let mut expects_resource_ref = false;
        let requested_resource_versions = requested_resource_versions(&req.input, cursor);
        for item in &grammar {
            if let Some(kind) = ModelKind::from_completion_label(item) {
                semantic_kinds.push(kind);
            } else if item == "ref:resource" {
                expects_resource_ref = true;
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
                && self.consensus.current_domain(domain).await.is_some()
                && let Ok(ids) = self.registry.list_identifiers(domain, *kind, &prefix)
            {
                suggestions.extend(ids.into_iter().map(|id| id.to_string()));
            }
        }

        if expects_resource_ref {
            let resources = self.consensus.current_resources().await;
            suggestions.extend(resource_ref_suggestions(&resources, &prefix));
            if let Some(resource_identifier) = requested_resource_versions.as_ref() {
                suggestions.extend(resource_version_suggestions(
                    &resources,
                    resource_identifier,
                    &prefix,
                ));
            }
        } else if let Some(resource_identifier) = requested_resource_versions.as_ref() {
            let resources = self.consensus.current_resources().await;
            suggestions.extend(resource_version_suggestions(
                &resources,
                resource_identifier,
                &prefix,
            ));
        }

        if grammar_input.contains("DOMAIN") || (semantic_kinds.is_empty() && !expects_resource_ref)
        {
            let domains = self.consensus.current_domains().await;
            suggestions.extend(domains.into_keys().filter_map(|id| {
                if prefix.is_empty() || id.as_str().starts_with(&prefix) {
                    Some(id.to_string())
                } else {
                    None
                }
            }));
        }

        let mut response_suggestions = SortedSet::from_unsorted(suggestions)
            .into_vec()
            .into_iter()
            .map(|value| ApiSuggestion {
                value,
                kind: SuggestionKind::Text as i32,
            })
            .collect::<Vec<_>>();

        if let Some(fragment) = upload_resource_path_fragment(&req.input, cursor) {
            response_suggestions.push(ApiSuggestion {
                value: fragment.to_string(),
                kind: SuggestionKind::LocalDirectoryLookup as i32,
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
        let client_statements = match parse_client_statements(&req.query) {
            Ok(statements) => statements,
            Err(ParseFromSourceError::Lex { diagnostics, .. }) => {
                return error_response("lex error", &diagnostics);
            }
            Err(ParseFromSourceError::Parse { diagnostics, .. }) => {
                return error_response("parse error", &diagnostics);
            }
        };

        let is_batch = client_statements.len() > 1;
        let mut results = Vec::new();
        let mut client_statements = client_statements.into_iter().peekable();
        while let Some(client_statement) = client_statements.next() {
            if let ClientStatement::Server(Statement::Create(create)) = client_statement {
                let mut creates = vec![create];
                while let Some(ClientStatement::Server(Statement::Create(_))) =
                    client_statements.peek()
                {
                    let Some(ClientStatement::Server(Statement::Create(create))) =
                        client_statements.next()
                    else {
                        unreachable!("peeked create statement must be next");
                    };
                    creates.push(create);
                }
                let result = if creates.len() == 1 {
                    self.process_client_statement(
                        ClientStatement::Server(Statement::Create(
                            creates
                                .pop()
                                .expect("single create batch must contain one create statement"),
                        )),
                        &req.query,
                        &req.domain,
                        tx,
                        subscriptions,
                    )
                    .await
                } else {
                    self.process_create_model_batch(creates, &req.query, &req.domain)
                        .await
                };
                if !result.success {
                    return command_batch_result(results, result, is_batch);
                }
                append_command_result(&mut results, result);
                continue;
            }

            let result = self
                .process_client_statement(
                    client_statement,
                    &req.query,
                    &req.domain,
                    tx,
                    subscriptions,
                )
                .await;
            if !result.success {
                return command_batch_result(results, result, is_batch);
            }
            append_command_result(&mut results, result);
        }

        if results.is_empty() {
            return command_error("empty command".to_string());
        }
        if !is_batch {
            return results
                .pop()
                .expect("non-empty results must contain the single result");
        }

        CommandResult {
            success: true,
            message: results
                .iter()
                .map(|result| result.message.as_str())
                .collect::<Vec<_>>()
                .join("\n"),
            diagnostics: Vec::new(),
            kind: CommandResultKind::Ok as i32,
            results,
            ..Default::default()
        }
    }

    async fn process_create_model_batch(
        &self,
        creates: Vec<CreateStatement<Box<Model>>>,
        query: &str,
        request_domain: &str,
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

        let leader = self.consensus.current_leader().await;
        if leader.as_deref() != Some(self.consensus.local_node_id()) {
            return self.not_leader_response(query, leader).await;
        }

        let Some(domain_state) = self.consensus.current_domain(&domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        if let Err(error) = self.reconcile_running_domain_runtime(&domain).await {
            return command_error(error);
        }

        let mut results = vec![None; creates.len()];
        let mut models = Vec::new();
        let mut created = Vec::new();

        for (index, create) in creates.into_iter().enumerate() {
            let if_not_exists = create.if_not_exists;
            let model = create.body;
            let model_id = model.identifier().clone();
            let model_kind = model.kind();
            if let Ok(Some(_)) = self.registry.get(&domain, model_kind, &model_id)
                && if_not_exists
            {
                results[index] = Some(command_ok_already_existed(format!(
                    "model '{}' already exists in domain '{}'",
                    model_id.as_str(),
                    domain.as_str()
                )));
                continue;
            }

            if let Model::Ingestor(ingestor) = model.as_ref()
                && let DomainPace::Paced = domain_state.config.pace
                && ingestor.timestamp_source.is_none()
            {
                return command_error(format!(
                    "paced domain '{}' requires ingestor '{}' to declare TIMESTAMP NOW or \
                     TIMESTAMP AT <field>",
                    domain.as_str(),
                    ingestor.name.as_str()
                ));
            }
            if let Model::Vhost(vhost) = model.as_ref()
                && let Some(tls) = vhost.tls.as_ref()
                && let Err(error) = self.validate_vhost_tls_binding(tls).await
            {
                return command_error(format!(
                    "invalid TLS resource for VHOST '{}': {error}",
                    vhost.name.as_str()
                ));
            }
            if let Model::Lookup(lookup) = model.as_ref()
                && let Err(error) = self.validate_lookup_binding(lookup).await
            {
                return command_error(format!(
                    "invalid HASH MAP '{}': {error}",
                    lookup.name.as_str()
                ));
            }
            if let Model::Inferencer(processor) = model.as_ref()
                && let Err(error) = self.validate_inferencer_binding(&domain, processor).await
            {
                return command_error(format!(
                    "invalid INFERENCER '{}': {error}",
                    processor.name.as_str()
                ));
            }

            created.push((index, model_id, model_kind));
            models.push(*model);
        }

        if !models.is_empty() {
            let error_target = created
                .first()
                .map(|(_, id, _)| id.clone())
                .expect("non-empty model batch must have a first created model");
            let runtime_changes = match self.registry.apply_batch(&domain, models) {
                Ok(changes) => changes,
                Err(err) => {
                    warn!(
                        domain = domain.as_str(),
                        error = %err,
                        "failed to apply create statement batch"
                    );
                    return create_registry_error_response(query, &domain, &error_target, &err);
                }
            };

            if let Err(err) = self
                .publish_domain_schedule(&domain, runtime_changes.graph.clone())
                .await
            {
                self.broadcast_error(format!(
                    "schedule publish failed in domain '{}': {}",
                    domain.as_str(),
                    err
                ));
                warn!(
                    domain = domain.as_str(),
                    error = %err,
                    "failed to publish schedule for create statement batch"
                );
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
                    kind: CommandResultKind::Error as i32,
                    ..Default::default()
                };
            }

            for (index, model_id, model_kind) in created {
                if model_kind == ModelKind::Vhost
                    && let Err(error) = self.refresh_http_tls_server_config().await
                {
                    self.broadcast_error(format!("failed to refresh HTTP TLS config: {error}"));
                }
                results[index] = Some(CommandResult {
                    success: true,
                    message: format!(
                        "stored model '{}' in domain '{}'",
                        model_id.as_str(),
                        domain.as_str()
                    ),
                    diagnostics: Vec::new(),
                    kind: CommandResultKind::Ok as i32,
                    ..Default::default()
                });
            }
        }

        let results = results
            .into_iter()
            .map(|result| result.expect("every create statement must produce a command result"))
            .collect::<Vec<_>>();
        CommandResult {
            success: true,
            message: command_results_message(&results),
            diagnostics: Vec::new(),
            kind: CommandResultKind::Ok as i32,
            results,
            ..Default::default()
        }
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
            ClientStatement::SubscribeSession(subscription) => {
                let domain = match parse_request_domain(request_domain) {
                    Ok(domain) => domain,
                    Err(RequestDomainError::Missing) => {
                        return command_error("no active domain selected".to_string());
                    }
                    Err(RequestDomainError::Invalid) => {
                        return command_error("invalid domain".to_string());
                    }
                };
                if self.consensus.current_domain(&domain).await.is_none() {
                    return command_error(format!("domain '{}' does not exist", domain.as_str()));
                }
                if let Err(error) = self.reconcile_running_domain_runtime(&domain).await {
                    return command_error(error);
                }
                return self
                    .subscribe_session(&domain, subscription, tx, subscriptions)
                    .await;
            }
            ClientStatement::UnsubscribeSession(subscription) => {
                let domain = match parse_request_domain(request_domain) {
                    Ok(domain) => domain,
                    Err(RequestDomainError::Missing) => {
                        return command_error("no active domain selected".to_string());
                    }
                    Err(RequestDomainError::Invalid) => {
                        return command_error("invalid domain".to_string());
                    }
                };
                if self.consensus.current_domain(&domain).await.is_none() {
                    return command_error(format!("domain '{}' does not exist", domain.as_str()));
                }
                if let Err(error) = self.reconcile_running_domain_runtime(&domain).await {
                    return command_error(error);
                }
                return self
                    .unsubscribe_session(&domain, subscription, subscriptions)
                    .await;
            }
            ClientStatement::Server(statement) => statement,
        };

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
            let leader = self.consensus.current_leader().await;
            if leader.as_deref() != Some(self.consensus.local_node_id()) {
                return self.not_leader_response(query, leader).await;
            }
        }

        if requires_existing_domain(&statement) {
            let domain = domain.as_ref().expect("domain required");
            if self.consensus.current_domain(domain).await.is_none() {
                return command_error(format!("domain '{}' does not exist", domain.as_str()));
            }
        }

        if requires_runtime_reconcile(&statement) {
            let domain = domain.as_ref().expect("domain required");
            if let Err(error) = self.reconcile_running_domain_runtime(domain).await {
                return command_error(error);
            }
        }

        match statement {
            Statement::CreateDomain(create) => self.create_domain(create).await,
            Statement::CreateUser(create) => self.create_user(create).await,
            Statement::CreateResource(create) => self.create_resource(create).await,
            Statement::UploadResource(upload) => self.upload_resource_command(upload).await,
            Statement::StartDomain(start) => {
                let domain = domain.as_ref().expect("domain required");
                self.start_domain(domain, start).await
            }
            Statement::StopDomain(stop) => {
                let domain = domain.as_ref().expect("domain required");
                self.stop_domain(domain, stop).await
            }
            Statement::Create(model) => {
                let domain = domain.as_ref().expect("domain required");
                let Some(domain_state) = self.consensus.current_domain(domain).await else {
                    return command_error(format!("domain '{}' does not exist", domain.as_str()));
                };
                let if_not_exists = model.if_not_exists;
                let model = model.body;
                if let Ok(Some(_)) = self.registry.get(domain, model.kind(), model.identifier())
                    && if_not_exists
                {
                    return command_ok_already_existed(format!(
                        "model '{}' already exists in domain '{}'",
                        model.identifier().as_str(),
                        domain.as_str()
                    ));
                }
                if let Model::Ingestor(ingestor) = model.as_ref()
                    && let DomainPace::Paced = domain_state.config.pace
                    && ingestor.timestamp_source.is_none()
                {
                    return command_error(format!(
                        "paced domain '{}' requires ingestor '{}' to declare TIMESTAMP NOW or \
                         TIMESTAMP AT <field>",
                        domain.as_str(),
                        ingestor.name.as_str()
                    ));
                }
                if let Model::Vhost(vhost) = model.as_ref()
                    && let Some(tls) = vhost.tls.as_ref()
                    && let Err(error) = self.validate_vhost_tls_binding(tls).await
                {
                    return command_error(format!(
                        "invalid TLS resource for VHOST '{}': {error}",
                        vhost.name.as_str()
                    ));
                }
                if let Model::Lookup(lookup) = model.as_ref()
                    && let Err(error) = self.validate_lookup_binding(lookup).await
                {
                    return command_error(format!(
                        "invalid HASH MAP '{}': {error}",
                        lookup.name.as_str()
                    ));
                }
                if let Model::Inferencer(processor) = model.as_ref()
                    && let Err(error) = self.validate_inferencer_binding(domain, processor).await
                {
                    return command_error(format!(
                        "invalid INFERENCER '{}': {error}",
                        processor.name.as_str()
                    ));
                }
                let model_id = model.as_ref().identifier().clone();
                let model_kind = model.as_ref().kind();
                info!(
                    domain = domain.as_str(),
                    model = model_id.as_str(),
                    kind = model_kind.as_str(),
                    "applying create statement"
                );

                let runtime_changes = match self
                    .registry
                    .apply_batch(domain, vec![(*model).clone()])
                {
                    Ok(changes) => changes,
                    Err(err) => {
                        if if_not_exists
                            && matches!(err.current_context(), RegistryError::AlreadyExists { .. })
                        {
                            return command_ok_already_existed(format!(
                                "model '{}' already exists in domain '{}'",
                                model_id.as_str(),
                                domain.as_str()
                            ));
                        }
                        warn!(
                            domain = domain.as_str(),
                            model = model_id.as_str(),
                            kind = model_kind.as_str(),
                            error = %err,
                            "failed to apply create statement"
                        );
                        return create_registry_error_response(query, domain, &model_id, &err);
                    }
                };

                if let Err(err) = self
                    .publish_domain_schedule(domain, runtime_changes.graph.clone())
                    .await
                {
                    self.broadcast_error(format!(
                        "schedule publish failed in domain '{}': {}",
                        domain.as_str(),
                        err
                    ));
                    warn!(
                        domain = domain.as_str(),
                        model = model_id.as_str(),
                        kind = model_kind.as_str(),
                        error = %err,
                        "failed to publish schedule for create statement"
                    );
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
                        kind: CommandResultKind::Error as i32,
                        ..Default::default()
                    };
                }

                info!(
                    domain = domain.as_str(),
                    model = model_id.as_str(),
                    kind = model_kind.as_str(),
                    "applied create statement"
                );

                if model_kind == ModelKind::Vhost
                    && let Err(error) = self.refresh_http_tls_server_config().await
                {
                    self.broadcast_error(format!("failed to refresh HTTP TLS config: {error}"));
                }

                CommandResult {
                    success: true,
                    message: format!(
                        "stored model '{}' in domain '{}'",
                        model_id.as_str(),
                        domain.as_str()
                    ),
                    diagnostics: Vec::new(),
                    kind: CommandResultKind::Ok as i32,
                    ..Default::default()
                }
            }
            Statement::AlterRelay(alter) => {
                let domain = domain.as_ref().expect("domain required");
                let model_id = alter.relay.clone();
                let model_kind = ModelKind::Relay;
                info!(
                    domain = domain.as_str(),
                    model = model_id.as_str(),
                    kind = model_kind.as_str(),
                    "applying alter relay statement"
                );

                let runtime_changes = match self.registry.alter_relay(domain, alter.clone()) {
                    Ok(changes) => changes,
                    Err(err) => {
                        warn!(
                            domain = domain.as_str(),
                            model = model_id.as_str(),
                            kind = model_kind.as_str(),
                            error = %err,
                            "failed to apply alter relay statement"
                        );
                        return create_registry_error_response(query, domain, &model_id, &err);
                    }
                };

                if let Err(err) = self
                    .publish_domain_schedule(domain, runtime_changes.graph.clone())
                    .await
                {
                    self.broadcast_error(format!(
                        "schedule publish failed in domain '{}': {}",
                        domain.as_str(),
                        err
                    ));
                    warn!(
                        domain = domain.as_str(),
                        model = model_id.as_str(),
                        kind = model_kind.as_str(),
                        error = %err,
                        "failed to publish schedule for alter relay statement"
                    );
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
                        kind: CommandResultKind::Error as i32,
                        ..Default::default()
                    };
                }

                info!(
                    domain = domain.as_str(),
                    model = model_id.as_str(),
                    kind = model_kind.as_str(),
                    "applied alter relay statement"
                );

                CommandResult {
                    success: true,
                    message: format!(
                        "altered relay '{}' in domain '{}'",
                        model_id.as_str(),
                        domain.as_str()
                    ),
                    diagnostics: Vec::new(),
                    kind: CommandResultKind::Ok as i32,
                    ..Default::default()
                }
            }
            Statement::Drop(drop) => {
                let domain = domain.as_ref().expect("domain required");
                let model_id = drop.name.clone();
                let model_kind = drop.kind;
                info!(
                    domain = domain.as_str(),
                    model = model_id.as_str(),
                    kind = model_kind.as_str(),
                    "applying drop statement"
                );

                let runtime_changes = match self.registry.drop_batch(domain, vec![drop.clone()]) {
                    Ok(changes) => changes,
                    Err(err) => {
                        warn!(
                            domain = domain.as_str(),
                            model = model_id.as_str(),
                            kind = model_kind.as_str(),
                            error = %err,
                            "failed to apply drop statement"
                        );
                        return create_registry_error_response(query, domain, &model_id, &err);
                    }
                };

                if let Err(err) = self
                    .publish_domain_schedule(domain, runtime_changes.graph.clone())
                    .await
                {
                    self.broadcast_error(format!(
                        "schedule publish failed in domain '{}': {}",
                        domain.as_str(),
                        err
                    ));
                    warn!(
                        domain = domain.as_str(),
                        model = model_id.as_str(),
                        kind = model_kind.as_str(),
                        error = %err,
                        "failed to publish schedule for drop statement"
                    );
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
                        kind: CommandResultKind::Error as i32,
                        ..Default::default()
                    };
                }

                info!(
                    domain = domain.as_str(),
                    model = model_id.as_str(),
                    kind = model_kind.as_str(),
                    "applied drop statement"
                );

                if model_kind == ModelKind::Vhost
                    && let Err(error) = self.refresh_http_tls_server_config().await
                {
                    self.broadcast_error(format!("failed to refresh HTTP TLS config: {error}"));
                }

                CommandResult {
                    success: true,
                    message: format!(
                        "dropped model '{}' from domain '{}'",
                        model_id.as_str(),
                        domain.as_str()
                    ),
                    diagnostics: Vec::new(),
                    kind: CommandResultKind::Ok as i32,
                    ..Default::default()
                }
            }
            Statement::DropNode(drop) => self.drop_node(drop.node_id).await,
            Statement::CordonNode(cordon) => self.set_node_cordoned(cordon.node_id, true).await,
            Statement::UncordonNode(uncordon) => {
                self.set_node_cordoned(uncordon.node_id, false).await
            }
            Statement::DrainNode(drain) => self.drain_node(drain.node_id).await,
            Statement::SubscribeSession(subscription) => {
                let domain = domain.as_ref().expect("domain required");
                self.subscribe_session(domain, subscription, tx, subscriptions)
                    .await
            }
            Statement::UnsubscribeSession(subscription) => {
                let domain = domain.as_ref().expect("domain required");
                self.unsubscribe_session(domain, subscription, subscriptions)
                    .await
            }
            Statement::DescribeRelay(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_stream(domain, describe).await
            }
            Statement::DescribeDomain(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_domain(domain, describe).await
            }
            Statement::DescribeEndpoint(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_endpoint(domain, describe).await
            }
            Statement::DescribeIngestor(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_ingestor(domain, describe).await
            }
            Statement::DescribeLookup(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_lookup(domain, describe).await
            }
            Statement::DescribeDeduplicator(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_deduplicator(domain, describe).await
            }
            Statement::DescribeReingestor(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_reingestor(domain, describe).await
            }
            Statement::DescribeCorrelator(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_correlator(domain, describe).await
            }
            Statement::DescribeReorderer(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_reorderer(domain, describe).await
            }
            Statement::DescribeEmitter(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_emitter(domain, describe).await
            }
            Statement::DescribeWindowProcessor(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_window_processor(domain, describe).await
            }
            Statement::DescribeWasmProcessor(describe) => {
                let domain = domain.as_ref().expect("domain required");
                self.describe_wasm_processor(domain, describe).await
            }
            Statement::DescribeResource(describe) => self.describe_resource(describe).await,
            Statement::LookupQuery(query) => {
                let domain = domain.as_ref().expect("domain required");
                self.lookup_query(domain, query).await
            }
            Statement::ShowCreate(show) => {
                let domain = domain.as_ref().expect("domain required");
                let name_span = find_identifier_span(query, &show.name).unwrap_or(0..0);
                let model = match self.registry.get(domain, show.kind, &show.name) {
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
                            kind: CommandResultKind::Error as i32,
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
                            kind: CommandResultKind::Error as i32,
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
                            kind: CommandResultKind::Error as i32,
                            ..Default::default()
                        };
                    }
                };

                CommandResult {
                    success: true,
                    message: canonical,
                    diagnostics: Vec::new(),
                    kind: CommandResultKind::Ok as i32,
                    ..Default::default()
                }
            }
            Statement::ShowRelayMaterializedState(show) => {
                let domain = domain.as_ref().expect("domain required");
                self.show_stream_materialized_state(domain, show).await
            }
            Statement::ShowClusterStatus(_) => CommandResult {
                success: true,
                message: render_cluster_status(&self.cluster, &self.consensus).await,
                diagnostics: Vec::new(),
                kind: CommandResultKind::Ok as i32,
                ..Default::default()
            },
        }
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
        let identifier = match Identifier::parse(resource_name.trim()) {
            Ok(identifier) => identifier,
            Err(_) => {
                return web_console_upload_text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid resource name",
                );
            }
        };
        let leader = self.consensus.current_leader().await;
        if leader.as_deref() != Some(self.consensus.local_node_id()) {
            return web_console_upload_text_response(
                StatusCode::CONFLICT,
                "resource uploads must be sent to the cluster leader",
            );
        }
        let resources = self.consensus.current_resources().await;
        if !resources
            .next_version_by_identifier
            .iter()
            .any(|(known_identifier, _)| known_identifier == &identifier)
        {
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
            .stage_web_console_resource_upload(request, boundary, identifier.clone())
            .await
        {
            Ok((archive_path, root_checksum)) => match self
                .install_uploaded_resource_archive(identifier, &archive_path, root_checksum)
                .await
            {
                Ok(version) => web_console_upload_text_response(
                    StatusCode::OK,
                    format!("uploaded resource version {version}"),
                ),
                Err(message) => web_console_upload_text_response(StatusCode::BAD_REQUEST, message),
            },
            Err((status, message)) => web_console_upload_text_response(status, message),
        }
    }

    async fn stage_web_console_resource_upload(
        &self,
        request: HyperRequest<HyperIncoming>,
        boundary: String,
        identifier: Identifier,
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
            file_count = file_count.saturating_add(1);
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
            None => {
                web_console_server_error_response("session request payload is missing".to_string())
            }
        }
    }

    async fn process_web_console_active_domain_request(
        &self,
        request: SetActiveDomainRequest,
        active_domain: &mut Option<Domain>,
    ) -> Result<SessionResponse, SessionResponse> {
        let domain = match Domain::parse(request.domain.trim()) {
            Ok(domain) => domain,
            Err(_) => {
                return Err(web_console_server_error_response(
                    "invalid active domain".to_string(),
                ));
            }
        };
        if self.consensus.current_domain(&domain).await.is_none() {
            return Err(web_console_server_error_response(format!(
                "domain '{}' does not exist",
                domain.as_str()
            )));
        }
        *active_domain = Some(domain.clone());
        Ok(SessionResponse {
            event: Some(proto::session_response::Event::Server(ServerEvent {
                level: ServerEventLevel::Info as i32,
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
            return command_error(
                "UPLOAD RESOURCE is not supported in the web console".to_string(),
            );
        }

        self.process_command(req, tx, subscriptions).await
    }

    async fn reconcile_running_domain_runtime(&self, domain: &Domain) -> Result<(), String> {
        let domains = self.consensus.current_domains().await;
        self.runtime.sync_domains(&domains);
        let Some(domain_state) = domains.get(domain) else {
            return Ok(());
        };
        if !matches!(domain_state.status, DomainStatus::Running) {
            return Ok(());
        }
        let schedule = self.consensus.current_schedule().await;
        self.runtime
            .apply_cluster_schedule(self.consensus.local_node_id(), &schedule)
            .await
            .map_err(|error| {
                format!(
                    "failed to restore runtime for running domain '{}': {error}",
                    domain.as_str()
                )
            })
    }

    async fn create_domain(&self, create: CreateStatement<CreateDomain>) -> CommandResult {
        if self.consensus.current_domain(&create.id).await.is_some() {
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
        };
        match self.consensus.put_domain(state).await {
            Ok(()) => {
                self.runtime
                    .sync_domains(&self.consensus.current_domains().await);
                command_ok(format!("created domain '{}'", create.id.as_str()))
            }
            Err(error) => command_error(format!(
                "failed to create domain '{}': {error}",
                create.id.as_str()
            )),
        }
    }

    async fn create_user(&self, create: CreateStatement<CreateUser>) -> CommandResult {
        let if_not_exists = create.if_not_exists;
        let create = create.body;
        if self.consensus.current_user(&create.name).await.is_some() {
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
        match self.consensus.create_user(user).await {
            Ok(()) => command_ok(format!("created user '{}'", create.name.as_str())),
            Err(error) => command_error(format!(
                "failed to create user '{}': {error}",
                create.name.as_str()
            )),
        }
    }

    async fn create_resource(&self, create: CreateStatement<CreateResource>) -> CommandResult {
        let resources = self.consensus.current_resources().await;
        if resources
            .next_version_by_identifier
            .iter()
            .any(|(identifier, _)| identifier == &create.identifier)
        {
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
            .consensus
            .create_resource_catalog(create.identifier.as_str())
            .await
        {
            Ok(()) => command_ok(format!("created resource '{}'", create.identifier.as_str())),
            Err(error) => command_error(format!(
                "failed to create resource '{}': {error}",
                create.identifier.as_str()
            )),
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
        identifier: Identifier,
        archive_path: &Path,
        root_checksum: String,
    ) -> Result<u64, String> {
        let created_at = current_timestamp();
        let version = match self
            .consensus
            .allocate_resource_version(identifier.as_str())
            .await
        {
            Ok(version) => version,
            Err(error) => {
                return Err(format!(
                    "failed to allocate version for resource '{}': {error}",
                    identifier.as_str()
                ));
            }
        };

        let manifest = match self
            .resource_store
            .install_from_archive_path(
                identifier.clone(),
                version,
                archive_path,
                root_checksum,
                self.consensus.local_node_id(),
                created_at,
            )
            .await
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return Err(format!(
                    "failed to install resource '{}': {}",
                    identifier.as_str(),
                    error,
                ));
            }
        };

        if let Err(error) = self
            .consensus
            .put_resource_version(manifest.resource.clone())
            .await
        {
            let cleanup_suffix = match self.resource_store.remove_version(
                &manifest.resource.id.identifier,
                manifest.resource.id.version,
            ) {
                Ok(()) => String::new(),
                Err(cleanup_error) => {
                    format!("; local cleanup also failed: {cleanup_error}")
                }
            };
            return Err(format!(
                "failed to publish resource '{}@{}': {error}{cleanup_suffix}",
                manifest.resource.id.identifier.as_str(),
                manifest.resource.id.version,
            ));
        }

        if let Err(error) = self
            .consensus
            .put_resource_replica(ResourceNodeStatus {
                key: ResourceReplicaKey::new(
                    manifest.resource.id.identifier.clone(),
                    manifest.resource.id.version,
                    self.consensus.local_node_id(),
                ),
                state: ResourceNodeState::Ready,
                root_checksum: Some(manifest.resource.root_checksum.clone()),
                last_verified_at: Some(created_at),
                source_node_id: Some(self.consensus.local_node_id().to_string()),
                error: None,
            })
            .await
        {
            return Err(format!(
                "failed to publish resource replica '{}@{}': {error}",
                manifest.resource.id.identifier.as_str(),
                manifest.resource.id.version
            ));
        }

        self.wait_for_resource_cluster_ready(
            &manifest.resource.id.identifier,
            manifest.resource.id.version,
        )
        .await?;
        self.runtime
            .sync_resource_versions(&self.consensus.current_resources().await);
        if let Err(error) = self.refresh_http_tls_server_config().await {
            self.broadcast_error(format!("failed to refresh HTTP TLS config: {error}"));
        }
        Ok(manifest.resource.id.version)
    }

    async fn start_domain(&self, domain_id: &Domain, start: StartDomain) -> CommandResult {
        let Some(domain) = self.consensus.current_domain(domain_id).await else {
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
        let (logical_start, time_rate) = match parse_start_point(&start.start) {
            Ok(value) => value,
            Err(message) => return command_error(message),
        };
        let wall_started_at = current_timestamp();
        let concrete_start = match &start.start {
            DomainStartPoint::Resume => DomainStartPoint::Resume,
            DomainStartPoint::Now { .. } => DomainStartPoint::At {
                timestamp: logical_start.as_datetime().to_rfc3339(),
                time_rate: time_rate.clone(),
            },
            DomainStartPoint::At { .. } => start.start.clone(),
        };
        match self
            .consensus
            .start_domain(domain_id.clone(), concrete_start)
            .await
        {
            Ok(()) => {
                self.runtime
                    .sync_domains(&self.consensus.current_domains().await);
                let schedule = self.consensus.current_schedule().await;
                if let Err(error) = self
                    .runtime
                    .apply_cluster_schedule(self.consensus.local_node_id(), &schedule)
                    .await
                {
                    let _ = self.consensus.stop_domain(domain_id.clone()).await;
                    self.runtime
                        .sync_domains(&self.consensus.current_domains().await);
                    return command_error(format!(
                        "failed to start domain '{}': {error}",
                        domain_id.as_str()
                    ));
                }
                if let DomainPace::Paced = domain.config.pace {
                    self.runtime.handle_domain_clock_start(
                        domain_id,
                        logical_start,
                        wall_started_at,
                        &time_rate,
                    );
                }
                if let DomainPace::Paced = domain.config.pace
                    && let Err(message) = self
                        .start_domain_clock(
                            domain_id.clone(),
                            wall_started_at,
                            logical_start,
                            time_rate,
                        )
                        .await
                {
                    let _ = self.consensus.stop_domain(domain_id.clone()).await;
                    return command_error(message);
                }
                command_ok(format!("starting domain '{}'", domain_id.as_str()))
            }
            Err(error) => command_error(format!(
                "failed to start domain '{}': {error}",
                domain_id.as_str()
            )),
        }
    }

    async fn stop_domain(&self, domain_id: &Domain, _stop: StopDomain) -> CommandResult {
        let Some(domain) = self.consensus.current_domain(domain_id).await else {
            return command_error(format!("domain '{}' does not exist", domain_id.as_str()));
        };
        if let DomainStatus::Stopped = domain.status {
            return command_error(format!(
                "domain '{}' is already stopped",
                domain_id.as_str()
            ));
        }
        if let DomainPace::Paced = domain.config.pace
            && let Err(message) = self.stop_domain_clock(domain_id).await
        {
            return command_error(message);
        }
        if let DomainPace::Paced = domain.config.pace {
            self.runtime.handle_domain_clock_stop(domain_id);
        }
        match self.consensus.stop_domain(domain_id.clone()).await {
            Ok(()) => {
                self.runtime
                    .sync_domains(&self.consensus.current_domains().await);
                command_ok(format!("stopped domain '{}'", domain_id.as_str()))
            }
            Err(error) => command_error(format!(
                "failed to stop domain '{}': {error}",
                domain_id.as_str()
            )),
        }
    }

    async fn show_stream_materialized_state(
        &self,
        domain: &Domain,
        show: ShowRelayMaterializedState,
    ) -> CommandResult {
        let schedule = self.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return command_error(format!(
                "domain '{}' has no active schedule",
                domain.as_str()
            ));
        };
        let Some(materializer) = domain_schedule
            .nodes
            .iter()
            .find(|node| node.kind == ModelKind::Materializer && node.identifier == show.relay)
        else {
            return command_error(format!(
                "stream '{}' in domain '{}' is not materialized",
                show.relay.as_str(),
                domain.as_str()
            ));
        };

        let entries = match self
            .runtime
            .local_materialized_stream_state(domain, &show.relay)
        {
            Ok(entries) if !entries.is_empty() => entries,
            Ok(_) if !materializer.executes_on(self.consensus.local_node_id()) => {
                if let Some(primary_node) = materializer.primary_node() {
                    match self
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
            format_materialized_stream_state_output(&show.relay, materializer, Vec::new())
        } else {
            let entry_lines = entries
                .into_iter()
                .map(|(key, record)| {
                    let metadata = record.metadata();
                    format!(
                        "key={} payload={} low={} high={}",
                        if key.is_empty() {
                            "(root)"
                        } else {
                            key.as_str()
                        },
                        record.to_json_string(),
                        metadata.ingested_at_low_watermark(),
                        metadata.ingested_at_high_watermark()
                    )
                })
                .collect::<Vec<_>>();
            format_materialized_stream_state_output(&show.relay, materializer, entry_lines)
        };
        command_ok(message)
    }

    async fn describe_resource(&self, describe: DescribeResource) -> CommandResult {
        if describe.version.is_none() {
            let resources = self.consensus.current_resources().await;
            let exists = resources
                .next_version_by_identifier
                .iter()
                .any(|(identifier, _)| identifier == &describe.identifier)
                || resources
                    .versions
                    .iter()
                    .any(|resource| resource.id.identifier == describe.identifier);
            if !exists {
                return command_error(format!(
                    "resource '{}' does not exist",
                    describe.identifier.as_str()
                ));
            }
            let versions = resources
                .versions
                .iter()
                .filter(|resource| resource.id.identifier == describe.identifier)
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

        let version = describe.version.expect("checked above");
        let resources = self.consensus.current_resources().await;
        let Some(resource) = resources
            .versions
            .iter()
            .find(|resource| {
                resource.id.identifier == describe.identifier && resource.id.version == version
            })
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
            .filter(|replica| {
                replica.key.identifier == describe.identifier && replica.key.version == version
            })
            .cloned()
            .collect::<Vec<_>>();
        let gossip = self.cluster.gossip_state().await;
        let live_node_ids = gossip
            .live_nodes
            .iter()
            .map(|node| node.node_id.clone())
            .collect::<BTreeSet<_>>();
        let mut live_node_ids = live_node_ids;
        if live_node_ids.is_empty() {
            live_node_ids.insert(self.consensus.local_node_id().to_string());
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
                let state = replica.map_or_else(
                    || {
                        if live_node_ids.contains(&node_id) {
                            "pending"
                        } else {
                            "untracked"
                        }
                    },
                    |replica| replica.state.as_ref(),
                );
                lines.push(format!(
                    "- {} topology={} state={} checksum={} verified_at={} source={} error={}",
                    node_id,
                    topology,
                    state,
                    replica
                        .and_then(|replica| replica.root_checksum.as_deref())
                        .unwrap_or("-"),
                    replica
                        .and_then(|replica| replica.last_verified_at)
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    replica
                        .and_then(|replica| replica.source_node_id.as_deref())
                        .unwrap_or("-"),
                    replica
                        .and_then(|replica| replica.error.as_deref())
                        .unwrap_or("-"),
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
        match self
            .resource_store
            .read_manifest(&resource.id.identifier, resource.id.version)
        {
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
        let entry_type = match entry.entry_type {
            ResourceEntryType::File => "file",
            ResourceEntryType::Directory => "directory",
        };
        let checksum = if entry.checksum.is_empty() {
            "-"
        } else {
            entry.checksum.as_str()
        };
        format!(
            "  - type={} path={} size={} checksum={}",
            entry_type, entry.path, entry.size, checksum
        )
    }

    async fn wait_for_resource_cluster_ready(
        &self,
        identifier: &Identifier,
        version: u64,
    ) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            tokio::task::consume_budget().await;
            let resources = self.consensus.current_resources().await;
            let replicas = resources
                .replicas
                .iter()
                .filter(|replica| {
                    replica.key.identifier == *identifier && replica.key.version == version
                })
                .collect::<Vec<_>>();
            let gossip = self.cluster.gossip_state().await;
            let live_node_ids = gossip
                .live_nodes
                .iter()
                .map(|node| node.node_id.as_str())
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
                    identifier.as_str(),
                    version
                ));
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn publish_domain_schedule(
        &self,
        domain: &Domain,
        graph: Option<ActiveGraph>,
    ) -> Result<(), String> {
        let live_node_ids = self.cluster.live_node_ids().await;
        let live_voters = self.consensus.live_voter_ids(live_node_ids.clone()).await;
        let cluster_nodes = self
            .consensus
            .schedulable_live_voter_ids(live_node_ids)
            .await;
        let schedule = match graph {
            Some(graph) => {
                let mut schedule =
                    graph.schedule_for_domain(domain, &cluster_nodes, self.replica_count);
                let current = self.consensus.current_schedule().await;
                Self::merge_existing_schedule_data(
                    &mut schedule,
                    current.domain(domain),
                    &live_voters,
                );
                Some(schedule)
            }
            None => None,
        };
        self.consensus
            .replace_domain_schedule(domain.clone(), schedule)
            .await
            .map_err(|error| error.to_string())
    }

    async fn drop_node(&self, node_id: String) -> CommandResult {
        let gossip = self.cluster.gossip_state().await;
        let is_live = gossip
            .live_nodes
            .iter()
            .any(|live_node| live_node.node_id == node_id);
        if is_live && !gossip.dead_node_ids.contains(&node_id) {
            return command_error(format!(
                "cannot drop live node '{node_id}'; stop the node before removing it"
            ));
        }

        let membership_nodes = self.consensus.membership_nodes().await;
        if membership_nodes.contains_key(&node_id) {
            let voters = self.consensus.membership_voter_ids().await;
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

        let current_schedule = self.consensus.current_schedule().await;
        match self.consensus.drop_node(&node_id).await {
            Ok(()) => {}
            Err(error) => {
                return command_error(format!("failed to drop node '{node_id}': {error}"));
            }
        }

        let live_node_ids = self.cluster.live_node_ids().await;
        let cluster_nodes = self.consensus.live_voter_ids(live_node_ids).await;
        for (domain, graph) in self.registry.active_graphs() {
            let mut schedule =
                graph.schedule_for_domain(&domain, &cluster_nodes, self.replica_count);
            Self::merge_existing_schedule_data(
                &mut schedule,
                current_schedule.domain(&domain),
                &cluster_nodes,
            );
            if let Err(error) = self
                .consensus
                .replace_domain_schedule(domain.clone(), Some(schedule))
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

    fn drop_node_quorum_error(
        node_id: &str,
        voters: &BTreeSet<String>,
        live_node_ids: &BTreeSet<String>,
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

    async fn set_node_cordoned(&self, node_id: String, cordoned: bool) -> CommandResult {
        let membership = self.consensus.membership_nodes().await;
        if !membership.contains_key(&node_id) {
            return command_error(format!("node '{node_id}' is not a raft member"));
        }

        if let Err(error) = self
            .consensus
            .set_node_cordoned(node_id.clone(), cordoned)
            .await
        {
            let action = if cordoned { "cordon" } else { "uncordon" };
            return command_error(format!("failed to {action} node '{node_id}': {error}"));
        }

        let action = if cordoned { "cordoned" } else { "uncordoned" };
        command_ok(format!("{action} node '{node_id}'"))
    }

    async fn drain_node(&self, node_id: String) -> CommandResult {
        let membership = self.consensus.membership_nodes().await;
        if !membership.contains_key(&node_id) {
            return command_error(format!("node '{node_id}' is not a raft member"));
        }

        if let Err(error) = self
            .consensus
            .set_node_cordoned(node_id.clone(), true)
            .await
        {
            return command_error(format!(
                "failed to cordon node '{node_id}' before drain: {error}"
            ));
        }

        let mut moved = 0usize;
        loop {
            let live_node_ids = self.cluster.live_node_ids().await;
            let replacement_nodes = self
                .consensus
                .schedulable_live_voter_ids(live_node_ids)
                .await;
            if replacement_nodes.is_empty() {
                return command_error(format!(
                    "cannot drain node '{node_id}': no live schedulable raft voters remain"
                ));
            }
            let replacement_node_set = replacement_nodes.iter().cloned().collect::<BTreeSet<_>>();
            let current_schedule = self.consensus.current_schedule().await;
            let mut moved_this_iteration = false;

            for (domain, graph) in self.registry.active_graphs() {
                let desired =
                    graph.schedule_for_domain(&domain, &replacement_nodes, self.replica_count);
                let Some(current_domain) = current_schedule.domain(&domain) else {
                    if let Err(error) = self
                        .consensus
                        .replace_domain_schedule(domain.clone(), Some(desired))
                        .await
                    {
                        return command_error(format!(
                            "cordoned node '{node_id}', but failed to publish schedule for domain \
                             '{}': {error}",
                            domain.as_str()
                        ));
                    }
                    moved_this_iteration = true;
                    break;
                };

                let mut next = current_domain.clone();
                let Some(drain_move) = Self::move_next_scheduled_node_for_drain(
                    &mut next,
                    &desired,
                    &node_id,
                    &replacement_node_set,
                ) else {
                    continue;
                };
                moved += 1;
                if let Some(replica) = drain_move.promoted_replica.as_deref() {
                    info!(
                        domain = domain.as_str(),
                        node = drain_move.label,
                        drained_node = node_id,
                        promoted_replica = replica,
                        "drain promoted live replica to primary"
                    );
                } else if let Some(fallback_node) = drain_move.fallback_node.as_deref() {
                    warn!(
                        domain = domain.as_str(),
                        node = drain_move.label,
                        drained_node = node_id,
                        fallback_node,
                        "drain found no live replica; moving scheduled node without local \
                         replicated state"
                    );
                }
                if let Err(error) = self
                    .consensus
                    .replace_domain_schedule(domain.clone(), Some(next))
                    .await
                {
                    return command_error(format!(
                        "cordoned node '{node_id}', but failed to move scheduled graph node {} \
                         for domain '{}': {error}",
                        moved,
                        domain.as_str()
                    ));
                }
                moved_this_iteration = true;
                break;
            }

            if !moved_this_iteration {
                break;
            }
        }

        command_ok(format!(
            "drained node '{node_id}' (moved {moved} scheduled graph node(s))"
        ))
    }

    async fn drain_local_node_before_shutdown(&self) {
        let local_node_id = self.consensus.local_node_id().to_string();
        let live_node_ids = self.cluster.live_node_ids().await;
        let drain_targets = self
            .consensus
            .schedulable_live_voter_ids(live_node_ids)
            .await;
        if !drain_targets
            .iter()
            .any(|node_id| node_id != &local_node_id)
        {
            warn!(
                node_id = local_node_id,
                "skipping graceful shutdown drain: no live schedulable replacement nodes remain"
            );
            return;
        }
        let leader = self.consensus.current_leader().await;
        match leader.as_deref() {
            Some(leader_id) if leader_id == local_node_id => {
                let result = self.drain_node(local_node_id.clone()).await;
                if result.success {
                    info!(
                        node_id = local_node_id,
                        message = result.message,
                        "drained local node before graceful shutdown"
                    );
                    self.uncordon_local_node_after_shutdown_drain(&local_node_id)
                        .await;
                } else {
                    warn!(
                        node_id = local_node_id,
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
                        node_id = local_node_id,
                        leader = leader_id,
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
                        self.configured_basic_auth.as_ref(),
                    ),
                )
                .await
                {
                    Ok(client) => {
                        match client.execute(format!("DRAIN NODE {local_node_id};")).await {
                            Ok(outcome) if outcome.success => {
                                info!(
                                    node_id = local_node_id,
                                    leader = leader_id,
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
                                    node_id = local_node_id,
                                    leader = leader_id,
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
                                    node_id = local_node_id,
                                    leader = leader_id,
                                    error = %error,
                                    "failed to drain local node through leader before graceful shutdown"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        warn!(
                            node_id = local_node_id,
                            leader = leader_id,
                            leader_grpc_uri,
                            error = %error,
                            "failed to connect to leader for graceful shutdown drain"
                        );
                    }
                }
            }
            None => {
                warn!(
                    node_id = local_node_id,
                    "failed to drain local node before graceful shutdown: raft leader is unknown"
                );
            }
        }
    }

    async fn uncordon_local_node_after_shutdown_drain(&self, local_node_id: &str) {
        match self
            .consensus
            .set_node_cordoned(local_node_id.to_string(), false)
            .await
        {
            Ok(()) => {
                info!(
                    node_id = local_node_id,
                    "cleared shutdown drain cordon before graceful shutdown"
                );
            }
            Err(error) => {
                warn!(
                    node_id = local_node_id,
                    error = %error,
                    "failed to clear shutdown drain cordon before graceful shutdown"
                );
            }
        }
    }

    async fn uncordon_local_node_through_leader_after_shutdown_drain(
        &self,
        client: &NervixClient,
        local_node_id: &str,
        leader_id: &str,
    ) {
        match client
            .execute(format!("UNCORDON NODE {local_node_id};"))
            .await
        {
            Ok(outcome) if outcome.success => {
                info!(
                    node_id = local_node_id,
                    leader = leader_id,
                    message = outcome.message,
                    "cleared shutdown drain cordon through leader"
                );
            }
            Ok(outcome) => {
                warn!(
                    node_id = local_node_id,
                    leader = leader_id,
                    message = outcome.message,
                    "failed to clear shutdown drain cordon through leader"
                );
            }
            Err(error) => {
                warn!(
                    node_id = local_node_id,
                    leader = leader_id,
                    error = %error,
                    "failed to clear shutdown drain cordon through leader"
                );
            }
        }
    }

    async fn leader_grpc_uri(&self, leader_id: &str) -> Option<String> {
        self.cluster
            .gossip_state()
            .await
            .live_nodes
            .into_iter()
            .find(|node| node.node_id == leader_id)
            .and_then(|node| grpc_uri_from_advertise_addr(&node.grpc_advertise_addr))
    }

    fn move_next_scheduled_node_for_drain(
        schedule: &mut nervix_models::DomainSchedule,
        desired: &nervix_models::DomainSchedule,
        node_id: &str,
        replacement_nodes: &BTreeSet<String>,
    ) -> Option<DrainMove> {
        for node in &mut schedule.nodes {
            if !node.is_assigned_to(node_id) {
                continue;
            }

            let Some(desired_node) = desired.nodes.iter().find(|candidate| {
                candidate.kind == node.kind && candidate.identifier == node.identifier
            }) else {
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

            return Self::relocate_scheduled_node_assignment(
                node,
                desired_node,
                node_id,
                replacement_nodes,
            );
        }
        None
    }

    fn relocate_scheduled_node_assignment(
        node: &mut ScheduledNode,
        desired_node: &ScheduledNode,
        unavailable_node_id: &str,
        replacement_nodes: &BTreeSet<String>,
    ) -> Option<DrainMove> {
        if !node.is_assigned_to(unavailable_node_id) {
            return None;
        }
        if desired_node.assigned_nodes.is_empty()
            || desired_node
                .assigned_nodes
                .iter()
                .any(|assigned| assigned == unavailable_node_id)
        {
            return None;
        }

        let label = format!("{} {}", node.kind.as_ref(), node.identifier.as_str());
        let live_replicas = node
            .assigned_nodes
            .iter()
            .filter(|assigned| assigned.as_str() != unavailable_node_id)
            .filter(|assigned| replacement_nodes.contains(*assigned))
            .cloned()
            .collect::<Vec<_>>();

        if let Some(replica) = live_replicas.first().cloned() {
            let mut assigned_nodes = vec![replica.clone()];
            for assigned in live_replicas.into_iter().skip(1) {
                if !assigned_nodes.contains(&assigned) {
                    assigned_nodes.push(assigned);
                }
            }
            for assigned in &desired_node.assigned_nodes {
                if assigned != unavailable_node_id
                    && replacement_nodes.contains(assigned)
                    && !assigned_nodes.contains(assigned)
                {
                    assigned_nodes.push(assigned.clone());
                }
            }
            node.primary_node = Some(replica.clone());
            node.assigned_nodes = assigned_nodes;
            return Some(DrainMove {
                label,
                promoted_replica: Some(replica),
                fallback_node: None,
            });
        }

        let assigned_nodes = desired_node
            .assigned_nodes
            .iter()
            .filter(|assigned| assigned.as_str() != unavailable_node_id)
            .filter(|assigned| replacement_nodes.contains(*assigned))
            .cloned()
            .collect::<Vec<_>>();
        let fallback_node = assigned_nodes.first().cloned()?;
        node.primary_node = Some(fallback_node.clone());
        node.assigned_nodes = assigned_nodes;
        Some(DrainMove {
            label,
            promoted_replica: None,
            fallback_node: Some(fallback_node),
        })
    }

    fn failover_unavailable_scheduled_nodes(
        schedule: &mut nervix_models::DomainSchedule,
        replacement_nodes: &BTreeSet<String>,
    ) -> Vec<DrainMove> {
        let mut moves = Vec::new();
        if replacement_nodes.is_empty() {
            return moves;
        }

        for node in &mut schedule.nodes {
            if node.assigned_nodes.is_empty() {
                continue;
            }
            let unavailable_node_ids = node
                .assigned_nodes
                .iter()
                .filter(|node_id| !replacement_nodes.contains(*node_id))
                .cloned()
                .collect::<Vec<_>>();
            if unavailable_node_ids.is_empty() {
                continue;
            }

            let replica_slots = node.assigned_nodes.len().max(1);
            let mut desired_node = node.clone();
            desired_node.assigned_nodes = replacement_nodes
                .iter()
                .take(replica_slots)
                .cloned()
                .collect();
            desired_node.primary_node = desired_node.assigned_nodes.first().cloned();

            for unavailable_node_id in unavailable_node_ids {
                if let Some(failover_move) = Self::relocate_scheduled_node_assignment(
                    node,
                    &desired_node,
                    &unavailable_node_id,
                    replacement_nodes,
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
        live_node_ids: &[String],
    ) {
        let Some(existing) = existing else {
            return;
        };
        let live_node_ids = live_node_ids.iter().cloned().collect::<BTreeSet<_>>();
        for node in &mut schedule.nodes {
            let Some(existing_node) = existing.nodes.iter().find(|candidate| {
                candidate.kind == node.kind && candidate.identifier == node.identifier
            }) else {
                continue;
            };
            node.kafka_partition_schedule = existing_node.kafka_partition_schedule.clone();
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
    }

    fn scheduled_node_should_follow_desired_assignment(node: &ScheduledNode) -> bool {
        if let Model::Ingestor(CreateIngestor {
            source: IngestSource::Endpoint { .. } | IngestSource::Websockets { .. },
            ..
        }) = node.config.as_ref()
        {
            return true;
        }
        false
    }

    fn kafka_partition_watcher_specs(
        &self,
        schedule: &nervix_models::ClusterSchedule,
    ) -> Vec<KafkaPartitionWatcherSpec> {
        let mut specs = Vec::new();
        for domain_schedule in &schedule.domains {
            for node in &domain_schedule.nodes {
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
                let Some(client_node) = domain_schedule.nodes.iter().find(|candidate| {
                    candidate.kind == ModelKind::Client && candidate.identifier == *client
                }) else {
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
        domain: &Domain,
        ingestor: &Identifier,
        topic: &str,
        instances: u64,
        observed_partitions: Vec<i32>,
    ) -> Result<(), String> {
        let leader = self.consensus.current_leader().await;
        if leader.as_deref() != Some(self.consensus.local_node_id()) {
            return Ok(());
        }

        let current = self.consensus.current_schedule().await;
        let Some(existing_domain_schedule) = current.domain(domain) else {
            return Ok(());
        };
        let mut next_domain_schedule = existing_domain_schedule.clone();
        let Some(ingestor_node) = next_domain_schedule
            .nodes
            .iter_mut()
            .find(|node| node.kind == ModelKind::Ingestor && node.identifier == *ingestor)
        else {
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
            next_schedule.rebalance_epoch = existing_schedule.rebalance_epoch.saturating_add(1);
        }
        ingestor_node.kafka_partition_schedule = Some(next_schedule);
        self.consensus
            .replace_domain_schedule(domain.clone(), Some(next_domain_schedule))
            .await
            .map_err(|error| error.to_string())
    }

    async fn reconcile_kafka_partition_watchers(
        &self,
        schedule: &nervix_models::ClusterSchedule,
        tasks: &mut HashMap<
            KafkaPartitionWatcherKey,
            (KafkaPartitionWatcherSpec, CancellationToken, JoinHandle<()>),
        >,
    ) {
        let leader = self.consensus.current_leader().await;
        if leader.as_deref() != Some(self.consensus.local_node_id()) {
            for (_, (_, cancel, handle)) in tasks.drain() {
                cancel.cancel();
                let _ = handle.await;
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

        let stale_keys = tasks
            .iter()
            .filter_map(|(key, (spec, _, _))| {
                desired
                    .get(key)
                    .filter(|desired_spec| *desired_spec == spec)
                    .is_none()
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        for key in stale_keys {
            if let Some((_, cancel, handle)) = tasks.remove(&key) {
                cancel.cancel();
                let _ = handle.await;
            }
        }

        for (key, spec) in desired {
            if tasks
                .get(&key)
                .is_some_and(|(existing, _, _)| existing == &spec)
            {
                continue;
            }
            let cancel = CancellationToken::new();
            let cancel_child = cancel.clone();
            let service = self.clone();
            let spec_for_task = spec.clone();
            let handle = tokio::spawn(async move {
                let resolved = match service.runtime.resolve_client_config(
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
                client_config.set("auto.offset.reset", "earliest");
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
                                _ = service.shutdown.cancelled() => break,
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
                        _ = service.shutdown.cancelled() => break,
                        _ = cancel_child.cancelled() => break,
                        _ = sleep(LEADER_KAFKA_PARTITION_WATCH_INTERVAL) => {}
                    }
                }
            });
            tasks.insert(key, (spec, cancel, handle));
        }
    }

    async fn subscription_target_from_schedule(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<
        Option<(
            nervix_models::CreateRelay,
            nervix_models::CreateSchema,
            Vec<Identifier>,
        )>,
        String,
    > {
        let schedule = self.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(relay_node) = domain_schedule
            .nodes
            .iter()
            .find(|node| node.kind == ModelKind::Relay && node.identifier == *relay)
        else {
            return Ok(None);
        };
        let Model::Relay(ack_model) = relay_node.config.as_ref() else {
            return Err("scheduled relay node has invalid model kind".to_string());
        };
        let Some(schema_node) = domain_schedule
            .nodes
            .iter()
            .find(|node| node.kind == ModelKind::Schema && node.identifier == ack_model.schema)
        else {
            return Err(format!(
                "stream '{}' references missing scheduled schema '{}'",
                relay.as_str(),
                ack_model.schema.as_str()
            ));
        };
        let Model::Schema(schema) = schema_node.config.as_ref() else {
            return Err("scheduled schema node has invalid model kind".to_string());
        };
        Ok(Some((
            ack_model.clone(),
            schema.clone(),
            relay_node
                .effective_parameterization
                .clone()
                .unwrap_or_default(),
        )))
    }

    async fn subscription_stream_schema(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<Option<nervix_models::CreateSchema>, String> {
        match self.registry.get(domain, ModelKind::Relay, relay) {
            Ok(Some(Model::Relay(ack_model))) => {
                match self
                    .registry
                    .get(domain, ModelKind::Schema, &ack_model.schema)
                {
                    Ok(Some(Model::Schema(schema))) => Ok(Some(schema)),
                    Ok(Some(_)) => Err(format!(
                        "stream '{}' references non-schema model '{}'",
                        relay.as_str(),
                        ack_model.schema.as_str()
                    )),
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
            Ok(Some(_)) => unreachable!("validated relay model kind must match"),
            Ok(None) => self
                .subscription_target_from_schedule(domain, relay)
                .await
                .map(|resolved| resolved.map(|(_, schema, _)| schema)),
            Err(err) => Err(format!(
                "failed to resolve relay '{}' for subscription: {err}",
                relay.as_str()
            )),
        }
    }

    async fn subscription_branch_schema(
        &self,
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<Option<Arc<arrow_schema::Schema>>, String> {
        match self.registry.get(domain, ModelKind::Relay, relay) {
            Ok(Some(Model::Relay(relay_model))) => {
                let Some(parameter_schema) = relay_model.parameterization.parameterized_by() else {
                    return Ok(None);
                };
                match self
                    .registry
                    .get(domain, ModelKind::Schema, parameter_schema)
                {
                    Ok(Some(Model::Schema(schema))) => {
                        Ok(Some(runtime_schema::compile_schema(&schema).arrow_schema()))
                    }
                    Ok(Some(_)) => Err(format!(
                        "stream '{}' references non-schema branch parameterization '{}'",
                        relay.as_str(),
                        parameter_schema.as_str()
                    )),
                    Ok(None) => Err(format!(
                        "stream '{}' references missing branch parameterization schema '{}'",
                        relay.as_str(),
                        parameter_schema.as_str()
                    )),
                    Err(err) => Err(format!(
                        "failed to resolve branch parameterization schema '{}' for relay '{}': \
                         {err}",
                        parameter_schema.as_str(),
                        relay.as_str()
                    )),
                }
            }
            Ok(Some(_)) => unreachable!("validated relay model kind must match"),
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
        domain: &Domain,
        relay: &Identifier,
    ) -> Result<Option<Arc<arrow_schema::Schema>>, String> {
        let schedule = self.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(relay_node) = domain_schedule
            .nodes
            .iter()
            .find(|node| node.kind == ModelKind::Relay && node.identifier == *relay)
        else {
            return Ok(None);
        };
        let Model::Relay(relay_model) = relay_node.config.as_ref() else {
            return Err("scheduled relay node has invalid model kind".to_string());
        };
        if let Some(parameter_schema) = relay_model.parameterization.parameterized_by() {
            let Some(schema_node) = domain_schedule.nodes.iter().find(|node| {
                node.kind == ModelKind::Schema && node.identifier == *parameter_schema
            }) else {
                return Err(format!(
                    "stream '{}' references missing scheduled branch parameterization schema '{}'",
                    relay.as_str(),
                    parameter_schema.as_str()
                ));
            };
            let Model::Schema(schema) = schema_node.config.as_ref() else {
                return Err(
                    "scheduled branch parameterization node has invalid model kind".to_string(),
                );
            };
            return Ok(Some(runtime_schema::compile_schema(schema).arrow_schema()));
        }

        let parameterization = relay_node
            .effective_parameterization
            .as_deref()
            .unwrap_or_default();
        if parameterization.is_empty() {
            return Ok(None);
        }
        let Some(schema_node) = domain_schedule
            .nodes
            .iter()
            .find(|node| node.kind == ModelKind::Schema && node.identifier == relay_model.schema)
        else {
            return Err(format!(
                "stream '{}' references missing scheduled schema '{}'",
                relay.as_str(),
                relay_model.schema.as_str()
            ));
        };
        let Model::Schema(schema) = schema_node.config.as_ref() else {
            return Err("scheduled relay schema node has invalid model kind".to_string());
        };
        let mut fields = Vec::with_capacity(parameterization.len());
        for parameter in parameterization {
            let Some(field) = schema.fields.iter().find(|field| field.name == *parameter) else {
                return Err(format!(
                    "stream '{}' inferred branch field '{}' from its parameterization, but the \
                     field is missing from schema '{}'",
                    relay.as_str(),
                    parameter.as_str(),
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
        domain: &Domain,
    ) -> Result<
        (
            HashMap<Identifier, RuntimeMaterializedRelaySpec>,
            HashMap<Identifier, Option<String>>,
        ),
        String,
    > {
        let schedule = self.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok((HashMap::default(), HashMap::default()));
        };

        let mut specs = HashMap::default();
        let mut owners = HashMap::default();
        for relay_node in domain_schedule
            .nodes
            .iter()
            .filter(|node| node.kind == ModelKind::Relay)
        {
            let Model::Relay(ack_model) = relay_node.config.as_ref() else {
                continue;
            };
            if ack_model.materialized_state.is_none() {
                continue;
            }
            let Some(schema_node) = domain_schedule
                .nodes
                .iter()
                .find(|node| node.kind == ModelKind::Schema && node.identifier == ack_model.schema)
            else {
                return Err(format!(
                    "stream '{}' references missing scheduled schema '{}'",
                    ack_model.name.as_str(),
                    ack_model.schema.as_str()
                ));
            };
            let Model::Schema(schema) = schema_node.config.as_ref() else {
                return Err("scheduled schema node has invalid model kind".to_string());
            };
            specs.insert(
                ack_model.name.clone(),
                RuntimeMaterializedRelaySpec {
                    schema: runtime_schema::compile_schema(schema).arrow_schema(),
                    sensitivity: runtime_schema::compile_schema(schema).vm_sensitivity(),
                    parameterization: relay_node
                        .effective_parameterization
                        .clone()
                        .unwrap_or_default(),
                },
            );
            owners.insert(ack_model.name.clone(), None);
        }
        for node in domain_schedule
            .nodes
            .iter()
            .filter(|node| node.kind == ModelKind::Materializer)
        {
            owners.insert(
                node.identifier.clone(),
                node.primary_node().map(str::to_string),
            );
        }

        Ok((specs, owners))
    }

    async fn lookup_target_from_schedule(
        &self,
        domain: &Domain,
        name: &Identifier,
    ) -> Result<Option<(CreateLookup, ScheduledNode, ParseAsType)>, String> {
        let schedule = self.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(lookup_node) = domain_schedule
            .nodes
            .iter()
            .find(|node| node.kind == ModelKind::Lookup && node.identifier == *name)
        else {
            return Ok(None);
        };
        let Model::Lookup(lookup) = lookup_node.config.as_ref() else {
            return Err("scheduled lookup node has invalid model kind".to_string());
        };
        let Some(codec_node) = domain_schedule.nodes.iter().find(|node| {
            node.kind == ModelKind::Codec && node.identifier == lookup.decode_using_codec
        }) else {
            return Err(format!(
                "lookup '{}' references missing scheduled codec '{}'",
                name.as_str(),
                lookup.decode_using_codec.as_str()
            ));
        };
        let Model::Codec(codec) = codec_node.config.as_ref() else {
            return Err("scheduled codec node has invalid model kind".to_string());
        };
        let Some(schema_node) = domain_schedule
            .nodes
            .iter()
            .find(|node| node.kind == ModelKind::Schema && node.identifier == codec.schema)
        else {
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
        Ok(Some((
            lookup.clone(),
            lookup_node.clone(),
            field.ty.clone(),
        )))
    }

    async fn ingestor_target_from_schedule(
        &self,
        domain: &Domain,
        name: &Identifier,
    ) -> Result<Option<(CreateIngestor, ScheduledNode)>, String> {
        let schedule = self.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(ingestor_node) = domain_schedule
            .nodes
            .iter()
            .find(|node| node.kind == ModelKind::Ingestor && node.identifier == *name)
        else {
            return Ok(None);
        };
        let Model::Ingestor(ingestor) = ingestor_node.config.as_ref() else {
            return Err("scheduled ingestor node has invalid model kind".to_string());
        };
        Ok(Some((ingestor.clone(), ingestor_node.clone())))
    }

    async fn subscribe_session(
        &self,
        domain: &Domain,
        subscription: nervix_models::SubscribeSession,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let definition = session_subscription_definition(
            &subscription.relay,
            subscription.delivery_behavior,
            subscription.batch_sample_rate.as_deref(),
            subscription.filter_map.as_deref(),
        );
        if subscriptions.subscriptions.contains_key(&definition) {
            return CommandResult {
                success: false,
                message: format!("session subscription '{}' already exists", definition),
                diagnostics: vec![Diagnostic {
                    message: format!("session subscription '{}' already exists", definition),
                    span_start: 0,
                    span_end: 0,
                }],
                kind: CommandResultKind::Error as i32,
                ..Default::default()
            };
        }

        let batch_sample_rate =
            match parse_subscription_batch_sample_rate(subscription.batch_sample_rate.as_deref()) {
                Ok(rate) => rate,
                Err(err) => {
                    return CommandResult {
                        success: false,
                        message: format!("failed to subscribe session {}: {err}", definition),
                        diagnostics: vec![Diagnostic {
                            message: err,
                            span_start: 0,
                            span_end: 0,
                        }],
                        kind: CommandResultKind::Error as i32,
                        ..Default::default()
                    };
                }
            };

        match self
            .registry
            .get(domain, ModelKind::Relay, &subscription.relay)
        {
            Ok(Some(Model::Relay(_))) => {}
            Ok(Some(_)) => unreachable!("validated relay model kind must match"),
            Ok(None) => match self
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
                        kind: CommandResultKind::Error as i32,
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
                        kind: CommandResultKind::Error as i32,
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
                    kind: CommandResultKind::Error as i32,
                    ..Default::default()
                };
            }
        }

        let relay_parameterization = self
            .subscription_target_from_schedule(domain, &subscription.relay)
            .await
            .ok()
            .flatten()
            .map(|(_, _, parameterization)| parameterization)
            .unwrap_or_default();
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
                    kind: CommandResultKind::Error as i32,
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
                        kind: CommandResultKind::Error as i32,
                        ..Default::default()
                    };
                }
            };
        let (filter_map, subscription_sensitivity) = match self
            .subscription_stream_schema(domain, &subscription.relay)
            .await
        {
            Ok(Some(schema)) => {
                let schema = runtime_schema::compile_schema(&schema);
                let input_sensitivity = schema.vm_sensitivity();
                let filter_map = match compile_session_filter_map_program(
                    domain,
                    &subscription.relay,
                    std::slice::from_ref(&subscription.relay),
                    subscription.filter_map.as_deref(),
                    schema.arrow_schema(),
                    input_sensitivity.clone(),
                    RuntimeVmCompileContext {
                        available_materialized_streams: &materialized_stream_specs,
                        available_lookups: &HashMap::default(),
                        current_parameterization: &relay_parameterization,
                        current_branch_schema: relay_branch_schema.as_ref(),
                        current_branch_sensitivity: None,
                    },
                ) {
                    Ok(filter_map) => filter_map,
                    Err(err) => {
                        return CommandResult {
                            success: false,
                            message: format!(
                                "failed to compile session subscription '{}': {err}",
                                definition
                            ),
                            diagnostics: vec![Diagnostic {
                                message: format!(
                                    "failed to compile session subscription '{}': {err}",
                                    definition
                                ),
                                span_start: 0,
                                span_end: 0,
                            }],
                            kind: CommandResultKind::Error as i32,
                            ..Default::default()
                        };
                    }
                };
                let sensitivity = filter_map
                    .as_ref()
                    .map(|filter_map| filter_map.output_sensitivity.clone())
                    .unwrap_or(input_sensitivity);
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
                    kind: CommandResultKind::Error as i32,
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
                    kind: CommandResultKind::Error as i32,
                    ..Default::default()
                };
            }
        };

        let relay = subscription.relay.clone();
        let subscribe_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let receiver = loop {
            match self.runtime.subscribe_stream(domain, &relay).await {
                Ok(receiver) => break receiver,
                Err(err) => {
                    if let crate::runtime::RuntimeError::RelayNotInstantiated { .. } = err
                        && tokio::time::Instant::now() < subscribe_deadline
                    {
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    return CommandResult {
                        success: false,
                        message: format!(
                            "failed to subscribe to relay '{}': {err}",
                            relay.as_str()
                        ),
                        diagnostics: vec![Diagnostic {
                            message: format!(
                                "failed to subscribe to relay '{}': {err}",
                                relay.as_str()
                            ),
                            span_start: 0,
                            span_end: 0,
                        }],
                        kind: CommandResultKind::Error as i32,
                        ..Default::default()
                    };
                }
            }
        };

        self.register_subscription_interest(domain, &relay).await;
        subscriptions.insert(
            domain.clone(),
            definition.clone(),
            relay.clone(),
            SessionSubscriptionTaskConfig {
                filter_map,
                sensitivity: subscription_sensitivity,
                delivery_behavior: subscription.delivery_behavior,
                batch_sample_rate,
                runtime: self.runtime.clone(),
                materialized_stream_owner_nodes,
                receiver,
                tx: tx.clone(),
            },
        );

        CommandResult {
            success: true,
            message: format!(
                "subscribed session {} in domain '{}'",
                definition,
                domain.as_str()
            ),
            diagnostics: Vec::new(),
            kind: CommandResultKind::Ok as i32,
            ..Default::default()
        }
    }

    async fn unsubscribe_session(
        &self,
        domain: &Domain,
        subscription: nervix_models::UnsubscribeSession,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let definition = session_subscription_definition(
            &subscription.relay,
            subscription.delivery_behavior,
            subscription.batch_sample_rate.as_deref(),
            subscription.filter_map.as_deref(),
        );
        match subscriptions.remove(&definition).await {
            Some((subscription_domain, relay, removed_definition)) => {
                if !subscriptions.contains_domain_stream(&subscription_domain, &relay) {
                    self.unregister_subscription_interest(&subscription_domain, &relay)
                        .await;
                }
                CommandResult {
                    success: true,
                    message: format!(
                        "unsubscribed session {} in domain '{}'",
                        removed_definition,
                        domain.as_str()
                    ),
                    diagnostics: Vec::new(),
                    kind: CommandResultKind::Ok as i32,
                    ..Default::default()
                }
            }
            None => CommandResult {
                success: false,
                message: format!("session subscription '{}' does not exist", definition),
                diagnostics: vec![Diagnostic {
                    message: format!("session subscription '{}' not found", definition),
                    span_start: 0,
                    span_end: 0,
                }],
                kind: CommandResultKind::Error as i32,
                ..Default::default()
            },
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RequestDomainError {
    Missing,
    Invalid,
}

fn parse_request_domain(raw: &str) -> Result<Domain, RequestDomainError> {
    if raw.trim().is_empty() {
        Err(RequestDomainError::Missing)
    } else {
        Domain::parse(raw.trim()).map_err(|_| RequestDomainError::Invalid)
    }
}

fn runtime_ingestor_describe_to_envelope(
    summary: RuntimeIngestorDescribe,
    metrics: Vec<String>,
) -> IngestorDescribeEnvelope {
    IngestorDescribeEnvelope {
        running: summary.running,
        ready: summary.ready,
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

fn dataflow_node_status_from_envelope(
    envelope: DataflowNodeStatusEnvelope,
) -> (DataflowNodeStatus, Option<String>, Option<u64>) {
    let status = if envelope.status.eq_ignore_ascii_case("ERROR") {
        DataflowNodeStatus::Error
    } else {
        DataflowNodeStatus::Ok
    };
    (status, envelope.detail, envelope.reconnect_wait_millis)
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
        IngestSource::Kinesis { .. } => "KINESIS",
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
    }
}

fn format_endpoint_describe_output(name: &Identifier, endpoint: &CreateEndpoint) -> String {
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
    name: &Identifier,
    ingestor: &CreateIngestor,
    ingestor_node: &ScheduledNode,
    summary: &RuntimeIngestorDescribe,
) -> String {
    let mut lines = vec![
        format!("ingestor: {}", name.as_str()),
        "kind: INGESTOR".to_string(),
        format!("source: {}", format_ingestor_source(&ingestor.source)),
        format!(
            "streams: {}",
            ingestor
                .output_routes
                .relays()
                .map(Identifier::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!("codec: {}", ingestor.decode_using_codec.as_str()),
        format!("owner: {}", ingestor_node.execution_node().unwrap_or("-"),),
        format!(
            "timestamp: {}",
            format_timestamp_source(ingestor.timestamp_source.as_ref())
        ),
        format!(
            "branch ttl: {}",
            ingestor.parameterized_by.ttl().unwrap_or("-")
        ),
        format!(
            "status: {}",
            if summary.running {
                "running"
            } else {
                "stopped"
            }
        ),
        format!("ready: {}", if summary.ready { "true" } else { "false" }),
    ];
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
    lines.push(format!(
        "reconnect wait: {}",
        summary
            .reconnect_wait_millis
            .map(format_millis_duration)
            .unwrap_or_else(|| "-".to_string())
    ));

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
            for instance_idx in 0..*instances {
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
    parameterization: &[Identifier],
) -> String {
    let mut lines = vec![
        format!("relay: {}", relay.name.as_str()),
        "kind: RELAY".to_string(),
        format!("schema: {}", relay.schema.as_str()),
        format!(
            "parameterized by: {}",
            relay
                .parameterization
                .parameterized_by()
                .map(Identifier::as_str)
                .unwrap_or_else(|| {
                    if relay.parameterization.is_unparameterized() {
                        "UNPARAMETERIZED"
                    } else {
                        "-"
                    }
                })
        ),
        format!(
            "parameter fields: {}",
            if parameterization.is_empty() {
                "-".to_string()
            } else {
                parameterization
                    .iter()
                    .map(Identifier::as_str)
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
    ];
    if !parameterization.is_empty() {
        lines.push("branch-local describe: use WHERE bindings".to_string());
    }
    lines.join("\n")
}

fn format_schedule_placement_lines(scheduled_node: Option<&ScheduledNode>) -> Vec<String> {
    vec![
        format!(
            "owner: {}",
            scheduled_node
                .and_then(ScheduledNode::execution_node)
                .unwrap_or("-")
        ),
        format!(
            "replicas: {}",
            scheduled_node
                .map(format_replica_nodes)
                .filter(|replicas| !replicas.is_empty())
                .unwrap_or_else(|| "-".to_string())
        ),
    ]
}

fn format_replica_nodes(scheduled_node: &ScheduledNode) -> String {
    scheduled_node.replica_nodes().join(", ")
}

fn format_processor_output_lines(outputs: &ProcessorOutputs) -> Vec<String> {
    let mut lines = Vec::new();
    let output_count = outputs.outputs().count();
    lines.push(format!("outputs: {output_count}"));

    for (index, output) in outputs.routes.iter().enumerate() {
        lines.push(format!(
            "output {index}: into={} filter-map={}",
            output.relay.as_str(),
            if output.filter_map.is_some() {
                "present"
            } else {
                "none"
            }
        ));
    }

    lines
}

fn format_lookup_describe_output(
    name: &Identifier,
    scheduled_node: &ScheduledNode,
    summary: &LookupDescribeEnvelope,
) -> String {
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

fn format_deduplicator_describe_output(
    name: &Identifier,
    deduplicator: &CreateDeduplicator,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let mut lines = vec![
        format!("deduplicator: {}", name.as_str()),
        "kind: DEDUPLICATOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", deduplicator.from_relay.as_str()),
        format!("mode: {}", deduplicator.mode.as_ref()),
        format!("deduplicate on: {}", deduplicator.deduplicate_on),
        format!("max time: {}", deduplicator.max_time),
        format!("flush each: {}", deduplicator.flush_each),
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
        format!("  key expressions: {}", deduplicator.deduplicate_on),
        format!("  max time: {}", deduplicator.max_time),
    ]);
    lines.extend(format_processor_output_lines(&deduplicator.output_routes));
    lines.join("\n")
}

fn format_reingestor_describe_output(
    name: &Identifier,
    reingestor: &CreateReingestor,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let mut lines = vec![
        format!("reingestor: {}", name.as_str()),
        "kind: REINGESTOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", reingestor.from_relay.as_str()),
        format!(
            "parameterized by: {}",
            reingestor
                .parameterized_by
                .schema()
                .map(Identifier::as_str)
                .unwrap_or("UNPARAMETERIZED")
        ),
        format!(
            "branch ttl: {}",
            reingestor.parameterized_by.ttl().unwrap_or("-")
        ),
        format!("mode: {}", reingestor.mode.as_ref()),
        format!("flush each: {}", reingestor.flush_each),
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
    name: &Identifier,
    correlator: &CreateCorrelator,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let mut lines = vec![
        format!("correlator: {}", name.as_str()),
        "kind: CORRELATOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("left: {}", correlator.left_relay.as_str()),
        format!("right: {}", correlator.right_relay.as_str()),
        format!(
            "parameterized by: {}",
            correlator
                .parameterized_by
                .schema()
                .map(Identifier::as_str)
                .unwrap_or("UNPARAMETERIZED")
        ),
        format!("mode: {}", correlator.mode.as_ref()),
        format!("match: {}", correlator.match_policy.as_ref()),
        format!("left on: {}", correlator.left_on.join(", ")),
        format!("right on: {}", correlator.right_on.join(", ")),
        format!("max time: {}", correlator.max_time),
        format!("flush each: {}", correlator.flush_each),
        format!("output: {}", correlator.output),
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
    name: &Identifier,
    reorderer: &CreateReorderer,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let mut lines = vec![
        format!("reorderer: {}", name.as_str()),
        "kind: REORDERER".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", reorderer.from_relay.as_str()),
        format!("mode: {}", reorderer.mode.as_ref()),
        format!("order by: {}", reorderer.order_by),
        format!("max time: {}", reorderer.max_time),
        format!("flush each: {}", reorderer.flush_each),
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
    name: &Identifier,
    emitter: &CreateEmitter,
    scheduled_node: Option<&ScheduledNode>,
    status: Option<&DataflowNodeStatusEnvelope>,
) -> String {
    let mut lines = vec![
        format!("emitter: {}", name.as_str()),
        "kind: EMITTER".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    if let Some(status) = status {
        lines.extend([
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
                status
                    .reconnect_wait_millis
                    .map(|millis| format!("{millis}ms"))
                    .unwrap_or_else(|| "-".to_string())
            ),
        ]);
    }
    lines.extend([
        format!("from: {}", emitter.from_relay.as_str()),
        format!(
            "codec: {}",
            emitter
                .encode_using_codec
                .as_ref()
                .map(Identifier::as_str)
                .unwrap_or("none")
        ),
        format!("sink: {}", format_emit_sink(&emitter.sink)),
        format!(
            "filter-map: {}",
            if emitter.filter_map.is_some() {
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
        EmitSink::Kinesis { client, relay } => {
            format!(
                "KINESIS client={} relay={}",
                client.as_str(),
                relay.as_str()
            )
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
        EmitSink::Sqs { client, queue } => {
            format!("SQS client={} queue={}", client.as_str(), queue.as_str())
        }
        EmitSink::ClickHouse {
            client,
            table,
            flush_each,
            ..
        } => format!(
            "CLICKHOUSE client={} table={} flush={}",
            client.as_str(),
            table.as_str(),
            flush_each
        ),
        EmitSink::Postgres {
            client,
            table,
            conflict_action,
            max_batch,
            flush_each,
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
                "POSTGRES client={} table={}{} max_batch={} flush={}",
                client.as_str(),
                table.as_str(),
                conflict,
                max_batch,
                flush_each
            )
        }
        EmitSink::MySql {
            client,
            table,
            conflict_action,
            max_batch,
            flush_each,
            ..
        } => {
            let conflict = match conflict_action {
                MySqlConflictAction::None => String::new(),
                MySqlConflictAction::DoNothing => " conflict=ON CONFLICT DO NOTHING".to_string(),
                MySqlConflictAction::DoUpdate => " conflict=ON CONFLICT DO UPDATE".to_string(),
            };
            format!(
                "MYSQL client={} table={}{} max_batch={} flush={}",
                client.as_str(),
                table.as_str(),
                conflict,
                max_batch,
                flush_each
            )
        }
        EmitSink::MongoDb {
            client,
            collection,
            conflict_action,
            max_batch,
            flush_each,
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
                "MONGODB client={} collection={}{} max_batch={} flush={}",
                client.as_str(),
                collection.as_str(),
                conflict,
                max_batch,
                flush_each
            )
        }
        EmitSink::Iceberg {
            backend,
            client,
            table,
            values: _,
            location,
            catalog,
            flush_each,
            max_batch_size,
            commit_each,
            max_commit_size,
        } => {
            let catalog = match catalog {
                IcebergCatalog::Rest { client } => format!("rest client={}", client.as_str()),
            };
            format!(
                "ICEBERG backend={} client={} table={} location={} catalog={} flush={} \
                 max_batch_size={} commit_each={} max_commit_size={}",
                backend.as_ref(),
                client.as_str(),
                table.as_str(),
                location,
                catalog,
                flush_each,
                max_batch_size.as_deref().unwrap_or("none"),
                commit_each,
                max_commit_size
            )
        }
    }
}

fn format_window_processor_describe_output(
    name: &Identifier,
    processor: &CreateWindowProcessor,
    aggregate: &WindowAggregateProgram,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let mut lines = vec![
        format!("window processor: {}", name.as_str()),
        "kind: WINDOW PROCESSOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor.from_relay.as_str()),
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
    name: &Identifier,
    processor: &nervix_models::CreateWasmProcessor,
    scheduled_node: Option<&ScheduledNode>,
    state_lines: Vec<String>,
) -> String {
    let mut lines = vec![
        format!("wasm processor: {}", name.as_str()),
        "kind: WASM PROCESSOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    let version = processor
        .resource_version
        .map(|version| version.to_string())
        .unwrap_or_else(|| "latest".to_string());
    lines.extend([
        format!("from: {}", processor.from_relay.as_str()),
        format!("mode: {}", processor.mode.as_ref()),
        format!("resource: {}", processor.resource.as_str()),
        format!("resource version: {version}"),
        format!("file: {}", processor.file),
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
    relay: &Identifier,
    scheduled_node: &ScheduledNode,
    entries: Vec<String>,
) -> String {
    let mut lines = vec![
        format!("materialized relay: {}", relay.as_str()),
        "kind: MATERIALIZER".to_string(),
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
        format!("  function: {}", demand.function.nspl_name()),
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

fn format_window_aggregate_input(expr: &nervix_nspl::vm_program::Expr) -> String {
    match expr {
        nervix_nspl::vm_program::Expr::FieldRef(field_ref) => {
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

fn requires_request_domain(statement: &Statement) -> bool {
    !matches!(
        statement,
        Statement::CreateDomain(_)
            | Statement::CreateUser(_)
            | Statement::StopDomain(_)
            | Statement::ShowClusterStatus(_)
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
            | Statement::SubscribeSession(_)
            | Statement::UnsubscribeSession(_)
            | Statement::DescribeResource(_)
            | Statement::DescribeDomain(_)
            | Statement::DescribeEndpoint(_)
            | Statement::DescribeIngestor(_)
            | Statement::DescribeRelay(_)
            | Statement::DescribeLookup(_)
            | Statement::DescribeDeduplicator(_)
            | Statement::DescribeReingestor(_)
            | Statement::DescribeCorrelator(_)
            | Statement::DescribeReorderer(_)
            | Statement::DescribeEmitter(_)
            | Statement::DescribeWasmProcessor(_)
            | Statement::DescribeWindowProcessor(_)
            | Statement::LookupQuery(_)
            | Statement::ShowCreate(_)
            | Statement::ShowRelayMaterializedState(_)
    )
}

fn validate_domain_config(config: &DomainConfig) -> Result<(), String> {
    if let DomainPace::Paced = config.pace {
        let _ = humantime::parse_duration(&config.period)
            .map_err(|err| format!("invalid domain period '{}': {err}", config.period))?;
        let _ = humantime::parse_duration(&config.skew)
            .map_err(|err| format!("invalid domain skew '{}': {err}", config.skew))?;
    }
    Ok(())
}

fn parse_time_rate(raw: &str) -> Result<String, String> {
    let rate = raw
        .parse::<f64>()
        .map_err(|err| format!("invalid time rate '{raw}': {err}"))?;
    if !rate.is_finite() || rate <= 0.0 {
        return Err(format!(
            "time rate '{raw}' must be a positive finite number"
        ));
    }
    Ok(raw.to_string())
}

fn parse_start_point(start: &DomainStartPoint) -> Result<(Timestamp, String), String> {
    match start {
        DomainStartPoint::Resume => Ok((current_timestamp(), "1.0".to_string())),
        DomainStartPoint::Now { time_rate } => {
            let time_rate = parse_time_rate(time_rate)?;
            Ok((current_timestamp(), time_rate))
        }
        DomainStartPoint::At {
            timestamp,
            time_rate,
        } => {
            let time_rate = parse_time_rate(time_rate)?;
            chrono::DateTime::parse_from_rfc3339(timestamp)
                .map(|value| (Timestamp::from(value.to_utc()), time_rate))
                .map_err(|err| format!("invalid start timestamp '{timestamp}': {err}"))
        }
    }
}

fn current_timestamp() -> Timestamp {
    Timestamp::now()
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
    .expect("testing Argon2 parameters must be valid");
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

async fn user_credentials(name: Identifier, password: String) -> Result<UserCredentials, String> {
    let password_hash = hash_password(password).await?;
    Ok(UserCredentials {
        name,
        password_hash,
    })
}

fn add_scaled_duration_to_timestamp(base: Timestamp, delta: Duration, scale: u64) -> Timestamp {
    let scaled_nanos = delta
        .as_nanos()
        .saturating_mul(u128::from(scale))
        .min(i64::MAX as u128) as i64;
    base.into_datetime()
        .checked_add_signed(TimeDelta::nanoseconds(scaled_nanos))
        .map(Timestamp::from)
        .unwrap_or(base)
}

fn logical_timestamp_at_wall_time(
    clock: &DomainClockRuntimeState,
    wall_time: Timestamp,
    time_rate: f64,
) -> Timestamp {
    let elapsed = wall_time
        .into_datetime()
        .signed_duration_since(clock.wall_started_at.into_datetime())
        .to_std()
        .unwrap_or(Duration::ZERO);
    let progressed_nanos = ((elapsed.as_nanos() as f64) * time_rate)
        .floor()
        .clamp(0.0, i64::MAX as f64) as i64;
    clock
        .logical_start
        .into_datetime()
        .checked_add_signed(TimeDelta::nanoseconds(progressed_nanos))
        .map(Timestamp::from)
        .unwrap_or(clock.logical_start)
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
        kind: CommandResultKind::Ok as i32,
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

    append_command_result(&mut previous_results, result);
    let success = previous_results.iter().all(|result| result.success);
    CommandResult {
        success,
        message: command_results_message(&previous_results),
        diagnostics: previous_results
            .last()
            .map(|result| result.diagnostics.clone())
            .unwrap_or_default(),
        kind: if success {
            CommandResultKind::Ok as i32
        } else {
            previous_results
                .last()
                .map(|result| result.kind)
                .unwrap_or(CommandResultKind::Error as i32)
        },
        results: previous_results,
        ..Default::default()
    }
}

fn command_results_message(results: &[CommandResult]) -> String {
    results
        .iter()
        .map(|result| result.message.as_str())
        .collect::<Vec<_>>()
        .join("\n")
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
        kind: CommandResultKind::Error as i32,
        ..Default::default()
    }
}

async fn fetch_resource_archive(
    cluster_api_clients: &ClusterApiClients,
    cluster_api_advertise_addr: &str,
    identifier: &Identifier,
    version: u64,
) -> Result<DownloadedResourceArchive, String> {
    let path = format!(
        "{RESOURCE_ARCHIVE_PATH_PREFIX}{}/{version}/archive",
        identifier.as_str()
    );
    let response = cluster_api_clients
        .for_url(cluster_api_advertise_addr)
        .get(format!("{cluster_api_advertise_addr}{path}"))
        .send()
        .await
        .map_err(|error| format!("resource fetch failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "resource fetch returned status {}",
            response.status()
        ));
    }
    let temp_archive = tempfile::NamedTempFile::new()
        .map_err(|error| format!("failed to create temporary resource archive: {error}"))?;
    let temp_path = temp_archive.into_temp_path();
    let mut file = File::create(<TempPath as AsRef<std::path::Path>>::as_ref(&temp_path))
        .await
        .map_err(|error| format!("failed to open temporary resource archive: {error}"))?;
    let mut hasher = Hasher::new();
    let mut relay = response.bytes_stream();
    while let Some(chunk_result) = relay.next().await {
        let chunk = chunk_result
            .map_err(|error| format!("failed to read resource archive body: {error}"))?;
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|error| format!("failed to write temporary resource archive: {error}"))?;
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

async fn post_resource_replica(
    cluster_api_clients: &ClusterApiClients,
    cluster_api_advertise_addr: &str,
    replica: &ResourceNodeStatus,
) -> Result<(), String> {
    let body = encode_cbor(replica)
        .map_err(|error| format!("failed to encode resource replica payload: {error}"))?;
    let response = cluster_api_clients
        .for_url(cluster_api_advertise_addr)
        .post(format!(
            "{cluster_api_advertise_addr}{RESOURCE_REPLICA_PATH}"
        ))
        .header(reqwest::header::CONTENT_TYPE, RAFT_CONTENT_TYPE_CBOR)
        .body(body)
        .send()
        .await
        .map_err(|error| format!("resource replica publish failed: {error}"))?;
    if response.status().is_success() {
        return Ok(());
    }
    Err(format!(
        "resource replica publish returned status {}",
        response.status()
    ))
}

fn error_response(kind: &str, diagnostics: &[ParseDiagnostic]) -> CommandResult {
    CommandResult {
        success: false,
        message: kind.to_string(),
        diagnostics: diagnostics.iter().map(map_diagnostic).collect(),
        kind: CommandResultKind::Error as i32,
        ..Default::default()
    }
}

impl SessionServiceImpl {
    async fn web_console_leadership_response(
        &self,
        already_connected_to_leader: bool,
    ) -> Option<SessionResponse> {
        let leader = self.consensus.current_leader().await;
        if leader.as_deref() != Some(self.consensus.local_node_id()) {
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
                level: ServerEventLevel::Info as i32,
                message: format!("connected to leader '{}'", self.consensus.local_node_id()),
            })),
        })
    }

    async fn domain_list_response(&self, response_to_request: bool) -> SessionResponse {
        let domains = self
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

    async fn web_console_domain_snapshot_responses(
        &self,
        active_domain: Option<&Domain>,
    ) -> Vec<SessionResponse> {
        let resources = self.consensus.current_resources().await;
        let domains = self.consensus.current_domains().await;
        let resource_entities = resources
            .next_version_by_identifier
            .iter()
            .map(|(identifier, next_version)| DomainEntitySnapshot {
                kind: "resource".to_string(),
                identifier: identifier.as_str().to_string(),
                detail: if *next_version > 1 {
                    format!("v{}", next_version - 1)
                } else {
                    "catalog".to_string()
                },
            })
            .collect::<Vec<_>>();
        let active_graphs = self
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
        domain: Domain,
        mut dataflow_graph: DataflowGraph,
        resource_entities: &[DomainEntitySnapshot],
    ) -> Option<SessionResponse> {
        dataflow_graph.statistics = self.runtime.dataflow_domain_statistics(&domain);
        for node in &mut dataflow_graph.nodes {
            let Some((kind, identifier)) = dataflow_metric_target(&node.id) else {
                continue;
            };
            (node.status, node.status_detail, node.reconnect_wait_millis) = self
                .dataflow_node_status_for_graph(&domain, &kind, &identifier)
                .await;
            if kind == "RELAY" {
                node.statistics = self
                    .runtime
                    .dataflow_relay_buffer_statistics(&domain, &identifier);
                let existing = node
                    .branches
                    .iter()
                    .map(|branch| branch.branch.clone())
                    .collect::<BTreeSet<_>>();
                node.branches.extend(
                    self.runtime
                        .dataflow_relay_branch_statistics(&domain, &identifier)
                        .into_iter()
                        .filter(|branch| !existing.contains(&branch.branch)),
                );
            }
        }
        for edge in &mut dataflow_graph.edges {
            let Some(metric) = edge.metric.as_ref() else {
                continue;
            };
            edge.statistics = self.runtime.dataflow_edge_statistics(&domain, metric);
            edge.branches = self
                .runtime
                .dataflow_edge_branch_statistics(&domain, metric);
        }
        match dataflow_graph.serialize() {
            Ok(graph_bytes) => Some(SessionResponse {
                event: Some(proto::session_response::Event::Snapshot(DomainSnapshot {
                    domain: domain.as_str().to_string(),
                    dataflow_graph: graph_bytes,
                    entities: self
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

    async fn not_leader_response(&self, query: &str, leader: Option<String>) -> CommandResult {
        let leader_node = match leader.as_deref() {
            Some(leader_id) => self
                .cluster
                .gossip_state()
                .await
                .live_nodes
                .into_iter()
                .find(|node| node.node_id == leader_id),
            None => None,
        };
        let leader_grpc_uri = leader_node
            .as_ref()
            .and_then(|node| grpc_uri_from_advertise_addr(&node.grpc_advertise_addr))
            .unwrap_or_default();
        let leader_web_console_uri = leader_node
            .map(|node| node.web_console_advertise_addr)
            .unwrap_or_default();
        let diagnostic = leader
            .as_deref()
            .map(|leader| format!("retry this command on leader '{leader}'"))
            .unwrap_or_else(|| "retry this command on the current leader".to_string());
        CommandResult {
            success: false,
            message: "not-a-leader".to_string(),
            diagnostics: vec![Diagnostic {
                message: diagnostic,
                span_start: 0,
                span_end: u32::try_from(query.len()).unwrap_or(0),
            }],
            kind: CommandResultKind::NotLeader as i32,
            leader: leader.unwrap_or_default(),
            leader_grpc_uri,
            leader_web_console_uri,
            ..Default::default()
        }
    }
}

fn dataflow_metric_target(id: &str) -> Option<(String, Identifier)> {
    let (kind, identifier) = id.split_once(':')?;
    Some((
        kind.to_ascii_uppercase(),
        Identifier::parse(identifier).ok()?,
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
    domain: &Domain,
    model_id: &Identifier,
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
                kind: CommandResultKind::Error as i32,
                ..Default::default()
            }
        }
        RegistryError::NotFound { .. }
        | RegistryError::InvalidModelKind { .. }
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
                kind: CommandResultKind::Error as i32,
                ..Default::default()
            }
        }
        RegistryError::MissingReference { reference, .. } => {
            let span = Identifier::try_from(reference.as_str())
                .ok()
                .and_then(|id| find_identifier_span(query, &id))
                .unwrap_or(0..0);
            CommandResult {
                success: false,
                message: format!("{err}"),
                diagnostics: vec![Diagnostic {
                    message: format!("{err}"),
                    span_start: u32::try_from(span.start).unwrap_or(0),
                    span_end: u32::try_from(span.end).unwrap_or(0),
                }],
                kind: CommandResultKind::Error as i32,
                ..Default::default()
            }
        }
        RegistryError::InvalidReferenceKind { reference, .. } => {
            let span = Identifier::try_from(reference.as_str())
                .ok()
                .and_then(|id| find_identifier_span(query, &id))
                .unwrap_or(0..0);
            CommandResult {
                success: false,
                message: format!("{err}"),
                diagnostics: vec![Diagnostic {
                    message: format!("{err}"),
                    span_start: u32::try_from(span.start).unwrap_or(0),
                    span_end: u32::try_from(span.end).unwrap_or(0),
                }],
                kind: CommandResultKind::Error as i32,
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
            kind: CommandResultKind::Error as i32,
            ..Default::default()
        },
    }
}

fn infer_kind_from_error_target(
    err: &error_stack::Report<RegistryError>,
    model_id: &Identifier,
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

fn find_identifier_span(query: &str, identifier: &Identifier) -> Option<std::ops::Range<usize>> {
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

fn completion_context(input: &str, cursor: usize) -> (String, usize, String) {
    let safe_cursor = cursor.min(input.len());
    let start = word_start(input, safe_cursor);
    let prefix = current_word_prefix(input, safe_cursor);

    let mut grammar_input = String::with_capacity(input.len() - (safe_cursor - start));
    grammar_input.push_str(&input[..start]);
    grammar_input.push_str(&input[safe_cursor..]);

    (grammar_input, start, prefix)
}

struct VhostTlsMaterials {
    certified_key: CertifiedKey,
}

fn cluster_api_base_url(mode: InternalTransportMode, advertise_addr: &cluster::HostPort) -> String {
    format!("{}://{}", mode.scheme(), advertise_addr.url_authority())
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

fn build_cluster_api_http_client(
    mode: InternalTransportMode,
) -> Result<HttpClient, Report<AppError>> {
    let builder = HttpClient::builder().tcp_nodelay(true);
    if !mode.is_tls() {
        return builder.build().map_err(|error| {
            Report::new(AppError::LoadClusterApiTls).attach_printable(error.to_string())
        });
    }

    builder
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .map_err(|error| {
            Report::new(AppError::LoadClusterApiTls).attach_printable(error.to_string())
        })
}

fn load_cluster_api_tls_server_config() -> Result<Arc<ServerConfig>, Report<AppError>> {
    nervix_interconnect::install_rustls_crypto_provider();
    let cert_path = internal_tls_path(INTERNAL_TLS_CERT_FILE);
    let key_path = internal_tls_path(INTERNAL_TLS_KEY_FILE);
    let cert_chain = load_certificates_from_pem_file(cert_path.as_path())
        .map_err(|error| Report::new(AppError::LoadClusterApiTls).attach_printable(error))?;
    let private_key = load_private_key_from_pem_file(key_path.as_path())
        .map_err(|error| Report::new(AppError::LoadClusterApiTls).attach_printable(error))?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|error| {
            Report::new(AppError::LoadClusterApiTls).attach_printable(error.to_string())
        })?;
    Ok(Arc::new(config))
}

fn load_web_console_tls_server_config(
    cert_path: &Path,
    key_path: &Path,
) -> Result<Arc<ServerConfig>, Report<AppError>> {
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
    Ok(Arc::new(config))
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

fn resolve_vhost_tls_resource_version(
    resources: &nervix_models::ResourceVersionStatus,
    tls: &VhostTlsResource,
) -> Result<u64, String> {
    if let Some(version) = tls.version {
        let exists = resources.versions.iter().any(|resource| {
            resource.id.identifier == tls.resource && resource.id.version == version
        });
        if exists {
            return Ok(version);
        }
        return Err(format!(
            "resource '{}@{}' does not exist",
            tls.resource.as_str(),
            version
        ));
    }

    resources
        .versions
        .iter()
        .filter(|resource| resource.id.identifier == tls.resource)
        .map(|resource| resource.id.version)
        .max()
        .ok_or_else(|| {
            format!(
                "resource '{}' has no uploaded versions",
                tls.resource.as_str()
            )
        })
}

fn resolve_latest_resource_version(
    resources: &nervix_models::ResourceVersionStatus,
    identifier: &Identifier,
) -> Result<u64, String> {
    resources
        .versions
        .iter()
        .filter(|resource| resource.id.identifier == *identifier)
        .map(|resource| resource.id.version)
        .max()
        .ok_or_else(|| {
            format!(
                "resource '{}' has no uploaded versions",
                identifier.as_str()
            )
        })
}

fn resolve_resource_version(
    resources: &nervix_models::ResourceVersionStatus,
    identifier: &Identifier,
    requested_version: Option<u64>,
) -> Result<u64, String> {
    if let Some(version) = requested_version {
        let exists = resources.versions.iter().any(|resource| {
            resource.id.identifier == *identifier && resource.id.version == version
        });
        if exists {
            return Ok(version);
        }
        return Err(format!(
            "resource '{}@{}' does not exist",
            identifier.as_str(),
            version
        ));
    }

    resolve_latest_resource_version(resources, identifier)
}

fn inferencer_field_tensor_spec(ty: &ParseAsType) -> Result<(TensorElementType, Vec<i64>), String> {
    match ty {
        ParseAsType::Array { element, len } if element.as_ref() == &ParseAsType::F32 => {
            Ok((TensorElementType::Float32, vec![-1, *len as i64]))
        }
        ParseAsType::Array { element, len } if element.as_ref() == &ParseAsType::F64 => {
            Ok((TensorElementType::Float64, vec![-1, *len as i64]))
        }
        ParseAsType::F32 => Ok((TensorElementType::Float32, vec![-1])),
        ParseAsType::F64 => Ok((TensorElementType::Float64, vec![-1])),
        other => Err(format!(
            "unsupported internal field type '{}'",
            inferencer_parse_as_label(other)
        )),
    }
}

fn inferencer_parse_as_label(ty: &ParseAsType) -> String {
    match ty {
        ParseAsType::U8 => "U8".to_string(),
        ParseAsType::I8 => "I8".to_string(),
        ParseAsType::U16 => "U16".to_string(),
        ParseAsType::I16 => "I16".to_string(),
        ParseAsType::U32 => "U32".to_string(),
        ParseAsType::I32 => "I32".to_string(),
        ParseAsType::U64 => "U64".to_string(),
        ParseAsType::I64 => "I64".to_string(),
        ParseAsType::Bool => "BOOL".to_string(),
        ParseAsType::String => "STRING".to_string(),
        ParseAsType::Datetime => "DATETIME".to_string(),
        ParseAsType::F32 => "F32".to_string(),
        ParseAsType::F64 => "F64".to_string(),
        ParseAsType::Array { element, len } => {
            format!("ARRAY<{}, {}>", inferencer_parse_as_label(element), len)
        }
        ParseAsType::Vec { element } => format!("VEC<{}>", inferencer_parse_as_label(element)),
    }
}

fn validate_inferencer_tensor_type(
    processor: &CreateInferencer,
    direction: &str,
    tensor: &str,
    relay: &Identifier,
    field: &Identifier,
    field_type: &ParseAsType,
    model_type: &ValueType,
) -> Result<(), String> {
    let (expected_element, expected_shape) =
        inferencer_field_tensor_spec(field_type).map_err(|reason| {
            format!(
                "inferencer '{}' {} tensor '{}' field '{}.{}' is incompatible: {}",
                processor.name.as_str(),
                direction,
                tensor,
                relay.as_str(),
                field.as_str(),
                reason
            )
        })?;
    let ValueType::Tensor { ty, shape, .. } = model_type else {
        return Err(format!(
            "inferencer '{}' {} tensor '{}' expected ONNX tensor type, got {}",
            processor.name.as_str(),
            direction,
            tensor,
            model_type
        ));
    };
    if *ty != expected_element {
        return Err(format!(
            "inferencer '{}' {} tensor '{}' field '{}.{}' has incompatible element type: ONNX {} \
             vs internal {}",
            processor.name.as_str(),
            direction,
            tensor,
            relay.as_str(),
            field.as_str(),
            ty,
            inferencer_parse_as_label(field_type)
        ));
    }
    if shape.len() != expected_shape.len()
        || shape
            .iter()
            .zip(expected_shape.iter())
            .any(|(actual, expected)| *expected != -1 && actual != expected)
    {
        return Err(format!(
            "inferencer '{}' {} tensor '{}' field '{}.{}' has incompatible shape: ONNX {:?} vs \
             internal {:?}",
            processor.name.as_str(),
            direction,
            tensor,
            relay.as_str(),
            field.as_str(),
            shape.as_ref(),
            expected_shape
        ));
    }
    Ok(())
}

async fn load_vhost_tls_materials(
    resource_store: &ResourceStore,
    identifier: &Identifier,
    version: u64,
) -> Result<VhostTlsMaterials, String> {
    let cert_path = resource_store
        .resolve_content_path(identifier, version, VHOST_TLS_CERT_PATH)
        .map_err(|error| error.to_string())?;
    let key_path = resource_store
        .resolve_content_path(identifier, version, VHOST_TLS_KEY_PATH)
        .map_err(|error| error.to_string())?;
    let ca_path = resource_store
        .resolve_content_path(identifier, version, VHOST_TLS_CA_PATH)
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
    prefix: &str,
) -> Vec<String> {
    resources
        .next_version_by_identifier
        .iter()
        .filter_map(|(identifier, _)| {
            if prefix.is_empty() || identifier.as_str().starts_with(prefix) {
                Some(identifier.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn resource_version_suggestions(
    resources: &nervix_models::ResourceVersionStatus,
    identifier: &Identifier,
    prefix: &str,
) -> Vec<String> {
    resources
        .versions
        .iter()
        .filter(|resource| resource.id.identifier == *identifier)
        .filter_map(|resource| {
            let version = resource.id.version.to_string();
            if prefix.is_empty() || version.starts_with(prefix) {
                Some(version)
            } else {
                None
            }
        })
        .collect()
}

fn requested_resource_versions(input: &str, cursor: usize) -> Option<Identifier> {
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
    Identifier::parse(identifier).ok()
}

fn word_start(input: &str, cursor: usize) -> usize {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    input[..cursor.min(input.len())]
        .char_indices()
        .rev()
        .find(|(_, c)| !is_word(*c))
        .map(|(idx, c)| idx + c.len_utf8())
        .unwrap_or(0)
}

fn encode_cbor<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, std::io::Error> {
    let mut out = Vec::new();
    ciborium::into_writer(value, &mut out).map_err(|err| std::io::Error::other(err.to_string()))?;
    Ok(out)
}

fn decode_cbor<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, std::io::Error> {
    ciborium::from_reader(std::io::Cursor::new(bytes))
        .map_err(|err| std::io::Error::other(err.to_string()))
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
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn decode_hex(input: &str) -> Option<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        let high = (bytes[index] as char).to_digit(16)?;
        let low = (bytes[index + 1] as char).to_digit(16)?;
        out.push(((high << 4) | low) as u8);
        index += 2;
    }
    Some(out)
}

fn decode_verifying_key(input: &str) -> Option<VerifyingKey> {
    let bytes = decode_hex(input)?;
    let array: [u8; 32] = bytes.try_into().ok()?;
    VerifyingKey::from_bytes(&array).ok()
}

fn should_initiate_interconnect(local_node_id: &str, peer_node_id: &str) -> bool {
    local_node_id < peer_node_id
}

async fn render_cluster_status(
    cluster: &cluster::ClusterHandle,
    consensus: &ConsensusHandle,
) -> String {
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
    for domain in &schedule.domains {
        if domain.nodes.is_empty() {
            lines.push(format!("- domain={} nodes=none", domain.domain.as_str()));
            continue;
        }

        for node in &domain.nodes {
            lines.push(format!(
                "- domain={} kind={} name={} owner={} replicas={}",
                domain.domain.as_str(),
                node.kind.as_str(),
                node.identifier.as_str(),
                node.execution_node().unwrap_or("-"),
                format_schedule_status_replicas(node)
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
        replicas.join(",")
    }
}

async fn reconcile_domain_clock_tasks(
    service: &SessionServiceImpl,
    shutdown: &CancellationToken,
    tasks: &mut HashMap<Domain, (CancellationToken, JoinHandle<()>)>,
) {
    let domains = service.consensus.current_domains().await;
    let desired = domains
        .iter()
        .filter(|(domain_id, domain)| {
            if let DomainStatus::Running = domain.status {
                service.domain_clocks.contains_key(*domain_id)
            } else {
                false
            }
        })
        .map(|(domain_id, _)| domain_id.clone())
        .collect::<Vec<_>>();

    let existing = tasks.keys().cloned().collect::<Vec<_>>();
    for domain_id in existing {
        if !desired.iter().any(|candidate| candidate == &domain_id)
            && let Some((token, handle)) = tasks.remove(&domain_id)
        {
            token.cancel();
            let _ = handle.await;
        }
    }

    for domain_id in desired {
        if tasks.contains_key(&domain_id) {
            continue;
        }
        let token = shutdown.child_token();
        let task_service = service.clone();
        let task_domain_id = domain_id.clone();
        let task_token = token.clone();
        let handle = tokio::spawn(async move {
            run_domain_clock(task_service, task_domain_id, task_token).await;
        });
        tasks.insert(domain_id, (token, handle));
    }
}

async fn run_domain_clock(
    service: SessionServiceImpl,
    domain_id: Domain,
    shutdown: CancellationToken,
) {
    loop {
        tokio::task::consume_budget().await;
        if shutdown.is_cancelled() {
            break;
        }
        let Some(domain) = service.consensus.current_domain(&domain_id).await else {
            break;
        };
        if let DomainStatus::Stopped = domain.status {
            break;
        }
        let Some(mut clock) = service
            .domain_clocks
            .get(&domain_id)
            .map(|state| state.clone())
        else {
            break;
        };
        let Ok(period) = humantime::parse_duration(&domain.config.period) else {
            warn!(
                domain = domain_id.as_str(),
                period = domain.config.period,
                "invalid domain period"
            );
            break;
        };
        let Ok(time_rate) = clock.time_rate.parse::<f64>() else {
            warn!(
                domain = domain_id.as_str(),
                time_rate = clock.time_rate,
                "invalid domain time rate"
            );
            break;
        };
        if !time_rate.is_finite() || time_rate <= 0.0 {
            warn!(
                domain = domain_id.as_str(),
                time_rate = clock.time_rate,
                "invalid domain time rate"
            );
            break;
        }

        let period_ms = u64::try_from(period.as_millis()).unwrap_or(u64::MAX);
        let next_logical = add_scaled_duration_to_timestamp(
            clock.logical_start,
            period,
            clock.next_tick_id.saturating_sub(1),
        );
        let reached_logical =
            logical_timestamp_at_wall_time(&clock, current_timestamp(), time_rate);

        if reached_logical >= next_logical {
            emit_domain_tick(&service, &domain_id, &mut clock, next_logical, period_ms).await;
            continue;
        }

        let remaining_logical = next_logical
            .into_datetime()
            .signed_duration_since(reached_logical.into_datetime())
            .to_std()
            .unwrap_or(Duration::ZERO);
        let wait_nanos = ((remaining_logical.as_nanos() as f64) / time_rate)
            .ceil()
            .clamp(0.0, u64::MAX as f64) as u64;
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = sleep(Duration::from_nanos(wait_nanos.clamp(1_000_000, 250_000_000))) => {}
        }
    }
}

async fn emit_due_domain_ticks(service: &SessionServiceImpl, domain_id: &Domain) {
    loop {
        tokio::task::consume_budget().await;
        let Some(domain) = service.consensus.current_domain(domain_id).await else {
            break;
        };
        if let DomainStatus::Stopped = domain.status {
            break;
        }
        let Some(mut clock) = service
            .domain_clocks
            .get(domain_id)
            .map(|state| state.clone())
        else {
            break;
        };
        let Ok(period) = humantime::parse_duration(&domain.config.period) else {
            warn!(
                domain = domain_id.as_str(),
                period = domain.config.period,
                "invalid domain period"
            );
            break;
        };
        let Ok(time_rate) = clock.time_rate.parse::<f64>() else {
            warn!(
                domain = domain_id.as_str(),
                time_rate = clock.time_rate,
                "invalid domain time rate"
            );
            break;
        };
        if !time_rate.is_finite() || time_rate <= 0.0 {
            warn!(
                domain = domain_id.as_str(),
                time_rate = clock.time_rate,
                "invalid domain time rate"
            );
            break;
        }

        let period_ms = u64::try_from(period.as_millis()).unwrap_or(u64::MAX);
        let next_logical = add_scaled_duration_to_timestamp(
            clock.logical_start,
            period,
            clock.next_tick_id.saturating_sub(1),
        );
        let reached_logical =
            logical_timestamp_at_wall_time(&clock, current_timestamp(), time_rate);

        if reached_logical < next_logical {
            break;
        }

        emit_domain_tick(service, domain_id, &mut clock, next_logical, period_ms).await;
    }
}

async fn emit_domain_tick(
    service: &SessionServiceImpl,
    domain_id: &Domain,
    clock: &mut DomainClockRuntimeState,
    logical_timestamp: Timestamp,
    duration_ms: u64,
) {
    let wall_clock = current_timestamp();
    let tick = DomainTick {
        tick_id: clock.next_tick_id,
        logical_timestamp,
        wall_clock,
        duration_ms,
    };
    clock.next_tick_id = clock.next_tick_id.saturating_add(1);
    service
        .domain_clocks
        .insert(domain_id.clone(), clock.clone());
    let target_nodes = service.domain_tick_target_nodes(domain_id).await;
    for node_id in target_nodes {
        if node_id == service.consensus.local_node_id() {
            service.handle_domain_tick(DomainTickEnvelope {
                domain_id: domain_id.clone(),
                tick: tick.clone(),
            });
            continue;
        }
        if let Err(error) = service
            .dispatch_interconnect_control(
                &node_id,
                ControlEnvelope::DomainTick(DomainTickEnvelope {
                    domain_id: domain_id.clone(),
                    tick: tick.clone(),
                }),
            )
            .await
        {
            warn!(
                domain = domain_id.as_str(),
                node = node_id,
                error = %error,
                "failed to deliver domain tick"
            );
        }
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
            let _ = tracer_provider.shutdown();
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

pub fn init_tracing_to_file(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let file = Arc::new(ParkingMutex::new(file));
    let make_writer = BoxMakeWriter::new(move || SharedFileWriter(file.clone()));
    let _ = fmt()
        .with_ansi(false)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(DEFAULT_TRACE_FILTER)),
        )
        .with_writer(make_writer)
        .try_init();
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
        let cluster_listen_addr = self.cluster_listen_addr;
        let cluster_advertise_addr = self.cluster_advertise_addr;
        let cluster_api_mode = self.cluster_api_mode;
        let cluster_api_listen_addr = match cluster_api_mode {
            InternalTransportMode::Http => self.cluster_api_listen_addr,
            InternalTransportMode::Https => self
                .cluster_api_https_listen_addr
                .ok_or_else(|| Report::new(AppError::MissingClusterApiHttpsListenAddress))?,
        };
        let cluster_api_advertise_addr = match cluster_api_mode {
            InternalTransportMode::Http => self.cluster_api_advertise_addr.clone(),
            InternalTransportMode::Https => self
                .cluster_api_https_advertise_addr
                .clone()
                .ok_or_else(|| Report::new(AppError::MissingClusterApiHttpsAdvertiseAddress))?,
        };
        let cluster_api_advertise_url =
            cluster_api_base_url(cluster_api_mode, &cluster_api_advertise_addr);
        let interconnect_mode = self.interconnect_mode;
        let interconnect_listen_addr = match interconnect_mode {
            InternalTransportMode::Http => self.interconnect_listen_addr,
            InternalTransportMode::Https => self
                .interconnect_https_listen_addr
                .ok_or_else(|| Report::new(AppError::MissingInterconnectHttpsListenAddress))?,
        };
        let interconnect_advertise_addr = match interconnect_mode {
            InternalTransportMode::Http => self.interconnect_advertise_addr.clone(),
            InternalTransportMode::Https => self
                .interconnect_https_advertise_addr
                .clone()
                .ok_or_else(|| Report::new(AppError::MissingInterconnectHttpsAdvertiseAddress))?,
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
        let replica_count = self.replica_count;
        let state_snapshot_interval = self.state_snapshot_interval;
        let memory_pressure = self.memory_pressure;
        let db_path = self.db_path.clone();
        let temp_dir = self.temp_dir.clone();
        let shutdown = self.shutdown.clone();
        let runtime_test_hooks = self.runtime_test_hooks.clone();
        let interconnect_identity = LocalIdentity::generate(node_id.clone());
        let interconnect_public_key = encode_hex(&interconnect_identity.public_key().to_bytes());
        let cluster_api_clients = Arc::new(ClusterApiClients::build().map_err(|error| {
            error!(?error, "failed to build cluster api http clients");
            error
        })?);
        let cluster_api_http_client = cluster_api_clients
            .for_url(&cluster_api_advertise_url)
            .clone();
        let grpc_tls_server_config = if grpc_mode.is_tls() {
            Some(load_grpc_tls_server_config().await.map_err(|error| {
                error!(?error, "failed to build grpc tls server config");
                error
            })?)
        } else {
            None
        };
        let cluster_api_tls_server_config = if cluster_api_mode.is_tls() {
            Some(load_cluster_api_tls_server_config().map_err(|error| {
                error!(?error, "failed to build cluster api tls server config");
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
        let cluster_api_listener = TcpListener::bind(cluster_api_listen_addr)
            .await
            .change_context(if cluster_api_mode.is_tls() {
                AppError::BindClusterApiHttpsListenAddress
            } else {
                AppError::BindClusterApiListenAddress
            })?;
        let peer_keys = Arc::new(RwLock::new(HashMap::new()));
        {
            peer_keys
                .write()
                .insert(node_id.clone(), interconnect_identity.public_key());
        }
        let interconnect_tls = if interconnect_mode.is_tls() {
            let ca_path = internal_tls_path(INTERNAL_TLS_CA_FILE);
            let cert_path = internal_tls_path(INTERNAL_TLS_CERT_FILE);
            let key_path = internal_tls_path(INTERNAL_TLS_KEY_FILE);
            Some(
                TlsConfigBundle::from_pem_files(ca_path, cert_path, key_path)
                    .change_context(AppError::LoadInterconnectTls)?,
            )
        } else {
            None
        };
        let peer_verifier = {
            let peer_keys = peer_keys.clone();
            PeerVerifier::new(move |node_id| peer_keys.read().get(node_id).copied())
        };
        let (interconnect, mut interconnect_rx) = Transport::bind(
            interconnect_listen_addr,
            interconnect_mode.interconnect_transport_mode(),
            interconnect_tls,
            interconnect_identity,
            peer_verifier,
            Default::default(),
        )
        .await
        .change_context(AppError::StartInterconnect)?;
        let interconnect = Arc::new(interconnect);
        let cluster_transport = cluster::bind_gossip_transport(cluster_listen_addr)
            .await
            .change_context(AppError::StartCluster)?;

        info!(
            grpc_mode = grpc_mode.scheme(),
            grpc_listen_addr = %grpc_listen_addr,
            grpc_advertise_addr = %grpc_advertise_url,
            http_listen_addr = %http_listen_addr,
            https_listen_addr = %https_listen_addr,
            observability_listen_addr = %observability_listen_addr,
            web_console_listen_addr = %web_console_listen_addr,
            cluster_listen_addr = %cluster_listen_addr,
            cluster_advertise_addr = %cluster_advertise_addr,
            cluster_api_mode = cluster_api_mode.scheme(),
            cluster_api_listen_addr = %cluster_api_listen_addr,
            cluster_api_advertise_addr = %cluster_api_advertise_url,
            interconnect_mode = interconnect_mode.scheme(),
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
            node_id,
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
        let resource_store = Arc::new(
            ResourceStore::open(db_path.join("resources")).map_err(|err| {
                error!(db_path = db_path.display().to_string(), error = %err, "failed to open resource store");
                Report::new(AppError::OpenResourceStore)
            })?,
        );
        let registry = Arc::new(
            match Registry::from_database(db.clone(), Some(db_path.as_path())) {
                Ok(registry) => registry,
                Err(err) => {
                    error!(db_path = db_path.display().to_string(), error = %err, "failed to open registry");
                    return Err(err.change_context(AppError::OpenRegistry));
                }
            },
        );
        let runtime = Arc::new(
            Runtime::with_persistence_and_temp_dir(
                Some(db.clone()),
                state_snapshot_interval,
                runtime_test_hooks.clone(),
                temp_dir.clone(),
            )
            .map_err(|error| {
                error!(error = %error, "failed to initialize runtime persistence");
                Report::new(AppError::OpenRuntimeState)
            })?,
        );
        runtime.attach_resource_store(resource_store.clone());
        info!(
            runtime_state_store_enabled = runtime.has_state_store(),
            runtime_state_snapshot_interval = ?runtime.state_snapshot_interval(),
            "initialized runtime persistence"
        );
        for changes in registry
            .startup_runtime_changes()
            .map_err(|err| Report::new(AppError::ApplyStartupRuntime(err.to_string())))?
        {
            if let Err(err) = runtime.apply_changes(changes).await {
                error!(error = %err, "failed to apply startup runtime changes");
                return Err(Report::new(AppError::ApplyStartupRuntime(err.to_string())));
            }
        }
        let mut consensus = ConsensusHandle::from_database(
            db.clone(),
            ConsensusSettings {
                cluster_name: cluster_id.clone(),
                node_id: node_id.clone(),
                cluster_api_advertise_url: cluster_api_advertise_url.clone(),
                cluster_api_http_client: cluster_api_http_client.clone(),
                node_unavailability_timeout,
                raft_heartbeat_interval,
                raft_election_timeout_min,
                raft_election_timeout_max,
            },
        )
        .await
        .change_context(AppError::StartConsensus)?;
        consensus.set_local_grpc_advertise_addr(grpc_advertise_url.clone());
        let consensus = Arc::new(consensus);
        runtime.attach_resources(resource_store.clone(), consensus.current_resources().await);
        let cluster = Arc::new(
            cluster::start_cluster_with_transport(
                cluster::ClusterSettings {
                    cluster_id,
                    node_id: node_id.clone(),
                    cluster_listen_addr,
                    cluster_advertise_addr,
                    grpc_listen_addr,
                    grpc_advertise_addr: grpc_advertise_url.clone(),
                    web_console_advertise_addr: web_console_advertise_url.clone(),
                    cluster_api_listen_addr,
                    cluster_api_advertise_addr: cluster_api_advertise_url.clone(),
                    interconnect_listen_addr,
                    interconnect_advertise_addr,
                    interconnect_mode: interconnect_mode.scheme().to_string(),
                    interconnect_public_key,
                    bootstrap_host: cluster_bootstrap_host.clone(),
                    node_unavailability_timeout,
                },
                &cluster_transport,
            )
            .await
            .change_context(AppError::StartCluster)?,
        );
        runtime.attach_remote_dispatcher(node_id.clone(), cluster.clone(), interconnect.clone());

        let cluster_for_reconcile = cluster.clone();
        let consensus_for_reconcile = consensus.clone();
        let registry_for_reconcile = registry.clone();
        let reconcile_shutdown = shutdown.clone();
        let mut background_tasks = Vec::new();
        if let Some(config) = memory_pressure {
            let controller = MemoryPressureController::new(config).map_err(|error| {
                error!(?error, "failed to initialize memory pressure monitor");
                Report::new(AppError::InitMemoryPressureMonitor).attach_printable(error)
            })?;
            let memory_runtime = runtime.as_ref().clone();
            let memory_shutdown = shutdown.clone();
            background_tasks.push(tokio::spawn(async move {
                controller.run(memory_runtime, memory_shutdown).await;
            }));
        }
        let mut leadership_transfer_rx = runtime_test_hooks.leadership_transfers.subscribe();
        let consensus_for_leadership_transfer = consensus.clone();
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
                                        from_node_id = request.from_node_id,
                                        to_node_id = request.to_node_id,
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
                    match consensus_for_reconcile.maybe_initialize().await {
                        Ok(did_initialize) => {
                            initialized = did_initialize;
                        }
                        Err(err) => {
                            warn!(error = %err, "raft bootstrap attempt failed");
                        }
                    }
                }
                if let Err(err) = consensus_for_reconcile.reconcile_nodes(gossip).await {
                    warn!(error = %err, "raft membership reconciliation failed");
                }
                if consensus_for_reconcile.current_leader().await.as_deref()
                    == Some(consensus_for_reconcile.local_node_id())
                {
                    if !default_user_resolved {
                        match Identifier::parse(&default_user) {
                            Ok(default_user_id)
                                if consensus_for_reconcile
                                    .current_user(&default_user_id)
                                    .await
                                    .is_none() =>
                            {
                                if let Some(password) = init_default_user_password.clone() {
                                    match user_credentials(default_user_id, password).await {
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
                    let live_voters = consensus_for_reconcile
                        .live_voter_ids(live_node_ids.clone())
                        .await;
                    let schedulable_node_ids = consensus_for_reconcile
                        .schedulable_live_voter_ids(live_node_ids)
                        .await;
                    let current_schedule = consensus_for_reconcile.current_schedule().await;
                    let live_voter_set = live_voters.iter().cloned().collect::<BTreeSet<_>>();
                    for domain_schedule in &current_schedule.domains {
                        let mut failover_schedule = domain_schedule.clone();
                        let failover_moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
                            &mut failover_schedule,
                            &live_voter_set,
                        );
                        if failover_moves.is_empty() {
                            continue;
                        }
                        for failover_move in &failover_moves {
                            if let Some(replica) = failover_move.promoted_replica.as_deref() {
                                info!(
                                    domain = domain_schedule.domain.as_str(),
                                    node = failover_move.label,
                                    promoted_replica = replica,
                                    "failover promoted live replica to primary"
                                );
                            } else if let Some(fallback_node) =
                                failover_move.fallback_node.as_deref()
                            {
                                warn!(
                                    domain = domain_schedule.domain.as_str(),
                                    node = failover_move.label,
                                    fallback_node,
                                    "failover found no live replica; moving scheduled node without \
                                     local replicated state"
                                );
                            }
                        }
                        if let Err(err) = consensus_for_reconcile
                            .replace_domain_schedule(
                                domain_schedule.domain.clone(),
                                Some(failover_schedule),
                            )
                            .await
                        {
                            warn!(error = %err, "failed to republish domain schedule after node failover");
                        }
                    }
                    for (domain, graph) in registry_for_reconcile.active_graphs() {
                        let mut schedule =
                            graph.schedule_for_domain(&domain, &schedulable_node_ids, replica_count);
                        SessionServiceImpl::merge_existing_schedule_data(
                            &mut schedule,
                            current_schedule.domain(&domain),
                            &live_voters,
                        );
                        if current_schedule.domain(&domain) == Some(&schedule) {
                            continue;
                        }
                        if let Err(err) = consensus_for_reconcile
                            .replace_domain_schedule(domain, Some(schedule))
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
        let peer_keys_for_interconnect = peer_keys.clone();
        let local_node_id = node_id;
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
                {
                    let mut keys = peer_keys_for_interconnect.write();
                    keys.retain(|node_id, _| {
                        live_node_ids.contains(node_id) || node_id == &local_node_id
                    });
                    for node in &gossip.live_nodes {
                        if let Some(key) = decode_verifying_key(&node.interconnect_public_key) {
                            keys.insert(node.node_id.clone(), key);
                        }
                    }
                }
                cluster_for_interconnect.retain_interconnect_live_set(&live_node_ids);
                for node in gossip.live_nodes {
                    if node.node_id == local_node_id {
                        continue;
                    }
                    let peer_interconnect_mode = match node.interconnect_mode.as_str() {
                        "https" => InterconnectTransportMode::Tls,
                        _ => InterconnectTransportMode::Plain,
                    };
                    let Ok(target_addr) = node
                        .interconnect_advertise_addr
                        .parse::<cluster::HostPort>()
                    else {
                        cluster_for_interconnect.record_interconnect_failure(&node.node_id, None);
                        continue;
                    };
                    let target_label = target_addr.to_string();
                    if peer_interconnect_mode == InterconnectTransportMode::Tls
                        && node.interconnect_public_key.is_empty()
                    {
                        cluster_for_interconnect
                            .record_interconnect_failure(&node.node_id, Some(target_label.clone()));
                        continue;
                    }
                    if !should_initiate_interconnect(&local_node_id, &node.node_id) {
                        if interconnect_for_membership.is_connected_to(&node.node_id) {
                            cluster_for_interconnect
                                .record_interconnect_connected(&node.node_id, target_label.clone());
                        } else {
                            cluster_for_interconnect.record_interconnect_failure(
                                &node.node_id,
                                Some(target_label.clone()),
                            );
                        }
                        continue;
                    }
                    let resolved_targets = match target_addr.resolve_all().await {
                        Ok(addrs) => addrs,
                        Err(_err) => {
                            cluster_for_interconnect.record_interconnect_failure(
                                &node.node_id,
                                Some(target_label.clone()),
                            );
                            continue;
                        }
                    };
                    let mut connected = false;
                    for resolved_target in resolved_targets {
                        if interconnect_for_membership
                            .connection_for(resolved_target, "localhost", peer_interconnect_mode)
                            .await
                            .is_ok()
                            && interconnect_for_membership.is_connected_to(&node.node_id)
                        {
                            connected = true;
                            break;
                        }
                    }
                    if connected {
                        cluster_for_interconnect
                            .record_interconnect_connected(&node.node_id, target_label);
                    } else {
                        cluster_for_interconnect
                            .record_interconnect_failure(&node.node_id, Some(target_label));
                    }
                }
                tokio::select! {
                    _ = interconnect_membership_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        let runtime_for_schedule = runtime.clone();
        let mut schedule_rx = consensus.subscribe_schedule();
        let schedule_local_node_id = consensus.local_node_id().to_string();
        let schedule_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            let initial_schedule = schedule_rx.borrow().clone();
            if let Err(error) = runtime_for_schedule
                .apply_cluster_schedule(&schedule_local_node_id, &initial_schedule)
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
                        let schedule = schedule_rx.borrow().clone();
                        if let Err(error) = runtime_for_schedule
                            .apply_cluster_schedule(&schedule_local_node_id, &schedule)
                            .await
                        {
                            warn!(error = %error, "failed to apply updated cluster schedule");
                        }
                    }
                }
            }
        }));
        let runtime_for_resources = runtime.clone();
        let mut resources_rx = consensus.subscribe_resources();
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
        let (events, _) = broadcast::channel(256);
        let service = SessionServiceImpl {
            cluster: cluster.clone(),
            consensus: consensus.clone(),
            registry,
            resource_store,
            cluster_api_clients: cluster_api_clients.clone(),
            http_tls_server_config: Arc::new(RwLock::new(None)),
            runtime: runtime.clone(),
            replica_count,
            shutdown: shutdown.clone(),
            events: events.clone(),
            subscription_interest_counts: Arc::new(DashMap::with_hasher(RandomState::new())),
            interconnect: interconnect.clone(),
            domain_clocks: Arc::new(DashMap::with_hasher(RandomState::new())),
            domain_clock_events: Arc::new(Notify::new()),
            next_cluster_command_correlation_id: Arc::new(AtomicU64::new(1)),
            pending_cluster_commands: Arc::new(DashMap::default()),
            service_tasks: TaskTracker::new(),
            configured_basic_auth,
            auth_rate_limiter: SessionServiceImpl::new_auth_rate_limiter(),
            failed_auth_rate_limit_keys: Arc::new(DashMap::with_hasher(RandomState::new())),
        };

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
        let domain_local_node_id = consensus.local_node_id().to_string();
        background_tasks.push(tokio::spawn(async move {
            let mut domains_rx = domain_service.consensus.subscribe_domains();
            let mut tasks: HashMap<Domain, (CancellationToken, JoinHandle<()>)> = HashMap::new();
            let initial_domains = domains_rx.borrow().clone();

            domain_service.runtime.sync_domains(&initial_domains);
            let initial_schedule = domain_service.consensus.current_schedule().await;
            if let Err(error) = domain_service
                .runtime
                .apply_cluster_schedule(&domain_local_node_id, &initial_schedule)
                .await
            {
                warn!(error = %error, "failed to apply cluster schedule after initial domain sync");
            }

            loop {
                tokio::task::consume_budget().await;
                reconcile_domain_clock_tasks(&domain_service, &domain_shutdown, &mut tasks).await;
                let domain_clock_event = domain_service.domain_clock_events.notified();
                tokio::select! {
                    _ = domain_shutdown.cancelled() => break,
                    changed = domains_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        domain_service
                            .runtime
                            .sync_domains(&domains_rx.borrow().clone());
                        let schedule = domain_service.consensus.current_schedule().await;
                        if let Err(error) = domain_service
                            .runtime
                            .apply_cluster_schedule(&domain_local_node_id, &schedule)
                            .await
                        {
                            warn!(error = %error, "failed to apply cluster schedule after domain sync");
                        }
                    }
                    _ = domain_clock_event => {}
                }
            }

            for (_, (token, handle)) in tasks {
                token.cancel();
                let _ = handle.await;
            }
        }));

        let kafka_schedule_service = service.clone();
        let kafka_schedule_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            let mut schedule_rx = kafka_schedule_service.consensus.subscribe_schedule();
            let mut tasks: HashMap<
                KafkaPartitionWatcherKey,
                (KafkaPartitionWatcherSpec, CancellationToken, JoinHandle<()>),
            > = HashMap::new();

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

            for (_, (_, cancel, handle)) in tasks {
                cancel.cancel();
                let _ = handle.await;
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
                        match message.envelope {
                            Envelope::RelayPayload(payload) => {
                                if let Err(error) = runtime_for_interconnect
                                    .handle_remote_stream(payload)
                                    .await
                                {
                                    warn!(error = %error, "failed to process remote relay payload");
                                }
                            }
                            Envelope::Ack(ack) => {
                                runtime_for_interconnect.handle_remote_ack_resolution(ack);
                            }
                            Envelope::Control(ControlEnvelope::Terminate) => {}
                            Envelope::Control(ControlEnvelope::DomainClockStart(start)) => {
                                service_for_interconnect.handle_domain_clock_start(start);
                            }
                            Envelope::Control(ControlEnvelope::DomainClockStop(stop)) => {
                                service_for_interconnect.handle_domain_clock_stop(stop);
                            }
                            Envelope::Control(ControlEnvelope::DomainTick(tick)) => {
                                service_for_interconnect.handle_domain_tick(tick);
                            }
                            Envelope::Control(ControlEnvelope::StateSyncRequest(request)) => {
                                let result = match crate::runtime::RuntimeStatePlacement::from_remote(
                                    request.placement,
                                ) {
                                    Ok(placement) => {
                                        service_for_interconnect
                                            .runtime
                                            .handle_state_sync_request(
                                                &placement,
                                                request.after_lsm,
                                            )
                                            .await
                                    }
                                    Err(error) => Err(error),
                                };
                                if let Err(error) = message.reply.send(Envelope::Control(
                                    ControlEnvelope::StateSyncResponse(
                                        RemoteStateSyncResponse {
                                            correlation_id: request.correlation_id,
                                            result: result.map(|snapshot| {
                                                snapshot.map(|snapshot| nervix_interconnect::StateSnapshotEnvelope {
                                                    lsm: snapshot.lsm,
                                                    payload: snapshot.payload,
                                                })
                                            }),
                                        },
                                    ),
                                )).await {
                                    warn!(error = %error, "failed to send state sync response");
                                }
                            }
                            Envelope::Control(ControlEnvelope::StateSyncResponse(response)) => {
                                service_for_interconnect.runtime.handle_state_sync_response(
                                    response.correlation_id,
                                    response.result.map(|snapshot| {
                                        snapshot.map(|snapshot| crate::runtime::PersistedRuntimeStateEntry {
                                            lsm: snapshot.lsm,
                                            payload: snapshot.payload,
                                        })
                                    }),
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
                                service_for_interconnect.runtime.handle_state_replication_ack(
                                    &message.peer_node_id,
                                    crate::runtime::StateSyncAck {
                                        placement,
                                        lsm: ack.lsm,
                                    },
                                );
                            }
                            Envelope::Control(ControlEnvelope::DescribeRelayRequest(request)) => {
                                let result = service_for_interconnect
                                    .handle_describe_stream_request(request.clone())
                                    .await;
                                if let Err(error) = message.reply.send(Envelope::Control(
                                    ControlEnvelope::DescribeRelayResponse(
                                        RemoteDescribeRelayResponse {
                                            correlation_id: request.correlation_id,
                                            result,
                                        },
                                    ),
                                )).await {
                                    warn!(error = %error, "failed to send DESCRIBE RELAY response");
                                }
                            }
                            Envelope::Control(ControlEnvelope::DescribeRelayResponse(response)) => {
                                service_for_interconnect.handle_describe_stream_response(response);
                            }
                            Envelope::Control(ControlEnvelope::DescribeIngestorRequest(request)) => {
                                let result = service_for_interconnect
                                    .handle_describe_ingestor_request(request.clone())
                                    .await;
                                if let Err(error) = message.reply.send(Envelope::Control(
                                    ControlEnvelope::DescribeIngestorResponse(
                                        RemoteDescribeIngestorResponse {
                                            correlation_id: request.correlation_id,
                                            result,
                                        },
                                    ),
                                )).await {
                                    warn!(error = %error, "failed to send DESCRIBE INGESTOR response");
                                }
                            }
                            Envelope::Control(ControlEnvelope::DescribeIngestorResponse(response)) => {
                                service_for_interconnect.handle_describe_ingestor_response(response);
                            }
                            Envelope::Control(ControlEnvelope::DataflowNodeStatusRequest(request)) => {
                                let result = service_for_interconnect
                                    .handle_dataflow_node_status_request(request.clone())
                                    .await;
                                if let Err(error) = message.reply.send(Envelope::Control(
                                    ControlEnvelope::DataflowNodeStatusResponse(
                                        RemoteDataflowNodeStatusResponse {
                                            correlation_id: request.correlation_id,
                                            result,
                                        },
                                    ),
                                )).await {
                                    warn!(error = %error, "failed to send dataflow node status response");
                                }
                            }
                            Envelope::Control(ControlEnvelope::DataflowNodeStatusResponse(response)) => {
                                service_for_interconnect.handle_dataflow_node_status_response(response);
                            }
                            Envelope::Control(ControlEnvelope::DescribeMetricsRequest(request)) => {
                                let result = service_for_interconnect
                                    .handle_describe_metrics_request(request.clone())
                                    .await;
                                if let Err(error) = message.reply.send(Envelope::Control(
                                    ControlEnvelope::DescribeMetricsResponse(
                                        RemoteDescribeMetricsResponse {
                                            correlation_id: request.correlation_id,
                                            result,
                                        },
                                    ),
                                )).await {
                                    warn!(error = %error, "failed to send DESCRIBE metrics response");
                                }
                            }
                            Envelope::Control(ControlEnvelope::DescribeMetricsResponse(response)) => {
                                service_for_interconnect.handle_describe_metrics_response(response);
                            }
                            Envelope::Control(ControlEnvelope::DescribeLookupRequest(request)) => {
                                let result = service_for_interconnect
                                    .handle_describe_lookup_request(request.clone())
                                    .await;
                                if let Err(error) = message.reply.send(Envelope::Control(
                                    ControlEnvelope::DescribeLookupResponse(
                                        RemoteDescribeLookupResponse {
                                            correlation_id: request.correlation_id,
                                            result,
                                        },
                                    ),
                                )).await {
                                    warn!(error = %error, "failed to send DESCRIBE LOOKUP response");
                                }
                            }
                            Envelope::Control(ControlEnvelope::DescribeLookupResponse(response)) => {
                                service_for_interconnect.handle_describe_lookup_response(response);
                            }
                            Envelope::Control(ControlEnvelope::LookupRequest(request)) => {
                                let result =
                                    service_for_interconnect.handle_lookup_request(request.clone()).await;
                                if let Err(error) = message.reply.send(Envelope::Control(
                                    ControlEnvelope::LookupResponse(RemoteLookupResponse {
                                        correlation_id: request.correlation_id,
                                        result: result.map(|record| record.map(|record| record.to_remote())),
                                    }),
                                )).await {
                                    warn!(error = %error, "failed to send LOOKUP response");
                                }
                            }
                            Envelope::Control(ControlEnvelope::LookupResponse(response)) => {
                                service_for_interconnect.handle_lookup_response(response);
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
                        Ok(message) => {
                            let _ = cluster_events.send(ServerEvent {
                                level: ServerEventLevel::Info as i32,
                                message,
                            });
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }));
        let consensus_events = events.clone();
        let mut consensus_event_rx = consensus.subscribe_events();
        let consensus_events_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    _ = consensus_events_shutdown.cancelled() => break,
                    received = consensus_event_rx.recv() => match received {
                        Ok(message) => {
                            let _ = consensus_events.send(ServerEvent {
                                level: ServerEventLevel::Info as i32,
                                message,
                            });
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }));

        info!(mode = grpc_mode.scheme(), addr = %grpc_listen_addr, "nervix gRPC server listening");
        info!(mode = cluster_api_mode.scheme(), addr = %cluster_api_listen_addr, "nervix cluster api server listening");
        info!(addr = %http_listen_addr, "nervix HTTP server listening");
        info!(addr = %https_listen_addr, "nervix HTTPS server listening");
        info!(addr = %observability_listen_addr, "nervix observability server listening");
        info!(addr = %web_console_listen_addr, "nervix web console server listening");
        if let Some(addr) = web_console_https_listen_addr {
            info!(addr = %addr, "nervix web console TLS server listening");
        }

        let cluster_api_resource_store = service.resource_store.clone();
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
                    let _ = relay.set_nodelay(true);
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
        let cluster_api_consensus = consensus.clone();
        let cluster_api_shutdown = shutdown.clone();
        let cluster_api_server = async move {
            if cluster_api_mode.is_tls() {
                serve_cluster_api_https(
                    cluster_api_consensus.clone(),
                    cluster_api_resource_store,
                    cluster_api_tls_server_config
                        .clone()
                        .ok_or_else(|| Report::new(AppError::LoadClusterApiTls))?,
                    cluster_api_listener,
                    cluster_api_shutdown.clone(),
                )
                .await
            } else {
                serve_cluster_api_http(
                    cluster_api_consensus.clone(),
                    cluster_api_resource_store,
                    cluster_api_listener,
                    cluster_api_shutdown.clone(),
                )
                .await
            }
        };
        let http_server = serve_http(runtime.clone(), http_listener, shutdown.clone());
        let https_server = serve_https(
            runtime.clone(),
            service.http_tls_server_config.clone(),
            https_listener,
            shutdown.clone(),
        );
        let observability_server = serve_observability_http(
            consensus.clone(),
            runtime.as_ref().clone(),
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

        let shutdown_after_run = self.shutdown.clone();
        let shutdown_cluster = cluster.clone();
        let shutdown_consensus = consensus.clone();
        let shutdown_interconnect = interconnect.clone();
        let shutdown_runtime = runtime.clone();
        let shutdown_service = service.clone();
        let shutdown_task = tokio::spawn(async move {
            shutdown.cancelled().await;
            if graceful_shutdown_drain {
                match tokio::time::timeout(
                    drain_timeout,
                    shutdown_service.drain_local_node_before_shutdown(),
                )
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
            shutdown_service.service_tasks.close();
            shutdown_service.service_tasks.wait().await;
            shutdown_runtime.shutdown().await;
            shutdown_consensus.shutdown().await;
            let _ = shutdown_cluster.initiate_shutdown();
            shutdown_interconnect.shutdown().await;
        });

        let result = tokio::try_join!(
            api_server,
            cluster_api_server,
            http_server,
            https_server,
            observability_server,
            web_console_server,
            web_console_https_server
        );
        if result.is_err() {
            shutdown_after_run.cancel();
        }
        if shutdown_after_run.is_cancelled() {
            let _ = shutdown_task.await;
            for task in background_tasks {
                await_background_task_shutdown(task, "application background task").await;
            }
        } else {
            shutdown_task.abort();
            for task in background_tasks {
                task.abort();
            }
        }

        tokio::task::spawn_blocking(move || {
            drop(service);
            drop(runtime);
            drop(consensus);
            drop(db);
        })
        .await
        .map_err(|error| {
            error!(error = %error, "failed to join database owner shutdown task");
            Report::new(AppError::OpenRegistry)
        })?;

        result?;

        Ok(())
    }
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
        process::Command,
        sync::atomic::{AtomicU64, Ordering},
    };

    use nervix_models::{
        AckMode, CreateDomain, CreateResource, CreateSchema, CreateStatement, DomainConfig,
        DomainPace, DomainSchedule, DomainState, DomainStatus, KafkaPartitionSchedule, Model,
        ModelKind, ResourceVersion, ResourceVersionStatus, ScheduledNode, SchemaField,
        SubscriptionLiteral,
    };
    use sorted_vec::SortedVec;

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn ensure_dev_tls_assets() {
        static DEV_TLS_READY: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        DEV_TLS_READY.get_or_init(|| {
            let status = Command::new("bash")
                .arg("scripts/generate_dev_tls.sh")
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .status()
                .expect("dev tls generation command should run");
            assert!(
                status.success(),
                "dev tls generation should succeed: {status}"
            );
        });
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

    fn identifier(raw: &str) -> Identifier {
        Identifier::try_from(raw).expect("valid identifier")
    }

    fn string_branch_key(field: &str, value: &str) -> Option<crate::runtime::BranchKey> {
        crate::runtime::BranchKey::from_fields([(
            identifier(field),
            runtime_schema::RuntimeValue::String(value.to_string()),
        )])
        .expect("test branch key must be non-empty")
        .into()
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
        let args = Args::parse_from([
            "nervix",
            "--node-id",
            "node-1",
            "--cluster-api-listen-addr",
            "127.0.0.1:47393",
            "--cluster-api-advertise-addr",
            "127.0.0.1:47393",
            "--observability-listen-addr",
            "127.0.0.1:19090",
        ]);
        let app = Application::try_from(args).expect("args should parse");
        assert_eq!(app.observability_listen_addr, test_addr(19090));
    }

    #[test]
    fn args_parse_temp_dir() {
        let args = Args::parse_from([
            "nervix",
            "--node-id",
            "node-1",
            "--cluster-api-listen-addr",
            "127.0.0.1:47393",
            "--cluster-api-advertise-addr",
            "127.0.0.1:47393",
            "--temp-dir",
            "/tmp/nervix-temp",
        ]);
        let app = Application::try_from(args).expect("args should parse");
        assert_eq!(app.temp_dir, PathBuf::from("/tmp/nervix-temp"));
    }

    #[test]
    fn args_parse_web_console_listen_addr() {
        let args = Args::parse_from([
            "nervix",
            "--node-id",
            "node-1",
            "--cluster-api-listen-addr",
            "127.0.0.1:47393",
            "--cluster-api-advertise-addr",
            "127.0.0.1:47393",
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

    async fn test_interconnect(node_id: &str) -> Arc<Transport> {
        ensure_dev_tls_assets();
        let tls = TlsConfigBundle::from_pem_files(
            "tls/dev/ca.pem",
            "tls/dev/node.pem",
            "tls/dev/node-key.pem",
        )
        .expect("tls bundle should load");
        let identity = LocalIdentity::generate(node_id.to_string());
        let verifier = PeerVerifier::new(|_| None);
        let addr = "127.0.0.1:0"
            .parse()
            .expect("ephemeral interconnect address must parse");
        let (transport, _rx) = Transport::bind(
            addr,
            InterconnectTransportMode::Tls,
            Some(tls),
            identity,
            verifier,
            Default::default(),
        )
        .await
        .expect("test transport should bind");
        Arc::new(transport)
    }

    fn schema_with_fields(fields: Vec<SchemaField>) -> CreateSchema {
        CreateSchema {
            name: identifier("events"),
            fields,
        }
    }

    fn scheduled_node(identifier_raw: &str, kind: ModelKind) -> ScheduledNode {
        ScheduledNode {
            identifier: identifier(identifier_raw),
            kind,
            config: Box::new(Model::Schema(schema_with_fields(Vec::new()))),
            effective_parameterization: None,
            kafka_partition_schedule: None,
            primary_node: Some("node-1".to_string()),
            assigned_nodes: vec!["node-1".to_string()],
        }
    }

    async fn create_test_domain(consensus: &ConsensusHandle, raw: &str) {
        let domain = Domain::parse(raw).expect("valid domain");
        let state = DomainState {
            id: domain,
            config: DomainConfig {
                pace: DomainPace::Paced,
                period: "30s".to_string(),
                skew: "1s".to_string(),
            },
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: DomainStartPoint::Resume,
        };

        for attempt in 0..50 {
            if consensus.put_domain(state.clone()).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(attempt < 49, "test domain should persist");
        }
    }

    async fn build_test_service(
        create_default_domain_flag: bool,
    ) -> (SessionServiceImpl, Arc<Registry>, PathBuf) {
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
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as u16;
        let grpc_addr = test_addr(64000u16.saturating_add(id));
        let cluster_listen_addr = test_addr(65000u16.saturating_add(id));
        let raft_addr = test_addr(49500u16.saturating_add(id));
        let interconnect_addr =
            cluster::derive_interconnect_addr(raft_addr).expect("must derive interconnect addr");
        let mut consensus = ConsensusHandle::from_database(
            db,
            ConsensusSettings {
                cluster_name: "test".to_string(),
                node_id: format!("test-node-{id}"),
                cluster_api_advertise_url: cluster_api_base_url(
                    InternalTransportMode::Http,
                    &cluster::HostPort::from(raft_addr),
                ),
                cluster_api_http_client: build_cluster_api_http_client(InternalTransportMode::Http)
                    .expect("test cluster api client should build"),
                node_unavailability_timeout: Duration::from_secs(10),
                raft_heartbeat_interval: Duration::from_millis(50),
                raft_election_timeout_min: Duration::from_millis(150),
                raft_election_timeout_max: Duration::from_millis(300),
            },
        )
        .await
        .expect("consensus should open");
        consensus.set_local_grpc_advertise_addr(grpc_addr.to_string());
        consensus
            .maybe_initialize()
            .await
            .expect("single-node consensus should initialize");
        if create_default_domain_flag {
            create_test_domain(&consensus, "default").await;
        }
        let expected_leader = format!("test-node-{id}");
        for _ in 0..50 {
            if consensus.current_leader().await.as_deref() == Some(expected_leader.as_str()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let consensus = Arc::new(consensus);
        let interconnect = test_interconnect(expected_leader.as_str()).await;
        let cluster = Arc::new(
            cluster::start_cluster(cluster::ClusterSettings {
                cluster_id: "test".to_string(),
                node_id: format!("test-node-{id}"),
                cluster_listen_addr,
                cluster_advertise_addr: cluster_listen_addr.into(),
                grpc_listen_addr: grpc_addr,
                grpc_advertise_addr: grpc_addr.to_string(),
                web_console_advertise_addr: format!("http://{}", grpc_addr),
                cluster_api_listen_addr: raft_addr,
                cluster_api_advertise_addr: cluster_api_base_url(
                    InternalTransportMode::Http,
                    &cluster::HostPort::from(raft_addr),
                ),
                interconnect_listen_addr: interconnect_addr,
                interconnect_advertise_addr: interconnect_addr.into(),
                interconnect_mode: "https".to_string(),
                interconnect_public_key: "00".repeat(32),
                bootstrap_host: None,
                node_unavailability_timeout: Duration::from_secs(10),
            })
            .await
            .expect("cluster should start"),
        );
        let service = SessionServiceImpl {
            cluster,
            consensus,
            registry: registry.clone(),
            resource_store: Arc::new(
                ResourceStore::open(path.join("resources")).expect("resource store should open"),
            ),
            cluster_api_clients: Arc::new(
                ClusterApiClients::build().expect("test cluster api clients should build"),
            ),
            http_tls_server_config: Arc::new(RwLock::new(None)),
            runtime: Arc::new(Runtime::new()),
            replica_count: 0,
            shutdown: CancellationToken::new(),
            events: broadcast::channel(16).0,
            subscription_interest_counts: Arc::new(DashMap::with_hasher(RandomState::new())),
            interconnect,
            domain_clocks: Arc::new(DashMap::with_hasher(RandomState::new())),
            domain_clock_events: Arc::new(Notify::new()),
            next_cluster_command_correlation_id: Arc::new(AtomicU64::new(1)),
            pending_cluster_commands: Arc::new(DashMap::default()),
            service_tasks: TaskTracker::new(),
            configured_basic_auth: None,
            auth_rate_limiter: SessionServiceImpl::new_auth_rate_limiter(),
            failed_auth_rate_limit_keys: Arc::new(DashMap::with_hasher(RandomState::new())),
        };
        (service, registry, path)
    }

    #[test]
    fn completion_context_preserves_prefix_for_post_filtering() {
        let input = "CREATE SCHE";
        let (grammar_input, grammar_cursor, prefix) = completion_context(input, input.len());

        assert_eq!(grammar_input, "CREATE ");
        assert_eq!(grammar_cursor, "CREATE ".len());
        assert_eq!(prefix, "sche");
    }

    #[test]
    fn keyword_completion_is_filtered_by_original_prefix() {
        let input = "CREATE SCHE";
        let (grammar_input, grammar_cursor, prefix) = completion_context(input, input.len());
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
    fn drop_node_quorum_error_allows_available_current_quorum() {
        let voters = BTreeSet::from([
            "node-1".to_string(),
            "node-2".to_string(),
            "node-3".to_string(),
        ]);
        let live_node_ids = BTreeSet::from(["node-1".to_string(), "node-3".to_string()]);

        assert!(
            SessionServiceImpl::drop_node_quorum_error("node-2", &voters, &live_node_ids).is_none()
        );
    }

    #[test]
    fn move_next_scheduled_node_for_drain_moves_only_one_assigned_node() {
        let domain = Domain::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule {
            domain: domain.clone(),
            nodes: vec![
                ScheduledNode {
                    primary_node: Some("node-2".to_string()),
                    assigned_nodes: vec!["node-2".to_string()],
                    ..scheduled_node("ingest_notifications", ModelKind::Ingestor)
                },
                ScheduledNode {
                    primary_node: Some("node-2".to_string()),
                    assigned_nodes: vec!["node-2".to_string()],
                    ..scheduled_node("emit_notifications", ModelKind::Emitter)
                },
            ],
        };
        let desired = DomainSchedule {
            domain,
            nodes: vec![
                ScheduledNode {
                    primary_node: Some("node-1".to_string()),
                    assigned_nodes: vec!["node-1".to_string()],
                    ..scheduled_node("ingest_notifications", ModelKind::Ingestor)
                },
                ScheduledNode {
                    primary_node: Some("node-3".to_string()),
                    assigned_nodes: vec!["node-3".to_string()],
                    ..scheduled_node("emit_notifications", ModelKind::Emitter)
                },
            ],
        };

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            "node-2",
            &BTreeSet::from(["node-1".to_string(), "node-3".to_string()]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "ingestor ingest_notifications".to_string(),
                promoted_replica: None,
                fallback_node: Some("node-1".to_string()),
            })
        );
        assert_eq!(schedule.nodes[0].assigned_nodes, vec!["node-1".to_string()]);
        assert_eq!(schedule.nodes[1].assigned_nodes, vec!["node-2".to_string()]);
    }

    #[test]
    fn move_next_scheduled_node_for_drain_promotes_live_replica() {
        let domain = Domain::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule {
            domain: domain.clone(),
            nodes: vec![ScheduledNode {
                primary_node: Some("node-2".to_string()),
                assigned_nodes: vec![
                    "node-2".to_string(),
                    "node-3".to_string(),
                    "node-4".to_string(),
                ],
                ..scheduled_node("dedup_notifications", ModelKind::Deduplicator)
            }],
        };
        let desired = DomainSchedule {
            domain,
            nodes: vec![ScheduledNode {
                primary_node: Some("node-1".to_string()),
                assigned_nodes: vec!["node-1".to_string(), "node-3".to_string()],
                ..scheduled_node("dedup_notifications", ModelKind::Deduplicator)
            }],
        };

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            "node-2",
            &BTreeSet::from(["node-1".to_string(), "node-3".to_string()]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "deduplicator dedup_notifications".to_string(),
                promoted_replica: Some("node-3".to_string()),
                fallback_node: None,
            })
        );
        assert_eq!(schedule.nodes[0].primary_node.as_deref(), Some("node-3"));
        assert_eq!(
            schedule.nodes[0].assigned_nodes,
            vec!["node-3".to_string(), "node-1".to_string()]
        );
    }

    #[test]
    fn move_next_scheduled_node_for_drain_ignores_unavailable_replica() {
        let domain = Domain::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule {
            domain: domain.clone(),
            nodes: vec![ScheduledNode {
                primary_node: Some("node-2".to_string()),
                assigned_nodes: vec!["node-2".to_string(), "node-3".to_string()],
                ..scheduled_node("dedup_notifications", ModelKind::Deduplicator)
            }],
        };
        let desired = DomainSchedule {
            domain,
            nodes: vec![ScheduledNode {
                primary_node: Some("node-1".to_string()),
                assigned_nodes: vec!["node-1".to_string()],
                ..scheduled_node("dedup_notifications", ModelKind::Deduplicator)
            }],
        };

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            "node-2",
            &BTreeSet::from(["node-1".to_string()]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "deduplicator dedup_notifications".to_string(),
                promoted_replica: None,
                fallback_node: Some("node-1".to_string()),
            })
        );
        assert_eq!(schedule.nodes[0].primary_node.as_deref(), Some("node-1"));
        assert_eq!(schedule.nodes[0].assigned_nodes, vec!["node-1".to_string()]);
    }

    #[test]
    fn merge_existing_schedule_data_promotes_live_replica_when_primary_dies() {
        let domain = Domain::parse("payments").expect("valid domain");
        let mut next = DomainSchedule {
            domain: domain.clone(),
            nodes: vec![ScheduledNode {
                primary_node: Some("node-1".to_string()),
                assigned_nodes: vec!["node-1".to_string(), "node-4".to_string()],
                ..scheduled_node("dedup_notifications", ModelKind::Deduplicator)
            }],
        };
        let existing = DomainSchedule {
            domain,
            nodes: vec![ScheduledNode {
                primary_node: Some("node-2".to_string()),
                assigned_nodes: vec![
                    "node-2".to_string(),
                    "node-3".to_string(),
                    "node-4".to_string(),
                ],
                ..scheduled_node("dedup_notifications", ModelKind::Deduplicator)
            }],
        };

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &["node-1".to_string(), "node-3".to_string()],
        );

        assert_eq!(next.nodes[0].primary_node.as_deref(), Some("node-3"));
        assert_eq!(
            next.nodes[0].assigned_nodes,
            vec!["node-3".to_string(), "node-1".to_string()]
        );
    }

    #[test]
    fn merge_existing_schedule_data_falls_back_to_fresh_assignment_without_live_replica() {
        let domain = Domain::parse("payments").expect("valid domain");
        let mut next = DomainSchedule {
            domain: domain.clone(),
            nodes: vec![ScheduledNode {
                primary_node: Some("node-1".to_string()),
                assigned_nodes: vec!["node-1".to_string()],
                ..scheduled_node("dedup_notifications", ModelKind::Deduplicator)
            }],
        };
        let existing = DomainSchedule {
            domain,
            nodes: vec![ScheduledNode {
                primary_node: Some("node-2".to_string()),
                assigned_nodes: vec!["node-2".to_string(), "node-3".to_string()],
                ..scheduled_node("dedup_notifications", ModelKind::Deduplicator)
            }],
        };

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &["node-1".to_string()],
        );

        assert_eq!(next.nodes[0].primary_node.as_deref(), Some("node-1"));
        assert_eq!(next.nodes[0].assigned_nodes, vec!["node-1".to_string()]);
    }

    #[test]
    fn merge_existing_schedule_data_preserves_matching_ingestor_schedule_and_assignment() {
        let domain = Domain::parse("payments").expect("valid domain");
        let preserved_schedule = KafkaPartitionSchedule::new(2, vec![0, 1], 7);
        let mut next = DomainSchedule {
            domain: domain.clone(),
            nodes: vec![ScheduledNode {
                assigned_nodes: vec!["node-2".to_string(), "node-3".to_string()],
                ..scheduled_node("ingest_notifications", ModelKind::Ingestor)
            }],
        };
        let existing = DomainSchedule {
            domain,
            nodes: vec![ScheduledNode {
                kafka_partition_schedule: Some(preserved_schedule.clone()),
                assigned_nodes: vec!["node-1".to_string()],
                ..scheduled_node("ingest_notifications", ModelKind::Ingestor)
            }],
        };

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[
                "node-1".to_string(),
                "node-2".to_string(),
                "node-3".to_string(),
            ],
        );

        assert_eq!(
            next.nodes[0].kafka_partition_schedule,
            Some(preserved_schedule)
        );
        assert_eq!(next.nodes[0].assigned_nodes, vec!["node-1".to_string()]);
    }

    #[test]
    fn merge_existing_schedule_data_ignores_non_matching_nodes() {
        let domain = Domain::parse("payments").expect("valid domain");
        let mut next = DomainSchedule {
            domain: domain.clone(),
            nodes: vec![
                scheduled_node("ingest_notifications", ModelKind::Ingestor),
                scheduled_node("kafka_main", ModelKind::Client),
            ],
        };
        let existing = DomainSchedule {
            domain,
            nodes: vec![
                ScheduledNode {
                    kafka_partition_schedule: Some(KafkaPartitionSchedule::new(2, vec![0, 1], 3)),
                    ..scheduled_node("other_ingestor", ModelKind::Ingestor)
                },
                ScheduledNode {
                    kafka_partition_schedule: Some(KafkaPartitionSchedule::new(1, vec![0], 2)),
                    ..scheduled_node("ingest_notifications", ModelKind::Client)
                },
            ],
        };

        SessionServiceImpl::merge_existing_schedule_data(&mut next, Some(&existing), &[]);

        assert_eq!(next.nodes[0].kafka_partition_schedule, None);
        assert_eq!(next.nodes[1].kafka_partition_schedule, None);
    }

    #[test]
    fn resource_ref_suggestions_expand_known_resource_names() {
        let resources = ResourceVersionStatus {
            next_version_by_identifier: SortedVec::from_unsorted(vec![
                (identifier("fraud_model"), 2),
                (identifier("proto"), 1),
            ]),
            ..Default::default()
        };

        assert_eq!(
            resource_ref_suggestions(&resources, "pr"),
            vec!["proto".to_string()]
        );
        assert_eq!(
            resource_ref_suggestions(&resources, ""),
            vec!["fraud_model".to_string(), "proto".to_string()]
        );
    }

    #[test]
    fn resource_version_suggestions_expand_known_versions() {
        let resources = ResourceVersionStatus {
            versions: SortedVec::from_unsorted(vec![
                ResourceVersion {
                    id: nervix_models::ResourceId::new(identifier("proto"), 1),
                    root_checksum: "a".to_string(),
                    manifest_checksum: "a".to_string(),
                    file_count: 1,
                    total_bytes: 1,
                    created_at: Timestamp::from_unix_nanos(1),
                    created_by_node: "node-1".to_string(),
                },
                ResourceVersion {
                    id: nervix_models::ResourceId::new(identifier("proto"), 12),
                    root_checksum: "b".to_string(),
                    manifest_checksum: "b".to_string(),
                    file_count: 1,
                    total_bytes: 1,
                    created_at: Timestamp::from_unix_nanos(1),
                    created_by_node: "node-1".to_string(),
                },
            ]),
            ..Default::default()
        };

        assert_eq!(
            resource_version_suggestions(&resources, &identifier("proto"), ""),
            vec!["1".to_string(), "12".to_string()]
        );
        assert_eq!(
            resource_version_suggestions(&resources, &identifier("proto"), "1"),
            vec!["1".to_string(), "12".to_string()]
        );
        assert_eq!(
            requested_resource_versions(
                "DESCRIBE RESOURCE proto VERSION ",
                "DESCRIBE RESOURCE proto VERSION ".len()
            ),
            Some(identifier("proto"))
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
        };

        assert!(validate_domain_config(&config).is_ok());
    }

    #[test]
    fn validate_domain_config_accepts_unpaced_domains_without_tick_period() {
        let config = DomainConfig {
            pace: DomainPace::Unpaced,
            period: "not-a-duration".to_string(),
            skew: "not-a-duration".to_string(),
        };

        assert!(validate_domain_config(&config).is_ok());
    }

    #[test]
    fn interconnect_initiation_uses_strict_node_id_order() {
        assert!(should_initiate_interconnect("node-1", "node-2"));
        assert!(!should_initiate_interconnect("node-2", "node-1"));
        assert!(!should_initiate_interconnect("node-2", "node-2"));
    }

    #[test]
    fn request_domain_helpers_cover_current_state_and_validation() {
        assert_eq!(parse_request_domain(""), Err(RequestDomainError::Missing));
        assert_eq!(
            parse_request_domain(" tenant_a "),
            Ok(Domain::parse("tenant_a").expect("valid domain"))
        );
        assert_eq!(
            parse_request_domain("bad.domain"),
            Err(RequestDomainError::Invalid)
        );
    }

    #[test]
    fn parse_and_encoding_helpers_roundtrip() {
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
        assert_eq!(decode_hex(&hex), Some(bytes));
        assert_eq!(decode_hex("abc"), None);

        let encoded = encode_cbor(&vec!["a".to_string(), "b".to_string()]).expect("encode");
        let decoded: Vec<String> = decode_cbor(&encoded).expect("decode");
        assert_eq!(decoded, vec!["a", "b"]);
        assert!(parse_human_duration("oops").is_err());
        assert!(parse_human_bytes("oops").is_err());
    }

    #[test]
    fn server_args_parse_memory_pressure_options() {
        let args = Args::parse_from([
            "nervix",
            "--node-id",
            "node-1",
            "--cluster-api-listen-addr",
            "127.0.0.1:47392",
            "--cluster-api-advertise-addr",
            "127.0.0.1:47392",
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
        let args = Args::parse_from([
            "nervix",
            "--node-id",
            "node-1",
            "--cluster-api-listen-addr",
            "127.0.0.1:47392",
            "--cluster-api-advertise-addr",
            "127.0.0.1:47392",
            "--memory-high-watermark",
            "2MiB",
        ]);

        let error = Application::try_from(args).expect_err("low watermark is required");
        assert!(format!("{error:?}").contains("memory high watermark requires"));
    }

    #[test]
    fn server_args_parse_opentelemetry_options() {
        let args = Args::parse_from([
            "nervix",
            "--node-id",
            "node-1",
            "--cluster-api-listen-addr",
            "127.0.0.1:47392",
            "--cluster-api-advertise-addr",
            "127.0.0.1:47392",
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
        let args = Args::parse_from([
            "nervix",
            "--node-id",
            "node-1",
            "--cluster-api-listen-addr",
            "127.0.0.1:47392",
            "--cluster-api-advertise-addr",
            "127.0.0.1:47392",
        ]);

        assert!(!args.otel_enabled);
        assert_eq!(args.otel_otlp_endpoint, "http://127.0.0.1:4317");
        assert_eq!(args.otel_service_name, "nervix");
        assert_eq!(args.otel_trace_sample_ratio, 1.0);
    }

    #[test]
    fn server_args_only_require_opentelemetry_enable_flag_to_enable_export() {
        let args = Args::parse_from([
            "nervix",
            "--node-id",
            "node-1",
            "--cluster-api-listen-addr",
            "127.0.0.1:47392",
            "--cluster-api-advertise-addr",
            "127.0.0.1:47392",
            "--otel-enabled",
        ]);

        assert!(args.otel_enabled);
        assert_eq!(args.otel_otlp_endpoint, "http://127.0.0.1:4317");
        assert_eq!(args.otel_service_name, "nervix");
        assert_eq!(args.otel_trace_sample_ratio, 1.0);
    }

    #[test]
    fn server_args_reject_invalid_opentelemetry_sample_ratio() {
        let result = Args::try_parse_from([
            "nervix",
            "--node-id",
            "node-1",
            "--cluster-api-listen-addr",
            "127.0.0.1:47392",
            "--cluster-api-advertise-addr",
            "127.0.0.1:47392",
            "--otel-trace-sample-ratio",
            "2.0",
        ]);

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

        let query = "CREATE RELAY orders SCHEMA notification;";
        let identifier = identifier("orders");
        assert_eq!(find_identifier_span(query, &identifier), Some(13..19));

        let domain = Domain::parse("default").expect("valid domain");
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
    fn decode_verifying_key_accepts_valid_hex_key() {
        use ed25519_dalek::SigningKey;

        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let verifying = signing_key.verifying_key();
        let hex = encode_hex(verifying.as_bytes());
        let decoded = decode_verifying_key(&hex).expect("must decode verifying key");
        assert_eq!(decoded, verifying);
        assert_eq!(decode_verifying_key("zz"), None);
    }

    #[test]
    fn session_subscription_definition_uses_collect_form() {
        assert_eq!(
            session_subscription_definition(
                &identifier("events"),
                SubscriptionDeliveryBehavior::Blocking,
                None,
                None,
            ),
            "TO events"
        );
        assert_eq!(
            session_subscription_definition(
                &identifier("events"),
                SubscriptionDeliveryBehavior::Blocking,
                None,
                Some("SET seen = true WHERE tenant == \"acme\"")
            ),
            "TO events SET seen = true WHERE tenant == \"acme\""
        );
        assert_eq!(
            session_subscription_definition(
                &identifier("events"),
                SubscriptionDeliveryBehavior::Dropping,
                Some("0.1"),
                Some("WHERE tenant == \"acme\"")
            ),
            "TO events DROPPING BATCH SAMPLE RATE 0.1 WHERE tenant == \"acme\""
        );
    }

    #[test]
    fn parse_subscription_literal_enforces_declared_types() {
        let field = identifier("created_at");
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
                &identifier("active"),
                &ParseAsType::Bool,
                &SubscriptionLiteral::Bool(true)
            ),
            Ok(runtime_schema::RuntimeValue::Bool(true))
        ));
        let err = parse_subscription_literal(
            &identifier("user_id"),
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
            record: runtime_schema::RuntimeRecord::from_fields([]),
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
            record: runtime_schema::RuntimeRecord::from_fields([(
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
            record: runtime_schema::RuntimeRecord::from_fields([(
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
    async fn session_subscriptions_track_definitions_and_cleanup_tasks() {
        let mut subscriptions = SessionSubscriptions::new();
        let (tx, _rx) = mpsc::channel(4);
        let events = crate::runtime::RelayBroadcast::with_capacity(
            std::num::NonZeroUsize::new(4).expect("test relay capacity must be nonzero"),
        );
        let events_rx = events.new_receiver();
        subscriptions.insert(
            Domain::parse("default").expect("valid domain"),
            "TO events".to_string(),
            identifier("events"),
            SessionSubscriptionTaskConfig {
                filter_map: None,
                sensitivity: nervix_vm::SchemaSensitivity::default(),
                delivery_behavior: SubscriptionDeliveryBehavior::Blocking,
                batch_sample_rate: None,
                runtime: Arc::new(Runtime::default()),
                materialized_stream_owner_nodes: HashMap::default(),
                receiver: events_rx,
                tx,
            },
        );

        let removed = subscriptions
            .remove("TO events")
            .await
            .expect("subscription should be removed");
        assert_eq!(removed.0.as_str(), "default");
        assert_eq!(removed.1.as_str(), "events");
        assert_eq!(removed.2, "TO events");
        assert!(subscriptions.remove("TO missing").await.is_none());
    }

    #[tokio::test]
    async fn create_domain_if_not_exists_returns_already_existed() {
        let (service, _registry, path) = build_test_service(false).await;

        let first = service
            .create_domain(CreateStatement::new(
                CreateDomain {
                    id: Domain::parse("prod").expect("valid domain"),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
                        period: "0ms".to_string(),
                        skew: "0ms".to_string(),
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
                    id: Domain::parse("prod").expect("valid domain"),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
                        period: "0ms".to_string(),
                        skew: "0ms".to_string(),
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
        let (service, _registry, path) = build_test_service(false).await;

        let first = service
            .create_resource(CreateStatement::new(
                CreateResource {
                    identifier: identifier("fraud_model"),
                },
                false,
            ))
            .await;
        assert!(first.success);
        assert!(!first.already_existed);

        let duplicate = service
            .create_resource(CreateStatement::new(
                CreateResource {
                    identifier: identifier("fraud_model"),
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
    async fn process_command_create_if_not_exists_returns_already_existed_for_models() {
        let (service, registry, path) = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions {
            subscriptions: HashMap::new(),
        };

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
            .get(
                &Domain::parse("default").expect("valid domain"),
                ModelKind::Schema,
                &identifier("notification"),
            )
            .expect("registry get should succeed")
            .expect("schema should exist");
        let Model::Schema(schema) = schema else {
            panic!("stored model must be a schema");
        };
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].name.as_str(), "user_id");

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_executes_semicolon_separated_batch_without_trailing_semicolon() {
        let (service, registry, path) = build_test_service(false).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions {
            subscriptions: HashMap::new(),
        };

        let result = service
            .process_command(
                CommandRequest {
                    query: "CREATE DOMAIN prod; CREATE RELAY notifications SCHEMA notification; \
                            CREATE SCHEMA notification ( user_id U32 )"
                        .to_string(),
                    domain: "prod".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(result.success, "command must succeed: {}", result.message);
        assert_eq!(result.results.len(), 3);
        assert!(result.message.contains("created domain 'prod'"));
        assert!(result.message.contains("stored model 'notifications'"));
        assert!(result.message.contains("stored model 'notification'"));

        let schema = registry
            .get(
                &Domain::parse("prod").expect("valid domain"),
                ModelKind::Schema,
                &identifier("notification"),
            )
            .expect("registry get should succeed");
        assert!(
            schema.is_some(),
            "batch should create schema in prod domain"
        );
        let relay = registry
            .get(
                &Domain::parse("prod").expect("valid domain"),
                ModelKind::Relay,
                &identifier("notifications"),
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
    async fn process_command_batch_returns_prior_successes_before_error() {
        let (service, _registry, path) = build_test_service(false).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions {
            subscriptions: HashMap::new(),
        };

        let result = service
            .process_command(
                CommandRequest {
                    query: "CREATE DOMAIN prod; CREATE DOMAIN prod".to_string(),
                    domain: "prod".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(!result.success);
        assert_eq!(result.results.len(), 2);
        assert!(result.results[0].success);
        assert_eq!(result.results[0].message, "created domain 'prod'");
        assert!(!result.results[1].success);
        assert!(result.results[1].message.contains("already exists"));
        assert!(result.message.contains("created domain 'prod'"));
        assert!(result.message.contains("already exists"));

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_batch_returns_each_domain_create_result() {
        let (service, _registry, path) = build_test_service(false).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions {
            subscriptions: HashMap::new(),
        };

        let result = service
            .process_command(
                CommandRequest {
                    query: "CREATE DOMAIN alpha; CREATE DOMAIN beta".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(result.success, "command must succeed: {}", result.message);
        assert_eq!(result.results.len(), 2);
        assert_eq!(result.results[0].message, "created domain 'alpha'");
        assert_eq!(result.results[1].message, "created domain 'beta'");
        assert!(result.message.contains("created domain 'alpha'"));
        assert!(result.message.contains("created domain 'beta'"));

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_model_create_batch_is_atomic_on_registry_failure() {
        let (service, registry, path) = build_test_service(false).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions {
            subscriptions: HashMap::new(),
        };

        let result = service
            .process_command(
                CommandRequest {
                    query: "CREATE DOMAIN prod; CREATE RELAY notifications SCHEMA missing_schema; \
                            CREATE SCHEMA notification ( user_id U32 )"
                        .to_string(),
                    domain: "prod".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(!result.success);
        assert_eq!(result.results.len(), 2);
        assert!(result.results[0].success);
        assert!(!result.results[1].success);

        let domain = Domain::parse("prod").expect("valid domain");
        let relay = registry
            .get(&domain, ModelKind::Relay, &identifier("notifications"))
            .expect("registry get should succeed");
        assert!(relay.is_none(), "failed model batch must not persist relay");
        let schema = registry
            .get(&domain, ModelKind::Schema, &identifier("notification"))
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
        let (service, registry, path) = build_test_service(true).await;
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
            .get(
                &Domain::parse("default").expect("valid domain"),
                ModelKind::Schema,
                &identifier("web_console_event"),
            )
            .expect("registry get should succeed");
        assert!(schema.is_some());

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn web_console_rejects_upload_and_supports_suggest_requests() {
        let (service, _registry, path) = build_test_service(true).await;
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
    async fn process_command_preserves_detached_deduplicator_and_emitter_modes() {
        let path = test_db_path();
        let db = Database::builder(&path)
            .open()
            .expect("database should open");
        let registry = Arc::new(
            Registry::from_database(db.clone(), Some(path.as_path()))
                .expect("registry should open"),
        );
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as u16;
        let grpc_addr = test_addr(51000u16.saturating_add(id));
        let cluster_listen_addr = test_addr(52000u16.saturating_add(id));
        let raft_addr = test_addr(53000u16.saturating_add(id));
        let interconnect_addr =
            cluster::derive_interconnect_addr(raft_addr).expect("must derive interconnect addr");
        let mut consensus = ConsensusHandle::from_database(
            db,
            ConsensusSettings {
                cluster_name: "test".to_string(),
                node_id: format!("test-node-{id}"),
                cluster_api_advertise_url: cluster_api_base_url(
                    InternalTransportMode::Http,
                    &cluster::HostPort::from(raft_addr),
                ),
                cluster_api_http_client: build_cluster_api_http_client(InternalTransportMode::Http)
                    .expect("test cluster api client should build"),
                node_unavailability_timeout: Duration::from_secs(10),
                raft_heartbeat_interval: Duration::from_millis(50),
                raft_election_timeout_min: Duration::from_millis(150),
                raft_election_timeout_max: Duration::from_millis(300),
            },
        )
        .await
        .expect("consensus should open");
        consensus.set_local_grpc_advertise_addr(grpc_addr.to_string());
        consensus
            .maybe_initialize()
            .await
            .expect("single-node consensus should initialize");
        let expected_leader = format!("test-node-{id}");
        for _ in 0..50 {
            if consensus.current_leader().await.as_deref() == Some(expected_leader.as_str()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            consensus.current_leader().await.as_deref(),
            Some(expected_leader.as_str()),
            "single-node consensus must report itself as leader before command processing",
        );
        create_test_domain(&consensus, "default").await;
        let consensus = Arc::new(consensus);
        let interconnect = test_interconnect(expected_leader.as_str()).await;
        let cluster = Arc::new(
            cluster::start_cluster(cluster::ClusterSettings {
                cluster_id: "test".to_string(),
                node_id: format!("test-node-{id}"),
                cluster_listen_addr,
                cluster_advertise_addr: cluster_listen_addr.into(),
                grpc_listen_addr: grpc_addr,
                grpc_advertise_addr: grpc_addr.to_string(),
                web_console_advertise_addr: format!("http://{}", grpc_addr),
                cluster_api_listen_addr: raft_addr,
                cluster_api_advertise_addr: cluster_api_base_url(
                    InternalTransportMode::Http,
                    &cluster::HostPort::from(raft_addr),
                ),
                interconnect_listen_addr: interconnect_addr,
                interconnect_advertise_addr: interconnect_addr.into(),
                interconnect_mode: "https".to_string(),
                interconnect_public_key: "00".repeat(32),
                bootstrap_host: None,
                node_unavailability_timeout: Duration::from_secs(10),
            })
            .await
            .expect("cluster should start"),
        );
        let service = SessionServiceImpl {
            cluster,
            consensus,
            registry: registry.clone(),
            resource_store: Arc::new(
                ResourceStore::open(path.join("resources")).expect("resource store should open"),
            ),
            cluster_api_clients: Arc::new(
                ClusterApiClients::build().expect("test cluster api clients should build"),
            ),
            http_tls_server_config: Arc::new(RwLock::new(None)),
            runtime: Arc::new(Runtime::new()),
            replica_count: 0,
            shutdown: CancellationToken::new(),
            events: broadcast::channel(16).0,
            subscription_interest_counts: Arc::new(DashMap::with_hasher(RandomState::new())),
            interconnect,
            domain_clocks: Arc::new(DashMap::with_hasher(RandomState::new())),
            domain_clock_events: Arc::new(Notify::new()),
            next_cluster_command_correlation_id: Arc::new(AtomicU64::new(1)),
            pending_cluster_commands: Arc::new(DashMap::default()),
            service_tasks: TaskTracker::new(),
            configured_basic_auth: None,
            auth_rate_limiter: SessionServiceImpl::new_auth_rate_limiter(),
            failed_auth_rate_limit_keys: Arc::new(DashMap::with_hasher(RandomState::new())),
        };
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions {
            subscriptions: HashMap::new(),
        };
        let commands = [
            "CREATE SCHEMA notification ( user_id I64 );",
            "CREATE JSON WIRE SCHEMA notification_wire ( user_id integer );",
            "CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA \
             notification;",
            "CREATE RELAY notifications SCHEMA notification UNPARAMETERIZED;",
            "CREATE RELAY forwarded_notifications SCHEMA notification UNPARAMETERIZED;",
            "CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' \
             };",
            "CREATE INGESTOR notifications_ingestor TO notifications DECODE USING \
             notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB TIMESTAMP \
             NOW FROM KAFKA kafka_main TOPIC notifications OFFSET BY CONSUMER GROUP \
             notifications_group MODE NO_ACK PARALLEL MAX 1 ON MESSAGE ERROR LOG ON GENERAL ERROR \
             LOG;",
            "CREATE DETACHED DEDUPLICATOR passthrough FROM notifications TO \
             forwarded_notifications UNPARAMETERIZED DEDUPLICATE ON notifications.user_id MAX \
             TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
            "CREATE DETACHED EMITTER kafka_forward FROM notifications ENCODE USING \
             notification_codec TO KAFKA kafka_main TOPIC notifications_out ON MESSAGE ERROR LOG \
             ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;",
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
            .get(
                &Domain::parse("default").expect("valid domain"),
                ModelKind::Deduplicator,
                &Identifier::parse("passthrough").expect("valid identifier"),
            )
            .expect("registry get should succeed")
            .expect("deduplicator should exist");
        let emitter = registry
            .get(
                &Domain::parse("default").expect("valid domain"),
                ModelKind::Emitter,
                &Identifier::parse("kafka_forward").expect("valid identifier"),
            )
            .expect("registry get should succeed")
            .expect("emitter should exist");

        let Model::Deduplicator(deduplicator) = deduplicator else {
            panic!("stored model must be a deduplicator");
        };
        let Model::Emitter(emitter) = emitter else {
            panic!("stored model must be an emitter");
        };
        assert_eq!(deduplicator.mode, AckMode::Detached);
        assert_eq!(emitter.mode, AckMode::Detached);

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_creates_unifier_model() {
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
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as u16;
        let grpc_addr = test_addr(61000u16.saturating_add(id));
        let cluster_listen_addr = test_addr(62000u16.saturating_add(id));
        let raft_addr = test_addr(63000u16.saturating_add(id));
        let interconnect_addr =
            cluster::derive_interconnect_addr(raft_addr).expect("must derive interconnect addr");
        let mut consensus = ConsensusHandle::from_database(
            db,
            ConsensusSettings {
                cluster_name: "test".to_string(),
                node_id: format!("test-node-{id}"),
                cluster_api_advertise_url: cluster_api_base_url(
                    InternalTransportMode::Http,
                    &cluster::HostPort::from(raft_addr),
                ),
                cluster_api_http_client: build_cluster_api_http_client(InternalTransportMode::Http)
                    .expect("test cluster api client should build"),
                node_unavailability_timeout: Duration::from_secs(10),
                raft_heartbeat_interval: Duration::from_millis(50),
                raft_election_timeout_min: Duration::from_millis(150),
                raft_election_timeout_max: Duration::from_millis(300),
            },
        )
        .await
        .expect("consensus should open");
        consensus.set_local_grpc_advertise_addr(grpc_addr.to_string());
        consensus
            .maybe_initialize()
            .await
            .expect("single-node consensus should initialize");
        create_test_domain(&consensus, "default").await;
        let expected_leader = format!("test-node-{id}");
        for _ in 0..50 {
            if consensus.current_leader().await.as_deref() == Some(expected_leader.as_str()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let consensus = Arc::new(consensus);
        let interconnect = test_interconnect(expected_leader.as_str()).await;
        let cluster = Arc::new(
            cluster::start_cluster(cluster::ClusterSettings {
                cluster_id: "test".to_string(),
                node_id: format!("test-node-{id}"),
                cluster_listen_addr,
                cluster_advertise_addr: cluster_listen_addr.into(),
                grpc_listen_addr: grpc_addr,
                grpc_advertise_addr: grpc_addr.to_string(),
                web_console_advertise_addr: format!("http://{}", grpc_addr),
                cluster_api_listen_addr: raft_addr,
                cluster_api_advertise_addr: cluster_api_base_url(
                    InternalTransportMode::Http,
                    &cluster::HostPort::from(raft_addr),
                ),
                interconnect_listen_addr: interconnect_addr,
                interconnect_advertise_addr: interconnect_addr.into(),
                interconnect_mode: "https".to_string(),
                interconnect_public_key: "00".repeat(32),
                bootstrap_host: None,
                node_unavailability_timeout: Duration::from_secs(10),
            })
            .await
            .expect("cluster should start"),
        );
        let service = SessionServiceImpl {
            cluster,
            consensus,
            registry: registry.clone(),
            resource_store: Arc::new(
                ResourceStore::open(path.join("resources")).expect("resource store should open"),
            ),
            cluster_api_clients: Arc::new(
                ClusterApiClients::build().expect("test cluster api clients should build"),
            ),
            http_tls_server_config: Arc::new(RwLock::new(None)),
            runtime: Arc::new(Runtime::new()),
            replica_count: 0,
            shutdown: CancellationToken::new(),
            events: broadcast::channel(16).0,
            subscription_interest_counts: Arc::new(DashMap::with_hasher(RandomState::new())),
            interconnect,
            domain_clocks: Arc::new(DashMap::with_hasher(RandomState::new())),
            domain_clock_events: Arc::new(Notify::new()),
            next_cluster_command_correlation_id: Arc::new(AtomicU64::new(1)),
            pending_cluster_commands: Arc::new(DashMap::default()),
            service_tasks: TaskTracker::new(),
            configured_basic_auth: None,
            auth_rate_limiter: SessionServiceImpl::new_auth_rate_limiter(),
            failed_auth_rate_limit_keys: Arc::new(DashMap::with_hasher(RandomState::new())),
        };
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions {
            subscriptions: HashMap::new(),
        };
        for command in [
            "CREATE SCHEMA notification ( user_id I64 );",
            "CREATE JSON WIRE SCHEMA notification_wire ( user_id integer );",
            "CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA \
             notification;",
            "CREATE RELAY notifications_a SCHEMA notification UNPARAMETERIZED;",
            "CREATE RELAY notifications_b SCHEMA notification UNPARAMETERIZED;",
            "CREATE RELAY notifications_all SCHEMA notification UNPARAMETERIZED;",
            "CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' \
             };",
            "CREATE INGESTOR ingest_a TO notifications_a DECODE USING notification_codec \
             UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB TIMESTAMP NOW FROM KAFKA \
             kafka_main TOPIC notifications_a OFFSET BY CONSUMER GROUP notifications_a_group MODE \
             NO_ACK PARALLEL MAX 1 ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            "CREATE INGESTOR ingest_b TO notifications_b DECODE USING notification_codec \
             UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB TIMESTAMP NOW FROM KAFKA \
             kafka_main TOPIC notifications_b OFFSET BY CONSUMER GROUP notifications_b_group MODE \
             NO_ACK PARALLEL MAX 1 ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            "CREATE UNIFIER join_streams FROM notifications_a, notifications_b TO \
             notifications_all UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE \
             ERROR LOG;",
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

        let unifier = registry
            .get(
                &Domain::parse("default").expect("valid domain"),
                ModelKind::Unifier,
                &Identifier::parse("join_streams").expect("valid identifier"),
            )
            .expect("registry get should succeed")
            .expect("unifier should exist");
        let Model::Unifier(unifier) = unifier else {
            panic!("stored model must be a unifier");
        };
        assert_eq!(unifier.from_relays.len(), 2);
        assert_eq!(
            unifier
                .output_routes
                .relays()
                .next()
                .expect("unifier should declare an output")
                .as_str(),
            "notifications_all"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_creates_deduplicator_model() {
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
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as u16;
        let grpc_addr = test_addr(61000u16.saturating_add(id));
        let cluster_listen_addr = test_addr(62000u16.saturating_add(id));
        let raft_addr = test_addr(63000u16.saturating_add(id));
        let interconnect_addr =
            cluster::derive_interconnect_addr(raft_addr).expect("must derive interconnect addr");
        let mut consensus = ConsensusHandle::from_database(
            db,
            ConsensusSettings {
                cluster_name: "test".to_string(),
                node_id: format!("test-node-{id}"),
                cluster_api_advertise_url: cluster_api_base_url(
                    InternalTransportMode::Http,
                    &cluster::HostPort::from(raft_addr),
                ),
                cluster_api_http_client: build_cluster_api_http_client(InternalTransportMode::Http)
                    .expect("test cluster api client should build"),
                node_unavailability_timeout: Duration::from_secs(10),
                raft_heartbeat_interval: Duration::from_millis(50),
                raft_election_timeout_min: Duration::from_millis(150),
                raft_election_timeout_max: Duration::from_millis(300),
            },
        )
        .await
        .expect("consensus should open");
        consensus.set_local_grpc_advertise_addr(grpc_addr.to_string());
        consensus
            .maybe_initialize()
            .await
            .expect("single-node consensus should initialize");
        create_test_domain(&consensus, "default").await;
        let expected_leader = format!("test-node-{id}");
        for _ in 0..50 {
            if consensus.current_leader().await.as_deref() == Some(expected_leader.as_str()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let consensus = Arc::new(consensus);
        let interconnect = test_interconnect(expected_leader.as_str()).await;
        let cluster = Arc::new(
            cluster::start_cluster(cluster::ClusterSettings {
                cluster_id: "test".to_string(),
                node_id: format!("test-node-{id}"),
                cluster_listen_addr,
                cluster_advertise_addr: cluster_listen_addr.into(),
                grpc_listen_addr: grpc_addr,
                grpc_advertise_addr: grpc_addr.to_string(),
                web_console_advertise_addr: format!("http://{}", grpc_addr),
                cluster_api_listen_addr: raft_addr,
                cluster_api_advertise_addr: cluster_api_base_url(
                    InternalTransportMode::Http,
                    &cluster::HostPort::from(raft_addr),
                ),
                interconnect_listen_addr: interconnect_addr,
                interconnect_advertise_addr: interconnect_addr.into(),
                interconnect_mode: "https".to_string(),
                interconnect_public_key: "00".repeat(32),
                bootstrap_host: None,
                node_unavailability_timeout: Duration::from_secs(10),
            })
            .await
            .expect("cluster should start"),
        );
        let service = SessionServiceImpl {
            cluster,
            consensus,
            registry: registry.clone(),
            resource_store: Arc::new(
                ResourceStore::open(path.join("resources")).expect("resource store should open"),
            ),
            cluster_api_clients: Arc::new(
                ClusterApiClients::build().expect("test cluster api clients should build"),
            ),
            http_tls_server_config: Arc::new(RwLock::new(None)),
            runtime: Arc::new(Runtime::new()),
            replica_count: 0,
            shutdown: CancellationToken::new(),
            events: broadcast::channel(16).0,
            subscription_interest_counts: Arc::new(DashMap::with_hasher(RandomState::new())),
            interconnect,
            domain_clocks: Arc::new(DashMap::with_hasher(RandomState::new())),
            domain_clock_events: Arc::new(Notify::new()),
            next_cluster_command_correlation_id: Arc::new(AtomicU64::new(1)),
            pending_cluster_commands: Arc::new(DashMap::default()),
            service_tasks: TaskTracker::new(),
            configured_basic_auth: None,
            auth_rate_limiter: SessionServiceImpl::new_auth_rate_limiter(),
            failed_auth_rate_limit_keys: Arc::new(DashMap::with_hasher(RandomState::new())),
        };
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions {
            subscriptions: HashMap::new(),
        };
        for command in [
            "CREATE SCHEMA transaction ( transaction_id STRING, amount I64 );",
            "CREATE JSON WIRE SCHEMA transaction_wire ( transaction_id string, amount integer );",
            "CREATE CODEC transaction_codec FROM WIRE JSON SCHEMA transaction_wire TO SCHEMA \
             transaction;",
            "CREATE RELAY inbound SCHEMA transaction UNPARAMETERIZED;",
            "CREATE RELAY deduped SCHEMA transaction UNPARAMETERIZED;",
            "CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' \
             };",
            "CREATE INGESTOR inbound_ingestor TO inbound DECODE USING transaction_codec \
             UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB TIMESTAMP NOW FROM KAFKA \
             kafka_main TOPIC inbound OFFSET BY CONSUMER GROUP inbound_group MODE NO_ACK PARALLEL \
             MAX 1 ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
            "CREATE DEDUPLICATOR dedup_txns FROM inbound TO deduped UNPARAMETERIZED DEDUPLICATE \
             ON inbound.transaction_id MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
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
            .get(
                &Domain::parse("default").expect("valid domain"),
                ModelKind::Deduplicator,
                &Identifier::parse("dedup_txns").expect("valid identifier"),
            )
            .expect("registry get should succeed")
            .expect("deduplicator should exist");
        let Model::Deduplicator(deduplicator) = deduplicator else {
            panic!("stored model must be a deduplicator");
        };
        assert_eq!(deduplicator.from_relay.as_str(), "inbound");
        assert_eq!(
            deduplicator
                .output_routes
                .relays()
                .next()
                .expect("deduplicator should declare an output")
                .as_str(),
            "deduped"
        );
        assert_eq!(deduplicator.deduplicate_on, "inbound.transaction_id");
        assert_eq!(deduplicator.max_time, "10m");
        assert_eq!(deduplicator.mode, nervix_models::AckMode::Attached);

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_describes_resource_metadata() {
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
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as u16;
        let expected_leader = format!("test-node-{id}");
        let grpc_addr = test_addr(61000u16.saturating_add(id));
        let cluster_listen_addr = test_addr(62000u16.saturating_add(id));
        let raft_addr = test_addr(63000u16.saturating_add(id));
        let interconnect_addr =
            cluster::derive_interconnect_addr(raft_addr).expect("must derive interconnect addr");
        let mut consensus = ConsensusHandle::from_database(
            db,
            ConsensusSettings {
                cluster_name: "test".to_string(),
                node_id: format!("test-node-{id}"),
                cluster_api_advertise_url: cluster_api_base_url(
                    InternalTransportMode::Http,
                    &cluster::HostPort::from(raft_addr),
                ),
                cluster_api_http_client: build_cluster_api_http_client(InternalTransportMode::Http)
                    .expect("test cluster api client should build"),
                node_unavailability_timeout: Duration::from_secs(10),
                raft_heartbeat_interval: Duration::from_millis(50),
                raft_election_timeout_min: Duration::from_millis(150),
                raft_election_timeout_max: Duration::from_millis(300),
            },
        )
        .await
        .expect("consensus should open");
        consensus.set_local_grpc_advertise_addr(grpc_addr.to_string());
        consensus
            .maybe_initialize()
            .await
            .expect("single-node consensus should initialize");
        let resource_store = Arc::new(
            ResourceStore::open(path.join("resources")).expect("resource store should open"),
        );
        let source_v1 = path.join("resource-source-v1");
        std::fs::create_dir_all(source_v1.join("nested"))
            .expect("test resource directory should exist");
        std::fs::write(source_v1.join("alpha.txt"), "alpha")
            .expect("test resource file should write");
        std::fs::write(source_v1.join("nested").join("beta.txt"), "beta")
            .expect("test resource file should write");
        let manifest_v1 = resource_store
            .install_from_directory(
                identifier("fraud_model"),
                1,
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
                identifier("fraud_model"),
                2,
                &source_v2,
                expected_leader.clone(),
                Timestamp::from_unix_nanos(79),
            )
            .await
            .expect("resource version should install");
        consensus
            .put_resource_version(manifest_v1.resource.clone())
            .await
            .expect("resource version should persist");
        consensus
            .put_resource_version(manifest_v2.resource.clone())
            .await
            .expect("resource version should persist");
        consensus
            .put_resource_replica(nervix_models::ResourceNodeStatus {
                key: nervix_models::ResourceReplicaKey::new(
                    identifier("fraud_model"),
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
        consensus
            .put_domain(DomainState {
                id: Domain::parse("default").expect("valid domain"),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    period: "0ms".to_string(),
                    skew: "0ms".to_string(),
                },
                status: DomainStatus::Stopped,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
            })
            .await
            .expect("domain should persist");
        for _ in 0..50 {
            if consensus.current_leader().await.as_deref() == Some(expected_leader.as_str()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let consensus = Arc::new(consensus);
        let interconnect = test_interconnect(expected_leader.as_str()).await;
        let cluster = Arc::new(
            cluster::start_cluster(cluster::ClusterSettings {
                cluster_id: "test".to_string(),
                node_id: format!("test-node-{id}"),
                cluster_listen_addr,
                cluster_advertise_addr: cluster_listen_addr.into(),
                grpc_listen_addr: grpc_addr,
                grpc_advertise_addr: grpc_addr.to_string(),
                web_console_advertise_addr: format!("http://{}", grpc_addr),
                cluster_api_listen_addr: raft_addr,
                cluster_api_advertise_addr: cluster_api_base_url(
                    InternalTransportMode::Http,
                    &cluster::HostPort::from(raft_addr),
                ),
                interconnect_listen_addr: interconnect_addr,
                interconnect_advertise_addr: interconnect_addr.into(),
                interconnect_mode: "https".to_string(),
                interconnect_public_key: "00".repeat(32),
                bootstrap_host: None,
                node_unavailability_timeout: Duration::from_secs(10),
            })
            .await
            .expect("cluster should start"),
        );
        let service = SessionServiceImpl {
            cluster,
            consensus,
            registry,
            resource_store,
            cluster_api_clients: Arc::new(
                ClusterApiClients::build().expect("test cluster api clients should build"),
            ),
            http_tls_server_config: Arc::new(RwLock::new(None)),
            runtime: Arc::new(Runtime::new()),
            replica_count: 0,
            shutdown: CancellationToken::new(),
            events: broadcast::channel(16).0,
            subscription_interest_counts: Arc::new(DashMap::with_hasher(RandomState::new())),
            interconnect,
            domain_clocks: Arc::new(DashMap::with_hasher(RandomState::new())),
            domain_clock_events: Arc::new(Notify::new()),
            next_cluster_command_correlation_id: Arc::new(AtomicU64::new(1)),
            pending_cluster_commands: Arc::new(DashMap::default()),
            service_tasks: TaskTracker::new(),
            configured_basic_auth: None,
            auth_rate_limiter: SessionServiceImpl::new_auth_rate_limiter(),
            failed_auth_rate_limit_keys: Arc::new(DashMap::with_hasher(RandomState::new())),
        };
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions {
            subscriptions: HashMap::new(),
        };

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
    fn parse_server_statement_accepts_unifier_from_application_crate() {
        let parsed = nervix_nspl::server_statement::parse_server_statement(
            "CREATE UNIFIER join_streams FROM ss1, ss2 TO ss10 UNPARAMETERIZED FLUSH EACH 100ms \
             MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        )
        .expect("unifier statement should parse");
        let Statement::Create(model) = parsed else {
            panic!("expected create statement");
        };
        assert!(matches!(model.body.as_ref(), Model::Unifier(_)));
    }

    #[test]
    fn parse_server_statement_accepts_deduplicator_from_application_crate() {
        let parsed = nervix_nspl::server_statement::parse_server_statement(
            "CREATE DEDUPLICATOR dedup_txns FROM ss1 TO ss2 UNPARAMETERIZED DEDUPLICATE ON \
             ss1.transaction_id MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE \
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

    #[test]
    fn internal_cluster_api_tls_configs_load() {
        ensure_dev_tls_assets();
        let _client = build_cluster_api_http_client(InternalTransportMode::Https)
            .expect("https cluster api client should build");
        let _server = load_cluster_api_tls_server_config()
            .expect("https cluster api server config should build");
    }
}
