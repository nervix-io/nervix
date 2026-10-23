//! The Models and the vocabulary every other layer speaks.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** Every NSPL Model, validated name and HTTP request-field types, `Timestamp`, branch
//!   and node references, the index that keys a domain's Models by the node each one configures,
//!   structured message errors, and the canonical NSPL rendering of a Model.
//! - **Depends on.** Serialization and primitive crates, and Arrow's schema types behind the
//!   `arrow` feature for the one Arrow data type each schema type is represented as.
//! - **Must not know.** How a Model was parsed, validated, scheduled or executed. No parser span,
//!   no registry state, no Arrow array and no Tokio type belongs here.
//!
//! This crate breaks its own contract. It also carries replicated control-plane state —
//! `ClusterSchedule`, `DomainState`, `ResourceVersionStatus` and their neighbours — which belongs to
//! consensus, and the interconnect's wire values in `remote`, which belong to the transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

mod archived_count;
#[cfg(feature = "arrow")]
mod arrow_types;
mod backup;
mod canonical;
mod client_consumer;
mod client_producer;
mod cluster_node;
mod command;
mod completion;
mod connector;
mod domain_clock;
mod duration_text;
mod emitter;
mod emitter_batch;
mod expression;
mod http_request;
mod ingestor_input;
mod json_path;
mod message_error;
mod model_index;
mod name_spelling;
mod names;
mod node_endpoint;
mod node_ref;
mod processor;
mod quiesce;
mod rebind_resource;
mod remote;
mod reset_wasm_state;
mod resource;
mod resource_binding;
mod restore;
mod schedule;
mod schema;
mod schema_fingerprint;
mod settings;
mod statement;
mod timestamp;
mod udf;
mod wasm_state_generation;
mod wasm_state_inspection;
mod window_model_digest;

pub use archived_count::{ArchivedCountError, CountAsU64};
pub use backup::{
    ArchiveDigest, Backup, BackupArchiveSummary, BackupCapture, BackupCut, BackupCutKind,
    BackupDomainSummary, BackupQuiesceCounters, BackupResources, BackupScope, DescribeBackup,
};
pub use canonical::{
    CanonicalNsplError, NSPL_LANGUAGE_VERSION, alter_avro_wire_schema_to_canonical_nspl,
    alter_cbor_wire_schema_to_canonical_nspl, alter_json_wire_schema_to_canonical_nspl,
    canonical_nspl_document, expression_to_nspl, ingest_quiesce_to_nspl,
    ingestor_input_body_to_nspl, string_literal,
};
pub use client_consumer::{
    CLIENT_CONSUMER_NODE_BYTES, CLIENT_CONSUMER_SESSION_BYTES, ClientConsumerLimits,
    MAX_CLIENT_CONSUMER_BATCHES, MAX_CLIENT_CONSUMERS_PER_SESSION,
};
pub use client_producer::{
    CLIENT_PRODUCER_NODE_BYTES, CLIENT_PRODUCER_SESSION_BYTES, ClientAttachmentId,
    ClientBatchDefect, ClientEndpointContract, ClientOutcomeUncertainty, ClientProcessingFailure,
    ClientProducerAdmission, ClientProducerDescription, ClientProducerEndReason,
    ClientProducerGrant, ClientProducerLimits, ClientProducerPolicy, ClientProducerRefusal,
    ClientSubmissionOutcome, ClientSubmissionRefusal, MAX_CLIENT_BATCH_ROWS,
    MAX_CLIENT_PRODUCER_BATCHES, MAX_CLIENT_PRODUCERS_PER_SESSION,
};
pub use cluster_node::{ClusterNodeIdentity, ClusterNodeIncarnation, CoordinationIdentity};
pub use command::{
    CommandExecutionReference, CommandExecutionReferenceError,
    CommandExecutionReferenceTimestampError,
};
pub use completion::{BuiltinFunctionScope, SemanticReference};
pub use connector::{
    AcknowledgementConfirmation, AzureBlobConfigEntry, BufferBound, BufferCapacity, BufferOverflow,
    ChoiceRequirement, ClickHouseConfigEntry, ClickHouseEmitterMode, ClickHouseSink,
    ClickHouseSinkEntity, ClickHouseTableName, ClickHouseTablePublishingMode, ClickHouseTableSink,
    ClickHouseValueMapping, ClientConfigEntry, ClientConfigOwner, ClientConsumerPublishingMode,
    ClientConsumerSink, ClientDeadline, ClientDeadlineError, ClientDeclaration, ClientEmitterMode,
    ClientIngestMode, ClientIngestorAlteration, ClientPoolBounds, ClientPoolBoundsError,
    ClientPooling, ClientProducerSource, ClientResourceMount, ClientSink, ClientSinkEntity,
    ClientSourceEntity, ConnectorKind, CreateClientAzureBlob, CreateClientClickHouse,
    CreateClientGcs, CreateClientHttp, CreateClientIcebergRest, CreateClientKafka,
    CreateClientMongoDb, CreateClientMqtt, CreateClientMySql, CreateClientNats, CreateClientOtel,
    CreateClientPostgres, CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq,
    CreateClientRedis, CreateClientS3, CreateClientSentry, CreateClientSqs, CreateClientSyslog,
    CreateClientWebsockets, CreateClientZeroMq, DEFAULT_CONNECT_TIMEOUT, DeadlineKey,
    DeclaredBounds, DeclaredBuffer, DeclaredDelivery, DeclaredQuiesce, DeclaredValueMapping,
    DeclaredWait, EmitSink, EmitSinkCapabilities, EmitSinkKind, EmitterBodyForm,
    EmitterEntityModel, EmitterModeChange, EmitterPublication, EndpointIngestMode,
    EnvelopeClauseKind, ExternalName, ExternalNameDeclaration, ExternalNameError, GcsConfigEntry,
    HeaderLayout, HeaderModel, HeaderNames, HttpConfigEntry, HttpEmitterMode, HttpEndpointSource,
    HttpIngestorAlteration, HttpRequestIngestMode, HttpRequestPublishingMode, HttpRequestSink,
    HttpRequestSource, HttpSink, HttpSinkEntity, HttpSource, HttpSourceEntity, IcebergCatalog,
    IcebergEmitterMode, IcebergRestConfigEntry, IcebergSink, IcebergSinkEntity,
    IcebergStorageBackend, IcebergTableName, IcebergTablePublishingMode, IcebergTableSink,
    IcebergValueMapping, IngestAcknowledgement, IngestDeliveryKind, IngestQuiesceKind,
    IngestQuiesceMode, IngestQuiesceOverflow, IngestSource, IngestSourceCapabilities,
    IngestSourceKind, IngestorAlteration, IngestorEntityModel, KafkaConfigEntry,
    KafkaConsumerGroupName, KafkaEmitterMode, KafkaIngestMode, KafkaIngestorAlteration,
    KafkaOffsetMode, KafkaSink, KafkaSinkEntity, KafkaSource, KafkaSourceEntity, KafkaTopicName,
    KafkaTopicPublishingMode, KafkaTopicSink, KafkaTopicSource, LiteralHeaderWrite,
    LocatorDeclaration, LocatorName, MQTT_SOURCE_MAXIMUM_PACKET_BYTES, MappedValueField,
    MappedValueSection, MappedValueType, MappedValueTypes, MongoDbCollectionName,
    MongoDbCollectionPublishingMode, MongoDbCollectionSink, MongoDbConfigEntry,
    MongoDbConflictAction, MongoDbEmitterMode, MongoDbSink, MongoDbSinkEntity, MongoDbValueMapping,
    MqttConfigEntry, MqttEmitterMode, MqttIngestMode, MqttIngestorAlteration, MqttQos, MqttSession,
    MqttSink, MqttSinkEntity, MqttSource, MqttSourceEntity, MqttTopicFilter, MqttTopicName,
    MqttTopicPublishingMode, MqttTopicSink, MqttTopicSource, MySqlConfigEntry, MySqlConflictAction,
    MySqlEmitterMode, MySqlSink, MySqlSinkEntity, MySqlTableName, MySqlTablePublishingMode,
    MySqlTableSink, MySqlValueMapping, NatsConfigEntry, NatsEmitterMode, NatsIngestMode,
    NatsIngestorAlteration, NatsPublishSubjectName, NatsQueueGroupName, NatsSink, NatsSinkEntity,
    NatsSource, NatsSourceEntity, NatsSubjectName, NatsSubjectPublishingMode, NatsSubjectSink,
    NatsSubjectSource, OTEL_HISTOGRAM_VALUE_TYPES, OTEL_LOG_VALUE_TYPES, OTEL_NUMBER_VALUE_TYPES,
    OTEL_TRACE_VALUE_TYPES, OmittedDeadline, OptionChoice, OptionDeclaration, OptionForm,
    OptionPresence, OptionValue, OtelAggregationTemporality, OtelConfigEntry, OtelEmitterMode,
    OtelLogsPublishingMode, OtelLogsSink, OtelMappings, OtelMetric, OtelMetricKind,
    OtelMetricsPublishingMode, OtelMetricsSink, OtelScope, OtelSink, OtelSinkEntity,
    OtelTracesPublishingMode, OtelTracesSink, OtelValueMapping, PROMETHEUS_DEFAULT_MAX_BODY_SIZE,
    PositionClass, PostgresConfigEntry, PostgresConflictAction, PostgresEmitterMode, PostgresSink,
    PostgresSinkEntity, PostgresTableName, PostgresTablePublishingMode, PostgresTableSink,
    PostgresValueMapping, PrometheusApiRoot, PrometheusApiRootError, PrometheusClientConfigError,
    PrometheusClientKey, PrometheusConfigEntry, PrometheusIngestMode, PrometheusIngestorAlteration,
    PrometheusQuerySource, PrometheusSource, PrometheusSourceEntity, PropertyDeclaration,
    PulsarConfigEntry, PulsarEmitterMode, PulsarIngestMode, PulsarIngestorAlteration, PulsarSink,
    PulsarSinkEntity, PulsarSource, PulsarSourceEntity, PulsarSubscription, PulsarSubscriptionName,
    PulsarSubscriptionOwnership, PulsarTopicName, PulsarTopicPublishingMode, PulsarTopicSink,
    PulsarTopicSource, RabbitMqConfigEntry, RabbitMqEmitterMode, RabbitMqIngestMode,
    RabbitMqIngestorAlteration, RabbitMqQueueName, RabbitMqQueuePublishingMode, RabbitMqQueueSink,
    RabbitMqQueueSource, RabbitMqSink, RabbitMqSinkEntity, RabbitMqSource, RabbitMqSourceEntity,
    RedisChannelName, RedisConfigEntry, RedisEmitterMode, RedisIngestorAlteration,
    RedisPubSubIngestMode, RedisPubSubPublishingMode, RedisPubSubSink, RedisPubSubSource,
    RedisSink, RedisSinkEntity, RedisSource, RedisSourceEntity, ReplacedHeaderWrite,
    RuntimeCapability, S3ConfigEntry, SentryConfigEntry, SentryEmitterMode,
    SentryEventPublishingMode, SentryEventSink, SentrySink, SentrySinkEntity, SinkBody,
    SinkClientReference, SinkEntity, SinkEntityKind, SinkReference, SinkReferenceKind,
    SinkValueMapping, SinkValueMappings, SourceBody, SourceEntity, SourceEntityKind,
    SourceMetadataScope, SourceReference, SourceReferenceKind, SqsConfigEntry, SqsEmitterMode,
    SqsFifoGroup, SqsIngestMode, SqsIngestorAlteration, SqsQueueName, SqsQueuePublishingMode,
    SqsQueueSink, SqsQueueSource, SqsSink, SqsSinkEntity, SqsSource, SqsSourceEntity,
    SyslogCollectorPublishingMode, SyslogCollectorSink, SyslogConfigEntry, SyslogEmitterMode,
    SyslogIngestMode, SyslogIngestorAlteration, SyslogListenerSource, SyslogSink, SyslogSinkEntity,
    SyslogSource, SyslogSourceEntity, WaitBound, WebsocketsConfigEntry, WebsocketsConnectionSource,
    WebsocketsEndpointSource, WebsocketsIngestMode, WebsocketsIngestorAlteration, WebsocketsSource,
    WebsocketsSourceEntity, ZeroMqConfigEntry, ZeroMqEmitterMode, ZeroMqIngestMode,
    ZeroMqIngestorAlteration, ZeroMqPullSource, ZeroMqPushPublishingMode, ZeroMqPushSink,
    ZeroMqSink, ZeroMqSinkEntity, ZeroMqSource, ZeroMqSourceEntity,
};
pub use domain_clock::{
    DomainAdmissionWindow, DomainClockAdvancement, DomainClockAuthority,
    DomainClockAuthorityRevision, DomainClockBoundary, DomainClockError, DomainClockObservation,
    DomainClockObservedState, DomainClockPeriod, DomainClockProgress, DomainClockSkew,
    DomainClockState, DomainClockTickObservation, DomainTimeRate, PacedDomainClock,
};
pub use duration_text::{DurationTextError, parse_duration_text};
pub use emitter::{
    AckMode, AlterEmitter, AlterEmitterError, AlterEmitterOperation, CreateEmitter,
    EmitterBatchContractError, EmitterBody, EmitterPublishing, EmitterPublishingModeKind,
};
pub use emitter_batch::{
    BatchMessageLimit, BatchUnavailability, ByteSizeUnit, EmitterBatchLegality,
    EmitterBatchLimitError, EmitterBatchPolicy, PayloadSizeError, PayloadSizeLimit,
};
pub use expression::{
    Assignment, AssignmentTarget, AssignmentTargetScope, BinaryOperator, CaseBranch, Expression,
    ExternalValue, FieldReference, FieldScope, Float64Literal, Inheritance, InheritedField,
    Invocation, Literal, MaterializedStateDependency, MaterializedStatePolicy, MembershipOperator,
    OutputBranch, RangeOperator, RouteConstruction, UnaryOperator,
};
pub use http_request::{
    HttpApplicationHeaders, HttpBodyMode, HttpHeaderName, HttpHeaderValue, HttpMethod, HttpOrigin,
    HttpRequestFieldError, HttpTarget,
};
pub use ingestor_input::{IngestorInput, TransportIngestorInput};
pub use json_path::{JsonPath, JsonPathError, JsonPathStep};
pub use message_error::{
    FieldPath, MessageErrorCode, MessageErrorOperation, StructuredMessageError,
};
pub use model_index::ModelIndex;
pub use name_spelling::NamePosition;
pub use names::{
    BranchName, BuiltinFunctionName, ClientName, ClusterNodeName, CodecName, CorrelatorName,
    DeduplicatorName, DomainName, DotPolicy, EmitterName, EndpointName, FieldName, GeneratorName,
    InferencerName, IngestorName, JunctionName, LookupName, ModelName, NameError, PlacementName,
    ReingestorName, RelayName, ReordererName, ResourceName, SchemaName, SignalingProtocolName,
    SubscriptionName, UdfName, UserName, VhostName, WasmProcessorName, WindowProcessorName,
    WireSchemaName,
};
pub use node_endpoint::{
    NodeEndpoint, NodeEndpointParseError, NodeServiceUrl, NodeServiceUrlParseError,
};
pub use node_ref::{DomainNodeRef, NodeRef};
pub use processor::{
    AlterDeduplicator, AlterDeduplicatorError, AlterDeduplicatorOperation, AlterJunction,
    AlterJunctionError, AlterProcessorError, AlterProcessorOperation, AlterReingestor,
    AlterReingestorError, AlterReorderer, AlterReordererError, AlterReordererOperation,
    CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy, CreateCorrelator,
    CreateDeduplicator, CreateInferencer, CreateJunction, CreateReingestor, CreateReorderer,
    CreateWasmProcessor, CreateWindowProcessor, FlushPolicy, InferencerExecutionMode,
    InferencerTensorDeclaration, InferencerTensorDimension, InferencerTensorElementType,
    InferencerTensorMapping, InferencerTensorRepresentation, InferencerTensorSchema,
    InferencerTensorSchemaError, InputCollectPolicy, ProcessorInputWhere, ProcessorInputs,
    ProcessorOutput, ProcessorOutputs, WasmProcessorLimits, WasmRejectedStatePolicy, WindowBound,
    WindowStateLimit,
};
pub use quiesce::{
    ActivationAction, ActivationImpact, ActualExecutionStepImpact, ActualQuiescence,
    AffectedTopology, AttributedGateBoundary, AttributedImpactNode, BranchKeyFingerprint,
    CanonicalImpactSet, ConcreteBranchCoverage, ConfigurationImpact, ConfigurationTransition,
    DomainLifecycleAction, DomainLifecycleImpact, DynamicModelUpdate, ExecutionStepImpactReport,
    ExecutionStepOutcome, ForceFlushImpact, ImpactAttribution, ImpactDiagnostic,
    ImpactDiagnosticKind, ImpactEdgeKind, ImpactEffects, ImpactGateBoundary, ImpactNodeCoverage,
    ImpactPlanningBasis, ImpactReportCompleteness, ImpactReportError, ImpactTopology,
    ImpactTopologyEdge, ModelChangeAspect, ModelChangeAspects, OperationImpactReason,
    OperationImpactReport, OwnershipMoveImpact, PauseRequirement, PlannedExecutionStepImpact,
    QuiesceLevel, QuiesceSubgraph, QuiescenceOutcome, RebuildImpact, RebuildReason,
    ResourceBindingImpact, ResourceCatalogAction, ResourceCatalogImpact, StatePurge,
    StateResetImpact, TransactionCommitPlan, TransactionCommitPlanHeader,
    TransactionCommitPlanStep, TransactionCommitStepKind, TransactionEntityGatePlan,
    TransactionImpactReport, TransactionImpactSummary, TransactionInspection,
    TransactionInspectionRejection, TransactionInspectionRequest, TransactionInspectionTarget,
    TransactionLifecycle, TransactionModelTransition, TransactionOperation,
    TransactionOperationAdmission, TransactionOperationNumber, TransactionOperationRange,
    TransactionPosition, TransactionPreviewIdentity, TransactionResolvedDomainStart,
    TransactionStatus, TransactionStatusError,
};
pub use rebind_resource::{RebindResource, RebindResourceMembers, RebindResourceSelection};
pub use remote::{
    RemoteAckOutcome, RemoteAckRegistration, RemoteAckResolution, RemoteRuntimeElementValue,
    RemoteRuntimeField, RemoteRuntimeRecordMetadata, RemoteRuntimeValue,
};
pub use reset_wasm_state::{
    ResetWasmBranchField, ResetWasmState, ResetWasmStateScope, ResetWasmStateSelectionError,
    ResolvedResetWasmStateScope,
};
pub use resource::{
    RequestedResourceVersion, ResourceDescription, ResourceEntryContent, ResourceId,
    ResourceManifestEntry, ResourceNodeState, ResourceNodeStatus, ResourceReplicaKey,
    ResourceUpload, ResourceUploadIdentity, ResourceUploadIdentityError, ResourceUploadKey,
    ResourceUploadState, ResourceUploads, ResourceUploadsError, ResourceUsage, ResourceVersion,
    ResourceVersionCounter, ResourceVersionDescription, ResourceVersionEntries, ResourceVersionKey,
    ResourceVersionResolutionError, ResourceVersionStatus,
};
pub use resource_binding::ResourceRebinding;
/// Restore policy, fenced state installation and lifecycle reporting.
pub use restore::{
    ExistingUserPolicy, Restore, RestoreArchive, RestoreLifecycle, RestoreMode, RestoreReport,
    RestoreScope, RestoreState, RestoreStateAuthority, RestoreStep, RestoreStepOutcome,
    RestoreStepReport, RestoredDomain, RestoredUsers,
};
pub use schedule::{
    ClusterSchedule, DomainSchedule, KafkaPartitionSchedule, OwnershipStateComponent,
    OwnershipStateRecoveryOutcome, OwnershipStateReset, OwnershipStateResetCause,
    OwnershipTransition, PlacementGroupSchedule, ScheduledModel, ScheduledNode, ScheduledNodes,
};
pub use schema::{
    AlterSchema, AlterSchemaError, AlterSchemaOperation, AlterWireSchema, AlterWireSchemaError,
    AlterWireSchemaOperation, AvroType, CborType, CreateAvroWireSchema, CreateCborWireSchema,
    CreateJsonWireSchema, CreateSchema, CreateWireSchema, JsonType, ParseAsType, SchemaField,
    WireSchemaField, WireSchemaStrictness,
};
pub use schema_fingerprint::SchemaFingerprint;
pub use settings::{
    ClusterParameters, NodeCapabilities, Prerequisites, Setting, SettingClass, SettingDefinition,
};
pub use statement::{
    AckWindow, AlterDomain, AlterGenerator, AlterGeneratorError, AlterGeneratorOperation,
    AlterIngestor, AlterIngestorError, AlterIngestorOperation, AlterPlacement, AlterPlacementError,
    AlterPlacementOperation, AlterRelay, AlterRelayError, AlterRelayOperation,
    AlterTransportIngestorOperation, BranchEviction, BranchSelection, CodecBatchContainer,
    CodecEncoding, CodecEncodingRule, CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig,
    CodecWireFormat, CordonNode, CreateBranch, CreateCodec, CreateDomain, CreateEndpoint,
    CreateGenerator, CreateIngestor, CreateLookup, CreatePlacement, CreateRelay, CreateResource,
    CreateSignalingProtocol, CreateStatement, CreateSubscription, CreateUser, CreateVhost,
    DeleteSubscription, DescribeCorrelator, DescribeDeduplicator, DescribeDomain, DescribeEmitter,
    DescribeEndpoint, DescribeIngestor, DescribeJunction, DescribeLookup, DescribePlacement,
    DescribeReingestor, DescribeRelay, DescribeReorderer, DescribeResource, DescribeTransaction,
    DescribeUdf, DescribeWasmProcessor, DescribeWindowProcessor, DomainConfig, DomainPace,
    DomainStartPoint, DomainState, DomainStatus, DomainTick, DrainNode, DropModel, DropNode,
    EndpointType, ErrorPolicies, GeneralErrorPolicy, IngestTimestampSource, InspectionFormat,
    LookupQuery, MaterializedRelayState, MessageErrorPolicy, Model, ModelKind, PlacementPolicy,
    RelayBranching, Relocation, RelocationMember, RelocationPreferenceOverride,
    RelocationPreferenceStrategy, RelocationSelection, ResolvedBranching, ResolvedCodecWireFormat,
    RetryPolicy, ShowClusterStatus, ShowCreate, ShowIngestors, ShowPlacements,
    ShowRelayMaterializedState, ShowTransactions, ShowUdfs, SignalingProtobufConfig,
    SignalingProtocolOnConnect, SignalingStep, SignalingWaitStep, SignalingWireFormat, StartDomain,
    Statement, StopDomain, SubscriptionBinding, SubscriptionDeliveryBehavior, SubscriptionLiteral,
    UncordonNode, UniquelyKindedModel, UploadResource, VhostTlsResource, WireSchemaLookup,
    default_relay_buffer,
};
pub use timestamp::{AtomicTimestamp, Timestamp, TimestampError};
pub use udf::{CreateUdf, UdfArgument, UdfLanguage, UdfReturn};
pub use wasm_state_generation::{
    InvalidWasmStateGeneration, WasmSavedStateRejection, WasmStateGeneration, WasmStateGenerations,
    WasmStateRecoveries, WasmStateRecovery, WasmStateRecoveryAdmission, WasmStateRecoveryOutcome,
    WasmStateReset, WasmStateResetPhase, WasmStateResetReason, WasmStateResetScope,
};
pub use wasm_state_inspection::{
    WasmCheckpointCounts, WasmCheckpointInspection, WasmCheckpointStage, WasmRecoveryInspection,
    WasmStateInspection, WasmStateResetInspection, WasmStateResetReadiness,
};
pub use window_model_digest::WindowModelDigest;
