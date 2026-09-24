//! The Models and the vocabulary every other layer speaks.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** Every NSPL Model, the validated name types, `Timestamp`, branch and node references,
//!   the index that keys a domain's Models by the node each one configures, structured message
//!   errors, and the canonical NSPL rendering of a Model.
//! - **Depends on.** Serialization and primitive crates.
//! - **Must not know.** How a Model was parsed, validated, scheduled or executed. No parser span,
//!   no registry state, no Arrow array and no Tokio type belongs here.
//!
//! This crate breaks its own contract. It also carries replicated control-plane state —
//! `ClusterSchedule`, `DomainState`, `ResourceVersionStatus` and their neighbours — which belongs to
//! consensus, and the interconnect's wire values in `remote`, which belong to the transport.
//! `RemoteRuntimeRecord` is row-oriented besides, which the columnar rule forbids of a payload.

mod canonical;
mod cluster_node;
mod command;
mod domain_clock;
mod expression;
mod json_path;
mod message_error;
mod model_index;
mod names;
mod node_endpoint;
mod node_ref;
mod quiesce;
mod rebind_resource;
mod remote;
mod reset_wasm_state;
mod resource;
mod resource_binding;
mod schema;
mod schema_fingerprint;
mod statement;
mod timestamp;
mod udf;
mod wasm_state_generation;

pub use canonical::{
    CanonicalNsplError, alter_avro_wire_schema_to_canonical_nspl,
    alter_cbor_wire_schema_to_canonical_nspl, alter_json_wire_schema_to_canonical_nspl,
    expression_to_nspl, ingest_quiesce_to_nspl,
};
pub use cluster_node::{ClusterNodeIdentity, ClusterNodeIncarnation, CoordinationIdentity};
pub use command::{
    CommandExecutionReference, CommandExecutionReferenceError,
    CommandExecutionReferenceTimestampError,
};
pub use domain_clock::{
    DomainAdmissionWindow, DomainClockAdvancement, DomainClockAuthority,
    DomainClockAuthorityRevision, DomainClockBoundary, DomainClockError, DomainClockPeriod,
    DomainClockProgress, DomainClockSkew, DomainClockState, DomainTimeRate,
};
pub use expression::{
    Assignment, AssignmentTarget, AssignmentTargetScope, BinaryOperator, CaseBranch, Expression,
    ExternalValue, FieldReference, FieldScope, Float64Literal, Inheritance, InheritedField,
    Invocation, Literal, MaterializedStateDependency, MaterializedStatePolicy, MembershipOperator,
    OutputBranch, RangeOperator, RouteConstruction, UnaryOperator,
};
pub use json_path::{JsonPath, JsonPathError, JsonPathStep};
pub use message_error::{
    FieldPath, MessageErrorCode, MessageErrorOperation, StructuredMessageError,
};
pub use model_index::ModelIndex;
pub use names::{
    BranchName, BuiltinFunctionName, ChannelName, ClientName, ClusterNodeName, CodecName,
    CollectionName, ConsumerGroupName, CorrelatorName, DeduplicatorName, DomainName, DotPolicy,
    EmitterName, EndpointName, FieldName, GeneratorName, InferencerName, IngestorName,
    JunctionName, LookupName, ModelName, NameError, PlacementName, PulsarSubscriptionName,
    QueueGroupName, QueueName, ReingestorName, RelayName, ReordererName, ResourceName, SchemaName,
    SignalingProtocolName, SubjectName, SubscriptionName, TableName, TopicName, UdfName, UserName,
    VhostName, WasmProcessorName, WindowProcessorName, WireSchemaName,
};
pub use node_endpoint::{
    NodeEndpoint, NodeEndpointParseError, NodeServiceUrl, NodeServiceUrlParseError,
};
pub use node_ref::{DomainNodeRef, NodeRef};
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
    RemoteRuntimeField, RemoteRuntimeRecord, RemoteRuntimeRecordMetadata, RemoteRuntimeValue,
};
pub use reset_wasm_state::{
    ResetWasmBranchField, ResetWasmState, ResetWasmStateScope, ResetWasmStateSelectionError,
    ResolvedResetWasmStateScope,
};
pub use resource::{
    RequestedResourceVersion, ResourceId, ResourceNodeState, ResourceNodeStatus,
    ResourceReplicaKey, ResourceUpload, ResourceUploadIdentity, ResourceUploadIdentityError,
    ResourceUploadKey, ResourceUploadState, ResourceUploads, ResourceUploadsError, ResourceVersion,
    ResourceVersionCounter, ResourceVersionKey, ResourceVersionResolutionError,
    ResourceVersionStatus,
};
pub use resource_binding::ResourceRebinding;
pub use schema::{
    AlterSchema, AlterSchemaError, AlterSchemaOperation, AlterWireSchema, AlterWireSchemaOperation,
    AvroType, CborType, CreateAvroWireSchema, CreateCborWireSchema, CreateJsonWireSchema,
    CreateSchema, CreateWireSchema, JsonType, ParseAsType, SchemaField, WireSchemaField,
    WireSchemaStrictness,
};
pub use schema_fingerprint::SchemaFingerprint;
pub use statement::{
    AckMode, AlterDeduplicator, AlterDeduplicatorError, AlterDeduplicatorOperation, AlterDomain,
    AlterEmitter, AlterEmitterError, AlterEmitterOperation, AlterGenerator, AlterGeneratorError,
    AlterGeneratorOperation, AlterIngestor, AlterIngestorError, AlterIngestorOperation,
    AlterJunction, AlterJunctionError, AlterPlacement, AlterPlacementError,
    AlterPlacementOperation, AlterProcessorError, AlterProcessorOperation, AlterReingestor,
    AlterReingestorError, AlterRelay, AlterRelayError, AlterRelayOperation, AlterReorderer,
    AlterReordererError, AlterReordererOperation, AzureBlobConfigEntry, BranchEviction,
    BranchSelection, ClickHouseConfigEntry, ClickHouseValueMapping, ClientConfigEntry,
    ClientPoolBounds, ClientPoolBoundsError, ClientResourceMount, ClusterSchedule, CodecEncoding,
    CodecEncodingRule, CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig,
    CodecWireFormat, CordonNode, CorrelationTimeoutAction, CorrelationTimeoutPolicy,
    CorrelatorMatchPolicy, CreateBranch, CreateClientAzureBlob, CreateClientClickHouse,
    CreateClientGcs, CreateClientHttp, CreateClientIcebergRest, CreateClientKafka,
    CreateClientMongoDb, CreateClientMqtt, CreateClientMySql, CreateClientNats, CreateClientOtel,
    CreateClientPostgres, CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq,
    CreateClientRedis, CreateClientS3, CreateClientSentry, CreateClientSqs, CreateClientSyslog,
    CreateClientWebsockets, CreateClientZeroMq, CreateCodec, CreateCorrelator, CreateDeduplicator,
    CreateDomain, CreateEmitter, CreateEndpoint, CreateGenerator, CreateInferencer, CreateIngestor,
    CreateJunction, CreateLookup, CreatePlacement, CreateReingestor, CreateRelay, CreateReorderer,
    CreateResource, CreateSignalingProtocol, CreateStatement, CreateSubscription, CreateUser,
    CreateVhost, CreateWasmProcessor, CreateWindowProcessor, DeleteSubscription,
    DescribeCorrelator, DescribeDeduplicator, DescribeDomain, DescribeEmitter, DescribeEndpoint,
    DescribeIngestor, DescribeJunction, DescribeLookup, DescribePlacement, DescribeReingestor,
    DescribeRelay, DescribeReorderer, DescribeResource, DescribeTransaction, DescribeUdf,
    DescribeWasmProcessor, DescribeWindowProcessor, DomainConfig, DomainPace, DomainSchedule,
    DomainStartPoint, DomainState, DomainStatus, DomainTick, DrainNode, DropModel, DropNode,
    EmitSink, EmitterAckWindow, EmitterPublishingMode, EndpointIngestMode, EndpointType,
    ErrorPolicies, FlushPolicy, GcsConfigEntry, GeneralErrorPolicy, HttpConfigEntry,
    IcebergCatalog, IcebergRestConfigEntry, IcebergStorageBackend, IcebergValueMapping,
    InferencerExecutionMode, InferencerTensorDeclaration, InferencerTensorDimension,
    InferencerTensorElementType, InferencerTensorMapping, InferencerTensorRepresentation,
    InferencerTensorSchema, InferencerTensorSchemaError, IngestAcknowledgement, IngestQuiesceMode,
    IngestQuiesceOverflow, IngestSource, IngestTimestampSource, InputCollectPolicy,
    KafkaConfigEntry, KafkaIngestMode, KafkaOffsetMode, KafkaPartitionSchedule, LookupQuery,
    MaterializedRelayState, MessageErrorPolicy, Model, ModelKind, MongoDbConfigEntry,
    MongoDbConflictAction, MongoDbValueMapping, MqttConfigEntry, MqttIngestMode, MqttQos,
    MqttSession, MySqlConfigEntry, MySqlConflictAction, MySqlValueMapping, NatsConfigEntry,
    NatsIngestMode, OtelAggregationTemporality, OtelConfigEntry, OtelMetric, OtelMetricKind,
    OtelScope, OtelSignal, OtelValueMapping, OwnershipStateComponent,
    OwnershipStateRecoveryOutcome, OwnershipStateReset, OwnershipStateResetCause,
    OwnershipTransition, PlacementGroupSchedule, PlacementPolicy, PostgresConfigEntry,
    PostgresConflictAction, PostgresValueMapping, ProcessorInputWhere, ProcessorInputs,
    ProcessorOutput, ProcessorOutputs, PrometheusConfigEntry, PulsarConfigEntry, PulsarIngestMode,
    RabbitMqConfigEntry, RabbitMqIngestMode, RedisConfigEntry, RedisPubSubIngestMode,
    RelayBranching, Relocation, RelocationMember, RelocationPreferenceOverride,
    RelocationPreferenceStrategy, RelocationSelection, ResolvedBranching, ResolvedCodecWireFormat,
    RetryPolicy, S3ConfigEntry, ScheduledModel, ScheduledNode, ScheduledNodes, SentryConfigEntry,
    ShowClusterStatus, ShowCreate, ShowPlacements, ShowRelayMaterializedState, ShowTransactions,
    ShowUdfs, SignalingProtobufConfig, SignalingProtocolOnConnect, SignalingStep,
    SignalingWaitStep, SignalingWireFormat, SinkCapabilities, SqsConfigEntry, SqsFifoGroup,
    SqsIngestMode, StartDomain, Statement, StopDomain, SubscriptionBinding,
    SubscriptionDeliveryBehavior, SubscriptionLiteral, SyslogConfigEntry, TransactionReportFormat,
    UncordonNode, UniquelyKindedModel, UploadResource, VhostTlsResource, WasmProcessorLimits,
    WasmRejectedStatePolicy, WebsocketsConfigEntry, WebsocketsIngestMode, WindowBound,
    WindowStateLimit, WireSchemaLookup, ZeroMqConfigEntry, ZeroMqIngestMode, default_relay_buffer,
};
pub use timestamp::{AtomicTimestamp, Timestamp, TimestampError};
pub use udf::{CreateUdf, UdfArgument, UdfLanguage, UdfReturn};
pub use wasm_state_generation::{
    InvalidWasmStateGeneration, WasmSavedStateRejection, WasmStateGeneration, WasmStateGenerations,
    WasmStateRecoveries, WasmStateRecovery, WasmStateRecoveryAdmission, WasmStateRecoveryOutcome,
    WasmStateReset, WasmStateResetPhase, WasmStateResetScope,
};
