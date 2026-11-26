//! The contract between the Nervix host and the connector crates it drives.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The ingestor and emitter contracts, including the rule a row or row request emitter divides
//!   mapped rows into requests by, the host handles a connector may call, and the value types that
//!   cross the boundary: a client's resolved configuration entries and the resource
//!   mounts they read files from, the deadline keys read from them with the one meaning every
//!   connector gives them, the TLS material and HTTP client settings built from those entries,
//!   service URL parsing, the parsed retry policy, physical deadlines and the actual-UTC
//!   read an ingestor stamps arrival with, the transport-header and declared-metadata traits an
//!   ingestor message implements, the ingest metadata row with its retained copy and transport
//!   position, and the confirmation window a brokered emitter awaits its broker's answers through.
//! - **Depends on.** The vocabulary, Arrow, `error-stack` and Tokio, the node's bounded executor a
//!   emitter host hands a connector, and the TLS, HTTP client, URL and template libraries a client
//!   configuration is built with.
//! - **Must not know.** Relays, branches, schedules, Models, the registry or the runtime. A
//!   connector receives a typed plan and one host handle, and reaches nothing past them. Its
//!   plan carries configuration only: the node's resolver, executor, outward TLS policy, pooled
//!   clients and recorders reach it through the handle, as the services it declares. It never
//!   resolves a resource mount: the host resolves one and hands in the resolved paths.
//!
//! Every external integration belongs in its own crate under `crates/connectors/`, which implements
//! the ingestor contract, the emitter contract, or both. A connector crate is an engine: the host drives
//! it, and it decides nothing about the graph. It names this crate, the vocabulary, Arrow,
//! `error-stack`, Tokio and its own driver, and never the server or another connector crate. The
//! host keeps task lifecycle, branch routing, acknowledgement tracking, quiesce, retry and flush
//! cadence, buffering, metrics and events.
//!
//! The server is the composition root and the only crate that names every connector. It implements
//! the host handles and converts Models into the typed plan each connector receives, so no
//! connector reads a Model. Capabilities the registry validates live in the vocabulary, where
//! validation reads them without naming a connector crate.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

mod client_config;
mod client_deadline;
mod confirmation_window;
mod connector_events;
mod domain_offsets;
mod host_service;
mod http_client;
mod ingest_metadata;
// Reached through its own path rather than re-exported here, so the owner the clock-boundary check
// declares stays the only file in this crate that names the capability constructor and UTC read.
mod emitter;
mod emitter_observation;
mod ingestor;
mod partition_inspector;
pub mod physical_time;
mod pooled_client;
mod row_requests;
mod service_url;
mod tls;

pub use client_config::{
    ClientConfigError, ClientConfigResult, ClientResourceMounts, ClientTlsPaths,
    OfflineClientConfigError, ParsedRetryPolicy, ResolvedClientConfig, client_config_value,
    client_tls_paths, next_retry_delay, optional_bool_client_config_value,
    optional_client_config_value, read_tls_file, render_client_config_template,
};
pub use client_deadline::{ConfiguredDeadline, DeadlineElapsed};
pub use confirmation_window::{
    BrokerAnswer, BrokerVerdict, ConfirmationWindow, ConfirmationWindowError,
};
pub use connector_events::{
    ConnectorEventCounter, ConnectorEventProvider, UndeclaredConnectorEvent,
};
pub use domain_offsets::{
    DomainOffsetError, DomainOffsetInitialization, DomainOffsetPosition, DomainOffsetResult,
    DomainOffsetServices, DomainOffsetStart, DomainOffsets,
};
pub use emitter::{
    AckConfirmation, BrokerPublishingMode, EmitterBoundedExecution, EmitterCommitReport,
    EmitterConnector, EmitterEventReporter, EmitterFinish, EmitterFinishError, EmitterFinishResult,
    EmitterGeneralErrorHandler, EmitterHost, EmitterHostServices, EmitterHttpRequest,
    EmitterLifecycle, EmitterLifecycleError, EmitterLifecycleResult, EmitterLifecycleStep,
    EmitterNameResolution, EmitterOpenError, EmitterOutwardTls, EmitterReadiness, EmitterRecord,
    EmitterRecordAcknowledgementServices, EmitterRecordAcknowledgements, EmitterRecordId,
    EmitterRecordPosition, EmitterResume, EmitterRetryDelay, EmitterRowRequest,
    EmitterStagedCommit, EmitterStagingDirectory, EmitterStartResult, EmitterTaskEnd,
    EmitterTransientErrorStatus, EmitterWriteError, EmitterWriteResult, HttpRequestEmitter,
    MappedEmitterCarrier, MappedEmitterMember, MappedEmitterRows, PerRecordOutcome,
    PerRecordOutcomeParts, PreparedRowRequest, PropertyValue, RecordDestination, RecordEmitter,
    RecordEnvelope, RecordKey, RecordProperty, RejectedEmitterMember, RowEmitter,
    RowRequestEmitter, RowRequestPreparation, TypedRecordHeader,
};
pub use emitter_observation::{EmitterPayloadMeasurement, EmitterPayloadObserver};
pub use host_service::HostServiceError;
pub use http_client::{DeclaredProxy, HttpClientConfig, HttpClientConfigError};
pub use ingest_metadata::{
    IngestMessageHeaders, IngestMessageMetadata, IngestMetadataFailure, IngestMetadataRow,
    IngestMetadataValues, IngestRowHeaders, IngestTransportPosition, NoIngestHeaders,
    NoIngestMetadata, RetainedIngestHeaders, RetainedIngestMetadata, RetainedIngestMetadataValue,
};
pub use ingestor::{
    BrokerIngestorConnector, IngestorAckPolicy, IngestorAcknowledgementOutcome,
    IngestorAcknowledgementSupport, IngestorBatch, IngestorBatchAcknowledgement,
    IngestorBatchAcknowledgementServices, IngestorBatchRequest, IngestorCapabilities,
    IngestorConnector, IngestorError, IngestorHost, IngestorHostServices, IngestorIntakeError,
    IngestorIntakeMessage, IngestorIntakeResult, IngestorLoopHost, IngestorLoopServices,
    IngestorMessage, IngestorPlan, IngestorPoll, IngestorPollMessage, IngestorReadAheadOverflow,
    IngestorReadBudget, IngestorReadCancellation, IngestorReadHold, IngestorReadOutcome,
    IngestorResult, IngestorResume, IngestorResumeOutcome, IngestorSettlement,
    IngestorSettlementOperation, IngestorSettlementOutcome, IngestorSuspendCause,
    PacedIngestorConnector,
};
pub use nervix_tls::OutwardTlsPolicy;
pub use partition_inspector::PartitionInspectorHost;
pub use pooled_client::{
    PooledClient, PooledClientHost, PooledClientLease, PooledClientWait, PooledClientWaits,
};
pub use row_requests::{
    ExceededLimit, MeasuredRequest, RowOversize, RowRequest, RowRequestLimits, RowRequests,
};
pub use service_url::{ServiceUrl, ServiceUrlError};
pub use tls::{RustlsClientConfigProvider, ServerCertificateTrust, TlsClientConfigError};

mod retry_settings;
pub use retry_settings::{
    ConnectorRetryJitterPercent, ConnectorRetryMultiplier, RetryBackoffSettings, RetrySettingsError,
};
