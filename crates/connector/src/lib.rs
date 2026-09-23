//! The contract between the Nervix host and the connector crates it drives.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The source and sink contracts, the host handles a connector may call, and the value
//!   types that cross the boundary: a client's resolved configuration entries and the resource
//!   mounts they read files from, the TLS material and HTTP client settings built from those
//!   entries, service URL parsing, the parsed retry policy, physical deadlines and the actual-UTC
//!   read a source stamps arrival with, the transport-header trait a source message implements,
//!   and the typed ingest metadata row with its Kafka, syslog and header scopes.
//! - **Depends on.** The vocabulary, Arrow, `error-stack` and Tokio, and the TLS, HTTP client, URL
//!   and template libraries a client configuration is built with.
//! - **Must not know.** Relays, branches, schedules, Models, the registry or the runtime. A
//!   connector receives a typed plan and host handles, and reaches nothing past them. It never
//!   resolves a resource mount: the host resolves one and hands in the resolved paths.
//!
//! Every external integration belongs in its own crate under `crates/connectors/`, which implements
//! the source contract, the sink contract, or both. A connector crate is an engine: the host drives
//! it, and it decides nothing about the graph. It names this crate, the vocabulary, Arrow,
//! `error-stack`, Tokio and its own driver, and never the server or another connector crate. The
//! host keeps task lifecycle, branch routing, acknowledgement tracking, quiesce, retry and flush
//! cadence, buffering, metrics and events.
//!
//! The server is the composition root and the only crate that names every connector. It implements
//! the host handles and converts Models into the typed plan each connector receives, so no
//! connector reads a Model. Capabilities the registry validates live in the vocabulary, where
//! validation reads them without naming a connector crate.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

mod client_config;
mod http_client;
mod ingest_metadata;
// Reached through its own path rather than re-exported here, so the owner the clock-boundary check
// declares stays the only file in this crate that names the capability constructor and UTC read.
pub mod physical_time;
mod service_url;
mod sink;
mod source;
mod tls;

pub use client_config::{
    ClientConfigError, ClientConfigResult, ClientResourceMounts, ClientTlsPaths, ParsedRetryPolicy,
    ResolvedClientConfig, client_config_value, client_tls_paths, next_retry_delay,
    optional_bool_client_config_value, optional_client_config_value, read_tls_file,
    render_client_config_template,
};
pub use http_client::{HttpClientConfig, HttpClientConfigError};
pub use ingest_metadata::{
    IngestMessageHeaders, IngestMetadataRow, NoIngestHeaders, RetainedIngestHeaders,
};
pub use service_url::{ServiceUrl, ServiceUrlError};
pub use sink::{
    AckConfirmation, BrokerPublishingMode, MappedSinkRows, PerRecordOutcome, PerRecordOutcomeParts,
    RecordSink, RejectedSinkRecord, RowSink, SinkAcknowledgementServices, SinkAcknowledgements,
    SinkCommitReport, SinkDeadline, SinkEventReporter, SinkGeneralErrorHandler, SinkHost,
    SinkHostServices, SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecord,
    SinkRecordPosition, SinkRetryDelay, SinkStagingDirectory, SinkStartError, SinkStartResult,
    SinkTransientErrorStatus,
};
pub use source::{
    BrokerSourceConnector, PacedSourceConnector, SourceAckPolicy, SourceAcknowledgement,
    SourceAcknowledgementOutcome, SourceAcknowledgementServices, SourceAcknowledgementSupport,
    SourceBatch, SourceBatchRequest, SourceCapabilities, SourceConnector, SourceError, SourceHost,
    SourceHostServices, SourceIntakeBatch, SourceIntakeError, SourceIntakeMessage,
    SourceIntakeMode, SourceIntakeOutcome, SourceIntakeResult, SourceMessage, SourceMetadataScope,
    SourcePlan, SourcePoll, SourcePollMessage, SourceResult, SourceResume,
};
pub use tls::{RustlsClientConfigSource, TlsClientConfigError, install_rustls_crypto_provider};
